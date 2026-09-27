"""PipelineRunner 七阶段编排（D05 §1–§3，PIPELINE-001）。

职责：
- RunContext / StageResult 数据契约 + Stage 协议
- 七个内置 Stage（fetch→raw_append→validate→normalize→canonical→quality→runlog），
  顺序固定，禁止跨阶段调用
- PipelineRunner：数据集锁、熔断（3 连败 → 打开，冷却期 half-open 恢复，
  R2-01）、终态判定、run_many 聚合、run_log 全部观测列记账
- PipelineRunner.run_windowed：分窗口执行（READY-001 方案 B，SR-04
  内存治理——单 run 内存 O(窗口)，不随 backfill 总跨度增长）
- finish_failed_run / compose_run_row：异常路径记账辅助（replay 复用）

异常映射（D05 §1 失败语义列 + D01 §3 错误层级）：
- AuthError / SchemaError / ConfigError / QualityError / StorageError /
  ProviderError / 其他异常 → 聚合已有 StageResult 计数 → finish_run(FAILED)
  → 熔断计数 +1 → 重新 raise（调用方可见）
- (TransportError, RateLimitError, ProviderError) → chunk 级失败：该 chunk
  计失败、cursor 停留在最后成功 chunk 右界、后续 chunk 跳过；
  存在成功 chunk → PARTIAL_SUCCESS；首个 chunk 即失败（无可保留数据）→
  FAILED（决策 DEC-F0）

阶段间传递经 RunContext 内部缓冲（下划线前缀私有语义）：
- _job / _batches / _raw_refs / _records / _canonical_groups /
  _quality_flags / _stage_results / _counts / _checkpoint_before /
  _final_cursor / _run_status / _schema_version
"""

from __future__ import annotations

import json
import math
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import Literal, Protocol, cast, runtime_checkable

import structlog

from chronoforge.config.settings import Settings
from chronoforge.connectors.base import DataConnector, RawBatch
from chronoforge.connectors.errors import (
    ConfigError,
    ProviderError,
    QualityError,
    RateLimitError,
    SchemaError,
    StorageError,
    TransportError,
)
from chronoforge.connectors.retry import retry
from chronoforge.models.enums import CanonicalType
from chronoforge.pipeline.cursor import advance_chunks
from chronoforge.pipeline.state import RunStatus
from chronoforge.pipeline.windows import (
    AcquisitionJob,
    Chunk,
    ChunkResult,
    _parse_cursor,
    plan_chunks,
    resolve_window_span,
    windowed_base,
)
from chronoforge.quality import rules as quality_rules
from chronoforge.quality.report import GapContext, QualityFinding
from chronoforge.storage.base import CanonicalStore, natural_key
from chronoforge.storage.meta import MetaStore, RunRow
from chronoforge.storage.raw import RawRef, RawStore, _BatchItem

logger = structlog.get_logger()

# 熔断阈值（架构 08 §3：同 dataset 连续 FAILED 上限 N=3）
_CIRCUIT_THRESHOLD = 3
# cursor 终态集合：仅 SUCCESS/PARTIAL_SUCCESS 写 checkpoint（D05 §3 / TC-P-005）
_CURSOR_TERMINAL = frozenset(
    {RunStatus.SUCCESS.value, RunStatus.PARTIAL_SUCCESS.value}
)
# 分窗口循环继续执行下一窗口的窗口终态集合（READY-001 DEC-W4）：
# FAILED/CANCELLED 一律停止（FAILED 交熔断计数，CANCELLED 含熔断打开）
_WINDOW_CONTINUE = _CURSOR_TERMINAL
# 计数聚合键（run_log 观测列，审计 F-01/F-11）
_COUNT_KEYS = (
    "input_count",
    "output_count",
    "error_count",
    "warning_count",
    "request_count",
    "retry_count",
    "duplicate_count",
    "missing_count",
    "latency_ms",
    "chunk_success",
    "chunk_failed",
)


# ---------------------------------------------------------------------------
# 数据契约（D05 §1）
# ---------------------------------------------------------------------------


@dataclass
class RunContext:
    """单次 run 的上下文（D05 §1）。

    run_id / ingest_batch_id 由 PipelineRunner 在 try_lock_dataset 成功后回填；
    其余字段由 ctx_factory 构造时填充。下划线前缀字段为阶段间内部缓冲，
    不属于公共契约。
    """

    run_id: str
    ingest_batch_id: str
    source_id: str
    dataset_id: str
    connector: DataConnector
    raw_store: RawStore
    canonical_store: CanonicalStore
    meta: MetaStore
    settings: Settings
    # 审计 H-3：Runner 负责把 upsert 产生的 drift findings 落盘
    # （CanonicalStage 追加 → RunLogStage 经 meta.add_quality_flags 写入）
    drift_findings: list[QualityFinding] = field(default_factory=list)
    # ── 内部缓冲（私有语义）──
    _job: AcquisitionJob | None = None
    # fetch → raw_append：(chunk, batch)；replay 路径 chunk 为 None
    _batches: list[tuple[Chunk | None, RawBatch]] = field(default_factory=list)
    # raw_append → normalize：(chunk, batch, raw_ref) 保序配对
    _raw_refs: list[tuple[Chunk | None, RawBatch, RawRef]] = field(default_factory=list)
    # normalize → canonical：(类型名, model_dump 字典)
    _records: list[tuple[str, dict[str, object]]] = field(default_factory=list)
    # canonical → quality：(canonical_type, entity_id, records) 分组
    _canonical_groups: list[tuple[CanonicalType, str, list[dict[str, object]]]] = field(
        default_factory=list
    )
    # quality → runlog：规则 findings 转成的 flag dict
    _quality_flags: list[dict[str, object]] = field(default_factory=list)
    # runner 逐 stage 追加（runlog 聚合计数用）；_counts 为最终聚合结果
    _stage_results: list[StageResult] = field(default_factory=list)
    _counts: dict[str, int] = field(default_factory=dict)
    _checkpoint_before: str | None = None
    _final_cursor: str | None = None
    _run_status: str = ""
    _schema_version: str = "1.0"


@dataclass
class StageResult:
    """单 stage 执行结果（D05 §1，全部带默认值）。"""

    input_count: int = 0
    output_count: int = 0
    error_count: int = 0
    warning_count: int = 0
    # 观测计数（审计 F-01/F-11：howto §34 全字段，随 stage 累加并入 run_log）
    request_count: int = 0
    retry_count: int = 0
    duplicate_count: int = 0
    missing_count: int = 0
    latency_ms: int = 0
    checkpoint_before: str | None = None
    checkpoint_after: str | None = None
    chunk_success: int = 0
    chunk_failed: int = 0
    errors: list[str] = field(default_factory=list)  # 截断入 error_summary


class Stage(Protocol):
    """阶段协议（D05 §1）：name 属性 + execute。"""

    name: str

    def execute(self, ctx: RunContext) -> StageResult: ...


@runtime_checkable
class _ConnectorObs(Protocol):
    """连接器可选观测接口：连接器自行上报请求/重试计数（F-01 观测列）。

    实现了 request_count / retry_count 属性的连接器（如测试 FakeConnector、
    带 ratelimit 统计的真实连接器）由 FetchStage 读取增量入 run_log。
    """

    request_count: int
    retry_count: int


# ---------------------------------------------------------------------------
# 聚合辅助
# ---------------------------------------------------------------------------


def _aggregate(results: Sequence[StageResult]) -> dict[str, int]:
    """聚合 StageResult 的观测计数（run_log 记账用）。"""
    counts = {key: 0 for key in _COUNT_KEYS}
    for r in results:
        counts["input_count"] += r.input_count
        counts["output_count"] += r.output_count
        counts["error_count"] += r.error_count
        counts["warning_count"] += r.warning_count
        counts["request_count"] += r.request_count
        counts["retry_count"] += r.retry_count
        counts["duplicate_count"] += r.duplicate_count
        counts["missing_count"] += r.missing_count
        counts["latency_ms"] += r.latency_ms
        counts["chunk_success"] += r.chunk_success
        counts["chunk_failed"] += r.chunk_failed
    return counts


def _stage_errors(results: Sequence[StageResult]) -> list[str]:
    """收集全部 stage 错误消息（入 error_summary）。"""
    return [msg for r in results for msg in r.errors]


def _utcnow_iso() -> str:
    """当前时间（UTC naive）ISO 字符串。"""
    return datetime.now(UTC).replace(tzinfo=None).isoformat()


# ---------------------------------------------------------------------------
# Stage 1：FetchStage
# ---------------------------------------------------------------------------


class FetchStage:
    """Stage 1：窗口拆分 + 连接器逐 chunk 拉取（流式缓冲，D05 §1/§5.2）。

    - checkpoint 取自 meta.get_checkpoint → plan_chunks（now = job.end 或 utcnow）
    - 空 chunks 直接返回（空结果 = 正常 0 行）
    - 逐 chunk 调 connector.fetch 并即时缓冲（不积压 API 分页）
    - R2-04（D05 §1「chunk 级重试耗尽」）：TransportError/RateLimitError 经
      retry() 指数退避（消费 settings.retry_max，架构 08 §2 重试矩阵；
      RateLimitError 的 Retry-After 由 retry() 尊重），耗尽才计 chunk 失败；
      ProviderError 4xx 不重试直接穿透为 chunk 失败
    - 可重试类耗尽 → 该 chunk 计失败并停止后续 chunk；cursor 经
      advance_chunks 停留在最后成功 chunk 右界
    - AuthError/SchemaError/ConfigError → 直接 raise（run FAILED）
    - R2-05：历史区间空 chunk（chunk 整体早于 now − 2×chunk 跨度且 0 行）
      记 WARNING（可观测「HTTP 200 + 正常退出 + checkpoint 推进 + 数据缺失」）
    """

    name = "fetch"

    def execute(self, ctx: RunContext) -> StageResult:
        job = ctx._job
        if job is None:
            raise ConfigError(
                "RunContext._job is not set",
                context={"stage": self.name, "dataset_id": ctx.dataset_id},
            )
        result = StageResult()
        result.checkpoint_before = ctx.meta.get_checkpoint(ctx.source_id, ctx.dataset_id)
        ctx._checkpoint_before = result.checkpoint_before

        now = job.end or datetime.now(UTC).replace(tzinfo=None)
        chunks = plan_chunks(
            job, result.checkpoint_before, now,
            chunk_bars=ctx.settings.ohlcv_chunk_bars,
        )
        result.input_count = len(chunks)
        if not chunks:
            # 空窗口：cursor 不推进（保持原值）
            ctx._final_cursor = result.checkpoint_before
            result.checkpoint_after = result.checkpoint_before
            return result

        obs = ctx.connector if isinstance(ctx.connector, _ConnectorObs) else None
        request_before = obs.request_count if obs is not None else 0
        retry_before = obs.retry_count if obs is not None else 0

        retry_calls = 0

        def _fetch_chunk(chunk: Chunk) -> int:
            """单 chunk 重试单元：先缓冲本地，成功才并入 ctx（重试不残留半批）。"""
            nonlocal retry_calls
            retry_calls += 1
            batches: list[RawBatch] = []
            for batch in ctx.connector.fetch(chunk.request):
                batches.append(batch)
            ctx._batches.extend((chunk, b) for b in batches)
            return len(batches)

        fetch_with_retry = retry(max_retries=ctx.settings.retry_max)(_fetch_chunk)

        chunk_results: list[ChunkResult] = []
        for chunk in chunks:
            calls_before = retry_calls
            batch_count = 0
            try:
                batch_count = fetch_with_retry(chunk)
            except (TransportError, RateLimitError, ProviderError) as exc:
                # 可重试类耗尽：该 chunk 计失败，后续 chunk 跳过（D05 §1）
                result.retry_count += retry_calls - calls_before - 1
                result.chunk_failed += 1
                result.errors.append(
                    f"chunk {chunk.chunk_id} fetch failed: "
                    f"{type(exc).__name__}: {exc}"
                )
                chunk_results.append(
                    ChunkResult(chunk=chunk, success=False, record_count=0)
                )
                logger.warning(
                    "pipeline.chunk_failed",
                    run_id=ctx.run_id,
                    dataset_id=ctx.dataset_id,
                    chunk_id=chunk.chunk_id,
                    error=type(exc).__name__,
                )
                break
            result.retry_count += retry_calls - calls_before - 1
            result.chunk_success += 1
            result.output_count += batch_count
            chunk_results.append(
                ChunkResult(chunk=chunk, success=True, record_count=batch_count)
            )
            # R2-05：完全处于历史区间（早于 now − 2×chunk 跨度）且 0 行 →
            # WARNING 计数 + 结构化日志（不改变 SUCCESS 语义与 cursor 推进）
            if batch_count == 0 and chunk.end < now - 2 * (chunk.end - chunk.start):
                result.warning_count += 1
                logger.warning(
                    "pipeline.empty_history_chunk",
                    run_id=ctx.run_id,
                    dataset_id=ctx.dataset_id,
                    chunk_id=chunk.chunk_id,
                    chunk_start=chunk.start.isoformat(),
                    chunk_end=chunk.end.isoformat(),
                )

        if obs is not None:
            result.request_count = obs.request_count - request_before
            # 连接器内部重试与 stage 级重试（R2-04）累加入观测列
            result.retry_count += obs.retry_count - retry_before

        final_cursor, updates = advance_chunks(result.checkpoint_before, chunk_results)
        ctx._final_cursor = final_cursor
        result.checkpoint_after = final_cursor
        # 回退防护告警（SKIP + WARNING，TC-P-009）
        for update in updates:
            if update.warning:
                logger.warning(
                    "pipeline.cursor_skip",
                    run_id=ctx.run_id,
                    dataset_id=ctx.dataset_id,
                    action=update.action.value,
                    warning=update.warning,
                )
        return result


# ---------------------------------------------------------------------------
# Stage 2：RawAppendStage
# ---------------------------------------------------------------------------


class RawAppendStage:
    """Stage 2：RawBatch 落盘 JSONL 并回填 RawRef（D05 §1）。

    - payload 序列化：json.dumps(payload).encode("utf-8")
    - _BatchItem.url 存 batch.endpoint（replay 经 RawRef.url 重建 endpoint）
    - 返回的 RawRef 与 (chunk, RawBatch) 保序配对存入 ctx（Normalize 注入
      raw_record_id 用）
    - StorageError → raise（run FAILED）
    """

    name = "raw_append"

    def execute(self, ctx: RunContext) -> StageResult:
        result = StageResult()
        if not ctx._batches:
            return result
        items: list[_BatchItem] = []
        now = datetime.now(UTC).replace(tzinfo=None)
        for _chunk, batch in ctx._batches:
            item: _BatchItem = {
                "url": batch.endpoint,
                "payload": json.dumps(batch.payload).encode("utf-8"),
                "fetched_at": batch.raw_meta["fetched_at"],
                "ingest_batch_id": ctx.ingest_batch_id,
                "ingest_timestamp": now,
            }
            items.append(item)
        refs = ctx.raw_store.append(ctx.source_id, ctx.dataset_id, items)
        for (chunk, batch), ref in zip(ctx._batches, refs, strict=True):
            ctx._raw_refs.append((chunk, batch, ref))
        result.input_count = len(ctx._batches)
        result.output_count = len(refs)
        return result


# ---------------------------------------------------------------------------
# Stage 3：ValidateStage
# ---------------------------------------------------------------------------


class ValidateStage:
    """Stage 3：payload shape 校验（D05 §1，SchemaError 熔断 dataset）。

    payload 为 None / str / int / float / bool（非 JSON 容器）→ SchemaError
    （run FAILED + 熔断计数）；空 list/dict 合法（空结果正常）。P0 最小实现。
    """

    name = "validate"

    def execute(self, ctx: RunContext) -> StageResult:
        result = StageResult()
        result.input_count = len(ctx._batches)
        result.output_count = len(ctx._batches)
        for chunk, batch in ctx._batches:
            payload = batch.payload
            if not isinstance(payload, (list, dict)):
                raise SchemaError(
                    f"payload shape invalid: {type(payload).__name__}",
                    context={
                        "stage": self.name,
                        "dataset_id": ctx.dataset_id,
                        "chunk_id": chunk.chunk_id if chunk is not None else None,
                        "payload_type": type(payload).__name__,
                    },
                )
        return result


# ---------------------------------------------------------------------------
# Stage 4：NormalizeStage
# ---------------------------------------------------------------------------


class NormalizeStage:
    """Stage 4：connector.normalize → BaseRecord + provenance 注入（D05 §1）。

    - 逐 (chunk, batch, raw_ref) 调 connector.normalize(batch)；失败计
      error_count + errors，不中断
    - 成功记录注入 raw_record_id / ingest_timestamp（UTC naive）；
      schema_version 保持模型已有值
    - 错误率 = errors/total > settings.normalize_error_threshold（total>0）
      → QualityError
    """

    name = "normalize"

    def execute(self, ctx: RunContext) -> StageResult:
        result = StageResult()
        ingest_now = datetime.now(UTC).replace(tzinfo=None)
        for _chunk, batch, raw_ref in ctx._raw_refs:
            try:
                records = ctx.connector.normalize(batch)
            except Exception as exc:  # 记录级失败不中断（D05 §1）
                result.error_count += 1
                result.errors.append(
                    f"normalize failed for {raw_ref.raw_record_id}: "
                    f"{type(exc).__name__}: {exc}"
                )
                continue
            for rec in records:
                result.input_count += 1
                try:
                    rec.raw_record_id = raw_ref.raw_record_id
                    rec.ingest_timestamp = ingest_now
                    dump: dict[str, object] = rec.model_dump()
                    type_name = type(rec).__name__
                except Exception as exc:  # provenance 注入/序列化失败不中断
                    result.error_count += 1
                    result.errors.append(
                        f"provenance inject failed: {type(exc).__name__}: {exc}"
                    )
                    continue
                result.output_count += 1
                ctx._records.append((type_name, dump))
                schema_version = dump.get("schema_version")
                if isinstance(schema_version, str) and schema_version:
                    ctx._schema_version = schema_version

        total = result.input_count
        if total > 0 and result.error_count / total > ctx.settings.normalize_error_threshold:
            raise QualityError(
                f"normalize error rate {result.error_count}/{total} exceeds "
                f"threshold {ctx.settings.normalize_error_threshold}",
                context={"stage": self.name, "dataset_id": ctx.dataset_id},
            )
        return result


# ---------------------------------------------------------------------------
# Stage 5：CanonicalStage
# ---------------------------------------------------------------------------


class CanonicalStage:
    """Stage 5：按 (CanonicalType, entity_id) 分组 upsert（D05 §1/D03 §3）。

    - 类型推断：CanonicalType(类名)（27 类型类名 == 枚举名，设计既定）
    - entity_id = natural_key(ctype)[0] 列的值；缺失 → SchemaError
    - 聚合 UpsertStats：inserted→output、updated→duplicate、drifted→drift
      计数；drift findings 转 QualityFinding 追加到 ctx.drift_findings
      （审计 H-3：Runner 负责落盘）
    - StorageError → raise（run FAILED）
    """

    name = "canonical"

    def execute(self, ctx: RunContext) -> StageResult:
        result = StageResult()
        result.input_count = len(ctx._records)
        groups: dict[tuple[CanonicalType, str], list[dict[str, object]]] = {}
        for type_name, rec in ctx._records:
            ctype = self._resolve_type(type_name)
            entity_id = self._resolve_entity(ctype, rec)
            groups.setdefault((ctype, entity_id), []).append(rec)

        for (ctype, entity_id), records in groups.items():
            stats = ctx.canonical_store.upsert(records, ctype, entity_id)
            result.output_count += stats.inserted
            result.duplicate_count += stats.updated
            for finding in stats.drift_findings:
                ctx.drift_findings.append(
                    QualityFinding(
                        record_key=finding.record_key,
                        rule_id=finding.rule_id,
                        severity=cast(
                            "Literal['ERROR', 'WARNING', 'INFO']", finding.severity
                        ),
                        detail=json.dumps(finding.detail, sort_keys=True, default=str),
                    )
                )
            if stats.drifted:
                logger.warning(
                    "pipeline.drift_detected",
                    run_id=ctx.run_id,
                    dataset_id=ctx.dataset_id,
                    canonical_type=ctype.value,
                    count=stats.drifted,
                )

        ctx._canonical_groups = [
            (ctype, entity_id, records) for (ctype, entity_id), records in groups.items()
        ]
        return result

    @staticmethod
    def _resolve_type(type_name: str) -> CanonicalType:
        """类名 → CanonicalType（类名==枚举名为设计既定，违反即 SchemaError）。"""
        try:
            return CanonicalType(type_name)
        except ValueError as exc:
            raise SchemaError(
                f"unknown canonical type name: {type_name!r}",
                context={"type_name": type_name},
            ) from exc

    @staticmethod
    def _resolve_entity(ctype: CanonicalType, rec: dict[str, object]) -> str:
        """entity_id = natural_key 首列值；缺失/为空 → SchemaError。"""
        nk = natural_key(ctype)
        first = rec.get(nk[0]) if nk else None
        if first is None or first == "":
            raise SchemaError(
                f"natural key column {nk[0]!r} missing in record",
                context={"canonical_type": ctype.value},
            )
        return str(first)


# ---------------------------------------------------------------------------
# Stage 6：QualityStage
# ---------------------------------------------------------------------------


class QualityStage:
    """Stage 6：质量规则检查（D05 §1/D06）。

    - 按 canonical_type 分组调 quality.rules.run；GapContext 尽力构造
      （dataset_registry 查询，无行则 context=None，gap 规则自动跳过）
    - block_on 命中时 rules.run 抛 StorageError → 转 raise QualityError
    - findings 转 flag dict 缓冲（RunLogStage 统一落盘）；规则结果不阻断
    """

    name = "quality"

    def execute(self, ctx: RunContext) -> StageResult:
        result = StageResult()
        for ctype, _entity_id, records in ctx._canonical_groups:
            if not records:
                continue
            result.input_count += len(records)
            result.output_count += len(records)
            try:
                report = quality_rules.run(
                    records,
                    ctype,
                    context=self._build_gap_context(ctx),
                    block_on=ctx.settings.quality_block_on,
                )
            except StorageError as exc:
                # block_on 命中 → 转 QualityError（保持错误语义清晰）
                raise QualityError(
                    str(exc),
                    context={"stage": self.name, "dataset_id": ctx.dataset_id},
                ) from exc
            for finding in report.findings:
                ctx._quality_flags.append(
                    {
                        "record_key": finding.record_key,
                        "dataset_id": ctx.dataset_id,
                        "rule_id": finding.rule_id,
                        "severity": finding.severity,
                        "detail": finding.detail,
                        "raw_ref": finding.raw_ref or "",
                        "payload_digest": finding.payload_digest or "",
                        "run_id": ctx.run_id,
                    }
                )
            result.warning_count += report.warning_count
        return result

    @staticmethod
    def _build_gap_context(ctx: RunContext) -> GapContext | None:
        """尽力构造 GapContext：dataset_registry 无行 → None。"""
        row = ctx.meta.connection.execute(
            "SELECT continuity_model, frequency FROM dataset_registry "
            "WHERE dataset_id = ?",
            (ctx.dataset_id,),
        ).fetchone()
        if row is None or row[0] is None:
            return None
        frequency = str(row[1]) if row[1] is not None else None
        return GapContext(
            dataset_id=ctx.dataset_id,
            continuity_model=str(row[0]),
            frequency=frequency,
        )


# ---------------------------------------------------------------------------
# Stage 7：RunLogStage
# ---------------------------------------------------------------------------


class RunLogStage:
    """Stage 7：run_log 终态记账 + checkpoint 终态更新（D05 §1/§3）。

    1) meta.add_quality_flags（规则 findings + ctx.drift_findings）
    2) 聚合全部 StageResult 计数 → meta.finish_run
    3) 终态 SUCCESS/PARTIAL_SUCCESS 且有 final cursor → meta.save_checkpoint
       （FAILED/CANCELLED 不写 cursor，TC-P-005）
    4) meta.derive_dataset_status（只读推导）经 structlog 记录
    """

    name = "runlog"

    def execute(self, ctx: RunContext) -> StageResult:
        result = StageResult()

        # 1) quality flags 落盘（规则 findings + upsert drift findings）
        flags: list[dict[str, object]] = list(ctx._quality_flags)
        for finding in ctx.drift_findings:
            flags.append(
                {
                    "record_key": finding.record_key,
                    "dataset_id": ctx.dataset_id,
                    "rule_id": finding.rule_id,
                    "severity": finding.severity,
                    "detail": finding.detail,
                    "raw_ref": finding.raw_ref or "",
                    "payload_digest": finding.payload_digest or "",
                    "run_id": ctx.run_id,
                }
            )
        ctx.meta.add_quality_flags(flags)
        result.output_count = len(flags)

        # 2) 终态判定 + 聚合计数
        status = self._decide_status(ctx)
        ctx._run_status = status
        counts = _aggregate(ctx._stage_results)
        ctx._counts = counts
        checkpoint_after = ctx._final_cursor if status in _CURSOR_TERMINAL else None
        error_summary = "; ".join(_stage_errors(ctx._stage_results)) or None

        ctx.meta.finish_run(
            ctx.run_id,
            status,
            source_id=ctx.source_id or ctx.connector.source_id,
            input_count=counts["input_count"],
            output_count=counts["output_count"],
            error_count=counts["error_count"],
            warning_count=counts["warning_count"],
            request_count=counts["request_count"],
            retry_count=counts["retry_count"],
            duplicate_count=counts["duplicate_count"],
            missing_count=counts["missing_count"],
            latency_ms=counts["latency_ms"],
            checkpoint_before=ctx._checkpoint_before,
            checkpoint_after=checkpoint_after,
            chunk_success=counts["chunk_success"],
            chunk_failed=counts["chunk_failed"],
            error_summary=error_summary,
            schema_version=ctx._schema_version,
            code_version=ctx.settings.version,
            ingest_batch_id=ctx.ingest_batch_id,
        )

        # 3) cursor 仅终态更新：SUCCESS/PARTIAL_SUCCESS 且有 cursor 才写
        if status in _CURSOR_TERMINAL and ctx._final_cursor is not None:
            ctx.meta.save_checkpoint(ctx.source_id, ctx.dataset_id, ctx._final_cursor)

        # 4) dataset 状态推导（只读）→ 结构化日志
        derived = ctx.meta.derive_dataset_status(ctx.dataset_id)
        logger.info(
            "pipeline.dataset_status",
            run_id=ctx.run_id,
            dataset_id=ctx.dataset_id,
            status=derived,
        )
        return result

    @staticmethod
    def _decide_status(ctx: RunContext) -> str:
        """终态判定（D05 §3 + 决策 DEC-F0）。

        - 任一 stage errors/error_count>0 或 chunk_failed>0 → PARTIAL_SUCCESS
        - 例外（DEC-F0）：chunk_success==0 且 chunk_failed>0（首个 chunk 即
          失败，无可保留数据）→ FAILED
        - 否则 → SUCCESS
        """
        has_problem = False
        chunk_success = 0
        chunk_failed = 0
        for r in ctx._stage_results:
            if r.error_count > 0 or r.errors:
                has_problem = True
            chunk_success += r.chunk_success
            chunk_failed += r.chunk_failed
        if chunk_failed > 0:
            if chunk_success == 0:
                return RunStatus.FAILED.value
            return RunStatus.PARTIAL_SUCCESS.value
        if has_problem:
            return RunStatus.PARTIAL_SUCCESS.value
        return RunStatus.SUCCESS.value


# ---------------------------------------------------------------------------
# PipelineRunner（D05 §2）
# ---------------------------------------------------------------------------


class PipelineRunner:
    """七阶段流水线编排器（D05 §2）。

    进程内单线程执行（SQLite 单写者）；dataset 锁经 MetaStore.try_lock_dataset；
    熔断状态持久化于 checkpoints 旁路字段（circuit_open/consecutive_failed，
    D05 §3 + 审计 SR-03：跨 job/进程累计，CLI 每 dataset 新建 runner 不再归零）。
    """

    def __init__(self, ctx_factory: Callable[[AcquisitionJob], RunContext]) -> None:
        self._ctx_factory = ctx_factory
        # 七阶段固定顺序（D05 §1，禁止跨阶段调用）
        self.stages: list[Stage] = [
            FetchStage(),
            RawAppendStage(),
            ValidateStage(),
            NormalizeStage(),
            CanonicalStage(),
            QualityStage(),
            RunLogStage(),
        ]

    def run(self, job: AcquisitionJob) -> RunRow:
        """执行单 job：锁 → 七阶段 → 终态记账 → RunRow（D05 §2）。"""
        ctx = self._ctx_factory(job)
        ctx._job = job
        dataset_id = ctx.dataset_id
        source_id = ctx.connector.source_id

        # 1. 熔断检查（SR-03：持久化状态，跨 job/进程生效；R2-01 冷却期）：
        #    circuit_open=1 且打开时长 < circuit_cooldown_s → 直接 CANCELLED；
        #    打开时长 ≥ 冷却期 → 放行本次探测 run（half-open）
        if ctx.meta.is_circuit_open(
            source_id, dataset_id, cooldown_seconds=ctx.settings.circuit_cooldown_s
        ):
            lock = ctx.meta.try_lock_dataset(dataset_id, source_id=source_id)
            ctx.meta.finish_run(
                lock.run_id,
                RunStatus.CANCELLED.value,
                source_id=source_id,
                error_summary="circuit open",
            )
            logger.warning(
                "pipeline.circuit_open",
                run_id=lock.run_id,
                dataset_id=dataset_id,
            )
            return replace(
                lock,
                status=RunStatus.CANCELLED.value,
                ended_at=_utcnow_iso(),
                error_summary="circuit open",
            )

        # 2. 数据集锁（锁冲突 StorageError 直接传播——不吞）
        lock = ctx.meta.try_lock_dataset(dataset_id, source_id=source_id)
        ctx.run_id = lock.run_id
        ctx.ingest_batch_id = lock.ingest_batch_id
        # SR-07：PENDING → RUNNING（D05 §3 状态机；区分「从未开始」与
        # 「执行中死亡」，crash 后由 startup_repair 按状态对账）
        ctx.meta.mark_run_running(ctx.run_id)

        # R2-01（审计 2026-09-22）half-open 探测标记：上方硬拦截未触发而
        # 打开标志仍置位（无 cooldown 查询）= 打开时长已超冷却期 → 本次为
        # 探测 run；成功经 reset_circuit 解除，失败经 record_circuit_failure
        # 刷新 circuit_opened_at 重新计时
        if ctx.meta.is_circuit_open(source_id, dataset_id):
            logger.warning(
                "pipeline.circuit_half_open",
                run_id=ctx.run_id,
                dataset_id=dataset_id,
            )

        stage_results: list[StageResult] = []
        try:
            for stage in self.stages:
                logger.info(
                    "pipeline.stage",
                    run_id=ctx.run_id,
                    dataset_id=dataset_id,
                    stage=stage.name,
                    phase="start",
                )
                t0 = time.perf_counter()
                result = stage.execute(ctx)
                result.latency_ms = int((time.perf_counter() - t0) * 1000)
                stage_results.append(result)
                ctx._stage_results.append(result)
                logger.info(
                    "pipeline.stage",
                    run_id=ctx.run_id,
                    dataset_id=dataset_id,
                    stage=stage.name,
                    phase="end",
                    input_count=result.input_count,
                    output_count=result.output_count,
                    error_count=result.error_count,
                    latency_ms=result.latency_ms,
                )
        except (KeyboardInterrupt, SystemExit) as exc:
            # 2a. 用户中断（SR-02）：BaseException 不被下述 except Exception
            #     捕获，此前直接穿透 → run_log 滞留 PENDING、锁不释放。
            #     D05 §3 状态机「用户取消 → CANCELLED」：写 CANCELLED 终态
            #     （锁随终态释放）后重新抛出；不递增熔断计数。
            _finish_aborted_run(
                ctx, stage_results, exc, RunStatus.CANCELLED.value
            )
            raise

        except Exception as exc:
            # 3. 异常映射 → FAILED（聚合已有计数）→ 熔断 +1 → 重新 raise
            finish_failed_run(ctx, stage_results, exc)
            ctx.meta.record_circuit_failure(
                source_id, dataset_id, threshold=_CIRCUIT_THRESHOLD
            )
            raise

        status = ctx._run_status or RunStatus.SUCCESS.value
        if status == RunStatus.FAILED.value:
            # DEC-F0：全 0 chunk 成功且 fetch 失败 → FAILED
            # （RunLogStage 已 finish_run，这里只计熔断并返回 FAILED RunRow）
            ctx.meta.record_circuit_failure(
                source_id, dataset_id, threshold=_CIRCUIT_THRESHOLD
            )
            row = compose_run_row(lock, ctx, status)
            logger.warning(
                "run.finish",
                run_id=ctx.run_id,
                dataset_id=dataset_id,
                status=status,
                output_count=row.output_count,
                chunk_failed=row.chunk_failed,
            )
            return row

        # 4. 成功路径：熔断计数归零并解除打开状态（SR-03）→ RunRow
        ctx.meta.reset_circuit(source_id, dataset_id)
        row = compose_run_row(lock, ctx, status)
        logger.info(
            "run.finish",
            run_id=ctx.run_id,
            dataset_id=dataset_id,
            status=status,
            input_count=row.input_count,
            output_count=row.output_count,
            error_count=row.error_count,
            warning_count=row.warning_count,
            duplicate_count=row.duplicate_count,
            checkpoint_after=row.checkpoint_after,
        )
        return row

    def run_many(self, jobs: list[AcquisitionJob]) -> list[RunRow]:
        """按 priority 降序逐个执行；单 job 失败不中断（D05 §2）。

        聚合终态经 structlog "pipeline.run_many" 事件上报：
        全部 SUCCESS → SUCCESS；全部 FAILED/CANCELLED → FAILED；混合 → PARTIAL。
        异常 job 无 RunRow 可返回则跳过（FAILED 路径 run() 正常返回 RunRow）。
        """
        rows: list[RunRow] = []
        for job in sorted(jobs, key=lambda j: j.priority, reverse=True):
            try:
                rows.append(self.run(job))
            except Exception as exc:
                logger.warning(
                    "pipeline.run_many_job_failed",
                    dataset_id=job.dataset_id,
                    error=f"{type(exc).__name__}: {exc}",
                )
        statuses = [r.status for r in rows]
        if rows and all(s == RunStatus.SUCCESS.value for s in statuses):
            aggregate = "SUCCESS"
        elif rows and all(
            s in (RunStatus.FAILED.value, RunStatus.CANCELLED.value) for s in statuses
        ):
            aggregate = "FAILED"
        else:
            aggregate = "PARTIAL"
        logger.info(
            "pipeline.run_many",
            total=len(jobs),
            succeeded=len(rows),
            aggregate_status=aggregate,
        )
        return rows

    def run_windowed(
        self,
        job: AcquisitionJob,
        *,
        window_seconds: int,
        progress: Callable[[int, int, RunRow], None] | None = None,
    ) -> list[RunRow]:
        """分窗口执行（READY-001 方案 B：SR-04 内存治理，审计 2026-09-21）。

        每次只回填一个时间窗（跨度 = window_seconds 向上对齐到
        chunk_size 整数倍），外层循环经 checkpoint 推进窗口；单 run 内存
        O(窗口)，不随 backfill 总跨度线性增长。七阶段数据流与单 run 语义
        完全不变，全部约束逐条满足：

        - DEC-W1 锁语义：窗口循环在锁外层——每个窗口是一次独立 run()
          （一次 try_lock / 一个 run_id / 一次 finish_run）；SR-03 熔断
          按 run 计数、SR-06 finish_run 写回均保持不变
        - DEC-W2 全窗口 diff 类（fred/sec）不拆分（span=None → 直接 run(job)）
        - DEC-W3 窗口统一以 mode="backfill" 的显式 [start, end) 子任务
          执行：与全跨度 plan_chunks 的 chunk 序列逐 chunk 一致（窗口
          边界对齐 chunk 边界，不重不漏；缝间仅标准 overlap 重复，
          由 natural key upsert 幂等收敛）
        - DEC-W4 续传与停止：窗口 SUCCESS/PARTIAL_SUCCESS 且 cursor 有
          推进 → 下一窗口从新 checkpoint 起（PARTIAL 的失败 chunk 由
          下一窗口自然重试）；FAILED/CANCELLED/无推进 → 停止；重入时
          checkpoint 已覆盖请求跨度 → 幂等返回 []（剩余窗口可恢复）
        - 失败恢复语义不变：单窗口失败 → 该窗口 FAILED + cursor 不推进
          失败 chunk（既有 checkpoint 对齐）

        一次逻辑 backfill 产生 N 行 run_log（方案 B 语义变化，任务单已列明）。

        Args:
            job: 获取任务（backfill 用 job.start；incremental 从 checkpoint 续传）
            window_seconds: 窗口跨度上限（秒，>0）
            progress: 可选进度回调，每个窗口结束后以
                (已完成窗口数, 预估总窗口数, 该窗口 RunRow) 调用一次。
                预估总数 = ceil((final_end - base) / span)，cursor 推进小于
                窗口跨度时实际窗口数可能超过预估（调用方展示时需容忍
                done > total）；回调异常会中断窗口循环（checkpoint 逐窗口
                持久化，重入可续传）。span=None（fred/sec 全窗口 diff）
                路径不触发回调。

        Returns:
            各窗口 RunRow（按执行顺序）；无可回填起点 → 单次 run(job)
            （保持既有空 SUCCESS 语义）；checkpoint 已覆盖 → []

        Raises:
            ConfigError: window_seconds <= 0
        """
        if window_seconds <= 0:
            raise ConfigError(
                "window_seconds must be positive",
                context={
                    "window_seconds": window_seconds,
                    "dataset_id": job.dataset_id,
                },
            )
        # 探针 ctx：读 meta/source（不执行任何 stage；每窗口由 run() 另建 ctx）
        probe = self._ctx_factory(job)
        meta = probe.meta
        source_id = probe.source_id
        dataset_id = probe.dataset_id
        final_end = job.end or datetime.now(UTC).replace(tzinfo=None)

        span = resolve_window_span(
            job.dataset_id, window_seconds, job.params,
            chunk_bars=probe.settings.ohlcv_chunk_bars,
        )
        if span is None:
            # DEC-W2：全窗口 diff 类不拆分（fred/sec 语义依赖全窗口 diff）
            return [self.run(job)]

        cursor = meta.get_checkpoint(source_id, dataset_id)
        base = windowed_base(job, cursor)
        if base is None:
            # 空任务（无 cursor 且无 start）：退化为单 run，保持既有语义
            return [self.run(job)]
        if base >= final_end:
            # DEC-W4 断点续传：checkpoint 已覆盖请求跨度 → 幂等跳过
            return []

        # 预估总窗口数（cursor 推进小于窗口跨度时实际数可能超过预估）
        total_estimate = max(1, math.ceil((final_end - base) / timedelta(seconds=span)))
        rows: list[RunRow] = []
        while base < final_end:
            window_end = min(base + timedelta(seconds=span), final_end)
            sub_job = replace(job, start=base, end=window_end, mode="backfill")
            row = self.run(sub_job)
            rows.append(row)
            if progress is not None:
                progress(len(rows), total_estimate, row)
            if row.status not in _WINDOW_CONTINUE:
                break
            next_cursor = row.checkpoint_after
            if next_cursor is None:
                break
            next_base = _parse_cursor(next_cursor)
            if next_base <= base:
                # 无推进（回退防护 SKIP 等）→ 停止，防死循环
                break
            base = next_base
        return rows


# ---------------------------------------------------------------------------
# 共享辅助（replay.py 复用）
# ---------------------------------------------------------------------------


def finish_failed_run(
    ctx: RunContext, stage_results: Sequence[StageResult], exc: BaseException
) -> None:
    """异常路径统一记账：finish_run(FAILED)（PipelineRunner 与 replay 共用）。

    聚合已有 StageResult 计数；chunk_success/chunk_failed 取 Fetch 已计值；
    checkpoint_after 恒为 None（FAILED 不写 cursor）。
    """
    _finish_aborted_run(ctx, stage_results, exc, RunStatus.FAILED.value)


def _finish_aborted_run(
    ctx: RunContext,
    stage_results: Sequence[StageResult],
    exc: BaseException,
    status: str,
) -> None:
    """异常/中断路径共享记账体（finish_failed_run 与 SR-02 取消路径共用）。

    status 为 FAILED（可重试类耗尽等失败）或 CANCELLED（用户中断）；
    两者均不推进 cursor（checkpoint_after=None，TC-P-005）。
    """
    counts = _aggregate(stage_results)
    msgs = _stage_errors(stage_results)
    msgs.append(f"{type(exc).__name__}: {exc}")
    summary = "; ".join(msgs)
    if status == RunStatus.CANCELLED.value:
        logger.warning(
            "pipeline.run_cancelled",
            run_id=ctx.run_id,
            dataset_id=ctx.dataset_id,
            reason=type(exc).__name__,
        )
    else:
        logger.error(
            "pipeline.run_failed",
            run_id=ctx.run_id,
            dataset_id=ctx.dataset_id,
            error=summary[:500],
        )
    ctx.meta.finish_run(
        ctx.run_id,
        status,
        source_id=ctx.source_id or ctx.connector.source_id,
        input_count=counts["input_count"],
        output_count=counts["output_count"],
        error_count=counts["error_count"],
        warning_count=counts["warning_count"],
        request_count=counts["request_count"],
        retry_count=counts["retry_count"],
        duplicate_count=counts["duplicate_count"],
        missing_count=counts["missing_count"],
        latency_ms=counts["latency_ms"],
        checkpoint_before=ctx._checkpoint_before,
        checkpoint_after=None,
        chunk_success=counts["chunk_success"],
        chunk_failed=counts["chunk_failed"],
        error_summary=summary,
        schema_version=ctx._schema_version,
        code_version=ctx.settings.version,
        ingest_batch_id=ctx.ingest_batch_id,
    )


def compose_run_row(lock: RunRow, ctx: RunContext, status: str) -> RunRow:
    """由 try_lock 返回行 + ctx 聚合计数构造终态 RunRow。

    MetaStore 无 get_run 方法（既定），Runner 自行从 stage_results 聚合构造
    （任务单伪代码 get_run 的偏差裁决）。
    """
    counts = ctx._counts
    return replace(
        lock,
        status=status,
        ended_at=_utcnow_iso(),
        input_count=counts.get("input_count", 0),
        output_count=counts.get("output_count", 0),
        error_count=counts.get("error_count", 0),
        warning_count=counts.get("warning_count", 0),
        request_count=counts.get("request_count", 0),
        retry_count=counts.get("retry_count", 0),
        duplicate_count=counts.get("duplicate_count", 0),
        missing_count=counts.get("missing_count", 0),
        latency_ms=counts.get("latency_ms", 0),
        checkpoint_before=ctx._checkpoint_before,
        checkpoint_after=(
            ctx._final_cursor if status in _CURSOR_TERMINAL else None
        ),
        chunk_success=counts.get("chunk_success", 0),
        chunk_failed=counts.get("chunk_failed", 0),
        error_summary=("; ".join(_stage_errors(ctx._stage_results)) or None),
        schema_version=ctx._schema_version,
        code_version=ctx.settings.version,
    )
