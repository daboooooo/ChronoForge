"""Replay 重放（D05 §4，PIPELINE-001）。

职责：
- canonical 层重放：RawStore.iter_refs 重建 RawBatch → 复用 Validate /
  Normalize / Canonical / Quality / RunLog 五阶段（跳过 fetch/raw_append，
  不调 API）→ 走 run_log 记账返回 RunRow；cursor 不推进（checkpoint 保持
  现值，仅当终态 SUCCESS/PARTIAL_SUCCESS 时按原值幂等回写）。
- derived 层重放：依赖 FeatureEngine（QUERY-002 未落地）→ NotImplementedError。

偏差登记 DEVIATION-1（任务单裁决确认）：D05 §4 要求 replay 的 run_id 带
"R" 前缀，但 run_id 由 MetaStore.try_lock_dataset 内部生成（无前缀注入点），
replay 复用其返回的 run_id 与 dataset 锁，前缀语义登记为偏差。
"""

from __future__ import annotations

import json
import time
from typing import Literal

import structlog

from chronoforge.config.settings import Settings
from chronoforge.connectors.base import DataConnector, RawBatch
from chronoforge.pipeline.runner import (
    CanonicalStage,
    NormalizeStage,
    QualityStage,
    RunContext,
    RunLogStage,
    Stage,
    StageResult,
    ValidateStage,
    compose_run_row,
    finish_failed_run,
)
from chronoforge.pipeline.state import RunStatus
from chronoforge.storage.base import CanonicalStore
from chronoforge.storage.meta import MetaStore, RunRow
from chronoforge.storage.raw import RawStore

logger = structlog.get_logger()

# 重放阶段序列：跳过 fetch / raw_append（D05 §4），五阶段顺序与主流水线一致
_REPLAY_STAGES: tuple[Stage, ...] = (
    ValidateStage(),
    NormalizeStage(),
    CanonicalStage(),
    QualityStage(),
    RunLogStage(),
)


def replay(
    layer: Literal["canonical", "derived"],
    dataset_id: str,
    *,
    meta: MetaStore,
    raw_store: RawStore,
    canonical_store: CanonicalStore,
    connector: DataConnector,
    settings: Settings,
) -> RunRow:
    """重放指定层（D05 §4）。

    Args:
        layer: "canonical"（从 Raw 重放五阶段）或 "derived"
            （NotImplementedError，待 FeatureEngine）。
        dataset_id: 数据集 ID。
        meta: 元数据存储。
        raw_store: Raw 存储（iter_refs 读取源数据）。
        canonical_store: Canonical 存储。
        connector: 连接器（仅用其 normalize；fetch 不会被调用）。
        settings: 全局配置。

    Returns:
        终态 RunRow（SUCCESS / PARTIAL_SUCCESS）。

    Raises:
        NotImplementedError: layer == "derived"。
        Exception: 任一阶段失败 → finish_run(FAILED) 后原样重新 raise。
    """
    if layer == "derived":
        raise NotImplementedError("derived replay requires FeatureEngine (QUERY-002)")

    source_id = connector.source_id
    # DEVIATION-1：run_id 由 try_lock_dataset 生成（无 "R" 前缀），复用其锁
    lock = meta.try_lock_dataset(dataset_id, source_id=source_id)
    # SR-07：PENDING → RUNNING（与 PipelineRunner.run 同一状态机路径）
    meta.mark_run_running(lock.run_id)

    ctx = RunContext(
        run_id=lock.run_id,
        ingest_batch_id=lock.ingest_batch_id,
        source_id=source_id,
        dataset_id=dataset_id,
        connector=connector,
        raw_store=raw_store,
        canonical_store=canonical_store,
        meta=meta,
        settings=settings,
    )
    # cursor 不推进：checkpoint 现值同时作为 before / after
    current_cursor = meta.get_checkpoint(source_id, dataset_id)
    ctx._checkpoint_before = current_cursor
    ctx._final_cursor = current_cursor

    stage_results: list[StageResult] = []
    try:
        # 从 Raw 层重建批次（json 反序列化 payload；chunk=None 表示非拉取窗口）
        for ref in raw_store.iter_refs(source_id, dataset_id):
            batch = RawBatch(
                endpoint=ref.url,
                payload=json.loads(ref.payload.decode("utf-8")),
                raw_meta={"fetched_at": ref.fetched_at},
            )
            ctx._batches.append((None, batch))
            ctx._raw_refs.append((None, batch, ref))

        for stage in _REPLAY_STAGES:
            logger.info(
                "pipeline.stage",
                run_id=ctx.run_id,
                dataset_id=dataset_id,
                stage=stage.name,
                phase="start",
                replay=True,
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
                replay=True,
                input_count=result.input_count,
                output_count=result.output_count,
                error_count=result.error_count,
                latency_ms=result.latency_ms,
            )
    except Exception as exc:
        # 异常映射 → FAILED（聚合已有计数）；不推进 cursor，不归零熔断
        # （熔断计数属 PipelineRunner 实例内状态，replay 独立入口不涉及）
        finish_failed_run(ctx, stage_results, exc)
        raise

    status = ctx._run_status or RunStatus.SUCCESS.value
    row = compose_run_row(lock, ctx, status)
    logger.info(
        "replay.finish",
        run_id=ctx.run_id,
        dataset_id=dataset_id,
        layer=layer,
        status=status,
        input_count=row.input_count,
        output_count=row.output_count,
        checkpoint_after=row.checkpoint_after,
    )
    return row
