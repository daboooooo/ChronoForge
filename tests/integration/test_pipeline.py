"""PIPELINE-001 集成测试 — PipelineRunner 七阶段编排 + 状态机 + replay。

依据：D09 §3 TC-P-001~011 + 任务单扩展要求（锁互斥 / drift 落盘 / DEC-F0）。
真实组件：RawStore / CanonicalStoreImpl / MetaStore / 质量规则引擎；
FakeConnector 不触网，按窗口生成小时级 OHLCV payload（半开区间 [start, end)，
与 cursor exclusive 语义一致，chunk 间无重叠 → 快乐路径零质量 findings）。

窗口基准（binance_spot_klines：overlap=3600s，chunk=86400s）：
- backfill start=2024-01-01 00:00, end=2024-01-03 23:00
  → 3 chunks：[12-31 23:00,01-01 23:00) / [01-01 23:00,01-02 23:00) /
    [01-02 23:00,01-03 23:00)，每 chunk 24 行，全量 72 行
- CHUNK1_END = 2024-01-01T23:00:00（第 1 chunk 右界）
- FULL_CURSOR = 2024-01-03T23:00:00（全量成功终态 cursor）
"""

from __future__ import annotations

import json
import tracemalloc
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pyarrow.parquet as pq
import pytest
from structlog.testing import capture_logs

from chronoforge.config.settings import Settings
from chronoforge.connectors.base import FetchRequest, RawBatch
from chronoforge.connectors.errors import (
    AuthError,
    ConfigError,
    ProviderError,
    QualityError,
    RateLimitError,
    SchemaError,
    StorageError,
    TransportError,
)
from chronoforge.models.market import OHLCV, Interval
from chronoforge.pipeline.replay import replay
from chronoforge.pipeline.runner import PipelineRunner, RunContext
from chronoforge.pipeline.windows import WINDOW_CONFIG, AcquisitionJob
from chronoforge.storage.canonical import CanonicalStoreImpl
from chronoforge.storage.meta import MetaStore
from chronoforge.storage.raw import RawStore

# ── 基准常量 ──────────────────────────────────────────────────────────

DS_ID = "binance_spot_klines"
SOURCE_ID = "binance"
MARKET_ID = "BINANCE:BTCUSDT:SPOT"
JOB_START = datetime(2024, 1, 1, 0, 0)
JOB_END = datetime(2024, 1, 3, 23, 0)
CHUNK1_END = "2024-01-01T23:00:00"
FULL_CURSOR = "2024-01-03T23:00:00"
# 全量成功 run 的聚合计数（fetch/raw_append/validate 各 3 + 72×3 行级 stage）
FULL_INPUT = 225
FULL_OUTPUT = 225

_STAGE_NAMES = ("fetch", "raw_append", "validate", "normalize", "canonical", "quality", "runlog")

# Settings 的 env 覆盖优先级高于 kwargs（_load_from_env 用 values.update），
# harness 构造 Settings 前必须删除 autouse fixture / 环境注入的相关变量。
_ENV_VARS = (
    "CHRONOFORGE_ENV",
    "CHRONOFORGE_DATA_DIR",
    "CHRONOFORGE_META_DIR",
    "CHRONOFORGE_QUALITY_BLOCK",
    "CHRONOFORGE_NORMALIZE_ERROR_THRESHOLD",
    "CHRONOFORGE_SEC_CONTACT",
    "CHRONOFORGE_VERSION",
    "CHRONOFORGE_RETRY_MAX",
    "CHRONOFORGE_CIRCUIT_COOLDOWN_S",
)


# ── FakeConnector（DataConnector 协议实现，不触网） ────────────────────


class FakeConnector:
    """测试假连接器：按窗口生成小时级 OHLCV payload，多注入点驱动失败场景。

    注入点（chunk 序号 1-based，按 chunk 窗口编号；同一 chunk 的重试尝试
    共享该序号——R2-04 后 FetchStage 对可重试异常做 chunk 级退避重试，
    chunk_failures 表示「该 chunk 全部尝试均失败」，fail_attempts 表示
    「该 chunk 前 N 次尝试失败、其后成功」）：
    - chunk_failures: 该 chunk 的 fetch 抛指定异常（可重试类 → 退避重试
      耗尽后 chunk 级失败）
    - fail_attempts: 该 chunk 前 N 次尝试抛 TransportError（瞬态故障语义）
    - bad_payload_chunks: payload=None（ValidateStage SchemaError）
    - fail_normalize_chunks: 该 chunk 的 normalize 抛 RuntimeError（记录级失败）
    - retry_transport_chunk: 该 chunk 首次尝试传输失败、连接器内部重试成功
    - empty_payload: 所有 chunk 返回 0 行（空结果）
    - empty_batches_chunks: 该 chunk 的 fetch 不产出任何批次（0 batch，
      R2-05 历史空 chunk 语义）
    - price_offset: 价格偏移（drift 场景）
    - pad_bytes: payload 附加填充字节（READY-001 内存断言用）
    """

    source_id = SOURCE_ID

    def __init__(
        self,
        *,
        chunk_failures: dict[int, type[BaseException]] | None = None,
        fail_attempts: dict[int, int] | None = None,
        bad_payload_chunks: set[int] | None = None,
        fail_normalize_chunks: set[int] | None = None,
        retry_transport_chunk: int | None = None,
        empty_payload: bool = False,
        empty_batches_chunks: set[int] | None = None,
        market_id: str = MARKET_ID,
        price_offset: float = 0.0,
        pad_bytes: int = 0,
    ) -> None:
        self.chunk_failures = dict(chunk_failures or {})
        self.fail_attempts = dict(fail_attempts or {})
        self.bad_payload_chunks = set(bad_payload_chunks or set())
        self.fail_normalize_chunks = set(fail_normalize_chunks or set())
        self.retry_transport_chunk = retry_transport_chunk
        self.empty_payload = empty_payload
        self.empty_batches_chunks = set(empty_batches_chunks or set())
        self.market_id = market_id
        self.price_offset = price_offset
        self.pad_bytes = pad_bytes
        self.fetch_calls: list[FetchRequest] = []
        self.request_count = 0
        self.retry_count = 0
        self._retried = False
        self._chunk_no = 0
        self._attempt_no = 0
        self._last_request: FetchRequest | None = None

    def start_new_run(self) -> None:
        """重置 chunk 序号（多次 run 复用同一连接器时注入按 run 内序号生效）。"""
        self._chunk_no = 0
        self._attempt_no = 0
        self._last_request = None

    # ── 协议方法 ──

    def capabilities(self) -> object:
        return None

    def health(self) -> object:
        return None

    def discover(self) -> list[dict[str, str]]:
        return []

    def fetch(self, request: FetchRequest) -> Iterator[RawBatch]:
        self.fetch_calls.append(request)
        self.request_count += 1
        # R2-04：同一 chunk 的重试尝试（同 request）共享 chunk 序号
        if request != self._last_request:
            self._chunk_no += 1
            self._attempt_no = 0
            self._last_request = request
        self._attempt_no += 1
        chunk_no = self._chunk_no
        if self.retry_transport_chunk == chunk_no and not self._retried:
            # TC-P-011h：连接器内部重试语义（首次尝试失败 → 重试成功）
            self._retried = True
            self.retry_count += 1
            self.request_count += 1
        transient = self.fail_attempts.get(chunk_no, 0)
        if self._attempt_no <= transient:
            raise TransportError(
                f"injected transient attempt {self._attempt_no}/{transient}",
                {"chunk_no": chunk_no},
            )
        failure = self.chunk_failures.get(chunk_no)
        if failure is not None:
            raise failure(f"injected {failure.__name__}", {"chunk_no": chunk_no})
        if chunk_no in self.empty_batches_chunks:
            return  # 0 batch：连接器对空窗口不产出任何批次（R2-05）
        rows: list[list[object]] = []
        if not self.empty_payload:
            assert request.start is not None and request.end is not None
            cur = request.start
            while cur < request.end:  # 半开区间 [start, end)
                rows.append(self._hour_row(cur))
                cur += timedelta(hours=1)
        payload: object
        if chunk_no in self.bad_payload_chunks:
            payload = None  # shape 违规（非 list/dict）
        else:
            payload = {"chunk_no": chunk_no, "rows": rows}
            if self.pad_bytes:
                payload["pad"] = "x" * self.pad_bytes
        yield RawBatch(endpoint=f"https://api.test/{request.dataset_id}", payload=payload)

    def normalize(self, raw: RawBatch) -> list[Any]:
        payload = raw.payload
        if payload["chunk_no"] in self.fail_normalize_chunks:
            raise RuntimeError(f"injected normalize failure chunk {payload['chunk_no']}")
        out: list[Any] = []
        for row in payload["rows"]:
            ts = datetime.fromisoformat(str(row[0]))
            out.append(
                OHLCV(
                    schema_version="1.0",
                    source=self.source_id,
                    source_id="BTCUSDT",
                    source_timestamp=ts,
                    ingest_timestamp=datetime.now(UTC).replace(tzinfo=None),
                    raw_record_id="pending",  # NormalizeStage 覆写为真实 RawRef id
                    market_id=self.market_id,
                    event_time=ts,
                    interval=Interval._1H,
                    open=float(row[1]),
                    high=float(row[2]),
                    low=float(row[3]),
                    close=float(row[4]),
                    volume=float(row[5]),
                )
            )
        return out

    def validate(self, records: list[Any]) -> object:
        return None

    def checkpoint_from(self, raw: RawBatch | list[Any]) -> str | None:
        return None

    # ── 内部 ──

    def _hour_row(self, ts: datetime) -> list[object]:
        seed = 50000.0 + ts.day * 100.0 + ts.hour + self.price_offset
        return [ts.isoformat(), seed, seed + 150.0, seed - 50.0, seed + 50.0, 10.0 + ts.hour]


# ── 测试环境搭建 ──────────────────────────────────────────────────────


def _register_dataset(meta: MetaStore, dataset_id: str) -> None:
    """注册 source_registry / dataset_registry 行（EVENT_BASED + 1h）。"""
    conn = meta.connection
    conn.execute(
        "INSERT OR IGNORE INTO source_registry "
        "(source_id, display_name, access_type, base_url, rate_limit_json, license) "
        "VALUES (?, 'Binance', 'PUBLIC', 'https://api.test', '{}', 'MIT')",
        (SOURCE_ID,),
    )
    conn.execute(
        "INSERT OR REPLACE INTO dataset_registry "
        "(dataset_id, source_id, canonical_type, entity_id, params_json, frequency, "
        "continuity_model, status, created_at, revision_supported) "
        "VALUES (?, ?, 'OHLCV', 'BTCUSDT', ?, '1h', 'EVENT_BASED', 'UNKNOWN', '2024-01-01', 0)",
        (dataset_id, SOURCE_ID, json.dumps({"symbol": "BTCUSDT", "interval": "1m"})),
    )
    conn.commit()


def _harness(
    tmp_stores,
    monkeypatch,
    name: str,
    connector: FakeConnector | None = None,
    connectors: dict[str, FakeConnector] | None = None,
    **settings_kwargs: object,
):
    """搭建 runner + 真实存储环境（每测试独立子目录，互不干扰）。

    返回 SimpleNamespace(runner, meta, raw, canonical, settings, ctxs,
    connectors, data_dir, meta_dir)；ctxs 捕获每次 run 的 RunContext，
    供断言 StageResult 全字段。
    """
    for var in _ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    data_dir = tmp_stores.data_dir / name
    meta_dir = tmp_stores.meta_dir / name
    data_dir.mkdir(parents=True, exist_ok=True)
    meta_dir.mkdir(parents=True, exist_ok=True)

    conns = connectors if connectors is not None else {DS_ID: connector or FakeConnector()}
    settings = Settings(
        env="test",
        data_dir=data_dir,
        meta_dir=meta_dir,
        sec_contact_email="research@example.com",
        **settings_kwargs,
    )
    meta = MetaStore(str(meta_dir))
    meta.migrate()
    raw = RawStore(str(data_dir))
    canonical = CanonicalStoreImpl(str(data_dir))
    for ds in conns:
        _register_dataset(meta, ds)

    ctxs: list[Any] = []

    def _ctx_factory(job: AcquisitionJob) -> Any:
        conn = conns[job.dataset_id]
        ctx = SimpleNamespace(
            run_id="",
            ingest_batch_id="",
            source_id=conn.source_id,
            dataset_id=job.dataset_id,
            connector=conn,
            raw_store=raw,
            canonical_store=canonical,
            meta=meta,
            settings=settings,
            drift_findings=[],
            _job=None,
            _batches=[],
            _raw_refs=[],
            _records=[],
            _canonical_groups=[],
            _quality_flags=[],
            _stage_results=[],
            _counts={},
            _checkpoint_before=None,
            _final_cursor=None,
            _run_status="",
            _schema_version="1.0",
        )
        ctxs.append(ctx)
        return ctx

    runner = PipelineRunner(_ctx_factory)  # type: ignore[arg-type]
    return SimpleNamespace(
        runner=runner,
        meta=meta,
        raw=raw,
        canonical=canonical,
        settings=settings,
        ctxs=ctxs,
        connectors=conns,
        data_dir=data_dir,
        meta_dir=meta_dir,
    )


def _make_job(dataset_id: str = DS_ID, *, priority: int = 0) -> AcquisitionJob:
    """标准 3-chunk backfill 任务（2024-01-01 00:00 → 2024-01-03 23:00）。"""
    return AcquisitionJob(
        dataset_id=dataset_id,
        start=JOB_START,
        end=JOB_END,
        mode="backfill",
        priority=priority,
        params={"symbol": "BTCUSDT", "interval": "1m"},
    )


# ── 断言辅助 ──────────────────────────────────────────────────────────


def _stage_results(ctx: Any) -> dict[str, Any]:
    """RunContext._stage_results → {stage_name: StageResult}（顺序固定）。"""
    return dict(zip(_STAGE_NAMES, ctx._stage_results, strict=True))


def _run_log_row(meta: MetaStore, run_id: str) -> dict[str, Any]:
    """按 run_id 读取 run_log 行为字典。"""
    cur = meta.connection.execute("SELECT * FROM run_log WHERE run_id = ?", (run_id,))
    row = cur.fetchone()
    assert row is not None, f"run_log 缺少 run_id={run_id}"
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row, strict=True))


def _canonical_rows(data_dir: Path) -> list[dict[str, Any]]:
    """读取 OHLCV entity 下全部 parquet 行（按 event_time 排序）。"""
    base = data_dir / "canonical" / "OHLCV" / f"entity={MARKET_ID}"
    rows: list[dict[str, Any]] = []
    if not base.exists():
        return rows
    for f in sorted(base.rglob("*.parquet")):
        rows.extend(pq.read_table(str(f)).to_pylist())
    rows.sort(key=lambda r: str(r["event_time"]))
    return rows


def _canonical_values(rows: list[dict[str, Any]]) -> list[tuple[Any, ...]]:
    """提取业务值列（TC-P-006 值列逐字节一致断言用）。"""
    return [
        (
            r["market_id"],
            str(r["event_time"]),
            str(r["interval"]),
            r["open"],
            r["high"],
            r["low"],
            r["close"],
            r["volume"],
        )
        for r in rows
    ]


class _SpyStage:
    """TC-P-001 spy：记录 execute 调用顺序并委托内部 stage。"""

    def __init__(self, inner: Any, calls: list[str]) -> None:
        self._inner = inner
        self._calls = calls
        self.name = inner.name

    def execute(self, ctx: Any) -> Any:
        self._calls.append(self._inner.name)
        return self._inner.execute(ctx)


def _full_run_harness(tmp_stores, monkeypatch, name: str):
    """搭建环境并完成一次全量成功 run（72 行、cursor=FULL_CURSOR）。"""
    h = _harness(tmp_stores, monkeypatch, name, connector=FakeConnector())
    row = h.runner.run(_make_job())
    assert row.status == "SUCCESS"
    assert h.meta.get_checkpoint(SOURCE_ID, DS_ID) == FULL_CURSOR
    return h


def _scenario_rollback_skip(h) -> list[dict[str, Any]]:
    """回退场景：重跑同窗口 → 新 cursor ≤ 旧 → SKIP + WARNING，checkpoint 不变。"""
    with capture_logs() as logs:
        row = h.runner.run(_make_job())
    assert row.status == "SUCCESS"
    assert h.meta.get_checkpoint(SOURCE_ID, DS_ID) == FULL_CURSOR
    skips = [e for e in logs if e.get("event") == "pipeline.cursor_skip"]
    assert skips, "应发出回退防护 WARNING"
    assert skips[0]["action"] == "skip"
    return skips


def _scenario_lost_rebuild(h) -> None:
    """丢失场景：DELETE checkpoints → rebuild_checkpoints 恢复 → raw 重放重建数据。"""
    h.meta.connection.execute("DELETE FROM checkpoints WHERE dataset_id = ?", (DS_ID,))
    h.meta.connection.commit()
    assert h.meta.get_checkpoint(SOURCE_ID, DS_ID) is None
    # 先从 run_log（最新 SUCCESS 行的 checkpoint_after + source_id lineage）重建
    h.meta.rebuild_checkpoints(DS_ID)
    assert h.meta.get_checkpoint(SOURCE_ID, DS_ID) == FULL_CURSOR
    # raw 重放重建数据（不依赖 cursor、不调 API）
    row = replay(
        "canonical",
        DS_ID,
        meta=h.meta,
        raw_store=h.raw,
        canonical_store=h.canonical,
        connector=h.connectors[DS_ID],
        settings=h.settings,
    )
    assert row.status == "SUCCESS"
    assert h.meta.get_checkpoint(SOURCE_ID, DS_ID) == FULL_CURSOR  # cursor 不推进


def _scenario_corrupt_rebuild(h) -> None:
    """损坏场景：'not-a-date' → incremental run FAILED → rebuild → 重跑成功。"""
    h.meta.save_checkpoint(SOURCE_ID, DS_ID, "not-a-date")
    job_inc = AcquisitionJob(DS_ID, start=None, end=JOB_END, mode="incremental")
    with pytest.raises(ValueError, match="invalid cursor"):
        h.runner.run(job_inc)
    db = _run_log_row(h.meta, h.ctxs[-1].run_id)
    assert db["status"] == "FAILED"
    assert "invalid cursor" in str(db["error_summary"])
    h.meta.rebuild_checkpoints(DS_ID)
    assert h.meta.get_checkpoint(SOURCE_ID, DS_ID) == FULL_CURSOR
    row = h.runner.run(job_inc)
    assert row.status == "SUCCESS"


# ── TC-P-001：七阶段顺序（spy 装配断言） ───────────────────────────────


def test_tcp001_stage_order_and_success(tmp_stores, monkeypatch) -> None:
    """TC-P-001：spy 断言七阶段按固定顺序执行；全量成功 72 行。"""
    h = _harness(tmp_stores, monkeypatch, "tcp001")
    calls: list[str] = []
    h.runner.stages = [_SpyStage(s, calls) for s in h.runner.stages]

    with capture_logs() as logs:
        row = h.runner.run(_make_job())

    assert calls == list(_STAGE_NAMES)
    start_events = [
        e["stage"] for e in logs if e.get("event") == "pipeline.stage" and e.get("phase") == "start"
    ]
    assert start_events == list(_STAGE_NAMES)

    assert row.status == "SUCCESS"
    assert row.checkpoint_before is None
    assert row.checkpoint_after == FULL_CURSOR
    assert row.input_count == FULL_INPUT
    assert row.output_count == FULL_OUTPUT
    assert row.request_count == 3
    assert row.retry_count == 0
    assert row.duplicate_count == 0
    assert row.chunk_success == 3
    assert row.chunk_failed == 0
    rows = _canonical_rows(h.data_dir)
    assert len(rows) == 72


# ── TC-P-002：各错误类 → 终态映射表逐行断言 ────────────────────────────

_FAILED_CASES = [
    ("AuthError", {"chunk_failures": {1: AuthError}}, {}, AuthError),
    ("ConfigError", {"chunk_failures": {1: ConfigError}}, {}, ConfigError),
    ("StorageError", {"chunk_failures": {1: StorageError}}, {}, StorageError),
    ("SchemaError", {"bad_payload_chunks": {1}}, {}, SchemaError),
    # 错误率 1/48 > 阈值 0.0 → QualityError
    (
        "QualityError",
        {"fail_normalize_chunks": {2}},
        {"normalize_error_threshold": 0.0},
        QualityError,
    ),
]


@pytest.mark.parametrize(("label", "conn_kwargs", "settings_kwargs", "exc_cls"), _FAILED_CASES)
def test_tcp002_failed_mapping(
    tmp_stores, monkeypatch, label: str, conn_kwargs: dict, settings_kwargs: dict, exc_cls: type
) -> None:
    """TC-P-002（FAILED 行）：Auth/Config/Storage/Schema/Quality → FAILED + raise。"""
    h = _harness(
        tmp_stores, monkeypatch, f"tcp002_{label}", connector=FakeConnector(**conn_kwargs),
        **settings_kwargs,
    )
    with pytest.raises(exc_cls):
        h.runner.run(_make_job())
    db = _run_log_row(h.meta, h.ctxs[0].run_id)
    assert db["status"] == "FAILED"
    assert db["error_summary"] is not None
    assert db["checkpoint_after"] is None  # FAILED 不写 cursor


@pytest.mark.parametrize("exc_cls", [TransportError, RateLimitError, ProviderError])
def test_tcp002_chunk_level_partial(tmp_stores, monkeypatch, exc_cls: type) -> None:
    """TC-P-002（PARTIAL 行）：可重试类 chunk 级失败 → PARTIAL_SUCCESS（不 raise）。"""
    h = _harness(
        tmp_stores, monkeypatch, f"tcp002_chunk_{exc_cls.__name__}",
        connector=FakeConnector(chunk_failures={2: exc_cls}),
    )
    row = h.runner.run(_make_job())
    assert row.status == "PARTIAL_SUCCESS"
    assert row.chunk_success == 1 and row.chunk_failed == 1
    assert row.checkpoint_after == CHUNK1_END


# ── DEC-F0：首个 chunk 即失败 → FAILED（任务单裁决确认） ────────────────


def test_dec_f0_first_chunk_failure_returns_failed(tmp_stores, monkeypatch) -> None:
    """DEC-F0：chunk_success==0 且 chunk_failed>0 → FAILED（不 raise，返回 RunRow）。"""
    h = _harness(
        tmp_stores,
        monkeypatch,
        "dec_f0",
        connector=FakeConnector(chunk_failures={1: TransportError}),
    )
    row = h.runner.run(_make_job())  # 不 raise
    assert row.status == "FAILED"
    assert row.chunk_success == 0 and row.chunk_failed == 1
    db = _run_log_row(h.meta, row.run_id)
    assert db["status"] == "FAILED"
    assert "TransportError" in str(db["error_summary"])
    assert db["checkpoint_after"] is None
    assert h.meta.get_checkpoint(SOURCE_ID, DS_ID) is None


# ── TC-P-003：PARTIAL 保留成功部分 ────────────────────────────────────


def test_tcp003_partial_preserves_successful_part(tmp_stores, monkeypatch) -> None:
    """TC-P-003：第 2 chunk 失败 → PARTIAL；chunk1 的 24 行已落盘可保留。"""
    h = _harness(
        tmp_stores,
        monkeypatch,
        "tcp003",
        connector=FakeConnector(chunk_failures={2: TransportError}),
    )
    row = h.runner.run(_make_job())
    assert row.status == "PARTIAL_SUCCESS"
    assert len(_canonical_rows(h.data_dir)) == 24  # chunk1 数据保留
    assert h.meta.get_checkpoint(SOURCE_ID, DS_ID) == CHUNK1_END
    db = _run_log_row(h.meta, row.run_id)
    assert db["status"] == "PARTIAL_SUCCESS"


# ── TC-P-004：熔断 3 连败 ─────────────────────────────────────────────


def test_tcp004_circuit_breaker_after_three_failures(tmp_stores, monkeypatch) -> None:
    """TC-P-004：连续 3 次 FAILED 后第 4 次 run 直接 CANCELLED（circuit open）。"""
    h = _harness(
        tmp_stores,
        monkeypatch,
        "tcp004",
        connector=FakeConnector(chunk_failures={1: TransportError}),
    )
    for _ in range(3):
        h.connectors[DS_ID].start_new_run()
        assert h.runner.run(_make_job()).status == "FAILED"

    h.connectors[DS_ID].start_new_run()
    calls_before = len(h.connectors[DS_ID].fetch_calls)
    row4 = h.runner.run(_make_job())
    assert row4.status == "CANCELLED"
    assert row4.error_summary == "circuit open"
    # 第 4 次未进入 stages（R2-04 后失败 run 含重试调用，故用增量断言）
    assert len(h.connectors[DS_ID].fetch_calls) == calls_before
    db = _run_log_row(h.meta, row4.run_id)
    assert db["status"] == "CANCELLED"

    row5 = h.runner.run(_make_job())  # 熔断保持直至成功重置
    assert row5.status == "CANCELLED"


# ── TC-P-005：cursor 仅终态更新 ───────────────────────────────────────


def test_tcp005_cursor_updated_only_on_terminal(tmp_stores, monkeypatch) -> None:
    """TC-P-005：FAILED/CANCELLED 不写 cursor；SUCCESS 才推进。"""
    # 1) 异常 FAILED：既有 checkpoint 保持原值
    h1 = _harness(
        tmp_stores,
        monkeypatch,
        "tcp005_auth",
        connector=FakeConnector(chunk_failures={1: AuthError}),
    )
    h1.meta.save_checkpoint(SOURCE_ID, DS_ID, "2024-01-01T00:00:00")
    with pytest.raises(AuthError):
        h1.runner.run(_make_job())
    assert h1.meta.get_checkpoint(SOURCE_ID, DS_ID) == "2024-01-01T00:00:00"
    db = _run_log_row(h1.meta, h1.ctxs[0].run_id)
    assert db["status"] == "FAILED"
    assert db["checkpoint_after"] is None
    assert db["checkpoint_before"] == "2024-01-01T00:00:00"

    # 2) DEC-F0 FAILED：无既有 checkpoint → 仍不写
    h2 = _harness(
        tmp_stores,
        monkeypatch,
        "tcp005_decf0",
        connector=FakeConnector(chunk_failures={1: TransportError}),
    )
    row = h2.runner.run(_make_job())
    assert row.status == "FAILED"
    assert row.checkpoint_after is None
    assert h2.meta.get_checkpoint(SOURCE_ID, DS_ID) is None


# ── TC-P-006：replay ──────────────────────────────────────────────────


def test_tcp006_replay_canonical_value_identical(tmp_stores, monkeypatch) -> None:
    """TC-P-006：canonical replay 不调 API，重放后业务值列与原一致，cursor 不推进。"""
    h = _full_run_harness(tmp_stores, monkeypatch, "tcp006")
    before = _canonical_rows(h.data_dir)
    assert len(before) == 72
    fetch_calls_before = len(h.connectors[DS_ID].fetch_calls)

    row = replay(
        "canonical",
        DS_ID,
        meta=h.meta,
        raw_store=h.raw,
        canonical_store=h.canonical,
        connector=h.connectors[DS_ID],
        settings=h.settings,
    )

    assert row.status == "SUCCESS"
    assert len(h.connectors[DS_ID].fetch_calls) == fetch_calls_before  # 不调 API
    after = _canonical_rows(h.data_dir)
    assert len(after) == len(before) == 72
    assert _canonical_values(before) == _canonical_values(after)  # 值列逐字节一致
    assert row.checkpoint_after == FULL_CURSOR  # cursor 不推进
    db = _run_log_row(h.meta, row.run_id)
    assert db["status"] == "SUCCESS"
    assert db["checkpoint_after"] == FULL_CURSOR


def test_tcp006_replay_derived_not_implemented(tmp_stores, monkeypatch) -> None:
    """TC-P-006：derived 层重放 → NotImplementedError（FeatureEngine 未落地）。"""
    h = _full_run_harness(tmp_stores, monkeypatch, "tcp006_derived")
    with pytest.raises(NotImplementedError, match="FeatureEngine"):
        replay(
            "derived",
            DS_ID,
            meta=h.meta,
            raw_store=h.raw,
            canonical_store=h.canonical,
            connector=h.connectors[DS_ID],
            settings=h.settings,
        )


# ── TC-P-007：run_many 部分失败聚合 ───────────────────────────────────


def test_tcp007_run_many_priority_and_partial_aggregate(tmp_stores, monkeypatch) -> None:
    """TC-P-007：priority 降序执行；单 job 异常不中断；聚合 PARTIAL/FAILED。"""
    ds2, ds3, ds4 = "tcp007_alpha", "tcp007_beta", "tcp007_gamma"
    for ds in (ds2, ds3, ds4):
        monkeypatch.setitem(
            WINDOW_CONFIG, ds, {"overlap_seconds": 3600, "chunk_size_seconds": 86400}
        )
    h = _harness(
        tmp_stores,
        monkeypatch,
        "tcp007",
        connectors={
            DS_ID: FakeConnector(),
            ds2: FakeConnector(market_id="BINANCE:ETHUSDT:SPOT"),
            ds3: FakeConnector(
                market_id="BINANCE:SOLUSDT:SPOT", chunk_failures={1: TransportError}
            ),
            ds4: FakeConnector(market_id="BINANCE:XRPUSDT:SPOT", chunk_failures={1: AuthError}),
        },
    )

    # 第一次：3 job，priority 10/5/1 → ds1, ds2, ds3(ds3 DEC-F0 FAILED 不 raise)
    jobs = [_make_job(ds3, priority=1), _make_job(DS_ID, priority=10), _make_job(ds2, priority=5)]
    with capture_logs() as logs:
        rows = h.runner.run_many(jobs)
    assert [r.dataset_id for r in rows] == [DS_ID, ds2, ds3]  # priority 降序执行顺序
    assert [r.status for r in rows] == ["SUCCESS", "SUCCESS", "FAILED"]
    agg = [e for e in logs if e.get("event") == "pipeline.run_many"]
    assert agg and agg[0]["aggregate_status"] == "PARTIAL"

    # 第二次：AuthError job 抛异常 → 被吞掉不中断，其余 job 正常
    with capture_logs() as logs2:
        rows2 = h.runner.run_many([_make_job(ds4, priority=100), _make_job(DS_ID, priority=1)])
    assert [r.dataset_id for r in rows2] == [DS_ID]  # ds4 异常被隔离
    assert rows2[0].status == "SUCCESS"
    assert any(e.get("event") == "pipeline.run_many_job_failed" for e in logs2)


# ── TC-P-008：空结果 = SUCCESS（0 行） ────────────────────────────────


def test_tcp008_empty_result_success(tmp_stores, monkeypatch) -> None:
    """TC-P-008：空窗口（无 chunks）与空 payload（0 行）均为 SUCCESS。"""
    # (a) incremental 且无 cursor 无 start → plan_chunks 返回 []
    h1 = _harness(tmp_stores, monkeypatch, "tcp008_window")
    row = h1.runner.run(AcquisitionJob(DS_ID, mode="incremental"))
    assert row.status == "SUCCESS"
    assert row.input_count == 0 and row.output_count == 0
    assert h1.connectors[DS_ID].fetch_calls == []  # 未发起任何请求
    assert _canonical_rows(h1.data_dir) == []

    # (b) 空 payload（每 chunk 0 行）
    h2 = _harness(
        tmp_stores, monkeypatch, "tcp008_payload", connector=FakeConnector(empty_payload=True)
    )
    row2 = h2.runner.run(_make_job())
    assert row2.status == "SUCCESS"
    assert len(_canonical_rows(h2.data_dir)) == 0


# ── TC-P-009：cursor 三态恢复（回退 / 丢失 / 损坏） ─────────────────────


def test_tcp009_rollback_skip_with_warning(tmp_stores, monkeypatch) -> None:
    """TC-P-009 回退：新 cursor ≤ 旧 → 跳过更新 + WARNING（TC-P-011d 同场景）。"""
    h = _full_run_harness(tmp_stores, monkeypatch, "tcp009_rollback")
    _scenario_rollback_skip(h)


def test_tcp009_lost_cursor_rebuild_from_run_log(tmp_stores, monkeypatch) -> None:
    """TC-P-009 丢失：DELETE checkpoints → raw 重放重建 + rebuild_checkpoints。"""
    h = _full_run_harness(tmp_stores, monkeypatch, "tcp009_lost")
    _scenario_lost_rebuild(h)


def test_tcp009_corrupt_cursor_rebuild_and_rerun(tmp_stores, monkeypatch) -> None:
    """TC-P-009 损坏：'not-a-date' → incremental FAILED → rebuild → 重跑成功。"""
    h = _full_run_harness(tmp_stores, monkeypatch, "tcp009_corrupt")
    _scenario_corrupt_rebuild(h)


# ── TC-P-010：chunk 语义全字段记账 ────────────────────────────────────


def test_tcp010_chunk_semantics_full_accounting(tmp_stores, monkeypatch) -> None:
    """TC-P-010：3-chunk 第 2 chunk 失败 → chunk_failed=1、cursor=第 1 chunk 右界、
    终态 PARTIAL；7 个 StageResult 与 run_log 全观测列逐项断言。"""
    h = _harness(
        tmp_stores,
        monkeypatch,
        "tcp010",
        connector=FakeConnector(chunk_failures={2: TransportError}),
    )
    row = h.runner.run(_make_job())
    assert row.status == "PARTIAL_SUCCESS"

    sr = _stage_results(h.ctxs[0])
    assert len(sr) == 7
    fetch = sr["fetch"]
    assert (fetch.input_count, fetch.output_count) == (3, 1)
    assert fetch.chunk_success == 1 and fetch.chunk_failed == 1
    # R2-04：失败 chunk 含 1 初次 + retry_max 次重试
    assert fetch.request_count == 2 + h.settings.retry_max
    assert fetch.retry_count == h.settings.retry_max
    assert fetch.checkpoint_before is None
    assert fetch.checkpoint_after == CHUNK1_END
    assert len(fetch.errors) == 1 and "TransportError" in fetch.errors[0]

    assert (sr["raw_append"].input_count, sr["raw_append"].output_count) == (1, 1)
    assert (sr["validate"].input_count, sr["validate"].output_count) == (1, 1)
    assert (sr["normalize"].input_count, sr["normalize"].output_count) == (24, 24)
    assert (sr["canonical"].input_count, sr["canonical"].output_count) == (24, 24)
    assert sr["canonical"].duplicate_count == 0
    assert (sr["quality"].input_count, sr["quality"].output_count) == (24, 24)
    for r in sr.values():
        assert r.latency_ms >= 0
        assert r.error_count == 0

    # run_log 全观测列（stage 聚合）
    assert row.input_count == 77
    assert row.output_count == 75  # fetch 仅缓冲 chunk1 的 1 个批次
    assert row.error_count == 0
    assert row.request_count == 2 + h.settings.retry_max
    assert row.retry_count == h.settings.retry_max
    assert row.duplicate_count == 0
    assert row.missing_count == 0
    assert row.latency_ms >= 0
    assert row.chunk_success == 1 and row.chunk_failed == 1
    assert row.checkpoint_before is None
    assert row.checkpoint_after == CHUNK1_END
    assert row.error_summary is not None and "TransportError" in row.error_summary
    assert row.schema_version == "1.0"

    # cursor = 第 1 chunk 右界（exclusive）；chunk1 数据保留
    assert h.meta.get_checkpoint(SOURCE_ID, DS_ID) == CHUNK1_END
    assert len(_canonical_rows(h.data_dir)) == 24


# ── TC-P-011：幂等 8 场景 ─────────────────────────────────────────────


def test_tcp011a_full_duplicate_idempotent(tmp_stores, monkeypatch) -> None:
    """TC-P-011a 全重复：同任务重跑 → inserted=0 / updated=72，终态行数不变。"""
    h = _full_run_harness(tmp_stores, monkeypatch, "tcp011a")
    can1 = _stage_results(h.ctxs[0])["canonical"]
    assert (can1.output_count, can1.duplicate_count) == (72, 0)

    row2 = h.runner.run(_make_job())
    assert row2.status == "SUCCESS"
    can2 = _stage_results(h.ctxs[1])["canonical"]
    assert (can2.output_count, can2.duplicate_count) == (0, 72)
    assert row2.checkpoint_after == FULL_CURSOR  # 相等 → SKIP，checkpoint 不变
    assert len(_canonical_rows(h.data_dir)) == 72


def test_tcp011b_partial_duplicate_extension(tmp_stores, monkeypatch) -> None:
    """TC-P-011b 部分重复：先短窗（2 chunk）再全量 → inserted=24 / updated=48。"""
    h = _harness(tmp_stores, monkeypatch, "tcp011b")
    job_short = AcquisitionJob(
        DS_ID, start=JOB_START, end=datetime(2024, 1, 2, 23, 0), mode="backfill"
    )
    row1 = h.runner.run(job_short)
    assert row1.status == "SUCCESS"
    can1 = _stage_results(h.ctxs[0])["canonical"]
    assert (can1.output_count, can1.duplicate_count) == (48, 0)

    row2 = h.runner.run(_make_job())
    assert row2.status == "SUCCESS"
    can2 = _stage_results(h.ctxs[1])["canonical"]
    assert (can2.output_count, can2.duplicate_count) == (24, 48)
    assert len(_canonical_rows(h.data_dir)) == 72


def test_tcp011c_overlapping_windows(tmp_stores, monkeypatch) -> None:
    """TC-P-011c 重叠窗口：短窗（2 chunk）+ 重叠起点回填 → 无重复终态（72 行）。"""
    h = _harness(tmp_stores, monkeypatch, "tcp011c")
    job_short = AcquisitionJob(
        DS_ID, start=JOB_START, end=datetime(2024, 1, 2, 23, 0), mode="backfill"
    )
    assert h.runner.run(job_short).status == "SUCCESS"  # 48 行 [12-31 23:00, 01-02 23:00)
    # 与前一窗口重叠的回填：[01-01 23:00, 01-03 23:00) → 24 行重复 + 24 行新增
    job_overlap = AcquisitionJob(
        DS_ID, start=datetime(2024, 1, 2, 0, 0), end=JOB_END, mode="backfill"
    )
    row2 = h.runner.run(job_overlap)
    assert row2.status == "SUCCESS"
    can2 = _stage_results(h.ctxs[1])["canonical"]
    assert (can2.output_count, can2.duplicate_count) == (24, 24)
    assert len(_canonical_rows(h.data_dir)) == 72


def test_tcp011d_cursor_rollback_idempotent(tmp_stores, monkeypatch) -> None:
    """TC-P-011d cursor 回退：幂等重跑不回退 checkpoint。"""
    h = _full_run_harness(tmp_stores, monkeypatch, "tcp011d")
    _scenario_rollback_skip(h)


def test_tcp011e_cursor_lost_rebuild(tmp_stores, monkeypatch) -> None:
    """TC-P-011e cursor 丢失：raw 重放重建 + rebuild_checkpoints 恢复。"""
    h = _full_run_harness(tmp_stores, monkeypatch, "tcp011e")
    _scenario_lost_rebuild(h)


def test_tcp011f_cursor_corrupt_rebuild(tmp_stores, monkeypatch) -> None:
    """TC-P-011f cursor 损坏：FAILED → rebuild_checkpoints → 重跑成功。"""
    h = _full_run_harness(tmp_stores, monkeypatch, "tcp011f")
    _scenario_corrupt_rebuild(h)


def test_tcp011g_resume_after_partial_exit(tmp_stores, monkeypatch) -> None:
    """TC-P-011g 中途退出：PARTIAL 后从 checkpoint 增量续传 → 全量 72 行。"""
    h = _harness(
        tmp_stores,
        monkeypatch,
        "tcp011g",
        connector=FakeConnector(chunk_failures={2: TransportError}),
    )
    row1 = h.runner.run(_make_job())
    assert row1.status == "PARTIAL_SUCCESS"
    assert row1.checkpoint_after == CHUNK1_END
    assert len(_canonical_rows(h.data_dir)) == 24

    resume = AcquisitionJob(DS_ID, start=None, end=JOB_END, mode="incremental")
    row2 = h.runner.run(resume)
    assert row2.status == "SUCCESS"
    assert row2.checkpoint_after == FULL_CURSOR
    assert len(_canonical_rows(h.data_dir)) == 72  # 补齐至全量，无缺口


def test_tcp011h_transport_retry_in_connector(tmp_stores, monkeypatch) -> None:
    """TC-P-011h 断网重试：连接器内部重试成功 → chunk 成功、retry_count=1、SUCCESS。"""
    h = _harness(
        tmp_stores, monkeypatch, "tcp011h", connector=FakeConnector(retry_transport_chunk=1)
    )
    row = h.runner.run(_make_job())
    assert row.status == "SUCCESS"
    assert row.retry_count == 1
    assert row.request_count == 4  # chunk1 两次尝试 + chunk2/3 各一次
    assert row.chunk_success == 3 and row.chunk_failed == 0
    assert row.checkpoint_after == FULL_CURSOR
    assert len(_canonical_rows(h.data_dir)) == 72


# ── 扩展：锁互斥 / drift 落盘 ─────────────────────────────────────────


def test_dataset_lock_mutex(tmp_stores, monkeypatch) -> None:
    """锁互斥：run 持锁期间第二个 run → StorageError；释放后可正常执行。"""
    h = _harness(tmp_stores, monkeypatch, "lock_mutex")
    lock = h.meta.try_lock_dataset(DS_ID, source_id=SOURCE_ID)
    with pytest.raises(StorageError, match="locked"):
        h.runner.run(_make_job())  # try_lock 冲突快速失败，不写 run_log 终态
    h.meta.finish_run(lock.run_id, "FAILED")  # 释放锁
    row = h.runner.run(_make_job())
    assert row.status == "SUCCESS"
    assert len(_canonical_rows(h.data_dir)) == 72


def test_drift_findings_persisted_to_quality_flags(tmp_stores, monkeypatch) -> None:
    """drift 落盘（审计 H-3）：同 nk 值漂移 → upsert 产出 Q-DRIFT-001 findings
    → RunLogStage 写入 quality_flags（run_id 可归因）。"""
    h = _harness(tmp_stores, monkeypatch, "drift")
    assert h.runner.run(_make_job()).status == "SUCCESS"

    # 第二次连接器价格 +100，增量窗口重写同 nk 记录 → 值漂移
    h.connectors[DS_ID] = FakeConnector(price_offset=100.0)
    job_inc = AcquisitionJob(DS_ID, start=None, end=JOB_END, mode="incremental")
    row2 = h.runner.run(job_inc)
    assert row2.status == "SUCCESS"
    can2 = _stage_results(h.ctxs[1])["canonical"]
    assert (can2.output_count, can2.duplicate_count) == (0, 1)  # 同 nk updated

    flags = h.meta.connection.execute(
        "SELECT rule_id, severity, run_id FROM quality_flags "
        "WHERE run_id = ? AND rule_id = 'Q-DRIFT-001'",
        (row2.run_id,),
    ).fetchall()
    assert flags, "drift findings 应落盘 quality_flags"
    assert all(f[1] == "WARNING" and f[2] == row2.run_id for f in flags)


# ── 服务就绪审计回归（2026-09-21 SR-02/03/06/07）──────────────────────


def test_sr02_keyboard_interrupt_writes_cancelled(tmp_stores, monkeypatch) -> None:
    """SR-02：fetch 中断（KeyboardInterrupt）→ CANCELLED 终态 + 锁释放。

    此前 runner 仅捕获 Exception，BaseException 穿透 → run_log 滞留
    PENDING、dataset 锁不释放（D05 §3「用户取消 → CANCELLED」）。
    """
    h = _harness(
        tmp_stores,
        monkeypatch,
        "sr02",
        connector=FakeConnector(chunk_failures={1: KeyboardInterrupt}),
    )
    with pytest.raises(KeyboardInterrupt):
        h.runner.run(_make_job())
    rows = h.meta.connection.execute(
        "SELECT status, error_summary FROM run_log"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "CANCELLED"
    assert "KeyboardInterrupt" in str(rows[0][1])
    # 取消不计入熔断（仅 FAILED 计数）
    assert not h.meta.is_circuit_open(SOURCE_ID, DS_ID)
    # 锁随终态释放：后续 run 正常执行
    h.connectors[DS_ID].start_new_run()
    h.connectors[DS_ID].chunk_failures = {}  # 清除注入，验证可完整跑通
    assert h.runner.run(_make_job()).status == "SUCCESS"


def test_sr03_circuit_persists_across_runner_instances(tmp_stores, monkeypatch) -> None:
    """SR-03：熔断状态持久化——新 runner 实例继承熔断（CLI 每 dataset
    build_runner 的真实形态下 3 连败熔断可触发）。"""
    h = _harness(
        tmp_stores,
        monkeypatch,
        "sr03",
        connector=FakeConnector(chunk_failures={1: TransportError}),
    )
    for _ in range(3):
        h.connectors[DS_ID].start_new_run()
        assert h.runner.run(_make_job()).status == "FAILED"
    assert h.meta.is_circuit_open(SOURCE_ID, DS_ID)

    runner2 = PipelineRunner(h.runner._ctx_factory)  # type: ignore[arg-type]
    h.connectors[DS_ID].start_new_run()
    calls_before = len(h.connectors[DS_ID].fetch_calls)
    row = runner2.run(_make_job())
    assert row.status == "CANCELLED"
    assert row.error_summary == "circuit open"
    assert len(h.connectors[DS_ID].fetch_calls) == calls_before  # 未进入 stages


def test_sr03_success_resets_persisted_circuit(tmp_stores, monkeypatch) -> None:
    """SR-03：SUCCESS 重置持久化的连续失败计数与打开标志。"""
    h = _harness(
        tmp_stores,
        monkeypatch,
        "sr03_reset",
        connector=FakeConnector(chunk_failures={1: TransportError}),
    )
    for _ in range(2):
        h.connectors[DS_ID].start_new_run()
        h.runner.run(_make_job())
    cnt = h.meta.connection.execute(
        "SELECT consecutive_failed FROM checkpoints "
        "WHERE source_id = ? AND dataset_id = ?",
        (SOURCE_ID, DS_ID),
    ).fetchone()[0]
    assert cnt == 2

    h.connectors[DS_ID].start_new_run()
    h.connectors[DS_ID].chunk_failures = {}  # 第三次成功
    assert h.runner.run(_make_job()).status == "SUCCESS"
    assert not h.meta.is_circuit_open(SOURCE_ID, DS_ID)
    cnt2 = h.meta.connection.execute(
        "SELECT consecutive_failed FROM checkpoints "
        "WHERE source_id = ? AND dataset_id = ?",
        (SOURCE_ID, DS_ID),
    ).fetchone()[0]
    assert cnt2 == 0


def test_sr03_empty_cursor_checkpoint_returns_none(tmp_stores, monkeypatch) -> None:
    """SR-03：熔断首败创建 last_cursor='' 行 → get_checkpoint 归一化 None。

    空串若透传 plan_chunks 会因 ISO 解析失败崩溃，故必须归一化。
    """
    h = _harness(
        tmp_stores,
        monkeypatch,
        "sr03_cursor",
        connector=FakeConnector(chunk_failures={1: TransportError}),
    )
    assert h.runner.run(_make_job()).status == "FAILED"
    assert h.meta.get_checkpoint(SOURCE_ID, DS_ID) is None


def test_sr07_run_transitions_pending_to_running(tmp_stores, monkeypatch) -> None:
    """SR-07：阶段执行期间 run_log 状态为 RUNNING（活性可观测）。"""
    h = _harness(tmp_stores, monkeypatch, "sr07")
    seen: list[str] = []
    conn = h.connectors[DS_ID]
    orig_fetch = conn.fetch

    def spy_fetch(request: Any) -> Iterator[RawBatch]:
        for batch in orig_fetch(request):
            row = h.meta.connection.execute(
                "SELECT status FROM run_log ORDER BY started_at DESC LIMIT 1"
            ).fetchone()
            seen.append(str(row[0]))
            yield batch

    conn.fetch = spy_fetch  # type: ignore[method-assign]
    row = h.runner.run(_make_job())
    assert row.status == "SUCCESS"
    assert seen, "spy fetch 未被调用"
    assert set(seen) == {"RUNNING"}


def test_sr06_finish_run_writes_back_dataset_status(tmp_stores, monkeypatch) -> None:
    """SR-06：finish_run 终态写回 dataset_registry.status（D03 §1）。

    最新 run SUCCESS 且无 ERROR finding → COMPLETE；circuit-open
    CANCELLED 路径同样不破坏写回。
    """
    h = _harness(tmp_stores, monkeypatch, "sr06")
    assert h.runner.run(_make_job()).status == "SUCCESS"
    st = h.meta.connection.execute(
        "SELECT status FROM dataset_registry WHERE dataset_id = ?", (DS_ID,)
    ).fetchone()[0]
    assert st == "COMPLETE"


# ── READY-001：分窗口 run（SR-04 内存治理，方案 B） ───────────────────


def _harness_mem(tmp_stores, monkeypatch, name: str, *, pad_bytes: int):
    """内存断言专用环境：ctx_factory 不保留 ctx 引用（同生产 _wiring 组合根）。

    _harness 的 ctxs 列表会保留每个窗口 ctx（其 _batches/_raw_refs 持有
    payload），干扰峰值测量；生产 build_runner 的工厂无保留，此处同构。
    """
    h = _harness(
        tmp_stores, monkeypatch, name, connector=FakeConnector(pad_bytes=pad_bytes)
    )

    def _factory(job: AcquisitionJob) -> RunContext:
        return RunContext(
            run_id="",
            ingest_batch_id="",
            source_id=SOURCE_ID,
            dataset_id=job.dataset_id,
            connector=h.connectors[DS_ID],
            raw_store=h.raw,
            canonical_store=h.canonical,
            meta=h.meta,
            settings=h.settings,
        )

    runner = PipelineRunner(_factory)  # type: ignore[arg-type]
    return SimpleNamespace(
        runner=runner,
        meta=h.meta,
        connectors=h.connectors,
        settings=h.settings,
        data_dir=h.data_dir,
    )


def test_ready001_window_coverage_and_status(tmp_stores, monkeypatch) -> None:
    """READY-001：2 天窗口 × 3 天任务 → 2 个窗口；fetch 覆盖区间连续无缝
    （起点 = start - overlap，终点 = end，缝间仅 overlap 幂等重取）；
    终态 cursor 与行数同全量单 run。"""
    h = _harness(tmp_stores, monkeypatch, "ready001_cover")
    with pytest.raises(ConfigError, match="window_seconds"):
        h.runner.run_windowed(_make_job(), window_seconds=0)

    rows = h.runner.run_windowed(_make_job(), window_seconds=2 * 86400)
    assert [r.status for r in rows] == ["SUCCESS", "SUCCESS"]
    assert h.meta.get_checkpoint(SOURCE_ID, DS_ID) == FULL_CURSOR
    assert len(_canonical_rows(h.data_dir)) == 72

    ivs = sorted((c.start, c.end) for c in h.connectors[DS_ID].fetch_calls)
    assert ivs[0][0] == JOB_START - timedelta(hours=1)  # 起点 = start - overlap
    assert ivs[-1][1] == JOB_END  # 终点 = end
    for (s1, e1), (s2, e2) in zip(ivs, ivs[1:], strict=False):
        assert s2 <= e1  # 无缝（≤ 允许 overlap 重复）
        assert s2 >= s1 and e2 >= e1  # 单调推进，无整体回退


def test_ready001_progress_callback(tmp_stores, monkeypatch) -> None:
    """progress 回调：每窗口结束回传 (done, est, RunRow)；est 为
    ceil((final_end - base) / span) 预估；回调异常中断窗口循环。"""
    h = _harness(tmp_stores, monkeypatch, "ready001_progress")
    calls: list[tuple[int, int, str]] = []
    rows = h.runner.run_windowed(
        _make_job(),
        window_seconds=86400,
        progress=lambda done, est, row: calls.append((done, est, row.status)),
    )
    assert [r.status for r in rows] == ["SUCCESS", "SUCCESS", "SUCCESS"]
    # 71h 跨度 / 24h 窗口 → est=3；每窗口回调一次，done 单调递增
    assert calls == [
        (1, 3, "SUCCESS"),
        (2, 3, "SUCCESS"),
        (3, 3, "SUCCESS"),
    ]

    # 回调异常 → 循环中断，已完成窗口保留（checkpoint 可续传）
    h2 = _harness(tmp_stores, monkeypatch, "ready001_progress_raise")
    seen: list[int] = []

    def _boom(done: int, est: int, row: object) -> None:
        seen.append(done)
        if done == 2:
            raise RuntimeError("callback boom")

    with pytest.raises(RuntimeError, match="callback boom"):
        h2.runner.run_windowed(_make_job(), window_seconds=86400, progress=_boom)
    assert seen == [1, 2]
    # 回调在窗口 run 完成后触发：窗口 2 的 checkpoint 已落盘，可续传
    assert h2.meta.get_checkpoint(SOURCE_ID, DS_ID) == "2024-01-03T00:00:00"


def test_ready001_windowed_matches_single_run(tmp_stores, monkeypatch) -> None:
    """READY-001 GWT-1：分窗口（3×1 天）与全量单 run 的 canonical 值列
    逐字节一致、终态 cursor 一致；run_log 产生 N 行（方案 B 语义变化）。"""
    h1 = _harness(tmp_stores, monkeypatch, "ready001_eq_single")
    row = h1.runner.run(_make_job())
    assert row.status == "SUCCESS"

    h2 = _harness(tmp_stores, monkeypatch, "ready001_eq_windowed")
    rows = h2.runner.run_windowed(_make_job(), window_seconds=86400)
    assert [r.status for r in rows] == ["SUCCESS", "SUCCESS", "SUCCESS"]
    assert (
        _canonical_values(_canonical_rows(h1.data_dir))
        == _canonical_values(_canonical_rows(h2.data_dir))
    )
    assert h2.meta.get_checkpoint(SOURCE_ID, DS_ID) == FULL_CURSOR
    n_runs = h2.meta.connection.execute(
        "SELECT COUNT(*) FROM run_log WHERE dataset_id = ?", (DS_ID,)
    ).fetchone()[0]
    assert n_runs == 3  # 一次逻辑 backfill → N 行 run_log


def test_ready001_resume_after_window_failure(tmp_stores, monkeypatch) -> None:
    """READY-001 GWT-2：窗口 2 首个 chunk 失败（DEC-F0 FAILED）→ 循环停止、
    cursor 停在窗口 1 末尾；重入从 checkpoint 精确续传，无重复无遗漏。"""
    h1 = _harness(tmp_stores, monkeypatch, "ready001_res_base")
    assert h1.runner.run(_make_job()).status == "SUCCESS"
    expected = _canonical_values(_canonical_rows(h1.data_dir))

    h = _harness(
        tmp_stores,
        monkeypatch,
        "ready001_res",
        connector=FakeConnector(chunk_failures={3: TransportError}),
    )
    rows = h.runner.run_windowed(_make_job(), window_seconds=86400)
    assert [r.status for r in rows] == ["SUCCESS", "FAILED"]
    assert h.meta.get_checkpoint(SOURCE_ID, DS_ID) == "2024-01-02T00:00:00"

    # 重入：已完成窗口不重取（window 1 的 2 个 fetch 不重复），续传收尾
    h.connectors[DS_ID].start_new_run()
    h.connectors[DS_ID].chunk_failures = {}
    rows2 = h.runner.run_windowed(_make_job(), window_seconds=86400)
    assert [r.status for r in rows2] == ["SUCCESS", "SUCCESS"]
    assert h.meta.get_checkpoint(SOURCE_ID, DS_ID) == FULL_CURSOR
    calls = h.connectors[DS_ID].fetch_calls
    # R2-04：首入 8（失败 chunk 含 5 次重试）+ 重入 3（窗口2 2 chunk + 窗口3 1 chunk）
    assert len(calls) == 8 + 3
    assert calls[0].start == datetime(2023, 12, 31, 23, 0)  # 首入：start - overlap
    assert calls[8].start == datetime(2024, 1, 1, 23, 0)  # 重入首 chunk：checkpoint 起的 overlap 窗
    assert len(_canonical_rows(h.data_dir)) == 72
    assert _canonical_values(_canonical_rows(h.data_dir)) == expected  # 无重复无遗漏


def test_ready001_partial_window_chains_retry(tmp_stores, monkeypatch) -> None:
    """READY-001 DEC-W4：窗口 1 的 chunk2 失败 → PARTIAL（cursor 停 chunk1
    右界）→ 循环继续，下一窗口从新 checkpoint 起自然重试失败区间 → 全量。"""
    h = _harness(
        tmp_stores,
        monkeypatch,
        "ready001_partial",
        connector=FakeConnector(chunk_failures={2: TransportError}),
    )
    rows = h.runner.run_windowed(_make_job(), window_seconds=2 * 86400)
    assert [r.status for r in rows] == ["PARTIAL_SUCCESS", "SUCCESS"]
    assert h.meta.get_checkpoint(SOURCE_ID, DS_ID) == FULL_CURSOR
    assert len(_canonical_rows(h.data_dir)) == 72


def test_ready001_interrupt_cancelled_resume(tmp_stores, monkeypatch) -> None:
    """READY-001 SR-02 联动：窗口 2 fetch 中断（KeyboardInterrupt）→
    run_windowed 透传异常，run_log 留 CANCELLED 终态、cursor 不越界；
    重入后剩余窗口可恢复。"""
    h = _harness(
        tmp_stores,
        monkeypatch,
        "ready001_cancel",
        connector=FakeConnector(chunk_failures={3: KeyboardInterrupt}),
    )
    with pytest.raises(KeyboardInterrupt):
        h.runner.run_windowed(_make_job(), window_seconds=86400)
    statuses = [
        r[0]
        for r in h.meta.connection.execute(
            "SELECT status FROM run_log ORDER BY started_at"
        ).fetchall()
    ]
    assert statuses == ["SUCCESS", "CANCELLED"]
    assert h.meta.get_checkpoint(SOURCE_ID, DS_ID) == "2024-01-02T00:00:00"

    h.connectors[DS_ID].start_new_run()
    h.connectors[DS_ID].chunk_failures = {}
    rows = h.runner.run_windowed(_make_job(), window_seconds=86400)
    assert [r.status for r in rows] == ["SUCCESS", "SUCCESS"]
    assert h.meta.get_checkpoint(SOURCE_ID, DS_ID) == FULL_CURSOR
    assert len(_canonical_rows(h.data_dir)) == 72


def test_ready001_circuit_breaker_stops_windowed_loop(tmp_stores, monkeypatch) -> None:
    """READY-001：FAILED 窗口停止循环（每调用 1 次失败计数）；跨调用 3 连败
    → 第 4 次调用首窗口 CANCELLED（SR-03 熔断按 run 计数语义不变）。"""
    h = _harness(
        tmp_stores,
        monkeypatch,
        "ready001_circuit",
        connector=FakeConnector(chunk_failures={1: TransportError}),
    )
    for _ in range(3):
        h.connectors[DS_ID].start_new_run()
        rows = h.runner.run_windowed(_make_job(), window_seconds=86400)
        assert [r.status for r in rows] == ["FAILED"]  # 首窗口即败，不继续
    h.connectors[DS_ID].start_new_run()
    rows = h.runner.run_windowed(_make_job(), window_seconds=86400)
    assert [r.status for r in rows] == ["CANCELLED"]
    assert rows[0].error_summary == "circuit open"


def test_ready001_full_window_dataset_not_split(tmp_stores, monkeypatch) -> None:
    """READY-001 DEC-W2：全窗口 diff 类（chunk_size=full_window）不拆分——
    window_seconds 传入也只产生单 run（fred/sec 语义依赖全窗口 diff）。"""
    ds = "ready001_full"
    monkeypatch.setitem(
        WINDOW_CONFIG,
        ds,
        {"overlap_seconds": "full_window", "chunk_size_seconds": "full_window"},
    )
    conn = FakeConnector(market_id="BINANCE:FULLUSDT:SPOT")
    h = _harness(tmp_stores, monkeypatch, "ready001_full", connectors={ds: conn})
    job = AcquisitionJob(
        ds, start=JOB_START, end=JOB_END, mode="backfill", params={"symbol": "X"}
    )
    rows = h.runner.run_windowed(job, window_seconds=3600)
    assert len(rows) == 1 and rows[0].status == "SUCCESS"
    assert len(conn.fetch_calls) == 1  # 单 chunk 全窗口


def test_ready001_completed_backfill_rerun_returns_empty(
    tmp_stores, monkeypatch
) -> None:
    """READY-001 DEC-W4：checkpoint 已覆盖请求跨度 → 幂等返回 []，零请求
    （断点续传的退化形态：全部窗口已完成）。"""
    h = _full_run_harness(tmp_stores, monkeypatch, "ready001_done")
    assert len(h.connectors[DS_ID].fetch_calls) == 3
    rows = h.runner.run_windowed(_make_job(), window_seconds=86400)
    assert rows == []
    assert len(h.connectors[DS_ID].fetch_calls) == 3  # 未发起任何新请求


def test_ready001_peak_memory_bounded_by_window(tmp_stores, monkeypatch) -> None:
    """READY-001 GWT-1 内存断言：tracemalloc 峰值不随总跨度线性增长。

    每 batch 附 200KB pad：1 窗口（2 天 = 3 chunk）vs 6 窗口（12 天 = 13
    chunk）——全量缓冲实现下峰值 ≈ 总 chunk 数 × batch 大小（≈4×），分窗口
    后单 run 内存 O(窗口)，两峰值应同量级（< 2.5×）。ctx 工厂不保留引用
    （同生产 _wiring.build_runner），排除测试自身保留的干扰。
    """
    pad = 200_000

    def _peak(name: str, days: int) -> int:
        h = _harness_mem(tmp_stores, monkeypatch, name, pad_bytes=pad)
        job = AcquisitionJob(
            DS_ID,
            start=JOB_START,
            end=JOB_START + timedelta(days=days),
            mode="backfill",
            params={"symbol": "BTCUSDT", "interval": "1m"},
        )
        tracemalloc.start()
        try:
            rows = h.runner.run_windowed(job, window_seconds=2 * 86400)
        finally:
            _current, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
        assert all(r.status == "SUCCESS" for r in rows)
        return peak

    peak1 = _peak("ready001_mem_1w", days=2)
    peak6 = _peak("ready001_mem_6w", days=12)
    assert peak1 > 300_000, "pad 应被 run 内缓冲持有（防断言失效）"
    assert peak6 < peak1 * 2.5, "峰值内存应 O(窗口)，不随总跨度线性增长"


# ── 采集就绪审计回归（2026-09-22 R2-01/04/05）────────────────────────


def test_r201_half_open_probe_after_cooldown(tmp_stores, monkeypatch) -> None:
    """R2-01：熔断打开超冷却期 → half-open 放行探测 run；成功重置熔断。

    冷却期内仍硬拦截（未进 stages）；回拨 circuit_opened_at（模拟冷却期
    流逝）后探测 run 放行并发出 pipeline.circuit_half_open 日志；探测
    成功经 reset_circuit 解除熔断。
    """
    h = _harness(
        tmp_stores,
        monkeypatch,
        "r201_probe",
        connector=FakeConnector(chunk_failures={1: TransportError}),
        circuit_cooldown_s=1800.0,
    )
    for _ in range(3):
        h.connectors[DS_ID].start_new_run()
        assert h.runner.run(_make_job()).status == "FAILED"
    assert h.meta.is_circuit_open(SOURCE_ID, DS_ID)

    # 冷却期内：仍硬拦截，未进入 stages
    h.connectors[DS_ID].start_new_run()
    calls_before = len(h.connectors[DS_ID].fetch_calls)
    row = h.runner.run(_make_job())
    assert row.status == "CANCELLED"
    assert len(h.connectors[DS_ID].fetch_calls) == calls_before

    # 冷却期流逝（回拨 opened_at 1 小时）→ 探测放行 + half-open 日志
    h.meta.connection.execute(
        "UPDATE checkpoints SET circuit_opened_at = datetime('now', '-3600 seconds') "
        "WHERE source_id = ? AND dataset_id = ?",
        (SOURCE_ID, DS_ID),
    )
    h.meta.connection.commit()
    h.connectors[DS_ID].start_new_run()
    h.connectors[DS_ID].chunk_failures = {}  # 探测成功
    with capture_logs() as logs:
        row = h.runner.run(_make_job())
    assert row.status == "SUCCESS"
    assert any(e.get("event") == "pipeline.circuit_half_open" for e in logs)
    assert not h.meta.is_circuit_open(SOURCE_ID, DS_ID)  # reset_circuit 解除


def test_r201_probe_failure_rearms_cooldown(tmp_stores, monkeypatch) -> None:
    """R2-01：half-open 探测失败 → record_circuit_failure 刷新 opened_at
    重新计时（防失败源被逐 tick 反复探测），后续 run 冷却期内仍 CANCELLED。"""
    h = _harness(
        tmp_stores,
        monkeypatch,
        "r201_rearm",
        connector=FakeConnector(chunk_failures={1: TransportError}),
        circuit_cooldown_s=1800.0,
    )
    for _ in range(3):
        h.connectors[DS_ID].start_new_run()
        assert h.runner.run(_make_job()).status == "FAILED"

    def _opened_at() -> str:
        row = h.meta.connection.execute(
            "SELECT circuit_opened_at FROM checkpoints "
            "WHERE source_id = ? AND dataset_id = ?",
            (SOURCE_ID, DS_ID),
        ).fetchone()
        assert row is not None and row[0] is not None
        return str(row[0])

    h.meta.connection.execute(
        "UPDATE checkpoints SET circuit_opened_at = datetime('now', '-3600 seconds') "
        "WHERE source_id = ? AND dataset_id = ?",
        (SOURCE_ID, DS_ID),
    )
    h.meta.connection.commit()
    rolled = _opened_at()

    # 探测 run 放行且真正执行 fetch（DEC-F0 FAILED，不 raise）
    h.connectors[DS_ID].start_new_run()
    calls_before = len(h.connectors[DS_ID].fetch_calls)
    row = h.runner.run(_make_job())
    assert row.status == "FAILED"
    assert len(h.connectors[DS_ID].fetch_calls) > calls_before

    # opened_at 刷新为当前时间（重新计时）→ 后续 run 冷却期内仍拦截
    assert _opened_at() > rolled  # datetime('now') 同格式：字典序 = 时间序
    h.connectors[DS_ID].start_new_run()
    assert h.runner.run(_make_job()).status == "CANCELLED"


def test_r201_is_circuit_open_cooldown_semantics(tmp_stores, monkeypatch) -> None:
    """R2-01 meta 层：is_circuit_open 的 cooldown_seconds 语义矩阵。

    None = 打开即 True（硬拦截/运维查询旧行为）；冷却未到 = True；
    冷却已到 = False（half-open 放行）；circuit_open=0 恒 False；
    opened_at 缺失/不可解析 = 保守保持打开。
    """
    h = _harness(
        tmp_stores,
        monkeypatch,
        "r201_meta",
        connector=FakeConnector(chunk_failures={1: TransportError}),
    )
    for _ in range(3):
        h.connectors[DS_ID].start_new_run()
        h.runner.run(_make_job())
    conn = h.meta.connection
    conn.execute(
        "UPDATE checkpoints SET circuit_opened_at = datetime('now', '-3600 seconds') "
        "WHERE source_id = ? AND dataset_id = ?",
        (SOURCE_ID, DS_ID),
    )
    conn.commit()

    is_open = h.meta.is_circuit_open
    assert is_open(SOURCE_ID, DS_ID)  # None：打开即 True（旧行为）
    assert is_open(SOURCE_ID, DS_ID, cooldown_seconds=7200.0)  # 冷却未到
    assert not is_open(SOURCE_ID, DS_ID, cooldown_seconds=1800.0)  # 冷却已过
    assert not is_open(SOURCE_ID, DS_ID, cooldown_seconds=0.0)  # 0 = 立即放行

    # circuit_open=0：恒 False（reset_circuit 后不受 cooldown 影响）
    h.meta.reset_circuit(SOURCE_ID, DS_ID)
    assert not is_open(SOURCE_ID, DS_ID, cooldown_seconds=10**9)

    # opened_at 缺失 → 保守保持打开
    conn.execute(
        "UPDATE checkpoints SET circuit_open = 1, circuit_opened_at = NULL "
        "WHERE source_id = ? AND dataset_id = ?",
        (SOURCE_ID, DS_ID),
    )
    conn.commit()
    assert is_open(SOURCE_ID, DS_ID, cooldown_seconds=1800.0)

    # opened_at 不可解析（历史行）→ 保守保持打开
    conn.execute(
        "UPDATE checkpoints SET circuit_opened_at = 'not-a-date' "
        "WHERE source_id = ? AND dataset_id = ?",
        (SOURCE_ID, DS_ID),
    )
    conn.commit()
    assert is_open(SOURCE_ID, DS_ID, cooldown_seconds=1800.0)


def test_r204_transient_retry_succeeds_within_budget(tmp_stores, monkeypatch) -> None:
    """R2-04：chunk1 前 2 次尝试 TransportError → chunk 级退避重试成功；
    retry_count=2、SUCCESS（D05 §1「chunk 级重试」消费 settings.retry_max）。"""
    h = _harness(
        tmp_stores,
        monkeypatch,
        "r204_transient",
        connector=FakeConnector(fail_attempts={1: 2}),
    )
    row = h.runner.run(_make_job())
    assert row.status == "SUCCESS"
    assert row.retry_count == 2
    assert row.request_count == 5  # chunk1×3 次尝试 + chunk2/3 各 1
    assert row.chunk_success == 3 and row.chunk_failed == 0
    assert row.checkpoint_after == FULL_CURSOR
    assert len(_canonical_rows(h.data_dir)) == 72


def test_r204_retry_exhausted_chunk_failed(tmp_stores, monkeypatch) -> None:
    """R2-04：chunk1 全部尝试（1 初次 + retry_max 次重试）失败 → 耗尽计
    chunk 失败 → DEC-F0 FAILED + retry_count=retry_max（不 raise）。"""
    h = _harness(
        tmp_stores,
        monkeypatch,
        "r204_exhausted",
        connector=FakeConnector(chunk_failures={1: TransportError}),
    )
    row = h.runner.run(_make_job())
    assert row.status == "FAILED"
    assert row.retry_count == h.settings.retry_max
    assert row.request_count == h.settings.retry_max + 1
    assert row.chunk_success == 0 and row.chunk_failed == 1
    db = _run_log_row(h.meta, row.run_id)
    assert "TransportError" in str(db["error_summary"])


def test_r205_empty_history_chunk_warns(tmp_stores, monkeypatch) -> None:
    """R2-05：完全处于历史区间（早于 now − 2×chunk 跨度）的 0 批次 chunk →
    warning_count + empty_history_chunk 日志；SUCCESS 语义与 cursor 推进
    不变；近期（阈值内）chunk 正常产出不告警。"""
    h = _harness(
        tmp_stores,
        monkeypatch,
        "r205_empty",
        connector=FakeConnector(empty_batches_chunks={1}),
    )
    # 4 chunk 窗口（end = start + 3d23h）：仅第 1 chunk.end 早于阈值
    job = AcquisitionJob(
        DS_ID,
        start=JOB_START,
        end=JOB_START + timedelta(days=3, hours=23),
        mode="backfill",
        params={"symbol": "BTCUSDT", "interval": "1m"},
    )
    with capture_logs() as logs:
        row = h.runner.run(job)
    assert row.status == "SUCCESS"
    assert row.warning_count == 1
    empties = [e for e in logs if e.get("event") == "pipeline.empty_history_chunk"]
    assert len(empties) == 1
    assert empties[0]["dataset_id"] == DS_ID
    assert h.meta.get_checkpoint(SOURCE_ID, DS_ID) == "2024-01-04T23:00:00"
    assert len(_canonical_rows(h.data_dir)) == 72  # 近期 3 chunk 正常落盘
