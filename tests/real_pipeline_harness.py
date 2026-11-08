"""e2e/quality 两层的共享 harness（真实连接器 + 真实三层存储）。

与 tests/integration/test_pipeline.py 的 FakeConnector harness 不同：本模块
使用真实 BinanceSpotConnector，HTTP 层由 httpx.MockTransport 回放
tests/fixtures/binance_spot 的录制响应，因此 fetch → normalize → canonical
→ quality → runlog 全链路均为生产代码，仅网络被替换。
"""

from __future__ import annotations

import base64
import json
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pyarrow.parquet as pq

from chronoforge.config.settings import Settings
from chronoforge.connectors.binance_spot import BinanceSpotConnector
from chronoforge.pipeline.runner import PipelineRunner
from chronoforge.pipeline.windows import AcquisitionJob
from chronoforge.storage.canonical import CanonicalStoreImpl
from chronoforge.storage.meta import MetaStore
from chronoforge.storage.raw import RawStore

DS_ID = "binance_spot_klines"
SOURCE_ID = "binance_spot"
MARKET_ID = "BINANCE:BTCUSDT:SPOT"

# 单 chunk 窗口：backfill start - overlap(3600s) = 2017-07-02 23:00，
# chunk end = job.end → cursor 2017-07-03T01:00:00
JOB_START = datetime(2017, 7, 3, 0, 0)
JOB_END = datetime(2017, 7, 3, 1, 0)
FULL_CURSOR = "2017-07-03T01:00:00"
NEXT_JOB_END = datetime(2017, 7, 3, 2, 0)
NEXT_CURSOR = "2017-07-03T02:00:00"

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "binance_spot" / "klines"

# Settings 的 env 覆盖优先级高于 kwargs（_load_from_env 用 values.update），
# harness 构造 Settings 前必须删除 autouse fixture 注入的变量。
_ENV_VARS = (
    "CHRONOFORGE_ENV",
    "CHRONOFORGE_DATA_DIR",
    "CHRONOFORGE_META_DIR",
    "CHRONOFORGE_QUALITY_BLOCK",
    "CHRONOFORGE_NORMALIZE_ERROR_THRESHOLD",
    "CHRONOFORGE_NORMALIZE_DROP_THRESHOLD",
    "CHRONOFORGE_NORMALIZE_DROP_MIN_UNITS",
    "CHRONOFORGE_SEC_CONTACT",
    "CHRONOFORGE_VERSION",
    "CHRONOFORGE_RETRY_MAX",
    "CHRONOFORGE_CIRCUIT_COOLDOWN_S",
)


def load_fixture(name: str = "happy.json") -> list[Any]:
    """读取录制 fixture（Binance klines 原始响应形状）。"""
    with open(FIXTURE_DIR / name, encoding="utf-8") as f:
        return json.load(f)


def make_handler(
    payloads: list[list[Any]],
) -> tuple[Callable[[httpx.Request], httpx.Response], list[httpx.Request]]:
    """顺序回放 payloads，用尽后返回空数组（终止 connector 分页）。

    Returns:
        (MockTransport handler, 已收到的请求列表)
    """
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        index = len(requests) - 1
        payload = payloads[index] if index < len(payloads) else []
        return httpx.Response(200, json=payload)

    return handler, requests


def _register_dataset(meta: MetaStore) -> None:
    conn = meta.connection
    conn.execute(
        "INSERT OR IGNORE INTO source_registry "
        "(source_id, display_name, access_type, base_url, rate_limit_json, license) "
        "VALUES (?, 'Binance Spot', 'PUBLIC', 'https://api.binance.com', '{}', 'MIT')",
        (SOURCE_ID,),
    )
    conn.execute(
        "INSERT OR REPLACE INTO dataset_registry "
        "(dataset_id, source_id, canonical_type, entity_id, params_json, frequency, "
        "continuity_model, status, created_at, revision_supported) "
        "VALUES (?, ?, 'OHLCV', 'BTCUSDT', ?, '1m', 'EVENT_BASED', 'UNKNOWN', "
        "'2024-01-01', 0)",
        (DS_ID, SOURCE_ID, json.dumps({"symbol": "BTCUSDT", "interval": "1m"})),
    )
    conn.commit()


def build_harness(
    tmp_stores,
    monkeypatch,
    name: str,
    handler: Callable[[httpx.Request], httpx.Response],
    **settings_kwargs: object,
) -> SimpleNamespace:
    """搭建 runner + 真实 Raw/Meta/Canonical 存储 + 真实 Binance 连接器。"""
    for var in _ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    data_dir = tmp_stores.data_dir / name
    meta_dir = tmp_stores.meta_dir / name
    data_dir.mkdir(parents=True, exist_ok=True)
    meta_dir.mkdir(parents=True, exist_ok=True)

    settings = Settings(
        env="test",
        data_dir=data_dir,
        meta_dir=meta_dir,
        sec_contact_email="research@example.com",
        **settings_kwargs,
    )
    meta = MetaStore(str(meta_dir))
    meta.migrate()
    _register_dataset(meta)
    raw = RawStore(str(data_dir))
    canonical = CanonicalStoreImpl(str(data_dir))

    connector = BinanceSpotConnector()
    connector._client = httpx.Client(
        base_url="https://api.binance.com",
        transport=httpx.MockTransport(handler),
    )

    ctxs: list[Any] = []

    def _ctx_factory(job: AcquisitionJob) -> Any:
        ctx = SimpleNamespace(
            run_id="",
            ingest_batch_id="",
            source_id=connector.source_id,
            dataset_id=job.dataset_id,
            connector=connector,
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
        connector=connector,
        meta=meta,
        raw=raw,
        canonical=canonical,
        settings=settings,
        ctxs=ctxs,
        data_dir=data_dir,
        meta_dir=meta_dir,
    )


def make_job(
    *,
    mode: str = "backfill",
    start: datetime | None = JOB_START,
    end: datetime | None = JOB_END,
) -> AcquisitionJob:
    return AcquisitionJob(
        dataset_id=DS_ID,
        start=start,
        end=end,
        mode=mode,
        params={"symbol": "BTCUSDT", "interval": "1m"},
    )


def run_log_row(meta: MetaStore, run_id: str) -> dict[str, Any]:
    cur = meta.connection.execute(
        "SELECT * FROM run_log WHERE run_id = ?", (run_id,)
    )
    row = cur.fetchone()
    assert row is not None, f"run_log 缺少 run_id={run_id}"
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row, strict=True))


def latest_run_log_row(meta: MetaStore) -> dict[str, Any]:
    cur = meta.connection.execute(
        "SELECT * FROM run_log ORDER BY started_at DESC, rowid DESC LIMIT 1"
    )
    row = cur.fetchone()
    assert row is not None, "run_log 为空"
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row, strict=True))


def canonical_files(data_dir: Path) -> list[Path]:
    base = data_dir / "canonical" / "OHLCV" / f"entity={MARKET_ID}"
    return sorted(base.rglob("*.parquet")) if base.exists() else []


def canonical_rows(data_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for f in canonical_files(data_dir):
        rows.extend(pq.read_table(str(f)).to_pylist())
    rows.sort(key=lambda r: r["event_time"])
    return rows


def raw_lines(data_dir: Path) -> tuple[Path, list[dict[str, Any]]]:
    files = sorted((data_dir / "raw" / SOURCE_ID / DS_ID).rglob("*.jsonl"))
    assert len(files) == 1, f"期望 1 个 raw 文件，实际 {len(files)}"
    lines = [json.loads(line) for line in files[0].read_text(
        encoding="utf-8").splitlines() if line]
    return files[0], lines


def decoded_raw_payloads(data_dir: Path) -> list[Any]:
    _path, lines = raw_lines(data_dir)
    return [json.loads(base64.b64decode(line["payload"])) for line in lines]


def get_cursor(meta: MetaStore) -> str | None:
    cur = meta.connection.execute(
        "SELECT last_cursor FROM checkpoints WHERE source_id = ? AND dataset_id = ?",
        (SOURCE_ID, DS_ID),
    )
    row = cur.fetchone()
    # save_checkpoint 首次建行可为空串（get_checkpoint 同样把空串视为 None）
    return None if row is None or not row[0] else str(row[0])
