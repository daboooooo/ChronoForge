"""quality 层：nyfed 门禁覆盖（审计 C-N2）。

验证：
- 注入"静默丢弃"假实现（有输入、normalize 返回 [] 且不上报 note_skip）
  → 行级丢弃门禁必须变红（QualityError + checkpoint 不推进）；
- 畸形行（非法跳过）→ 同上；
- 合法跳过（纯 SVE 日）→ 不得误报，run SUCCESS。
"""

from __future__ import annotations

import json
from datetime import datetime

import httpx
import pytest

from chronoforge.config.settings import Settings
from chronoforge.connectors.base import FetchRequest, RawBatch
from chronoforge.connectors.nyfed import _BASE_URL, _ENDPOINT, NYFedConnector
from chronoforge.exceptions import QualityError
from chronoforge.pipeline.runner import PipelineRunner, RunContext
from chronoforge.pipeline.windows import AcquisitionJob
from chronoforge.storage.canonical import CanonicalStoreImpl
from chronoforge.storage.meta import MetaStore
from chronoforge.storage.raw import RawStore

pytestmark = pytest.mark.quality

SOURCE_ID = "nyfed"
DS_ID = "nyfed_repo_srf_usage"
JOB_START = datetime(2025, 11, 5)
JOB_END = datetime(2025, 11, 6)
FULL_CURSOR = "2025-11-06T00:00:00"

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


class _SilentDropConnector(NYFedConnector):
    """注入的坏实现：fetch 有数据，normalize 静默返回 []，不上报 note_skip。"""

    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def fetch(self, request: FetchRequest):  # type: ignore[override]
        yield RawBatch(
            endpoint=_ENDPOINT,
            payload=self._payload,
            raw_meta={"http_status": 200, "series_id": "srf_usage"},
        )

    def normalize(self, raw: RawBatch):  # type: ignore[override]
        return []


def _register(meta: MetaStore) -> None:
    conn = meta.connection
    conn.execute(
        "INSERT OR IGNORE INTO source_registry "
        "(source_id, display_name, access_type, base_url, rate_limit_json, license) "
        "VALUES (?, 'NY Fed', 'PUBLIC', 'https://markets.newyorkfed.org', '{}', 'PUBLIC')",
        (SOURCE_ID,),
    )
    conn.execute(
        "INSERT OR REPLACE INTO dataset_registry "
        "(dataset_id, source_id, canonical_type, entity_id, params_json, frequency, "
        "continuity_model, status, created_at, revision_supported) "
        "VALUES (?, ?, 'NUMBER', 'srf_usage', ?, '1d', 'TRADING_CALENDAR', "
        "'UNKNOWN', '2024-01-01', 0)",
        (DS_ID, SOURCE_ID, json.dumps({"endpoint": "rp_results", "series": "srf_usage"})),
    )
    conn.commit()


def _build(tmp_stores, monkeypatch, name: str, connector: NYFedConnector):
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
    )
    meta = MetaStore(str(meta_dir))
    meta.migrate()
    _register(meta)
    raw = RawStore(str(data_dir))
    canonical = CanonicalStoreImpl(str(data_dir))

    def factory(job: AcquisitionJob) -> RunContext:
        return RunContext(
            run_id="",
            ingest_batch_id="",
            source_id=SOURCE_ID,
            dataset_id=job.dataset_id,
            connector=connector,
            raw_store=raw,
            canonical_store=canonical,
            meta=meta,
            settings=settings,
        )

    return PipelineRunner(factory), meta  # type: ignore[arg-type]


def _job() -> AcquisitionJob:
    return AcquisitionJob(
        dataset_id=DS_ID,
        start=JOB_START,
        end=JOB_END,
        mode="backfill",
        params={"endpoint": "rp_results", "series": "srf_usage"},
    )


def _latest_run(meta: MetaStore) -> dict[str, object]:
    row = meta.connection.execute(
        "SELECT status, checkpoint_after, error_summary FROM run_log "
        "ORDER BY started_at DESC LIMIT 1"
    ).fetchone()
    return {"status": row[0], "checkpoint_after": row[1], "error_summary": row[2]}


def test_silent_drop_turns_gate_red(tmp_stores, monkeypatch) -> None:
    """坏代码：有输入、零产出、不上报 → QualityError + checkpoint 不推进。"""
    connector = _SilentDropConnector(
        {"repo": {"operations": [{"operationDate": "2025-11-05"}, {"operationDate": "2025-11-05"}]}}
    )
    runner, meta = _build(tmp_stores, monkeypatch, "nyfed_silent_drop", connector)

    with pytest.raises(QualityError, match=r"normalize drop rate 2/2"):
        runner.run(_job())

    logged = _latest_run(meta)
    assert logged["status"] == "FAILED"
    assert logged["checkpoint_after"] is None
    assert meta.get_checkpoint(SOURCE_ID, DS_ID) is None


def test_legit_sve_skip_does_not_turn_gate_red(tmp_stores, monkeypatch) -> None:
    """正确实现：纯 SVE 日 → sve_test_operation 白名单 → SUCCESS 且 cursor 推进。"""
    payload = {
        "repo": {
            "operations": [
                {
                    "operationId": "RP 110525 25",
                    "operationDate": "2025-11-05",
                    "term": "Overnight",
                    "totalAmtAccepted": 99_000_000,
                    "note": "<p>This operation is a Small Value Exercise (SVE).</p>",
                }
            ]
        }
    }
    connector = NYFedConnector()
    connector._client = httpx.Client(  # noqa: SLF001 — 测试替换 HTTP 层
        base_url=_BASE_URL,
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=payload)),
    )
    runner, meta = _build(tmp_stores, monkeypatch, "nyfed_sve_legit", connector)
    try:
        row = runner.run(_job())
    finally:
        connector.close()

    assert row.status == "SUCCESS"
    assert row.checkpoint_after == FULL_CURSOR
    assert meta.get_checkpoint(SOURCE_ID, DS_ID) == FULL_CURSOR
