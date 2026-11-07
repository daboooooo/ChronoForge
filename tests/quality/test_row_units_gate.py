"""quality 层：行单位声明门禁全覆盖（审计 C-N2 方案 B）。

对全部 14 个连接器逐一注入假实现，验证 Runner 只消费连接器声明的
``row_units``，且声明异常不会被伪装成 SUCCESS：

- **声明取消**（``row_units`` 抛 NotImplementedError，等价于继承基类默认）
  → ``QualityError`` + checkpoint 不推进；
- **声明错误**（返回真实值 +2，normalize 只产出真实行数）→ 丢行被计为
  未上报 → ``QualityError`` + checkpoint 不推进；
- **合法空批**（真实声明 0 / None，源端确实无数据）→ SUCCESS 且 cursor
  照常推进，不得误报。

真实连接器的 ``fetch`` 依赖 HTTP/settings，因此这里用薄壳连接器直接产出
合成 ``RawBatch``，但 ``row_units`` 委托给真实连接器实现——门禁验的是
"声明→runner"契约，而非各连接器的网络行为（后者见
``tests/unit/test_connector_row_units.py``）。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pytest

from chronoforge.config.settings import Settings
from chronoforge.connectors.alternative import AlternativeConnector
from chronoforge.connectors.base import FetchRequest, RawBatch
from chronoforge.connectors.binance_futures import BinanceFuturesConnector
from chronoforge.connectors.binance_spot import BinanceSpotConnector
from chronoforge.connectors.ccxt_bridge import CcxtBridgeConnector
from chronoforge.connectors.coinmarketcap import CoinMarketCapConnector
from chronoforge.connectors.coinmetrics import CoinMetricsConnector
from chronoforge.connectors.deribit import _CHART_ENDPOINT, DeribitConnector
from chronoforge.connectors.fred import FREDConnector
from chronoforge.connectors.nyfed import _ENDPOINT as NYFED_SRF
from chronoforge.connectors.nyfed import _SOMA_ENDPOINT, NYFedConnector
from chronoforge.connectors.sec_edgar import SECEdgarConnector
from chronoforge.connectors.sosovalue import SoSoValueConnector
from chronoforge.connectors.treasury_fiscal import TreasuryFiscalConnector
from chronoforge.connectors.treasury_refunding import TreasuryRefundingConnector
from chronoforge.connectors.yahoo import YahooConnector
from chronoforge.exceptions import QualityError
from chronoforge.pipeline.runner import PipelineRunner, RunContext
from chronoforge.pipeline.windows import AcquisitionJob
from chronoforge.storage.canonical import CanonicalStoreImpl
from chronoforge.storage.meta import MetaStore
from chronoforge.storage.raw import RawStore

pytestmark = pytest.mark.quality

JOB_START = datetime(2025, 11, 5, 0, 0)
JOB_END = datetime(2025, 11, 6, 0, 0)
FULL_CURSOR = "2025-11-06T00:00:00"

_ENV_VARS = (
    "CHRONOFORGE_ENV",
    "CHRONOFORGE_DATA_DIR",
    "CHRONOFORGE_META_DIR",
    "CHRONOFORGE_QUALITY_BLOCK",
    "CHRONOFORGE_NORMALIZE_ERROR_THRESHOLD",
    "CHRONOFORGE_NORMALIZE_DROP_THRESHOLD",
    "CHRONOFORGE_SEC_CONTACT",
    "CHRONOFORGE_VERSION",
    "CHRONOFORGE_RETRY_MAX",
    "CHRONOFORGE_CIRCUIT_COOLDOWN_S",
)


@dataclass(frozen=True)
class _Case:
    """单个连接器的门禁用例：真实声明 + 两行合成 payload + 合法空批。"""

    name: str
    connector: type
    source_id: str
    dataset_id: str
    endpoint: str
    payload: object
    empty_payload: object


_CASES: tuple[_Case, ...] = (
    _Case(
        "alternative",
        AlternativeConnector,
        "alternative",
        "alternative_fng_probe",
        "https://api.alternative.me/fng/",
        {"data": [{}, {}]},
        {"data": []},
    ),
    _Case(
        "binance_spot",
        BinanceSpotConnector,
        "binance_spot",
        "binance_spot_klines",
        "/api/v3/klines",
        [[], []],
        [],
    ),
    _Case(
        "binance_futures",
        BinanceFuturesConnector,
        "binance_futures",
        "binance_futures_klines",
        "/fapi/v1/klines",
        [[], []],
        [],
    ),
    _Case(
        "ccxt_bridge",
        CcxtBridgeConnector,
        "ccxt",
        "ccxt_ohlcv",
        "fetchOHLCV",
        [[], []],
        [],
    ),
    _Case(
        "coinmarketcap",
        CoinMarketCapConnector,
        "coinmarketcap",
        "coinmarketcap_global_probe",
        "/v1/cryptocurrency/quotes/latest",
        {"data": {"quotes": [{}, {}]}},
        {"data": {"quotes": []}},
    ),
    _Case(
        "coinmetrics",
        CoinMetricsConnector,
        "coinmetrics",
        "coinmetrics_btc_probe",
        "/v4/timeseries/asset-metrics",
        {"data": [{}, {}]},
        {"data": []},
    ),
    _Case(
        "deribit",
        DeribitConnector,
        "deribit",
        "deribit_open_interest",
        _CHART_ENDPOINT,
        {"result": {"ticks": [1, 2]}},
        {"result": {"ticks": []}},
    ),
    _Case(
        "fred",
        FREDConnector,
        "fred",
        "fred_series_probe",
        "/series/observations",
        {"observations": [{}, {}]},
        {"observations": []},
    ),
    _Case(
        "nyfed_srf",
        NYFedConnector,
        "nyfed",
        "nyfed_repo_srf_probe",
        NYFED_SRF,
        {"repo": {"operations": [{}, {}]}},
        {"repo": {"operations": []}},
    ),
    _Case(
        "nyfed_soma",
        NYFedConnector,
        "nyfed",
        "nyfed_soma_probe",
        _SOMA_ENDPOINT,
        {"soma": {"summary": [{}, {}]}},
        {"soma": {"summary": []}},
    ),
    _Case(
        "sec_edgar",
        SECEdgarConnector,
        "sec_edgar",
        "sec_submissions",
        "https://data.sec.gov/submissions/CIK0000320193.json",
        {"filings": {"recent": {"accessionNumber": ["a", "b"]}}},
        {"filings": {"recent": {"accessionNumber": []}}},
    ),
    _Case(
        "sosovalue",
        SoSoValueConnector,
        "sosovalue",
        "sosovalue_etf_probe",
        "/etfs/summary-history",
        {"code": 0, "data": [{}, {}]},
        {"code": 0, "data": []},
    ),
    _Case(
        "treasury_fiscal",
        TreasuryFiscalConnector,
        "treasury_fiscal",
        "treasury_fiscal_dts_probe",
        "/api/v2/accounting/od/operating_cash_balance",
        {"data": [{}, {}]},
        {"data": []},
    ),
    _Case(
        "treasury_refunding",
        TreasuryRefundingConnector,
        "treasury_refunding",
        "treasury_refunding_probe",
        "https://home.treasury.gov/refunding",
        {"release_html": "<html/>"},
        {"release_html": "<html/>"},
    ),
    _Case(
        "yahoo",
        YahooConnector,
        "yahoo",
        "yahoo_chart",
        "/v8/finance/chart",
        {"chart": {"result": [{"timestamp": [1, 2]}]}},
        {"chart": {"result": [{"timestamp": []}]}},
    ),
)


class _FakeRecord:
    """NormalizeStage 只依赖 raw_record_id / ingest_timestamp / model_dump。"""

    def __init__(self) -> None:
        self.raw_record_id = "pending"
        self.ingest_timestamp: datetime | None = None
        self.schema_version = "1.0"

    def model_dump(self) -> dict[str, object]:
        return {"schema_version": self.schema_version}


class _InjectedConnector:
    """薄壳连接器：合成 payload + 委托真实 row_units + 可注入声明故障。"""

    def __init__(self, case: _Case, injected: str) -> None:
        self.source_id = case.source_id
        self._endpoint = case.endpoint
        self._payload = case.payload if injected != "legit_empty" else case.empty_payload
        self._injected = injected
        self._rows = 0 if injected == "legit_empty" else 2
        self._real_row_units = case.connector.__dict__["row_units"]  # type: ignore[attr-defined]

    def capabilities(self) -> object:
        return None

    def health(self) -> object:
        return None

    def discover(self) -> list[dict[str, str]]:
        return []

    def fetch(self, request: FetchRequest) -> Any:  # noqa: ARG002 — 忽略窗口
        yield RawBatch(endpoint=self._endpoint, payload=self._payload)

    def normalize(self, raw: RawBatch) -> list[Any]:  # noqa: ARG002
        return [_FakeRecord() for _ in range(self._rows)]

    def row_units(self, raw: RawBatch) -> int | None:
        if self._injected == "undeclared":
            raise NotImplementedError("injected: connector 未声明 row_units")
        declared = self._real_row_units(self, raw)
        if self._injected == "wrong":
            # 声明比真实产出多 2 行 → 2 行未上报丢弃（2/4 ≥ 0.5 → 门禁红）
            return self._rows + 2 if declared is None else declared + 2
        return declared

    def validate(self, records: list[Any]) -> object:  # noqa: ARG002
        return None

    def checkpoint_from(self, raw: object) -> str | None:  # noqa: ARG002
        return None


def _build(tmp_stores, monkeypatch, case: _Case, injected: str):
    for var in _ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    data_dir = tmp_stores.data_dir / f"{case.name}_{injected}"
    meta_dir = tmp_stores.meta_dir / f"{case.name}_{injected}"
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
    conn = meta.connection
    conn.execute(
        "INSERT OR IGNORE INTO source_registry "
        "(source_id, display_name, access_type, base_url, rate_limit_json, license) "
        "VALUES (?, ?, 'PUBLIC', 'https://example.com', '{}', 'PUBLIC')",
        (case.source_id, case.name),
    )
    conn.execute(
        "INSERT OR REPLACE INTO dataset_registry "
        "(dataset_id, source_id, canonical_type, entity_id, params_json, frequency, "
        "continuity_model, status, created_at, revision_supported) "
        "VALUES (?, ?, 'NUMBER', 'probe', ?, '1d', 'TRADING_CALENDAR', "
        "'UNKNOWN', '2024-01-01', 0)",
        (case.dataset_id, case.source_id, json.dumps({})),
    )
    conn.commit()

    connector = _InjectedConnector(case, injected)

    def factory(job: AcquisitionJob) -> RunContext:
        return RunContext(
            run_id="",
            ingest_batch_id="",
            source_id=case.source_id,
            dataset_id=job.dataset_id,
            connector=connector,  # type: ignore[arg-type]
            raw_store=RawStore(str(data_dir)),
            canonical_store=CanonicalStoreImpl(str(data_dir)),
            meta=meta,
            settings=settings,
        )

    return PipelineRunner(factory), meta, connector


def _job(case: _Case) -> AcquisitionJob:
    return AcquisitionJob(
        dataset_id=case.dataset_id,
        start=JOB_START,
        end=JOB_END,
        mode="backfill",
        params={},
    )


def _latest_run(meta: MetaStore) -> dict[str, object]:
    row = meta.connection.execute(
        "SELECT status, checkpoint_after, error_summary FROM run_log "
        "ORDER BY started_at DESC LIMIT 1"
    ).fetchone()
    return {"status": row[0], "checkpoint_after": row[1], "error_summary": row[2]}


@pytest.mark.parametrize("case", _CASES, ids=[c.name for c in _CASES])
def test_undeclared_row_units_rejects_run(
    case: _Case, tmp_stores, monkeypatch
) -> None:
    """声明取消（继承基类默认）→ 拒绝执行，不得静默当作 0 行。"""
    runner, meta, _connector = _build(tmp_stores, monkeypatch, case, "undeclared")

    with pytest.raises(QualityError, match="未声明 row_units"):
        runner.run(_job(case))

    logged = _latest_run(meta)
    assert logged["status"] == "FAILED"
    assert logged["checkpoint_after"] is None
    assert meta.get_checkpoint(case.source_id, case.dataset_id) is None


@pytest.mark.parametrize("case", _CASES, ids=[c.name for c in _CASES])
def test_wrong_row_units_turns_gate_red(
    case: _Case, tmp_stores, monkeypatch
) -> None:
    """声明行数多于实际产出 → 未上报丢弃被计数 → QualityError。"""
    runner, meta, _connector = _build(tmp_stores, monkeypatch, case, "wrong")

    with pytest.raises(QualityError, match="normalize drop rate"):
        runner.run(_job(case))

    logged = _latest_run(meta)
    assert logged["status"] == "FAILED"
    assert logged["checkpoint_after"] is None
    assert meta.get_checkpoint(case.source_id, case.dataset_id) is None


@pytest.mark.parametrize("case", _CASES, ids=[c.name for c in _CASES])
def test_legit_empty_batch_is_not_reported(
    case: _Case, tmp_stores, monkeypatch
) -> None:
    """源端确实无数据（声明 0 / None）→ SUCCESS，cursor 照常推进。"""
    runner, meta, _connector = _build(tmp_stores, monkeypatch, case, "legit_empty")

    row = runner.run(_job(case))

    assert row.status == "SUCCESS"
    assert row.checkpoint_after == FULL_CURSOR
    assert meta.get_checkpoint(case.source_id, case.dataset_id) == FULL_CURSOR
