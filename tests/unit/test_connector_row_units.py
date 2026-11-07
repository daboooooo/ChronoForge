"""连接器行单位声明（``row_units``）契约测试（审计 C-N2 方案 B）。

运行侧不再按 payload 形状猜行数；每个连接器必须自行声明：
- 返回值为 normalize 的候选行数（产出 + 上报跳过）；
- 0 表示源端确实无数据（合法空批）；
- None 仅用于无行级粒度的单文档源（treasury_refunding）。
未声明 → NotImplementedError（Runner 转 QualityError 拒绝执行）。
"""

from __future__ import annotations

import pytest

from chronoforge.connectors.alternative import AlternativeConnector
from chronoforge.connectors.base import DataConnector, RawBatch
from chronoforge.connectors.binance_futures import BinanceFuturesConnector
from chronoforge.connectors.binance_spot import BinanceSpotConnector
from chronoforge.connectors.ccxt_bridge import CcxtBridgeConnector
from chronoforge.connectors.coinmarketcap import CoinMarketCapConnector
from chronoforge.connectors.coinmetrics import CoinMetricsConnector
from chronoforge.connectors.deribit import _CHART_ENDPOINT, DeribitConnector
from chronoforge.connectors.errors import SchemaError
from chronoforge.connectors.fred import FREDConnector
from chronoforge.connectors.nyfed import _ENDPOINT as NYFED_SRF
from chronoforge.connectors.nyfed import _SOMA_ENDPOINT, NYFedConnector
from chronoforge.connectors.sec_edgar import SECEdgarConnector
from chronoforge.connectors.sosovalue import SoSoValueConnector
from chronoforge.connectors.treasury_fiscal import TreasuryFiscalConnector
from chronoforge.connectors.treasury_refunding import TreasuryRefundingConnector
from chronoforge.connectors.yahoo import YahooConnector


def _batch(endpoint: str, payload: object) -> RawBatch:
    return RawBatch(endpoint=endpoint, payload=payload, raw_meta={})


def _units(cls: type, endpoint: str, payload: object) -> int | None:
    # row_units 不依赖实例状态（HTTP client 等），直接走类方法
    return cls.__new__(cls).row_units(_batch(endpoint, payload))


_SOMA_ENDPOINT_LOCAL = _SOMA_ENDPOINT
_COMPANYFACTS = "https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json"
_SUBMISSIONS = "https://data.sec.gov/submissions/CIK0000320193.json"
_CHART = _CHART_ENDPOINT

# (连接器, endpoint, 非空 payload, 期望行数, 空 payload)
CASES: list[tuple[type, str, object, int, object]] = [
    (AlternativeConnector, "https://api.alternative.me/fng/", {"data": [{}, {}]}, 2, {"data": []}),
    (BinanceSpotConnector, "/api/v3/klines", [[], []], 2, []),
    (BinanceFuturesConnector, "/fapi/v1/klines", [[], []], 2, []),
    (CcxtBridgeConnector, "fetchOHLCV", [[], []], 2, []),
    (
        CoinMarketCapConnector,
        "/v1/cryptocurrency/quotes/latest",
        {"data": {"quotes": [{}, {}]}},
        2,
        {"data": {"quotes": []}},
    ),
    (CoinMetricsConnector, "/v4/timeseries/asset-metrics", {"data": [{}, {}]}, 2, {"data": []}),
    (
        DeribitConnector,
        _CHART,
        {"result": {"ticks": [1, 2]}},
        2,
        {"result": {"ticks": []}},
    ),
    (
        FREDConnector,
        "/series/observations",
        {"observations": [{}, {}]},
        2,
        {"observations": []},
    ),
    (
        NYFedConnector,
        NYFED_SRF,
        {"repo": {"operations": [{}, {}]}},
        2,
        {"repo": {"operations": []}},
    ),
    (
        NYFedConnector,
        _SOMA_ENDPOINT_LOCAL,
        {"soma": {"summary": [{}, {}]}},
        2,
        {"soma": {"summary": []}},
    ),
    (
        SECEdgarConnector,
        _SUBMISSIONS,
        {"filings": {"recent": {"accessionNumber": ["a", "b"]}}},
        2,
        {"filings": {"recent": {"accessionNumber": []}}},
    ),
    (
        SECEdgarConnector,
        _COMPANYFACTS,
        {"facts": {"us-gaap": {"Revenues": {"units": {"USD": [{}, {}]}}}, "dei": {}}},
        2,
        {"facts": {}},
    ),
    (
        SoSoValueConnector,
        "/etf/summary",
        {"code": 0, "data": [{}, {}]},
        2,
        {"code": 0, "data": []},
    ),
    (
        TreasuryFiscalConnector,
        "/api/v2/accounting/od/operating_cash_balance",
        {"data": [{}, {}]},
        2,
        {"data": []},
    ),
    (
        YahooConnector,
        "/v8/finance/chart",
        {"chart": {"result": [{"timestamp": [1, 2]}]}},
        2,
        {"chart": {"result": []}},
    ),
]


@pytest.mark.parametrize(("cls", "endpoint", "payload", "expected", "_empty"), CASES)
def test_non_empty_row_units(
    cls: type, endpoint: str, payload: object, expected: int, _empty: object
) -> None:
    assert _units(cls, endpoint, payload) == expected


@pytest.mark.parametrize(("cls", "endpoint", "_payload", "_expected", "empty"), CASES)
def test_legit_empty_batch_returns_zero(
    cls: type, endpoint: str, _payload: object, _expected: int, empty: object
) -> None:
    assert _units(cls, endpoint, empty) == 0


def test_single_document_source_declares_no_row_unit() -> None:
    """treasury_refunding：单文档源显式声明 None（不是静默猜 0）。"""
    assert (
        _units(
            TreasuryRefundingConnector,
            "https://www.treasury.gov",
            {"release_html": "<html/>"},
        )
        is None
    )


def test_deribit_chart_uses_ticks_not_result_length() -> None:
    """chart 的 result 只有 1 个元素；行单位必须是 ticks 长度。"""
    payload = {"result": {"ticks": [1, 2, 3], "open": [1, 2, 3]}}
    assert _units(DeribitConnector, _CHART, payload) == 3


def test_yahoo_uses_timestamps_not_result_length() -> None:
    """yahoo 的 result 只有 1 个元素；行单位必须是 timestamp 长度。"""
    payload = {"chart": {"result": [{"timestamp": [1, 2, 3, 4]}]}}
    assert _units(YahooConnector, "/v8/finance/chart", payload) == 4


def test_companyfacts_counts_nested_observation_entries() -> None:
    """companyfacts 四层嵌套：units 下每个观测数组的条目累计。"""
    payload = {
        "facts": {
            "us-gaap": {
                "Revenues": {"units": {"USD": [{}, {}, {}]}},
                "Assets": {"units": {"USD": [{}, {}]}},
            },
            "dei": {"EntityCommonStockSharesOutstanding": {"units": {"shares": [{}]}}},
        }
    }
    assert _units(SECEdgarConnector, _COMPANYFACTS, payload) == 6


@pytest.mark.parametrize(
    ("cls", "endpoint", "payload"),
    [
        (NYFedConnector, NYFED_SRF, {"repo": {"operations": "not-a-list"}}),
        (NYFedConnector, NYFED_SRF, {}),
        (DeribitConnector, _CHART, {"result": [1, 2]}),
        (YahooConnector, "/v8/finance/chart", {"chart": {"result": [{}]}}),
        (CoinMarketCapConnector, "/quotes", {"data": {"quotes": "bad"}}),
        (FREDConnector, "/series/observations", {"observations": None}),
    ],
)
def test_shape_drift_raises_schema_error(cls: type, endpoint: str, payload: object) -> None:
    """形状漂移不得静默返回 0，必须抛 SchemaError 让 run 失败。"""
    with pytest.raises(SchemaError):
        _units(cls, endpoint, payload)


def test_undeclared_connector_raises_not_implemented() -> None:
    """未声明 row_units 的连接器 → NotImplementedError（Runner 转 QualityError）。"""
    with pytest.raises(NotImplementedError, match="未声明 row_units"):
        DataConnector.row_units(object(), _batch("/probe", []))
