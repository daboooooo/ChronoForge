"""DATA-SOURCE sosovalue 集成测试 — SoSoValueConnector。

覆盖：
- 包装解析：{code:0,message,data} → RawBatch.payload 保持包装原样
- 错误映射：400001/400003 → ProviderError；401 → AuthError；
  429/code=42901 + retry_after → RateLimitError（含 on_rate_limited 冷却）；
  TransportError；非法 JSON → SchemaError
- fetch：日期换算（datetime→YYYY-MM-DD）、30 天钳制、长窗口拆分、坏 ticker 拒绝
- normalize：NUMBER/FLOW 逐字段断言（observation=D T00:00、
  release=revision=(D+1) T00:00、FLOW period 右开、units=USD、
  字符串带逗号强转）、T+1 null 行跳过、volume bug 不采、坏行 skip
- checkpoint_from / capabilities
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest

from chronoforge.connectors.base import (
    CapabilityMatrix,
    FetchRequest,
    RawBatch,
)
from chronoforge.connectors.errors import (
    AuthError,
    ProviderError,
    RateLimitError,
    SchemaError,
    TransportError,
)
from chronoforge.connectors.sosovalue import (
    SoSoValueConnector,
    _SoSoValueSettings,
)
from chronoforge.models.enums import CanonicalType
from chronoforge.models.macro import FLOW, NUMBER

# Fixture paths
_FIXTURES = Path(__file__).parent.parent / "fixtures" / "sosovalue"


# ── Helpers ────────────────────────────────────────────────────────────


def _load_fixture(name: str) -> Any:
    """Load a JSON fixture file."""
    with open(_FIXTURES / name, encoding="utf-8") as f:
        return json.load(f)


def _mock_response(
    status_code: int = 200,
    json_data: Any = None,
) -> MagicMock:
    """Create a mock httpx Response."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    if json_data is None:
        resp.json.side_effect = ValueError("no json")
    else:
        resp.json.return_value = json_data
    return resp


def _create_mock_connector(
    resp: MagicMock | list[MagicMock],
) -> tuple[SoSoValueConnector, MagicMock]:
    """Create a mock connector instance for fetch/normalize tests."""
    mock_client = MagicMock()
    if isinstance(resp, list):
        queue = list(resp)

        def _side_effect(*args: Any, **kwargs: Any) -> MagicMock:
            if not queue:
                return _mock_response(200, {"code": 0, "message": "success",
                                            "data": []})
            return queue.pop(0)

        mock_client.get.side_effect = _side_effect
    else:
        mock_client.get.return_value = resp

    conn = SoSoValueConnector.__new__(SoSoValueConnector)
    conn._client = mock_client
    conn._rate_limiter = MagicMock()
    conn._api_key = "test_key"

    return conn, mock_client


def _summary_request(
    metric: str = "total_net_inflow",
    start: datetime | None = datetime(2026, 9, 20),
    end: datetime | None = datetime(2026, 9, 24),
) -> FetchRequest:
    """构造 summary-history FetchRequest。"""
    return FetchRequest(
        dataset_id="sosovalue_etf_us_btc_summary_total_net_inflow",
        params={
            "endpoint": "summary-history",
            "symbol": "BTC",
            "country_code": "US",
            "metric": metric,
        },
        start=start,
        end=end,
        cursor=None,
    )


def _ticker_request(
    ticker: str = "IBIT",
    metric: str = "net_inflow",
    start: datetime | None = datetime(2026, 9, 20),
    end: datetime | None = datetime(2026, 9, 24),
) -> FetchRequest:
    """构造 etf-history FetchRequest。"""
    return FetchRequest(
        dataset_id=f"sosovalue_etf_us_btc_{ticker}_{metric}",
        params={
            "endpoint": "etf-history",
            "symbol": "BTC",
            "country_code": "US",
            "ticker": ticker,
            "metric": metric,
        },
        start=start,
        end=end,
        cursor=None,
    )


# ── capabilities ───────────────────────────────────────────────────────


class TestCapabilities:
    """SoSoValueConnector.capabilities() 测试。"""

    def test_capabilities(self) -> None:
        conn = SoSoValueConnector(
            _SoSoValueSettings(api_key="test_key")
        )
        caps = conn.capabilities()
        assert isinstance(caps, CapabilityMatrix)
        assert caps.canonical_types == frozenset({
            CanonicalType.NUMBER,
            CanonicalType.FLOW,
        })
        assert caps.intervals == frozenset()
        assert caps.supports_revision is False
        assert caps.supports_websocket is False
        assert caps.max_history_days == 30
        conn.close()


# ── fetch：日期换算与钳制 ──────────────────────────────────────────────


class TestFetch:
    """fetch() 日期参数、30 天钳制、长窗口拆分、坏 ticker 校验。"""

    def test_date_params_format(self) -> None:
        """start/end datetime → YYYY-MM-DD 查询参数（含 T+1 回退 1 天）。"""
        fixture = _load_fixture("summary_history.json")
        conn, mock_client = _create_mock_connector(
            _mock_response(200, fixture)
        )
        batches = list(conn.fetch(_summary_request()))
        assert len(batches) == 1

        call = mock_client.get.call_args
        assert call.args[0] == "/etfs/summary-history"
        params = call.kwargs["params"]
        assert params["start_date"] == "2026-09-19"  # 09-20 回退 1 天
        assert params["end_date"] == "2026-09-24"
        assert params["symbol"] == "BTC"
        assert params["country_code"] == "US"
        assert params["limit"] == 300

        # payload 保持包装原样 + raw_meta 带 series_id/metric
        assert batches[0].payload == fixture
        assert batches[0].raw_meta["metric"] == "total_net_inflow"
        assert batches[0].raw_meta["series_id"] == (
            "etf_us_btc_summary_total_net_inflow"
        )
        assert batches[0].endpoint == "/etfs/summary-history"

    def test_long_range_split_into_sub_ranges(self) -> None:
        """跨度 > 15 天 → 拆为多个 ≤15 天子请求（demo 每响应 ≤20 行）。"""
        conn, mock_client = _create_mock_connector(
            _mock_response(200, {"code": 0, "message": "success", "data": []})
        )
        batches = list(
            conn.fetch(_summary_request(start=datetime(2026, 9, 1)))
        )
        assert len(batches) == 2
        calls = mock_client.get.call_args_list
        # 起点 09-01 回退 1 天 = 08-31；子区间 1：08-31..09-15；
        # 子区间 2：09-16..09-24（无重叠）
        assert calls[0].kwargs["params"]["start_date"] == "2026-08-31"
        assert calls[0].kwargs["params"]["end_date"] == "2026-09-15"
        assert calls[1].kwargs["params"]["start_date"] == "2026-09-16"
        assert calls[1].kwargs["params"]["end_date"] == "2026-09-24"

    def test_ticker_history_path_and_meta(self) -> None:
        """etf-history：URL path 拼接 + raw_meta 带 ticker。"""
        fixture = _load_fixture("etf_history_ibit.json")
        conn, mock_client = _create_mock_connector(
            _mock_response(200, fixture)
        )
        batches = list(conn.fetch(_ticker_request()))
        assert len(batches) == 1

        call = mock_client.get.call_args
        assert call.args[0] == "/etfs/IBIT/history"
        assert batches[0].raw_meta["ticker"] == "IBIT"
        assert batches[0].raw_meta["series_id"] == "etf_us_btc_IBIT_net_inflow"

    def test_start_clamped_to_30_days(self) -> None:
        """start 早于 30 天前 → 钳制为 today-30d（首个子请求的 start）。"""
        conn, mock_client = _create_mock_connector(
            _mock_response(200, {"code": 0, "message": "success", "data": []})
        )
        list(conn.fetch(_summary_request(start=datetime(2020, 1, 1))))

        calls = mock_client.get.call_args_list
        today = datetime.now(UTC).replace(tzinfo=None).date()
        assert calls[0].kwargs["params"]["start_date"] == (
            today - timedelta(days=30)
        ).isoformat()

    def test_window_older_than_limit_yields_nothing(self) -> None:
        """窗口整体早于可回看范围（钳制后 start > end）→ 不发请求。"""
        conn, mock_client = _create_mock_connector(
            _mock_response(200, {"code": 0, "data": []})
        )
        batches = list(
            conn.fetch(
                _summary_request(
                    start=datetime(2020, 1, 1), end=datetime(2020, 2, 1)
                )
            )
        )
        assert batches == []
        mock_client.get.assert_not_called()

    def test_invalid_ticker_rejected(self) -> None:
        """ticker 含 path 注入字符 → ValueError，不发请求。"""
        conn, mock_client = _create_mock_connector(
            _mock_response(200, {"code": 0, "data": []})
        )
        with pytest.raises(ValueError, match="invalid ticker"):
            list(conn.fetch(_ticker_request(ticker="../evil")))
        mock_client.get.assert_not_called()

    def test_missing_params_rejected(self) -> None:
        """endpoint/metric 缺失 → ValueError。"""
        conn, _ = _create_mock_connector(_mock_response(200, {"code": 0}))
        req = FetchRequest(
            dataset_id="d",
            params={"endpoint": "summary-history"},
            start=None, end=None, cursor=None,
        )
        with pytest.raises(ValueError, match="required"):
            list(conn.fetch(req))

    def test_unknown_endpoint_rejected(self) -> None:
        """endpoint 不识别 → ValueError。"""
        conn, _ = _create_mock_connector(_mock_response(200, {"code": 0}))
        req = FetchRequest(
            dataset_id="d",
            params={"endpoint": "nope", "metric": "net_inflow",
                    "symbol": "BTC", "country_code": "US"},
            start=None, end=None, cursor=None,
        )
        with pytest.raises(ValueError, match="unknown endpoint"):
            list(conn.fetch(req))


# ── 错误映射 ───────────────────────────────────────────────────────────


class TestErrorMapping:
    """_request() 错误映射（HTTP 200 + body code 非 0 风格）。"""

    def test_code_400001_provider_error(self) -> None:
        """无效 symbol+country 组合 → ProviderError。"""
        conn, _ = _create_mock_connector(
            _mock_response(200, {
                "code": 400001,
                "message": "Unsupported ETF category: symbol=LTC, "
                           "country_code=HK",
            })
        )
        with pytest.raises(ProviderError, match="400001"):
            conn.list_etfs("LTC", "HK")

    def test_code_400003_provider_error(self) -> None:
        """参数非法 → ProviderError。"""
        conn, _ = _create_mock_connector(
            _mock_response(200, {
                "code": 400003,
                "message": "Invalid parameter: symbol",
            })
        )
        with pytest.raises(ProviderError, match="400003"):
            conn.list_etfs("FOO", "US")

    def test_http_401_auth_error(self) -> None:
        """key 无效 → AuthError（不重试）。"""
        conn, _ = _create_mock_connector(_mock_response(401, None))
        with pytest.raises(AuthError):
            conn.list_etfs("BTC", "US")

    def test_http_429_rate_limit_error(self) -> None:
        """HTTP 429 + body retry_after → RateLimitError。"""
        conn, _ = _create_mock_connector(
            _mock_response(429, {
                "code": 42901,
                "message": "Rate limit exceeded",
                "details": {"limit": 20, "window": "60s", "retry_after": 7},
            })
        )
        with pytest.raises(RateLimitError) as exc_info:
            conn.list_etfs("BTC", "US")
        assert exc_info.value.retry_after == 7
        conn._rate_limiter.on_rate_limited.assert_called_once_with(7)

    def test_transport_error(self) -> None:
        """网络异常 → TransportError（可重试）。"""
        conn, mock_client = _create_mock_connector(_mock_response(200, {}))
        mock_client.get.side_effect = httpx.ConnectError("boom")
        with pytest.raises(TransportError):
            conn.list_etfs("BTC", "US")

    def test_invalid_json_schema_error(self) -> None:
        """响应非合法 JSON → SchemaError。"""
        conn, _ = _create_mock_connector(_mock_response(200, None))
        with pytest.raises(SchemaError):
            conn.list_etfs("BTC", "US")

    def test_http_500_provider_error(self) -> None:
        """其他 HTTP >=400 → ProviderError。"""
        conn, _ = _create_mock_connector(_mock_response(500, None))
        with pytest.raises(ProviderError, match="500"):
            conn.list_etfs("BTC", "US")


# ── normalize ──────────────────────────────────────────────────────────


class TestNormalize:
    """normalize() NUMBER/FLOW 逐字段、T+1 null 跳过、坏行 skip。"""

    def _normalize_summary(self, metric: str) -> list[Any]:
        """用 summary fixture 对指定 metric 跑 normalize。"""
        conn = SoSoValueConnector.__new__(SoSoValueConnector)
        raw = RawBatch(
            endpoint="/etfs/summary-history",
            payload=_load_fixture("summary_history.json"),
            raw_meta={
                "series_id": f"etf_us_btc_summary_{metric}",
                "metric": metric,
            },
        )
        return conn.normalize(raw)

    def test_flow_metric_fields(self) -> None:
        """total_net_inflow → FLOW，时间语义逐字段断言。"""
        records = self._normalize_summary("total_net_inflow")
        # 2026-09-24 为 T+0 null 行 → 跳过；剩 09-23 与 09-22
        assert len(records) == 2
        rec = records[0]
        assert isinstance(rec, FLOW)
        assert rec.source == "sosovalue"
        assert rec.source_id == "etf_us_btc_summary_total_net_inflow"
        assert rec.observation_time == datetime(2026, 9, 23, 0, 0, 0)
        # T+1 结算：release = revision = 次日 00:00（固定值）
        assert rec.release_time == datetime(2026, 9, 24, 0, 0, 0)
        assert rec.revision_time == rec.release_time
        # FLOW period 右开日界
        assert rec.period_start == datetime(2026, 9, 23, 0, 0, 0)
        assert rec.period_end == datetime(2026, 9, 24, 0, 0, 0)
        assert rec.value == 346900000.0
        assert rec.units == "USD"
        assert rec.seasonal_adjustment == "NOT_SEASONALLY_ADJUSTED"
        assert rec.vintage_date == ""
        assert rec.quality_status.value == "VALID"

    def test_number_metric_fields(self) -> None:
        """cum_net_inflow → NUMBER，period 字段不出现。"""
        records = self._normalize_summary("cum_net_inflow")
        assert len(records) == 2
        rec = records[0]
        assert isinstance(rec, NUMBER)
        assert not isinstance(rec, FLOW)
        assert rec.source_id == "etf_us_btc_summary_cum_net_inflow"
        assert rec.observation_time == datetime(2026, 9, 23, 0, 0, 0)
        assert rec.value == 57680000000.0

    def test_string_with_commas_coerced(self) -> None:
        """字符串数值带千分位逗号 → float 强转。"""
        records = self._normalize_summary("total_net_inflow")
        by_date = {
            r.observation_time.date(): r for r in records
        }
        assert by_date[datetime(2026, 9, 22).date()].value == 1234567.89

    def test_t1_null_row_skipped(self) -> None:
        """T+0 未结算行（flow 字段 null）→ skip，不产记录。"""
        records = self._normalize_summary("total_net_inflow")
        dates = {r.observation_time.date() for r in records}
        assert datetime(2026, 9, 24).date() not in dates

    def test_bad_rows_skipped(self) -> None:
        """坏日期行与非数值行 → skip 不抛。"""
        conn = SoSoValueConnector.__new__(SoSoValueConnector)
        raw = RawBatch(
            endpoint="/etfs/IBIT/history",
            payload=_load_fixture("etf_history_ibit.json"),
            raw_meta={
                "series_id": "etf_us_btc_IBIT_net_inflow",
                "metric": "net_inflow",
                "ticker": "IBIT",
            },
        )
        records = conn.normalize(raw)
        # 09-24 null 跳过、bad-date 跳过、09-19 非数值跳过 → 仅 09-23
        assert len(records) == 1
        assert records[0].observation_time == datetime(2026, 9, 23, 0, 0, 0)
        assert isinstance(records[0], FLOW)
        assert records[0].value == 50195600.0

    def test_volume_field_never_read(self) -> None:
        """官方 volume bug（返回 value_traded 字符串值）→ 任何字段不采自 volume。"""
        conn = SoSoValueConnector.__new__(SoSoValueConnector)
        raw = RawBatch(
            endpoint="/etfs/IBIT/history",
            payload=_load_fixture("etf_history_ibit.json"),
            raw_meta={
                "series_id": "etf_us_btc_IBIT_net_inflow",
                "metric": "net_inflow",
                "ticker": "IBIT",
            },
        )
        records = conn.normalize(raw)
        # volume 的假值是 value_traded 的字符串（"1810000000"/"910925300"），
        # net_inflow 记录值必须来自 net_inflow 字段
        assert records[0].value == 50195600.0

    def test_empty_data(self) -> None:
        """data=[] → []。"""
        conn = SoSoValueConnector.__new__(SoSoValueConnector)
        raw = RawBatch(
            endpoint="/etfs/summary-history",
            payload={"code": 0, "message": "success", "data": []},
            raw_meta={
                "series_id": "etf_us_btc_summary_total_net_inflow",
                "metric": "total_net_inflow",
            },
        )
        assert conn.normalize(raw) == []

    def test_unknown_metric_raises(self) -> None:
        """metric 不在映射表 → ValueError（防御注册错误）。"""
        conn = SoSoValueConnector.__new__(SoSoValueConnector)
        raw = RawBatch(
            endpoint="/etfs/summary-history",
            payload=_load_fixture("summary_history.json"),
            raw_meta={
                "series_id": "x",
                "metric": "currency_share",
            },
        )
        with pytest.raises(ValueError, match="unknown metric"):
            conn.normalize(raw)

    def test_unknown_endpoint_raises(self) -> None:
        """endpoint 不识别 → ValueError。"""
        conn = SoSoValueConnector.__new__(SoSoValueConnector)
        raw = RawBatch(
            endpoint="/other",
            payload={"code": 0, "data": []},
            raw_meta={},
        )
        with pytest.raises(ValueError, match="unknown endpoint"):
            conn.normalize(raw)


# ── checkpoint_from / discover ────────────────────────────────────────


class TestCheckpointAndDiscover:
    """checkpoint_from 与 discover。"""

    def test_checkpoint_from_raw(self) -> None:
        """RawBatch → 最新 date（源行序不保证升序，取 max 而非末元素）。"""
        conn = SoSoValueConnector.__new__(SoSoValueConnector)
        raw = RawBatch(
            endpoint="/etfs/summary-history",
            payload=_load_fixture("summary_history.json"),
            raw_meta={},
        )
        # 源响应按日期倒序（09-24/09-23/09-22）→ max = 09-24
        assert conn.checkpoint_from(raw) == "2026-09-24"

    def test_checkpoint_from_records(self) -> None:
        """records → 最后一条 observation 的 YYYY-MM-DD。"""
        conn = SoSoValueConnector.__new__(SoSoValueConnector)
        records = conn.normalize(RawBatch(
            endpoint="/etfs/summary-history",
            payload=_load_fixture("summary_history.json"),
            raw_meta={
                "series_id": "etf_us_btc_summary_total_net_inflow",
                "metric": "total_net_inflow",
            },
        ))
        assert conn.checkpoint_from(records) == "2026-09-23"

    def test_discover(self) -> None:
        """discover → InstrumentRef 列表（entity_id=ticker）。"""
        conn, _ = _create_mock_connector(
            _mock_response(200, _load_fixture("etf_list.json"))
        )
        refs = conn.discover()
        assert refs == [
            {"entity_id": "IBIT", "instrument_id": "IBIT",
             "market_id": "US_ETF"},
            {"entity_id": "FBTC", "instrument_id": "FBTC",
             "market_id": "US_ETF"},
        ]

    def test_checkpoint_from_empty(self) -> None:
        """空 payload → None。"""
        conn = SoSoValueConnector.__new__(SoSoValueConnector)
        assert conn.checkpoint_from(RawBatch(
            endpoint="/etfs/summary-history",
            payload={"code": 0, "data": []},
            raw_meta={},
        )) is None
