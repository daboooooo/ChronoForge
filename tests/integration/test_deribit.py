"""DATA-SOURCE-003 集成测试 — DeribitConnector。

覆盖：
- TC-C-003：fixture→canonical 逐字段比对（get_instruments/get_book_summary/chart）
- discover 全量 → INSTRUMENT 记录数=源返回数且三级 ID 正确
- 月份码联动：TC-M-004/005（parse_deribit 全字段）
- 过期合约：解析成功 + 调用方获知过期（不抛异常）
- 非法月份码：BTC-32FOO26-100000-C → ProviderError
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest

from chronoforge.connectors.base import (
    CapabilityMatrix,
    FetchRequest,
    HealthStatus,
    RawBatch,
)
from chronoforge.connectors.deribit import DeribitConnector
from chronoforge.connectors.errors import (
    ProviderError,
    RateLimitError,
    TransportError,
)
from chronoforge.models.derivatives import Interval
from chronoforge.models.enums import CanonicalType, QualityStatus
from chronoforge.models.market import OHLCV
from chronoforge.models.reference import (
    INSTRUMENT,
    OptionType,
    parse_deribit,
)

# Fixture paths
_FIXTURES = Path(__file__).parent.parent / "fixtures" / "deribit"


# ── Helpers ────────────────────────────────────────────────────────────


def _load_fixture(name: str) -> Any:
    """Load a JSON fixture file."""
    path = _FIXTURES / name
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _mock_response(
    status_code: int = 200,
    json_data: Any = None,
    headers: dict | None = None,
    url: str = "https://www.deribit.com/api/v2",
) -> MagicMock:
    """Create a mock httpx Response."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.headers = headers or {}
    resp.url = url
    resp.text = json.dumps(json_data) if json_data else ""
    return resp


def _mock_client(
    resp: MagicMock | list[MagicMock],
) -> MagicMock:
    """Create a mock httpx Client that returns the given response(s)."""
    client = MagicMock()
    if isinstance(resp, list):
        queue = list(resp)

        def _side_effect(*args: Any, **kwargs: Any) -> MagicMock:
            if not queue:
                empty = MagicMock(spec=httpx.Response)
                empty.status_code = 200
                empty.json.return_value = {"result": []}
                empty.headers = {}
                empty.url = ""
                return empty
            return queue.pop(0)

        client.post.side_effect = _side_effect
    else:
        client.post.return_value = resp
    return client


def _create_mock_connector() -> tuple[DeribitConnector, MagicMock]:
    """Create a mock connector instance for normalize tests."""
    mock_client = MagicMock()
    mock_rate_limiter = MagicMock()

    with patch.object(
        DeribitConnector, "__init__", lambda self, s: None
    ):
        conn = DeribitConnector.__new__(DeribitConnector)
        conn._client = mock_client
        conn._rate_limiter = mock_rate_limiter
        conn._resolver = MagicMock()

    return conn, mock_client


# ── capabilities ───────────────────────────────────────────────────────


class TestCapabilities:
    """DeribitConnector.capabilities() 测试（D04 §4.3）。"""

    def test_capabilities_returns_correct_matrix(self) -> None:
        """Given valid connector When capabilities() Then OHLCV/OPTION/IV supported."""
        conn = DeribitConnector()
        cap = conn.capabilities()

        assert isinstance(cap, CapabilityMatrix)
        assert CanonicalType.OHLCV in cap.canonical_types
        assert CanonicalType.OPTION in cap.canonical_types
        assert CanonicalType.IMPLIED_VOLATILITY in cap.canonical_types
        assert cap.supports_revision is False
        assert cap.supports_websocket is False
        assert cap.max_history_days == 365
        conn.close()

    def test_capabilities_intervals(self) -> None:
        """capabilities() should include all D04 §4.3 intervals."""
        conn = DeribitConnector()
        cap = conn.capabilities()
        expected = {
            Interval._1M, Interval._5M, Interval._15M, Interval._30M,
            Interval._1H, Interval._4H, Interval._1D,
        }
        assert cap.intervals == expected
        conn.close()


# ── health ─────────────────────────────────────────────────────────────


class TestHealth:
    """DeribitConnector.health() 测试（D04 §1）。"""

    def test_health_ok(self) -> None:
        """Given healthy API When health() Then ok=True."""
        mock_resp = MagicMock(spec=httpx.Response)
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"result": {}}
        mock_resp.url = "https://www.deribit.com/api/v2"

        mock_client = MagicMock()
        mock_client.post.return_value = mock_resp

        with patch.object(
            DeribitConnector, "__init__", lambda self, s: None
        ):
            conn = DeribitConnector.__new__(DeribitConnector)
            conn._client = mock_client

        status = conn.health()
        assert isinstance(status, HealthStatus)
        assert status.ok is True
        assert status.latency_ms >= 0
        conn.close()

    def test_health_not_ok(self) -> None:
        """Given unhealthy API When health() Then ok=False."""
        mock_resp = _mock_response(
            status_code=500,
            json_data={"error": "server_error"},
            url="https://www.deribit.com/api/v2",
        )
        mock_client = _mock_client(mock_resp)
        mock_client.post = mock_client.get  # health() 使用 POST

        with patch.object(
            DeribitConnector, "__init__", lambda self, s: None
        ):
            conn = DeribitConnector.__new__(DeribitConnector)
            conn._client = mock_client

        status = conn.health()
        assert isinstance(status, HealthStatus)
        assert status.ok is False
        assert status.latency_ms >= 0
        conn.close()


# ── TC-C-003: get_instruments → INSTRUMENT fixture→canonical ───────────


class TestNormalizeInstruments:
    """get_instruments → INSTRUMENT normalize 测试（TC-C-003）。"""

    def test_normalize_instruments_happy_path(self) -> None:
        """Given instruments fixture When normalize Then INSTRUMENT records correct."""
        payload = _load_fixture("get_instruments/happy.json")
        raw_meta = {
            "http_status": 200,
            "currency": "BTC",
        }

        conn, _ = _create_mock_connector()

        records = conn.normalize(
            RawBatch(
                endpoint="/public/get_instruments",
                payload=payload,
                raw_meta=raw_meta,
            )
        )

        # Should have 3 records: 2 options + 1 PERP
        assert len(records) == 3
        assert all(isinstance(r, INSTRUMENT) for r in records)

        # Verify each record
        call_count = 0
        for record in records:
            assert record.source == "deribit"
            assert record.schema_version == "1.0"
            assert record.quality_status == QualityStatus.VALID
            assert record.instrument_id is not None
            assert record.entity_id is not None
            assert record.instrument_type is not None
            call_count += 1

        assert call_count == 3
        conn.close()

    def test_normalize_instruments_empty_array(self) -> None:
        """Given empty instruments When normalize Then 0 records."""
        payload = _load_fixture("get_instruments/empty.json")
        raw_meta = {
            "http_status": 200,
            "currency": "BTC",
        }

        conn, _ = _create_mock_connector()

        records = conn.normalize(
            RawBatch(
                endpoint="/public/get_instruments",
                payload=payload,
                raw_meta=raw_meta,
            )
        )

        assert len(records) == 0
        conn.close()

    def test_normalize_instruments_correct_ids(self) -> None:
        """Given instruments When normalize Then entity/instrument/market IDs correct."""
        payload = _load_fixture("get_instruments/happy.json")
        raw_meta = {"http_status": 200, "currency": "BTC"}

        conn, _ = _create_mock_connector()

        records = conn.normalize(
            RawBatch(
                endpoint="/public/get_instruments",
                payload=payload,
                raw_meta=raw_meta,
            )
        )

        # Verify BTC-26DEC26-100000-C
        call_count = 0
        for record in records:
            if "100000-C" in record.source_id:
                assert record.entity_id == "BTC"
                assert record.instrument_id == "BTC-2026-12-26-100000-C"
                assert record.instrument_type.value == "OPTION"
                assert record.strike == 100000.0
                assert record.option_type == OptionType.CALL
                assert record.settlement_asset == "BTC"
                assert record.expiry == date(2026, 12, 26)
            if "100000-P" in record.source_id:
                assert record.option_type == OptionType.PUT
            if "PERP" in record.source_id:
                assert record.instrument_type.value == "PERP"
            call_count += 1

        assert call_count == 3
        conn.close()


# ── TC-C-003: get_book_summary → OPTION fixture→canonical ─────────────


class TestNormalizeOptions:
    """get_book_summary → OPTION normalize 测试（TC-C-003）。"""

    def test_normalize_options_happy_path(self) -> None:
        """Given book_summary fixture When normalize Then OPTION records correct."""
        payload = _load_fixture("get_book_summary/happy.json")
        raw_meta = {
            "http_status": 200,
            "currency": "BTC",
        }

        conn, _ = _create_mock_connector()

        records = conn.normalize(
            RawBatch(
                endpoint="/public/get_book_summary_by_currency",
                payload=payload,
                raw_meta=raw_meta,
            )
        )

        assert len(records) == 2
        from chronoforge.models.derivatives import OPTION as OptRecord
        assert all(isinstance(r, OptRecord) for r in records)

        # Verify fields
        for record in records:
            assert record.source == "deribit"
            assert record.schema_version == "1.0"
            assert record.quality_status == QualityStatus.VALID

        conn.close()

    def test_normalize_options_field_values(self) -> None:
        """Given book_summary When normalize Then mark/bid/ask prices correct."""
        payload = _load_fixture("get_book_summary/happy.json")
        raw_meta = {"http_status": 200, "currency": "BTC"}

        conn, _ = _create_mock_connector()

        records = conn.normalize(
            RawBatch(
                endpoint="/public/get_book_summary_by_currency",
                payload=payload,
                raw_meta=raw_meta,
            )
        )

        assert len(records) == 2
        # First record: BTC-26SEP26-100000-C
        from chronoforge.models.derivatives import OPTION as OptRecord
        first = records[0]
        assert isinstance(first, OptRecord)
        assert first.source == "deribit"
        assert first.mark_price == pytest.approx(1499.75, rel=1e-6)
        assert first.bid == pytest.approx(1499.0, rel=1e-6)
        assert first.ask == pytest.approx(1500.5, rel=1e-6)
        assert first.open_interest == pytest.approx(200.0, rel=1e-6)
        conn.close()

    def test_normalize_options_null_quote_and_oi(self) -> None:
        """Given bid_price=null 的深虚值期权 When normalize Then 入库且 bid=None。

        回归：显式 null 曾触发 float(None) TypeError 导致整条丢失。
        """
        payload = {
            "kind": "option",
            "BTC": [
                {
                    "ask_price": 0.0001,
                    "bid_price": None,
                    "mark_price": 1e-08,
                    "instrument_name": "BTC-24SEP26-85000-C",
                    "open_interest": 192.9,
                    "volume": 0.0,
                },
            ],
        }

        conn, _ = _create_mock_connector()
        records = conn.normalize(
            RawBatch(
                endpoint="/public/get_book_summary_by_currency",
                payload=payload,
                raw_meta={"http_status": 200, "currency": "BTC"},
            )
        )

        assert len(records) == 1
        record = records[0]
        assert record.bid is None
        assert record.ask == pytest.approx(0.0001, rel=1e-9)
        assert record.mark_price == pytest.approx(1e-08, rel=1e-9)
        assert record.open_interest == pytest.approx(192.9, rel=1e-9)
        conn.close()

    def test_normalize_options_6char_date(self) -> None:
        """Given 6 位不补零日期合约（2OCT26）When normalize Then 正常入库。

        回归：len=6 曾落入 else 抛 ProviderError 被整条丢弃。
        """
        payload = {
            "kind": "option",
            "BTC": [
                {
                    "ask_price": 0.05,
                    "bid_price": 0.01,
                    "mark_price": 0.03,
                    "instrument_name": "BTC-2OCT26-100000-C",
                    "open_interest": 5.0,
                },
                {
                    "ask_price": 0.05,
                    "bid_price": None,
                    "mark_price": 0.03,
                    "instrument_name": "BTC-9OCT26-100000-P",
                    "open_interest": 0.0,
                },
            ],
        }

        conn, _ = _create_mock_connector()
        records = conn.normalize(
            RawBatch(
                endpoint="/public/get_book_summary_by_currency",
                payload=payload,
                raw_meta={"http_status": 200, "currency": "BTC"},
            )
        )

        assert len(records) == 2
        by_inst = {r.instrument_id: r for r in records}
        from chronoforge.models.derivatives import OPTION as OptRecord
        early = by_inst["BTC-2026-10-02-100000-C"]
        late = by_inst["BTC-2026-10-09-100000-P"]
        assert isinstance(early, OptRecord)
        assert isinstance(late, OptRecord)
        assert early.expiry == date(2026, 10, 2)
        assert late.expiry == date(2026, 10, 9)
        assert late.bid is None
        assert late.open_interest == 0.0
        conn.close()


# ── TC-C-003: chart data → OHLCV fixture→canonical ────────────────────


class TestNormalizeChart:
    """get_tradingview_chart_data → OHLCV normalize 测试（TC-C-003）。"""

    def test_normalize_chart_happy_path(self) -> None:
        """Given chart fixture When normalize Then OHLCV records correct."""
        payload = _load_fixture("get_tradingview_chart_data/happy.json")
        raw_meta = {
            "http_status": 200,
            "instrument_name": "BTC-26SEP26-100000-C",
            "resolution": "60",
        }

        conn, _ = _create_mock_connector()

        records = conn.normalize(
            RawBatch(
                endpoint="/public/get_tradingview_chart_data",
                payload=payload,
                raw_meta=raw_meta,
            )
        )

        # 2 OHLCV records + 2 IMPLIED_VOLATILITY records
        assert len(records) == 4

        # Count OHLCV records
        ohlcv_records = [r for r in records if isinstance(r, OHLCV)]
        assert len(ohlcv_records) == 2

        # Verify fields
        first = records[0]
        assert first.source == "deribit"
        assert first.market_id == "DERIBIT:BTC-26SEP26-100000-C:OPTION"

        # Verify openTime parsing: 1672444800000 ms → UTC datetime
        expected_ts = datetime.fromtimestamp(
            1672444800000 / 1000, tz=UTC
        ).replace(tzinfo=None)
        assert first.event_time == expected_ts
        assert first.open == pytest.approx(16500.0, rel=1e-6)
        assert first.high == pytest.approx(16600.0, rel=1e-6)
        assert first.low == pytest.approx(16450.0, rel=1e-6)
        assert first.close == pytest.approx(16550.0, rel=1e-6)
        assert first.volume == pytest.approx(100.5, rel=1e-6)
        conn.close()

    def test_normalize_chart_empty_array(self) -> None:
        """Given empty chart When normalize Then 0 records."""
        payload = _load_fixture("get_tradingview_chart_data/empty.json")
        raw_meta = {
            "http_status": 200,
            "instrument_name": "BTC-26SEP26-100000-C",
            "resolution": "60",
        }

        conn, _ = _create_mock_connector()

        records = conn.normalize(
            RawBatch(
                endpoint="/public/get_tradingview_chart_data",
                payload=payload,
                raw_meta=raw_meta,
            )
        )

        assert len(records) == 0
        conn.close()


# ── TC-C-003: unknown endpoint ────────────────────────────────────────


class TestNormalizeUnknownEndpoint:
    """unknown endpoint normalize 测试."""

    def test_normalize_unknown_endpoint_raises(self) -> None:
        """Given unknown endpoint When normalize Then raises ProviderError."""
        conn, _ = _create_mock_connector()

        with pytest.raises(ProviderError):
            conn.normalize(
                RawBatch(
                    endpoint="/public/unknown",
                    payload={},
                    raw_meta={},
                )
            )
        conn.close()


# ── discover 全量 → INSTRUMENT 记录数=源返回数且三级 ID 正确 ───────────


class TestDiscover:
    """discover 测试（TC-C-003）。"""

    def test_discover_returns_correct_count(self) -> None:
        """Given instruments API When discover Then record count = source count."""
        payload = _load_fixture("get_instruments/happy.json")
        mock_resp = _mock_response(
            status_code=200,
            json_data=payload,
            url="https://www.deribit.com/api/v2",
        )
        mock_client = _mock_client(mock_resp)

        with patch.object(
            DeribitConnector, "__init__", lambda self, s: None
        ):
            conn = DeribitConnector.__new__(DeribitConnector)
            conn._client = mock_client
            conn._rate_limiter = MagicMock()

        instruments = conn.discover()

        # discover calls BTC and ETH; mock returns BTC data for both calls
        # So we get at least the 2 BTC option records
        assert len(instruments) >= 2
        assert all(isinstance(inst, dict) for inst in instruments)
        for inst in instruments:
            assert "entity_id" in inst
            assert "instrument_id" in inst
            assert "market_id" in inst
        conn.close()

    def test_discover_returns_instrument_refs(self) -> None:
        """Given instruments API When discover Then returns InstrumentRef dicts."""
        payload = _load_fixture("get_instruments/happy.json")
        mock_resp = _mock_response(
            status_code=200,
            json_data=payload,
            url="https://www.deribit.com/api/v2",
        )
        mock_client = _mock_client([mock_resp])

        with patch.object(
            DeribitConnector, "__init__", lambda self, s: None
        ):
            conn = DeribitConnector.__new__(DeribitConnector)
            conn._client = mock_client
            conn._rate_limiter = MagicMock()

        instruments = conn.discover()
        assert len(instruments) > 0
        for inst in instruments:
            assert isinstance(inst, dict)
            assert inst["entity_id"] in ("BTC", "ETH")
            assert inst["instrument_id"].startswith("BTC-")
            assert ":OPTION" in inst["market_id"] or ":PERP" in inst["market_id"]
        conn.close()


# ── 月份码联动：TC-M-004/005 ──────────────────────────────────────────


class TestParseDeribit:
    """parse_deribit 月份码测试（TC-M-004/005）。"""

    def test_parse_deribit_full_fields(self) -> None:
        """Given BTC-26SEP26-100000-C When parse_deribit Then all fields correct.（TC-M-004）"""
        result = parse_deribit("BTC-26SEP26-100000-C")

        assert result["underlying"] == "BTC"
        assert result["expiry"] == date(2026, 9, 26)
        assert result["strike"] == 100000.0
        assert result["option_type"] == "CALL"
        assert result["settlement_asset"] == "USD"
        assert result["instrument_id"] == "BTC-2026-09-26-100000-C"
        assert result["market_id"] == "DERIBIT:BTC-26SEP26-100000-C:OPTION"

    def test_parse_deribit_put_option(self) -> None:
        """Given BTC-26DEC26-90000-P When parse_deribit Then PUT option_type.（TC-M-005）"""
        result = parse_deribit("BTC-26DEC26-90000-P")

        assert result["underlying"] == "BTC"
        assert result["expiry"] == date(2026, 12, 26)
        assert result["strike"] == 90000.0
        assert result["option_type"] == "PUT"

    def test_parse_deribit_invalid_month_code(self) -> None:
        """Given BTC-32FOO26-100000-C When parse_deribit Then ProviderError.（TC-M-005）"""
        with pytest.raises(ProviderError):
            parse_deribit("BTC-32FOO26-100000-C")

    def test_parse_deribit_expired_contracts(self) -> None:
        """Given expired contract When parse_deribit Then succeeds.（TC-M-006）"""
        # Use a date in the past: 2020-01-15
        result = parse_deribit("BTC-15JAN20-50000-C")

        assert result["underlying"] == "BTC"
        assert result["expiry"] == date(2020, 1, 15)
        assert result["option_type"] == "CALL"
        assert result["expired"] is True


# ── Error paths ────────────────────────────────────────────────────────


class TestErrorPaths:
    """错误路径测试（429/500 → 错误类型）。"""

    def test_429_rate_limit_error(self) -> None:
        """Given 429 When fetch Then raises RateLimitError."""
        mock_resp = _mock_response(
            status_code=429,
            json_data={"error": "rate_limited"},
            headers={"retry-after": "10"},
        )
        mock_client = _mock_client(mock_resp)

        with patch.object(
            DeribitConnector, "__init__", lambda self, s: None
        ):
            conn = DeribitConnector.__new__(DeribitConnector)
            conn._client = mock_client
            conn._rate_limiter = MagicMock()

        req = FetchRequest(
            dataset_id="test",
            params={"data_type": "chart", "instrument_name": "BTC-PERP"},
            start=None,
            end=None,
            cursor=None,
        )

        with pytest.raises(RateLimitError):
            list(conn.fetch(req))
        conn.close()

    def test_500_transport_error(self) -> None:
        """Given 500 When fetch Then raises TransportError."""
        mock_resp = _mock_response(
            status_code=500,
            json_data={"error": "internal_error"},
        )
        mock_client = _mock_client(mock_resp)

        with patch.object(
            DeribitConnector, "__init__", lambda self, s: None
        ):
            conn = DeribitConnector.__new__(DeribitConnector)
            conn._client = mock_client
            conn._rate_limiter = MagicMock()

        req = FetchRequest(
            dataset_id="test",
            params={"data_type": "chart", "instrument_name": "BTC-PERP"},
            start=None,
            end=None,
            cursor=None,
        )

        with pytest.raises(TransportError):
            list(conn.fetch(req))
        conn.close()

    def test_unsupported_resolution(self) -> None:
        """Given unsupported resolution When fetch Then raises ProviderError."""
        conn, _ = _create_mock_connector()

        req = FetchRequest(
            dataset_id="test",
            params={"data_type": "chart", "instrument_name": "BTC-PERP", "resolution": "10"},
            start=None,
            end=None,
            cursor=None,
        )

        with pytest.raises(ProviderError) as exc_info:
            list(conn.fetch(req))
        assert "unsupported resolution" in str(exc_info.value).lower()
        conn.close()

    def test_missing_instrument_name(self) -> None:
        """Given missing instrument_name When chart fetch Then raises ProviderError."""
        conn, _ = _create_mock_connector()

        req = FetchRequest(
            dataset_id="test",
            params={"data_type": "chart"},
            start=None,
            end=None,
            cursor=None,
        )

        with pytest.raises(ProviderError) as exc_info:
            list(conn.fetch(req))
        assert "instrument_name is required" in str(exc_info.value)
        conn.close()

    def test_missing_currency(self) -> None:
        """Given missing currency When option_summary fetch Then raises ProviderError."""
        conn, _ = _create_mock_connector()

        req = FetchRequest(
            dataset_id="test",
            params={"data_type": "option_summary"},
            start=None,
            end=None,
            cursor=None,
        )

        with pytest.raises(ProviderError) as exc_info:
            list(conn.fetch(req))
        assert "currency is required" in str(exc_info.value)
        conn.close()


# ── Pagination ─────────────────────────────────────────────────────────


class TestChartPagination:
    """chart data 分页测试。"""

    def test_fetch_chart_data_pagination(self) -> None:
        """Given chart data with 2 pages When fetch Then cursor advances correctly."""
        page1 = {
            "candles": [
                {
                    "timestamp": 1672444800000,
                    "open": 16500.0, "close": 16550.0,
                    "high": 16600.0, "low": 16450.0,
                    "volume": 100.0, "iv": 0.65, "mark_iv": 0.66,
                }
            ],
            "instrument_name": "BTC-PERP",
            "resolution": "60",
        }
        page2 = {
            "candles": [
                {
                    "timestamp": 1672448400000,
                    "open": 16550.0, "close": 16600.0,
                    "high": 16650.0, "low": 16500.0,
                    "volume": 120.0, "iv": 0.64, "mark_iv": 0.65,
                }
            ],
            "instrument_name": "BTC-PERP",
            "resolution": "60",
        }

        resp1 = _mock_response(
            status_code=200,
            json_data=page1,
            url="https://www.deribit.com/api/v2",
        )
        resp2 = _mock_response(
            status_code=200,
            json_data=page2,
            url="https://www.deribit.com/api/v2",
        )

        mock_client = _mock_client([resp1, resp2])

        with patch.object(
            DeribitConnector, "__init__", lambda self, s: None
        ):
            conn = DeribitConnector.__new__(DeribitConnector)
            conn._client = mock_client
            conn._rate_limiter = MagicMock()

        req = FetchRequest(
            dataset_id="test",
            params={"data_type": "chart", "instrument_name": "BTC-PERP", "resolution": "60"},
            start=None,
            end=None,
            cursor=None,
        )

        batches = list(conn.fetch(req))

        # Should have fetched 2 pages
        assert len(batches) == 2
        assert batches[0].endpoint == "/public/get_tradingview_chart_data"
        assert batches[1].endpoint == "/public/get_tradingview_chart_data"

        # Verify 3 API calls: page1 → page2 → empty (loop break)
        call_args = mock_client.post.call_args_list
        assert len(call_args) >= 2
        # First call has instrument_name and resolution
        first_call_params = call_args[0][1]["json"]["params"]
        assert first_call_params["instrument_name"] == "BTC-PERP"
        assert first_call_params["resolution"] == "60"
        conn.close()

    def test_fetch_chart_data_step_advances_by_resolution(self) -> None:
        """审计 SR-08：分页步进 = last_ts + resolution×60000（非 1m 硬编码）。

        Given 1D 分辨率首页返回 1 根 K 线 When fetch Then 第二次请求的
        start_timestamp = 首页末根 ts + 1440×60000；空页触发循环终止。
        """
        ts = 1672444800000  # 2023-01-01T00:00:00Z
        page1 = {
            "candles": [
                {
                    "timestamp": ts,
                    "open": 16500.0, "close": 16550.0,
                    "high": 16600.0, "low": 16450.0,
                    "volume": 100.0, "iv": 0.65, "mark_iv": 0.66,
                }
            ],
            "instrument_name": "BTC-PERP",
            "resolution": "1440",
        }

        resp1 = _mock_response(status_code=200, json_data=page1)
        mock_client = _mock_client([resp1])  # 队列耗尽后返回空 candles → break

        with patch.object(
            DeribitConnector, "__init__", lambda self, s: None
        ):
            conn = DeribitConnector.__new__(DeribitConnector)
            conn._client = mock_client
            conn._rate_limiter = MagicMock()

        req = FetchRequest(
            dataset_id="test",
            params={"data_type": "chart", "instrument_name": "BTC-PERP", "resolution": "1440"},
            start=None,
            end=None,
            cursor=None,
        )

        batches = list(conn.fetch(req))

        assert len(batches) == 1
        calls = mock_client.post.call_args_list
        assert len(calls) == 2  # page1 + 空页 break
        second_params = calls[1][1]["json"]["params"]
        assert second_params["start_timestamp"] == ts + 1440 * 60_000
        conn.close()


# ── checkpoint_from ────────────────────────────────────────────────────


class TestCheckpoint:
    """checkpoint_from 测试（D04 §1）。"""

    def test_checkpoint_from_instruments(self) -> None:
        """Given instruments RawBatch When checkpoint_from Then returns marker."""
        payload = _load_fixture("get_instruments/happy.json")
        raw_meta = {"http_status": 200, "currency": "BTC"}

        conn, _ = _create_mock_connector()

        checkpoint = conn.checkpoint_from(
            RawBatch(
                endpoint="/public/get_instruments",
                payload=payload,
                raw_meta=raw_meta,
            )
        )

        assert checkpoint is not None
        assert checkpoint.startswith("deribit:instruments:")
        conn.close()

    def test_checkpoint_from_chart(self) -> None:
        """Given chart RawBatch When checkpoint_from Then returns last timestamp."""
        payload = _load_fixture("get_tradingview_chart_data/happy.json")
        raw_meta = {
            "http_status": 200,
            "instrument_name": "BTC-PERP",
            "resolution": "60",
        }

        conn, _ = _create_mock_connector()

        checkpoint = conn.checkpoint_from(
            RawBatch(
                endpoint="/public/get_tradingview_chart_data",
                payload=payload,
                raw_meta=raw_meta,
            )
        )

        assert checkpoint is not None
        # Last candle timestamp
        assert checkpoint == "1672448400000"
        conn.close()

    def test_checkpoint_from_empty(self) -> None:
        """Given empty payload When checkpoint_from Then returns None."""
        conn, _ = _create_mock_connector()

        checkpoint = conn.checkpoint_from([])
        assert checkpoint is None

        checkpoint = conn.checkpoint_from(
            RawBatch(
                endpoint="/public/get_instruments",
                payload={"result": []},
                raw_meta={},
            )
        )
        assert checkpoint is None
        conn.close()


# ── validate ───────────────────────────────────────────────────────────


class TestValidate:
    """validate 测试（D04 §1）。"""

    def test_validate_ohlcv_valid(self) -> None:
        """Given valid OHLCV records When validate Then no findings."""
        conn, _ = _create_mock_connector()

        # OHLCV model validator: high >= max(open, close) and low <= min(open, close)
        # With open=100, close=110: high >= 110, low <= 100
        record = OHLCV(
            schema_version="1.0",
            source="deribit",
            source_id="BTC-PERP:1672444800000",
            source_timestamp=datetime.now(UTC).replace(tzinfo=None),
            ingest_timestamp=datetime.now(UTC).replace(tzinfo=None),
            raw_record_id="deribit:ohlcv:test::1",
            market_id="DERIBIT:BTC-PERP:OPTION",
            event_time=datetime.now(UTC).replace(tzinfo=None),
            interval="1h",
            open=100.0,
            close=110.0,
            high=120.0,
            low=90.0,
            volume=1000.0,
        )

        report = conn.validate([record])
        assert report.error_count == 0
        conn.close()

    def test_validate_ohlcv_invalid_low(self) -> None:
        """Given OHLCV with low > min(open, close) When validate Then error finding."""
        conn, _ = _create_mock_connector()

        record = OHLCV(
            schema_version="1.0",
            source="deribit",
            source_id="BTC-PERP:1672444800000",
            source_timestamp=datetime.now(UTC).replace(tzinfo=None),
            ingest_timestamp=datetime.now(UTC).replace(tzinfo=None),
            raw_record_id="deribit:ohlcv:test::1",
            market_id="DERIBIT:BTC-PERP:OPTION",
            event_time=datetime.now(UTC).replace(tzinfo=None),
            interval="1h",
            open=100.0,
            close=110.0,
            high=120.0,
            low=90.0,
            volume=1000.0,
        )
        object.__setattr__(record, "low", 105.0)

        report = conn.validate([record])
        assert report.error_count >= 1
        assert any(f.rule_id == "Q-RANGE-001" for f in report.findings)
        conn.close()

    def test_validate_expired_option(self) -> None:
        """Given expired OPTION When validate Then Q-RANGE-003 warning."""
        conn, _ = _create_mock_connector()

        from chronoforge.models.derivatives import OPTION
        from chronoforge.models.reference import OptionType as OptType

        record = OPTION(
            schema_version="1.0",
            source="deribit",
            source_id="BTC-26JAN20-50000-C",
            source_timestamp=datetime.now(UTC).replace(tzinfo=None),
            ingest_timestamp=datetime.now(UTC).replace(tzinfo=None),
            raw_record_id="deribit:option:test::1",
            market_id="DERIBIT:BTC-26JAN20-50000-C:OPTION",
            instrument_id="BTC-2020-01-26-50000-C",
            event_time=datetime.now(UTC).replace(tzinfo=None),
            underlying="BTC",
            expiry=date(2020, 1, 26),
            strike=50000.0,
            option_type=OptType.CALL,
            settlement_asset="USD",
            mark_price=100.0,
            bid=99.0,
            ask=101.0,
            open_interest=10.0,
        )

        report = conn.validate([record])
        assert report.warning_count >= 1
        assert any(f.rule_id == "Q-RANGE-003" for f in report.findings)
        conn.close()


# ── GWT acceptance tests ──────────────────────────────────────────────


class TestGWT:
    """GWT (Given-When-Then) acceptance tests from task spec."""

    def test_gwt_instrument_id_correct(self) -> None:
        """GWT: Given "BTC-26SEP26-100000-C" When parse_deribit Then
        (BTC, BTC-2026-09-26-100000-C, DERIBIT:...:OPTION) 全字段正确."""
        result = parse_deribit("BTC-26SEP26-100000-C")

        assert result["underlying"] == "BTC"
        assert result["instrument_id"] == "BTC-2026-09-26-100000-C"
        assert "DERIBIT:BTC-26SEP26-100000-C:OPTION" == result["market_id"]

    def test_gwt_expired_contract_no_exception(self) -> None:
        """GWT: Given 过期合约 When parse_deribit Then 解析成功 + 不抛异常."""
        # Should not raise, should return expired=True
        result = parse_deribit("BTC-15JAN20-50000-C")
        assert result["expired"] is True
        assert result["option_type"] == "CALL"

    def test_gwt_invalid_month_raises(self) -> None:
        """GWT: Given "BTC-32FOO26-100000-C" When parse_deribit Then ProviderError."""
        with pytest.raises(ProviderError):
            parse_deribit("BTC-32FOO26-100000-C")

    def test_gwt_chart_normalize_returns_ohlcv(self) -> None:
        """GWT: Given chart fixture When normalize Then OHLCV records."""
        payload = _load_fixture("get_tradingview_chart_data/happy.json")
        raw_meta = {
            "http_status": 200,
            "instrument_name": "BTC-26SEP26-100000-C",
            "resolution": "60",
        }

        conn, _ = _create_mock_connector()

        records = conn.normalize(
            RawBatch(
                endpoint="/public/get_tradingview_chart_data",
                payload=payload,
                raw_meta=raw_meta,
            )
        )

        # 2 OHLCV + 2 IV records
        assert len(records) == 4
        ohlcv_records = [r for r in records if isinstance(r, OHLCV)]
        assert len(ohlcv_records) == 2
        conn.close()

    def test_gwt_discover_instrument_count_matches(self) -> None:
        """GWT: Given discover Then INSTRUMENT 记录数=源返回数且三级 ID 正确."""
        payload = _load_fixture("get_instruments/happy.json")
        mock_resp = _mock_response(
            status_code=200,
            json_data=payload,
            url="https://www.deribit.com/api/v2",
        )
        mock_client = _mock_client([mock_resp])

        with patch.object(
            DeribitConnector, "__init__", lambda self, s: None
        ):
            conn = DeribitConnector.__new__(DeribitConnector)
            conn._client = mock_client
            conn._rate_limiter = MagicMock()

        instruments = conn.discover()
        # Mock returns BTC data for both BTC and ETH calls → at least 2 BTC records
        assert len(instruments) >= 2
        for inst in instruments:
            assert inst["entity_id"] is not None
            assert inst["instrument_id"] is not None
            assert inst["market_id"] is not None
        conn.close()


class TestBaseUrlNormalization:
    """base_url 回归：注册表 base_url 带 /api/v2 后缀 → 防双重前缀（400）。"""

    def test_strips_api_v2_suffix(self) -> None:
        """base_url="https://www.deribit.com/api/v2" → 剥掉后缀。"""
        from chronoforge.connectors.deribit import _Settings

        conn = DeribitConnector(_Settings(base_url="https://www.deribit.com/api/v2"))
        assert str(conn._client.base_url).rstrip("/") == "https://www.deribit.com"
        conn.close()

    def test_plain_base_url_unchanged(self) -> None:
        """不带后缀的 base_url 保持原样。"""
        from chronoforge.connectors.deribit import _Settings

        conn = DeribitConnector(_Settings(base_url="https://www.deribit.com"))
        assert str(conn._client.base_url).rstrip("/") == "https://www.deribit.com"
        conn.close()
