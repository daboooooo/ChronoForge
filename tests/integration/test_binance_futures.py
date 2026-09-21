"""DATA-SOURCE-002 集成测试 — BinanceFuturesConnector。

覆盖：
- TC-C-002：fixture→canonical 逐字段比对（klines/funding/OI）
- funding 8h 周期窗口切分：多窗口测试 cursor 滚动
- OI 快照全量 upsert：同 nk 覆盖
- 分页：fundingRate 2 页 → cursor=fundingTime 滚动无重叠
- 边界：空数组（正常 0 行）、跨月边界
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
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
from chronoforge.connectors.binance_futures import BinanceFuturesConnector
from chronoforge.connectors.errors import (
    AuthError,
    RateLimitError,
    TransportError,
)
from chronoforge.models.enums import CanonicalType, QualityStatus
from chronoforge.models.market import FUNDING, OHLCV, OPEN_INTEREST

# Fixture paths
_FIXTURES = Path(__file__).parent.parent / "fixtures" / "binance_futures"


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
    url: str = "https://fapi.binance.com/fapi/v1/klines",
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
                empty.json.return_value = []
                empty.headers = {}
                empty.url = ""
                return empty
            return queue.pop(0)

        client.get.side_effect = _side_effect
    else:
        client.get.return_value = resp
    return client


def _create_mock_connector() -> tuple[BinanceFuturesConnector, MagicMock]:
    """Create a mock connector instance for normalize tests."""
    mock_client = MagicMock()
    mock_rate_limiter = MagicMock()

    with patch.object(
        BinanceFuturesConnector, "__init__", lambda self, s: None
    ):
        conn = BinanceFuturesConnector.__new__(BinanceFuturesConnector)
        conn._client = mock_client
        conn._rate_limiter = mock_rate_limiter

    return conn, mock_client


# ── capabilities ───────────────────────────────────────────────────────


class TestCapabilities:
    """BinanceFuturesConnector.capabilities() 测试（D04 §4.2）。"""

    def test_capabilities_returns_correct_matrix(self) -> None:
        """Given valid connector When capabilities() Then OHLCV/FUNDING/OI supported."""
        conn = BinanceFuturesConnector()
        cap = conn.capabilities()

        assert isinstance(cap, CapabilityMatrix)
        assert CanonicalType.OHLCV in cap.canonical_types
        assert CanonicalType.FUNDING in cap.canonical_types
        assert CanonicalType.OPEN_INTEREST in cap.canonical_types
        assert cap.supports_revision is False
        assert cap.supports_websocket is False
        assert cap.max_history_days is None
        conn.close()

    def test_capabilities_no_extra_types(self) -> None:
        """capabilities() should not include unexpected types."""
        conn = BinanceFuturesConnector()
        cap = conn.capabilities()
        assert CanonicalType.TRADE not in cap.canonical_types
        assert CanonicalType.TICKER not in cap.canonical_types
        assert CanonicalType.LIQUIDATION_EVENT not in cap.canonical_types
        conn.close()

    def test_capabilities_supported_intervals(self) -> None:
        """capabilities() should include all D04 §4.2 intervals."""
        conn = BinanceFuturesConnector()
        cap = conn.capabilities()
        expected = {"1m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "1w"}
        assert cap.intervals == expected
        conn.close()


# ── health ─────────────────────────────────────────────────────────────


class TestHealth:
    """BinanceFuturesConnector.health() 测试（D04 §1）。"""

    def test_health_ok(self) -> None:
        """Given healthy API When health() Then ok=True."""
        mock_resp = _mock_response(
            status_code=200,
            json_data={},
            url="https://fapi.binance.com/fapi/v1/ping",
        )
        mock_client = _mock_client(mock_resp)

        with patch.object(
            BinanceFuturesConnector, "__init__", lambda self, s: None
        ):
            conn = BinanceFuturesConnector.__new__(BinanceFuturesConnector)
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
            json_data={"code": -1000},
            url="https://fapi.binance.com/fapi/v1/ping",
        )
        mock_client = _mock_client(mock_resp)

        with patch.object(
            BinanceFuturesConnector, "__init__", lambda self, s: None
        ):
            conn = BinanceFuturesConnector.__new__(BinanceFuturesConnector)
            conn._client = mock_client

        status = conn.health()
        assert isinstance(status, HealthStatus)
        assert status.ok is False
        assert status.latency_ms >= 0
        conn.close()


# ── TC-C-002: klines → OHLCV fixture→canonical ────────────────────────


class TestNormalizeKlines:
    """kline → OHLCV normalize 测试（TC-C-002）。"""

    def test_normalize_klines_happy_path(self) -> None:
        """Given klines fixture When normalize Then OHLCV records correct."""
        payload = _load_fixture("klines/happy.json")
        raw_meta = {
            "http_status": 200,
            "url": "https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT",
            "symbol": "BTCUSDT",
            "interval": "1m",
        }

        conn, _ = _create_mock_connector()

        records = conn.normalize(
            RawBatch(endpoint="/fapi/v1/klines", payload=payload, raw_meta=raw_meta)
        )

        assert len(records) == 2
        assert all(isinstance(r, OHLCV) for r in records)

        # Field-by-field verification
        first = records[0]
        assert first.source == "binance_futures"
        assert first.schema_version == "1.0"
        assert first.quality_status == QualityStatus.VALID
        assert first.market_id == "BINANCE:BTCUSDT:USDT-FUT"

        # Verify openTime parsing: 1499040000000 ms → UTC datetime
        expected_ts = datetime.fromtimestamp(
            1499040000000 / 1000, tz=UTC
        ).replace(tzinfo=None)
        assert first.event_time == expected_ts
        assert first.open == pytest.approx(0.01634790, rel=1e-6)
        assert first.high == pytest.approx(0.80000000, rel=1e-6)
        assert first.low == pytest.approx(0.01575800, rel=1e-6)
        assert first.close == pytest.approx(0.01577100, rel=1e-6)
        assert first.volume == pytest.approx(148976.11427815, rel=1e-6)

        conn.close()

    def test_normalize_klines_empty_array(self) -> None:
        """Given empty klines When normalize Then 0 records."""
        payload = _load_fixture("klines/empty.json")
        raw_meta = {
            "http_status": 200,
            "url": "https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT",
            "symbol": "BTCUSDT",
            "interval": "1m",
        }

        conn, _ = _create_mock_connector()

        records = conn.normalize(
            RawBatch(endpoint="/fapi/v1/klines", payload=payload, raw_meta=raw_meta)
        )

        assert len(records) == 0
        conn.close()

    def test_normalize_klines_cross_month_boundary(self) -> None:
        """Given cross-month klines When normalize Then event_time correct."""
        payload = _load_fixture("klines/edge.json")
        raw_meta = {
            "http_status": 200,
            "url": "https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT",
            "symbol": "BTCUSDT",
            "interval": "1m",
        }

        conn, _ = _create_mock_connector()

        records = conn.normalize(
            RawBatch(endpoint="/fapi/v1/klines", payload=payload, raw_meta=raw_meta)
        )

        assert len(records) == 2
        # Verify first kline event_time
        expected_ts = datetime.fromtimestamp(
            1672444800000 / 1000, tz=UTC
        ).replace(tzinfo=None)
        assert records[0].event_time == expected_ts
        conn.close()


# ── TC-C-002: funding → FUNDING fixture→canonical ─────────────────────


class TestNormalizeFunding:
    """fundingRate → FUNDING normalize 测试（TC-C-002）。"""

    def test_normalize_funding_happy_path(self) -> None:
        """Given fundingRate fixture When normalize Then FUNDING records correct."""
        payload = _load_fixture("fundingRate/happy.json")
        raw_meta = {
            "http_status": 200,
            "url": "https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT",
            "symbol": "BTCUSDT",
        }

        conn, _ = _create_mock_connector()

        records = conn.normalize(
            RawBatch(
                endpoint="/fapi/v1/fundingRate",
                payload=payload,
                raw_meta=raw_meta,
            )
        )

        assert len(records) == 2
        assert all(isinstance(r, FUNDING) for r in records)

        # Field-by-field verification
        first = records[0]
        assert first.source == "binance_futures"
        assert first.schema_version == "1.0"
        assert first.quality_status == QualityStatus.VALID
        assert first.market_id == "BINANCE:BTCUSDT:USDT-FUT"

        # Verify fundingTime parsing: 1672444800000 ms → UTC datetime
        expected_ts = datetime.fromtimestamp(
            1672444800000 / 1000, tz=UTC
        ).replace(tzinfo=None)
        assert first.event_time == expected_ts
        assert first.funding_rate == pytest.approx(0.00001000, rel=1e-6)
        assert first.next_funding_time == expected_ts + timedelta(hours=8)

        # Verify second record
        second = records[1]
        assert second.funding_rate == pytest.approx(0.00000800, rel=1e-6)
        conn.close()

    def test_normalize_funding_empty_array(self) -> None:
        """Given empty fundingRate When normalize Then 0 records."""
        payload = _load_fixture("fundingRate/empty.json")
        raw_meta = {
            "http_status": 200,
            "url": "https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT",
            "symbol": "BTCUSDT",
        }

        conn, _ = _create_mock_connector()

        records = conn.normalize(
            RawBatch(
                endpoint="/fapi/v1/fundingRate",
                payload=payload,
                raw_meta=raw_meta,
            )
        )

        assert len(records) == 0
        conn.close()

    def test_normalize_funding_missing_next_funding_time(self) -> None:
        """Given fundingRate without nextFundingTime When normalize Then fallback event_time."""
        payload = [{
            "symbol": "BTCUSDT",
            "fundingRate": "0.00001000",
            "fundingTime": 1672444800000,
            "markPrice": "16550.50000000",
            # No nextFundingTime
        }]
        raw_meta = {
            "http_status": 200,
            "url": "https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT",
            "symbol": "BTCUSDT",
        }

        conn, _ = _create_mock_connector()

        records = conn.normalize(
            RawBatch(
                endpoint="/fapi/v1/fundingRate",
                payload=payload,
                raw_meta=raw_meta,
            )
        )

        assert len(records) == 1
        # next_funding_time should fallback to event_time + 1s (validator requires > event_time)
        assert records[0].next_funding_time == records[0].event_time + timedelta(seconds=1)
        conn.close()


# ── TC-C-002: OI → OPEN_INTEREST fixture→canonical ────────────────────


class TestNormalizeOI:
    """openInterest → OPEN_INTEREST normalize 测试（TC-C-002）。"""

    def test_normalize_oi_happy_path(self) -> None:
        """Given openInterest fixture When normalize Then OPEN_INTEREST record correct."""
        payload = _load_fixture("openInterest/happy.json")
        raw_meta = {
            "http_status": 200,
            "url": "https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT",
            "symbol": "BTCUSDT",
        }

        conn, _ = _create_mock_connector()

        records = conn.normalize(
            RawBatch(
                endpoint="/fapi/v1/openInterest",
                payload=payload,
                raw_meta=raw_meta,
            )
        )

        assert len(records) == 1
        record = records[0]
        assert isinstance(record, OPEN_INTEREST)
        assert record.source == "binance_futures"
        assert record.schema_version == "1.0"
        assert record.quality_status == QualityStatus.VALID
        assert record.market_id == "BINANCE:BTCUSDT:USDT-FUT"
        assert record.open_interest == pytest.approx(83630.503, rel=1e-6)
        assert record.unit == "contracts"
        conn.close()

    def test_normalize_oi_with_timestamp(self) -> None:
        """Given openInterest with timestamp When normalize Then event_time from timestamp."""
        payload = _load_fixture("openInterest/happy.json")
        raw_meta = {
            "http_status": 200,
            "url": "https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT",
            "symbol": "BTCUSDT",
        }

        conn, _ = _create_mock_connector()

        records = conn.normalize(
            RawBatch(
                endpoint="/fapi/v1/openInterest",
                payload=payload,
                raw_meta=raw_meta,
            )
        )

        assert len(records) == 1
        expected_ts = datetime.fromtimestamp(
            1672444800000 / 1000, tz=UTC
        ).replace(tzinfo=None)
        assert records[0].event_time == expected_ts
        conn.close()

    def test_normalize_oi_invalid_payload(self) -> None:
        """Given invalid openInterest When normalize Then empty list."""
        payload = {"invalid": "data"}
        raw_meta = {
            "http_status": 200,
            "url": "https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT",
            "symbol": "BTCUSDT",
        }

        conn, _ = _create_mock_connector()

        records = conn.normalize(
            RawBatch(
                endpoint="/fapi/v1/openInterest",
                payload=payload,
                raw_meta=raw_meta,
            )
        )

        assert len(records) == 0
        conn.close()


# ── TC-C-002: unknown endpoint ────────────────────────────────────────


class TestNormalizeUnknownEndpoint:
    """unknown endpoint normalize 测试."""

    def test_normalize_unknown_endpoint_raises(self) -> None:
        """Given unknown endpoint When normalize Then raises ProviderError."""
        conn, _ = _create_mock_connector()

        with pytest.raises(Exception) as exc_info:
            conn.normalize(
                RawBatch(
                    endpoint="/fapi/v1/unknown",
                    payload={},
                    raw_meta={},
                )
            )
        assert "unknown endpoint" in str(exc_info.value).lower()
        conn.close()


# ── TC-C-011: fundingRate 分页 cursor=fundingTime 滚动无重叠 ───────────


class TestFundingPagination:
    """fundingRate 分页测试（TC-C-011）。"""

    def test_fetch_funding_cursor_advances(self) -> None:
        """Given fundingRate 2 pages When fetch Then cursor=fundingTime rolling no overlap."""
        page1 = _load_fixture("fundingRate/two_pages.json")
        # Page 2: next funding slot (8h later)
        page2 = [{
            "symbol": "BTCUSDT",
            "fundingRate": "0.00001200",
            "fundingTime": 1672502400000,  # 1672473600000 + 8h
            "markPrice": "16600.00000000",
            "nextFundingTime": 1672531200000,
        }]

        resp1 = _mock_response(
            status_code=200,
            json_data=page1,
            url="https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT",
        )
        resp2 = _mock_response(
            status_code=200,
            json_data=page2,
            url=(
                "https://fapi.binance.com/fapi/v1/fundingRate"
                "?symbol=BTCUSDT&startTime=1672473600001"
            ),
        )

        mock_client = _mock_client([resp1, resp2])

        with patch.object(
            BinanceFuturesConnector, "__init__", lambda self, s: None
        ):
            conn = BinanceFuturesConnector.__new__(BinanceFuturesConnector)
            conn._client = mock_client
            conn._rate_limiter = MagicMock()

        req = FetchRequest(
            dataset_id="test",
            params={"symbol": "BTCUSDT", "data_type": "funding"},
            start=None,
            end=None,
            cursor=None,
        )

        batches = list(conn.fetch(req))

        # Should have fetched 2 pages
        assert len(batches) == 2
        assert batches[0].endpoint == "/fapi/v1/fundingRate"
        assert batches[1].endpoint == "/fapi/v1/fundingRate"

        # Verify cursor advances (Binance API may snap to funding windows)
        call_args = mock_client.get.call_args_list
        assert len(call_args) == 3  # page1 + page2 + empty page3
        # All calls after the first should have startTime set
        for call in call_args[1:]:
            params = call[1]["params"]
            assert "startTime" in params
            # Cursor should advance monotonically
            assert int(params["startTime"]) > 1672444800000
        conn.close()

    def test_fetch_funding_cursor_no_overlap(self) -> None:
        """Given fundingRate 2 pages When normalize Then no duplicate records."""
        page1 = _load_fixture("fundingRate/two_pages.json")
        page2 = [{
            "symbol": "BTCUSDT",
            "fundingRate": "0.00001200",
            "fundingTime": 1672502400000,
            "markPrice": "16600.00000000",
            "nextFundingTime": 1672531200000,
        }]

        resp1 = _mock_response(
            status_code=200, json_data=page1,
            url="https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT",
        )
        resp2 = _mock_response(
            status_code=200, json_data=page2,
            url=(
                "https://fapi.binance.com/fapi/v1/fundingRate"
                "?symbol=BTCUSDT&startTime=1672473600001"
            ),
        )

        mock_client = _mock_client([resp1, resp2])

        with patch.object(
            BinanceFuturesConnector, "__init__", lambda self, s: None
        ):
            conn = BinanceFuturesConnector.__new__(BinanceFuturesConnector)
            conn._client = mock_client
            conn._rate_limiter = MagicMock()

        req = FetchRequest(
            dataset_id="test",
            params={"symbol": "BTCUSDT", "data_type": "funding"},
            start=None,
            end=None,
            cursor=None,
        )

        batches = list(conn.fetch(req))

        # Normalize both pages
        all_records = []
        for batch in batches:
            records = conn.normalize(batch)
            all_records.extend(records)

        # Page 1 has 1 record, page 2 has 1 record = total 2
        assert len(all_records) == 2
        assert all(isinstance(r, FUNDING) for r in all_records)

        # Verify no duplicate fundingTimes
        funding_times = [r.event_time for r in all_records]
        assert len(funding_times) == len(set(funding_times))
        conn.close()


# ── OI 快照全量 upsert ─────────────────────────────────────────────────


class TestOISnapshot:
    """OI 快照全量 upsert 测试."""

    def test_oi_snapshot_same_nk_cover(self) -> None:
        """Given OI snapshot with same market_id+event_time When upsert Then overwrite."""
        ts_ms = 1672444800000
        payload1 = {
            "symbol": "BTCUSDT",
            "openInterest": "83630.50300000",
            "timestamp": ts_ms,
            "time": ts_ms,
        }
        payload2 = {
            "symbol": "BTCUSDT",
            "openInterest": "84000.00000000",
            "timestamp": ts_ms,
            "time": ts_ms,
        }

        raw_meta = {
            "http_status": 200,
            "url": "https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT",
            "symbol": "BTCUSDT",
        }

        conn, _ = _create_mock_connector()

        records1 = conn.normalize(
            RawBatch(
                endpoint="/fapi/v1/openInterest",
                payload=payload1,
                raw_meta=raw_meta,
            )
        )
        records2 = conn.normalize(
            RawBatch(
                endpoint="/fapi/v1/openInterest",
                payload=payload2,
                raw_meta=raw_meta,
            )
        )

        assert len(records1) == 1
        assert len(records2) == 1
        # Same nk (market_id + event_time) → same natural_key
        assert records1[0].natural_key() == records2[0].natural_key()
        # Different values → upsert would overwrite
        assert records1[0].open_interest != records2[0].open_interest
        conn.close()


# ── Error paths ────────────────────────────────────────────────────────


class TestErrorPaths:
    """错误路径测试（401/429/500 → 错误类型）。"""

    def test_401_auth_error(self) -> None:
        """Given 401 When fetch Then raises AuthError."""
        mock_resp = _mock_response(
            status_code=401,
            json_data={"code": -2008, "msg": "Invalid API call"},
        )
        mock_client = _mock_client(mock_resp)

        with patch.object(
            BinanceFuturesConnector, "__init__", lambda self, s: None
        ):
            conn = BinanceFuturesConnector.__new__(BinanceFuturesConnector)
            conn._client = mock_client
            conn._rate_limiter = MagicMock()

        req = FetchRequest(
            dataset_id="test",
            params={"symbol": "BTCUSDT", "interval": "1m", "data_type": "klines"},
            start=None,
            end=None,
            cursor=None,
        )

        with pytest.raises(AuthError):
            list(conn.fetch(req))
        conn.close()

    def test_429_rate_limit_error(self) -> None:
        """Given 429 When fetch Then raises RateLimitError."""
        mock_resp = _mock_response(
            status_code=429,
            json_data={"code": -1015, "msg": "Rate limit exceeded"},
            headers={"retry-after": "60"},
        )
        mock_client = _mock_client(mock_resp)

        with patch.object(
            BinanceFuturesConnector, "__init__", lambda self, s: None
        ):
            conn = BinanceFuturesConnector.__new__(BinanceFuturesConnector)
            conn._client = mock_client
            conn._rate_limiter = MagicMock()

        req = FetchRequest(
            dataset_id="test",
            params={"symbol": "BTCUSDT", "interval": "1m", "data_type": "klines"},
            start=None,
            end=None,
            cursor=None,
        )

        with pytest.raises(RateLimitError) as exc_info:
            list(conn.fetch(req))
        assert exc_info.value.retry_after == 60
        conn.close()

    def test_500_transport_error(self) -> None:
        """Given 500 When fetch Then raises TransportError."""
        mock_resp = _mock_response(
            status_code=500,
            json_data={"code": -1000, "msg": "Server error"},
        )
        mock_client = _mock_client(mock_resp)

        with patch.object(
            BinanceFuturesConnector, "__init__", lambda self, s: None
        ):
            conn = BinanceFuturesConnector.__new__(BinanceFuturesConnector)
            conn._client = mock_client
            conn._rate_limiter = MagicMock()

        req = FetchRequest(
            dataset_id="test",
            params={"symbol": "BTCUSDT", "interval": "1m", "data_type": "klines"},
            start=None,
            end=None,
            cursor=None,
        )

        with pytest.raises(TransportError):
            list(conn.fetch(req))
        conn.close()

    def test_missing_symbol_raises_provider_error(self) -> None:
        """Given missing symbol When fetch Then raises ProviderError."""
        conn, _ = _create_mock_connector()

        req = FetchRequest(
            dataset_id="test",
            params={"interval": "1m", "data_type": "klines"},
            start=None,
            end=None,
            cursor=None,
        )

        with pytest.raises(Exception) as exc_info:
            list(conn.fetch(req))
        assert "symbol is required" in str(exc_info.value)
        conn.close()


# ── GWT acceptance tests ──────────────────────────────────────────────


class TestGWT:
    """GWT (Given-When-Then) acceptance tests from task spec."""

    def test_gwt_funding_cursor_no_overlap(self) -> None:
        """GWT: Given fundingRate 分页 Then cursor=fundingTime 滚动无重叠。"""
        page1 = [{
            "symbol": "BTCUSDT",
            "fundingRate": "0.00001000",
            "fundingTime": 1672444800000,
            "nextFundingTime": 1672473600000,
        }]
        page2 = [{
            "symbol": "BTCUSDT",
            "fundingRate": "0.00001200",
            "fundingTime": 1672473600000,
            "nextFundingTime": 1672502400000,
        }]

        resp1 = _mock_response(
            status_code=200, json_data=page1,
            url="https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT",
        )
        resp2 = _mock_response(
            status_code=200, json_data=page2,
            url=(
                "https://fapi.binance.com/fapi/v1/fundingRate"
                "?symbol=BTCUSDT&startTime=1672473600001"
            ),
        )

        mock_client = _mock_client([resp1, resp2])

        with patch.object(
            BinanceFuturesConnector, "__init__", lambda self, s: None
        ):
            conn = BinanceFuturesConnector.__new__(BinanceFuturesConnector)
            conn._client = mock_client
            conn._rate_limiter = MagicMock()

        req = FetchRequest(
            dataset_id="test",
            params={"symbol": "BTCUSDT", "data_type": "funding"},
            start=None,
            end=None,
            cursor=None,
        )

        batches = list(conn.fetch(req))
        assert len(batches) == 2

        # Verify cursor = last fundingTime + 1
        call_args = mock_client.get.call_args_list
        params = call_args[1][1]["params"]
        assert int(params["startTime"]) == 1672473600001
        conn.close()

    def test_gwt_oi_snapshot_correct_market_id(self) -> None:
        """GWT: Given OI 快照 When normalize Then OPEN_INTEREST 记录含 correct market_id."""
        payload = {
            "symbol": "BTCUSDT",
            "openInterest": "12345.678",
            "timestamp": 1672444800000,
            "time": 1672444800000,
        }
        raw_meta = {
            "http_status": 200,
            "url": "https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT",
            "symbol": "BTCUSDT",
        }

        with patch.object(
            BinanceFuturesConnector, "__init__", lambda self, s: None
        ):
            conn = BinanceFuturesConnector.__new__(BinanceFuturesConnector)
            conn._client = MagicMock()
            conn._rate_limiter = MagicMock()

        records = conn.normalize(
            RawBatch(
                endpoint="/fapi/v1/openInterest",
                payload=payload,
                raw_meta=raw_meta,
            )
        )

        assert len(records) == 1
        assert isinstance(records[0], OPEN_INTEREST)
        assert records[0].market_id == "BINANCE:BTCUSDT:USDT-FUT"
        conn.close()


# ── checkpoint_from ────────────────────────────────────────────────────


class TestCheckpoint:
    """checkpoint_from 测试（D04 §1）。"""

    def test_checkpoint_from_klines(self) -> None:
        """Given klines RawBatch When checkpoint_from Then returns last closeTime."""
        payload = _load_fixture("klines/happy.json")
        raw_meta = {
            "http_status": 200,
            "url": "https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT",
            "symbol": "BTCUSDT",
            "interval": "1m",
        }

        with patch.object(
            BinanceFuturesConnector, "__init__", lambda self, s: None
        ):
            conn = BinanceFuturesConnector.__new__(BinanceFuturesConnector)
            conn._client = MagicMock()

        checkpoint = conn.checkpoint_from(
            RawBatch(endpoint="/fapi/v1/klines", payload=payload, raw_meta=raw_meta)
        )

        assert checkpoint is not None
        assert checkpoint == str(payload[-1][6])  # last closeTime
        conn.close()

    def test_checkpoint_from_funding(self) -> None:
        """Given fundingRate RawBatch When checkpoint_from Then returns last fundingTime."""
        payload = _load_fixture("fundingRate/happy.json")
        raw_meta = {
            "http_status": 200,
            "url": "https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT",
            "symbol": "BTCUSDT",
        }

        with patch.object(
            BinanceFuturesConnector, "__init__", lambda self, s: None
        ):
            conn = BinanceFuturesConnector.__new__(BinanceFuturesConnector)
            conn._client = MagicMock()

        checkpoint = conn.checkpoint_from(
            RawBatch(
                endpoint="/fapi/v1/fundingRate",
                payload=payload,
                raw_meta=raw_meta,
            )
        )

        assert checkpoint is not None
        assert checkpoint == str(int(payload[-1]["fundingTime"]))
        conn.close()

    def test_checkpoint_from_oi(self) -> None:
        """Given openInterest RawBatch When checkpoint_from Then returns timestamp."""
        payload = _load_fixture("openInterest/happy.json")
        raw_meta = {
            "http_status": 200,
            "url": "https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT",
            "symbol": "BTCUSDT",
        }

        with patch.object(
            BinanceFuturesConnector, "__init__", lambda self, s: None
        ):
            conn = BinanceFuturesConnector.__new__(BinanceFuturesConnector)
            conn._client = MagicMock()

        checkpoint = conn.checkpoint_from(
            RawBatch(
                endpoint="/fapi/v1/openInterest",
                payload=payload,
                raw_meta=raw_meta,
            )
        )

        assert checkpoint is not None
        assert checkpoint == str(int(payload["timestamp"]))
        conn.close()

    def test_checkpoint_from_empty(self) -> None:
        """Given empty payload When checkpoint_from Then returns None."""
        conn = BinanceFuturesConnector()

        checkpoint = conn.checkpoint_from([])
        assert checkpoint is None

        checkpoint = conn.checkpoint_from(
            RawBatch(endpoint="/fapi/v1/klines", payload=[], raw_meta={})
        )
        assert checkpoint is None

        conn.close()


# ── validate ───────────────────────────────────────────────────────────


class TestValidate:
    """validate 测试（D04 §1）。"""

    def test_validate_ohlcv_valid(self) -> None:
        """Given valid OHLCV records When validate Then no findings."""
        conn = BinanceFuturesConnector()
        conn._rate_limiter = MagicMock()

        record = OHLCV(
            schema_version="1.0",
            source="binance_futures",
            source_id="1499040000000",
            source_timestamp=datetime.now(UTC).replace(tzinfo=None),
            ingest_timestamp=datetime.now(UTC).replace(tzinfo=None),
            raw_record_id="binance_futures:test::1",
            market_id="BINANCE:BTCUSDT:USDT-FUT",
            event_time=datetime.now(UTC).replace(tzinfo=None),
            interval="1m",
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
        conn = BinanceFuturesConnector()
        conn._rate_limiter = MagicMock()

        record = OHLCV(
            schema_version="1.0",
            source="binance_futures",
            source_id="1499040000000",
            source_timestamp=datetime.now(UTC).replace(tzinfo=None),
            ingest_timestamp=datetime.now(UTC).replace(tzinfo=None),
            raw_record_id="binance_futures:test::1",
            market_id="BINANCE:BTCUSDT:USDT-FUT",
            event_time=datetime.now(UTC).replace(tzinfo=None),
            interval="1m",
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

    def test_validate_ohlcv_invalid_high(self) -> None:
        """Given OHLCV with high < max(open, close) When validate Then error finding."""
        conn = BinanceFuturesConnector()
        conn._rate_limiter = MagicMock()

        record = OHLCV(
            schema_version="1.0",
            source="binance_futures",
            source_id="1499040000000",
            source_timestamp=datetime.now(UTC).replace(tzinfo=None),
            ingest_timestamp=datetime.now(UTC).replace(tzinfo=None),
            raw_record_id="binance_futures:test::1",
            market_id="BINANCE:BTCUSDT:USDT-FUT",
            event_time=datetime.now(UTC).replace(tzinfo=None),
            interval="1m",
            open=100.0,
            close=110.0,
            high=120.0,
            low=90.0,
            volume=1000.0,
        )
        object.__setattr__(record, "high", 105.0)

        report = conn.validate([record])
        assert report.error_count >= 1
        assert any(f.rule_id == "Q-RANGE-001" for f in report.findings)
        conn.close()


# ── Q-TS-003: unfinished kline discard ─────────────────────────────────


class TestUnfinishedKline:
    """未收盘 K 线丢弃（Q-TS-003）。"""

    def test_unfinished_kline_discarded(self) -> None:
        """Given kline with closeTime in future When normalize Then discarded."""
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        future_close = now_ms + 86400000  # 1 day in future

        payload = [[
            now_ms - 60000, "1.0", "2.0", "0.5", "1.5", "100.0",
            future_close, "0",
        ]]
        raw_meta = {
            "http_status": 200,
            "url": "https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT",
            "symbol": "BTCUSDT",
            "interval": "1m",
        }

        with patch.object(
            BinanceFuturesConnector, "__init__", lambda self, s: None
        ):
            conn = BinanceFuturesConnector.__new__(BinanceFuturesConnector)
            conn._client = MagicMock()
            conn._rate_limiter = MagicMock()

        records = conn.normalize(
            RawBatch(endpoint="/fapi/v1/klines", payload=payload, raw_meta=raw_meta)
        )

        assert len(records) == 0
        conn.close()

    def test_finished_kline_kept(self) -> None:
        """Given kline with closeTime in past When normalize Then kept."""
        past_close = 1767225600000  # well in the past

        payload = [[
            past_close - 60000, "1.0", "2.0", "0.5", "1.5", "100.0",
            past_close, "0",
        ]]
        raw_meta = {
            "http_status": 200,
            "url": "https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT",
            "symbol": "BTCUSDT",
            "interval": "1m",
        }

        with patch.object(
            BinanceFuturesConnector, "__init__", lambda self, s: None
        ):
            conn = BinanceFuturesConnector.__new__(BinanceFuturesConnector)
            conn._client = MagicMock()
            conn._rate_limiter = MagicMock()

        records = conn.normalize(
            RawBatch(endpoint="/fapi/v1/klines", payload=payload, raw_meta=raw_meta)
        )

        assert len(records) == 1
        assert isinstance(records[0], OHLCV)
        conn.close()
