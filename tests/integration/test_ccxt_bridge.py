"""DATA-SOURCE-004 集成测试 — CcxtBridgeConnector。

覆盖：
- TC-C-004：fixture→canonical 逐字段比对（OHLCV/trades）
- TC-C-013：capability detection（exchange.has[fetchOHLCV]=False → 不含 OHLCV）
- fetchOHLCV 2 页 fixture → cursor 推进正确
- 边界：空结果=正常（0 行）
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import ccxt
import pytest

from chronoforge.connectors.base import (
    CapabilityMatrix,
    FetchRequest,
    HealthStatus,
    RawBatch,
)
from chronoforge.connectors.ccxt_bridge import CcxtBridgeConnector, _map_ccxt_error
from chronoforge.exceptions import ProviderError, RateLimitError, TransportError
from chronoforge.models.enums import CanonicalType, QualityStatus
from chronoforge.models.market import OHLCV, TRADE

# Fixture paths
_FIXTURES = Path(__file__).parent.parent / "fixtures" / "ccxt_bridge"


# ── Helpers ────────────────────────────────────────────────────────────


def _load_fixture(name: str) -> Any:
    """Load a JSON fixture file."""
    path = _FIXTURES / name
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _mock_exchange(has_overrides: dict | None = None) -> MagicMock:
    """Create a mock ccxt Exchange with configurable has capabilities."""
    mock_exchange = MagicMock()
    mock_exchange.markets = {"BTCUSDT": {"type": "spot", "active": True}}

    base_has = {
        "fetchOHLCV": True,
        "fetchTrades": True,
        "fetchTicker": True,
        "fetchOrderBook": False,
        "watchOHLCV": False,
    }
    if has_overrides:
        base_has.update(has_overrides)
    mock_exchange.has = base_has

    mock_exchange.timeframes = {
        "1m": "1m",
        "5m": "5m",
        "15m": "15m",
        "1h": "1h",
        "4h": "4h",
        "1d": "1d",
        "1w": "1w",
    }
    mock_exchange.close = MagicMock()
    return mock_exchange


def _create_mock_connector(
    has_overrides: dict | None = None,
) -> tuple[CcxtBridgeConnector, MagicMock]:
    """Create a mock connector instance with configurable exchange.has."""
    mock_exchange = _mock_exchange(has_overrides)

    with patch.object(
        CcxtBridgeConnector, "_init_exchange", return_value=mock_exchange
    ):
        conn = CcxtBridgeConnector(exchange="binance")

    return conn, mock_exchange


# ── capabilities ───────────────────────────────────────────────────────


class TestCapabilities:
    """CcxtBridgeConnector.capabilities() 测试（D04 §4.4）。"""

    def test_capabilities_with_full_support(self) -> None:
        """Given exchange with all features When capabilities() Then all supported types."""
        conn, _ = _create_mock_connector()
        cap = conn.capabilities()

        assert isinstance(cap, CapabilityMatrix)
        assert CanonicalType.OHLCV in cap.canonical_types
        assert CanonicalType.TRADE in cap.canonical_types
        assert CanonicalType.TICKER in cap.canonical_types
        assert cap.supports_revision is False
        assert cap.supports_websocket is False
        conn.close()

    def test_capabilities_no_orderbook(self) -> None:
        """exchange.has[fetchOrderBook]=False → no ORDERBOOK in capabilities."""
        conn, _ = _create_mock_connector()
        cap = conn.capabilities()
        assert CanonicalType.ORDERBOOK not in cap.canonical_types
        conn.close()

    def test_capabilities_intervals_from_timeframes(self) -> None:
        """capabilities() should include intervals from exchange.timeframes."""
        conn, _ = _create_mock_connector()
        cap = conn.capabilities()

        # Check that intervals from the mock are present
        assert len(cap.intervals) > 0
        conn.close()

    def test_capabilities_websocket_detection(self) -> None:
        """exchange.has[watchOHLCV]=True → supports_websocket=True."""
        conn, _ = _create_mock_connector(has_overrides={"watchOHLCV": True})
        cap = conn.capabilities()
        assert cap.supports_websocket is True
        conn.close()


# ── TC-C-013: capability detection ────────────────────────────────────


class TestCapabilityDetection:
    """capability detection 测试（TC-C-013）。"""

    def test_no_ohlcv_when_fetchOHLCV_false(self) -> None:
        """Given exchange.has[fetchOHLCV]=False Then capabilities 不含 OHLCV。"""
        conn, _ = _create_mock_connector(has_overrides={"fetchOHLCV": False})
        cap = conn.capabilities()
        assert CanonicalType.OHLCV not in cap.canonical_types
        conn.close()

    def test_no_trades_when_fetchTrades_false(self) -> None:
        """Given exchange.has[fetchTrades]=False Then capabilities 不含 TRADE。"""
        conn, _ = _create_mock_connector(has_overrides={"fetchTrades": False})
        cap = conn.capabilities()
        assert CanonicalType.TRADE not in cap.canonical_types
        conn.close()

    def test_no_ticker_when_fetchTicker_false(self) -> None:
        """Given exchange.has[fetchTicker]=False Then capabilities 不含 TICKER。"""
        conn, _ = _create_mock_connector(has_overrides={"fetchTicker": False})
        cap = conn.capabilities()
        assert CanonicalType.TICKER not in cap.canonical_types
        conn.close()


# ── TC-C-004: OHLCV fixture→canonical ─────────────────────────────────


class TestNormalizeOHLCV:
    """OHLCV normalize 测试（TC-C-004）。"""

    def test_normalize_ohlcv_happy_path(self) -> None:
        """Given fetchOHLCV fixture When normalize Then OHLCV records correct."""
        payload = _load_fixture("fetch_ohlcv/happy.json")

        conn, _ = _create_mock_connector()

        records = conn.normalize(
            RawBatch(
                endpoint="fetchOHLCV",
                payload=payload,
                raw_meta={"symbol": "BTCUSDT", "timeframe": "1h"},
            )
        )

        assert len(records) == 2
        assert all(isinstance(r, OHLCV) for r in records)

        # Field-by-field verification for first record
        first = records[0]
        assert first.source == "ccxt"
        assert first.schema_version == "1.0"
        assert first.quality_status == QualityStatus.VALID
        assert first.market_id == "CCXT-BINANCE:BTCUSDT:SPOT"

        # Verify event_time from timestamp: 1672444800000 ms → UTC
        expected_ts = datetime.fromtimestamp(
            1672444800000 / 1000, tz=UTC
        ).replace(tzinfo=None)
        assert first.event_time == expected_ts
        assert first.open == pytest.approx(16547.32, rel=1e-6)
        assert first.high == pytest.approx(16600.50, rel=1e-6)
        assert first.low == pytest.approx(16500.00, rel=1e-6)
        assert first.close == pytest.approx(16580.10, rel=1e-6)
        assert first.volume == pytest.approx(1234.56, rel=1e-6)

        # Verify second record
        second = records[1]
        assert second.open == pytest.approx(16580.10, rel=1e-6)
        conn.close()

    def test_normalize_ohlcv_empty(self) -> None:
        """Given empty OHLCV When normalize Then 0 records."""
        payload = _load_fixture("fetch_ohlcv/empty.json")

        conn, _ = _create_mock_connector()

        records = conn.normalize(
            RawBatch(
                endpoint="fetchOHLCV",
                payload=payload,
                raw_meta={"symbol": "BTCUSDT", "timeframe": "1h"},
            )
        )

        assert len(records) == 0
        conn.close()

    def test_normalize_ohlcv_record_count_matches_fixture(self) -> None:
        """Given fetchOHLCV fixture When normalize Then record count matches fixture."""
        payload = _load_fixture("fetch_ohlcv/happy.json")
        expected_count = len(payload)

        conn, _ = _create_mock_connector()

        records = conn.normalize(
            RawBatch(
                endpoint="fetchOHLCV",
                payload=payload,
                raw_meta={"symbol": "BTCUSDT", "timeframe": "1h"},
            )
        )

        assert len(records) == expected_count
        conn.close()

    def test_normalize_ohlcv_invalid_candle_skipped(self) -> None:
        """Given mixed valid/invalid candles When normalize Then only valid kept."""
        payload = [
            [1672444800000, "2023-01-01T00:00:00.000Z",
             100.0, 110.0, 90.0, 105.0, 100.0],
            [1672448400000, "2023-01-01T01:00:00.000Z",
             -1.0, 110.0, 90.0, 105.0, 100.0],  # negative open
            [1672452000000, "2023-01-01T02:00:00.000Z",
             100.0, 110.0, 90.0, 105.0, 100.0],
        ]

        conn, _ = _create_mock_connector()

        records = conn.normalize(
            RawBatch(
                endpoint="fetchOHLCV",
                payload=payload,
                raw_meta={"symbol": "BTCUSDT", "timeframe": "1h"},
            )
        )

        assert len(records) == 2
        conn.close()

    def test_normalize_ohlcv_unknown_endpoint_raises(self) -> None:
        """Given unknown endpoint When normalize Then raises ValueError."""
        conn, _ = _create_mock_connector()

        with pytest.raises(ValueError) as exc_info:
            conn.normalize(
                RawBatch(
                    endpoint="fetchUnknown",
                    payload={},
                    raw_meta={},
                )
            )
        assert "unknown endpoint" in str(exc_info.value).lower()
        conn.close()


# ── TC-C-004: Trades fixture→canonical ────────────────────────────────


class TestNormalizeTrades:
    """TRADE normalize 测试（TC-C-004）。"""

    def test_normalize_trades_happy_path(self) -> None:
        """Given fetchTrades fixture When normalize Then TRADE records correct."""
        payload = _load_fixture("fetch_trades/happy.json")

        conn, _ = _create_mock_connector()

        records = conn.normalize(
            RawBatch(
                endpoint="fetchTrades",
                payload=payload,
                raw_meta={"symbol": "BTCUSDT", "timeframe": "1h"},
            )
        )

        assert len(records) == 2
        assert all(isinstance(r, TRADE) for r in records)

        first = records[0]
        assert first.source == "ccxt"
        assert first.schema_version == "1.0"
        assert first.quality_status == QualityStatus.VALID
        assert first.market_id == "CCXT-BINANCE:BTCUSDT:SPOT"
        assert first.side.value == "BUY"
        assert first.price == pytest.approx(16580.50, rel=1e-6)
        assert first.quantity == pytest.approx(0.5, rel=1e-6)
        assert first.trade_id == "trade_001"

        second = records[1]
        assert second.side.value == "SELL"
        assert second.trade_id == "trade_002"

        conn.close()

    def test_normalize_trades_empty(self) -> None:
        """Given empty trades When normalize Then 0 records."""
        payload: list[dict[str, Any]] = []

        conn, _ = _create_mock_connector()

        records = conn.normalize(
            RawBatch(
                endpoint="fetchTrades",
                payload=payload,
                raw_meta={"symbol": "BTCUSDT", "timeframe": "1h"},
            )
        )

        assert len(records) == 0
        conn.close()


# ── TC-C-013: capability detection edge cases ─────────────────────────


class TestCapabilityDetectionEdge:
    """capability detection 边界测试（TC-C-013）。"""

    def test_empty_intervals_when_no_ohlcv(self) -> None:
        """exchange.has[fetchOHLCV]=False → intervals 为空。"""
        conn, _ = _create_mock_connector(has_overrides={"fetchOHLCV": False})
        cap = conn.capabilities()
        assert len(cap.intervals) == 0
        conn.close()


# ── fetchOHLCV 2 页 cursor ────────────────────────────────────────────


class TestFetchOHLCVPagination:
    """fetchOHLCV 分页 cursor 测试。"""

    def test_fetch_ohlcv_cursor_advances(self) -> None:
        """Given fetchOHLCV 2 pages When fetch Then cursor advances correctly."""
        page1 = _load_fixture("fetch_ohlcv/two_pages.json")

        # Page 2: next hour's candles
        page2 = [
            [
                1672452000000,
                "2023-01-01T02:00:00.000Z",
                16600.00,
                16650.00,
                16580.00,
                16620.00,
                800.0,
            ],
        ]

        mock_exchange = _mock_exchange()
        call_count = [0]

        def _fetch_ohlcv(
            symbol: str,
            timeframe: str,
            since: int | None = None,
            limit: int | None = None,
        ) -> list:
            call_count[0] += 1
            if call_count[0] == 1:
                return page1
            return page2

        mock_exchange.fetchOHLCV = _fetch_ohlcv

        with patch.object(
            CcxtBridgeConnector, "_init_exchange",
            return_value=mock_exchange,
        ):
            conn = CcxtBridgeConnector(exchange="binance")

        start_ts = datetime.fromtimestamp(
            1672444800, tz=UTC
        ).replace(tzinfo=None)
        end_ts = datetime.fromtimestamp(
            1672452060, tz=UTC
        ).replace(tzinfo=None)

        req = FetchRequest(
            dataset_id="test",
            params={
                "symbol": "BTCUSDT",
                "type": "ohlcv",
                "interval": "1m",
            },
            start=start_ts,
            end=end_ts,
            cursor=None,
        )

        batches = list(conn.fetch(req))

        # Should have fetched 2 pages
        assert len(batches) == 2
        assert batches[0].endpoint == "fetchOHLCV"
        assert batches[1].endpoint == "fetchOHLCV"

        conn.close()

    def test_fetch_ohlcv_no_overlap(self) -> None:
        """Given fetchOHLCV 2 pages When normalize Then no duplicate timestamps."""
        page1 = _load_fixture("fetch_ohlcv/two_pages.json")
        page2 = [
            [
                1672452000000,
                "2023-01-01T02:00:00.000Z",
                16600.00,
                16650.00,
                16580.00,
                16620.00,
                800.0,
            ],
        ]

        mock_exchange = _mock_exchange()
        call_count = [0]

        def _fetch_ohlcv(
            symbol: str,
            timeframe: str,
            since: int | None = None,
            limit: int | None = None,
        ) -> list:
            call_count[0] += 1
            if call_count[0] == 1:
                return page1
            return page2

        mock_exchange.fetchOHLCV = _fetch_ohlcv

        with patch.object(
            CcxtBridgeConnector, "_init_exchange", return_value=mock_exchange
        ):
            conn = CcxtBridgeConnector(exchange="binance")

        start_ts = datetime.fromtimestamp(1672444800, tz=UTC).replace(tzinfo=None)
        end_ts = datetime.fromtimestamp(1672452060, tz=UTC).replace(tzinfo=None)

        req = FetchRequest(
            dataset_id="test",
            params={"symbol": "BTCUSDT", "type": "ohlcv", "interval": "1m"},
            start=start_ts,
            end=end_ts,
            cursor=None,
        )

        batches = list(conn.fetch(req))

        # Normalize both pages
        all_records = []
        for batch in batches:
            records = conn.normalize(batch)
            all_records.extend(records)

        # Collect timestamps
        timestamps = [r.event_time for r in all_records]
        # No duplicates
        assert len(timestamps) == len(set(timestamps))
        conn.close()


# ── GWT acceptance tests ──────────────────────────────────────────────


class TestGWT:
    """GWT (Given-When-Then) acceptance tests from task spec."""

    def test_gwt_ohlcv_record_count(self) -> None:
        """GWT: Given fetchOHLCV fixture When normalize Then OHLCV 记录数与 fixture 一致。"""
        payload = _load_fixture("fetch_ohlcv/happy.json")
        expected_count = len(payload)

        conn, _ = _create_mock_connector()

        records = conn.normalize(
            RawBatch(
                endpoint="fetchOHLCV",
                payload=payload,
                raw_meta={"symbol": "BTCUSDT", "timeframe": "1h"},
            )
        )

        assert len(records) == expected_count
        conn.close()

    def test_gwt_empty_result_normal(self) -> None:
        """GWT: Given empty OHLCV When normalize Then 0 rows (正常，非错误)。"""
        payload = _load_fixture("fetch_ohlcv/empty.json")

        conn, _ = _create_mock_connector()

        records = conn.normalize(
            RawBatch(
                endpoint="fetchOHLCV",
                payload=payload,
                raw_meta={"symbol": "BTCUSDT", "timeframe": "1h"},
            )
        )

        assert len(records) == 0
        conn.close()

    def test_gwt_market_id_format(self) -> None:
        """GWT: Given fetchOHLCV When normalize Then market_id=CCXT-BINANCE:BTCUSDT:SPOT。"""
        payload = _load_fixture("fetch_ohlcv/happy.json")

        conn, _ = _create_mock_connector()

        records = conn.normalize(
            RawBatch(
                endpoint="fetchOHLCV",
                payload=payload,
                raw_meta={"symbol": "BTCUSDT", "timeframe": "1h"},
            )
        )

        for record in records:
            assert record.market_id == "CCXT-BINANCE:BTCUSDT:SPOT"

        conn.close()


# ── Health ─────────────────────────────────────────────────────────────


class TestHealth:
    """CcxtBridgeConnector.health() 测试。"""

    def test_health_ok(self) -> None:
        """Given healthy exchange When health() Then ok=True."""
        mock_exchange = _mock_exchange()
        mock_exchange.load_markets = MagicMock()

        with patch.object(
            CcxtBridgeConnector, "_init_exchange", return_value=mock_exchange
        ):
            conn = CcxtBridgeConnector(exchange="binance")

        status = conn.health()
        assert isinstance(status, HealthStatus)
        assert status.ok is True
        assert status.latency_ms >= 0
        conn.close()

    def test_health_not_ok(self) -> None:
        """Given unhealthy exchange When health() Then ok=False."""
        mock_exchange = _mock_exchange()
        mock_exchange.load_markets = MagicMock(side_effect=ccxt.NetworkError("connection failed"))

        with patch.object(
            CcxtBridgeConnector, "_init_exchange", return_value=mock_exchange
        ):
            conn = CcxtBridgeConnector(exchange="binance")

        status = conn.health()
        assert isinstance(status, HealthStatus)
        assert status.ok is False
        conn.close()


# ── checkpoint_from ────────────────────────────────────────────────────


class TestCheckpoint:
    """checkpoint_from 测试。"""

    def test_checkpoint_from_ohlcv(self) -> None:
        """Given OHLCV RawBatch When checkpoint_from Then returns last timestamp."""
        payload = _load_fixture("fetch_ohlcv/happy.json")

        conn, _ = _create_mock_connector()

        checkpoint = conn.checkpoint_from(
            RawBatch(
                endpoint="fetchOHLCV",
                payload=payload,
                raw_meta={},
            )
        )

        assert checkpoint is not None
        assert checkpoint == str(payload[-1][0])
        conn.close()

    def test_checkpoint_from_trades(self) -> None:
        """Given TRADE RawBatch When checkpoint_from Then returns last trade_id."""
        payload = _load_fixture("fetch_trades/happy.json")

        conn, _ = _create_mock_connector()

        checkpoint = conn.checkpoint_from(
            RawBatch(
                endpoint="fetchTrades",
                payload=payload,
                raw_meta={},
            )
        )

        assert checkpoint is not None
        assert checkpoint == "trade_002"
        conn.close()

    def test_checkpoint_from_empty(self) -> None:
        """Given empty payload When checkpoint_from Then returns None."""
        conn, _ = _create_mock_connector()

        assert conn.checkpoint_from([]) is None
        assert conn.checkpoint_from(
            RawBatch(endpoint="fetchOHLCV", payload=[], raw_meta={})
        ) is None
        conn.close()


# ── validate ───────────────────────────────────────────────────────────


class TestValidate:
    """validate 测试。"""

    def test_validate_ohlcv_valid(self) -> None:
        """Given valid OHLCV When validate Then no findings."""
        conn, _ = _create_mock_connector()

        record = OHLCV(
            schema_version="1.0",
            source="ccxt",
            source_id="1",
            source_timestamp=datetime.now(UTC).replace(tzinfo=None),
            ingest_timestamp=datetime.now(UTC).replace(tzinfo=None),
            raw_record_id="ccxt:test::1",
            market_id="CCXT-BINANCE:BTCUSDT:SPOT",
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

    def test_validate_ohlcv_invalid_high(self) -> None:
        """Given OHLCV with low > open When validate Then Q-RANGE-001."""
        conn, _ = _create_mock_connector()

        record = OHLCV(
            schema_version="1.0",
            source="ccxt",
            source_id="1",
            source_timestamp=datetime.now(UTC).replace(tzinfo=None),
            ingest_timestamp=datetime.now(UTC).replace(tzinfo=None),
            raw_record_id="ccxt:test::1",
            market_id="CCXT-BINANCE:BTCUSDT:SPOT",
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


# ── missing symbol ────────────────────────────────────────────────────


class TestMissingSymbol:
    """missing symbol 测试。"""

    def test_fetch_without_symbol_raises(self) -> None:
        """Given no symbol When fetch Then raises ValueError."""
        conn, _ = _create_mock_connector()

        req = FetchRequest(
            dataset_id="test",
            params={"type": "ohlcv"},
            start=None,
            end=None,
            cursor=None,
        )

        with pytest.raises(ValueError) as exc_info:
            list(conn.fetch(req))
        assert "symbol" in str(exc_info.value).lower()
        conn.close()

    def test_unknown_type_raises(self) -> None:
        """Given unknown type When fetch Then raises ValueError."""
        conn, _ = _create_mock_connector()

        req = FetchRequest(
            dataset_id="test",
            params={"symbol": "BTCUSDT", "type": "unknown"},
            start=None,
            end=None,
            cursor=None,
        )

        with pytest.raises(ValueError) as exc_info:
            list(conn.fetch(req))
        assert "unknown" in str(exc_info.value).lower()
        conn.close()


# ── ccxt 异常映射（审计 SR-12）────────────────────────────────────────


class TestCcxtErrorMapping:
    """_map_ccxt_error 三分支（架构 08 §1 错误分类）。

    ccxt ≥4 层级：RateLimitExceeded ⊂ NetworkError；
    ExchangeError 与 NetworkError 同级 → 判定顺序 RateLimit → Network → else。
    """

    def test_rate_limit_error(self) -> None:
        """ccxt.RateLimitExceeded → RateLimitError（可重试 + 退避）。"""
        mapped = _map_ccxt_error("fetchOHLCV", ccxt.RateLimitExceeded("slow down"))
        assert isinstance(mapped, RateLimitError)
        assert "fetchOHLCV" in str(mapped)

    def test_network_error_maps_to_transport(self) -> None:
        """非限流 NetworkError（RequestTimeout）→ TransportError（可重试）。"""
        mapped = _map_ccxt_error("fetchTrades", ccxt.RequestTimeout("timed out"))
        assert isinstance(mapped, TransportError)
        assert not isinstance(mapped, RateLimitError)

    def test_exchange_error_maps_to_provider(self) -> None:
        """ccxt.ExchangeError（BadSymbol）→ ProviderError（不重试）。"""
        mapped = _map_ccxt_error("fetchTicker", ccxt.BadSymbol("no such symbol"))
        assert isinstance(mapped, ProviderError)
        assert not isinstance(mapped, TransportError)
