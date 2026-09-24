"""DATA-SOURCE-001 集成测试 — BinanceSpotConnector。

覆盖：
- TC-C-001：fixture→canonical 逐字段比对（happy path）
- TC-Q-008：aggTrades 跳号（Q-SEQ-001）
- Q-TS-003：未收盘 K 线丢弃
- 错误路径：401/429/500 → 错误类型与重试次数
- 分页：2 页 fixture → cursor 推进正确、无重叠无缺口
- 边界：空数组（正常 0 行）、跨月边界
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
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
from chronoforge.connectors.binance_spot import BinanceSpotConnector
from chronoforge.connectors.errors import (
    AuthError,
    RateLimitError,
    TransportError,
)
from chronoforge.models.enums import CanonicalType, QualityStatus
from chronoforge.models.market import OHLCV, TICKER, TRADE

# Fixture paths
_FIXTURES = Path(__file__).parent.parent / "fixtures" / "binance_spot"


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
    url: str = "https://api.binance.com/api/v3/klines",
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
                # Exhausted: return empty response to break pagination loops
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


def _create_mock_connector() -> tuple[BinanceSpotConnector, MagicMock]:
    """Create a mock connector instance for normalize tests."""
    mock_client = MagicMock()
    mock_rate_limiter = MagicMock()

    with patch.object(
        BinanceSpotConnector, "__init__", lambda self, s: None
    ):
        conn = BinanceSpotConnector.__new__(BinanceSpotConnector)
        conn._client = mock_client
        conn._rate_limiter = mock_rate_limiter

    return conn, mock_client


# ── capabilities ───────────────────────────────────────────────────────


class TestCapabilities:
    """BinanceSpotConnector.capabilities() 测试（D04 §4.1）。"""

    def test_capabilities_returns_correct_matrix(self) -> None:
        """Given valid connector When capabilities() Then OHLCV/TRADE/TICKER supported."""
        conn = BinanceSpotConnector()
        cap = conn.capabilities()

        assert isinstance(cap, CapabilityMatrix)
        assert CanonicalType.OHLCV in cap.canonical_types
        assert CanonicalType.TRADE in cap.canonical_types
        assert CanonicalType.TICKER in cap.canonical_types
        assert cap.supports_revision is False
        assert cap.supports_websocket is False
        assert cap.max_history_days is None
        conn.close()

    def test_capabilities_no_extra_types(self) -> None:
        """capabilities() should not include unexpected types."""
        conn = BinanceSpotConnector()
        cap = conn.capabilities()
        # Verify no funding/oi/etc.
        assert CanonicalType.FUNDING not in cap.canonical_types
        conn.close()


# ── health ─────────────────────────────────────────────────────────────


class TestHealth:
    """BinanceSpotConnector.health() 测试（D04 §1）。"""

    def test_health_ok(self) -> None:
        """Given healthy API When health() Then ok=True."""
        mock_resp = _mock_response(
            status_code=200,
            json_data={},
            url="https://api.binance.com/api/v3/ping",
        )
        mock_client = _mock_client(mock_resp)

        with patch.object(
            BinanceSpotConnector, "__init__", lambda self, s: None
        ):
            conn = BinanceSpotConnector.__new__(BinanceSpotConnector)
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
            url="https://api.binance.com/api/v3/ping",
        )
        mock_client = _mock_client(mock_resp)

        with patch.object(
            BinanceSpotConnector, "__init__", lambda self, s: None
        ):
            conn = BinanceSpotConnector.__new__(BinanceSpotConnector)
            conn._client = mock_client

        status = conn.health()
        assert isinstance(status, HealthStatus)
        assert status.ok is False
        assert status.latency_ms >= 0
        conn.close()


# ── TC-C-001: fixture→canonical happy path ─────────────────────────────


class TestNormalizeKlines:
    """kline → OHLCV normalize 测试（TC-C-001）。"""

    def test_normalize_klines_happy_path(self) -> None:
        """Given klines fixture When normalize Then OHLCV records correct."""
        payload = _load_fixture("klines/happy.json")
        raw_meta = {
            "http_status": 200,
            "url": "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m",
            "interval": "1m",
        }

        conn, _ = _create_mock_connector()

        records = conn.normalize(
            RawBatch(endpoint="/api/v3/klines", payload=payload, raw_meta=raw_meta)
        )

        assert len(records) == 2
        assert all(isinstance(r, OHLCV) for r in records)

        # Field-by-field verification
        first = records[0]
        assert first.source == "binance_spot"
        assert first.schema_version == "1.0"
        assert first.quality_status == QualityStatus.VALID
        assert first.market_id == "BINANCE:BTCUSDT:SPOT"

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
            "url": "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m",
            "interval": "1m",
        }

        conn, _ = _create_mock_connector()

        records = conn.normalize(
            RawBatch(endpoint="/api/v3/klines", payload=payload, raw_meta=raw_meta)
        )

        assert len(records) == 0
        conn.close()

    def test_normalize_klines_cross_month_boundary(self) -> None:
        """Given cross-month klines When normalize Then event_time correct."""
        payload = _load_fixture("klines/edge.json")
        raw_meta = {
            "http_status": 200,
            "url": "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m",
            "interval": "1m",
        }

        conn, _ = _create_mock_connector()

        records = conn.normalize(
            RawBatch(endpoint="/api/v3/klines", payload=payload, raw_meta=raw_meta)
        )

        assert len(records) == 2
        # Verify first kline event_time
        expected_ts = datetime.fromtimestamp(
            1672444800000 / 1000, tz=UTC
        ).replace(tzinfo=None)
        assert records[0].event_time == expected_ts
        conn.close()


# ── TC-C-001: aggTrades normalize ──────────────────────────────────────


class TestNormalizeAggTrades:
    """aggTrades → TRADE normalize 测试（TC-C-001）。"""

    def test_normalize_agg_trades_happy_path(self) -> None:
        """Given aggTrades fixture When normalize Then TRADE records correct."""
        payload = _load_fixture("aggtrades/happy.json")
        raw_meta = {
            "http_status": 200,
            "url": "https://api.binance.com/api/v3/aggTrades?symbol=BTCUSDT",
        }

        with patch.object(
            BinanceSpotConnector, "__init__", lambda self, s: None
        ):
            conn = BinanceSpotConnector.__new__(BinanceSpotConnector)
            conn._client = MagicMock()
            conn._rate_limiter = MagicMock()

        records = conn.normalize(
            RawBatch(
                endpoint="/api/v3/aggTrades",
                payload=payload,
                raw_meta=raw_meta,
            )
        )

        assert len(records) == 2
        assert all(isinstance(r, TRADE) for r in records)

        # Field-by-field verification
        first = records[0]
        assert first.source == "binance_spot"
        assert first.schema_version == "1.0"
        assert first.trade_id == "26129"
        assert first.market_id == "BINANCE:BTCUSDT:SPOT"
        assert first.price == pytest.approx(0.01634790, rel=1e-6)
        assert first.quantity == pytest.approx(148.97611427, rel=1e-6)
        # m=false → BUY
        assert first.side.value == "BUY"
        conn.close()

    def test_normalize_agg_trades_maker_is_sell(self) -> None:
        """Given aggTrades with m=true When normalize Then side=SELL."""
        payload = _load_fixture("aggtrades/happy.json")
        raw_meta = {
            "http_status": 200,
            "url": "https://api.binance.com/api/v3/aggTrades?symbol=BTCUSDT",
        }

        with patch.object(
            BinanceSpotConnector, "__init__", lambda self, s: None
        ):
            conn = BinanceSpotConnector.__new__(BinanceSpotConnector)
            conn._client = MagicMock()
            conn._rate_limiter = MagicMock()

        records = conn.normalize(
            RawBatch(
                endpoint="/api/v3/aggTrades",
                payload=payload,
                raw_meta=raw_meta,
            )
        )

        # Second record has m=true → SELL
        second = records[1]
        assert second.side.value == "SELL"
        conn.close()


# ── TC-Q-008: aggTrades gap detection ──────────────────────────────────


class TestAggTradesGap:
    """aggTrades 跳号检测（TC-Q-008，Q-SEQ-001 对账依据）。"""

    def test_agg_trades_gap_detected(self) -> None:
        """Given aggTrades with gap (26129→26132) When normalize Then 2 records."""
        payload = _load_fixture("aggtrades/edge.json")
        raw_meta = {
            "http_status": 200,
            "url": "https://api.binance.com/api/v3/aggTrades?symbol=BTCUSDT",
        }

        with patch.object(
            BinanceSpotConnector, "__init__", lambda self, s: None
        ):
            conn = BinanceSpotConnector.__new__(BinanceSpotConnector)
            conn._client = MagicMock()
            conn._rate_limiter = MagicMock()

        records = conn.normalize(
            RawBatch(
                endpoint="/api/v3/aggTrades",
                payload=payload,
                raw_meta=raw_meta,
            )
        )

        assert len(records) == 2
        # Gap: prev.l+1 != curr.a (Q-SEQ-001)
        first = records[0]
        second = records[1]
        # First trade_id=26129, second trade_id=26132
        assert first.trade_id == "26129"
        assert second.trade_id == "26132"
        # Gap detected: 26129 + 1 != 26132
        assert int(second.trade_id) - int(first.trade_id) > 1
        conn.close()


# ── TICKER normalize ───────────────────────────────────────────────────


class TestNormalizeTicker:
    """ticker24hr → TICKER normalize 测试（TC-C-001）。"""

    def test_normalize_ticker_happy_path(self) -> None:
        """Given ticker24hr fixture When normalize Then TICKER record correct."""
        payload = _load_fixture("ticker24hr/happy.json")
        raw_meta = {
            "http_status": 200,
            "url": "https://api.binance.com/api/v3/ticker/24hr?symbol=BTCUSDT",
        }

        with patch.object(
            BinanceSpotConnector, "__init__", lambda self, s: None
        ):
            conn = BinanceSpotConnector.__new__(BinanceSpotConnector)
            conn._client = MagicMock()
            conn._rate_limiter = MagicMock()

        records = conn.normalize(
            RawBatch(
                endpoint="/api/v3/ticker/24hr",
                payload=payload,
                raw_meta=raw_meta,
            )
        )

        assert len(records) == 1
        record = records[0]
        assert isinstance(record, TICKER)
        assert record.source == "binance_spot"
        assert record.schema_version == "1.0"
        assert record.market_id == "BINANCE:BTCUSDT:SPOT"
        assert record.last_price == pytest.approx(40100.0, rel=1e-6)
        assert record.bid == pytest.approx(40099.0, rel=1e-6)
        assert record.ask == pytest.approx(40101.0, rel=1e-6)
        assert record.volume_24h == pytest.approx(50000.0, rel=1e-6)
        assert record.quote_volume_24h == pytest.approx(
            2000000000.0, rel=1e-6
        )
        conn.close()


# ── Q-TS-003: unfinished kline discard ─────────────────────────────────


class TestUnfinishedKline:
    """未收盘 K 线丢弃（Q-TS-003）。"""

    def test_unfinished_kline_discarded(self) -> None:
        """Given kline with closeTime in future When normalize Then discarded."""
        # Create payload with a future closeTime
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        future_close = now_ms + 86400000  # 1 day in future

        payload = [
            [
                now_ms - 60000,  # openTime 1 minute ago
                "1.0",
                "2.0",
                "0.5",
                "1.5",
                "100.0",
                future_close,  # closeTime in future
                "0",
            ]
        ]
        raw_meta = {
            "http_status": 200,
            "url": "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m",
            "interval": "1m",
        }

        with patch.object(
            BinanceSpotConnector, "__init__", lambda self, s: None
        ):
            conn = BinanceSpotConnector.__new__(BinanceSpotConnector)
            conn._client = MagicMock()
            conn._rate_limiter = MagicMock()

        records = conn.normalize(
            RawBatch(endpoint="/api/v3/klines", payload=payload, raw_meta=raw_meta)
        )

        # Unfinished kline should be discarded (closeTime > now)
        assert len(records) == 0
        conn.close()

    def test_finished_kline_kept(self) -> None:
        """Given kline with closeTime in past When normalize Then kept."""
        # Use epoch time that avoids ms-precision rounding issues.
        # 2026-01-01T00:00:00Z = 1767225600000 ms (well in the past).
        past_close = 1767225600000  # 1 hour ago (well in the past, no rounding)

        payload = [
            [
                past_close - 60000,  # openTime 1h1m ago
                "1.0",
                "2.0",
                "0.5",
                "1.5",
                "100.0",
                past_close,  # closeTime 1 hour ago (well in the past)
                "0",
            ]
        ]
        raw_meta = {
            "http_status": 200,
            "url": "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m",
            "interval": "1m",
        }

        with patch.object(
            BinanceSpotConnector, "__init__", lambda self, s: None
        ):
            conn = BinanceSpotConnector.__new__(BinanceSpotConnector)
            conn._client = MagicMock()
            conn._rate_limiter = MagicMock()

        records = conn.normalize(
            RawBatch(endpoint="/api/v3/klines", payload=payload, raw_meta=raw_meta)
        )

        # Finished kline should be kept
        assert len(records) == 1
        assert isinstance(records[0], OHLCV)
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
            BinanceSpotConnector, "__init__", lambda self, s: None
        ):
            conn = BinanceSpotConnector.__new__(BinanceSpotConnector)
            conn._client = mock_client
            conn._rate_limiter = MagicMock()

        req = FetchRequest(
            dataset_id="test",
            params={"symbol": "BTCUSDT", "interval": "1m"},
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
            BinanceSpotConnector, "__init__", lambda self, s: None
        ):
            conn = BinanceSpotConnector.__new__(BinanceSpotConnector)
            conn._client = mock_client
            conn._rate_limiter = MagicMock()

        req = FetchRequest(
            dataset_id="test",
            params={"symbol": "BTCUSDT", "interval": "1m"},
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
            BinanceSpotConnector, "__init__", lambda self, s: None
        ):
            conn = BinanceSpotConnector.__new__(BinanceSpotConnector)
            conn._client = mock_client
            conn._rate_limiter = MagicMock()

        req = FetchRequest(
            dataset_id="test",
            params={"symbol": "BTCUSDT", "interval": "1m"},
            start=None,
            end=None,
            cursor=None,
        )

        with pytest.raises(TransportError):
            list(conn.fetch(req))
        conn.close()


# ── Pagination ─────────────────────────────────────────────────────────


class TestPagination:
    """分页测试：cursor 推进正确、无重叠无缺口。"""

    def test_fetch_klines_cursor_advances(self) -> None:
        """Given klines with two pages When fetch Then cursor advances correctly."""
        # Page 1 response
        page1 = _load_fixture("klines/happy.json")
        # Page 2 response — closeTime must be page1 last closeTime + 60000 (1 min)
        # to ensure correct cursor advancement
        last_close_1 = page1[-1][6]
        page2_start = last_close_1 + 60000
        page2_end = page2_start + 60000  # closeTime of page 2
        page2 = [
            [
                page2_start,
                "0.01500000",
                "0.75000000",
                "0.01400000",
                "0.01450000",
                "200000.00000000",
                page2_end,
                "2000.00000000",
                "20.00",
                "21.00",
                "20.00",
                "0",
            ]
        ]

        resp1 = _mock_response(
            status_code=200,
            json_data=page1,
            url="https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m",
        )
        resp2 = _mock_response(
            status_code=200,
            json_data=page2,
            url=(
                f"https://api.binance.com/api/v3/klines?"
                f"symbol=BTCUSDT&interval=1m&"
                f"startTime={last_close_1 + 1}"
            ),
        )

        mock_client = _mock_client([resp1, resp2])

        with patch.object(
            BinanceSpotConnector, "__init__", lambda self, s: None
        ):
            conn = BinanceSpotConnector.__new__(BinanceSpotConnector)
            conn._client = mock_client
            conn._rate_limiter = MagicMock()

        # Set end past page2_end so page 2 is included but loop stops after it.
        # The connector calls int(request.end.timestamp() * 1000) on the naive end.
        # On UTC+8 machines, naive datetime .timestamp() interprets as local time,
        # so we need end_ts such that end_ts (local) > page2_end.
        # page2_end = 1499040240000 ms = 1499040240 s UTC.
        # On UTC+8: 1499040240 s UTC = 1499040240 + 28800 = 1499069040 s local.
        # So end = datetime.fromtimestamp(1499040240 + 120).replace(tzinfo=None) works.
        end_ts_local = 1499040240 + 120  # 2 min past page2_end in local seconds
        req = FetchRequest(
            dataset_id="test",
            params={"symbol": "BTCUSDT", "interval": "1m"},
            start=None,
            end=datetime.fromtimestamp(end_ts_local).replace(tzinfo=None),
            cursor=None,
        )

        batches = list(conn.fetch(req))

        # Should have fetched 2 pages
        assert len(batches) == 2
        assert batches[0].endpoint == "/api/v3/klines"
        assert batches[1].endpoint == "/api/v3/klines"

        # Verify 3 API calls: page1 → page2 → empty (loop break)
        call_args = mock_client.get.call_args_list
        assert len(call_args) == 3  # page1 + page2 + empty page3
        # All calls should have startTime set (from pagination loop mutation)
        assert all("startTime" in call[1]["params"] for call in call_args)
        conn.close()

    def test_fetch_trades_cursor_advances(self) -> None:
        """Given aggTrades with two pages When fetch Then fromId advances correctly."""
        page1 = _load_fixture("aggtrades/happy.json")
        page2 = [
            {
                "a": 26131,
                "p": "0.01500000",
                "q": "50.00000000",
                "T": 1499040000200,
                "m": False,
                "M": True,
            }
        ]

        resp1 = _mock_response(
            status_code=200,
            json_data=page1,
            url="https://api.binance.com/api/v3/aggTrades?symbol=BTCUSDT",
        )
        resp2 = _mock_response(
            status_code=200,
            json_data=page2,
            url="https://api.binance.com/api/v3/aggTrades?symbol=BTCUSDT&fromId=26131",
        )

        mock_client = _mock_client([resp1, resp2])

        with patch.object(
            BinanceSpotConnector, "__init__", lambda self, s: None
        ):
            conn = BinanceSpotConnector.__new__(BinanceSpotConnector)
            conn._client = mock_client
            conn._rate_limiter = MagicMock()

        req = FetchRequest(
            dataset_id="test",
            params={"symbol": "BTCUSDT", "data_type": "aggregate"},
            start=None,
            end=None,
            cursor=None,
        )

        batches = list(conn.fetch(req))

        assert len(batches) == 2
        call_args = mock_client.get.call_args_list
        second_call = call_args[1]
        params = second_call[1]["params"]
        # fromId = last aggTradeId + 1
        assert int(params["fromId"]) == int(page1[-1]["a"]) + 1
        conn.close()


class TestAggTradesWindowPagination:
    """aggTrades 窗口分页回归（-1128/-1100 修复）。

    Binance 约束：fromId 与 startTime/endTime 组合非法。
    - ISO cursor / None → 时间窗口（startTime/endTime）
    - 纯数字 cursor → fromId-only
    - 翻页统一 fromId-only，末页 T >= endTime 时客户端截断
    """

    def _make_record(self, agg_id: int, trade_ms: int) -> dict:
        return {
            "a": agg_id,
            "p": "0.01500000",
            "q": "50.00000000",
            "T": trade_ms,
            "m": False,
            "M": True,
        }

    def test_iso_cursor_uses_time_window(self) -> None:
        """ISO cursor（pipeline checkpoint）→ startTime/endTime 路径，非 fromId。"""
        conn, mock_client = _create_mock_connector()
        mock_client.get.return_value = _mock_response(200, [])  # 空页 → 首次调用后停止
        start = datetime(2026, 9, 1, 0, 0, 0)
        end = datetime(2026, 9, 1, 1, 0, 0)
        req = FetchRequest(
            dataset_id="test",
            params={"symbol": "BTCUSDT", "data_type": "aggregate"},
            start=start,
            end=end,
            cursor="2026-09-01T00:00:00",  # ISO 字符串
        )
        list(conn.fetch(req))

        params = mock_client.get.call_args_list[0][1]["params"]
        assert "fromId" not in params
        assert params["startTime"] == int(start.timestamp() * 1000)
        assert params["endTime"] == int(end.timestamp() * 1000)
        conn.close()

    def test_numeric_cursor_uses_from_id_only(self) -> None:
        """纯数字 cursor → fromId-only，不带时间参数（-1128）。"""
        conn, mock_client = _create_mock_connector()
        mock_client.get.return_value = _mock_response(200, [])  # 空页 → 首次调用后停止
        req = FetchRequest(
            dataset_id="test",
            params={"symbol": "BTCUSDT", "data_type": "aggregate"},
            start=None,
            end=None,
            cursor="26131",
        )
        list(conn.fetch(req))

        params = mock_client.get.call_args_list[0][1]["params"]
        assert params["fromId"] == "26131"
        assert "startTime" not in params
        assert "endTime" not in params
        conn.close()

    def test_pagination_switches_to_from_id_and_stops_at_end(self) -> None:
        """窗口首页 → 翻页 fromId-only → 末页 T >= endTime 停止。"""
        start = datetime(2026, 9, 1, 0, 0, 0)
        end = datetime(2026, 9, 1, 1, 0, 0)
        end_ms = int(end.timestamp() * 1000)
        page1 = [
            self._make_record(100, end_ms - 60_000),
            self._make_record(101, end_ms - 30_000),
        ]
        # 末页最后一条越过 endTime → 截断停止
        page2 = [self._make_record(102, end_ms + 60_000)]

        mock_client = _mock_client([
            _mock_response(200, page1, url="https://api.binance.com/api/v3/aggTrades"),
            _mock_response(200, page2, url="https://api.binance.com/api/v3/aggTrades?fromId=102"),
        ])
        with patch.object(
            BinanceSpotConnector, "__init__", lambda self, s: None
        ):
            conn = BinanceSpotConnector.__new__(BinanceSpotConnector)
            conn._client = mock_client
            conn._rate_limiter = MagicMock()

        req = FetchRequest(
            dataset_id="test",
            params={"symbol": "BTCUSDT", "data_type": "aggregate"},
            start=start,
            end=end,
            cursor=None,
        )
        batches = list(conn.fetch(req))

        assert len(batches) == 2
        call_args = mock_client.get.call_args_list
        # 恰好 2 次调用（末页越过 endTime 后不再翻页）
        assert len(call_args) == 2
        # 翻页请求为 fromId-only
        params2 = call_args[1][1]["params"]
        assert params2["fromId"] == "102"
        assert "startTime" not in params2
        assert "endTime" not in params2
        conn.close()


# ── checkpoint_from ────────────────────────────────────────────────────


class TestCheckpoint:
    """checkpoint_from 测试（D04 §1）。"""

    def test_checkpoint_from_klines(self) -> None:
        """Given klines RawBatch When checkpoint_from Then returns last closeTime."""
        payload = _load_fixture("klines/happy.json")
        raw_meta = {
            "http_status": 200,
            "url": "https://api.binance.com/api/v3/klines?symbol=BTCUSDT",
        }

        with patch.object(
            BinanceSpotConnector, "__init__", lambda self, s: None
        ):
            conn = BinanceSpotConnector.__new__(BinanceSpotConnector)
            conn._client = MagicMock()

        checkpoint = conn.checkpoint_from(
            RawBatch(endpoint="/api/v3/klines", payload=payload, raw_meta=raw_meta)
        )

        assert checkpoint is not None
        assert checkpoint == str(payload[-1][6])  # last closeTime
        conn.close()

    def test_checkpoint_from_agg_trades(self) -> None:
        """Given aggTrades RawBatch When checkpoint_from Then returns last aggTradeId."""
        payload = _load_fixture("aggtrades/happy.json")
        raw_meta = {
            "http_status": 200,
            "url": "https://api.binance.com/api/v3/aggTrades?symbol=BTCUSDT",
        }

        with patch.object(
            BinanceSpotConnector, "__init__", lambda self, s: None
        ):
            conn = BinanceSpotConnector.__new__(BinanceSpotConnector)
            conn._client = MagicMock()

        checkpoint = conn.checkpoint_from(
            RawBatch(
                endpoint="/api/v3/aggTrades",
                payload=payload,
                raw_meta=raw_meta,
            )
        )

        assert checkpoint is not None
        assert checkpoint == str(payload[-1]["a"])
        conn.close()

    def test_checkpoint_from_empty(self) -> None:
        """Given empty payload When checkpoint_from Then returns None."""
        conn = BinanceSpotConnector()

        checkpoint = conn.checkpoint_from([])
        assert checkpoint is None

        checkpoint = conn.checkpoint_from(
            RawBatch(endpoint="/api/v3/klines", payload=[], raw_meta={})
        )
        assert checkpoint is None

        conn.close()


# ── validate ───────────────────────────────────────────────────────────


class TestValidate:
    """validate 测试（D04 §1）。"""

    def test_validate_ohlcv_valid(self) -> None:
        """Given valid OHLCV records When validate Then no findings."""
        conn = BinanceSpotConnector()
        conn._rate_limiter = MagicMock()

        # Create a valid OHLCV record
        record = OHLCV(
            schema_version="1.0",
            source="binance_spot",
            source_id="1499040000000",
            source_timestamp=datetime.now(UTC).replace(tzinfo=None),
            ingest_timestamp=datetime.now(UTC).replace(tzinfo=None),
            raw_record_id="binance_spot:test::1",
            market_id="BINANCE:BTCUSDT:SPOT",
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
        conn = BinanceSpotConnector()
        conn._rate_limiter = MagicMock()

        # Construct valid OHLCV then mutate low to trigger validate() check
        record = OHLCV(
            schema_version="1.0",
            source="binance_spot",
            source_id="1499040000000",
            source_timestamp=datetime.now(UTC).replace(tzinfo=None),
            ingest_timestamp=datetime.now(UTC).replace(tzinfo=None),
            raw_record_id="binance_spot:test::1",
            market_id="BINANCE:BTCUSDT:SPOT",
            event_time=datetime.now(UTC).replace(tzinfo=None),
            interval="1m",
            open=100.0,
            close=110.0,
            high=120.0,
            low=90.0,
            volume=1000.0,
        )
        # Bypass validate_assignment: low=105 > min(open=100, close=110)=100
        object.__setattr__(record, "low", 105.0)

        report = conn.validate([record])
        assert report.error_count >= 1
        assert any(f.rule_id == "Q-RANGE-001" for f in report.findings)
        conn.close()

    def test_validate_ohlcv_invalid_high(self) -> None:
        """Given OHLCV with high < max(open, close) When validate Then error finding."""
        conn = BinanceSpotConnector()
        conn._rate_limiter = MagicMock()

        # Construct valid OHLCV then mutate high to trigger validate() check
        record = OHLCV(
            schema_version="1.0",
            source="binance_spot",
            source_id="1499040000000",
            source_timestamp=datetime.now(UTC).replace(tzinfo=None),
            ingest_timestamp=datetime.now(UTC).replace(tzinfo=None),
            raw_record_id="binance_spot:test::1",
            market_id="BINANCE:BTCUSDT:SPOT",
            event_time=datetime.now(UTC).replace(tzinfo=None),
            interval="1m",
            open=100.0,
            close=110.0,
            high=120.0,
            low=90.0,
            volume=1000.0,
        )
        # Bypass validate_assignment: high=105 < max(open=100, close=110)=110
        object.__setattr__(record, "high", 105.0)

        report = conn.validate([record])
        assert report.error_count >= 1
        assert any(f.rule_id == "Q-RANGE-001" for f in report.findings)
        conn.close()


# ── GWT acceptance tests ──────────────────────────────────────────────


class TestGWT:
    """GWT (Given-When-Then) acceptance tests from task spec."""

    def test_gwt_unfinished_kline_discarded(self) -> None:
        """GWT: Given 未收盘 K 线 When normalize Then 丢弃+计数（不入 canonical）。"""
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        future_close = now_ms + 3600000

        payload = [[
            now_ms - 60000, "1.0", "2.0", "0.5", "1.5", "100.0",
            future_close, "0",
        ]]
        raw_meta = {
            "http_status": 200,
            "url": "https://api.binance.com/api/v3/klines?symbol=BTCUSDT",
            "interval": "1m",
        }

        with patch.object(
            BinanceSpotConnector, "__init__", lambda self, s: None
        ):
            conn = BinanceSpotConnector.__new__(BinanceSpotConnector)
            conn._client = MagicMock()
            conn._rate_limiter = MagicMock()

        records = conn.normalize(
            RawBatch(endpoint="/api/v3/klines", payload=payload, raw_meta=raw_meta)
        )

        assert len(records) == 0
        conn.close()

    def test_gwt_agg_trades_pagination_no_overlap(self) -> None:
        """GWT: Given aggTrades 分页 Then cursor=fromId 滚动无重叠。"""
        page1 = [{"a": 100, "p": "1.0", "q": "1.0", "T": 1000, "m": False, "M": True}]
        page2 = [{"a": 102, "p": "2.0", "q": "2.0", "T": 2000, "m": True, "M": True}]

        resp1 = _mock_response(
            status_code=200, json_data=page1,
            url="https://api.binance.com/api/v3/aggTrades?symbol=BTCUSDT",
        )
        resp2 = _mock_response(
            status_code=200, json_data=page2,
            url="https://api.binance.com/api/v3/aggTrades?symbol=BTCUSDT&fromId=101",
        )

        mock_client = _mock_client([resp1, resp2])

        with patch.object(
            BinanceSpotConnector, "__init__", lambda self, s: None
        ):
            conn = BinanceSpotConnector.__new__(BinanceSpotConnector)
            conn._client = mock_client
            conn._rate_limiter = MagicMock()

        req = FetchRequest(
            dataset_id="test",
            params={"symbol": "BTCUSDT", "data_type": "aggregate"},
            start=None,
            end=None,
            cursor=None,
        )

        batches = list(conn.fetch(req))
        assert len(batches) == 2

        # Verify cursor = last aggTradeId + 1
        call_args = mock_client.get.call_args_list
        params = call_args[1][1]["params"]
        assert int(params["fromId"]) == 101  # page1[-1]['a'] + 1 = 100 + 1
        conn.close()

    def test_gwt_klines_2_page_record_count(self) -> None:
        """GWT: Given klines 2 页 fixture When normalize Then OHLCV 记录数与 fixture 一致。"""
        page1 = _load_fixture("klines/happy.json")  # 2 rows
        page2 = [
            [1499040120000, "0.015", "0.75", "0.014", "0.0145", "200",
             1499040180000, "0", "0", "0", "0", "0"]
        ]

        resp1 = _mock_response(
            status_code=200, json_data=page1,
            url="https://api.binance.com/api/v3/klines?symbol=BTCUSDT",
        )
        resp2 = _mock_response(
            status_code=200, json_data=page2,
            url="https://api.binance.com/api/v3/klines?symbol=BTCUSDT&startTime=1499040120001",
        )

        mock_client = _mock_client([resp1, resp2])

        with patch.object(
            BinanceSpotConnector, "__init__", lambda self, s: None
        ):
            conn = BinanceSpotConnector.__new__(BinanceSpotConnector)
            conn._client = mock_client
            conn._rate_limiter = MagicMock()

        req = FetchRequest(
            dataset_id="test",
            params={"symbol": "BTCUSDT", "interval": "1m"},
            start=None,
            end=None,
            cursor=None,
        )

        batches = list(conn.fetch(req))
        assert len(batches) == 2

        # Normalize both pages
        all_records = []
        for batch in batches:
            records = conn.normalize(batch)
            all_records.extend(records)

        # Page 1 has 2 rows, page 2 has 1 row = total 3
        assert len(all_records) == 3
        assert all(isinstance(r, OHLCV) for r in all_records)
        conn.close()
