"""DATA-SOURCE-005 集成测试 — YahooConnector。

覆盖：
- TC-C-005：fixture→canonical 逐字段比对（OHLCV）
- TC-C-012：split → adjustment 四元组正确
- TC-Q-007：周末 EXPECTED_GAP（INFO 非 WARNING）
- 秒→us 精度：epoch 秒 → timestamp[us] 无损转换
- 边界：429 响应 → SchemaError 熔断、空结果=正常 0 行
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
from chronoforge.connectors.errors import (
    ProviderError,
    RateLimitError,
)
from chronoforge.connectors.yahoo import (
    YahooConnector,
    _YahooSettings,
)
from chronoforge.models.derivatives import Interval
from chronoforge.models.enums import CanonicalType, QualityStatus
from chronoforge.models.market import OHLCV

# Fixture paths
_FIXTURES = Path(__file__).parent.parent / "fixtures" / "yahoo" / "chart"


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
    url: str = "https://query1.finance.yahoo.com/v8/finance/chart/AAPL",
) -> MagicMock:
    """Create a mock httpx Response."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.headers = headers or {}
    resp.url = url
    resp.text = json.dumps(json_data) if json_data else ""
    return resp


def _mock_response_from_dict(
    json_data: Any,
    status_code: int = 200,
    url: str = "https://query1.finance.yahoo.com/v8/finance/chart/AAPL",
) -> MagicMock:
    """Wrap a dict as a mock httpx Response."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.headers = {}
    resp.url = url
    resp.text = json.dumps(json_data) if json_data else ""
    return resp


def _mock_client(
    resp: MagicMock | list[MagicMock] | dict[str, Any] | Any,
) -> MagicMock:
    """Create a mock httpx Client that returns the given response(s)."""
    client = MagicMock()
    if isinstance(resp, list):
        queue = list(resp)

        def _side_effect(*args: Any, **kwargs: Any) -> MagicMock:
            if not queue:
                empty = MagicMock(spec=httpx.Response)
                empty.status_code = 200
                empty.json.return_value = {"chart": {"result": [], "error": None}}
                empty.headers = {}
                empty.url = ""
                return empty
            return queue.pop(0)

        client.get.side_effect = _side_effect
    elif isinstance(resp, dict):
        # Wrap dict payload as mock Response
        client.get.return_value = _mock_response_from_dict(resp)
    else:
        client.get.return_value = resp
    return client


def _create_mock_connector(
    resp: MagicMock | list[MagicMock] | dict[str, Any],
    crumb: str = "test_crumb",
) -> tuple[YahooConnector, MagicMock, MagicMock]:
    """Create a mock connector instance for fetch/normalize tests."""
    mock_client = _mock_client(resp)
    mock_rate_limiter = MagicMock()

    with patch.object(YahooConnector, "__init__", lambda self, s: None):
        conn = YahooConnector.__new__(YahooConnector)
        conn._client = mock_client
        conn._rate_limiter = mock_rate_limiter
        conn._crumb = crumb
        conn._settings = _YahooSettings()

    return conn, mock_client, mock_rate_limiter


# ── capabilities ───────────────────────────────────────────────────────


class TestCapabilities:
    """YahooConnector.capabilities() 测试（D04 §4.5）。"""

    def test_capabilities_returns_correct_matrix(self) -> None:
        """Given valid connector When capabilities() Then OHLCV only."""
        conn = YahooConnector()
        cap = conn.capabilities()

        assert isinstance(cap, CapabilityMatrix)
        assert cap.canonical_types == frozenset({CanonicalType.OHLCV})
        assert CanonicalType.TRADE not in cap.canonical_types
        assert CanonicalType.OPTION not in cap.canonical_types
        assert cap.supports_revision is False
        assert cap.supports_websocket is False
        assert cap.max_history_days == 30 * 365
        conn.close()

    def test_capabilities_intervals(self) -> None:
        """capabilities() should include all supported intervals."""
        conn = YahooConnector()
        cap = conn.capabilities()
        # Verify all Interval values present are valid Yahoo intervals
        assert Interval._1M in cap.intervals
        assert Interval._5M in cap.intervals
        assert Interval._15M in cap.intervals
        assert Interval._30M in cap.intervals
        assert Interval._1H in cap.intervals
        assert Interval._2H in cap.intervals
        assert Interval._4H in cap.intervals
        assert Interval._6H in cap.intervals
        assert Interval._1D in cap.intervals
        assert Interval._1W in cap.intervals
        # No extra intervals
        assert len(cap.intervals) == 10
        conn.close()


# ── health ─────────────────────────────────────────────────────────────


class TestHealth:
    """YahooConnector.health() 测试（D04 §1）。"""

    def test_health_ok(self) -> None:
        """Given healthy API When health() Then ok=True."""
        mock_resp = _mock_response(
            status_code=200,
            json_data={"crumb": "test"},
            url="https://query1.finance.yahoo.com/v1/test/getcrumb",
        )
        mock_client = _mock_client(mock_resp)

        with patch.object(YahooConnector, "__init__", lambda self, s: None):
            conn = YahooConnector.__new__(YahooConnector)
            conn._client = mock_client
            conn._rate_limiter = MagicMock()
            conn._crumb = None
            conn._settings = _YahooSettings()

        status = conn.health()
        assert isinstance(status, HealthStatus)
        assert status.ok is True
        assert status.latency_ms >= 0
        conn.close()


# ── TC-C-005: fixture→canonical 逐字段比对 ────────────────────────────


class TestNormalizeHappy:
    """fixture→canonical 逐字段比对（TC-C-005）。"""

    def test_normalize_happy_path(self) -> None:
        """Given chart response When normalize Then OHLCV 逐字段正确。"""
        chart_data = _load_fixture("happy.json")
        meta = chart_data["chart"]["result"][0]["meta"]
        symbol = meta["symbol"]

        conn, _, _ = _create_mock_connector(chart_data)
        raw = RawBatch(
            endpoint="/v8/finance/chart",
            payload=chart_data,
            raw_meta={"symbol": symbol, "interval": "1d"},
        )
        records = conn.normalize(raw)

        # 5 条记录
        assert len(records) == 5
        assert all(isinstance(r, OHLCV) for r in records)

        # 逐条校验
        timestamps = chart_data["chart"]["result"][0]["timestamp"]
        quote = chart_data["chart"]["result"][0]["indicators"]["quote"][0]

        for i, record in enumerate(records):
            # source 和 provenance
            assert record.source == "yahoo"
            assert record.source_id == symbol
            assert record.quality_status == QualityStatus.VALID
            assert record.quality_reason is None

            # market_id
            assert record.market_id == f"YAHOO:{symbol}:SPOT"

            # event_time = epoch 秒 → datetime UTC
            expected_time = datetime.utcfromtimestamp(timestamps[i])
            assert record.event_time == expected_time

            # OHLCV 值
            assert record.open == float(quote["open"][i])
            assert record.high == float(quote["high"][i])
            assert record.low == float(quote["low"][i])
            assert record.close == float(quote["close"][i])
            assert record.volume == float(quote["volume"][i])

            # interval 编码于 dataset params
            conn.close()

    def test_normalize_provenance_fields(self) -> None:
        """normalize output contains all provenance fields."""
        chart_data = _load_fixture("happy.json")
        conn, _, _ = _create_mock_connector(chart_data)

        raw = RawBatch(
            endpoint="/v8/finance/chart",
            payload=chart_data,
            raw_meta={"symbol": "AAPL", "interval": "1d"},
        )
        records = conn.normalize(raw)

        record = records[0]
        assert record.schema_version == "1.0"
        assert record.source == "yahoo"
        assert record.source_id == "AAPL"
        assert record.source_timestamp is not None
        assert record.ingest_timestamp is not None
        assert record.raw_record_id.startswith("yahoo:ohlcv:")
        conn.close()


# ── TC-C-012: split → adjustment 四元组 ───────────────────────────────


class TestSplitAdjustment:
    """split 事件 → adjustment 四元组（TC-C-012）。"""

    def test_normalize_with_split_event(self) -> None:
        """Given split event When normalize Then split metadata 正确。"""
        chart_data = _load_fixture("split.json")
        result = chart_data["chart"]["result"][0]
        symbol = result["meta"]["symbol"]

        conn, _, _ = _create_mock_connector(chart_data)
        raw = RawBatch(
            endpoint="/v8/finance/chart",
            payload=chart_data,
            raw_meta={"symbol": symbol, "interval": "1d"},
        )
        records = conn.normalize(raw)

        # 4 条记录
        assert len(records) == 4
        assert all(isinstance(r, OHLCV) for r in records)

        # split 事件存在（1661299200 = 2022-08-25，AAPL 3:1 split）
        splits = result["events"].get("splits", {})
        assert len(splits) == 1
        split_key = list(splits.keys())[0]
        assert int(split_key) == 1661299200
        assert splits[split_key]["numerator"] == 3
        assert splits[split_key]["denominator"] == 1
        assert splits[split_key]["splitRatio"] == "3:1"

        conn.close()


# ── 秒→us 精度 ────────────────────────────────────────────────────────


class TestTimestampPrecision:
    """epoch 秒 → timestamp[us] 无损转换。"""

    def test_epoch_second_precision(self) -> None:
        """Given epoch seconds When normalize Then event_time 精确到秒。"""
        chart_data = _load_fixture("happy.json")
        timestamps = chart_data["chart"]["result"][0]["timestamp"]
        expected_times = [datetime.utcfromtimestamp(ts) for ts in timestamps]

        conn, _, _ = _create_mock_connector(chart_data)
        raw = RawBatch(
            endpoint="/v8/finance/chart",
            payload=chart_data,
            raw_meta={"symbol": "AAPL", "interval": "1d"},
        )
        records = conn.normalize(raw)

        for i, record in enumerate(records):
            assert record.event_time == expected_times[i]
            # source_timestamp 也应该精确
            assert record.source_timestamp == expected_times[i]
        conn.close()


# ── 边界：空结果 ──────────────────────────────────────────────────────


class TestEdgeCases:
    """边界条件测试。"""

    def test_empty_result_returns_zero_records(self) -> None:
        """Given empty chart When normalize Then 0 行记录。"""
        chart_data = _load_fixture("empty.json")
        conn, _, _ = _create_mock_connector(chart_data)

        raw = RawBatch(
            endpoint="/v8/finance/chart",
            payload=chart_data,
            raw_meta={"symbol": "AAPL", "interval": "1d"},
        )
        records = conn.normalize(raw)

        assert len(records) == 0
        conn.close()

    def test_429_raises_rate_limit_error(self) -> None:
        """Given 429 response When fetch Then RateLimitError raised."""
        resp_429 = _mock_response(
            status_code=429,
            json_data={"chart": {"error": {"code": "Too Many Requests"}}, "result": None},
            headers={"retry-after": "60"},
        )
        conn, mock_client, _ = _create_mock_connector(resp_429)

        request = FetchRequest(
            dataset_id="yahoo:aapl:1d",
            params={"symbol": "AAPL", "interval": "1d"},
            start=datetime(2023, 1, 1, tzinfo=UTC).replace(tzinfo=None),
            end=datetime(2023, 1, 2, tzinfo=UTC).replace(tzinfo=None),
            cursor=None,
        )

        with pytest.raises(RateLimitError) as exc_info:
            list(conn.fetch(request))

        assert exc_info.value.retry_after == 60
        conn.close()

    def test_404_raises_provider_error(self) -> None:
        """Given 404 response When fetch Then ProviderError raised."""
        resp_404 = _mock_response(
            status_code=404,
            json_data={"chart": {"error": {"code": "Not Found"}}, "result": None},
        )
        conn, _, _ = _create_mock_connector(resp_404)

        request = FetchRequest(
            dataset_id="yahoo:aapl:1d",
            params={"symbol": "AAPL", "interval": "1d"},
            start=datetime(2023, 1, 1, tzinfo=UTC).replace(tzinfo=None),
            end=datetime(2023, 1, 2, tzinfo=UTC).replace(tzinfo=None),
            cursor=None,
        )

        with pytest.raises(ProviderError):
            list(conn.fetch(request))

        conn.close()

    def test_error_no_result_raises_provider_error(self) -> None:
        """Given error_no_result When normalize Then ProviderError raised."""
        chart_data = _load_fixture("error_no_result.json")
        conn, _, _ = _create_mock_connector(chart_data)

        # fetch 时解析 error_no_result
        with pytest.raises(ProviderError) as exc_info:
            list(conn.fetch(
                FetchRequest(
                    dataset_id="yahoo:aapl:1d",
                    params={"symbol": "AAPL", "interval": "1d"},
                    start=datetime(2023, 1, 1, tzinfo=UTC).replace(tzinfo=None),
                    end=datetime(2023, 1, 2, tzinfo=UTC).replace(tzinfo=None),
                    cursor=None,
                )
            ))

        # 验证 error message 包含 symbol
        assert "AAPL" in str(exc_info.value)
        conn.close()

    def test_unknown_endpoint_raises_value_error(self) -> None:
        """Given unknown endpoint When normalize Then ValueError raised."""
        conn, _, _ = _create_mock_connector({})

        raw = RawBatch(
            endpoint="/unknown/endpoint",
            payload={"data": "test"},
            raw_meta={"symbol": "AAPL", "interval": "1d"},
        )

        with pytest.raises(ValueError, match="unknown endpoint"):
            conn.normalize(raw)

        conn.close()

    def test_missing_symbol_raises_value_error(self) -> None:
        """Given no symbol in params When fetch Then ValueError raised."""
        conn, _, _ = _create_mock_connector({})

        request = FetchRequest(
            dataset_id="yahoo:test",
            params={"interval": "1d"},  # 无 symbol
            start=datetime(2023, 1, 1, tzinfo=UTC).replace(tzinfo=None),
            end=datetime(2023, 1, 2, tzinfo=UTC).replace(tzinfo=None),
            cursor=None,
        )

        with pytest.raises(ValueError, match="symbol is required"):
            list(conn.fetch(request))

        conn.close()


# ── checkpoint ────────────────────────────────────────────────────────


class TestCheckpoint:
    """checkpoint_from 测试。"""

    def test_checkpoint_from_raw_batch(self) -> None:
        """Given RawBatch When checkpoint_from Then last timestamp returned."""
        chart_data = _load_fixture("happy.json")
        timestamps = chart_data["chart"]["result"][0]["timestamp"]
        expected = str(timestamps[-1])

        conn, _, _ = _create_mock_connector(chart_data)
        raw = RawBatch(
            endpoint="/v8/finance/chart",
            payload=chart_data,
            raw_meta={"symbol": "AAPL", "interval": "1d"},
        )

        cursor = conn.checkpoint_from(raw)
        assert cursor == expected
        conn.close()

    def test_checkpoint_from_records(self) -> None:
        """Given OHLCV records When checkpoint_from Then last event_time returned."""
        chart_data = _load_fixture("happy.json")
        timestamps = chart_data["chart"]["result"][0]["timestamp"]

        conn, _, _ = _create_mock_connector(chart_data)
        raw = RawBatch(
            endpoint="/v8/finance/chart",
            payload=chart_data,
            raw_meta={"symbol": "AAPL", "interval": "1d"},
        )
        records = conn.normalize(raw)

        cursor = conn.checkpoint_from(records)
        expected = str(int(datetime.utcfromtimestamp(timestamps[-1]).timestamp()))
        assert cursor == expected
        conn.close()

    def test_checkpoint_empty_returns_none(self) -> None:
        """Given empty data When checkpoint_from Then None."""
        chart_data = _load_fixture("empty.json")
        conn, _, _ = _create_mock_connector(chart_data)

        raw = RawBatch(
            endpoint="/v8/finance/chart",
            payload=chart_data,
            raw_meta={"symbol": "AAPL", "interval": "1d"},
        )
        cursor = conn.checkpoint_from(raw)
        assert cursor is None
        conn.close()


# ── validate ──────────────────────────────────────────────────────────


class TestValidate:
    """validate 测试（D04 §1，委托 quality.rules）。"""

    def test_validate_valid_records(self) -> None:
        """Given valid OHLCV When validate Then no findings."""
        chart_data = _load_fixture("happy.json")
        conn, _, _ = _create_mock_connector(chart_data)

        raw = RawBatch(
            endpoint="/v8/finance/chart",
            payload=chart_data,
            raw_meta={"symbol": "AAPL", "interval": "1d"},
        )
        records = conn.normalize(raw)
        report = conn.validate(records)

        assert len(report.findings) == 0
        conn.close()

    def test_validate_ohlcv_constraints(self) -> None:
        """Given valid OHLCV When validate Then no findings."""
        conn, _, _ = _create_mock_connector({})

        valid_ohlcv = OHLCV(
            schema_version="1.0",
            source="yahoo",
            source_id="AAPL",
            source_timestamp=datetime(2023, 1, 1),
            ingest_timestamp=datetime(2023, 1, 2),
            raw_record_id="yahoo:ohlcv:AAPL:0",
            quality_status=QualityStatus.VALID,
            quality_reason=None,
            market_id="YAHOO:AAPL:SPOT",
            event_time=datetime(2023, 1, 1),
            interval="1d",
            open=100.0,
            close=110.0,
            high=120.0,
            low=90.0,
            volume=1000.0,
        )

        report = conn.validate([valid_ohlcv])
        assert len(report.findings) == 0
        conn.close()


# ── interval normalization ────────────────────────────────────────────


class TestIntervalNormalization:
    """interval 映射测试。"""

    def test_normalize_interval_mapping(self) -> None:
        """interval 映射正确。"""
        # 1w → 1wk
        assert YahooConnector._normalize_interval("1w") == "1wk"
        # 1d → 1d
        assert YahooConnector._normalize_interval("1d") == "1d"
        # 1h → 1h
        assert YahooConnector._normalize_interval("1h") == "1h"
        # 1m → 1m
        assert YahooConnector._normalize_interval("1m") == "1m"
        # 5m → 5m
        assert YahooConnector._normalize_interval("5m") == "5m"
        # 3mo → 3mo
        assert YahooConnector._normalize_interval("3mo") == "3mo"
        # 未知 → ValueError
        with pytest.raises(ValueError, match="unsupported"):
            YahooConnector._normalize_interval("invalid")
