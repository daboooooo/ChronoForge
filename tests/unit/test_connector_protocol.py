"""Connector 协议测试（D04 §1，TC-C 组 + unit）。

验证 FetchRequest/RawBatch/CapabilityMatrix/HealthStatus 数据类契约。
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime

import pytest

from chronoforge.connectors.base import (
    CapabilityMatrix,
    FetchRequest,
    HealthStatus,
    InstrumentRef,
    RawBatch,
)
from chronoforge.models.derivatives import Interval
from chronoforge.models.enums import CanonicalType


class TestFetchRequest:
    """FetchRequest 数据类测试（D04 §1，frozen=True）。"""

    def test_fetch_request_all_fields_preserved(self) -> None:
        """Given valid FetchRequest When 构造 Then dataset_id/params/start/end/cursor 全部保留。"""
        now = datetime.now(UTC).replace(tzinfo=None)
        end = now.replace(hour=23)
        req = FetchRequest(
            dataset_id="BINANCE:BTCUSDT:SPOT",
            params={"symbol": "BTCUSDT", "interval": "1m"},
            start=now,
            end=end,
            cursor=None,
        )
        assert req.dataset_id == "BINANCE:BTCUSDT:SPOT"
        assert req.params == {"symbol": "BTCUSDT", "interval": "1m"}
        assert req.start == now
        assert req.end == end
        assert req.cursor is None

    def test_fetch_request_cursor_preserved(self) -> None:
        """FetchRequest cursor 字段支持非空字符串。"""
        req = FetchRequest(
            dataset_id="test-ds",
            params={},
            start=None,
            end=None,
            cursor="eyJ0aW1lc3RhbXAiOjE2MDAwMDAwMDB9",
        )
        assert req.cursor == "eyJ0aW1lc3RhbXAiOjE2MDAwMDAwMDB9"

    def test_fetch_request_is_frozen(self) -> None:
        """unit：FetchRequest 不可变（frozen=True）。"""
        req = FetchRequest(
            dataset_id="test",
            params={},
            start=None,
            end=None,
            cursor=None,
        )
        with pytest.raises(FrozenInstanceError):
            req.dataset_id = "modified"

    def test_fetch_request_params_mapping(self) -> None:
        """FetchRequest params 接受 Mapping 类型。"""
        from collections import OrderedDict

        params = OrderedDict([("symbol", "ETHUSDT"), ("interval", "5m")])
        req = FetchRequest(
            dataset_id="TEST",
            params=params,
            start=None,
            end=None,
            cursor=None,
        )
        assert req.params["symbol"] == "ETHUSDT"
        assert req.params["interval"] == "5m"


class TestRawBatch:
    """RawBatch 数据类测试（D04 §1）。"""

    def test_raw_batch_payload_any_type(self) -> None:
        """unit：RawBatch.payload 可任意类型（Any）。"""
        # String payload
        batch_str = RawBatch(
            endpoint="/api/v3/klines",
            payload='{"data": []}',
        )
        assert isinstance(batch_str.payload, str)

        # Dict payload
        batch_dict = RawBatch(
            endpoint="/api/v3/klines",
            payload={"data": [{"o": "1", "h": "2", "l": "0.5", "c": "1.5", "v": "100"}]},
        )
        assert isinstance(batch_dict.payload, dict)

        # List payload
        batch_list = RawBatch(
            endpoint="/api/v3/aggTrades",
            payload=[{"a": 1, "p": "100", "q": "1"}],
        )
        assert isinstance(batch_list.payload, list)

        # None payload
        batch_none = RawBatch(
            endpoint="/health",
            payload=None,
        )
        assert batch_none.payload is None

    def test_raw_batch_auto_fetched_at(self) -> None:
        """RawBatch 自动注入 fetched_at 时间戳。"""
        batch = RawBatch(
            endpoint="/api/v3/klines",
            payload={},
        )
        assert "fetched_at" in batch.raw_meta
        assert isinstance(batch.raw_meta["fetched_at"], datetime)

    def test_raw_batch_preserves_existing_fetched_at(self) -> None:
        """RawBatch 不覆盖已有的 fetched_at。"""
        custom_time = datetime(2024, 1, 1, 0, 0, 0)
        batch = RawBatch(
            endpoint="/api/v3/klines",
            payload={},
            raw_meta={"fetched_at": custom_time},
        )
        assert batch.raw_meta["fetched_at"] == custom_time

    def test_raw_batch_meta_fields(self) -> None:
        """RawBatch raw_meta 支持 http_status/headers_subset/url 等字段。"""
        batch = RawBatch(
            endpoint="/api/v3/klines",
            payload={},
            raw_meta={
                "http_status": 200,
                "url": "https://api.binance.com/api/v3/klines",
            },
        )
        assert batch.raw_meta["http_status"] == 200
        assert batch.raw_meta["url"] == "https://api.binance.com/api/v3/klines"
        assert "fetched_at" in batch.raw_meta


class TestCapabilityMatrix:
    """CapabilityMatrix 数据类测试（D04 §1，frozen=True）。"""

    def test_capability_matrix_all_fields(self) -> None:
        """CapabilityMatrix 包含全部必要字段。"""
        matrix = CapabilityMatrix(
            canonical_types=frozenset([CanonicalType.OHLCV, CanonicalType.TRADE]),
            intervals=frozenset([Interval._1M, Interval._5M]),
            supports_revision=True,
            supports_websocket=True,
            max_history_days=365,
        )
        assert matrix.canonical_types == frozenset([CanonicalType.OHLCV, CanonicalType.TRADE])
        assert matrix.intervals == frozenset([Interval._1M, Interval._5M])
        assert matrix.supports_revision is True
        assert matrix.supports_websocket is True
        assert matrix.max_history_days == 365

    def test_capability_matrix_is_frozen(self) -> None:
        """unit：CapabilityMatrix 不可变（frozen=True）。"""
        matrix = CapabilityMatrix(
            canonical_types=frozenset(),
            intervals=frozenset(),
            supports_revision=False,
            supports_websocket=False,
            max_history_days=None,
        )
        with pytest.raises(FrozenInstanceError):
            matrix.supports_revision = True

    def test_capability_matrix_empty_frozensets(self) -> None:
        """CapabilityMatrix 支持空 frozenset。"""
        matrix = CapabilityMatrix(
            canonical_types=frozenset(),
            intervals=frozenset(),
            supports_revision=False,
            supports_websocket=False,
            max_history_days=None,
        )
        assert matrix.canonical_types == frozenset()
        assert matrix.intervals == frozenset()
        assert matrix.max_history_days is None


class TestHealthStatus:
    """HealthStatus 数据类测试（D04 §1）。"""

    def test_health_status_fields(self) -> None:
        """HealthStatus 包含 ok/latency_ms/detail 字段。"""
        status = HealthStatus(ok=True, latency_ms=50, detail="OK")
        assert status.ok is True
        assert status.latency_ms == 50
        assert status.detail == "OK"

    def test_health_status_defaults(self) -> None:
        """HealthStatus detail 默认为空字符串。"""
        status = HealthStatus(ok=False, latency_ms=0)
        assert status.detail == ""


class TestInstrumentRef:
    """InstrumentRef 类型测试（D04 §1）。"""

    def test_instrument_ref_structure(self) -> None:
        """InstrumentRef 是包含 entity_id/instrument_id/market_id 的字典。"""
        ref: InstrumentRef = {
            "entity_id": "BTC",
            "instrument_id": "BTC-SPOT",
            "market_id": "BINANCE:BTCUSDT:SPOT",
        }
        assert "entity_id" in ref
        assert "instrument_id" in ref
        assert "market_id" in ref
