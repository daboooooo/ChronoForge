"""Market type tests（D09 TC-M 组，MODEL-002.1）。

覆盖 OHLCV/TRADE/TICKER/FUNDING/OPEN_INTEREST/ORDERBOOK 的合法实例化、
边界校验、失败拒绝及 natural_key() 契约。
"""

from datetime import datetime, timedelta

import pytest
from pydantic import ValidationError

from chronoforge.models.market import (
    FUNDING,
    OHLCV,
    OPEN_INTEREST,
    ORDERBOOK,
    TICKER,
    TRADE,
    Side,
)


class TestSide:
    """Side 枚举合法值遍历（D02 §2）。"""

    def test_side_buy_value(self) -> None:
        """TC-M-007a: Side.BUY 值为 "BUY"。"""
        assert Side.BUY == "BUY"

    def test_side_sell_value(self) -> None:
        """TC-M-007b: Side.SELL 值为 "SELL"。"""
        assert Side.SELL == "SELL"

    def test_side_all_values(self) -> None:
        """TC-M-007: 全枚举值遍历（仅 BUY/SELL）。"""
        assert list(Side) == [Side.BUY, Side.SELL]

    def test_side_invalid_value(self) -> None:
        """TC-M-007c: 非法值不能创建 Side。"""
        with pytest.raises(ValueError):
            Side("INVALID")


# ---------- OHLCV ----------


class TestOHLCV:
    """OHLCV 合法/边界/失败用例（TC-M-004 + D02 §2）。"""

    _now = datetime(2026, 9, 11, 12, 0, 0)

    def _sample(self, **overrides) -> dict:
        base = {
            "market_id": "BINANCE:BTCUSDT:SPOT",
            "event_time": self._now,
            "interval": "1m",
            "open": 50000.0,
            "high": 51000.0,
            "low": 49000.0,
            "close": 50500.0,
            "volume": 100.0,
            "schema_version": "1.0",
            "source": "binance_spot",
            "source_id": "BTCUSDT",
            "source_timestamp": self._now,
            "ingest_timestamp": self._now,
            "raw_record_id": "binance_spot:test:file.jsonl:1",
        }
        base.update(overrides)
        return base

    def test_ohlcv_valid(self) -> None:
        """TC-M-004: OHLCV 合法实例化（必填字段全提供）。"""
        record = OHLCV(**self._sample())
        assert record.market_id == "BINANCE:BTCUSDT:SPOT"
        assert record.high == 51000.0
        assert record.low == 49000.0

    def test_ohlcv_high_eq_open_ok(self) -> None:
        """TC-M-004边界: high == open == close（合法，非零区间 bar）。"""
        record = OHLCV(**self._sample(high=50000.0, open=50000.0, close=50000.0))
        assert record.high == 50000.0

    def test_ohlcv_high_eq_close_ok(self) -> None:
        """TC-M-004边界: high == close == open（合法）。"""
        record = OHLCV(**self._sample(high=50500.0, close=50500.0, open=50500.0))
        assert record.high == 50500.0

    def test_ohlcv_high_lt_open_rejected(self) -> None:
        """TC-M-004失败: high < open → ValidationError。"""
        with pytest.raises(ValidationError):
            OHLCV(**self._sample(high=49999.0, open=50000.0))

    def test_ohlcv_high_lt_close_rejected(self) -> None:
        """TC-M-004失败: high < close → ValidationError。"""
        with pytest.raises(ValidationError):
            OHLCV(**self._sample(high=50499.0, close=50500.0))

    def test_ohlcv_high_lt_low_rejected(self) -> None:
        """TC-M-002: high < low → ValidationError（OHLC 不变量蕴含拒绝）。"""
        with pytest.raises(ValidationError):
            OHLCV(**self._sample(high=100.0, low=200.0))

    def test_ohlcv_low_eq_open_ok(self) -> None:
        """TC-M-004边界: low == open（合法）。"""
        record = OHLCV(**self._sample(low=50000.0, open=50000.0))
        assert record.low == 50000.0

    def test_ohlcv_low_eq_close_ok(self) -> None:
        """TC-M-004边界: low == close == open（合法）。"""
        record = OHLCV(**self._sample(low=50000.0, close=50000.0, open=50000.0))
        assert record.low == 50000.0

    def test_ohlcv_low_gt_open_rejected(self) -> None:
        """TC-M-004失败: low > open → ValidationError。"""
        with pytest.raises(ValidationError):
            OHLCV(**self._sample(low=50001.0, open=50000.0))

    def test_ohlcv_low_gt_close_rejected(self) -> None:
        """TC-M-004失败: low > close → ValidationError。"""
        with pytest.raises(ValidationError):
            OHLCV(**self._sample(low=50501.0, close=50500.0))

    def test_ohlcv_nan_price_rejected(self) -> None:
        """TC-M-004失败: NaN 价格 → ValidationError。"""
        with pytest.raises(ValidationError):
            OHLCV(**self._sample(open=float("nan")))

    def test_ohlcv_inf_volume_rejected(self) -> None:
        """TC-M-004失败: Inf volume → ValidationError。"""
        with pytest.raises(ValidationError):
            OHLCV(**self._sample(volume=float("inf")))

    def test_ohlcv_zero_price_rejected(self) -> None:
        """TC-M-004失败: price = 0 → ValidationError（必须 > 0）。"""
        with pytest.raises(ValidationError):
            OHLCV(**self._sample(open=0.0))

    def test_ohlcv_natural_key(self) -> None:
        """TC-M: OHLCV natural_key = (market_id, event_time, interval)。"""
        record = OHLCV(**self._sample())
        assert record.natural_key() == ("BINANCE:BTCUSDT:SPOT", self._now, "1m")

    def test_ohlcv_extra_field_rejected(self) -> None:
        """TC-M: extra="forbid" → 未知字段拒绝。"""
        with pytest.raises(ValidationError):
            OHLCV(
                **{**self._sample(), "unknown_field": "boom"},
            )

    def test_ohlcv_9_combinations(self) -> None:
        """TC-M property: OHLCV high/low 对 open/close 的 9 种组合全覆盖。"""
        o, c = 50000.0, 50500.0
        # 合法：high >= max(o,c) 且 low <= min(o,c) → 4 种
        for high in [max(o, c), max(o, c) + 100]:
            for low in [min(o, c), min(o, c) - 100]:
                rec = OHLCV(
                    **self._sample(high=high, low=low),
                )
                assert rec.high == high
                assert rec.low == low
        # 非法：5 种（high < max 或 low > min）
        invalid_combos = [
            {"high": max(o, c) - 1, "low": min(o, c)},
            {"high": min(o, c), "low": max(o, c)},
            {"high": min(o, c), "low": min(o, c)},
            {"high": max(o, c), "low": min(o, c) + 1},
            {"high": max(o, c) - 1, "low": max(o, c)},
        ]
        for combo in invalid_combos:
            with pytest.raises(ValidationError):
                OHLCV(**self._sample(**combo))


# ---------- TRADE ----------


class TestTRADE:
    """TRADE 合法/边界/失败用例（TC-M-007 + D02 §2）。"""

    _now = datetime(2026, 9, 11, 12, 0, 0)

    def _sample(self, **overrides) -> dict:
        base = {
            "market_id": "BINANCE:BTCUSDT:SPOT",
            "event_time": self._now,
            "price": 50000.0,
            "quantity": 1.0,
            "side": Side.BUY,
            "trade_id": "abc123",
            "schema_version": "1.0",
            "source": "binance_spot",
            "source_id": "BTCUSDT",
            "source_timestamp": self._now,
            "ingest_timestamp": self._now,
            "raw_record_id": "binance_spot:test:file.jsonl:1",
        }
        base.update(overrides)
        return base

    def test_trade_valid_buy(self) -> None:
        """TC-M-007a: TRADE 合法 BUY 实例化。"""
        record = TRADE(**self._sample(side=Side.BUY))
        assert record.side == Side.BUY

    def test_trade_valid_sell(self) -> None:
        """TC-M-007b: TRADE 合法 SELL 实例化。"""
        record = TRADE(**self._sample(side=Side.SELL))
        assert record.side == Side.SELL

    def test_trade_invalid_side(self) -> None:
        """TC-M-007c: TRADE side="INVALID" → ValidationError。"""
        with pytest.raises(ValidationError):
            TRADE(**self._sample(side="INVALID"))  # type: ignore[arg-type]

    def test_trade_nan_price_rejected(self) -> None:
        """TC-M: price=NaN → ValidationError。"""
        with pytest.raises(ValidationError):
            TRADE(**self._sample(price=float("nan")))

    def test_trade_inf_quantity_rejected(self) -> None:
        """TC-M: quantity=Inf → ValidationError。"""
        with pytest.raises(ValidationError):
            TRADE(**self._sample(quantity=float("inf")))

    def test_trade_zero_price_rejected(self) -> None:
        """TC-M: price=0 → ValidationError。"""
        with pytest.raises(ValidationError):
            TRADE(**self._sample(price=0.0))

    def test_trade_natural_key(self) -> None:
        """TC-M: TRADE natural_key = (market_id, trade_id)。"""
        record = TRADE(**self._sample())
        assert record.natural_key() == ("BINANCE:BTCUSDT:SPOT", "abc123")


# ---------- TICKER ----------


class TestTICKER:
    """TICKER 合法/边界/失败用例（D02 §2）。"""

    _now = datetime(2026, 9, 11, 12, 0, 0)

    def _sample(self, **overrides) -> dict:
        base = {
            "market_id": "BINANCE:BTCUSDT:SPOT",
            "event_time": self._now,
            "last_price": 50000.0,
            "bid": 49999.0,
            "ask": 50001.0,
            "volume_24h": 1000.0,
            "quote_volume_24h": 50000000.0,
            "schema_version": "1.0",
            "source": "binance_spot",
            "source_id": "BTCUSDT",
            "source_timestamp": self._now,
            "ingest_timestamp": self._now,
            "raw_record_id": "binance_spot:test:file.jsonl:1",
        }
        base.update(overrides)
        return base

    def test_ticker_valid(self) -> None:
        """TC-M: TICKER 合法实例化。"""
        record = TICKER(**self._sample())
        assert record.last_price == 50000.0

    def test_ticker_zero_bid_ok(self) -> None:
        """TC-M: bid=0 合法（>= 0）。"""
        record = TICKER(**self._sample(bid=0.0))
        assert record.bid == 0.0

    def test_ticker_negative_ask_rejected(self) -> None:
        """TC-M: ask < 0 → ValidationError。"""
        with pytest.raises(ValidationError):
            TICKER(**self._sample(ask=-1.0))

    def test_ticker_nan_last_price_rejected(self) -> None:
        """TC-M: last_price=NaN → ValidationError。"""
        with pytest.raises(ValidationError):
            TICKER(**self._sample(last_price=float("nan")))

    def test_ticker_zero_last_price_rejected(self) -> None:
        """TC-M: last_price=0 → ValidationError。"""
        with pytest.raises(ValidationError):
            TICKER(**self._sample(last_price=0.0))

    def test_ticker_natural_key(self) -> None:
        """TC-M: TICKER natural_key = (market_id, event_time)。"""
        record = TICKER(**self._sample())
        assert record.natural_key() == ("BINANCE:BTCUSDT:SPOT", self._now)


# ---------- FUNDING ----------


class TestFUNDING:
    """FUNDING 合法/边界/失败用例（D02 §2）。"""

    def _sample(self, **overrides) -> dict:
        event_time = datetime(2026, 9, 11, 8, 0, 0)
        base = {
            "market_id": "BINANCE:BTCUSDT:USDT-FUT",
            "event_time": event_time,
            "funding_rate": 0.0001,
            "next_funding_time": datetime(2026, 9, 11, 16, 0, 0),
            "schema_version": "1.0",
            "source": "binance_spot",
            "source_id": "BTCUSDT",
            "source_timestamp": datetime(2026, 9, 11, 12, 0, 0),
            "ingest_timestamp": datetime(2026, 9, 11, 12, 0, 0),
            "raw_record_id": "binance_spot:test:file.jsonl:1",
        }
        base.update(overrides)
        return base

    def test_funding_valid(self) -> None:
        """TC-M: FUNDING 合法实例化。"""
        record = FUNDING(**self._sample())
        assert record.funding_rate == 0.0001

    def test_funding_negative_rate_ok(self) -> None:
        """TC-M: funding_rate < 0 合法（无界）。"""
        record = FUNDING(**self._sample(funding_rate=-0.0001))
        assert record.funding_rate == -0.0001

    def test_funding_nan_rate_rejected(self) -> None:
        """TC-M: funding_rate=NaN → ValidationError。"""
        with pytest.raises(ValidationError):
            FUNDING(**self._sample(funding_rate=float("nan")))

    def test_funding_next_eq_event_rejected(self) -> None:
        """TC-M: next_funding_time == event_time → ValidationError。"""
        event_time = datetime(2026, 9, 11, 8, 0, 0)
        with pytest.raises(ValidationError):
            FUNDING(**self._sample(event_time=event_time, next_funding_time=event_time))

    def test_funding_next_before_event_rejected(self) -> None:
        """TC-M: next_funding_time < event_time → ValidationError。"""
        with pytest.raises(ValidationError):
            FUNDING(
                **self._sample(
                    event_time=datetime(2026, 9, 11, 16, 0, 0),
                    next_funding_time=datetime(2026, 9, 11, 8, 0, 0),
                ),
            )

    def test_funding_natural_key(self) -> None:
        """TC-M: FUNDING natural_key = (market_id, event_time)。"""
        record = FUNDING(**self._sample())
        assert record.natural_key() == ("BINANCE:BTCUSDT:USDT-FUT", record.event_time)


# ---------- OPEN_INTEREST ----------


class TestOPEN_INTEREST:
    """OPEN_INTEREST 合法/边界/失败用例（D02 §2）。"""

    _now = datetime(2026, 9, 11, 12, 0, 0)

    def _sample(self, **overrides) -> dict:
        base = {
            "market_id": "BINANCE:BTCUSDT:USDT-FUT",
            "event_time": self._now,
            "open_interest": 50000.0,
            "unit": "BTC",
            "schema_version": "1.0",
            "source": "binance_spot",
            "source_id": "BTCUSDT",
            "source_timestamp": self._now,
            "ingest_timestamp": self._now,
            "raw_record_id": "binance_spot:test:file.jsonl:1",
        }
        base.update(overrides)
        return base

    def test_oi_valid(self) -> None:
        """TC-M: OPEN_INTEREST 合法实例化。"""
        record = OPEN_INTEREST(**self._sample())
        assert record.open_interest == 50000.0

    def test_oi_zero_ok(self) -> None:
        """TC-M: open_interest=0 合法（>= 0）。"""
        record = OPEN_INTEREST(**self._sample(open_interest=0.0))
        assert record.open_interest == 0.0

    def test_oi_negative_rejected(self) -> None:
        """TC-M: open_interest < 0 → ValidationError。"""
        with pytest.raises(ValidationError):
            OPEN_INTEREST(**self._sample(open_interest=-1.0))

    def test_oi_nan_rejected(self) -> None:
        """TC-M: open_interest=NaN → ValidationError。"""
        with pytest.raises(ValidationError):
            OPEN_INTEREST(**self._sample(open_interest=float("nan")))

    def test_oi_natural_key(self) -> None:
        """TC-M: OPEN_INTEREST natural_key = (market_id, event_time)。"""
        record = OPEN_INTEREST(**self._sample())
        assert record.natural_key() == ("BINANCE:BTCUSDT:USDT-FUT", self._now)


# ---------- ORDERBOOK ----------


class TestORDERBOOK:
    """ORDERBOOK 合法/边界/失败用例（TC-M-008 + D02 §2）。"""

    _now = datetime(2026, 9, 11, 12, 0, 0)

    def _sample(self, **overrides) -> dict:
        base = {
            "market_id": "BINANCE:BTCUSDT:SPOT",
            "event_time": self._now,
            "transaction_time": self._now,
            "bids": [[50000.0, 1.0], [49999.0, 2.0]],
            "asks": [[50001.0, 1.0], [50002.0, 2.0]],
            "schema_version": "1.0",
            "source": "binance_spot",
            "source_id": "BTCUSDT",
            "source_timestamp": self._now,
            "ingest_timestamp": self._now,
            "raw_record_id": "binance_spot:test:file.jsonl:1",
        }
        base.update(overrides)
        return base

    def test_orderbook_valid(self) -> None:
        """TC-M-008: ORDERBOOK 合法实例化。"""
        record = ORDERBOOK(**self._sample())
        assert record.bids == [[50000.0, 1.0], [49999.0, 2.0]]

    def test_orderbook_single_entry(self) -> None:
        """TC-M-008边界: 单 entry bids/asks（合法）。"""
        record = ORDERBOOK(
            **self._sample(
                bids=[[50000.0, 1.0]],
                asks=[[50001.0, 1.0]],
            ),
        )
        assert len(record.bids) == 1

    def test_orderbook_empty_bids_asks_ok(self) -> None:
        """TC-M-008边界: 空 bids/asks（合法）。"""
        record = ORDERBOOK(**self._sample(bids=[], asks=[]))
        assert record.bids == []

    def test_orderbook_depth_1000_ok(self) -> None:
        """TC-M-008边界: depth=1000（合法上限）。"""
        depth = 1000
        record = ORDERBOOK(
            **self._sample(
                bids=[[50000.0 - i, 1.0] for i in range(depth)],
                asks=[[50000.0 + i, 1.0] for i in range(depth)],
            ),
        )
        assert len(record.bids) == depth

    def test_orderbook_depth_1001_rejected(self) -> None:
        """TC-M-008边界: depth=1001（超限拒绝）。"""
        depth = 1001
        with pytest.raises(ValidationError):
            ORDERBOOK(
                **self._sample(
                    bids=[[50000.0 - i, 1.0] for i in range(depth)],
                    asks=[[50000.0 + i, 1.0] for i in range(depth)],
                ),
            )

    def test_orderbook_price_zero_rejected(self) -> None:
        """TC-M: bids 中 price=0 → ValidationError。"""
        with pytest.raises(ValidationError):
            ORDERBOOK(**self._sample(bids=[[0.0, 1.0]]))

    def test_orderbook_qty_negative_rejected(self) -> None:
        """TC-M: asks 中 qty < 0 → ValidationError。"""
        with pytest.raises(ValidationError):
            ORDERBOOK(**self._sample(asks=[[50001.0, -1.0]]))

    def test_orderbook_wrong_entry_length_rejected(self) -> None:
        """TC-M-008: bids entry 长度≠2 → ValidationError。"""
        with pytest.raises(ValidationError):
            ORDERBOOK(**self._sample(bids=[[50000.0]]))

    def test_orderbook_natural_key(self) -> None:
        """TC-M: ORDERBOOK natural_key = (market_id, event_time, transaction_time)。"""
        record = ORDERBOOK(**self._sample())
        assert record.natural_key() == (
            "BINANCE:BTCUSDT:SPOT",
            self._now,
            self._now,
        )


# ---------- natural_key 完整性 ----------


class TestNaturalKeyCompleteness:
    """natural_key() 返回值与 D02 身份键声明逐列一致（架构测试）。"""

    _now = datetime(2026, 9, 11, 12, 0, 0)

    def test_ohlcv_natural_key_signature(self) -> None:
        """D02 §2: OHLCV natural_key = (market_id, event_time, interval)。"""
        rec = OHLCV(
            market_id="M",
            event_time=self._now,
            interval="1m",
            open=1.0,
            high=2.0,
            low=0.5,
            close=1.5,
            volume=10.0,
            schema_version="1.0",
            source="s",
            source_id="sid",
            source_timestamp=self._now,
            ingest_timestamp=self._now,
            raw_record_id="r",
        )
        assert rec.natural_key() == ("M", self._now, "1m")

    def test_trade_natural_key_signature(self) -> None:
        """D02 §2: TRADE natural_key = (market_id, trade_id)。"""
        rec = TRADE(
            market_id="M",
            event_time=self._now,
            price=1.0,
            quantity=1.0,
            side=Side.BUY,
            trade_id="T1",
            schema_version="1.0",
            source="s",
            source_id="sid",
            source_timestamp=self._now,
            ingest_timestamp=self._now,
            raw_record_id="r",
        )
        assert rec.natural_key() == ("M", "T1")

    def test_ticker_natural_key_signature(self) -> None:
        """D02 §2: TICKER natural_key = (market_id, event_time)。"""
        rec = TICKER(
            market_id="M",
            event_time=self._now,
            last_price=1.0,
            bid=0.5,
            ask=1.5,
            volume_24h=10.0,
            quote_volume_24h=10.0,
            schema_version="1.0",
            source="s",
            source_id="sid",
            source_timestamp=self._now,
            ingest_timestamp=self._now,
            raw_record_id="r",
        )
        assert rec.natural_key() == ("M", self._now)

    def test_funding_natural_key_signature(self) -> None:
        """D02 §2: FUNDING natural_key = (market_id, event_time)。"""
        rec = FUNDING(
            market_id="M",
            event_time=self._now,
            funding_rate=0.0001,
            next_funding_time=self._now + timedelta(hours=8),
            schema_version="1.0",
            source="s",
            source_id="sid",
            source_timestamp=self._now,
            ingest_timestamp=self._now,
            raw_record_id="r",
        )
        assert rec.natural_key() == ("M", self._now)

    def test_open_interest_natural_key_signature(self) -> None:
        """D02 §2: OPEN_INTEREST natural_key = (market_id, event_time)。"""
        rec = OPEN_INTEREST(
            market_id="M",
            event_time=self._now,
            open_interest=100.0,
            unit="BTC",
            schema_version="1.0",
            source="s",
            source_id="sid",
            source_timestamp=self._now,
            ingest_timestamp=self._now,
            raw_record_id="r",
        )
        assert rec.natural_key() == ("M", self._now)

    def test_orderbook_natural_key_signature(self) -> None:
        """D02 §2: ORDERBOOK natural_key = (market_id, event_time, transaction_time)。"""
        tx_time = self._now
        rec = ORDERBOOK(
            market_id="M",
            event_time=self._now,
            transaction_time=tx_time,
            bids=[[1.0, 1.0]],
            asks=[[2.0, 1.0]],
            schema_version="1.0",
            source="s",
            source_id="sid",
            source_timestamp=self._now,
            ingest_timestamp=self._now,
            raw_record_id="r",
        )
        assert rec.natural_key() == ("M", self._now, tx_time)
