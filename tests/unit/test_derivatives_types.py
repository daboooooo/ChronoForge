"""Derivatives type tests（D09 TC-M 组，MODEL-002.2）。

覆盖 OPTION/IMPLIED_VOLATILITY/GREEKS/LIQUIDATION_EVENT/LIQUIDATION_AGGREGATE 的合法实例化、
边界校验、失败拒绝及 natural_key() 契约。
"""

from datetime import date, datetime

import pytest
from pydantic import ValidationError

from chronoforge.models.derivatives import (
    GREEKS,
    IMPLIED_VOLATILITY,
    LIQUIDATION_AGGREGATE,
    LIQUIDATION_EVENT,
    OPTION,
    DataTier,
    Interval,
    OptionType,
)
from chronoforge.models.market import Side


class TestOptionType:
    """OptionType 枚举合法值遍历（D02 §2）。"""

    def test_option_type_call_value(self) -> None:
        """TC-M-005: OptionType.CALL 值为 "CALL"。"""
        assert OptionType.CALL == "CALL"

    def test_option_type_put_value(self) -> None:
        """TC-M-005: OptionType.PUT 值为 "PUT"。"""
        assert OptionType.PUT == "PUT"

    def test_option_type_all_values(self) -> None:
        """TC-M: 全枚举值遍历（仅 CALL/PUT）。"""
        assert list(OptionType) == [OptionType.CALL, OptionType.PUT]

    def test_option_type_invalid_value(self) -> None:
        """TC-M: 非法值不能创建 OptionType。"""
        with pytest.raises(ValueError):
            OptionType("INVALID")


class TestDataTier:
    """DataTier 枚举合法值遍历（D02 §2）。"""

    def test_data_tier_all_values(self) -> None:
        """DataTier 全枚举值遍历（OTM/ATM/FULL）。"""
        assert list(DataTier) == [DataTier.OTM, DataTier.ATM, DataTier.FULL]

    def test_data_tier_invalid_value(self) -> None:
        """DataTier 非法值拒绝。"""
        with pytest.raises(ValueError):
            DataTier("BOGUS")


class TestInterval:
    """Interval 枚举合法值遍历（D02 §2）。"""

    def test_interval_all_values(self) -> None:
        """Interval 全枚举值遍历。"""
        assert list(Interval) == [
            Interval._1M, Interval._5M, Interval._15M, Interval._30M,
            Interval._1H, Interval._2H, Interval._4H, Interval._6H,
            Interval._8H, Interval._12H, Interval._1D, Interval._1W,
        ]

    def test_interval_invalid_value(self) -> None:
        """Interval 非法值拒绝。"""
        with pytest.raises(ValueError):
            Interval("INVALID")


# ---------- helper ----------


class _SampleFactory:
    """通用样本工厂。"""

    _now = datetime(2026, 9, 11, 12, 0, 0)

    @staticmethod
    def _base(**overrides) -> dict:
        return {
            "schema_version": "1.0",
            "source": "deribit",
            "source_id": "BTC-26SEP26-100000-C",
            "source_timestamp": _SampleFactory._now,
            "ingest_timestamp": _SampleFactory._now,
            "raw_record_id": "deribit:test:file.jsonl:1",
        }


# ---------- OPTION ----------


class TestOPTION:
    """OPTION 合法/边界/失败用例（TC-M-005 + TC-M-006 + D02 §2）。"""

    _now = datetime(2026, 9, 11, 12, 0, 0)

    def _sample(self, **overrides) -> dict:
        base = {
            "market_id": "DERIBIT:BTC-26SEP26-100000-C:OPTION",
            "instrument_id": "BTC-26SEP26-100000-C",
            "event_time": self._now,
            "underlying": "BTC",
            "expiry": date(2026, 9, 26),
            "strike": 100000.0,
            "option_type": OptionType.CALL,
            "settlement_asset": "USDC",
            "mark_price": 5000.0,
            "bid": 4900.0,
            "ask": 5100.0,
            **_SampleFactory._base(),
        }
        base.update(overrides)
        return base

    def test_option_full_parse(self) -> None:
        """TC-M-005: OPTION 完整解析（underlying/expiry/strike/option_type 全字段）。"""
        record = OPTION(**self._sample())
        assert record.underlying == "BTC"
        assert record.expiry == date(2026, 9, 26)
        assert record.strike == 100000.0
        assert record.option_type == OptionType.CALL
        assert record.settlement_asset == "USDC"
        assert record.mark_price == 5000.0
        assert record.bid == 4900.0
        assert record.ask == 5100.0

    def test_option_put_type(self) -> None:
        """TC-M-005: OPTION PUT 类型。"""
        record = OPTION(**self._sample(option_type=OptionType.PUT))
        assert record.option_type == OptionType.PUT

    def test_option_expired_expiry(self) -> None:
        """TC-M-006: 过期合约 OPTION（expiry < today）实例化通过。"""
        record = OPTION(**self._sample(expiry=date(2020, 1, 1)))
        assert record.expiry == date(2020, 1, 1)
        assert record.quality_status == "SUSPECT"
        assert record.quality_reason == "expiry is in the past"

    def test_option_future_expiry(self) -> None:
        """OPTION 未来 expiry（不 reject）。"""
        record = OPTION(**self._sample(expiry=date(2030, 1, 1)))
        assert record.expiry == date(2030, 1, 1)
        assert record.quality_status == "VALID"

    def test_option_zero_strike_rejected(self) -> None:
        """OPTION strike=0 → ValidationError。"""
        with pytest.raises(ValidationError):
            OPTION(**self._sample(strike=0.0))

    def test_option_negative_strike_rejected(self) -> None:
        """OPTION strike < 0 → ValidationError。"""
        with pytest.raises(ValidationError):
            OPTION(**self._sample(strike=-100.0))

    def test_option_negative_mark_price_rejected(self) -> None:
        """OPTION mark_price < 0 → ValidationError。"""
        with pytest.raises(ValidationError):
            OPTION(**self._sample(mark_price=-1.0))

    def test_option_zero_bid_ok(self) -> None:
        """OPTION bid=0 合法（>= 0）。"""
        record = OPTION(**self._sample(bid=0.0))
        assert record.bid == 0.0

    def test_option_negative_bid_rejected(self) -> None:
        """OPTION bid < 0 → ValidationError。"""
        with pytest.raises(ValidationError):
            OPTION(**self._sample(bid=-1.0))

    def test_option_negative_ask_rejected(self) -> None:
        """OPTION ask < 0 → ValidationError。"""
        with pytest.raises(ValidationError):
            OPTION(**self._sample(ask=-1.0))

    def test_option_nan_strike_rejected(self) -> None:
        """OPTION strike=NaN → ValidationError。"""
        with pytest.raises(ValidationError):
            OPTION(**self._sample(strike=float("nan")))

    def test_option_inf_mark_price_rejected(self) -> None:
        """OPTION mark_price=Inf → ValidationError。"""
        with pytest.raises(ValidationError):
            OPTION(**self._sample(mark_price=float("inf")))

    def test_option_invalid_option_type_rejected(self) -> None:
        """OPTION option_type="INVALID" → ValidationError。"""
        with pytest.raises(ValidationError):
            OPTION(**self._sample(option_type="INVALID"))  # type: ignore[arg-type]

    def test_option_natural_key(self) -> None:
        """OPTION natural_key = (instrument_id, event_time)。"""
        record = OPTION(**self._sample())
        assert record.natural_key() == ("BTC-26SEP26-100000-C", self._now)

    def test_option_extra_field_rejected(self) -> None:
        """OPTION extra="forbid" → 未知字段拒绝。"""
        with pytest.raises(ValidationError):
            OPTION(**{**self._sample(), "bogus_field": "boom"})


# ---------- IMPLIED_VOLATILITY ----------


class TestIMPLIED_VOLATILITY:
    """IMPLIED_VOLATILITY 合法/边界/失败用例（TC-M-009 + D02 §2）。"""

    _now = datetime(2026, 9, 11, 12, 0, 0)

    def _sample(self, **overrides) -> dict:
        base = {
            "instrument_id": "BTC-26SEP26-100000-C",
            "event_time": self._now,
            "iv": 0.65,
            "mark_iv": 0.64,
            "bid_iv": 0.63,
            "ask_iv": 0.65,
            "implied_forward": 1.2,
            "data_tier": DataTier.FULL,
            **_SampleFactory._base(),
        }
        base.update(overrides)
        return base

    def test_iv_valid(self) -> None:
        """IMPLIED_VOLATILITY 合法实例化。"""
        record = IMPLIED_VOLATILITY(**self._sample())
        assert record.iv == 0.65
        assert record.data_tier == DataTier.FULL

    def test_iv_boundary_zero(self) -> None:
        """TC-M-009: iv=0 合法（下边界）。"""
        record = IMPLIED_VOLATILITY(**self._sample(iv=0.0))
        assert record.iv == 0.0

    def test_iv_boundary_five(self) -> None:
        """TC-M-009: iv=5 合法（上边界）。"""
        record = IMPLIED_VOLATILITY(**self._sample(iv=5.0))
        assert record.iv == 5.0

    def test_iv_above_five_rejected(self) -> None:
        """TC-M: iv=5.1 → ValidationError。"""
        with pytest.raises(ValidationError):
            IMPLIED_VOLATILITY(**self._sample(iv=5.1))

    def test_iv_above_five_point_zero_rejected(self) -> None:
        """GWT: iv=6.0 → ValidationError。"""
        with pytest.raises(ValidationError):
            IMPLIED_VOLATILITY(**self._sample(iv=6.0))

    def test_iv_below_zero_rejected(self) -> None:
        """IMPLIED_VOLATILITY iv=-0.1 → ValidationError。"""
        with pytest.raises(ValidationError):
            IMPLIED_VOLATILITY(**self._sample(iv=-0.1))

    def test_iv_nan_rejected(self) -> None:
        """IMPLIED_VOLATILITY iv=NaN → ValidationError。"""
        with pytest.raises(ValidationError):
            IMPLIED_VOLATILITY(**self._sample(iv=float("nan")))

    def test_iv_inf_rejected(self) -> None:
        """IMPLIED_VOLATILITY iv=Inf → ValidationError。"""
        with pytest.raises(ValidationError):
            IMPLIED_VOLATILITY(**self._sample(iv=float("inf")))

    def test_iv_mark_iv_negative_rejected(self) -> None:
        """IMPLIED_VOLATILITY mark_iv < 0 → ValidationError。"""
        with pytest.raises(ValidationError):
            IMPLIED_VOLATILITY(**self._sample(mark_iv=-0.01))

    def test_iv_bid_iv_negative_rejected(self) -> None:
        """IMPLIED_VOLATILITY bid_iv < 0 → ValidationError。"""
        with pytest.raises(ValidationError):
            IMPLIED_VOLATILITY(**self._sample(bid_iv=-0.01))

    def test_iv_ask_iv_negative_rejected(self) -> None:
        """IMPLIED_VOLATILITY ask_iv < 0 → ValidationError。"""
        with pytest.raises(ValidationError):
            IMPLIED_VOLATILITY(**self._sample(ask_iv=-0.01))

    def test_iv_null_mark_iv_ok(self) -> None:
        """IMPLIED_VOLATILITY mark_iv=None 合法。"""
        record = IMPLIED_VOLATILITY(**self._sample(mark_iv=None))
        assert record.mark_iv is None

    def test_iv_oti_data_tier(self) -> None:
        """IMPLIED_VOLATILITY DataTier.OTM 合法。"""
        record = IMPLIED_VOLATILITY(**self._sample(data_tier=DataTier.OTM))
        assert record.data_tier == DataTier.OTM

    def test_iv_atm_data_tier(self) -> None:
        """IMPLIED_VOLATILITY DataTier.ATM 合法。"""
        record = IMPLIED_VOLATILITY(**self._sample(data_tier=DataTier.ATM))
        assert record.data_tier == DataTier.ATM

    def test_iv_invalid_data_tier_rejected(self) -> None:
        """IMPLIED_VOLATILITY invalid DataTier → ValidationError。"""
        with pytest.raises(ValueError):
            IMPLIED_VOLATILITY(**self._sample(data_tier="BOGUS"))  # type: ignore[arg-type]

    def test_iv_natural_key(self) -> None:
        """IMPLIED_VOLATILITY natural_key = (instrument_id, event_time)。"""
        record = IMPLIED_VOLATILITY(**self._sample())
        assert record.natural_key() == ("BTC-26SEP26-100000-C", self._now)

    def test_iv_extra_field_rejected(self) -> None:
        """IMPLIED_VOLATILITY extra="forbid" → 未知字段拒绝。"""
        with pytest.raises(ValidationError):
            IMPLIED_VOLATILITY(**{**self._sample(), "bogus_field": "boom"})


# ---------- GREEKS ----------


class TestGREEKS:
    """GREEKS 合法/边界/失败用例（TC-M-010 + D02 §2）。"""

    _now = datetime(2026, 9, 11, 12, 0, 0)

    def _sample(self, **overrides) -> dict:
        base = {
            "instrument_id": "BTC-26SEP26-100000-C",
            "event_time": self._now,
            "delta": 0.55,
            "gamma": 0.00001,
            "vega": 0.12,
            "theta": -0.05,
            "rho": 0.03,
            **_SampleFactory._base(),
        }
        base.update(overrides)
        return base

    def test_greeks_valid(self) -> None:
        """GREEKS 合法实例化（全部希腊字母）。"""
        record = GREEKS(**self._sample())
        assert record.delta == 0.55
        assert record.gamma == 0.00001
        assert record.vega == 0.12
        assert record.theta == -0.05
        assert record.rho == 0.03

    def test_greeks_delta_at_negative_one(self) -> None:
        """GREEKS delta=-1 合法（下边界）。"""
        record = GREEKS(**self._sample(delta=-1.0))
        assert record.delta == -1.0

    def test_greeks_delta_at_one(self) -> None:
        """GREEKS delta=1 合法（上边界）。"""
        record = GREEKS(**self._sample(delta=1.0))
        assert record.delta == 1.0

    def test_greeks_delta_above_one_rejected(self) -> None:
        """TC-M-010 + GWT: delta=1.5 → ValidationError。"""
        with pytest.raises(ValidationError):
            GREEKS(**self._sample(delta=1.5))

    def test_greeks_delta_below_minus_one_rejected(self) -> None:
        """GREEKS delta=-1.1 → ValidationError。"""
        with pytest.raises(ValidationError):
            GREEKS(**self._sample(delta=-1.1))

    def test_greeks_gamma_zero_ok(self) -> None:
        """GREEKS gamma=0 合法（>= 0）。"""
        record = GREEKS(**self._sample(gamma=0.0))
        assert record.gamma == 0.0

    def test_greeks_gamma_negative_rejected(self) -> None:
        """TC-M: gamma=-1 → ValidationError。"""
        with pytest.raises(ValidationError):
            GREEKS(**self._sample(gamma=-1.0))

    def test_greeks_nan_delta_rejected(self) -> None:
        """GREEKS delta=NaN → ValidationError。"""
        with pytest.raises(ValidationError):
            GREEKS(**self._sample(delta=float("nan")))

    def test_greeks_inf_gamma_rejected(self) -> None:
        """GREEKS gamma=Inf → ValidationError。"""
        with pytest.raises(ValidationError):
            GREEKS(**self._sample(gamma=float("inf")))

    def test_greeks_vega_zero_ok(self) -> None:
        """GREEKS vega=0 合法（无负值限制）。"""
        record = GREEKS(**self._sample(vega=0.0))
        assert record.vega == 0.0

    def test_greeks_theta_zero_ok(self) -> None:
        """GREEKS theta=0 合法。"""
        record = GREEKS(**self._sample(theta=0.0))
        assert record.theta == 0.0

    def test_greeks_rho_zero_ok(self) -> None:
        """GREEKS rho=0 合法。"""
        record = GREEKS(**self._sample(rho=0.0))
        assert record.rho == 0.0

    def test_greeks_natural_key(self) -> None:
        """GREEKS natural_key = (instrument_id, event_time)。"""
        record = GREEKS(**self._sample())
        assert record.natural_key() == ("BTC-26SEP26-100000-C", self._now)

    def test_greeks_extra_field_rejected(self) -> None:
        """GREEKS extra="forbid" → 未知字段拒绝。"""
        with pytest.raises(ValidationError):
            GREEKS(**{**self._sample(), "bogus_field": "boom"})


# ---------- LIQUIDATION_EVENT ----------


class TestLIQUIDATION_EVENT:
    """LIQUIDATION_EVENT 合法/边界/失败用例（D02 §2）。"""

    _now = datetime(2026, 9, 11, 12, 0, 0)

    def _sample(self, **overrides) -> dict:
        base = {
            "market_id": "BINANCE:BTCUSDT:USDT-FUT",
            "event_time": self._now,
            "side": Side.BUY,
            "price": 50000.0,
            "quantity": 10.0,
            "order_id": "liq-order-001",
            **_SampleFactory._base(),
        }
        base.update(overrides)
        return base

    def test_liquidation_event_buy(self) -> None:
        """LIQUIDATION_EVENT BUY（空头爆仓）合法实例化。"""
        record = LIQUIDATION_EVENT(**self._sample(side=Side.BUY))
        assert record.side == Side.BUY

    def test_liquidation_event_sell(self) -> None:
        """LIQUIDATION_EVENT SELL（多头爆仓）合法实例化。"""
        record = LIQUIDATION_EVENT(**self._sample(side=Side.SELL))
        assert record.side == Side.SELL

    def test_liquidation_event_valid(self) -> None:
        """LIQUIDATION_EVENT 完整合法实例化。"""
        record = LIQUIDATION_EVENT(**self._sample())
        assert record.price == 50000.0
        assert record.quantity == 10.0
        assert record.order_id == "liq-order-001"

    def test_liquidation_event_zero_price_rejected(self) -> None:
        """LIQUIDATION_EVENT price=0 → ValidationError。"""
        with pytest.raises(ValidationError):
            LIQUIDATION_EVENT(**self._sample(price=0.0))

    def test_liquidation_event_negative_price_rejected(self) -> None:
        """LIQUIDATION_EVENT price < 0 → ValidationError。"""
        with pytest.raises(ValidationError):
            LIQUIDATION_EVENT(**self._sample(price=-1.0))

    def test_liquidation_event_zero_quantity_rejected(self) -> None:
        """LIQUIDATION_EVENT quantity=0 → ValidationError。"""
        with pytest.raises(ValidationError):
            LIQUIDATION_EVENT(**self._sample(quantity=0.0))

    def test_liquidation_event_negative_quantity_rejected(self) -> None:
        """LIQUIDATION_EVENT quantity < 0 → ValidationError。"""
        with pytest.raises(ValidationError):
            LIQUIDATION_EVENT(**self._sample(quantity=-1.0))

    def test_liquidation_event_empty_order_id_rejected(self) -> None:
        """TC-M: order_id="" → ValidationError。"""
        with pytest.raises(ValidationError):
            LIQUIDATION_EVENT(**self._sample(order_id=""))

    def test_liquidation_event_nan_price_rejected(self) -> None:
        """LIQUIDATION_EVENT price=NaN → ValidationError。"""
        with pytest.raises(ValidationError):
            LIQUIDATION_EVENT(**self._sample(price=float("nan")))

    def test_liquidation_event_inf_quantity_rejected(self) -> None:
        """LIQUIDATION_EVENT quantity=Inf → ValidationError。"""
        with pytest.raises(ValidationError):
            LIQUIDATION_EVENT(**self._sample(quantity=float("inf")))

    def test_liquidation_event_invalid_side_rejected(self) -> None:
        """LIQUIDATION_EVENT side="INVALID" → ValidationError。"""
        with pytest.raises(ValidationError):
            LIQUIDATION_EVENT(**self._sample(side="INVALID"))  # type: ignore[arg-type]

    def test_liquidation_event_natural_key(self) -> None:
        """LIQUIDATION_EVENT natural_key = (market_id, order_id)。"""
        record = LIQUIDATION_EVENT(**self._sample())
        assert record.natural_key() == ("BINANCE:BTCUSDT:USDT-FUT", "liq-order-001")

    def test_liquidation_event_extra_field_rejected(self) -> None:
        """LIQUIDATION_EVENT extra="forbid" → 未知字段拒绝。"""
        with pytest.raises(ValidationError):
            LIQUIDATION_EVENT(**{**self._sample(), "bogus_field": "boom"})


# ---------- LIQUIDATION_AGGREGATE ----------


class TestLIQUIDATION_AGGREGATE:
    """LIQUIDATION_AGGREGATE 合法/边界/失败用例（D02 §2）。"""

    _now = datetime(2026, 9, 11, 12, 0, 0)

    def _sample(self, **overrides) -> dict:
        base = {
            "market_id": "BINANCE:BTCUSDT:USDT-FUT",
            "event_time": self._now,
            "interval": Interval._1H,
            "buy_vol": 100.0,
            "sell_vol": 80.0,
            "total_notional": 9000000.0,
            **_SampleFactory._base(),
        }
        base.update(overrides)
        return base

    def test_liquidation_aggregate_valid(self) -> None:
        """LIQUIDATION_AGGREGATE 合法实例化。"""
        record = LIQUIDATION_AGGREGATE(**self._sample())
        assert record.buy_vol == 100.0
        assert record.sell_vol == 80.0
        assert record.total_notional == 9000000.0
        assert record.interval == Interval._1H

    def test_liquidation_aggregate_zero_vol_ok(self) -> None:
        """LIQUIDATION_AGGREGATE buy_vol=0 合法（>= 0）。"""
        record = LIQUIDATION_AGGREGATE(**self._sample(buy_vol=0.0))
        assert record.buy_vol == 0.0

    def test_liquidation_aggregate_negative_buy_vol_rejected(self) -> None:
        """LIQUIDATION_AGGREGATE buy_vol < 0 → ValidationError。"""
        with pytest.raises(ValidationError):
            LIQUIDATION_AGGREGATE(**self._sample(buy_vol=-1.0))

    def test_liquidation_aggregate_negative_sell_vol_rejected(self) -> None:
        """LIQUIDATION_AGGREGATE sell_vol < 0 → ValidationError。"""
        with pytest.raises(ValidationError):
            LIQUIDATION_AGGREGATE(**self._sample(sell_vol=-1.0))

    def test_liquidation_aggregate_negative_notional_rejected(self) -> None:
        """LIQUIDATION_AGGREGATE total_notional < 0 → ValidationError。"""
        with pytest.raises(ValidationError):
            LIQUIDATION_AGGREGATE(**self._sample(total_notional=-1.0))

    def test_liquidation_aggregate_nan_buy_vol_rejected(self) -> None:
        """LIQUIDATION_AGGREGATE buy_vol=NaN → ValidationError。"""
        with pytest.raises(ValidationError):
            LIQUIDATION_AGGREGATE(**self._sample(buy_vol=float("nan")))

    def test_liquidation_aggregate_inf_notional_rejected(self) -> None:
        """LIQUIDATION_AGGREGATE total_notional=Inf → ValidationError。"""
        with pytest.raises(ValidationError):
            LIQUIDATION_AGGREGATE(**self._sample(total_notional=float("inf")))

    def test_liquidation_aggregate_invalid_interval_rejected(self) -> None:
        """LIQUIDATION_AGGREGATE invalid Interval → ValidationError。"""
        with pytest.raises(ValidationError):
            LIQUIDATION_AGGREGATE(**self._sample(interval="INVALID"))  # type: ignore[arg-type]

    def test_liquidation_aggregate_all_intervals_ok(self) -> None:
        """LIQUIDATION_AGGREGATE 所有 Interval 值合法。"""
        for interval in Interval:
            record = LIQUIDATION_AGGREGATE(**self._sample(interval=interval))
            assert record.interval == interval

    def test_liquidation_aggregate_natural_key(self) -> None:
        """LIQUIDATION_AGGREGATE natural_key = (market_id, event_time, interval)。"""
        record = LIQUIDATION_AGGREGATE(**self._sample())
        assert record.natural_key() == ("BINANCE:BTCUSDT:USDT-FUT", self._now, Interval._1H)

    def test_liquidation_aggregate_extra_field_rejected(self) -> None:
        """LIQUIDATION_AGGREGATE extra="forbid" → 未知字段拒绝。"""
        with pytest.raises(ValidationError):
            LIQUIDATION_AGGREGATE(**{**self._sample(), "bogus_field": "boom"})


# ---------- property tests ----------


class TestProperty:
    """Property-based 测试（D09 TC-M）。"""

    _now = datetime(2026, 9, 11, 12, 0, 0)

    def test_greeks_all_finite_random_rejection(self) -> None:
        """GREEKS 6 字段全 isfinite 对随机 float 生成的拒绝率=100%。

        用大量随机浮点数（含 NaN/Inf）注入 GREEKS，全部应被拒绝。
        """
        import random

        random.seed(42)
        rejected_count = 0
        total_trials = 100

        for _ in range(total_trials):
            fields = {
                "instrument_id": "TEST",
                "event_time": self._now,
                "delta": random.uniform(-10, 10),
                "gamma": random.uniform(-10, 10),
                "vega": random.uniform(-10, 10),
                "theta": random.uniform(-10, 10),
                "rho": random.uniform(-10, 10),
                "schema_version": "1.0",
                "source": "test",
                "source_id": "sid",
                "source_timestamp": self._now,
                "ingest_timestamp": self._now,
                "raw_record_id": "r",
            }
            # 随机注入 NaN 或 Inf
            key = random.choice(["delta", "gamma", "vega", "theta", "rho"])
            fields[key] = random.choice([float("nan"), float("inf"), float("-inf")])
            with pytest.raises(ValidationError):
                GREEKS(**fields)
            rejected_count += 1

        assert rejected_count == total_trials, "所有随机样本均被正确拒绝"


# ---------- natural_key 完整性 ----------


class TestNaturalKeyCompleteness:
    """natural_key() 返回值与 D02 身份键声明逐列一致（架构测试）。"""

    _now = datetime(2026, 9, 11, 12, 0, 0)

    def test_option_natural_key_signature(self) -> None:
        """D02 §2: OPTION natural_key = (instrument_id, event_time)。"""
        rec = OPTION(
            market_id="M",
            instrument_id="INST",
            event_time=self._now,
            underlying="BTC",
            expiry=date(2026, 12, 31),
            strike=1.0,
            option_type=OptionType.CALL,
            settlement_asset="USDC",
            mark_price=1.0,
            bid=0.5,
            ask=1.5,
            schema_version="1.0",
            source="s",
            source_id="sid",
            source_timestamp=self._now,
            ingest_timestamp=self._now,
            raw_record_id="r",
        )
        assert rec.natural_key() == ("INST", self._now)

    def test_implied_volatility_natural_key_signature(self) -> None:
        """D02 §2: IMPLIED_VOLATILITY natural_key = (instrument_id, event_time)。"""
        rec = IMPLIED_VOLATILITY(
            instrument_id="INST",
            event_time=self._now,
            iv=0.5,
            mark_iv=0.5,
            bid_iv=0.4,
            ask_iv=0.6,
            implied_forward=1.0,
            data_tier=DataTier.FULL,
            schema_version="1.0",
            source="s",
            source_id="sid",
            source_timestamp=self._now,
            ingest_timestamp=self._now,
            raw_record_id="r",
        )
        assert rec.natural_key() == ("INST", self._now)

    def test_greeks_natural_key_signature(self) -> None:
        """D02 §2: GREEKS natural_key = (instrument_id, event_time)。"""
        rec = GREEKS(
            instrument_id="INST",
            event_time=self._now,
            delta=0.5,
            gamma=0.01,
            vega=0.1,
            theta=-0.05,
            rho=0.02,
            schema_version="1.0",
            source="s",
            source_id="sid",
            source_timestamp=self._now,
            ingest_timestamp=self._now,
            raw_record_id="r",
        )
        assert rec.natural_key() == ("INST", self._now)

    def test_liquidation_event_natural_key_signature(self) -> None:
        """D02 §2: LIQUIDATION_EVENT natural_key = (market_id, order_id)。"""
        rec = LIQUIDATION_EVENT(
            market_id="M",
            event_time=self._now,
            side=Side.BUY,
            price=100.0,
            quantity=10.0,
            order_id="ORD-1",
            schema_version="1.0",
            source="s",
            source_id="sid",
            source_timestamp=self._now,
            ingest_timestamp=self._now,
            raw_record_id="r",
        )
        assert rec.natural_key() == ("M", "ORD-1")

    def test_liquidation_aggregate_natural_key_signature(self) -> None:
        """D02 §2: LIQUIDATION_AGGREGATE natural_key = (market_id, event_time, interval)。"""
        rec = LIQUIDATION_AGGREGATE(
            market_id="M",
            event_time=self._now,
            interval=Interval._1H,
            buy_vol=10.0,
            sell_vol=5.0,
            total_notional=1000.0,
            schema_version="1.0",
            source="s",
            source_id="sid",
            source_timestamp=self._now,
            ingest_timestamp=self._now,
            raw_record_id="r",
        )
        assert rec.natural_key() == ("M", self._now, Interval._1H)
