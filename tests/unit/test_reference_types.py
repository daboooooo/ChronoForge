"""Reference type tests（D09，MODEL-002.4）。

覆盖 ENTITY/INSTRUMENT 的合法实例化、
边界校验、失败拒绝及 natural_key() 契约。
"""

from datetime import date, datetime

import pytest
from pydantic import ValidationError

from chronoforge.models.reference import ENTITY, INSTRUMENT, EntityType, InstrumentType, OptionType


class TestEntityType:
    """EntityType 枚举合法值遍历（D02 §2）。"""

    def test_all_values(self) -> None:
        """EntityType 全枚举值遍历。"""
        assert list(EntityType) == [
            EntityType.CRYPTO,
            EntityType.EQUITY,
            EntityType.INDEX,
            EntityType.FOREX,
            EntityType.COMMODITY,
            EntityType.BOND,
            EntityType.CRYPTOCURRENCY,
        ]

    def test_invalid_value(self) -> None:
        """EntityType 非法值拒绝。"""
        with pytest.raises(ValueError):
            EntityType("BOGUS")


class TestENTITY:
    """ENTITY 合法/边界/失败用例（D02 §2）。"""

    _now = datetime(2026, 9, 11, 12, 0, 0)

    def _sample(self, **overrides) -> dict:
        base = {
            "source_id": "sec-001",
            "raw_record_id": "sec:test:file.jsonl:1",
            "entity_id": "AAPL",
            "canonical_name": "Apple Inc.",
            "entity_type": EntityType.EQUITY,
            "aliases": ["Apple Computer", "AAPL"],
            "schema_version": "1.0",
            "source": "sec",
            "source_timestamp": self._now,
            "ingest_timestamp": self._now,
        }
        base.update(overrides)
        return base

    def test_entity_valid(self) -> None:
        """ENTITY 合法实例化。"""
        record = ENTITY(**self._sample())
        assert record.entity_id == "AAPL"
        assert record.entity_type == EntityType.EQUITY
        assert record.canonical_name == "Apple Inc."

    def test_entity_different_types(self) -> None:
        """ENTITY 不同 entity_type 合法。"""
        for et in EntityType:
            rec = ENTITY(**self._sample(entity_type=et))
            assert rec.entity_type == et

    def test_entity_empty_aliases_ok(self) -> None:
        """ENTITY aliases=[] 合法。"""
        record = ENTITY(**self._sample(aliases=[]))
        assert record.aliases == []

    def test_entity_natural_key(self) -> None:
        """ENTITY natural_key = (entity_id,)。"""
        rec = ENTITY(**self._sample())
        assert rec.natural_key() == ("AAPL",)


class TestInstrumentType:
    """InstrumentType 枚举合法值遍历（D02 §2）。"""

    def test_all_values(self) -> None:
        """InstrumentType 全枚举值遍历。"""
        assert list(InstrumentType) == [
            InstrumentType.SPOT,
            InstrumentType.PERP,
            InstrumentType.FUTURE,
            InstrumentType.OPTION,
        ]


class TestINSTRUMENT:
    """INSTRUMENT 合法/边界/失败用例（D02 §2）。"""

    _now = datetime(2026, 9, 11, 12, 0, 0)

    def _sample(self, **overrides) -> dict:
        base = {
            "source_id": "cm-eq",
            "raw_record_id": "cm:test:file.jsonl:1",
            "instrument_id": "AAPL-US-EQUITY",
            "entity_id": "AAPL",
            "instrument_type": InstrumentType.SPOT,
            "expiry": None,
            "strike": None,
            "option_type": None,
            "settlement_asset": None,
            "schema_version": "1.0",
            "source": "cm",
            "source_timestamp": self._now,
            "ingest_timestamp": self._now,
        }
        base.update(overrides)
        return base

    def _option_sample(self, **overrides) -> dict:
        base = {
            "source_id": "cm-opt",
            "raw_record_id": "cm:test:file.jsonl:2",
            "instrument_id": "AAPL2401C00150000",
            "entity_id": "AAPL",
            "instrument_type": InstrumentType.OPTION,
            "expiry": date(2026, 1, 17),
            "strike": 150.0,
            "option_type": OptionType.CALL,
            "settlement_asset": "USD",
            "schema_version": "1.0",
            "source": "cm",
            "source_timestamp": self._now,
            "ingest_timestamp": self._now,
        }
        base.update(overrides)
        return base

    def test_instrument_spot_valid(self) -> None:
        """GWT: INSTRUMENT type=SPOT When expiry/strike/option_type 为 None Then 成功。"""
        rec = INSTRUMENT(**self._sample())
        assert rec.instrument_type == InstrumentType.SPOT
        assert rec.expiry is None
        assert rec.strike is None
        assert rec.option_type is None

    def test_instrument_perp_valid(self) -> None:
        """INSTRUMENT type=PERP（无 expiry/strike/option_type）合法。"""
        rec = INSTRUMENT(**self._sample(instrument_type=InstrumentType.PERP))
        assert rec.instrument_type == InstrumentType.PERP
        assert rec.expiry is None
        assert rec.strike is None

    def test_instrument_future_valid(self) -> None:
        """INSTRUMENT type=FUTURE（有 expiry）合法。"""
        rec = INSTRUMENT(**self._sample(
            instrument_type=InstrumentType.FUTURE,
            expiry=date(2026, 12, 18),
        ))
        assert rec.instrument_type == InstrumentType.FUTURE
        assert rec.expiry == date(2026, 12, 18)

    def test_instrument_option_valid(self) -> None:
        """INSTRUMENT type=OPTION（有 expiry/strike/option_type）合法。"""
        rec = INSTRUMENT(**self._option_sample())
        assert rec.instrument_type == InstrumentType.OPTION
        assert rec.expiry == date(2026, 1, 17)
        assert rec.strike == 150.0
        assert rec.option_type == OptionType.CALL

    def test_instrument_option_put_valid(self) -> None:
        """INSTRUMENT type=OPTION PUT 合法。"""
        rec = INSTRUMENT(**self._option_sample(option_type=OptionType.PUT, strike=160.0))
        assert rec.option_type == OptionType.PUT

    def test_instrument_option_strike_none_rejected(self) -> None:
        """GWT: INSTRUMENT type=OPTION When strike=None Then ValidationError。"""
        with pytest.raises(ValidationError):
            INSTRUMENT(**self._option_sample(strike=None))

    def test_instrument_option_strike_zero_rejected(self) -> None:
        """INSTRUMENT option strike=0 → ValidationError。"""
        with pytest.raises(ValidationError):
            INSTRUMENT(**self._option_sample(strike=0.0))

    def test_instrument_option_strike_negative_rejected(self) -> None:
        """INSTRUMENT option strike<0 → ValidationError。"""
        with pytest.raises(ValidationError):
            INSTRUMENT(**self._option_sample(strike=-10.0))

    def test_instrument_option_strike_nan_rejected(self) -> None:
        """INSTRUMENT option strike=NaN → ValidationError。"""
        with pytest.raises(ValidationError):
            INSTRUMENT(**self._option_sample(strike=float("nan")))  # type: ignore[arg-type]

    def test_instrument_spot_with_option_type_rejected(self) -> None:
        """SPOT type 有 option_type → ValidationError。"""
        with pytest.raises(ValidationError):
            INSTRUMENT(**self._sample(
                instrument_type=InstrumentType.SPOT,
                option_type=OptionType.CALL,
                strike=100.0,
            ))

    def test_instrument_spot_with_strike_rejected(self) -> None:
        """SPOT type 有 strike 但无 option_type → ValidationError。"""
        with pytest.raises(ValidationError):
            INSTRUMENT(**self._sample(
                instrument_type=InstrumentType.SPOT,
                strike=100.0,
            ))

    def test_instrument_entity_id_uppercased(self) -> None:
        """INSTRUMENT entity_id 存储原值（由上游标准化为大写）。"""
        rec = INSTRUMENT(**self._sample(entity_id="aapl"))
        assert rec.entity_id == "aapl"

    def test_instrument_natural_key(self) -> None:
        """INSTRUMENT natural_key = (instrument_id,)。"""
        rec = INSTRUMENT(**self._sample())
        assert rec.natural_key() == ("AAPL-US-EQUITY",)
