"""Macro type tests（D09，MODEL-002.3）。

覆盖 NUMBER/FLOW/MACRO_EVENT 的合法实例化、
边界校验、失败拒绝及 natural_key() 契约。
"""

from datetime import datetime

import pytest
from pydantic import ValidationError

from chronoforge.models.macro import FLOW, MACRO_EVENT, NUMBER


class TestNUMBER:
    """NUMBER 合法/边界/失败用例（D02 §2）。"""

    _now = datetime(2026, 9, 11, 12, 0, 0)

    def _sample(self, **overrides) -> dict:
        base = {
            "source_id": "GDP",
            "observation_time": self._now,
            "release_time": self._now,
            "revision_time": self._now,
            "value": 21000.0,
            "units": "USD_BN",
            "seasonal_adjustment": "SA",
            "vintage_date": "2026-09-11",
            "schema_version": "1.0",
            "source": "fred",
            "source_timestamp": self._now,
            "ingest_timestamp": self._now,
            "raw_record_id": "fred:test:file.jsonl:1",
        }
        base.update(overrides)
        return base

    def test_number_valid(self) -> None:
        """NUMBER 合法实例化。"""
        record = NUMBER(**self._sample())
        assert record.source_id == "GDP"
        assert record.value == 21000.0

    def test_number_value_none_legal(self) -> None:
        """GWT: NUMBER value=None → 成功（缺失值合法）。"""
        record = NUMBER(**self._sample(value=None))
        assert record.value is None

    def test_number_value_nan_rejected(self) -> None:
        """NUMBER value=NaN → ValidationError。"""
        with pytest.raises(ValidationError):
            NUMBER(**self._sample(value=float("nan")))

    def test_number_value_inf_rejected(self) -> None:
        """NUMBER value=Inf → ValidationError。"""
        with pytest.raises(ValidationError):
            NUMBER(**self._sample(value=float("inf")))

    def test_number_release_before_observation_rejected(self) -> None:
        """NUMBER release_time < observation_time → ValidationError。"""
        with pytest.raises(ValidationError):
            NUMBER(**self._sample(
                observation_time=datetime(2026, 9, 11, 12, 0, 0),
                release_time=datetime(2026, 9, 11, 10, 0, 0),
            ))

    def test_number_revision_before_observation_rejected(self) -> None:
        """NUMBER revision_time < observation_time → ValidationError。"""
        with pytest.raises(ValidationError):
            NUMBER(**self._sample(
                observation_time=datetime(2026, 9, 11, 12, 0, 0),
                revision_time=datetime(2026, 9, 11, 10, 0, 0),
            ))

    def test_number_natural_key(self) -> None:
        """NUMBER natural_key = (source_id, observation_time, revision_time)。"""
        rec = NUMBER(**self._sample())
        assert rec.natural_key() == ("GDP", self._now, self._now)


class TestFLOW:
    """FLOW 合法/边界/失败用例（D02 §2）。"""

    _now = datetime(2026, 9, 11, 12, 0, 0)

    def _sample(self, **overrides) -> dict:
        base = {
            "source_id": "GDP",
            "observation_time": self._now,
            "release_time": self._now,
            "revision_time": self._now,
            "value": 21000.0,
            "units": "USD_BN",
            "seasonal_adjustment": "SA",
            "vintage_date": "2026-09-11",
            "period_start": datetime(2026, 7, 1, 0, 0, 0),
            "period_end": datetime(2026, 9, 11, 0, 0, 0),
            "schema_version": "1.0",
            "source": "fred",
            "source_timestamp": self._now,
            "ingest_timestamp": self._now,
            "raw_record_id": "fred:test:file.jsonl:1",
        }
        base.update(overrides)
        return base

    def test_flow_valid(self) -> None:
        """FLOW 合法实例化。"""
        record = FLOW(**self._sample())
        assert record.value == 21000.0

    def test_flow_value_none_legal(self) -> None:
        """FLOW value=None → 合法（缺失值）。"""
        record = FLOW(**self._sample(value=None))
        assert record.value is None

    def test_flow_period_end_after_start_ok(self) -> None:
        """FLOW period_end > period_start → 合法。"""
        record = FLOW(**self._sample())
        assert record.period_end > record.period_start

    def test_flow_period_end_eq_start_rejected(self) -> None:
        """FLOW period_end == period_start → ValidationError。"""
        with pytest.raises(ValidationError):
            FLOW(**self._sample(
                period_start=self._now,
                period_end=self._now,
            ))

    def test_flow_period_end_before_start_rejected(self) -> None:
        """GWT: FLOW period_end < period_start → ValidationError。"""
        with pytest.raises(ValidationError):
            FLOW(**self._sample(
                period_start=datetime(2026, 9, 11, 12, 0, 0),
                period_end=datetime(2026, 9, 11, 10, 0, 0),
            ))

    def test_flow_natural_key(self) -> None:
        """FLOW natural_key = (source_id, observation_time, revision_time)。"""
        rec = FLOW(**self._sample())
        assert rec.natural_key() == ("GDP", self._now, self._now)


class TestMACRO_EVENT:
    """MACRO_EVENT 合法/边界/失败用例（D02 §2）。"""

    _now = datetime(2026, 9, 11, 12, 0, 0)

    def _sample(self, **overrides) -> dict:
        base = {
            "source_id": "fred-cpi",
            "event_ref": "CPI",
            "scheduled_time": self._now,
            "actual_time": self._now,
            "actual": "3.2%",
            "forecast": "3.1%",
            "previous": "3.0%",
            "schema_version": "1.0",
            "source": "fred",
            "source_timestamp": self._now,
            "ingest_timestamp": self._now,
            "raw_record_id": "fred:test:file.jsonl:1",
        }
        base.update(overrides)
        return base

    def test_macro_event_valid(self) -> None:
        """MACRO_EVENT 合法实例化。"""
        record = MACRO_EVENT(**self._sample())
        assert record.event_ref == "CPI"
        assert record.actual == "3.2%"

    def test_macro_event_no_actual_time_ok(self) -> None:
        """MACRO_EVENT actual_time=None（未公布）合法。"""
        record = MACRO_EVENT(**self._sample(actual_time=None))
        assert record.actual_time is None

    def test_macro_event_actual_before_scheduled_rejected(self) -> None:
        """MACRO_EVENT actual_time < scheduled_time → ValidationError。"""
        with pytest.raises(ValidationError):
            MACRO_EVENT(**self._sample(
                scheduled_time=datetime(2026, 9, 11, 12, 0, 0),
                actual_time=datetime(2026, 9, 11, 10, 0, 0),
            ))

    def test_macro_event_actual_eq_scheduled_ok(self) -> None:
        """MACRO_EVENT actual_time == scheduled_time（合法）。"""
        record = MACRO_EVENT(**self._sample())
        assert record.actual_time == self._now

    def test_macro_event_natural_key(self) -> None:
        """MACRO_EVENT natural_key = (event_ref, scheduled_time)。"""
        rec = MACRO_EVENT(**self._sample())
        assert rec.natural_key() == ("CPI", self._now)
