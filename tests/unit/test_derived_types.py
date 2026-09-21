"""Derived type tests（D09，MODEL-002.4）。

覆盖 DERIVED/FEATURE 的合法实例化、
边界校验、失败拒绝及 natural_key() 契约。
"""

from datetime import datetime

import pytest
from pydantic import ValidationError

from chronoforge.models.derived import DERIVED, FEATURE


class TestDERIVED:
    """DERIVED 合法/边界/失败用例（D02 §2）。"""

    _now = datetime(2026, 9, 11, 12, 0, 0)

    def _sample(self, **overrides) -> dict:
        base = {
            "source_id": "deriv-stress",
            "raw_record_id": "deriv:test:file.jsonl:1",
            "name": "BTC_MARKET_STRESS",
            "computed_at": self._now,
            "dependencies": [("ohlcv", "v1"), ("funding", "v1")],
            "value": 0.75,
            "params": {"window_hours": 24, "threshold": 0.5},
            "source_type": "stress_index",
            "schema_version": "1.0",
            "source": "deriv",
            "source_timestamp": self._now,
            "ingest_timestamp": self._now,
        }
        base.update(overrides)
        return base

    def test_derived_valid(self) -> None:
        """DERIVED 合法实例化。"""
        rec = DERIVED(**self._sample())
        assert rec.name == "BTC_MARKET_STRESS"
        assert rec.value == 0.75
        assert len(rec.dependencies) == 2

    def test_derived_single_dependency_ok(self) -> None:
        """DERIVED 单个 dependency 合法。"""
        rec = DERIVED(**self._sample(dependencies=[("ohlcv", "v1")]))
        assert len(rec.dependencies) == 1

    def test_derived_empty_dependencies_rejected(self) -> None:
        """GWT: DERIVED dependencies=[] → ValidationError。"""
        with pytest.raises(ValidationError):
            DERIVED(**self._sample(dependencies=[]))

    def test_derived_value_nan_rejected(self) -> None:
        """DERIVED value=NaN → ValidationError。"""
        with pytest.raises(ValidationError):
            DERIVED(**self._sample(value=float("nan")))  # type: ignore[arg-type]

    def test_derived_value_inf_rejected(self) -> None:
        """DERIVED value=Inf → ValidationError。"""
        with pytest.raises(ValidationError):
            DERIVED(**self._sample(value=float("inf")))  # type: ignore[arg-type]

    def test_derived_value_finite_ok(self) -> None:
        """DERIVED 有限 value 合法。"""
        for val in [0.0, 1.0, -0.5, 1e10, -1e10]:
            rec = DERIVED(**self._sample(value=val))
            assert rec.value == val

    def test_derived_natural_key(self) -> None:
        """DERIVED natural_key = (name, computed_at)。"""
        rec = DERIVED(**self._sample())
        assert rec.natural_key() == ("BTC_MARKET_STRESS", self._now)


class TestFEATURE:
    """FEATURE 合法/边界/失败用例（D02 §2）。"""

    _now = datetime(2026, 9, 11, 12, 0, 0)

    def _sample(self, **overrides) -> dict:
        base = {
            "source_id": "fe-engine",
            "raw_record_id": "fe:test:file.jsonl:1",
            "name": "btc_volatility_ratio",
            "computed_at": self._now,
            "dependencies": [("ohlcv", "v1")],
            "value": 1.23,
            "params": {"lookback_days": 30},
            "feature_engine_version": "v2.1.0",
            "schema_version": "1.0",
            "source": "fe",
            "source_timestamp": self._now,
            "ingest_timestamp": self._now,
        }
        base.update(overrides)
        return base

    def test_feature_valid(self) -> None:
        """FEATURE 合法实例化。"""
        rec = FEATURE(**self._sample())
        assert rec.name == "btc_volatility_ratio"
        assert rec.value == 1.23
        assert rec.feature_engine_version == "v2.1.0"

    def test_feature_empty_dependencies_rejected(self) -> None:
        """GWT: FEATURE dependencies=[] → ValidationError。"""
        with pytest.raises(ValidationError):
            FEATURE(**self._sample(dependencies=[]))

    def test_feature_value_nan_rejected(self) -> None:
        """FEATURE value=NaN → ValidationError。"""
        with pytest.raises(ValidationError):
            FEATURE(**self._sample(value=float("nan")))  # type: ignore[arg-type]

    def test_feature_value_inf_rejected(self) -> None:
        """FEATURE value=Inf → ValidationError。"""
        with pytest.raises(ValidationError):
            FEATURE(**self._sample(value=float("inf")))  # type: ignore[arg-type]

    def test_feature_empty_engine_version_rejected(self) -> None:
        """FEATURE feature_engine_version="" → ValidationError。"""
        with pytest.raises(ValidationError):
            FEATURE(**self._sample(feature_engine_version=""))

    def test_feature_natural_key(self) -> None:
        """FEATURE natural_key = (name, computed_at)。"""
        rec = FEATURE(**self._sample())
        assert rec.natural_key() == ("btc_volatility_ratio", self._now)
