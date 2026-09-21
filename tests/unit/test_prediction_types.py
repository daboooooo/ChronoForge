"""Prediction type tests（D09，MODEL-002.3）。

覆盖 PREDICTION_MARKET/PREDICTION_PRICE 的合法实例化、
边界校验、失败拒绝及 natural_key() 契约。
"""

from datetime import datetime

import pytest
from pydantic import ValidationError

from chronoforge.models.prediction import (
    PREDICTION_MARKET,
    PREDICTION_PRICE,
    PredictionStatus,
)


class TestPredictionStatus:
    """PredictionStatus 枚举合法值遍历（D02 §2）。"""

    def test_all_values(self) -> None:
        """PredictionStatus 全枚举值遍历。"""
        assert list(PredictionStatus) == [
            PredictionStatus.OPEN,
            PredictionStatus.CLOSED,
            PredictionStatus.RESOLVED,
        ]

    def test_invalid_value(self) -> None:
        """PredictionStatus 非法值拒绝。"""
        with pytest.raises(ValueError):
            PredictionStatus("BOGUS")


class TestPREDICTION_MARKET:
    """PREDICTION_MARKET 合法/边界/失败用例（D02 §2）。"""

    _now = datetime(2026, 9, 11, 12, 0, 0)

    def _sample(self, **overrides) -> dict:
        base = {
            "source_id": "polymarket-biden-president",
            "raw_record_id": "poly:test:file.jsonl:1",
            "event_id": "evt-001",
            "question": "Will Biden win the 2024 election?",
            "outcomes": ["Yes", "No"],
            "close_time": datetime(2026, 11, 5, 12, 0, 0),
            "status": PredictionStatus.OPEN,
            "schema_version": "1.0",
            "source": "polymarket",
            "source_timestamp": self._now,
            "ingest_timestamp": self._now,
        }
        base.update(overrides)
        return base

    def test_prediction_market_valid(self) -> None:
        """PREDICTION_MARKET 合法实例化。"""
        record = PREDICTION_MARKET(**self._sample())
        assert record.question == "Will Biden win the 2024 election?"
        assert record.status == PredictionStatus.OPEN

    def test_prediction_market_three_outcomes_ok(self) -> None:
        """PREDICTION_MARKET 3 outcomes 合法。"""
        record = PREDICTION_MARKET(**self._sample(
            question="Who wins?",
            outcomes=["A", "B", "C"],
        ))
        assert len(record.outcomes) == 3

    def test_prediction_market_two_outcomes_min(self) -> None:
        """PREDICTION_MARKET 2 outcomes（最小值）合法。"""
        record = PREDICTION_MARKET(**self._sample(
            question="X or Y?",
            outcomes=["X", "Y"],
        ))
        assert len(record.outcomes) == 2

    def test_prediction_market_one_outcome_rejected(self) -> None:
        """PREDICTION_MARKET 1 outcome → ValidationError。"""
        with pytest.raises(ValidationError):
            PREDICTION_MARKET(**self._sample(
                question="Only one?",
                outcomes=["Only"],
            ))

    def test_prediction_market_closed_status_ok(self) -> None:
        """PREDICTION_MARKET status=CLOSED 合法。"""
        record = PREDICTION_MARKET(**self._sample(status=PredictionStatus.CLOSED))
        assert record.status == PredictionStatus.CLOSED

    def test_prediction_market_resolved_status_ok(self) -> None:
        """PREDICTION_MARKET status=RESOLVED 合法。"""
        record = PREDICTION_MARKET(**self._sample(status=PredictionStatus.RESOLVED))
        assert record.status == PredictionStatus.RESOLVED

    def test_prediction_market_invalid_status_rejected(self) -> None:
        """PREDICTION_MARKET invalid status → ValidationError。"""
        with pytest.raises(ValueError):
            PREDICTION_MARKET(**self._sample(status="BOGUS"))  # type: ignore[arg-type]

    def test_prediction_market_natural_key(self) -> None:
        """PREDICTION_MARKET natural_key = (source_id, event_id)。"""
        rec = PREDICTION_MARKET(**self._sample())
        assert rec.natural_key() == ("polymarket-biden-president", "evt-001")


class TestPREDICTION_PRICE:
    """PREDICTION_PRICE 合法/边界/失败用例（D02 §2）。"""

    _now = datetime(2026, 9, 11, 12, 0, 0)

    def _sample(self, **overrides) -> dict:
        base = {
            "source_id": "poly-price",
            "raw_record_id": "poly:test:file.jsonl:1",
            "market_id": "polymarket-biden-president",
            "outcome_id": "Yes",
            "event_time": self._now,
            "price": 0.5,
            "implied_probability": 0.5,
            "volume": 100000.0,
            "liquidity": 50000.0,
            "schema_version": "1.0",
            "source": "polymarket",
            "source_timestamp": self._now,
            "ingest_timestamp": self._now,
        }
        base.update(overrides)
        return base

    def test_prediction_price_valid(self) -> None:
        """PREDICTION_PRICE 合法实例化。"""
        record = PREDICTION_PRICE(**self._sample())
        assert record.price == 0.5
        assert record.implied_probability == 0.5

    def test_prediction_price_zero_ok(self) -> None:
        """PREDICTION_PRICE price=0 合法。"""
        record = PREDICTION_PRICE(**self._sample(price=0.0))
        assert record.price == 0.0

    def test_prediction_price_one_ok(self) -> None:
        """PREDICTION_PRICE price=1 合法（边界）。"""
        record = PREDICTION_PRICE(**self._sample(price=1.0))
        assert record.price == 1.0

    def test_prediction_price_0_5_legal(self) -> None:
        """GWT: PREDICTION_PRICE price=0.5 → implied_probability=0.5。"""
        rec = PREDICTION_PRICE(**self._sample(price=0.5, implied_probability=0.5))
        assert rec.price == 0.5
        assert rec.implied_probability == 0.5

    def test_prediction_price_above_one_rejected(self) -> None:
        """GWT: PREDICTION_PRICE price=1.1 → ValidationError。"""
        with pytest.raises(ValidationError):
            PREDICTION_PRICE(**self._sample(price=1.1))

    def test_prediction_price_negative_rejected(self) -> None:
        """PREDICTION_PRICE price=-0.1 → ValidationError。"""
        with pytest.raises(ValidationError):
            PREDICTION_PRICE(**self._sample(price=-0.1))

    def test_prediction_price_nan_rejected(self) -> None:
        """PREDICTION_PRICE price=NaN → ValidationError。"""
        with pytest.raises(ValidationError):
            PREDICTION_PRICE(**self._sample(price=float("nan")))

    def test_prediction_price_inf_rejected(self) -> None:
        """PREDICTION_PRICE price=Inf → ValidationError。"""
        with pytest.raises(ValidationError):
            PREDICTION_PRICE(**self._sample(price=float("inf")))

    def test_prediction_price_negative_implied_probability_rejected(self) -> None:
        """PREDICTION_PRICE implied_probability < 0 → ValidationError。"""
        with pytest.raises(ValidationError):
            PREDICTION_PRICE(**self._sample(implied_probability=-0.1))

    def test_prediction_price_natural_key(self) -> None:
        """PREDICTION_PRICE natural_key = (market_id, outcome_id, event_time)。"""
        rec = PREDICTION_PRICE(**self._sample())
        assert rec.natural_key() == ("polymarket-biden-president", "Yes", self._now)
