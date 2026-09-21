"""Prediction canonical type schemas（D02 §2 prediction.py，MODEL-002.3）。

包含 PREDICTION_MARKET/PREDICTION_PRICE。全部继承 BaseRecord。
"""

from __future__ import annotations

import math
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import field_validator

from chronoforge.models.base import BaseRecord


class PredictionStatus(str, Enum):  # noqa: UP042
    """预测市场状态（D02 §2）。"""

    OPEN = "OPEN"
    CLOSED = "CLOSED"
    RESOLVED = "RESOLVED"


class PREDICTION_MARKET(BaseRecord):
    """预测市场（D02 §2）。

    身份键 = (source_id, event_id)。
    """

    source_id: str
    event_id: str
    question: str
    outcomes: list[str]
    close_time: datetime
    status: PredictionStatus

    @field_validator("outcomes")
    @classmethod
    def _check_outcomes_count(cls, v: list[str]) -> list[str]:
        if len(v) < 2:
            raise ValueError("outcomes must have at least 2 elements")
        return v

    def natural_key(self) -> tuple[str, str]:
        return (self.source_id, self.event_id)


class PREDICTION_PRICE(BaseRecord):
    """预测市场价格（D02 §2）。

    身份键 = (market_id, outcome_id, event_time)。
    implied_probability 默认值 = price。
    """

    market_id: str
    outcome_id: str
    event_time: datetime
    price: float
    implied_probability: float
    volume: float
    liquidity: float

    @field_validator("price", mode="before")
    @classmethod
    def _check_price_finite(cls, v: Any) -> Any:
        if v is not None and not math.isfinite(v):
            raise ValueError("price must be finite (not NaN/Inf)")
        return v

    @field_validator("price")
    @classmethod
    def _check_price_range(cls, v: float) -> float:
        if v < 0 or v > 1:
            raise ValueError("price must be in [0, 1]")
        return v

    @field_validator("implied_probability", "volume", "liquidity", mode="before")
    @classmethod
    def _check_non_negative(cls, v: Any) -> Any:
        if v is not None and v < 0:
            raise ValueError("must be >= 0")
        return v

    def natural_key(self) -> tuple[str, str, datetime]:
        return (self.market_id, self.outcome_id, self.event_time)
