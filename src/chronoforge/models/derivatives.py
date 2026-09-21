"""Derivatives canonical type schemas（D02 §2 derivatives.py，MODEL-002.2）。

包含 OPTION/IMPLIED_VOLATILITY/GREEKS/LIQUIDATION_EVENT/LIQUIDATION_AGGREGATE 及
OptionType/DataTier 枚举；Interval 枚举定义于 market.py（共用），此处再导出。
全部继承 BaseRecord。
"""

from __future__ import annotations

import math
from datetime import date, datetime
from enum import Enum
from typing import Any

from pydantic import field_validator, model_validator

from chronoforge.models.base import BaseRecord
from chronoforge.models.market import Interval as Interval
from chronoforge.models.market import Side
from chronoforge.models.reference import OptionType


class DataTier(str, Enum):  # noqa: UP042
    """IV 数据层级（D02 §2，OTM/ATM 插值标注）。"""

    OTM = "OTM"
    ATM = "ATM"
    FULL = "FULL"


class OPTION(BaseRecord):
    """期权合约快照（D02 §2）。

    身份键 = (instrument_id, event_time)。
    """

    market_id: str
    instrument_id: str
    event_time: datetime
    underlying: str
    expiry: date
    strike: float
    option_type: OptionType
    settlement_asset: str
    mark_price: float
    bid: float
    ask: float

    @field_validator("strike", "mark_price", "bid", "ask", mode="before")
    @classmethod
    def _check_finite(cls, v: Any) -> Any:
        if v is not None and not math.isfinite(v):
            raise ValueError("must be finite (not NaN/Inf)")
        return v

    @field_validator("strike")
    @classmethod
    def _check_strike_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("strike must be > 0")
        return v

    @field_validator("mark_price", "bid", "ask")
    @classmethod
    def _check_non_negative(cls, v: float) -> float:
        if v < 0:
            raise ValueError("must be >= 0")
        return v

    @model_validator(mode="after")
    def _validate_expiry_not_future(self) -> OPTION:
        if self.expiry < date.today():
            self.__dict__["quality_status"] = "SUSPECT"
            self.__dict__["quality_reason"] = "expiry is in the past"
        return self

    def natural_key(self) -> tuple[str, datetime]:
        return (self.instrument_id, self.event_time)


class IMPLIED_VOLATILITY(BaseRecord):
    """隐含波动率记录（D02 §2）。

    身份键 = (instrument_id, event_time)。
    """

    instrument_id: str
    event_time: datetime
    iv: float
    mark_iv: float | None
    bid_iv: float | None
    ask_iv: float | None
    implied_forward: float | None
    data_tier: DataTier

    @field_validator("iv", "mark_iv", "bid_iv", "ask_iv", "implied_forward", mode="before")
    @classmethod
    def _check_finite(cls, v: Any) -> Any:
        if v is not None and not math.isfinite(v):
            raise ValueError("must be finite (not NaN/Inf)")
        return v

    @field_validator("iv")
    @classmethod
    def _check_iv_range(cls, v: float) -> float:
        if v < 0 or v > 5:
            raise ValueError("iv must be in [0, 5]")
        return v

    @field_validator("mark_iv", "bid_iv", "ask_iv")
    @classmethod
    def _check_non_negative(cls, v: float | None) -> float | None:
        if v is not None and v < 0:
            raise ValueError("must be >= 0")
        return v

    def natural_key(self) -> tuple[str, datetime]:
        return (self.instrument_id, self.event_time)


class GREEKS(BaseRecord):
    """希腊字母风险参数（D02 §2）。

    身份键 = (instrument_id, event_time)。
    """

    instrument_id: str
    event_time: datetime
    delta: float
    gamma: float
    vega: float
    theta: float
    rho: float

    @field_validator("delta", "gamma", "vega", "theta", "rho", mode="before")
    @classmethod
    def _check_finite(cls, v: Any) -> Any:
        if v is not None and not math.isfinite(v):
            raise ValueError("must be finite (not NaN/Inf)")
        return v

    @field_validator("delta")
    @classmethod
    def _check_delta_range(cls, v: float) -> float:
        if v < -1 or v > 1:
            raise ValueError("delta must be in [-1, 1]")
        return v

    @field_validator("gamma")
    @classmethod
    def _check_gamma_non_negative(cls, v: float) -> float:
        if v < 0:
            raise ValueError("gamma must be >= 0")
        return v

    def natural_key(self) -> tuple[str, datetime]:
        return (self.instrument_id, self.event_time)


class LIQUIDATION_EVENT(BaseRecord):
    """爆仓事件记录（D02 §2）。

    身份键 = (market_id, order_id)。
    """

    market_id: str
    event_time: datetime
    side: Side  # BUY=空头爆仓/SELL=多头爆仓
    price: float
    quantity: float
    order_id: str

    @field_validator("price", "quantity", mode="before")
    @classmethod
    def _check_finite(cls, v: Any) -> Any:
        if v is not None and not math.isfinite(v):
            raise ValueError("must be finite (not NaN/Inf)")
        return v

    @field_validator("price", "quantity")
    @classmethod
    def _check_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("must be > 0")
        return v

    @field_validator("order_id")
    @classmethod
    def _check_order_id_non_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("order_id must not be empty")
        return v

    def natural_key(self) -> tuple[str, str]:
        return (self.market_id, self.order_id)


class LIQUIDATION_AGGREGATE(BaseRecord):
    """爆仓聚合统计（D02 §2）。

    身份键 = (market_id, event_time, interval)。
    """

    market_id: str
    event_time: datetime
    interval: Interval
    buy_vol: float
    sell_vol: float
    total_notional: float

    @field_validator("buy_vol", "sell_vol", "total_notional", mode="before")
    @classmethod
    def _check_finite(cls, v: Any) -> Any:
        if v is not None and not math.isfinite(v):
            raise ValueError("must be finite (not NaN/Inf)")
        return v

    @field_validator("buy_vol", "sell_vol", "total_notional")
    @classmethod
    def _check_non_negative(cls, v: float) -> float:
        if v < 0:
            raise ValueError("must be >= 0")
        return v

    def natural_key(self) -> tuple[str, datetime, Interval]:
        return (self.market_id, self.event_time, self.interval)
