"""Market canonical type schemas（D02 §2 market.py，MODEL-002.1）。

包含 Interval/Side 枚举及 OHLCV/TRADE/TICKER/FUNDING/OPEN_INTEREST/ORDERBOOK。
全部继承 BaseRecord，零业务依赖（仅 pydantic + stdlib + models.base）。
"""

from __future__ import annotations

import math
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import field_validator, model_validator

from chronoforge.models.base import BaseRecord


class Side(str, Enum):  # noqa: UP042
    """交易方向（D02 §2 TRADE/LIQUIDATION_EVENT 使用）。"""

    BUY = "BUY"
    SELL = "SELL"


class Interval(str, Enum):  # noqa: UP042
    """时间区间粒度（D02 §2，OHLCV/LIQUIDATION_AGGREGATE 使用）。

    D02 §2 将本枚举登记于 derivatives.py 模块，供 OHLCV（market.py）与
    LIQUIDATION_AGGREGATE 共用；因 derivatives.py 反向依赖 market.py 的 Side，
    为避免循环导入，枚举定义置于 market.py，derivatives.py 再导出保持
    ``from chronoforge.models.derivatives import Interval`` 兼容。
    """

    _1M = "1m"
    _5M = "5m"
    _15M = "15m"
    _30M = "30m"
    _1H = "1h"
    _2H = "2h"
    _4H = "4h"
    _6H = "6h"
    _8H = "8h"
    _12H = "12h"
    _1D = "1d"
    _1W = "1w"


class OHLCV(BaseRecord):
    """OHLCV K 线记录（D02 §2）。

    身份键 = (market_id, event_time, interval)（D02 §2 L43）；
    event_time 为区间起点。
    """

    market_id: str
    event_time: datetime
    interval: Interval
    # 注意：open/close 排在 high/low 之前，确保 model_validator 可访问这些字段
    open: float
    close: float
    high: float
    low: float
    volume: float

    @field_validator("open", "high", "low", "close", "volume", mode="before")
    @classmethod
    def _check_finite(cls, v: Any) -> Any:
        if v is not None and not math.isfinite(v):
            raise ValueError("must be finite (not NaN/Inf)")
        return v

    @field_validator("open", "high", "low", "close")
    @classmethod
    def _check_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("must be > 0")
        return v

    @field_validator("volume")
    @classmethod
    def _check_non_negative(cls, v: float) -> float:
        # volume=0 合法：交易所停机维护/无成交时段 K 线（价格冻结、量为零）
        if v < 0:
            raise ValueError("must be >= 0")
        return v

    @model_validator(mode="after")
    def _validate_ohlcv_constraints(self) -> OHLCV:
        if self.high < max(self.open, self.close):
            raise ValueError("high must be >= max(open, close)")
        if self.low > min(self.open, self.close):
            raise ValueError("low must be <= min(open, close)")
        return self

    def natural_key(self) -> tuple[str, datetime, Interval]:
        return (self.market_id, self.event_time, self.interval)


class TRADE(BaseRecord):
    """成交记录（D02 §2）。"""

    market_id: str
    event_time: datetime
    price: float
    quantity: float
    side: Side
    trade_id: str

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

    def natural_key(self) -> tuple[str, str]:
        return (self.market_id, self.trade_id)


class TICKER(BaseRecord):
    """Ticker 24h 行情快照（D02 §2）。"""

    market_id: str
    event_time: datetime
    last_price: float
    bid: float
    ask: float
    volume_24h: float
    quote_volume_24h: float

    @field_validator("last_price", "bid", "ask", "volume_24h", "quote_volume_24h", mode="before")
    @classmethod
    def _check_finite(cls, v: Any) -> Any:
        if v is not None and not math.isfinite(v):
            raise ValueError("must be finite (not NaN/Inf)")
        return v

    @field_validator("last_price")
    @classmethod
    def _check_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("must be > 0")
        return v

    @field_validator("bid", "ask")
    @classmethod
    def _check_non_negative(cls, v: float) -> float:
        if v < 0:
            raise ValueError("must be >= 0")
        return v

    def natural_key(self) -> tuple[str, datetime]:
        return (self.market_id, self.event_time)


class FUNDING(BaseRecord):
    """资金费率记录（D02 §2）。

    event_time = 结算时间，next_funding_time > event_time。
    """

    market_id: str
    event_time: datetime
    funding_rate: float
    next_funding_time: datetime

    @field_validator("funding_rate", mode="before")
    @classmethod
    def _check_finite(cls, v: Any) -> Any:
        if v is not None and not math.isfinite(v):
            raise ValueError("must be finite (not NaN/Inf)")
        return v

    @field_validator("next_funding_time")
    @classmethod
    def _next_after_event(cls, v: datetime, info: Any) -> datetime:
        data = info.data if hasattr(info, "data") else getattr(info, "data", {})
        if data and "event_time" in data:
            if v <= data["event_time"]:
                raise ValueError("next_funding_time must be > event_time")
        return v

    def natural_key(self) -> tuple[str, datetime]:
        return (self.market_id, self.event_time)


class OPEN_INTEREST(BaseRecord):
    """持仓量快照（D02 §2）。"""

    market_id: str
    event_time: datetime
    open_interest: float
    unit: str

    @field_validator("open_interest", mode="before")
    @classmethod
    def _check_finite(cls, v: Any) -> Any:
        if v is not None and not math.isfinite(v):
            raise ValueError("must be finite (not NaN/Inf)")
        return v

    @field_validator("open_interest")
    @classmethod
    def _check_non_negative(cls, v: float) -> float:
        if v < 0:
            raise ValueError("must be >= 0")
        return v

    def natural_key(self) -> tuple[str, datetime]:
        return (self.market_id, self.event_time)


class ORDERBOOK(BaseRecord):
    """订单簿快照（D02 §2，P0 仅快照，增量 P1）。

    bids/asks: list[[price, qty]]，N ≤ 1000。
    """

    market_id: str
    event_time: datetime
    transaction_time: datetime
    bids: list[list[float]]
    asks: list[list[float]]

    @field_validator("bids", "asks")
    @classmethod
    def _validate_depth(cls, v: list[list[float]]) -> list[list[float]]:
        if len(v) > 1000:
            raise ValueError("depth must be <= 1000")
        return v

    @field_validator("bids", "asks")
    @classmethod
    def _validate_entries(cls, v: list[list[float]]) -> list[list[float]]:
        for entry in v:
            if len(entry) != 2:
                raise ValueError("each entry must have exactly 2 elements [price, qty]")
            price, qty = entry[0], entry[1]
            if price <= 0:
                raise ValueError("price must be > 0")
            if qty < 0:
                raise ValueError("qty must be >= 0")
        return v

    def natural_key(self) -> tuple[str, datetime, datetime]:
        return (self.market_id, self.event_time, self.transaction_time)
