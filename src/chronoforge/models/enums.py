"""Canonical 枚举定义（D02 §1，MODEL-001）。

QualityStatus 与 CanonicalType 为冻结数据契约（D10 §0.2），成员与取值不得偏离 D02。
类型特定枚举（Interval/Side/OptionType 等）属 MODEL-002，不在本模块。
"""

from enum import Enum


# noqa 理由：D02 §1 代码块逐行实现，(str, Enum) 为冻结契约，不改 StrEnum
class QualityStatus(str, Enum):  # noqa: UP042
    """记录质量状态（架构 05 §4）。"""

    VALID = "VALID"
    SUSPECT = "SUSPECT"
    INVALID = "INVALID"


class CanonicalType(str, Enum):  # noqa: UP042
    """全部 Canonical Type（D02 §1，共 27 个，str 值与成员名相同）。"""

    TICKER = "TICKER"
    TRADE = "TRADE"
    OHLCV = "OHLCV"
    ORDERBOOK = "ORDERBOOK"
    FUNDING = "FUNDING"
    OPEN_INTEREST = "OPEN_INTEREST"
    OPTION = "OPTION"
    IMPLIED_VOLATILITY = "IMPLIED_VOLATILITY"
    GREEKS = "GREEKS"
    LIQUIDATION_EVENT = "LIQUIDATION_EVENT"
    LIQUIDATION_AGGREGATE = "LIQUIDATION_AGGREGATE"
    NUMBER = "NUMBER"
    FLOW = "FLOW"
    MACRO_EVENT = "MACRO_EVENT"
    FUNDAMENTAL = "FUNDAMENTAL"
    FILING = "FILING"
    DOCUMENT = "DOCUMENT"
    POSITION = "POSITION"
    POSITION_AGGREGATE = "POSITION_AGGREGATE"
    PREDICTION_MARKET = "PREDICTION_MARKET"
    PREDICTION_PRICE = "PREDICTION_PRICE"
    TEXT_MESSAGE = "TEXT_MESSAGE"
    TEXT_EVENT = "TEXT_EVENT"
    ENTITY = "ENTITY"
    INSTRUMENT = "INSTRUMENT"
    DERIVED = "DERIVED"
    FEATURE = "FEATURE"
