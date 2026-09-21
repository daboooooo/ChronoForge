"""Reference canonical type schemas（D02 §2 reference.py，MODEL-002.4）。

包含 ENTITY/INSTRUMENT 及 EntityType/InstrumentType/OptionType 枚举，
以及 parse_binance、parse_deribit、parse_yahoo 解析函数和
EntityResolver、InstrumentResolver 解析器类。
"""

from __future__ import annotations

from datetime import date
from enum import Enum
from typing import Any

from pydantic import field_validator, model_validator

from chronoforge.exceptions import ProviderError
from chronoforge.models.base import BaseRecord


class EntityType(str, Enum):  # noqa: UP042
    """实体类型（D02 §2）。"""

    CRYPTO = "CRYPTO"
    EQUITY = "EQUITY"
    INDEX = "INDEX"
    FOREX = "FOREX"
    COMMODITY = "COMMODITY"
    BOND = "BOND"
    CRYPTOCURRENCY = "CRYPTOCURRENCY"


class InstrumentType(str, Enum):  # noqa: UP042
    """合约类型（D02 §2）。"""

    SPOT = "SPOT"
    PERP = "PERP"
    FUTURE = "FUTURE"
    OPTION = "OPTION"


class OptionType(str, Enum):  # noqa: UP042
    """期权类型（D02 §2）。"""

    CALL = "CALL"
    PUT = "PUT"


class ENTITY(BaseRecord):
    """实体参考数据（D02 §2）。

    身份键 = (entity_id,)。
    """

    entity_id: str
    canonical_name: str
    entity_type: EntityType
    aliases: list[str]

    def natural_key(self) -> tuple[str]:
        return (self.entity_id,)


class INSTRUMENT(BaseRecord):
    """合约参考数据（D02 §2）。

    身份键 = (instrument_id,)。
    option_type 存在时 strike 必须 > 0。
    """

    instrument_id: str
    entity_id: str
    instrument_type: InstrumentType
    expiry: date | None
    strike: float | None
    option_type: OptionType | None
    settlement_asset: str | None

    @field_validator("strike", mode="before")
    @classmethod
    def _check_strike_finite(cls, v: Any) -> Any:
        if v is not None and not isinstance(v, (int, float)):
            raise ValueError("strike must be a number")
        if isinstance(v, (int, float)) and not (__import__("math").isfinite(v)):
            raise ValueError("strike must be finite (not NaN/Inf)")
        return v

    @field_validator("strike")
    @classmethod
    def _check_strike_positive_for_option(cls, v: Any, info: Any) -> Any:
        if v is not None and v <= 0:
            raise ValueError("strike must be > 0 when set")
        return v

    @field_validator("expiry")
    @classmethod
    def _check_expiry_allowed(cls, v: Any, info: Any) -> Any:
        data = info.data if hasattr(info, "data") else getattr(info, "data", {})
        it = data.get("instrument_type") if isinstance(data, dict) else None
        if v is not None and it not in (InstrumentType.OPTION, InstrumentType.FUTURE):
            raise ValueError("expiry only valid for OPTION/FUTURE type instruments")
        return v

    @model_validator(mode="after")
    def _validate_instrument_constraints(self) -> INSTRUMENT:
        is_option = self.instrument_type == InstrumentType.OPTION
        is_future = self.instrument_type == InstrumentType.FUTURE
        if not is_option:
            if self.option_type is not None:
                raise ValueError("option_type only valid for OPTION type instruments")
            if self.strike is not None:
                raise ValueError("strike only valid for OPTION type instruments")
        if not (is_option or is_future):
            if self.expiry is not None:
                raise ValueError("expiry only valid for OPTION/FUTURE type instruments")
        if is_option and self.strike is None:
            raise ValueError("strike is required for OPTION type instruments")
        return self

    def natural_key(self) -> tuple[str]:
        return (self.instrument_id,)


# ————————————————————————————————————————————————————————————————
# Binance symbol 解析（D02 §3 表，MODEL-003.1）

_QUOTE_ASSETS = ("USDT", "BTC", "ETH", "BUSD")


def parse_binance(symbol: str, market_type: str) -> tuple[str, str, str]:
    """将 Binance symbol 解析为 (entity_id, instrument_id, market_id)。

    Args:
        symbol: Binance 交易对，如 "BTCUSDT"、"ETHUSDT"、"BTC-USDT"。
        market_type: SPOT / USDT-FUT / COIN-FUT。

    Returns:
        (entity_id, instrument_id, market_id) 三元组。

    Raises:
        ProviderError: 无法识别 symbol 格式或 market_type。
    """
    if not symbol or len(symbol) < 4:
        raise ProviderError(f"无法解析 Binance symbol: {symbol}")

    # Strip trailing hyphen (e.g. "BTC-" → "BTC")
    clean = symbol.rstrip("-")
    if not clean:
        raise ProviderError(f"无法解析 Binance symbol: {symbol}")

    # Try to strip a known quote asset from the right
    entity: str | None = None
    for qa in _QUOTE_ASSETS:
        if clean.endswith(qa):
            idx = clean.rfind(qa)
            entity = clean[:idx].rstrip("-")
            break

    if entity is None:
        raise ProviderError(f"无法解析 Binance symbol: {symbol}")

    if len(entity) < 2:
        raise ProviderError(f"无法解析 Binance symbol: {symbol}")

    entity_id = entity.upper()

    # market_type → instrument type
    if market_type == "SPOT":
        inst_type = "SPOT"
    elif market_type in ("USDT-FUT", "COIN-FUT"):
        inst_type = "PERP"
    else:
        raise ProviderError(f"无法解析 Binance symbol: {symbol}")

    instrument_id = f"{entity_id}-{inst_type}"
    market_id = f"BINANCE:{symbol}:{market_type}"

    return (entity_id, instrument_id, market_id)


class EntityResolver:
    """实体解析器（单点实现，connector 只调用不实现解析逻辑）。

    封装 parse_binance 并提供 resolve() 方法统一入口。
    """

    def resolve(
        self, source: str, symbol: str, market_type: str
    ) -> tuple[str, str, str]:
        """调用底层解析函数。

        Args:
            source: 数据源标识。
            symbol: Binance symbol。
            market_type: SPOT / USDT-FUT / COIN-FUT。

        Returns:
            (entity_id, instrument_id, market_id) 三元组。
        """
        return parse_binance(symbol, market_type)


# ————————————————————————————————————————————————————————————————
# Deribit instrument_name 解析（D02 §3，MODEL-003.2）

# Deribit 月份单字符码表（D02 §3）
# 注意：M 可能表示 Mar 或 May，需通过上下文区分
_DERIBIT_MONTH_CODES: dict[str, int] = {
    "J": 1,   # Jan
    "F": 2,   # Feb
    "M": 3,   # Mar (默认，A 后的 M 为 May)
    "A": 4,   # Apr
    "S": 9,   # Sep
    "O": 10,  # Oct
    "N": 11,  # Nov
    "D": 12,  # Dec
}

# 位置映射：月份码在 date_str 中的位置 → 月份（用于解析 5 字符日期串）
# date_str 格式：DDMYY（日2位+月份码1位+年2位）
_VALID_MONTH_CHARS = set(_DERIBIT_MONTH_CODES.keys())


def _parse_deribit_month(date_str: str) -> tuple[int, int, int]:
    """从 Deribit 日期字符串解析 (day, month, year)。

    date_str 格式：DDMYY（如 "26JAN26" 拆为 "26" + "J" + "26"，其中月份为 3 字符缩写）。
    也支持简化格式 DMY（如 "26J26"，月份为单字符码）。

    Returns:
        (day, month, year) 三元组。

    Raises:
        ProviderError: 月份码非法。
    """
    # 尝试 3 字符月份格式（如 "26SEP26"）
    if len(date_str) == 7:
        day_str = date_str[:2]
        month_str = date_str[2:5].upper()
        year_str = date_str[5:7]
        month = _long_month_to_int(month_str)
        if month is None:
            raise ProviderError(f"Deribit 非法月份码: {date_str}")
    elif len(date_str) == 5:
        # 简化格式：DDMYY（单字符月份码）
        day_str = date_str[:2]
        month_char = date_str[2].upper()
        year_str = date_str[3:5]
        if month_char not in _VALID_MONTH_CHARS:
            raise ProviderError(f"Deribit 非法月份码: {month_char}")
        month = _DERIBIT_MONTH_CODES[month_char]
        # 上下文区分：A 后的 M = May(5)，否则 Mar(3)
        if month_char == "M" and date_str[1] == "A":
            month = 5  # May
    else:
        raise ProviderError(f"Deribit 日期格式错误: {date_str}")

    day = int(day_str)
    year = int(year_str)
    # 2 位年份：≥50 → 19xx，<50 → 20xx
    if year >= 50:
        year += 1900
    else:
        year += 2000

    return (day, month, year)


def _long_month_to_int(month_str: str) -> int | None:
    """将长月份名（JAN, FEB, ...）转为月份数字。

    Returns:
        1-12 或 None（非法月份）。
    """
    _LONG_MONTH_MAP: dict[str, int] = {
        "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4,
        "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8,
        "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
    }
    return _LONG_MONTH_MAP.get(month_str)


def parse_deribit(instrument_name: str) -> dict[str, object]:
    """解析 Deribit instrument_name 为结构化结果。

    输入格式：
    - 期权/期货：{UNDERLYING}-{DD}{MON}{YY}-{STRIKE}-{C|P}
      示例：BTC-26SEP26-100000-C
    - 永续合约：{UNDERLYING}-PERP
      示例：BTC-PERP

    Args:
        instrument_name: Deribit 合约名称。

    Returns:
        包含 underlying, expiry, strike, option_type, settlement_asset,
        instrument_id, market_id 的字典。过期合约额外含 "expired": True。

    Raises:
        ProviderError: instrument_name 格式非法或月份码无效。
    """
    if not instrument_name or not isinstance(instrument_name, str):
        raise ProviderError(f"Deribit instrument_name 为空: {instrument_name!r}")

    # Handle PERP (perpetual) instruments
    if instrument_name.upper().endswith("-PERP"):
        underlying = instrument_name[:-5].upper()
        if not underlying:
            raise ProviderError(f"Deribit underlying 为空: {instrument_name}")
        return {
            "underlying": underlying,
            "expiry": None,
            "strike": None,
            "option_type": None,
            "settlement_asset": "USD",
            "instrument_id": f"{underlying}-PERP",
            "market_id": f"DERIBIT:{instrument_name}:PERP",
        }

    parts = instrument_name.split("-")
    if len(parts) != 4:
        raise ProviderError(
            f"Deribit instrument_name 格式错误（期望 4 段，非 PERP 合约）: {instrument_name}"
        )

    underlying, date_str, strike_str, option_char = parts

    if not underlying:
        raise ProviderError(f"Deribit underlying 为空: {instrument_name}")

    # 解析日期
    day, month, year = _parse_deribit_month(date_str)
    try:
        expiry = date(year, month, day)
    except (ValueError, TypeError) as e:
        raise ProviderError(f"Deribit 日期非法: {date_str} → {e}") from e

    # 解析行权价
    try:
        strike = float(strike_str)
    except (ValueError, TypeError):
        raise ProviderError(f"Deribit 行权价非数字: {strike_str}") from None

    # 解析期权类型
    option_map = {"C": "CALL", "P": "PUT"}
    if option_char not in option_map:
        raise ProviderError(f"Deribit 期权类型非法: {option_char}")
    option_type = option_map[option_char]

    # 生成标准化 ID
    expiry_str = expiry.strftime("%Y-%m-%d")
    # Format strike: use integer when whole number, else float
    strike_formatted = int(strike) if strike == int(strike) else strike
    instrument_id = f"{underlying}-{expiry_str}-{strike_formatted}-{option_char}"
    market_id = f"DERIBIT:{instrument_name}:OPTION"

    result: dict[str, object] = {
        "underlying": underlying.upper(),
        "expiry": expiry,
        "strike": strike,
        "option_type": option_type,
        "settlement_asset": "USD",
        "instrument_id": instrument_id,
        "market_id": market_id,
    }

    # 检查是否过期
    if expiry < date.today():
        result["expired"] = True

    return result


# ————————————————————————————————————————————————————————————————
# Yahoo Finance ticker 解析（MODEL-003.2）


def parse_yahoo(symbol: str) -> tuple[str, str, str]:
    """将 Yahoo Finance ticker 解析为 (entity_id, instrument_id, market_id)。

    Args:
        symbol: Yahoo Finance ticker，如 "AAPL"、"MSFT"、"SPY"。

    Returns:
        (entity_id, instrument_id, market_id) 三元组。

    Raises:
        ProviderError: symbol 为空。
    """
    if not symbol or not isinstance(symbol, str) or not symbol.strip():
        raise ProviderError(f"Yahoo Finance symbol 为空: {symbol!r}")

    entity_id = symbol.strip().upper()
    instrument_id = f"{entity_id}-SPOT"
    market_id = f"YAHOO:{entity_id}:SPOT"

    return (entity_id, instrument_id, market_id)


# ————————————————————————————————————————————————————————————————
# InstrumentResolver（MODEL-003.2，单点实现原则）


class InstrumentResolver:
    """合约解析器（单点实现，connector 只调用不实现解析逻辑）。

    封装 parse_deribit、parse_yahoo 和 parse_binance，
    提供统一的 resolve() 入口按 source 分发。
    """

    def resolve(
        self, source: str, identifier: str, market_type: str | None = None
    ) -> dict[str, object] | tuple[str, str, str]:
        """统一入口，按 source 分发到对应的 parse_* 函数。

        Args:
            source: 数据源标识，支持 "deribit"、"yahoo"、"binance"。
            identifier: 源侧标识符（Deribit instrument_name / Yahoo ticker / Binance symbol）。
            market_type: 市场类型（binance 需要：SPOT/USDT-FUT/COIN-FUT）。

        Returns:
            解析结果字典或 (entity_id, instrument_id, market_id) 三元组。

        Raises:
            ProviderError: source 未知或解析失败。
        """
        source_lower = source.lower().strip() if source else ""

        if source_lower == "deribit":
            return self.resolve_deribit(identifier)
        elif source_lower == "yahoo":
            return self.resolve_yahoo(identifier)
        elif source_lower == "binance":
            if not market_type:
                raise ProviderError("binance source 需要提供 market_type")
            result = parse_binance(identifier, market_type)
            return {
                "entity_id": result[0],
                "instrument_id": result[1],
                "market_id": result[2],
            }
        else:
            raise ProviderError(f"未知数据源: {source}")

    def resolve_deribit(self, instrument_name: str) -> dict[str, object]:
        """解析 Deribit instrument_name。

        Args:
            instrument_name: Deribit 合约名称。

        Returns:
            结构化解析结果字典。
        """
        return parse_deribit(instrument_name)

    def resolve_yahoo(self, symbol: str) -> tuple[str, str, str]:
        """解析 Yahoo Finance ticker。

        Args:
            symbol: Yahoo Finance ticker。

        Returns:
            (entity_id, instrument_id, market_id) 三元组。
        """
        return parse_yahoo(symbol)
