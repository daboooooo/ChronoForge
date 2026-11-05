"""配置化交易日历（D06 §2, VALIDATION-002）。

职责：
- 按年份显式配置 NYSE 全日休市日（周末另判），不做规则推算
- is_trading_day() 判定函数
- EARLY_CLOSINGS 提前闭市日参考集合（早闭日仍是交易日）

扩展方式：在 `_NYSE_HOLIDAYS` 中显式新增年份条目（浮动假日与 observed
规则落到具体日期）。查询未配置年份时显式抛 `ConfigError`（可诊断），
不隐式兜底。P1 若引入正式交易日历库，替换 is_trading_day 实现，接口不变。
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date

from chronoforge.exceptions import ConfigError

# ── 配置化假日表（NYSE 官方日历，覆盖 2026–2027） ──────────────────
# 按年份显式列出具体日期；浮动假日（MLK/总统日/感恩节等）与周末 observed
# 规则已在编写时落到日期，运行时不做推算。

_NYSE_HOLIDAYS: dict[int, frozenset[date]] = {
    2026: frozenset({
        date(2026, 1, 1),    # New Year's Day
        date(2026, 1, 19),   # MLK Day（1 月第 3 个周一）
        date(2026, 2, 16),   # Presidents' Day（2 月第 3 个周一）
        date(2026, 4, 3),    # Good Friday
        date(2026, 5, 25),   # Memorial Day（5 月最后一个周一）
        date(2026, 6, 19),   # Juneteenth
        date(2026, 7, 3),    # Independence Day observed（7/4 周六）
        date(2026, 9, 7),    # Labor Day（9 月第 1 个周一）
        date(2026, 11, 26),  # Thanksgiving（11 月第 4 个周四）
        date(2026, 12, 25),  # Christmas
    }),
    2027: frozenset({
        date(2027, 1, 1),    # New Year's Day
        date(2027, 1, 18),   # MLK Day
        date(2027, 2, 15),   # Presidents' Day
        date(2027, 3, 26),   # Good Friday
        date(2027, 5, 31),   # Memorial Day
        date(2027, 6, 18),   # Juneteenth observed（6/19 周六）
        date(2027, 7, 5),    # Independence Day observed（7/4 周日）
        date(2027, 9, 6),    # Labor Day
        date(2027, 11, 25),  # Thanksgiving
        date(2027, 12, 24),  # Christmas observed（12/25 周六）
    }),
}

# ── 提前闭市日（早闭，参考集合） ──────────────────────────────────
# 早闭日仍是交易日，不影响 is_trading_day；仅供需要半日交易语义的调用方参考。

EARLY_CLOSINGS: frozenset[date] = frozenset({
    date(2026, 11, 27),  # Thanksgiving 次日
    date(2026, 12, 24),  # Christmas Eve
    date(2027, 11, 26),  # Thanksgiving 次日
    date(2027, 12, 23),  # Christmas Eve observed 前一日
})


@dataclass(frozen=True)
class MarketCalendar:
    """按年份显式配置的美股交易日历。

    Attributes:
        holidays_by_year: 年份 → 全日休市日集合。未配置的年份查询显式失败。
    """

    holidays_by_year: Mapping[int, frozenset[date]]

    def holidays_for(self, year: int) -> frozenset[date]:
        """返回指定年份的假日集合；未配置年份抛 ConfigError。"""
        try:
            return self.holidays_by_year[year]
        except KeyError:
            known = ", ".join(str(y) for y in sorted(self.holidays_by_year))
            raise ConfigError(
                f"US market calendar has no holidays configured for {year}; "
                f"known years: {known}"
            ) from None

    def is_trading_day(self, d: date) -> bool:
        """周末或该年假日 → False；未配置年份显式抛 ConfigError。"""
        holidays = self.holidays_for(d.year)
        if d.weekday() >= 5:  # Saturday=5, Sunday=6
            return False
        return d not in holidays


US_MARKET_CALENDAR = MarketCalendar(holidays_by_year=_NYSE_HOLIDAYS)


def is_trading_day(d: date) -> bool:
    """判断是否为交易日（配置化日历；未配置年份显式失败）。

    Args:
        d: 待判定的日期。

    Returns:
        True 表示交易日，False 表示非交易日。

    Raises:
        ConfigError: 该年份未在假日表中配置。
    """
    return US_MARKET_CALENDAR.is_trading_day(d)
