"""简化交易日历（D06 §2, VALIDATION-002）。

职责：
- P0 内置美股简化日历（周末 + 固定节假日）
- is_trading_day() 判定函数
- EARLY_CLOSINGS 提前闭市日集合

扩展点：
- P1 评估正式交易日历库（如 nasdaq-basemaps）
- 届时替换 is_trading_day 实现，接口不变
"""

from __future__ import annotations

from datetime import date

# ── 固定美股节假日（NYSE 官方日历的简化版本） ─────────────────────
# (month, day) 格式。部分日期为近似值（如感恩节、阵亡将士纪念日）。

US_MARKET_HOLIDAYS: frozenset[tuple[int, int]] = frozenset({
    # (month, day)
    (1, 1),    # New Year's Day
    (1, 17),   # MLK Day (3rd Monday of January) → simplified
    (2, 14),   # Presidents' Day
    (5, 31),   # Memorial Day (last Monday of May) → simplified
    (6, 19),   # Juneteenth
    (7, 4),    # Independence Day
    (9, 7),    # Labor Day (1st Monday of September) → simplified
    (11, 27),  # Thanksgiving (4th Thursday of November) → simplified
    (12, 25),  # Christmas
})

# ── 提前闭市日（早闭） ──────────────────────────────────────────────
# 每年不同，P0 硬编码当年 + 次年

EARLY_CLOSINGS: frozenset[date] = frozenset({
    date(2026, 7, 3),   # Independence Day observed (Friday)
    date(2027, 7, 5),   # Independence Day observed (Monday)
    date(2027, 12, 24), # Christmas observed (Friday)
})


def is_trading_day(d: date) -> bool:
    """判断是否为交易日（P0 简化日历）。

    周末 + 固定节假日 → False；早闭日仍是交易日（即使也是节假日）。

    Args:
        d: 待判定的日期。

    Returns:
        True 表示交易日，False 表示非交易日。
    """
    if d.weekday() >= 5:  # Saturday=5, Sunday=6
        return False
    if d in EARLY_CLOSINGS:
        return True  # 早闭日仍是交易日（只是早收盘）
    if (d.month, d.day) in US_MARKET_HOLIDAYS:
        return False
    return True
