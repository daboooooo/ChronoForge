"""报告日期与交易日历辅助。"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

from chronoforge.quality.calendar import is_trading_day

NY = ZoneInfo("America/New_York")
SHANGHAI = ZoneInfo("Asia/Shanghai")

_WEEKDAY_CN = ("周一", "周二", "周三", "周四", "周五", "周六", "周日")


def us_report_date(now: datetime | None = None) -> date:
    """返回当前时刻对应的美股交易日日期（America/New_York）。"""
    current = now or datetime.now(UTC)
    if current.tzinfo is None:
        current = current.replace(tzinfo=UTC)
    return current.astimezone(NY).date()


def weekday_cn(day: date) -> str:
    return _WEEKDAY_CN[day.weekday()]


def shift_trading_days(day: date, count: int) -> date:
    """按美股交易日移动日期（count 为负表示向前）。"""
    step = -1 if count < 0 else 1
    remaining = abs(count)
    current = day
    while remaining:
        current += timedelta(days=step)
        if is_trading_day(current):
            remaining -= 1
    return current


def require_trading_day(day: date) -> None:
    """非交易日抛 ValueError（入口据此不生成报告）。"""
    if not is_trading_day(day):
        raise ValueError(f"{day.isoformat()} 不是美股交易日，不生成日报")
