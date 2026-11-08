"""税期日历：读取 ``config/tga/calendar.yaml``，按联邦工作日顺延。

报告侧只取与「税收流入 TGA」直接相关的可核验截止日：
- ``tax_events``：季度预缴与年度申报（IRS 截止日）；
- ``monthly_events`` 中 ``subcategories`` 含 ``federal_tax_deposits`` 的月度存款税
  （每月 15 日，遇非工作日顺延）。

不把「季末 25 日」等非联邦截止日纳入；也不按周几推算未公布的日期。
遇周末/联邦假日一律顺延到下一工作日（配置里的 ``shift: next``）。
"""

from __future__ import annotations

import calendar as _calendar
import re
from datetime import date, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]

PROJECT_ROOT = Path(__file__).resolve().parents[3]
CALENDAR_PATH = PROJECT_ROOT / "config" / "tga" / "calendar.yaml"
_MD_RE = re.compile(r"^(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$")


class TaxCalendarError(ValueError):
    """calendar.yaml 税期字段校验失败。"""


@lru_cache(maxsize=1)
def _federal_holidays() -> frozenset[date]:
    from pandas.tseries.holiday import USFederalHolidayCalendar  # type: ignore[import-untyped]

    return frozenset(ts.date() for ts in USFederalHolidayCalendar().holidays())


def is_business_day(day: date) -> bool:
    """联邦工作日 = 周一至周五且非联邦假日。"""
    return day.weekday() < 5 and day not in _federal_holidays()


def next_business_day(day: date) -> date:
    """严格晚于 day 的下一个联邦工作日。"""
    nxt = day + timedelta(days=1)
    while not is_business_day(nxt):
        nxt += timedelta(days=1)
    return nxt


def previous_business_day(day: date) -> date:
    """严格早于 day 的上一个联邦工作日。"""
    prv = day - timedelta(days=1)
    while not is_business_day(prv):
        prv -= timedelta(days=1)
    return prv


def _parse_md(value: Any, where: str) -> tuple[int, int]:
    if not isinstance(value, str) or not _MD_RE.fullmatch(value):
        raise TaxCalendarError(f"{where}: 日期必须是 MM-DD 字符串，实际 {value!r}")
    month, day = (int(part) for part in value.split("-"))
    return month, day


def _month_days(value: Any, where: str) -> list[int]:
    raw = [value] if isinstance(value, int) else value
    if not isinstance(raw, list) or not raw:
        raise TaxCalendarError(f"{where}: day_of_month 必须是整数或非空整数列表")
    days: list[int] = []
    for item in raw:
        if not isinstance(item, int) or not 1 <= item <= 31:
            raise TaxCalendarError(f"{where}: 非法 day_of_month {item!r}")
        days.append(item)
    return days


def load_tax_calendar(path: str | Path | None = None) -> dict[str, Any]:
    """读取并校验税期相关字段；不做缓存，配置改动立即可见。"""
    config_path = Path(path) if path else CALENDAR_PATH
    try:
        with open(config_path, encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
    except OSError as exc:
        raise TaxCalendarError(f"无法读取税期配置 {config_path}: {exc}") from exc
    if not isinstance(data, dict):
        raise TaxCalendarError("calendar.yaml 根节点必须是映射")
    tax_events = data.get("tax_events")
    monthly = data.get("monthly_events")
    if not isinstance(tax_events, list) or not tax_events:
        raise TaxCalendarError("tax_events: 必须是非空列表")
    if not isinstance(monthly, list) or not monthly:
        raise TaxCalendarError("monthly_events: 必须是非空列表")
    for index, event in enumerate(tax_events):
        where = f"tax_events[{index}]"
        if not isinstance(event, dict):
            raise TaxCalendarError(f"{where}: 必须是映射")
        due_dates = event.get("due_dates")
        if not isinstance(due_dates, list) or not due_dates:
            raise TaxCalendarError(f"{where}.due_dates: 必须是非空列表")
        for md in due_dates:
            _parse_md(md, f"{where}.due_dates")
    for index, event in enumerate(monthly):
        where = f"monthly_events[{index}]"
        if not isinstance(event, dict):
            raise TaxCalendarError(f"{where}: 必须是映射")
        _month_days(event.get("day_of_month"), f"{where}.day_of_month")
        shift = event.get("shift", "next")
        if shift not in {"next", "previous"}:
            raise TaxCalendarError(f"{where}.shift: 必须是 next/previous，实际 {shift!r}")
    return data


def _shift(raw: date, shift: str) -> date:
    if is_business_day(raw):
        return raw
    return next_business_day(raw) if shift == "next" else previous_business_day(raw)


def tax_period_dates(
    start: date, end: date, calendar_data: dict[str, Any] | None = None
) -> list[date]:
    """[start, end] 内的税期截止日（去重、升序、已按联邦工作日顺延）。"""
    cal = calendar_data if calendar_data is not None else load_tax_calendar()
    out: set[date] = set()

    for event in cal.get("tax_events", []):
        for year in range(start.year, end.year + 1):
            for md in event.get("due_dates", []):
                month, day = _parse_md(md, f"tax_events.{event.get('id')}")
                try:
                    raw = date(year, month, day)
                except ValueError:
                    continue
                shifted = _shift(raw, "next")
                if start <= shifted <= end:
                    out.add(shifted)

    for event in cal.get("monthly_events", []):
        if "federal_tax_deposits" not in (event.get("subcategories") or []):
            continue
        shift = str(event.get("shift", "next"))
        for index in range(start.year * 12 + start.month - 1, end.year * 12 + end.month):
            year, month = divmod(index, 12)
            month += 1
            last = _calendar.monthrange(year, month)[1]
            for day in _month_days(event.get("day_of_month"), f"monthly_events.{event.get('id')}"):
                if day > last:
                    continue
                shifted = _shift(date(year, month, day), shift)
                if start <= shifted <= end:
                    out.add(shifted)
    return sorted(out)


def next_tax_period(report_day: date, calendar_data: dict[str, Any] | None = None) -> date:
    """报告日（含）之后最近一个税期截止日；找不到时返回报告日。"""
    horizon = report_day + timedelta(days=400)
    dates = tax_period_dates(report_day, horizon, calendar_data)
    return dates[0] if dates else report_day
