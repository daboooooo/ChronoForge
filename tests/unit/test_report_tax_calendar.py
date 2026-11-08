"""税期日历读取与规则接入测试（config/tga/calendar.yaml 为单一事实来源）。"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pytest

from chronoforge.report import rules as rules_mod
from chronoforge.report import tax_calendar as cal_mod


def _calendar(monthly_day: int, shift: str = "next") -> str:
    return f"""version: "2"
tax_events:
  - id: individual_estimated
    due_dates: ["01-15", "04-15", "06-15", "09-15"]
    subcategories: ["nonwithheld_seca"]
monthly_events:
  - id: monthly_tax_deposit
    day_of_month: {monthly_day}
    shift: {shift}
    category: TAX
    subcategories: ["federal_tax_deposits"]
  - id: semi_monthly_withholding
    day_of_month: [1, 15]
    shift: previous
    category: TAX
    subcategories: ["withheld_individual"]
"""


def _write(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "calendar.yaml"
    path.write_text(text, encoding="utf-8")
    return path


def test_repo_calendar_dates_shift_to_business_days() -> None:
    dates = cal_mod.tax_period_dates(date(2026, 1, 1), date(2026, 12, 31))
    assert len(dates) == 12
    # 11-15 为周日 → 顺延到 11-16
    assert date(2026, 11, 16) in dates
    # 02-15 为周日，02-16 为总统日 → 顺延到 02-17
    assert date(2026, 2, 17) in dates
    # 非联邦截止日的 25 日不得出现
    assert not [d for d in dates if d.day == 25]


def test_next_tax_period_uses_shifted_date() -> None:
    assert cal_mod.next_tax_period(date(2026, 11, 9)) == date(2026, 11, 16)
    assert cal_mod.next_tax_period(date(2026, 11, 16)) == date(2026, 11, 16)


def test_config_change_changes_reported_tax_dates(tmp_path: Path, monkeypatch) -> None:
    path = _write(tmp_path, _calendar(monthly_day=20))
    monkeypatch.setattr(cal_mod, "CALENDAR_PATH", path)
    dates = cal_mod.tax_period_dates(date(2026, 11, 1), date(2026, 11, 30))
    assert dates == [date(2026, 11, 20)]

    # 未配置 federal_tax_deposits 的月度事件不参与报告税期
    path.write_text(_calendar(20).replace(
        'subcategories: ["federal_tax_deposits"]',
        'subcategories: ["withheld_individual"]',
    ), encoding="utf-8")
    assert cal_mod.tax_period_dates(date(2026, 11, 1), date(2026, 11, 30)) == []


def test_invalid_due_date_rejected(tmp_path: Path) -> None:
    path = _write(tmp_path, _calendar(15).replace('"01-15"', '"15-15"'))
    with pytest.raises(cal_mod.TaxCalendarError, match="MM-DD"):
        cal_mod.load_tax_calendar(path)


def test_tax_rule_reads_configured_dates(tmp_path: Path, monkeypatch) -> None:
    """EV_TGA_TAX 的回测参考日必须来自配置，而不是代码里的 15/25 硬编码。"""
    path = _write(tmp_path, _calendar(monthly_day=20))
    monkeypatch.setattr(cal_mod, "CALENDAR_PATH", path)

    start = date(2025, 12, 1)
    tga = [(start + timedelta(days=i), 900.0) for i in range(365)]
    frame = rules_mod._Frame(
        reserve_w=[], tga_d=tga, rrp_d=[], srf_d=[], spread_d=[], bdays=[]
    )
    refs = [ref for ref, _ in rules_mod._tax_rebuilds(frame, date(2026, 12, 31))]
    assert date(2026, 11, 20) in refs
    assert date(2026, 11, 16) not in refs
    assert not [ref for ref in refs if ref.day == 25]
