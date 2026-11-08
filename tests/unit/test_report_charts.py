"""图表论证数据：滚动分位带、转折点、税期效应与卡片文字摘要。"""

from __future__ import annotations

from datetime import date, timedelta

from chronoforge.report.facts import _rolling_bands, _tax_effect, _turning_point
from chronoforge.report.render import _chart_card


def test_rolling_bands_keep_order_and_cover_display_window() -> None:
    start = date(2020, 1, 1)
    t = [(start + timedelta(days=7 * i)).isoformat() for i in range(300)]
    v = [float(i) for i in range(300)]
    bands = _rolling_bands(t, v, date(2022, 1, 1))
    assert bands["t"][-1] == t[-1]
    assert len(bands["p20"]) == len(bands["t"])
    assert bands["p20"][-1] < bands["p50"][-1] < bands["p80"][-1]


def test_turning_point_reports_latest_four_week_sign_flip() -> None:
    t = [f"2026-0{1 + i // 4}-{1 + (i % 4) * 7:02d}" for i in range(15)]
    v = [10, 10, 10, 10, 9, 8, 7, 6, 5, 4, 3, 4, 6, 9, 13]
    turning = _turning_point(t, [float(x) for x in v])
    assert turning is not None
    assert turning["t"] == t[12]
    assert turning["label"] == "4 周动量转正"


def test_tax_effect_reports_average_in_billions() -> None:
    tax_day = date(2026, 4, 15)
    points = [
        (tax_day - timedelta(days=2), 2_000_000.0),
        (tax_day - timedelta(days=1), 2_010_000.0),
        (tax_day + timedelta(days=4), 2_100_000.0),
        (tax_day + timedelta(days=5), 2_090_000.0),
    ]
    effect = _tax_effect(points, [tax_day])
    assert effect["n"] == 1
    assert effect["avg_chg_b"] == 80.0


def test_chart_card_keeps_note_visible_with_or_without_data() -> None:
    with_data = _chart_card("标题", "chart-x", True, note="关键读数")
    assert "chart-note" in with_data and "关键读数" in with_data
    without_data = _chart_card("标题", "chart-x", False, note="缺失说明")
    assert "chart-note" in without_data and "缺失说明" in without_data
    bare = _chart_card("标题", "chart-x", True)
    assert "chart-note" not in bare
