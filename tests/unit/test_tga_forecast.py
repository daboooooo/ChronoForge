"""tga_forecast 单元测试（Phase 3）：组件 / 聚合守恒 / PIT 无泄漏 / 存储。"""

from __future__ import annotations

import json
import sys
from datetime import date, datetime
from pathlib import Path

import polars as pl
import pytest

_SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from tga_common import is_business_day  # noqa: E402
from tga_forecast import (  # noqa: E402
    ForecastContext,
    _coupon_dates,
    _rows_to_components,
    add_months,
    aggregate_forecast,
    annotate_scenario_output,
    announced_auctions,
    data_through,
    debt_component,
    interest_component,
    nth_weekday_of_month,
    rel_business_index,
    resulted_auctions,
    tax_component,
)
from tga_store import ForecastStore  # noqa: E402

ORIGIN = date(2026, 11, 2)  # 周一


# ── fixtures ───────────────────────────────────────────────────────────


def _model() -> dict:
    return {
        "horizons": [7, 14, 30],
        "windows": {
            "recent_run_rate_weeks": 4, "monthly_median_weeks": 13,
            "same_month_years": 3, "trend_months": 12,
            "interest_baseline_weeks": 13, "coupon_baseline_weeks": 26,
        },
        "trend_cap": [0.8, 1.2],
        "bands": {
            "KNOWN_FINAL": 0.0, "KNOWN_PROVISIONAL": 0.05,
            "SEASONAL": 0.5, "STATISTICAL": 0.5, "UNCLASSIFIED": 1.0,
        },
        "confidence": {
            "KNOWN_FINAL": 1.0, "KNOWN_PROVISIONAL": 0.8,
            "SEASONAL": 0.5, "STATISTICAL": 0.5, "UNCLASSIFIED": 0.2,
        },
        "issuance": {
            "cadence_history_weeks": 104, "offering_median_count": 8,
            "layer3_confidence": 0.4, "layer2_confidence": 0.7,
            "cadence_dedupe_days": 3,
            "coupon_frequencies": {"Semi-Annual": 2, "Quarterly": 4, "Monthly": 12},
        },
        "limits": {"cutoff_stale_days": 7},
    }


def _calendar() -> dict:
    return {
        "event_window_business_days": 10,
        "tax_events": [
            {"id": "individual_estimated", "due_dates": ["04-15", "06-15"],
             "subcategories": ["individual"]},
        ],
        "outlay_cycles": {
            "social_security": {
                "rule": "nth_weekday_of_month", "weekday": 2,
                "ordinals": [2, 3, 4], "subcategory": "social_security",
            },
        },
    }


def _balance_df() -> pl.DataFrame:
    return pl.DataFrame({
        "date": [date(2026, 10, 28), date(2026, 10, 29)],
        "opening": [1000.0, 1100.0],
        "closing": [1100.0, 1200.0],
        "daily_change": [100.0, 100.0],
        "total_deposits": [None, None],
        "total_withdrawals": [None, None],
        "quality_status": ["VALID", "VALID"],
        "flags": ["", ""],
    })


def _cash_row(d: date, category: str, sub: str, direction: str,
              amount: float, **kw) -> dict:
    row = {
        "date": d, "source_table": "II", "raw_category": sub,
        "category": category, "subcategory": sub, "direction": direction,
        "amount_usd_millions": amount, "include_in_net": True,
        "is_subtotal": False, "is_duplicate_candidate": False,
        "classification_confidence": 1.0,
    }
    row.update(kw)
    return row


_CASHFLOW_SCHEMA: dict[str, pl.DataType] = {
    "date": pl.Date, "source_table": pl.String, "raw_category": pl.String,
    "category": pl.String, "subcategory": pl.String, "direction": pl.String,
    "amount_usd_millions": pl.Float64, "include_in_net": pl.Boolean,
    "is_subtotal": pl.Boolean, "is_duplicate_candidate": pl.Boolean,
    "classification_confidence": pl.Float64,
}


def _cashflow_df(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=_CASHFLOW_SCHEMA)


def _auction_row(cusip: str, term: str, *, auction: date, issue: date | None,
                 maturity: date | None, offering: float, accepted: float | None,
                 status: str, release: date, rate: float | None = None,
                 freq: str | None = "Semi-Annual", price: float | None = None,
                 reopening: bool = False, cmb: bool = False) -> dict:
    return {
        "auction_key": f"{cusip}:{auction}:{status}",
        "cusip": cusip, "security_type": "Bill" if "Week" in term else "Note",
        "security_term": term,
        "announcement_date": auction, "auction_date": auction,
        "issue_date": issue, "maturity_date": maturity,
        "offering_amount_usd_millions": offering,
        "total_accepted_usd_millions": accepted,
        "interest_rate": rate, "high_yield": None, "price_per_100": price,
        "interest_payment_frequency": freq,
        "is_reopening": reopening, "is_cmb": cmb,
        "data_status": status,
        "release_time": datetime(release.year, release.month, release.day),
    }


def _auctions_df(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows)


def _ctx(cashflow: pl.DataFrame, auctions: pl.DataFrame) -> ForecastContext:
    return ForecastContext(
        ORIGIN, [7, 14, 30], _model(), _calendar(),
        _balance_df(), cashflow, auctions,
    )


# ── 日历工具 ───────────────────────────────────────────────────────────


def test_data_through_conservative():
    # 周一 origin：周五 DTS（次工作日=周一发布）不可用 → 截至周四
    assert data_through(date(2026, 11, 2)) == date(2026, 10, 29)
    # 周六 origin：周五 DTS 次工作日=周一（≥origin）→ 截至周四（11-05）
    assert data_through(date(2026, 11, 7)) == date(2026, 11, 5)


def test_nth_weekday_and_add_months():
    # 2026-11 第一个周三 = 11-04
    assert nth_weekday_of_month(2026, 11, 2, [1, 2, 3, 4]) == [
        date(2026, 11, 4), date(2026, 11, 11), date(2026, 11, 18),
        date(2026, 11, 25),
    ]
    assert add_months(date(2026, 3, 31), -1) == date(2026, 2, 28)
    assert add_months(date(2026, 5, 15), -18) == date(2024, 11, 15)


def test_rel_business_index():
    anchor = date(2026, 11, 2)  # 周一
    assert rel_business_index(anchor, date(2026, 11, 2)) == 0
    assert rel_business_index(anchor, date(2026, 11, 6)) == 4
    assert rel_business_index(anchor, date(2026, 10, 30)) == -1
    # 跨周末：周一→下周一 = 5
    assert rel_business_index(anchor, date(2026, 11, 9)) == 5


# ── 国债组件：三层与 PIT ───────────────────────────────────────────────


def test_debt_announced_vs_result_and_no_leak():
    auctions = _auctions_df([
        # RESULT：release 在 cutoff（10-29）当天 → 用 total_accepted × price/100
        _auction_row("A", "4-Week", auction=date(2026, 10, 29),
                     issue=date(2026, 11, 5), maturity=date(2026, 12, 3),
                     offering=100000.0, accepted=100000.0, status="RESULT",
                     release=date(2026, 10, 29), freq="None", price=99.5),
        # ANNOUNCED：release 在 cutoff 前 → 用 offering（禁止偷看结果）
        _auction_row("B", "8-Week", auction=date(2026, 10, 28),
                     issue=date(2026, 11, 6), maturity=date(2026, 12, 4),
                     offering=85000.0, accepted=None, status="ANNOUNCED",
                     release=date(2026, 10, 28), freq="None"),
        # release 在 cutoff 后（10-30 发布 > through 10-29）→ 整行不可用
        _auction_row("C", "13-Week", auction=date(2026, 10, 30),
                     issue=date(2026, 11, 6), maturity=date(2026, 12, 6),
                     offering=90000.0, accepted=91000.0, status="RESULT",
                     release=date(2026, 10, 30), freq="None"),
    ])
    comps = debt_component(_ctx(_cashflow_df([]), auctions))
    issues = comps.filter(pl.col("event_key").str.starts_with("ISSUE:"))
    assert issues.height == 2  # C 被 PIT 过滤
    a = issues.filter(pl.col("event_key").str.contains(":A:"))
    assert abs(a["predicted"][0] - 99500.0) < 0.01  # 100000 × 99.5/100
    assert a["source_type"][0] == "KNOWN_FINAL"
    b = issues.filter(pl.col("event_key").str.contains(":B:"))
    assert abs(b["predicted"][0] - 85000.0) < 0.01
    assert b["source_type"][0] == "KNOWN_PROVISIONAL"


def test_announcement_layer_uses_official_announcement_date():
    auctions = _auctions_df([
        # 公告日 11-03（cutoff 之后）→ 不进入公告层
        _auction_row("X", "4-Week", auction=date(2026, 11, 3),
                     issue=date(2026, 11, 5), maturity=date(2026, 12, 3),
                     offering=100000.0, accepted=100000.0, status="RESULT",
                     release=date(2026, 11, 3), freq="None"),
        # 公告已发、结果未出 → 公告层可用，结果层不可用
        _auction_row("Y", "8-Week", auction=date(2026, 10, 28),
                     issue=date(2026, 11, 6), maturity=date(2026, 12, 4),
                     offering=85000.0, accepted=None, status="ANNOUNCED",
                     release=date(2026, 10, 28), freq="None"),
        # 公告与结果均已在 cutoff 前 → 两层都可用
        _auction_row("Z", "13-Week", auction=date(2026, 10, 27),
                     issue=date(2026, 10, 29), maturity=date(2027, 1, 28),
                     offering=90000.0, accepted=91000.0, status="RESULT",
                     release=date(2026, 10, 29), freq="None"),
    ])
    ctx = _ctx(_cashflow_df([]), auctions)
    announced = announced_auctions(ctx)
    resulted = resulted_auctions(ctx)
    assert set(announced["cusip"].to_list()) == {"Y", "Z"}
    assert resulted["cusip"].to_list() == ["Z"]


def test_announcement_missing_is_not_backfilled():
    # 未出结果且公告日缺失 → 公告层不可用（不回填、不推断）
    provisional = _auction_row(
        "M", "4-Week", auction=date(2026, 10, 27), issue=date(2026, 10, 29),
        maturity=date(2026, 11, 26), offering=100000.0, accepted=None,
        status="ANNOUNCED", release=date(2026, 10, 27), freq="None",
    )
    provisional["announcement_date"] = None
    ctx = _ctx(_cashflow_df([]), _auctions_df([provisional]))
    assert announced_auctions(ctx).is_empty()


def test_calendar_layer_switch_controls_event_curve():
    rows: list[dict] = []
    d = date(2022, 10, 3)
    while d <= date(2026, 10, 29):
        if is_business_day(d):
            amount = 5000.0 if d.day == 15 else 100.0
            rows.append(_cash_row(d, "TAX", "nonwithheld_seca", "INFLOW", amount))
        d = date.fromordinal(d.toordinal() + 1)
    cashflow = _cashflow_df(rows)
    calendar = {
        "enabled": True, "event_window_business_days": 10,
        "tax_events": [{"id": "q", "due_dates": ["01-15", "04-15", "06-15", "09-15"],
                        "subcategories": ["nonwithheld_seca"]}],
        "monthly_events": [{"id": "m", "day_of_month": 15, "shift": "next",
                            "category": "TAX", "subcategories": ["nonwithheld_seca"]}],
    }
    auctions = _auctions_df([])
    on = tax_component(ForecastContext(
        ORIGIN, [7, 14, 30], _model(), calendar, _balance_df(), cashflow, auctions
    ))
    off_cal = {**calendar, "enabled": False}
    off = tax_component(ForecastContext(
        ORIGIN, [7, 14, 30], _model(), off_cal, _balance_df(), cashflow, auctions
    ))
    assert "event_curve" in on["method"].unique().to_list()
    assert "event_curve" not in off["method"].unique().to_list()


def test_redemption_principal_sums_reopenings_and_shifts_weekend():
    # 同一 CUSIP 多次拍卖（原始 + 再发行）→ 本金求和；到期日周日 → 顺延周一
    auctions = _auctions_df([
        _auction_row("R", "17-Week", auction=date(2026, 7, 1),
                     issue=date(2026, 7, 2), maturity=date(2026, 11, 15),
                     offering=100000.0, accepted=100000.0, status="RESULT",
                     release=date(2026, 7, 2), freq="None"),
        _auction_row("R", "4-Week", auction=date(2026, 10, 1),
                     issue=date(2026, 10, 6), maturity=date(2026, 11, 15),
                     offering=50000.0, accepted=50000.0, status="RESULT",
                     release=date(2026, 10, 2), freq="None", reopening=True),
    ])
    comps = debt_component(_ctx(_cashflow_df([]), auctions))
    redeems = comps.filter(pl.col("event_key").str.starts_with("REDEEM:"))
    # 同一 CUSIP 的两行拍卖 → 同一赎回事件（同 event_key），聚合后只计一次
    assert redeems["event_key"].unique().to_list() == ["REDEEM:R:2026-11-16"]
    assert (redeems["predicted"] == -150000.0).all()
    _, forecast = aggregate_forecast(
        _ctx(_cashflow_df([]), auctions), comps
    )
    row = forecast.filter(pl.col("target_date") == date(2026, 11, 16))
    assert abs(row["daily_net"][0] + 150000.0) < 0.5
    assert redeems["source_type"][0] == "KNOWN_FINAL"


def test_coupon_schedule_weekend_shift():
    # 3-Year Note：issue 2026-11-15（周日）前的票息 2026-11-15 → 顺延 11-16
    auctions = _auctions_df([
        _auction_row("N", "3-Year", auction=date(2026, 11, 10),
                     issue=date(2026, 11, 16), maturity=date(2029, 11, 15),
                     offering=48000.0, accepted=48913.47, status="RESULT",
                     release=date(2026, 10, 29), rate=4.5, freq="Semi-Annual"),
    ])
    ctx = _ctx(_cashflow_df([]), auctions)
    assert date(2026, 11, 15) in _coupon_dates(date(2029, 11, 15), 2)
    comps = interest_component(ctx)
    coupons = comps.filter(pl.col("event_key").str.starts_with("COUPON:"))
    assert coupons.height >= 1
    nov16 = coupons.filter(pl.col("target_date") == date(2026, 11, 16))
    assert nov16.height == 1
    # 48913.47 × 4.5% / 2 = 1100.55
    assert abs(nov16["predicted"][0] + 1100.55) < 0.01
    assert nov16["source_type"][0] == "KNOWN_FINAL"


# ── 税收组件：PIT 无泄漏 ───────────────────────────────────────────────


def test_tax_component_ignores_future_data():
    hist = [
        # 数据截止（10-29）前的历史 TAX 流入
        _cash_row(date(2026, 10, 15), "TAX", "individual", "INFLOW", 500.0),
        _cash_row(date(2026, 10, 20), "TAX", "individual", "INFLOW", 400.0),
        _cash_row(date(2026, 10, 27), "TAX", "individual", "INFLOW", 300.0),
    ]
    future = [
        # 未来数据（11 月）——不得影响预测
        _cash_row(date(2026, 11, 10), "TAX", "individual", "INFLOW", 999999.0),
    ]
    base = tax_component(_ctx(_cashflow_df(hist), _auctions_df([])))
    leaked = tax_component(_ctx(_cashflow_df(hist + future), _auctions_df([])))
    assert base.height == leaked.height
    assert base.equals(leaked)


def _ctx_cal(cashflow: pl.DataFrame, auctions: pl.DataFrame, mode: str,
             **cal: object) -> ForecastContext:
    model = _model()
    model["issuance"]["calibration"] = {
        "mode": mode, "trailing_weeks": 26, "scale_cap": [0.75, 1.35], **cal,
    }
    return ForecastContext(
        ORIGIN, [7, 14, 30], model, _calendar(),
        _balance_df(), cashflow, auctions,
    )


def _weekly_bills(last: date, n: int) -> list[dict]:
    from datetime import timedelta

    out = []
    for i in range(n):
        d = last - timedelta(days=7 * (n - 1 - i))
        out.append(_auction_row(
            f"W{i}", "4-Week", auction=d, issue=d,
            maturity=d + timedelta(days=28), offering=100000.0,
            accepted=100000.0, status="RESULT", release=d,
            freq="None", price=100.0,
        ))
    return out


def test_issuance_trailing_scale_caps_size():
    # 近 26 周实际发行远低于 median_gap 外推隐含 → 比例触底 0.75
    auctions = _auctions_df(_weekly_bills(date(2026, 10, 22), 4))
    comps = debt_component(
        _ctx_cal(_cashflow_df([]), auctions, "trailing_scale")
    )
    row = comps.filter(
        (pl.col("event_key").str.contains("PRED-ISSUE"))
        & (pl.col("target_date") == date(2026, 11, 5))
    )
    assert abs(row["predicted"][0] - 75000.0) < 0.01


def test_issuance_seasonal_window_replays_prior_year():
    # 用去年同日历窗口（−364 天）的真实日期/规模重放；PIT 仅用 cutoff 前数据
    from datetime import timedelta

    rows = []
    for d, amt in [(date(2025, 11, 5), 100000.0), (date(2025, 11, 12), 110000.0),
                   (date(2025, 11, 19), 120000.0)]:
        rows.append(_auction_row(
            f"S{d.isoformat()}", "4-Week", auction=d, issue=d,
            maturity=d + timedelta(days=28), offering=amt, accepted=amt,
            status="RESULT", release=d, freq="None", price=100.0,
        ))
    auctions = _auctions_df(rows)
    comps = debt_component(
        _ctx_cal(_cashflow_df([]), auctions, "seasonal_window")
    )
    pred = comps.filter(pl.col("event_key").str.contains("PRED-ISSUE"))
    got = {
        r["target_date"]: round(r["predicted"], 2)
        for r in pred.iter_rows(named=True)
    }
    # 日期来自去年同窗口；规模用当前窗口中位数（100k/110k/120k → 110k）
    assert got == {
        date(2026, 11, 4): 110000.0,
        date(2026, 11, 12): 110000.0,  # 11-11 退伍军人节 → 顺延
        date(2026, 11, 18): 110000.0,
    }


def test_calibration_never_adjusts_known_events():
    # KNOWN_FINAL 发行金额不受任何校准模式影响（且与 Layer 3 去重）
    auctions = _auctions_df(_weekly_bills(date(2026, 10, 22), 4) + [
        _auction_row("K", "4-Week", auction=date(2026, 10, 28),
                     issue=date(2026, 11, 5), maturity=date(2026, 12, 3),
                     offering=50000.0, accepted=50000.0, status="RESULT",
                     release=date(2026, 10, 28), freq="None", price=100.0),
    ])
    comps = debt_component(
        _ctx_cal(_cashflow_df([]), auctions, "seasonal_window")
    )
    known = comps.filter(pl.col("event_key").str.starts_with("ISSUE:"))
    assert known.height == 1
    assert abs(known["predicted"][0] - 50000.0) < 0.01
    assert known["source_type"][0] == "KNOWN_FINAL"
    pred = comps.filter(
        (pl.col("event_key").str.contains("PRED-ISSUE"))
        & (pl.col("target_date") == date(2026, 11, 5))
    )
    assert pred.is_empty()  # 与 KNOWN 同日 → 去重


# ── 聚合：去重 / 守恒 / 累计 / 边界 ────────────────────────────────────


def _comp(d: date, category: str, amount: float, event_key: str,
          source: str = "SEASONAL", detail: str = "") -> dict:
    return {
        "target_date": d, "category": category, "predicted": amount,
        "lower": amount, "upper": amount, "method": "m",
        "confidence": 0.5, "source_type": source, "event_key": event_key,
        "detail": detail,
    }


def test_aggregate_dedupes_same_event_and_conserves():
    ctx = _ctx(_cashflow_df([]), _auctions_df([]))
    d = ctx.grid[0]
    comps = _rows_to_components([
        _comp(d, "DEBT_ISSUANCE", 100.0, "ISSUE:X"),
        _comp(d, "DEBT_ISSUANCE", 100.0, "ISSUE:X"),  # 同事件重复 → 只计一次
        _comp(d, "TAX", 30.0, None),
    ])
    _, forecast = aggregate_forecast(ctx, comps)
    assert forecast.height == len(ctx.grid)
    row = forecast.filter(pl.col("target_date") == d)
    assert abs(row["daily_net"][0] - 130.0) < 0.5
    assert abs(row["predicted_tga"][0] - (1200.0 + 130.0)) < 0.5
    # 守恒：Σdaily_net == Σ组件
    assert abs(forecast["daily_net"].sum() - 130.0) < 0.5


def test_aggregate_preserves_multi_subcategory_baseline_rows():
    """TGA-1：同类别不同子类的同日基线行不得被 keep="first" 丢弃。"""
    ctx = _ctx(_cashflow_df([]), _auctions_df([]))
    d = ctx.grid[0]
    comps = _rows_to_components([
        _comp(d, "TAX", 40.0, None, detail="federal_tax_deposits"),
        _comp(d, "TAX", 25.0, None, detail="excise_tax"),
    ])
    kept, forecast = aggregate_forecast(ctx, comps)

    assert kept.height == 2
    assert abs(kept["predicted"].sum() - 65.0) < 0.5
    row = forecast.filter(pl.col("target_date") == d)
    assert abs(row["daily_net"][0] - 65.0) < 0.5


def test_aggregate_preserves_shared_event_label_across_subcategories():
    """TGA-1：不同子类共享同一 event label/event_key 时不得跨子类合并。"""
    ctx = _ctx(_cashflow_df([]), _auctions_df([]))
    d = ctx.grid[0]
    event_key = "TAX:q1:2026-11-09:+0"
    comps = _rows_to_components([
        _comp(d, "TAX", 10.0, event_key, detail="federal_tax_deposits"),
        _comp(d, "TAX", 20.0, event_key, detail="excise_tax"),
    ])
    kept, forecast = aggregate_forecast(ctx, comps)

    assert kept.height == 2
    assert abs(kept["predicted"].sum() - 30.0) < 0.5
    row = forecast.filter(pl.col("target_date") == d)
    assert abs(row["daily_net"][0] - 30.0) < 0.5


def test_aggregate_preserves_inflow_outflow_baseline_rows():
    """TGA-1：同类别同子类同日的流入/流出基线是独立组件，不得被去重丢弃。"""
    ctx = _ctx(_cashflow_df([]), _auctions_df([]))
    d = ctx.grid[0]
    comps = _rows_to_components([
        _comp(d, "OTHER", 2246.0, None, detail="agency_misc"),
        _comp(d, "OTHER", -6889.0, None, detail="agency_misc"),
    ])
    kept, forecast = aggregate_forecast(ctx, comps)
    assert kept.height == 2
    assert abs(kept["predicted"].sum() + 4643.0) < 0.5
    row = forecast.filter(pl.col("target_date") == d)
    assert abs(row["daily_net"][0] + 4643.0) < 0.5


def test_aggregate_rejects_conflicting_event_identity():
    """TGA-1：同一事件身份出现不同金额 → 显式失败，而非 keep="first" 静默丢弃。"""
    ctx = _ctx(_cashflow_df([]), _auctions_df([]))
    d = ctx.grid[0]
    comps = _rows_to_components([
        _comp(d, "DEBT_ISSUANCE", 100.0, "ISSUE:X"),
        _comp(d, "DEBT_ISSUANCE", 200.0, "ISSUE:X"),
    ])
    with pytest.raises(RuntimeError, match="collides distinct event components"):
        aggregate_forecast(ctx, comps)


def test_aggregate_horizon_boundary_flags():
    ctx = _ctx(_cashflow_df([]), _auctions_df([]))
    comps = _rows_to_components(
        [_comp(d, "TAX", 1.0, None) for d in ctx.grid]
    )
    _, forecast = aggregate_forecast(ctx, comps)
    boundary = forecast.filter(pl.col("is_horizon_boundary")).sort("horizon")
    assert boundary.height == 3
    assert boundary["horizon"].to_list() == [7, 14, 30]
    # T+7 边界 = origin+7 内最后一个网格日
    assert boundary["target_date"][0] == date(2026, 11, 9)


def test_aggregate_rejects_outside_grid():
    ctx = _ctx(_cashflow_df([]), _auctions_df([]))
    comps = _rows_to_components([_comp(date(2026, 11, 1), "TAX", 1.0, None)])
    with pytest.raises(RuntimeError, match="outside forecast grid"):
        aggregate_forecast(ctx, comps)


def test_aggregate_empty_components():
    ctx = _ctx(_cashflow_df([]), _auctions_df([]))
    comps, forecast = aggregate_forecast(ctx, _rows_to_components([]))
    assert comps.is_empty() and forecast.is_empty()


def test_annotate_scenario_output_marks_path_and_drivers():
    """机器可读标签：下游不得把情景输出当点位预测消费。"""
    ctx = _ctx(_cashflow_df([]), _auctions_df([]))
    d = ctx.grid[0]
    comps = _rows_to_components([
        _comp(d, "DEBT_ISSUANCE", 100.0, "ISSUE:X"),
        _comp(d, "TAX", 30.0, None),
    ])
    comps, forecast = aggregate_forecast(ctx, comps)
    out = annotate_scenario_output(
        forecast, comps, {"usage": "scenario_anchor", "confidence": "exploratory"}
    )
    assert out["output_type"].unique().to_list() == ["scenario"]
    assert out["usage"].unique().to_list() == ["scenario_anchor"]
    assert out["confidence"].unique().to_list() == ["exploratory"]
    row = out.filter(pl.col("target_date") == d)
    assert row["drivers"][0] == "DEBT_ISSUANCE|TAX"
    # 既有列不变（向后兼容）
    assert row["predicted_tga"][0] == forecast.filter(
        pl.col("target_date") == d
    )["predicted_tga"][0]


# ── ForecastStore 往返 ────────────────────────────────────────────────


def test_forecast_store_roundtrip(tmp_path):
    from chronoforge.storage.meta import MetaStore

    meta = MetaStore(str(tmp_path / "meta"))
    meta.migrate()
    try:
        store = ForecastStore(tmp_path / "data", meta)
        run_id, is_repeat = store.create_run(
            "2026-11-02", "2026-10-29", "tga-forecast-0.3.0", "abc",
            ["base"], [7, 14, 30],
        )
        assert not is_repeat
        comps = _rows_to_components(
            [_comp(date(2026, 11, 3), "TAX", 10.0, None)]
        )
        fcst = pl.DataFrame({
            "target_date": [date(2026, 11, 3)], "horizon": [7],
            "is_horizon_boundary": [True], "predicted_tga": [1210.0],
            "lower": [1200.0], "upper": [1220.0], "daily_net": [10.0],
            "source_type": ["SEASONAL"],
        })
        n_comp, n_fcst = store.write_outputs(run_id, date(2026, 11, 2), "base",
                                             comps, fcst)
        assert (n_comp, n_fcst) == (1, 1)
        store.finalize_run(run_id, "SUCCESS", output_hash="x")

        # 幂等复用 + 重复运行 is_repeat
        assert store.find_success_run(
            "2026-11-02", "2026-10-29", "tga-forecast-0.3.0", "abc",
            ["base"], [7, 14, 30],
        ) == run_id
        run_id2, is_repeat2 = store.create_run(
            "2026-11-02", "2026-10-29", "tga-forecast-0.3.0", "abc",
            ["base"], [7, 14, 30],
        )
        assert is_repeat2 and run_id2 != run_id

        # 同 run 重写覆盖（幂等）
        assert store.write_outputs(run_id, date(2026, 11, 2), "base",
                                   comps, fcst) == (1, 1)

        latest = store.latest_success_run()
        assert latest is not None
        assert latest["forecast_run_id"] in (run_id, run_id2)
        got = store.get_run(run_id)
        assert got["status"] == "SUCCESS"
        assert json.loads(got["horizons_json"]) == [7, 14, 30]

        read_comps = store.read("tga_forecast_components")
        assert read_comps.filter(pl.col("run_id") == run_id).height == 1
        assert "run_date" in read_comps.columns
    finally:
        meta.close()
