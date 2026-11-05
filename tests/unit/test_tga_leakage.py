"""TGA Leak Sentinel（Phase 5，§7.2 规则 6 / §7.6 验收 2）。

注入 release_time > data_cutoff 的记录，断言预测输出完全不变；同时验证
Derived 现金流的历史窗口不会吸入 cutoff 之后的数据。
"""

from __future__ import annotations

import sys
from datetime import date, datetime
from pathlib import Path

import polars as pl

_SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from tga_forecast import (  # noqa: E402
    ForecastContext,
    compute_base_components,
    pit_auctions,
)

ORIGIN = date(2026, 11, 2)  # 周一；data_through = 2026-10-29


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


def _balance() -> pl.DataFrame:
    return pl.DataFrame({
        "date": [date(2026, 10, 28), date(2026, 10, 29)],
        "opening": [1100.0, 1100.0], "closing": [1100.0, 1200.0],
        "daily_change": [0.0, 100.0],
        "total_deposits": [None, None], "total_withdrawals": [None, None],
        "quality_status": ["VALID", "VALID"], "flags": ["", ""],
    })


_CASHFLOW_SCHEMA: dict[str, pl.DataType] = {
    "date": pl.Date, "source_table": pl.String, "raw_category": pl.String,
    "category": pl.String, "subcategory": pl.String, "direction": pl.String,
    "amount_usd_millions": pl.Float64, "include_in_net": pl.Boolean,
    "is_subtotal": pl.Boolean, "is_duplicate_candidate": pl.Boolean,
    "classification_confidence": pl.Float64,
}


def _cashflow(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=_CASHFLOW_SCHEMA)


def _auction_row(
    cusip: str, *, issue: date, maturity: date, offering: float,
    accepted: float, release: date,
) -> dict:
    return {
        "auction_key": f"{cusip}:{issue}:RESULT", "cusip": cusip,
        "security_type": "Bill", "security_term": "4-Week",
        "announcement_date": issue, "auction_date": issue,
        "issue_date": issue, "maturity_date": maturity,
        "offering_amount_usd_millions": offering,
        "total_accepted_usd_millions": accepted,
        "interest_rate": None, "high_yield": None, "price_per_100": None,
        "interest_payment_frequency": "None",
        "is_reopening": False, "is_cmb": False, "data_status": "RESULT",
        "release_time": datetime(release.year, release.month, release.day),
    }


def _ctx(auctions: pl.DataFrame, cashflow: pl.DataFrame) -> ForecastContext:
    return ForecastContext(
        ORIGIN, [7, 14, 30], _model(), {}, _balance(), cashflow, auctions,
    )


def _snapshot(components: pl.DataFrame) -> list[tuple]:
    cols = ["target_date", "category", "predicted", "source_type", "event_key"]
    return sorted(
        tuple(row[c] for c in cols)
        for row in components.select(cols).iter_rows(named=True)
    )


def test_leak_sentinel_auction_after_cutoff_changes_nothing():
    """release_time > cutoff 的拍卖记录不得进入预测（§7.2 规则 1/2）。"""
    baseline = _auction_row(
        "A", issue=date(2026, 11, 5), maturity=date(2026, 12, 3),
        offering=100000.0, accepted=100000.0, release=date(2026, 10, 29),
    )
    leaked = _auction_row(
        "B", issue=date(2026, 11, 6), maturity=date(2026, 12, 4),
        offering=500000.0, accepted=500000.0, release=ORIGIN,  # 起点当天发布
    )
    clean = pl.DataFrame([baseline])
    polluted = pl.DataFrame([baseline, leaked])
    empty_cash = _cashflow([])

    ctx_clean = _ctx(clean, empty_cash)
    ctx_polluted = _ctx(polluted, empty_cash)

    assert _snapshot(compute_base_components(ctx_clean)) == _snapshot(
        compute_base_components(ctx_polluted)
    )
    assert pit_auctions(ctx_polluted)["cusip"].to_list() == ["A"]


def test_leak_sentinel_cashflow_after_cutoff_not_in_hist():
    """cutoff 之后的 Derived 现金流不得进入历史统计窗口（§7.2 规则 5）。"""
    future = _cashflow([{
        "date": date(2026, 11, 3), "source_table": "II", "raw_category": "Taxes",
        "category": "TAX", "subcategory": "individual", "direction": "INFLOW",
        "amount_usd_millions": 500000.0, "include_in_net": True,
        "is_subtotal": False, "is_duplicate_candidate": False,
        "classification_confidence": 0.95,
    }])
    ctx = _ctx(pl.DataFrame([]), future)
    assert ctx.through == date(2026, 10, 29)
    assert ctx.hist(("TAX",), "INFLOW").is_empty()
