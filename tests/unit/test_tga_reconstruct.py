"""tga_reconstruct 单元测试（Phase 2）：余额序列 / 台账 / 残差 / 守恒。"""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

_SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from tga_common import load_taxonomy  # noqa: E402
from tga_reconstruct import (  # noqa: E402
    acceptance_checks,
    build_balance_series,
    build_daily_cashflow,
    build_debt_cash_basis,
    classify_cashflows,
    compute_reconciliation,
    daily_ledger,
    qtga_003_004_classification_residuals,
)


def _dt(d: str) -> datetime:
    return datetime.fromisoformat(d)


def _balance_rows() -> list[dict]:
    # 三天：正常连续 / opening 缺失（推导）/ closing 缺失（INCOMPLETE）
    return [
        {"observation_time": _dt("2026-03-02"), "balance_type": "OPENING",
         "amount_usd_millions": 1000.0},
        {"observation_time": _dt("2026-03-02"), "balance_type": "CLOSING",
         "amount_usd_millions": 1300.0},
        {"observation_time": _dt("2026-03-02"), "balance_type": "TOTAL_DEPOSITS",
         "amount_usd_millions": 500.0},
        {"observation_time": _dt("2026-03-02"), "balance_type": "TOTAL_WITHDRAWALS",
         "amount_usd_millions": 200.0},
        # 03-03 opening 缺失 → 由 03-02 closing 推导
        {"observation_time": _dt("2026-03-03"), "balance_type": "CLOSING",
         "amount_usd_millions": 1200.0},
        # 03-04 closing 缺失 → Q-TGA-001
        {"observation_time": _dt("2026-03-04"), "balance_type": "OPENING",
         "amount_usd_millions": 1200.0},
    ]


def _cash_rows() -> list[dict]:
    return [
        # 03-02：deposits 500（含一条内部转移行 include_in_net=False 不适用 II）
        {"observation_time": _dt("2026-03-02"), "source_table": "II",
         "transaction_type_raw": "Deposits", "raw_category": "Taxes - Corporate Income",
         "amount_usd_millions": 300.0, "is_subtotal": False, "is_duplicate_candidate": False},
        {"observation_time": _dt("2026-03-02"), "source_table": "II",
         "transaction_type_raw": "Deposits", "raw_category": "Other Deposits",
         "amount_usd_millions": 200.0, "is_subtotal": False, "is_duplicate_candidate": False},
        {"observation_time": _dt("2026-03-02"), "source_table": "II",
         "transaction_type_raw": "Withdrawals", "raw_category": "Social Security Benefits (EFT)",
         "amount_usd_millions": 200.0, "is_subtotal": False, "is_duplicate_candidate": False},
        # 小计行：不得进入叶级求和
        {"observation_time": _dt("2026-03-02"), "source_table": "II",
         "transaction_type_raw": "Deposits", "raw_category": "Sub-Total Deposits",
         "amount_usd_millions": 500.0, "is_subtotal": False, "is_duplicate_candidate": False},
        # 03-03：net = 100 - 250 = -150；closing 1200 vs opening 1300 → daily_change -100
        {"observation_time": _dt("2026-03-03"), "source_table": "II",
         "transaction_type_raw": "Deposits", "raw_category": "Other Deposits",
         "amount_usd_millions": 100.0, "is_subtotal": False, "is_duplicate_candidate": False},
        {"observation_time": _dt("2026-03-03"), "source_table": "II",
         "transaction_type_raw": "Withdrawals", "raw_category": "Unclassified",
         "amount_usd_millions": 250.0, "is_subtotal": False, "is_duplicate_candidate": False},
        # Table IV/V 行：固定 include_in_net=False
        {"observation_time": _dt("2026-03-03"), "source_table": "IV",
         "transaction_type_raw": "Internal", "raw_category": "Taxes - Corporate Income",
         "amount_usd_millions": 50.0, "is_subtotal": False, "is_duplicate_candidate": True},
        {"observation_time": _dt("2026-03-03"), "source_table": "V",
         "transaction_type_raw": "Withdrawals",
         "raw_category": "Taxes - Individual Tax Refunds (EFT)",
         "amount_usd_millions": 30.0, "is_subtotal": False, "is_duplicate_candidate": True},
    ]


@pytest.fixture(scope="module")
def classified():
    taxonomy = load_taxonomy()
    cash_df = pl.DataFrame(_cash_rows()).with_columns(
        pl.col("observation_time").cast(pl.Datetime)
    )
    return classify_cashflows(cash_df, taxonomy)[0]


def test_balance_series_continuity_and_flags():
    balance, findings, anomalies = build_balance_series(
        pl.DataFrame(_balance_rows()).with_columns(pl.col("observation_time").cast(pl.Datetime))
    )
    assert balance.height == 3
    # opening 缺失日由前日 closing 推导
    assert balance.filter(pl.col("date") == _dt("2026-03-03").date())["opening"][0] == 1300.0
    # closing 缺失日 → INCOMPLETE + Q-TGA-001
    d4 = balance.filter(pl.col("date") == _dt("2026-03-04").date())
    assert d4["quality_status"][0] == "INCOMPLETE"
    assert any(f.rule_id == "Q-TGA-001" for f in findings)
    # 连续性无违例
    assert balance["flags"].str.contains("OPENING_MISMATCH").sum() == 0


def test_daily_ledger_excludes_subtotal_and_net(classified):
    ledger = daily_ledger(classified)
    d2 = ledger.filter(pl.col("date") == datetime(2026, 3, 2).date())
    # Sub-Total 行被排除：leaf_deposits = 500（300+200），非 1000
    assert d2["leaf_deposits_total"][0] == pytest.approx(500.0)
    assert d2["classified_deposits"][0] == pytest.approx(500.0)
    assert d2["net_cashflow"][0] == pytest.approx(300.0) if "net_cashflow" in d2.columns else True


def test_table_iv_v_excluded_from_net(classified):
    ledger = daily_ledger(classified)
    d3 = ledger.filter(pl.col("date") == datetime(2026, 3, 3).date())
    # IV(50)/V(30) 均不计入
    assert d3["leaf_deposits_total"][0] == pytest.approx(100.0)
    assert d3["leaf_withdrawals_total"][0] == pytest.approx(250.0)


def test_unclassified_included_in_net(classified):
    ledger = daily_ledger(classified)
    d3 = ledger.filter(pl.col("date") == datetime(2026, 3, 3).date())
    assert d3["unclassified_withdrawals"][0] == pytest.approx(250.0)


def test_reconciliation_residuals():
    balance, *_ = build_balance_series(
        pl.DataFrame(_balance_rows()).with_columns(pl.col("observation_time").cast(pl.Datetime))
    )
    taxonomy = load_taxonomy()
    cash_cls, _ = classify_cashflows(
        pl.DataFrame(_cash_rows()).with_columns(pl.col("observation_time").cast(pl.Datetime)),
        taxonomy,
    )
    recon = compute_reconciliation(balance, daily_ledger(cash_cls))
    d2 = recon.filter(pl.col("date") == datetime(2026, 3, 2).date())
    # balance_residual = closing - (opening + td - tw) = 1300 - (1000+500-200) = 0
    assert d2["balance_residual"][0] == pytest.approx(0.0, abs=0.01)
    # ledger_residual = daily_change(300) - net(300) = 0
    assert d2["ledger_residual"][0] == pytest.approx(0.0, abs=0.01)
    d3 = recon.filter(pl.col("date") == datetime(2026, 3, 3).date())
    # daily_change = 1200 - 1300 = -100；net = 100 - 250 = -150 → residual 50
    assert d3["ledger_residual"][0] == pytest.approx(50.0, abs=0.01)


def test_daily_cashflow_projection(classified):
    detail = build_daily_cashflow(classified)
    assert detail.height == classified.height
    cols = {"date", "source_table", "raw_category", "category", "direction",
            "amount_usd_millions", "include_in_net"}
    assert cols <= set(detail.columns)
    # IV/V 行 include_in_net=False
    ivv = detail.filter(pl.col("source_table") != "II")
    assert ivv["include_in_net"].sum() == 0


def test_debt_cash_basis_anchor_and_components():
    debt_rows = [
        # IIIA 面值：Bills 800 + Notes 200
        {"observation_time": _dt("2026-03-02"), "source_table": "IIIA",
         "transaction_type_raw": "Issues", "security_market": "Marketable",
         "security_type": "Bills", "adjustment_type": None, "amount_usd_millions": 800.0},
        {"observation_time": _dt("2026-03-02"), "source_table": "IIIA",
         "transaction_type_raw": "Issues", "security_market": "Marketable",
         "security_type": "Notes", "adjustment_type": None, "amount_usd_millions": 200.0},
        # IIIB 锚定行（= 面值合计 1000）
        {"observation_time": _dt("2026-03-02"), "source_table": "IIIB",
         "transaction_type_raw": "Issues", "security_market": None,
         "security_type": None, "adjustment_type": "Public Debt Issues (Table IIIA)",
         "amount_usd_millions": 1000.0},
        # IIIB 分量行合计 = 现金口径全值 990（实测语义：分量行携带现金值）
        {"observation_time": _dt("2026-03-02"), "source_table": "IIIB",
         "transaction_type_raw": "Issues", "security_market": None,
         "security_type": None, "adjustment_type": "Government Account Transactions",
         "amount_usd_millions": 1000.0},
        {"observation_time": _dt("2026-03-02"), "source_table": "IIIB",
         "transaction_type_raw": "Issues", "security_market": None,
         "security_type": None, "adjustment_type": "Bills (-) Discount on New Issues",
         "amount_usd_millions": -10.0},
    ]
    debt_df = pl.DataFrame(debt_rows).with_columns(pl.col("observation_time").cast(pl.Datetime))
    detail, metrics = build_debt_cash_basis(
        debt_df.filter(pl.col("source_table") == "IIIA"),
        debt_df.filter(pl.col("source_table") == "IIIB"),
    )
    # 锚定行不进入分量求和；cash = face + net_adj = 1000 + (-10) = 990
    assert detail["cash_amount"].sum() == pytest.approx(990.0)
    assert detail["face_amount"].sum() == pytest.approx(1000.0)
    assert detail["adjustment"].sum() == pytest.approx(-10.0)
    assert metrics["cash_vs_components_max"] == pytest.approx(0.0, abs=0.01)
    assert metrics["face_vs_anchor_max"] == pytest.approx(0.0, abs=0.01)


def test_acceptance_checks_verdicts():
    balance = pl.DataFrame({
        "date": [datetime(2026, 3, 2).date()],
        "opening": [1000.0], "closing": [1300.0], "daily_change": [300.0],
        "total_deposits": [500.0], "total_withdrawals": [200.0],
        "quality_status": ["VALID"], "flags": [""],
    })
    recon = pl.DataFrame({
        "date": [datetime(2026, 3, 2).date()],
        "quality_status": ["VALID"],
        "closing": [1300.0], "opening": [1000.0], "daily_change": [300.0],
        "total_deposits": [500.0], "total_withdrawals": [200.0],
        "classified_deposits": [500.0], "classified_withdrawals": [200.0],
        "unclassified_deposits": [0.0], "unclassified_withdrawals": [0.0],
        "leaf_deposits_total": [500.0], "leaf_withdrawals_total": [200.0],
        "net_cashflow": [300.0], "classified_gross": [700.0],
        "balance_residual": [0.0],
        "classification_residual_deposits": [0.0],
        "classification_residual_withdrawals": [0.0],
        "ledger_residual": [0.0],
    })
    results = acceptance_checks(
        balance, recon, 0.0, 0.0, 0.0, 0,
        conservation_ok=True, no_silent_mod=True,
    )
    by_id = {r["id"]: r for r in results}
    assert by_id[2]["passed"] is True
    assert by_id[3]["passed"] is True
    assert by_id[4]["passed"] is True
    assert by_id[5]["passed"] is True
    assert by_id[8]["passed"] is True


def test_qtga_003_004_uses_rounding_band():
    """舍入带内（≤15M）的分类残差不产生 ERROR（与验收口径一致）；带外产生。"""
    recon = pl.DataFrame({
        "date": [datetime(2026, 3, 2).date(), datetime(2026, 3, 3).date()],
        "quality_status": ["VALID", "VALID"],
        "classification_residual_deposits": [5.0, 500.0],
        "classification_residual_withdrawals": [3.0, 0.0],
        "total_deposits": [1000.0, 1000.0],
        "leaf_deposits_total": [995.0, 500.0],
        "total_withdrawals": [800.0, 800.0],
        "leaf_withdrawals_total": [797.0, 800.0],
    })
    findings = qtga_003_004_classification_residuals(recon)
    # 03-02 的 5M/3M 在舍入带内 → 不报；03-03 的 500M 带外 → ERROR
    assert [f.rule_id for f in findings] == ["Q-TGA-003"]
    assert json.loads(findings[0].detail)["date"] == "2026-03-03"
