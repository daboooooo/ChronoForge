"""可结算判断与昨日计分卡的单元测试。"""

from __future__ import annotations

from datetime import date

from chronoforge.report import narrative
from chronoforge.report.judgments import build_judgments, build_scorecard, evaluate_judgment


def _facts(
    chg_4w: float | None = -99.6,
    chg_13w: float | None = -150.0,
    spread: float | None = 4.0,
    srf: float | None = 0.0,
    rrp_5dma: float | None = 0.2,
    report_date: str = "2026-11-09",
) -> dict:
    return {
        "report_date": report_date,
        "metrics": {
            "reserve": {"chg_4w_b": chg_4w, "chg_13w_b": chg_13w, "level_t": 2.93},
            "funding": {"spread_bp": spread, "srf_usage_b": srf},
        },
        "rules": {"stats": {"rrp_5dma_b": rrp_5dma}},
    }


def test_build_judgments_emits_unsatisfied_conditions_with_falsification() -> None:
    items = build_judgments(_facts(), date(2026, 11, 9))
    ids = {item["id"] for item in items}
    assert ids == {
        "reserve_4w_drawdown", "reserve_13w_drawdown", "funding_spread_12bp",
        "srf_usage_3b", "onrrp_restore_20b",
    }
    by_id = {item["id"]: item for item in items}
    assert by_id["reserve_4w_drawdown"]["deadline"] == "2026-11-13"  # 周四发布、周五可见
    assert by_id["funding_spread_12bp"]["deadline"] == "2026-11-11"  # 日频滞后 2 个交易日
    assert by_id["reserve_4w_drawdown"]["threshold_text"] == "<= -125 B"
    assert by_id["reserve_4w_drawdown"]["falsify"]
    assert by_id["reserve_4w_drawdown"]["value_at_creation"] == -99.6


def test_build_judgments_skips_already_satisfied_conditions() -> None:
    items = build_judgments(_facts(chg_4w=-130.0, spread=13.0), date(2026, 11, 9))
    ids = {item["id"] for item in items}
    assert "reserve_4w_drawdown" not in ids  # 已满足 → 是现状，不是前瞻
    assert "funding_spread_12bp" not in ids
    assert "reserve_13w_drawdown" in ids


def test_evaluate_verified_falsified_and_pending() -> None:
    judgment = build_judgments(_facts(), date(2026, 11, 9))[0]
    assert judgment["id"] == "reserve_4w_drawdown"

    verified = evaluate_judgment(judgment, _facts(chg_4w=-130.0), date(2026, 11, 10))
    assert verified["status"] == "verified"
    assert verified["actual_text"] == "-130.0 B"

    pending = evaluate_judgment(judgment, _facts(chg_4w=-100.0), date(2026, 11, 11))
    assert pending["status"] == "pending"
    assert "距到期 2 天" in pending["note"]

    falsified = evaluate_judgment(judgment, _facts(chg_4w=-100.0), date(2026, 11, 13))
    assert falsified["status"] == "falsified"


def test_scorecard_without_previous_explains_instead_of_blank() -> None:
    card = build_scorecard(None, _facts(), date(2026, 11, 9))
    assert card["available"] is False
    assert "无上一份报告" in card["reason"]
    assert card["items"] == []


def test_scorecard_flags_old_report_without_judgments() -> None:
    previous = {"report_date": "2026-11-08", "judgments": []}
    card = build_scorecard(previous, _facts(), date(2026, 11, 9))
    assert card["available"] is False
    assert "未记录可结算判断" in card["reason"]
    assert "2026-11-08" in card["reason"]


def test_scorecard_settles_previous_judgments() -> None:
    previous = {
        "report_date": "2026-11-09",
        "judgments": build_judgments(_facts(), date(2026, 11, 9)),
    }
    card = build_scorecard(previous, _facts(chg_4w=-130.0), date(2026, 11, 10))
    assert card["available"] is True
    assert card["summary"]["verified"] == 1
    assert card["summary"]["pending"] == 4
    verified = next(item for item in card["items"] if item["status"] == "verified")
    assert verified["value_at_creation"] == -99.6
    assert verified["actual_text"] == "-130.0 B"


def test_scorecard_narrative_lists_threshold_and_today_value() -> None:
    previous = {
        "report_date": "2026-11-09",
        "judgments": build_judgments(_facts(), date(2026, 11, 9)),
    }
    facts = _facts(chg_4w=-130.0)
    facts["scorecard"] = build_scorecard(previous, facts, date(2026, 11, 10))
    lines = narrative._scorecard_bullets(facts)
    joined = "\n".join(lines)
    assert "已验证 1" in joined
    assert "当时阈值 <= -125 B" in joined
    assert "今天 -130.0 B" in joined
    assert "证伪条件" in joined
