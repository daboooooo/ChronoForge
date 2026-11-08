"""可结算的前瞻判断：生成、存储与次日计分卡。

设计原则：
- 每条判断都有可观测指标、比较符、阈值、到期日与证伪条件；
- 生成时已经满足的条件不再写成「前瞻」（那是现状，不是判断）；
- 次日（或之后）读取上一份报告里的 judgments，逐条结算：
  已验证 / 已证伪 / 未到期，并给出当时阈值与今天的实际值。
- 只结算已写入 JSON 的判断；旧版本报告没有该字段时明确说明，不补造。
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, timedelta
from typing import Any

OPS: dict[str, Callable[[float, float], bool]] = {
    "<=": lambda a, b: a <= b,
    ">=": lambda a, b: a >= b,
}

UNIT_TEXT = {"B": "B", "bp": "bp", "T": "T"}


def _reserve(facts: dict[str, Any], key: str) -> float | None:
    value = (facts.get("metrics", {}).get("reserve") or {}).get(key)
    return float(value) if value is not None else None


def _funding(facts: dict[str, Any], key: str) -> float | None:
    value = (facts.get("metrics", {}).get("funding") or {}).get(key)
    return float(value) if value is not None else None


def _rule_stat(facts: dict[str, Any], key: str) -> float | None:
    value = (facts.get("rules", {}).get("stats") or {}).get(key)
    return float(value) if value is not None else None


RESOLVERS: dict[str, Callable[[dict[str, Any]], float | None]] = {
    "reserve.chg_4w_b": lambda f: _reserve(f, "chg_4w_b"),
    "reserve.chg_13w_b": lambda f: _reserve(f, "chg_13w_b"),
    "funding.spread_bp": lambda f: _funding(f, "spread_bp"),
    "funding.srf_usage_b": lambda f: _funding(f, "srf_usage_b"),
    "rules.rrp_5dma_b": lambda f: _rule_stat(f, "rrp_5dma_b"),
}


def _next_trading_day(day: date) -> date:
    from chronoforge.quality.calendar import is_trading_day

    candidate = day + timedelta(days=1)
    while not is_trading_day(candidate):
        candidate += timedelta(days=1)
    return candidate


def _daily_verify_day(day: date) -> date:
    """日频序列的验证日：日频数据在报告时点结构滞后 2 个交易日。"""
    return _next_trading_day(_next_trading_day(day))


def _next_h41_day(day: date) -> date:
    """下一份 H.4.1 的发布日（周四，含当天之后的下一个周四）。"""
    candidate = day + timedelta(days=1)
    while candidate.weekday() != 3:  # Thursday
        candidate += timedelta(days=1)
    return candidate


def _weekly_verify_day(day: date) -> date:
    """周频判断的验证日：周四 16:30 ET 发布，次日（周五）报告才可见。"""
    release = _next_h41_day(day)
    candidate = release + timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate += timedelta(days=1)
    return candidate


def _fmt_value(value: float | None, unit: str, digits: int = 1) -> str:
    if value is None:
        return "缺失"
    if value == 0:
        value = 0.0
    return f"{value:+.{digits}f} {UNIT_TEXT.get(unit, unit)}" if value else f"{0:.{digits}f} {unit}"


def _threshold_text(op: str, threshold: float, unit: str) -> str:
    sign = "+" if threshold > 0 else ""
    return f"{op} {sign}{threshold:g} {unit}"


def build_judgments(facts: dict[str, Any], report_day: date | None = None) -> list[dict[str, Any]]:
    """生成下一交易日可结算的前瞻判断；已满足的条件不写入。"""
    day = report_day or date.fromisoformat(facts["report_date"])

    defs: list[dict[str, Any]] = [
        {
            "id": "reserve_4w_drawdown",
            "claim": "准备金 4 周变化达到 −125 B（抽水升级）",
            "metric": "reserve.chg_4w_b",
            "op": "<=",
            "threshold": -125.0,
            "unit": "B",
            "deadline": _weekly_verify_day(day).isoformat(),
            "falsify": "到期时 4 周变化仍高于 −125 B",
            "basis": "EV_RESERVE_DRAWDOWN 阈值（4 周）",
        },
        {
            "id": "reserve_13w_drawdown",
            "claim": "准备金 13 周变化达到 −200 B（中期抽水）",
            "metric": "reserve.chg_13w_b",
            "op": "<=",
            "threshold": -200.0,
            "unit": "B",
            "deadline": _weekly_verify_day(day).isoformat(),
            "falsify": "到期时 13 周变化仍高于 −200 B",
            "basis": "EV_RESERVE_DRAWDOWN 阈值（13 周）",
        },
        {
            "id": "funding_spread_12bp",
            "claim": "SOFR-EFFR 达到 12 bp（走廊预警）",
            "metric": "funding.spread_bp",
            "op": ">=",
            "threshold": 12.0,
            "unit": "bp",
            "deadline": _daily_verify_day(day).isoformat(),
            "falsify": "下一交易日 SOFR-EFFR 仍低于 12 bp",
            "basis": "EV_SOFR_EFFR 预警线",
        },
        {
            "id": "srf_usage_3b",
            "claim": "SRF 单日用量达到 3 B（边际工具切换）",
            "metric": "funding.srf_usage_b",
            "op": ">=",
            "threshold": 3.0,
            "unit": "B",
            "deadline": _daily_verify_day(day).isoformat(),
            "falsify": "下一交易日 SRF 用量仍低于 3 B",
            "basis": "EV_SRF_USAGE 阈值",
        },
        {
            "id": "onrrp_restore_20b",
            "claim": "ON RRP 5 日均值达到 20 B（缓冲恢复）",
            "metric": "rules.rrp_5dma_b",
            "op": ">=",
            "threshold": 20.0,
            "unit": "B",
            "deadline": _daily_verify_day(day).isoformat(),
            "falsify": "下一交易日 ON RRP 5 日均值仍低于 20 B",
            "basis": "BG_ONRRP_RECOVER 阈值",
        },
    ]

    out: list[dict[str, Any]] = []
    for item in defs:
        resolver = RESOLVERS[item["metric"]]
        actual = resolver(facts)
        if actual is None:
            continue  # 指标缺失时不下判断，避免无法结算的空头承诺
        if OPS[item["op"]](actual, float(item["threshold"])):
            continue  # 条件已满足 → 这是现状，不是前瞻
        out.append({
            **item,
            "created_on": day.isoformat(),
            "value_at_creation": round(actual, 3),
            "threshold_text": _threshold_text(item["op"], float(item["threshold"]), item["unit"]),
        })
    return out


def evaluate_judgment(
    judgment: dict[str, Any], facts: dict[str, Any], report_day: date
) -> dict[str, Any]:
    metric = str(judgment.get("metric") or "")
    resolver = RESOLVERS.get(metric)
    actual = resolver(facts) if resolver else None
    op = str(judgment.get("op") or "")
    threshold = judgment.get("threshold")
    deadline = judgment.get("deadline")
    unit = str(judgment.get("unit") or "")

    status = "pending"
    note = ""
    if actual is None or resolver is None or op not in OPS or threshold is None:
        status = "unsettled"
        note = "指标缺失或判断字段不完整，无法结算"
    elif OPS[op](actual, float(threshold)):
        status = "verified"
        note = "阈值已达成"
    elif deadline and report_day.isoformat() >= str(deadline):
        status = "falsified"
        note = "已到期且未达成"
    else:
        status = "pending"
        remaining = (
            (date.fromisoformat(str(deadline)) - report_day).days
            if deadline else None
        )
        note = f"距到期 {remaining} 天" if remaining is not None else "未设到期日"

    return {
        "id": judgment.get("id"),
        "claim": judgment.get("claim"),
        "threshold_text": judgment.get("threshold_text"),
        "deadline": deadline,
        "falsify": judgment.get("falsify"),
        "basis": judgment.get("basis"),
        "created_on": judgment.get("created_on"),
        "value_at_creation": judgment.get("value_at_creation"),
        "unit": unit,
        "actual": round(actual, 3) if actual is not None else None,
        "actual_text": _fmt_value(actual, unit),
        "status": status,
        "note": note,
    }


STATUS_LABEL = {
    "verified": "已验证",
    "falsified": "已证伪",
    "pending": "未到期",
    "unsettled": "无法结算",
}

STATUS_ORDER = ("verified", "falsified", "pending", "unsettled")


def build_scorecard(
    previous: dict[str, Any] | None, facts: dict[str, Any], report_day: date
) -> dict[str, Any]:
    """读取上一份报告的 judgments 并逐条结算。"""
    if not previous:
        return {
            "available": False,
            "reason": "无上一份报告（首次生成或上一交易日文件缺失），本日无可结算判断",
            "prev_date": None,
            "items": [],
            "summary": {key: 0 for key in STATUS_ORDER},
        }
    items = [
        evaluate_judgment(item, facts, report_day)
        for item in (previous.get("judgments") or [])
        if isinstance(item, dict)
    ]
    if not items:
        return {
            "available": False,
            "reason": (
                f"上一份报告（{previous.get('report_date')}）未记录可结算判断"
                "（旧版本或当日无未满足条件），从本篇起开始记录"
            ),
            "prev_date": previous.get("report_date"),
            "items": [],
            "summary": {key: 0 for key in STATUS_ORDER},
        }
    summary = {key: 0 for key in STATUS_ORDER}
    for item in items:
        summary[str(item["status"])] += 1
    return {
        "available": True,
        "reason": "",
        "prev_date": previous.get("report_date"),
        "items": items,
        "summary": summary,
    }
