"""事实装配：读取现有链路、新鲜度、缺口与批次状态，产出报告 JSON。

数据来源：
- ``visualization/export_data.py::build_payload``（canonical/derived 只读）
- ``meta/chronoforge.db`` 的 quality_flags（源侧异常）
- ``var/run/daily_update.json``（最近一次 07:40 批次状态）
"""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, cast

from chronoforge.cli._wiring import open_meta
from chronoforge.config.settings import Settings
from chronoforge.quality.calendar import (
    is_early_close,
    next_early_close,
    next_market_holiday,
)
from chronoforge.report.events import (
    EVENTS_PATH,
    EventConfigError,
    load_known_events,
    summarize_events,
)
from chronoforge.report.freshness import build_freshness, build_gaps, classify_gaps
from chronoforge.report.judgments import build_judgments, build_scorecard
from chronoforge.report.metrics import compute_metrics, to_pairs
from chronoforge.report.rules import backtest_rules
from chronoforge.report.tax_calendar import tax_period_dates

PROJECT_ROOT = Path(__file__).resolve().parents[3]
EXPORT_DATA_PATH = PROJECT_ROOT / "visualization" / "export_data.py"
BATCH_STATUS_PATH = PROJECT_ROOT / "var" / "run" / "daily_update.json"
REPORT_DIR = PROJECT_ROOT / "reports" / "daily_liquidity"
_EXPORT_MODULE: Any | None = None


def load_export_module() -> Any:
    """按文件路径加载 export_data（visualization/ 不是包）。"""
    global _EXPORT_MODULE
    if _EXPORT_MODULE is not None:
        return _EXPORT_MODULE
    spec = importlib.util.spec_from_file_location(
        "chronoforge_visualization_export", EXPORT_DATA_PATH
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载导出模块: {EXPORT_DATA_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    _EXPORT_MODULE = module
    return module


def build_payload() -> dict[str, Any]:
    return cast(dict[str, Any], load_export_module().build_payload())


def load_batch_status() -> dict[str, Any] | None:
    if not BATCH_STATUS_PATH.exists():
        return None
    try:
        data = json.loads(BATCH_STATUS_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"status": "unknown", "note": "批次状态文件无法读取"}
    if not isinstance(data, dict):
        return {"status": "unknown", "note": "批次状态文件格式异常"}
    return data


def _source_anomalies(report_day: date, window_days: int = 30) -> list[dict[str, Any]]:
    """读取最近窗口内的 Q-NORM-SRC-ANOMALY 质量 flag（报告侧缺口来源）。"""
    settings = Settings.load()
    meta = open_meta(settings)
    start = (report_day - timedelta(days=window_days)).isoformat()
    try:
        rows = meta.connection.execute(
            "SELECT dataset_id, COUNT(*) AS n, MAX(created_at) AS last_at, "
            "MAX(detail) AS detail FROM quality_flags "
            "WHERE rule_id = 'Q-NORM-SRC-ANOMALY' AND created_at >= ? "
            "GROUP BY dataset_id ORDER BY last_at DESC",
            (start,),
        ).fetchall()
    finally:
        meta.close()
    out: list[dict[str, Any]] = []
    for dataset_id, count, last_at, detail in rows:
        out.append({
            "kind": "source_anomaly",
            "dataset_id": dataset_id,
            "count": int(count),
            "last_at": last_at,
            "detail": detail,
        })
    return out


def _quantile(sorted_values: list[float], q: float) -> float:
    """线性插值分位（输入已排序）。"""
    n = len(sorted_values)
    if n == 1:
        return sorted_values[0]
    pos = (n - 1) * q
    lo = int(pos)
    hi = min(lo + 1, n - 1)
    frac = pos - lo
    return sorted_values[lo] * (1 - frac) + sorted_values[hi] * frac


def _rolling_bands(
    t: list[Any], v: list[float], start: date, days: int = 365 * 5
) -> dict[str, list[Any]]:
    """滚动 5 年 P20/P50/P80 分位带（只输出展示窗口内的点）。"""
    dates = [date.fromisoformat(str(x)[:10]) for x in t]
    out: dict[str, list[Any]] = {"t": [], "p20": [], "p50": [], "p80": []}
    lo = 0
    for i, d in enumerate(dates):
        if d < start:
            continue
        while dates[lo] < d - timedelta(days=days):
            lo += 1
        window = sorted(v[lo : i + 1])
        out["t"].append(str(t[i])[:10])
        out["p20"].append(round(_quantile(window, 0.2), 3))
        out["p50"].append(round(_quantile(window, 0.5), 3))
        out["p80"].append(round(_quantile(window, 0.8), 3))
    return out


def _turning_point(
    t: list[Any], v: list[float], back: int = 26
) -> dict[str, Any] | None:
    """最近一次 4 周动量变号（周序列），用于标注转折日。"""
    if len(v) < 6:
        return None
    chg = [v[i] - v[i - 4] for i in range(4, len(v))]
    for i in range(len(chg) - 1, max(len(chg) - back, 0), -1):
        if (chg[i] > 0) != (chg[i - 1] > 0):
            idx = i + 4
            return {
                "t": str(t[idx])[:10],
                "v": v[idx],
                "label": "4 周动量转正" if chg[i] > 0 else "4 周动量转负",
            }
    return None


def _value_between(
    points: list[tuple[date, float]], start: date, end: date
) -> tuple[date, float] | None:
    """[start, end] 内最后一个观测。"""
    found = None
    for d, v in points:
        if start <= d <= end:
            found = (d, v)
        elif d > end:
            break
    return found


def _tax_effect(
    points: list[tuple[date, float]], tax_dates: list[date]
) -> dict[str, Any]:
    """税期前后 TGA 变化（B）：截止日前最后观测 → 截止日后第 5 个自然日附近。"""
    deltas: list[float] = []
    for d in tax_dates:
        base = _value_between(points, d - timedelta(days=3), d - timedelta(days=1))
        after = _value_between(points, d + timedelta(days=3), d + timedelta(days=7))
        if base and after:
            deltas.append(after[1] - base[1])
    if not deltas:
        return {"n": 0, "avg_chg_b": None}
    return {"n": len(deltas), "avg_chg_b": round(sum(deltas) / len(deltas) / 1000.0, 1)}


def _chart_series(payload: dict[str, Any], report_day: date,
                  rrp_zero: bool = False) -> dict[str, Any]:
    """图表数据子集（沿用站点现有序列形状，只做裁剪/换算）。"""
    hist_start = report_day - timedelta(days=365 * 5)

    def _window(block: dict[str, Any], start: date) -> dict[str, list[Any]]:
        pairs = [(t, v) for t, v in zip(
            block.get("t", []), block.get("v", []), strict=False
        ) if date.fromisoformat(str(t)[:10]) >= start]
        return {"t": [p[0] for p in pairs], "v": [float(p[1]) for p in pairs]}

    wres = _window(payload["series"]["WRESBAL"], hist_start)
    reserve_t = [str(x)[:10] for x in wres["t"]]
    reserve_v = [round(v / 1e6, 3) for v in wres["v"]]
    full = payload["series"]["WRESBAL"]
    full_t = list(full.get("t", []))
    full_v = [float(v) / 1e6 for v in full.get("v", [])]
    reserve_chart = {
        "t": reserve_t,
        "v": reserve_v,
        "thresholds": [3.0, 3.2],
        "bands": _rolling_bands(full_t, full_v, hist_start),
        "turning": _turning_point(reserve_t, reserve_v),
    }

    pool = payload["weekly"]["three_pool"]
    pool_start = report_day - timedelta(days=365 * 2)
    pool_chart: dict[str, Any] = {
        "t": [t for t in pool["t"] if date.fromisoformat(t) >= pool_start],
    }
    for key in ("reserve", "tga", "onrrp"):
        pool_chart[key] = [
            round(float(v), 3)
            for t, v in zip(pool["t"], pool[key], strict=False)
            if date.fromisoformat(t) >= pool_start
        ]
    zero_since = None
    onrrp_values = [float(v) for v in pool["onrrp"]]
    if onrrp_values and onrrp_values[-1] < 0.001:
        start = len(onrrp_values) - 1
        while start > 0 and onrrp_values[start - 1] < 0.001:
            start -= 1
        zero_since = str(pool["t"][start])[:10]
    pool_chart["zero_since"] = zero_since

    decomp = payload["weekly"]["decomp"][-13:]
    decomp_chart = {
        "t": [str(r["t"]) for r in decomp],
        "series": {
            key: [round(float(r[key]), 2) for r in decomp]
            for key in ("dR", "dA", "tga", "onrrp", "cash", "other")
        },
    }

    sofr = dict(to_pairs(payload["series"].get("SOFR")))
    effr = dict(to_pairs(payload["series"].get("EFFR")))
    common = sorted(set(sofr) & set(effr))
    fund_start = report_day - timedelta(days=365)
    spread_points = [
        (d, round((sofr[d] - effr[d]) * 100, 2)) for d in common if d >= fund_start
    ]
    srf = to_pairs(payload["series"].get("SRF_USAGE"))
    srf_points = [(d, v) for d, v in srf if d >= fund_start]
    srf_by_day = {d: float(v) for d, v in srf_points}
    coincident = [
        d.isoformat() for d, v in spread_points
        if v >= 12.0 and srf_by_day.get(d, 0.0) > 0.0
    ]
    srf_first = next((d for d, v in srf_points if v >= 3.0), None)
    srf_peak = max(srf_points, key=lambda item: item[1]) if srf_points else None
    funding_chart = {
        "t": [d.isoformat() for d, _ in spread_points],
        "spread": [v for _, v in spread_points],
        "srf_t": [d.isoformat() for d, _ in srf_points],
        "srf": [float(v) for _, v in srf_points],
        "thresholds": {"spread_warn": 12.0, "spread_danger": 17.0, "srf_warn": 3.0},
        "coincident": coincident,
        "stats": {
            "days_spread_ge_warn": sum(1 for _, v in spread_points if v >= 12.0),
            "days_srf_ge_warn": sum(1 for _, v in srf_points if v >= 3.0),
            "days_both": len(coincident),
            "srf_first_ge_warn": srf_first.isoformat() if srf_first else None,
            "srf_peak": (
                {"t": srf_peak[0].isoformat(), "v": round(float(srf_peak[1]), 2)}
                if srf_peak else None
            ),
        },
    }

    tga = to_pairs(payload["tga"].get("daily"))
    tga_start = report_day - timedelta(days=365)
    tga_points = [(d, v) for d, v in tga if d >= tga_start]
    tax_dates = tax_period_dates(tga_start, report_day)
    next_tax = tax_period_dates(
        report_day + timedelta(days=1), report_day + timedelta(days=120)
    )
    daily_chart = {
        "t": [d.isoformat() for d, _ in tga_points],
        "v": [round(v / 1000.0, 3) for _, v in tga_points],
        "tax_dates": [d.isoformat() for d in tax_dates],
        "next_tax": next_tax[0].isoformat() if next_tax else None,
        "tax_effect": _tax_effect(tga_points, tax_dates),
    }
    fc = payload["tga"].get("forecast", {})
    run_date = str(fc.get("run_date"))[:10] if fc.get("run_date") else None
    daily_chart["forecast_origin"] = run_date
    scenarios = {}
    for name, block in fc.get("scenarios", {}).items():
        scenarios[name] = {
            "t": [str(d) for d in block.get("t", [])],
            "v": [round(float(v) / 1000.0, 3) for v in block.get("v", [])],
            "lo": [round(float(v) / 1000.0, 3) for v in block.get("lo", [])],
            "hi": [round(float(v) / 1000.0, 3) for v in block.get("hi", [])],
        }
    return {
        "reserve": reserve_chart,
        "three_pool": {**pool_chart, "onrrp_zero": rrp_zero},
        "decomp": {**decomp_chart, "onrrp_zero": rrp_zero},
        "funding": funding_chart,
        "tga_daily": daily_chart,
        "tga_forecast": {"run_date": fc.get("run_date"), "scenarios": scenarios},
        "reserve_forecast": payload.get("reserve_forecast", {}).get("forecast", []),
    }


def load_events_block(report_day: date) -> dict[str, Any]:
    """读取 FOMC/CPI/NFP 事件配置；配置损坏时降级为显式缺口，不阻断报告。"""
    try:
        return summarize_events(load_known_events(), report_day)
    except EventConfigError as exc:
        return {
            "config_path": str(EVENTS_PATH),
            "error": str(exc),
            "horizon_days": 90,
            "upcoming_count": 0,
            "tentative_upcoming": 0,
            "next_by_type": {},
            "items": [],
        }


def _calendar_block(report_day: date) -> dict[str, Any]:
    """报告日与近期的交易日历状态（半日市/假期标注）。"""
    early = is_early_close(report_day)
    nxt_early = next_early_close(report_day + timedelta(days=1))
    nxt_holiday = next_market_holiday(report_day)
    return {
        "early_close": early,
        "early_close_time_et": "13:00" if early else None,
        "next_early_close": nxt_early.isoformat() if nxt_early else None,
        "next_holiday": nxt_holiday.isoformat() if nxt_holiday else None,
    }


def _freshness_summary(rows: list[Any]) -> dict[str, Any]:
    core = [r for r in rows if r.core]
    return {
        "core_total": len(core),
        "core_ok": sum(1 for r in core if r.status == "ok"),
        "core_lagged": sum(1 for r in core if r.status == "lagged"),
        "core_missing": sum(1 for r in core if r.status == "missing"),
        "total": len(rows),
        "ok": sum(1 for r in rows if r.status == "ok"),
        "lagged": sum(1 for r in rows if r.status == "lagged"),
        "missing": sum(1 for r in rows if r.status == "missing"),
    }


def load_previous_report(report_day: date, report_dir: Path | None = None) -> dict[str, Any] | None:
    """读取报告日之前最近一份日报 JSON（用于昨日对比与新颖度检查）。"""
    folder = Path(report_dir) if report_dir else REPORT_DIR
    if not folder.exists():
        return None
    candidates: list[tuple[date, Path]] = []
    for path in folder.glob("*.json"):
        try:
            day = date.fromisoformat(path.stem)
        except ValueError:
            continue
        if day < report_day:
            candidates.append((day, path))
    if not candidates:
        return None
    _, path = max(candidates, key=lambda item: item[0])
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    charts = data.get("charts", {})
    return {
        "report_date": data.get("report_date"),
        "metrics": data.get("metrics", {}),
        "rules": data.get("rules", {}),
        "judgments": data.get("judgments", []),
        "charts": {
            "three_pool": {
                "onrrp_zero": charts.get("three_pool", {}).get("onrrp_zero"),
            },
        },
    }


def _rrp_zero(rules: dict[str, Any]) -> bool:
    for row in rules.get("rows", []):
        if row.get("id") == "BG_ONRRP_ZERO":
            return bool(row.get("current", {}).get("active"))
    return False


def build_facts(report_day: date, report_dir: Path | None = None) -> dict[str, Any]:
    payload = build_payload()
    cutoffs = payload.get("cutoffs", {})
    rows = build_freshness(cutoffs, report_day)
    metrics = compute_metrics(payload)
    rules = backtest_rules(payload, report_day)
    gaps = build_gaps(rows)
    anomalies = _source_anomalies(report_day)
    for item in anomalies:
        gaps.append(item)

    events = load_events_block(report_day)
    if events.get("error"):
        gaps.append({
            "kind": "events_config_invalid",
            "label": "已知事件配置无效",
            "detail": events["error"],
        })

    batch = load_batch_status()
    if batch and batch.get("status") not in {"success", "ok"}:
        gaps.append({
            "kind": "batch_status",
            "status": batch.get("status"),
            "finished_at": batch.get("finished_at"),
            "exit_code": batch.get("exit_code"),
            "detail": "最近一次数据更新批次非成功态；报告照常产出，但结论以 as-of 为准",
        })
    if batch:
        finished = str(batch.get("finished_at") or "")[:10]
        if finished and finished < report_day.isoformat():
            gaps.append({
                "kind": "batch_stale",
                "label": "批次状态早于报告日",
                "expected_asof": report_day.isoformat(),
                "actual_asof": finished,
                "detail": "批次状态不是报告当日；不得用陈旧数据当作当日结论",
            })
    else:
        gaps.append({
            "kind": "batch_status_missing",
            "label": "缺少批次状态文件",
            "expected_asof": report_day.isoformat(),
            "actual_asof": None,
            "detail": "未找到 var/run/daily_update.json；报告以各序列 as-of 为准",
        })

    classify_gaps(gaps, {str(item["dataset_id"]) for item in anomalies})

    facts: dict[str, Any] = {
        "schema_version": "1.0",
        "report_date": report_day.isoformat(),
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "mode": "template",
        "payload_meta": payload.get("meta", {}),
        "cutoffs": cutoffs,
        "calendar": _calendar_block(report_day),
        "batch_status": batch,
        "freshness": [r.to_dict() for r in rows],
        "freshness_summary": _freshness_summary(rows),
        "metrics": metrics,
        "rules": rules,
        "events": events,
        "h41_detail": payload.get("h41_detail", {}),
        "previous": load_previous_report(report_day, report_dir),
        "gaps": gaps,
        "charts": _chart_series(payload, report_day, _rrp_zero(rules)),
        "source": {
            "payload": "visualization/export_data.py::build_payload",
            "quality_flags": "meta/chronoforge.db::quality_flags",
            "batch_status": "var/run/daily_update.json",
        },
    }
    facts["judgments"] = build_judgments(facts, report_day)
    facts["scorecard"] = build_scorecard(facts.get("previous"), facts, report_day)
    return facts
