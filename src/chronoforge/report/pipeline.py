"""日报生成主流程：交易日检查 → 装配 facts → 模板文案 → 三件产物。"""

from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import structlog

from chronoforge.quality.calendar import is_early_close, is_trading_day
from chronoforge.report.facts import PROJECT_ROOT, build_facts
from chronoforge.report.freshness import build_freshness, build_gaps
from chronoforge.report.metrics import compute_metrics
from chronoforge.report.narrative import build_narrative
from chronoforge.report.render import (
    dump_json,
    render_html,
    render_index,
    render_json,
    render_markdown,
)
from chronoforge.report.session import us_report_date, weekday_cn

logger = structlog.get_logger()

DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "reports" / "daily_liquidity"
ECHARTS_SOURCE = PROJECT_ROOT / "visualization" / "vendor" / "echarts.min.js"

# 产物保留：默认保留 180 天（约 6 个月）的三件套；index.html 始终保留。
REPORT_RETENTION_DAYS = 180


@dataclass(frozen=True)
class ReportResult:
    status: str  # generated | degraded | skipped
    report_date: date
    html_path: Path | None = None
    markdown_path: Path | None = None
    json_path: Path | None = None
    message: str = ""


def _degraded_facts(report_day: date, exc: Exception) -> dict[str, Any]:
    """数据链路整体不可用时的最小 facts：所有指标显式缺失，仍产出报告。"""
    rows = build_freshness({}, report_day)
    gaps = build_gaps(rows)
    gaps.append({
        "kind": "data_build_error",
        "label": "数据装配失败",
        "expected_asof": report_day.isoformat(),
        "actual_asof": None,
        "detail": f"{type(exc).__name__}: {exc}",
    })
    return {
        "schema_version": "1.0",
        "report_date": report_day.isoformat(),
        "generated_at": None,
        "mode": "template",
        "payload_meta": {"data_cutoff": None},
        "cutoffs": {},
        "calendar": {
            "early_close": is_early_close(report_day),
            "early_close_time_et": "13:00" if is_early_close(report_day) else None,
            "next_early_close": None,
            "next_holiday": None,
        },
        "batch_status": None,
        "freshness": [r.to_dict() for r in rows],
        "freshness_summary": {
            "core_total": sum(1 for r in rows if r.core),
            "core_ok": 0,
            "core_lagged": 0,
            "core_missing": sum(1 for r in rows if r.core),
            "total": len(rows),
            "ok": 0,
            "lagged": 0,
            "missing": len(rows),
        },
        "metrics": compute_metrics({}),
        "rules": {"window_years": 2, "asof": None, "rows": [], "transitions": [],
                  "stats": {}, "deleted": []},
        "previous": None,
        "gaps": gaps,
        "charts": {
            "reserve": {"t": [], "v": [], "thresholds": [3.0, 3.2],
                        "bands": {}, "turning": None},
            "three_pool": {"t": [], "reserve": [], "tga": [], "onrrp": [],
                           "onrrp_zero": False, "zero_since": None},
            "decomp": {"t": [], "series": {}, "onrrp_zero": False},
            "funding": {"t": [], "spread": [], "srf_t": [], "srf": [],
                        "thresholds": {}, "coincident": [], "stats": {}},
            "tga_daily": {"t": [], "v": [], "tax_dates": [],
                          "next_tax": None, "tax_effect": {},
                          "forecast_origin": None},
            "tga_forecast": {"run_date": None, "scenarios": {}},
            "reserve_forecast": [],
        },
        "source": {
            "payload": "visualization/export_data.py::build_payload",
            "error": f"{type(exc).__name__}: {exc}",
        },
    }


def _ensure_assets(output_dir: Path) -> None:
    assets = output_dir / "assets"
    assets.mkdir(parents=True, exist_ok=True)
    target = assets / "echarts.min.js"
    if target.exists() and target.stat().st_size > 0:
        return
    if not ECHARTS_SOURCE.exists():
        raise FileNotFoundError(f"缺少 ECharts 资源: {ECHARTS_SOURCE}")
    shutil.copyfile(ECHARTS_SOURCE, target)


def _write_atomic(path: Path, content: str) -> None:
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    os.replace(tmp, path)


def _write_set(contents: dict[Path, str]) -> None:
    """三件产物一起落盘：先全部写临时文件，再统一 replace。

    任何一步渲染/写临时文件失败都会清掉临时文件，已有产物保持原样，
    不会出现「html 更新了、json 还是旧的」这种半份产物。
    """
    staged: list[tuple[Path, Path]] = []
    try:
        for path, content in contents.items():
            tmp = path.with_name(path.name + ".tmp")
            tmp.write_text(content, encoding="utf-8")
            staged.append((tmp, path))
    except Exception:
        for tmp, _ in staged:
            tmp.unlink(missing_ok=True)
        raise
    for tmp, path in staged:
        os.replace(tmp, path)


def _prune_reports(out_dir: Path, report_day: date) -> list[str]:
    """删除超出保留窗口的日报三件套；返回被删除的日期。"""
    cutoff = report_day - timedelta(days=REPORT_RETENTION_DAYS)
    removed: list[str] = []
    for path in sorted(out_dir.glob("*.json")):
        try:
            day = date.fromisoformat(path.stem)
        except ValueError:
            continue
        if day >= cutoff:
            continue
        for suffix in (".json", ".md", ".html"):
            (out_dir / f"{day.isoformat()}{suffix}").unlink(missing_ok=True)
        removed.append(day.isoformat())
    return removed


def _index_entries(out_dir: Path) -> list[dict[str, Any]]:
    """扫描历史 JSON，供索引页使用；坏文件跳过而不是让整页失败。"""
    entries: list[dict[str, Any]] = []
    for path in out_dir.glob("*.json"):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        day = data.get("report_date") or path.stem
        metrics = data.get("metrics") or {}
        reserve = metrics.get("reserve") or {}
        summary = data.get("freshness_summary") or {}
        entries.append({
            "date": str(day),
            "status": str(data.get("status") or "unknown"),
            "generated_at": data.get("generated_at"),
            "reserve_t": reserve.get("level_t"),
            "reserve_chg_4w_b": reserve.get("chg_4w_b"),
            "core_ok": summary.get("core_ok"),
            "core_total": summary.get("core_total"),
            "gaps": len(data.get("gaps") or []),
        })
    entries.sort(key=lambda item: item["date"], reverse=True)
    return entries


def run_report(
    report_day: date | None = None,
    output_dir: Path | None = None,
) -> ReportResult:
    """生成单日日报。非交易日返回 skipped（不写任何文件）。"""
    day = report_day or us_report_date()
    if not is_trading_day(day):
        logger.info(
            "report.skipped_non_trading_day",
            report_date=day.isoformat(),
            weekday=weekday_cn(day),
        )
        return ReportResult(
            status="skipped",
            report_date=day,
            message=f"{day.isoformat()}（{weekday_cn(day)}）不是美股交易日，未生成日报",
        )

    out_dir = Path(output_dir) if output_dir else DEFAULT_OUTPUT_DIR
    if not out_dir.is_absolute():
        out_dir = PROJECT_ROOT / out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    _ensure_assets(out_dir)

    status = "generated"
    try:
        facts = build_facts(day, out_dir)
    except Exception as exc:  # 数据链路失败仍需产出报告（显式缺口）
        logger.error(
            "report.data_build_failed", report_date=day.isoformat(), exc_info=True
        )
        facts = _degraded_facts(day, exc)
        status = "degraded"

    narrative = build_narrative(facts)
    html_path = out_dir / f"{day.isoformat()}.html"
    md_path = out_dir / f"{day.isoformat()}.md"
    json_path = out_dir / f"{day.isoformat()}.json"
    audit = render_json(facts, narrative, day)
    audit["status"] = status

    try:
        _write_set({
            html_path: render_html(facts, narrative, day),
            md_path: render_markdown(facts, narrative, day),
            json_path: dump_json(audit),
        })
        _write_atomic(
            out_dir / "index.html",
            render_index(_index_entries(out_dir), day),
        )
    except Exception:
        logger.error(
            "report.write_failed", report_date=day.isoformat(), exc_info=True
        )
        raise

    removed = _prune_reports(out_dir, day)
    if removed:
        logger.info("report.pruned", report_date=day.isoformat(), removed=removed)

    logger.info(
        "report.generated",
        report_date=day.isoformat(),
        status=status,
        html=str(html_path),
        gaps=len(facts.get("gaps", [])),
    )
    return ReportResult(
        status=status,
        report_date=day,
        html_path=html_path,
        markdown_path=md_path,
        json_path=json_path,
        message=f"已生成 {day.isoformat()} 日报（{status}）",
    )
