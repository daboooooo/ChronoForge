"""日报生成主流程：交易日检查 → 装配 facts → 模板文案 → 三件产物。"""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import structlog

from chronoforge.quality.calendar import is_trading_day
from chronoforge.report.facts import PROJECT_ROOT, build_facts
from chronoforge.report.freshness import build_freshness, build_gaps
from chronoforge.report.metrics import compute_metrics
from chronoforge.report.narrative import build_narrative
from chronoforge.report.render import (
    dump_json,
    render_html,
    render_json,
    render_markdown,
)
from chronoforge.report.session import us_report_date, weekday_cn

logger = structlog.get_logger()

DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "reports" / "daily_liquidity"
ECHARTS_SOURCE = PROJECT_ROOT / "visualization" / "vendor" / "echarts.min.js"


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
        "gaps": gaps,
        "charts": {
            "reserve": {"t": [], "v": [], "thresholds": [3.0, 3.2]},
            "three_pool": {"t": [], "reserve": [], "tga": [], "onrrp": [], "onrrp_zero": False},
            "decomp": {"t": [], "series": {}, "onrrp_zero": False},
            "funding": {"t": [], "spread": [], "srf_t": [], "srf": []},
            "tga_daily": {"t": [], "v": []},
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
        facts = build_facts(day)
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

    _write_atomic(html_path, render_html(facts, narrative, day))
    _write_atomic(md_path, render_markdown(facts, narrative, day))
    _write_atomic(json_path, dump_json(render_json(facts, narrative, day)))

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
