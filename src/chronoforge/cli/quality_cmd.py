"""quality 命令组（D08 §2）：report —— 渲染已落库 quality_flags（D03 §5）。

D06 §4 完整 SQL 检查器（对入库数据直查 DuckDB 产出
meta/quality-report-{date}.json）未派发实现——CLI-001 输出存量
quality_flags（Deferred 登记，见 01-dispatch-log.md）。
"""

from __future__ import annotations

import json

import typer

from chronoforge.cli import _wiring
from chronoforge.config.settings import Settings
from chronoforge.connectors.errors import ChronoForgeError

quality_app = typer.Typer(help="数据质量（D06）")


@quality_app.command("report")
def quality_report(
    dataset: str | None = typer.Option(None, "--dataset", help="按 dataset 过滤"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
) -> None:
    """质量发现报告（读 quality_flags）。"""
    try:
        settings = Settings.load()
        meta = _wiring.open_meta(settings)
        rows = _wiring.list_quality_flags(meta, dataset)
        if as_json:
            typer.echo(json.dumps(_wiring.to_jsonable(rows), ensure_ascii=False))
            return
        if not rows:
            typer.echo("(no findings)")
            return
        by_severity: dict[str, int] = {}
        for r in rows:
            sev = str(r["severity"])
            by_severity[sev] = by_severity.get(sev, 0) + 1
        summary = " ".join(f"{k}={v}" for k, v in sorted(by_severity.items()))
        typer.echo(f"findings={len(rows)} {summary}")
        for r in rows:
            typer.echo(
                f"{r['severity']:<8} {r['rule_id']} dataset={r['dataset_id']} "
                f"record_key={r['record_key']} run={r['run_id']}"
            )
    except ChronoForgeError as exc:
        typer.secho(f"error: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1) from exc
