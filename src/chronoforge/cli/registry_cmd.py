"""registry 命令组（D08 §2）：sync / list-sources / list-datasets（D04 §5）。"""

from __future__ import annotations

import contextlib
import json

import typer

from chronoforge import ui
from chronoforge.cli import _wiring
from chronoforge.config.settings import Settings
from chronoforge.connectors.errors import ChronoForgeError
from chronoforge.registry import bootstrap_defaults, list_datasets, list_sources

registry_app = typer.Typer(help="源/数据集注册表（D04 §5）")


@registry_app.command("sync")
def registry_sync() -> None:
    """引导 source_registry 默认规格（幂等，ON CONFLICT UPDATE）。"""
    try:
        with contextlib.ExitStack() as stack:
            settings = Settings.load()
            meta = _wiring.open_meta(settings)
            stack.callback(meta.close)  # 审计 SR-11：资源统一管理
            count = bootstrap_defaults(meta)
            ui.get_console().print(f"source_registry bootstrapped: {count} sources")
    except ChronoForgeError as exc:
        ui.print_error(str(exc))
        raise typer.Exit(code=1) from exc


@registry_app.command("list-sources")
def registry_list_sources(
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
) -> None:
    """列出全部数据源。"""
    try:
        with contextlib.ExitStack() as stack:
            settings = Settings.load()
            meta = _wiring.open_meta(settings)
            stack.callback(meta.close)  # 审计 SR-11：资源统一管理
            rows = list_sources(meta)
            if as_json:
                typer.echo(json.dumps(_wiring.to_jsonable(rows), ensure_ascii=False))
                return
            if not rows:
                ui.print_hint(
                    "(no sources; run `chronoforge registry sync` first)"
                )
                return
            table = ui.make_table(
                "source_id", "access_type", "enabled", "base_url", title="sources"
            )
            for r in rows:
                table.add_row(
                    str(r["source_id"]),
                    str(r["access_type"]),
                    ui.bool_text(r["enabled"]),
                    str(r["base_url"]),
                )
            ui.print_table(table)
    except ChronoForgeError as exc:
        ui.print_error(str(exc))
        raise typer.Exit(code=1) from exc


@registry_app.command("list-datasets")
def registry_list_datasets(
    source: str | None = typer.Option(None, "--source", help="按 source 过滤"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
) -> None:
    """列出全部数据集。"""
    try:
        with contextlib.ExitStack() as stack:
            settings = Settings.load()
            meta = _wiring.open_meta(settings)
            stack.callback(meta.close)
            rows = list_datasets(meta, source_id=source)
            if as_json:
                typer.echo(json.dumps(_wiring.to_jsonable(rows), ensure_ascii=False))
                return
            if not rows:
                ui.print_hint("(no datasets)")
                return
            table = ui.make_table(
                "dataset_id", "type", "source", "status", "revision", title="datasets"
            )
            for r in rows:
                table.add_row(
                    str(r["dataset_id"]),
                    str(r["canonical_type"]),
                    str(r["source_id"]),
                    ui.status_text(r["status"]),
                    ui.bool_text(r["revision_supported"]),
                )
            ui.print_table(table)
    except ChronoForgeError as exc:
        ui.print_error(str(exc))
        raise typer.Exit(code=1) from exc
