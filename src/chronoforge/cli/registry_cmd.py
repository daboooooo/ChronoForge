"""registry 命令组（D08 §2）：sync / list-sources / list-datasets（D04 §5）。"""

from __future__ import annotations

import contextlib
import json

import typer

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
            typer.echo(f"source_registry bootstrapped: {count} sources")
    except ChronoForgeError as exc:
        typer.secho(f"error: {exc}", fg=typer.colors.RED, err=True)
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
                typer.echo("(no sources; run `chronoforge registry sync` first)")
                return
            for r in rows:
                typer.echo(
                    f"{r['source_id']:<16} {r['access_type']:<16} "
                    f"enabled={r['enabled']} base_url={r['base_url']}"
                )
    except ChronoForgeError as exc:
        typer.secho(f"error: {exc}", fg=typer.colors.RED, err=True)
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
            stack.callback(meta.close)  # 审计 SR-11：资源统一管理
            rows = list_datasets(meta, source_id=source)
            if as_json:
                typer.echo(json.dumps(_wiring.to_jsonable(rows), ensure_ascii=False))
                return
            if not rows:
                typer.echo("(no datasets)")
                return
            for r in rows:
                typer.echo(
                    f"{r['dataset_id']:<32} type={r['canonical_type']} "
                    f"source={r['source_id']} status={r['status']} "
                    f"revision={r['revision_supported']}"
                )
    except ChronoForgeError as exc:
        typer.secho(f"error: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1) from exc
