"""dataset 命令组（D08 §2）：add / set-enabled —— dataset_registry 写侧（经 registry.service）。"""

from __future__ import annotations

import contextlib
import json
import sqlite3

import typer

from chronoforge import ui
from chronoforge.cli import _wiring
from chronoforge.config.settings import Settings
from chronoforge.connectors.errors import ChronoForgeError
from chronoforge.registry import add_dataset, set_dataset_enabled

dataset_app = typer.Typer(help="数据集登记（dataset_registry 写侧）")


@dataset_app.command("add")
def dataset_add(
    dataset_id: str = typer.Option(..., "--dataset-id", help="Dataset ID（主键）"),
    source: str = typer.Option(..., "--source", help="Source ID（须已在 source_registry）"),
    canonical_type: str = typer.Option(
        ..., "--type", help="Canonical Type 名（如 OHLCV / NUMBER）"
    ),
    entity_id: str | None = typer.Option(
        None, "--entity-id", help="实体 ID（缺省取 params.symbol，再缺省 = dataset_id）"
    ),
    params: str = typer.Option("{}", "--params", help="源侧参数（JSON 对象）"),
    frequency: str | None = typer.Option(None, "--frequency", help="频率（如 1m/1h）"),
    continuity_model: str = typer.Option(
        "ALWAYS_OPEN",
        "--continuity-model",
        help="ALWAYS_OPEN | TRADING_CALENDAR | EVENT_BASED | RELEASE_SCHEDULE",
    ),
    revision_supported: bool = typer.Option(
        False, "--revision-supported", help="启用修订（as-of 点时查询）"
    ),
) -> None:
    """登记/更新数据集（写 dataset_registry）。"""
    try:
        parsed_params = json.loads(params)
        if not isinstance(parsed_params, dict):
            raise ValueError("--params must be a JSON object")
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"--params is not valid JSON: {exc}") from exc

    entity = entity_id or str(parsed_params.get("symbol") or dataset_id)
    try:
        settings = Settings.load()
        meta = _wiring.open_meta(settings)
        add_dataset(
            meta,
            dataset_id=dataset_id,
            source_id=source,
            canonical_type=canonical_type,
            entity_id=entity,
            params=parsed_params,
            frequency=frequency,
            continuity_model=continuity_model,
            revision_supported=revision_supported,
        )
        ui.get_console().print(f"dataset {dataset_id} registered (source={source})")
    except (ChronoForgeError, ValueError, sqlite3.Error) as exc:
        ui.print_error(str(exc))
        raise typer.Exit(code=1) from exc


@dataset_app.command("set-enabled")
def dataset_set_enabled(
    dataset_id: str = typer.Option(..., "--dataset-id", help="Dataset ID（主键）"),
    enabled: bool = typer.Option(
        ..., "--enabled/--disabled",
        help="true=启用（进入采集队列）；false=停用（不再采集，历史数据保留）",
    ),
) -> None:
    """启用/停用 dataset（dataset_registry.enabled 列，决策 D-6）。

    停用后不进入 pipeline --all-due / backfill --due-only 的采集队列；
    raw/canonical/checkpoint 原样保留，不删历史数据。
    """
    try:
        with contextlib.ExitStack() as stack:
            settings = Settings.load()
            meta = _wiring.open_meta(settings)
            stack.callback(meta.close)
            updated = set_dataset_enabled(meta, dataset_id, enabled=enabled)
        if not updated:
            ui.print_error(f"dataset not found in registry: {dataset_id}")
            raise typer.Exit(code=1)
        state = "enabled" if enabled else "disabled"
        ui.get_console().print(f"dataset {dataset_id} {state}")
    except (ChronoForgeError, ValueError, sqlite3.Error) as exc:
        ui.print_error(str(exc))
        raise typer.Exit(code=1) from exc
