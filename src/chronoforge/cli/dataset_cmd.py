"""dataset 命令组（D08 §2）：add —— dataset_registry 写侧（经 registry.service）。"""

from __future__ import annotations

import json
import sqlite3

import typer

from chronoforge.cli import _wiring
from chronoforge.config.settings import Settings
from chronoforge.connectors.errors import ChronoForgeError
from chronoforge.registry import add_dataset

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
        typer.echo(f"dataset {dataset_id} registered (source={source})")
    except (ChronoForgeError, ValueError, sqlite3.Error) as exc:
        typer.secho(f"error: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1) from exc
