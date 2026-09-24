"""research 命令组（D08 §2）：reproduce —— snapshot 复现（D07 §3）。

复现参数约定：snapshot.params_json 含 "queries"（query kwargs 列表，
键与 DuckDBQueryService.query 一致），由创建侧经 research_snapshot
的 params 传入；缺失时命令报错（决策 D-8，见 CLI-001.md）。
"""

from __future__ import annotations

import contextlib
import json
from typing import Any

import typer

from chronoforge.cli import _wiring
from chronoforge.config.settings import Settings
from chronoforge.connectors.errors import ChronoForgeError
from chronoforge.research.snapshot import get_snapshot, snapshot_reproduce

research_app = typer.Typer(help="研究快照复现（D07 §3）")


@research_app.command("reproduce")
def research_reproduce(
    snapshot: str = typer.Option(..., "--snapshot", help="Snapshot ID"),
) -> None:
    """重跑快照查询并比对 output_hash（架构 02 §5 复现契约）。"""
    try:
        # 审计 SR-11：meta / DuckDB 连接一并纳入 ExitStack（LIFO：先 con 后 meta）
        with contextlib.ExitStack() as stack:
            settings = Settings.load()
            meta = _wiring.open_meta(settings)
            stack.callback(meta.close)
            record = get_snapshot(meta, snapshot)
            if record is None:
                raise ValueError(f"Snapshot not found: {snapshot}")

            service, con = _wiring.open_query_service(settings, meta)
            stack.callback(con.close)
            result = snapshot_reproduce(snapshot, meta, service, _query_func(record.params))

            typer.echo(
                json.dumps(
                    {
                        "snapshot_id": result.snapshot_id,
                        "hash_match": result.hash_match,
                        "original_hash": result.original_hash,
                        "new_hash": result.new_hash,
                        "version_changes": _wiring.to_jsonable(result.version_changes),
                    },
                    ensure_ascii=False,
                )
            )
    except (ChronoForgeError, ValueError) as exc:
        typer.secho(f"error: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1) from exc


def _query_func(params: dict[str, Any]):  # type: ignore[no-untyped-def]
    """由 snapshot.params["queries"] 构造 query_func（qs → 合并帧）。"""
    import polars as pl

    def _run(qs: Any):  # type: ignore[no-untyped-def]
        queries = params.get("queries") if isinstance(params, dict) else None
        if not queries:
            raise ValueError(
                "snapshot params missing 'queries' (list of query kwargs); "
                "cannot reconstruct query"
            )
        frames = [qs.query(**q).frame for q in queries]
        if len(frames) == 1:
            return frames[0]
        return pl.concat(frames, how="diagonal_relaxed")

    return _run
