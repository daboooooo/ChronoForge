"""query 命令（D08 §2 顶层单命令）：DuckDB 只读查询（D07 §1）。

--json 输出 = QueryResult 字段直序列化（D07 §4：元信息 + 行数据）。
"""

from __future__ import annotations

import contextlib
import json
import sys

import typer

from chronoforge import ui
from chronoforge.cli import _wiring
from chronoforge.config.settings import Settings
from chronoforge.connectors.errors import ChronoForgeError


def query(
    dataset: str = typer.Option(..., "--dataset", help="Dataset ID"),
    start: str | None = typer.Option(None, "--start", help="主时间列下界（ISO，含）"),
    end: str | None = typer.Option(None, "--end", help="主时间列上界（ISO，不含）"),
    asof: str | None = typer.Option(
        None, "--asof", help="点时语义时刻（ISO；仅 revision_supported 类型生效）"
    ),
    filters: str | None = typer.Option(None, "--filters", help="相等过滤（JSON 对象）"),
    columns: str | None = typer.Option(
        None, "--columns", help="投影列（逗号分隔；缺省全列）"
    ),
    limit: int | None = typer.Option(
        None, "--limit", help="最大返回行数（可低于服务上限；超上限按上限截断）"
    ),
    as_json: bool = typer.Option(False, "--json", help="QueryResult 直序列化输出"),
    as_csv: bool = typer.Option(False, "--csv", help="CSV 输出（stdout）"),
) -> None:
    """查询 canonical 数据（D07 §1 六规则）。"""
    if as_json and as_csv:
        raise typer.BadParameter("--json and --csv are mutually exclusive")
    parsed_filters: dict[str, object] | None = None
    if filters is not None:
        try:
            parsed = json.loads(filters)
        except json.JSONDecodeError as exc:
            raise typer.BadParameter(f"--filters is not valid JSON: {exc}") from exc
        if not isinstance(parsed, dict):
            raise typer.BadParameter("--filters must be a JSON object")
        parsed_filters = parsed
    cols = [c.strip() for c in columns.split(",") if c.strip()] if columns else None

    try:
        # 审计 SR-11：meta / DuckDB 连接一并纳入 ExitStack（LIFO：先 con 后 meta）
        with contextlib.ExitStack() as stack:
            settings = Settings.load()
            meta = _wiring.open_meta(settings)
            stack.callback(meta.close)
            service, con = _wiring.open_query_service(settings, meta)
            stack.callback(con.close)
            result = service.query(
                dataset,
                columns=cols,
                start=_wiring.parse_dt(start),
                end=_wiring.parse_dt(end),
                asof=_wiring.parse_dt(asof),
                filters=parsed_filters,
                limit=limit,
            )

            if result.truncated:
                # READY-003：截断提示走 stderr，不污染 stdout 的 JSON/CSV 数据流
                ui.print_warning(
                    f"result truncated at {result.row_count} rows "
                    "(row limit reached); narrow the time range or raise --limit"
                )
            if as_json:
                # D07 §4：QueryResult 元信息 + 行数据（AI Agent 消费）
                payload = {
                    "dataset_id": result.dataset_id,
                    "dataset_version": result.dataset_version,
                    "schema_version": result.schema_version,
                    "row_count": result.row_count,
                    "elapsed_ms": result.elapsed_ms,
                    "truncated": result.truncated,
                    "rows": _wiring.to_jsonable(result.frame.to_dicts()),
                }
                typer.echo(json.dumps(payload, ensure_ascii=False))
            elif as_csv:
                result.frame.write_csv(sys.stdout)
            else:
                ui.kv_line(
                    None,
                    [
                        ("dataset", result.dataset_id),
                        ("version", result.dataset_version),
                        ("rows", result.row_count),
                        ("elapsed_ms", result.elapsed_ms),
                    ],
                )
                if result.row_count == 0:
                    ui.print_hint("(empty result)")
                else:
                    ui.print_table(ui.dataframe_table(result.frame, max_rows=20))
    except (ChronoForgeError, ValueError) as exc:
        ui.print_error(str(exc))
        raise typer.Exit(code=1) from exc
