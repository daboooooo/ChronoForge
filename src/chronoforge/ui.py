"""终端呈现统一入口（rich 15）。

与 :mod:`chronoforge.logging` 的分工（两者是项目唯一的输出门面）：

- ``logging`` → **stderr 结构化日志**（给运维/机器，走 structlog）
- ``ui`` → **stdout/stderr 人类可读呈现**（表格、状态行、错误提示）

流契约（不可破坏）：

- 机器可读数据流（``--json`` / ``--csv``）一律直接写 stdout，禁止经本模块；
- 表格与 key=value 状态行走 ``console``（stdout）；
- 错误/警告走 ``err_console``（stderr），保留 ``error:`` / ``warning:``
  文本前缀（CLI 测试按该前缀断言）；
- 非 TTY（管道/CliRunner）下 rich 自动去色，key=value 行保持逐字连续，
  现有子串断言不受影响。

分层约束：仅 ``chronoforge.cli`` 与 scripts 允许 import 本模块；
connectors/pipeline/storage 等业务层只用 ``chronoforge.logging``。
"""

from __future__ import annotations

import sys
from collections.abc import Sequence
from typing import Any

from rich.box import ROUNDED
from rich.console import Console
from rich.table import Table
from rich.text import Text

# Console 必须在首次使用时按「当前」sys.stdout/stderr 构造：测试框架
# （CliRunner/capsys）会逐用例替换流，模块导入期构造的单例会持有过期流。
# 按流对象身份缓存：同一条流复用单例，流被替换则重建。
_console: Console | None = None
_err_console: Console | None = None


def get_console() -> Console:
    """stdout Console（流被替换时自动重建）。"""
    global _console
    if _console is None or _console.file is not sys.stdout:
        _console = Console(file=sys.stdout)
    return _console


def get_err_console() -> Console:
    """stderr Console（流被替换时自动重建）。"""
    global _err_console
    if _err_console is None or _err_console.file is not sys.stderr:
        _err_console = Console(file=sys.stderr)
    return _err_console


# run_log 状态 → 样式（pipeline status / run 结果）。
STATUS_STYLES: dict[str, str] = {
    "SUCCESS": "bold green",
    "PARTIAL_SUCCESS": "bold yellow",
    "COVERED": "bold cyan",
    "PENDING": "bold yellow",
    "RUNNING": "bold cyan",
    "CANCELLED": "bold red",
    "FAILED": "bold red",
    "RESET": "bold green",
}

# quality_flags severity → 样式。
SEVERITY_STYLES: dict[str, str] = {
    "ERROR": "bold red",
    "WARNING": "bold yellow",
    "INFO": "cyan",
}

_BOOL_STYLES: dict[bool, str] = {True: "bold green", False: "bright_black"}


def status_text(value: Any) -> Text:
    """按 run 状态着色的 Text（未知状态不着色）。"""
    return Text(str(value), style=STATUS_STYLES.get(str(value)))


def severity_text(value: Any) -> Text:
    """按质量 severity 着色的 Text。"""
    return Text(str(value), style=SEVERITY_STYLES.get(str(value)))


def bool_text(value: Any) -> Text:
    """布尔/0-1 字段着色（True/1 绿，False/0 灰）。"""
    flag = value in (True, 1, "1")
    return Text(str(int(flag)), style=_BOOL_STYLES[flag])


def make_table(*columns: str, title: str | None = None) -> Table:
    """构造统一样式的表格（圆角框、青色表头、斑马纹）。"""
    table = Table(
        title=title,
        box=ROUNDED,
        header_style="bold cyan",
        row_styles=("", "dim"),
        expand=False,
    )
    for column in columns:
        table.add_column(column, overflow="fold")
    return table


def print_table(table: Table) -> None:
    """输出表格到 stdout。"""
    get_console().print(table)


def kv_line(
    prefix: str | None,
    items: Sequence[tuple[str, Any]],
    *,
    value_styles: dict[str, Text] | None = None,
) -> None:
    """打印 key=value 单行状态（语法与历史输出逐字一致，仅值可着色）。

    Args:
        prefix: 行首（如 ``"run <run_id>"``），其后空一格接键值对。
        items: 有序 ``(key, value)`` 序列，序列化为 ``key=value`` 并以
            空格连接；值经 Text 着色不插入额外字符，非 TTY 下去色后
            与原 f-string 输出完全一致。
        value_styles: 键 → Text（已带样式）映射，用于 status 等值着色。
    """
    text = Text()
    if prefix:
        text.append(prefix)
        text.append(" ")
    for index, (key, value) in enumerate(items):
        if index > 0:
            text.append(" ")
        text.append(f"{key}=")
        if value_styles and key in value_styles:
            text.append(value_styles[key])
        else:
            text.append(str(value))
    get_console().print(text)


def print_hint(text: str) -> None:
    """空态/提示信息（dim，stdout）。"""
    get_console().print(Text(text, style="dim"))


def print_warning(message: str) -> None:
    """警告行（stderr，保留 ``warning:`` 前缀）。"""
    get_err_console().print(Text.assemble(("warning: ", "bold yellow"), message))


def print_error(message: str) -> None:
    """错误行（stderr，保留 ``error:`` 前缀——CLI 失败断言锚点）。"""
    get_err_console().print(Text.assemble(("error: ", "bold red"), message))


def dataframe_table(frame: Any, *, max_rows: int = 20) -> Table:
    """polars DataFrame → rich Table（数字列右对齐；超 max_rows 截断）。

    Args:
        frame: polars DataFrame（query 人类可读模式）。
        max_rows: 最多展示行数（与历史 ``head(20)`` 一致）。
    """
    sub = frame.head(max_rows)
    table = make_table(*sub.columns)
    numeric_cols = {
        name for name, dtype in sub.schema.items() if getattr(dtype, "is_numeric", lambda: False)()
    }
    for column in table.columns:
        if column.header in numeric_cols:
            column.justify = "right"
    for row in sub.iter_rows():
        table.add_row(*("" if value is None else str(value) for value in row))
    return table
