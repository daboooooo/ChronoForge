"""``chronoforge.ui`` 呈现门面契约测试（rich 15）。

锚点：
- ``kv_line`` 非 TTY 下与历史 f-string 输出逐字一致（CLI 子串断言依赖）；
- ``error:`` / ``warning:`` 前缀固定走 stderr；
- 状态/severity/布尔 Text 带样式；
- polars DataFrame → 表格（数字列右对齐、null 空串）；
- 流替换（CliRunner/capsys）后 Console 自动重建。
"""

from __future__ import annotations

import io
import sys

import polars as pl
import pytest

from chronoforge import ui


class TestKvLine:
    def test_verbatim_grammar(self, capsys: pytest.CaptureFixture[str]) -> None:
        ui.kv_line(
            "run rid",
            [
                ("dataset", "d"),
                ("status", "SUCCESS"),
                ("output", 3),
                ("errors", 0),
            ],
        )
        assert (
            capsys.readouterr().out.rstrip("\n")
            == "run rid dataset=d status=SUCCESS output=3 errors=0"
        )

    def test_prefix_none(self, capsys: pytest.CaptureFixture[str]) -> None:
        ui.kv_line(None, [("mode", "incremental"), ("window", 21600)])
        assert capsys.readouterr().out.rstrip("\n") == ("mode=incremental window=21600")

    def test_styled_value_keeps_verbatim_text(self, capsys: pytest.CaptureFixture[str]) -> None:
        ui.kv_line(
            None,
            [("status", "SUCCESS")],
            value_styles={"status": ui.status_text("SUCCESS")},
        )
        # 非 TTY：去色后文本逐字不变。
        assert capsys.readouterr().out.rstrip("\n") == "status=SUCCESS"


class TestStyledText:
    def test_status_known_and_unknown(self) -> None:
        t = ui.status_text("FAILED")
        assert t.plain == "FAILED"
        assert str(t.style) == "bold red"
        assert ui.status_text("WEIRD").style is None

    def test_severity_style(self) -> None:
        assert ui.severity_text("ERROR").plain == "ERROR"
        assert str(ui.severity_text("ERROR").style) == "bold red"

    @pytest.mark.parametrize(
        ("value", "label"),
        [(True, "1"), (1, "1"), ("1", "1"), (False, "0"), (0, "0"), ("0", "0")],
    )
    def test_bool_text_normalizes(self, value: object, label: str) -> None:
        assert ui.bool_text(value).plain == label


class TestMessages:
    def test_error_prefix_on_stderr(self, capsys: pytest.CaptureFixture[str]) -> None:
        ui.print_error("1/2 dataset(s) failed: boom")
        captured = capsys.readouterr()
        assert captured.out == ""
        assert captured.err.rstrip("\n") == ("error: 1/2 dataset(s) failed: boom")

    def test_warning_prefix_on_stderr(self, capsys: pytest.CaptureFixture[str]) -> None:
        ui.print_warning("only first 20 of 99 rows shown")
        captured = capsys.readouterr()
        assert captured.out == ""
        assert captured.err.rstrip("\n") == ("warning: only first 20 of 99 rows shown")

    def test_hint_on_stdout(self, capsys: pytest.CaptureFixture[str]) -> None:
        ui.print_hint("(no findings) — run 'registry sync'")
        captured = capsys.readouterr()
        assert captured.err == ""
        assert captured.out.rstrip("\n") == "(no findings) — run 'registry sync'"

    def test_square_brackets_not_treated_as_markup(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """消息中的 [] 不得被 rich 当标记解析（原样输出）。"""
        ui.print_hint("(empty result) [see docs]")
        assert capsys.readouterr().out.rstrip("\n") == ("(empty result) [see docs]")


class TestTable:
    def test_table_contains_header_and_cells(self, capsys: pytest.CaptureFixture[str]) -> None:
        table = ui.make_table("dataset", "status", "enabled")
        table.add_row("ds_ok", ui.status_text("SUCCESS"), ui.bool_text(1))
        ui.print_table(table)
        out = capsys.readouterr().out
        assert "dataset" in out and "status" in out and "enabled" in out
        assert "ds_ok" in out and "SUCCESS" in out and "1" in out

    def test_dataframe_table_numeric_align_and_null(self) -> None:
        frame = pl.DataFrame({"symbol": ["BTC", None], "price": [100000, 42]})
        table = ui.dataframe_table(frame)
        assert [c.header for c in table.columns] == ["symbol", "price"]
        assert table.columns[1].justify == "right"
        assert table.columns[0].justify != "right"

        buf = io.StringIO()
        import rich

        rich.console.Console(file=buf, width=120).print(table)
        rendered = buf.getvalue()
        assert "BTC" in rendered and "100000" in rendered and "42" in rendered
        # null 渲染为空串（不出现字面 None）。
        assert "None" not in rendered

    def test_dataframe_table_truncation(self) -> None:
        frame = pl.DataFrame({"x": list(range(5))})
        assert ui.dataframe_table(frame, max_rows=2).row_count == 2


class TestConsoleRebinding:
    def test_console_rebuilds_when_stream_changes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        first = ui.get_console()
        fake = io.StringIO()
        monkeypatch.setattr(sys, "stdout", fake)
        try:
            rebuilt = ui.get_console()
            assert rebuilt is not first
            assert rebuilt.file is fake
        finally:
            monkeypatch.undo()
        # 真实流恢复后再次重建回来，避免泄漏到后续用例。
        assert ui.get_console().file is sys.stdout
