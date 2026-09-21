"""logging 单元测试（D08 §3/§4，TC-SEC-001/002 + D09 §2 附加断言 ③）。

③ 事件词表 == 代码中实际事件名集合：AST 内省 src/chronoforge 全部
logger.<level>("<event>") 调用，与 EVENT_VOCABULARY 相等校验。
"""

from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest
import structlog

from chronoforge.logging import (
    EVENT_VOCABULARY,
    bind_context,
    clear_context,
    get_logger,
    redact_processor,
    setup_logging,
)

_SRC = Path(__file__).resolve().parents[2] / "src" / "chronoforge"


@pytest.fixture(autouse=True)
def _reset_logging() -> None:
    """每用例重置 structlog + contextvar（配置不跨用例泄漏）。"""
    structlog.reset_defaults()
    structlog.contextvars.clear_contextvars()
    yield
    structlog.reset_defaults()
    structlog.contextvars.clear_contextvars()


def _emit_lines(capsys: pytest.CaptureFixture[str]) -> list[dict[str, object]]:
    """捕获 stderr 的 JSON lines 并解析。"""
    out = capsys.readouterr().err
    return [json.loads(line) for line in out.strip().splitlines() if line]


class TestSetupLogging:
    """setup_logging：JSON lines → stderr（D08 §3）。"""

    def test_json_output_fields(self, capsys: pytest.CaptureFixture[str]) -> None:
        setup_logging("INFO")
        get_logger().info("pipeline.stage", stage="fetch")
        (record,) = _emit_lines(capsys)
        assert record["event"] == "pipeline.stage"
        assert record["level"] == "info"
        assert record["stage"] == "fetch"
        assert "timestamp" in record

    def test_level_filtering(self, capsys: pytest.CaptureFixture[str]) -> None:
        setup_logging("WARNING")
        get_logger().info("pipeline.stage")  # 被过滤
        get_logger().warning("run.finish")
        lines = _emit_lines(capsys)
        assert [r["event"] for r in lines] == ["run.finish"]

    def test_contextvars_binding(self, capsys: pytest.CaptureFixture[str]) -> None:
        """run_id/dataset 经 contextvar 贯穿（D08 §3）。"""
        setup_logging("INFO")
        bind_context(run_id="abc123", dataset="ds1")
        get_logger().info("pipeline.stage")
        (record,) = _emit_lines(capsys)
        assert record["run_id"] == "abc123"
        assert record["dataset"] == "ds1"
        clear_context()
        get_logger().info("pipeline.stage")
        (record2,) = _emit_lines(capsys)
        assert "run_id" not in record2


class TestRedactProcessor:
    """TC-SEC-001/002：日志脱敏（递归键命中 + URL query 掩码）。"""

    def test_context_key_masked(self, capsys: pytest.CaptureFixture[str]) -> None:
        """GWT-2: Given 含 api_key 的日志上下文 When 输出 Then 掩码为 ***。"""
        setup_logging("INFO")
        get_logger().info(
            "pipeline.stage",
            context={"api_key": "PLAINTEXT", "url": "https://x.io?a=1"},
        )
        (record,) = _emit_lines(capsys)
        assert record["context"] == {"api_key": "***", "url": "https://x.io?a=1"}
        assert "PLAINTEXT" not in json.dumps(record)

    def test_nested_and_url_masked(self, capsys: pytest.CaptureFixture[str]) -> None:
        setup_logging("INFO")
        get_logger().info(
            "connector.retry",
            request={
                "url": "https://api.stlouisfed.org/fred?api_key=SECRET&series_id=GDP",
                "headers": {"Authorization": "Bearer tok"},
            },
        )
        (record,) = _emit_lines(capsys)
        flat = json.dumps(record)
        assert "SECRET" not in flat
        assert "tok" not in flat
        assert "api_key=***" in record["request"]["url"]  # type: ignore[index]

    def test_exception_no_plaintext_key(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """TC-SEC-002：异常链 format 后无明文 key（URL query 掩码兜底）。"""
        setup_logging("INFO")
        try:
            raise RuntimeError(
                "401 for https://api.stlouisfed.org/fred/series/observations"
                "?api_key=TOPSECRET&file_type=json"
            )
        except RuntimeError as exc:
            get_logger().error("run.finish", exc_info=exc)
        (record,) = _emit_lines(capsys)
        flat = json.dumps(record)
        assert "TOPSECRET" not in flat
        assert "api_key=***" in record["exception"]  # type: ignore[operator]

    def test_redact_processor_direct(self) -> None:
        """processor 单元级：dict 键命中 → ***。"""
        out = redact_processor(None, "info", {"event": "e", "token": "abc"})
        assert out == {"event": "e", "token": "***"}


class TestEventVocabulary:
    """D09 §2 附加断言 ③：词表 == 代码实际事件名集合。"""

    def _collect_code_events(self) -> set[str]:
        """AST 扫描 logger.<info|warning|error|debug>(<str 字面量>)。"""
        events: set[str] = set()
        for path in sorted(_SRC.rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call) or not isinstance(
                    node.func, ast.Attribute
                ):
                    continue
                if node.func.attr not in {"info", "warning", "error", "debug"}:
                    continue
                if not node.args or not isinstance(node.args[0], ast.Constant):
                    continue
                value = node.args[0].value
                if isinstance(value, str) and value:
                    events.add(value)
        return events

    def test_vocabulary_equals_code_events(self) -> None:
        code_events = self._collect_code_events()
        assert EVENT_VOCABULARY == frozenset(code_events), (
            f"词表与代码事件名不一致：\n"
            f" 仅在词表: {sorted(set(EVENT_VOCABULARY) - code_events)}\n"
            f" 仅在代码: {sorted(code_events - set(EVENT_VOCABULARY))}"
        )

    def test_d08_canonical_events_present(self) -> None:
        """D08 §3 五条规范事件名中已落地的三条必须在词表内。"""
        assert {
            "pipeline.stage",
            "connector.retry",
            "run.finish",
        } <= EVENT_VOCABULARY
