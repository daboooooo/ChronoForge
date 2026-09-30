"""logging 单元测试（D08 §3/§4，TC-SEC-001/002 + D09 §2 附加断言 ③）。

③ 事件词表 == 代码中实际事件名集合：AST 内省 src/chronoforge 全部
logger.<level>("<event>") 调用，与 EVENT_VOCABULARY 相等校验。
"""

from __future__ import annotations

import ast
import io
import json
import logging
import sys
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
        setup_logging("INFO", profile="json")
        get_logger().info("pipeline.stage", stage="fetch")
        (record,) = _emit_lines(capsys)
        assert record["event"] == "pipeline.stage"
        assert record["level"] == "info"
        assert record["stage"] == "fetch"
        assert "timestamp" in record

    def test_level_filtering(self, capsys: pytest.CaptureFixture[str]) -> None:
        setup_logging("WARNING", profile="json")
        get_logger().info("pipeline.stage")  # 被过滤
        get_logger().warning("run.finish")
        lines = _emit_lines(capsys)
        assert [r["event"] for r in lines] == ["run.finish"]

    def test_contextvars_binding(self, capsys: pytest.CaptureFixture[str]) -> None:
        """run_id/dataset 经 contextvar 贯穿（D08 §3）。"""
        setup_logging("INFO", profile="json")
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
        setup_logging("INFO", profile="json")
        get_logger().info(
            "pipeline.stage",
            context={"api_key": "PLAINTEXT", "url": "https://x.io?a=1"},
        )
        (record,) = _emit_lines(capsys)
        assert record["context"] == {"api_key": "***", "url": "https://x.io?a=1"}
        assert "PLAINTEXT" not in json.dumps(record)

    def test_nested_and_url_masked(self, capsys: pytest.CaptureFixture[str]) -> None:
        setup_logging("INFO", profile="json")
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
        setup_logging("INFO", profile="json")
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


class TestLoggingProfiles:
    """渲染 profile（auto/rich/test）与环境变量解析——测试日志 vs 运行日志。"""

    def test_auto_non_tty_resolves_json(self, capsys: pytest.CaptureFixture[str]) -> None:
        """capsys 下 stderr 非 TTY：auto → json（机器可读契约不依赖显式参数）。"""
        setup_logging("INFO")
        get_logger().info("pipeline.stage", stage="fetch")
        (record,) = _emit_lines(capsys)
        assert record["event"] == "pipeline.stage"

    def test_env_format_override(
        self, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("CHRONOFORGE_LOG_FORMAT", "json")
        setup_logging("INFO")
        get_logger().warning("run.finish")
        (record,) = _emit_lines(capsys)
        assert record["event"] == "run.finish"

    def test_test_profile_fixed_warning_and_no_file(
        self, capsys: pytest.CaptureFixture[str], tmp_path: Path
    ) -> None:
        """test profile：固定 WARNING（INFO 被过滤）且强制无文件 sink。"""
        forbidden = tmp_path / "must_not_exist.jsonl"
        setup_logging("INFO", profile="test", log_file=forbidden)
        get_logger().info("pipeline.stage")
        assert capsys.readouterr().err == ""
        assert not forbidden.exists()

    def test_test_profile_warning_still_json(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        setup_logging(profile="test")
        get_logger().warning("run.finish", k="v")
        (record,) = _emit_lines(capsys)
        assert record["event"] == "run.finish"
        assert record["k"] == "v"

    def test_test_profile_compatible_with_capture_logs(self) -> None:
        """过滤必须在 processor 链上：structlog.testing.capture_logs
        只替换 processors 不换 wrapper，集成测试依赖捕获 INFO 事件。"""
        setup_logging(profile="test")
        with structlog.testing.capture_logs() as logs:
            get_logger().info("pipeline.stage", stage="fetch", phase="start")
        assert [e["event"] for e in logs] == ["pipeline.stage"]
        assert logs[0]["stage"] == "fetch"

    def test_rich_profile_renders_event_text(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        setup_logging("INFO", profile="rich")
        get_logger().info("pipeline.stage", stage="fetch", dataset="ds1")
        err = capsys.readouterr().err
        assert "pipeline.stage" in err
        assert "stage=fetch" in err
        assert "dataset=ds1" in err

    def test_rich_profile_level_filtering(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        setup_logging("WARNING", profile="rich")
        get_logger().info("pipeline.stage")  # 被控制台级别过滤
        get_logger().warning("run.finish")
        err = capsys.readouterr().err
        assert "pipeline.stage" not in err
        assert "run.finish" in err

    def test_rich_profile_exception_redacted(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """rich profile 的结构化异常（Rich traceback）同样过 redact。

        密钥按真实模式经变量拼接（生产代码不会在 raise 行写字面量），
        断言对象是异常消息行；回溯源码帧渲染的是 raise 表达式本身。
        """
        setup_logging("INFO", profile="rich")
        secret_qs = "?api_key=TOPSECRET&file_type=json"
        try:
            raise RuntimeError(
                f"401 for https://api.stlouisfed.org/fred/series/observations{secret_qs}"
            )
        except RuntimeError as exc:
            get_logger().error("run.finish", exc_info=exc)
        err = capsys.readouterr().err
        assert "TOPSECRET" not in err
        assert "api_key=***" in err

    def test_rich_profile_tty_emits_csi_single_line(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """ConsoleRenderer 必须直写 stderr：真 CSI 序列、一条记录一个换行，
        且不得被 RichHandler 二次渲染（裸 ``[2m`` / ``[[33m``、软换行）。"""

        class _FakeTTY(io.StringIO):
            def isatty(self) -> bool:  # noqa: D401
                return True

        buf = _FakeTTY()
        monkeypatch.setattr(sys, "stderr", buf)
        setup_logging("INFO", profile="rich")
        get_logger().warning(
            "storage.drift_detected", canonical_type="OHLCV", count=1
        )

        out = buf.getvalue()
        assert "\x1b[" in out  # 真正的 ANSI CSI（终端可识别）
        assert "[[" not in out  # 未被二次渲染成对 bracket 转义
        assert out.count("\n") == 1  # 一条记录仅一个换行（无软换行切碎）

    def test_foreign_stdlib_record_routed_to_rich_handler(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """第三方库 stdlib 日志经 foreign_pre_chain 收敛到同一 RichHandler。"""
        setup_logging("WARNING", profile="rich")
        logging.getLogger("chronoforge.test.thirdparty").warning("foreign-warning")
        assert "foreign-warning" in capsys.readouterr().err

    def test_reconfigure_detaches_old_handlers(self) -> None:
        """rich → json 重配置：旧我方 handler 被摘除并关闭，可安全重复调用。"""
        setup_logging("INFO", profile="rich")
        root = logging.getLogger()
        assert any(
            getattr(h, "_chronoforge_logging_handler", False) for h in root.handlers
        )
        setup_logging("INFO", profile="json")
        assert not any(
            getattr(h, "_chronoforge_logging_handler", False) for h in root.handlers
        )


class TestFileSink:
    """JSONL 轮转文件 sink：终端与文件渲染分叉，文件恒 DEBUG 全量。"""

    def test_json_profile_file_debug_console_warning(
        self, capsys: pytest.CaptureFixture[str], tmp_path: Path
    ) -> None:
        log_file = tmp_path / "logs" / "app.jsonl"
        setup_logging("WARNING", profile="json", log_file=log_file)
        get_logger().info("pipeline.stage", dataset="ds1")
        get_logger().warning("run.finish")

        err = capsys.readouterr().err  # 控制台仅 WARNING+
        assert "pipeline.stage" not in err
        assert "run.finish" in err

        lines = [json.loads(line) for line in log_file.read_text().splitlines()]
        assert [r["event"] for r in lines] == ["pipeline.stage", "run.finish"]
        assert lines[0]["dataset"] == "ds1"

    def test_rich_profile_console_pretty_file_json(
        self, capsys: pytest.CaptureFixture[str], tmp_path: Path
    ) -> None:
        log_file = tmp_path / "app.jsonl"
        setup_logging("INFO", profile="rich", log_file=log_file)
        get_logger().info("pipeline.stage", stage="fetch")

        assert "pipeline.stage" in capsys.readouterr().err  # 终端富文本
        lines = [json.loads(line) for line in log_file.read_text().splitlines()]
        assert lines[0]["event"] == "pipeline.stage"
        assert lines[0]["stage"] == "fetch"

    def test_env_log_file(
        self,
        capsys: pytest.CaptureFixture[str],
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        log_file = tmp_path / "env.jsonl"
        monkeypatch.setenv("CHRONOFORGE_LOG_FILE", str(log_file))
        setup_logging("INFO", profile="json")
        get_logger().info("pipeline.stage")
        assert log_file.exists()

    def test_file_sink_redacted(self, tmp_path: Path) -> None:
        log_file = tmp_path / "app.jsonl"
        setup_logging("INFO", profile="json", log_file=log_file)
        get_logger().info(
            "connector.retry",
            request={"url": "https://x.io?api_key=SECRET&a=1"},
        )
        flat = log_file.read_text()
        assert "SECRET" not in flat
        assert "api_key=***" in flat


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
