"""结构化日志统一入口（D08 §3/§4，D01 §4）。

渲染 profile（``setup_logging(profile=...)`` 或 ``CHRONOFORGE_LOG_FORMAT``）：

- ``auto``（默认）：stderr 为 TTY → ``rich``；否则 ``json``
- ``rich``：structlog 经 stdlib ``ProcessorFormatter`` 桥接，由
  ``ConsoleRenderer`` 直写 stderr（彩色级别/键值列 + Rich traceback，
  StreamHandler 一次性输出，不经 RichHandler 二次渲染）；第三方库日志
  （httpx/ccxt 等）经 ``foreign_pre_chain`` 收敛到同一处理器与同一脱敏链
- ``json``：JSON lines → stderr（机器可读，字段契约与历史完全一致）
- ``test``：JSON + 固定 WARNING + **禁止文件 sink**（conftest 统一配置，
  测试套件零噪音、零文件残留）

文件 sink：``log_file`` / ``CHRONOFORGE_LOG_FILE`` 指定时附加
``RotatingFileHandler``（JSONL、DEBUG 全量、10MB×5）。终端渲染与文件渲染
在 redact 之后才分叉，两端同密；存在文件 sink 时 structlog 包装层放开到
DEBUG，由各 handler 自行过滤级别。

- 级别语义：INFO=阶段/重试/锁；WARNING=质量 finding、429；ERROR=终态失败
- 脱敏：redact_processor 位于一切渲染器之前（D08 §4 键表 + URL query 掩码）
- 事件名受控词表（D08 §3；D09 §2 附加断言 ③：词表 == 代码实际事件名集合，
  由 tests/unit/test_logging.py 内省校验；新事件名必须先入 EVENT_VOCABULARY）

All logging goes through :func:`get_logger`（D01 §4 运行时全局约定）。
"""

from __future__ import annotations

import logging
import os
import sys
from collections.abc import MutableMapping
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

import structlog

# ---------------------------------------------------------------------------
# 事件词表（D08 §3 受控词表）
# ---------------------------------------------------------------------------
# 存量并入说明（CLI-001 偏差登记）：D08 §3 列出 5 条规范事件名；存量代码
# （PIPELINE/STORAGE/ACQUISITION/QUERY-003，均已验收）实际发射以下 21 条。
# 按 D09 §2 ③「词表 == 代码实际事件名集合」建立相等校验，词表常量收录
# 实际集合；storage.upsert / quality.finding 两条缺口登记 Deferred。
# 新增事件名的唯一途径：先入本表，再写发射点（architecture test 强制）。
EVENT_VOCABULARY: frozenset[str] = frozenset(
    {
        # pipeline 域
        "pipeline.stage",
        "pipeline.run_many",
        "pipeline.run_many_job_failed",
        "pipeline.run_failed",
        "pipeline.run_cancelled",  # SR-02：用户中断 → CANCELLED 终态发射点
        "pipeline.drift_detected",
        "pipeline.dataset_status",
        "pipeline.cursor_skip",
        "pipeline.circuit_open",
        "pipeline.circuit_half_open",  # R2-01：冷却期届满 half-open 探测发射点
        "pipeline.chunk_failed",
        "pipeline.empty_history_chunk",  # R2-05：历史区间空 chunk 可观测
        # connector 域
        "connector.retry",
        "connector.normalize_skip",
        "connector.normalize_summary",  # 一批 normalize 结束后的跳过计数汇总
        # storage 域
        "storage.staleness_frequency_unparsed",
        "storage.release_stale_locks",
        "storage.reconcile",
        "storage.partition_time_fallback",
        "storage.drift_detected",
        "storage.cleanup_orphans",
        # quality / research / replay 域
        "consistency.update_dataset_status_failed",
        "research.snapshot_created",
        "replay.finish",
        "run.finish",
    }
)

# ---------------------------------------------------------------------------
# profile / sink 常量
# ---------------------------------------------------------------------------
VALID_PROFILES: tuple[str, ...] = ("auto", "rich", "json", "test")
_FILE_MAX_BYTES = 10 * 1024 * 1024
_FILE_BACKUP_COUNT = 5
_HANDLER_TAG = "_chronoforge_logging_handler"
# 第三方噪声源：显式抬到 WARNING（root 放 DEBUG 以支持文件全量 sink 时，
# 避免 httpx/ccxt 的 DEBUG 淹没文件）。
_NOISY_THIRD_PARTY_LOGGERS: tuple[str, ...] = (
    "httpx",
    "httpcore",
    "ccxt",
    "urllib3",
    "asyncio",
)


def get_logger() -> structlog.stdlib.BoundLogger:
    """获取结构化 logger（D01 §4：全部日志经此入口）。"""
    return structlog.get_logger()  # type: ignore[no-any-return]


def bind_context(**fields: Any) -> None:
    """绑定贯穿上下文（run_id/dataset 等，D08 §3 contextvar 贯穿）。"""
    structlog.contextvars.bind_contextvars(**fields)


def clear_context() -> None:
    """清空贯穿上下文（run 结束/CLI 命令边界调用）。"""
    structlog.contextvars.clear_contextvars()


def redact_processor(
    logger: Any, method_name: str, event_dict: MutableMapping[str, Any]
) -> dict[str, Any]:
    """脱敏 processor（D08 §4）：链内置于任何渲染器之前。

    - 整个 event_dict 过 chronoforge.security.redact（键命中→"***"；
      URL query 参数同规则掩码）
    - 异常链（字符串或结构化异常 dict）按同一递归规则掩码
    """
    from chronoforge.security import redact

    result = redact(event_dict)
    return result  # type: ignore[no-any-return]


# ---------------------------------------------------------------------------
# processor 链组装
# ---------------------------------------------------------------------------
def _mask_exception_args_processor(
    logger: Any, method_name: str, event_dict: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    """rich 回溯渲染前掩码异常链参数（安全契约：redact 在一切渲染之前）。

    Rich 回溯按异常对象**实时**渲染 ``str(exc)``，无法事后在文本上掩码，
    故在 rich 链中先沿 ``__cause__/__context__`` 链对每个异常的 ``args``
    （及 3.11+ ``__notes__``）执行 redact 再交给 ConsoleRenderer。
    副作用：日志调用后该异常对象的文本即为脱敏形态（CLI 随后向用户展示
    str(exc) 时同样不会泄漏，属期望行为）。
    """
    exc_info = event_dict.get("exc_info")
    if not exc_info:
        return event_dict
    from chronoforge.security import redact
    from chronoforge.security.redact import _redact_value

    def _mask(arg: Any) -> Any:
        # redact() 对裸字符串原样返回；URL 掩码只在 dict 值路径触发，
        # 异常文本必须显式走字符串值路径（中性键名 "" 不命中键表）。
        if isinstance(arg, str):
            return _redact_value("", arg)
        return redact(arg)

    if exc_info is True:
        exc_info = sys.exc_info()
    if isinstance(exc_info, BaseException):
        exc: BaseException | None = exc_info
    elif isinstance(exc_info, tuple) and len(exc_info) == 3:
        exc = exc_info[1]
    else:
        return event_dict

    seen: set[int] = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        exc.args = tuple(_mask(arg) for arg in exc.args)
        notes = getattr(exc, "__notes__", None)
        if notes:
            exc.__notes__ = [_mask(note) for note in notes]
        exc = exc.__cause__ or exc.__context__
    return event_dict


def _base_processors(*, rich_exceptions: bool) -> list[Any]:
    """redact 之前（含）的共享 processor 链；末端渲染器由调用方追加。

    Args:
        rich_exceptions: True（rich profile）→ 异常保持对象形态，渲染前
            掩码 args，由 ConsoleRenderer 的 RichTracebackFormatter 绘制；
            False（json profile）→ format_exc_info 转 traceback 字符串
            （历史 JSON 字段契约），随后由 redact 在字符串上掩码 URL。
    """
    processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
    ]
    if rich_exceptions:
        processors.append(_mask_exception_args_processor)
    else:
        processors.append(structlog.processors.format_exc_info)
    processors.extend(
        [
            redact_processor,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
        ]
    )
    return processors


def _configure_structlog_json_direct(level_no: int) -> None:
    """structlog 直写 stderr JSON（不经 stdlib）——历史行为，测试契约锚点。"""
    structlog.configure(
        processors=[
            *_base_processors(rich_exceptions=False),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level_no),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=False,
    )


# 方法名 → 级别数值（structlog 别名一并覆盖）。
_METHOD_LEVELS: dict[str, int] = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warn": logging.WARNING,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "exception": logging.ERROR,
    "critical": logging.CRITICAL,
    "fatal": logging.CRITICAL,
}


def _drop_below_level(level_no: int) -> Any:
    """processor 级级别过滤（替代过滤型 wrapper）。

    test profile 专用：structlog.testing.capture_logs 只替换 processors、
    不换 wrapper_class——过滤必须放在 processor 链上，集成测试的
    capture_logs 才能采到 INFO 事件（它会临时清空整条 processor 链）。
    """

    def _processor(
        logger: Any, method_name: str, event_dict: MutableMapping[str, Any]
    ) -> MutableMapping[str, Any]:
        if _METHOD_LEVELS.get(method_name, logging.NOTSET) < level_no:
            raise structlog.DropEvent
        return event_dict

    return _processor


def _configure_structlog_test_direct() -> None:
    """test profile：非过滤 wrapper + processor 级 WARNING 过滤 + JSON 直写。

    与生产 json 直写的区别：过滤在链上而非 wrapper 上，保证
    ``structlog.testing.capture_logs()``（仅替换 processors）能在集成
    测试中捕获 INFO 及以上全部事件；非捕获场景下 WARNING 以下静默。
    """
    structlog.configure(
        processors=[
            _drop_below_level(logging.WARNING),
            *_base_processors(rich_exceptions=False),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=False,
    )


def _detach_our_handlers(root_logger: logging.Logger) -> None:
    """摘除并关闭此前由本模块挂到 root 的 handler（重复配置安全）。"""
    for handler in list(root_logger.handlers):
        if getattr(handler, _HANDLER_TAG, False):
            root_logger.removeHandler(handler)
            handler.close()


def _build_rich_handler(level_no: int) -> logging.Handler:
    """ConsoleRenderer 直写 stderr（StreamHandler，**不可**再套 RichHandler）。

    ConsoleRenderer 输出的已是带 ANSI 的完整日志行（时间戳/级别/键值/回溯）。
    若挂 RichHandler，它会把该行当作普通 message 再经 rich Console 渲染一次：
    ANSI 被包进 Text（裸 ``[2m``/``[[33m`` 可见）且 LogRender 按其内部宽度
    软换行，一条日志被切成多行并带大片尾部空格。故按 structlog 官方桥接
    方式用普通 StreamHandler，终端只做原生硬换行。
    """
    is_terminal = sys.stderr.isatty()
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(level_no)
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            processor=structlog.dev.ConsoleRenderer(
                colors=is_terminal,
                exception_formatter=structlog.dev.RichTracebackFormatter(
                    show_locals=False,  # locals 可能含明文凭证
                    max_frames=20,
                    extra_lines=0,  # 不渲染错误行外的源码上下文：收缩敏感字面量泄漏面，且更紧凑
                    width=None,  # 由终端宽度自适应 reflow
                    color_system="truecolor" if is_terminal else None,
                ),
            ),
            foreign_pre_chain=_base_processors(rich_exceptions=True),
        )
    )
    setattr(handler, _HANDLER_TAG, True)
    return handler


def _build_json_stream_handler(level_no: int) -> logging.Handler:
    """stdlib JSON 流处理器（桥接模式下的 stderr 输出 / 第三方库日志）。"""
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(level_no)
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            processor=structlog.processors.JSONRenderer(),
            foreign_pre_chain=_base_processors(rich_exceptions=False),
        )
    )
    setattr(handler, _HANDLER_TAG, True)
    return handler


def _build_json_file_handler(log_file: Path) -> RotatingFileHandler:
    """JSONL 轮转文件 sink（DEBUG 全量；父目录自动创建）。"""
    log_file.parent.mkdir(parents=True, exist_ok=True)
    handler = RotatingFileHandler(
        log_file,
        maxBytes=_FILE_MAX_BYTES,
        backupCount=_FILE_BACKUP_COUNT,
        encoding="utf-8",
    )
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            processor=structlog.processors.JSONRenderer(),
            foreign_pre_chain=_base_processors(rich_exceptions=False),
        )
    )
    setattr(handler, _HANDLER_TAG, True)
    return handler


def _configure_stdlib_bridge(*, console_mode: str, level_no: int, log_file: Path | None) -> None:
    """structlog → stdlib（ProcessorFormatter）→ rich/json handler（+文件 sink）。"""
    rich_exceptions = console_mode == "rich"
    # 有文件 sink 时包装层放开到 DEBUG（文件 handler 收全量），
    # 控制台 handler 各自按 level_no 过滤；无文件时包装层即按控制台级别过滤。
    structlog.configure(
        processors=[
            *_base_processors(rich_exceptions=rich_exceptions),
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.DEBUG if log_file is not None else level_no
        ),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=False,
    )

    root_logger = logging.getLogger()
    _detach_our_handlers(root_logger)

    handlers: list[logging.Handler] = []
    if console_mode == "rich":
        handlers.append(_build_rich_handler(level_no))
    else:
        handlers.append(_build_json_stream_handler(level_no))
    if log_file is not None:
        handlers.append(_build_json_file_handler(log_file))

    root_logger.setLevel(logging.DEBUG)
    for logger_name in _NOISY_THIRD_PARTY_LOGGERS:
        logging.getLogger(logger_name).setLevel(logging.WARNING)
    for handler in handlers:
        root_logger.addHandler(handler)


def _resolve_profile(profile: str | None) -> str:
    """profile 解析：显式参数 → CHRONOFORGE_LOG_FORMAT → auto（再按 TTY 落定）。"""
    resolved = profile or os.environ.get("CHRONOFORGE_LOG_FORMAT") or "auto"
    if resolved not in VALID_PROFILES:
        resolved = "auto"
    if resolved == "auto":
        resolved = "rich" if sys.stderr.isatty() else "json"
    return resolved


def setup_logging(
    level: str = "INFO",
    *,
    profile: str | None = None,
    log_file: str | Path | None = None,
) -> None:
    """配置统一日志（终端渲染 profile + 可选 JSONL 文件 sink）。

    Args:
        level: 控制台日志级别名（DEBUG/INFO/...，非法名回退 INFO）。
            文件 sink 恒为 DEBUG 全量。
        profile: ``auto|rich|json|test``；None 时读 CHRONOFORGE_LOG_FORMAT，
            仍缺省为 auto（TTY→rich，管道→json）。
        log_file: JSONL 轮转文件路径；None 时读 CHRONOFORGE_LOG_FILE，
            未设置则不落盘。``test`` profile 强制忽略本参数。

    Note:
        cache_logger_on_first_use=False——测试与 CLI 多次重配置安全；
        本函数挂载的 stdlib handler 带内部标记，重配时先摘除旧实例
        （含关闭文件句柄），可安全重复调用。
    """
    level_no = getattr(logging, str(level).upper(), logging.INFO)
    resolved_profile = _resolve_profile(profile)

    if log_file is None:
        env_file = os.environ.get("CHRONOFORGE_LOG_FILE")
        if env_file:
            log_file = Path(env_file)

    if resolved_profile == "test":
        # 测试契约：固定 WARNING、JSON 直写 stderr、绝不产生文件。
        _detach_our_handlers(logging.getLogger())
        _configure_structlog_test_direct()
        return

    if resolved_profile == "json" and log_file is None:
        # 无文件 sink 的 json：保持历史直写路径（字段/格式零变化）。
        _detach_our_handlers(logging.getLogger())
        _configure_structlog_json_direct(level_no)
        return

    _configure_stdlib_bridge(
        console_mode=resolved_profile,
        level_no=level_no,
        log_file=Path(log_file) if log_file is not None else None,
    )
