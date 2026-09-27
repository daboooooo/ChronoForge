"""结构化日志（D08 §3/§4，D01 §4）。

- structlog JSON lines → stderr；run_id/dataset 经 contextvar 贯穿
- 级别语义：INFO=阶段/重试/锁；WARNING=质量 finding、429；ERROR=终态失败
- 脱敏：redact_processor 位于格式化链末位（JSONRenderer 前），
  复用 chronoforge.security.redact（D08 §4 键表 + URL query 掩码）
- 事件名受控词表（D08 §3；D09 §2 附加断言 ③：词表 == 代码实际事件名集合，
  由 tests/unit/test_logging.py 内省校验；新事件名必须先入 EVENT_VOCABULARY）

All logging goes through :func:`get_logger`（D01 §4 运行时全局约定）。
"""

from __future__ import annotations

import logging
import sys
from collections.abc import MutableMapping
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
EVENT_VOCABULARY: frozenset[str] = frozenset({
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
})


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
    """脱敏 processor（D08 §4）：链内置于 JSONRenderer 之前。

    - 整个 event_dict 过 chronoforge.security.redact（键命中→"***"；
      URL query 参数同规则掩码）
    - 异常链在 format_exc_info 已转为 "exception" 字符串后按 URL 规则掩码
    """
    from chronoforge.security import redact

    result = redact(event_dict)
    return result  # type: ignore[no-any-return]


def setup_logging(level: str = "INFO") -> None:
    """配置 structlog（D08 §3）：JSON lines → stderr。

    Args:
        level: 日志级别名（DEBUG/INFO/...，非法名回退 INFO）。

    Note:
        cache_logger_on_first_use=False——测试与 CLI 多次重配置安全。
    """
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.processors.format_exc_info,
            redact_processor,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=False,
    )
