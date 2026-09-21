"""Cursor 推进 + 回退防护（D05 §3，ACQUISITION-002）。

包含：
- CursorAction：光标操作枚举
- CursorUpdate：光标更新结果
- advance_cursor：cursor 推进主逻辑

D05 §3 不变量：
- cursor = 最后一个已落盘且通过校验的 chunk 右边界（exclusive = 下窗口 start）
- SUCCESS 与 PARTIAL_SUCCESS 均按此推进
- 失败 chunk 之后所有窗口不推进
- 恒有 checkpoint ≤ durable_valid_data_boundary
- 回退防护：新 cursor ≤ 旧 cursor → 跳过更新 + WARNING
"""

from __future__ import annotations

from enum import Enum

from chronoforge.pipeline.windows import (
    ChunkResult,
    _format_cursor,
    _parse_cursor,
)

# ---------------------------------------------------------------------------
# CursorAction（D05 §3）
# ---------------------------------------------------------------------------


class CursorAction(Enum):
    """光标操作类型（D05 §3）。

    Args:
        ADVANCE: 推进 cursor（新 > 旧）
        SKIP: 跳过（新 ≤ 旧，回退防护）
        NOOP: 不变（chunk 失败）
    """

    ADVANCE = "advance"
    SKIP = "skip"
    NOOP = "noop"


# ---------------------------------------------------------------------------
# CursorUpdate（D05 §3 返回值）
# ---------------------------------------------------------------------------


class CursorUpdate:
    """光标更新结果（D05 §3）。

    Args:
        action: 操作类型
        new_cursor: 新 cursor（ISO datetime 字符串），SKIP 时返回旧 cursor
        warning: 警告信息（回退时填写）
    """

    __slots__ = ("action", "new_cursor", "warning")

    def __init__(
        self,
        action: CursorAction,
        new_cursor: str | None,
        warning: str | None = None,
    ) -> None:
        self.action = action
        self.new_cursor = new_cursor
        self.warning = warning

    def __repr__(self) -> str:
        return (
            f"CursorUpdate(action={self.action.value!r}, "
            f"new_cursor={self.new_cursor!r}, warning={self.warning!r})"
        )


# ---------------------------------------------------------------------------
# advance_cursor（D05 §3 核心逻辑）
# ---------------------------------------------------------------------------


def advance_cursor(
    old_cursor: str | None,
    chunk_result: ChunkResult,
    *,
    _chunk_failed: bool = False,
) -> CursorUpdate:
    """推进 cursor（D05 §3 不变量 + 回退防护）。

    推进规则：
    1. chunk 失败（success=False）：NOOP（cursor 停留）
    2. 任意前序 chunk 失败 → 当前 chunk 也 NOOP
    3. chunk 成功（success=True）：new_cursor = chunk.end
    4. 回退防护：新 cursor ≤ 旧 cursor → SKIP + WARNING

    D05 §3 不变量验证：
    - cursor = 最后一个已落盘且通过校验的 chunk 右边界
    - SUCCESS 与 PARTIAL_SUCCESS 均按此推进
    - 失败 chunk 之后所有窗口不推进

    Args:
        old_cursor: 上次 checkpoint（ISO datetime 字符串），None = 首次
        chunk_result: 本次 chunk 结果（含 chunk.end + 是否成功）
        _chunk_failed: 内部标记，前序 chunk 是否失败

    Returns:
        CursorUpdate（含 action/new_cursor/warning）

    Raises:
        ValueError: cursor 格式无效
    """
    # 前序 chunk 失败 → NOOP（cursor 停留）
    if _chunk_failed:
        return CursorUpdate(
            action=CursorAction.NOOP,
            new_cursor=old_cursor,
            warning="previous chunk failed, cursor not advancing",
        )

    # chunk 失败 → NOOP（cursor 停留）
    if not chunk_result.success:
        return CursorUpdate(
            action=CursorAction.NOOP,
            new_cursor=old_cursor,
            warning="chunk failed, cursor not advancing",
        )

    # chunk 成功 → new_cursor = chunk.end
    new_cursor_str = _format_cursor(chunk_result.chunk.end)

    if old_cursor is not None:
        old_dt = _parse_cursor(old_cursor)
        new_dt = _parse_cursor(new_cursor_str)

        # 回退防护：新 cursor ≤ 旧 cursor → SKIP
        if new_dt <= old_dt:
            return CursorUpdate(
                action=CursorAction.SKIP,
                new_cursor=old_cursor,
                warning=f"cursor not advancing: {new_cursor_str} <= {old_cursor}",
            )

    return CursorUpdate(
        action=CursorAction.ADVANCE,
        new_cursor=new_cursor_str,
    )


# ---------------------------------------------------------------------------
# 批量推进 helper（供 pipeline runner 使用）
# ---------------------------------------------------------------------------


def advance_chunks(
    old_cursor: str | None,
    results: list[ChunkResult],
) -> tuple[str | None, list[CursorUpdate]]:
    """批量推进 cursor（D05 §3）。

    规则：
    - 逐 chunk 检查，遇到第一个失败的 chunk 后全部 NOOP
    - 成功 chunk 正常推进或跳过
    - 返回最终 cursor（最后一个 ADVANCE 的结果）和每 chunk 的 update

    Args:
        old_cursor: 上次 checkpoint
        results: chunk 结果列表（按时间顺序）

    Returns:
        (最终 cursor, 每个 chunk 的 CursorUpdate)
    """
    updates: list[CursorUpdate] = []
    final_cursor = old_cursor
    any_failed = False

    for result in results:
        update = advance_cursor(
            old_cursor if not updates else final_cursor,
            result,
            _chunk_failed=any_failed,
        )
        updates.append(update)
        if update.action == CursorAction.ADVANCE:
            final_cursor = update.new_cursor
            any_failed = False
        elif update.action == CursorAction.NOOP:
            any_failed = True

    return final_cursor, updates
