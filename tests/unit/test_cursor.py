"""Cursor 推进 + 回退防护测试（D05 §3，ACQUISITION-002，D09 TC-P 组）。

覆盖：
- 基础推进：success=True → ADVANCE
- chunk 失败：success=False → NOOP
- 回退防护：new ≤ old → SKIP + WARNING
- 首次推进：old_cursor=None → ADVANCE
- 批量推进：advance_chunks
- 3-chunk 窗口第 2 chunk 失败 → PARTIAL（TC-P-010）
"""

from __future__ import annotations

from datetime import datetime, timedelta

from chronoforge.pipeline.cursor import (
    CursorAction,
    CursorUpdate,
    advance_chunks,
    advance_cursor,
)
from chronoforge.pipeline.windows import (
    Chunk,
    ChunkResult,
    FetchRequest,
)

# ---------------------------------------------------------------------------
# 辅助函数
# ---------------------------------------------------------------------------


def make_chunk(start: datetime, end: datetime, dataset_id: str = "test") -> Chunk:
    """构造测试用 Chunk。"""
    return Chunk(
        chunk_id="abcd1234",
        dataset_id=dataset_id,
        start=start,
        end=end,
        request=FetchRequest(
            dataset_id=dataset_id,
            params={},
            start=start,
            end=end,
            cursor=None,
        ),
    )


def make_result(
    chunk: Chunk, success: bool = True, record_count: int = 10
) -> ChunkResult:
    """构造测试用 ChunkResult。"""
    return ChunkResult(
        chunk=chunk,
        success=success,
        record_count=record_count,
    )


def make_dt(hour: int = 0, minute: int = 0) -> datetime:
    """快捷构造 datetime（UTC naive）。"""
    return datetime(2026, 1, 15, hour, minute)


# ---------------------------------------------------------------------------
# advance_cursor：基础推进
# ---------------------------------------------------------------------------


class TestAdvanceCursorBasic:
    """基础推进测试。"""

    def test_first_chunk_always_advances(self) -> None:
        """首次推进（old_cursor=None）→ ADVANCE。"""
        chunk = make_chunk(make_dt(10, 0), make_dt(11, 0))
        result = make_result(chunk, success=True)
        update = advance_cursor(None, result)

        assert update.action == CursorAction.ADVANCE
        assert update.new_cursor == "2026-01-15T11:00:00"
        assert update.warning is None

    def test_success_advances_cursor(self) -> None:
        """success=True → ADVANCE。"""
        old = "2026-01-15T10:00:00"
        chunk = make_chunk(make_dt(10, 0), make_dt(12, 0))
        result = make_result(chunk, success=True)
        update = advance_cursor(old, result)

        assert update.action == CursorAction.ADVANCE
        assert update.new_cursor == "2026-01-15T12:00:00"
        assert update.warning is None

    def test_success_with_records_zero(self) -> None:
        """成功但 record_count=0（空结果）→ 仍 ADVANCE。"""
        old = "2026-01-15T10:00:00"
        chunk = make_chunk(make_dt(10, 0), make_dt(12, 0))
        result = make_result(chunk, success=True, record_count=0)
        update = advance_cursor(old, result)

        assert update.action == CursorAction.ADVANCE


# ---------------------------------------------------------------------------
# chunk 失败 → NOOP
# ---------------------------------------------------------------------------


class TestChunkFailure:
    """chunk 失败 → NOOP。"""

    def test_failure_noops(self) -> None:
        """success=False → NOOP，cursor 停留。"""
        old = "2026-01-15T10:00:00"
        chunk = make_chunk(make_dt(10, 0), make_dt(12, 0))
        result = make_result(chunk, success=False, record_count=5)
        update = advance_cursor(old, result)

        assert update.action == CursorAction.NOOP
        assert update.new_cursor == old
        assert "chunk failed" in (update.warning or "")

    def test_failure_first_chunk(self) -> None:
        """首次 chunk 失败 → NOOP。"""
        chunk = make_chunk(make_dt(10, 0), make_dt(12, 0))
        result = make_result(chunk, success=False)
        update = advance_cursor(None, result)

        assert update.action == CursorAction.NOOP
        assert update.new_cursor is None


# ---------------------------------------------------------------------------
# 回退防护：new ≤ old → SKIP + WARNING
# ---------------------------------------------------------------------------


class TestRetreatProtection:
    """回退防护：新 cursor ≤ 旧 cursor → SKIP + WARNING。"""

    def test_equal_cursor_skips(self) -> None:
        """新 cursor = 旧 cursor → SKIP。"""
        old = "2026-01-15T10:00:00"
        chunk = make_chunk(make_dt(8, 0), make_dt(10, 0))
        result = make_result(chunk, success=True)
        update = advance_cursor(old, result)

        assert update.action == CursorAction.SKIP
        assert update.new_cursor == old
        assert "cursor not advancing" in (update.warning or "")

    def test_retreat_cursor_skips(self) -> None:
        """新 cursor < 旧 cursor → SKIP。"""
        old = "2026-01-15T12:00:00"
        chunk = make_chunk(make_dt(8, 0), make_dt(10, 0))
        result = make_result(chunk, success=True)
        update = advance_cursor(old, result)

        assert update.action == CursorAction.SKIP
        assert update.new_cursor == old
        assert "cursor not advancing" in (update.warning or "")

    def test_warning_contains_timestamps(self) -> None:
        """WARNING 信息包含时间戳。"""
        old = "2026-01-15T12:00:00"
        chunk = make_chunk(make_dt(8, 0), make_dt(10, 0))
        result = make_result(chunk, success=True)
        update = advance_cursor(old, result)

        assert update.warning is not None
        assert "2026-01-15T10:00:00" in update.warning
        assert "2026-01-15T12:00:00" in update.warning


# ---------------------------------------------------------------------------
# 3-chunk 窗口第 2 chunk 失败（TC-P-010）
# ---------------------------------------------------------------------------


class TestTC_P_010:
    """TC-P-010：3-chunk 窗口第 2 chunk 失败 → PARTIAL。"""

    def test_third_chunk_second_failed(self) -> None:
        """3-chunk 窗口，第 2 chunk 失败 → cursor 停留+chunk3 也 NOOP。"""
        t0 = datetime(2026, 1, 15, 0, 0)
        chunks = [
            make_chunk(t0, t0 + timedelta(hours=8)),
            make_chunk(t0 + timedelta(hours=8), t0 + timedelta(hours=16)),
            make_chunk(t0 + timedelta(hours=16), t0 + timedelta(hours=24)),
        ]
        results = [
            make_result(chunks[0], success=True, record_count=100),
            make_result(chunks[1], success=False, record_count=50),
            make_result(chunks[2], success=True, record_count=80),
        ]

        final_cursor, updates = advance_chunks(None, results)

        assert len(updates) == 3
        assert updates[0].action == CursorAction.ADVANCE
        assert updates[1].action == CursorAction.NOOP
        assert updates[2].action == CursorAction.NOOP  # 前序失败，NOOP
        # cursor 停留在 chunk 1 end
        expected_cursor = "2026-01-15T08:00:00"
        assert final_cursor == expected_cursor, (
            f"Expected cursor={expected_cursor}, got {final_cursor}"
        )

    def test_all_chunks_success(self) -> None:
        """全部成功 → cursor=最后一 chunk end。"""
        t0 = datetime(2026, 1, 15, 0, 0)
        chunks = [
            make_chunk(t0, t0 + timedelta(hours=8)),
            make_chunk(t0 + timedelta(hours=8), t0 + timedelta(hours=16)),
            make_chunk(t0 + timedelta(hours=16), t0 + timedelta(hours=24)),
        ]
        results = [make_result(c, success=True, record_count=100) for c in chunks]

        final_cursor, updates = advance_chunks(None, results)

        assert len(updates) == 3
        assert all(u.action == CursorAction.ADVANCE for u in updates)
        expected_cursor = "2026-01-16T00:00:00"  # 24:00 = next day 00:00
        assert final_cursor == expected_cursor


# ---------------------------------------------------------------------------
# 批量推进（advance_chunks）
# ---------------------------------------------------------------------------


class TestAdvanceChunks:
    """批量推进测试。"""

    def test_batch_all_success(self) -> None:
        """全部成功 → 最终 cursor=最后一 chunk end。"""
        t0 = datetime(2026, 1, 15, 0, 0)
        chunk1 = make_chunk(t0, t0 + timedelta(hours=8))
        chunk2 = make_chunk(t0 + timedelta(hours=8), t0 + timedelta(hours=16))
        results = [
            make_result(chunk1, success=True, record_count=100),
            make_result(chunk2, success=True, record_count=100),
        ]

        final_cursor, updates = advance_chunks(None, results)

        assert len(updates) == 2
        assert all(u.action == CursorAction.ADVANCE for u in updates)
        assert final_cursor == "2026-01-15T16:00:00"

    def test_batch_mixed_results(self) -> None:
        """混合结果 → 最终 cursor=最后一个 ADVANCE 的 chunk end。"""
        t0 = datetime(2026, 1, 15, 0, 0)
        chunk1 = make_chunk(t0, t0 + timedelta(hours=8))
        chunk2 = make_chunk(t0 + timedelta(hours=8), t0 + timedelta(hours=16))
        chunk3 = make_chunk(t0 + timedelta(hours=16), t0 + timedelta(hours=24))
        results = [
            make_result(chunk1, success=True, record_count=100),
            make_result(chunk2, success=False, record_count=50),
            make_result(chunk3, success=True, record_count=80),
        ]

        final_cursor, updates = advance_chunks(None, results)

        assert len(updates) == 3
        assert updates[0].action == CursorAction.ADVANCE
        assert updates[1].action == CursorAction.NOOP
        assert updates[2].action == CursorAction.NOOP  # 前序失败，NOOP
        assert final_cursor == "2026-01-15T08:00:00"

    def test_batch_with_retreat(self) -> None:
        """含回退 → SKIP，最终 cursor=回退前的值。"""
        t0 = datetime(2026, 1, 15, 0, 0)
        # 两个 chunk end=08:00，old=08:00 → 第一次 SKIP，第二次 SKIP
        chunk1 = make_chunk(t0, t0 + timedelta(hours=8))
        chunk2 = make_chunk(t0, t0 + timedelta(hours=8))
        results = [
            make_result(chunk1, success=True, record_count=100),
            make_result(chunk2, success=True, record_count=100),
        ]

        old = "2026-01-15T08:00:00"
        final_cursor, updates = advance_chunks(old, results)

        assert len(updates) == 2
        # chunk end (08:00) == old (08:00) → SKIP
        assert updates[0].action == CursorAction.SKIP
        assert updates[1].action == CursorAction.SKIP
        assert final_cursor == old


# ---------------------------------------------------------------------------
# CursorUpdate 验证
# ---------------------------------------------------------------------------


class TestCursorUpdate:
    """CursorUpdate 对象验证。"""

    def test_repr(self) -> None:
        """CursorUpdate repr 包含所有字段。"""
        update = CursorUpdate(
            action=CursorAction.ADVANCE,
            new_cursor="2026-01-15T10:00:00",
            warning=None,
        )
        repr_str = repr(update)
        assert "advance" in repr_str
        assert "2026-01-15T10:00:00" in repr_str

    def test_skip_with_warning(self) -> None:
        """SKIP action 携带 warning。"""
        update = CursorUpdate(
            action=CursorAction.SKIP,
            new_cursor="2026-01-15T10:00:00",
            warning="cursor not advancing: test",
        )
        assert update.action == CursorAction.SKIP
        assert update.new_cursor == "2026-01-15T10:00:00"
        assert update.warning == "cursor not advancing: test"

    def test_noop_preserves_old_cursor(self) -> None:
        """NOOP action 保留旧 cursor。"""
        old = "2026-01-15T10:00:00"
        update = CursorUpdate(
            action=CursorAction.NOOP,
            new_cursor=old,
            warning="chunk failed",
        )
        assert update.action == CursorAction.NOOP
        assert update.new_cursor == old


# ---------------------------------------------------------------------------
# 边界：时间精度
# ---------------------------------------------------------------------------


class TestBoundaryPrecision:
    """边界精度测试。"""

    def test_second_precision(self) -> None:
        """秒精度：10:00:01 > 10:00:00。"""
        old = "2026-01-15T10:00:00"
        chunk = make_chunk(make_dt(10, 0), datetime(2026, 1, 15, 10, 0, 1))
        result = make_result(chunk, success=True)
        update = advance_cursor(old, result)

        assert update.action == CursorAction.ADVANCE

    def test_same_second_boundary(self) -> None:
        """同秒边界：10:00:00 == 10:00:00 → SKIP。"""
        old = "2026-01-15T10:00:00"
        chunk = make_chunk(make_dt(9, 0), datetime(2026, 1, 15, 10, 0, 0))
        result = make_result(chunk, success=True)
        update = advance_cursor(old, result)

        assert update.action == CursorAction.SKIP
