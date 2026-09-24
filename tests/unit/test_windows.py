"""窗口拆分 + Chunk 计算测试（D05 §2/§5.2，ACQUISITION-002，D09 TC-P 组）。

覆盖：
- 基础功能：incremental/backfill、无 cursor 首次、空窗口
- 六数据集×两 mode 窗口快照（D05 §5.2 全部数据集）
- 边界：闰日/年边界窗口切分、00:00 分区归属
- 失败：unknown dataset_id → ValueError
- property：TC-PROP-002 区间拼接律
- cursor 推进验证：incremental mode 下 cursor=T1000 → 下窗口 start=T1000-overlap
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta

import pytest

from chronoforge.connectors.base import FetchRequest
from chronoforge.pipeline.windows import (
    WINDOW_CONFIG,
    AcquisitionJob,
    Chunk,
    plan_chunks,
    verify_interval_concatenation,
)

# ---------------------------------------------------------------------------
# 辅助函数
# ---------------------------------------------------------------------------


def make_dt(hour: int = 0, minute: int = 0) -> datetime:
    """快捷构造 datetime（UTC naive）。"""
    return datetime(2026, 1, 15, hour, minute)


# ---------------------------------------------------------------------------
# AcquisitionJob frozen 测试
# ---------------------------------------------------------------------------


class TestAcquisitionJobFrozen:
    """AcquisitionJob 不可变性验证。"""

    def test_frozen_raises_on_assignment(self) -> None:
        job = AcquisitionJob(dataset_id="test_ds")
        with pytest.raises(FrozenInstanceError):  # frozen dataclass
            job.dataset_id = "changed"

    def test_default_mode_is_incremental(self) -> None:
        job = AcquisitionJob(dataset_id="test_ds")
        assert job.mode == "incremental"

    def test_defaults(self) -> None:
        job = AcquisitionJob(dataset_id="test_ds", priority=5)
        assert job.dataset_id == "test_ds"
        assert job.start is None
        assert job.end is None
        assert job.mode == "incremental"
        assert job.priority == 5


# ---------------------------------------------------------------------------
# Chunk frozen 测试
# ---------------------------------------------------------------------------


class TestChunkFrozen:
    """Chunk 不可变性验证。"""

    def test_frozen(self) -> None:
        request = FetchRequest(
            dataset_id="test",
            params={},
            start=datetime(2026, 1, 1),
            end=datetime(2026, 1, 2),
            cursor=None,
        )
        chunk = Chunk(
            chunk_id="abcd1234",
            dataset_id="test",
            start=request.start,
            end=request.end,
            request=request,
        )
        assert chunk.chunk_id == "abcd1234"
        with pytest.raises(FrozenInstanceError):  # frozen dataclass
            chunk.chunk_id = "changed"


# ---------------------------------------------------------------------------
# plan_chunks：基础功能
# ---------------------------------------------------------------------------


class TestPlanChunksBasic:
    """基础功能测试。"""

    def test_incremental_no_cursor_no_start_returns_empty(self) -> None:
        """incremental + 无 cursor + 无 start → 空窗口。"""
        job = AcquisitionJob(
            dataset_id="binance_spot_klines",
            mode="incremental",
        )
        now = make_dt(12, 0)
        chunks = plan_chunks(job, None, now)
        assert chunks == []

    def test_incremental_with_cursor_creates_chunks(self) -> None:
        """incremental + 有 cursor → 生成窗口。"""
        cursor = "2026-01-15T10:00:00"
        job = AcquisitionJob(
            dataset_id="binance_spot_klines",
            mode="incremental",
        )
        now = make_dt(12, 0)
        chunks = plan_chunks(job, cursor, now)
        assert len(chunks) > 0
        # 第一个 chunk start = cursor - overlap(3600s=1h) = 09:00
        assert chunks[0].start == datetime(2026, 1, 15, 9, 0)
        # chunk end = now = 12:00
        assert chunks[0].end == datetime(2026, 1, 15, 12, 0)
        # 每个 chunk 包含 FetchRequest
        assert isinstance(chunks[0].request, FetchRequest)
        assert chunks[0].request.dataset_id == "binance_spot_klines"

    def test_backfill_with_start_end(self) -> None:
        """backfill + start/end → 生成窗口。"""
        start = make_dt(0, 0)
        end = start + timedelta(hours=48)  # 48 hours later
        job = AcquisitionJob(
            dataset_id="binance_spot_klines",
            start=start,
            end=end,
            mode="backfill",
        )
        now = end
        chunks = plan_chunks(job, None, now)
        # chunk_size = 86400s = 24h，48h 窗口含 overlap → 约 2-3 个 chunk
        assert len(chunks) >= 1
        # 非末尾 chunk 的跨度 = 24h（仅最后一个可能是余数）
        for chunk in chunks[:-1]:
            assert (chunk.end - chunk.start).total_seconds() == 86400
        # 第一个 chunk start = start - overlap = -1h（前一天 23:00）
        expected_first_start = datetime(2026, 1, 14, 23, 0)
        assert chunks[0].start == expected_first_start

    def test_backfill_no_start_returns_empty(
        self,
    ) -> None:
        """backfill + 无 start → 空窗口。"""
        job = AcquisitionJob(
            dataset_id="binance_spot_klines",
            mode="backfill",
        )
        now = make_dt(12, 0)
        chunks = plan_chunks(job, None, now)
        assert chunks == []

    def test_empty_window_range(self) -> None:
        """start >= end → 空窗口。"""
        job = AcquisitionJob(
            dataset_id="binance_spot_klines",
            start=make_dt(12, 0),
            end=make_dt(10, 0),  # end < start
            mode="backfill",
        )
        now = make_dt(12, 0)
        chunks = plan_chunks(job, None, now)
        assert chunks == []


# ---------------------------------------------------------------------------
# 六数据集×两 mode 窗口快照（D05 §5.2）
# ---------------------------------------------------------------------------


class TestSixDatasetsTwoModes:
    """D05 §5.2 全部数据集的窗口计算验证（含 backfill）。"""

    # 所有在 WINDOW_CONFIG 中注册的数据集
    DATASET_CONFIGS = [
        # (dataset_id, expected_overlap_seconds, expected_chunk_size_seconds)
        ("binance_spot_klines", 3600, 86400),
        ("binance_futures_klines", 3600, 86400),
        ("binance_futures_funding", 28800, 86400),
        ("binance_spot_aggtrades", 60, 3600),
        ("binance_futures_open_interest", 0, 86400),
        ("deribit_open_interest", 0, 86400),
        ("ccxt_ohlcv", 3600, 86400),
        ("ccxt_trades", 60, 3600),
        ("ccxt_ticker", 0, 86400),
        ("fred_series", "full_window", "full_window"),
        ("sec_submissions", 86400, "full_window"),
        ("yahoo_chart", 3600, 86400),
        # 用户自定义数据集（klines 语义）
        ("btcusdt-ohlcv-1h", 3600, 86400),
        ("btcusdt-ohlcv-4h", 3600, 86400),
        ("btcusd-yahoo-1h", 3600, 86400),
    ]

    @pytest.mark.parametrize(
        "dataset_id,expected_overlap,expected_chunk_size",
        DATASET_CONFIGS,
    )
    def test_incremental_mode_creates_chunks(
        self,
        dataset_id: str,
        expected_overlap: int | str,
        expected_chunk_size: int | str,
    ) -> None:
        """incremental mode：有 cursor → 生成至少 1 个 chunk。"""
        cursor = "2026-01-15T10:00:00"
        job = AcquisitionJob(
            dataset_id=dataset_id,
            mode="incremental",
        )
        now = datetime(2026, 1, 15, 12, 0)
        chunks = plan_chunks(job, cursor, now)

        # 至少一个 chunk
        assert len(chunks) >= 1, f"{dataset_id} incremental should produce chunks"

        # 检查 chunk 时间范围正确
        if expected_overlap == 0:
            # 快照类：无 overlap，start = cursor
            assert chunks[0].start == datetime(2026, 1, 15, 10, 0)
        elif expected_overlap == "full_window":
            # 全窗口 diff：start = cursor
            assert chunks[0].start == datetime(2026, 1, 15, 10, 0)
        else:
            # 有 overlap：start = cursor - overlap
            expected_start = datetime(2026, 1, 15, 10, 0) - timedelta(seconds=expected_overlap)
            assert chunks[0].start == expected_start

    @pytest.mark.parametrize(
        "dataset_id,expected_overlap,expected_chunk_size",
        DATASET_CONFIGS,
    )
    def test_backfill_mode_creates_chunks(
        self,
        dataset_id: str,
        expected_overlap: int | str,
        expected_chunk_size: int | str,
    ) -> None:
        """backfill mode：有 start/end → 生成至少 1 个 chunk。"""
        start = datetime(2026, 1, 13, 0, 0)
        end = datetime(2026, 1, 15, 12, 0)
        job = AcquisitionJob(
            dataset_id=dataset_id,
            start=start,
            end=end,
            mode="backfill",
        )
        now = end
        chunks = plan_chunks(job, None, now)

        assert len(chunks) >= 1, f"{dataset_id} backfill should produce chunks"
        # chunk 的 start 应在 start 附近（含 overlap）
        if expected_overlap == 0:
            assert chunks[0].start == start
        elif expected_overlap == "full_window":
            assert chunks[0].start == start
        else:
            expected_start = start - timedelta(seconds=expected_overlap)
            assert chunks[0].start == expected_start

    def test_fred_full_window_no_split(self) -> None:
        """FRED 全窗口 diff：不拆分，仅 1 个 chunk。"""
        job = AcquisitionJob(
            dataset_id="fred_series",
            start=datetime(2026, 1, 1),
            end=datetime(2026, 1, 15),
            mode="backfill",
        )
        chunks = plan_chunks(job, None, job.end)
        assert len(chunks) == 1, "FRED full_window should produce exactly 1 chunk"
        # 覆盖 incremental 也相同
        job_inc = AcquisitionJob(
            dataset_id="fred_series",
            mode="incremental",
        )
        cursor = "2025-12-31T00:00:00"
        chunks_inc = plan_chunks(job_inc, cursor, datetime(2026, 1, 15))
        assert len(chunks_inc) == 1, "FRED incremental should also produce exactly 1 chunk"

    def test_sec_full_window_no_split(self) -> None:
        """SEC filing-recent：全窗口不拆分。"""
        job = AcquisitionJob(
            dataset_id="sec_submissions",
            mode="incremental",
        )
        cursor = "2026-01-14T00:00:00"
        chunks = plan_chunks(job, cursor, datetime(2026, 1, 15))
        assert len(chunks) == 1, "SEC should produce exactly 1 chunk (full_window)"

    def test_chunk_contains_correct_fetch_request(self) -> None:
        """每个 chunk 包含正确的 FetchRequest。"""
        cursor = "2026-01-15T10:00:00"
        job = AcquisitionJob(
            dataset_id="binance_spot_klines",
            mode="incremental",
        )
        now = datetime(2026, 1, 15, 12, 0)
        chunks = plan_chunks(job, cursor, now)

        for chunk in chunks:
            assert chunk.request.dataset_id == "binance_spot_klines"
            assert chunk.request.start == chunk.start
            assert chunk.request.end == chunk.end
            assert isinstance(chunk.request.cursor, str) or chunk.request.cursor is None


# ---------------------------------------------------------------------------
# 边界测试：闰日/年边界/00:00 分区
# ---------------------------------------------------------------------------


class TestBoundaryConditions:
    """边界条件测试。"""

    def test_leap_year_boundary(self) -> None:
        """闰年 2 月 29 日窗口切分。"""
        start = datetime(2024, 2, 28, 0, 0)
        end = datetime(2024, 3, 2, 0, 0)  # 跨越闰日
        job = AcquisitionJob(
            dataset_id="binance_spot_klines",
            start=start,
            end=end,
            mode="backfill",
        )
        chunks = plan_chunks(job, None, end)
        assert len(chunks) >= 1
        # 所有 chunk 的日期应在有效范围内
        for chunk in chunks:
            assert chunk.start >= start - timedelta(seconds=3600)  # overlap
            assert chunk.end <= end

    def test_year_boundary(self) -> None:
        """年边界窗口切分。"""
        start = datetime(2025, 12, 31, 12, 0)
        end = datetime(2026, 1, 1, 12, 0)
        job = AcquisitionJob(
            dataset_id="binance_spot_klines",
            start=start,
            end=end,
            mode="backfill",
        )
        chunks = plan_chunks(job, None, end)
        assert len(chunks) >= 1
        # 检查跨年
        has_dec_chunk = any(c.start.year == 2025 for c in chunks)
        has_jan_chunk = any(c.end.year == 2026 for c in chunks)
        assert has_dec_chunk or has_jan_chunk, "Should span year boundary"

    def test_midnight_partition_boundary(self) -> None:
        """00:00 分区归属：chunk end 恰好为 00:00 应正确归属下一天。"""
        # 构造一个恰好 24h 的窗口，end=00:00
        start = datetime(2026, 1, 15, 0, 0)
        end = datetime(2026, 1, 16, 0, 0)
        job = AcquisitionJob(
            dataset_id="binance_spot_klines",
            start=start,
            end=end,
            mode="backfill",
        )
        chunks = plan_chunks(job, None, end)
        # 含 overlap（1h），实际窗口 25h，拆分为 2 个 chunk
        assert len(chunks) == 2
        # 最后一个 chunk 的 end = 00:00
        assert chunks[-1].end == datetime(2026, 1, 16, 0, 0)

    def test_multiple_chunk_split(self) -> None:
        """大窗口正确拆分为多个 chunk。"""
        start = datetime(2026, 1, 1, 0, 0)
        end = datetime(2026, 1, 4, 0, 0)  # 72h + overlap=1h → 73h
        job = AcquisitionJob(
            dataset_id="binance_spot_klines",
            start=start,
            end=end,
            mode="backfill",
        )
        chunks = plan_chunks(job, None, end)
        # chunk_size = 86400s = 24h，73h → 4 个 chunk
        assert len(chunks) == 4
        # 非末尾 chunk 的跨度 = 24h（仅最后一个可能是余数）
        for chunk in chunks[:-1]:
            assert chunk.end - chunk.start == timedelta(hours=24)
        # chunks 应时间连续
        for i in range(1, len(chunks)):
            assert chunks[i].start == chunks[i - 1].end


# ---------------------------------------------------------------------------
# 错误处理
# ---------------------------------------------------------------------------


class TestErrorHandling:
    """错误处理测试。"""

    def test_unknown_dataset_id_raises(self) -> None:
        """未知 dataset_id → ValueError。"""
        job = AcquisitionJob(
            dataset_id="unknown_dataset_xyz",
            mode="backfill",
        )
        with pytest.raises(ValueError, match="unknown dataset_id"):
            plan_chunks(job, None, datetime(2026, 1, 15))

    def test_invalid_cursor_format_raises(self) -> None:
        """无效 cursor 格式 → ValueError。"""
        job = AcquisitionJob(
            dataset_id="binance_spot_klines",
            mode="incremental",
        )
        with pytest.raises(ValueError, match="invalid cursor format"):
            plan_chunks(job, "not-a-datetime", datetime(2026, 1, 15))


# ---------------------------------------------------------------------------
# property 测试：区间拼接律（TC-PROP-002）
# ---------------------------------------------------------------------------


class TestPropertyIntervalConcatenation:
    """TC-PROP-002：acquire(A,B) + acquire(B,C) ≡ acquire(A,C)。"""

    def test_klines_incremental_concatenation(self) -> None:
        """klines incremental：split_point 拼接验证。"""
        split_point = datetime(2026, 1, 15, 12, 0)
        now = datetime(2026, 1, 15, 20, 0)

        job = AcquisitionJob(
            dataset_id="binance_spot_klines",
            start=datetime(2026, 1, 15, 0, 0),
            end=datetime(2026, 1, 15, 20, 0),
            mode="backfill",
        )

        result = verify_interval_concatenation(job, split_point, None, now)
        assert result is True, "Interval concatenation should hold for klines"

    def test_fred_full_window_concatenation(self) -> None:
        """FRED full_window：单 chunk，拼接恒成立。"""
        split_point = datetime(2026, 1, 8, 0, 0)  # 中点
        now = datetime(2026, 1, 15, 0, 0)

        job = AcquisitionJob(
            dataset_id="fred_series",
            start=datetime(2026, 1, 1, 0, 0),
            end=datetime(2026, 1, 15, 0, 0),
            mode="backfill",
        )

        result = verify_interval_concatenation(job, split_point, None, now)
        assert result is True

    def test_cursor_based_incremental_concatenation(self) -> None:
        """基于 cursor 的 incremental 拼接验证。"""
        split_point = datetime(2026, 1, 15, 10, 0)
        cursor = "2026-01-15T08:00:00"
        now = datetime(2026, 1, 15, 20, 0)

        job = AcquisitionJob(
            dataset_id="binance_spot_klines",
            mode="incremental",
        )

        result = verify_interval_concatenation(job, split_point, cursor, now)
        assert result is True


# ---------------------------------------------------------------------------
# cursor 推进验证：overlap 生效
# ---------------------------------------------------------------------------


class TestOverlapEffective:
    """overlap 生效验证。"""

    def test_incremental_cursor_overlaps(self) -> None:
        """incremental mode 下 cursor=T1000 → 下窗口 start=T1000-overlap。"""
        cursor = "2026-01-15T10:00:00"
        job = AcquisitionJob(
            dataset_id="binance_spot_klines",
            mode="incremental",
        )
        now = datetime(2026, 1, 15, 12, 0)
        chunks = plan_chunks(job, cursor, now)

        assert len(chunks) >= 1
        # klines overlap = 3600s = 1h
        expected_start = datetime(2026, 1, 15, 9, 0)
        assert chunks[0].start == expected_start, (
            f"Expected start {expected_start}, got {chunks[0].start}"
        )

    def test_funding_overlaps(self) -> None:
        """funding overlap = 8h。"""
        cursor = "2026-01-15T10:00:00"
        job = AcquisitionJob(
            dataset_id="binance_futures_funding",
            mode="incremental",
        )
        now = datetime(2026, 1, 15, 12, 0)
        chunks = plan_chunks(job, cursor, now)

        assert len(chunks) >= 1
        # funding overlap = 28800s = 8h
        expected_start = datetime(2026, 1, 15, 2, 0)
        assert chunks[0].start == expected_start


# ---------------------------------------------------------------------------
# FetchRequest 构造验证
# ---------------------------------------------------------------------------


class TestFetchRequestConstruction:
    """FetchRequest 参数验证。"""

    def test_first_chunk_cursor_is_job_cursor(self) -> None:
        """首个 chunk 的 cursor = job.cursor（非已推进的 chunk_start）。"""
        cursor = "2026-01-15T10:00:00"
        job = AcquisitionJob(
            dataset_id="binance_spot_klines",
            mode="incremental",
        )
        now = datetime(2026, 1, 15, 12, 0)
        chunks = plan_chunks(job, cursor, now)

        # 第一个 chunk 的 request cursor = 传入的 cursor
        assert chunks[0].request.cursor == cursor

    def test_subsequent_chunks_have_cursor(self) -> None:
        """后续 chunk 的 cursor = 前一个 chunk 的 end。"""
        start = datetime(2026, 1, 1, 0, 0)
        end = datetime(2026, 1, 3, 0, 0)  # 48h → 2 chunks
        job = AcquisitionJob(
            dataset_id="binance_spot_klines",
            start=start,
            end=end,
            mode="backfill",
        )
        chunks = plan_chunks(job, None, end)

        if len(chunks) >= 2:
            assert chunks[1].request.cursor is not None
            assert chunks[1].request.cursor == chunks[0].end.isoformat()

    def test_job_params_threaded_to_chunked_requests(self) -> None:
        """审计 M-3：AcquisitionJob.params 贯通至每个 Chunk 的 FetchRequest.params
        （connectors 依赖 symbol/data_type/interval 构造请求）"""
        job = AcquisitionJob(
            dataset_id="binance_spot_klines",
            start=datetime(2026, 1, 1, 0, 0),
            end=datetime(2026, 1, 3, 0, 0),  # 48h → 2 chunks
            mode="backfill",
            params={"symbol": "BTCUSDT", "interval": "1m", "data_type": "klines"},
        )
        chunks = plan_chunks(job, None, job.end)

        assert len(chunks) >= 2
        for chunk in chunks:
            assert chunk.request.params == {
                "symbol": "BTCUSDT", "interval": "1m", "data_type": "klines",
            }

    def test_job_params_threaded_to_full_window_request(self) -> None:
        """审计 M-3：全窗口 diff 类（fred）同样贯通 params"""
        job = AcquisitionJob(
            dataset_id="fred_series",
            start=datetime(2026, 1, 1),
            end=datetime(2026, 1, 15),
            mode="backfill",
            params={"series_id": "GDP"},
        )
        chunks = plan_chunks(job, None, job.end)

        assert len(chunks) == 1
        assert chunks[0].request.params == {"series_id": "GDP"}

    def test_job_params_default_empty(self) -> None:
        """未传 params 时 FetchRequest.params 为空 dict（向后兼容）"""
        job = AcquisitionJob(
            dataset_id="binance_spot_klines",
            start=datetime(2026, 1, 1, 0, 0),
            end=datetime(2026, 1, 1, 1, 0),
            mode="backfill",
        )
        chunks = plan_chunks(job, None, job.end)
        assert chunks[0].request.params == {}

    def test_window_config_keys_match_test_registry(self) -> None:
        """审计 M-2 守卫：DATASET_CONFIGS 与 WINDOW_CONFIG 键集一致，
        且 spot 无 funding/OI（源无对应端点）"""
        registered = set(WINDOW_CONFIG.keys())
        tested = {ds for ds, _, _ in TestSixDatasetsTwoModes.DATASET_CONFIGS}
        assert registered == tested
        assert "binance_spot_funding" not in registered
        assert "binance_spot_open_interest" not in registered
