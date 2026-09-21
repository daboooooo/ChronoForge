"""窗口拆分 + Chunk 计算（D05 §2/§5.2，ACQUISITION-002）。

包含：
- AcquisitionJob：获取任务（审计 F-01）
- Chunk：单次窗口请求（含 FetchRequest）
- plan_chunks：表驱动窗口计算

窗口参数由 dataset_registry 中的 continuity_model 和 frequency 决定，
按 D05 §5.2 语义表驱动，禁止硬编码。
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal

from chronoforge.connectors.base import FetchRequest

if TYPE_CHECKING:
    pass

# ---------------------------------------------------------------------------
# AcquisitionJob（D05 §2，审计 F-01）
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AcquisitionJob:
    """获取任务（D05 §2，审计 F-01）。

    Args:
        dataset_id: 数据集唯一标识（如 "binance_spot_klines_1m"）
        start: 起始时间（None → incremental 从 checkpoint 续传）
        end: 结束时间（None → now()）
        mode: 获取模式（incremental / backfill）
        priority: 优先级（run_many 排序用）
        params: 源侧查询参数（如 {"symbol": "BTCUSDT", "interval": "1m"}，
                来自 dataset_registry.params_json，审计 M-3：贯通至
                FetchRequest.params——connectors 依赖 symbol/data_type/
                interval 等参数构造请求）
    """

    dataset_id: str
    start: datetime | None = None
    end: datetime | None = None
    mode: Literal["incremental", "backfill"] = "incremental"
    priority: int = 0
    params: Mapping[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Chunk（D05 §5.2）
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Chunk:
    """单次窗口请求（D05 §5.2）。

    Args:
        chunk_id: 窗口唯一标识（uuid hex[:8]）
        dataset_id: 数据集标识
        start: 窗口起始（UTC naive）
        end: 窗口结束（UTC naive）
        request: 构造后由 FetchStage 调用 connector.fetch()
    """

    chunk_id: str
    dataset_id: str
    start: datetime
    end: datetime
    request: FetchRequest


# ---------------------------------------------------------------------------
# ChunkResult（窗口请求返回结果）
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ChunkResult:
    """单次窗口请求结果（供 cursor.py 使用）。

    Args:
        chunk: 对应的 Chunk
        success: 是否成功（含 PARTIAL_SUCCESS）
        record_count: 返回记录数（0 = 正常空结果）
    """

    chunk: Chunk
    success: bool
    record_count: int = 0


# ---------------------------------------------------------------------------
# 表驱动窗口配置（D05 §5.2 语义表）
# ---------------------------------------------------------------------------

# overlap 值：
#   int = 固定秒数（如 klines=3600s=1h）
#   "full_window" = 全窗口 diff（如 fred）
#
# 端点语义：全部数据集含端点（D05 §5.2），由 plan_chunks 实现固定，
# 不再暴露为配置项（审计 M-4：删除从不读取的 "boundary" 死配置）
#
# chunk_size_seconds：
#   int = 单次窗口最大跨度（秒）
#   "full_window" = 不拆分（如 fred/sec）

WINDOW_CONFIG: dict[str, dict[str, int | str]] = {
    # binance klines（1m interval → overlap=1×interval=3600s）
    "binance_spot_klines": {
        "overlap_seconds": 3600,
        "chunk_size_seconds": 86400,  # 24h
    },
    "binance_futures_klines": {
        "overlap_seconds": 3600,
        "chunk_size_seconds": 86400,
    },
    # funding（仅 futures，spot 无 funding 端点；8h cycle → overlap=28800s）
    "binance_futures_funding": {
        "overlap_seconds": 28800,
        "chunk_size_seconds": 86400,
    },
    # aggTrades：按时间窗口拆分
    "binance_spot_aggtrades": {
        "overlap_seconds": 60,
        "chunk_size_seconds": 3600,
    },
    # OI / deribit 快照：全量按 nk upsert，无需窗口
    # （审计 M-2：OI 仅 futures/deribit，spot 无 openInterest 端点）
    "binance_futures_open_interest": {
        "overlap_seconds": 0,
        "chunk_size_seconds": 86400,
    },
    "deribit_open_interest": {
        "overlap_seconds": 0,
        "chunk_size_seconds": 86400,
    },
    # ccxt 通用桥（type=ohlcv/trades/ticker，审计 M-2：ccxt 数据集注册）
    "ccxt_ohlcv": {
        "overlap_seconds": 3600,
        "chunk_size_seconds": 86400,
    },
    "ccxt_trades": {
        "overlap_seconds": 60,
        "chunk_size_seconds": 3600,
    },
    "ccxt_ticker": {
        "overlap_seconds": 0,
        "chunk_size_seconds": 86400,
    },
    # FRED：全窗口 diff
    "fred_series": {
        "overlap_seconds": "full_window",
        "chunk_size_seconds": "full_window",
    },
    # SEC：filing-recent 增量
    "sec_submissions": {
        "overlap_seconds": 86400,
        "chunk_size_seconds": "full_window",
    },
    # Yahoo：同 klines 语义
    "yahoo_chart": {
        "overlap_seconds": 3600,
        "chunk_size_seconds": 86400,
    },
}


# ---------------------------------------------------------------------------
# 辅助函数
# ---------------------------------------------------------------------------


def _parse_cursor(cursor: str) -> datetime:
    """将 cursor 字符串解析为 datetime（ISO 格式）。

    Args:
        cursor: ISO datetime 字符串

    Returns:
        datetime（UTC naive）

    Raises:
        ValueError: cursor 格式无效
    """
    try:
        return datetime.fromisoformat(cursor).replace(tzinfo=None)
    except (ValueError, TypeError) as exc:
        raise ValueError(f"invalid cursor format: {cursor!r}") from exc


def _format_cursor(dt: datetime) -> str:
    """将 datetime 格式化为 cursor 字符串（ISO 格式）。"""
    return dt.isoformat()


def _get_config(dataset_id: str) -> dict[str, int | str]:
    """获取数据集窗口配置。

    Args:
        dataset_id: 数据集标识

    Returns:
        窗口配置字典

    Raises:
        ValueError: 表外数据集
    """
    if dataset_id not in WINDOW_CONFIG:
        raise ValueError(
            f"unknown dataset_id: {dataset_id!r}. "
            f"Registered: {sorted(WINDOW_CONFIG.keys())}"
        )
    return WINDOW_CONFIG[dataset_id]


def _is_full_window(config: dict[str, int | str]) -> bool:
    """判断是否全窗口 diff 模式（fred/sec）。"""
    return config.get("chunk_size_seconds") == "full_window"


# ---------------------------------------------------------------------------
# plan_chunks（D05 §5.2 核心算法）
# ---------------------------------------------------------------------------


def plan_chunks(
    job: AcquisitionJob,
    cursor: str | None,
    now: datetime,
) -> list[Chunk]:
    """计算窗口序列（D05 §5.2 语义表驱动）。

    返回按时间排序的 Chunk 列表，每个 Chunk 含对应的 FetchRequest。

    窗口计算逻辑：
    - incremental：start = cursor - overlap（首次=start），end = job.end or now
    - backfill：start = job.start - overlap，end = job.end or now
    - 全窗口 diff 类（fred/sec）：不拆分，单次完整窗口请求
    - 普通窗口：按 chunk_size 滚动拆分

    Args:
        job: 获取任务
        cursor: 上次 checkpoint（ISO datetime 字符串），None = 首次/回填
        now: 当前时间（UTC naive）

    Returns:
        Chunk 列表（空 = 无数据需获取）

    Raises:
        ValueError: dataset_id 不在配置表中 / cursor 格式无效
    """
    config = _get_config(job.dataset_id)
    overlap_seconds = config["overlap_seconds"]
    chunk_size_seconds = config["chunk_size_seconds"]

    # 1. 确定窗口范围
    if job.mode == "incremental":
        if cursor is not None:
            cursor_dt = _parse_cursor(cursor)
            if isinstance(overlap_seconds, int) and overlap_seconds > 0:
                # overlap 生效：窗口回退
                start = cursor_dt - timedelta(seconds=overlap_seconds)
            else:
                # 无 overlap（快照类）或全窗口（fred/sec）
                start = cursor_dt
        elif job.start is not None:
            if isinstance(overlap_seconds, int) and overlap_seconds > 0:
                start = job.start - timedelta(seconds=overlap_seconds)
            else:
                start = job.start
        else:
            # incremental 且无 cursor 无 start → 无数据
            return []
    else:
        # backfill
        if job.start is None:
            return []
        if isinstance(overlap_seconds, int) and overlap_seconds > 0:
            start = job.start - timedelta(seconds=overlap_seconds)
        else:
            start = job.start

    end = job.end or now

    # 窗口范围为空
    if start >= end:
        return []

    # 2. 全窗口 diff 类（fred/sec）：不拆分
    if _is_full_window(config):
        request = FetchRequest(
            dataset_id=job.dataset_id,
            params=dict(job.params),  # 审计 M-3：源侧参数贯通
            start=start,
            end=end,
            cursor=cursor,
        )
        return [Chunk(
            chunk_id=uuid.uuid4().hex[:8],
            dataset_id=job.dataset_id,
            start=start,
            end=end,
            request=request,
        )]

    # 3. 普通窗口：按 chunk_size 拆分
    chunks: list[Chunk] = []
    chunk_start = start
    # After _is_full_window check, chunk_size_seconds is guaranteed int
    assert isinstance(chunk_size_seconds, int)

    while chunk_start < end:
        chunk_end = min(chunk_start + timedelta(seconds=chunk_size_seconds), end)

        request = FetchRequest(
            dataset_id=job.dataset_id,
            params=dict(job.params),  # 审计 M-3：源侧参数贯通
            start=chunk_start,
            end=chunk_end,
            cursor=cursor if len(chunks) == 0 else _format_cursor(chunk_start),
        )

        chunks.append(Chunk(
            chunk_id=uuid.uuid4().hex[:8],
            dataset_id=job.dataset_id,
            start=chunk_start,
            end=chunk_end,
            request=request,
        ))

        chunk_start = chunk_end

    return chunks


# ---------------------------------------------------------------------------
# property test helper：区间拼接律验证
# ---------------------------------------------------------------------------


def verify_interval_concatenation(
    job: AcquisitionJob,
    split_point: datetime,
    cursor: str | None,
    now: datetime,
) -> bool:
    """验证区间拼接律：acquire(A,B) + acquire(B,C) ≡ acquire(A,C)。

    D09 TC-PROP-002 性质测试用。

    Args:
        job: 获取任务（需设置 start/end 覆盖完整范围）
        split_point: 中间分割点
        cursor: 上次 checkpoint
        now: 当前时间

    Returns:
        True 如果拼接律成立
    """
    # acquire(A,C)：完整窗口
    full_chunks = plan_chunks(job, cursor, now)

    # acquire(A,B)：到 split_point
    job_ab = AcquisitionJob(
        dataset_id=job.dataset_id,
        start=job.start,
        end=split_point,
        mode=job.mode,
        priority=job.priority,
        params=job.params,
    )
    chunks_ab = plan_chunks(job_ab, cursor, now)

    # acquire(B,C)：从 split_point 开始
    # cursor 设为 split_point 的 ISO 字符串
    cursor_b = _format_cursor(split_point)
    job_bc = AcquisitionJob(
        dataset_id=job.dataset_id,
        start=None,  # incremental：从 cursor 续传
        end=job.end or now,
        mode="incremental",
        priority=job.priority,
        params=job.params,
    )
    chunks_bc = plan_chunks(job_bc, cursor_b, now)

    # 合并后的 start/end 应等于完整窗口的 start/end
    if not full_chunks:
        return not chunks_ab and not chunks_bc

    if not chunks_ab or not chunks_bc:
        # 全窗口 diff 模式：单 chunk 不拆分
        return len(full_chunks) == 1 and len(chunks_ab) == 1 and len(chunks_bc) == 1

    # 拼接后的时间范围应连续
    combined_start = chunks_ab[0].start
    combined_end = chunks_bc[-1].end

    return (
        combined_start == full_chunks[0].start
        and combined_end == full_chunks[-1].end
    )
