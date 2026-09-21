"""continuity_model 驱动的 gap 判定（D06 §2, VALIDATION-002）。

职责：
- ContinuityModel 枚举（ALWAYS_OPEN / TRADING_CALENDAR / EVENT_BASED / RELEASE_SCHEDULE）
- expected_grid()：按 continuity_model 生成期望网格，标注 EXPECTED_GAP
- 与 Q-GAP-001 集成：EXPECTED_GAP → INFO，交易日缺失 → WARNING

依赖：
- quality.calendar.is_trading_day()
- VALIDATION-001 的 gap_detection / QualityFinding
"""

from __future__ import annotations

from datetime import datetime, timedelta
from enum import StrEnum
from typing import Literal

from .calendar import is_trading_day

# ── 频率秒数映射 ─────────────────────────────────────────────────────
# 同时支持简写（D06 约定）和 pandas 风格别名

_FREQ_SECONDS: dict[str, int] = {
    # 简写（D06 约定）
    "1m": 60, "5m": 300, "15m": 900, "30m": 1800,
    "1h": 3600, "2h": 7200, "4h": 14400,
    "1d": 86400, "1w": 604800,
    # pandas 风格别名
    "1min": 60, "5min": 300, "15min": 900, "30min": 1800,
    "1H": 3600, "2H": 7200, "4H": 14400,
    "1D": 86400, "1W": 604800,
    "h": 3600, "d": 86400, "w": 604800, "min": 60,
}


def _interval_delta(freq: str) -> timedelta:
    """将频率字符串转为 timedelta。"""
    seconds = _FREQ_SECONDS.get(freq)
    if seconds is None:
        raise ValueError(f"Unsupported frequency: {freq}")
    return timedelta(seconds=seconds)


# ── ContinuityModel 枚举 ─────────────────────────────────────────────


class ContinuityModel(StrEnum):
    """连续性模型（D06 §2, D03 §1 continuity_model 列）。

    枚举值与 dataset_registry.continuity_model 列值一一对应。
    """

    ALWAYS_OPEN = "ALWAYS_OPEN"        # 7x24 不间断（加密源）
    TRADING_CALENDAR = "TRADING_CALENDAR"  # 交易日历（美股等）
    EVENT_BASED = "EVENT_BASED"        # 事件驱动（SEC filing）
    RELEASE_SCHEDULE = "RELEASE_SCHEDULE"  # 发布节奏（FRED）


# ── expected_grid ────────────────────────────────────────────────────

# 返回类型：[(start, end, gap_type), ...]
# gap_type = "EXPECTED_GAP" | "REGULAR"
_GridEntry = tuple[datetime, datetime, Literal["EXPECTED_GAP", "REGULAR"]]


def expected_grid(
    market_id: str,
    freq: str,
    continuity_model: ContinuityModel,
    start: datetime,
    end: datetime,
) -> list[_GridEntry]:
    """生成期望网格（D06 §2 Q-GAP-001）。

    按 continuity_model 生成期望的时间网格，标注非交易日缺失为
    EXPECTED_GAP。

    Args:
        market_id: 市场标识（仅用于调试）。
        freq: 频率字符串（如 "1d", "1h"）。
        continuity_model: 连续性模型。
        start: 起始时间。
        end: 结束时间。

    Returns:
        [(start, end, gap_type), ...]
        EXPECTED_GAP 标注非交易日缺失
        REGULAR 标注正常网格
    """
    match continuity_model:
        case ContinuityModel.ALWAYS_OPEN:
            # 全网格，无豁免
            grid = _date_range(start, end, freq)
            delta = _interval_delta(freq)
            return [
                (t, t + delta, "REGULAR") for t in grid
            ]

        case ContinuityModel.TRADING_CALENDAR:
            # 剔除非交易日 → EXPECTED_GAP
            return _trading_calendar_grid(start, end, freq)

        case ContinuityModel.EVENT_BASED | ContinuityModel.RELEASE_SCHEDULE:
            # 无网格 gap 概念
            return []


def _date_range(start: datetime, end: datetime, freq: str) -> list[datetime]:
    """生成等间距 datetime 列表。"""
    delta = _interval_delta(freq)
    result = []
    current = start
    while current <= end:
        result.append(current)
        current = current + delta
    return result


def _trading_calendar_grid(
    start: datetime, end: datetime, freq: str,
) -> list[_GridEntry]:
    """TRADING_CALENDAR 模型：按交易日生成网格，非交易日标 EXPECTED_GAP。

    合并连续的非交易日为一个 EXPECTED_GAP 区间。
    """
    delta = _interval_delta(freq)
    grid: list[_GridEntry] = []
    current = start
    non_trade_start: datetime | None = None

    while current <= end:
        if is_trading_day(current.date()):
            if non_trade_start is not None:
                # 结束连续非交易日区间
                grid.append((non_trade_start, current, "EXPECTED_GAP"))
                non_trade_start = None
            grid.append((current, current + delta, "REGULAR"))
            current = current + delta
        else:
            if non_trade_start is None:
                non_trade_start = current
            current = current + delta

    # 处理末尾连续非交易日
    if non_trade_start is not None:
        grid.append((non_trade_start, current, "EXPECTED_GAP"))

    return grid
