"""UTC 安全的时间换算助手（D04 §1：FetchRequest 时间为 UTC naive）。

项目全链路以 UTC naive datetime 表示时间。``datetime.timestamp()`` 会把
naive datetime 解释为主机本地时区，在 UTC+8 主机上产生 -8h 偏移，导致
请求窗口右界提前、未收盘判定提前、checkpoint 游标回退等静默错误。

本模块提供唯一的换算入口：naive 一律按 UTC 解释；aware datetime 按其
自带时偏移换算。连接器与存储层不得再直接对 naive datetime 调用
``.timestamp()``。
"""

from __future__ import annotations

import calendar
from datetime import UTC, datetime
from typing import Final

_MS_PER_SECOND: Final = 1000


def to_epoch_seconds(dt: datetime) -> int:
    """datetime → epoch 秒。naive 按 UTC 解释，aware 按其时区换算。"""
    if dt.tzinfo is None:
        return calendar.timegm(dt.timetuple())
    return int(dt.timestamp())


def to_epoch_milliseconds(dt: datetime) -> int:
    """datetime → epoch 毫秒。naive 按 UTC 解释，aware 按其时区换算。"""
    if dt.tzinfo is None:
        return (
            calendar.timegm(dt.timetuple()) * _MS_PER_SECOND
            + dt.microsecond // 1000
        )
    return int(dt.timestamp() * _MS_PER_SECOND)


def to_epoch_microseconds(dt: datetime) -> int:
    """datetime → epoch 微秒。naive 按 UTC 解释，aware 按其时区换算。"""
    if dt.tzinfo is None:
        return (
            calendar.timegm(dt.timetuple()) * 1_000_000
            + dt.microsecond
        )
    return int(dt.timestamp() * 1_000_000)


def utcnow() -> datetime:
    """当前 UTC 时间（aware，tzinfo=UTC）。"""
    return datetime.now(UTC)


def utcnow_naive() -> datetime:
    """当前 UTC 时间（naive，项目内部时间表示约定）。"""
    return datetime.now(UTC).replace(tzinfo=None)
