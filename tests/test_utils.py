import pytest
from datetime import datetime, timezone
import pandas as pd
from chronoforge.utils import (
    parse_timeframe_to_milliseconds,
    parse_timeframe_to_seconds,
    parse_timeframe_to_minutes,
    format_size,
    round_timeframe,
    prev_tf_timestamp,
    next_tf_timestamp,
    prev_tf_datetime,
    next_tf_datetime,
    TimeSlot,
    TimeSlotManager,
    TimeRange
)


def test_parse_timeframe_to_milliseconds():
    """测试时间框架转换为毫秒"""
    # 测试支持的时间框架
    assert parse_timeframe_to_milliseconds("1w") == 7 * 24 * 60 * 60 * 1000
    assert parse_timeframe_to_milliseconds("1d") == 24 * 60 * 60 * 1000
    assert parse_timeframe_to_milliseconds("4h") == 4 * 60 * 60 * 1000
    assert parse_timeframe_to_milliseconds("1h") == 60 * 60 * 1000

    # 测试不支持的时间框架
    with pytest.raises(ValueError):
        parse_timeframe_to_milliseconds("invalid")


def test_time_slot_initialization():
    """测试时间槽初始化"""
    # 测试daily类型时间槽
    time_slot = TimeSlot(start="00:00:00", end="23:59:59")
    assert time_slot.start == "00:00:00"
    assert time_slot.end == "23:59:59"
    assert time_slot.type == "daily"

    # 测试hourly类型时间槽
    time_slot = TimeSlot(start="00:00", end="59:59")
    assert time_slot.start == "00:00"
    assert time_slot.end == "59:59"
    assert time_slot.type == "hourly"

    # 测试无效时间格式
    with pytest.raises(ValueError):
        TimeSlot(start="0:00", end="23:59")


def test_time_slot_manager():
    """测试时间槽管理器"""
    manager = TimeSlotManager()

    # 添加时间槽
    time_slot = TimeSlot(start="00:00:00", end="23:59:59")
    manager.add_slot("test_slot", time_slot)

    # 检查时间槽是否存在
    assert "test_slot" in manager.timeslots

    # 测试删除时间槽
    manager.delete_slot("test_slot")
    assert "test_slot" not in manager.timeslots


def test_time_range_parsing():
    """测试时间范围解析"""
    # 测试基本时间范围解析
    timerange = TimeRange.parse_timerange("20240101-")
    assert timerange.start_ts_ms is not None
    assert timerange.end_ts_ms is not None  # 注意：实际实现会自动设置结束时间为当前时间

    # 测试完整时间范围解析
    timerange = TimeRange.parse_timerange("20240101-20240131")
    assert timerange.start_ts_ms is not None
    assert timerange.end_ts_ms is not None
    assert timerange.start_ts_ms < timerange.end_ts_ms


def test_time_range_initialization():
    """测试时间范围初始化"""
    # 测试基本初始化 - 注意：实际实现会验证时间范围并调整无效时间
    timerange = TimeRange(start_ts_ms=1640995200000, end_ts_ms=1643673600000)
    assert timerange.start_ts_ms == 1640995200000  # 2022-01-01
    assert timerange.end_ts_ms == 1643673600000  # 2022-02-01

    # 测试只有开始时间的初始化
    timerange = TimeRange(start_ts_ms=1640995200000)
    assert timerange.start_ts_ms == 1640995200000
    assert timerange.end_ts_ms is None  # 实际实现中，只有开始时间时，结束时间保持为None


def test_time_range_properties():
    """测试时间范围的属性和方法"""
    timerange = TimeRange(start_ts_ms=1640995200000, end_ts_ms=1643673600000)

    # 测试start_dt和end_dt属性
    assert isinstance(timerange.start_dt, datetime)
    assert isinstance(timerange.end_dt, datetime)

    # 测试parse_time方法
    assert timerange.parse_time("2024-01-01") is not None
    assert timerange.parse_time(1704067200) is not None  # 秒级时间戳
    assert timerange.parse_time(datetime(2024, 1, 1)) is not None

    # 测试contains方法
    test_date = datetime(2022, 1, 15, tzinfo=timezone.utc)
    assert timerange.contains(test_date) is True

    # 测试to_pandas_datetime方法
    start_pd, end_pd = timerange.to_pandas_datetime()
    assert isinstance(start_pd, pd.Timestamp)
    assert isinstance(end_pd, pd.Timestamp)

    # 测试__str__方法
    assert isinstance(str(timerange), str)

    # 测试align_to_timeframe方法
    aligned = timerange.align_to_timeframe(1640995200000, "1d", "prev")
    assert isinstance(aligned, int)


def test_parse_timeframe_functions():
    """测试时间框架转换函数"""
    # 测试parse_timeframe_to_minutes
    assert parse_timeframe_to_minutes("1h") == 60
    assert parse_timeframe_to_minutes("1d") == 1440

    # 测试parse_timeframe_to_seconds
    assert parse_timeframe_to_seconds("1h") == 3600
    assert parse_timeframe_to_seconds("1d") == 86400

    # 测试parse_timeframe_to_milliseconds
    assert parse_timeframe_to_milliseconds("1h") == 3600000
    assert parse_timeframe_to_milliseconds("1d") == 86400000


def test_format_size():
    """测试文件大小格式化函数"""
    assert format_size(0) == "0 B"
    assert format_size(1023) == "1023 B"
    assert format_size(1024) == "1.0 KB"
    assert format_size(1024 * 1024) == "1.0 MB"
    assert format_size(1024 * 1024 * 1024) == "1.0 GB"


def test_round_timeframe():
    """测试时间框架对齐函数"""
    # 测试ROUND_DOWN方向
    rounded = round_timeframe("1h", 1640995200000 + 300000)  # 1h时间框架，添加5分钟
    assert rounded == 1640995200000

    # 测试ROUND_UP方向
    rounded = round_timeframe("1h", 1640995200000 + 300000, direction="ROUND_UP")
    assert rounded == 1640998800000  # 下一个小时


def test_timeframe_functions():
    """测试时间框架相关函数"""
    # 测试prev_tf_timestamp和next_tf_timestamp
    now = datetime.now(timezone.utc)
    # now_ts = int(now.timestamp() * 1000)

    prev_ts = prev_tf_timestamp("1h", now)
    next_ts = next_tf_timestamp("1h", now)
    assert isinstance(prev_ts, int)
    assert isinstance(next_ts, int)
    assert prev_ts < next_ts

    # 测试prev_tf_datetime和next_tf_datetime
    prev_dt = prev_tf_datetime("1h", now)
    next_dt = next_tf_datetime("1h", now)
    assert isinstance(prev_dt, datetime)
    assert isinstance(next_dt, datetime)
    assert prev_dt < next_dt


def test_time_slot_methods():
    """测试时间槽方法"""
    time_slot = TimeSlot(start="00:00:00", end="23:59:59")

    # 测试__str__方法
    assert isinstance(str(time_slot), str)


def test_time_slot_manager_methods():
    """测试时间槽管理器方法"""
    manager = TimeSlotManager()
    time_slot = TimeSlot(start="00:00:00", end="23:59:59")
    manager.add_slot("test_slot", time_slot)

    # 测试is_in_timeslot方法
    result = manager.is_in_timeslot("test_slot")
    assert isinstance(result, bool)

    # 测试is_at_timeframe_end方法
    result = manager.is_at_timeframe_end("1h", 60)
    assert isinstance(result, bool)

    # 测试is_at_timeframe_start方法
    result = manager.is_at_timeframe_start("1h", 60)
    assert isinstance(result, bool)
