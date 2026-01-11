import pytest
from datetime import datetime, timezone
import pandas as pd
import time
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
    TimeRange,
    with_retry,
    ParsedCCXTSymbol
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
    assert timerange.end_ts_ms is None

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

    # 测试None情况下的start_dt和end_dt
    timerange_none = TimeRange(start_ts_ms=None, end_ts_ms=None)
    assert timerange_none.start_dt is None
    assert timerange_none.end_dt is None

    # 测试parse_time方法
    assert timerange.parse_time("2024-01-01") is not None
    assert timerange.parse_time(1704067200) is not None  # 秒级时间戳
    assert timerange.parse_time(datetime(2024, 1, 1)) is not None

    # 测试parse_time的相对时间处理
    assert timerange.parse_time("1d") is not None  # 1天前
    assert timerange.parse_time("+2h") is not None  # 2小时后
    assert timerange.parse_time("-30m") is not None  # 30分钟前

    # 测试parse_time的无效输入
    assert timerange.parse_time("invalid-time") is None

    # 测试contains方法
    test_date = datetime(2022, 1, 15, tzinfo=timezone.utc)
    assert timerange.contains(test_date) is True
    assert timerange.contains(datetime(2021, 1, 1, tzinfo=timezone.utc)) is False

    # 测试to_pandas_datetime方法
    start_pd, end_pd = timerange.to_pandas_datetime()
    assert isinstance(start_pd, pd.Timestamp)
    assert isinstance(end_pd, pd.Timestamp)

    # 测试__str__方法
    assert isinstance(str(timerange), str)

    # 测试align_to_timeframe方法的prev标志
    aligned_prev = timerange.align_to_timeframe(1640995200000 + 300000, "1d", "prev")
    assert isinstance(aligned_prev, int)

    # 测试align_to_timeframe方法的next标志
    aligned_next = timerange.align_to_timeframe(1640995200000 + 300000, "1d", "next")
    assert isinstance(aligned_next, int)
    assert aligned_next > aligned_prev

    # 测试align_to_timeframe方法的无效标志
    with pytest.raises(ValueError):
        timerange.align_to_timeframe(1640995200000, "1d", "invalid_flag")

    # 测试align_to_timeframe方法的无效时间框架
    aligned_invalid = timerange.align_to_timeframe(1640995200000, "invalid_tf", "prev")
    assert isinstance(aligned_invalid, int)


def test_time_range_validation():
    """测试时间范围的验证逻辑"""
    # 测试开始时间不在有效范围内
    timerange = TimeRange(start_ts_ms=900000000000, end_ts_ms=1643673600000)  # 1998年，小于1e12
    assert timerange.start_ts_ms == 1640995200000  # 应该被自动设置为2022年1月1日

    # 测试结束时间不在有效范围内
    timerange = TimeRange(start_ts_ms=1640995200000, end_ts_ms=3100000000000)  # 2068年，大于3e12
    assert timerange.end_ts_ms <= int(time.time() * 1000)  # 应该被自动设置为当前时间

    # 测试开始时间晚于结束时间
    timerange = TimeRange(start_ts_ms=1643673600000, end_ts_ms=1640995200000)  # 开始时间晚于结束时间
    assert timerange.start_ts_ms < timerange.end_ts_ms  # 应该被交换


def test_time_range_parse_time():
    """测试时间范围的parse_time方法"""
    timerange = TimeRange(start_ts_ms=1640995200000, end_ts_ms=1643673600000)

    # 测试ISO格式解析
    assert timerange.parse_time("2024-01-01T12:00:00") is not None

    # 测试不同时间单位的相对时间
    assert timerange.parse_time("5m") is not None  # 5分钟前
    assert timerange.parse_time("1h") is not None  # 1小时前
    assert timerange.parse_time("3d") is not None  # 3天前
    assert timerange.parse_time("1w") is not None  # 1周前
    assert timerange.parse_time("1M") is not None  # 1个月前
    assert timerange.parse_time("1y") is not None  # 1年前

    # 测试带正负号的相对时间
    assert timerange.parse_time("+1d") is not None  # 1天后
    assert timerange.parse_time("-2h") is not None  # 2小时前

    # 测试None输入
    assert timerange.parse_time(None) is None


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


def test_parse_timeframe_to_minutes_comprehensive():
    """全面测试parse_timeframe_to_minutes函数，覆盖各种格式"""
    # 测试标准时间框架格式
    assert parse_timeframe_to_minutes("1m") == 1
    assert parse_timeframe_to_minutes("5m") == 5
    assert parse_timeframe_to_minutes("15m") == 15
    assert parse_timeframe_to_minutes("30m") == 30
    assert parse_timeframe_to_minutes("1h") == 60
    assert parse_timeframe_to_minutes("2h") == 120
    assert parse_timeframe_to_minutes("4h") == 240
    assert parse_timeframe_to_minutes("6h") == 360
    assert parse_timeframe_to_minutes("8h") == 480
    assert parse_timeframe_to_minutes("12h") == 720
    assert parse_timeframe_to_minutes("1d") == 1440
    assert parse_timeframe_to_minutes("3d") == 4320
    assert parse_timeframe_to_minutes("1w") == 10080
    assert parse_timeframe_to_minutes("1M") == 43200
    assert parse_timeframe_to_minutes("1y") == 525600

    # 测试数字+单位格式（单数）
    assert parse_timeframe_to_minutes("1min") == 1
    assert parse_timeframe_to_minutes("1hour") == 60
    assert parse_timeframe_to_minutes("1day") == 1440
    assert parse_timeframe_to_minutes("1week") == 10080
    assert parse_timeframe_to_minutes("1month") == 43200
    assert parse_timeframe_to_minutes("1year") == 525600

    # 测试数字+单位格式（复数）
    assert parse_timeframe_to_minutes("5mins") == 5
    assert parse_timeframe_to_minutes("2hours") == 120
    assert parse_timeframe_to_minutes("3days") == 4320
    assert parse_timeframe_to_minutes("2weeks") == 20160
    assert parse_timeframe_to_minutes("6months") == 259200
    assert parse_timeframe_to_minutes("3years") == 1576800

    # 测试纯数字格式
    assert parse_timeframe_to_minutes("10") == 10
    assert parse_timeframe_to_minutes("0") == 0
    assert parse_timeframe_to_minutes("1440") == 1440

    # 测试小写和大写
    assert parse_timeframe_to_minutes("1H") == 60
    assert parse_timeframe_to_minutes("1D") == 1440
    assert parse_timeframe_to_minutes("1W") == 10080
    assert parse_timeframe_to_minutes("1M") == 43200
    assert parse_timeframe_to_minutes("1Y") == 525600

    # 测试错误情况
    with pytest.raises(ValueError):
        parse_timeframe_to_minutes("")
    with pytest.raises(ValueError):
        parse_timeframe_to_minutes("invalid")
    with pytest.raises(ValueError):
        parse_timeframe_to_minutes("1x")
    with pytest.raises(ValueError):
        parse_timeframe_to_minutes("abc")


def test_parse_timeframe_to_seconds_comprehensive():
    """全面测试parse_timeframe_to_seconds函数"""
    # 测试各种时间框架
    assert parse_timeframe_to_seconds("1m") == 60
    assert parse_timeframe_to_seconds("5m") == 300
    assert parse_timeframe_to_seconds("1h") == 3600
    assert parse_timeframe_to_seconds("1d") == 86400
    assert parse_timeframe_to_seconds("1w") == 604800
    assert parse_timeframe_to_seconds("1M") == 2592000
    assert parse_timeframe_to_seconds("1y") == 31536000

    # 测试数字+单位格式
    assert parse_timeframe_to_seconds("5min") == 300
    assert parse_timeframe_to_seconds("2hours") == 7200

    # 测试纯数字格式
    assert parse_timeframe_to_seconds("10") == 600


def test_parse_timeframe_to_milliseconds_comprehensive():
    """全面测试parse_timeframe_to_milliseconds函数"""
    # 测试各种时间框架
    assert parse_timeframe_to_milliseconds("1m") == 60000
    assert parse_timeframe_to_milliseconds("5m") == 300000
    assert parse_timeframe_to_milliseconds("1h") == 3600000
    assert parse_timeframe_to_milliseconds("1d") == 86400000
    assert parse_timeframe_to_milliseconds("1w") == 604800000
    assert parse_timeframe_to_milliseconds("1M") == 2592000000
    assert parse_timeframe_to_milliseconds("1y") == 31536000000

    # 测试数字+单位格式
    assert parse_timeframe_to_milliseconds("5min") == 300000
    assert parse_timeframe_to_milliseconds("2hours") == 7200000

    # 测试纯数字格式
    assert parse_timeframe_to_milliseconds("10") == 600000


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


def test_prev_tf_timestamp():
    """测试prev_tf_timestamp函数"""
    # 测试1小时时间框架
    test_date = datetime(2024, 1, 1, 12, 30, 0, tzinfo=timezone.utc)
    prev_hour = prev_tf_timestamp("1h", test_date)
    assert prev_hour == datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp()

    # 测试1天时间框架
    prev_day = prev_tf_timestamp("1d", test_date)
    assert prev_day == datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp()

    # 测试1周时间框架
    # 2024-01-01是星期一
    prev_week = prev_tf_timestamp("1w", test_date)
    assert prev_week == datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp()

    # 测试ms参数
    prev_hour_ms = prev_tf_timestamp("1h", test_date, ms=True)
    assert prev_hour_ms == int(datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)

    # 测试默认日期
    prev_default = prev_tf_timestamp("1h")
    assert isinstance(prev_default, int)


def test_next_tf_timestamp():
    """测试next_tf_timestamp函数"""
    # 测试1小时时间框架
    test_date = datetime(2024, 1, 1, 12, 30, 0, tzinfo=timezone.utc)
    next_hour = next_tf_timestamp("1h", test_date)
    assert next_hour == datetime(2024, 1, 1, 13, 0, 0, tzinfo=timezone.utc).timestamp()

    # 测试1天时间框架
    next_day = next_tf_timestamp("1d", test_date)
    assert next_day == datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc).timestamp()

    # 测试1周时间框架
    # 2024-01-01是星期一，下周一应该是2024-01-08
    next_week = next_tf_timestamp("1w", test_date)
    assert next_week == datetime(2024, 1, 8, 0, 0, 0, tzinfo=timezone.utc).timestamp()

    # 测试ms参数
    next_hour_ms = next_tf_timestamp("1h", test_date, ms=True)
    assert next_hour_ms == int(datetime(2024, 1, 1, 13, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)

    # 测试默认日期
    next_default = next_tf_timestamp("1h")
    assert isinstance(next_default, int)


def test_prev_tf_datetime():
    """测试prev_tf_datetime函数"""
    # 测试1小时时间框架
    test_date = datetime(2024, 1, 1, 12, 30, 0, tzinfo=timezone.utc)
    prev_hour = prev_tf_datetime("1h", test_date)
    assert prev_hour == datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    # 测试1天时间框架
    prev_day = prev_tf_datetime("1d", test_date)
    assert prev_day == datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    # 测试默认日期
    prev_default = prev_tf_datetime("1h")
    assert isinstance(prev_default, datetime)


def test_next_tf_datetime():
    """测试next_tf_datetime函数"""
    # 测试1小时时间框架
    test_date = datetime(2024, 1, 1, 12, 30, 0, tzinfo=timezone.utc)
    next_hour = next_tf_datetime("1h", test_date)
    assert next_hour == datetime(2024, 1, 1, 13, 0, 0, tzinfo=timezone.utc)

    # 测试1天时间框架
    next_day = next_tf_datetime("1d", test_date)
    assert next_day == datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc)

    # 测试默认日期
    next_default = next_tf_datetime("1h")
    assert isinstance(next_default, datetime)


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


# 测试with_retry装饰器
def test_with_retry_sync_success():
    """测试同步函数成功执行时不重试"""
    call_count = 0

    @with_retry
    def success_func():
        nonlocal call_count
        call_count += 1
        return "success"

    result = success_func()
    assert result == "success"
    assert call_count == 1


def test_with_retry_sync_failure():
    """测试同步函数失败时重试"""
    call_count = 0

    @with_retry
    def fail_func():
        nonlocal call_count
        call_count += 1
        raise IOError("Test IO error")

    with pytest.raises(IOError):
        fail_func()

    assert call_count == 3  # 1次原始调用 + 2次重试


@pytest.mark.asyncio
async def test_with_retry_async_success():
    """测试异步函数成功执行时不重试"""
    call_count = 0

    @with_retry
    async def async_success_func():
        nonlocal call_count
        call_count += 1
        return "async success"

    result = await async_success_func()
    assert result == "async success"
    assert call_count == 1


@pytest.mark.asyncio
async def test_with_retry_async_failure():
    """测试异步函数失败时重试"""
    call_count = 0

    @with_retry
    async def async_fail_func():
        nonlocal call_count
        call_count += 1
        raise IOError("Async test IO error")

    with pytest.raises(IOError):
        await async_fail_func()

    assert call_count == 3  # 1次原始调用 + 2次重试


# 测试ParsedCCXTSymbol类
def test_parsed_ccxt_symbol_basic():
    """测试基本符号解析"""
    symbol = ParsedCCXTSymbol("BTC/USDT")
    assert symbol.original == "BTC/USDT"
    assert symbol.unified == "BTC/USDT"
    assert symbol.base == "BTC"
    assert symbol.quote == "USDT"
    assert symbol.settlement == ""
    assert symbol.identifier == ""
    assert symbol.strike == ""
    assert symbol.type_ == ""

    symbol2 = ParsedCCXTSymbol("BTC/USDT:BTC")
    assert symbol2.original == "BTC/USDT:BTC"
    assert symbol2.unified == "BTC/USDT"
    assert symbol2.base == "BTC"
    assert symbol2.quote == "USDT"
    assert symbol2.settlement == "BTC"
    assert symbol2.identifier == ""
    assert symbol2.strike == ""
    assert symbol2.type_ == ""


def test_parsed_ccxt_symbol_with_suffix():
    """测试带后缀的符号解析"""
    symbol = ParsedCCXTSymbol("BTC/USDT:BTC-211225-60000-P")
    assert symbol.original == "BTC/USDT:BTC-211225-60000-P"
    assert symbol.unified == "BTC/USDT"
    assert symbol.base == "BTC"
    assert symbol.quote == "USDT"
    assert symbol.settlement == "BTC"
    assert symbol.identifier == "211225"
    assert symbol.strike == "60000"
    assert symbol.type_ == "P"


def test_parsed_ccxt_symbol_lowercase():
    """测试小写符号解析"""
    symbol = ParsedCCXTSymbol("eth/usdt")
    assert symbol.base == "eth"
    assert symbol.quote == "usdt"


def test_parsed_ccxt_symbol_invalid():
    """测试无效符号的处理"""
    # 测试没有'/'的符号
    with pytest.raises(ValueError):
        ParsedCCXTSymbol("BTCUSDT")

    # 测试空符号
    with pytest.raises(ValueError):
        ParsedCCXTSymbol("")

    # 测试只有一个部分的符号
    with pytest.raises(ValueError):
        ParsedCCXTSymbol("BTC")
