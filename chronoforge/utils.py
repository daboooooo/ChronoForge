import re
import logging
import time
import functools
import asyncio
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Optional, Union, Any

logger = logging.getLogger(__name__)

# 时间单位映射
TIME_UNITS = {
    's': 'seconds',
    'm': 'minutes',
    'h': 'hours',
    'd': 'days',
    'w': 'weeks',
    'M': 'months',  # 30天近似
    'y': 'years'    # 365天近似
}


def with_retry(func):
    """重试装饰器，用于网络请求失败时自动重试，支持异步函数"""
    @functools.wraps(func)
    async def async_wrapper(*args, **kwargs):
        retry_delay = 5
        for attempt in range(3):
            try:
                return await func(*args, **kwargs)
            except IOError as e:
                logger.warning("尝试 %d/3 失败: %s", attempt + 1, str(e))
                if attempt < 2:
                    logger.info(f"{retry_delay} 秒后重试...")
                    await asyncio.sleep(retry_delay)  # 使用异步sleep
                    retry_delay *= 2  # 指数退避
                else:
                    logger.error(f"达到最大重试次数: {str(e)}")
                    raise
        return None  # 这行理论上不会执行到

    @functools.wraps(func)
    def sync_wrapper(*args, **kwargs):
        retry_delay = 5
        for attempt in range(3):
            try:
                return func(*args, **kwargs)
            except IOError as e:
                logger.warning("尝试 %d/3 失败: %s", attempt + 1, str(e))
                if attempt < 2:
                    logger.info(f"{retry_delay} 秒后重试...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # 指数退避
                else:
                    logger.error(f"达到最大重试次数: {str(e)}")
                    raise
        return None  # 这行理论上不会执行到

    # 根据原始函数类型返回相应的wrapper
    if asyncio.iscoroutinefunction(func):
        # 设置属性，供测试使用
        async_wrapper._with_retry_decorated = True
        return async_wrapper
    else:
        # 设置属性，供测试使用
        sync_wrapper._with_retry_decorated = True
        return sync_wrapper


class TimeRange:
    """
    时间范围类，用于处理时间范围的解析和转换
    """

    def __init__(self,
                 start_ts_ms: Optional[int] = None,
                 end_ts_ms: Optional[int] = None):
        """
        初始化时间范围

        Args:
            start_ts_ms: 开始时间戳ms
            end_ts_ms: 结束时间戳ms
        """
        self.start_ts_ms = start_ts_ms
        self.end_ts_ms = end_ts_ms
        self._validate_range()

    def _validate_range(self):
        """
        验证时间范围的有效性并设置默认值
        """
        # 判断start_ts_ms的范围是否在2000年1月1日之后并且在2050年1月1日之前
        if (self.start_ts_ms is not None and
                (self.start_ts_ms < 1e12 or self.start_ts_ms > 3e12)):
            logger.warning(f"开始时间 {self.start_ts_ms} 不在有效时间范围内，自动设置成2022年1月1日")
            self.start_ts_ms = 1640995200000  # 2022年1月1日 00:00:00

        # 判断end_ts_ms的范围是否在2000年1月1日之后并且在2050年1月1日之前
        if (self.end_ts_ms is not None and
                (self.end_ts_ms < 1e12 or self.end_ts_ms > 3e12)):
            logger.warning(f"结束时间 {self.end_ts_ms} 不在有效时间范围内，自动设置成当前时间")
            self.end_ts_ms = int(time.time() * 1000)  # 当前时间戳ms

        # 确保start_ts_ms不晚于end_ts_ms
        if (self.start_ts_ms is not None and self.end_ts_ms is not None and
                self.start_ts_ms > self.end_ts_ms):
            logger.warning(f"开始时间 {self.start_ts_ms} 晚于结束时间 {self.end_ts_ms}，交换两者")
            self.start_ts_ms, self.end_ts_ms = self.end_ts_ms, self.start_ts_ms

    def align_to_timeframe(self, timestamp_ms: int, timeframe: str, flag: str) -> int:
        """
        将时间戳对齐到前一个时间框架或下一个时间框架

        Args:
            timestamp_ms: 要对齐的时间戳ms
            timeframe: 时间框架，如'1h', '4h', '1d'等
            flag: 'prev' 或 'next'，指定对齐到前一个时间框架还是下一个时间框架

        Returns:
            int: 对齐后的时间戳ms
        """
        # 解析时间框架
        try:
            tf_ms = parse_timeframe_to_milliseconds(timeframe)
        except (ValueError, NameError):
            # 如果parse_timeframe_to_minutes不可用或解析失败，返回原始时间戳
            logger.warning(f"无法解析时间框架: {timeframe}，返回原始时间戳")
            return timestamp_ms

        # 计算有多少个tf_ms, 并取整
        count = timestamp_ms // tf_ms
        remain = timestamp_ms % tf_ms
        # 看有多少个tf_ms
        if flag == 'prev':
            # 如果remain为0, 则对齐到前一个时间框架的开始
            if remain == 0 and count > 0:
                count -= 1
        elif flag == 'next':
            # 如果remain不为0, 则对齐到下一个时间框架的开始
            if remain != 0:
                count += 1
        else:
            raise ValueError("flag必须是'prev'或'next'")

        return int(count * tf_ms)

    @property
    def start_dt(self) -> datetime:
        """开始日期时间对象"""
        if self.start_ts_ms:
            return datetime.fromtimestamp(self.start_ts_ms / 1000, tz=timezone.utc)
        return None

    @property
    def end_dt(self) -> datetime:
        """结束日期时间对象"""
        if self.end_ts_ms:
            return datetime.fromtimestamp(self.end_ts_ms / 1000, tz=timezone.utc)
        return None

    def parse_time(self, time_input: Optional[Union[str, float, datetime]]) -> Optional[int]:
        """
        解析时间输入，支持多种格式

        Args:
            time_input: 时间输入，可以是字符串、浮点数或datetime对象

        Returns:
            int: 时间戳ms，解析失败返回None
        """
        if time_input is None:
            return None

        # 直接处理datetime对象
        if isinstance(time_input, datetime):
            return int(time_input.timestamp() * 1000)  # 转换为毫秒

        # 直接处理浮点数（时间戳）
        if isinstance(time_input, (int, float)):
            return int(time_input * 1000)  # 转换为毫秒

        # 尝试解析ISO格式
        try:
            return int(datetime.fromisoformat(str(time_input)).timestamp() * 1000)
        except (ValueError, TypeError):
            pass

        # 尝试解析相对时间描述
        # 支持相对时间如 "1d" (1天前), "+2h" (2小时后)
        match = re.match(r'^([+-])?(\d+)([smhdwMy])$', str(time_input).lower())
        if match:
            sign, amount, unit = match.groups()
            amount = int(amount)
            now = datetime.now(tz=timezone.utc)

            # 确定时间单位
            if unit in TIME_UNITS:
                kwargs = {}
                if unit == 'M':  # 月（近似30天）
                    kwargs['days'] = amount * 30
                elif unit == 'y':  # 年（近似365天）
                    kwargs['days'] = amount * 365
                else:
                    kwargs[TIME_UNITS[unit]] = amount

                # 计算时间
                if sign == '-':
                    return int((now - timedelta(**kwargs)).timestamp() * 1000)
                else:
                    return int((now + timedelta(**kwargs)).timestamp() * 1000)
        return None

    @classmethod
    def parse_timerange(cls, timerange_str: str) -> 'TimeRange':
        """
        解析时间范围字符串

        Args:
            timerange_str: UTC时间范围字符串，支持以下几种格式:

                '20230101-20230630'

                '20230101-'

                '-20230630'

                '20230101_000000-20230630_235959'

                '20230101_000000-'

                '-20230630_235959'

        Returns:
            TimeRange: 解析后的时间范围对象
        """
        if not timerange_str or '-' not in timerange_str:
            raise ValueError(
                f"无效的时间范围格式: {timerange_str}。正确格式: "
                "'YYYYMMDD-YYYYMMDD'，'YYYYMMDD_000000-YYYYMMDD_235959'")

        start_str, end_str = timerange_str.split('-', 1)

        # 处理开始时间
        if not start_str:
            start_str = '20240101_000000'
        else:
            if '_' not in start_str:
                start_str += '_000000'
        start_ts_ms = int(datetime.strptime(
            start_str, '%Y%m%d_%H%M%S').replace(tzinfo=timezone.utc).timestamp() * 1000)

        # 处理结束时间
        if end_str:
            if '_' not in end_str:
                end_str += '_235959'
            end_ts_ms = int(datetime.strptime(
                end_str, '%Y%m%d_%H%M%S').replace(tzinfo=timezone.utc).timestamp() * 1000)
        else:
            # 当不设置结束时间时，应视为结束时间是不断更新的当前时间，不能设置为固定时间
            end_ts_ms = None

        return cls(start_ts_ms, end_ts_ms)

    def __str__(self) -> str:
        """返回时间范围的字符串表示（秒粒度）"""
        if self.start_ts_ms and self.end_ts_ms:
            return f"{self.start_ts_ms} ({self.start_dt}) - {self.end_ts_ms} ({self.end_dt})"
        elif self.start_ts_ms:
            return f"{self.start_ts_ms} ({self.start_dt}) -"
        elif self.end_ts_ms:
            return f"- {self.end_ts_ms} ({self.end_dt})"
        else:
            raise ValueError("时间范围必须包含开始时间或结束时间")

    def contains(self, date: datetime) -> bool:
        """
        检查日期是否在时间范围内

        Args:
            date: 要检查的日期时间对象

        Returns:
            bool: 日期是否在时间范围内
        """
        timestamp = int(date.timestamp() * 1000)
        if self.start_ts_ms and timestamp < self.start_ts_ms:
            return False
        if self.end_ts_ms and timestamp > self.end_ts_ms:
            return False
        return True

    def to_pandas_datetime(self) -> tuple:
        """
        将时间范围转换为pandas datetime对象

        Returns:
            tuple: (start_dt, end_dt) - pandas datetime对象的元组
        """
        start_dt = pd.to_datetime(self.start_ts_ms, unit='ms', utc=True) if self.start_ts_ms \
            else None
        end_dt = pd.to_datetime(self.end_ts_ms, unit='ms', utc=True) if self.end_ts_ms else None
        return start_dt, end_dt


def parse_timeframe_to_minutes(timeframe: str) -> int:
    """
    将时间框架字符串转换为分钟数，支持多种格式

    Args:
        timeframe: 时间框架字符串，如 '1m', '1h', '1d', '1w', '1M', '1y'
                  也支持数字+单位格式，如 '1min', '1hour', '1day'

    Returns:
        int: 分钟数

    Raises:
        ValueError: 当时间框架无法解析时
    """
    # 检查输入
    if not timeframe:
        raise ValueError("时间框架不能为空")

    # 标准时间框架映射
    standard_timeframes = {
        '1m': 1,
        '3m': 3,
        '5m': 5,
        '15m': 15,
        '30m': 30,
        '1h': 60,
        '2h': 120,
        '4h': 240,
        '6h': 360,
        '8h': 480,
        '12h': 720,
        '1d': 1440,
        '3d': 4320,
        '1w': 10080,
        '1M': 43200,  # 30天近似
        '1y': 525600  # 365天近似
    }

    # 检查标准时间框架
    if timeframe in standard_timeframes:
        return standard_timeframes[timeframe]

    # 尝试解析数字+单位格式
    import re
    match = re.match(r'^(\d+)([mhdwMy]|min|minute|hour|day|week|month|year)s?$', timeframe.lower())

    if match:
        amount, unit = match.groups()
        amount = int(amount)

        # 单位映射到分钟
        unit_map = {
            'm': 1,
            'min': 1,
            'minute': 1,
            'h': 60,
            'hour': 60,
            'd': 1440,
            'day': 1440,
            'w': 10080,
            'week': 10080,
            'M': 43200,
            'month': 43200,
            'y': 525600,
            'year': 525600
        }

        if unit in unit_map:
            return amount * unit_map[unit]

    # 尝试解析纯数字（假设单位是分钟）
    try:
        return int(timeframe)
    except ValueError:
        pass

    # 如果所有尝试都失败，抛出ValueError
    error_string = f"时间框架解析错误: {timeframe}"
    raise ValueError(error_string)


def parse_timeframe_to_seconds(timeframe: str) -> int:
    """
    将时间框架字符串转换为秒数

    Args:
        timeframe: 时间框架字符串，如 '1m', '1h', '1d', '1w', '1M', '1y'

    Returns:
        int: 秒数
    """
    return parse_timeframe_to_minutes(timeframe) * 60


def parse_timeframe_to_milliseconds(timeframe: str) -> int:
    """
    将时间框架字符串转换为毫秒数

    Args:
        timeframe: 时间框架字符串，如 '1m', '1h', '1d', '1w', '1M', '1y'

    Returns:
        int: 毫秒数
    """
    return parse_timeframe_to_minutes(timeframe) * 60 * 1000


def format_size(size_bytes: int) -> str:
    """
    格式化字节大小为可读字符串

    Args:
        size_bytes: 字节数

    Returns:
        str: 格式化后的大小字符串
    """
    if not size_bytes or size_bytes <= 0:
        return "0 B"

    # 确保size_bytes是整数
    size_bytes = int(size_bytes)

    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"


def round_timeframe(timeframe, timestamp_ms, direction='ROUND_DOWN'):
    ms = parse_timeframe_to_milliseconds(timeframe)
    # Get offset based on timeframe in milliseconds
    offset = timestamp_ms % ms
    return timestamp_ms - offset + (ms if direction == 'ROUND_UP' else 0)


def prev_tf_timestamp(timeframe: str,
                      date: Optional[datetime] = None,
                      ms: bool = False) -> int:
    if not date:
        date = datetime.now(timezone.utc)
    if timeframe != '1w':
        timestamp = round_timeframe(timeframe, date.timestamp() * 1000,
                                    'ROUND_DOWN') // 1000
    else:
        # Special case for weekly candles
        # Get the day of the week (0 is Monday, 6 is Sunday)
        weekday = date.weekday()
        # Calculate the date of Monday of the current week
        previous_monday = date - timedelta(days=weekday)
        # Set time to 00:00:00
        date = datetime(previous_monday.year, previous_monday.month,
                        previous_monday.day, 0, 0, 0, tzinfo=timezone.utc)
        timestamp = date.timestamp()
    if ms:
        timestamp *= 1000
    return int(timestamp)


def next_tf_timestamp(timeframe: str,
                      date: Optional[datetime] = None,
                      ms: bool = False) -> int:
    if not date:
        date = datetime.now(timezone.utc)
    if timeframe != '1w':
        next_timestamp = round_timeframe(
            timeframe, date.timestamp() * 1000, 'ROUND_UP') // 1000
    else:
        # Special case for weekly candles
        next_monday = date + timedelta(days=7 - date.weekday())
        next_timestamp = datetime(
            next_monday.year, next_monday.month, next_monday.day,
            tzinfo=timezone.utc).timestamp()
        if next_timestamp < date.timestamp():
            raise ValueError("Found BUG!")
    if ms:
        next_timestamp *= 1000
    return int(next_timestamp)


def prev_tf_datetime(timeframe: str,
                     date: Optional[datetime] = None) -> datetime:
    """
    Use Timeframe and determine the candle start date for this date.
    Does not round when given a candle start date.
    :param timeframe: timeframe in string format (e.g. "5m")
    :param date: date to use. Defaults to now(utc)
    :returns: date of previous candle (with utc timezone)
    """
    new_timestamp = prev_tf_timestamp(timeframe, date)
    return datetime.fromtimestamp(new_timestamp, tz=timezone.utc)


def next_tf_datetime(timeframe: str,
                     date: Optional[datetime] = None) -> datetime:
    """
    Use Timeframe and determine next candle.
    :param timeframe: timeframe in string format (e.g. "5m")
    :param date: date to use. Defaults to now(utc)
    :returns: date of next candle (with utc timezone)
    """
    new_timestamp = next_tf_timestamp(timeframe, date)
    return datetime.fromtimestamp(new_timestamp, tz=timezone.utc)


class TimeSlot:
    """
    Represent a time period, e.g. "11:00" to "11:15" or "00:08:00" to "00:18:59".

    if start and end are in HH:MM:SS format, then it is a daily period.
    if start and end are in MM:SS format, then it is an hourly period.
    """
    def __init__(self, start: str, end: str) -> None:
        """
        Args:
            start: start time in string format (e.g. "11:00" or "00:08:00")
            end: end time in string format (e.g. "11:59" or "00:18:59")
        """
        self.start = start
        self.end = end
        if len(start) == 8 and len(end) == 8:
            self.type = 'daily'

        elif len(start) == 5 and len(end) == 5:
            self.type = 'hourly'
        else:
            raise ValueError("time range format error: invalid time format")

        try:
            if self.type == 'daily':
                self.start_dt = datetime.strptime(self.start, '%H:%M:%S')
                self.end_dt = datetime.strptime(self.end, '%H:%M:%S')
            elif self.type == 'hourly':
                self.start_dt = datetime.strptime(self.start, '%M:%S')
                self.end_dt = datetime.strptime(self.end, '%M:%S')
        except ValueError:
            raise ValueError("timeslot format error: each timeslot must be a list of 2 strings "
                             "in HH:MM:SS or MM:SS format")

    def __str__(self) -> str:
        return f"{self.start} to {self.end}"


class TimeSlotManager:
    """Support timeframe slots and random slots
        - timeframe slot
        - slot format:
        {
            "slot_name 1": {
                "type": "hourly",
                "timeslots": [
                    ["11:00", "11:59"],
                    ...
                    ["59:00", "59:59"]
                ]
            },
            "slot_name 2": {
                "type": "daily",
                "timeslots": [
                    ["00:08:00", "00:18:59"],
                    ...
                    ["11:22:00", "11:33:59"]
                ]
            },
            ...
        }
    """
    def __init__(self, timeslots: Optional[dict[str, TimeSlot]] = None) -> None:
        self.timeslots: dict[str, TimeSlot] = {}
        self.last_slot: dict[str, Any] = {}
        if timeslots is not None:
            self.timeslots = timeslots
            for name in self.timeslots:
                self.add_slot(name, self.timeslots[name])

    def add_slot(self, name: str, timeslot: TimeSlot, inplace: bool = False) -> None:
        if not inplace and name in self.timeslots:
            raise ValueError(f"slot name `{name}` already exists. Set `inplace=True` to overwrite.")
        if not isinstance(timeslot, TimeSlot):
            raise ValueError("`timeslot` must be a TimeSlot object.")
        # add slot
        self.timeslots[name] = timeslot

    def delete_slot(self, name: str) -> None:
        if name in self.timeslots:
            del self.timeslots[name]

    @staticmethod
    def _is_in_timeslot(timeslot: TimeSlot) -> bool:
        """is in timeslot
        Args:
            timeslot (TimeSlot): timeslot object

        Returns:
            bool: true means current time is in timeslot
        """
        _now = datetime.now()
        if timeslot.type == 'daily':
            _start_time = datetime.strptime(
                str(_now.date()) + timeslot.start, '%Y-%m-%d%H:%M:%S')
            _end_time = datetime.strptime(
                str(_now.date()) + timeslot.end, '%Y-%m-%d%H:%M:%S')
        else:
            _start_time = datetime.strptime(
                str(_now.date()) +
                str(_now.hour) + ':' + timeslot.start, '%Y-%m-%d%H:%M:%S')
            _end_time = datetime.strptime(
                str(_now.date()) +
                str(_now.hour) + ':' + timeslot.end, '%Y-%m-%d%H:%M:%S')
        if _start_time < _now < _end_time:
            return True
        return False

    def is_in_timeslot(self, name: str, once: bool = False) -> bool:
        """Check if current time is in timeslot

        Args:
            name (str): timeslot name
            once (bool, optional): if True, each timeslot is checked only once. Defaults to False.

        Returns:
            bool: true means current time is in timeslot
        """
        if name not in self.timeslots:
            return False
        timeslot = self.timeslots[name]
        result = False
        if self._is_in_timeslot(timeslot):
            result = True
        if once:
            # only the first bingo return slot index
            last = self.last_slot.get(name, False)
            self.last_slot[name] = result
            if not result or result == last:
                return False
        return result

    def is_at_timeframe_end(self, timeframe: str, run_interval: int, once: bool = False) -> bool:
        """Check if current time is at timeframe end

        `current time + run_interval > next_tf_datetime` means current time is at timeframe end

        Args:
            timeframe (str): timeframe in string format (e.g. "5m")
            run_interval (int): run interval seconds
            once (bool, optional): only the first bingo return True. Defaults to False.

        Returns:
            bool: true means current time is at timeframe end
        """
        _now = datetime.now(timezone.utc)
        _next_loop_start_time = _now + timedelta(seconds=run_interval)
        _next_candle_time = next_tf_datetime(timeframe, _now)
        result = False
        if _next_loop_start_time >= _next_candle_time:
            result = True
        if once:
            # only the first bingo return True
            last = self.last_slot.get(f"{timeframe}-end", False)
            self.last_slot[f"{timeframe}-end"] = result
            if not result or result == last:
                return False
        return result

    def is_at_timeframe_start(self, timeframe: str, run_interval: int, once: bool = False) -> bool:
        """Check if current time is at timeframe start

        `current time - run_interval < prev_tf_datetime` means current time is at timeframe start

        Args:
            timeframe (str): timeframe in string format (e.g. "5m")
            run_interval (int): run interval seconds
            once (bool, optional): only the first bingo return True. Defaults to False.

        Returns:
            bool: true means current time is at timeframe start
        """
        _now = datetime.now(timezone.utc)
        _prev_loop_end_time = _now - timedelta(seconds=run_interval)
        _prev_candle_time = prev_tf_datetime(timeframe, _now)
        result = False
        if _prev_loop_end_time <= _prev_candle_time:
            result = True
        if once:
            # only the first bingo return True
            last = self.last_slot.get(f"{timeframe}-start", False)
            self.last_slot[f"{timeframe}-start"] = result
            if not result or result == last:
                return False
        return result


class ParsedCCXTSymbol:
    """
    https://github.com/ccxt/ccxt/wiki/Manual#contract-naming-conventions

      base asset or currency
      ↓
      ↓  quote asset or currency
      ↓  ↓
      ↓  ↓    settlement asset or currency [[[Perpetual Swap, Futures, Options]]]
      ↓  ↓    ↓
      ↓  ↓    ↓       identifier (settlement date) [[[Futures, Options]]]
      ↓  ↓    ↓       ↓
      ↓  ↓    ↓       ↓   strike price [[[Options]]]
      ↓  ↓    ↓       ↓   ↓
      ↓  ↓    ↓       ↓   ↓   type, put (P) or call (C) [[[Options]]]
      ↓  ↓    ↓       ↓   ↓   ↓
    BTC/USDT:BTC-211225-60000-P

    BTC/USDT put option contract strike price 60000 USDT settled in BTC (inverse) on 2021-12-25
    """
    original: str  # original symbol
    unified: str  # unified symbol without suffix
    base: str  # base asset or currency
    quote: str  # quote asset or currency
    settlement: str  # settlement asset or currency [[[Perpetual Swap, Futures, Options]]]
    identifier: str  # settlement date [[[Futures, Options]]]
    strike: str  # strike price [[[Options]]]
    type_: str  # type, put (P) or call (C) [[[Options]]]

    def __init__(self, symbol: str) -> None:
        if '/' not in symbol:
            raise ValueError(f"Invalid symbol: {symbol}")
        self.original = symbol
        if ':' not in symbol:
            # spot market
            self.unified = symbol
            suffix = ''
        else:
            items = symbol.split(':')
            self.unified, suffix = items + [''] * (2 - len(items))
        self.base, self.quote = self.unified.split('/')
        if suffix:
            items = suffix.split('-')
            self.settlement, self.identifier, self.strike, self.type_ = \
                items + [''] * (4 - len(items))
        else:
            self.settlement, self.identifier, self.strike, self.type_ = \
                ['', '', '', '']
