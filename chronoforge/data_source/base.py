"""插件系统基类定义"""

import abc
import inspect
from enum import Enum
from typing import get_type_hints, Optional, Any, Dict, List
from dataclasses import dataclass
import pandas as pd


class DataSourceError(Exception):
    """数据源基础异常类"""

    def __init__(self, message: str, data_source: Optional[str] = None,
                 symbol: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.data_source = data_source
        self.symbol = symbol
        self.details = details or {}

    def __str__(self) -> str:
        parts = [self.message]
        if self.data_source:
            parts.append(f"data_source={self.data_source}")
        if self.symbol:
            parts.append(f"symbol={self.symbol}")
        if self.details:
            parts.append(f"details={self.details}")
        return ", ".join(parts)


class DataSourceConnectionError(DataSourceError):
    """数据源连接异常"""
    pass


class DataSourceRateLimitError(DataSourceError):
    """数据源速率限制异常"""

    def __init__(self, message: str, retry_after: Optional[int] = None, **kwargs):
        super().__init__(message, **kwargs)
        self.retry_after = retry_after


class DataSourceAuthenticationError(DataSourceError):
    """数据源认证异常"""
    pass


class DataSourceNotFoundError(DataSourceError):
    """数据源未找到异常（如交易对不存在）"""
    pass


class DataSourceDataError(DataSourceError):
    """数据源数据异常（如返回数据格式错误）"""
    pass


class DataSourceTimeoutError(DataSourceError):
    """数据源超时异常"""
    pass


class DataSchemaType(Enum):
    """数据源返回数据的Schema类型枚举"""
    OHLCV = "ohlcv"
    TIME_SERIES = "time_series"
    TICKER = "ticker"
    MULTI_FIELD = "multi_field"


@dataclass(frozen=True)
class DataField:
    """数据字段定义"""
    name: str
    dtype: str
    description: str
    nullable: bool = True


@dataclass(frozen=True)
class DataSchema:
    """数据Schema定义，描述DataFrame的列结构和类型"""
    schema_type: DataSchemaType
    fields: List[DataField]
    required_fields: Optional[List[str]] = None


class StandardSchemas:
    """标准数据Schema定义"""

    OHLCV = DataSchema(
        schema_type=DataSchemaType.OHLCV,
        fields=[
            DataField("time", "datetime64[ns, UTC]", "时间戳，UTC时区", False),
            DataField("open", "float64", "开盘价", False),
            DataField("high", "float64", "最高价", False),
            DataField("low", "float64", "最低价", False),
            DataField("close", "float64", "收盘价", False),
            DataField("volume", "float64", "成交量", False),
        ],
        required_fields=["time", "open", "high", "low", "close", "volume"]
    )

    TIME_SERIES = DataSchema(
        schema_type=DataSchemaType.TIME_SERIES,
        fields=[
            DataField("time", "datetime64[ns, UTC]", "时间戳，UTC时区", False),
            DataField("value", "float64", "数值", True),
        ],
        required_fields=["time", "value"]
    )

    TICKER = DataSchema(
        schema_type=DataSchemaType.TICKER,
        fields=[
            DataField("symbol", "object", "交易对/资产符号", False),
            DataField("price", "float64", "当前价格", True),
            DataField("volume", "float64", "24小时成交量", True),
            DataField("time", "datetime64[ns, UTC]", "时间戳，UTC时区", True),
        ],
        required_fields=["symbol"]
    )


class DataSourceBase(abc.ABC):
    """数据源插件基类

    每个数据源插件负责从特定来源获取数据，无需关心底层存储实现。

    返回数据格式规范:
        - OHLCV数据: 必须包含 time, open, high, low, close, volume 列
        - 时间序列数据: 必须包含 time, value 列
        - 时间戳统一使用 UTC 时区，datetime64[ns, UTC] 类型
    """

    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}

    @property
    def name(self):
        """返回数据源名称"""
        return self.__class__.__name__.replace("DataSource", "")

    @property
    def plugin_type(self):
        """返回数据源插件类型"""
        return "datasource"

    @property
    def supported_schema(self) -> DataSchema:
        """返回该数据源支持的数据Schema，子类应重写此方法"""
        return StandardSchemas.OHLCV

    @abc.abstractmethod
    async def fetch(
        self,
        symbol: str,
        timeframe: str,
        start_ts_ms: int,
        end_ts_ms: Optional[int] = None
    ) -> pd.DataFrame:
        """获取指定时间范围内的数据, 不需要考虑分页获取

        Args:
            symbol: 数据源标识符，用于指定要获取的具体数据，如交易对、股票代码等
            timeframe: 时间粒度，如 '1m', '5m', '1h', '1d'
            start_ts_ms: 开始时间戳（Unix时间，毫秒）
            end_ts_ms: 结束时间戳（Unix时间，毫秒），默认为当前时间

        Returns:
            pandas.DataFrame: 包含时间序列数据的DataFrame
                - OHLCV数据: 必须包含 'time', 'open', 'high', 'low', 'close', 'volume' 列
                - 时间序列数据: 必须包含 'time', 'value' 列
                - 时间戳列 'time' 必须是 datetime64[ns, UTC] 类型
        """
        pass


def verify_datasource_instance(obj) -> tuple[bool, str]:
    """严格验证一个类或实例是否符合 DataSourceBase 的要求"""
    errors = []
    # 处理传入类或实例的情况
    if inspect.isclass(obj):
        cls = obj
        # 创建一个临时实例用于检查方法（使用默认配置）
        try:
            temp_instance = obj({})
        except Exception as e:
            return False, f"无法创建{obj.__name__}实例: {str(e)}"
    else:
        cls = obj.__class__
        temp_instance = obj

    # ---- 1. 检查 name 是否为 property ----
    # name属性可能在基类中定义，需要使用hasattr检查
    if not hasattr(temp_instance, 'name'):
        errors.append("Missing required @property 'name'.")
    else:
        # 验证name是property类型
        name_in_dict = cls.__dict__.get('name')
        if name_in_dict is not None and not isinstance(name_in_dict, property):
            errors.append("'name' must be a @property.")

    # ---- 2. 检查 fetch 是否存在且为 async ----
    fetch = getattr(temp_instance, "fetch", None)
    if not fetch:
        errors.append("'fetch' method is missing.")
    else:
        # 检查是否为异步函数 - 更灵活的检测方式，考虑装饰器的影响
        is_async = (inspect.iscoroutinefunction(fetch) or
                    inspect.iscoroutinefunction(getattr(fetch, '__wrapped__', None)))
        if not is_async:
            errors.append("'fetch' must be defined as an async function.")
        else:
            sig = inspect.signature(fetch)
            expected = ["symbol", "timeframe", "start_ts_ms", "end_ts_ms"]
            if list(sig.parameters.keys()) != expected:
                errors.append(f"'fetch' must have parameters {expected}, "
                              f"got {list(sig.parameters.keys())}")

            # 检查返回类型注解
            hints = get_type_hints(fetch)
            if hints.get("return") is not pd.DataFrame:
                errors.append("'fetch' must have return annotation 'pd.DataFrame'")

    # ---- 3. 检查构造函数 config 参数是否存在 ----
    init_sig = inspect.signature(cls.__init__)
    if "config" not in init_sig.parameters:
        errors.append("__init__ must accept 'config' parameter.")

    # ---- 输出结果 ----
    if errors:
        msg = "\n".join(f"- {e}" for e in errors)
        result_msg = (f"{cls.__name__} does not conform to "
                      f"DataSourceBase requirements:\n{msg}")
        return False, result_msg
    else:
        result_msg = (f"{cls.__name__} ✅ passed all "
                      f"DataSourceBase validation checks.")
        return True, result_msg


class ParsedSymbol:
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
