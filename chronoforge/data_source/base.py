"""插件系统基类定义"""

import abc
import inspect
from typing import get_type_hints, Optional, Any, Dict
import pandas as pd


class DataSourceBase(abc.ABC):
    """数据源插件基类

    每个数据源插件负责从特定来源获取数据，无需关心底层存储实现。
    """

    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}

    @property
    @abc.abstractmethod
    def name(self):
        """子类必须实现 name 属性"""
        pass

    @property
    def plugin_type(self):
        """返回数据源插件类型"""
        return "datasource"

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
            pandas.DataFrame: 包含时间序列数据的DataFrame，至少包含'time'和'value'列
        """
        pass

    @abc.abstractmethod
    async def close_all_connections(self):
        """关闭所有与数据源的连接"""
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
    if not isinstance(getattr(cls, "name", None), property):
        errors.append("Missing required @property 'name'.")

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

    # ---- 3. 检查 close_all_connections ----
    close_all_connections = getattr(temp_instance, "close_all_connections", None)
    if not callable(close_all_connections):
        errors.append("Missing required method 'close_all_connections'.")
    else:
        sig = inspect.signature(close_all_connections)
        if list(sig.parameters.keys()) != []:
            errors.append("'close_all_connections' must accept no parameters")

        # 检查返回类型注解
        hints = get_type_hints(close_all_connections)
        if hints.get("return") is not None:
            errors.append("'close_all_connections' must have no return annotation")

    # ---- 4. 检查构造函数 config 参数是否存在 ----
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
