"""插件装饰器定义"""
from typing import Callable, Any, List, Dict


def api_callable(func: Callable[..., Any]) -> Callable[..., Any]:
    """标记插件的函数可以被delegate_call调用

    装饰器用于标识插件中哪些函数可以通过delegate_call动态调用。
    只有被此装饰器标记的函数才能被外部通过API调用。

    Args:
        func: 要标记的函数

    Returns:
        Callable[..., Any]: 被标记的函数
    """
    func.is_api_callable = True
    return func


def create_task(interval: int = None, symbols: List[str] = None, timeframe: str = None,
                timerange_str: str = None, storage_name: str = "LocalFileStorage",
                storage_config: Dict = None, params: Dict = None, max_retries: int = 3,
                retry_delay: int = 1, time_slot: Dict = None, enable_storage: bool = False):
    """装饰器，用于标记需要执行的函数，可以是周期性任务或基于time_slot的任务

    Args:
        interval: 执行间隔（秒），默认None表示非周期性任务
        symbols: 交易对列表，可选
        timeframe: 时间框架，可选
        timerange_str: 时间范围字符串，可选
        storage_name: 存储名称，默认LocalFileStorage
        storage_config: 存储配置，可选
        params: 方法调用参数，可选
        max_retries: 最大重试次数，默认3次
        retry_delay: 重试延迟（秒），默认1秒
        time_slot: 时间槽配置，格式为{'start': 'HH:MM:SS', 'end': 'HH:MM:SS'}，可选
        enable_storage: 是否启用任务结果自动存储，默认False
    """
    def decorator(func):
        # 构建任务配置
        func.task_config = {
            'interval': interval,
            'symbols': symbols or [],
            'timeframe': timeframe,
            'timerange_str': timerange_str,
            'storage_name': storage_name,
            'storage_config': storage_config or {},
            'params': params or {},
            'max_retries': max_retries,
            'retry_delay': retry_delay,
            'time_slot': time_slot,
            'enable_storage': enable_storage
        }
        # 添加is_internal_task属性，标记这是一个周期性任务
        # scheduler利用它来判断一个函数是否被装饰器装饰
        func.is_internal_task = True
        return func
    return decorator
