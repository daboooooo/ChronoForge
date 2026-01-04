"""装饰器模块"""


def periodic_task(interval: int = 60, symbols: list = None, timeframe: str = None,
                  timerange_str: str = None, storage_name: str = "LocalFileStorage",
                  storage_config: dict = None, params: dict = None, max_retries: int = 3,
                  retry_delay: int = 1, priority: int = 0):
    """装饰器，用于标记需要周期性执行的函数

    Args:
        interval: 执行间隔（秒），默认60秒
        symbols: 交易对列表，可选
        timeframe: 时间框架，可选
        timerange_str: 时间范围字符串，可选
        storage_name: 存储名称，默认LocalFileStorage
        storage_config: 存储配置，可选
        params: 方法调用参数，可选
        max_retries: 最大重试次数，默认3次
        retry_delay: 重试延迟（秒），默认1秒
        priority: 任务优先级，默认0
    """
    def decorator(func):
        func.is_periodic_task = True
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
            'priority': priority
        }
        return func
    return decorator
