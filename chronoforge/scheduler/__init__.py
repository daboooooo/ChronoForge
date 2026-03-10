"""
集成调度器 - 整合数据更新到ChronoForge
使用统一配置和 TaskScheduler 实现可靠的任务调度
"""
import asyncio
import os
import psutil
import threading
import time
from typing import Any, Dict, Optional

from chronoforge.logging_config import setup_logging, get_logger
from chronoforge.data_source import init_data_source_manager
from chronoforge.storage.manager import storage_manager, init_storage_manager

from chronoforge.task_manager_ultimate import ultimate_task_manager, Task
from .task_scheduler import TaskScheduler, TaskPriority, TaskStatus, TaskResult
from .scheduler_config import (
    SchedulerConfig,
    DataType,
    get_config,
    get_data_config,
    get_enabled_data_types
)
from .data_update import (
    DataUpdateManager,
    init_data_update_manager,
    UpdateStrategy
)

setup_logging()
logger = get_logger(__name__)


class Scheduler:
    """集成调度器 - 整合数据源和存储的数据更新"""

    def __init__(self, config: Optional[SchedulerConfig] = None):
        self.config = config or get_config()
        self._running = False
        self._shutdown = threading.Event()
        self._lock = threading.Lock()
        self._scheduler_task: Optional[asyncio.Task] = None

        self.data_source_manager = init_data_source_manager()
        self.storage_manager = storage_manager
        self.task_scheduler = TaskScheduler(
            max_concurrent_tasks=self.config.max_concurrent_tasks,
            default_timeout=self.config.default_timeout,
            enable_metrics=self.config.enable_metrics
        )
        self.data_update_manager: Optional[DataUpdateManager] = None

        self._executing_tasks: Dict[str, bool] = {}

    async def initialize(self) -> 'Scheduler':
        """初始化调度器"""
        logger.info("Initializing scheduler...")
        self.data_update_manager = await init_data_update_manager(
            data_source_manager=self.data_source_manager,
            storage_manager=self.storage_manager
        )
        self._register_scheduled_tasks()
        logger.info("Scheduler initialized successfully")
        return self

    def _register_scheduled_tasks(self) -> None:
        """注册定时任务"""
        enabled_types = get_enabled_data_types()

        for data_type in enabled_types:
            config = get_data_config(data_type)
            if not config:
                continue

            task_name = f"update_{data_type.value}"
            logger.info(f"Registering task: {task_name}, interval: {config.interval_seconds}s")

            self.task_scheduler.add_interval_task(
                name=task_name,
                func=self._create_update_func(data_type),
                seconds=config.interval_seconds,
                priority=TaskPriority(config.priority),
                metadata={'data_type': data_type.value}
            )

    def _create_update_func(self, data_type: DataType):
        """创建数据更新函数"""

        async def update_func():
            if not self.data_update_manager:
                logger.warning("DataUpdateManager not initialized")
                return

            config = get_data_config(data_type)
            if not config:
                return

            try:
                if data_type == DataType.TICKER:
                    await self.data_update_manager.update_spot_tickers(
                        exchanges=self.config.tickers_exchanges,
                        quotes=self.config.tickers_quotes
                    )
                elif data_type == DataType.OHLCV:
                    await self.data_update_manager.update_spot_ohlcv(
                        exchanges=self.config.tickers_exchanges,
                        timeframe=self.config.ohlcv_timeframe,
                        top_percent=self.config.ohlcv_top_percent,
                        update_strategy=UpdateStrategy.INCREMENTAL
                    )
                elif data_type == DataType.FUTURES:
                    await self.data_update_manager.update_futures_metrics(
                        timeframe=self.config.futures_timeframe,
                        update_strategy=UpdateStrategy.INCREMENTAL
                    )
                elif data_type == DataType.COIN_MARKETS:
                    await self.data_update_manager.update_coin_markets(
                        update_strategy=UpdateStrategy.INCREMENTAL
                    )
                elif data_type == DataType.MACRO:
                    await self.data_update_manager.update_macro_data(
                        update_strategy=UpdateStrategy.INCREMENTAL
                    )
                elif data_type == DataType.GLOBAL_MARKET:
                    await self.data_update_manager.update_global_crypto_market(
                        strategy=UpdateStrategy.INCREMENTAL
                    )
            except Exception as e:
                logger.error(f"Error updating {data_type.value}: {e}")

        return update_func

    async def start(self) -> None:
        """启动调度器"""
        if self._running:
            logger.warning("Scheduler already running")
            return

        logger.info("Starting scheduler...")
        self._running = True
        self._shutdown.clear()

        await self.task_scheduler.start()
        logger.info("Scheduler started successfully")

    async def stop(self, graceful: bool = True) -> None:
        """停止调度器"""
        if not self._running:
            return

        logger.info(f"Stopping scheduler (graceful={graceful})...")
        self._running = False

        await self.task_scheduler.stop()

        # 停止 ultimate_task_manager 并关闭数据源连接
        ultimate_task_manager.stop()

        if graceful:
            wait_start = time.time()
            while self._executing_tasks and (time.time() - wait_start) < self.config.grace_period:
                await asyncio.sleep(0.5)
                logger.info(f"Waiting for {len(self._executing_tasks)} tasks to complete...")

        logger.info("Scheduler stopped successfully")

    async def async_stop(self, graceful: bool = True) -> None:
        """停止调度器（async_stop 是 stop 的别名）"""
        await self.stop(graceful)

    def get_status(self) -> Dict[str, Any]:
        """获取调度器状态"""
        return {
            'running': self._running,
            'tasks': self.task_scheduler.get_task_status(),
            'metrics': self.task_scheduler.get_metrics(),
            'update_stats': (self.data_update_manager.get_update_stats() if
                             self.data_update_manager else {})
        }

    def get_memory_usage(self) -> Dict[str, Any]:
        """获取内存使用情况"""
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()

        return {
            'rss': memory_info.rss / 1024 / 1024,
            'vms': memory_info.vms / 1024 / 1024,
            'percent': process.memory_percent(),
            'threads': process.num_threads(),
            'active_tasks': len(self.task_scheduler.tasks),
            'executing_tasks': len(self._executing_tasks)
        }

    def add_task(self, name: str, data_source_name: str, data_source_config: Dict[str, Any],
                 storage_name: str, storage_config: Dict[str, Any], time_slot: Any,
                 symbols: Optional[list] = None, timeframe: Optional[str] = None,
                 timerange: Optional[Any] = None, inplace: bool = False,
                 is_auto_created: bool = False, task_type: Optional[str] = None,
                 interval_seconds: Optional[int] = 3600) -> None:
        """添加任务

        Args:
            name: 任务名称
            data_source_name: 数据源名称
            data_source_config: 数据源配置
            storage_name: 存储名称
            storage_config: 存储配置
            time_slot: 时间槽
            symbols: 交易对列表
            timeframe: 时间框架
            timerange: 时间范围（TimeRange对象）
            inplace: 是否覆盖已存在的任务
            is_auto_created: 是否为自动创建的任务
            task_type: 任务类型
            interval_seconds: 执行间隔（秒），默认3600秒
        """
        timerange_obj = timerange
        if isinstance(timerange, str):
            from chronoforge.utils import TimeRange
            timerange_obj = TimeRange.parse_timerange(timerange)

        task = Task(
            name=name,
            data_source_name=data_source_name,
            storage_name=storage_name,
            time_slot=time_slot,
            symbols=symbols,
            timeframe=timeframe,
            timerange=timerange_obj,
            data_source_config=data_source_config,
            storage_config=storage_config,
            is_auto_created=is_auto_created
        )
        ultimate_task_manager.add_task(name=name, task=task, inplace=inplace)

        has_time_slot = time_slot is not None and hasattr(time_slot, 'type')

        if has_time_slot:
            self._register_time_slot_task(name, time_slot)
        elif interval_seconds and interval_seconds > 0:
            self._register_interval_task(name, interval_seconds)

    def _register_time_slot_task(self, task_name: str, time_slot) -> None:
        """注册基于时间槽的任务调度

        Args:
            task_name: 任务名称
            time_slot: TimeSlot 对象
        """
        logger.info(
            f"Registering time_slot task: {task_name}, "
            f"slot: {time_slot.start} - {time_slot.end}"
        )

        from chronoforge.utils import TimeSlotManager
        timeslot_manager = TimeSlotManager({task_name: time_slot})

        async def time_slot_check_func():
            in_slot = timeslot_manager.is_in_timeslot(task_name, once=True)

            if in_slot:
                logger.info(f"Time slot active for task '{task_name}', executing...")
                try:
                    result = await self.run_task_now(task_name)
                    return result
                except Exception as e:
                    logger.error(f"Error executing time_slot task {task_name}: {e}")
                    return {"success": False, "error": str(e)}
            else:
                logger.debug(f"Task '{task_name}' skipped - outside time_slot")
                return {"success": True, "skipped": True, "message": "Outside time_slot"}

        self.task_scheduler.add_interval_task(
            name=f"user_{task_name}",
            func=time_slot_check_func,
            seconds=60,
            priority=TaskPriority.MEDIUM,
            metadata={
                'task_name': task_name,
                'is_user_task': True,
                'time_slot': str(time_slot)
            }
        )

    def _register_interval_task(self, task_name: str, interval_seconds: int) -> None:
        """注册基于间隔的任务调度

        Args:
            task_name: 任务名称
            interval_seconds: 执行间隔（秒）
        """
        logger.info(f"Registering interval task: {task_name}, interval: {interval_seconds}s")

        async def interval_task_func():
            try:
                result = await self.run_task_now(task_name)
                return result
            except Exception as e:
                logger.error(f"Error executing interval task {task_name}: {e}")
                return {"success": False, "error": str(e)}

        self.task_scheduler.add_interval_task(
            name=f"user_{task_name}",
            func=interval_task_func,
            seconds=interval_seconds,
            priority=TaskPriority.MEDIUM,
            metadata={
                'task_name': task_name,
                'is_user_task': True,
                'interval_seconds': interval_seconds
            }
        )

    async def _get_incremental_start_time(
        self,
        storage,
        task,
        symbols: list,
        timeframe: str,
        data_type: Optional[str] = None
    ) -> Optional[int]:
        """获取增量更新的起始时间

        Args:
            storage: 存储实例
            task: 任务对象
            symbols: 交易对列表
            timeframe: 时间框架
            data_type: 数据类型（如 'macro_fred', 'futures_metrics', 'ohlcv' 等）

        Returns:
            起始时间戳（毫秒），如果无法获取则返回None
        """
        try:
            if not symbols or not timeframe:
                return None

            first_symbol = symbols[0]
            symbol_name = first_symbol.split(':')[1] if ':' in first_symbol else first_symbol

            if data_type == 'macro_fred':
                data_id = symbol_name
            elif data_type == 'futures_metrics':
                data_id = symbol_name
            else:
                data_id = f"{symbol_name}_{timeframe}"

            if hasattr(storage, 'get_time_range'):
                time_range = await storage.get_time_range(id=data_id, data_type=data_type)
                logger.info(
                    f"Task incremental time_range for '{data_id}' "
                    f"(data_type={data_type}): {time_range}"
                )
                if time_range and 'end_time' in time_range:
                    end_time = time_range['end_time']
                    if hasattr(end_time, 'timestamp'):
                        return int(end_time.timestamp() * 1000) + 1
                    elif isinstance(end_time, str):
                        from dateutil import parser
                        dt = parser.parse(end_time)
                        return int(dt.timestamp() * 1000) + 1
                    else:
                        return int(end_time) + 1 if end_time else None
        except Exception as e:
            logger.debug(f"Failed to get incremental start time: {e}")
        return None

    def delete_task(self, name: str) -> None:
        """删除任务"""
        ultimate_task_manager.delete_task(name)

    def get_task(self, name: str) -> Optional[Task]:
        """获取任务"""
        return ultimate_task_manager.get_task(name)

    @property
    def tasks(self) -> Dict[str, Task]:
        return ultimate_task_manager.get_all_tasks()

    @property
    def task_states(self) -> Dict[str, Any]:
        """获取所有任务状态"""
        return ultimate_task_manager.task_states

    @property
    def storage_instances(self) -> Dict[str, Any]:
        """获取所有存储实例"""
        return ultimate_task_manager.storage_instances

    def list_supported_plugins(self, plugin_type: str = "all") -> Dict[str, list]:
        """列出支持的插件

        Args:
            plugin_type: 插件类型，可选 "data_source", "storage", "all"

        Returns:
            包含插件信息的字典
        """
        from chronoforge.data_source import data_source_manager
        from chronoforge.storage.manager import storage_manager

        result = {}
        if plugin_type in ("all", "data_source"):
            result["data_source"] = list(data_source_manager.data_source_classes.keys())
        if plugin_type in ("all", "storage"):
            result["storage"] = list(storage_manager.storages.keys())
        return result

    def api_callable_function(self, plugin_name: str, plugin_type: str) -> Dict[str, Any]:
        """获取插件的可调用函数列表

        Args:
            plugin_name: 插件名称
            plugin_type: 插件类型，可选 "data_source", "storage"

        Returns:
            包含函数信息的字典
        """
        import inspect

        if plugin_type == "data_source":
            from chronoforge.data_source import data_source_manager
            data_source = data_source_manager.get_data_source(plugin_name)
            if not data_source:
                # 尝试创建数据源实例
                data_source = data_source_manager.create_data_source(plugin_name)
            if not data_source:
                raise ValueError(f"Data source {plugin_name} not found")

            functions = []
            for name, method in inspect.getmembers(
                data_source, predicate=inspect.ismethod
            ):
                if hasattr(method, 'is_api_callable') and method.is_api_callable:
                    sig = inspect.signature(method)
                    parameters = []
                    for param_name, param in sig.parameters.items():
                        if param_name == 'self':
                            continue
                        param_type = "Any"
                        if param.annotation != inspect.Parameter.empty:
                            param_type = str(param.annotation)
                        param_default = None
                        if param.default != inspect.Parameter.empty:
                            param_default = param.default
                        param_info = {
                            "name": param_name,
                            "type": param_type,
                            "default": param_default
                        }
                        parameters.append(param_info)

                    return_type = "Any"
                    if sig.return_annotation != inspect.Parameter.empty:
                        return_type = str(sig.return_annotation)

                    functions.append({
                        "name": name,
                        "docstring": method.__doc__ or "",
                        "parameters": parameters,
                        "return_type": return_type
                    })

            return {"plugin_name": plugin_name, "functions": functions}
        else:
            raise ValueError(f"Plugin type {plugin_type} not supported")

    async def delegate_call(
        self,
        plugin_name: str,
        plugin_type: str,
        function_name: str,
        **kwargs
    ) -> Any:
        """代理调用插件的函数

        Args:
            plugin_name: 插件名称
            plugin_type: 插件类型，可选 "data_source", "storage"
            function_name: 函数名称
            **kwargs: 函数参数

        Returns:
            函数返回值
        """
        if plugin_type == "data_source":
            from chronoforge.data_source import data_source_manager
            data_source = data_source_manager.get_data_source(plugin_name)
            if not data_source:
                # 尝试创建数据源实例
                data_source = data_source_manager.create_data_source(plugin_name)
            if not data_source:
                raise ValueError(f"Data source {plugin_name} not found")

            # 获取函数
            func = getattr(data_source, function_name, None)
            if not func:
                raise ValueError(
                    f"Function {function_name} not found in {plugin_name}"
                )

            # 检查是否是api_callable
            if not hasattr(func, 'is_api_callable') or not func.is_api_callable:
                raise ValueError(f"Function {function_name} is not api_callable")

            # 调用函数
            if asyncio.iscoroutinefunction(func):
                result = await func(**kwargs)
            else:
                result = func(**kwargs)

            return result
        else:
            raise ValueError(f"Plugin type {plugin_type} not supported")

    async def run_task_now(self, task_name: str) -> Dict[str, Any]:
        """立即执行指定任务（跳过时间槽限制）

        Args:
            task_name: 任务名称

        Returns:
            执行结果字典
        """
        task = ultimate_task_manager.tasks.get(task_name)
        if not task:
            return {"success": False, "error": f"Task '{task_name}' not found"}

        data_source = None
        try:
            from chronoforge.data_source import data_source_manager
            init_storage_manager()
            storage = ultimate_task_manager.storage_instances.get(task_name)
            if not storage:
                if task.storage_name == "DUCKDBStorage":
                    from chronoforge.storage.duckdb_storage import DUCKDBStorage
                    storage = DUCKDBStorage(task.storage_config)
                elif task.storage_name == "LocalFileStorage":
                    from chronoforge.storage.localfile_storage import LocalFileStorage
                    storage = LocalFileStorage(task.storage_config)
                else:
                    from chronoforge.storage.manager import storage_manager as sm
                    storage = sm.get_storage(task.storage_name)
                if storage:
                    ultimate_task_manager.storage_instances[task_name] = storage

            data_source = data_source_manager.get_data_source(task.data_source_name)
            if not data_source:
                data_source = data_source_manager.create_data_source(
                    task.data_source_name, task.data_source_config
                )

            symbols = task.symbols or []
            timeframe = task.timeframe
            timerange = task.timerange

            data_type_map = {
                'FREDDataSource': 'macro_fred',
                'CryptoUMFutureDataSource': 'futures_metrics',
                'AlthernativeDataSource': 'btc_fgi',
                'CoinGeckoDataSource': 'coin_markets',
            }
            data_type = data_type_map.get(task.data_source_name, 'ohlcv')

            start_ts_ms = None
            end_ts_ms = int(time.time() * 1000)

            logger.info(
                f"Task '{task_name}' timerange: start_ts_ms="
                f"{timerange.start_ts_ms if timerange else None}, "
                f"end_ts_ms={timerange.end_ts_ms if timerange else None}"
            )

            if timerange and timerange.start_ts_ms and timerange.end_ts_ms:
                start_ts_ms = timerange.start_ts_ms
                end_ts_ms = timerange.end_ts_ms
            elif timerange and timerange.start_ts_ms:
                start_ts_ms = timerange.start_ts_ms
                incremental_start = await self._get_incremental_start_time(
                    storage, task, symbols, timeframe, data_type
                )
                if incremental_start:
                    start_ts_ms = incremental_start
                    logger.info(f"Task '{task_name}' using incremental start: {start_ts_ms}")
            else:
                start_ts_ms = await self._get_incremental_start_time(
                    storage, task, symbols, timeframe, data_type
                )

            if start_ts_ms is None:
                import time as time_module
                start_ts_ms = int((time_module.time() - 86400 * 7) * 1000)

            from chronoforge.utils import parse_timeframe_to_milliseconds
            try:
                tf_ms = parse_timeframe_to_milliseconds(timeframe) * 2
            except Exception:
                tf_ms = 86400000 * 2  # 默认 1 天

            time_diff_ms = end_ts_ms - start_ts_ms

            for symbol in symbols:
                if time_diff_ms < tf_ms:
                    logger.info(
                        f"Skipping fetching symbol '{symbol} {timeframe}': time range "
                        f"({time_diff_ms}ms) is less than two timeframe ({tf_ms}ms)"
                    )
                    continue

                data = await data_source.fetch(
                    symbol=symbol,
                    timeframe=timeframe,
                    start_ts_ms=start_ts_ms,
                    end_ts_ms=end_ts_ms
                )

                if data is not None and not data.empty:
                    data_name = f"{symbol}_{timeframe}"

                    metadata = {
                        'exchange': symbol.split(':')[0] if ':' in symbol else 'binance',
                        'market_type': 'spot',
                        'symbol': symbol.split(':')[1] if ':' in symbol else symbol,
                        'timeframe': timeframe,
                        'data_type': data_type
                    }
                    await storage.save(id=data_name, data=data, metadata=metadata)

            ultimate_task_manager.task_states[task_name]['run_count'] += 1
            ultimate_task_manager.task_states[task_name]['last_run_time'] = time.time()
            ultimate_task_manager.task_states[task_name]['last_run_status'] = \
                ultimate_task_manager.task_states[task_name]['status']
            ultimate_task_manager.task_states[task_name]['status'] = 'completed'
            return {"success": True, "message": f"Task '{task_name}' executed successfully"}

        except Exception as e:
            ultimate_task_manager.task_states[task_name]['run_count'] += 1
            ultimate_task_manager.task_states[task_name]['last_run_time'] = time.time()
            ultimate_task_manager.task_states[task_name]['last_run_status'] = \
                ultimate_task_manager.task_states[task_name]['status']
            ultimate_task_manager.task_states[task_name]['status'] = 'failed'
            ultimate_task_manager.task_states[task_name]['error_message'] = str(e)
            return {"success": False, "error": str(e)}
        finally:
            # 关闭数据源连接
            if data_source and hasattr(data_source, '_close_all_connections'):
                try:
                    await data_source._close_all_connections()
                    logger.info(f"Task '{task_name}' data source connections closed")
                except Exception as e:
                    logger.warning(
                        f"Failed to close data source connections for task "
                        f"'{task_name}': {e}"
                    )


_global_scheduler: Optional[Scheduler] = None


async def init_scheduler(config: Optional[SchedulerConfig] = None) -> Scheduler:
    """初始化全局调度器"""
    global _global_scheduler
    _global_scheduler = Scheduler(config)
    await _global_scheduler.initialize()
    return _global_scheduler


def get_scheduler() -> Optional[Scheduler]:
    """获取全局调度器"""
    return _global_scheduler


__all__ = [
    "Scheduler",
    "init_scheduler",
    "get_scheduler",
    "TaskScheduler",
    "TaskPriority",
    "TaskStatus",
    "TaskResult",
]
