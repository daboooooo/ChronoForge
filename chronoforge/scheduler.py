"""调度器 - ChronoForge的中央控制器"""

import asyncio
import logging
from typing import Any, Dict, Optional, Tuple
import pandas as pd
import concurrent.futures as cf

from chronoforge.data_source import DataSourceBase, verify_datasource_instance
from chronoforge.data_source import (CryptoSpotDataSource, FREDDataSource, BitcoinFGIDataSource,
                                     CryptoUMFutureDataSource, GlobalMarketDataSource)
from chronoforge.storage import StorageBase, verify_storage_instance
from chronoforge.storage import LocalFileStorage, DUCKDBStorage

# 使RedisStorage成为可选依赖
try:
    from chronoforge.storage import RedisStorage
except ImportError:
    RedisStorage = None
from chronoforge.utils import TimeSlot, TimeSlotManager, TimeRange, parse_timeframe_to_milliseconds
import threading
import time
from datetime import datetime

logger = logging.getLogger(__name__)
SUPPORTE_TIMEFRAMES = ["1w", "1d", "4h", "1h"]


class LockManager:
    def __init__(self):
        self._locks = {}
        self._lock_dict_lock = threading.Lock()

    def get_lock(self, key):
        with self._lock_dict_lock:
            if key not in self._locks:
                self._locks[key] = threading.RLock()
            return self._locks[key]


lock_manager = LockManager()


class Task:
    """任务类，封装任务相关信息"""
    def __init__(self, name: str,
                 data_source_name: str,
                 storage_name: str,
                 time_slot: TimeSlot,
                 symbols: Optional[list[str]] = None,
                 sub: Optional[str] = None,
                 timeframe: Optional[str] = None,
                 timerange: Optional[TimeRange] = None):
        self.name = name
        self.data_source_name = data_source_name
        self.storage_name = storage_name
        self.time_slot = time_slot
        self.symbols = symbols
        self.sub = sub
        self.timeframe = timeframe
        self.timerange = timerange


async def _load_data_for_updating(
        storage: StorageBase,
        symbol: str, timeframe: str,
        sub: Optional[str] = None,
        timerange: Optional[TimeRange] = None) -> Tuple[Optional[pd.DataFrame],
                                                        Optional[TimeRange]]:
    """加载缓存数据并计算需要更新的时间范围

        Args:
            storage (StorageBase): 数据存储实例
            symbol (str): 交易对符号
            timeframe (str): 时间周期
            sub (Optional[str], optional): 子类型. Defaults to None.
            timerange (Optional[TimeRange], optional): 时间范围. Defaults to None.

        Returns:
            Tuple[Optional[pd.DataFrame], Optional[TimeRange]]:
                缓存数据、需要更新的时间范围
    """
    try:
        # 获取存储锁 - 支持单写多读模式
        storage_lock = lock_manager.get_lock(storage.name)

        # 在异步锁内部获取线程锁，确保跨线程安全
        async with asyncio.Lock():
            with storage_lock:
                # 检查数据是否存在
                exists = await storage.exists(id=f"{symbol}_{timeframe}", sub=sub)
                if not exists:
                    logger.debug(f"{symbol} - {timeframe} 数据不存在")
                    return None, timerange

                # 加载缓存数据
                cached_data = await storage.load(id=f"{symbol}_{timeframe}", sub=sub)
                if cached_data is None or cached_data.empty:
                    logger.debug(f"{symbol} - {timeframe} 缓存数据为空")
                    return None, timerange

        # 读取缓存数据中的最大时间戳ms（最后一个K线的时间），并转换为int
        min_cached_ts = cached_data['time'].min()
        min_cached_ts_ms = int(min_cached_ts.timestamp() * 1000)
        max_cached_ts = cached_data['time'].max()
        max_cached_ts_ms = int(max_cached_ts.timestamp() * 1000)

        logger.info(f"{sub} - {symbol} - {timeframe} - 缓存范围: "
                    f"{min_cached_ts_ms} ({min_cached_ts}) - {max_cached_ts_ms} ({max_cached_ts})")

        # 解析时间周期长度（毫秒）
        timeframe_ms = parse_timeframe_to_milliseconds(timeframe)

        # 计算下一个数据点的起始时间戳（加一个时间周期）
        next_ts_ms = max_cached_ts_ms + timeframe_ms

        # 计算指定的结束时间戳（如果没有指定，则使用当前时间）
        if timerange and timerange.end_ts_ms:
            specified_end_time_ms = timerange.end_ts_ms
        else:
            # 使用当前时间作为结束时间
            specified_end_time_ms = int(time.time() * 1000)

        # 判断是否需要下载新数据，落后两个时间周期才需要更新
        need_download = next_ts_ms < (specified_end_time_ms - timeframe_ms)

        if not need_download:
            return cached_data, None

        # 创建新的时间范围对象
        updated_timerange = TimeRange(
            start_ts_ms=next_ts_ms,
            end_ts_ms=specified_end_time_ms
        )

        return cached_data, updated_timerange

    except Exception as e:
        logger.error(
            f"❌加载 {symbol} - {timeframe} 缓存数据或计算更新时间范围时出错: {str(e)}",
            exc_info=True
        )
        return None, timerange


async def _update_data(
        data_source: DataSourceBase,
        storage: StorageBase,
        symbol: str,
        timeframe: str,
        sub: Optional[str] = None,
        timerange: Optional[TimeRange] = None) -> Tuple[bool, str]:
    """下载单个交易对的单个时间周期的K线数据"""
    try:
        # 首先尝试加载缓存数据 - 支持增量更新，避免重复下载
        cached_data, updated_timerange = await _load_data_for_updating(
            storage=storage,
            symbol=symbol,
            timeframe=timeframe,
            sub=sub,
            timerange=timerange
        )

        # 如果updated_timerange为None，表示不需要下载新数据
        if updated_timerange is None:
            return True, f"✅ {symbol} - {timeframe} 数据符合time range: {timerange}"

        logger.info(
            "%s - %s - %s - 更新范围: %s",
            sub, symbol, timeframe, updated_timerange
        )

        # 从交易所下载数据 - 重试机制在装饰器中处理
        df = await data_source.fetch(
            symbol=symbol,
            timeframe=timeframe,
            start_ts_ms=updated_timerange.start_ts_ms,
            end_ts_ms=updated_timerange.end_ts_ms
        )

        # 检查下载结果
        if df is None or df.empty:
            return True, f"⚠️ 未下载到 {symbol} - {timeframe} 新数据"

        # 合并新旧数据
        if cached_data is not None and not cached_data.empty:
            min_date = cached_data['time'].min()
            max_date = cached_data['time'].max()
            logger.debug(
                f"{symbol} - {timeframe} 合并前，缓存数据时间: {min_date} 到 {max_date}"
            )
            # 合并并去重（按时间）
            combined_df = pd.concat([cached_data, df], ignore_index=True).drop_duplicates(
                subset="time").sort_values("time")
            # 重置索引
            df = combined_df.reset_index(drop=True)
            logger.debug(
                f"{symbol} - {timeframe} 合并后，数据时间范围: {df['time'].min()} 到 {df['time'].max()}"
            )

        # 带锁数据持久化 - 使用storage name作为锁key
        storage_lock = lock_manager.get_lock(storage.name)
        async with asyncio.Lock():
            # 在异步锁内部获取线程锁，确保跨线程安全
            with storage_lock:
                success = await storage.save(
                    id=f"{symbol}_{timeframe}",
                    data=df,
                    sub=sub,
                )
        if not success:
            return False, f"保存 {symbol} - {timeframe} 数据时出错"

        if cached_data is not None and not cached_data.empty:
            new_items_len = len(df) - len(cached_data)
        else:
            new_items_len = len(df)

        return True, f"✅{symbol} - {timeframe} 新数据下载并更新成功, " + \
            f"共 {len(df)} 条记录, 新增 {new_items_len} 条, " + \
            f"时间范围: {df['time'].min()} 到 {df['time'].max()}"

    except Exception as e:
        return False, f"下载 {symbol} - {timeframe} 时出错: {str(e)}"


class Scheduler:
    """调度器类，作为ChronoForge的中央控制器

    使用流程：
    1. 初始化调度器
    2. 注册支持的插件（数据来源、存储、分析器）
    3. 创建任务实例，指定任务名称、数据来源、存储、分析器、时间槽、ID列表（可选）
    4. 添加任务实例，调度器根据任务实例创建数据来源、存储、分析器实例，并创建时间调度任务
    5. 运行调度器，开始执行任务

    以上步骤的2、3、4可以重复执行，也可以在调度器运行过程中动态执行。
    """

    def __init__(self, max_workers: int = 5):
        """创建调度器

        Args:
            max_workers: 最大并发任务数，默认5
        """
        # 初始化时创建线程池
        self.thread_pool = cf.ThreadPoolExecutor(max_workers=max_workers)
        # plugins
        self.supported_data_sources: list[DataSourceBase] = []
        self.supported_storages: list[StorageBase] = []

        # plugin instances
        self.storage_instances: dict[str, StorageBase] = {}
        self.data_source_instances: dict[str, DataSourceBase] = {}

        # inside states
        self.tasks: dict[str, Task] = {}  # 任务名称到任务实例的映射
        self.task_states: dict[str, Any] = {}  # 任务名称到任务状态的映射
        self._runner_thread: Optional[threading.Thread] = None  # 运行线程
        self.time_slot_manager = TimeSlotManager()

        # register inside data source plugins
        self.register_plugin(CryptoSpotDataSource)
        self.register_plugin(FREDDataSource)
        self.register_plugin(BitcoinFGIDataSource)
        self.register_plugin(CryptoUMFutureDataSource)
        self.register_plugin(GlobalMarketDataSource)

        # register inside storage plugins
        self.register_plugin(LocalFileStorage)
        self.register_plugin(DUCKDBStorage)
        # 只有在RedisStorage可用时才注册
        if RedisStorage is not None:
            self.register_plugin(RedisStorage)

    def list_supported_plugins(self, plugin_type: str) -> list[str]:
        """列出所有支持的插件

        Args:
            plugin_type: 插件类型，可选值为"data_source", "storage"

        Returns:
            list[str]: 所有支持的插件名称
        """
        if plugin_type == "data_source":
            return [ds.__name__ for ds in self.supported_data_sources]
        elif plugin_type == "storage":
            return [ds.__name__ for ds in self.supported_storages]
        else:
            raise ValueError(f"Invalid plugin type: {plugin_type}")

    def get_supported_plugin(self, plugin_type: str, plugin_name: str) -> Any:
        """获取支持的插件实例

        Args:
            plugin_type: 插件类型，可选值为"data_source", "storage"
            plugin_name: 插件名称

        Returns:
            Any: 插件实例
        """
        if plugin_type == "data_source":
            supported_plugins = self.supported_data_sources
        elif plugin_type == "storage":
            supported_plugins = self.supported_storages
        else:
            raise ValueError(f"Invalid plugin type: {plugin_type}")

        for plugin in supported_plugins:
            if plugin.__name__ == plugin_name:
                return plugin
        raise ValueError(f"Plugin {plugin_name} not supported")

    def register_plugin(self, plugin: Any) -> Tuple[bool, str]:
        """ 识别插件类型，验证插件，完成注册

        Args:
            plugin: 任意插件实例

        Returns:
            Tuple[bool, str]: 注册结果（成功/失败）和消息
        """
        # 检查插件类型并添加到相应列表
        if issubclass(plugin, StorageBase):
            success, msg = verify_storage_instance(plugin)
            if success:
                self.supported_storages.append(plugin)
                return True, "Storage instance registered successfully"
            else:
                return False, msg
        elif issubclass(plugin, DataSourceBase):
            success, msg = verify_datasource_instance(plugin)
            if success:
                self.supported_data_sources.append(plugin)
                return True, "Data source instance registered successfully"
            else:
                return False, msg
        else:
            logger.error("Unsupported instance type: %s", type(plugin))
            return False, "Unsupported instance type"

    def delete_task(self, name: str) -> None:
        """删除任务

        Args:
            name: 任务名称
        """
        logger.info(f"Deleting task: {name}")

        # 检查任务是否存在
        if name not in self.tasks:
            raise ValueError(f"Task {name} not found")

        # 更新任务状态为已删除
        if name in self.task_states:
            self.task_states[name].update({
                'status': 'deleted',
                'last_updated_at': time.time()
            })

        # 删除时间槽
        self.time_slot_manager.delete_slot(name)

        # 删除任务实例引用
        if name in self.data_source_instances:
            del self.data_source_instances[name]
        if name in self.storage_instances:
            del self.storage_instances[name]

        # 删除任务
        del self.tasks[name]

        logger.info(f"Task {name} deleted successfully")

    def add_task(self, name: str,
                 data_source_name: str, data_source_config: Dict[str, Any],
                 storage_name: str, storage_config: Dict[str, Any],
                 time_slot: TimeSlot,
                 symbols: Optional[list[str]] = None,
                 timeframe: Optional[str] = None,
                 timerange_str: Optional[str] = None,
                 inplace: bool = False) -> None:
        """添加任务

        Args:
            name: 任务名称
            data_source_name: 数据源名称
            data_source_config: 数据源配置
            storage_name: 存储名称
            storage_config: 存储配置
            time_slot: 时间槽
            symbols: 交易对列表，可选. 对于 CryptoSpotDataSource, 格式为"exchange:symbol"
            timeframe: 时间框架，可选, 默认"1d"
            timerange_str: 时间范围字符串，可选, 默认"20220101-"
            inplace: 是否覆盖已存在任务，默认True
        """
        logger.debug(f"Adding task '{name}' with data source '{data_source_name}' and "
                     f"storage '{storage_name}'")

        # 检查任务名称是否已存在
        if not name:
            raise ValueError("Task name cannot be empty")

        # 检查任务是否已存在
        is_replacing = name in self.tasks

        if not inplace and is_replacing:
            raise ValueError(f"Task name {name} already exists")

        logger.debug(f"Task name '{name}' validation passed")

        # check task params
        if timeframe and timeframe not in SUPPORTE_TIMEFRAMES:
            raise ValueError(f"timeframe must be one of {SUPPORTE_TIMEFRAMES}")
        if not timeframe:
            timeframe = "1d"
        logger.debug(f"Task '{name}' timeframe set to '{timeframe}'")

        if data_source_name not in self.list_supported_plugins("data_source"):
            raise ValueError(f"Data source {data_source_name} not supported")
        logger.debug(f"Data source '{data_source_name}' validation passed")

        sub = data_source_name
        logger.debug(f"Task '{name}' sub parameter set to '{sub}'")

        if storage_name:
            if storage_name not in self.list_supported_plugins("storage"):
                raise ValueError(f"Storage {storage_name} not supported")
        logger.debug(f"Storage '{storage_name}' validation passed")

        if not timerange_str:
            timerange_str = "20220101-"
        timerange = TimeRange.parse_timerange(timerange_str)
        logger.debug(f"Task '{name}' timerange: {timerange}")

        # 创建 plugin 实例
        try:
            if data_source_name:
                logger.debug(f"Creating data source instance for '{name}'")
                data_source_instance = self.get_supported_plugin(
                    "data_source", data_source_name)(data_source_config)
                self.data_source_instances[name] = data_source_instance

            if storage_name:
                logger.debug(f"Creating storage instance for '{name}'")
                storage_instance = self.get_supported_plugin(
                    "storage", storage_name)(storage_config)
                self.storage_instances[name] = storage_instance

            # 添加timeslots
            logger.info(f"Adding time slot for task '{name}': {time_slot}")
            self.time_slot_manager.add_slot(
                name=name, timeslot=time_slot, inplace=inplace)

        except Exception as e:
            # 出现异常时，删除已创建的实例和timeslot
            logger.error(f"Failed to add task {name}: {e}")
            if name in self.data_source_instances:
                del self.data_source_instances[name]
            if name in self.storage_instances:
                del self.storage_instances[name]
            self.time_slot_manager.delete_slot(name)
            raise

        # 添加任务列表
        logger.debug(f"Creating Task object for '{name}'")
        self.tasks[name] = Task(
            name=name,
            data_source_name=data_source_name,
            storage_name=storage_name,
            time_slot=time_slot,
            symbols=symbols,
            sub=sub,
            timeframe=timeframe,
            timerange=timerange,
        )

        # 更新任务状态
        status = "replaced" if is_replacing else "created"
        self.task_states[name] = {
            'status': status,
            'created_at': time.time(),
            'last_updated_at': time.time(),
            'next_run_time': None,
            'run_count': 0,
            'last_run_time': None,
            'last_run_status': None,
            'error_message': None
        }

        logger.info(f"Task '{name}' {status} successfully. Total tasks: {len(self.tasks)}")

    def start(self) -> None:
        """启动调度器，在线程中运行run方法"""
        if (hasattr(self, '_runner_thread') and self._runner_thread is not None and
                self._runner_thread.is_alive()):
            logger.warning("Scheduler already running")
            return
        self._stop_event = threading.Event()
        self._runner_thread = threading.Thread(target=self.run, daemon=True)
        self._runner_thread.start()
        logger.info("Scheduler started")

    def run(self) -> None:
        """运行调度器，检查时间槽并执行任务"""
        logger.info("Scheduler running")
        try:
            # 使用_stop_event.is_set()作为循环条件，与start方法保持一致
            while not self._stop_event.is_set():
                # 清理已完成的任务状态
                self._clean_completed_tasks()

                # 调试日志：显示当前时间和任务列表
                current_time = datetime.now()
                logger.debug(f"Current time: {current_time} ({current_time.timestamp() * 1000})")
                logger.debug(f"Found {len(self.tasks)} Tasks: {self.tasks.keys()}")

                for task_name, task in list(self.tasks.items()):
                    # 确保任务状态存在
                    if task_name not in self.task_states:
                        self.task_states[task_name] = {
                            'status': 'created',
                            'created_at': time.time(),
                            'last_updated_at': time.time(),
                            'next_run_time': None,
                            'run_count': 0,
                            'last_run_time': None,
                            'last_run_status': None,
                            'error_message': None
                        }

                    # 检查时间槽
                    is_in_slot = self.time_slot_manager.is_in_timeslot(name=task_name, once=True)
                    logger.debug(f"Task {task_name}: is_in_timeslot={is_in_slot}, "
                                 f"time_slot={task.time_slot}")

                    # 更新任务状态为等待下次执行
                    if not is_in_slot:
                        logger.debug(f"Task {task_name} is not in timeslot, skipping")
                        # 更新任务状态为等待
                        if self.task_states[task_name]['status'] not in ['waiting', 'created',
                                                                         'replaced']:
                            self.task_states[task_name].update({
                                'status': 'waiting',
                                'last_updated_at': time.time()
                            })
                        continue

                    # 检查任务是否已在运行
                    task_state = self.task_states[task_name]
                    if 'future' in task_state and isinstance(task_state['future'], cf.Future):
                        if not task_state['future'].done():
                            logger.debug(f"Task {task_name} is already running "
                                         "in thread pool, skipping")
                            continue

                    # 使用线程池执行任务
                    future = self.thread_pool.submit(self.execute_task, task)

                    # 更新任务状态为运行中
                    self.task_states[task_name].update({
                        'future': future,
                        'status': 'running',
                        'last_updated_at': time.time()
                    })

                    logger.debug(f"Task {task_name} submitted to thread pool")
                    time.sleep(0.1)
                # 每5秒检查一次
                time.sleep(5)
        except Exception as e:
            logger.error(f"Error in scheduler run loop: {e}")
        finally:
            logger.info("Scheduler run loop exited")

    def _clean_completed_tasks(self) -> None:
        """清理已完成的任务状态，只清理future对象，保留任务历史状态"""
        for task_name, state in self.task_states.items():
            if isinstance(state, dict) and 'future' in state:
                if state['future'].done():
                    # 清理future对象，但保留其他状态信息
                    del state['future']
                    logger.debug(f"Cleaned up future for completed task: {task_name}")

    def stop(self) -> None:
        """停止调度器"""
        if self._runner_thread and self._runner_thread.is_alive():
            logger.info("Stopping scheduler...")
            self._stop_event.set()

            # 等待运行线程完成
            self._runner_thread.join(timeout=30)  # 设置超时

            # 关闭数据源连接
            logger.info("Closing data source connections...")
            for name, data_source in list(self.data_source_instances.items()):
                try:
                    # 检查当前是否已经有事件循环在运行
                    try:
                        loop = asyncio.get_event_loop()
                        is_loop_running = loop.is_running()
                    except RuntimeError:
                        # 如果没有事件循环，创建一个新的
                        loop = None
                        is_loop_running = False

                    if is_loop_running:
                        # 如果当前已经有事件循环在运行，使用当前循环
                        # 由于我们在同步方法中，不能直接await，所以只能创建任务
                        loop.create_task(data_source.close_all_connections())
                        logger.info(f"Scheduled closing connections for data source: {name}")
                    else:
                        # 如果没有事件循环，使用asyncio.run()
                        asyncio.run(data_source.close_all_connections())
                        logger.info(f"Closed connections for data source: {name}")

                    # 从实例字典中移除已关闭的数据源
                    del self.data_source_instances[name]
                except Exception as e:
                    logger.error(f"Error closing connections for data source {name}: "
                                 f"{e}")

            # 关闭线程池
            if hasattr(self, 'thread_pool'):
                self.thread_pool.shutdown(wait=True, cancel_futures=True)
                logger.info("Thread pool shut down")

            # 清理任务状态
            self.task_states.clear()
            logger.info("Scheduler stopped")

    async def _execute_task(self, task: Task) -> None:
        """执行单个任务, 对于task中的每个symbol, 执行以下步骤:
        1. load 缓存数据
        2. fetch 最新数据
        3. merge 数据
        4. store 数据
        """
        try:
            ds = self.data_source_instances.get(task.name)
            st = self.storage_instances.get(task.name)

            for symbol in task.symbols:
                success, message = await _update_data(
                    data_source=ds,
                    storage=st,
                    symbol=symbol,
                    timeframe=task.timeframe,
                    sub=task.sub,
                    timerange=task.timerange,
                )
                if not success:
                    logger.error(f"Failed to update data for {symbol}: {message}")
                    continue
                logger.info(message)

        except Exception as e:
            logger.exception(f"Task {task.name} execution error: {e}")

    def execute_task(self, task: Task) -> None:
        """执行异步任务
        """
        task_name = task.name
        logger.info(f"Executing task: {task_name}")

        try:
            # 确保任务状态存在
            if task_name not in self.task_states:
                self.task_states[task_name] = {
                    'status': 'created',
                    'created_at': time.time(),
                    'last_updated_at': time.time(),
                    'next_run_time': None,
                    'run_count': 0,
                    'last_run_time': None,
                    'last_run_status': None,
                    'error_message': None
                }

            # 更新任务状态为执行中
            self.task_states[task_name].update({
                'status': 'executing',
                'last_updated_at': time.time(),
                'last_run_time': time.time(),
                'error_message': None
            })

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(self._execute_task(task))

                # 更新任务状态为完成
                if task_name in self.task_states:
                    self.task_states[task_name].update({
                        'status': 'completed',
                        'last_updated_at': time.time(),
                        'run_count': self.task_states[task_name].get('run_count', 0) + 1,
                        'last_run_status': 'success',
                        'error_message': None
                    })
                logger.info(f"Task {task_name} completed successfully")
            except Exception as e:
                error_msg = str(e)
                logger.exception(f"Task {task_name} execution error: {e}")

                # 更新任务状态为失败
                if task_name in self.task_states:
                    self.task_states[task_name].update({
                        'status': 'failed',
                        'last_updated_at': time.time(),
                        'run_count': self.task_states[task_name].get('run_count', 0) + 1,
                        'last_run_status': 'failed',
                        'error_message': error_msg
                    })
                raise
            finally:
                try:
                    # 优雅地关闭事件循环
                    tasks = asyncio.all_tasks(loop)
                    for t in tasks:
                        t.cancel()
                    loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
                    loop.close()
                except Exception as cleanup_error:
                    logger.error(f"Error during loop cleanup for task {task_name}: {cleanup_error}")
        except Exception as outer_e:
            error_msg = str(outer_e)
            logger.error(f"Unhandled exception in task {task_name}: {outer_e}")

            # 更新任务状态为失败
            if task_name in self.task_states:
                self.task_states[task_name].update({
                    'status': 'failed',
                    'last_updated_at': time.time(),
                    'run_count': self.task_states[task_name].get('run_count', 0) + 1,
                    'last_run_status': 'failed',
                    'error_message': error_msg
                })
        finally:
            # 确保即使发生异常也不会阻止任务状态清理
            pass
