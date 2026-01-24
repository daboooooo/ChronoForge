"""调度器 - ChronoForge的中央控制器"""
import os
import asyncio
import inspect
import weakref
import psutil
from typing import Any, Dict, Optional, Tuple, get_type_hints
from collections import defaultdict
import pandas as pd
import concurrent.futures as cf

from chronoforge.data_source import DataSourceBase, verify_datasource_instance
from chronoforge.data_source import (CryptoSpotDataSource, FREDDataSource, BitcoinFGIDataSource,
                                     CryptoUMFutureDataSource, GlobalMarketDataSource)
from chronoforge.storage import StorageBase, verify_storage_instance
from chronoforge.storage import LocalFileStorage


# 使DUCKDBStorage、RedisStorage和MongoDBStorage成为可选依赖
try:
    from chronoforge.storage import DUCKDBStorage
except ImportError:
    DUCKDBStorage = None

try:
    from chronoforge.storage import RedisStorage
except ImportError:
    RedisStorage = None

try:
    from chronoforge.storage import MongoDBStorage
except ImportError:
    MongoDBStorage = None
from chronoforge.utils import TimeSlot, TimeSlotManager, TimeRange, parse_timeframe_to_milliseconds
from chronoforge.logging_config import setup_logging, get_logger
import threading
import time
from datetime import datetime

# 配置日志
setup_logging()

logger = get_logger(__name__)
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
    # 任务类型枚举
    TASK_TYPES = {
        'PERIODIC': 'periodic',       # 周期性任务
        'TIME_SLOT': 'time_slot',     # 基于时间槽的任务
        'INNER': 'inner'              # 内部任务（由装饰器创建）
    }

    def __init__(self, name: str,
                 data_source_name: str,
                 storage_name: str,
                 time_slot: TimeSlot,
                 symbols: Optional[list[str]] = None,
                 sub: Optional[str] = None,
                 timeframe: Optional[str] = None,
                 timerange: Optional[TimeRange] = None,
                 data_source_config: Optional[Dict[str, Any]] = None,
                 storage_config: Optional[Dict[str, Any]] = None,
                 is_auto_created: bool = False,
                 task_type: Optional[str] = None):
        self.name = name
        self.data_source_name = data_source_name
        self.storage_name = storage_name
        self.time_slot = time_slot
        self.symbols = symbols
        self.sub = sub
        self.timeframe = timeframe
        self.timerange = timerange
        self.data_source_config = data_source_config
        self.storage_config = storage_config
        self.is_auto_created = is_auto_created

        # 任务类型，默认为TIME_SLOT
        self.task_type = task_type or self.TASK_TYPES['TIME_SLOT']

        # 统一配置存储，用于存储任务的各种配置参数
        self.config = {
            'interval': None,           # 执行间隔（秒），仅用于周期性任务
            'method_name': None,         # 内部任务的方法名
            'method_params': None,       # 内部任务的方法参数
            'max_retries': 3,            # 最大重试次数
            'retry_delay': 1,            # 重试延迟（秒）
            'priority': 0,               # 任务优先级
            'run_count': 0,              # 任务运行次数
            'last_run_time': None,       # 上次运行时间
            'last_run_status': None,     # 上次运行状态
            'error_message': None        # 错误信息
        }

    @property
    def method_name(self):
        """保持向后兼容性：获取任务的方法名"""
        return self.config['method_name']

    @method_name.setter
    def method_name(self, value):
        """保持向后兼容性：设置任务的方法名"""
        self.config['method_name'] = value

    @property
    def method_params(self):
        """保持向后兼容性：获取任务的方法参数"""
        return self.config['method_params']

    @method_params.setter
    def method_params(self, value):
        """保持向后兼容性：设置任务的方法参数"""
        self.config['method_params'] = value


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

    def api_callable_function(self, plugin_name: str, plugin_type: str) -> Dict[str, Any]:
        """枚举指定插件的所有被@api_callable装饰的函数、参数和返回值

        Args:
            plugin_name: 插件名称
            plugin_type: 插件类型，可选值为"data_source", "storage"

        Returns:
            Dict[str, Any]: 包含插件函数信息的字典，格式为：
            {
                "plugin_name": "插件名称",
                "plugin_type": "插件类型",
                "functions": [
                    {
                        "name": "函数名称",
                        "docstring": "函数文档字符串",
                        "parameters": [
                            {
                                "name": "参数名称",
                                "type": "参数类型",
                                "default": "默认值"
                            }
                        ],
                        "return_type": "返回值类型"
                    }
                ]
            }
        """
        # 检查插件类型是否有效
        if plugin_type not in ["data_source", "storage"]:
            raise ValueError(f"Invalid plugin type: {plugin_type}")

        # 检查插件是否支持
        if plugin_name not in self.list_supported_plugins(plugin_type):
            raise ValueError(f"{plugin_type} {plugin_name} not supported")

        # 获取插件类
        plugin_class = self.get_supported_plugin(plugin_type, plugin_name)

        # 创建插件实例（使用默认配置）
        plugin_instance = plugin_class({})

        # 准备结果字典
        result = {
            "plugin_name": plugin_name,
            "plugin_type": plugin_type,
            "functions": []
        }

        # 获取所有公共方法（只保留被@api_callable装饰的方法）
        for name, method in inspect.getmembers(plugin_instance, inspect.ismethod):
            # 排除私有方法（以_开头）
            if name.startswith('_'):
                continue

            # 排除继承自object类的基本方法
            if name in ['__class__', '__delattr__', '__dir__', '__eq__', '__format__',
                        '__ge__', '__getattribute__', '__gt__', '__hash__', '__init__',
                        '__init_subclass__', '__le__', '__lt__', '__ne__', '__new__',
                        '__reduce__', '__reduce_ex__', '__repr__', '__setattr__',
                        '__sizeof__', '__str__', '__subclasshook__']:
                continue

            # 只保留被@api_callable装饰的方法
            if not hasattr(method, 'is_api_callable') or not method.is_api_callable:
                continue

            # 获取方法签名和类型注解
            sig = inspect.signature(method)
            type_hints = get_type_hints(method)

            # 解析参数信息
            parameters = []
            for param_name, param in sig.parameters.items():
                # 跳过self参数
                if param_name == 'self':
                    continue

                param_info = {
                    "name": param_name,
                    "type": str(type_hints.get(param_name, type(None).__name__)),
                    "default": (str(param.default) if param.default is not inspect.Parameter.empty
                                else "None")
                }
                parameters.append(param_info)

            # 解析返回值类型
            return_type = str(type_hints.get('return', type(None).__name__))

            # 获取方法文档字符串
            docstring = inspect.getdoc(method) or ""

            # 添加函数信息到结果中
            function_info = {
                "name": name,
                "docstring": docstring,
                "parameters": parameters,
                "return_type": return_type
            }
            result["functions"].append(function_info)

        return result

    def _init_shared_event_loop(self):
        """
        初始化共享事件循环
        """
        if self._shared_event_loop is None or self._shared_event_loop.is_closed():
            # 如果事件循环已存在但已关闭，先确保之前的线程已结束
            if self._shared_event_loop is not None:
                try:
                    self._shared_event_loop.stop()
                    self._shared_event_loop.close()
                except (RuntimeError, OSError):
                    # 忽略清理异常
                    pass

            # 创建新的事件循环
            self._shared_event_loop = asyncio.new_event_loop()

            # 创建线程来运行事件循环
            def run_shared_loop():
                asyncio.set_event_loop(self._shared_event_loop)
                try:
                    self._shared_event_loop.run_forever()
                except Exception as e:
                    logger.error(f"Shared event loop exited with error: {e}")
                finally:
                    # 确保事件循环关闭
                    try:
                        self._shared_event_loop.close()
                    except (RuntimeError, OSError):
                        pass

            # 启动事件循环线程
            self._shared_event_loop_thread = threading.Thread(target=run_shared_loop, daemon=True)
            self._shared_event_loop_thread.start()
            logger.info("Shared event loop initialized and started")

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

        # delegate call plugin instances cache
        self.delegate_plugin_instances: dict[tuple[str, str], Any] = {}

        # 共享事件循环：用于所有任务和插件
        self._shared_event_loop = None
        self._shared_event_loop_thread = None

        # 初始化共享事件循环
        self._init_shared_event_loop()

        # inside states
        self.tasks: dict[str, Task] = {}  # 任务名称到任务实例的映射
        self.task_states: dict[str, Any] = {}  # 任务名称到任务状态的映射
        self._task_states_lock = threading.RLock()  # 任务状态锁，确保线程安全
        self._runner_thread: Optional[threading.Thread] = None  # 运行线程
        self.time_slot_manager = TimeSlotManager()

        # 全局并发控制：为每个交易所创建一个信号量
        # 用于控制同一个交易所的所有任务的并发请求数量
        self.exchange_semaphores = defaultdict(lambda: asyncio.Semaphore(5))
        # 可以根据需要为特定交易所设置不同的并发限制
        self.exchange_semaphores['binance'] = asyncio.Semaphore(10)
        self.exchange_semaphores['okx'] = asyncio.Semaphore(10)
        self.exchange_semaphores['bybit'] = asyncio.Semaphore(10)

        # 定义内置插件列表
        self.builtin_data_sources = [
            "CryptoSpotDataSource",
            "FREDDataSource",
            "BitcoinFGIDataSource",
            "CryptoUMFutureDataSource",
            "GlobalMarketDataSource"
        ]
        self.builtin_storages = [
            "LocalFileStorage",
            "DUCKDBStorage",
            "RedisStorage",
            "MongoDBStorage"
        ]

        # 定义任务存储文件路径
        self.tasks_file_path = os.path.join(os.path.expanduser("~"), ".chronoforge", "tasks.json")

        # 创建目录（如果不存在）
        os.makedirs(os.path.dirname(self.tasks_file_path), exist_ok=True)

        # register inside storage plugins first
        self.register_plugin(LocalFileStorage)
        # 只有在DUCKDBStorage可用时才注册
        if DUCKDBStorage is not None:
            self.register_plugin(DUCKDBStorage)
        # 只有在RedisStorage可用时才注册
        if RedisStorage is not None:
            self.register_plugin(RedisStorage)
        # 只有在MongoDBStorage可用时才注册
        if MongoDBStorage is not None:
            self.register_plugin(MongoDBStorage)

        # register inside data source plugins after storage plugins
        self.register_plugin(CryptoSpotDataSource)
        self.register_plugin(FREDDataSource)
        self.register_plugin(BitcoinFGIDataSource)
        self.register_plugin(CryptoUMFutureDataSource)
        self.register_plugin(GlobalMarketDataSource)

        # 从本地文件加载任务
        self.load_tasks_from_file()

        # 注册资源清理函数
        def _cleanup():
            """资源清理函数，用于在对象被垃圾回收时关闭共享事件循环"""
            if hasattr(self, '_shared_event_loop') and self._shared_event_loop is not None:
                try:
                    if not self._shared_event_loop.is_closed():
                        # 停止事件循环
                        self._shared_event_loop.stop()
                        # 关闭事件循环
                        self._shared_event_loop.close()
                except (RuntimeError, OSError):
                    # 在清理阶段忽略异常
                    pass
            self._shared_event_loop = None
            self._shared_event_loop_thread = None

        # 使用weakref.finalize注册清理函数
        self._finalizer = weakref.finalize(self, _cleanup)

    def delegate_call(self, plugin_name: str, plugin_type: str,
                      function_name: str, **kwargs) -> Any:
        """动态调用插件的指定函数，支持同步和异步函数

        Args:
            plugin_name: 插件名称
            plugin_type: 插件类型，可选值为"data_source", "storage"
            function_name: 要调用的函数名称
            **kwargs: 传递给函数的关键字参数

        Returns:
            Any: 函数执行结果

        Raises:
            ValueError: 当插件类型无效、插件不支持或函数不存在时
            Exception: 函数执行过程中发生的异常
        """
        # 快速参数验证
        if not plugin_name or not isinstance(plugin_name, str):
            raise ValueError("plugin_name must be a non-empty string")
        if not function_name or not isinstance(function_name, str):
            raise ValueError("function_name must be a non-empty string")

        # 检查插件类型是否有效
        if plugin_type not in ["data_source", "storage"]:
            raise ValueError(f"Invalid plugin type: {plugin_type}")

        # 检查插件是否支持
        if plugin_name not in self.list_supported_plugins(plugin_type):
            raise ValueError(f"{plugin_type} {plugin_name} not supported")

        # 获取插件类
        plugin_class = self.get_supported_plugin(plugin_type, plugin_name)

        # 从缓存中获取或创建插件实例
        plugin_key = (plugin_type, plugin_name)
        plugin_instance = self.delegate_plugin_instances.get(plugin_key)
        if plugin_instance is None:
            try:
                # 创建插件实例（使用默认配置）
                plugin_instance = plugin_class({})
                self.delegate_plugin_instances[plugin_key] = plugin_instance
                logger.debug(f"Created new plugin instance for {plugin_type}: {plugin_name}")
            except Exception as e:
                logger.error(
                    f"Failed to create plugin instance for {plugin_type}: {plugin_name}: {str(e)}")
                raise ValueError(f"Failed to create plugin instance: {str(e)}") from e

        # 检查函数是否存在
        if not hasattr(plugin_instance, function_name):
            raise ValueError(f"Function {function_name} not found in {plugin_name}")

        # 获取函数对象
        func = getattr(plugin_instance, function_name)

        # 检查函数是否为公共方法
        if function_name.startswith('_'):
            raise ValueError(f"Function {function_name} is not a public method")

        # 检查函数是否为基本方法
        if function_name in ['__class__', '__delattr__', '__dir__', '__eq__', '__format__',
                             '__ge__', '__getattribute__', '__gt__', '__hash__', '__init__',
                             '__init_subclass__', '__le__', '__lt__', '__ne__', '__new__',
                             '__reduce__', '__reduce_ex__', '__repr__', '__setattr__',
                             '__sizeof__', '__str__', '__subclasshook__']:
            raise ValueError(f"Function {function_name} is a basic method and cannot be called")

        # 检查函数是否可调用
        if not callable(func):
            raise ValueError(f"Function {function_name} is not callable")

        # 检查函数是否被@api_callable装饰
        if not hasattr(func, 'is_api_callable') or not func.is_api_callable:
            raise ValueError(f"Function {function_name} is not marked as api_callable")

        logger.info(f"Calling {plugin_type} {plugin_name}.{function_name} with kwargs: {kwargs}")

        # 调用函数并返回结果
        try:
            # 调用函数
            if inspect.iscoroutinefunction(func):
                # 异步函数处理
                logger.debug(f"Calling async function {function_name} using shared event loop")
                self._init_shared_event_loop()
                future = asyncio.run_coroutine_threadsafe(func(**kwargs), self._shared_event_loop)
                result = future.result()
            else:
                # 同步函数处理
                logger.debug(f"Calling sync function {function_name}")
                result = func(**kwargs)

            logger.info(
                f"Successfully called {function_name} in {plugin_name}, "
                f"result type: {type(result)}")

            # 处理特殊返回类型
            try:
                import pandas as pd
                if isinstance(result, pd.DataFrame):
                    logger.debug(
                        f"Converting DataFrame to serializable format, shape: {result.shape}")
                    return {
                        "data": result.to_dict(orient="records"),
                        "metadata": {
                            "columns": result.columns.tolist(),
                            "shape": result.shape,
                            "dtypes": result.dtypes.astype(str).to_dict()
                        }
                    }
            except ImportError:
                # 如果没有安装pandas，直接返回结果
                pass

            return result
        except Exception as e:
            logger.error(f"Error calling {plugin_name}.{function_name}: {str(e)}", exc_info=True)
            raise

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

    def _create_inner_tasks(self, plugin: Any, plugin_type: str):
        """为插件中被@create_task装饰的方法创建任务，可以是周期性任务或基于time_slot的任务

        Args:
            plugin: 插件类
            plugin_type: 插件类型
        """
        if plugin_type != "data_source":
            return

        # 创建插件实例（使用默认配置）
        plugin_instance = plugin({})

        # 遍历插件的所有方法
        for name, method in inspect.getmembers(plugin_instance, inspect.ismethod):
            # 检查方法是否被@create_task装饰
            if hasattr(method, 'is_periodic_task'):
                task_config = method.task_config

                # 确定任务类型
                interval = task_config.get('interval')
                is_periodic = interval is not None

                # 优先使用装饰器中指定的task_type，如果没有则根据是否有interval确定
                decorator_task_type = task_config.get('task_type')
                if decorator_task_type:
                    task_type = decorator_task_type
                elif is_periodic:
                    task_type = Task.TASK_TYPES['PERIODIC']
                else:
                    task_type = Task.TASK_TYPES['TIME_SLOT']

                # 根据任务类型设置任务后缀
                task_suffix = "_periodic" if task_type == Task.TASK_TYPES['PERIODIC'] else ""

                # 生成任务名称
                task_name = f"{plugin.__name__}_{name}{task_suffix}"

                # 创建时间槽
                if 'time_slot' in task_config and task_config['time_slot']:
                    # 使用装饰器中指定的time_slot
                    time_slot_config = task_config['time_slot']
                    time_slot = TimeSlot(
                        start=time_slot_config['start'],
                        end=time_slot_config['end']
                    )
                else:
                    # 使用全天时间段
                    time_slot = TimeSlot(
                        start="00:00:00",
                        end="23:59:59"
                    )

                # 添加任务，保存方法名和参数
                try:
                    # 使用装饰器中指定的存储配置
                    storage_name = task_config['storage_name']
                    storage_config = task_config['storage_config']

                    self.add_task(
                        name=task_name,
                        data_source_name=plugin.__name__,
                        data_source_config={},
                        storage_name=storage_name,
                        storage_config=storage_config,
                        time_slot=time_slot,
                        symbols=task_config['symbols'],
                        timeframe=task_config['timeframe'] or "1d",
                        timerange_str=task_config['timerange_str'] or "20220101-",
                        inplace=True,
                        is_auto_created=True,
                        task_type=task_type
                    )

                    # 保存方法名和完整的任务配置到任务中
                    if task_name in self.tasks:
                        # 使用统一配置存储
                        self.tasks[task_name].config['method_name'] = name
                        self.tasks[task_name].config['method_params'] = task_config

                        # 保存间隔配置（仅用于周期性任务）
                        if is_periodic:
                            self.tasks[task_name].config['interval'] = interval

                        # 保存其他配置参数
                        if 'max_retries' in task_config:
                            self.tasks[task_name].config['max_retries'] = task_config['max_retries']
                        if 'retry_delay' in task_config:
                            self.tasks[task_name].config['retry_delay'] = task_config['retry_delay']
                        if 'priority' in task_config:
                            self.tasks[task_name].config['priority'] = task_config['priority']

                    logger.info(
                        f"Created {task_type} task {task_name} for "
                        f"method {name} in plugin {plugin.__name__}")
                except (ValueError, KeyError, RuntimeError) as e:
                    logger.error(
                        f"Failed to create task for "
                        f"method {name} in plugin {plugin.__name__}: {e}")

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
                # 检查插件中是否有被@create_task装饰的方法
                self._create_inner_tasks(plugin, "data_source")
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

        # 从任务状态字典中删除
        if name in self.task_states:
            del self.task_states[name]

        # 删除时间槽
        self.time_slot_manager.delete_slot(name)

        # 删除任务实例引用
        if name in self.data_source_instances:
            del self.data_source_instances[name]
        if name in self.storage_instances:
            del self.storage_instances[name]

        # 删除任务
        del self.tasks[name]

        # 从本地文件中移除任务
        import json
        try:
            with open(self.tasks_file_path, 'r') as f:
                tasks_dict = json.load(f)

            if name in tasks_dict:
                del tasks_dict[name]
                with open(self.tasks_file_path, 'w') as f:
                    json.dump(tasks_dict, f, indent=2)
                logger.debug(f"从本地文件中删除任务 {name} 成功")
        except Exception as e:
            logger.error(f"从本地文件中删除任务 {name} 时出错: {e}")

        logger.info(f"Task {name} deleted successfully")

    def is_builtin_plugin(self, plugin_name: str, plugin_type: str) -> bool:
        """检查插件是否为内置插件

        Args:
            plugin_name: 插件名称
            plugin_type: 插件类型，可选值为"data_source", "storage"

        Returns:
            bool: 是否为内置插件
        """
        if plugin_type == "data_source":
            return plugin_name in self.builtin_data_sources
        elif plugin_type == "storage":
            return plugin_name in self.builtin_storages
        else:
            return False

    def _build_task_info_dict(self, task: Task) -> Dict[str, Any]:
        """构建任务信息字典

        Args:
            task: 任务实例

        Returns:
            Dict[str, Any]: 任务信息字典
        """
        import datetime
        # 将毫秒时间戳转换为YYYYMMDD格式
        start_date = datetime.datetime.fromtimestamp(
            task.timerange.start_ts_ms / 1000).strftime("%Y%m%d")
        end_date = "" if not task.timerange.end_ts_ms else datetime.datetime.fromtimestamp(
            task.timerange.end_ts_ms / 1000).strftime("%Y%m%d")
        timerange_str = f"{start_date}-{end_date}" if end_date else f"{start_date}-"

        return {
            "name": task.name,
            "data_source_name": task.data_source_name,
            "data_source_config": task.data_source_config,
            "storage_name": task.storage_name,
            "storage_config": task.storage_config,
            "time_slot": {
                "start": task.time_slot.start,
                "end": task.time_slot.end
            },
            "symbols": task.symbols,
            "timeframe": task.timeframe,
            "timerange_str": timerange_str
        }

    def _save_tasks_dict_to_file(self, tasks_dict: Dict[str, Any], task_name: str = None) -> None:
        """将任务字典保存到本地文件

        Args:
            tasks_dict: 任务字典
            task_name: 单个任务名称（可选），用于日志记录
        """
        import json
        try:
            with open(self.tasks_file_path, 'w') as f:
                json.dump(tasks_dict, f, indent=2)
            if task_name:
                logger.debug(f"Task {task_name} 已保存到本地文件")
            else:
                logger.debug(f"已保存 {len(tasks_dict)} 个任务到本地文件")
        except Exception as e:
            logger.error(f"保存任务到文件时出错: {e}")

    def save_task_to_file(self, task_name: str) -> None:
        """将单个任务保存到本地文件

        Args:
            task_name: 任务名称
        """
        task = self.tasks.get(task_name)
        if not task:
            return

        # 只保存由add_task创建的任务，不存储自动生成的任务
        if task.is_auto_created:
            logger.debug(f"Task {task_name} 是自动生成的任务，不保存到本地文件")
            return

        # 检查任务是否使用内置插件
        if not self.is_builtin_plugin(task.data_source_name, "data_source"):
            logger.debug(f"Task {task_name} 使用了非内置数据源 {task.data_source_name}，不保存到本地文件")
            return

        if task.storage_name and not self.is_builtin_plugin(task.storage_name, "storage"):
            logger.debug(f"Task {task_name} 使用了非内置存储 {task.storage_name}，不保存到本地文件")
            return

        # 读取现有任务
        import json
        tasks_dict = {}

        try:
            with open(self.tasks_file_path, 'r') as f:
                tasks_dict = json.load(f)
        except FileNotFoundError:
            # 文件不存在，创建新文件
            pass
        except Exception as e:
            logger.error(f"读取任务文件时出错: {e}")
            return

        # 保存任务信息
        tasks_dict[task_name] = self._build_task_info_dict(task)
        self._save_tasks_dict_to_file(tasks_dict, task_name)

    def save_all_tasks_to_file(self) -> None:
        """将所有使用内置插件的任务保存到本地文件
        """
        tasks_dict = {}

        # 遍历所有任务
        for task_name, task in self.tasks.items():
            # 只保存由add_task创建的任务，不存储自动生成的任务
            if task.is_auto_created:
                logger.debug(f"Task {task_name} 是自动生成的任务，不保存到本地文件")
                continue

            # 检查任务是否使用内置插件
            if not self.is_builtin_plugin(task.data_source_name, "data_source"):
                continue

            if task.storage_name and not self.is_builtin_plugin(task.storage_name, "storage"):
                continue

            # 保存任务信息
            tasks_dict[task_name] = self._build_task_info_dict(task)

        self._save_tasks_dict_to_file(tasks_dict)

    def load_tasks_from_file(self) -> None:
        """从本地文件加载任务
        """
        import json

        try:
            with open(self.tasks_file_path, 'r') as f:
                tasks_dict = json.load(f)
        except FileNotFoundError:
            # 文件不存在，跳过加载
            logger.debug("任务文件不存在，跳过加载")
            return
        except Exception as e:
            logger.error(f"读取任务文件时出错: {e}")
            return

        # 加载任务
        loaded_count = 0
        for task_name, task_info in tasks_dict.items():
            try:
                # 检查任务是否已存在
                if task_name in self.tasks:
                    logger.debug(f"Task {task_name} 已存在，跳过加载")
                    continue

                # 创建TimeSlot对象
                time_slot = TimeSlot(
                    start=task_info["time_slot"]["start"],
                    end=task_info["time_slot"]["end"]
                )

                # 添加任务
                self.add_task(
                    name=task_info["name"],
                    data_source_name=task_info["data_source_name"],
                    data_source_config=task_info["data_source_config"],
                    storage_name=task_info["storage_name"],
                    storage_config=task_info["storage_config"],
                    time_slot=time_slot,
                    symbols=task_info["symbols"],
                    timeframe=task_info["timeframe"],
                    timerange_str=task_info["timerange_str"],
                    inplace=True
                )
                loaded_count += 1
                logger.info(f"从本地文件加载任务 {task_name} 成功")
            except Exception as e:
                logger.error(f"加载任务 {task_name} 时出错: {e}")

        logger.info(f"共从本地文件加载 {loaded_count} 个任务")

    def add_task(self, name: str,
                 data_source_name: str, data_source_config: Dict[str, Any],
                 storage_name: str, storage_config: Dict[str, Any],
                 time_slot: TimeSlot,
                 symbols: Optional[list[str]] = None,
                 timeframe: Optional[str] = None,
                 timerange_str: Optional[str] = None,
                 inplace: bool = False,
                 is_auto_created: bool = False,
                 task_type: Optional[str] = None) -> None:
        """
        添加任务

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
            is_auto_created: 是否由scheduler自动创建，默认False
            task_type: 任务类型，可选值为'periodic', 'time_slot', 'inner'
        """
        logger.debug(f"Adding task '{name}' with data source '{data_source_name}' and "
                     f"storage '{storage_name}'")

        # 检查任务名称是否已存在
        if not name:
            raise ValueError("Task name cannot be empty")

        # 检查FRED数据源是否包含api_key
        if data_source_name == "FREDDataSource":
            if not data_source_config or "api_key" not in data_source_config:
                raise ValueError("FREDDataSource 必须包含有效的 api_key 配置")

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
            data_source_config=data_source_config,
            storage_config=storage_config,
            is_auto_created=is_auto_created,
            task_type=task_type
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

        # 如果是替换任务，清除TimeSlotManager中的last_slot记录
        if is_replacing:
            self.time_slot_manager.last_slot.pop(name, None)
            logger.debug(f"Cleared last_slot for replaced task: {name}")

        logger.info(f"Task '{name}' {status} successfully. Total tasks: {len(self.tasks)}")

        # 保存任务到本地文件
        self.save_task_to_file(name)

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

        # 内存检测相关变量
        last_memory_check_time = time.time()
        memory_check_interval = 60  # 每60秒检查一次内存

        try:
            # 使用_stop_event.is_set()作为循环条件，与start方法保持一致
            while not self._stop_event.is_set():
                # 清理已完成的任务状态
                self._clean_completed_tasks()

                # 调试日志：显示当前时间和任务列表
                current_time = datetime.now()
                logger.debug(f"Current time: {current_time} ({current_time.timestamp() * 1000})")
                logger.debug(f"Found {len(self.tasks)} Tasks: {self.tasks.keys()}")

                # 定期检测内存占用
                current_time_sec = time.time()
                if current_time_sec - last_memory_check_time >= memory_check_interval:
                    # 获取内存统计信息
                    mem_stats = self._get_memory_stats()

                    # 记录内存使用情况
                    logger.info(f"Memory Usage - RSS: {mem_stats['rss_mb']:.2f} MB, "
                                f"VMS: {mem_stats['vms_mb']:.2f} MB, "
                                f"Threads: {mem_stats['thread_count']}, "
                                f"Tasks: {mem_stats['task_count']}, "
                                f"Task Status: {mem_stats['task_status_counts']}, "
                                f"Data Sources: {mem_stats['data_source_count']}, "
                                f"Storage: {mem_stats['storage_count']}, "
                                f"Plugin Instances: {mem_stats['plugin_instance_count']}")

                    # 更新上次检查时间
                    last_memory_check_time = current_time_sec

                # 任务分组：根据交易所或数据源对任务进行分组
                # 这样可以更有效地管理连接和并发，减少资源竞争
                grouped_tasks = {}
                for task_name, task in list(self.tasks.items()):
                    # 确保任务状态存在
                    with self._task_states_lock:
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

                    # 统一判断任务类型
                    is_periodic = task.task_type == Task.TASK_TYPES['PERIODIC']
                    is_inner_task = task.config['method_name'] is not None

                    # 检查时间槽
                    # 对于周期性任务，每个时间槽内根据间隔执行；对于其他任务，每个时间槽内只执行一次
                    is_in_slot = self.time_slot_manager.is_in_timeslot(name=task_name,
                                                                       once=not is_periodic)
                    logger.debug(f"Task {task_name}: is_in_timeslot={is_in_slot}, "
                                 f"time_slot={task.time_slot}, is_periodic={is_periodic}, "
                                 f"is_inner_task={is_inner_task}")

                    # 更新任务状态为等待下次执行
                    if not is_in_slot:
                        logger.debug(f"Task {task_name} is not in timeslot, skipping")
                        # 更新任务状态为等待
                        with self._task_states_lock:
                            if self.task_states[task_name]['status'] not in ['waiting', 'created',
                                                                             'replaced']:
                                self.task_states[task_name].update({
                                    'status': 'waiting',
                                    'last_updated_at': time.time()
                                })
                        continue

                    # 如果任务在时间槽内，更新状态为pending
                    with self._task_states_lock:
                        if self.task_states[task_name]['status'] not in ['pending', 'created',
                                                                         'replaced', 'running',
                                                                         'executing', 'completed',
                                                                         'failed']:
                            self.task_states[task_name].update({
                                'status': 'pending',
                                'last_updated_at': time.time()
                            })

                    # 检查任务是否已在运行
                    with self._task_states_lock:
                        task_state = self.task_states[task_name].copy()
                    if 'future' in task_state and isinstance(task_state['future'], cf.Future):
                        if not task_state['future'].done():
                            logger.debug(f"Task {task_name} is already running "
                                         "in thread pool, skipping")
                            continue

                    # 检查是否到了执行时间
                    current_time = time.time()
                    is_time_to_execute = True

                    # 对于周期性任务，检查执行间隔
                    if is_periodic:
                        interval = task.config['interval'] or 60

                        # 初始化下次执行时间
                        with self._task_states_lock:
                            if self.task_states[task_name]['next_run_time'] is None:
                                self.task_states[task_name]['next_run_time'] = current_time
                                task_state = self.task_states[task_name].copy()
                            else:
                                task_state = self.task_states[task_name].copy()

                        # 检查是否到了执行时间
                        if current_time < task_state['next_run_time']:
                            logger.debug(f"Task {task_name} is not yet time to execute, "
                                         f"next run at {task_state['next_run_time']}, "
                                         f"interval={interval}")
                            is_time_to_execute = False
                        else:
                            # 计算下次执行时间
                            with self._task_states_lock:
                                self.task_states[task_name]['next_run_time'] = \
                                    current_time + interval

                    # 如果还没到执行时间，跳过执行但更新状态为pending
                    if not is_time_to_execute:
                        with self._task_states_lock:
                            if self.task_states[task_name]['status'] not in ['pending', 'created',
                                                                             'replaced']:
                                self.task_states[task_name].update({
                                    'status': 'pending',
                                    'last_updated_at': time.time()
                                })
                        continue

                    # -------------------------------
                    # 任务分组逻辑
                    # 目的：将任务按照交易所、数据源或连接池进行分组，以便更有效地管理资源
                    # 分组策略：
                    # 1. 优先按交易所分组，避免单个交易所请求过多
                    # 2. 其次按数据源分组，确保同一数据源的任务有序执行
                    # 3. 最后按连接池分组，优化连接使用
                    # 4. 默认分组为'default'
                    # -------------------------------
                    group_key = 'default'
                    if hasattr(task, 'method_params') and task.method_params is not None:
                        # 检查是否有交易所参数
                        if 'exchange' in task.method_params:
                            group_key = f"exchange:{task.method_params['exchange']}"
                        # 检查是否有数据源参数
                        elif 'data_source' in task.method_params:
                            group_key = f"datasource:{task.method_params['data_source']}"
                        # 检查是否有连接池参数
                        elif 'connection_pool' in task.method_params:
                            group_key = f"pool:{task.method_params['connection_pool']}"

                    if group_key not in grouped_tasks:
                        grouped_tasks[group_key] = []
                    grouped_tasks[group_key].append((task_name, task, task_state))

                # -------------------------------
                # 任务执行逻辑
                # 目的：按组执行任务，确保每组内部的任务顺序执行，避免资源竞争
                # 执行策略：
                # 1. 遍历所有任务组
                # 2. 对每个任务组，顺序提交任务到线程池
                # 3. 更新任务状态为运行中
                # -------------------------------
                for group_key, group_tasks in grouped_tasks.items():
                    logger.debug(
                        f"Processing task group: {group_key} with {len(group_tasks)} tasks")
                    for task_name, task, task_state in group_tasks:
                        # 使用线程池执行任务
                        future = self.thread_pool.submit(self.execute_task, task)

                        # 更新任务状态为运行中
                        with self._task_states_lock:
                            self.task_states[task_name].update({
                                'future': future,
                                'status': 'running',
                                'last_updated_at': time.time()
                            })

                        logger.debug(
                            f"Task {task_name} submitted to thread pool from group {group_key}")
                # -------------------------------
                # 动态间隔调整逻辑
                # 目的：根据任务数量动态调整检查间隔，平衡响应性和CPU占用
                # 调整策略：
                # - 无任务时：5秒间隔，减少CPU占用
                # - 少量任务（<10）：1秒间隔，保证响应性
                # - 中等任务量（10-50）：0.5秒间隔
                # - 大量任务（>50）：0.2秒间隔，确保高并发下的任务及时执行
                # -------------------------------
                task_count = len(self.tasks)
                if task_count == 0:
                    interval = 5.0  # 无任务时，间隔5秒
                elif task_count < 10:
                    interval = 1.0  # 少量任务，间隔1秒
                elif task_count < 50:
                    interval = 0.5  # 中等任务量，间隔0.5秒
                else:
                    interval = 0.2  # 大量任务，间隔0.2秒
                self._stop_event.wait(interval)
        except Exception as e:
            logger.error(f"Error in scheduler run loop: {e}")
        finally:
            logger.info("Scheduler run loop exited")

    def _get_memory_stats(self) -> Dict[str, Any]:
        """获取详细的内存使用统计信息"""
        # 获取进程内存信息
        process = psutil.Process(os.getpid())
        mem_info = process.memory_info()

        # 计算内存使用量（MB）
        rss_mb = mem_info.rss / (1024 * 1024)  # 物理内存
        vms_mb = mem_info.vms / (1024 * 1024)  # 虚拟内存

        # 获取组件统计信息
        task_count = len(self.tasks)
        data_source_count = len(self.data_source_instances)
        storage_count = len(self.storage_instances)
        plugin_instance_count = len(self.delegate_plugin_instances)
        thread_count = process.num_threads()

        # 获取任务状态统计
        task_status_counts = defaultdict(int)
        with self._task_states_lock:
            for task_state in self.task_states.values():
                status = task_state.get('status', 'unknown')
                task_status_counts[status] += 1

        return {
            'rss_mb': rss_mb,
            'vms_mb': vms_mb,
            'thread_count': thread_count,
            'task_count': task_count,
            'task_status_counts': dict(task_status_counts),
            'data_source_count': data_source_count,
            'storage_count': storage_count,
            'plugin_instance_count': plugin_instance_count
        }

    def _clean_completed_tasks(self) -> None:
        """清理已完成的任务状态，只清理future对象，保留任务历史状态"""
        with self._task_states_lock:
            for task_name, state in self.task_states.items():
                if isinstance(state, dict) and 'future' in state:
                    if state['future'].done():
                        # 清理future对象，但保留其他状态信息
                        del state['future']
                        logger.debug(f"Cleaned up future for completed task: {task_name}")

    def stop(self) -> None:
        """停止调度器（同步版本）"""
        if self._runner_thread and self._runner_thread.is_alive():
            logger.info("Stopping scheduler...")
            self._stop_event.set()

            # 等待运行线程完成
            self._runner_thread.join(timeout=5)  # 减少超时时间

            # 关闭线程池
            if hasattr(self, 'thread_pool'):
                self.thread_pool.shutdown(wait=True, cancel_futures=True)
                logger.info("Thread pool shut down")

            # 保存所有任务到文件
            self.save_all_tasks_to_file()
            logger.info("Tasks saved to file")

            # 清理任务状态和插件缓存
            self.task_states.clear()
            self.delegate_plugin_instances.clear()

            # 关闭数据源连接（放在最后，确保其他资源已释放）
            logger.info("Closing data source connections...")
            for name, data_source in list(self.data_source_instances.items()):
                try:
                    # 尝试关闭数据源的所有连接
                    if hasattr(data_source, '_close_all_connections'):
                        # 使用独立的临时事件循环关闭连接，避免依赖共享事件循环
                        temp_loop = asyncio.new_event_loop()
                        try:
                            # 为关闭连接操作设置超时
                            task = temp_loop.create_task(data_source._close_all_connections())
                            # 使用wait_for设置超时，避免无限等待
                            temp_loop.run_until_complete(asyncio.wait_for(task, timeout=5))
                            logger.info(f"Closed connections for data source: {name}")
                        except asyncio.TimeoutError:
                            logger.warning(f"关闭数据源 {name} 连接超时，强制继续")
                        except Exception as e:
                            logger.warning(f"关闭数据源 {name} 连接失败: {e}")
                        finally:
                            temp_loop.close()

                    # 从实例字典中移除已关闭的数据源
                    del self.data_source_instances[name]
                except Exception as e:
                    logger.error(f"Error closing connections for data source {name}: {e}")
                    # 确保数据源实例被移除，避免内存泄漏
                    if name in self.data_source_instances:
                        del self.data_source_instances[name]

            logger.info("Scheduler stopped (sync)")

    async def async_stop(self) -> None:
        """停止调度器（异步版本）"""
        if self._runner_thread and self._runner_thread.is_alive():
            logger.info("Stopping scheduler asynchronously...")
            self._stop_event.set()

            # 等待运行线程完成
            self._runner_thread.join(timeout=30)  # 设置超时

            # 关闭线程池
            if hasattr(self, 'thread_pool'):
                self.thread_pool.shutdown(wait=True, cancel_futures=True)
                logger.info("Thread pool shut down")

            # 保存所有任务到文件
            self.save_all_tasks_to_file()
            logger.info("Tasks saved to file")

            # 关闭数据源连接 - 利用当前运行的事件循环
            logger.info("Closing data source connections...")
            for name, data_source in list(self.data_source_instances.items()):
                try:
                    # 直接使用当前事件循环关闭数据源连接
                    # 由于我们在async_stop方法中，可以直接await
                    if hasattr(data_source, '_close_all_connections'):
                        await data_source._close_all_connections()
                        logger.info(f"Closed connections for data source: {name}")

                    # 从实例字典中移除已关闭的数据源
                    del self.data_source_instances[name]
                except Exception as e:
                    logger.error(f"Error closing connections for data source {name}: "
                                 f"{e}")

            # 清理任务状态和插件缓存
            self.task_states.clear()
            self.delegate_plugin_instances.clear()
            logger.info("Scheduler stopped (async)")

    async def _convert_tickers_dataframe(self, tickers_dict: dict) -> pd.DataFrame:
        """将tickers字典转换为标准化的DataFrame

        Args:
            tickers_dict: tickers字典，键为交易对，值为交易对数据

        Returns:
            pd.DataFrame: 标准化的tickers DataFrame
        """
        import pandas as pd
        # 将每个交易对转换为一行，创建合理的DataFrame
        tickers_df = pd.DataFrame.from_dict(tickers_dict, orient='index')

        # 数据类型处理：统一数据类型，确保Feather格式兼容
        if not tickers_df.empty:
            # 重命名索引为symbol，保留交易对信息
            tickers_df = tickers_df.reset_index().rename(
                columns={'index': 'symbol'})

            # 确保所有列名都是字符串类型
            tickers_df.columns = [str(col) for col in tickers_df.columns]

            # 转换所有列的数据类型，确保Feather格式兼容
            for col in tickers_df.columns:
                try:
                    # 特殊处理symbol列，确保为字符串类型
                    if col == 'symbol':
                        tickers_df[col] = tickers_df[col].astype(str)
                    # 转换所有数值列为float，避免混合类型
                    elif pd.api.types.is_numeric_dtype(tickers_df[col]):
                        tickers_df[col] = tickers_df[col].astype(float)
                    # 转换时间戳列为int，避免混合类型
                    elif 'timestamp' in col.lower():
                        if pd.api.types.is_numeric_dtype(tickers_df[col]):
                            tickers_df[col] = tickers_df[col].astype(int)
                        else:
                            # 尝试转换为datetime，然后转换为int（毫秒）
                            try:
                                tickers_df[col] = pd.to_datetime(tickers_df[col])
                                tickers_df[col] = tickers_df[col].astype(int) // 10**6
                            except Exception:
                                # 如果转换失败，将列转换为字符串类型，确保类型一致
                                tickers_df[col] = tickers_df[col].astype(str)
                    # 转换所有其他列为字符串类型，确保类型一致
                    else:
                        tickers_df[col] = tickers_df[col].astype(str)
                except Exception as e:
                    logger.warning(f"转换列 {col} 数据类型失败: {e}")
                    # 转换失败时，将列转换为字符串类型，确保类型一致
                    tickers_df[col] = tickers_df[col].astype(str)

        return tickers_df

    async def _save_tickers_dataframe(self, storage, storage_id: str, tickers_df: pd.DataFrame,
                                      task: Task, quote: str) -> None:
        """将tickers DataFrame保存到存储

        Args:
            storage: 存储实例
            storage_id: 基础存储ID
            tickers_df: 要保存的tickers DataFrame
            task: 任务实例
            quote: quote值，用于生成存储ID后缀
        """
        # 保存到存储
        nested_storage_id = f"{storage_id}_{quote}"
        if not tickers_df.empty:
            success = await storage.save(
                id=nested_storage_id,
                data=tickers_df,
                sub=task.sub
            )
            if not success:
                logger.error(f"Failed to save task {task.name} result "
                             f"for {quote} to "
                             f"storage {task.storage_name}")
        else:
            logger.debug(f"跳过保存空数据: {quote} - {nested_storage_id}")

    async def _process_tickers_result(self, storage, storage_id: str, task: Task,
                                      result: dict, quote: str) -> None:
        """处理tickers任务结果

        Args:
            storage: 存储实例
            storage_id: 存储ID
            task: 任务实例
            result: 任务执行结果
            quote: 方法参数中的quote值
        """
        # 检查返回结果是否已经是按quote过滤后的单层字典
        # 如果是嵌套字典（未指定quote或quote为空），则按原逻辑处理
        is_nested_dict = any(isinstance(v, dict) for v in result.values())

        if is_nested_dict:
            # 处理嵌套字典类型的tickers结果
            # 例如：{'USDT': {'BTC/USDT': {...}, 'ETH/USDT': {...}}}
            for nested_quote, nested_tickers_data in result.items():
                if isinstance(nested_tickers_data, dict):
                    # 转换tickers数据为DataFrame
                    tickers_df = await self._convert_tickers_dataframe(nested_tickers_data)
                    # 保存tickers DataFrame到存储
                    await self._save_tickers_dataframe(storage, storage_id, tickers_df,
                                                       task, nested_quote)
        else:
            # 处理单层字典类型的tickers结果
            # 例如：{'BTC/USDT': {...}, 'ETH/USDT': {...}}
            # tickers结果是单层字典（已按quote过滤），直接转换为DataFrame
            tickers_df = await self._convert_tickers_dataframe(result)
            # 保存tickers DataFrame到存储
            await self._save_tickers_dataframe(storage, storage_id, tickers_df, task,
                                               quote if quote else 'all')

    async def _handle_task_result(self, task: Task, result: Any) -> None:
        """处理任务执行结果

        Args:
            task: 任务实例
            result: 任务执行结果
        """
        try:
            # -------------------------------
            # 任务结果处理主流程
            # 1. 检查是否配置了存储
            # 2. 根据任务类型和结果类型选择合适的处理方式
            # 3. 将结果转换为适合存储的格式
            # 4. 保存结果到存储
            # -------------------------------
            # 检查是否配置了存储
            if task.storage_name and hasattr(task, 'method_name'):
                storage = self.storage_instances.get(task.name)
                if storage:
                    # 生成存储ID
                    storage_id = f"{task.data_source_name}_{task.method_name}"

                    # 保存结果
                    # 检查结果类型，如果是字典，需要转换为DataFrame
                    import pandas as pd
                    data_to_save = result

                    # -------------------------------
                    # 特殊处理tickers任务结果
                    # 目的：避免创建超宽DataFrame，优化存储格式
                    # -------------------------------
                    if isinstance(result, dict) and task.method_name in ['tickers_binance',
                                                                         'tickers_okx']:
                        # 获取方法参数中的quote值
                        quote = None
                        if hasattr(task, 'method_params') and 'params' in task.method_params:
                            quote = task.method_params['params'].get('quote')

                        # 处理tickers结果
                        await self._process_tickers_result(storage, storage_id, task, result, quote)
                        return

                    # -------------------------------
                    # 处理一般字典类型结果
                    # -------------------------------
                    elif isinstance(result, dict):
                        # 将字典转换为适合存储的格式
                        # 如果字典值是DataFrame，直接使用
                        if all(isinstance(v, pd.DataFrame) for v in result.values()):
                            # 对于嵌套的DataFrame字典，我们需要为每个值单独存储
                            for key, df in result.items():
                                nested_storage_id = f"{storage_id}_{key}"
                                success = await storage.save(
                                    id=nested_storage_id,
                                    data=df,
                                    sub=task.sub
                                )
                                if not success:
                                    logger.error(f"Failed to save task {task.name} result {key} to "
                                                 f"storage {task.storage_name}")
                            return
                        else:
                            # 将字典转换为简单的DataFrame
                            data_to_save = pd.DataFrame([result])

                    # -------------------------------
                    # 处理其他类型结果
                    # -------------------------------
                    # 如果不是DataFrame且不是空值，尝试转换
                    elif not isinstance(result, pd.DataFrame) and result is not None:
                        try:
                            data_to_save = pd.DataFrame([result])
                        except Exception:
                            logger.warning("Unable to convert result to DataFrame "
                                           f"for task {task.name}")
                            return

                    # -------------------------------
                    # 保存数据到存储
                    # -------------------------------
                    success = await storage.save(
                        id=storage_id,
                        data=data_to_save,
                        sub=task.sub
                    )
                    if not success:
                        logger.error(f"Failed to save task {task.name} result to storage")
        except Exception as e:
            logger.error(f"Error handling task {task.name} result: {e}")

    async def _execute_task(self, task: Task) -> None:
        """执行单个任务
        如果任务有method_name属性，则调用该方法；否则执行默认的K线更新逻辑
        """
        try:
            ds = self.data_source_instances.get(task.name)
            st = self.storage_instances.get(task.name)

            # 检查是否是周期性任务（有指定的method_name）
            if hasattr(task, 'method_name') and task.method_name is not None:
                logger.info(f"Executing periodic method {task.method_name} for task {task.name}")

                # 获取方法对象
                method = getattr(ds, task.method_name)

                # 获取任务配置
                method_params = task.method_params
                max_retries = method_params.get('max_retries', 3)
                retry_delay = method_params.get('retry_delay', 1)

                # 准备方法调用参数
                params = method_params.get('params', {})  # 从装饰器配置中获取参数

                # 根据方法签名确定需要的参数
                sig = inspect.signature(method)
                param_names = list(sig.parameters.keys())
                # 移除self参数（如果存在）
                if 'self' in param_names:
                    param_names.remove('self')

                # 如果装饰器中没有提供必要的参数，使用默认值
                if not params:
                    if 'exchange_name' in param_names:
                        params['exchange_name'] = 'binance'  # 默认使用binance交易所
                    if 'quote' in param_names:
                        params['quote'] = 'USDT'  # 默认使用USDT作为报价货币

                # 实现重试机制
                for attempt in range(max_retries + 1):
                    try:
                        # 调用方法
                        result = await method(**params)
                        logger.info(
                            f"Periodic method {task.method_name} executed successfully "
                            f"(attempt {attempt+1}/{max_retries+1}), "
                            f"result type: {type(result)}")

                        # 处理任务结果（可以根据需要扩展）
                        await self._handle_task_result(task, result)
                        break
                    except Exception as e:
                        if attempt < max_retries:
                            # 不再检查事件循环状态，直接重试
                            logger.warning(
                                f"Periodic method {task.method_name} failed "
                                f"(attempt {attempt+1}/{max_retries+1}), "
                                f"retrying in {retry_delay}s: {e}")
                            await asyncio.sleep(retry_delay)
                        else:
                            logger.error(
                                f"Periodic method {task.method_name} failed after {max_retries+1} "
                                f"attempts: {e}")
                            # 更新任务状态为失败
                            if task.name in self.task_states:
                                self.task_states[task.name].update({
                                    'last_run_status': 'failed',
                                    'error_message': str(e)
                                })
                            raise
                # 连接池管理下，不再每次任务后关闭连接，让连接保持在池中以便复用
                # 连接会在连接池有效期内自动管理，或在应用关闭时统一关闭

            # 否则执行默认的K线更新逻辑
            else:
                logger.info(f"Executing default K-line update for task {task.name}")

                async def update_with_semaphore(symbol):
                    """使用全局信号量包装的更新函数"""
                    # 解析symbol中的交易所名称
                    try:
                        parts = symbol.split(":")
                        if len(parts) == 3:
                            # 格式：datasource:exchange:symbol
                            _, symbol_exchange_name, _ = parts
                        elif len(parts) == 2:
                            # 格式：exchange:symbol
                            if 'datasource' in parts[0].lower():
                                # 如果第一部分包含datasource，则使用默认binance
                                symbol_exchange_name = 'binance'
                            else:
                                symbol_exchange_name, _ = parts
                        else:
                            # 如果无法分割，则使用数据源的exchange_name属性或默认binance
                            symbol_exchange_name = getattr(ds, 'exchange_name', 'binance')
                    except Exception:
                        # 如果解析失败，使用数据源的exchange_name属性或默认binance
                        symbol_exchange_name = getattr(ds, 'exchange_name', 'binance')

                    symbol_exchange_name = symbol_exchange_name.lower()

                    async with self.exchange_semaphores[symbol_exchange_name]:
                        return await _update_data(
                            data_source=ds,
                            storage=st,
                            symbol=symbol,
                            timeframe=task.timeframe,
                            sub=task.sub,
                            timerange=task.timerange,
                        )

                # 使用asyncio.gather并行处理所有symbols，但受信号量限制
                results = await asyncio.gather(
                    *[update_with_semaphore(symbol) for symbol in task.symbols],
                    return_exceptions=True
                )

                # 处理结果
                for symbol, (success, message) in zip(task.symbols, results):
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
            with self._task_states_lock:
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
            with self._task_states_lock:
                self.task_states[task_name].update({
                    'status': 'executing',
                    'last_updated_at': time.time(),
                    'last_run_time': time.time(),
                    'error_message': None
                })

            # 使用共享事件循环执行任务
            self._init_shared_event_loop()
            logger.debug(f"Using shared event loop for task {task_name}")

            # 在共享事件循环中执行任务
            future = asyncio.run_coroutine_threadsafe(self._execute_task(task),
                                                      self._shared_event_loop)
            _ = future.result()  # 阻塞直到任务完成

            # 更新任务状态为完成
            with self._task_states_lock:
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
            with self._task_states_lock:
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
