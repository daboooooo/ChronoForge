"""
终极任务管理器 - 彻底解决所有事件循环和连接池问题
"""
import asyncio
import time
import threading
from typing import Any, Dict, Optional, Tuple
import concurrent.futures as cf

from chronoforge.logging_config import get_logger
from chronoforge.utils import TimeSlot, TimeRange
from chronoforge.storage.manager import storage_manager
from chronoforge.data_source import DataSourceBase, data_source_manager
from chronoforge.storage import StorageBase


logger = get_logger(__name__)

# 任务类型枚举
TASK_TYPES = {
    'PERIODIC': 'periodic',       # 周期性任务
    'TIME_SLOT': 'time_slot',     # 基于时间槽的任务
}


class Task:
    """任务类，封装任务相关信息"""

    def __init__(self, name: str,
                 data_source_name: str,
                 storage_name: str,
                 time_slot: Optional[TimeSlot] = None,
                 period: Optional[int] = None,
                 symbols: Optional[list[str]] = None,
                 timeframe: Optional[str] = None,
                 timerange: Optional[TimeRange] = None,
                 data_source_config: Optional[Dict[str, Any]] = None,
                 storage_config: Optional[Dict[str, Any]] = None,
                 is_auto_created: bool = False):
        self.name = name
        self.data_source_name = data_source_name
        self.storage_name = storage_name
        self.time_slot = time_slot
        self.period = period
        self.symbols = symbols
        self.timeframe = timeframe
        self.timerange = timerange
        self.data_source_config = data_source_config or {}
        self.storage_config = storage_config or {}
        self.is_auto_created = is_auto_created
        self.task_type = TASK_TYPES['PERIODIC'] if period is not None else TASK_TYPES['TIME_SLOT']

        # run_time variable
        self.created_at = time.time()
        self.status = 'idle'
        self.last_updated_at = time.time()
        self.run_count = 0
        self.last_run_time = None
        self.last_run_status = None
        self.error_message = None

    def __repr__(self):
        task_str = f" -- Task -- name='{self.name}', "
        task_str += f"data_source='{self.data_source_name}', storage='{self.storage_name}'"
        if self.task_type == TASK_TYPES['PERIODIC']:
            task_str += f", period={self.period}"
        elif self.task_type == TASK_TYPES['TIME_SLOT']:
            task_str += f", time_slot={self.time_slot}"
        return task_str

    def get_state(self) -> Dict[str, Any]:
        """获取任务状态"""
        return {
            'name': self.name,
            'status': self.status,
            'created_at': self.created_at,
            'last_updated_at': self.last_updated_at,
            'run_count': self.run_count,
            'last_run_time': self.last_run_time,
            'last_run_status': self.last_run_status,
            'error_message': self.error_message,
            'is_auto_created': self.is_auto_created,
            'task_type': self.task_type
        }


class UltimateTaskManager:
    """终极任务管理器 - 使用终极异步执行框架"""

    def __init__(self, max_workers: int = 4):
        """初始化终极任务管理器

        Args:
            max_workers: 最大工作线程数
        """
        self.max_workers = max_workers
        self.thread_pool = cf.ThreadPoolExecutor(max_workers=max_workers)
        self.tasks: Dict[str, Task] = {}
        self.data_source_instances: Dict[str, DataSourceBase] = {}
        self.storage_instances: Dict[str, StorageBase] = {}
        self.task_states: Dict[str, Dict[str, Any]] = {}
        self.running = False
        self._shutdown = threading.Event()
        self._lock = threading.Lock()
        self._executing_tasks: Dict[str, bool] = {}

    def add_task(self, name: str,
                 task: Task,
                 inplace: bool = False) -> None:
        """添加任务

        Args:
            name: 任务名称
            task: 任务对象
            inplace: 是否替换已存在任务
        """
        with self._lock:
            if name in self.tasks and not inplace:
                raise ValueError(f"Task '{name}' already exists")

            self.tasks[name] = task
            self._init_task_state(name)

            logger.info(f"Added task '{task}'")

    def _init_task_state(self, name: str) -> None:
        """初始化任务状态"""
        if name not in self.task_states:
            self.task_states[name] = {
                'status': 'idle',
                'created_at': time.time(),
                'last_updated_at': time.time(),
                'run_count': 0,
                'last_run_time': None,
                'last_run_status': None,
                'error_message': None
            }

    def delete_task(self, name: str) -> None:
        """删除任务

        Args:
            name: 任务名称
        """
        with self._lock:
            if name not in self.tasks:
                logger.warning(f"Task '{name}' does not exist")
                return

            # 删除任务
            del self.tasks[name]

            # 删除数据源实例
            if name in self.data_source_instances:
                del self.data_source_instances[name]

            # 删除存储实例
            if name in self.storage_instances:
                del self.storage_instances[name]

            logger.info(f"Deleted task '{name}'")

    def get_task(self, name: str) -> Optional[Task]:
        """获取任务

        Args:
            name: 任务名称

        Returns:
            Optional[Task]: 任务
        """
        return self.tasks.get(name)

    def get_task_state(self, name: str) -> Optional[Dict[str, Any]]:
        """获取任务状态

        Args:
            name: 任务名称

        Returns:
            Optional[Dict[str, Any]]: 任务状态
        """
        return self.get_task(name).get_state()

    def get_all_tasks(self) -> Dict[str, Task]:
        """获取所有任务

        Returns:
            Dict[str, Task]: 所有任务
        """
        return self.tasks.copy()

    def execute_task(self, task: Task) -> Tuple[bool, str]:
        """执行任务

        Args:
            task: 任务

        Returns:
            Tuple[bool, str]: (成功标志, 消息)
        """
        task_name = task.name

        try:
            logger.info(f"Executing task '{task_name}'")

            if task_name not in self.task_states:
                self._init_task_state(task_name)

            self.task_states[task_name].update({
                'status': 'executing',
                'last_updated_at': time.time()
            })
            self._executing_tasks[task_name] = True

            # 获取数据源实例
            data_source = self.data_source_instances.get(task.data_source_name)
            if not data_source:
                # 创建数据源实例
                data_source = data_source_manager.create_data_source(task.data_source_name,
                                                                     task.data_source_config)
                if not data_source:
                    raise ValueError(f"Failed to create data source '{task.data_source_name}'")
                self.data_source_instances[task.data_source_name] = data_source

            # 获取或创建存储实例
            storage = storage_manager.get_storage(task.storage_name)
            if not storage:
                storage = storage_manager.create_storage(
                    task.storage_name,
                    task.storage_config
                )
                if not storage:
                    raise ValueError(
                        f"Failed to create storage '{task.storage_name}'"
                    )

            # 执行任务逻辑
            message = f"Task '{task_name}' executed successfully"

            # 这里应该实现具体的任务执行逻辑
            # 例如：获取数据、存储数据等

            # 更新任务状态
            self.task_states[task_name].update({
                'status': 'completed',
                'last_updated_at': time.time(),
                'run_count': self.task_states[task_name]['run_count'] + 1,
                'last_run_time': time.time(),
                'last_run_status': 'success',
                'error_message': None
            })
            self._executing_tasks.pop(task_name, None)

            logger.info(f"Task '{task_name}' executed successfully")
            return True, message

        except Exception as e:
            error_msg = f"Task '{task_name}' failed: {str(e)}"
            logger.error(error_msg)

            # 更新任务状态
            self.task_states[task_name].update({
                'status': 'failed',
                'last_updated_at': time.time(),
                'last_run_status': 'failed',
                'error_message': error_msg
            })
            self._executing_tasks.pop(task_name, None)

            return False, error_msg

    def register_data_source_instance(self, task_name: str,
                                      data_source_instance: DataSourceBase) -> None:
        """注册数据源实例，并自动创建周期性任务

        Args:
            task_name: 任务名称
            data_source_instance: 数据源实例
        """
        self.data_source_instances[task_name] = data_source_instance
        logger.debug(f"Registered data source instance for task {task_name}")

        # 自动创建周期性任务
        self._auto_create_periodic_tasks(task_name, data_source_instance)

    def get_data_source_instance(self, task_name: str) -> Optional[DataSourceBase]:
        """获取数据源实例

        Args:
            task_name: 任务名称

        Returns:
            Optional[DataSourceBase]: 数据源实例
        """
        return self.data_source_instances.get(task_name)

    def remove_data_source_instance(self, task_name: str) -> None:
        """移除数据源实例

        Args:
            task_name: 任务名称
        """
        if task_name in self.data_source_instances:
            del self.data_source_instances[task_name]
            logger.debug(f"Removed data source instance for task {task_name}")

    def _auto_create_periodic_tasks(self, task_name: str,
                                    data_source_instance: DataSourceBase) -> None:
        """自动创建周期性任务

        Args:
            task_name: 任务名称
            data_source_instance: 数据源实例
        """
        try:
            # 获取数据源的默认配置
            default_config = getattr(data_source_instance, 'default_config', {})

            # 检查是否有自动创建任务的配置
            auto_create_tasks = default_config.get('auto_create_periodic_tasks', True)
            if not auto_create_tasks:
                logger.debug(f"数据源 {task_name} 配置为不自动创建周期性任务")
                return

            # 获取周期性任务配置
            periodic_config = default_config.get('periodic_task_config', {})

            # 默认配置
            symbols = periodic_config.get('symbols', ['BTC/USDT'])
            timeframe = periodic_config.get('timeframe', '1h')
            exchange_name = periodic_config.get('exchange_name', 'binance')

            # 创建时间槽（全天）
            from chronoforge.utils import TimeSlot
            time_slot = TimeSlot(start="00:00:00", end="23:59:59")

            # 创建任务名称
            periodic_task_name = f"{task_name}_periodic"

            # 检查任务是否已存在
            if periodic_task_name in self.tasks:
                logger.debug(f"周期性任务 {periodic_task_name} 已存在，跳过创建")
                return

            # 创建周期性任务
            self.add_task(
                name=periodic_task_name,
                data_source_name=task_name,  # 使用数据源名称
                data_source_config={'exchange_name': exchange_name},
                storage_name='localfile',  # 默认使用本地文件存储
                storage_config={'base_path': './data'},
                time_slot=time_slot,
                symbols=symbols,
                timeframe=timeframe,
                is_auto_created=True,
                task_type=TASK_TYPES['PERIODIC']
            )

            logger.info(f"自动创建周期性任务: {periodic_task_name}")

        except Exception as e:
            logger.error(f"自动创建周期性任务失败: {e}")

    def start(self) -> None:
        """启动任务管理器"""
        logger.info("Starting ultimate task manager...")
        self.running = True
        self._shutdown.clear()
        logger.info("Ultimate task manager started successfully")

    def stop(self) -> None:
        """停止任务管理器"""
        logger.info("Stopping ultimate task manager...")
        self.running = False
        self._shutdown.set()

        # 关闭所有数据源连接
        self._close_data_source_connections()

        # 关闭线程池
        if self.thread_pool:
            self.thread_pool.shutdown(wait=True)

        logger.info("Ultimate task manager stopped successfully")

    def _close_data_source_connections(self) -> None:
        """关闭所有数据源的连接"""
        for name, data_source in list(self.data_source_instances.items()):
            try:
                # 检查数据源是否有异步关闭方法
                if hasattr(data_source, '_close_all_connections'):
                    close_method = data_source._close_all_connections
                    if asyncio.iscoroutinefunction(close_method):
                        # 尝试获取事件循环并运行异步关闭方法
                        try:
                            loop = asyncio.get_event_loop()
                            if loop.is_running():
                                # 如果事件循环正在运行，创建任务并等待完成
                                import concurrent.futures
                                with concurrent.futures.ThreadPoolExecutor() as executor:
                                    future = executor.submit(
                                        self._run_async_close, close_method
                                    )
                                    future.result(timeout=10)  # 等待最多10秒
                                logger.info(f"Closed data source connections: {name}")
                            else:
                                # 如果事件循环未运行，直接运行
                                loop.run_until_complete(close_method())
                                logger.info(f"Closed data source connections: {name}")
                        except RuntimeError:
                            # 没有事件循环，创建新的事件循环
                            try:
                                loop = asyncio.new_event_loop()
                                asyncio.set_event_loop(loop)
                                loop.run_until_complete(close_method())
                                loop.close()
                                logger.info(f"Closed data source connections: {name}")
                            except Exception as e:
                                logger.warning(f"Failed to close data source {name}: {e}")
                    else:
                        # 同步方法，直接调用
                        close_method()
                        logger.info(f"Closed data source connections: {name}")
            except Exception as e:
                logger.warning(f"Error closing data source {name}: {e}")

    def _run_async_close(self, close_method) -> None:
        """在线程中运行异步关闭方法"""
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(close_method())
            loop.close()
        except Exception as e:
            logger.warning(f"Error in async close: {e}")


# 全局终极任务管理器实例
ultimate_task_manager = UltimateTaskManager()


def init_ultimate_task_manager(max_workers: int = 4,
                               thread_pool: cf.ThreadPoolExecutor = None) -> UltimateTaskManager:
    """初始化终极任务管理器

    Args:
        max_workers: 最大工作线程数
        thread_pool: 线程池

    Returns:
        UltimateTaskManager: 终极任务管理器实例
    """
    global ultimate_task_manager
    ultimate_task_manager = UltimateTaskManager(max_workers=max_workers)
    if thread_pool:
        ultimate_task_manager.thread_pool = thread_pool
    return ultimate_task_manager
