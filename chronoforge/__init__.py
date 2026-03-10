"""ChronoForge - 异步、插件式时间序列数据处理框架"""

__version__ = "0.2.0"
__author__ = "horsen666@gmail.com"
__description__ = "异步、插件式的时间序列数据处理框架"

from .data_source import DataSourceBase
from .storage import StorageBase
from .services import DataService
from .utils import parse_timeframe_to_milliseconds, TimeSlot
from .scheduler import TaskScheduler, TaskPriority, TaskStatus, TaskResult
from .scheduler.data_update import DataUpdateManager, UpdateStrategy
from .scheduler import Scheduler


__all__ = [
    "StorageBase",
    "DataSourceBase",
    "DataService",
    "parse_timeframe_to_milliseconds",
    "TimeSlot",
    "Scheduler",
    "TaskScheduler",
    "TaskPriority",
    "TaskStatus",
    "TaskResult",
    "DataUpdateManager",
    "UpdateStrategy"
]
