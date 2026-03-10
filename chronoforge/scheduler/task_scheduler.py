"""
定时任务调度器模块

提供灵活的时间调度机制，支持：
- 固定间隔执行任务
- 整点/半点/每小时执行任务
- Cron-like调度模式
- 任务依赖管理
- 错误处理和恢复
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Callable, Any
from enum import Enum
from dataclasses import dataclass, field
from croniter import croniter
import uuid

logger = logging.getLogger(__name__)


class TaskPriority(Enum):
    """任务优先级枚举"""
    CRITICAL = 0
    HIGH = 1
    MEDIUM = 2
    LOW = 3


class TaskStatus(Enum):
    """任务状态枚举"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"


@dataclass
class TaskResult:
    """任务执行结果"""
    task_id: str
    task_name: str
    status: TaskStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_ms: Optional[float] = None
    error: Optional[str] = None
    result: Any = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if self.end_time and self.start_time:
            delta = self.end_time - self.start_time
            self.duration_ms = delta.total_seconds() * 1000


@dataclass
class ScheduledTask:
    """定时任务配置"""
    task_id: str
    name: str
    func: Callable
    schedule_type: str  # 'interval', 'cron', 'hourly'
    schedule_config: Dict[str, Any]  # 调度配置
    priority: TaskPriority = TaskPriority.MEDIUM
    enabled: bool = True
    timeout_seconds: Optional[float] = None
    retry_count: int = 0
    retry_delay_seconds: float = 1.0
    dependencies: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None
    last_result: Optional[TaskResult] = None


class TaskScheduler:
    """
    统一任务调度器

    核心功能：
    - 支持多种调度模式（间隔、Cron、每小时）
    - 任务依赖管理
    - 并发控制
    - 错误重试机制
    - 执行历史记录
    - 优雅关闭

    使用示例：
        scheduler = TaskScheduler()
        scheduler.add_task(
            name="fetch_tickers",
            func=fetch_tickers_task,
            schedule_type="interval",
            schedule_config={"seconds": 30}
        )
        scheduler.add_hourly_task(
            name="update_ohlcv",
            func=update_ohlcv_task,
            offset_seconds=30  # 每小时结束30秒后执行
        )
        await scheduler.start()
    """

    def __init__(
        self,
        max_concurrent_tasks: int = 10,
        default_timeout: Optional[float] = None,
        enable_metrics: bool = True
    ):
        """
        初始化任务调度器

        Args:
            max_concurrent_tasks: 最大并发任务数
            default_timeout: 默认任务超时时间（秒）
            enable_metrics: 是否启用任务指标收集
        """
        self.tasks: Dict[str, ScheduledTask] = {}
        self.task_results: List[TaskResult] = []
        self.max_concurrent_tasks = max_concurrent_tasks
        self.default_timeout = default_timeout
        self.enable_metrics = enable_metrics

        self._running = False
        self._task_semaphore = asyncio.Semaphore(max_concurrent_tasks)
        self._task_locks: Dict[str, asyncio.Lock] = {}
        self._task_futures: Dict[str, asyncio.Task] = {}

        self._execution_history: List[TaskResult] = []
        self._metrics: Dict[str, Any] = {
            'total_tasks': 0,
            'completed_tasks': 0,
            'failed_tasks': 0,
            'total_execution_time_ms': 0.0
        }

    def add_task(
        self,
        name: str,
        func: Callable,
        schedule_type: str,
        schedule_config: Dict[str, Any],
        priority: TaskPriority = TaskPriority.MEDIUM,
        enabled: bool = True,
        timeout_seconds: Optional[float] = None,
        retry_count: int = 0,
        retry_delay_seconds: float = 1.0,
        dependencies: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        添加定时任务

        Args:
            name: 任务名称
            func: 任务函数（异步）
            schedule_type: 调度类型 ('interval', 'cron')
            schedule_config: 调度配置
                - interval: {'seconds': int} 或 {'minutes': int}
                - cron: {'cron_expr': str}
            priority: 任务优先级
            enabled: 是否启用
            timeout_seconds: 超时时间
            retry_count: 重试次数
            retry_delay_seconds: 重试延迟
            dependencies: 依赖的任务ID列表
            metadata: 任务元数据

        Returns:
            str: 任务ID
        """
        task_id = str(uuid.uuid4())[:8]
        now = datetime.now(timezone.utc)

        task = ScheduledTask(
            task_id=task_id,
            name=name,
            func=func,
            schedule_type=schedule_type,
            schedule_config=schedule_config,
            priority=priority,
            enabled=enabled,
            timeout_seconds=timeout_seconds or self.default_timeout,
            retry_count=retry_count,
            retry_delay_seconds=retry_delay_seconds,
            dependencies=dependencies or [],
            metadata=metadata or {},
            next_run=self._calculate_next_run(schedule_type, schedule_config, now)
        )

        self.tasks[task_id] = task
        self._task_locks[task_id] = asyncio.Lock()

        logger.info(f"Added task: {name} ({task_id}), schedule: {schedule_type}:{schedule_config}")
        return task_id

    def add_interval_task(
        self,
        name: str,
        func: Callable,
        seconds: Optional[int] = None,
        minutes: Optional[int] = None,
        priority: TaskPriority = TaskPriority.MEDIUM,
        enabled: bool = True,
        timeout_seconds: Optional[float] = None,
        retry_count: int = 0,
        dependencies: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """添加固定间隔任务"""
        schedule_config = {}
        if seconds is not None:
            schedule_config['seconds'] = seconds
        elif minutes is not None:
            schedule_config['minutes'] = minutes
        else:
            raise ValueError("必须指定 seconds 或 minutes")

        return self.add_task(
            name=name,
            func=func,
            schedule_type='interval',
            schedule_config=schedule_config,
            priority=priority,
            enabled=enabled,
            timeout_seconds=timeout_seconds,
            retry_count=retry_count,
            dependencies=dependencies,
            metadata=metadata
        )

    def add_hourly_task(
        self,
        name: str,
        func: Callable,
        offset_seconds: int = 0,
        minute: int = 0,
        priority: TaskPriority = TaskPriority.MEDIUM,
        enabled: bool = True,
        timeout_seconds: Optional[float] = None,
        retry_count: int = 0,
        dependencies: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        添加每小时执行的任务

        Args:
            name: 任务名称
            func: 任务函数
            offset_seconds: 每小时开始后的偏移秒数
            minute: 分钟（与offset_seconds二选一）
            priority: 优先级
            enabled: 是否启用
            timeout_seconds: 超时时间
            retry_count: 重试次数
            dependencies: 依赖任务
            metadata: 元数据
        """
        if minute > 0:
            offset_seconds = minute * 60

        return self.add_task(
            name=name,
            func=func,
            schedule_type='hourly',
            schedule_config={'offset_seconds': offset_seconds},
            priority=priority,
            enabled=enabled,
            timeout_seconds=timeout_seconds,
            retry_count=retry_count,
            dependencies=dependencies,
            metadata=metadata
        )

    def add_cron_task(
        self,
        name: str,
        func: Callable,
        cron_expr: str,
        priority: TaskPriority = TaskPriority.MEDIUM,
        enabled: bool = True,
        timeout_seconds: Optional[float] = None,
        retry_count: int = 0,
        dependencies: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """添加Cron表达式任务"""
        return self.add_task(
            name=name,
            func=func,
            schedule_type='cron',
            schedule_config={'cron_expr': cron_expr},
            priority=priority,
            enabled=enabled,
            timeout_seconds=timeout_seconds,
            retry_count=retry_count,
            dependencies=dependencies,
            metadata=metadata
        )

    def remove_task(self, task_id: str) -> bool:
        """移除任务"""
        if task_id in self.tasks:
            del self.tasks[task_id]
            if task_id in self._task_locks:
                del self._task_locks[task_id]
            logger.info(f"Removed task: {task_id}")
            return True
        return False

    def enable_task(self, task_id: str) -> bool:
        """启用任务"""
        if task_id in self.tasks:
            self.tasks[task_id].enabled = True
            logger.info(f"Enabled task: {task_id}")
            return True
        return False

    def disable_task(self, task_id: str) -> bool:
        """禁用任务"""
        if task_id in self.tasks:
            self.tasks[task_id].enabled = False
            logger.info(f"Disabled task: {task_id}")
            return True
        return False

    async def _execute_task(self, task: ScheduledTask) -> TaskResult:
        """执行单个任务"""
        start_time = datetime.now(timezone.utc)
        result = TaskResult(
            task_id=task.task_id,
            task_name=task.name,
            status=TaskStatus.RUNNING,
            start_time=start_time
        )

        try:
            if task.timeout_seconds:
                await asyncio.wait_for(task.func(), timeout=task.timeout_seconds)
            else:
                await task.func()

            result.status = TaskStatus.COMPLETED
            result.result = "success"

        except asyncio.TimeoutError:
            result.status = TaskStatus.FAILED
            result.error = f"Task timed out after {task.timeout_seconds}s"
            logger.error(f"Task {task.name} timed out")

        except asyncio.CancelledError:
            result.status = TaskStatus.CANCELLED
            result.error = "Task was cancelled"
            logger.warning(f"Task {task.name} was cancelled")

        except Exception as e:
            result.status = TaskStatus.FAILED
            result.error = str(e)
            logger.error(f"Task {task.name} failed: {str(e)}", exc_info=True)

        finally:
            result.end_time = datetime.now(timezone.utc)
            result.duration_ms = (result.end_time - start_time).total_seconds() * 1000
            task.last_run = start_time
            task.last_result = result
            self._execution_history.append(result)
            self._metrics['total_tasks'] += 1

            if result.status == TaskStatus.COMPLETED:
                self._metrics['completed_tasks'] += 1
            else:
                self._metrics['failed_tasks'] += 1

            self._metrics['total_execution_time_ms'] += result.duration_ms or 0

        return result

    async def _run_task_with_retry(self, task: ScheduledTask) -> None:
        """带重试的任务执行"""
        last_error = None

        for attempt in range(task.retry_count + 1):
            try:
                async with self._task_semaphore:
                    await self._execute_task(task)
                return

            except Exception as e:
                last_error = e
                if attempt < task.retry_count:
                    await asyncio.sleep(task.retry_delay_seconds * (attempt + 1))

        if last_error:
            logger.error(f"Task {task.name} failed after {task.retry_count + 1} attempts")

    async def _check_dependencies(self, task: ScheduledTask) -> bool:
        """检查任务依赖是否满足"""
        for dep_id in task.dependencies:
            if dep_id in self.tasks:
                dep_task = self.tasks[dep_id]
                if dep_task.last_result and dep_task.last_result.status != TaskStatus.COMPLETED:
                    logger.warning(f"Task {task.name} skipped: dependency {dep_id} not completed")
                    return False
        return True

    async def _scheduler_loop(self):
        """主调度循环"""
        logger.info("Task scheduler started")

        while self._running:
            now = datetime.now(timezone.utc)
            tasks_to_run = []

            for task_id, task in self.tasks.items():
                if not task.enabled:
                    continue

                if task.next_run and now >= task.next_run:
                    tasks_to_run.append(task)

            if tasks_to_run:
                tasks_to_run.sort(key=lambda t: t.priority.value)

                for task in tasks_to_run:
                    if await self._check_dependencies(task):
                        task_future = asyncio.create_task(self._run_task_with_retry(task))
                        self._task_futures[task.task_id] = task_future

                        task.next_run = self._calculate_next_run(
                            task.schedule_type,
                            task.schedule_config,
                            task.last_run or now
                        )

                        logger.debug(f"Scheduled task: {task.name} at {task.next_run}")

            await asyncio.sleep(1)

        logger.info("Task scheduler stopped")

    async def start(self):
        """启动调度器"""
        if self._running:
            logger.info("Task scheduler already running")
            return
        self._running = True
        asyncio.create_task(self._scheduler_loop())
        logger.info("Task scheduler started")

    async def stop(self):
        """停止调度器"""
        self._running = False

        for task_id, future in self._task_futures.items():
            if not future.done():
                future.cancel()

        logger.info("Task scheduler stopped")

    async def wait_for_completion(self, timeout: Optional[float] = None):
        """等待所有任务完成"""
        if self._task_futures:
            await asyncio.wait(
                self._task_futures.values(),
                timeout=timeout
            )

    def get_pending_tasks(self) -> List[ScheduledTask]:
        """获取待执行的任务"""
        now = datetime.now(timezone.utc)
        return [
            task for task in self.tasks.values()
            if task.enabled and task.next_run and now >= task.next_run
        ]

    def get_task_status(self, task_id: Optional[str] = None) -> Dict[str, Any]:
        """获取任务状态"""
        if task_id and task_id in self.tasks:
            task = self.tasks[task_id]
            return {
                'task_id': task.task_id,
                'name': task.name,
                'status': 'enabled' if task.enabled else 'disabled',
                'schedule_type': task.schedule_type,
                'next_run': task.next_run.isoformat() if task.next_run else None,
                'last_run': task.last_run.isoformat() if task.last_run else None,
                'priority': task.priority.name,
                'last_result_status': task.last_result.status.value if task.last_result else None
            }

        return {
            task_id: self.get_task_status(task_id)
            for task_id in self.tasks
        }

    def get_metrics(self) -> Dict[str, Any]:
        """获取调度器指标"""
        return {
            **self._metrics,
            'active_tasks': len(self.tasks),
            'running_tasks': sum(
                1 for task in self.tasks.values()
                if task.last_result and task.last_result.status == TaskStatus.RUNNING
            ),
            'avg_execution_time_ms': (
                self._metrics['total_execution_time_ms'] / self._metrics['total_tasks']
                if self._metrics['total_tasks'] > 0 else 0
            )
        }

    def _calculate_next_run(
        self,
        schedule_type: str,
        schedule_config: Dict[str, Any],
        from_time: datetime
    ) -> datetime:
        """计算下次执行时间"""
        now = from_time

        if schedule_type == 'interval':
            seconds = schedule_config.get('seconds', 0) or schedule_config.get('minutes', 0) * 60
            return datetime.fromtimestamp(now.timestamp() + seconds, tz=timezone.utc)

        elif schedule_type == 'hourly':
            offset_seconds = schedule_config.get('offset_seconds', 0)
            next_hour = now.replace(
                minute=0, second=0, microsecond=0
            )
            next_run = next_hour.timestamp() + 3600 + offset_seconds
            return datetime.fromtimestamp(next_run, tz=timezone.utc)

        elif schedule_type == 'cron':
            cron_expr = schedule_config.get('cron_expr')
            if cron_expr:
                cron = croniter(cron_expr, now)
                return cron.get_next(datetime)

        return now


# 全局调度器实例
default_scheduler = TaskScheduler()


def init_scheduler(
    max_concurrent_tasks: int = 10,
    default_timeout: Optional[float] = None
) -> TaskScheduler:
    """初始化全局调度器"""
    global default_scheduler
    default_scheduler = TaskScheduler(
        max_concurrent_tasks=max_concurrent_tasks,
        default_timeout=default_timeout
    )
    logger.info("Default scheduler initialized")
    return default_scheduler
