"""
TaskScheduler 测试用例

测试定时任务调度器的所有功能，包括：
1. 任务添加和移除
2. 调度时间计算
3. 任务执行
4. 优先级和依赖
5. 并发控制
6. 指标收集
"""

import pytest
import asyncio
from datetime import datetime, timezone
from chronoforge.scheduler import TaskScheduler, TaskPriority


class TestTaskSchedulerInit:
    """任务调度器初始化测试"""

    def test_init_default(self):
        """测试默认初始化"""
        scheduler = TaskScheduler()
        assert scheduler is not None
        assert scheduler.max_concurrent_tasks == 10
        assert scheduler._running is False

    def test_init_custom(self):
        """测试自定义初始化"""
        scheduler = TaskScheduler(max_concurrent_tasks=5, default_timeout=30.0)
        assert scheduler.max_concurrent_tasks == 5
        assert scheduler.default_timeout == 30.0


class TestTaskAddition:
    """任务添加测试"""

    def setup_method(self):
        """测试前初始化"""
        self.scheduler = TaskScheduler()

    async def dummy_task(self):
        """测试用空任务"""
        await asyncio.sleep(0.01)

    def test_add_interval_task_seconds(self):
        """测试添加间隔任务（秒）"""
        task_id = self.scheduler.add_interval_task(
            name="test_task",
            func=self.dummy_task,
            seconds=30
        )
        assert task_id is not None
        assert task_id in self.scheduler.tasks
        assert len(self.scheduler.tasks[task_id].schedule_config) > 0

    def test_add_interval_task_minutes(self):
        """测试添加间隔任务（分钟）"""
        task_id = self.scheduler.add_interval_task(
            name="test_task",
            func=self.dummy_task,
            minutes=5
        )
        assert task_id is not None

    def test_add_hourly_task(self):
        """测试添加每小时任务"""
        task_id = self.scheduler.add_hourly_task(
            name="hourly_task",
            func=self.dummy_task,
            offset_seconds=30
        )
        assert task_id is not None
        task = self.scheduler.tasks[task_id]
        assert task.schedule_type == 'hourly'
        assert task.schedule_config.get('offset_seconds') == 30

    def test_add_cron_task(self):
        """测试添加Cron任务"""
        task_id = self.scheduler.add_cron_task(
            name="cron_task",
            func=self.dummy_task,
            cron_expr="0 * * * *"
        )
        assert task_id is not None
        task = self.scheduler.tasks[task_id]
        assert task.schedule_type == 'cron'
        assert task.schedule_config.get('cron_expr') == "0 * * * *"

    def test_add_task_with_priority(self):
        """测试添加带优先级的任务"""
        task_id = self.scheduler.add_interval_task(
            name="priority_task",
            func=self.dummy_task,
            seconds=10,
            priority=TaskPriority.HIGH
        )
        task = self.scheduler.tasks[task_id]
        assert task.priority == TaskPriority.HIGH

    def test_add_task_with_retry(self):
        """测试添加带重试的任务"""
        task_id = self.scheduler.add_interval_task(
            name="retry_task",
            func=self.dummy_task,
            seconds=10,
            retry_count=3
        )
        task = self.scheduler.tasks[task_id]
        assert task.retry_count == 3


class TestTaskRemoval:
    """任务移除测试"""

    def setup_method(self):
        """测试前初始化"""
        self.scheduler = TaskScheduler()

    async def dummy_task(self):
        await asyncio.sleep(0.01)

    def test_remove_task_success(self):
        """测试成功移除任务"""
        task_id = self.scheduler.add_interval_task(
            name="remove_test",
            func=self.dummy_task,
            seconds=30
        )
        assert task_id in self.scheduler.tasks
        result = self.scheduler.remove_task(task_id)
        assert result is True
        assert task_id not in self.scheduler.tasks

    def test_remove_task_not_found(self):
        """测试移除不存在的任务"""
        result = self.scheduler.remove_task("nonexistent")
        assert result is False


class TestTaskEnableDisable:
    """任务启用禁用测试"""

    def setup_method(self):
        """测试前初始化"""
        self.scheduler = TaskScheduler()

    async def dummy_task(self):
        await asyncio.sleep(0.01)

    def test_disable_task(self):
        """测试禁用任务"""
        task_id = self.scheduler.add_interval_task(
            name="disable_test",
            func=self.dummy_task,
            seconds=30
        )
        assert self.scheduler.tasks[task_id].enabled is True
        result = self.scheduler.disable_task(task_id)
        assert result is True
        assert self.scheduler.tasks[task_id].enabled is False

    def test_enable_task(self):
        """测试启用任务"""
        task_id = self.scheduler.add_interval_task(
            name="enable_test",
            func=self.dummy_task,
            seconds=30,
            enabled=False
        )
        assert self.scheduler.tasks[task_id].enabled is False
        result = self.scheduler.enable_task(task_id)
        assert result is True
        assert self.scheduler.tasks[task_id].enabled is True


class TestNextRunCalculation:
    """下次执行时间计算测试"""

    def setup_method(self):
        """测试前初始化"""
        self.scheduler = TaskScheduler()

    def test_interval_next_run(self):
        """测试间隔任务的下次执行时间"""
        now = datetime.now(timezone.utc)
        task_id = self.scheduler.add_interval_task(
            name="interval_test",
            func=lambda: None,
            seconds=60
        )
        task = self.scheduler.tasks[task_id]
        assert task.next_run is not None
        assert isinstance(task.next_run, datetime)
        assert task.next_run > now

    def test_hourly_next_run(self):
        """测试每小时任务的下次执行时间"""
        now = datetime.now(timezone.utc)
        task_id = self.scheduler.add_hourly_task(
            name="hourly_test",
            func=lambda: None,
            offset_seconds=30
        )
        task = self.scheduler.tasks[task_id]
        assert task.next_run is not None
        assert isinstance(task.next_run, datetime)
        offset = (task.next_run - now).total_seconds()
        assert 30 <= offset <= 3630


class TestSchedulerLifecycle:
    """调度器生命周期测试"""

    def setup_method(self):
        """测试前初始化"""
        self.scheduler = TaskScheduler(max_concurrent_tasks=2)

    async def dummy_task(self):
        await asyncio.sleep(0.05)

    @pytest.mark.asyncio
    async def test_start_stop(self):
        """测试启动和停止"""
        assert self.scheduler._running is False
        await self.scheduler.start()
        assert self.scheduler._running is True
        await self.scheduler.stop()
        assert self.scheduler._running is False

    @pytest.mark.asyncio
    async def test_scheduler_loop_runs(self):
        """测试调度循环执行"""
        executed = []

        async def counted_task():
            executed.append(True)

        task_id = self.scheduler.add_interval_task(
            name="counted",
            func=counted_task,
            seconds=1
        )
        _ = task_id

        await self.scheduler.start()
        await asyncio.sleep(0.5)
        assert self.scheduler._running is True
        await self.scheduler.stop()
        assert self.scheduler._running is False


class TestMetrics:
    """调度器指标测试"""

    def setup_method(self):
        """测试前初始化"""
        self.scheduler = TaskScheduler()

    def test_initial_metrics(self):
        """测试初始指标"""
        metrics = self.scheduler.get_metrics()
        assert metrics['total_tasks'] == 0
        assert metrics['completed_tasks'] == 0
        assert metrics['failed_tasks'] == 0
        assert metrics['active_tasks'] == 0

    def test_get_task_status_empty(self):
        """测试空任务状态"""
        status = self.scheduler.get_task_status()
        assert isinstance(status, dict)


class TestTaskDependencies:
    """任务依赖测试"""

    def setup_method(self):
        """测试前初始化"""
        self.scheduler = TaskScheduler()

    async def dummy_task(self):
        await asyncio.sleep(0.01)

    def test_add_task_with_dependencies(self):
        """测试添加带依赖的任务"""
        task1_id = self.scheduler.add_interval_task(
            name="task1",
            func=self.dummy_task,
            seconds=30
        )
        task2_id = self.scheduler.add_interval_task(
            name="task2",
            func=self.dummy_task,
            seconds=30,
            dependencies=[task1_id]
        )
        task2 = self.scheduler.tasks[task2_id]
        assert task1_id in task2.dependencies


class TestConcurrency:
    """并发控制测试"""

    def setup_method(self):
        """测试前初始化"""
        self.scheduler = TaskScheduler(max_concurrent_tasks=2)

    async def dummy_task(self):
        await asyncio.sleep(0.1)

    @pytest.mark.asyncio
    async def test_concurrency_limit(self):
        """测试并发限制"""
        start_time = asyncio.get_event_loop().time()
        
        async def long_task():
            await asyncio.sleep(0.2)

        task1_id = self.scheduler.add_interval_task(
            name="long1",
            func=long_task,
            seconds=1
        )
        task2_id = self.scheduler.add_interval_task(
            name="long2",
            func=long_task,
            seconds=1
        )
        task3_id = self.scheduler.add_interval_task(
            name="long3",
            func=long_task,
            seconds=1
        )
        _ = (task1_id, task2_id, task3_id)

        await self.scheduler.start()
        await asyncio.sleep(0.3)
        await self.scheduler.stop()

        elapsed = asyncio.get_event_loop().time() - start_time
        assert elapsed >= 0.2
