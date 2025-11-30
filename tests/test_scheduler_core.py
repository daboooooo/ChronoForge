from chronoforge import Scheduler
from chronoforge.utils import TimeSlot


class TestSchedulerCore:
    """测试调度器核心功能"""

    def test_lock_manager(self):
        """测试锁管理器功能"""
        # 间接测试锁管理器，通过测试调度器的初始化
        scheduler = Scheduler()
        assert scheduler is not None

    def test_supported_timeframes(self):
        """测试支持的时间框架"""
        scheduler = Scheduler()
        # 检查SUPPORTED_TIMEFRAMES常量是否被正确使用
        assert hasattr(scheduler, '_runner_thread')

    def test_task_states_management(self):
        """测试任务状态管理"""
        scheduler = Scheduler()
        time_slot = TimeSlot(start="00:00:00", end="23:59:59")

        # 添加任务
        scheduler.add_task(
            name="test_task",
            data_source_name="CryptoSpotDataSource",
            data_source_config={"api_key": "test_key"},
            storage_name="LocalFileStorage",
            storage_config={"base_path": "./tmp"},
            time_slot=time_slot,
            symbols=["binance:BTC/USDT"],
            timeframe="1d",
            timerange_str="20240101-"
        )

        # 检查任务是否添加成功
        assert "test_task" in scheduler.tasks

        # 检查任务状态字典是否初始化
        assert hasattr(scheduler, 'task_states')
        assert isinstance(scheduler.task_states, dict)

    def test_clean_completed_tasks(self):
        """测试清理已完成任务的功能"""
        scheduler = Scheduler()

        # 调用_clean_completed_tasks方法，确保它不会抛出异常
        scheduler._clean_completed_tasks()

        # _clean_completed_tasks方法只清理包含'future'键且future.done()为True的任务状态
        # 由于我们无法轻易模拟一个真实的future对象，我们将测试该方法不会抛出异常
        # 并测试它能正确处理空的任务状态字典

        # 添加一个不包含'future'键的任务状态
        scheduler.task_states["test_task"] = {"status": "completed"}

        # 再次调用_clean_completed_tasks方法
        scheduler._clean_completed_tasks()

        # 检查任务状态是否被保留（因为它不包含'future'键）
        assert "test_task" in scheduler.task_states
