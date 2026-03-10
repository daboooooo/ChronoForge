import pytest
from unittest.mock import Mock, patch
from chronoforge.task_manager_ultimate import Task, UltimateTaskManager, TASK_TYPES
from chronoforge.utils import TimeSlot, TimeRange


class TestTask:
    """测试Task类"""

    def test_task_initialization_periodic(self):
        """测试周期性任务初始化"""
        task = Task(
            name="test_task",
            data_source_name="test_ds",
            storage_name="test_storage",
            period=3600,
            symbols=["BTC/USDT", "ETH/USDT"],
            timeframe="1h"
        )
        
        assert task.name == "test_task"
        assert task.data_source_name == "test_ds"
        assert task.storage_name == "test_storage"
        assert task.symbols == ["BTC/USDT", "ETH/USDT"]
        assert task.timeframe == "1h"
        assert task.task_type == TASK_TYPES['PERIODIC']
        assert task.data_source_config == {}
        assert task.storage_config == {}
        assert not task.is_auto_created
        assert task.status == "idle"
        assert task.run_count == 0

    def test_task_initialization_time_slot(self):
        """测试时间槽任务初始化"""
        time_slot = Mock(spec=TimeSlot)
        task = Task(
            name="test_task",
            data_source_name="test_ds",
            storage_name="test_storage",
            time_slot=time_slot,
            symbols=["BTC/USDT"],
            timeframe="1d"
        )
        
        assert task.name == "test_task"
        assert task.data_source_name == "test_ds"
        assert task.storage_name == "test_storage"
        assert task.time_slot == time_slot
        assert task.symbols == ["BTC/USDT"]
        assert task.timeframe == "1d"
        assert task.task_type == TASK_TYPES['TIME_SLOT']

    def test_task_with_configs(self):
        """测试带配置的任务初始化"""
        data_source_config = {"api_key": "test_key"}
        storage_config = {"db_path": "/tmp/test.db"}
        
        task = Task(
            name="test_task",
            data_source_name="test_ds",
            storage_name="test_storage",
            period=60,
            data_source_config=data_source_config,
            storage_config=storage_config,
            is_auto_created=True
        )
        
        assert task.data_source_config == data_source_config
        assert task.storage_config == storage_config
        assert task.is_auto_created

    def test_task_with_timerange(self):
        """测试带时间范围的任务初始化"""
        timerange = Mock(spec=TimeRange)
        task = Task(
            name="test_task",
            data_source_name="test_ds",
            storage_name="test_storage",
            period=300,
            timerange=timerange
        )
        
        assert task.timerange == timerange

    def test_task_get_state(self):
        """测试获取任务状态"""
        task = Task(
            name="test_task",
            data_source_name="test_ds",
            storage_name="test_storage",
            period=3600
        )
        
        state = task.get_state()
        assert isinstance(state, dict)
        assert state['name'] == "test_task"
        assert state['status'] == "idle"
        assert state['run_count'] == 0

    def test_task_repr(self):
        """测试任务的字符串表示"""
        task = Task(
            name="test_task",
            data_source_name="test_ds",
            storage_name="test_storage",
            period=3600
        )
        
        repr_str = repr(task)
        assert "test_task" in repr_str
        assert "test_ds" in repr_str
        assert "test_storage" in repr_str


class TestUltimateTaskManager:
    """测试UltimateTaskManager类"""
    
    @pytest.fixture
    def task_manager(self):
        """创建UltimateTaskManager实例"""
        from chronoforge.task_manager_ultimate import UltimateTaskManager
        return UltimateTaskManager()

    def test_task_manager_initialization(self, task_manager):
        """测试UltimateTaskManager初始化"""
        assert task_manager is not None
        assert hasattr(task_manager, 'tasks')
        assert isinstance(task_manager.tasks, dict)

    def test_add_task(self, task_manager):
        """测试添加任务"""
        task = Task(
            name="test_task",
            data_source_name="test_ds",
            storage_name="test_storage",
            period=3600
        )
        
        task_manager.add_task("test_task", task)
        assert "test_task" in task_manager.tasks

    def test_delete_task(self, task_manager):
        """测试删除任务"""
        task = Task(
            name="test_task",
            data_source_name="test_ds",
            storage_name="test_storage",
            period=3600
        )
        task_manager.add_task("test_task", task)
        
        task_manager.delete_task("test_task")
        assert "test_task" not in task_manager.tasks

    def test_delete_nonexistent_task(self, task_manager):
        """测试删除不存在的任务"""
        # 应该不会抛出异常
        task_manager.delete_task("nonexistent")

    def test_get_task(self, task_manager):
        """测试获取任务"""
        task = Task(
            name="test_task",
            data_source_name="test_ds",
            storage_name="test_storage",
            period=3600
        )
        task_manager.add_task("test_task", task)
        
        retrieved_task = task_manager.get_task("test_task")
        assert retrieved_task == task

    def test_get_nonexistent_task(self, task_manager):
        """测试获取不存在的任务"""
        retrieved_task = task_manager.get_task("nonexistent")
        assert retrieved_task is None

    def test_get_all_tasks(self, task_manager):
        """测试获取所有任务"""
        task1 = Task(
            name="task1",
            data_source_name="test_ds",
            storage_name="test_storage",
            period=3600
        )
        task2 = Task(
            name="task2",
            data_source_name="test_ds",
            storage_name="test_storage",
            period=7200
        )
        task_manager.add_task("task1", task1)
        task_manager.add_task("task2", task2)
        
        tasks = task_manager.get_all_tasks()
        assert len(tasks) == 2
        assert "task1" in tasks
        assert "task2" in tasks


if __name__ == "__main__":
    pytest.main([__file__])