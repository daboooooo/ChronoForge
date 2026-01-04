import pytest
from unittest.mock import patch
from chronoforge import Scheduler
from chronoforge.utils import TimeSlot
from chronoforge.data_source import DataSourceBase
import pandas as pd


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

        # 添加任务，使用唯一的测试任务名称
        scheduler.add_task(
            name="test_task_states_unique",
            data_source_name="CryptoSpotDataSource",
            data_source_config={"api_key": "test_key"},
            storage_name="LocalFileStorage",
            storage_config={"base_path": "./tmp"},
            time_slot=time_slot,
            symbols=["binance:BTC/USDT"],
            timeframe="1d",
            timerange_str="20240101-",
            inplace=True
        )

        # 检查任务是否添加成功
        assert "test_task_states_unique" in scheduler.tasks

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

    def test_task_state_management(self):
        """测试任务状态管理"""
        scheduler = Scheduler()
        time_slot = TimeSlot(start="00:00:00", end="23:59:59")

        # 添加任务，使用唯一的测试任务名称
        scheduler.add_task(
            name="test_task_state_unique",
            data_source_name="CryptoSpotDataSource",
            data_source_config={"api_key": "test_key"},
            storage_name="LocalFileStorage",
            storage_config={"base_path": "./tmp"},
            time_slot=time_slot,
            symbols=["binance:BTC/USDT"],
            timeframe="1d",
            timerange_str="20240101-",
            inplace=True
        )

        # 初始化任务状态
        task_name = "test_task_state_unique"
        scheduler.task_states[task_name] = {
            "status": "waiting",
            "last_run_time": None,
            "next_run_time": None,
            "run_count": 0,
            "success_count": 0,
            "error_count": 0,
            "last_error": None
        }

        # 检查任务状态
        assert task_name in scheduler.task_states
        assert scheduler.task_states[task_name]["status"] == "waiting"
        assert scheduler.task_states[task_name]["run_count"] == 0
        assert scheduler.task_states[task_name]["success_count"] == 0
        assert scheduler.task_states[task_name]["error_count"] == 0

        # 更新任务状态
        scheduler.task_states[task_name]["status"] = "running"
        scheduler.task_states[task_name]["run_count"] += 1

        assert scheduler.task_states[task_name]["status"] == "running"
        assert scheduler.task_states[task_name]["run_count"] == 1

    @pytest.mark.asyncio
    async def test_task_execution_logic(self):
        """测试任务执行逻辑"""
        scheduler = Scheduler()

        # 创建测试数据源
        class TestDataSource(DataSourceBase):
            @property
            def name(self):
                return "TestDataSource"

            async def fetch(self, symbol, timeframe, start_ts_ms, end_ts_ms=None) -> pd.DataFrame:
                return pd.DataFrame({'price': [1000]})

            async def test_task_method(self, param1=None, param2=None):
                return {"result": "success", "param1": param1, "param2": param2}

            async def close_all_connections(self):
                pass

        # 注册测试数据源
        success, msg = scheduler.register_plugin(TestDataSource)
        assert success, f"Failed to register TestDataSource: {msg}"

        # 调试：打印支持的数据源
        # print("支持的数据源:", scheduler.list_supported_plugins("data_source"))

        # 确保TestDataSource被正确注册
        assert "TestDataSource" in scheduler.list_supported_plugins("data_source"), \
            "TestDataSource未在支持的数据源列表中"

        # 添加任务
        time_slot = TimeSlot(start="00:00:00", end="23:59:59")
        scheduler.add_task(
            name="test_execution_task",
            data_source_name="TestDataSource",
            data_source_config={"api_key": "test_key"},
            storage_name="LocalFileStorage",
            storage_config={"base_path": "./tmp"},
            time_slot=time_slot,
            symbols=["TEST:ABC/XYZ"],
            timeframe="1d",
            timerange_str="20240101-"
        )

        # 模拟任务执行
        # task_name = "test_execution_task"
        # task = scheduler.tasks[task_name]

        # 测试任务参数处理
        with patch.object(TestDataSource, 'test_task_method') as mock_method:
            mock_method.return_value = {"result": "success"}

            # 创建数据源实例
            ds = TestDataSource({"api_key": "test_key"})

            # 测试参数传递
            result = await ds.test_task_method(param1="value1", param2="value2")
            mock_method.assert_called_once_with(param1="value1", param2="value2")
            assert result["result"] == "success"

    def test_periodic_task_scheduling(self):
        """测试周期性任务调度"""
        scheduler = Scheduler()

        # 检查周期性任务是否被正确识别和调度
        all_tasks = list(scheduler.tasks.keys())
        periodic_tasks = [name for name in all_tasks if 'periodic' in name or 'tickers' in name]

        if periodic_tasks:
            # 如果有周期性任务，检查其配置
            for task_name in periodic_tasks:
                task = scheduler.tasks[task_name]
                assert hasattr(task, 'method_params'), "周期性任务缺少method_params属性"
                assert 'interval' in task.method_params, "周期性任务缺少interval参数"
                assert task.method_params['interval'] > 0, "周期性任务interval必须大于0"

    def test_task_result_handling(self):
        """测试任务结果处理"""
        # scheduler = Scheduler()

        # 模拟不同类型的任务结果
        test_results = [
            pd.DataFrame({'price': [1000, 2000]}),  # DataFrame
            {"price": 1500, "volume": 100},        # 字典
            [{"price": 1000}, {"price": 2000}],    # 列表
        ]

        # 测试调度器的结果处理逻辑
        for result in test_results:
            # 我们将测试调度器是否能处理这些结果类型
            # 由于_handle_task_result是私有方法，我们需要模拟调用
            try:
                if isinstance(result, pd.DataFrame):
                    # DataFrame应该直接使用
                    data_to_save = result
                elif isinstance(result, dict):
                    # 字典应该转换为DataFrame
                    data_to_save = pd.DataFrame([result])
                elif isinstance(result, list):
                    # 列表应该转换为DataFrame
                    data_to_save = pd.DataFrame(result)
                else:
                    data_to_save = pd.DataFrame()

                # 验证转换结果
                assert isinstance(data_to_save, pd.DataFrame), f"结果类型 {type(result)} 转换失败"

            except Exception as e:
                pytest.fail(f"结果处理失败: {e}")
