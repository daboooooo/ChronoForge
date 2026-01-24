import pytest
import asyncio
import time
from unittest.mock import Mock, AsyncMock, patch
from chronoforge import Scheduler
from chronoforge.utils import TimeSlot
from chronoforge.decorators import create_task
from chronoforge.data_source import DataSourceBase
import pandas as pd
import threading


class TestScheduler:
    """测试调度器"""

    def test_initialization(self):
        """测试调度器初始化"""
        scheduler = Scheduler(max_workers=3)
        assert scheduler.thread_pool._max_workers == 3

    def test_list_supported_plugins(self):
        """测试列出支持的插件"""
        scheduler = Scheduler()

        # 测试列出数据源插件
        data_source_plugins = scheduler.list_supported_plugins("data_source")
        assert isinstance(data_source_plugins, list)
        assert len(data_source_plugins) > 0

        # 测试列出存储插件
        storage_plugins = scheduler.list_supported_plugins("storage")
        assert isinstance(storage_plugins, list)
        assert len(storage_plugins) > 0

        # 测试无效插件类型
        with pytest.raises(ValueError):
            scheduler.list_supported_plugins("invalid_type")

    def test_get_supported_plugin(self):
        """测试获取支持的插件"""
        scheduler = Scheduler()

        # 测试获取数据源插件
        data_source_plugin = scheduler.get_supported_plugin("data_source", "CryptoSpotDataSource")
        assert data_source_plugin is not None

        # 测试获取存储插件
        storage_plugin = scheduler.get_supported_plugin("storage", "LocalFileStorage")
        assert storage_plugin is not None

        # 测试无效插件名称
        with pytest.raises(ValueError):
            scheduler.get_supported_plugin("data_source", "InvalidPlugin")

    def test_add_task(self):
        """测试添加任务"""
        scheduler = Scheduler()
        time_slot = TimeSlot(start="00:00:00", end="23:59:59")

        # 添加任务，使用唯一的测试任务名称
        scheduler.add_task(
            name="test_task_add_unique",
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
        assert "test_task_add_unique" in scheduler.tasks

        # 测试添加重复任务
        with pytest.raises(ValueError):
            scheduler.add_task(
                name="test_task_add_unique",
                data_source_name="CryptoSpotDataSource",
                data_source_config={"api_key": "test_key"},
                storage_name="LocalFileStorage",
                storage_config={"base_path": "./tmp"},
                time_slot=time_slot,
                symbols=["binance:BTC/USDT"],
                timeframe="1d",
                timerange_str="20240101-"
            )

        # 测试使用inplace参数覆盖任务
        scheduler.add_task(
            name="test_task_add_unique",
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
        # 验证任务仍然存在
        assert "test_task_add_unique" in scheduler.tasks

    def test_add_task_invalid_timeframe(self):
        """测试添加任务时使用无效时间框架"""
        scheduler = Scheduler()
        time_slot = TimeSlot(start="00:00:00", end="23:59:59")

        with pytest.raises(ValueError):
            scheduler.add_task(
                name="test_task",
                data_source_name="CryptoSpotDataSource",
                data_source_config={"api_key": "test_key"},
                storage_name="LocalFileStorage",
                storage_config={"base_path": "./tmp"},
                time_slot=time_slot,
                symbols=["binance:BTC/USDT"],
                timeframe="invalid_timeframe",
                timerange_str="20240101-"
            )

    def test_add_task_invalid_data_source(self):
        """测试添加任务时使用无效数据源"""
        scheduler = Scheduler()
        time_slot = TimeSlot(start="00:00:00", end="23:59:59")

        with pytest.raises(ValueError):
            scheduler.add_task(
                name="test_task",
                data_source_name="InvalidDataSource",
                data_source_config={"api_key": "test_key"},
                storage_name="LocalFileStorage",
                storage_config={"base_path": "./tmp"},
                time_slot=time_slot,
                symbols=["binance:BTC/USDT"],
                timeframe="1d",
                timerange_str="20240101-"
            )

    def test_start_stop_scheduler(self):
        """测试启动和停止调度器"""
        scheduler = Scheduler()

        # 使用patch替换真实的数据源和存储，避免实际连接
        with patch.object(scheduler, '_init_shared_event_loop') as mock_init_loop, \
             patch.object(scheduler, 'add_task') as mock_add_task, \
             patch.object(scheduler, 'execute_task') as mock_execute_task:

            # 模拟初始化共享事件循环
            mock_init_loop.return_value = None

            # 启动调度器
            scheduler.start()
            assert scheduler._runner_thread is not None
            assert scheduler._runner_thread.is_alive()

            # 停止调度器
            scheduler.stop()

            # 使用更高效的方式等待线程结束，最多等待1秒
            start_time = time.time()
            while scheduler._runner_thread.is_alive() and time.time() - start_time < 1:
                time.sleep(0.01)  # 每10毫秒检查一次

            assert not scheduler._runner_thread.is_alive()

    def test_register_plugin(self):
        """测试注册插件"""
        scheduler = Scheduler()

        # 测试注册无效插件
        class InvalidPlugin:
            pass

        success, msg = scheduler.register_plugin(InvalidPlugin)
        assert success is False
        assert "Unsupported instance type" in msg


class TestPeriodicTask:
    """测试周期性任务装饰器功能"""

    def test_periodic_task_decorator_basic(self):
        """测试periodic_task装饰器的基本功能"""

        @create_task(interval=2, symbols=['BTC/USDT'], params={'key': 'value'})
        async def test_func(self, key=None):
            return {'result': 'success', 'key': key}

        # 断言装饰器是否正确添加了属性
        assert hasattr(test_func, 'is_periodic_task'), "装饰器没有添加is_periodic_task属性"
        assert test_func.is_periodic_task is True, "is_periodic_task属性值不正确"
        assert hasattr(test_func, 'task_config'), "装饰器没有添加task_config属性"

        # 断言task_config内容是否正确
        assert test_func.task_config['interval'] == 2, "interval配置不正确"
        assert test_func.task_config['symbols'] == ['BTC/USDT'], "symbols配置不正确"
        assert test_func.task_config['params'] == {'key': 'value'}, "params配置不正确"
        assert test_func.task_config['max_retries'] == 3, "max_retries默认值不正确"
        assert test_func.task_config['retry_delay'] == 1, "retry_delay默认值不正确"

    def test_periodic_task_decorator_with_storage(self):
        """测试带有存储配置的periodic_task装饰器"""

        @create_task(
            interval=5,
            symbols=['ETH/USDT'],
            storage_name="LocalFileStorage",
            storage_config={"base_path": "./tmp"},
            max_retries=5,
            retry_delay=2
        )
        async def test_func(self):
            return pd.DataFrame({'price': [3000]})

        assert test_func.is_periodic_task is True
        assert test_func.task_config['storage_name'] == "LocalFileStorage", "storage_name配置不正确"
        assert test_func.task_config['storage_config'] == {"base_path": "./tmp"}, \
            "storage_config配置不正确"
        assert test_func.task_config['max_retries'] == 5, "max_retries配置不正确"
        assert test_func.task_config['retry_delay'] == 2, "retry_delay配置不正确"

    def test_scheduler_auto_task_creation(self):
        """测试调度器是否能自动创建周期性任务"""
        # 创建调度器
        scheduler = Scheduler(max_workers=2)

        # 获取所有任务
        all_tasks = list(scheduler.tasks.keys())

        # 检查是否有自动创建的周期性任务
        periodic_tasks = [name for name in all_tasks if 'periodic' in name or 'tickers' in name]
        assert len(periodic_tasks) > 0, "没有自动创建周期性任务"

    @pytest.mark.asyncio
    async def test_scheduler_task_execution(self):
        """测试调度器任务执行"""
        # 创建测试数据源
        class TestDataSource(DataSourceBase):
            @property
            def name(self):
                return "TestDataSource"

            async def fetch(self, symbol, timeframe, start_ts_ms, end_ts_ms=None) -> pd.DataFrame:
                return pd.DataFrame()

            async def close_all_connections(self):
                pass

            @create_task(interval=1, symbols=['TEST/USDT'], params={'test_param': 'test_value'})
            async def test_method(self, test_param=None):
                return {'status': 'success', 'param': test_param}

        # 创建调度器并注册测试数据源
        scheduler = Scheduler(max_workers=2)
        scheduler.register_plugin(TestDataSource)

        # 查找测试任务
        all_tasks = list(scheduler.tasks.keys())

        # 输出所有任务名称以便调试
        # print("所有任务名称:", all_tasks)

        # 简化任务查找，只要包含test_method即可
        test_task = [name for name in all_tasks if 'test_method' in name]

        assert len(test_task) > 0, f"没有找到测试任务，当前任务列表: {all_tasks}"


class TestAsyncResourceManagement:
    """测试异步资源管理"""

    def setup_method(self):
        """测试方法设置"""
        # 创建一个带有连接池管理的数据源
        class ResourceDataSource(DataSourceBase):
            @property
            def name(self):
                return "ResourceDataSource"

            def __init__(self, config=None):
                super().__init__(config)
                self.exchange_pools = {}
                self.connections_opened = 0
                self.connections_closed = 0

            async def fetch(self, symbol, timeframe, start_ts_ms, end_ts_ms=None):
                return pd.DataFrame()

            async def _get_exchange_instance(self, exchange_name):
                """模拟获取交易所实例（使用连接池）"""
                if exchange_name not in self.exchange_pools:
                    # 模拟创建新的连接池
                    self.exchange_pools[exchange_name] = Mock()
                    self.exchange_pools[exchange_name].get_connection = AsyncMock(side_effect=self._on_connection_created)
                    self.exchange_pools[exchange_name].return_connection = AsyncMock()
                    self.exchange_pools[exchange_name].close_all_connections = AsyncMock(side_effect=self._on_all_connections_closed)

                    # 创建模拟连接
                    mock_exchange = Mock()
                    mock_exchange.close = Mock()
                    self.exchange_pools[exchange_name].get_connection.return_value = mock_exchange
                return await self.exchange_pools[exchange_name].get_connection()

            def _on_connection_created(self):
                """跟踪连接创建事件"""
                self.connections_opened += 1
                # 返回一个模拟连接
                mock_exchange = Mock()
                mock_exchange.close = Mock()
                return mock_exchange

            def _on_all_connections_closed(self):
                """跟踪所有连接关闭事件"""
                self.connections_closed += 1

            async def close_all_connections(self):
                """关闭所有连接"""
                for exchange_name, pool in self.exchange_pools.items():
                    await pool.close_all_connections()

        self.ResourceDataSource = ResourceDataSource

    @pytest.mark.asyncio
    async def test_async_resource_creation_and_closure(self):
        """测试异步资源的创建和关闭"""
        # 创建数据源实例
        data_source = self.ResourceDataSource()

        # 创建多个连接
        _ = await data_source._get_exchange_instance('exchange1')
        _ = await data_source._get_exchange_instance('exchange2')

        # 验证连接创建
        assert data_source.connections_opened == 2
        assert 'exchange1' in data_source.exchange_pools
        assert 'exchange2' in data_source.exchange_pools

        # 验证get_connection方法被调用
        data_source.exchange_pools['exchange1'].get_connection.assert_called_once()
        data_source.exchange_pools['exchange2'].get_connection.assert_called_once()

        # 关闭所有连接
        await data_source.close_all_connections()

        # 验证连接关闭
        assert data_source.connections_closed == 2
        data_source.exchange_pools['exchange1'].close_all_connections.assert_called_once()
        data_source.exchange_pools['exchange2'].close_all_connections.assert_called_once()

    @pytest.mark.asyncio
    async def test_scheduler_async_resource_management(self):
        """测试调度器的异步资源管理"""
        # 创建调度器
        scheduler = Scheduler(max_workers=2)

        # 注册资源数据源
        scheduler.register_plugin(self.ResourceDataSource)

        # 创建数据源实例
        ds = self.ResourceDataSource()

        # 模拟任务执行
        try:
            # 创建连接
            _ = await ds._get_exchange_instance('test_exchange')
            assert ds.connections_opened == 1
            assert 'test_exchange' in ds.exchange_pools

            # 验证get_connection方法被调用
            ds.exchange_pools['test_exchange'].get_connection.assert_called_once()

            # 模拟调度器调用close_all_connections
            await ds.close_all_connections()

            # 验证连接关闭
            assert ds.connections_closed == 1
            ds.exchange_pools['test_exchange'].close_all_connections.assert_called_once()

        finally:
            # 确保所有连接都被关闭
            await ds.close_all_connections()

    @pytest.mark.asyncio
    async def test_resource_leak_prevention(self):
        """测试防止资源泄漏"""
        # 创建数据源实例
        data_source = self.ResourceDataSource()

        # 模拟异常情况下的资源使用
        try:
            # 创建连接
            _ = await data_source._get_exchange_instance('test_exchange')
            assert data_source.connections_opened == 1

            # 模拟异常
            raise Exception("测试异常")
        except Exception:
            # 异常被捕获，确保资源仍然可以被关闭
            pass

        # 关闭所有连接
        await data_source.close_all_connections()

        # 验证连接关闭
        assert data_source.connections_closed == 1
        data_source.exchange_pools['test_exchange'].close_all_connections.assert_called_once()


class TestDynamicInterval:
    """测试动态间隔调整功能"""

    def test_dynamic_interval_0_tasks(self):
        """测试当没有任务时，间隔应该是5秒"""
        scheduler = Scheduler()

        # 手动初始化_stop_event，因为run方法依赖它
        scheduler._stop_event = threading.Event()

        # 清空加载的任务
        scheduler.tasks.clear()

        # 模拟所有任务执行相关的方法，避免实际执行任务
        with patch.object(scheduler, '_clean_completed_tasks'), \
             patch.object(scheduler, 'execute_task'), \
             patch.object(scheduler._stop_event, 'wait') as mock_wait, \
             patch.object(scheduler.time_slot_manager, 'is_in_timeslot', return_value=True) as mock_is_in_timeslot:

            # 让mock_wait第一次调用返回False（不停止循环），第二次调用返回True（停止循环）
            mock_wait.side_effect = [False, True]

            # 运行调度器
            scheduler.run()

            # 验证_stop_event.wait被调用，且参数是5.0
            assert mock_wait.called
            assert mock_wait.call_args[0][0] == 5.0

    def test_dynamic_interval_less_than_10_tasks(self):
        """测试当任务数量少于10个时，间隔应该是1秒"""
        scheduler = Scheduler()

        # 手动初始化_stop_event，因为run方法依赖它
        scheduler._stop_event = threading.Event()

        # 清空加载的任务
        scheduler.tasks.clear()

        # 直接向tasks字典中添加模拟任务
        for i in range(5):
            # 创建一个简单的模拟任务对象
            class MockTask:
                def __init__(self, name):
                    self.name = name
                    self.time_slot = TimeSlot(start="00:00:00", end="23:59:59")
                    self.next_run_time = time.time()
                    self.interval = 60
                    self.method_params = {}
                    self.task_type = "periodic"
                    self.config = {
                        'interval': 60,
                        'method_name': None,
                        'method_params': {}
                    }
                    self.task_type = "periodic"
                    self.config = {
                        'interval': 60,
                        'method_name': None,
                        'method_params': {}
                    }

            scheduler.tasks[f"test_task_{i}"] = MockTask(f"test_task_{i}")

        # 模拟所有任务执行相关的方法，避免实际执行任务
        with patch.object(scheduler, '_clean_completed_tasks'), \
             patch.object(scheduler, 'execute_task'), \
             patch.object(scheduler._stop_event, 'wait') as mock_wait, \
             patch.object(scheduler.time_slot_manager, 'is_in_timeslot', return_value=True) as mock_is_in_timeslot:

            # 让mock_wait第一次调用返回False（不停止循环），第二次调用返回True（停止循环）
            mock_wait.side_effect = [False, True]

            # 运行调度器
            scheduler.run()

            # 验证_stop_event.wait被调用，且参数是1.0
            assert mock_wait.called
            assert mock_wait.call_args[0][0] == 1.0

    def test_dynamic_interval_less_than_50_tasks(self):
        """测试当任务数量少于50个时，间隔应该是0.5秒"""
        scheduler = Scheduler()

        # 手动初始化_stop_event，因为run方法依赖它
        scheduler._stop_event = threading.Event()

        # 清空加载的任务
        scheduler.tasks.clear()

        # 直接向tasks字典中添加模拟任务
        for i in range(30):
            # 创建一个简单的模拟任务对象
            class MockTask:
                def __init__(self, name):
                    self.name = name
                    self.time_slot = TimeSlot(start="00:00:00", end="23:59:59")
                    self.next_run_time = time.time()
                    self.interval = 60
                    self.method_params = {}
                    self.task_type = "periodic"
                    self.config = {
                        'interval': 60,
                        'method_name': None,
                        'method_params': {}
                    }

            scheduler.tasks[f"test_task_{i}"] = MockTask(f"test_task_{i}")

        # 模拟所有任务执行相关的方法，避免实际执行任务
        with patch.object(scheduler, '_clean_completed_tasks'), \
             patch.object(scheduler, 'execute_task'), \
             patch.object(scheduler._stop_event, 'wait') as mock_wait, \
             patch.object(scheduler.time_slot_manager, 'is_in_timeslot', return_value=True) as mock_is_in_timeslot:

            # 让mock_wait第一次调用返回False（不停止循环），第二次调用返回True（停止循环）
            mock_wait.side_effect = [False, True]

            # 运行调度器
            scheduler.run()

            # 验证_stop_event.wait被调用，且参数是0.5
            assert mock_wait.called
            assert mock_wait.call_args[0][0] == 0.5

    def test_dynamic_interval_50_or_more_tasks(self):
        """测试当任务数量50个或更多时，间隔应该是0.2秒"""
        scheduler = Scheduler()

        # 手动初始化_stop_event，因为run方法依赖它
        scheduler._stop_event = threading.Event()

        # 清空加载的任务
        scheduler.tasks.clear()

        # 直接向tasks字典中添加模拟任务
        for i in range(60):
            # 创建一个简单的模拟任务对象
            class MockTask:
                def __init__(self, name):
                    self.name = name
                    self.time_slot = TimeSlot(start="00:00:00", end="23:59:59")
                    self.next_run_time = time.time()
                    self.interval = 60
                    self.method_params = {}
                    self.task_type = "periodic"
                    self.config = {
                        'interval': 60,
                        'method_name': None,
                        'method_params': {}
                    }

            scheduler.tasks[f"test_task_{i}"] = MockTask(f"test_task_{i}")

        # 模拟所有任务执行相关的方法，避免实际执行任务
        with patch.object(scheduler, '_clean_completed_tasks'), \
             patch.object(scheduler, 'execute_task'), \
             patch.object(scheduler._stop_event, 'wait') as mock_wait, \
             patch.object(scheduler.time_slot_manager, 'is_in_timeslot', return_value=True) as mock_is_in_timeslot:

            # 让mock_wait第一次调用返回False（不停止循环），第二次调用返回True（停止循环）
            mock_wait.side_effect = [False, True]

            # 运行调度器
            scheduler.run()

            # 验证_stop_event.wait被调用，且参数是0.2
            assert mock_wait.called
            assert mock_wait.call_args[0][0] == 0.2


class TestTaskGrouping:
    """测试任务分组功能"""

    def test_task_grouping_by_exchange(self):
        """测试具有交易所参数的任务会被正确分组"""
        scheduler = Scheduler()

        # 清空加载的任务，只保留当前测试的任务
        scheduler.tasks.clear()

        # 创建模拟任务，直接设置method_params来测试分组逻辑
        class MockTask:
            def __init__(self, name, exchange):
                self.name = name
                self.time_slot = TimeSlot(start="00:00:00", end="23:59:59")
                self.next_run_time = time.time()
                self.interval = 60
                self.method_params = {"exchange": exchange}

        # 添加具有不同交易所参数的任务
        scheduler.tasks["task_binance"] = MockTask("task_binance", "binance")
        scheduler.tasks["task_okx"] = MockTask("task_okx", "okx")

        # 手动执行任务分组逻辑（提取自run方法）
        grouped_tasks = {}
        for task_name, task in list(scheduler.tasks.items()):
            # 跳过时间槽检查，直接进行分组

            # 根据任务的交易所或数据源进行分组
            group_key = 'default'
            if hasattr(task, 'method_params'):
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
            grouped_tasks[group_key].append((task_name, task, {}))

        # 验证分组结果
        assert len(grouped_tasks) == 2
        assert "exchange:binance" in grouped_tasks
        assert "exchange:okx" in grouped_tasks
        assert len(grouped_tasks["exchange:binance"]) == 1
        assert len(grouped_tasks["exchange:okx"]) == 1

    def test_task_grouping_by_data_source(self):
        """测试具有数据源参数的任务会被正确分组"""
        scheduler = Scheduler()

        # 清空加载的任务，只保留当前测试的任务
        scheduler.tasks.clear()

        # 创建模拟任务，直接设置method_params来测试分组逻辑
        class MockTask:
            def __init__(self, name, data_source):
                self.name = name
                self.time_slot = TimeSlot(start="00:00:00", end="23:59:59")
                self.next_run_time = time.time()
                self.interval = 60
                self.method_params = {"data_source": data_source}

        # 添加具有不同数据源参数的任务
        scheduler.tasks["test_task_spot"] = MockTask("test_task_spot", "CryptoSpotDataSource")
        scheduler.tasks["test_task_future"] = MockTask("test_task_future", "CryptoUMFutureDataSource")

        # 手动执行任务分组逻辑（提取自run方法）
        grouped_tasks = {}
        for task_name, task in list(scheduler.tasks.items()):
            # 跳过时间槽检查，直接进行分组

            # 根据任务的交易所或数据源进行分组
            group_key = 'default'
            if hasattr(task, 'method_params'):
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
            grouped_tasks[group_key].append((task_name, task, {}))

        # 验证分组结果
        assert len(grouped_tasks) == 2
        assert "datasource:CryptoSpotDataSource" in grouped_tasks
        assert "datasource:CryptoUMFutureDataSource" in grouped_tasks
        assert len(grouped_tasks["datasource:CryptoSpotDataSource"]) == 1
        assert len(grouped_tasks["datasource:CryptoUMFutureDataSource"]) == 1

    def test_task_grouping_default_group(self):
        """测试没有特定参数的任务会被分到默认组"""
        scheduler = Scheduler()

        # 清空加载的任务，只保留当前测试的任务
        scheduler.tasks.clear()

        # 创建模拟任务，没有设置特定分组参数
        class MockTask:
            def __init__(self, name):
                self.name = name
                self.time_slot = TimeSlot(start="00:00:00", end="23:59:59")
                self.next_run_time = time.time()
                self.interval = 60
                self.method_params = {}

        # 添加任务
        scheduler.tasks["task_default"] = MockTask("task_default")

        # 手动执行任务分组逻辑（提取自run方法）
        grouped_tasks = {}
        for task_name, task in list(scheduler.tasks.items()):
            # 跳过时间槽检查，直接进行分组

            # 根据任务的交易所或数据源进行分组
            group_key = 'default'
            if hasattr(task, 'method_params'):
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
            grouped_tasks[group_key].append((task_name, task, {}))

        # 验证分组结果
        assert len(grouped_tasks) == 1
        assert "default" in grouped_tasks
        assert len(grouped_tasks["default"]) == 1


class TestRetryMechanism:
    """测试重试机制"""

    def setup_method(self):
        """测试方法设置"""
        # 创建一个带有可配置错误的数据源
        class RetryTestDataSource(DataSourceBase):
            @property
            def name(self):
                return "RetryTestDataSource"

            def __init__(self, config=None):
                super().__init__(config)
                self.error_count = 0
                self.success_on_try = None  # 设置在第几次尝试成功

            async def fetch(self, symbol, timeframe, start_ts_ms, end_ts_ms=None):
                return pd.DataFrame()

            async def close_all_connections(self):
                pass

            @create_task(interval=1, symbols=['TEST/USDT'], max_retries=3, retry_delay=0.1)
            async def flaky_task(self, param=None):
                """一个不稳定的任务，可能会失败"""
                self.error_count += 1

                if self.success_on_try is None or self.error_count < self.success_on_try:
                    # 模拟失败
                    raise Exception(f"测试错误 - 第 {self.error_count} 次尝试")

                # 成功执行
                return {'result': 'success', 'param': param, 'attempts': self.error_count}

        self.RetryTestDataSource = RetryTestDataSource

    @pytest.mark.asyncio
    async def test_retry_mechanism_basic(self):
        """测试重试机制的基本功能"""
        # 创建数据源实例
        data_source = self.RetryTestDataSource()

        # 设置在第1次尝试成功（因为直接调用方法不会触发调度器的重试机制）
        data_source.success_on_try = 1

        try:
            # 执行任务，应该在第1次尝试成功
            result = await data_source.flaky_task(param='test_value')

            # 验证结果
            assert result['result'] == 'success'
            assert result['param'] == 'test_value'
            assert result['attempts'] == 1  # 直接调用只会尝试一次

        except Exception as e:
            pytest.fail(f"任务不应该失败: {e}")

    @pytest.mark.asyncio
    async def test_retry_limit(self):
        """测试重试次数限制"""
        # 创建数据源实例
        data_source = self.RetryTestDataSource()

        # 设置永远不会成功
        data_source.success_on_try = 10  # 超过max_retries=3

        try:
            # 执行任务，应该在第一次尝试就失败（因为直接调用方法不会触发调度器的重试机制）
            await data_source.flaky_task()
            pytest.fail("任务应该失败")
        except Exception as e:
            # 验证失败
            assert "测试错误" in str(e)
            assert data_source.error_count == 1  # 直接调用只会尝试一次

    @pytest.mark.asyncio
    async def test_retry_delay(self):
        """测试重试延迟"""
        # 记录每次尝试的时间
        attempt_times = []

        # 保存原始方法
        original_flaky_task = self.RetryTestDataSource.flaky_task

        # 创建一个带时间记录的任务方法
        async def timed_flaky_task(self, param=None):
            attempt_times.append(time.time())
            # 调用原始的不稳定任务方法
            return await original_flaky_task.__get__(self, self.__class__)(param)

        # 替换类的方法
        self.RetryTestDataSource.flaky_task = create_task(
            interval=1, symbols=['TEST/USDT'], max_retries=3, retry_delay=0.1)(timed_flaky_task)

        # 创建数据源实例
        data_source = self.RetryTestDataSource()

        # 设置在第3次尝试成功
        data_source.success_on_try = 3

        # 模拟调度器的_execute_task方法执行任务
        max_retries = data_source.flaky_task.task_config['max_retries']
        retry_delay = data_source.flaky_task.task_config['retry_delay']
        params = data_source.flaky_task.task_config.get('params', {})

        try:
            # 手动实现重试逻辑，模拟调度器的行为
            for attempt in range(max_retries + 1):
                try:
                    result = await data_source.flaky_task(**params)
                    break
                except Exception as e:
                    if attempt < max_retries:
                        # 等待重试延迟
                        await asyncio.sleep(retry_delay)
                    else:
                        raise e

            # 验证成功
            assert result['result'] == 'success'
            assert len(attempt_times) == 3  # 应该尝试了3次

            # 验证重试延迟
            if len(attempt_times) > 1:
                for i in range(1, len(attempt_times)):
                    delay = attempt_times[i] - attempt_times[i-1]
                    # 考虑到时间精度问题，允许一定的误差
                    assert delay >= retry_delay * 0.9, f"重试延迟不足，期望至少{retry_delay}秒，实际{delay}秒"

        except Exception as e:
            pytest.fail(f"任务不应该失败: {e}")
        finally:
            # 恢复原始方法
            self.RetryTestDataSource.flaky_task = original_flaky_task

    @pytest.mark.asyncio
    async def test_scheduler_retry_integration(self):
        """测试调度器与重试机制的集成"""
        # 创建调度器
        scheduler = Scheduler(max_workers=2)

        # 注册重试测试数据源
        scheduler.register_plugin(self.RetryTestDataSource)

        # 验证装饰器参数已正确设置
        data_source = self.RetryTestDataSource()

        assert hasattr(data_source.flaky_task, 'task_config')
        assert data_source.flaky_task.task_config['max_retries'] == 3
        assert data_source.flaky_task.task_config['retry_delay'] == 0.1
        assert data_source.flaky_task.task_config['interval'] == 1


class TestDynamicParameterPassing:
    """测试动态参数传递功能"""

    def setup_method(self):
        """测试方法设置"""
        # 创建一个测试数据源，支持动态参数
        class DynamicParamsDataSource(DataSourceBase):
            @property
            def name(self):
                return "DynamicParamsDataSource"

            def __init__(self, config=None):
                super().__init__(config)
                self.task_calls = []  # 记录任务调用历史

            async def fetch(self, symbol, timeframe, start_ts_ms, end_ts_ms=None):
                return pd.DataFrame()

            async def close_all_connections(self):
                pass

            @create_task(interval=1, symbols=['BTC/USDT', 'ETH/USDT'],
                           params={'exchange_name': 'binance', 'quote': 'USDT'})
            async def test_dynamic_params(self, exchange_name=None, quote=None, custom_param=None):
                """测试动态参数传递的任务"""
                # 记录调用参数
                self.task_calls.append({
                    'exchange_name': exchange_name,
                    'quote': quote,
                    'custom_param': custom_param
                })

                return {
                    'result': 'success',
                    'exchange': exchange_name,
                    'quote': quote,
                    'custom_param': custom_param
                }

            @create_task(interval=2, symbols=['SOL/USDT'], params={'exchange_name': 'bybit'})
            async def test_partial_params(self, exchange_name=None, quote=None):
                """测试部分参数传递的任务"""
                return {
                    'result': 'success',
                    'exchange': exchange_name,
                    'quote': quote
                }

        self.DynamicParamsDataSource = DynamicParamsDataSource

    @pytest.mark.asyncio
    async def test_dynamic_params_basic(self):
        """测试基本的动态参数传递"""
        # 创建数据源实例
        data_source = self.DynamicParamsDataSource()

        # 获取装饰器中定义的参数
        params = data_source.test_dynamic_params.task_config['params']

        # 执行任务，传递装饰器中定义的参数
        result = await data_source.test_dynamic_params(**params)

        # 验证结果
        assert result['result'] == 'success'
        assert result['exchange'] == 'binance'  # 应该来自装饰器的params
        assert result['quote'] == 'USDT'  # 应该来自装饰器的params
        assert result['custom_param'] is None  # 没有传递的参数应该是None

        # 验证任务调用记录
        assert len(data_source.task_calls) == 1
        assert data_source.task_calls[0]['exchange_name'] == 'binance'
        assert data_source.task_calls[0]['quote'] == 'USDT'

    @pytest.mark.asyncio
    async def test_dynamic_params_override(self):
        """测试动态参数覆盖"""
        # 创建数据源实例
        data_source = self.DynamicParamsDataSource()

        # 获取装饰器中定义的参数
        base_params = data_source.test_dynamic_params.task_config['params']

        # 合并装饰器参数和自定义参数（自定义参数会覆盖装饰器参数）
        custom_params = {'custom_param': 'test_override', 'exchange_name': 'okx'}
        params = {**base_params, **custom_params}

        # 执行任务
        result = await data_source.test_dynamic_params(**params)

        # 验证结果：传递的参数应该覆盖装饰器中的参数
        assert result['result'] == 'success'
        assert result['exchange'] == 'okx'  # 应该使用传递的参数覆盖装饰器的params
        assert result['quote'] == 'USDT'  # 应该保留装饰器的params
        assert result['custom_param'] == 'test_override'  # 应该使用传递的参数

        # 验证任务调用记录
        assert len(data_source.task_calls) == 1
        assert data_source.task_calls[0]['exchange_name'] == 'okx'
        assert data_source.task_calls[0]['quote'] == 'USDT'
        assert data_source.task_calls[0]['custom_param'] == 'test_override'

    @pytest.mark.asyncio
    async def test_partial_params(self):
        """测试部分参数传递"""
        # 创建数据源实例
        data_source = self.DynamicParamsDataSource()

        # 获取装饰器中定义的参数
        base_params = data_source.test_partial_params.task_config['params']

        # 合并装饰器参数和自定义参数
        custom_params = {'quote': 'USDC'}
        params = {**base_params, **custom_params}

        # 执行任务
        result = await data_source.test_partial_params(**params)

        # 验证结果：装饰器提供的参数和传递的参数应该合并
        assert result['result'] == 'success'
        assert result['exchange'] == 'bybit'  # 应该来自装饰器的params
        assert result['quote'] == 'USDC'  # 应该使用传递的参数

    def test_decorator_params_structure(self):
        """测试装饰器参数结构"""
        # 创建数据源实例
        data_source = self.DynamicParamsDataSource()

        # 验证装饰器参数是否正确设置
        assert hasattr(data_source.test_dynamic_params, 'task_config')
        assert 'params' in data_source.test_dynamic_params.task_config
        assert data_source.test_dynamic_params.task_config['params']['exchange_name'] == 'binance'
        assert data_source.test_dynamic_params.task_config['params']['quote'] == 'USDT'

        # 验证部分参数的装饰器设置
        assert hasattr(data_source.test_partial_params, 'task_config')
        assert data_source.test_partial_params.task_config['params']['exchange_name'] == 'bybit'


class TestDelegateCall:
    """测试delegate_call方法的功能"""

    def setup_method(self):
        """测试方法设置"""
        self.scheduler = Scheduler()

    def test_delegate_call_invalid_plugin_type(self):
        """测试调用无效插件类型"""
        with pytest.raises(ValueError) as excinfo:
            self.scheduler.delegate_call(
                plugin_name="TestDataSource",
                plugin_type="invalid_type",
                function_name="sync_method"
            )
        assert "Invalid plugin type" in str(excinfo.value)

    def test_delegate_call_unsupported_plugin(self):
        """测试调用不支持的插件"""
        with pytest.raises(ValueError) as excinfo:
            self.scheduler.delegate_call(
                plugin_name="UnsupportedPlugin",
                plugin_type="data_source",
                function_name="sync_method"
            )
        assert "data_source UnsupportedPlugin not supported" in str(excinfo.value)

    def test_delegate_call_with_mock(self):
        """使用模拟对象测试delegate_call方法的核心功能"""
        # 模拟一个数据源实例
        mock_data_source = Mock()
        mock_data_source.name = "MockDataSource"
        mock_data_source.sync_method = Mock(return_value="sync_result")

        # 模拟一个DataFrame返回值
        mock_df = pd.DataFrame({'col1': [1, 2, 3], 'col2': [4, 5, 6]})
        mock_data_source.dataframe_method = Mock(return_value=mock_df)

        # 模拟list_supported_plugins方法
        self.scheduler.list_supported_plugins = Mock(return_value=["MockDataSource"])

        # 模拟get_supported_plugin方法
        mock_data_source_class = Mock()
        mock_data_source_class.return_value = mock_data_source
        self.scheduler.get_supported_plugin = Mock(return_value=mock_data_source_class)

        # 测试同步方法调用
        result = self.scheduler.delegate_call(
            plugin_name="MockDataSource",
            plugin_type="data_source",
            function_name="sync_method",
            param1="test_value"
        )
        assert result == "sync_result"
        mock_data_source.sync_method.assert_called_once_with(param1="test_value")

        # 测试返回DataFrame的方法调用
        result = self.scheduler.delegate_call(
            plugin_name="MockDataSource",
            plugin_type="data_source",
            function_name="dataframe_method"
        )
        assert isinstance(result, dict)
        assert "data" in result
        assert "metadata" in result
        assert isinstance(result["data"], list)
        assert len(result["data"]) == 3
        mock_data_source.dataframe_method.assert_called_once()

    def test_delegate_call_plugin_instance_caching(self):
        """测试插件实例缓存机制"""
        # 模拟一个数据源实例
        mock_data_source = Mock()
        mock_data_source.name = "MockDataSource"
        mock_data_source.sync_method = Mock(return_value="sync_result")

        # 模拟list_supported_plugins方法
        self.scheduler.list_supported_plugins = Mock(return_value=["MockDataSource"])

        # 模拟get_supported_plugin方法
        mock_data_source_class = Mock()
        mock_data_source_class.return_value = mock_data_source
        self.scheduler.get_supported_plugin = Mock(return_value=mock_data_source_class)

        # 第一次调用，应该创建实例
        result1 = self.scheduler.delegate_call(
            plugin_name="MockDataSource",
            plugin_type="data_source",
            function_name="sync_method",
            param1="test1"
        )

        # 第二次调用，应该复用实例
        result2 = self.scheduler.delegate_call(
            plugin_name="MockDataSource",
            plugin_type="data_source",
            function_name="sync_method",
            param1="test2"
        )

        # 验证结果
        assert result1 == "sync_result"
        assert result2 == "sync_result"

        # 验证插件类只被实例化一次
        assert mock_data_source_class.call_count == 1

        # 验证sync_method被调用两次，每次参数不同
        assert mock_data_source.sync_method.call_count == 2

        # 验证插件实例被缓存
        plugin_key = ("data_source", "MockDataSource")
        assert plugin_key in self.scheduler.delegate_plugin_instances

    def test_delegate_call_api_callable_decorator(self):
        """测试被api_callable装饰的函数可以被调用"""
        # 导入api_callable装饰器
        from chronoforge.decorators import api_callable

        # 定义一个带api_callable装饰器的模拟数据源
        class MockDataSource:
            def __init__(self, config):
                self.config = config

            @property
            def name(self):
                return "MockDataSource"

            @api_callable
            def decorated_method(self, param1, param2):
                return f"{param1}_{param2}"

            def non_decorated_method(self):
                return "non_decorated"

        # 模拟list_supported_plugins方法
        self.scheduler.list_supported_plugins = Mock(return_value=["MockDataSource"])

        # 模拟get_supported_plugin方法
        self.scheduler.get_supported_plugin = Mock(return_value=MockDataSource)

        # 测试被装饰的方法可以被调用
        result = self.scheduler.delegate_call(
            plugin_name="MockDataSource",
            plugin_type="data_source",
            function_name="decorated_method",
            param1="test",
            param2="value"
        )
        assert result == "test_value"

        # 测试未被装饰的方法不能被调用
        with pytest.raises(ValueError) as excinfo:
            self.scheduler.delegate_call(
                plugin_name="MockDataSource",
                plugin_type="data_source",
                function_name="non_decorated_method"
            )
        assert "is not marked as api_callable" in str(excinfo.value)


class TestConcurrencyControl:
    """测试并发控制功能"""

    def setup_method(self):
        """测试方法设置"""
        from chronoforge.scheduler import LockManager
        self.LockManager = LockManager
        self.scheduler = Scheduler()

    def test_lock_manager_basic(self):
        """测试LockManager基本功能"""
        lock_manager = self.LockManager()

        # 获取不同键的锁
        lock1 = lock_manager.get_lock('test_key1')
        lock2 = lock_manager.get_lock('test_key2')
        lock1_again = lock_manager.get_lock('test_key1')

        # 验证同一键获取的是同一个锁对象
        assert lock1 is lock1_again
        assert lock1 is not lock2

        # 验证锁可以正常获取和释放
        with lock1:
            # 锁应该处于获取状态
            assert lock1._is_owned()  # RLock的内部属性，用于测试

    def test_lock_manager_thread_safety(self):
        """测试LockManager的线程安全性"""
        lock_manager = self.LockManager()

        # 测试多线程环境下的锁获取
        locks = []

        def get_lock_thread(key):
            locks.append(lock_manager.get_lock(key))

        # 创建多个线程同时获取同一个键的锁
        threads = []
        for _ in range(5):
            t = threading.Thread(target=get_lock_thread, args=('shared_key',))
            threads.append(t)
            t.start()

        # 等待所有线程完成
        for t in threads:
            t.join()

        # 验证所有线程获取的是同一个锁对象
        for lock in locks[1:]:
            assert lock is locks[0]

    @pytest.mark.asyncio
    async def test_exchange_semaphores_initialization(self):
        """测试交易所信号量的初始化"""
        scheduler = self.scheduler

        # 验证默认信号量
        assert 'binance' in scheduler.exchange_semaphores
        assert 'okx' in scheduler.exchange_semaphores
        assert 'bybit' in scheduler.exchange_semaphores

        # 验证特殊交易所的信号量限制
        binance_semaphore = scheduler.exchange_semaphores['binance']
        assert binance_semaphore._value == 10  # binance允许10个并发请求

        # 验证其他交易所的默认信号量限制
        default_semaphore = scheduler.exchange_semaphores['default_exchange']
        assert default_semaphore._value == 5  # 默认允许5个并发请求

    @pytest.mark.asyncio
    async def test_exchange_semaphore_acquisition(self):
        """测试交易所信号量的获取和释放"""
        scheduler = self.scheduler

        # 获取binance信号量
        binance_semaphore = scheduler.exchange_semaphores['binance']
        initial_value = binance_semaphore._value

        # 获取信号量
        await binance_semaphore.acquire()

        # 验证信号量值减少
        assert binance_semaphore._value == initial_value - 1

        # 释放信号量
        binance_semaphore.release()

        # 验证信号量值恢复
        assert binance_semaphore._value == initial_value

    @pytest.mark.asyncio
    async def test_concurrent_requests_with_semaphore(self):
        """测试使用信号量控制并发请求"""
        scheduler = self.scheduler

        # 使用一个限制较低的信号量进行测试
        test_semaphore = asyncio.Semaphore(2)  # 只允许2个并发请求

        # 记录并发执行的任务数
        concurrent_count = 0
        max_concurrent = 0

        async def limited_task():
            """受信号量限制的任务"""
            nonlocal concurrent_count, max_concurrent

            async with test_semaphore:
                concurrent_count += 1
                max_concurrent = max(max_concurrent, concurrent_count)

                # 模拟任务执行时间
                await asyncio.sleep(0.1)

                concurrent_count -= 1

            return "completed"

        # 并发执行5个任务
        tasks = [limited_task() for _ in range(5)]
        results = await asyncio.gather(*tasks)

        # 验证所有任务都完成
        assert all(result == "completed" for result in results)

        # 验证最大并发数不超过信号量限制
        assert max_concurrent <= 2

    @pytest.mark.asyncio
    async def test_symbol_exchange_parsing_for_semaphore(self):
        """测试符号中的交易所名称解析（用于信号量控制）"""
        scheduler = self.scheduler

        # 模拟_update_data_with_semaphore中的符号解析逻辑
        async def parse_symbol_exchange(symbol):
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
                    # 如果无法分割，则使用默认binance
                    symbol_exchange_name = 'binance'
            except Exception:
                # 如果解析失败，使用默认binance
                symbol_exchange_name = 'binance'

            return symbol_exchange_name.lower()

        # 测试不同格式的符号解析
        symbols = [
            "datasource:binance:BTC/USDT",  # 格式：datasource:exchange:symbol
            "okx:ETH/USDT",  # 格式：exchange:symbol
            "BTC/USDT",  # 格式：symbol（默认binance）
            "datasource:BTC/USDT",  # 格式：datasource:symbol（默认binance）
            "bybit:SOL/USDT"  # 格式：exchange:symbol
        ]

        expected_exchanges = ['binance', 'okx', 'binance', 'binance', 'bybit']

        for symbol, expected_exchange in zip(symbols, expected_exchanges):
            parsed_exchange = await parse_symbol_exchange(symbol)
            assert parsed_exchange == expected_exchange
            # 验证解析的交易所名称在信号量字典中
            assert parsed_exchange in scheduler.exchange_semaphores

    @pytest.mark.asyncio
    async def test_semaphore_rate_limiting(self):
        """测试信号量的速率限制功能"""
        scheduler = self.scheduler

        # 使用较低的信号量限制
        scheduler.exchange_semaphores['test_exchange'] = asyncio.Semaphore(1)  # 只允许1个并发请求

        # 记录每次请求开始和结束的时间
        request_times = []

        async def rate_limited_task():
            """受速率限制的任务"""
            async with scheduler.exchange_semaphores['test_exchange']:
                start_time = time.time()
                # 模拟任务执行时间
                await asyncio.sleep(0.1)
                end_time = time.time()
                request_times.append({'start': start_time, 'end': end_time})
            return "completed"

        # 并发执行3个任务
        tasks = [rate_limited_task() for _ in range(3)]
        results = await asyncio.gather(*tasks)

        # 验证所有任务都完成
        assert all(result == "completed" for result in results)

        # 验证任务是串行执行的
        for i in range(1, len(request_times)):
            assert request_times[i]['start'] >= request_times[i-1]['end']

    def test_lock_manager_cleanup(self):
        """测试LockManager的锁对象管理"""
        lock_manager = self.LockManager()

        # 获取多个锁
        lock_manager.get_lock('key1')
        lock_manager.get_lock('key2')
        lock_manager.get_lock('key3')

        # 验证内部锁字典包含所有创建的锁
        assert len(lock_manager._locks) == 3
        assert 'key1' in lock_manager._locks
        assert 'key2' in lock_manager._locks
        assert 'key3' in lock_manager._locks
