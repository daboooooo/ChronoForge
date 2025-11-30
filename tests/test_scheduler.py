import pytest
from chronoforge import Scheduler
from chronoforge.utils import TimeSlot


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
        assert len(scheduler.tasks) == 1

        # 测试添加重复任务
        with pytest.raises(ValueError):
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

        # 测试使用inplace参数覆盖任务
        scheduler.add_task(
            name="test_task",
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
        assert len(scheduler.tasks) == 1

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

        # 启动调度器
        scheduler.start()
        assert scheduler._runner_thread is not None
        assert scheduler._runner_thread.is_alive()

        # 停止调度器
        scheduler.stop()
        # 等待线程结束
        import time
        time.sleep(1)
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
