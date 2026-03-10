"""
DataUpdateManager 集成测试

测试数据更新管理器的核心功能：
1. 初始化和配置
2. 交易对全集管理
3. 增量更新流程
4. 数据合并和去重
"""

import asyncio
import pytest
import pandas as pd
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock


class TestDataUpdateManagerInit:
    """DataUpdateManager初始化测试"""

    def test_import(self):
        """测试导入"""
        from chronoforge.scheduler.data_update import DataUpdateManager, UpdateStrategy
        assert DataUpdateManager is not None
        assert UpdateStrategy is not None
        assert UpdateStrategy.INCREMENTAL is not None
        assert UpdateStrategy.FULL is not None

    def test_update_strategy_values(self):
        """测试更新策略枚举值"""
        from chronoforge.scheduler.data_update import UpdateStrategy
        assert UpdateStrategy.INCREMENTAL.value == "incremental"
        assert UpdateStrategy.FULL.value == "full"
        assert UpdateStrategy.SMART.value == "smart"


class TestUpdateResult:
    """更新结果测试"""

    def test_update_result_creation(self):
        """测试创建更新结果"""
        from chronoforge.scheduler.data_update import UpdateResult
        from chronoforge.scheduler.task_scheduler import TaskStatus

        result = UpdateResult(
            task_name="test_task",
            symbol="BTC/USDT",
            status=TaskStatus.RUNNING,
            start_time=datetime.now(timezone.utc)
        )
        assert result.task_name == "test_task"
        assert result.symbol == "BTC/USDT"
        assert result.status == TaskStatus.RUNNING
        assert result.records_updated == 0


class TestSymbolUniverse:
    """交易对全集测试"""

    def test_symbol_universe_empty(self):
        """测试空交易对全集"""
        from chronoforge.scheduler.data_update import SymbolUniverse

        universe = SymbolUniverse()
        assert universe.spot_symbols == {}
        assert universe.futures_symbols == set()
        assert universe.coin_symbols == set()
        assert universe.macro_symbols == set()

    def test_get_all_spot_symbols_empty(self):
        """测试获取空现货交易对"""
        from chronoforge.scheduler.data_update import SymbolUniverse

        universe = SymbolUniverse()
        symbols = universe.get_all_spot_symbols()
        assert symbols == []

    def test_get_top_volume_symbols_empty(self):
        """测试获取空成交量交易对"""
        from chronoforge.scheduler.data_update import SymbolUniverse

        universe = SymbolUniverse()
        symbols = universe.get_top_volume_symbols("binance", "USDT", 80)
        assert symbols == []

    def test_symbol_universe_with_data(self):
        """测试带数据的交易对全集"""
        from chronoforge.scheduler.data_update import SymbolUniverse

        universe = SymbolUniverse()
        universe.spot_symbols = {
            "binance": {"BTC/USDT", "ETH/USDT", "SOL/USDT"},
            "okx": {"BTC/USDT", "ETH/USDT"}
        }
        universe.futures_symbols = {"BTC/USDT", "ETH/USDT"}

        all_symbols = universe.get_all_spot_symbols()
        assert len(all_symbols) == 5
        assert "BTC/USDT" in all_symbols

        top_symbols = universe.get_top_volume_symbols("binance", "USDT", 80)
        assert len(top_symbols) == 2


class TestDataUpdateManagerMocked:
    """带Mock的DataUpdateManager测试"""

    @pytest.fixture
    def mock_dsm(self):
        """创建Mock数据源管理器"""
        dsm = MagicMock()
        return dsm

    @pytest.fixture
    def mock_sm(self):
        """创建Mock存储管理器"""
        sm = MagicMock()
        return sm

    @pytest.mark.asyncio
    async def test_initialize(self, mock_dsm, mock_sm):
        """测试初始化"""
        from chronoforge.scheduler.data_update import DataUpdateManager

        dum = DataUpdateManager(mock_dsm, mock_sm)
        await dum.initialize()

        assert dum.symbol_universe is not None
        assert dum._update_stats is not None

    @pytest.mark.asyncio
    async def test_get_cached_data_timestamp(self, mock_dsm, mock_sm):
        """测试获取缓存数据时间戳"""
        from chronoforge.scheduler.data_update import DataUpdateManager

        mock_sm.get_time_range = AsyncMock(return_value={
            "start_time": datetime.now(timezone.utc) - timedelta(days=1),
            "end_time": datetime.now(timezone.utc)
        })

        dum = DataUpdateManager(mock_dsm, mock_sm)
        ts = await dum._get_cached_data_timestamp("BTC_USDT", "ohlcv")

        assert ts is not None
        assert isinstance(ts, int)

    @pytest.mark.asyncio
    async def test_get_cached_data_timestamp_empty(self, mock_dsm, mock_sm):
        """测试获取空缓存数据时间戳"""
        from chronoforge.scheduler.data_update import DataUpdateManager

        mock_sm.get_time_range = AsyncMock(return_value=None)

        dum = DataUpdateManager(mock_dsm, mock_sm)
        ts = await dum._get_cached_data_timestamp("new_symbol", "ohlcv")

        assert ts is None

    @pytest.mark.asyncio
    async def test_merge_and_save_empty_data(self, mock_dsm, mock_sm):
        """测试合并空数据"""
        from chronoforge.scheduler.data_update import DataUpdateManager
        from chronoforge.scheduler.task_scheduler import TaskStatus

        dum = DataUpdateManager(mock_dsm, mock_sm)

        empty_df = pd.DataFrame()
        result = await dum._merge_and_save(
            data_id="test_symbol",
            new_data=empty_df,
            data_type="ohlcv"
        )

        assert result.status == TaskStatus.COMPLETED
        assert result.records_updated == 0

    @pytest.mark.asyncio
    async def test_merge_and_save_new_data(self, mock_dsm, mock_sm):
        """测试合并新数据"""
        from chronoforge.scheduler.data_update import DataUpdateManager
        from chronoforge.scheduler.task_scheduler import TaskStatus

        mock_sm.load = AsyncMock(return_value=None)
        mock_sm.save = AsyncMock(return_value=True)

        dum = DataUpdateManager(mock_dsm, mock_sm)

        new_data = pd.DataFrame({
            "ts": [datetime.now(timezone.utc).timestamp() * 1000],
            "open": [50000.0],
            "high": [51000.0],
            "low": [49000.0],
            "close": [50500.0],
            "volume": [1000.0]
        })

        result = await dum._merge_and_save(
            data_id="BTC_USDT_1h",
            new_data=new_data,
            data_type="ohlcv"
        )

        assert result.status == TaskStatus.COMPLETED
        assert result.records_updated == 1
        assert result.records_total == 1
        mock_sm.save.assert_called_once()

    @pytest.mark.asyncio
    async def test_merge_and_save_existing_data(self, mock_dsm, mock_sm):
        """测试合并已存在数据（去重）"""
        from chronoforge.scheduler.data_update import DataUpdateManager
        from chronoforge.scheduler.task_scheduler import TaskStatus

        existing_ts = datetime.now(timezone.utc).timestamp() * 1000
        existing_data = pd.DataFrame({
            "ts": [existing_ts],
            "open": [50000.0],
            "high": [51000.0],
            "low": [49000.0],
            "close": [50500.0],
            "volume": [1000.0]
        })

        mock_sm.load = AsyncMock(return_value=existing_data)
        mock_sm.save = AsyncMock(return_value=True)

        dum = DataUpdateManager(mock_dsm, mock_sm)

        new_ts = existing_ts + 3600000
        new_data = pd.DataFrame({
            "ts": [existing_ts, new_ts],
            "open": [50000.0, 51000.0],
            "high": [51000.0, 52000.0],
            "low": [49000.0, 50000.0],
            "close": [50500.0, 51500.0],
            "volume": [1000.0, 1500.0]
        })

        result = await dum._merge_and_save(
            data_id="BTC_USDT_1h",
            new_data=new_data,
            data_type="ohlcv"
        )

        assert result.status == TaskStatus.COMPLETED
        assert result.records_total == 2
        mock_sm.save.assert_called_once()
        call_args = mock_sm.save.call_args
        merged_df = call_args[1]['data']
        assert len(merged_df) == 2

    @pytest.mark.asyncio
    async def test_get_update_stats(self, mock_dsm, mock_sm):
        """测试获取更新统计"""
        from chronoforge.scheduler.data_update import DataUpdateManager

        dum = DataUpdateManager(mock_dsm, mock_sm)
        stats = dum.get_update_stats()

        assert 'total_updates' in stats
        assert 'successful_updates' in stats
        assert 'failed_updates' in stats
        assert 'ticker_cache' in stats
        assert 'symbol_universe' in stats


class TestDataUpdateIntegration:
    """数据更新集成测试"""

    @pytest.fixture
    def setup_managers(self):
        """设置测试用管理器"""
        from chronoforge.scheduler.data_update import DataUpdateManager
        from chronoforge.scheduler.task_scheduler import TaskScheduler

        dsm = MagicMock()
        sm = MagicMock()

        dum = DataUpdateManager(dsm, sm)
        scheduler = TaskScheduler(max_concurrent_tasks=5)

        return dum, scheduler

    @pytest.mark.asyncio
    async def test_spot_tickers_update_mock(self, setup_managers):
        """测试模拟现货Ticker更新"""
        dum, scheduler = setup_managers

        dum.dsm.get_data_source = MagicMock(return_value=MagicMock(
            tickers=AsyncMock(return_value={
                "USDT": {
                    "BTC/USDT": {"price": 50000, "volume": 1000},
                    "ETH/USDT": {"price": 3000, "volume": 500}
                }
            })
        ))

        dum.sm.save = AsyncMock(return_value=True)

        results = await dum.update_spot_tickers(exchanges=["binance"])

        assert "binance" in results
        assert results["binance"].records_updated > 0

    @pytest.mark.asyncio
    async def test_hourly_task_with_offset(self, setup_managers):
        """测试每小时任务配置"""
        dum, scheduler = setup_managers

        task_executed = []

        async def hourly_task():
            task_executed.append(True)

        scheduler.add_hourly_task(
            name="test_hourly",
            func=hourly_task,
            offset_seconds=30,
            enabled=True
        )

        assert len(scheduler.tasks) == 1
        task = list(scheduler.tasks.values())[0]
        assert task.schedule_type == "hourly"
        assert task.schedule_config.get("offset_seconds") == 30


class TestUpdateWorkflow:
    """更新工作流测试"""

    @pytest.mark.asyncio
    async def test_full_workflow_simulation(self):
        """模拟完整更新工作流"""
        from chronoforge.scheduler.data_update import DataUpdateManager
        from chronoforge.scheduler.task_scheduler import TaskScheduler, TaskPriority

        dsm = MagicMock()
        sm = MagicMock()

        dum = DataUpdateManager(dsm, sm)
        scheduler = TaskScheduler(max_concurrent_tasks=3)

        dum.symbol_universe.spot_symbols = {
            "binance": {"BTC/USDT", "ETH/USDT"}
        }

        dum.dsm.get_data_source = MagicMock(return_value=MagicMock(
            top_volume_symbols=AsyncMock(return_value=["BTC/USDT", "ETH/USDT"]),
            fetch=AsyncMock(return_value=pd.DataFrame({
                "ts": [datetime.now(timezone.utc).timestamp() * 1000],
                "open": [50000.0],
                "high": [51000.0],
                "low": [49000.0],
                "close": [50500.0],
                "volume": [1000.0]
            }))
        ))

        sm.get_time_range = AsyncMock(return_value={
            "end_time": datetime.now(timezone.utc) - timedelta(hours=1)
        })
        sm.load = AsyncMock(return_value=None)
        sm.save = AsyncMock(return_value=True)

        scheduler.add_interval_task(
            name="tickers",
            func=lambda: dum.update_spot_tickers(),
            seconds=30,
            priority=TaskPriority.HIGH
        )

        scheduler.add_hourly_task(
            name="ohlcv",
            func=lambda: dum.update_spot_ohlcv(),
            offset_seconds=30,
            priority=TaskPriority.MEDIUM
        )

        assert len(scheduler.tasks) == 2

        await scheduler.start()
        await asyncio.sleep(0.5)
        await scheduler.stop()

        metrics = scheduler.get_metrics()
        assert metrics['active_tasks'] == 2
