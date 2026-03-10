"""
DataSourceManager 测试用例

测试数据源管理器的所有功能，包括：
1. 初始化与配置
2. 数据源注册
3. 数据源创建
4. 数据获取
5. 并发获取
6. 能力检测
7. 上下文管理器
"""

import pytest
import pandas as pd
from chronoforge.data_source import DataSourceBase
from chronoforge.data_source.manager import DataSourceManager


class MockDataSource(DataSourceBase):
    """用于测试的模拟数据源"""

    def __init__(self, config=None):
        super().__init__(config)
        self.exchange_name = "test_exchange"
        self.timeframes = ["1m", "5m", "1h", "1d"]

    @property
    def name(self):
        return "MockDataSource"

    async def fetch(self, symbol, timeframe, start_ts_ms, end_ts_ms=None) -> pd.DataFrame:
        return pd.DataFrame({
            "time": [start_ts_ms],
            "open": [50000.0],
            "high": [51000.0],
            "low": [49000.0],
            "close": [50500.0],
            "volume": [1000.0]
        })

    async def close_all_connections(self):
        pass


class TestDataSourceManagerInit:
    """数据源管理器初始化测试"""

    def test_init_default(self):
        """测试默认初始化"""
        manager = DataSourceManager()
        assert manager is not None
        assert len(manager.data_source_classes) > 0


class TestDataSourceRegistration:
    """数据源注册测试"""

    def setup_method(self):
        """测试前初始化"""
        self.manager = DataSourceManager()
        self.mock_ds = MockDataSource()

    def test_register_success(self):
        """测试成功注册数据源"""
        result = self.manager.register_data_source("test_ds", self.mock_ds)
        assert result is True
        assert "test_ds" in self.manager.data_source_instances

    def test_register_invalid_instance(self):
        """测试注册无效实例"""
        result = self.manager.register_data_source("invalid", object())
        assert result is False

    def test_get_registered(self):
        """测试获取已注册的数据源"""
        self.manager.register_data_source("test_ds", self.mock_ds)
        retrieved = self.manager.get_data_source("test_ds")
        assert retrieved is self.mock_ds

    def test_get_nonexistent(self):
        """测试获取不存在的数据源"""
        result = self.manager.get_data_source("nonexistent")
        assert result is None


class TestDataSourceCreation:
    """数据源创建测试"""

    def setup_method(self):
        """测试前初始化"""
        self.manager = DataSourceManager()

    def test_create_existing_class(self):
        """测试创建已存在的数据源类"""
        ds = self.manager.create_data_source("CryptoSpotDataSource", {})
        assert ds is not None
        assert ds.name == "CryptoSpot"

    def test_create_nonexistent_class(self):
        """测试创建不存在的数据源类"""
        result = self.manager.create_data_source("NonExistentDataSource", {})
        assert result is None


class TestDataSourceCapabilities:
    """数据源能力测试"""

    def setup_method(self):
        """测试前初始化"""
        self.manager = DataSourceManager()
        self.mock_ds = MockDataSource()

    def test_detect_capabilities(self):
        """测试检测数据源能力"""
        self.manager.register_data_source("test_ds", self.mock_ds)
        capabilities = self.manager.get_data_source_capabilities("test_ds")

        assert capabilities is not None
        assert hasattr(capabilities, 'supported_timeframes')
        assert "1h" in capabilities.supported_timeframes

    def test_get_capabilities_cached(self):
        """测试获取缓存的能力信息"""
        self.manager.register_data_source("test_ds", self.mock_ds)
        cap1 = self.manager.get_data_source_capabilities("test_ds")
        cap2 = self.manager.get_data_source_capabilities("test_ds")
        assert cap1 is cap2


class TestDataFetching:
    """数据获取测试"""

    def setup_method(self):
        """测试前初始化"""
        self.manager = DataSourceManager()

    @pytest.mark.asyncio
    async def test_fetch_data_success(self):
        """测试成功获取数据"""
        self.manager.data_source_classes["MockDataSource"] = MockDataSource
        result = await self.manager.fetch_data(
            data_source_name="MockDataSource",
            symbol="BTC/USDT",
            timeframe="1d",
            start_ts_ms=1640995200000
        )

        assert result is not None
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        assert "close" in result.columns

    @pytest.mark.asyncio
    async def test_fetch_nonexistent_source(self):
        """测试获取不存在的数据源"""
        result = await self.manager.fetch_data(
            data_source_name="NonExistentSource",
            symbol="BTC/USDT",
            timeframe="1d",
            start_ts_ms=1640995200000
        )
        assert result is None


class TestConcurrentFetching:
    """并发数据获取测试"""

    def setup_method(self):
        """测试前初始化"""
        self.manager = DataSourceManager()
        self.manager.data_source_classes["MockDataSource"] = MockDataSource

    @pytest.mark.asyncio
    async def test_fetch_concurrent(self):
        """测试并发获取多个任务"""
        from chronoforge.data_source.manager import FetchTask

        tasks = [
            FetchTask(
                data_source_name="MockDataSource",
                symbol="BTC/USDT",
                timeframe="1d",
                start_ts_ms=1640995200000
            ),
            FetchTask(
                data_source_name="MockDataSource",
                symbol="ETH/USDT",
                timeframe="1h",
                start_ts_ms=1640995200000
            )
        ]

        results = await self.manager.fetch_data_concurrent(tasks)

        assert results is not None
        assert isinstance(results, dict)
        assert len(results) == 2
        assert 0 in results
        assert 1 in results


class TestDataSourceListings:
    """数据源列表测试"""

    def setup_method(self):
        """测试前初始化"""
        self.manager = DataSourceManager()

    def test_get_supported_sources(self):
        """测试获取支持的数据源列表"""
        sources = self.manager.get_supported_data_sources()
        assert isinstance(sources, list)
        assert len(sources) > 0
        assert "CryptoSpotDataSource" in sources

    def test_get_active_sources_empty(self):
        """测试获取活跃数据源（初始为空）"""
        sources = self.manager.get_active_data_sources()
        assert isinstance(sources, list)
        assert len(sources) == 0

    def test_get_active_sources_with_instances(self):
        """测试获取活跃数据源（有实例时）"""
        mock_ds = MockDataSource()
        self.manager.register_data_source("test_ds", mock_ds)
        sources = self.manager.get_active_data_sources()
        assert "test_ds" in sources


class TestContextManager:
    """上下文管理器测试"""

    def setup_method(self):
        """测试前初始化"""
        self.manager = DataSourceManager()

    @pytest.mark.asyncio
    async def test_async_context_manager(self):
        """测试异步上下文管理器"""
        async with self.manager as mgr:
            assert mgr is not None

    @pytest.mark.asyncio
    async def test_context_manager_cleanup(self):
        """测试上下文管理器清理"""
        mock_ds = MockDataSource()
        self.manager.register_data_source("test_ds", mock_ds)

        async with self.manager:
            pass

        assert len(self.manager.data_source_instances) == 0


class TestRetryConfig:
    """重试配置测试"""

    def test_default_retry_config(self):
        """测试默认重试配置"""
        manager = DataSourceManager()
        assert manager.retry_config.max_retries == 3
        assert manager.retry_config.retry_delay == 1.0
        assert manager.retry_config.backoff_factor == 1.5

    def test_custom_retry_config(self):
        """测试自定义重试配置"""
        manager = DataSourceManager()
        manager.retry_config = {
            'max_retries': 5,
            'retry_delay': 2.0,
            'backoff_factor': 2.0
        }
        assert manager.retry_config['max_retries'] == 5
