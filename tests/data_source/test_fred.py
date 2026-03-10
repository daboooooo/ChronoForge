import pytest
import asyncio
from unittest.mock import Mock, patch
import pandas as pd
from chronoforge.data_source import FREDDataSource


class TestFREDDataSource:
    """测试FRED数据源"""

    def setup_method(self):
        """测试方法设置"""
        # 使用测试API密钥初始化
        self.test_config = {"api_key": "test_api_key"}
        self.data_source = FREDDataSource(config=self.test_config)

    def test_initialization(self):
        """测试初始化"""
        assert self.data_source is not None
        assert self.data_source.name == "FRED"
        assert self.data_source.config == self.test_config
        assert self.data_source.fred_instance is None

    def test_initialization_missing_api_key(self):
        """测试缺少API密钥的初始化"""
        # 不提供API密钥
        data_source = FREDDataSource(config={})
        assert data_source is not None
        assert data_source.fred_instance is None

    @pytest.mark.asyncio
    async def test_fetch_valid_parameters(self):
        """测试使用有效参数调用fetch方法"""
        # 模拟FRED API响应
        import numpy as np
        date_rng = pd.date_range(start="2025-10-01", end="2025-10-10", freq="D")
        mock_data = pd.Series(np.random.randn(len(date_rng)), index=date_rng)

        with patch('chronoforge.data_source.fred.Fred') as mock_fred_class:
            # 创建模拟实例
            mock_fred_instance = Mock()
            mock_fred_class.return_value = mock_fred_instance
            # 设置模拟方法的返回值
            mock_fred_instance.get_series.return_value = mock_data

            # 调用fetch方法
            result = await self.data_source.fetch(
                symbol="GDP",
                timeframe="1d",
                start_ts_ms=1761955200000,  # 2025-10-31
                end_ts_ms=1762041600000   # 2025-11-01
            )

            # 验证结果
            assert result is not None
            assert isinstance(result, pd.DataFrame)
            assert len(result) > 0
            assert "time" in result.columns
            assert "volume" in result.columns

            # 验证FRED实例已初始化
            assert self.data_source.fred_instance is not None

    @pytest.mark.asyncio
    async def test_fetch_missing_api_key(self):
        """测试缺少API密钥的情况"""
        # 创建没有API密钥的数据源
        data_source = FREDDataSource(config={})

        with pytest.raises(ValueError):
            await data_source.fetch(
                symbol="GDP",
                timeframe="1d",
                start_ts_ms=1761955200000
            )

    @pytest.mark.asyncio
    async def test_fetch_empty_data(self):
        """测试API返回空数据的情况"""
        # 模拟FRED API返回空数据
        mock_empty_series = pd.Series([], dtype=float)

        with patch('chronoforge.data_source.fred.Fred') as mock_fred_class:
            mock_fred_instance = Mock()
            mock_fred_class.return_value = mock_fred_instance
            mock_fred_instance.get_series.return_value = mock_empty_series

            result = await self.data_source.fetch(
                symbol="GDP",
                timeframe="1d",
                start_ts_ms=1761955200000
            )

            # 应该返回None
            assert result is None

    @pytest.mark.asyncio
    async def test_fetch_api_error(self):
        """测试API错误情况"""
        with patch('chronoforge.data_source.fred.Fred') as mock_fred_class:
            mock_fred_instance = Mock()
            mock_fred_class.return_value = mock_fred_instance
            # 模拟API调用抛出异常
            mock_fred_instance.get_series.side_effect = Exception("API Error")

            result = await self.data_source.fetch(
                symbol="GDP",
                timeframe="1d",
                start_ts_ms=1761955200000
            )

            # 应该返回空DataFrame
            assert isinstance(result, pd.DataFrame)
            assert "time" in result.columns
            assert "volume" in result.columns

    @pytest.mark.asyncio
    async def test_async_context_manager(self):
        """测试异步上下文管理器"""
        async with FREDDataSource(config=self.test_config) as manager:
            assert manager is not None
            assert isinstance(manager, FREDDataSource)

    @pytest.mark.asyncio
    async def test_fetch_with_none_end_ts(self):
        """测试不指定结束时间戳的情况"""
        # 模拟FRED API响应
        import numpy as np
        date_rng = pd.date_range(start="2025-10-01", end="2025-10-10", freq="D")
        mock_data = pd.Series(np.random.randn(len(date_rng)), index=date_rng)

        with patch('chronoforge.data_source.fred.Fred') as mock_fred_class:
            mock_fred_instance = Mock()
            mock_fred_class.return_value = mock_fred_instance
            mock_fred_instance.get_series.return_value = mock_data

            # 不指定结束时间戳
            result = await self.data_source.fetch(
                symbol="GDP",
                timeframe="1d",
                start_ts_ms=1761955200000
            )

            # 验证结果
            assert result is not None
            assert isinstance(result, pd.DataFrame)
