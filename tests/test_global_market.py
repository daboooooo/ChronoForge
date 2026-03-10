import pytest
from unittest.mock import Mock, patch
import pandas as pd
from chronoforge.data_source import GlobalMarketDataSource


class TestGlobalMarketDataSource:
    """测试全球市场数据源"""

    def setup_method(self):
        """测试方法设置"""
        self.data_source = GlobalMarketDataSource()

    def test_initialization(self):
        """测试初始化"""
        assert self.data_source is not None
        assert self.data_source.name == "GlobalMarket"

    @pytest.mark.asyncio
    async def test_fetch_valid_parameters(self):
        """测试使用有效参数调用fetch方法"""
        # 模拟yfinance.download的响应
        date_rng = pd.date_range(start="2025-10-01", end="2025-10-10", freq="D")
        mock_data = pd.DataFrame(
            {
                "Open": [4500, 4550, 4600, 4580, 4620, 4650, 4700, 4750, 4720, 4780],
                "High": [4520, 4570, 4610, 4600, 4630, 4660, 4710, 4760, 4730, 4790],
                "Low": [4490, 4540, 4590, 4570, 4610, 4640, 4690, 4740, 4710, 4770],
                "Close": [4510, 4560, 4605, 4590, 4625, 4655, 4705, 4755, 4725, 4785],
                "Volume": [
                    1000000, 1100000, 1200000, 1150000, 1250000,
                    1300000, 1350000, 1400000, 1370000, 1420000
                ]
            },
            index=date_rng
        )

        with patch('chronoforge.data_source.global_market.yf.download') as mock_download:
            mock_download.return_value = mock_data

            # 调用fetch方法
            result = await self.data_source.fetch(
                symbol="^GSPC",
                timeframe="1d",
                start_ts_ms=1761955200000,  # 2025-10-31
                end_ts_ms=1762041600000   # 2025-11-01
            )

            # 验证结果
            assert result is not None
            assert isinstance(result, pd.DataFrame)
            assert len(result) > 0
            assert "time" in result.columns
            assert "open" in result.columns
            assert "high" in result.columns
            assert "low" in result.columns
            assert "close" in result.columns
            assert "volume" in result.columns

    @pytest.mark.asyncio
    async def test_fetch_with_none_end_ts(self):
        """测试不指定结束时间戳的情况"""
        # 模拟yfinance.download的响应
        date_rng = pd.date_range(start="2025-10-01", end="2025-10-10", freq="D")
        mock_data = pd.DataFrame({
            "Open": [4500, 4550],
            "High": [4520, 4570],
            "Low": [4490, 4540],
            "Close": [4510, 4560],
            "Volume": [1000000, 1100000]
        }, index=date_rng[:2])

        with patch('chronoforge.data_source.global_market.yf.download') as mock_download:
            mock_download.return_value = mock_data

            # 不指定结束时间戳
            result = await self.data_source.fetch(
                symbol="^GSPC",
                timeframe="1d",
                start_ts_ms=1761955200000
            )

            # 验证结果
            assert result is not None
            assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_fetch_api_error(self):
        """测试API错误情况"""
        with patch('chronoforge.data_source.global_market.yf.download') as mock_download:
            # 模拟API调用抛出异常
            mock_download.side_effect = Exception("API Error")

            result = await self.data_source.fetch(
                symbol="^GSPC",
                timeframe="1d",
                start_ts_ms=1761955200000
            )

            # 应该返回空DataFrame
            assert isinstance(result, pd.DataFrame)
            assert "time" in result.columns
            assert "open" in result.columns
            assert "high" in result.columns
            assert "low" in result.columns
            assert "close" in result.columns
            assert "volume" in result.columns

    @pytest.mark.asyncio
    async def test_async_context_manager(self):
        """测试异步上下文管理器"""
        async with GlobalMarketDataSource() as manager:
            assert manager is not None
            assert isinstance(manager, GlobalMarketDataSource)

    @pytest.mark.asyncio
    async def test_fetch_with_multi_index_columns(self):
        """测试处理多级索引列的情况"""
        # 模拟yfinance.download返回多级索引列的数据
        date_rng = pd.date_range(start="2025-10-01", end="2025-10-02", freq="D")
        # 创建多级索引列
        columns = pd.MultiIndex.from_tuples([
            ("^GSPC", "Open"),
            ("^GSPC", "High"),
            ("^GSPC", "Low"),
            ("^GSPC", "Close"),
            ("^GSPC", "Volume")
        ])
        mock_data = pd.DataFrame(
            [[4500, 4520, 4490, 4510, 1000000], [4550, 4570, 4540, 4560, 1100000]],
            index=date_rng,
            columns=columns
        )

        with patch('chronoforge.data_source.global_market.yf.download') as mock_download:
            mock_download.return_value = mock_data

            # 调用fetch方法
            result = await self.data_source.fetch(
                symbol="^GSPC",
                timeframe="1d",
                start_ts_ms=1761955200000,
                end_ts_ms=1762041600000
            )

            # 验证结果
            assert result is not None
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2
            assert "time" in result.columns
            assert "open" in result.columns  # 应该被转换为小写
