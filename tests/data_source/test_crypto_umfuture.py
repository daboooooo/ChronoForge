import pytest
from unittest.mock import Mock, patch
import pandas as pd
from chronoforge.data_source import CryptoUMFutureDataSource


class TestCryptoUMFutureDataSource:
    """测试CryptoUMFutureDataSource"""

    def setup_method(self):
        """测试方法设置"""
        self.data_source = CryptoUMFutureDataSource()

    def test_initialization(self):
        """测试初始化"""
        assert self.data_source is not None
        assert self.data_source.name == "CryptoUMFuture"
        assert self.data_source.exchange_info is None
        assert self.data_source.symbols is None

    def test_get_exchange_info(self):
        """测试_get_exchange_info方法"""
        # 模拟client的exchange_info方法
        mock_exchange_info = {
            "symbols": [
                {"symbol": "BTCUSDT"},
                {"symbol": "ETHUSDT"}
            ]
        }

        with patch.object(self.data_source.client, 'exchange_info') as mock_exchange_info_method:
            mock_exchange_info_method.return_value = mock_exchange_info

            # 调用方法
            self.data_source._get_exchange_info()

            # 验证结果
            assert self.data_source.exchange_info == mock_exchange_info
            assert self.data_source.symbols == ["BTCUSDT", "ETHUSDT"]

    @pytest.mark.asyncio
    async def test_fetch_valid_parameters(self):
        """测试使用有效参数调用fetch方法"""
        # 模拟交易所信息
        mock_exchange_info = {
            "symbols": [
                {"symbol": "BTCUSDT"}
            ]
        }

        # 模拟各种API响应
        mock_oi_hist = [
            {
                "symbol": "BTCUSDT",
                "sumOpenInterest": "20403.63700000",
                "sumOpenInterestValue": "150570784.07809979",
                "CMCCirculatingSupply": "165880.538",
                "timestamp": "1761436800000"
            }
        ]

        mock_taker_ratio = [
            {
                "buySellRatio": "1.5586",
                "buyVol": "387.3300",
                "sellVol": "248.5030",
                "timestamp": "1761436800000"
            }
        ]

        mock_top_position_ratio = [
            {
                "symbol": "BTCUSDT",
                "longShortRatio": "1.4342",
                "longAccount": "0.5891",
                "shortAccount": "0.4108",
                "timestamp": "1761436800000"
            }
        ]

        mock_top_account_ratio = [
            {
                "symbol": "BTCUSDT",
                "longShortRatio": "1.8105",
                "longAccount": "0.6442",
                "shortAccount": "0.3558",
                "timestamp": "1761436800000"
            }
        ]

        mock_global_account_ratio = [
            {
                "symbol": "BTCUSDT",
                "longShortRatio": "1.9559",
                "longAccount": "0.6617",
                "shortAccount": "0.3382",
                "timestamp": "1761436800000"
            }
        ]

        # 设置模拟
        with patch.object(
                self.data_source.client, 'exchange_info'
        ) as mock_exchange_info_method:
            mock_exchange_info_method.return_value = mock_exchange_info

            with patch.object(
                    self.data_source.client, 'open_interest_hist'
            ) as mock_oi_method:
                mock_oi_method.return_value = mock_oi_hist

                with patch.object(
                        self.data_source.client, 'taker_long_short_ratio'
                ) as mock_taker_method:
                    mock_taker_method.return_value = mock_taker_ratio

                    with patch.object(
                            self.data_source.client,
                            'top_long_short_position_ratio'
                    ) as mock_top_position_method:
                        mock_top_position_method.return_value = \
                            mock_top_position_ratio

                        with patch.object(
                                self.data_source.client,
                                'top_long_short_account_ratio'
                        ) as mock_top_account_method:
                            mock_top_account_method.return_value = \
                                mock_top_account_ratio

                            with patch.object(
                                    self.data_source.client,
                                    'long_short_account_ratio'
                            ) as mock_global_account_method:
                                mock_global_account_method.return_value = \
                                    mock_global_account_ratio

                                # 计算30天内的时间戳
                                import time
                                now_ts_ms = int(time.time() * 1000)
                                start_ts_ms = now_ts_ms - 15 * 24 * 60 * 60 * 1000  # 15天前
                                end_ts_ms = now_ts_ms - 1000

                                # 调用fetch方法
                                result = await self.data_source.fetch(
                                    symbol="BTCUSDT",
                                    timeframe="1d",
                                    start_ts_ms=start_ts_ms,
                                    end_ts_ms=end_ts_ms
                                )

                                # 验证结果
                                assert result is not None
                                assert isinstance(result, pd.DataFrame)
                                assert len(result) > 0
                                assert "time" in result.columns
                                assert "open_interest_value" in result.columns
                                assert "taker_long_short_ratio" in result.columns
                                assert "top_long_short_position_ratio" in result.columns
                                assert "top_long_short_account_ratio" in result.columns
                                assert "global_long_short_account_ratio" in result.columns

    @pytest.mark.asyncio
    async def test_fetch_invalid_time_range(self):
        """测试使用无效时间范围调用fetch方法"""
        # 测试超过30天的时间范围
        import time
        now_ts_ms = int(time.time() * 1000)
        start_ts_ms = now_ts_ms - 31 * 24 * 60 * 60 * 1000  # 31天前

        with pytest.raises(ValueError):
            await self.data_source.fetch(
                symbol="BTCUSDT",
                timeframe="1d",
                start_ts_ms=start_ts_ms
            )

    @pytest.mark.asyncio
    async def test_fetch_end_before_start(self):
        """测试结束时间早于开始时间的情况"""
        import time
        now_ts_ms = int(time.time() * 1000)
        start_ts_ms = now_ts_ms - 1 * 24 * 60 * 60 * 1000  # 1天前
        end_ts_ms = start_ts_ms - 1000  # 早于开始时间

        with pytest.raises(ValueError):
            await self.data_source.fetch(
                symbol="BTCUSDT",
                timeframe="1d",
                start_ts_ms=start_ts_ms,
                end_ts_ms=end_ts_ms
            )

    @pytest.mark.asyncio
    async def test_fetch_invalid_symbol(self):
        """测试使用无效交易对调用fetch方法"""
        # 模拟交易所信息
        mock_exchange_info = {
            "symbols": [
                {"symbol": "BTCUSDT"}
            ]
        }

        with patch.object(self.data_source.client, 'exchange_info') as mock_exchange_info_method:
            mock_exchange_info_method.return_value = mock_exchange_info

            import time
            now_ts_ms = int(time.time() * 1000)
            start_ts_ms = now_ts_ms - 1 * 24 * 60 * 60 * 1000  # 1天前

            with pytest.raises(ValueError):
                await self.data_source.fetch(
                    symbol="INVALIDSYMBOL",
                    timeframe="1d",
                    start_ts_ms=start_ts_ms
                )

    @pytest.mark.asyncio
    async def test_fetch_api_error(self):
        """测试fetch方法API错误情况"""
        # 模拟交易所信息
        mock_exchange_info = {
            "symbols": [
                {"symbol": "BTCUSDT"}
            ]
        }

        with patch.object(self.data_source.client, 'exchange_info') as mock_exchange_info_method:
            mock_exchange_info_method.return_value = mock_exchange_info

            with patch.object(self.data_source.client, 'open_interest_hist') as mock_oi_method:
                mock_oi_method.side_effect = Exception("API Error")

                import time
                now_ts_ms = int(time.time() * 1000)
                start_ts_ms = now_ts_ms - 1 * 24 * 60 * 60 * 1000  # 1天前

                # 调用fetch方法，应该返回None
                result = await self.data_source.fetch(
                    symbol="BTCUSDT",
                    timeframe="1d",
                    start_ts_ms=start_ts_ms
                )

                assert result is None

    @pytest.mark.asyncio
    async def test_tickers(self):
        """测试tickers方法"""
        mock_tickers = [
            {"symbol": "BTCUSDT", "price": "50000.00"},
            {"symbol": "ETHUSDT", "price": "3000.00"}
        ]

        with patch.object(self.data_source.client, 'ticker_price') as mock_ticker_price_method:
            mock_ticker_price_method.return_value = mock_tickers

            result = await self.data_source.tickers()

            assert result is not None
            assert isinstance(result, dict)
            assert len(result) == 2
            assert "BTC/USDT" in result

    @pytest.mark.asyncio
    async def test_async_context_manager(self):
        """测试异步上下文管理器"""
        async with CryptoUMFutureDataSource() as manager:
            assert manager is not None
            assert isinstance(manager, CryptoUMFutureDataSource)

    @pytest.mark.asyncio
    async def test_fetch_with_slash_symbol(self):
        """测试使用带斜杠的交易对符号"""
        # 模拟交易所信息
        mock_exchange_info = {
            "symbols": [
                {"symbol": "BTCUSDT"}
            ]
        }

        # 模拟API响应
        mock_oi_hist = [
            {
                "symbol": "BTCUSDT",
                "sumOpenInterest": "20403.63700000",
                "sumOpenInterestValue": "150570784.07809979",
                "CMCCirculatingSupply": "165880.538",
                "timestamp": "1761436800000"
            }
        ]

        mock_taker_ratio = [
            {
                "buySellRatio": "1.5586",
                "buyVol": "387.3300",
                "sellVol": "248.5030",
                "timestamp": "1761436800000"
            }
        ]

        mock_top_position_ratio = [
            {
                "symbol": "BTCUSDT",
                "longShortRatio": "1.4342",
                "longAccount": "0.5891",
                "shortAccount": "0.4108",
                "timestamp": "1761436800000"
            }
        ]

        mock_top_account_ratio = [
            {
                "symbol": "BTCUSDT",
                "longShortRatio": "1.8105",
                "longAccount": "0.6442",
                "shortAccount": "0.3558",
                "timestamp": "1761436800000"
            }
        ]

        mock_global_account_ratio = [
            {
                "symbol": "BTCUSDT",
                "longShortRatio": "1.9559",
                "longAccount": "0.6617",
                "shortAccount": "0.3382",
                "timestamp": "1761436800000"
            }
        ]

        # 设置模拟
        with patch.object(
                self.data_source.client, 'exchange_info'
        ) as mock_exchange_info_method:
            mock_exchange_info_method.return_value = mock_exchange_info

            with patch.object(
                    self.data_source.client, 'open_interest_hist'
            ) as mock_oi_method:
                mock_oi_method.return_value = mock_oi_hist

                with patch.object(
                        self.data_source.client, 'taker_long_short_ratio'
                ) as mock_taker_method:
                    mock_taker_method.return_value = mock_taker_ratio

                    with patch.object(
                            self.data_source.client,
                            'top_long_short_position_ratio'
                    ) as mock_top_position_method:
                        mock_top_position_method.return_value = \
                            mock_top_position_ratio

                        with patch.object(
                                self.data_source.client,
                                'top_long_short_account_ratio'
                        ) as mock_top_account_method:
                            mock_top_account_method.return_value = \
                                mock_top_account_ratio

                            with patch.object(
                                    self.data_source.client,
                                    'long_short_account_ratio'
                            ) as mock_global_account_method:
                                mock_global_account_method.return_value = \
                                    mock_global_account_ratio

                                # 计算30天内的时间戳
                                import time
                                now_ts_ms = int(time.time() * 1000)
                                start_ts_ms = now_ts_ms - 15 * 24 * 60 * 60 * 1000  # 15天前
                                end_ts_ms = now_ts_ms - 1000

                                # 调用fetch方法，使用带斜杠的符号
                                result = await self.data_source.fetch(
                                    symbol="BTC/USDT",  # 带斜杠
                                    timeframe="1d",
                                    start_ts_ms=start_ts_ms,
                                    end_ts_ms=end_ts_ms
                                )

                                # 验证结果
                                assert result is not None
                                assert isinstance(result, pd.DataFrame)
