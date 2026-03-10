import pytest
from unittest.mock import Mock, patch
import pandas as pd
import time
from chronoforge.data_source import AlthernativeDataSource


class TestAlthernativeDataSource:
    """测试替代数据源"""

    def setup_method(self):
        """测试方法设置"""
        self.data_source = AlthernativeDataSource()

    def test_initialization(self):
        """测试初始化"""
        assert self.data_source is not None
        assert self.data_source.name == "Althernative"

    @pytest.mark.asyncio
    async def test_bitcoin_fgi(self):
        """测试bitcoin_fgi方法"""
        mock_response = {
            "data": [
                {
                    "value": "51",
                    "value_classification": "Neutral",
                    "timestamp": "1761523200",
                    "time_until_update": "66568"
                }
            ]
        }

        with patch('requests.get') as mock_get:
            mock_response_obj = Mock()
            mock_response_obj.status_code = 200
            mock_response_obj.json.return_value = mock_response
            mock_get.return_value = mock_response_obj

            result = await self.data_source.bitcoin_fgi(
                start_ts_ms=1761436800000
            )

            assert result is not None
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1
            assert "time" in result.columns
            assert "value" in result.columns
            assert "value_classification" in result.columns

    @pytest.mark.asyncio
    async def test_bitcoin_fgi_empty_data(self):
        """测试bitcoin_fgi方法返回空数据的情况"""
        mock_response = {"data": []}

        with patch('requests.get') as mock_get:
            mock_response_obj = Mock()
            mock_response_obj.status_code = 200
            mock_response_obj.json.return_value = mock_response
            mock_get.return_value = mock_response_obj

            result = await self.data_source.bitcoin_fgi(
                start_ts_ms=1761436800000
            )

            assert result is None

    @pytest.mark.asyncio
    async def test_bitcoin_fgi_api_error(self):
        """测试bitcoin_fgi方法API错误情况"""
        with patch('requests.get') as mock_get:
            mock_response_obj = Mock()
            mock_response_obj.status_code = 404
            mock_get.return_value = mock_response_obj

            result = await self.data_source.bitcoin_fgi(
                start_ts_ms=1761436800000
            )

            assert result is not None
            assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_tickers(self):
        """测试tickers方法"""
        mock_response = {
            "data": {
                "1": {
                    "id": 1,
                    "name": "Bitcoin",
                    "symbol": "BTC",
                    "slug": "bitcoin",
                    "rank": 1,
                    "price_usd": "50000.00",
                    "percent_change_24h": "1.5"
                }
            }
        }

        with patch('requests.get') as mock_get:
            mock_response_obj = Mock()
            mock_response_obj.status_code = 200
            mock_response_obj.json.return_value = mock_response
            mock_get.return_value = mock_response_obj

            result = await self.data_source.tickers()

            assert result is not None
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1

    @pytest.mark.asyncio
    async def test_tickers_api_error(self):
        """测试tickers方法API错误情况"""
        with patch('requests.get') as mock_get:
            mock_response_obj = Mock()
            mock_response_obj.status_code = 404
            mock_get.return_value = mock_response_obj

            with pytest.raises(IOError):
                await self.data_source.tickers()

    @pytest.mark.asyncio
    async def test_crypto_global_market(self):
        """测试crypto_global_market方法"""
        mock_response = {
            "data": {
                "active_cryptocurrencies": 1000,
                "active_markets": 500,
                "bitcoin_percentage_of_market_cap": 50.0,
                "quotes": {
                    "USD": {
                        "total_market_cap": 1000000000000,
                        "total_volume_24h": 50000000000
                    }
                },
                "last_updated": 1761436800
            }
        }

        with patch('requests.get') as mock_get:
            mock_response_obj = Mock()
            mock_response_obj.status_code = 200
            mock_response_obj.json.return_value = mock_response
            mock_get.return_value = mock_response_obj

            result = await self.data_source.crypto_global_market()

            assert result is not None
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1

    @pytest.mark.asyncio
    async def test_crypto_global_market_api_error(self):
        """测试crypto_global_market方法API错误情况"""
        with patch('requests.get') as mock_get:
            mock_response_obj = Mock()
            mock_response_obj.status_code = 404
            mock_get.return_value = mock_response_obj

            with pytest.raises(IOError):
                await self.data_source.crypto_global_market()

    @pytest.mark.asyncio
    async def test_async_context_manager(self):
        """测试异步上下文管理器"""
        async with AlthernativeDataSource() as manager:
            assert manager is not None
            assert isinstance(manager, AlthernativeDataSource)

    @pytest.mark.asyncio
    async def test_cache_expiration(self):
        """测试缓存过期机制"""
        mock_response = {
            "data": {
                "1": {
                    "id": 1,
                    "name": "Bitcoin",
                    "symbol": "BTC",
                    "slug": "bitcoin",
                    "rank": 1,
                    "price_usd": "50000.00",
                    "percent_change_24h": "1.5"
                }
            }
        }

        with patch('requests.get') as mock_get:
            mock_response_obj = Mock()
            mock_response_obj.status_code = 200
            mock_response_obj.json.return_value = mock_response
            mock_get.return_value = mock_response_obj

            await self.data_source.tickers()
            first_update_time = self.data_source.last_tickers_update

            self.data_source.last_tickers_update = time.time() - 300

            await self.data_source.tickers()
            second_update_time = self.data_source.last_tickers_update

            assert second_update_time > first_update_time
