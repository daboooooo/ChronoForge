import pytest
from unittest.mock import Mock, patch
import pandas as pd
from chronoforge.data_source import CoinGeckoDataSource
from chronoforge.data_source.coingecko import (
    get_coingecko_coin_markets_tops,
    get_coingecko_coin_categories,
    get_coingecko_tops
)


class TestCoinGeckoDataSource:
    """测试CoinGecko数据源"""

    def setup_method(self):
        """测试方法设置"""
        self.data_source = CoinGeckoDataSource()

    def test_initialization(self):
        """测试初始化"""
        assert self.data_source is not None
        assert self.data_source.name == "CoinGecko"

    @pytest.mark.asyncio
    async def test_fetch(self):
        """测试fetch方法 - 非特殊symbol返回coin_markets数据"""
        result = await self.data_source.fetch(
            symbol="BTC",
            timeframe="1d",
            start_ts_ms=1761436800000
        )
        # 非特殊symbol会返回coin_markets数据
        assert result is not None
        assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_get_coin_markets(self):
        """测试get_coin_markets方法"""
        mock_coin_markets = [{"symbol": "BTC", "name": "Bitcoin", "market_cap": 553.16}]

        with patch(
                'chronoforge.data_source.coingecko.get_coingecko_coin_markets_tops'
        ) as mock_get_markets:
            mock_get_markets.return_value = mock_coin_markets

            result = await self.data_source.get_coin_markets()

            assert result is not None
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1

    @pytest.mark.asyncio
    async def test_get_coin_categories(self):
        """测试get_coin_categories方法"""
        mock_coin_categories = [
            {"id": "centralized-exchange-token-cex", "name": "Centralized Exchange (CEX)"}
        ]

        with patch(
                'chronoforge.data_source.coingecko.get_coingecko_coin_categories'
        ) as mock_get_categories:
            mock_get_categories.return_value = mock_coin_categories

            result = await self.data_source.get_coin_categories()

            assert result is not None
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1

    @pytest.mark.asyncio
    async def test_get_tops(self):
        """测试get_tops方法"""
        mock_tops = [{"symbol": "BTC", "name": "Bitcoin", "market_cap_rank": 1}]

        with patch(
                'chronoforge.data_source.coingecko.get_coingecko_tops'
        ) as mock_get_tops:
            mock_get_tops.return_value = mock_tops

            result = await self.data_source.get_tops()

            assert result is not None
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1


class TestCoinGeckoHelperFunctions:
    """测试CoinGecko辅助函数"""

    @pytest.mark.asyncio
    async def test_get_coingecko_coin_markets_tops(self):
        """测试get_coingecko_coin_markets_tops函数"""
        mock_coins = [
            {
                "id": "bitcoin",
                "symbol": "btc",
                "name": "Bitcoin",
                "image": "https://assets.coingecko.com/coins/images/4463/btc.png",
                "current_price": 50000,
                "market_cap": 950000000000,
                "fully_diluted_valuation": 1000000000000
            }
        ]

        with patch('chronoforge.data_source.coingecko.CoinGeckoAPI') as mock_cg_class:
            mock_cg_instance = Mock()
            mock_cg_class.return_value = mock_cg_instance
            mock_cg_instance.get_coins_markets.return_value = mock_coins

            result = await get_coingecko_coin_markets_tops()

            assert result is not None
            assert isinstance(result, list)
            assert len(result) > 0
            assert result[0]["symbol"] == "BTC"
            assert "image_id" in result[0]
            assert result[0]["image_id"] == "4463"

    @pytest.mark.asyncio
    async def test_get_coingecko_coin_categories(self):
        """测试get_coingecko_coin_categories函数"""
        mock_categories = [
            {
                "id": "centralized-exchange-token-cex",
                "name": "Centralized Exchange (CEX)",
                "market_cap": 64518287373.88498,
                "market_cap_change_24h": -1.278955708563099,
                "top_3_coins": ["https://assets.coingecko.com/coins/images/4463/btc.png"],
                "volume_24h": 1778642173.2336328
            }
        ]

        mock_coin_markets = [
            {
                "symbol": "BTC",
                "name": "Bitcoin",
                "image_id": "4463"
            }
        ]

        with patch('chronoforge.data_source.coingecko.CoinGeckoAPI') as mock_cg_class:
            mock_cg_instance = Mock()
            mock_cg_class.return_value = mock_cg_instance
            mock_cg_instance.get_coins_categories.return_value = mock_categories

            result = await get_coingecko_coin_categories(coin_markets=mock_coin_markets)

            assert result is not None
            assert isinstance(result, list)
            assert len(result) == 1
            assert "top_3_coins_symbol" in result[0]
            assert "top_3_coins_name" in result[0]
            assert result[0]["top_3_coins_symbol"] == ["BTC"]
            assert result[0]["top_3_coins_name"] == ["Bitcoin"]

    @pytest.mark.asyncio
    async def test_get_coingecko_tops(self):
        """测试get_coingecko_tops函数"""
        mock_tops = {
            "coins": [
                {
                    "item": {
                        "symbol": "btc",
                        "name": "Bitcoin",
                        "market_cap_rank": 1
                    }
                }
            ]
        }

        with patch('chronoforge.data_source.coingecko.CoinGeckoAPI') as mock_cg_class:
            mock_cg_instance = Mock()
            mock_cg_class.return_value = mock_cg_instance
            mock_cg_instance.get_search_trending.return_value = mock_tops

            result = await get_coingecko_tops()

            assert result is not None
            assert isinstance(result, list)
            assert len(result) == 1
            assert result[0]["symbol"] == "btc"
            assert result[0]["name"] == "Bitcoin"
            assert result[0]["market_cap_rank"] == 1

    @pytest.mark.asyncio
    async def test_get_coingecko_coin_markets_tops_with_api_error(self):
        """测试get_coingecko_coin_markets_tops函数API错误情况"""
        with patch('chronoforge.data_source.coingecko.CoinGeckoAPI') as mock_cg_class:
            mock_cg_instance = Mock()
            mock_cg_class.return_value = mock_cg_instance
            mock_cg_instance.get_coins_markets.side_effect = Exception("API Error")

            with pytest.raises(Exception):
                await get_coingecko_coin_markets_tops()

    @pytest.mark.asyncio
    async def test_get_coingecko_coin_categories_with_api_error(self):
        """测试get_coingecko_coin_categories函数API错误情况"""
        with patch('chronoforge.data_source.coingecko.CoinGeckoAPI') as mock_cg_class:
            mock_cg_instance = Mock()
            mock_cg_class.return_value = mock_cg_instance
            mock_cg_instance.get_coins_categories.side_effect = Exception("API Error")

            with pytest.raises(Exception):
                await get_coingecko_coin_categories()

    @pytest.mark.asyncio
    async def test_get_coingecko_tops_with_api_error(self):
        """测试get_coingecko_tops函数API错误情况"""
        with patch('chronoforge.data_source.coingecko.CoinGeckoAPI') as mock_cg_class:
            mock_cg_instance = Mock()
            mock_cg_class.return_value = mock_cg_instance
            mock_cg_instance.get_search_trending.side_effect = Exception("API Error")

            with pytest.raises(Exception):
                await get_coingecko_tops()
