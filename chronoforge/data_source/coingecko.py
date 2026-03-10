import logging
import asyncio
import time
from typing import Any, Optional, Dict
from pycoingecko import CoinGeckoAPI
import pandas as pd
from .base import DataSourceBase
from chronoforge.utils import to_human_readable_format
from chronoforge.decorators import with_retry, api_callable

logger = logging.getLogger(__name__)


async def get_coingecko_coin_markets_tops(api_key: str = '', retries: int = 3):
    """
    https://www.coingecko.com/en/api/documentation
    Get coin markets from Coingecko. Return data's structure:
    [{
        "id": "bitcoin",
        "symbol": "btc",
        "name": "Bitcoin",
        "image": "https://assets.coingecko.com/coins/images/4463/...",,
        "current_price": 28622,
        "market_cap": 553157571676,
        "market_cap_rank": 1,
        "fully_diluted_valuation": 600832391680,
        "total_volume": 19178327772,
        "high_24h": 28654,
        "low_24h": 27626,
        "price_change_24h": 673.94,
        "price_change_percentage_24h": 2.41141,
        "market_cap_change_24h": 12923205781,
        "market_cap_change_percentage_24h": 2.39215,
        "circulating_supply": 19333693,
        "total_supply": 21000000,
        "max_supply": 21000000,
        "ath": 69045,
        "ath_change_percentage": -58.55628,
        "ath_date": "2021-11-10T14:24:11.849Z",
        "atl": 67.81,
        "atl_change_percentage": 42099.00124,
        "atl_date": "2013-07-06T00:00:00.000Z",
        "roi": null,
        "last_updated": "2023-03-31T23:07:49.494Z"
    }]
    """
    cg = CoinGeckoAPI(api_key=api_key, retries=retries)
    coin_markets: list[dict[str, Any]] = []
    coin_markets = cg.get_coins_markets(vs_currency='usd', per_page=200, page=4)
    await asyncio.sleep(2)
    coin_markets.extend(cg.get_coins_markets(vs_currency='usd', per_page=200, page=3))
    await asyncio.sleep(2)
    coin_markets.extend(cg.get_coins_markets(vs_currency='usd', per_page=200, page=2))
    await asyncio.sleep(2)
    coin_markets.extend(cg.get_coins_markets(vs_currency='usd', per_page=200, page=1))
    await asyncio.sleep(2)
    for coin in coin_markets:
        coin['symbol'] = coin['symbol'].upper()
        try:
            market_cap = int(coin['market_cap'])
        except TypeError:
            market_cap = 0
        # coin['market_cap'] = to_human_readable_format(market_cap, unit='USD')
        coin['market_cap'] = round(market_cap / 1000000000, 4)
        try:
            fully_diluted_valuation = int(coin['fully_diluted_valuation'])
        except TypeError:
            fully_diluted_valuation = 0
        # coin['fully_diluted_valuation'] = to_human_readable_format(
        #     fully_diluted_valuation, unit='USD')
        coin['fully_diluted_valuation'] = round(fully_diluted_valuation / 1000000000, 4)
        if 'coins/images/' in str(coin['image']):
            coin['image_id'] = str(coin['image']).split(
                'coins/images/')[1].split('/')[0]
        else:
            # logger.error(
            #     f"{coin['symbol']} do not have a valid image url: {coin['image']}")
            coin['image_id'] = '0'
    return coin_markets


async def get_coingecko_coin_categories(api_key: str = '',
                                        retries: int = 3,
                                        coin_markets: Optional[list[dict[str, Any]]] = None):
    """
    https://www.coingecko.com/en/api/documentation
    [{
            "id": "centralized-exchange-token-cex",
            "name": "Centralized Exchange (CEX)",
            "market_cap": 64518287373.88498,
            "market_cap_change_24h": -1.278955708563099,
            "content": "These utility tokens are issued by a centralized exchange...",
            "top_3_coins": [
                "https://assets.coingecko.com/coins/images/825/...",
                "https://assets.coingecko.com/coins/images/8418/...",
                "https://assets.coingecko.com/coins/images/4463/..."
            ],
            "volume_24h": 1778642173.2336328,
            "updated_at": "2023-04-28T22:20:11.429Z"
    }]

    note:
    The id in top_3_coins field is the same as the id in coin markets image field.
    Example: 4463 is the id of OKB.
    """
    cg = CoinGeckoAPI(api_key=api_key, retries=retries)
    coin_categories: list[dict[str, Any]] = []
    coin_categories = cg.get_coins_categories(order='market_cap_change_24h_desc')
    await asyncio.sleep(2)
    for category in coin_categories:
        top_3_coins_symbol = []
        top_3_coins_name = []
        for coin_image in category['top_3_coins']:
            coin_symbol = '0'
            coin_name = '0'
            if 'coins/images/' in str(coin_image) and coin_markets is not None:
                coin_image_id = str(coin_image).split('coins/images/')[1].split('/')[0]
                # update coin_symbol and coin_name according to coin_markets
                for coin in coin_markets:
                    if coin_image_id == coin['image_id']:
                        coin_symbol = coin['symbol']
                        coin_name = coin['name']
                        break
            # else:
            #     logger.warning(f"{category} do not have a valid image url.\n" +
            #                    f"{category['top_3_coins']}")
            top_3_coins_symbol.append(coin_symbol)
            top_3_coins_name.append(coin_name)
        category['top_3_coins_symbol'] = top_3_coins_symbol
        category['top_3_coins_name'] = top_3_coins_name
        try:
            market_cap = int(category['market_cap'])
        except TypeError:
            market_cap = 0
        category['market_cap'] = to_human_readable_format(market_cap, unit='USD')
        try:
            market_cap_change_24h = int(category['market_cap_change_24h'])
        except TypeError:
            market_cap_change_24h = 0
        category['market_cap_change_24h'] = to_human_readable_format(
            market_cap_change_24h, unit='USD')
        try:
            volume_24h = int(category['volume_24h'])
        except TypeError:
            volume_24h = 0
        category['volume_24h'] = to_human_readable_format(volume_24h, unit='USD')
    return coin_categories


async def get_coingecko_tops(api_key: str = '', retries: int = 3):
    cg = CoinGeckoAPI(api_key=api_key, retries=retries)
    tops_data = cg.get_search_trending()
    await asyncio.sleep(2)
    tops: list[Any] = []
    for item in tops_data['coins']:
        tops.append({
            'symbol': item['item']['symbol'],
            'name': item['item']['name'],
            'market_cap_rank': item['item']['market_cap_rank']})
    return tops


class CoinGeckoDataSource(DataSourceBase):
    """CoinGecko数据源插件，支持获取CoinGecko数据"""

    def __init__(self, config: Dict[str, Any] = None):
        """初始化CoinGecko插件

        Args:
            config: None
        """
        super().__init__(config)
        self.coin_markets = None
        self.last_coin_markets_update = 0
        self.coin_markets_cache_duration = 30 * 60  # 30分钟，单位：秒

        self.coin_categories = None
        self.last_coin_categories_update = 0
        self.coin_categories_cache_duration = 30 * 60  # 30分钟，单位：秒

        self.tops = None
        self.last_tops_update = 0
        self.tops_cache_duration = 30 * 60  # 30分钟，单位：秒

    async def fetch(self, symbol: str, timeframe: str, start_ts_ms: int,
                    end_ts_ms: Optional[int] = None) -> pd.DataFrame:
        if symbol == 'coin_markets' or symbol == 'markets':
            return await self.get_coin_markets()
        elif symbol == 'coin_categories' or symbol == 'categories':
            return await self.get_coin_categories()
        elif symbol == 'tops' or symbol == 'trending':
            return await self.get_tops()
        else:
            return await self.get_coin_markets()

    @api_callable
    @with_retry
    async def get_coin_markets(self) -> pd.DataFrame:
        """获取CoinGecko加密货币市场数据

        Returns:
            pd.DataFrame: 包含加密货币市场数据的DataFrame，
                列名包括'id', 'symbol', 'name', 'image', 'current_price',
                'market_cap', 'market_cap_rank', 'fully_diluted_valuation',
                'total_volume', 'high_24h', 'low_24h', 'price_change_24h',
                'price_change_percentage_24h', 'market_cap_change_24h',
                'market_cap_change_percentage_24h', 'circulating_supply',
                'total_supply', 'max_supply', 'ath', 'ath_change_percentage',
                'ath_date', 'atl', 'atl_change_percentage', 'atl_date',
                'last_updated', 'image_id'
        """
        current_time = time.time()
        if self.coin_markets is not None and not self.coin_markets.empty and (
            (current_time - self.last_coin_markets_update) < self.coin_markets_cache_duration
        ):
            logger.debug(
                f"使用缓存的coin_markets数据，距上次更新: "
                f"{int(current_time - self.last_coin_markets_update)}秒"
            )
            return self.coin_markets

        coin_markets_data = await get_coingecko_coin_markets_tops()
        df = pd.DataFrame(coin_markets_data)
        if not df.empty:
            df.set_index('symbol', inplace=True)

        self.coin_markets = df
        self.last_coin_markets_update = current_time
        logger.info(f"成功获取并转换{len(df)}条coin_markets数据")

        return self.coin_markets

    @api_callable
    @with_retry
    async def get_coin_categories(self) -> pd.DataFrame:
        """获取CoinGecko加密货币分类数据

        Returns:
            pd.DataFrame: 包含加密货币分类数据的DataFrame，
                列名包括'id', 'name', 'market_cap', 'market_cap_change_24h',
                'content', 'volume_24h', 'updated_at', 'top_3_coins_symbol',
                'top_3_coins_name'
        """
        current_time = time.time()
        if self.coin_categories is not None and not self.coin_categories.empty and (
            (current_time - self.last_coin_categories_update) < self.coin_categories_cache_duration
        ):
            logger.debug(
                f"使用缓存的coin_categories数据，距上次更新: "
                f"{int(current_time - self.last_coin_categories_update)}秒"
            )
            return self.coin_categories

        coin_categories_data = await get_coingecko_coin_categories(
            coin_markets=self.coin_markets.to_dict('records')
            if self.coin_markets is not None else None
        )
        df = pd.DataFrame(coin_categories_data)

        self.coin_categories = df
        self.last_coin_categories_update = current_time
        logger.info(f"成功获取并转换{len(df)}条coin_categories数据")

        return self.coin_categories

    @api_callable
    @with_retry
    async def get_tops(self) -> pd.DataFrame:
        """获取CoinGecko热门搜索数据

        Returns:
            pd.DataFrame: 包含热门加密货币数据的DataFrame，
                列名包括'symbol', 'name', 'market_cap_rank'
        """
        current_time = time.time()
        if self.tops is not None and not self.tops.empty and (
            (current_time - self.last_tops_update) < self.tops_cache_duration
        ):
            logger.debug(f"使用缓存的tops数据，距离上次更新: {int(current_time - self.last_tops_update)}秒")
            return self.tops

        tops_data = await get_coingecko_tops()
        df = pd.DataFrame(tops_data)

        self.tops = df
        self.last_tops_update = current_time
        logger.info(f"成功获取并转换{len(df)}条tops数据")

        return self.tops
