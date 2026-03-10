import logging
import pandas as pd
import requests
import time
from typing import Any, Dict, Optional

from .base import DataSourceBase
from chronoforge.decorators import with_retry

logger = logging.getLogger(__name__)


class AlthernativeDataSource(DataSourceBase):
    """替代数据源插件，支持获取替代数据。Althernative.me 数据每5分钟更新一次"""

    def __init__(self, config: Dict[str, Any] = None):
        """初始化替代插件

        Args:
            config: None
        """
        super().__init__(config)
        self.cached_bitcoin_fgi = None
        self.last_bitcoin_fgi_update = 0
        self.bitcoin_fgi_cache_duration = 60 * 60  # 60分钟，单位：秒, Althernative.me 5分钟更新一次

        self.cached_tickers = None
        self.last_tickers_update = 0
        self.tickers_cache_duration = 3 * 60  # 3分钟，单位：秒, Althernative.me 5分钟更新一次

        self.cached_global_market = None
        self.last_global_market_update = 0
        self.global_market_cache_duration = 3 * 60  # 3分钟，单位：秒, Althernative.me 5分钟更新一次

    async def __aenter__(self):
        """异步上下文管理器的进入方法"""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器的退出方法"""
        return False

    async def fetch(
        self,
        symbol: str,
        timeframe: str,
        start_ts_ms: int,
        end_ts_ms: Optional[int] = None
    ) -> pd.DataFrame:
        pass

    @with_retry
    async def bitcoin_fgi(
        self,
        start_ts_ms: int,
        end_ts_ms: Optional[int] = None
    ) -> pd.DataFrame:
        """从alternative.me获取比特币FGI数据

        Args:
            symbol: 比特币FGI指标symbol，如'bitcoin_fgi'
            timeframe: 时间粒度，'1d'
            start_ts_ms: 开始时间戳（毫秒）
            end_ts_ms: 结束时间戳（毫秒）, 默认为当前时间

        Returns:
            pandas.DataFrame: 包含时间序列数据的DataFrame，
                列名包括'time', 'volume', 'classification'
                索引为时间戳（毫秒级），
                时区为UTC
        """
        current_time = time.time()
        if self.cached_bitcoin_fgi and ((current_time - self.last_bitcoin_fgi_update) <
                                        self.bitcoin_fgi_cache_duration):
            logger.debug("使用缓存的Bitcoin FGI数据，距离上次更新: "
                         f"{int(current_time - self.last_bitcoin_fgi_update)}秒")
            return self.cached_bitcoin_fgi

        # 转换时间戳格式为FRED API要求的格式（YYYY-MM-DD）
        start_time = pd.Timestamp(start_ts_ms, unit='ms', tz='UTC').strftime('%Y-%m-%d')
        end_time = pd.Timestamp.now(tz='UTC').strftime('%Y-%m-%d')

        # 计算limit
        limit = (pd.Timestamp(end_time) - pd.Timestamp(start_time)).days

        logger.info(f"Fetching Bitcoin FGI, "
                    f"start_ts_ms: {start_ts_ms}, end_ts_ms: {end_ts_ms}")

        # 连续下载数据，直到获取全部数据或达到目标时间范围
        try:
            """
            Get Bitcoin Fear and Greed Index from alternative.me. Return data's structure:
                [
                    {
                        "value": "51",
                        "value_classification": "Neutral",
                        "timestamp": "1761523200",
                        "time_until_update": "66568"
                    }
                ]
            """
            url = "https:" + f"//api.alternative.me/fng/?limit={limit}"
            result = requests.get(url, timeout=30)
            if result.status_code != 200:
                raise IOError(f"Can't get Bitcoin Fear and Greed Index <{result.status_code}>")
            else:
                fgi_data = dict(result.json())['data']

            # 检查是否返回了空数据
            if fgi_data is None or len(fgi_data) == 0:
                return None

            # 转换为DataFrame格式, 索引为时间字符串 '%Y-%m-%d'
            # 只要value、value_classification、timestamp三列
            df = pd.DataFrame(fgi_data)
            # timestamp = timestamp - 1d's seconds, 与其它datasource的time对齐
            df['timestamp'] = df['timestamp'].astype(int) - 86400
            # 显式将timestamp字符串转换为整数，避免FutureWarning
            df['time'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
            df = df[['time', 'value', 'value_classification']]
            df.sort_values(by='time', inplace=True)
            df.reset_index(drop=True, inplace=True)

            # 缓存数据
            self.cached_bitcoin_fgi = df
            self.last_bitcoin_fgi_update = current_time
            logger.info(f"Fetched {len(df)} bars for Bitcoin FGI, "
                        f"start_time: {start_time}, end_time: {end_time}")

        except Exception as e:
            logger.warning(f"获取比特币FGI数据失败: {e}")
            df = pd.DataFrame(columns=['time', 'value', 'value_classification'])

        return df

    @with_retry
    async def tickers(self, **kwargs) -> pd.DataFrame:
        """从alternative.me获取加密货币ticker数据

        Args:
            **kwargs: 可选参数，不影响当前实现

        Returns:
            pandas.DataFrame: 包含加密货币ticker数据的DataFrame，
                列名包括'id', 'name', 'symbol', 'website_slug', 'rank',
                'circulating_supply', 'total_supply', 'max_supply',
                'price', 'volume_24h', 'market_cap',
                'percentage_change_1h', 'percentage_change_24h', 'percentage_change_7d',
                'last_updated'
                索引为symbol
        """
        current_time = time.time()
        if self.cached_tickers is not None and (
                (current_time - self.last_tickers_update) <
                self.tickers_cache_duration
        ):
            logger.debug("使用缓存的Crypto ticker数据，距离上次更新: "
                         f"{int(current_time - self.last_tickers_update)}秒")
            return self.cached_tickers

        url = "https://api.alternative.me/v2/ticker/?limit=0"
        result = requests.get(url, timeout=30)

        if result.status_code != 200:
            self.cached_tickers = None
            raise IOError(f"从 alternative.me 获取ticker数据失败，状态码: {result.status_code}")

        ticker_data = result.json()
        raw_data = ticker_data.get('data', {})

        rows = []
        for _, coin in raw_data.items():
            quotes_usd = coin.get('quotes', {}).get('USD', {})
            row = {
                'id': coin.get('id'),
                'name': coin.get('name'),
                'symbol': coin.get('symbol'),
                'website_slug': coin.get('website_slug'),
                'rank': coin.get('rank'),
                'circulating_supply': coin.get('circulating_supply'),
                'total_supply': coin.get('total_supply'),
                'max_supply': coin.get('max_supply'),
                'price': quotes_usd.get('price'),
                'volume_24h': quotes_usd.get('volume_24h'),
                'market_cap': quotes_usd.get('market_cap'),
                'percentage_change_1h': quotes_usd.get('percentage_change_1h'),
                'percentage_change_24h': quotes_usd.get('percentage_change_24h'),
                'percentage_change_7d': quotes_usd.get('percentage_change_7d'),
                'last_updated': coin.get('last_updated')
            }
            rows.append(row)

        df = pd.DataFrame(rows)
        df.set_index('symbol', inplace=True)

        self.cached_tickers = df
        self.last_tickers_update = current_time
        logger.info(f"成功从 alternative.me 获取 {len(df)} 个ticker数据")

        return self.cached_tickers

    @with_retry
    async def crypto_global_market(self, **kwargs) -> pd.DataFrame:
        """从alternative.me获取加密货币全球市场数据

        Returns:
            pd.DataFrame: 包含加密货币全球市场数据的DataFrame，
                列名包括'active_cryptocurrencies', 'active_markets',
                'bitcoin_percentage_of_market_cap', 'total_market_cap',
                'total_volume_24h', 'last_updated', 'metadata_timestamp'
        """
        current_time = time.time()
        gap_seconds = current_time - self.last_global_market_update
        if self.cached_global_market and (gap_seconds < self.global_market_cache_duration):
            logger.debug("使用缓存的Crypto全球市场数据，距离上次更新: "
                         f"{int(gap_seconds)}秒")
            return self.cached_global_market

        url = "https://api.alternative.me/v2/global/"
        result = requests.get(url, timeout=30)

        if result.status_code != 200:
            raise IOError(f"从 alternative.me 获取全球市场数据失败，状态码: {result.status_code}")

        market_data = result.json()
        data = market_data.get('data', {})
        quotes_usd = data.get('quotes', {}).get('USD', {})

        row = {
            'active_cryptocurrencies': data.get('active_cryptocurrencies'),
            'active_markets': data.get('active_markets'),
            'bitcoin_percentage_of_market_cap': data.get('bitcoin_percentage_of_market_cap'),
            'total_market_cap': quotes_usd.get('total_market_cap'),
            'total_volume_24h': quotes_usd.get('total_volume_24h'),
            'last_updated': data.get('last_updated')
        }

        df = pd.DataFrame([row])
        df.set_index('last_updated', inplace=True)

        self.cached_global_market = df
        self.last_global_market_update = current_time
        logger.info("成功从 alternative.me 获取并转换全球市场数据")

        return self.cached_global_market
