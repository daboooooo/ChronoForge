import logging
import pandas as pd
import requests
import time
from typing import Any, Dict, Optional

from .base import DataSourceBase
from chronoforge.utils import with_retry

logger = logging.getLogger(__name__)


class BitcoinFGIDataSource(DataSourceBase):
    """比特币FGI数据源插件，支持获取比特币FGI数据"""

    def __init__(self, config: Dict[str, Any] = None):
        """初始化比特币FGI插件

        Args:
            config: None
        """
        super().__init__(config)
        self.cached_tickers = None
        self.last_tickers_update = 0
        self.tickers_cache_duration = 3 * 60  # 3分钟，单位：秒, Althernative.me 5分钟更新一次

        self.cached_global_market = None
        self.last_global_market_update = 0
        self.global_market_cache_duration = 3 * 60

    @property
    def name(self):
        """返回数据源名称"""
        return self.__class__.__name__.replace("DataSource", "")

    def __del__(self):
        """析构函数，清理资源"""
        pass

    async def __aenter__(self):
        """异步上下文管理器的进入方法"""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器的退出方法"""
        return False

    @with_retry
    async def fetch(
        self,
        symbol: str,
        timeframe: str,
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
        # 检查参数是否有效
        if symbol and symbol != "bitcoin_fgi":
            raise ValueError(f"Invalid symbol: {symbol}. Expected 'bitcoin_fgi'.")
        if timeframe != "1d":
            raise ValueError(f"Invalid timeframe: {timeframe}. Expected '1d'.")

        # 转换时间戳格式为FRED API要求的格式（YYYY-MM-DD）
        start_time = pd.Timestamp(start_ts_ms, unit='ms', tz='UTC').strftime('%Y-%m-%d')
        end_time = pd.Timestamp.now(tz='UTC').strftime('%Y-%m-%d')

        # 计算limit
        limit = (pd.Timestamp(end_time) - pd.Timestamp(start_time)).days

        logger.info(f"Fetching {self.name} for symbol: {symbol}, timeframe: {timeframe}, "
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
            logger.info(f"Fetched {len(df)} bars for symbol: {symbol}")

        except Exception as e:
            logger.warning(f"获取比特币FGI数据失败: {e}")
            df = pd.DataFrame(columns=['time', 'value', 'value_classification'])

        return df

    async def close_all_connections(self):
        """关闭所有与数据源的连接"""
        pass

    @with_retry
    async def tickers(self, **kwargs) -> Any:
        """从alternative.me获取加密货币ticker数据，并缓存5分钟

        Args:
            **kwargs: 可选参数，不影响当前实现

        Returns:
            Any: 包含加密货币ticker数据的字典，结构与API返回一致
        """
        # 检查缓存是否有效
        current_time = time.time()
        if self.cached_tickers and ((current_time - self.last_tickers_update) <
                                    self.tickers_cache_duration):
            logger.debug(f"使用缓存的ticker数据，距离上次更新: {int(current_time - self.last_tickers_update)}秒")
        else:
            logger.info("从 alternative.me 获取最新ticker数据")

            # 从API获取数据
            url = "https://api.alternative.me/v2/ticker/?limit=0"
            result = requests.get(url, timeout=30)

            if result.status_code != 200:
                self.cached_tickers = None
                raise IOError(f"从 alternative.me 获取ticker数据失败，状态码: {result.status_code}")

            # 解析数据
            ticker_data = result.json()

            # 更新缓存
            self.cached_tickers = ticker_data['data']
            self.last_tickers_update = current_time

            logger.info(f"成功从 alternative.me 获取并缓存{len(self.cached_tickers)}个ticker数据")

        return self.cached_tickers

    @with_retry
    async def crypto_global_market(self, **kwargs) -> pd.DataFrame:
        """从alternative.me获取加密货币全球市场数据

        Args:
            **kwargs: 可选参数，不影响当前实现

        Returns:
            pd.DataFrame: 包含加密货币全球市场数据的DataFrame，
                列名包括'time', 'volume'
        """
        # 检查缓存是否有效
        current_time = time.time()
        gap_seconds = current_time - self.last_global_market_update
        if self.cached_global_market and (gap_seconds < self.global_market_cache_duration):
            logger.debug(f"使用缓存的全球市场数据，距离上次更新: {int(gap_seconds)}秒")
            return self.cached_global_market

        logger.info("从 alternative.me 获取最新全球加密货币市场数据")

        # 从API获取数据
        url = "https://api.alternative.me/v2/global/"
        result = requests.get(url, timeout=30)

        if result.status_code != 200:
            raise IOError(f"从 alternative.me 获取全球市场数据失败，状态码: {result.status_code}")

        # 解析数据
        global_market_data = result.json()

        # 更新缓存
        self.cached_global_market = global_market_data
        self.last_global_market_update = current_time

        logger.info("成功从 alternative.me 获取并缓存全球市场数据")

        return global_market_data
