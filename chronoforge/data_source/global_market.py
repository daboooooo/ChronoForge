import logging
import pandas as pd
import yfinance as yf
from typing import Any, Dict, Optional

from .base import DataSourceBase
from chronoforge.decorators import with_retry

logger = logging.getLogger(__name__)

GLOBAL_MARKET_CATEGORY = {
    'Commodity': [
        'GC=F',  # Gold
        'SI=F',  # Silver
        'HG=F',  # Copper
        'CL=F',  # Crude Oil
    ],
    'Crypto': [
        'BTC-USD',  # Bitcoin
        'ETH-USD',  # Ethereum
        'SOL-USD',  # Solana
    ],
    'US Stock': [
        "AAPL",  # Apple
        "MSFT",  # Microsoft
        "GOOGL",  # Google
        "AMZN",  # Amazon
        "NVDA",  # NVIDIA
        "META",  # Meta
        "TSLA",  # Tesla
    ],
    'US Indices': [
        '^GSPC',  # S&P 500
        '^RUT',  # Russell 2000
        '^IXIC',  # NASDAQ 100
    ],
    'CN Stock': [
        "000001.SS",  # 上证指数
        "399001.SZ",  # 深证成指
        "399300.SZ",  # 创业板指
        "399006.SZ",  # 科创50
    ],
    'Financial Market': [
        'US02Y',  # 2年国债
        'VIX',  # VIX 指数
        'DXY',  # 美元指数
        'US10Y',  # 10年国债
        '13 Week Treasury Bill',  # 13周国债
    ],
    'Currency': [
        'JPYUSD',  # 日元兑美元
        'CNYUSD',  # 人民币兑美元
        'ERUUSD',  # 欧元兑美元
    ]}


def get_global_market_symbols(category: str) -> list:
    """获取全球市场指定分类的所有symbol

    Args:
        category: 分类名称，如'Commodity', 'Crypto', 'US Stock', 'US Indices',
            'CN Stock', 'Financial Market', 'Currency'

    Returns:
        list: 包含该分类所有symbol的列表
    """
    return GLOBAL_MARKET_CATEGORY.get(category, [])


def get_global_market_all_symbols() -> list:
    """获取全球市场所有symbol

    Returns:
        list: 包含所有分类所有symbol的列表
    """
    return [symbol for category in GLOBAL_MARKET_CATEGORY.values() for symbol in category]


class GlobalMarketDataSource(DataSourceBase):
    """全球市场数据源插件，支持获取YFinance数据"""

    def __init__(self, config: Dict[str, Any] = None):
        """初始化全球市场插件

        Args:
            config: 配置，可选包含 symbols 列表
        """
        super().__init__(config)

        self.default_symbols = get_global_market_all_symbols()

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
        """从FRED获取时间序列数据

        Args:
            symbol: YFinance股票symbol，如'^GSPC'
            timeframe: 时间粒度，如'1m', '5m', '1h', '1d'
            start_ts_ms: 开始时间戳（毫秒）
            end_ts_ms: 结束时间戳（毫秒）, 默认为当前时间

        Returns:
            pandas.DataFrame: 包含时间序列数据的DataFrame，
                列名包括'time', 'volume'
        """
        # 转换时间戳格式为YFinance API要求的格式（YYYY-MM-DD）
        start_time = pd.Timestamp(start_ts_ms, unit='ms', tz='UTC').strftime('%Y-%m-%d')
        if end_ts_ms is None:
            end_time = pd.Timestamp.now(tz='UTC').strftime('%Y-%m-%d')
        else:
            end_time = pd.Timestamp(end_ts_ms, unit='ms', tz='UTC').strftime('%Y-%m-%d')

        # 连续下载数据，直到获取全部数据或达到目标时间范围
        try:
            df: pd.DataFrame = yf.download(
                tickers=symbol,
                start=start_time,
                end=end_time,
                interval=timeframe,

                # group by ticker (to access via data['SPY'])
                # (optional, default is 'column')
                group_by='ticker',

                # adjust all OHLC automatically
                # (optional, default is False)
                auto_adjust=True,

                # download pre/post regular market hours data
                # (optional, default is False)
                prepost=True,

                # use threads for mass downloading? (True/False/Integer)
                # (optional, default is True)
                threads=True,

                # proxy URL scheme use use when downloading?
                # (optional, default is None)
                # proxy=None,

                # disable progress bar
                progress=False
            )
            # remove outside ticker index
            df = df.droplevel(0, axis=1) if isinstance(df.columns, pd.MultiIndex) else df
            # remove column white spaces and convert to lower case
            df.columns = [col.strip().lower() for col in df.columns]
            df['time'] = df.index
            df = df.reset_index()
            df = df[['time', 'open', 'high', 'low', 'close', 'volume']]
            # convert time to datetime
            df['time'] = pd.to_datetime(df['time'], unit='ms', utc=True)
            # 确保 index 是数字连续的
            df.reset_index(drop=True, inplace=True)

        except Exception as e:
            logger.error(f"获取YFinance数据失败: {e}", exc_info=True)
            df = pd.DataFrame(columns=['time', 'open', 'high', 'low', 'close', 'volume'])

        logger.info(f"Fetched {len(df)} bars for symbol: {symbol}")
        return df
