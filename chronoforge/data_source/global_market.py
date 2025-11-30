import logging
import pandas as pd
import yfinance as yf
from typing import Any, Dict, Optional

from .base import DataSourceBase
from chronoforge.utils import with_retry

logger = logging.getLogger(__name__)


class GlobalMarketDataSource(DataSourceBase):
    """全球市场数据源插件，支持获取YFinance数据"""

    def __init__(self, config: Dict[str, Any] = None):
        """初始化全球市场插件

        Args:
            config: None
        """
        super().__init__(config)
        self.yfinance_instance = None

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
                proxy=None,

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

    async def close_all_connections(self):
        """关闭所有与数据源的连接"""
        pass

    def validate(self, data: pd.DataFrame) -> tuple[bool, str]:
        """验证FRED数据的完整性

        Args:
            data: 要验证的数据

        Returns:
            tuple[bool, str]: (数据是否有效, 错误信息)
        """
        if data is None or data.empty:
            return False, "数据为空"

        # 检查必需的列
        required_columns = ['time', 'open', 'high', 'low', 'close', 'volume']
        if not all(col in data.columns for col in required_columns):
            missing = [col for col in required_columns if col not in data.columns]
            return False, f"缺少必要列: {missing}"

        # 检查数据是否有NaN值
        if data[required_columns].isna().any().any():
            logger.warning("数据中包含NaN值")

        # 检查数据是否按时间排序
        if not data['time'].is_monotonic_increasing:
            return False, "数据未按时间排序"

        return True, ""
