import logging
import pandas as pd
from fredapi import Fred
from typing import Any, Dict, Optional

from .base import DataSourceBase
from chronoforge.utils import with_retry

logger = logging.getLogger(__name__)

# ST. LOUIS FRED API SERIES
FRED_RATES = {
    "Daily": {
        "IORB": {
            "name": "Interest Rate Overnight Reverse Repurchase Agreements",
            "id": "IORB",
            "comment": "银行在美联储的准备金利率，是联邦基金利率的‘天花板’，处于美联储利率区间的中间。"
        },
        "ON_RRP_Award": {
            "name": "Overnight Reverse Repurchase Agreements Award Rate",
            "id": "RRPONTSYAWARD",
            "comment": "隔夜逆回购利率，是联邦基金利率区间的‘地板’"
        },
        "EFFR": {
            "name": "Federal Funds Effective Rate",
            "id": "EFFR",
            "comment": "联邦基金有效利率，是美联储在每个工作日发布的利率，是市场中间值。ON RRP < EFFR < IORB"
        },
        "SOFR": {
            "name": "Secured Overnight Financing Rate",
            "id": "SOFR",
            "high_freq_data": {
                "URL": (
                    "https://markets.newyorkfed.org/read?"
                    "productCode=50&eventCodes=520&limit=25&"
                    "startPosition=0&sort=postDt:-1&format=xml"
                ),
                "format": "xml"
            },
            "comment": "以国债作为抵押的回购市场利率。SOFR高于EFFR 0.17%时，相当于美元流动性'心肌梗塞'"
        },
        "T-Bill_1M": {
            "name": "1-Month T-Bill Secondary Market Rate",
            "id": "DTB4WK",
            "comment": "1月国债收益率，是指在1月到期的国债的收益率"
        },
        "T-Bill_3M": {
            "name": "3-Month T-Bill Secondary Market Rate",
            "id": "DTB3",
            "comment": "3月国债收益率，是指在3月到期的国债的收益率"
        },
        "T-Bill_6M": {
            "name": "6-Month T-Bill Secondary Market Rate",
            "id": "DTB6",
            "comment": "6月国债收益率，是指在6月到期的国债的收益率"
        },
        "T-Bill_1Y": {
            "name": "1-Year T-Bill Secondary Market Rate",
            "id": "DTB1YR",
            "comment": "1年国债收益率，是指在1年到期的国债的收益率"
        },
        "T-Note_2Y": {
            "name": "2-Year T-Note Secondary Market Rate",
            "id": "DGS2",
            "comment": "2年国债收益率，是指在2年到期的国债的收益率"
        },
        "T-Note_5Y": {
            "name": "5-Year T-Note Secondary Market Rate",
            "id": "DGS5",
            "comment": "5年国债收益率，是指在5年到期的国债的收益率"
        },
        "T-Note_10Y": {
            "name": "10-Year T-Note Secondary Market Rate",
            "id": "DGS10",
            "comment": "10年国债收益率，是指在10年到期的国债的收益率"
        },
        "T-Bond_20Y": {
            "name": "20-Year T-Bond Secondary Market Rate",
            "id": "DGS20",
            "comment": "20年国债收益率，是指在20年到期的国债的收益率"
        },
        "T-Bond_30Y": {
            "name": "30-Year T-Bond Secondary Market Rate",
            "id": "DGS30",
            "comment": "30年国债收益率，是指在30年到期的国债的收益率"
        }
    },
    "Weekly": {
    }
}

fred_daily_rates = [
    "IORB",
    "RRPONTSYAWARD",
    "EFFR",
    "SOFR",
    "DTB4WK",
    "DTB3",
    "DTB6",
    "DTB1YR",
    "DGS2",
    "DGS5",
    "DGS10",
    "DGS20",
    "DGS30"
]

FRED_VOLUMES = {
    "Daily": {
        "ON_RRP": {
            "name": "Overnight Reverse Repurchase Agreements",
            "id": "RRPONTSYD",
            "comment": "隔夜逆回购成交量"
        },
        "EFFR": {
            "name": "Effective Federal Funds Volume",
            "id": "EFFRVOL",
            "comment": "联邦基金市场成交量"
        },
        "SOFR": {
            "name": "Secured Overnight Financing Rate Volume",
            "id": "SOFRVOL",
            "comment": "以国债作为抵押的回购市场交易的成交量",
        },
        "SRF_TS": {
            "name": "Standing Repo Facility - Treasury Securities Purchased in OMO",
            "id": "RPONTSYD",
            "url": "https://www.newyorkfed.org/markets/desk-operations/repo",
            "comment": "回购国债的常备回购便利成交量"
        },
        "SRF_MBS": {
            "name": "Standing Repo Facility - MBS Purchased in OMO",
            "id": "RPMBSD",
            "url": "https://www.newyorkfed.org/markets/desk-operations/repo",
            "comment": "以住宅或商业抵押贷款为基础打包证券的常备回购便利成交量"
        },
        "SRF_Agency": {
            "name": "Standing Repo Facility - Agency Securities Purchased in OMO",
            "id": "RPAGYD",
            "url": "https://www.newyorkfed.org/markets/desk-operations/repo",
            "comment": "回购机构债券的常备回购便利成交量"
        },
    },
    "Weekly": {
        "Reserve_Balances": {
            "name": "Reserve Balances with Federal Reserve Banks: Wednesday Level",
            "id": "WRBWFRBL",
            "comment": "准备金余额。接近 3,000,000 说明流动性紧张, 区域银行可能会暴雷。美联储官员认为的红线是 2,700,000, 会主动干预。",
            "pattern_1": "准备金余额连续两周低于3万亿之后，次日做空。退出条件：下跌总幅度超过5%后，遇到第一个日线收阳。止损条件：上涨超过2%后，次日平仓。",
            "pattern_2": "准备金余额连续4周下跌。"
        },
    }
}

fred_daily_volumes = [
    "RRPONTSYD",  # 隔夜逆回购成交量，时间戳早1天
    "EFFRVOL",
    "SOFRVOL",
    "RPONTSYD",  # 回购国债的常备回购便利成交量，时间戳早1天
    "RPMBSD",  # 以住宅或商业抵押贷款为基础打包证券的常备回购便利成交量，时间戳早1天
    "RPAGYD",
]

fred_weekly_volumes = [
    "WRBWFRBL",
]


class FREDDataSource(DataSourceBase):
    """FRED数据源插件，支持获取FRED数据"""

    def __init__(self, config: Dict[str, Any] = None):
        """初始化FRED插件

        Args:
            config: must contain "api_key"
        """
        super().__init__(config)
        self.fred_instance = None

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
            symbol: FRED系列symbol，如'GDP'
            timeframe: 时间粒度，如'1m', '5m', '1h', '1d'
            start_ts_ms: 开始时间戳（毫秒）
            end_ts_ms: 结束时间戳（毫秒）, 默认为当前时间

        Returns:
            pandas.DataFrame: 包含时间序列数据的DataFrame，
                列名包括'time', 'volume'
        """
        # 初始化FRED实例
        if self.fred_instance is None:
            if "api_key" not in self.config:
                raise ValueError("FRED插件配置必须包含 'api_key'")
            self.fred_instance = Fred(api_key=self.config.get("api_key"))

        # 转换时间戳格式为FRED API要求的格式（YYYY-MM-DD）
        start_time = pd.Timestamp(start_ts_ms, unit='ms', tz='US/Central').strftime('%Y-%m-%d')
        if end_ts_ms is None:
            end_time = pd.Timestamp.now(tz='US/Central').strftime('%Y-%m-%d')
        else:
            end_time = pd.Timestamp(end_ts_ms, unit='ms', tz='US/Central').strftime('%Y-%m-%d')

        logger.info(f"Fetching {self.name} for symbol: {symbol}, timeframe: {timeframe}, "
                    f"start_ts_ms: {start_ts_ms}, end_ts_ms: {end_ts_ms}")

        # 连续下载数据，直到获取全部数据或达到目标时间范围
        try:
            # 从FRED获取数据
            fred_series = self.fred_instance.get_series(
                symbol, observation_start=start_time, observation_end=end_time
            )

            # 检查是否返回了空数据
            if fred_series is None or fred_series.empty:
                return None

            # fred 返回数据，转换为DataFrame格式, 索引为时间字符串 '%Y-%m-%d'
            df = pd.DataFrame(fred_series, columns=['volume'])

            # 将索引（'%Y-%m-%d'）转换为时间戳（毫秒级）并赋值给新列'time', 并设置时区为UTC
            # FRED 数据发布时间 Central Time (CT)，夏令时期间：比北京时间晚 13 小时；冬令时期间：比北京时间晚 14 小时
            # 首先创建一个带时区的DatetimeIndex
            dt_index = pd.to_datetime(df.index).tz_localize('US/Central')
            # 转换为UTC时区
            dt_index_utc = dt_index.tz_convert('UTC')
            # 赋值给time列并转换为毫秒级时间戳
            df['time'] = dt_index_utc.astype(int) // 10**6
            # 转换为datetime格式并设置时区为UTC
            df['time'] = pd.to_datetime(df['time'], unit='ms', utc=True)
            # 调换 time 和 volume 列
            df = df[['time', 'volume']]
            # 确保 index 是数字连续的
            df.reset_index(drop=True, inplace=True)

            logger.info(f"Fetched {len(df)} bars for symbol: {symbol}")

        except Exception as e:
            logger.warning(f"⚠️ 未下载到 {symbol} - {timeframe} 新数据: {e}")
            df = pd.DataFrame(columns=['time', 'volume'])

        return df

    async def close_all_connections(self):
        """关闭所有与数据源的连接"""
        pass
