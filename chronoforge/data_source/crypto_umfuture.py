import logging
import pandas as pd
from binance.um_futures import UMFutures
from typing import Any, Dict, Optional

from .base import DataSourceBase
from chronoforge.utils import with_retry, parse_timeframe_to_milliseconds

logger = logging.getLogger(__name__)


class CryptoUMFutureDataSource(DataSourceBase):
    """UMFutures数据源插件，支持获取UMFutures数据"""

    def __init__(self, config: Dict[str, Any] = None):
        """初始化UMFutures插件

        Args:
            config: None
        """
        super().__init__(config)
        self.client = UMFutures()
        self.exchange_info = None
        self.symbols = None

    def _get_exchange_info(self):
        """获取UMFutures交易所信息"""
        logger.info("初始化UMFutures...")
        self.exchange_info = self.client.exchange_info()
        self.symbols = [symbol['symbol'] for symbol in self.exchange_info['symbols']]
        logger.info(f"UMFutures 交易对数量: {len(self.symbols)}")

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
        """从UMFutures获取时间序列数据。受UMFutures API限制，仅可以获取近30天内的数据。

        Args:
            symbol: UMFutures合约symbol，如'BTCUSDT'或'BTC/USDT'
            timeframe: 时间粒度，如'1m', '5m', '1h', '1d'
            start_ts_ms: 开始时间戳（毫秒）
            end_ts_ms: 结束时间戳（毫秒）, 默认为当前时间

        Returns:
            pandas.DataFrame: 包含时间序列数据的DataFrame
                列名包括'time', 'volume'
        """
        now_ts_ms = int(pd.Timestamp.now(tz='UTC').timestamp() * 1000)
        timeframe_ms = parse_timeframe_to_milliseconds(timeframe)
        # 检查 start_ts_ms 是否在 Binance UMFutures API 允许的时间范围内
        if start_ts_ms < (now_ts_ms - 30 * 24 * 60 * 60 * 1000):
            raise ValueError("UMFutures API 仅支持获取近30天内的数据")
        if end_ts_ms is None:
            end_ts_ms = now_ts_ms - 1000
        # 检查 end_ts_ms 是否在 UMFutures API 允许的时间范围内
        if end_ts_ms <= start_ts_ms:
            raise ValueError("end_ts_ms 必须晚于 start_ts_ms")

        # 如果 exchange_info 为空，先获取交易所信息
        if self.exchange_info is None:
            self._get_exchange_info()

        symbol = symbol.replace("/", "")
        if symbol not in self.symbols:
            raise ValueError(f"UMFutures 不支持交易对 {symbol}")

        _run_start_ts_ms = start_ts_ms
        all_df = pd.DataFrame()

        logger.info(f"Fetching {self.name} for symbol: {symbol}, timeframe: {timeframe}, "
                    f"start_ts_ms: {start_ts_ms}, end_ts_ms: {end_ts_ms}")

        # 连续下载数据，直到获取全部数据或达到目标时间范围
        while True:
            _max_end_ts_ms = _run_start_ts_ms + 500 * timeframe_ms
            if end_ts_ms <= _max_end_ts_ms:
                _run_end_ts_ms = end_ts_ms + 1000
                limit = (_run_end_ts_ms - _run_start_ts_ms) // timeframe_ms
            else:
                _run_end_ts_ms = _max_end_ts_ms + 1000
                limit = 500
            if limit == 0:
                break
            try:
                # open interest history
                # [
                #     {
                #         "symbol":"BTCUSDT",
                #         "sumOpenInterest":"20403.63700000",  // total open interest
                #         "sumOpenInterestValue": "150570784.07809979", // total open interest value
                #         "CMCCirculatingSupply": "165880.538", // circulating supply
                #         "timestamp":"1583127900000"
                #     },
                #     {
                #         "symbol":"BTCUSDT",
                #         "sumOpenInterest":"20401.36700000",
                #         "sumOpenInterestValue":"149940752.14464448",
                #         "CMCCirculatingSupply": "165900.14853",
                #         "timestamp":"1583128200000"
                #     },
                # ]
                oi_hist = self.client.open_interest_hist(
                    symbol=symbol,
                    period=timeframe,
                    limit=limit,
                    startTime=_run_start_ts_ms,
                    endTime=_run_end_ts_ms
                )
                oi_hist_df = pd.DataFrame(oi_hist)
                oi_hist_df['time'] = pd.to_datetime(oi_hist_df['timestamp'], unit='ms', utc=True)
                oi_hist_df['open_interest_value'] = oi_hist_df['sumOpenInterestValue'].astype(float)
                oi_hist_df = oi_hist_df[['time', 'open_interest_value']]

                # takerlongshortRatio
                # [
                #     {
                #         "buySellRatio":"1.5586",
                #         "buyVol": "387.3300",
                #         "sellVol":"248.5030",
                #         "timestamp":"1585614900000"
                #     },
                #     {
                #         "buySellRatio":"1.3104",
                #         "buyVol": "343.9290",
                #         "sellVol":"248.5030",
                #         "timestamp":"1583139900000"
                #     },
                # ]
                taker_long_short_ratio = self.client.taker_long_short_ratio(
                    symbol=symbol,
                    period=timeframe,
                    limit=limit,
                    startTime=_run_start_ts_ms,
                    endTime=_run_end_ts_ms
                )
                taker_long_short_ratio_df = pd.DataFrame(taker_long_short_ratio)
                taker_long_short_ratio_df['time'] = pd.to_datetime(
                    taker_long_short_ratio_df['timestamp'], unit='ms', utc=True)
                taker_long_short_ratio_df['taker_long_short_ratio'] = \
                    taker_long_short_ratio_df['buySellRatio'].astype(float)
                taker_long_short_ratio_df = taker_long_short_ratio_df[
                    ['time', 'taker_long_short_ratio']]

                # top 20% trader long short position ratio
                # [
                #     {
                #         "symbol":"BTCUSDT",
                #         "longShortRatio":"1.4342",// long/short position ratio of top traders
                #         "longAccount": "0.5891", // long positions ratio of top traders
                #         "shortAccount":"0.4108", // short positions ratio of top traders
                #         "timestamp":"1583139600000"
                #     },
                #     {
                #         "symbol":"BTCUSDT",
                #         "longShortRatio":"1.4337",
                #         "longAccount": "0.3583",
                #         "shortAccount":"0.6417",
                #         "timestamp":"1583139900000"
                #         },
                # ]
                top_long_short_position_ratio = self.client.top_long_short_position_ratio(
                    symbol=symbol,
                    period=timeframe,
                    limit=limit,
                    startTime=_run_start_ts_ms,
                    endTime=_run_end_ts_ms
                )
                top_long_short_position_ratio_df = pd.DataFrame(top_long_short_position_ratio)
                top_long_short_position_ratio_df['time'] = pd.to_datetime(
                    top_long_short_position_ratio_df['timestamp'], unit='ms', utc=True)
                top_long_short_position_ratio_df['top_long_short_position_ratio'] = \
                    top_long_short_position_ratio_df['longShortRatio'].astype(float)
                top_long_short_position_ratio_df = top_long_short_position_ratio_df[
                    ['time', 'top_long_short_position_ratio']]

                # top 20% trader long short account ratio
                # [
                #     {
                #         "symbol":"BTCUSDT",
                #         "longShortRatio":"1.8105",  // long/short account num ratio of top traders
                #         "longAccount": "0.6442",   // long account num ratio of top traders
                #         "shortAccount":"0.3558",   // short account num ratio of top traders
                #         "timestamp":"1583139600000"
                #     },
                #     {
                #         "symbol":"BTCUSDT",
                #         "longShortRatio":"0.5576",
                #         "longAccount": "0.3580",
                #         "shortAccount":"0.6420",
                #         "timestamp":"1583139900000"
                #     }
                # ]
                top_long_short_account_ratio = self.client.top_long_short_account_ratio(
                    symbol=symbol,
                    period=timeframe,
                    limit=limit,
                    startTime=_run_start_ts_ms,
                    endTime=_run_end_ts_ms
                )
                top_long_short_account_ratio_df = pd.DataFrame(top_long_short_account_ratio)
                top_long_short_account_ratio_df['time'] = pd.to_datetime(
                    top_long_short_account_ratio_df['timestamp'], unit='ms', utc=True)
                top_long_short_account_ratio_df['top_long_short_account_ratio'] = \
                    top_long_short_account_ratio_df['longShortRatio'].astype(float)
                top_long_short_account_ratio_df = top_long_short_account_ratio_df[
                    ['time', 'top_long_short_account_ratio']]

                # Global Long Short Account Ratio
                # [
                #     {
                #         "symbol":"BTCUSDT",  // long/short account num ratio of all traders
                #         "longShortRatio":"0.1960",  //long account num ratio of all traders
                #         "longAccount": "0.6622",   // short account num ratio of all traders
                #         "shortAccount":"0.3378",
                #         "timestamp":"1583139600000"
                #     },
                #     {
                #         "symbol":"BTCUSDT",
                #         "longShortRatio":"1.9559",
                #         "longAccount": "0.6617",
                #         "shortAccount":"0.3382",
                #         "timestamp":"1583139900000"
                #         },
                # ]
                global_long_short_account_ratio = self.client.long_short_account_ratio(
                    symbol=symbol,
                    period=timeframe,
                    limit=limit,
                    startTime=_run_start_ts_ms,
                    endTime=_run_end_ts_ms
                )
                global_long_short_account_ratio_df = pd.DataFrame(global_long_short_account_ratio)
                global_long_short_account_ratio_df['time'] = pd.to_datetime(
                    global_long_short_account_ratio_df['timestamp'], unit='ms', utc=True)
                global_long_short_account_ratio_df['global_long_short_account_ratio'] = \
                    global_long_short_account_ratio_df['longShortRatio'].astype(float)
                global_long_short_account_ratio_df = global_long_short_account_ratio_df[
                    ['time', 'global_long_short_account_ratio']]

                # funding rate history
                # [
                #     {
                #         "symbol": "BTCUSDT",
                #         "fundingRate": "-0.03750000",
                #         "fundingTime": 1570608000000,
                #         "markPrice": "34287.54619963"
                #           // mark price associated with a particular funding fee charge
                #     },
                #     {
                #         "symbol": "BTCUSDT",
                #         "fundingRate": "0.00010000",
                #         "fundingTime": 1570636800000,
                #         "markPrice": "34287.54619963"
                #     }
                # ]
                funding_rate_hist = self.client.funding_rate(
                    symbol=symbol,
                    limit=limit,
                    startTime=_run_start_ts_ms,
                    endTime=_run_end_ts_ms
                )
                # 使fundingTime为整秒时间，避免时间戳不匹配
                for item in funding_rate_hist:
                    item['fundingTime'] = int(item['fundingTime'] // 1000) * 1000
                funding_rate_hist_df = pd.DataFrame(funding_rate_hist)
                funding_rate_hist_df['time'] = pd.to_datetime(
                    funding_rate_hist_df['fundingTime'], unit='ms', utc=True)
                funding_rate_hist_df['funding_rate'] = \
                    funding_rate_hist_df['fundingRate'].astype(float)
                funding_rate_hist_df = funding_rate_hist_df[['time', 'funding_rate']]

                # merge all dataframes,
                merged_df = pd.merge(oi_hist_df, taker_long_short_ratio_df,
                                     on='time', how='outer')
                merged_df = pd.merge(merged_df, top_long_short_position_ratio_df,
                                     on='time', how='outer')
                merged_df = pd.merge(merged_df, top_long_short_account_ratio_df,
                                     on='time', how='outer')
                merged_df = pd.merge(merged_df, global_long_short_account_ratio_df,
                                     on='time', how='outer')
                merged_df = pd.merge(merged_df, funding_rate_hist_df,
                                     on='time', how='outer')

                # 填充缺失值
                merged_df = merged_df.sort_values('time').ffill().bfill()

                # 合并到总数据框
                all_df = pd.concat([all_df, merged_df], ignore_index=True)

                # 根据 time 去重（保留首次出现）
                all_df = all_df.drop_duplicates(subset=["time"], keep="last")

                # 重置索引
                all_df = all_df.reset_index(drop=True)

                # 设置_run_start_ts_ms为第一个时间点
                _run_start_ts_ms = int(oi_hist[-1]["timestamp"]) + timeframe_ms
                if _run_start_ts_ms >= end_ts_ms:
                    break

            except Exception as e:
                logger.error(f"❌下载 UMFuture {symbol} - {timeframe} "
                             f"数据时出错: {str(e)}", exc_info=True)
                return None

        logger.info(f"Fetched {len(all_df)} bars for symbol: {symbol}")
        return all_df

    async def close_all_connections(self):
        """关闭所有与数据源的连接"""
        pass

    async def tickers(self, **kwargs) -> Any:
        """获取所有UMFutures交易对"""
        return self.client.ticker_price()
