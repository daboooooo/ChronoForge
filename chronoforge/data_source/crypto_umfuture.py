import logging
import pandas as pd
from binance.um_futures import UMFutures
from typing import Any, Dict, Optional

from .base import DataSourceBase
from chronoforge.utils import parse_timeframe_to_milliseconds
from chronoforge.decorators import with_retry, api_callable

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
        min_allowed_ts = now_ts_ms - 30 * 24 * 60 * 60 * 1000
        
        now_dt = pd.to_datetime(now_ts_ms, unit='ms', utc=True)
        min_dt = pd.to_datetime(min_allowed_ts, unit='ms', utc=True)
        start_dt = pd.to_datetime(start_ts_ms, unit='ms', utc=True)
        end_dt = pd.to_datetime(end_ts_ms, unit='ms', utc=True) if end_ts_ms else None
        # 强制限制 start_ts_ms 在最近30天内
        original_start_ts_ms = start_ts_ms
        if start_ts_ms < min_allowed_ts:
            raise ValueError(
                f"start_ts_ms {start_ts_ms} 超出 UMFutures API 允许范围，"
                f"仅支持近30天内的数据"
            )
        # 如果 start_ts_ms 是未来时间，也调整为当前时间
        if start_ts_ms > now_ts_ms:
            logger.warning(
                f"start_ts_ms {start_ts_ms} 是未来时间，"
                f"调整为当前时间: {now_ts_ms - 60000}"
            )
            start_ts_ms = now_ts_ms - 60000  # 1分钟前

        if end_ts_ms is None:
            end_ts_ms = now_ts_ms - 1000
        # 确保 end_ts_ms 不超过当前时间
        if end_ts_ms > now_ts_ms:
            logger.warning(
                f"end_ts_ms {end_ts_ms} 超过当前时间，调整为: {now_ts_ms - 1000}"
            )
            end_ts_ms = now_ts_ms - 1000
        # 确保 end_ts_ms 不早于 start_ts_ms
        if end_ts_ms <= start_ts_ms:
            raise ValueError(
                f"end_ts_ms {end_ts_ms} 早于或等于 start_ts_ms {start_ts_ms}"
            )

        # 强制确保 start_ts_ms 和 end_ts_ms 在有效范围内
        start_ts_ms = max(min_allowed_ts, min(start_ts_ms, now_ts_ms - 60000))
        end_ts_ms = max(start_ts_ms + 60000, min(end_ts_ms, now_ts_ms - 1000))
        if original_start_ts_ms != start_ts_ms:
            logger.info(f"强制调整 start_ts_ms: {original_start_ts_ms} -> {start_ts_ms}")

        # Binance API 要求 startTime 和 endTime 必须与 period 对齐
        # 对于 1d 周期，时间戳应该是每天的 00:00:00
        # 向更大的值对齐（向上取整到下一个周期的开始）
        start_dt = pd.to_datetime(start_ts_ms, unit='ms', utc=True)
        end_dt = pd.to_datetime(end_ts_ms, unit='ms', utc=True)

        # 使用 ceil 向更大的值对齐
        start_dt_aligned = start_dt.ceil('D')
        end_dt_aligned = end_dt.ceil('D')

        # 确保对齐后的时间戳不早于 min_allowed_ts
        min_allowed_dt = pd.to_datetime(min_allowed_ts, unit='ms', utc=True)
        if start_dt_aligned < min_allowed_dt.ceil('D'):
            # 如果对齐后的时间戳早于 min_allowed_ts，
            # 使用 min_allowed_ts 的下一个对齐时间点
            start_dt_aligned = min_allowed_dt.ceil('D')

        start_ts_ms = int(start_dt_aligned.timestamp() * 1000)
        end_ts_ms = int(end_dt_aligned.timestamp() * 1000)

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
        max_iterations = 10  # 防止无限循环的保护机制
        iteration_count = 0

        try:
            while True:
                iteration_count += 1
                if iteration_count > max_iterations:
                    logger.warning(
                        f"Reached maximum iterations ({max_iterations}) "
                        f"in fetch method, breaking loop"
                    )
                    break

                # Binance API 限制：最多查询 30 天的数据，limit 最大 500
                # 计算 30 天内的最大结束时间
                _30_days_ms = 30 * 24 * 60 * 60 * 1000
                _max_end_ts_ms = min(_run_start_ts_ms + _30_days_ms, now_ts_ms)
                
                if end_ts_ms <= _max_end_ts_ms:
                    _run_end_ts_ms = end_ts_ms
                else:
                    _run_end_ts_ms = _max_end_ts_ms
                
                # 计算 limit，确保不超过 500
                limit = min(500, (_run_end_ts_ms - _run_start_ts_ms) // timeframe_ms)
                
                if limit <= 0:
                    break
                if _run_start_ts_ms < min_allowed_ts:
                    logger.warning(
                        f"_run_start_ts_ms {_run_start_ts_ms} 超出允许范围，跳过"
                    )
                    break

                # open interest history
                # 确保时间戳与 period 对齐
                # 向更大的值对齐（向上取整到下一个周期的开始）
                run_start_dt = pd.to_datetime(_run_start_ts_ms, unit='ms', utc=True)
                run_end_dt = pd.to_datetime(_run_end_ts_ms, unit='ms', utc=True)
                run_start_aligned = run_start_dt.ceil('D')
                run_end_aligned = run_end_dt.ceil('D')

                # 确保对齐后的时间戳不早于 min_allowed_ts
                min_allowed_dt = pd.to_datetime(min_allowed_ts, unit='ms', utc=True)
                if run_start_aligned < min_allowed_dt.ceil('D'):
                    run_start_aligned = min_allowed_dt.ceil('D')

                _run_start_ts_aligned = int(run_start_aligned.timestamp() * 1000)
                _run_end_ts_aligned = int(run_end_aligned.timestamp() * 1000)

                # 如果对齐后的 start 晚于 end，跳过本次循环
                if _run_start_ts_aligned >= _run_end_ts_aligned:
                    logger.warning(
                        f"对齐后的 start_ts_ms ({_run_start_ts_aligned}) "
                        f"晚于或等于 end_ts_ms ({_run_end_ts_aligned})，跳过"
                    )
                    break
                oi_hist = self.client.open_interest_hist(
                    symbol=symbol,
                    period=timeframe,
                    limit=limit,
                    startTime=_run_start_ts_aligned,
                    endTime=_run_end_ts_aligned
                )
                oi_hist_df = pd.DataFrame(oi_hist)
                oi_hist_df['time'] = pd.to_datetime(
                    oi_hist_df['timestamp'].astype(int), unit='ms', utc=True
                )
                oi_hist_df['open_interest_value'] = oi_hist_df['sumOpenInterestValue'].astype(float)
                oi_hist_df = oi_hist_df[['time', 'open_interest_value']]
                oi_hist_df = oi_hist_df.sort_values('time').reset_index(drop=True)
                oi_hist_df['time'] = pd.to_datetime(oi_hist_df['time'], utc=True)

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
                    startTime=_run_start_ts_aligned,
                    endTime=_run_end_ts_aligned
                )
                taker_long_short_ratio_df = pd.DataFrame(taker_long_short_ratio)
                taker_long_short_ratio_df['time'] = pd.to_datetime(
                    taker_long_short_ratio_df['timestamp'].astype(int), unit='ms', utc=True)
                taker_long_short_ratio_df['taker_long_short_ratio'] = \
                    taker_long_short_ratio_df['buySellRatio'].astype(float)
                taker_long_short_ratio_df = taker_long_short_ratio_df[
                    ['time', 'taker_long_short_ratio']]
                taker_long_short_ratio_df['time'] = pd.to_datetime(
                    taker_long_short_ratio_df['time'], utc=True)

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
                    startTime=_run_start_ts_aligned,
                    endTime=_run_end_ts_aligned
                )
                top_long_short_position_ratio_df = pd.DataFrame(top_long_short_position_ratio)
                top_long_short_position_ratio_df['time'] = pd.to_datetime(
                    top_long_short_position_ratio_df['timestamp'].astype(int), unit='ms', utc=True)
                top_long_short_position_ratio_df['top_long_short_position_ratio'] = \
                    top_long_short_position_ratio_df['longShortRatio'].astype(float)
                top_long_short_position_ratio_df = top_long_short_position_ratio_df[
                    ['time', 'top_long_short_position_ratio']]
                top_long_short_position_ratio_df['time'] = pd.to_datetime(
                    top_long_short_position_ratio_df['time'], utc=True)

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
                    startTime=_run_start_ts_aligned,
                    endTime=_run_end_ts_aligned
                )
                top_long_short_account_ratio_df = pd.DataFrame(top_long_short_account_ratio)
                top_long_short_account_ratio_df['time'] = pd.to_datetime(
                    top_long_short_account_ratio_df['timestamp'].astype(int), unit='ms', utc=True)
                top_long_short_account_ratio_df['top_long_short_account_ratio'] = \
                    top_long_short_account_ratio_df['longShortRatio'].astype(float)
                top_long_short_account_ratio_df = top_long_short_account_ratio_df[
                    ['time', 'top_long_short_account_ratio']]
                top_long_short_account_ratio_df['time'] = pd.to_datetime(
                    top_long_short_account_ratio_df['time'], utc=True)

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
                    startTime=_run_start_ts_aligned,
                    endTime=_run_end_ts_aligned
                )
                global_long_short_account_ratio_df = pd.DataFrame(global_long_short_account_ratio)
                global_long_short_account_ratio_df['time'] = pd.to_datetime(
                    global_long_short_account_ratio_df['timestamp'].astype(int),
                    unit='ms', utc=True
                )
                global_long_short_account_ratio_df['global_long_short_account_ratio'] = \
                    global_long_short_account_ratio_df['longShortRatio'].astype(float)
                global_long_short_account_ratio_df = global_long_short_account_ratio_df[
                    ['time', 'global_long_short_account_ratio']]
                global_long_short_account_ratio_df['time'] = pd.to_datetime(
                    global_long_short_account_ratio_df['time'], utc=True)

                # merge all dataframes,
                merged_df = pd.merge(oi_hist_df, taker_long_short_ratio_df,
                                     on='time', how='outer')
                merged_df = pd.merge(merged_df, top_long_short_position_ratio_df,
                                     on='time', how='outer')
                merged_df = pd.merge(merged_df, top_long_short_account_ratio_df,
                                     on='time', how='outer')
                merged_df = pd.merge(merged_df, global_long_short_account_ratio_df,
                                     on='time', how='outer')

                # 填充缺失值
                merged_df = merged_df.sort_values('time').ffill().bfill()

                # 合并到总数据框
                all_df = pd.concat([all_df, merged_df], ignore_index=True)

                # 根据 time 去重（保留首次出现）
                all_df = all_df.drop_duplicates(subset=["time"], keep="last")

                # 重置索引
                all_df = all_df.reset_index(drop=True)

                # 设置_run_start_ts_ms为下一个时间点
                if oi_hist and len(oi_hist) > 0:
                    _run_start_ts_ms = int(oi_hist[-1]["timestamp"]) + timeframe_ms
                else:
                    _run_start_ts_ms += 500 * timeframe_ms
                if _run_start_ts_ms >= end_ts_ms:
                    break

        except Exception as e:
            logger.error(f"❌下载 UMFuture {symbol} - {timeframe} "
                         f"数据时出错: {str(e)}", exc_info=True)
            return None

        logger.info(f"Fetched {len(all_df)} bars for symbol: {symbol}")

        if all_df is not None and not all_df.empty:
            if 'time' in all_df.columns:
                all_df['time'] = pd.to_datetime(all_df['time'], utc=True)
                if not pd.api.types.is_datetime64_any_dtype(all_df['time']):
                    all_df['time'] = all_df['time'].astype('datetime64[ns, UTC]')

        # 如果没有获取到数据，返回None
        if all_df is None or all_df.empty:
            return None

        return all_df

    @api_callable
    @with_retry
    async def tickers(self, quote: Optional[str] = None) -> Dict[str, Dict]:
        """获取所有UMFutures交易对

        Args:
            quote (Optional[str], optional): 报价资产，如'USDT'. Defaults to None.

        Returns:
            Dict[str, Dict]: 交易对价格数据字典
                键为交易对符号(如'BTC/USDT')，值为包含价格信息的字典
                包含字段: 'last'(最新价), 'bid'(买价), 'ask'(卖价), 'volume'(成交量)等
        """
        try:
            tickers = self.client.ticker_price()
            if not tickers:
                logger.warning("No ticker data received")
                return {}

            # 转换为与 CryptoSpotDataSource 兼容的格式
            result = {}
            for ticker in tickers:
                symbol = ticker.get('symbol', '')
                if not symbol:
                    continue

                # 转换为标准格式: BTCUSDT -> BTC/USDT
                if symbol.endswith('USDT'):
                    base = symbol[:-4]
                    standard_symbol = f"{base}/USDT"
                else:
                    continue

                # 构建与 Spot tickers 兼容的数据结构
                result[standard_symbol] = {
                    'symbol': standard_symbol,
                    'last': float(ticker.get('price', 0)),
                    'bid': float(ticker.get('price', 0)),  # UM Future 不区分 bid/ask
                    'ask': float(ticker.get('price', 0)),
                    'volume': 0,  # ticker_price 不返回成交量
                    'quoteVolume': 0,
                    'baseVolume': 0,
                    'timestamp': ticker.get('time', 0),
                    'datetime': None,
                    'high': None,
                    'low': None,
                    'open': None,
                    'close': float(ticker.get('price', 0)),
                    'change': None,
                    'percentage': None,
                    'average': None,
                    'vwap': None,
                    'previousClose': None,
                    'change': None
                }

            logger.info(f"Fetched {len(result)} UM future tickers")

            # 如果指定了 quote，过滤返回
            if quote:
                filtered = {}
                for symbol, data in result.items():
                    if symbol.endswith(f"/{quote}"):
                        filtered[symbol] = data
                return filtered

            return result

        except Exception as e:
            logger.error(f"获取UMFutures交易对价格失败: {str(e)}", exc_info=True)
            return {}

    @api_callable
    @with_retry
    async def tickers_df(self) -> pd.DataFrame:
        """获取所有UMFutures交易对（DataFrame格式）

        Returns:
            pandas.DataFrame: 包含交易对价格数据的DataFrame
                列名包括 'symbol', 'price', 'time'
        """
        try:
            tickers = self.client.ticker_price()
            if not tickers:
                logger.warning("No ticker data received")
                return pd.DataFrame()

            tickers_df = pd.DataFrame(tickers)

            if 'time' in tickers_df.columns:
                tickers_df['time'] = pd.to_datetime(
                    tickers_df['time'].astype(int), unit='ms', utc=True
                )

            if 'price' in tickers_df.columns:
                tickers_df['price'] = tickers_df['price'].astype(float)

            logger.info(f"Fetched {len(tickers_df)} UM future tickers (DataFrame)")
            return tickers_df

        except Exception as e:
            logger.error(f"获取UMFutures交易对价格失败: {str(e)}", exc_info=True)
            return pd.DataFrame()
