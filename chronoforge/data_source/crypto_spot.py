import logging
from typing import Any, Dict, Optional

import ccxt.async_support as ccxt
import pandas as pd
import time
from datetime import datetime, timezone

from .base import DataSourceBase
from chronoforge.utils import parse_timeframe_to_milliseconds, with_retry

logger = logging.getLogger(__name__)


class CryptoSpotDataSource(DataSourceBase):
    """CCXT交易所数据源插件，支持多种加密货币交易所"""

    def __init__(self, config: Dict[str, Any] = None):
        """初始化CCXT插件

        Args:
            config: None
        """
        super().__init__(config)
        self.exchange_instances: Dict[str, ccxt.Exchange] = {}

    @property
    def name(self):
        """返回数据源名称"""
        return self.__class__.__name__.replace("DataSource", "")

    def __del__(self):
        """析构函数，清理资源"""
        try:
            # 清理交易所实例
            for exchange_name, exchange_instance in self.exchange_instances.items():
                try:
                    # 尝试调用close方法关闭连接
                    if hasattr(exchange_instance, 'close'):
                        # 在同步上下文中，我们无法直接await，所以这里只是记录警告
                        logger.warning(f"请在异步代码中手动调用 await {exchange_name}.close() 来关闭连接")
                except Exception as e:
                    logger.warning(f"关闭交易所连接 {exchange_name} 时出错: {str(e)}")
        except Exception:
            # 析构函数中不应抛出异常
            pass

    async def __aenter__(self):
        """异步上下文管理器的进入方法"""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器的退出方法，确保所有exchange连接都被正确关闭"""
        await self.close_all_connections()
        return False  # 不抑制异常

    async def close_all_connections(self):
        """关闭所有交易所连接

        此方法可以在异步代码中直接调用，确保所有的exchange连接都被正确关闭。
        """
        for exchange_name, exchange_instance in list(self.exchange_instances.items()):
            try:
                if hasattr(exchange_instance, 'close'):
                    await exchange_instance.close()
                    logger.info(f"成功关闭交易所连接: {exchange_name}")
                    # 从实例字典中移除已关闭的连接
                    del self.exchange_instances[exchange_name]
            except Exception as e:
                logger.error(f"关闭交易所连接 {exchange_name} 时出错: {str(e)}")

    async def _get_ccxt_exchange(self, exchange_name: str,
                                 exchange_config: Dict[str, Any] = None,
                                 force_reinit: bool = False) -> ccxt.Exchange:
        """
        获取或初始化指定交易所实例

        Args:
            exchange_name: 交易所名称，如'binance', 'okx'
            exchange_config: 交易所配置，包含apiKey, secret等
            force_reinit: 是否强制重新初始化，默认为False

        Returns:
            ccxt.Exchange: 初始化后的交易所实例
        """
        # 转换交易所名称为小写
        exchange_name = exchange_name.lower()

        # 如果交易所实例已存在，直接返回
        if not force_reinit and exchange_name in self.exchange_instances:
            return self.exchange_instances[exchange_name]

        # 获取ccxt交易所类
        if exchange_name not in ccxt.exchanges:
            raise ValueError(f"不支持的交易所: {exchange_name}")

        # 创建交易所实例
        exchange_class: ccxt.Exchange = getattr(ccxt, exchange_name)

        # 准备ccxt配置参数
        ccxt_config = {
            'enableRateLimit': True,  # 启用速率限制
        }

        # 添加API凭据
        if exchange_config and 'apiKey' in exchange_config:
            ccxt_config['apiKey'] = exchange_config['apiKey']
        if exchange_config and 'secret' in exchange_config:
            ccxt_config['secret'] = exchange_config['secret']

        # 创建交易所实例
        exchange_instance = exchange_class(ccxt_config)

        # 如果需要验证连接
        try:
            # 加载市场数据作为验证
            await exchange_instance.load_markets()
            self.exchange_instances[exchange_name] = exchange_instance
            logger.info(f"成功连接到交易所: {exchange_name}")
        except Exception as e:
            logger.error(f"交易所连接失败: {str(e)}")
            raise
        return exchange_instance

    @with_retry
    async def fetch(
        self,
        symbol: str,
        timeframe: str,
        start_ts_ms: int,
        end_ts_ms: Optional[int] = None
    ) -> pd.DataFrame:
        """从交易所获取OHLCV数据

        Args:
            symbol: 包含交易所和交易对标识符，如'binance:BTC/USDT'
            timeframe: 时间粒度，如'1m', '5m', '1h', '1d'
            start_ts_ms: 开始时间戳（毫秒）
            end_ts_ms: 结束时间戳（毫秒）, 默认为当前时间
            close_after_fetch: 是否在获取数据后关闭交易所连接，默认为False
            （保持连接以提高性能）

        Returns:
            pandas.DataFrame: 包含OHLCV数据的DataFrame，
                列名包括'time', 'open', 'high', 'low', 'close', 'volume'

        Notes:
            推荐使用异步上下文管理器来自动管理连接生命周期:
            ```python
            async with CCXTPlugin(config) as plugin:
                data = await plugin.fetch_data('binance:BTC/USDT', '1m', start_ts, end_ts)
            # 退出上下文时会自动关闭所有连接
            ```
            或者在所有操作完成后手动调用:
            ```python
            await plugin.close_all_connections()
            ```
        """
        # 尝试多种方式获取交易所名称
        if hasattr(self, 'ccxt_exchange_name') and self.ccxt_exchange_name:
            # 如果实例有ccxt_exchange_name属性，则直接使用
            exchange_name = self.ccxt_exchange_name.lower()
            # 直接使用原始symbol，不需要分割
            actual_symbol = symbol
        else:
            # 尝试从symbol中分割交易所名称
            try:
                parts = symbol.split(":")
                if len(parts) == 3:
                    # 格式：datasource:exchange:symbol
                    _, exchange_name, actual_symbol = parts
                elif len(parts) == 2:
                    # 格式：exchange:symbol 或 datasource_name:exchange_name:symbol的一部分
                    # 检查第一部分是否包含datasource，如果是，则取第二部分作为exchange_name
                    if 'datasource' in parts[0].lower():
                        # 这种情况下，第二部分可能是完整的exchange_name或者exchange_name:symbol
                        sub_parts = parts[1].split(":")
                        if len(sub_parts) == 2:
                            exchange_name, actual_symbol = sub_parts
                        else:
                            # 如果无法分割，则使用默认的binance
                            exchange_name = 'binance'
                            actual_symbol = parts[1]
                    else:
                        # 正常情况：exchange:symbol
                        exchange_name, actual_symbol = parts
                else:
                    # 如果无法分割，则使用默认的binance
                    exchange_name = 'binance'
                    actual_symbol = symbol

                exchange_name = exchange_name.lower()
                symbol = actual_symbol

            except Exception as e:
                # 如果分割失败，默认使用binance
                print(f"警告：无法从symbol中解析交易所名称，使用默认值binance。错误：{e}")
                exchange_name = 'binance'

        # 获取或初始化交易所实例
        exchange = await self._get_ccxt_exchange(exchange_name, self.config)

        # 转换时间戳格式
        since_ts_ms = start_ts_ms  # CCXT使用毫秒

        if end_ts_ms is None:
            until_ts_ms = int(time.time() * 1000)
        else:
            until_ts_ms = end_ts_ms

        timeframe_ms = parse_timeframe_to_milliseconds(timeframe)

        # 初始化数据容器和控制变量
        all_ohlcv = []

        logger.info(f"Fetching {self.name} for symbol: {symbol}, timeframe: {timeframe}, "
                    f"start_ts_ms: {start_ts_ms} "
                    f"({datetime.fromtimestamp(start_ts_ms / 1000, tz=timezone.utc)}), "
                    f"end_ts_ms: {end_ts_ms} "
                    f"({datetime.fromtimestamp(end_ts_ms / 1000, tz=timezone.utc)})")

        # 连续下载数据，直到获取全部数据或达到目标时间范围
        while True:
            try:
                # 计算最大可获取数据量，向下取整
                limit_max = (until_ts_ms - since_ts_ms) // timeframe_ms

                if limit_max <= 0:
                    break

                # 计算初次请求的limit值
                if exchange_name == 'okx':
                    limit = 300 if limit_max > 300 else limit_max
                else:
                    limit = 1000 if limit_max > 1000 else limit_max

                # 从交易所获取数据
                ohlcv = await exchange.fetch_ohlcv(
                    symbol, timeframe, since=since_ts_ms, limit=limit
                )

                if not ohlcv or len(ohlcv) == 0:
                    # 没有更多数据了，结束循环
                    break

                # 添加到总数据列表
                all_ohlcv.extend(ohlcv)

                # 检查是否已达到目标时间范围
                last_timestamp = ohlcv[-1][0]  # 毫秒级timestamp
                # 计算下一个K线周期的起始时间（使用timestamp计算，避免时区问题）
                next_candle_timestamp = last_timestamp + timeframe_ms

                # 如果最后一个K线还没有终结，即结束时间大于当前时间，删除它
                if next_candle_timestamp >= until_ts_ms:
                    # 删除最后一个K线
                    all_ohlcv.pop()
                    break

                # 处理不同交易所的循环终止条件
                if len(ohlcv) < limit:
                    break

                # 更新下一次请求的起始时间（使用最后一条数据的时间戳）
                since_ts_ms = ohlcv[-1][0] + 1  # +1 避免重复获取同一条数据

            except Exception as e:
                logger.warning(f"❌ 从 {exchange_name} 下载 {symbol} - {timeframe} 新数据时出错: {e}")
                return None

        # 所有数据批次下载完成后，转换为DataFrame
        df = None
        if all_ohlcv:
            # last kline may not be complete, remove it
            last_ts_ms = all_ohlcv[-1][0]
            if last_ts_ms + timeframe_ms > int(time.time() * 1000):
                logger.warning(f"删除未结束的bar {last_ts_ms}")
                all_ohlcv.pop()
            # 转换为DataFrame格式
            columns = ['time', 'open', 'high', 'low', 'close', 'volume']
            df = pd.DataFrame.from_records(all_ohlcv, columns=columns)
            # 将时间戳转换为datetime格式
            df['time'] = pd.to_datetime(df['time'], unit='ms', utc=True)

            logger.info(f"Fetched {len(all_ohlcv)} OHLCV bars for symbol: {symbol}")

            return df
        else:
            logger.warning(f"⚠️ 未下载到 {symbol} - {timeframe} 新数据")
            return pd.DataFrame(columns=['time', 'open', 'high', 'low', 'close', 'volume'])

    def validate(self, data: pd.DataFrame) -> tuple[bool, str]:
        """验证OHLCV数据的完整性

        Args:
            data: 要验证的数据

        Returns:
            tuple[bool, str]: (数据是否有效, 错误信息)
        """
        if data is None or data.empty:
            return False, "数据为空"

        # 检查必需的列
        required_columns = ["time", "open", "high", "low", "close", "volume"]
        if not all(col in data.columns for col in required_columns):
            missing = [col for col in required_columns if col not in data.columns]
            return False, f"缺少必要列: {missing}"

        # 检查价格数据是否有效
        price_columns = ["open", "high", "low", "close"]
        if data[price_columns].isnull().values.any():
            return False, "价格数据包含空值"

        # 检查high >= low
        if not (data["high"] >= data["low"]).all():
            return False, "价格数据异常: high < low"

        # 检查数据是否有NaN值
        if data[required_columns].isna().any().any():
            logger.warning("数据中包含NaN值")

        # 检查数据是否按时间排序
        if not data['time'].is_monotonic_increasing:
            return False, "数据未按时间排序"

        return True, ""
