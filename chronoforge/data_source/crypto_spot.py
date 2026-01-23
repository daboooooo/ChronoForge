import logging
import inspect
from typing import Any, Dict, Optional, List, Deque
from collections import deque

import ccxt.async_support as ccxt
import pandas as pd
import time
import requests
from datetime import datetime, timezone
from .base import DataSourceBase, ParsedSymbol
from chronoforge.utils import parse_timeframe_to_milliseconds, with_retry
from chronoforge.decorators import periodic_task

logger = logging.getLogger(__name__)


class ExchangeConnectionPool:
    """交易所连接池管理类"""

    def __init__(self, exchange_name: str, config: Dict[str, Any], max_connections: int = 5,
                 connection_validity: int = 3600, min_connections: int = 1,
                 adaptive_factor: float = 0.2):
        """
        初始化连接池

        Args:
            exchange_name: 交易所名称
            config: 交易所配置
            max_connections: 最大连接数
            connection_validity: 连接有效期（秒）
            min_connections: 最小连接数
            adaptive_factor: 自适应调整因子（0-1之间）
        """
        self.exchange_name = exchange_name.lower()
        self.config = config
        self._max_connections = max_connections
        self._min_connections = min_connections
        self.connection_validity = connection_validity
        self.adaptive_factor = adaptive_factor

        # 可用连接队列（双向队列，支持从两端操作）
        self.available_connections: Deque[Dict[str, Any]] = deque()
        # 所有连接映射
        self.all_connections: List[Dict[str, Any]] = []

        # 自适应连接数相关统计
        self._request_count = 0
        self._last_request_time = time.time()
        self._connection_usage_counts = {}  # 连接使用次数统计
        self._last_adjust_time = time.time()
        self._adjust_interval = 60  # 每60秒调整一次连接数

    async def get_connection(self) -> ccxt.Exchange:
        """
        从连接池获取一个可用的交易所连接

        Returns:
            ccxt.Exchange: 可用的交易所实例
        """
        current_time = time.time()
        self._request_count += 1
        self._last_request_time = current_time

        # 智能连接复用：优先使用最近使用的连接
        connections_to_check = list(self.available_connections)
        self.available_connections.clear()

        valid_connections = []
        expired_connections = []

        # 首先检查所有可用连接，区分有效和过期连接
        for connection_info in connections_to_check:
            conn_id = id(connection_info['instance'])

            # 检查连接是否过期（基于创建时间和使用频率的双重标准）
            is_expired = False

            # 时间标准：超过有效期
            if current_time - connection_info['create_time'] > self.connection_validity:
                is_expired = True
            # 使用频率标准：长期未使用（超过有效期的一半）
            elif conn_id in self._connection_usage_counts and (
                    current_time - connection_info['last_used_time'] >
                    self.connection_validity / 2):
                is_expired = True

            if not is_expired:
                valid_connections.append(connection_info)
            else:
                expired_connections.append(connection_info)

        # 关闭所有过期连接
        for connection_info in expired_connections:
            try:
                await self._close_connection(connection_info)
                logger.info(f"成功关闭过期连接: {self.exchange_name}")
                # 从所有连接列表中移除
                self._remove_connection(connection_info)
                # 清理使用计数
                conn_id = id(connection_info['instance'])
                if conn_id in self._connection_usage_counts:
                    del self._connection_usage_counts[conn_id]
            except Exception as e:
                logger.warning(f"关闭过期连接 {self.exchange_name} 时出错: {str(e)}")

        # 将有效连接放回可用连接队列
        for connection_info in valid_connections:
            self.available_connections.append(connection_info)

        # 如果有可用连接，返回最近使用的一个
        if self.available_connections:
            connection_info = self.available_connections.pop()
            conn_id = id(connection_info['instance'])
            # 更新连接使用统计
            self._connection_usage_counts[conn_id] = \
                self._connection_usage_counts.get(conn_id, 0) + 1
            # 更新最后使用时间
            connection_info['last_used_time'] = current_time
            return connection_info['instance']

        # 自适应调整连接数
        self._adjust_max_connections()

        # 如果没有可用连接，检查是否可以创建新连接
        if len(self.all_connections) < self._max_connections:
            return await self._create_new_connection()

        # 如果所有连接都在使用中，创建新连接并记录警告
        logger.warning(f"连接池已满({len(self.all_connections)}/{self._max_connections})，"
                       f"正在创建额外连接: {self.exchange_name}")
        return await self._create_new_connection()

    async def return_connection(self, exchange_instance: ccxt.Exchange):
        """
        将连接归还到连接池

        Args:
            exchange_instance: 要归还的交易所实例
        """
        # 查找连接信息
        for connection_info in self.all_connections:
            if connection_info['instance'] is exchange_instance:
                # 智能归还：将最近使用的连接放在队列末尾，优先复用
                self.available_connections.append(connection_info)
                break

    def _adjust_max_connections(self):
        """
        自适应调整最大连接数
        根据最近的请求频率和连接使用情况动态调整
        """
        current_time = time.time()

        # 检查是否需要调整
        if current_time - self._last_adjust_time < self._adjust_interval:
            return

        # 计算请求频率（请求数/调整间隔）
        request_rate = self._request_count / (current_time - self._last_adjust_time)

        # 基于请求频率调整最大连接数
        # 公式：新连接数 = 当前连接数 + 频率 * 自适应因子 # 乘以10是为了更明显的调整效果
        # 对于高请求频率，我们需要更激进的调整
        new_max = int(self._max_connections + request_rate * self.adaptive_factor * 10)

        # 确保连接数在合理范围内
        new_max = max(self._min_connections, min(new_max, 20))  # 限制最大连接数为20

        if new_max != self._max_connections:
            logger.info(f"自适应调整连接数: {self.exchange_name} 从 {self._max_connections} 调整到 {new_max}")
            self._max_connections = new_max

        # 重置统计数据
        self._request_count = 0
        self._last_adjust_time = current_time

        # 清理不常用的连接
        self._clean_unused_connections()

    def _clean_unused_connections(self):
        """
        清理不常用的连接，保持连接池高效
        """
        current_time = time.time()

        # 只保留最常用的连接
        if len(self.all_connections) > self._max_connections:
            # 按使用频率排序连接
            connections_with_usage = []
            for connection_info in self.all_connections:
                conn_id = id(connection_info['instance'])
                usage = self._connection_usage_counts.get(conn_id, 0)
                connections_with_usage.append(
                    (-usage, current_time - connection_info['create_time'], connection_info))

            # 按使用频率和创建时间排序（优先保留使用频率高的）
            connections_with_usage.sort()

            # 关闭多余的连接
            connections_to_remove = []
            for i in range(self._max_connections, len(self.all_connections)):
                _, _, connection_info = connections_with_usage[i]
                connections_to_remove.append(connection_info)

            for connection_info in connections_to_remove:
                # 从可用连接队列中移除
                for conn in list(self.available_connections):
                    if conn['instance'] is connection_info['instance']:
                        self.available_connections.remove(conn)
                        break

                # 从所有连接列表中移除
                if connection_info in self.all_connections:
                    self.all_connections.remove(connection_info)

                # 清理使用计数
                conn_id = id(connection_info['instance'])
                if conn_id in self._connection_usage_counts:
                    del self._connection_usage_counts[conn_id]

                # 标记为需要关闭，在下次获取连接时会被关闭
                # 不再在同步方法中调用异步的close方法，避免死锁
                logger.info(f"标记不常用连接为需要关闭: {self.exchange_name}")

    async def _create_new_connection(self) -> ccxt.Exchange:
        """
        创建一个新的交易所连接

        Returns:
            ccxt.Exchange: 新创建的交易所实例
        """
        if self.exchange_name not in ccxt.exchanges:
            raise ValueError(f"不支持的交易所: {self.exchange_name}")

        # 准备ccxt配置参数
        ccxt_config = {
            'enableRateLimit': True,  # 启用速率限制
        }

        # 添加API凭据
        if self.config and self.exchange_name in self.config:
            exchange_config = self.config[self.exchange_name]
            if 'apiKey' in exchange_config:
                ccxt_config['apiKey'] = exchange_config['apiKey']
            if 'secret' in exchange_config:
                ccxt_config['secret'] = exchange_config['secret']

        # 创建交易所实例
        exchange_class: ccxt.Exchange = getattr(ccxt, self.exchange_name)
        exchange_instance = exchange_class(ccxt_config)

        try:
            # 加载市场数据作为验证
            await exchange_instance.load_markets()
            logger.info(f"成功连接到交易所: {self.exchange_name}")

            # 存储连接信息
            connection_info = {
                'instance': exchange_instance,
                'create_time': time.time(),
                'last_used_time': time.time()
            }
            self.all_connections.append(connection_info)
            return exchange_instance
        except Exception as e:
            logger.error(f"交易所连接失败: {str(e)}")
            raise

    async def _close_connection(self, connection_info: Dict[str, Any]):
        """
        关闭单个连接

        Args:
            connection_info: 连接信息
        """
        try:
            exchange_instance = connection_info['instance']

            # 关闭连接的所有尝试都使用try-except包裹，确保即使部分关闭失败，也能继续尝试其他关闭操作
            closed = False

            # 1. 尝试调用ccxt的close方法（这应该是最主要的关闭方式）
            if hasattr(exchange_instance, 'close'):
                try:
                    await exchange_instance.close()
                    logger.debug(f"成功关闭交易所连接: {self.exchange_name}")
                    closed = True
                except Exception as e:
                    logger.warning(f"调用ccxt.close关闭交易所连接 {self.exchange_name} 时出错: {str(e)}")

            # 2. 尝试关闭ccxt内部的aiohttp客户端（如果存在）
            if hasattr(exchange_instance, 'aiohttp_client') and exchange_instance.aiohttp_client:
                try:
                    await exchange_instance.aiohttp_client.close()
                    logger.debug(f"成功关闭交易所aiohttp客户端: {self.exchange_name}")
                    closed = True
                except Exception as e:
                    logger.warning(f"关闭交易所aiohttp客户端 {self.exchange_name} 时出错: {str(e)}")

            # 3. 尝试关闭ccxt内部的session（如果存在）
            if hasattr(exchange_instance, 'session') and exchange_instance.session:
                try:
                    if hasattr(exchange_instance.session, 'close'):
                        if inspect.iscoroutinefunction(exchange_instance.session.close):
                            await exchange_instance.session.close()
                        else:
                            exchange_instance.session.close()
                    logger.debug(f"成功关闭交易所会话: {self.exchange_name}")
                    closed = True
                except Exception as e:
                    logger.warning(f"关闭交易所会话 {self.exchange_name} 时出错: {str(e)}")

            # 4. 尝试清理ccxt内部的connector（如果存在）
            if hasattr(exchange_instance, '_connector') and exchange_instance._connector:
                try:
                    if hasattr(exchange_instance._connector, 'close'):
                        if inspect.iscoroutinefunction(exchange_instance._connector.close):
                            await exchange_instance._connector.close()
                        else:
                            exchange_instance._connector.close()
                    logger.debug(f"成功关闭交易所connector: {self.exchange_name}")
                    closed = True
                except Exception as e:
                    logger.warning(f"关闭交易所connector {self.exchange_name} 时出错: {str(e)}")

            if closed:
                logger.info(f"成功关闭交易所连接资源: {self.exchange_name}")
            else:
                logger.warning(f"无法关闭交易所连接资源: {self.exchange_name}")

            # 5. 尝试关闭ccxt内部的connector（如果存在，检查小写形式）
            if hasattr(exchange_instance, 'connector') and exchange_instance.connector:
                try:
                    if hasattr(exchange_instance.connector, 'close'):
                        if inspect.iscoroutinefunction(exchange_instance.connector.close):
                            await exchange_instance.connector.close()
                        else:
                            exchange_instance.connector.close()
                    logger.debug(f"成功关闭交易所connector: {self.exchange_name}")
                    closed = True
                except Exception as e:
                    logger.warning(f"关闭交易所connector {self.exchange_name} 时出错: {str(e)}")

            # 5. 尝试直接访问和关闭底层连接（ccxt可能将连接存储在不同的属性中）
            if hasattr(exchange_instance, '_http_client') and exchange_instance._http_client:
                try:
                    await exchange_instance._http_client.close()
                    logger.debug(f"成功关闭交易所HTTP客户端: {self.exchange_name}")
                except Exception as e:
                    logger.warning(f"关闭交易所HTTP客户端 {self.exchange_name} 时出错: {str(e)}")

            # 6. 尝试关闭可能存在的连接池
            if hasattr(exchange_instance, 'pool') and exchange_instance.pool:
                try:
                    await exchange_instance.pool.close()
                    logger.debug(f"成功关闭交易所连接池: {self.exchange_name}")
                except Exception as e:
                    logger.warning(f"关闭交易所连接池 {self.exchange_name} 时出错: {str(e)}")

            # 7. 尝试设置连接实例为None，帮助垃圾回收
            connection_info['instance'] = None

        except RuntimeError as e:
            if "Event loop is closed" in str(e):
                logger.warning(f"事件循环已关闭，跳过关闭交易所连接: {self.exchange_name}")
            else:
                logger.error(f"关闭交易所连接 {self.exchange_name} 时出错: {str(e)}")
        except Exception as e:
            logger.error(f"关闭交易所连接 {self.exchange_name} 时出错: {str(e)}")
        finally:
            # 无论如何都要从连接列表中移除
            self._remove_connection(connection_info)

    def _remove_connection(self, connection_info: Dict[str, Any]):
        """
        从连接列表中移除连接信息

        Args:
            connection_info: 连接信息
        """
        if connection_info in self.all_connections:
            self.all_connections.remove(connection_info)

    async def close_all_connections(self):
        """
        关闭连接池中的所有连接
        """
        for connection_info in list(self.all_connections):
            await self._close_connection(connection_info)
            self._remove_connection(connection_info)

        # 清空队列
        self.available_connections.clear()

    def get_pool_stats(self) -> Dict[str, Any]:
        """
        获取连接池统计信息

        Returns:
            Dict[str, Any]: 连接池统计信息
        """
        # 计算平均连接使用率
        avg_usage = 0.0
        if self._connection_usage_counts:
            avg_usage = sum(self._connection_usage_counts.values()) /\
                len(self._connection_usage_counts)

        return {
            'exchange_name': self.exchange_name,
            'total_connections': len(self.all_connections),
            'available_connections': len(self.available_connections),
            'max_connections': self._max_connections,
            'min_connections': self._min_connections,
            'connection_validity': self.connection_validity,
            'adaptive_factor': self.adaptive_factor,
            'avg_connection_usage': round(avg_usage, 2),
            'request_count': self._request_count,
            'last_adjust_time': self._last_adjust_time
        }


def okx_convert_contract_coin(parsed_symbol, rate, amount) -> float:
    """convert contract amount to coin amount in okx"""
    # instId format: ETH-USDT-SWAP
    instId = f"{parsed_symbol.base}-{parsed_symbol.quote}-SWAP"
    url = "https://www.okx.com/api/v5/public/convert-contract-coin?"
    url += f"type=2&instId={instId}"
    url += f"&px={rate:.12f}&sz={amount}"
    result = requests.get(url)
    if result.status_code != 200:
        logger.error(f"Request Error, URL: {url}")
        return 0.0
    data = dict(result.json())['data']
    if not data:
        logger.error(f"okx_convert_contract_coin, URL: {url}")
        return 0.0
    return float(data[0]['sz'])


def get_quote_volume(ticker, exchange_name: str, parsed_symbol: ParsedSymbol) -> float:
    quoteVolume = ticker['quoteVolume']
    if quoteVolume is not None:
        return float(quoteVolume)
    # calculate quoteVolume based on last price and baseVolume
    quoteVolume = 0.0
    if exchange_name == 'okx':
        baseVolume = ticker['baseVolume']
        if baseVolume is None:
            return 0.0
        last = ticker['last']
        coinVolume = okx_convert_contract_coin(parsed_symbol, last, baseVolume)
        quoteVolume = float(coinVolume) * float(last)
    else:
        logger.warning(f"Can't get {exchange_name} {parsed_symbol.original} quoteVolume.")
    return quoteVolume


class CryptoSpotDataSource(DataSourceBase):
    """CCXT交易所数据源插件，支持多种加密货币交易所"""

    def __init__(self, config: Dict[str, Any] = None):
        """初始化CCXT插件

        Args:
            config: 数据源配置
        """
        super().__init__(config)

        # 连接池配置
        self.pool_config = {
            'max_connections': 5,
            'connection_validity': 3600  # 1小时
        }

        # 覆盖默认配置
        if config and 'connection_pool' in config:
            self.pool_config.update(config['connection_pool'])

        # 存储交易所连接池，格式：{exchange_name: ExchangeConnectionPool}
        self.exchange_pools: Dict[str, ExchangeConnectionPool] = {}

        # 缓存tickers数据, 60秒缓存
        self.cache_tickers: Dict[str, tuple[dict, float]] = {}
        self.ticker_validity = 30  # 30秒缓存

    @property
    def name(self):
        """返回数据源名称"""
        return self.__class__.__name__.replace("DataSource", "")

    async def __aenter__(self):
        """异步上下文管理器的进入方法"""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器的退出方法，确保所有exchange连接都被正确关闭"""
        await self._close_all_connections()
        return False  # 不抑制异常

    async def _close_all_connections(self):
        """关闭所有交易所连接池

        此方法可以在异步代码中直接调用，确保所有的exchange连接池都被正确关闭。
        """
        for exchange_name, pool in list(self.exchange_pools.items()):
            try:
                await pool.close_all_connections()
                if exchange_name in self.exchange_pools:
                    del self.exchange_pools[exchange_name]
            except Exception as e:
                logger.error(f"关闭交易所连接池 {exchange_name} 时出错: {str(e)}")
                # 无论如何都要从字典中移除，避免内存泄漏
                del self.exchange_pools[exchange_name]

    async def _get_ccxt_exchange(self, exchange_name: str) -> ccxt.Exchange:
        """
        从连接池获取指定交易所实例

        Args:
            exchange_name: 交易所名称，如'binance', 'okx'

        Returns:
            ccxt.Exchange: 初始化后的交易所实例
        """
        # 转换交易所名称为小写
        exchange_name = exchange_name.lower()

        # 检查是否已有连接池
        if exchange_name not in self.exchange_pools:
            # 创建新的连接池
            self.exchange_pools[exchange_name] = ExchangeConnectionPool(
                exchange_name=exchange_name,
                config=self.config,
                max_connections=self.pool_config['max_connections'],
                connection_validity=self.pool_config['connection_validity']
            )

        # 从连接池获取连接
        pool = self.exchange_pools[exchange_name]
        return await pool.get_connection()

    def _is_in_blacklist(self, symbol: str) -> bool:
        if 'blacklist' not in self.config:
            return False
        for item in self.config.get('blacklist', []):
            if item in symbol:
                return True
        return False

    async def _fetch_quote_tickers(self, exchange_name: str, force: bool = False):
        """获取交易所当前价格 tickers

        Args:
            exchange_name: 交易所名称，如'binance', 'okx'
            force: 是否强制重新获取，默认为False

        Returns:
            dict: 包含当前价格信息的字典，包含'ask', 'bid', 'last', 'volume'等字段
        """
        quote_tickers: dict[str, dict[str, dict]] = {}
        if not force and exchange_name in self.cache_tickers:
            quote_tickers, created_time = self.cache_tickers[exchange_name]
            if time.time() - created_time < self.ticker_validity:
                return quote_tickers

        # fetch tickers from exchange
        exchange_instance = await self._get_ccxt_exchange(exchange_name)
        try:
            tickers: dict[str, dict] = await exchange_instance.fetchTickers()
            logger.info(f"Got {len(tickers)} tickers from {exchange_name}.")
            # filter valid tickers and group by quote asset
            for symbol, ticker in tickers.items():
                # ignore invalid symbol
                if '/' not in symbol:
                    continue
                # get parsed symbol
                parsed_symbol = ParsedSymbol(symbol)
                # check whether ticker is valid
                if ticker['last'] is None or \
                        (ticker['baseVolume'] == 0.0 and ticker['quoteVolume'] == 0.0):
                    # In binance swap ticker, ask and bit are both 0.0.
                    # (ticker['ask'] is None or ticker['ask'] == 0.0) or \
                    # (ticker['bid'] is None or ticker['bid'] == 0.0) or \
                    # logger.info(
                    #     f"Invalid {self.exchange_id} {symbol} ticker: {ticker}")
                    continue
                # ignore to base token if it is in the blacklist
                if self._is_in_blacklist(parsed_symbol.base):
                    logger.debug(f"Ignore {exchange_name} {symbol} ticker [blacklist].")
                    continue
                # Ignore expired tickers
                # It may be missing, not all exchanges provide a timestamp there.
                # e.g. Gate.io
                # Upbit timestamp is always wrong.
                try:
                    if 'timestamp' in ticker and ticker['timestamp'] is not None:
                        ticker_timestamp_ms = int(ticker['timestamp'])
                        expired_time_ms = int(time.time() * 1000) - self.ticker_validity * 1000
                        if ticker_timestamp_ms < expired_time_ms:
                            logger.debug(
                                f"Expired ticker: {exchange_name} {symbol}." +
                                f" {ticker_timestamp_ms} < {expired_time_ms}")
                            continue
                except (TypeError, ValueError):
                    pass
                # add ticker to quote_tickers
                quote_tickers.setdefault(parsed_symbol.quote, {}).update({
                    symbol: ticker
                })
            self.cache_tickers[exchange_name] = (quote_tickers, time.time())
        finally:
            # 将连接归还到连接池
            if exchange_name in self.exchange_pools:
                await self.exchange_pools[exchange_name].return_connection(exchange_instance)

        return quote_tickers

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
        """
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
        exchange = await self._get_ccxt_exchange(exchange_name)
        all_ohlcv = []

        try:
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
                        f"end_ts_ms: {until_ts_ms} "
                        f"({datetime.fromtimestamp(until_ts_ms / 1000, tz=timezone.utc)})")

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

                    # 检查事件循环是否已关闭
                    import asyncio
                    try:
                        loop = asyncio.get_event_loop()
                        if loop.is_closed():
                            logger.error(f"事件循环已关闭，无法获取 {symbol} 数据")
                            return None
                    except RuntimeError:
                        logger.error(f"无法获取事件循环，无法获取 {symbol} 数据")
                        return None

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
        finally:
            # 将连接归还到连接池
            if exchange_name in self.exchange_pools:
                await self.exchange_pools[exchange_name].return_connection(exchange)

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

    async def tickers(self, exchange_name: str, quote: Optional[str] = None) -> Any:
        """获取所有交易所的Spot交易对tickers

        Args:
            exchange_name (str): 交易所名称
            quote (Optional[str], optional): 报价资产. Defaults to None.

        Returns:
            dict: 键为交易所名称，值为该交易所的tickers数据
        """
        # 获取交易所的tickers数据
        tickers: dict[str, dict] = await self._fetch_quote_tickers(exchange_name)
        if not quote:
            return tickers

        return tickers.get(quote, {})

    @periodic_task(interval=60, symbols=[], timeframe=None, timerange_str=None,
                   params={'exchange_name': 'binance', 'quote': 'USDT'})
    async def tickers_binance(self, exchange_name: str, quote: Optional[str] = None) -> Any:
        """获取Binance交易所的Spot交易对tickers

        Args:
            exchange_name (str): 交易所名称
            quote (Optional[str], optional): 报价资产. Defaults to None.

        Returns:
            dict: 键为交易所名称，值为该交易所的tickers数据
        """
        return await self.tickers(exchange_name, quote)

    @periodic_task(interval=60, symbols=[], timeframe=None, timerange_str=None,
                   params={'exchange_name': 'okx', 'quote': 'USDT'})
    async def tickers_okx(self, exchange_name: str, quote: Optional[str] = None) -> Any:
        """获取OKX交易所的Spot交易对tickers

        Args:
            exchange_name (str): 交易所名称
            quote (Optional[str], optional): 报价资产. Defaults to None.

        Returns:
            dict: 键为交易所名称，值为该交易所的tickers数据
        """
        return await self.tickers(exchange_name, quote)

    async def top_volume_symbols(self, exchange_name: str, quote: str,
                                 top_n: Optional[int] = None,
                                 top_percent: Optional[int] = None) -> list[str]:
        """获取指定交易所的按成交量排序的交易对

        Args:
            exchange_name (str): 交易所名称
            quote (str): 报价资产
            top_n (Optional[int], optional): 要获取的交易对数量. Defaults to None.
            top_percent (Optional[int], optional): 要获取的交易对数量占比. Defaults to None.

        Returns:
            list[str]: top N交易对的列表
        """
        tickers = await self.tickers(exchange_name, quote)
        if not tickers:
            return []
        # rank symbols by volume
        sorted_symbols = sorted(
            tickers.keys(),
            key=lambda x: get_quote_volume(tickers[x], exchange_name, ParsedSymbol(x)),
            reverse=True
        )
        if top_n:
            return sorted_symbols[:top_n]
        if top_percent:
            return sorted_symbols[:int(len(sorted_symbols) * top_percent / 100)]
        return sorted_symbols
