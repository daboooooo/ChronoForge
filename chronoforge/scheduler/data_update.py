"""
数据获取和更新任务模块

实现TODOs中定义的数据更新任务：
1. 每30秒获取一次binance和okx的spot tickers，以及binance umfuture的tickers
2. 每小时结束30秒后，刷新spot ohlcv数据（前80% volume symbols）
3. 每小时结束30秒后，刷新umfuture合约市场数据
4. 每小时结束30秒后，刷新coingecko的coin markets数据
5. 每小时结束30秒后，刷新fred的经济数据
6. 每小时结束30秒后，刷新global crypto market数据

核心功能：
- 增量数据更新（基于时间戳）
- 数据合并和去重
- 错误处理和重试
- 进度追踪
- 统计和监控
"""

import logging
import time
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass, field
from enum import Enum
import pandas as pd

from chronoforge.data_source import DataSourceManager
from chronoforge.storage.manager import StorageManager
from chronoforge.scheduler.task_scheduler import TaskStatus
from chronoforge.scheduler.scheduler_config import get_data_config, DataType

logger = logging.getLogger(__name__)


class UpdateStrategy(Enum):
    """数据更新策略"""
    INCREMENTAL = "incremental"
    FULL = "full"
    SMART = "smart"


RETRYABLE_ERRORS = (
    "timeout",
    "connection",
    "rate limit",
    "429",
    "500",
    "502",
    "503",
    "504"
)


def is_retryable_error(error: Exception) -> bool:
    """判断错误是否可重试"""
    error_str = str(error).lower()
    return any(keyword in error_str for keyword in RETRYABLE_ERRORS)


@dataclass
class UpdateResult:
    """数据更新结果"""
    task_name: str
    symbol: str
    status: TaskStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    records_updated: int = 0
    records_total: int = 0
    error: Optional[str] = None
    duration_ms: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SymbolUniverse:
    """交易对全集管理"""
    spot_symbols: Dict[str, Set[str]] = field(default_factory=dict)  # {exchange: Set[symbol]}
    futures_symbols: Set[str] = field(default_factory=set)
    coin_symbols: Set[str] = field(default_factory=set)
    macro_symbols: Set[str] = field(default_factory=set)
    last_updated: Optional[datetime] = None

    def get_all_spot_symbols(self) -> List[str]:
        """获取所有spot交易对"""
        all_symbols = []
        for exchange, symbols in self.spot_symbols.items():
            all_symbols.extend(list(symbols))
        return all_symbols

    def get_top_volume_symbols(self, exchange: str, quote: str, top_percent: float) -> List[str]:
        """获取成交量前N%的交易对"""
        if exchange not in self.spot_symbols:
            return []
        symbols = list(self.spot_symbols[exchange])
        n = int(len(symbols) * top_percent / 100)
        return symbols[:n]


class DataUpdateManager:
    """
    数据更新管理器

    职责：
    - 协调数据源和存储
    - 执行增量数据更新
    - 管理交易对全集
    - 提供更新进度追踪
    - 错误处理和恢复

    设计原则：
    1. 先读取缓存数据
    2. 根据缓存数据的timestamp计算需要更新的时间范围
    3. 调用数据源获取最新数据
    4. 合并新旧数据并去重
    5. 更新缓存
    """

    def __init__(
        self,
        data_source_manager: DataSourceManager,
        storage_manager: StorageManager,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        初始化数据更新管理器

        Args:
            data_source_manager: 数据源管理器
            storage_manager: 存储管理器
            config: 配置参数
        """
        self.dsm = data_source_manager
        self.sm = storage_manager
        self.config = config or {}

        self.symbol_universe = SymbolUniverse()

        self._spot_tickers_cache: Dict[str, tuple[Dict, float]] = {}
        self._ticker_cache_validity = 30

        ohlcv_config = get_data_config(DataType.OHLCV)
        self._ohlcv_retry_max = ohlcv_config.retry_max if ohlcv_config else 2
        self._ohlcv_retry_backoff = ohlcv_config.retry_backoff if ohlcv_config else 3.0

        futures_config = get_data_config(DataType.FUTURES)
        self._futures_retry_max = futures_config.retry_max if futures_config else 2
        self._futures_retry_backoff = futures_config.retry_backoff if futures_config else 3.0

        self._update_stats: Dict[str, Any] = {
            'total_updates': 0,
            'successful_updates': 0,
            'failed_updates': 0,
            'total_records_updated': 0,
            'last_update_time': None
        }

        self._running = False

        ohlcv_config = get_data_config(DataType.OHLCV)
        self._ohlcv_concurrent_limit = ohlcv_config.concurrent_limit if ohlcv_config else 20

        futures_config = get_data_config(DataType.FUTURES)
        self._futures_concurrent_limit = futures_config.concurrent_limit if futures_config else 15

        ticker_config = get_data_config(DataType.TICKER)
        self._ticker_cache_validity = ticker_config.cache_validity_seconds if ticker_config else 30

    async def initialize(self) -> 'DataUpdateManager':
        """初始化并加载交易对全集"""
        logger.info("Initializing DataUpdateManager...")

        await self._load_symbol_universe()

        spot_count = sum(len(s) for s in self.symbol_universe.spot_symbols.values())
        futures_count = len(self.symbol_universe.futures_symbols)
        logger.info(f"Loaded symbol universe: {spot_count} spot symbols, "
                    f"{futures_count} futures symbols")

        return self

    async def _load_symbol_universe(self):
        """加载交易对全集"""
        try:
            for exchange in ['binance', 'okx']:
                spot_source = self.dsm.get_data_source(exchange)
                if spot_source:
                    try:
                        top_symbols = await spot_source.top_volume_symbols(
                            exchange_name=exchange,
                            quote='USDT',
                            top_percent=80
                        )
                        if top_symbols:
                            self.symbol_universe.spot_symbols[exchange] = set(top_symbols)
                            logger.info(f"Loaded {len(top_symbols)} symbols from {exchange}")
                    except Exception as e:
                        logger.warning(f"Failed to load {exchange} symbols: {e}")

            futures_source = self.dsm.get_data_source('futures')
            if futures_source:
                try:
                    futures_source._get_exchange_info()
                    self.symbol_universe.futures_symbols = set(futures_source.symbols or [])
                    futures_count = len(self.symbol_universe.futures_symbols)
                    logger.info(f"Loaded {futures_count} futures symbols")
                except Exception as e:
                    logger.warning(f"Failed to load futures symbols: {e}")

            self.symbol_universe.last_updated = datetime.now(timezone.utc)

        except Exception as e:
            logger.error(f"Failed to load symbol universe: {e}")

    async def _get_cached_data_timestamp(
        self,
        data_id: str,
        data_type: str
    ) -> Optional[int]:
        """获取缓存数据的时间戳"""
        try:
            time_range = await self.sm.get_time_range(
                id=data_id,
                task_type=data_type
            )
            if time_range and 'end_time' in time_range:
                end_time = time_range['end_time']
                if hasattr(end_time, 'timestamp'):
                    return int(end_time.timestamp() * 1000)
                return int(end_time)
        except Exception as e:
            logger.debug(f"Failed to get cached timestamp for {data_id}: {e}")
        return None

    def _should_update_tickers(self, exchange: str) -> bool:
        """检查是否应该更新tickers（基于缓存有效性）"""
        if exchange not in self._spot_tickers_cache:
            return True

        _, cached_time = self._spot_tickers_cache[exchange]
        age = time.time() - cached_time
        should_update = age >= self._ticker_cache_validity

        logger.debug(
            f"Tickers cache for {exchange}: age={age:.1f}s, "
            f"validity={self._ticker_cache_validity}s, should_update={should_update}"
        )
        return should_update

    def _update_tickers_cache(self, exchange: str, data: Dict) -> None:
        """更新tickers缓存"""
        self._spot_tickers_cache[exchange] = (data, time.time())
        logger.debug(f"Updated tickers cache for {exchange}")

    async def _merge_and_save(
        self,
        data_id: str,
        new_data: pd.DataFrame,
        data_type: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> UpdateResult:
        """合并新旧数据并保存"""
        start_time = datetime.now(timezone.utc)
        result = UpdateResult(
            task_name=f"merge_and_save_{data_type}",
            symbol=data_id,
            status=TaskStatus.RUNNING,
            start_time=start_time
        )

        try:
            if new_data.empty:
                result.status = TaskStatus.COMPLETED
                result.records_updated = 0
                result.records_total = 0
                return result

            time_col = None
            for col in ['ts', 'time', 'timestamp']:
                if col in new_data.columns:
                    time_col = col
                    break

            existing_data = await self.sm.load(
                id=data_id,
                task_type=data_type
            )

            if existing_data is not None and not existing_data.empty:
                if time_col and time_col in new_data.columns:
                    new_ts_set = set(new_data[time_col])
                    existing_time_col = 'ts' if 'ts' in existing_data.columns else time_col
                    existing_data = existing_data[
                        ~existing_data[existing_time_col].isin(new_ts_set)
                    ]
                    merged_data = pd.concat([existing_data, new_data], ignore_index=True)
                else:
                    merged_data = new_data
            else:
                merged_data = new_data

            success = await self.sm.save(
                id=data_id,
                data=merged_data,
                task_type=data_type,
                metadata=metadata
            )

            if success:
                result.status = TaskStatus.COMPLETED
                result.records_updated = len(new_data)
                result.records_total = len(merged_data)
            else:
                result.status = TaskStatus.FAILED
                result.error = "Save failed"

        except Exception as e:
            result.status = TaskStatus.FAILED
            result.error = str(e)
            logger.error(f"Failed to merge and save {data_id}: {e}")

        finally:
            result.end_time = datetime.now(timezone.utc)
            result.duration_ms = (result.end_time - start_time).total_seconds() * 1000

        return result

    async def update_spot_tickers(
        self,
        exchanges: Optional[List[str]] = None,
        quotes: Optional[List[str]] = None
    ) -> Dict[str, UpdateResult]:
        """
        更新spot tickers数据（每30秒执行）

        流程：
        1. 获取各交易所的tickers数据
        2. 过滤并标准化数据
        3. 保存到存储

        Args:
            exchanges: 交易所列表
            quotes: 报价资产列表

        Returns:
            Dict[str, UpdateResult]: 各交易所的更新结果
        """
        exchanges = exchanges or ['binance', 'okx']
        quotes = quotes or ['USDT']
        results = {}

        exchanges_to_update = [ex for ex in exchanges if self._should_update_tickers(ex)]

        if not exchanges_to_update:
            logger.info(f"Skipping tickers update: all caches valid for {exchanges}")
            for exchange in exchanges:
                results[exchange] = UpdateResult(
                    task_name="update_spot_tickers",
                    symbol=exchange,
                    status=TaskStatus.COMPLETED,
                    start_time=datetime.now(timezone.utc),
                    records_updated=0,
                    records_total=0
                )
            return results

        logger.info(f"Starting spot tickers update for exchanges: {exchanges_to_update}")

        for exchange in exchanges_to_update:
            start_time = datetime.now(timezone.utc)
            result = UpdateResult(
                task_name="update_spot_tickers",
                symbol=exchange,
                status=TaskStatus.RUNNING,
                start_time=start_time
            )

            try:
                spot_source = self.dsm.get_data_source(exchange)
                if not spot_source:
                    raise ValueError(f"Data source for {exchange} not found")

                tickers_data = {}
                for quote in quotes:
                    quote_tickers = await spot_source.tickers(exchange, quote)
                    if quote_tickers:
                        tickers_data[quote] = quote_tickers

                if tickers_data:
                    self._update_tickers_cache(exchange, tickers_data)

                    for quote, tickers in tickers_data.items():
                        if tickers:
                            df = pd.DataFrame(tickers).T
                            df['ts'] = pd.Timestamp.now(tz='utc')
                            df['market_type'] = 'spot'

                            await self.sm.save(
                                id=f"{exchange}_tickers_{quote}",
                                data=df,
                                task_type='tickers',
                                metadata={
                                    'exchange': exchange,
                                    'quote': quote,
                                    'data_type': 'tickers',
                                    'updated_at': start_time.isoformat()
                                }
                            )

                    result.status = TaskStatus.COMPLETED
                    result.records_total = sum(len(t) for t in tickers_data.values())
                    result.records_updated = result.records_total

            except Exception as e:
                result.status = TaskStatus.FAILED
                result.error = str(e)
                logger.error(f"Failed to update {exchange} tickers: {e}")

            finally:
                result.end_time = datetime.now(timezone.utc)
                result.duration_ms = (result.end_time - start_time).total_seconds() * 1000
                results[exchange] = result

        return results

    async def update_spot_ohlcv(
        self,
        exchanges: Optional[List[str]] = None,
        timeframe: str = '1h',
        top_percent: float = 80.0,
        update_strategy: UpdateStrategy = UpdateStrategy.INCREMENTAL
    ) -> Dict[str, UpdateResult]:
        """
        更新spot OHLCV数据（每小时执行）- 并发版本

        流程：
        1. 获取需要更新的symbols（前80% volume）
        2. 使用信号量控制并发数
        3. 并发获取多个symbol数据

        Args:
            exchanges: 交易所列表
            timeframe: 时间框架
            top_percent: 成交量前N%
            update_strategy: 更新策略

        Returns:
            Dict[str, UpdateResult]: 各symbol的更新结果
        """
        exchanges = exchanges or ['binance', 'okx']
        results = {}

        logger.info(f"Starting spot OHLCV update: timeframe={timeframe}, "
                    f"top_percent={top_percent}%, concurrent_limit={self._ohlcv_concurrent_limit}")

        semaphore = asyncio.Semaphore(self._ohlcv_concurrent_limit)

        async def limited_update(exchange: str, symbol: str) -> tuple:
            async with semaphore:
                result = await self._update_single_ohlcv(
                    exchange=exchange,
                    symbol=symbol,
                    timeframe=timeframe,
                    strategy=update_strategy
                )
                return symbol, result

        all_tasks = []
        for exchange in exchanges:
            symbols = self.symbol_universe.get_top_volume_symbols(exchange, 'USDT', top_percent)
            logger.info(f"Queueing {len(symbols)} symbols for {exchange}")
            for symbol in symbols:
                all_tasks.append(limited_update(exchange, symbol))

        if all_tasks:
            task_results = await asyncio.gather(*all_tasks, return_exceptions=True)
            for item in task_results:
                if isinstance(item, Exception):
                    logger.error(f"Task failed with exception: {item}")
                else:
                    symbol, result = item
                    results[symbol] = result

        logger.info(f"Completed OHLCV update: {len(results)} symbols processed")
        return results

    async def _update_single_ohlcv(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        strategy: UpdateStrategy
    ) -> UpdateResult:
        """更新单个symbol的OHLCV数据"""
        start_time = datetime.now(timezone.utc)
        data_id = f"{symbol}_{timeframe}"
        result = UpdateResult(
            task_name="update_spot_ohlcv",
            symbol=data_id,
            status=TaskStatus.RUNNING,
            start_time=start_time
        )

        try:
            spot_source = self.dsm.get_data_source(exchange)
            if not spot_source:
                raise ValueError(f"Data source for {exchange} not found")

            if strategy == UpdateStrategy.INCREMENTAL:
                cached_ts = await self._get_cached_data_timestamp(data_id, 'ohlcv')
                if cached_ts:
                    start_ts_ms = cached_ts + 1
                else:
                    thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=30)
                    start_ts_ms = int(thirty_days_ago.timestamp() * 1000)
            else:
                thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=30)
                start_ts_ms = int(thirty_days_ago.timestamp() * 1000)

            end_ts_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

            full_symbol = f"{exchange}:{symbol}"

            new_data = None
            for attempt in range(self._ohlcv_retry_max + 1):
                try:
                    new_data = await spot_source.fetch(
                        symbol=full_symbol,
                        timeframe=timeframe,
                        start_ts_ms=start_ts_ms,
                        end_ts_ms=end_ts_ms
                    )
                    break
                except Exception as fetch_error:
                    if attempt < self._ohlcv_retry_max and is_retryable_error(fetch_error):
                        wait_time = self._ohlcv_retry_backoff ** attempt
                        logger.warning(
                            f"Retry {attempt + 1}/{self._ohlcv_retry_max} for "
                            f"{exchange}:{symbol} after {wait_time}s: {fetch_error}"
                        )
                        await asyncio.sleep(wait_time)
                    else:
                        raise

            if new_data is None or new_data.empty:
                result.status = TaskStatus.COMPLETED
                result.records_updated = 0
                return result

            merge_result = await self._merge_and_save(
                data_id=data_id,
                new_data=new_data,
                data_type='ohlcv',
                metadata={
                    'exchange': exchange,
                    'symbol': symbol,
                    'timeframe': timeframe,
                    'data_type': 'ohlcv',
                    'update_strategy': strategy.value
                }
            )

            result.records_updated = merge_result.records_updated
            result.records_total = merge_result.records_total
            result.status = merge_result.status

        except Exception as e:
            result.status = TaskStatus.FAILED
            result.error = str(e)
            logger.error(f"Failed to update OHLCV for {exchange}:{symbol}: {e}")

        finally:
            result.end_time = datetime.now(timezone.utc)
            result.duration_ms = (result.end_time - start_time).total_seconds() * 1000

        return result

    async def update_futures_metrics(
        self,
        symbols: Optional[List[str]] = None,
        timeframe: str = '1h',
        update_strategy: UpdateStrategy = UpdateStrategy.INCREMENTAL
    ) -> Dict[str, UpdateResult]:
        """
        更新umfuture合约市场数据（每小时执行）- 并发版本

        流程：
        1. 获取所有umfuture symbols
        2. 使用信号量控制并发数
        3. 并发获取多个symbol数据

        Args:
            symbols: 合约symbol列表
            timeframe: 时间框架
            update_strategy: 更新策略

        Returns:
            Dict[str, UpdateResult]: 各symbol的更新结果
        """
        if not symbols:
            symbols = list(self.symbol_universe.futures_symbols)

        results = {}
        logger.info(f"Starting futures metrics update: {len(symbols)} symbols, "
                    f"concurrent_limit={self._futures_concurrent_limit}")

        semaphore = asyncio.Semaphore(self._futures_concurrent_limit)

        async def limited_update(symbol: str) -> tuple:
            async with semaphore:
                result = await self._update_single_futures_metrics(
                    symbol=symbol,
                    timeframe=timeframe,
                    strategy=update_strategy
                )
                return symbol, result

        all_tasks = [limited_update(s) for s in symbols]

        if all_tasks:
            task_results = await asyncio.gather(*all_tasks, return_exceptions=True)
            for item in task_results:
                if isinstance(item, Exception):
                    logger.error(f"Task failed with exception: {item}")
                else:
                    symbol, result = item
                    results[symbol] = result

        logger.info(f"Completed futures metrics update: {len(results)} symbols processed")
        return results

    async def _update_single_futures_metrics(
        self,
        symbol: str,
        timeframe: str,
        strategy: UpdateStrategy
    ) -> UpdateResult:
        """更新单个合约的指标数据"""
        start_time = datetime.now(timezone.utc)
        result = UpdateResult(
            task_name="update_futures_metrics",
            symbol=symbol,
            status=TaskStatus.RUNNING,
            start_time=start_time
        )

        try:
            futures_source = self.dsm.get_data_source('futures')
            if not futures_source:
                raise ValueError("Futures data source not found")

            if strategy == UpdateStrategy.INCREMENTAL:
                cached_ts = await self._get_cached_data_timestamp(symbol, 'futures_metrics')
                if cached_ts:
                    start_ts_ms = cached_ts + 1
                else:
                    seven_days_ago = datetime.now(timezone.utc) - timedelta(days=7)
                    start_ts_ms = int(seven_days_ago.timestamp() * 1000)
            else:
                seven_days_ago = datetime.now(timezone.utc) - timedelta(days=7)
                start_ts_ms = int(seven_days_ago.timestamp() * 1000)

            end_ts_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

            new_data = None
            for attempt in range(self._futures_retry_max + 1):
                try:
                    new_data = await futures_source.fetch(
                        symbol=symbol,
                        timeframe=timeframe,
                        start_ts_ms=start_ts_ms,
                        end_ts_ms=end_ts_ms
                    )
                    break
                except Exception as fetch_error:
                    if attempt < self._futures_retry_max and is_retryable_error(fetch_error):
                        wait_time = self._futures_retry_backoff ** attempt
                        logger.warning(
                            f"Retry {attempt + 1}/{self._futures_retry_max} for "
                            f"{symbol} after {wait_time}s: {fetch_error}"
                        )
                        await asyncio.sleep(wait_time)
                    else:
                        raise

            if new_data is None or new_data.empty:
                result.status = TaskStatus.COMPLETED
                result.records_updated = 0
                return result

            merge_result = await self._merge_and_save(
                data_id=f"{symbol}_futures",
                new_data=new_data,
                data_type='futures_metrics',
                metadata={
                    'symbol': symbol,
                    'timeframe': timeframe,
                    'data_type': 'futures_metrics',
                    'exchange': 'binance_um'
                }
            )

            result.records_updated = merge_result.records_updated
            result.records_total = merge_result.records_total
            result.status = merge_result.status

        except Exception as e:
            result.status = TaskStatus.FAILED
            result.error = str(e)
            logger.error(f"Failed to update futures metrics for {symbol}: {e}")

        finally:
            result.end_time = datetime.now(timezone.utc)
            result.duration_ms = (result.end_time - start_time).total_seconds() * 1000

        return result

    async def update_coin_markets(
        self,
        update_strategy: UpdateStrategy = UpdateStrategy.INCREMENTAL
    ) -> UpdateResult:
        """
        更新coingecko coin markets数据（每小时执行）

        流程：
        1. 读取本地缓存的coin markets数据
        2. 根据缓存数据的timestamp计算需要更新的时间范围
        3. 调用coingecko数据源的update方法获取最新数据
        4. 合并更新缓存中的coin markets数据

        Args:
            update_strategy: 更新策略

        Returns:
            UpdateResult: 更新结果
        """
        start_time = datetime.now(timezone.utc)
        result = UpdateResult(
            task_name="update_coin_markets",
            symbol="coingecko_global",
            status=TaskStatus.RUNNING,
            start_time=start_time
        )

        try:
            coingecko_source = self.dsm.get_data_source('coingecko')
            if not coingecko_source:
                raise ValueError("CoinGecko data source not found")

            new_data = await coingecko_source.get_coin_markets()

            if new_data is None or new_data.empty:
                result.status = TaskStatus.COMPLETED
                return result

            merge_result = await self._merge_and_save(
                data_id="coingecko_coin_markets",
                new_data=new_data,
                data_type='coin_markets',
                metadata={
                    'data_type': 'coin_markets',
                    'source': 'coingecko',
                    'updated_at': start_time.isoformat()
                }
            )

            result.records_updated = merge_result.records_updated
            result.records_total = merge_result.records_total
            result.status = merge_result.status

        except Exception as e:
            result.status = TaskStatus.FAILED
            result.error = str(e)
            logger.error(f"Failed to update coin markets: {e}")

        finally:
            result.end_time = datetime.now(timezone.utc)
            result.duration_ms = (result.end_time - start_time).total_seconds() * 1000

        return result

    async def update_macro_data(
        self,
        indicators: Optional[List[str]] = None,
        update_strategy: UpdateStrategy = UpdateStrategy.INCREMENTAL
    ) -> Dict[str, UpdateResult]:
        """
        更新fred经济数据（每小时执行）

        流程：
        1. 读取本地缓存的经济数据
        2. 根据缓存数据的timestamp计算需要更新的时间范围
        3. 调用fred数据源的update方法获取最新数据
        4. 合并更新缓存中的经济数据

        Args:
            indicators: 经济指标列表
            update_strategy: 更新策略

        Returns:
            Dict[str, UpdateResult]: 各指标的更新结果
        """
        from chronoforge.data_source.fred import FRED_DAILY_RATES

        if not indicators:
            indicators = FRED_DAILY_RATES

        results = {}
        logger.info(f"Starting macro data update: {len(indicators)} indicators")

        for indicator in indicators:
            result = await self._update_single_macro(
                indicator=indicator,
                strategy=update_strategy
            )
            results[indicator] = result

        return results

    async def _update_single_macro(
        self,
        indicator: str,
        strategy: UpdateStrategy
    ) -> UpdateResult:
        """更新单个经济指标数据"""
        start_time = datetime.now(timezone.utc)
        result = UpdateResult(
            task_name="update_macro_data",
            symbol=indicator,
            status=TaskStatus.RUNNING,
            start_time=start_time
        )

        try:
            fred_source = self.dsm.get_data_source('fred')
            if not fred_source:
                raise ValueError("FRED data source not found")

            if strategy == UpdateStrategy.INCREMENTAL:
                cached_ts = await self._get_cached_data_timestamp(indicator, 'macro_fred')
                if cached_ts:
                    start_ts_ms = cached_ts + 1
                else:
                    year_ago = datetime.now(timezone.utc) - timedelta(days=365)
                    start_ts_ms = int(year_ago.timestamp() * 1000)
            else:
                year_ago = datetime.now(timezone.utc) - timedelta(days=365)
                start_ts_ms = int(year_ago.timestamp() * 1000)

            end_ts_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

            new_data = await fred_source.fetch(
                symbol=indicator,
                timeframe='1d',
                start_ts_ms=start_ts_ms,
                end_ts_ms=end_ts_ms
            )

            if new_data is None or new_data.empty:
                result.status = TaskStatus.COMPLETED
                return result

            merge_result = await self._merge_and_save(
                data_id=indicator,
                new_data=new_data,
                data_type='macro_fred',
                metadata={
                    'series_id': indicator,
                    'data_type': 'macro_fred',
                    'source': 'fred'
                }
            )

            result.records_updated = merge_result.records_updated
            result.records_total = merge_result.records_total
            result.status = merge_result.status

        except Exception as e:
            result.status = TaskStatus.FAILED
            result.error = str(e)
            logger.error(f"Failed to update macro data for {indicator}: {e}")

        finally:
            result.end_time = datetime.now(timezone.utc)
            result.duration_ms = (result.end_time - start_time).total_seconds() * 1000

        return result

    async def update_global_crypto_market(
        self,
        strategy: UpdateStrategy = UpdateStrategy.INCREMENTAL
    ) -> UpdateResult:
        """
        更新global crypto market数据（每小时执行）

        流程：
        1. 读取本地数据库中的缓存的global crypto market数据
        2. 根据缓存数据的timestamp，计算需要更新的时间范围
        3. 调用global_crypto_market数据源的update方法，获取最新的global crypto market数据
        4. 合并更新缓存中的global crypto market数据

        Args:
            update_strategy: 更新策略

        Returns:
            UpdateResult: 更新结果
        """
        start_time = datetime.now(timezone.utc)
        result = UpdateResult(
            task_name="update_global_crypto_market",
            symbol="global_market",
            status=TaskStatus.RUNNING,
            start_time=start_time
        )

        try:
            global_source = self.dsm.get_data_source('global_market')
            if not global_source:
                raise ValueError("Global market data source not found")

            if strategy == UpdateStrategy.INCREMENTAL:
                cached_ts = await self._get_cached_data_timestamp("global_market", 'global_market')
                if cached_ts:
                    start_ts_ms = cached_ts + 1
                else:
                    thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=30)
                    start_ts_ms = int(thirty_days_ago.timestamp() * 1000)
            else:
                thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=30)
                start_ts_ms = int(thirty_days_ago.timestamp() * 1000)

            end_ts_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

            new_data = await global_source.fetch(
                symbol='BTC/USDT',
                timeframe='1d',
                start_ts_ms=start_ts_ms,
                end_ts_ms=end_ts_ms
            )

            if new_data is None or new_data.empty:
                result.status = TaskStatus.COMPLETED
                return result

            merge_result = await self._merge_and_save(
                data_id="global_crypto_market",
                new_data=new_data,
                data_type='global_market',
                metadata={
                    'data_type': 'global_market',
                    'source': 'yfinance',
                    'updated_at': start_time.isoformat()
                }
            )

            result.records_updated = merge_result.records_updated
            result.records_total = merge_result.records_total
            result.status = merge_result.status

        except Exception as e:
            result.status = TaskStatus.FAILED
            result.error = str(e)
            logger.error(f"Failed to update global crypto market: {e}")

        finally:
            result.end_time = datetime.now(timezone.utc)
            result.duration_ms = (result.end_time - start_time).total_seconds() * 1000

        return result

    def get_update_stats(self) -> Dict[str, Any]:
        """获取更新统计信息"""
        return {
            **self._update_stats,
            'ticker_cache': {
                exchange: {
                    'cached': exchange in self._spot_tickers_cache,
                    'cache_age': time.time() - self._spot_tickers_cache.get(exchange, (None, 0))[1]
                    if exchange in self._spot_tickers_cache else None
                }
                for exchange in ['binance', 'okx']
            },
            'symbol_universe': {
                'spot_symbols_count': sum(
                    len(s) for s in self.symbol_universe.spot_symbols.values()
                ),
                'futures_symbols_count': len(self.symbol_universe.futures_symbols),
                'last_updated': (
                    self.symbol_universe.last_updated.isoformat()
                    if self.symbol_universe.last_updated else None
                )
            }
        }


# 全局数据更新管理器实例
data_update_manager: Optional[DataUpdateManager] = None


async def init_data_update_manager(
    data_source_manager: DataSourceManager,
    storage_manager: StorageManager,
    config: Optional[Dict[str, Any]] = None
) -> DataUpdateManager:
    """初始化全局数据更新管理器"""
    global data_update_manager
    data_update_manager = DataUpdateManager(
        data_source_manager=data_source_manager,
        storage_manager=storage_manager,
        config=config
    )
    await data_update_manager.initialize()
    logger.info("DataUpdateManager initialized")
    return data_update_manager


def get_data_update_manager() -> Optional[DataUpdateManager]:
    """获取全局数据更新管理器"""
    return data_update_manager
