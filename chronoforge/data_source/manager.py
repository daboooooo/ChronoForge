"""数据源管理器 - 统一管理数据源实例，协调数据获取"""

import asyncio
import logging
from typing import Dict, List, Optional, Type, Any
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime

import pandas as pd

from .base import (
    DataSourceBase,
    verify_datasource_instance,
    DataSchema,
    StandardSchemas,
)
from . import CryptoSpotDataSource, FREDDataSource, GlobalMarketDataSource
from . import CryptoUMFutureDataSource, AlthernativeDataSource, CoinGeckoDataSource

logger = logging.getLogger(__name__)


@dataclass
class RetryConfig:
    """重试配置"""
    max_retries: int = 3
    retry_delay: float = 1.0
    backoff_factor: float = 1.5


@dataclass
class DataSourceCapability:
    """数据源能力描述"""
    supports_spot: bool = False
    supports_futures: bool = False
    supports_options: bool = False
    supports_tickers: bool = False
    supports_ohlcv: bool = False
    supported_timeframes: List[str] = field(default_factory=list)
    supported_exchanges: List[str] = field(default_factory=list)
    supported_schema: DataSchema = StandardSchemas.OHLCV

    def __post_init__(self) -> None:
        if not self.supported_timeframes:
            self.supported_timeframes = ['1m', '5m', '15m', '30m', '1h', '4h', '1d', '1w']
        if not self.supported_exchanges:
            self.supported_exchanges = []


@dataclass
class FetchTask:
    """数据获取任务定义"""
    data_source_name: str
    symbol: str
    timeframe: str
    start_ts_ms: int
    end_ts_ms: Optional[int] = None


@dataclass
class FetchResult:
    """数据获取结果"""
    success: bool
    data: Optional[pd.DataFrame]
    error: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)


class DataSourceManager:
    """数据源管理器 - 统一管理数据源实例，协调数据获取"""

    def __init__(self, max_workers: int = 5):
        self.data_source_instances: Dict[str, DataSourceBase] = {}
        self.data_source_classes: Dict[str, Type[DataSourceBase]] = {
            'CryptoSpotDataSource': CryptoSpotDataSource,
            'FREDDataSource': FREDDataSource,
            'GlobalMarketDataSource': GlobalMarketDataSource,
            'CryptoUMFutureDataSource': CryptoUMFutureDataSource,
            'AlthernativeDataSource': AlthernativeDataSource,
            'CoinGeckoDataSource': CoinGeckoDataSource
        }
        self.thread_pool = ThreadPoolExecutor(max_workers=max_workers)
        self.data_source_capabilities: Dict[str, DataSourceCapability] = {}
        self.exchange_semaphores: Dict[str, asyncio.Semaphore] = {}
        self.retry_config = RetryConfig()

    def register_data_source(self, name: str, data_source: DataSourceBase) -> bool:
        """注册数据源实例"""
        try:
            valid, msg = verify_datasource_instance(data_source)
            if not valid:
                logger.error(f"数据源验证失败: {msg}")
                return False
            self.data_source_instances[name] = data_source
            logger.info(f"数据源 {name} 注册成功")
            self._detect_data_source_capabilities(name, data_source)
            return True
        except Exception as e:
            logger.error(f"注册数据源 {name} 失败: {str(e)}")
            return False

    def get_data_source(self, name: str) -> Optional[DataSourceBase]:
        """获取数据源实例"""
        return self.data_source_instances.get(name)

    def create_data_source(
        self,
        name: str,
        config: Optional[Dict[str, Any]] = None
    ) -> Optional[DataSourceBase]:
        """创建数据源实例"""
        try:
            if name not in self.data_source_classes:
                logger.error(f"数据源类 {name} 不存在")
                return None
            data_source_class = self.data_source_classes[name]
            data_source = data_source_class(config or {})
            self.register_data_source(name, data_source)
            return data_source
        except Exception as e:
            logger.error(f"创建数据源 {name} 失败: {str(e)}")
            return None

    def _detect_data_source_capabilities(
        self,
        name: str,
        data_source: DataSourceBase
    ) -> DataSourceCapability:
        """检测数据源能力"""
        capabilities = DataSourceCapability()

        if hasattr(data_source, 'timeframes'):
            capabilities.supported_timeframes = data_source.timeframes

        if hasattr(data_source, 'tickers'):
            capabilities.supports_tickers = True

        if hasattr(data_source, 'supported_schema'):
            capabilities.supported_schema = data_source.supported_schema

        self.data_source_capabilities[name] = capabilities
        logger.debug(f"数据源 {name} 能力检测完成: {capabilities}")
        return capabilities

    def get_data_source_capabilities(
        self,
        name: str
    ) -> Optional[DataSourceCapability]:
        """获取数据源能力"""
        if name in self.data_source_capabilities:
            return self.data_source_capabilities[name]
        data_source = self.get_data_source(name)
        if not data_source:
            return None
        return self._detect_data_source_capabilities(name, data_source)

    async def fetch_data(
        self,
        data_source_name: str,
        symbol: str,
        timeframe: str,
        start_ts_ms: int,
        end_ts_ms: Optional[int] = None
    ) -> Optional[pd.DataFrame]:
        """从数据源获取数据"""
        try:
            data_source = self.get_data_source(data_source_name)
            if not data_source:
                data_source = self.create_data_source(data_source_name)
                if not data_source:
                    logger.error(f"数据源 {data_source_name} 不存在且无法创建")
                    return None

            capabilities = self.get_data_source_capabilities(data_source_name)
            if capabilities:
                if (capabilities.supported_timeframes
                        and timeframe not in capabilities.supported_timeframes):
                    logger.warning(
                        f"数据源 {data_source_name} 不支持时间框架 {timeframe}"
                    )

            max_retries = self.retry_config.max_retries
            retry_delay = self.retry_config.retry_delay
            backoff_factor = self.retry_config.backoff_factor

            for attempt in range(max_retries + 1):
                try:
                    result = await data_source.fetch(
                        symbol, timeframe, start_ts_ms, end_ts_ms
                    )
                    logger.info(
                        f"从数据源 {data_source_name} 获取数据成功: "
                        f"{symbol} {timeframe}"
                    )
                    return result
                except Exception as e:
                    if attempt < max_retries:
                        delay = retry_delay * (backoff_factor ** attempt)
                        logger.warning(
                            f"从数据源 {data_source_name} 获取数据失败 "
                            f"(尝试 {attempt+1}/{max_retries+1}): {str(e)}, "
                            f"重试中..."
                        )
                        await asyncio.sleep(delay)
                    else:
                        logger.error(
                            f"从数据源 {data_source_name} 获取数据失败，"
                            f"已达到最大重试次数: {str(e)}"
                        )
                        raise
        except Exception as e:
            logger.error(f"获取数据失败: {str(e)}")
            return None

    async def fetch_data_concurrent(
        self,
        tasks: List[FetchTask]
    ) -> Dict[int, Optional[pd.DataFrame]]:
        """并发从多个数据源获取数据"""
        results: Dict[int, Optional[pd.DataFrame]] = {}

        async def execute_task(task_index: int, task: FetchTask) -> None:
            result = await self.fetch_data(
                task.data_source_name,
                task.symbol,
                task.timeframe,
                task.start_ts_ms,
                task.end_ts_ms
            )
            results[task_index] = result

        async_tasks = [
            execute_task(i, task)
            for i, task in enumerate(tasks)
        ]
        await asyncio.gather(*async_tasks)
        return results

    def get_supported_data_sources(self) -> List[str]:
        """获取所有支持的数据源"""
        return list(self.data_source_classes.keys())

    def get_active_data_sources(self) -> List[str]:
        """获取所有活跃的数据源实例"""
        return list(self.data_source_instances.keys())

    def close(self) -> None:
        """关闭数据源管理器，清理资源"""
        if self.thread_pool:
            self.thread_pool.shutdown(wait=True)
            logger.info("数据源管理器线程池已关闭")

        for name, data_source in self.data_source_instances.items():
            if hasattr(data_source, 'close'):
                try:
                    if asyncio.iscoroutinefunction(data_source.close):
                        asyncio.run(data_source.close())
                    else:
                        data_source.close()
                    logger.info(f"数据源 {name} 已关闭")
                except Exception as e:
                    logger.error(f"关闭数据源 {name} 失败: {str(e)}")

        self.data_source_instances.clear()
        self.data_source_capabilities.clear()
        self.exchange_semaphores.clear()

    async def __aenter__(self) -> 'DataSourceManager':
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


data_source_manager = DataSourceManager()


def init_data_source_manager(max_workers: int = 5) -> DataSourceManager:
    """初始化数据源管理器"""
    global data_source_manager
    data_source_manager = DataSourceManager(max_workers=max_workers)
    logger.info("数据源管理器初始化成功")
    return data_source_manager
