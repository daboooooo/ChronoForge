import logging
from typing import Any, Dict, Optional, Callable, Awaitable
from functools import wraps

import pandas as pd

from chronoforge.cache import LRUTTLCache, CacheManager

logger = logging.getLogger(__name__)

cache_manager = CacheManager()


def create_data_source_cache(
    name: str,
    maxsize: int = 100,
    ttl: int = 3600
) -> LRUTTLCache:
    """创建数据源专用的缓存实例

    Args:
        name: 缓存名称，通常为数据源名称
        maxsize: 缓存最大容量
        ttl: 缓存过期时间（秒）

    Returns:
        LRUTTLCache: 缓存实例
    """
    return cache_manager.create_cache(name, maxsize=maxsize, ttl=ttl)


def generate_cache_key(
    prefix: str,
    symbol: Optional[str] = None,
    timeframe: Optional[str] = None,
    start_ts_ms: Optional[int] = None,
    end_ts_ms: Optional[int] = None,
    **kwargs
) -> str:
    """生成标准化的缓存键

    Args:
        prefix: 缓存键前缀，通常为数据源名称
        symbol: 交易对/资产符号
        timeframe: 时间粒度
        start_ts_ms: 开始时间戳（毫秒）
        end_ts_ms: 结束时间戳（毫秒）
        **kwargs: 其他参数

    Returns:
        str: 格式化的缓存键，格式为 "prefix:symbol:timeframe:start:end"
    """
    parts = [prefix]

    if symbol:
        parts.append(str(symbol))
    else:
        parts.append("none")

    if timeframe:
        parts.append(str(timeframe))
    else:
        parts.append("none")

    if start_ts_ms is not None:
        parts.append(str(start_ts_ms))
    else:
        parts.append("none")

    if end_ts_ms is not None:
        parts.append(str(end_ts_ms))
    else:
        parts.append("none")

    for key, value in sorted(kwargs.items()):
        if value is not None:
            parts.append(f"{key}={value}")

    return ":".join(parts)


class DataSourceCacheMixin:
    """数据源缓存混入类，提供统一的缓存功能

    使用方式:
        class MyDataSource(DataSourceBase, DataSourceCacheMixin):
            def __init__(self, config):
                super().__init__(config)
                self._init_cache(maxsize=100, ttl=300)

            async def fetch(self, symbol, timeframe, start_ts_ms, end_ts_ms):
                cache_key = generate_cache_key(self.name, symbol, timeframe, start_ts_ms, end_ts_ms)
                cached = await self._cache_get(cache_key)
                if cached is not None:
                    return cached

                data = await self._fetch_data(...)
                await self._cache_set(cache_key, data)
                return data
    """

    def _init_cache(self, maxsize: int = 100, ttl: int = 3600) -> None:
        """初始化缓存

        Args:
            maxsize: 缓存最大容量
            ttl: 缓存过期时间（秒）
        """
        self._cache: LRUTTLCache = create_data_source_cache(
            f"datasource_{self.name}",
            maxsize=maxsize,
            ttl=ttl
        )

    async def _cache_get(self, key: str) -> Optional[pd.DataFrame]:
        """从缓存获取数据

        Args:
            key: 缓存键

        Returns:
            Optional[pd.DataFrame]: 缓存的数据，如果不存在或已过期返回None
        """
        try:
            return await self._cache.get(key)
        except Exception as e:
            logger.warning(f"Cache get error for key {key}: {e}")
            return None

    async def _cache_set(
        self,
        key: str,
        value: pd.DataFrame,
        ttl: Optional[int] = None
    ) -> None:
        """设置缓存数据

        Args:
            key: 缓存键
            value: 要缓存的数据
            ttl: 自定义过期时间（秒），None使用默认值
        """
        try:
            await self._cache.put(key, value, ttl)
        except Exception as e:
            logger.warning(f"Cache set error for key {key}: {e}")

    async def _cache_delete(self, key: str) -> None:
        """删除缓存数据

        Args:
            key: 缓存键
        """
        try:
            await self._cache.delete(key)
        except Exception as e:
            logger.warning(f"Cache delete error for key {key}: {e}")

    async def _cache_clear(self) -> None:
        """清空缓存"""
        try:
            await self._cache.clear()
        except Exception as e:
            logger.warning(f"Cache clear error: {e}")

    def _cache_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息

        Returns:
            Dict[str, Any]: 缓存统计信息
        """
        return self._cache.get_stats()


def cached_fetch(
    cache: LRUTTLCache,
    ttl: Optional[int] = None,
    key_generator: Optional[Callable[..., str]] = None
):
    """装饰器：为fetch方法添加缓存功能

    Args:
        cache: 缓存实例
        ttl: 缓存过期时间（秒）
        key_generator: 自定义缓存键生成函数

    使用示例:
        @cached_fetch(cache)
        async def fetch(self, symbol, timeframe, start_ts_ms, end_ts_ms):
            ...
    """
    def decorator(func: Callable[..., Awaitable[pd.DataFrame]]):
        @wraps(func)
        async def wrapper(self, symbol: str, timeframe: str,
                          start_ts_ms: int, end_ts_ms: Optional[int] = None) -> pd.DataFrame:
            if key_generator:
                cache_key = key_generator(self, symbol, timeframe, start_ts_ms, end_ts_ms)
            else:
                cache_key = generate_cache_key(
                    self.name,
                    symbol,
                    timeframe,
                    start_ts_ms,
                    end_ts_ms
                )

            cached = await cache.get(cache_key)
            if cached is not None:
                logger.debug(f"Cache hit for {cache_key}")
                return cached

            result = await func(self, symbol, timeframe, start_ts_ms, end_ts_ms)

            if result is not None and not result.empty:
                await cache.put(cache_key, result, ttl)
                logger.debug(f"Cached data for {cache_key}")

            return result

        return wrapper
    return decorator


class DataSourceCacheConfig:
    """数据源缓存配置"""

    DEFAULT_TTL: Dict[str, int] = {
        "ticker": 30,
        "ohlcv": 300,
        "time_series": 600,
        "global_market": 180,
        "coin_markets": 1800,
        "coin_categories": 1800,
    }

    DEFAULT_MAXSIZE: Dict[str, int] = {
        "ticker": 50,
        "ohlcv": 200,
        "time_series": 200,
        "global_market": 10,
        "coin_markets": 10,
        "coin_categories": 10,
    }

    @classmethod
    def get_ttl(cls, data_type: str, custom_ttl: Optional[int] = None) -> int:
        """获取指定数据类型的TTL

        Args:
            data_type: 数据类型（如 'ticker', 'ohlcv' 等）
            custom_ttl: 自定义TTL，如果提供则优先使用

        Returns:
            int: TTL值（秒）
        """
        return custom_ttl or cls.DEFAULT_TTL.get(data_type, 3600)

    @classmethod
    def get_maxsize(cls, data_type: str, custom_maxsize: Optional[int] = None) -> int:
        """获取指定数据类型的最大缓存容量

        Args:
            data_type: 数据类型
            custom_maxsize: 自定义最大容量，如果提供则优先使用

        Returns:
            int: 最大缓存容量
        """
        return custom_maxsize or cls.DEFAULT_MAXSIZE.get(data_type, 100)
