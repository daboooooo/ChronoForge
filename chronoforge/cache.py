import asyncio
import time
import logging
from typing import Dict, Optional, TypeVar, Generic, Any, List
from collections import OrderedDict

T = TypeVar('T')

logger = logging.getLogger(__name__)


class LRUTTLCache(Generic[T]):
    """LRU+TTL缓存实现"""

    def __init__(self, maxsize: int = 100, ttl: int = 3600):
        self.cache: OrderedDict[str, tuple[float, T, Optional[int]]] = \
            OrderedDict()
        self.maxsize = maxsize
        self.ttl = ttl
        self.lock = asyncio.Lock()
        self.hits = 0
        self.misses = 0
        self.evictions = 0

    async def get(self, key: str) -> Optional[T]:
        """获取缓存数据"""
        async with self.lock:
            if key not in self.cache:
                self.misses += 1
                return None

            timestamp, value, custom_ttl = self.cache[key]
            current_time = time.time()

            # 检查是否过期
            if current_time - timestamp > (custom_ttl or self.ttl):
                del self.cache[key]
                self.evictions += 1
                self.misses += 1
                return None

            # 更新访问顺序
            self.cache.move_to_end(key)
            self.hits += 1
            return value

    async def put(self, key: str, value: T, ttl: Optional[int] = None) -> None:
        """设置缓存数据"""
        async with self.lock:
            # 移除过期数据
            current_time = time.time()
            keys_to_remove = []
            for k, (ts, _, custom_ttl) in self.cache.items():
                if current_time - ts > (custom_ttl or self.ttl):
                    keys_to_remove.append(k)

            for k in keys_to_remove:
                del self.cache[k]
                self.evictions += 1

            # 移除最久未使用的数据
            if len(self.cache) >= self.maxsize:
                self.cache.popitem(last=False)
                self.evictions += 1

            # 设置新数据
            self.cache[key] = (current_time, value, ttl)

    async def delete(self, key: str) -> None:
        """删除缓存数据"""
        async with self.lock:
            if key in self.cache:
                del self.cache[key]

    async def clear(self) -> None:
        """清空缓存"""
        async with self.lock:
            self.cache.clear()
            self.hits = 0
            self.misses = 0
            self.evictions = 0

    def size(self) -> int:
        """获取缓存大小"""
        return len(self.cache)

    def get_stats(self) -> Dict[str, int]:
        """获取缓存统计信息"""
        return {
            'hits': self.hits,
            'misses': self.misses,
            'evictions': self.evictions,
            'size': self.size(),
            'maxsize': self.maxsize
        }

    def get_hit_rate(self) -> float:
        """获取缓存命中率"""
        total = self.hits + self.misses
        if total == 0:
            return 0.0
        return self.hits / total


class CacheManager:
    """缓存管理器

    提供多级缓存策略，支持内存缓存和持久化缓存
    管理缓存配置，保证缓存一致性
    提供缓存性能监控
    """

    def __init__(self):
        self.caches: Dict[str, LRUTTLCache] = {}
        self.default_cache = None
        self.lock = asyncio.Lock()

    def create_cache(self, name: str, maxsize: int = 100, ttl: int = 3600) -> LRUTTLCache:
        """创建缓存实例

        Args:
            name: 缓存名称
            maxsize: 缓存最大容量
            ttl: 缓存过期时间（秒）

        Returns:
            LRUTTLCache: 缓存实例
        """
        cache = LRUTTLCache(maxsize=maxsize, ttl=ttl)
        self.caches[name] = cache

        # 如果是第一个创建的缓存，设为默认缓存
        if self.default_cache is None:
            self.default_cache = name
            logger.info(f"Set default cache: {name}")

        logger.info(f"Created cache: {name} (maxsize={maxsize}, ttl={ttl})")
        return cache

    def get_cache(self, name: Optional[str] = None) -> Optional[LRUTTLCache]:
        """获取缓存实例

        Args:
            name: 缓存名称，None 表示使用默认缓存

        Returns:
            Optional[LRUTTLCache]: 缓存实例
        """
        if name:
            return self.caches.get(name)
        elif self.default_cache:
            return self.caches.get(self.default_cache)
        else:
            logger.error("No cache available")
            return None

    async def get(self, key: str, cache_name: Optional[str] = None) -> Optional[Any]:
        """从缓存获取数据

        Args:
            key: 缓存键
            cache_name: 缓存名称

        Returns:
            Optional[Any]: 缓存数据
        """
        cache = self.get_cache(cache_name)
        if not cache:
            return None

        return await cache.get(key)

    async def put(self, key: str, value: Any,
                  ttl: Optional[int] = None, cache_name: Optional[str] = None) -> None:
        """向缓存存入数据

        Args:
            key: 缓存键
            value: 缓存值
            ttl: 过期时间（秒）
            cache_name: 缓存名称
        """
        cache = self.get_cache(cache_name)
        if not cache:
            return

        await cache.put(key, value, ttl)

    async def delete(self, key: str, cache_name: Optional[str] = None) -> None:
        """从缓存删除数据

        Args:
            key: 缓存键
            cache_name: 缓存名称
        """
        cache = self.get_cache(cache_name)
        if not cache:
            return

        await cache.delete(key)

    async def clear(self, cache_name: Optional[str] = None) -> None:
        """清空缓存

        Args:
            cache_name: 缓存名称，None 表示清空所有缓存
        """
        if cache_name:
            cache = self.get_cache(cache_name)
            if cache:
                await cache.clear()
                logger.info(f"Cleared cache: {cache_name}")
        else:
            for name, cache in self.caches.items():
                await cache.clear()
                logger.info(f"Cleared cache: {name}")

    def get_stats(self, cache_name: Optional[str] = None) -> Dict[str, Any]:
        """获取缓存统计信息

        Args:
            cache_name: 缓存名称，None 表示获取所有缓存的统计信息

        Returns:
            Dict[str, Any]: 缓存统计信息
        """
        if cache_name:
            cache = self.get_cache(cache_name)
            if cache:
                return {
                    cache_name: {
                        **cache.get_stats(),
                        'hit_rate': cache.get_hit_rate()
                    }
                }
            return {}
        else:
            stats = {}
            total_hits = 0
            total_misses = 0
            total_evictions = 0
            total_size = 0

            for name, cache in self.caches.items():
                cache_stats = cache.get_stats()
                stats[name] = {
                    **cache_stats,
                    'hit_rate': cache.get_hit_rate()
                }
                total_hits += cache_stats['hits']
                total_misses += cache_stats['misses']
                total_evictions += cache_stats['evictions']
                total_size += cache_stats['size']

            # 计算总体统计
            total = total_hits + total_misses
            overall_hit_rate = total_hits / total if total > 0 else 0.0

            stats['overall'] = {
                'hits': total_hits,
                'misses': total_misses,
                'evictions': total_evictions,
                'size': total_size,
                'hit_rate': overall_hit_rate,
                'cache_count': len(self.caches)
            }

            return stats

    def list_caches(self) -> List[str]:
        """列出所有缓存名称

        Returns:
            List[str]: 缓存名称列表
        """
        return list(self.caches.keys())


# 全局缓存管理器实例
cache_manager = CacheManager()
