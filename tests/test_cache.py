import pytest
import asyncio
import time
from chronoforge.cache import LRUTTLCache, CacheManager, cache_manager


class TestLRUTTLCache:
    """LRU+TTL缓存测试"""
    
    async def test_get_put(self):
        """测试基本的get和put操作"""
        # 创建缓存实例
        cache = LRUTTLCache(maxsize=10, ttl=60)
        
        # 测试put操作
        await cache.put("key1", "value1")
        
        # 测试get操作
        value = await cache.get("key1")
        assert value == "value1"
        
        # 测试获取不存在的键
        value = await cache.get("non_existent")
        assert value is None
    
    async def test_ttl_expiration(self):
        """测试缓存过期机制"""
        # 创建缓存实例，TTL设置为1秒
        cache = LRUTTLCache(maxsize=10, ttl=1)
        
        # 存入数据
        await cache.put("key1", "value1")
        
        # 立即获取，应该能获取到
        value = await cache.get("key1")
        assert value == "value1"
        
        # 等待2秒，让缓存过期
        await asyncio.sleep(2)
        
        # 再次获取，应该返回None
        value = await cache.get("key1")
        assert value is None
    
    async def test_lru_eviction(self):
        """测试LRU淘汰机制"""
        # 创建缓存实例，最大容量为3
        cache = LRUTTLCache(maxsize=3, ttl=60)
        
        # 存入3个键值对
        await cache.put("key1", "value1")
        await cache.put("key2", "value2")
        await cache.put("key3", "value3")
        
        # 验证所有键都存在
        assert await cache.get("key1") == "value1"
        assert await cache.get("key2") == "value2"
        assert await cache.get("key3") == "value3"
        
        # 访问key1，将其移到最近使用
        await cache.get("key1")
        
        # 存入第4个键值对，应该淘汰key2
        await cache.put("key4", "value4")
        
        # 验证key1、key3、key4存在，key2被淘汰
        assert await cache.get("key1") == "value1"
        assert await cache.get("key2") is None
        assert await cache.get("key3") == "value3"
        assert await cache.get("key4") == "value4"
    
    async def test_delete(self):
        """测试删除操作"""
        # 创建缓存实例
        cache = LRUTTLCache(maxsize=10, ttl=60)
        
        # 存入数据
        await cache.put("key1", "value1")
        
        # 验证数据存在
        assert await cache.get("key1") == "value1"
        
        # 删除数据
        await cache.delete("key1")
        
        # 验证数据不存在
        assert await cache.get("key1") is None
    
    async def test_clear(self):
        """测试清空操作"""
        # 创建缓存实例
        cache = LRUTTLCache(maxsize=10, ttl=60)
        
        # 存入数据
        await cache.put("key1", "value1")
        await cache.put("key2", "value2")
        
        # 验证数据存在
        assert await cache.get("key1") == "value1"
        assert await cache.get("key2") == "value2"
        
        # 清空缓存
        await cache.clear()
        
        # 验证缓存为空
        assert await cache.get("key1") is None
        assert await cache.get("key2") is None
        assert cache.size() == 0
    
    async def test_size(self):
        """测试获取缓存大小"""
        # 创建缓存实例
        cache = LRUTTLCache(maxsize=10, ttl=60)
        
        # 验证初始大小为0
        assert cache.size() == 0
        
        # 存入数据
        await cache.put("key1", "value1")
        await cache.put("key2", "value2")
        
        # 验证大小为2
        assert cache.size() == 2
        
        # 删除一个键
        await cache.delete("key1")
        
        # 验证大小为1
        assert cache.size() == 1
    
    async def test_stats(self):
        """测试获取缓存统计信息"""
        # 创建缓存实例
        cache = LRUTTLCache(maxsize=10, ttl=60)
        
        # 初始统计信息
        stats = cache.get_stats()
        assert stats['hits'] == 0
        assert stats['misses'] == 0
        assert stats['evictions'] == 0
        assert stats['size'] == 0
        assert stats['maxsize'] == 10
        
        # 测试命中和未命中
        await cache.put("key1", "value1")
        await cache.get("key1")  # 命中
        await cache.get("non_existent")  # 未命中
        
        stats = cache.get_stats()
        assert stats['hits'] == 1
        assert stats['misses'] == 1
    
    async def test_hit_rate(self):
        """测试获取缓存命中率"""
        # 创建缓存实例
        cache = LRUTTLCache(maxsize=10, ttl=60)
        
        # 初始命中率为0
        assert cache.get_hit_rate() == 0.0
        
        # 测试命中率
        await cache.put("key1", "value1")
        await cache.get("key1")  # 命中
        await cache.get("non_existent")  # 未命中
        
        # 命中率应该是0.5
        assert cache.get_hit_rate() == 0.5
    
    async def test_custom_ttl(self):
        """测试自定义TTL"""
        # 创建缓存实例，默认TTL为60秒
        cache = LRUTTLCache(maxsize=10, ttl=60)
        
        # 存入数据，设置自定义TTL为1秒
        await cache.put("key1", "value1", ttl=1)
        
        # 立即获取，应该能获取到
        value = await cache.get("key1")
        assert value == "value1"
        
        # 等待2秒，让缓存过期
        await asyncio.sleep(2)
        
        # 再次获取，应该返回None
        value = await cache.get("key1")
        assert value is None


class TestCacheManager:
    """缓存管理器测试"""
    
    def setup_method(self):
        """测试前初始化"""
        # 清空全局缓存管理器
        asyncio.run(cache_manager.clear())
        # 创建新的缓存管理器实例
        self.cache_manager = CacheManager()
    
    async def test_create_cache(self):
        """测试创建缓存"""
        # 创建缓存
        cache = self.cache_manager.create_cache("test_cache", maxsize=50, ttl=300)
        
        # 验证缓存被创建
        assert "test_cache" in self.cache_manager.list_caches()
        assert self.cache_manager.get_cache("test_cache") == cache
    
    async def test_get_cache(self):
        """测试获取缓存"""
        # 创建缓存
        self.cache_manager.create_cache("test_cache")
        
        # 测试获取指定缓存
        cache = self.cache_manager.get_cache("test_cache")
        assert cache is not None
        
        # 测试获取默认缓存
        default_cache = self.cache_manager.get_cache()
        assert default_cache is not None
        
        # 测试获取不存在的缓存
        non_existent = self.cache_manager.get_cache("non_existent")
        assert non_existent is None
    
    async def test_get_put(self):
        """测试基本的get和put操作"""
        # 创建缓存
        self.cache_manager.create_cache("test_cache")
        
        # 测试put操作
        await self.cache_manager.put("key1", "value1", cache_name="test_cache")
        
        # 测试get操作
        value = await self.cache_manager.get("key1", cache_name="test_cache")
        assert value == "value1"
        
        # 测试获取不存在的键
        value = await self.cache_manager.get("non_existent", cache_name="test_cache")
        assert value is None
    
    async def test_delete(self):
        """测试删除操作"""
        # 创建缓存
        self.cache_manager.create_cache("test_cache")
        
        # 存入数据
        await self.cache_manager.put("key1", "value1", cache_name="test_cache")
        
        # 验证数据存在
        assert await self.cache_manager.get("key1", cache_name="test_cache") == "value1"
        
        # 删除数据
        await self.cache_manager.delete("key1", cache_name="test_cache")
        
        # 验证数据不存在
        assert await self.cache_manager.get("key1", cache_name="test_cache") is None
    
    async def test_clear(self):
        """测试清空操作"""
        # 创建两个缓存
        self.cache_manager.create_cache("test_cache1")
        self.cache_manager.create_cache("test_cache2")
        
        # 向两个缓存中存入数据
        await self.cache_manager.put("key1", "value1", cache_name="test_cache1")
        await self.cache_manager.put("key2", "value2", cache_name="test_cache2")
        
        # 验证数据存在
        assert await self.cache_manager.get("key1", cache_name="test_cache1") == "value1"
        assert await self.cache_manager.get("key2", cache_name="test_cache2") == "value2"
        
        # 清空单个缓存
        await self.cache_manager.clear(cache_name="test_cache1")
        
        # 验证test_cache1为空，test_cache2仍然有数据
        assert await self.cache_manager.get("key1", cache_name="test_cache1") is None
        assert await self.cache_manager.get("key2", cache_name="test_cache2") == "value2"
        
        # 清空所有缓存
        await self.cache_manager.clear()
        
        # 验证所有缓存为空
        assert await self.cache_manager.get("key1", cache_name="test_cache1") is None
        assert await self.cache_manager.get("key2", cache_name="test_cache2") is None
    
    async def test_get_stats(self):
        """测试获取缓存统计信息"""
        # 创建缓存
        self.cache_manager.create_cache("test_cache")
        
        # 存入数据并访问
        await self.cache_manager.put("key1", "value1", cache_name="test_cache")
        await self.cache_manager.get("key1", cache_name="test_cache")
        await self.cache_manager.get("non_existent", cache_name="test_cache")
        
        # 获取单个缓存的统计信息
        stats = self.cache_manager.get_stats(cache_name="test_cache")
        assert "test_cache" in stats
        assert stats["test_cache"]["hits"] >= 1
        assert stats["test_cache"]["misses"] >= 1
        
        # 获取所有缓存的统计信息
        all_stats = self.cache_manager.get_stats()
        assert "test_cache" in all_stats
        assert "overall" in all_stats
    
    async def test_list_caches(self):
        """测试列出所有缓存"""
        # 验证初始为空
        assert len(self.cache_manager.list_caches()) == 0
        
        # 创建缓存
        self.cache_manager.create_cache("cache1")
        self.cache_manager.create_cache("cache2")
        
        # 验证缓存列表
        caches = self.cache_manager.list_caches()
        assert len(caches) == 2
        assert "cache1" in caches
        assert "cache2" in caches
    
    async def test_default_cache(self):
        """测试默认缓存功能"""
        # 创建第一个缓存，应该成为默认缓存
        self.cache_manager.create_cache("default_cache")
        
        # 向默认缓存存入数据
        await self.cache_manager.put("key1", "value1")
        
        # 从默认缓存获取数据
        value = await self.cache_manager.get("key1")
        assert value == "value1"
    
    async def test_global_cache_manager(self):
        """测试全局缓存管理器"""
        # 测试全局缓存管理器
        global_cache = cache_manager.create_cache("global_test")
        assert "global_test" in cache_manager.list_caches()
        
        # 存入数据
        await cache_manager.put("global_key", "global_value", cache_name="global_test")
        
        # 获取数据
        value = await cache_manager.get("global_key", cache_name="global_test")
        assert value == "global_value"


if __name__ == '__main__':
    pytest.main(['-v', __file__])