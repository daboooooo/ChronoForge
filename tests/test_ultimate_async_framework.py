import pytest
import asyncio
from unittest.mock import Mock, patch
from chronoforge.ultimate_async_framework import UltimateAsyncExecutor


class TestUltimateAsyncExecutor:
    """测试UltimateAsyncExecutor类"""

    def test_initialization(self):
        """测试执行器初始化"""
        executor = UltimateAsyncExecutor(max_workers=2)
        assert executor.max_workers == 2
        assert executor._thread_event_loops == {}
        assert not executor._shutdown

    def test_get_or_create_thread_loop(self):
        """测试获取或创建线程事件循环"""
        executor = UltimateAsyncExecutor()
        loop = executor._get_or_create_thread_loop(12345)
        assert loop is not None
        assert 12345 in executor._thread_event_loops

    async def test_run_async_task(self):
        """测试运行异步任务"""
        executor = UltimateAsyncExecutor()
        
        async def test_coroutine(x, y):
            await asyncio.sleep(0.1)
            return x + y
        
        result = executor.run_async_task(test_coroutine, 1, 2)
        assert result == 3

    async def test_run_async_task_with_kwargs(self):
        """测试运行带关键字参数的异步任务"""
        executor = UltimateAsyncExecutor()
        
        async def test_coroutine(x, y, z=0):
            await asyncio.sleep(0.1)
            return x + y + z
        
        result = executor.run_async_task(test_coroutine, 1, 2, z=3)
        assert result == 6

    async def test_run_async_task_exception(self):
        """测试运行异步任务时的异常处理"""
        executor = UltimateAsyncExecutor()
        
        async def test_coroutine():
            await asyncio.sleep(0.1)
            raise ValueError("Test exception")
        
        with pytest.raises(ValueError, match="Test exception"):
            executor.run_async_task(test_coroutine)

    def test_shutdown(self):
        """测试关闭执行器"""
        executor = UltimateAsyncExecutor()
        executor.shutdown()
        assert executor._shutdown


if __name__ == "__main__":
    pytest.main([__file__])