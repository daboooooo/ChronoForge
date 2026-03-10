"""
完整的异步任务执行框架 - 彻底解决事件循环管理问题
"""
import asyncio
import threading
import time
import concurrent.futures as cf
from typing import Any, Callable, Optional
from chronoforge.logging_config import get_logger

logger = get_logger(__name__)


class AsyncTaskExecutor:
    """异步任务执行器 - 管理线程池中的事件循环"""
    
    def __init__(self, max_workers: int = 4):
        self.max_workers = max_workers
        self.thread_pool = cf.ThreadPoolExecutor(max_workers=max_workers)
        self._thread_loops = {}  # 线程ID到事件循环的映射
        self._lock = threading.Lock()
        self._shutdown = False
    
    def _get_or_create_thread_loop(self, thread_id: int) -> asyncio.AbstractEventLoop:
        """获取或创建指定线程的事件循环"""
        with self._lock:
            if thread_id not in self._thread_loops or self._thread_loops[thread_id].is_closed():
                # 创建新的事件循环
                loop = asyncio.new_event_loop()
                self._thread_loops[thread_id] = loop
                logger.debug(f"为线程 {thread_id} 创建新的事件循环")
            else:
                loop = self._thread_loops[thread_id]
                logger.debug(f"复用线程 {thread_id} 的事件循环")
            
            return loop
    
    def _run_async_in_thread(self, coro_func: Callable, *args, **kwargs) -> Any:
        """在线程中运行异步函数"""
        thread_id = threading.get_ident()
        
        # 获取或创建当前线程的事件循环
        loop = self._get_or_create_thread_loop(thread_id)
        
        # 确保设置为当前线程的事件循环
        asyncio.set_event_loop(loop)
        
        try:
            # 在事件循环中运行协程
            return loop.run_until_complete(coro_func(*args, **kwargs))
        except Exception as e:
            logger.error(f"线程 {thread_id} 执行异步任务出错: {e}")
            raise
    
    def submit_async_task(self, coro_func: Callable, *args, **kwargs) -> cf.Future:
        """提交异步任务到线程池"""
        if self._shutdown:
            raise RuntimeError("Executor has been shutdown")
        
        # 提交任务到线程池
        return self.thread_pool.submit(self._run_async_in_thread, coro_func, *args, **kwargs)
    
    def run_async_task(self, coro_func: Callable, *args, **kwargs) -> Any:
        """直接运行异步任务（阻塞）"""
        if self._shutdown:
            raise RuntimeError("Executor has been shutdown")
        
        # 提交任务并等待结果
        future = self.submit_async_task(coro_func, *args, **kwargs)
        return future.result()
    
    def shutdown(self, wait: bool = True) -> None:
        """关闭执行器"""
        self._shutdown = True
        
        # 关闭线程池
        self.thread_pool.shutdown(wait=wait)
        
        # 关闭所有事件循环
        with self._lock:
            for thread_id, loop in list(self._thread_loops.items()):
                try:
                    if not loop.is_closed():
                        loop.close()
                    del self._thread_loops[thread_id]
                except Exception as e:
                    logger.error(f"关闭线程 {thread_id} 的事件循环时出错: {e}")


# 全局异步任务执行器
async_executor = AsyncTaskExecutor()


def create_async_task_executor(max_workers: int = 4) -> AsyncTaskExecutor:
    """创建异步任务执行器"""
    return AsyncTaskExecutor(max_workers=max_workers)


# 向后兼容的函数
def run_async_task(coro_func: Callable, *args, **kwargs) -> Any:
    """运行异步任务（使用全局执行器）"""
    return async_executor.run_async_task(coro_func, *args, **kwargs)


def submit_async_task(coro_func: Callable, *args, **kwargs) -> cf.Future:
    """提交异步任务（使用全局执行器）"""
    return async_executor.submit_async_task(coro_func, *args, **kwargs)