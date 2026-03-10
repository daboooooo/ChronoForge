"""
终极异步任务执行框架 - 彻底解决所有事件循环问题
"""
import asyncio
import threading
import time
import concurrent.futures as cf
from typing import Any, Callable, Optional, Dict
from chronoforge.logging_config import get_logger

logger = get_logger(__name__)


class UltimateAsyncExecutor:
    """终极异步任务执行器 - 管理所有异步任务执行"""

    def __init__(self, max_workers: int = 4):
        self.max_workers = max_workers
        self.thread_pool = cf.ThreadPoolExecutor(max_workers=max_workers)
        self._thread_event_loops: Dict[int, asyncio.AbstractEventLoop] = {}
        self._lock = threading.Lock()
        self._shutdown = False
        self._main_loop = None

        # 确保主线程有事件循环
        self._main_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._main_loop)

    def _get_or_create_thread_loop(self, thread_id: int) -> asyncio.AbstractEventLoop:
        """获取或创建指定线程的事件循环"""
        with self._lock:
            if thread_id not in self._thread_event_loops or self._thread_event_loops[thread_id].is_closed():
                # 创建新的事件循环
                loop = asyncio.new_event_loop()
                self._thread_event_loops[thread_id] = loop
                logger.debug(f"为线程 {thread_id} 创建新的事件循环")
            else:
                loop = self._thread_event_loops[thread_id]
                logger.debug(f"复用线程 {thread_id} 的事件循环")

            # 确保设置为当前线程的事件循环
            asyncio.set_event_loop(loop)
            return loop

    def _run_coroutine_in_thread(self, coro_func: Callable, *args, **kwargs) -> Any:
        """在线程中运行协程"""
        thread_id = threading.get_ident()

        # 获取或创建当前线程的事件循环
        loop = self._get_or_create_thread_loop(thread_id)

        try:
            # 创建任务并运行
            if asyncio.iscoroutinefunction(coro_func):
                # 如果是协程函数，直接调用
                coro = coro_func(*args, **kwargs)
            else:
                # 如果是普通函数，包装成协程
                async def wrapper():
                    return coro_func(*args, **kwargs)
                coro = wrapper()

            # 在事件循环中运行协程
            return loop.run_until_complete(coro)
        except Exception as e:
            logger.error(f"线程 {thread_id} 执行协程出错: {e}")
            raise

    def submit_async_task(self, coro_func: Callable, *args, **kwargs) -> cf.Future:
        """提交异步任务到线程池"""
        if self._shutdown:
            raise RuntimeError("Executor has been shutdown")

        # 提交任务到线程池
        return self.thread_pool.submit(self._run_coroutine_in_thread, coro_func, *args, **kwargs)

    def run_async_task(self, coro_func: Callable, *args, **kwargs) -> Any:
        """直接运行异步任务（阻塞）"""
        if self._shutdown:
            raise RuntimeError("Executor has been shutdown")

        # 提交任务并等待结果
        future = self.submit_async_task(coro_func, *args, **kwargs)
        return future.result()

    def create_task_in_thread(self, coro_func: Callable, *args, **kwargs) -> cf.Future:
        """在线程中创建任务（非阻塞）"""
        return self.submit_async_task(coro_func, *args, **kwargs)

    def shutdown(self, wait: bool = True) -> None:
        """关闭执行器"""
        self._shutdown = True

        # 关闭线程池
        self.thread_pool.shutdown(wait=wait)

        # 关闭所有事件循环
        with self._lock:
            for thread_id, loop in list(self._thread_event_loops.items()):
                try:
                    if not loop.is_closed():
                        # 取消所有待处理的任务
                        pending_tasks = [task for task in asyncio.all_tasks(loop) if not task.done()]
                        if pending_tasks:
                            for task in pending_tasks:
                                task.cancel()
                            # 等待所有任务完成
                            loop.run_until_complete(asyncio.gather(*pending_tasks, return_exceptions=True))

                        loop.close()
                    del self._thread_event_loops[thread_id]
                except Exception as e:
                    logger.error(f"关闭线程 {thread_id} 的事件循环时出错: {e}")


# 全局终极异步执行器
ultimate_executor = UltimateAsyncExecutor()


def create_ultimate_executor(max_workers: int = 4) -> UltimateAsyncExecutor:
    """创建终极异步执行器"""
    return UltimateAsyncExecutor(max_workers=max_workers)


# 向后兼容的函数
def run_ultimate_async_task(coro_func: Callable, *args, **kwargs) -> Any:
    """运行终极异步任务（使用全局执行器）"""
    return ultimate_executor.run_async_task(coro_func, *args, **kwargs)


def submit_ultimate_async_task(coro_func: Callable, *args, **kwargs) -> cf.Future:
    """提交终极异步任务（使用全局执行器）"""
    return ultimate_executor.submit_async_task(coro_func, *args, **kwargs)