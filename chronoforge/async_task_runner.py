"""
异步任务运行器 - 解决事件循环管理问题
"""
import asyncio
import threading
from typing import Any, Callable, Optional
from chronoforge.logging_config import get_logger

logger = get_logger(__name__)


class AsyncTaskRunner:
    """异步任务运行器，确保事件循环正确管理"""
    
    def __init__(self):
        self._lock = threading.Lock()
        self._loops = {}  # 存储每个线程的事件循环
    
    def get_or_create_event_loop(self) -> asyncio.AbstractEventLoop:
        """获取或创建当前线程的事件循环"""
        thread_id = threading.get_ident()
        
        with self._lock:
            if thread_id not in self._loops or self._loops[thread_id].is_closed():
                # 创建新的事件循环
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                self._loops[thread_id] = loop
                logger.debug(f"为线程 {thread_id} 创建新的事件循环")
            else:
                loop = self._loops[thread_id]
                # 确保设置为当前线程的事件循环
                asyncio.set_event_loop(loop)
                logger.debug(f"复用线程 {thread_id} 的事件循环")
            
            return loop
    
    def run_async_task(self, coro_func: Callable, *args, **kwargs) -> Any:
        """运行异步任务"""
        loop = self.get_or_create_event_loop()
        
        try:
            # 创建协程并运行
            return loop.run_until_complete(coro_func(*args, **kwargs))
        except Exception as e:
            logger.error(f"异步任务执行出错: {e}")
            raise
    
    def cleanup(self) -> None:
        """清理所有事件循环"""
        with self._lock:
            for thread_id, loop in list(self._loops.items()):
                try:
                    if not loop.is_closed():
                        loop.close()
                    del self._loops[thread_id]
                except Exception as e:
                    logger.error(f"清理线程 {thread_id} 的事件循环时出错: {e}")


# 全局异步任务运行器实例
async_task_runner = AsyncTaskRunner()