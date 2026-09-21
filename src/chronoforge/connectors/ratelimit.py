"""令牌桶限流器（D04 §2）。

实现标准令牌桶算法，支持：
- 可注入时钟（测试用）
- 线程安全（threading.Lock 保护状态）
- 按权重获取令牌（Binance endpoint 权重适配）
- 429 全局冷却（on_rate_limited）
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable


class RateLimiter:
    """令牌桶限流器（D04 §2）。

    Args:
        rate: 每秒产生令牌数（req/s）
        burst: 桶容量（最大累积令牌数）
        clock: 可注入时钟（测试用），默认 time.monotonic
    """

    def __init__(
        self,
        rate: float,
        burst: int,
        clock: Callable[[], float] | None = None,
    ) -> None:
        if rate < 0:
            raise ValueError("rate must be >= 0")
        if burst < 1:
            raise ValueError("burst must be >= 1")

        self.rate = rate
        self.burst = burst
        self._clock = clock or _monotonic
        self._tokens: float = float(burst)  # 初始满桶
        self._last_refill: float = self._clock()
        self._lock = threading.Lock()
        # 可注入的 sleep 函数（测试用），默认 time.sleep
        self._sleep: Callable[[float], None] = _sleep

    def acquire(self, n: int = 1) -> float:
        """获取 n 个令牌，阻塞至可用。

        Args:
            n: 需要获取的令牌数（对应 Binance endpoint 权重）

        Returns:
            等待秒数（0 = 立即可用）
        """
        if n < 1:
            raise ValueError("n must be >= 1")

        with self._lock:
            wait_time = self._wait_for_tokens(n)

        return wait_time

    def _wait_for_tokens(self, n: int) -> float:
        """在持有锁的情况下计算并等待所需令牌。

        Returns:
            等待秒数（0 = 立即可用）
        """
        self._refill()

        if self._tokens >= n:
            self._tokens -= n
            return 0.0

        total_wait = 0.0
        for _ in range(50):  # 最多重试 50 次，防死循环
            # 需要等待令牌补充
            needed = n - self._tokens
            wait_seconds = needed / self.rate if self.rate > 0 else float("inf")

            if wait_seconds == float("inf"):
                # rate=0 且令牌不足，永久等待
                self._lock.release()
                try:
                    self._sleep(1.0)
                finally:
                    self._lock.acquire()
                self._refill()
                continue

            # 释放锁后睡眠（避免阻塞其他线程的 refill 计算）
            self._lock.release()
            try:
                self._sleep(wait_seconds)
            finally:
                self._lock.acquire()

            total_wait += wait_seconds
            self._refill()

            if self._tokens >= n:
                self._tokens -= n
                return total_wait
            # 否则继续循环等待
        # 超过重试次数，扣除令牌（防止极端情况下死锁）
        self._tokens -= n
        return total_wait

    def _refill(self) -> None:
        """按时间增量补充令牌（不超过 burst）（D04 §2）。

        令牌补充公式：new_tokens = min(burst, old_tokens + elapsed * rate)
        """
        now = self._clock()
        elapsed = now - self._last_refill
        if elapsed <= 0:
            return

        self._tokens = min(
            float(self.burst),
            self._tokens + elapsed * self.rate,
        )
        self._last_refill = now

    def on_rate_limited(self, retry_after: int) -> None:
        """收到 429 + Retry-After 时的全局冷却（D04 §2）。

        清空令牌并重置时间戳，强制后续 acquire 等待。

        Args:
            retry_after: Retry-After 秒数
        """
        with self._lock:
            self._tokens = 0.0
            self._last_refill = self._clock()

    def available_tokens(self) -> float:
        """返回当前可用令牌数（用于测试/调试）。"""
        with self._lock:
            self._refill()
            return self._tokens


def _monotonic() -> float:
    """返回单调时钟（秒）。"""
    return time.monotonic()


def _sleep(seconds: float) -> None:
    """默认 sleep 实现。"""
    time.sleep(seconds)
