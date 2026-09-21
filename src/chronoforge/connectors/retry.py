"""指数退避重试装饰器（D04 §3，D10 ACQUISITION-001.3）。

重试矩阵（D04 §3）：
- TransportError: 是，指数退避 1→2→4→8→16s + jitter(0~0.5s)，最大 5 次
- RateLimitError: 是，同上 + Retry-After（由 connector 实现处理）
- AuthError: 否，立即抛出
- ProviderError(4xx≠429): 否，立即抛出
- SchemaError: 否，立即抛出

structlog 事件：connector.retry（D08 §3）
- 级别：info（重试是正常路径）
- 字段：attempt, max_retries, delay, error, error_type
"""

from __future__ import annotations

import functools
import random
import time
from collections.abc import Callable
from typing import Any, TypeVar

import structlog

from .errors import RateLimitError, TransportError

logger = structlog.get_logger()

F = TypeVar("F", bound=Callable[..., Any])


def retry(
    max_retries: int = 5,
    base_delay: float = 1.0,
    jitter_range: float = 0.5,
    retryable_errors: tuple[type[Exception], ...] | None = None,
) -> Callable[[F], F]:
    """指数退避重试装饰器（D04 §3 重试矩阵）。

    Args:
        max_retries: 最大重试次数（含首次调用共 max_retries+1 次尝试）
        base_delay: 基础退避秒数（第 1 次重试 = base_delay，第 2 次 = base_delay*2，...）
        jitter_range: jitter 范围 [0, jitter_range] 秒（防 thundering herd）
        retryable_errors: 可重试错误类元组，默认 (TransportError, RateLimitError)

    Returns:
        装饰器函数
    """
    if retryable_errors is None:
        retryable_errors = (TransportError, RateLimitError)

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exc: Exception | None = None
            for attempt in range(1, max_retries + 2):  # 1 次初始 + max_retries 次重试
                try:
                    return func(*args, **kwargs)
                except retryable_errors as exc:
                    last_exc = exc
                    if attempt >= max_retries + 1:  # 已达最大重试，退出循环
                        break
                    # 计算退避：base_delay * 2^(attempt-1) + jitter
                    delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, jitter_range)
                    # 审计 M-8：尊重服务端 Retry-After（D04 §3）——
                    # 退避不小于服务端要求的最小等待秒数
                    retry_after: int | None = None
                    if isinstance(exc, RateLimitError):
                        retry_after = exc.retry_after
                        if retry_after is not None:
                            delay = max(delay, float(retry_after))
                    # 记录 structlog 事件
                    error_msg = str(exc)[:500]
                    logger.info(
                        "connector.retry",
                        attempt=attempt,
                        max_retries=max_retries,
                        delay=delay,
                        error=error_msg,
                        error_type=type(exc).__name__,
                        retry_after=retry_after,
                    )
                    time.sleep(delay)
            # 重试耗尽，抛出最后一次异常
            raise last_exc  # type: ignore[misc]
        return wrapper  # type: ignore[return-value]
    return decorator


__all__ = ["retry"]
