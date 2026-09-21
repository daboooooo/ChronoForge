"""ChronoForge 异常层级（D01 §3） + HTTP 状态码映射（D04 §3）。

全部异常继承 ChronoForgeError 根类。
D01 §3 定义的错误分类：
- TransportError: 传输层错误（可重试）
- RateLimitError: 速率限制（可重试 + 退避，携带 retry_after）
- AuthError: 认证失败（不重试）
- ProviderError: 提供者错误 4xx（不重试）
- SchemaError: 数据模式错误（dataset 熔断）
- QualityError: 质量检查失败（由配置决定）
- StorageError: 存储层错误（SQLite/Parquet）
- ConfigError: 配置错误（启动 fail fast）

D04 §3 HTTP→错误映射表：
- 401/403 → AuthError
- 429 → RateLimitError
- 400/404/422 → ProviderError
- 5xx → TransportError
- 其他 4xx → ProviderError
- 其他 → TransportError
"""

from __future__ import annotations

from typing import Any


class ChronoForgeError(Exception):
    """所有 ChronoForge 异常的根类（D01 §3）。

    所有异常携带 context: dict 用于调试（source/dataset/endpoint/status）。
    """

    def __init__(self, message: str, context: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.context: dict[str, Any] = context or {}


class TransportError(ChronoForgeError):
    """传输层错误（网络断开、连接超时等，可重试）。"""


class RateLimitError(TransportError):
    """速率限制错误（可重试 + 退避，尊重 Retry-After）。"""

    def __init__(
        self,
        message: str,
        context: dict[str, Any] | None = None,
        retry_after: int | None = None,
    ) -> None:
        super().__init__(message, context)
        self.retry_after: int | None = retry_after


class AuthError(ChronoForgeError):
    """认证失败（不重试，run FAILED）。"""


class ProviderError(ChronoForgeError):
    """数据源提供者错误 4xx（不重试）。"""


class SchemaError(ChronoForgeError):
    """数据模式错误（不重试，dataset 熔断）。"""


class QualityError(ChronoForgeError):
    """数据质量检查失败（由配置决定阻断/放行）。"""


class ConfigError(ChronoForgeError):
    """配置错误（启动 fail fast）。"""


class StorageError(ChronoForgeError):
    """存储层错误（SQLite 连接/迁移/查询等）（D01 §3）。"""

    def __init__(self, message: str, context: dict[str, Any] | None = None) -> None:
        super().__init__(message, context)


__all__ = [
    "ChronoForgeError",
    "TransportError",
    "RateLimitError",
    "AuthError",
    "ProviderError",
    "SchemaError",
    "QualityError",
    "StorageError",
    "ConfigError",
    "map_http_status",
]


def map_http_status(
    status_code: int, response_body: Any = None
) -> type[ChronoForgeError]:
    """HTTP 状态码 → 错误类（D04 §3 映射表）。

    Returns an **error class** (not instance). Caller should instantiate
    with appropriate context dict.

    Mapping:
        401/403 → AuthError
        429     → RateLimitError
        400/404/422 → ProviderError
        5xx     → TransportError
        other 4xx → ProviderError
        other   → TransportError
    """
    match status_code:
        case 401 | 403:
            return AuthError
        case 429:
            return RateLimitError
        case 400 | 404 | 422:
            return ProviderError
        case status if status >= 500:
            return TransportError
        case _:
            if 400 <= status_code < 500:
                return ProviderError
            return TransportError
