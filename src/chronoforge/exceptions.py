"""ChronoForge 异常层次（D04，向后兼容）。

从 connectors/errors.py 导入根类和 connector 相关异常。
StorageError 定义在 connectors/errors.py（无循环导入）。
"""

from __future__ import annotations

from chronoforge.connectors.errors import (
    AuthError,
    ChronoForgeError,
    ConfigError,
    ProviderError,
    QualityError,
    RateLimitError,
    SchemaError,
    StorageError,
    TransportError,
)

__all__ = [
    "ChronoForgeError",
    "AuthError",
    "ConfigError",
    "ProviderError",
    "QualityError",
    "RateLimitError",
    "SchemaError",
    "StorageError",
    "TransportError",
]
