"""ChronoForge 异常层级（D01 §3）— 兼容 re-export 壳。

审计 SR-05：错误层级真身上移至 chronoforge/exceptions.py（零依赖顶层
模块），本文件保持既有导入路径（connectors 内部、config、tests 等）
不变。
"""

from __future__ import annotations

from chronoforge.exceptions import (
    AuthError as AuthError,
)
from chronoforge.exceptions import (
    ChronoForgeError as ChronoForgeError,
)
from chronoforge.exceptions import (
    ConfigError as ConfigError,
)
from chronoforge.exceptions import (
    ProviderError as ProviderError,
)
from chronoforge.exceptions import (
    QualityError as QualityError,
)
from chronoforge.exceptions import (
    RateLimitError as RateLimitError,
)
from chronoforge.exceptions import (
    SchemaError as SchemaError,
)
from chronoforge.exceptions import (
    StorageError as StorageError,
)
from chronoforge.exceptions import (
    TransportError as TransportError,
)
from chronoforge.exceptions import (
    map_http_status as map_http_status,
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
    "map_http_status",
]
