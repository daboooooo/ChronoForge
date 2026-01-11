from .base import StorageBase, verify_storage_instance
from .localfile import LocalFileStorage

# 使DUCKDBStorage成为可选依赖
try:
    from .duckdb import DUCKDBStorage
    duckdb_available = True
except ImportError:
    duckdb_available = False

# 使RedisStorage成为可选依赖
try:
    from .redisdb import RedisStorage
    redis_available = True
except ImportError:
    redis_available = False

# 构建__all__列表
__all__ = [
    "StorageBase",
    "verify_storage_instance",
    "LocalFileStorage"
]

if duckdb_available:
    __all__.append("DUCKDBStorage")

if redis_available:
    __all__.append("RedisStorage")
