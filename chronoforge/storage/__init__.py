from .base import StorageBase, verify_storage_instance
from .localfile import LocalFileStorage
from .duckdb import DUCKDBStorage

# 使RedisStorage成为可选依赖
try:
    from .redisdb import RedisStorage
    __all__ = [
        "StorageBase",
        "verify_storage_instance",
        "LocalFileStorage",
        "DUCKDBStorage",
        "RedisStorage"
    ]
except ImportError:
    __all__ = [
        "StorageBase",
        "verify_storage_instance",
        "LocalFileStorage",
        "DUCKDBStorage"
    ]
