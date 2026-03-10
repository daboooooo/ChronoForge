"""
存储模块 - 提供多种存储后端支持

当前主要支持DuckDB存储，提供高性能的金融数据存储解决方案。
"""

# 优先导出DuckDB存储模块
from .duckdb_storage import (
    FinancialDataWarehouse,
    DUCKDBStorage,
    SmartIncrementalManager,
    DataValidator,
    DataQualityMonitor
)

# 本地文件存储
from .localfile_storage.adapter import LocalFileStorage

# 向后兼容性支持
from .base import StorageBase, verify_storage_instance

# 数据标准化器
from .normalizer import DataFrameNormalizer

__all__ = [
    # DuckDB存储核心组件
    "FinancialDataWarehouse",
    "DUCKDBStorage",
    "SmartIncrementalManager",
    "DataValidator",
    "DataQualityMonitor",

    # 本地文件存储
    "LocalFileStorage",

    # 基础接口（向后兼容）
    "StorageBase",
    "verify_storage_instance",

    # 数据标准化器
    "DataFrameNormalizer"
]
