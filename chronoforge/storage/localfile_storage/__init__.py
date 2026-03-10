"""
本地文件存储模块 - 基于文件系统的轻量级金融数据存储

提供统一的本地文件存储解决方案，支持CSV/JSON/Parquet格式，
适用于小规模数据存储或需要便于查看和编辑的场景。
"""

from .adapter import LocalFileStorage
from .warehouse import LocalDataWarehouse
from .incremental import LocalIncrementalManager
from .quality import LocalDataValidator, LocalDataQualityMonitor

__all__ = [
    "LocalFileStorage",
    "LocalDataWarehouse",
    "LocalIncrementalManager",
    "LocalDataValidator",
    "LocalDataQualityMonitor"
]
