"""ChronoForge插件系统"""

# 导出基类
from .base import DataSourceBase, verify_datasource_instance
from .crypto_spot import CryptoSpotDataSource
from .fred import FREDDataSource
from .global_market import GlobalMarketDataSource
from .crypto_umfuture import CryptoUMFutureDataSource
from .bitcoin_fgi import BitcoinFGIDataSource

__all__ = [
    "DataSourceBase",
    "verify_datasource_instance",
    "CryptoSpotDataSource",
    "FREDDataSource",
    "GlobalMarketDataSource",
    "CryptoUMFutureDataSource",
    "BitcoinFGIDataSource",
]
