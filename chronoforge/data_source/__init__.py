"""ChronoForge插件系统"""

from .base import (
    DataSourceBase,
    verify_datasource_instance,
    DataSourceError,
    DataSourceConnectionError,
    DataSourceRateLimitError,
    DataSourceAuthenticationError,
    DataSourceNotFoundError,
    DataSourceDataError,
    DataSourceTimeoutError,
    DataSchemaType,
    DataField,
    DataSchema,
    StandardSchemas,
)
from .cache import (
    DataSourceCacheMixin,
    cached_fetch,
    generate_cache_key,
    create_data_source_cache,
    DataSourceCacheConfig,
)
from .crypto_spot import CryptoSpotDataSource
from .fred import FREDDataSource
from .global_market import GlobalMarketDataSource
from .crypto_umfuture import CryptoUMFutureDataSource
from .althernative import AlthernativeDataSource
from .coingecko import CoinGeckoDataSource
from .manager import (
    DataSourceManager,
    data_source_manager,
    init_data_source_manager,
    RetryConfig,
    DataSourceCapability,
    FetchTask,
    FetchResult,
)

__all__ = [
    "DataSourceBase",
    "verify_datasource_instance",
    "DataSourceError",
    "DataSourceConnectionError",
    "DataSourceRateLimitError",
    "DataSourceAuthenticationError",
    "DataSourceNotFoundError",
    "DataSourceDataError",
    "DataSourceTimeoutError",
    "DataSchemaType",
    "DataField",
    "DataSchema",
    "StandardSchemas",
    "DataSourceCacheMixin",
    "cached_fetch",
    "generate_cache_key",
    "create_data_source_cache",
    "DataSourceCacheConfig",
    "CryptoSpotDataSource",
    "FREDDataSource",
    "GlobalMarketDataSource",
    "CryptoUMFutureDataSource",
    "AlthernativeDataSource",
    "CoinGeckoDataSource",
    "DataSourceManager",
    "data_source_manager",
    "init_data_source_manager",
    "RetryConfig",
    "DataSourceCapability",
    "FetchTask",
    "FetchResult",
]
