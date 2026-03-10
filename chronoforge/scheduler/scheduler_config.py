"""
Scheduler 统一配置模块

定义数据更新任务的配置，包括调度参数、并发限制、重试策略等
"""
from enum import Enum
from dataclasses import dataclass, field
from typing import Dict, Optional


class DataType(Enum):
    """数据类型枚举"""
    TICKER = "ticker"
    OHLCV = "ohlcv"
    FUTURES = "futures"
    COIN_MARKETS = "coin_markets"
    MACRO = "macro"
    GLOBAL_MARKET = "global_market"

    @property
    def task_type(self) -> str:
        """对应存储的任务类型"""
        type_mapping = {
            DataType.TICKER: "tickers",
            DataType.OHLCV: "ohlcv",
            DataType.FUTURES: "futures_metrics",
            DataType.COIN_MARKETS: "coin_markets",
            DataType.MACRO: "macro_fred",
            DataType.GLOBAL_MARKET: "global_market",
        }
        return type_mapping.get(self, self.value)


@dataclass
class DataUpdateConfig:
    """数据更新配置"""
    data_type: DataType
    interval_seconds: int
    offset_seconds: int
    cache_validity_seconds: int
    default_range_days: int
    concurrent_limit: int
    retry_max: int
    retry_backoff: float
    enabled: bool = True
    priority: int = 5


SCHEDULE_CONFIGS: Dict[DataType, DataUpdateConfig] = {
    DataType.TICKER: DataUpdateConfig(
        data_type=DataType.TICKER,
        interval_seconds=30,
        offset_seconds=0,
        cache_validity_seconds=30,
        default_range_days=0,
        concurrent_limit=5,
        retry_max=3,
        retry_backoff=2.0,
        priority=1
    ),
    DataType.OHLCV: DataUpdateConfig(
        data_type=DataType.OHLCV,
        interval_seconds=3600,
        offset_seconds=30,
        cache_validity_seconds=3600,
        default_range_days=30,
        concurrent_limit=10,
        retry_max=2,
        retry_backoff=3.0,
        priority=2
    ),
    DataType.FUTURES: DataUpdateConfig(
        data_type=DataType.FUTURES,
        interval_seconds=3600,
        offset_seconds=30,
        cache_validity_seconds=3600,
        default_range_days=7,
        concurrent_limit=10,
        retry_max=2,
        retry_backoff=3.0,
        priority=2
    ),
    DataType.COIN_MARKETS: DataUpdateConfig(
        data_type=DataType.COIN_MARKETS,
        interval_seconds=3600,
        offset_seconds=60,
        cache_validity_seconds=3600,
        default_range_days=1,
        concurrent_limit=2,
        retry_max=3,
        retry_backoff=2.0,
        priority=3
    ),
    DataType.MACRO: DataUpdateConfig(
        data_type=DataType.MACRO,
        interval_seconds=3600,
        offset_seconds=90,
        cache_validity_seconds=7200,
        default_range_days=365,
        concurrent_limit=5,
        retry_max=2,
        retry_backoff=3.0,
        priority=4
    ),
    DataType.GLOBAL_MARKET: DataUpdateConfig(
        data_type=DataType.GLOBAL_MARKET,
        interval_seconds=3600,
        offset_seconds=120,
        cache_validity_seconds=3600,
        default_range_days=30,
        concurrent_limit=2,
        retry_max=3,
        retry_backoff=2.0,
        priority=5
    ),
}


@dataclass
class SchedulerConfig:
    """调度器全局配置"""
    max_workers: int = 4
    max_concurrent_tasks: int = 10
    default_timeout: float = 300.0
    grace_period: float = 30.0
    enable_metrics: bool = True
    enable_retry: bool = True
    default_retry_max: int = 3
    default_retry_backoff: float = 2.0
    tickers_exchanges: list = field(default_factory=lambda: ["binance", "okx"])
    tickers_quotes: list = field(default_factory=lambda: ["USDT"])
    ohlcv_top_percent: float = 80.0
    ohlcv_timeframe: str = "1h"
    futures_timeframe: str = "1h"


def get_config() -> SchedulerConfig:
    """获取调度器配置"""
    return SchedulerConfig()


def get_data_config(data_type: DataType) -> Optional[DataUpdateConfig]:
    """获取特定数据类型的配置"""
    return SCHEDULE_CONFIGS.get(data_type)


def get_enabled_data_types() -> list[DataType]:
    """获取启用的数据类型列表"""
    return [cfg.data_type for cfg in SCHEDULE_CONFIGS.values() if cfg.enabled]
