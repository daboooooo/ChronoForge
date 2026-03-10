# 数据获取和更新框架使用指南

## 概述

本文档描述ChronoForge数据获取和更新框架的整体架构、核心组件以及使用方法。该框架基于TODOs中定义的数据更新需求，实现了统一的定时任务调度和数据增量更新机制。

框架核心设计原则包括以下几个方面。首先是时间轴驱动的增量更新，每次更新都基于本地缓存数据的时间戳，计算需要补充的时间范围，避免重复获取已存在的数据。其次是统一的数据合并策略，新数据与本地缓存数据按照时间戳合并，自动去重并保持数据完整性。第三是分层的错误处理机制，任务级重试、记录级容错相结合，确保单个数据点的失败不影响整体更新流程。最后是灵活的任务调度，支持固定间隔、Cron表达式、每小时定点等多种调度模式。

## 核心组件

### 任务调度器

任务调度器（TaskScheduler）是整个框架的核心调度引擎，负责管理所有定时任务的执行。其主要职责包括任务的添加与移除、调度时间计算、并发控制、执行历史记录以及优雅关闭。

调度器支持三种调度模式。间隔调度模式通过指定秒数或分钟数来定义任务执行间隔，适用于每30秒执行一次的tickers更新场景。定点调度模式在每小时指定秒数后执行任务，适用于每小时结束30秒后执行的数据刷新场景。Cron表达式调度模式使用标准Cron语法定义复杂的执行时间规则，适用于需要精确控制执行时间的场景。

任务调度器的关键配置参数包括max_concurrent_tasks（最大并发任务数，默认为10）和default_timeout（默认任务超时时间）。任务执行时会自动进行依赖检查，只有当依赖任务成功完成后，当前任务才会执行。

### 数据更新管理器

数据更新管理器（DataUpdateManager）是数据获取逻辑的统一入口，协调数据源管理器（DataSourceManager）和存储管理器（StorageManager）完成数据的获取、合并和持久化。其核心职责包括加载和管理交易对全集、执行增量数据更新、实现数据合并去重机制以及提供更新进度和统计信息。

交易对全集（SymbolUniverse）管理所有待更新的交易对信息，包括现货交易对（按交易所和报价资产分组）、期货合约交易对、币种市场列表以及宏观经济指标列表。全集在初始化时从数据源动态获取，并支持定期刷新以跟踪市场变化。

数据合并流程遵循以下标准步骤。第一步从存储加载现有数据，获取缓存数据的最新时间戳。第二步计算更新范围，根据增量更新策略确定需要获取的时间区间。第三步调用数据源获取新数据，使用计算出的时间范围调用对应数据源的fetch方法。第四步合并新旧数据，去除重复数据点后合并为一个完整数据集。第五步持久化更新结果，将合并后的数据保存回存储系统。

## 使用方法

### 初始化框架

初始化框架需要依次完成数据源管理器、存储管理器和任务调度器的创建与配置。首先创建数据源管理器实例并注册所需的数据源插件，然后创建存储管理器实例并注册存储后端，接着创建数据更新管理器实例并初始化交易对全集，最后创建任务调度器并配置定时任务。

```python
import asyncio
from chronoforge.data_source import init_data_source_manager
from chronoforge.storage.manager import init_storage_manager
from chronoforge.scheduler import init_scheduler, TaskPriority
from chronoforge.scheduler.data_update import init_data_update_manager

async def main():
    # 初始化数据源管理器
    dsm = init_data_source_manager(max_workers=5)
    dsm.create_data_source('CryptoSpotDataSource', {'apiKey': 'xxx'})
    dsm.create_data_source('CryptoUMFutureDataSource')
    dsm.create_data_source('CoinGeckoDataSource')
    dsm.create_data_source('FREDDataSource', {'api_key': 'fred_api_key'})
    dsm.create_data_source('GlobalMarketDataSource')

    # 初始化存储管理器
    sm = init_storage_manager()
    from chronoforge.storage.duckdb_storage import DUCKDBStorage
    sm.register_storage('duckdb', DUCKDBStorage({'db_path': './data.duckdb'}))

    # 初始化数据更新管理器
    dum = await init_data_update_manager(dsm, sm)

    # 初始化任务调度器
    scheduler = init_scheduler(max_concurrent_tasks=10)
```

### 配置定时任务

框架提供了简洁的API来配置各种定时任务。每30秒执行一次的tickers更新任务通过add_interval_task方法配置，参数seconds=30指定执行间隔。每小时结束30秒后执行的数据刷新任务通过add_hourly_task方法配置，参数offset_seconds=30指定每小时开始后的偏移秒数。

```python
# 配置30秒周期的tickers更新任务
async def spot_tickers_task():
    dum = get_data_update_manager()
    await dum.update_spot_tickers(exchanges=['binance', 'okx'])

scheduler.add_interval_task(
    name='spot_tickers',
    func=spot_tickers_task,
    seconds=30,
    priority=TaskPriority.HIGH
)

# 配置每小时周期的OHLCV更新任务
async def spot_ohlcv_task():
    dum = get_data_update_manager()
    await dum.update_spot_ohlcv(
        exchanges=['binance', 'okx'],
        timeframe='1h',
        top_percent=80
    )

scheduler.add_hourly_task(
    name='spot_ohlcv',
    func=spot_ohlcv_task,
    offset_seconds=30,
    priority=TaskPriority.MEDIUM
)

# 配置每小时周期的期货指标更新任务
async def futures_metrics_task():
    dum = get_data_update_manager()
    await dum.update_futures_metrics(timeframe='1h')

scheduler.add_hourly_task(
    name='futures_metrics',
    func=futures_metrics_task,
    offset_seconds=30
)

# 配置每小时周期的CoinGecko市场数据更新任务
async def coin_markets_task():
    dum = get_data_update_manager()
    await dum.update_coin_markets()

scheduler.add_hourly_task(
    name='coin_markets',
    func=coin_markets_task,
    offset_seconds=30
)

# 配置每小时周期的宏观经济数据更新任务
async def macro_data_task():
    dum = get_data_update_manager()
    await dum.update_macro_data()

scheduler.add_hourly_task(
    name='macro_data',
    func=macro_data_task,
    offset_seconds=30
)

# 配置每小时周期的全球加密货币市场数据更新任务
async def global_market_task():
    dum = get_data_update_manager()
    await dum.update_global_crypto_market()

scheduler.add_hourly_task(
    name='global_market',
    func=global_market_task,
    offset_seconds=30
)
```

### 启动和停止调度器

配置完成后启动调度器开始执行定时任务，在应用退出时优雅停止调度器确保所有任务完成。

```python
# 启动调度器
await scheduler.start()

# 保持运行
try:
    while True:
        await asyncio.sleep(60)
        stats = scheduler.get_metrics()
        print(f"Tasks: {stats['total_tasks']}, "
              f"Completed: {stats['completed_tasks']}, "
              f"Failed: {stats['failed_tasks']}")
except KeyboardInterrupt:
    pass
finally:
    await scheduler.stop()
```

## 高级配置

### 更新策略配置

数据更新支持三种策略选择。INCREMENTAL模式（默认）仅获取自上次更新以来的新数据，适用于常规的增量更新场景。FULL模式每次都全量获取数据，适用于需要重建缓存或数据完整性要求极高的场景。SMART模式根据数据源特性自动判断最优策略，适用于不确定数据变化频率的场景。

```python
# 使用全量更新策略
await dum.update_spot_ohlcv(
    exchanges=['binance'],
    timeframe='1h',
    update_strategy=UpdateStrategy.FULL
)
```

### 任务优先级配置

任务优先级影响并发执行顺序，优先级高的任务优先获取执行资源。可用优先级从高到低为CRITICAL、HIGH、MEDIUM、LOW。重要任务应配置较高优先级以确保及时执行。

```python
scheduler.add_interval_task(
    name='critical_tickers',
    func=critical_tickers_task,
    seconds=10,
    priority=TaskPriority.CRITICAL
)
```

### 重试机制配置

单个任务支持自动重试，配置retry_count指定重试次数，retry_delay_seconds指定重试间隔。首次重试延迟为配置的秒数，后续重试延迟递增。

```python
scheduler.add_hourly_task(
    name='retry_example',
    func=some_task,
    offset_seconds=30,
    retry_count=3,
    retry_delay_seconds=5
)
```

## 监控和统计

### 获取任务状态

调度器提供详细的任务状态查询功能，可以获取单个任务或所有任务的运行状态。

```python
# 获取所有任务状态
status = scheduler.get_task_status()
for task_id, info in status.items():
    print(f"{task_id}: {info['status']}, Next: {info['next_run']}")

# 获取单个任务状态
single_status = scheduler.get_task_status('task_123')
```

### 获取执行指标

调度器和数据更新管理器都提供丰富的执行指标，便于监控和优化。

```python
# 调度器指标
metrics = scheduler.get_metrics()
print(f"Total tasks: {metrics['total_tasks']}")
print(f"Success rate: {metrics['completed_tasks'] / max(1, metrics['total_tasks']) * 100:.1f}%")
print(f"Avg execution time: {metrics['avg_execution_time_ms']:.2f}ms")

# 数据更新统计
update_stats = dum.get_update_stats()
print(f"Symbols loaded: {update_stats['symbol_universe']['spot_symbols_count']}")
```

### 任务执行历史

调度器维护所有任务的执行历史，包括执行时间、执行结果、耗时以及错误信息。

```python
# 获取执行历史
for result in scheduler._execution_history[-10:]:
    print(f"{result.task_name}: {result.status.value} "
          f"({result.duration_ms:.0f}ms)")
    if result.error:
        print(f"  Error: {result.error}")
```

## 最佳实践

### 资源管理

在生产环境中使用框架时，应遵循以下资源管理建议。合理设置max_concurrent_tasks以避免数据源API限流，建议值为数据源数量的1.5到2倍。为每个数据源配置独立的重试策略，API不稳定的外部数据源应配置更多重试次数。定期监控执行指标，及时发现和解决性能问题。

### 数据一致性

确保数据一致性的关键措施包括始终使用INCREMENTAL策略进行常规更新，定期使用FULL策略进行数据一致性校验，在任务失败时检查执行历史并手动触发重试，以及使用事务性的存储后端确保更新原子性。

### 错误处理

健壮的错误处理机制应配置合理的重试次数和延迟，实施任务级别的错误隔离防止单任务失败影响其他任务，定期清理执行历史避免内存溢出，以及设置合理的超时时间防止任务无限期阻塞。

## 完整示例

以下是一个完整的应用示例，演示如何配置和运行数据更新框架。

```python
import asyncio
import logging
from chronoforge.data_source import init_data_source_manager
from chronoforge.storage import init_storage_manager
from chronoforge.scheduler import init_scheduler, TaskPriority
from chronoforge.scheduler.data_update import init_data_update_manager, get_data_update_manager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    # 初始化数据源
    dsm = init_data_source_manager(max_workers=5)
    dsm.create_data_source('CryptoSpotDataSource')
    dsm.create_data_source('CryptoUMFutureDataSource')
    dsm.create_data_source('CoinGeckoDataSource')
    dsm.create_data_source('FREDDataSource')
    dsm.create_data_source('GlobalMarketDataSource')

    # 初始化存储
    sm = init_storage_manager()
    from chronoforge.storage.duckdb_storage import DUCKDBStorage
    sm.register_storage('duckdb', DUCKDBStorage({'db_path': './data.duckdb'}))

    # 初始化数据更新
    dum = await init_data_update_manager(dsm, sm)

    # 初始化调度器
    scheduler = init_scheduler(max_concurrent_tasks=10)

    # 添加tickers更新任务（每30秒）
    async def update_tickers():
        await dum.update_spot_tickers(exchanges=['binance', 'okx'])

    scheduler.add_interval_task(
        name='tickers',
        func=update_tickers,
        seconds=30,
        priority=TaskPriority.HIGH
    )

    # 添加OHLCV更新任务（每小时）
    async def update_ohlcv():
        await dum.update_spot_ohlcv(
            exchanges=['binance', 'okx'],
            timeframe='1h',
            top_percent=80
        )

    scheduler.add_hourly_task(
        name='ohlcv',
        func=update_ohlcv,
        offset_seconds=30,
        priority=TaskPriority.MEDIUM
    )

    # 添加其他每小时任务
    async def update_futures():
        await dum.update_futures_metrics(timeframe='1h')

    scheduler.add_hourly_task(
        name='futures',
        func=update_futures,
        offset_seconds=30
    )

    async def update_coingecko():
        await dum.update_coin_markets()

    scheduler.add_hourly_task(
        name='coingecko',
        func=update_coingecko,
        offset_seconds=30
    )

    async def update_macro():
        await dum.update_macro_data()

    scheduler.add_hourly_task(
        name='macro',
        func=update_macro,
        offset_seconds=30
    )

    async def update_global():
        await dum.update_global_crypto_market()

    scheduler.add_hourly_task(
        name='global',
        func=update_global,
        offset_seconds=30
    )

    # 启动调度器
    await scheduler.start()
    logger.info("Scheduler started")

    # 定期打印状态
    try:
        while True:
            await asyncio.sleep(60)
            scheduler_stats = scheduler.get_metrics()
            update_stats = dum.get_update_stats()
            logger.info(f"Scheduler: {scheduler_stats}")
            logger.info(f"Update: {update_stats['symbol_universe']}")
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        await scheduler.stop()
        logger.info("Shutdown complete")

if __name__ == '__main__':
    asyncio.run(main())
```
