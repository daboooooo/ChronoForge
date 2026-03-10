# ChronoForge 快速运行指南

## 概述

本文档指导你如何运行ChronoForge框架，实现TODO.md中定义的6个数据更新任务。所有任务已实现并通过测试，可以直接运行。

## 任务清单

根据TODO.md，需要实现以下6个数据更新任务：

| 任务 | 更新频率 | 数据类型 | 状态 |
|-----|---------|---------|------|
| 1. Spot Tickers | 每30秒 | Binance/OKX 现货行情 | ✅ 已完成 |
| 2. Spot OHLCV | 每小时+30秒 | 前80%交易量现货K线 | ✅ 已完成 |
| 3. Futures Metrics | 每小时+30秒 | 合约市场数据 | ✅ 已完成 |
| 4. CoinGecko Markets | 每小时+30秒 | 代币市场数据 | ✅ 已完成 |
| 5. FRED Macro Data | 每小时+30秒 | 宏观经济指标 | ✅ 已完成 |
| 6. Global Crypto Market | 每小时+30秒 | 全球加密货币市场 | ✅ 已完成 |

## 快速开始

### 1. 环境准备

```bash
# 进入项目目录
cd /Users/horsenli/Works/ChronoForge

# 激活虚拟环境
source .venv/bin/activate

# 安装依赖
pip install croniter

# 运行集成测试验证安装
python test_integration.py --test
```

### 2. 运行演示模式

```bash
# 查看框架功能演示
python test_integration.py --demo
```

## 完整配置示例

创建一个配置文件 `config.yaml`：

```yaml
# ChronoForge 配置示例

# 存储配置
storage:
  type: "duckdb"
  options:
    db_path: "./data/chronoforge.duckdb"

# 数据源配置
data_sources:
  binance:
    type: "CryptoSpotDataSource"
    apiKey: "${BINANCE_API_KEY}"
    secret: "${BINANCE_API_SECRET}"
  
  okx:
    type: "CryptoSpotDataSource"
    apiKey: "${OKX_API_KEY}"
    secret: "${OKX_API_SECRET}"
  
  futures:
    type: "CryptoUMFutureDataSource"
  
  coingecko:
    type: "CoinGeckoDataSource"
  
  fred:
    type: "FREDDataSource"
    api_key: "${FRED_API_KEY}"
  
  global_market:
    type: "GlobalMarketDataSource"

# 调度配置
scheduler:
  max_concurrent_tasks: 10
  default_timeout: 300

# 任务配置
tasks:
  spot_tickers:
    enabled: true
    interval_seconds: 30
    exchanges:
      - binance
      - okx
  
  spot_ohlcv:
    enabled: true
    hourly_offset_seconds: 30
    exchanges:
      - binance
      - okx
    top_percent: 80
    timeframes:
      - "1h"
      - "4h"
      - "1d"
  
  futures_metrics:
    enabled: true
    hourly_offset_seconds: 30
    timeframes:
      - "1h"
      - "4h"
  
  coin_markets:
    enabled: true
    hourly_offset_seconds: 30
  
  macro_data:
    enabled: true
    hourly_offset_seconds: 30
  
  global_market:
    enabled: true
    hourly_offset_seconds: 30
```

## 运行脚本

创建 `run_chronoforge.py` 文件：

```python
#!/usr/bin/env python3
"""
ChronoForge 数据更新任务主程序

实现TODO.md中定义的6个数据更新任务：
1. 每30秒更新 Spot Tickers
2. 每小时+30秒更新 Spot OHLCV
3. 每小时+30秒更新 Futures Metrics
4. 每小时+30秒更新 CoinGecko Markets
5. 每小时+30秒更新 FRED Macro Data
6. 每小时+30秒更新 Global Crypto Market
"""

import asyncio
import logging
import signal
import sys
from datetime import datetime, timezone

from chronoforge.data_source import init_data_source_manager
from chronoforge.storage.manager import init_storage_manager
from chronoforge.scheduler import TaskScheduler, TaskPriority
from chronoforge.scheduler.data_update import DataUpdateManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 全局变量
scheduler: TaskScheduler = None
dum: DataUpdateManager = None


async def setup_data_sources(dsm):
    """配置数据源"""
    logger.info("配置数据源...")

    # Binance 现货数据源
    dsm.create_data_source('CryptoSpotDataSource', {
        'name': 'binance_spot',
        'exchange': 'binance'
    })

    # OKX 现货数据源
    dsm.create_data_source('CryptoSpotDataSource', {
        'name': 'okx_spot',
        'exchange': 'okx'
    })

    # Binance U-Margin 期货数据源
    dsm.create_data_source('CryptoUMFutureDataSource', {
        'name': 'binance_futures'
    })

    # CoinGecko 数据源
    dsm.create_data_source('CoinGeckoDataSource')

    # FRED 数据源
    dsm.create_data_source('FREDDataSource')

    # Global Market 数据源
    dsm.create_data_source('GlobalMarketDataSource')

    logger.info(f"已配置 {len(dsm.data_sources)} 个数据源")


async def setup_storage(sm):
    """配置存储后端"""
    logger.info("配置存储后端...")

    from chronoforge.storage.duckdb_storage import DUCKDBStorage

    sm.register_storage('duckdb', DUCKDBStorage({
        'db_path': './data/chronoforge.duckdb'
    }))

    logger.info("存储后端已配置")


async def setup_tasks(scheduler: TaskScheduler, dum: DataUpdateManager):
    """配置定时任务 - 实现TODO.md中的6个任务"""

    # ===== 任务1: 每30秒更新 Spot Tickers =====
    async def spot_tickers_task():
        logger.info("任务1: 更新 Spot Tickers (Binance + OKX)")
        try:
            results = await dum.update_spot_tickers(
                exchanges=['binance', 'okx']
            )
            for exchange, result in results.items():
                logger.info(f"  {exchange}: {result.records_updated} 条记录")
        except Exception as e:
            logger.error(f"  Spot Tickers 更新失败: {e}")

    scheduler.add_interval_task(
        name='spot_tickers',
        func=spot_tickers_task,
        seconds=30,
        priority=TaskPriority.HIGH
    )
    logger.info("已添加任务1: Spot Tickers (每30秒)")

    # ===== 任务2: 每小时+30秒更新 Spot OHLCV =====
    async def spot_ohlcv_task():
        logger.info("任务2: 更新 Spot OHLCV (前80%交易量)")
        try:
            results = await dum.update_spot_ohlcv(
                exchanges=['binance', 'okx'],
                timeframe='1h',
                top_percent=80
            )
            for symbol, result in results.items():
                if result.status.value == 'completed':
                    logger.info(f"  {symbol}: {result.records_updated} 条新记录")
        except Exception as e:
            logger.error(f"  Spot OHLCV 更新失败: {e}")

    scheduler.add_hourly_task(
        name='spot_ohlcv',
        func=spot_ohlcv_task,
        offset_seconds=30,
        priority=TaskPriority.MEDIUM
    )
    logger.info("已添加任务2: Spot OHLCV (每小时+30秒)")

    # ===== 任务3: 每小时+30秒更新 Futures Metrics =====
    async def futures_metrics_task():
        logger.info("任务3: 更新 Futures Metrics")
        try:
            results = await dum.update_futures_metrics(timeframe='1h')
            for symbol, result in results.items():
                if result.status.value == 'completed':
                    logger.info(f"  {symbol}: {result.records_updated} 条记录")
        except Exception as e:
            logger.error(f"  Futures Metrics 更新失败: {e}")

    scheduler.add_hourly_task(
        name='futures_metrics',
        func=futures_metrics_task,
        offset_seconds=30,
        priority=TaskPriority.MEDIUM
    )
    logger.info("已添加任务3: Futures Metrics (每小时+30秒)")

    # ===== 任务4: 每小时+30秒更新 CoinGecko Markets =====
    async def coin_markets_task():
        logger.info("任务4: 更新 CoinGecko Markets")
        try:
            results = await dum.update_coin_markets()
            for coin, result in results.items():
                if result.status.value == 'completed':
                    logger.info(f"  {coin}: {result.records_updated} 条记录")
        except Exception as e:
            logger.error(f"  CoinGecko Markets 更新失败: {e}")

    scheduler.add_hourly_task(
        name='coin_markets',
        func=coin_markets_task,
        offset_seconds=30,
        priority=TaskPriority.LOW
    )
    logger.info("已添加任务4: CoinGecko Markets (每小时+30秒)")

    # ===== 任务5: 每小时+30秒更新 FRED Macro Data =====
    async def macro_data_task():
        logger.info("任务5: 更新 FRED Macro Data")
        try:
            results = await dum.update_macro_data()
            for indicator, result in results.items():
                if result.status.value == 'completed':
                    logger.info(f"  {indicator}: {result.records_updated} 条记录")
        except Exception as e:
            logger.error(f"  Macro Data 更新失败: {e}")

    scheduler.add_hourly_task(
        name='macro_data',
        func=macro_data_task,
        offset_seconds=30,
        priority=TaskPriority.LOW
    )
    logger.info("已添加任务5: FRED Macro Data (每小时+30秒)")

    # ===== 任务6: 每小时+30秒更新 Global Crypto Market =====
    async def global_market_task():
        logger.info("任务6: 更新 Global Crypto Market")
        try:
            result = await dum.update_global_crypto_market()
            if result.status.value == 'completed':
                logger.info(f"  Global Market: {result.records_updated} 条记录")
        except Exception as e:
            logger.error(f"  Global Market 更新失败: {e}")

    scheduler.add_hourly_task(
        name='global_market',
        func=global_market_task,
        offset_seconds=30,
        priority=TaskPriority.MEDIUM
    )
    logger.info("已添加任务6: Global Crypto Market (每小时+30秒)")

    logger.info(f"\n共配置 {len(scheduler.tasks)} 个定时任务")


async def print_status():
    """定期打印状态"""
    while scheduler and scheduler._running:
        try:
            await asyncio.sleep(60)
            metrics = scheduler.get_metrics()
            stats = dum.get_update_stats()

            logger.info("=" * 50)
            logger.info(f"时间: {datetime.now(timezone.utc).isoformat()}")
            logger.info(f"调度器指标: {metrics}")
            logger.info(f"交易对全集: {stats['symbol_universe']}")
            logger.info("=" * 50)
        except Exception as e:
            logger.error(f"状态打印失败: {e}")


async def main():
    """主函数"""
    global scheduler, dum

    logger.info("=" * 60)
    logger.info("ChronoForge 数据更新任务")
    logger.info("实现TODO.md中的6个数据更新任务")
    logger.info("=" * 60)

    # 初始化数据源管理器
    logger.info("\n步骤1: 初始化数据源管理器")
    dsm = init_data_source_manager(max_workers=5)
    await setup_data_sources(dsm)

    # 初始化存储管理器
    logger.info("\n步骤2: 初始化存储管理器")
    sm = init_storage_manager()
    await setup_storage(sm)

    # 初始化数据更新管理器
    logger.info("\n步骤3: 初始化数据更新管理器")
    dum = await DataUpdateManager(dsm, sm).initialize()
    logger.info(f"交易对全集已加载: {len(dum.symbol_universe.spot_symbols)} 个交易所")

    # 初始化调度器
    logger.info("\n步骤4: 初始化任务调度器")
    scheduler = TaskScheduler(max_concurrent_tasks=10)

    # 配置任务
    logger.info("\n步骤5: 配置定时任务")
    await setup_tasks(scheduler, dum)

    # 打印配置的任务列表
    logger.info("\n配置的任务列表:")
    for task_id, task in scheduler.tasks.items():
        next_run = task.next_run.isoformat() if task.next_run else "N/A"
        logger.info(f"  - {task.name}: {task.schedule_type}, 下次执行: {next_run}")

    # 启动调度器
    logger.info("\n步骤6: 启动调度器")
    await scheduler.start()
    logger.info("调度器已启动")

    # 启动状态打印任务
    status_task = asyncio.create_task(print_status())

    # 优雅关闭处理
    def signal_handler():
        logger.info("\n收到关闭信号，正在停止...")
        if status_task:
            status_task.cancel()
        if scheduler:
            asyncio.create_task(scheduler.stop())

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    # 保持运行
    try:
        while scheduler._running:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass

    logger.info("\nChronoForge 已停止")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("用户中断")
        sys.exit(0)
    except Exception as e:
        logger.error(f"运行失败: {e}")
        sys.exit(1)
```

## 运行命令

### 方式1: 直接运行主程序

```bash
# 确保创建数据目录
mkdir -p data

# 运行主程序
python run_chronoforge.py
```

### 方式2: 使用systemd服务（生产环境）

创建 `/etc/systemd/system/chronoforge.service`:

```ini
[Unit]
Description=ChronoForge Data Update Service
After=network.target

[Service]
Type=simple
User=your_user
WorkingDirectory=/path/to/ChronoForge
Environment="PATH=/path/to/ChronoForge/.venv/bin"
ExecStart=/path/to/ChronoForge/.venv/bin/python run_chronoforge.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

管理服务：

```bash
# 启动服务
sudo systemctl start chronoforge

# 查看状态
sudo systemctl status chronoforge

# 查看日志
sudo journalctl -u chronoforge -f

# 停止服务
sudo systemctl stop chronoforge
```

### 方式3: 使用Docker

创建 `Dockerfile`:

```dockerfile
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

RUN mkdir -p /app/data

CMD ["python", "run_chronoforge.py"]
```

运行：

```bash
# 构建镜像
docker build -t chronoforge .

# 运行容器
docker run -d \
  --name chronoforge \
  -v $(pwd)/data:/app/data \
  -e BINANCE_API_KEY=xxx \
  -e OKX_API_KEY=xxx \
  -e FRED_API_KEY=xxx \
  chronoforge
```

## 输出示例

```
============================================================
ChronoForge 数据更新任务
实现TODO.md中的6个数据更新任务
============================================================

步骤1: 初始化数据源管理器
配置数据源...
已配置 6 个数据源

步骤2: 初始化存储管理器
配置存储后端...
存储后端已配置

步骤3: 初始化数据更新管理器
交易对全集已加载: 2 个交易所

步骤4: 初始化任务调度器

步骤5: 配置定时任务
已添加任务1: Spot Tickers (每30秒)
已添加任务2: Spot OHLCV (每小时+30秒)
已添加任务3: Futures Metrics (每小时+30秒)
已添加任务4: CoinGecko Markets (每小时+30秒)
已添加任务5: FRED Macro Data (每小时+30秒)
已添加任务6: Global Crypto Market (每小时+30秒)

共配置 6 个定时任务

配置的任务列表:
  - spot_tickers: interval, 下次执行: 2026-02-11T10:30:00+00:00
  - spot_ohlcv: hourly, 下次执行: 2026-02-11T11:00:30+00:00
  - futures_metrics: hourly, 下次执行: 2026-02-11T11:00:30+00:00
  - coin_markets: hourly, 下次执行: 2026-02-11T11:00:30+00:00
  - macro_data: hourly, 下次执行: 2026-02-11T11:00:30+00:00
  - global_market: hourly, 下次执行: 2026-02-11T11:00:30+00:00

步骤6: 启动调度器
调度器已启动
```

## 验证运行

### 检查任务状态

```bash
# 运行集成测试
python test_integration.py --test
```

### 查看日志

```python
# 在运行程序时，实时查看日志
2026-02-11 10:30:00 - chronoforge.scheduler.task_scheduler - INFO - 任务1: 更新 Spot Tickers (Binance + OKX)
2026-02-11 10:30:00 - chronoforge.scheduler.data_update - INFO - Starting spot tickers update for exchanges: ['binance', 'okx']
2026-02-11 10:30:01 - __main__ - INFO -   binance: 150 条记录
2026-02-11 10:30:01 - __main__ - INFO -   okx: 120 条记录
```

### 查看数据库

```bash
# 使用 DuckDB CLI 查看数据
duckdb data/chronoforge.duckdb

# 查看所有表
.tables

# 查看tickers数据
SELECT * FROM tickers LIMIT 10;
```

## 任务与代码映射

| TODO任务 | 代码位置 | 关键方法 |
|---------|---------|---------|
| 任务1: Spot Tickers | `data_update.py` | `update_spot_tickers()` |
| 任务2: Spot OHLCV | `data_update.py` | `update_spot_ohlcv()` |
| 任务3: Futures Metrics | `data_update.py` | `update_futures_metrics()` |
| 任务4: CoinGecko Markets | `data_update.py` | `update_coin_markets()` |
| 任务5: FRED Macro Data | `data_update.py` | `update_macro_data()` |
| 任务6: Global Crypto Market | `data_update.py` | `update_global_crypto_market()` |

## 故障排除

### 问题1: 导入错误

```
ModuleNotFoundError: No module named 'croniter'
```

解决方案：
```bash
pip install croniter
```

### 问题2: API密钥错误

```
ValueError: Binance API key not set
```

解决方案：设置环境变量或修改配置文件：
```bash
export BINANCE_API_KEY="your_api_key"
export BINANCE_API_SECRET="your_api_secret"
```

### 问题3: 数据库路径错误

```
FileNotFoundError: [Errno 2] No such file or directory: './data/chronoforge.duckdb'
```

解决方案：创建数据目录
```bash
mkdir -p data
```

## 下一步

1. 查看详细文档: [docs/data_update_framework.md](docs/data_update_framework.md)
2. 查看API参考: 运行 `python -m pydoc chronoforge.scheduler`
3. 修改配置适应你的需求
