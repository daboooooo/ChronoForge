# DuckDB统一金融数据仓库使用指南

## 概述

`chronoforge.storage` 模块提供统一的金融数据存储解决方案，支持多种存储后端。

### 模块结构

```
chronoforge/storage/
├── __init__.py              # 主模块入口，导出所有存储组件
├── base.py                  # 存储抽象基类 (StorageBase)
├── manager.py               # 存储管理器 (StorageManager)
├── normalizer.py            # 数据标准化器 (DataFrameNormalizer)
├── design.md                # 设计文档
├── duckdb_storage/          # DuckDB存储实现
│   ├── __init__.py          # 导出组件
│   ├── adapter.py           # DUCKDBStorage 统一适配器
│   ├── warehouse.py         # FinancialDataWarehouse 核心仓库
│   ├── incremental.py       # SmartIncrementalManager 增量管理
│   └── quality.py           # DataValidator 数据验证
└── localfile_storage/       # 本地文件存储实现
    ├── __init__.py          # 导出组件
    ├── adapter.py           # LocalFileStorage 文件适配器
    ├── warehouse.py         # LocalDataWarehouse 本地仓库
    ├── incremental.py       # LocalIncrementalManager 增量管理
    └── quality.py           # LocalDataValidator 数据验证
```

### 核心组件

| 组件 | 职责 |
|------|------|
| `StorageBase` | 抽象基类，定义存储接口，包含缓存逻辑 |
| `DUCKDBStorage` | DuckDB统一存储适配器，兼容StorageBase接口 |
| `FinancialDataWarehouse` | DuckDB核心数据仓库，处理实际持久化 |
| `LocalFileStorage` | 本地文件存储适配器，支持CSV/JSON/Parquet |
| `LocalDataWarehouse` | 本地文件数据仓库 |
| `StorageManager` | 存储路由器，管理多存储实例和路由规则 |
| `DataFrameNormalizer` | 数据标准化器，统一列名映射和数据类型检测 |
| `SmartIncrementalManager` | 智能增量插入，避免重复数据 |
| `DataValidator` | 数据格式验证 |

## 快速开始

### 方式一：使用DuckDB存储适配器（推荐）

```python
from chronoforge.storage import DUCKDBStorage
import pandas as pd
from datetime import datetime, timezone

# 创建存储实例
storage = DUCKDBStorage({
    "db_path": "./my_warehouse.db",
    "read_only": False,
    "enable_cache": True,
    "cache_ttl": 3600,
    "cache_name": "ohlcv_cache"
})

# 初始化
await storage.initialize()

# 保存OHLCV数据
ohlcv_data = pd.DataFrame({
    'ts': [datetime(2024, 1, 1, tzinfo=timezone.utc)],
    'open': [42000.0],
    'high': [43000.0],
    'low': [41500.0],
    'close': [42500.0],
    'volume': [1000.0]
})

await storage.save(
    "BTC_USDT_1d",
    ohlcv_data,
    metadata={
        'exchange': 'binance',
        'market_type': 'spot',
        'symbol': 'BTC/USDT',
        'timeframe': '1d'
    }
)

# 加载数据
loaded_data = await storage.load("BTC_USDT_1d")

# 健康检查
health = await storage.health_check()

# 关闭连接
await storage.close()
```

### 方式二：使用本地文件存储

```python
from chronoforge.storage import LocalFileStorage
import pandas as pd

# 创建存储实例
storage = LocalFileStorage({
    "data_dir": "./data",
    "file_format": "parquet",  # 可选: csv, json, parquet
    "enable_cache": True
})

# 初始化
await storage.initialize()

# 保存OHLCV数据
await storage.save(
    "BTC_USDT_1d",
    ohlcv_data,
    metadata={
        'exchange': 'binance',
        'market_type': 'spot',
        'symbol': 'BTC/USDT',
        'timeframe': '1d'
    }
)

# 加载数据
loaded_data = await storage.load("BTC_USDT_1d")

# 获取统计
stats = await storage.get_stats()
```

### 方式三：使用异步上下文管理器

```python
async with DUCKDBStorage({
    "db_path": "./warehouse.db",
    "read_only": False,
    "enable_cache": True,
    "cache_ttl": 3600,
    "cache_name": "ohlcv_cache"
}) as storage:
    # 保存数据
    await storage.save("BTC_USDT", ohlcv_data, metadata={})
    
    # 加载数据
    data = await storage.load("BTC_USDT", metadata={})
    
    # 获取统计
    stats = await storage.get_stats()
```

### 方式四：通过存储管理器

```python
from chronoforge.storage.manager import storage_manager
from chronoforge.storage import DUCKDBStorage

# 注册存储
storage_manager.register_storage('duckdb', DUCKDBStorage({"db_path": "./data.db"}))

# 设置路由规则
storage_manager.set_storage_route('ohlcv', 'spot', 'duckdb')

# 通过管理器保存
await storage_manager.save(
    "BTC_USDT_1d",
    ohlcv_data,
    metadata={'exchange': 'binance'},
    task_type='ohlcv',
    data_type='spot'
)

# 通过管理器加载
data = await storage_manager.load("BTC_USDT_1d", task_type='ohlcv')
```

## 数据表结构

### 1. Symbols表（维度表）

```sql
CREATE TABLE IF NOT EXISTS symbols (
    symbol        TEXT PRIMARY KEY,
    exchange      TEXT NOT NULL,
    market_type   TEXT NOT NULL,
    base_asset    TEXT NOT NULL,
    quote_asset   TEXT,
    active        BOOLEAN DEFAULT TRUE,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)

-- 唯一约束
CREATE UNIQUE INDEX IF NOT EXISTS idx_symbols_unique
ON symbols (exchange, symbol, market_type)
```

### 2. OHLCV表（事实表）

```sql
CREATE TABLE IF NOT EXISTS ohlcv (
    exchange      TEXT NOT NULL,
    market_type   TEXT NOT NULL,
    symbol        TEXT NOT NULL,
    timeframe     TEXT NOT NULL,
    ts            TIMESTAMP NOT NULL,
    open          DOUBLE NOT NULL,
    high          DOUBLE NOT NULL,
    low           DOUBLE NOT NULL,
    close         DOUBLE NOT NULL,
    volume        DOUBLE NOT NULL,
    quote_volume  DOUBLE,
    source        TEXT,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (exchange, market_type, symbol, timeframe, ts)
)
```

### 3. Tickers表（市场快照）

```sql
CREATE TABLE IF NOT EXISTS tickers (
    exchange      TEXT NOT NULL,
    symbol        TEXT NOT NULL,
    quote_asset   TEXT,
    ts            TIMESTAMP NOT NULL,
    price         DOUBLE,
    price_change_24h DOUBLE,
    price_change_pct_24h DOUBLE,
    volume        DOUBLE,
    quote_volume  DOUBLE,
    high_24h      DOUBLE,
    low_24h       DOUBLE,
    market_type   TEXT DEFAULT 'spot',
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (exchange, symbol, quote_asset, ts)
)
```

### 4. Futures Metrics表（期货指标）

```sql
CREATE TABLE IF NOT EXISTS futures_metrics (
    exchange      TEXT NOT NULL,
    symbol        TEXT NOT NULL,
    ts            TIMESTAMP NOT NULL,
    funding_rate  DOUBLE,
    open_interest DOUBLE,
    oi_value      DOUBLE,
    taker_long_short_ratio DOUBLE,
    top_long_short_position_ratio DOUBLE,
    top_long_short_account_ratio DOUBLE,
    global_long_short_account_ratio DOUBLE,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (exchange, symbol, ts)
)
```

### 5. BTC Fear & Greed Index表

```sql
CREATE TABLE IF NOT EXISTS btc_fgi (
    ts       TIMESTAMP NOT NULL PRIMARY KEY,
    value    INTEGER NOT NULL CHECK (value >= 0 AND value <= 100),
    label    TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
```

### 6. 宏观经济数据表

```sql
CREATE TABLE IF NOT EXISTS macro_fred (
    series_id    TEXT NOT NULL,
    symbol       TEXT NOT NULL,
    ts           TIMESTAMP NOT NULL,
    value        DOUBLE,
    frequency    TEXT,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (series_id, ts)
)
```

### 7. 币种分类表

```sql
CREATE TABLE IF NOT EXISTS coin_categories (
    symbol          TEXT NOT NULL,
    category        TEXT NOT NULL,
    market_cap_rank INTEGER,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, category)
)
```

### 8. Coin Markets表（CoinGecko市场数据）

```sql
CREATE TABLE IF NOT EXISTS coin_markets (
    id                  TEXT NOT NULL,
    symbol              TEXT NOT NULL,
    name                TEXT NOT NULL,
    ts                  TIMESTAMP NOT NULL,
    current_price       DOUBLE,
    market_cap          DOUBLE,
    market_cap_rank     INTEGER,
    total_volume        DOUBLE,
    high_24h            DOUBLE,
    low_24h             DOUBLE,
    price_change_24h    DOUBLE,
    price_change_pct_24h DOUBLE,
    market_cap_change_24h DOUBLE,
    circulating_supply  DOUBLE,
    total_supply        DOUBLE,
    max_supply          DOUBLE,
    image_id            TEXT,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, ts)
)
```

## 存储接口

### StorageBase 公共方法

所有存储实现都继承以下方法：

| 方法 | 说明 |
|------|------|
| `save(id, data, sub, metadata)` | 保存数据（自动缓存） |
| `load(id, sub, metadata)` | 加载数据（尝试缓存） |
| `delete(id, sub, metadata)` | 删除数据（清除缓存） |
| `exists(id, sub, metadata)` | 检查数据是否存在 |
| `lists(sub)` | 列出所有数据 |
| `get_time_range(id, sub)` | 获取数据时间范围 |
| `get_metadata(id, sub)` | 获取元信息 |
| `update_metadata(id, metadata, sub)` | 更新元信息 |

### DUCKDBStorage 特有方法

| 方法 | 说明 |
|------|------|
| `health_check()` | 健康检查 |
| `get_stats()` | 获取数据库统计 |
| `check_data_integrity()` | 检查数据完整性 |
| `get_warehouse()` | 获取底层仓库实例 |

### LocalFileStorage 特有方法

| 方法 | 说明 |
|------|------|
| `health_check()` | 健康检查（检查目录权限） |
| `get_stats()` | 获取文件统计（数量、大小） |
| `check_data_integrity()` | 检查数据完整性 |
| `get_warehouse()` | 获取底层仓库实例 |

## 数据标准化器

`DataFrameNormalizer` 提供统一的数据标准化功能，用于消除存储适配器中的重复逻辑。

### 列名映射

```python
from chronoforge.storage import DataFrameNormalizer

# OHLCV列名映射
DataFrameNormalizer.OHLCV_COLUMN_MAPPING = {
    'time': 'ts',
    'timestamp': 'ts',
    'datetime': 'ts',
    'open': 'open',
    'high': 'high',
    'low': 'low',
    'close': 'close',
    'volume': 'volume',
    'quote_volume': 'quote_volume',
    'amount': 'quote_volume',
    'quoteAmount': 'quote_volume',
}

# Tickers列名映射
DataFrameNormalizer.TICKERS_COLUMN_MAPPING = {
    'time': 'ts',
    'timestamp': 'ts',
    'last_price': 'last',
    'price': 'last',
    'baseVolume': 'base_volume',
    'quoteVolume': 'quote_volume',
}
```

### 自动检测数据类型

```python
from chronoforge.storage import DataFrameNormalizer

# 自动检测数据类型
data_type = DataFrameNormalizer.detect_data_type(data, id, sub, metadata)
# 返回: 'ohlcv', 'tickers', 'futures_metrics', 'btc_fgi', 'macro_fred', 'coin_categories', 'coin_markets', 'symbols'
```

### 数据标准化

```python
# 标准化OHLCV数据
normalized_data = DataFrameNormalizer.normalize_ohlcv(data, id, metadata)

# 标准化Tickers数据
normalized_data = DataFrameNormalizer.normalize_tickers(data, id, metadata)

# 标准化期货指标数据
normalized_data = DataFrameNormalizer.normalize_futures_metrics(data, id, metadata)

# 标准化BTC恐惧贪婪指数
normalized_data = DataFrameNormalizer.normalize_btc_fgi(data, id, metadata)

# 标准化宏观数据
normalized_data = DataFrameNormalizer.normalize_macro_fred(data, id, metadata)
```

## 高级功能

### 智能增量插入

```python
from chronoforge.storage import SmartIncrementalManager

manager = SmartIncrementalManager(storage)

# 增量插入，自动检测重复
result = await manager.smart_insert_ohlcv(
    ohlcv_data,
    validation=True,
    incremental=True,
    batch_size=10000
)

print(f"插入: {result['inserted']}, 跳过: {result['skipped']}")
```

### 获取最新时间戳

```python
# 获取最新时间戳
latest_ts = await manager.get_latest_timestamp(
    table_name='ohlcv',
    symbol='BTC/USDT',
    timeframe='1d',
    exchange='binance'
)

# 获取数据更新建议
suggestion = await manager.suggest_data_fetch_range(
    symbol='BTC/USDT',
    timeframe='1d',
    exchange='binance'
)
# 返回: {'status': 'needs_update', 'suggested_start': ..., 'suggested_end': ...}
```

### 数据验证

```python
from chronoforge.storage import DataValidator

validator = DataValidator()

# 验证OHLCV数据
result = validator.validate_data(ohlcv_df, 'ohlcv')

if result['valid']:
    print("数据验证通过")
else:
    print(f"错误: {result['errors']}")
    print(f"警告: {result['warnings']}")
```

### 数据质量监控

```python
from chronoforge.storage import DataQualityMonitor

monitor = DataQualityMonitor()

# 监控数据质量
report = monitor.monitor_data_quality(
    data=ohlcv_df,
    data_type='ohlcv',
    source='binance_api'
)

print(f"质量分数: {report['overall_quality_score']}")

# 获取质量趋势
trends = monitor.get_quality_trends(days=7)
```

## 路由配置

### 注册多个存储

```python
from chronoforge.storage.manager import storage_manager
from chronoforge.storage import DUCKDBStorage, LocalFileStorage

# 注册DuckDB存储
duckdb = DUCKDBStorage({"db_path": "./primary.db"})
storage_manager.register_storage('duckdb_primary', duckdb)

# 注册本地文件存储
local = LocalFileStorage({"data_dir": "./data", "file_format": "parquet"})
storage_manager.register_storage('local_file', local)

# 注册所有内部存储
storage_manager.register_all_internal_storages()
```

### 配置路由规则

```python
# OHLCV数据走DuckDB
storage_manager.set_storage_route('ohlcv', 'spot', 'duckdb_primary')
storage_manager.set_storage_route('ohlcv', 'futures', 'duckdb_primary')

# 备份数据走本地文件
storage_manager.set_storage_route('backup', 'daily', 'local_file')

# 设置默认存储
storage_manager.set_default_storage('duckdb_primary')
```

### 获取存储实例

```python
# 根据路由获取
storage = storage_manager.get_storage(
    task_type='ohlcv',
    data_type='spot'
)

# 获取默认存储
default_storage = storage_manager.get_storage()
```

## 缓存配置

```python
# 禁用缓存
storage = DUCKDBStorage({
    "db_path": "./data.db",
    "enable_cache": False
})

# 自定义缓存
storage = DUCKDBStorage({
    "db_path": "./data.db",
    "enable_cache": True,
    "cache_ttl": 7200,        # 缓存时间（秒）
    "cache_name": 'my_cache'  # 缓存名称
})
```

## 本地文件存储配置

```python
# CSV格式（便于查看和编辑）
storage = LocalFileStorage({
    "data_dir": "./data",
    "file_format": "csv"
})

# JSON格式（结构化数据）
storage = LocalFileStorage({
    "data_dir": "./data",
    "file_format": "json"
})

# Parquet格式（高效查询，推荐）
storage = LocalFileStorage({
    "data_dir": "./data",
    "file_format": "parquet"
})
```

## 最佳实践

### 1. 使用统一适配器

```python
# 推荐：直接使用 DUCKDBStorage
storage = DUCKDBStorage({"db_path": "./warehouse.db"})
await storage.initialize()
```

### 2. 正确关闭连接

```python
# 方式一：使用try-finally
storage = DUCKDBStorage({"db_path": "./data.db"})
await storage.initialize()
try:
    await storage.save("BTC_USDT", data, metadata={})
finally:
    await storage.close()

# 方式二：使用上下文管理器（推荐）
async with DUCKDBStorage({"db_path": "./data.db"}) as storage:
    await storage.save("BTC_USDT", data, metadata={})
```

### 3. 批量操作

```python
# 大批量数据分批处理
batch_size = 10000
for i in range(0, len(large_dataset), batch_size):
    batch = large_dataset[i:i + batch_size]
    await storage.save(f"batch_{i}", batch, metadata={})
```

### 4. 使用元数据

```python
await storage.save(
    "BTC_USDT_1d",
    ohlcv_data,
    metadata={
        'exchange': 'binance',
        'market_type': 'spot',
        'symbol': 'BTC/USDT',
        'timeframe': '1d',
        'source': 'ccxt',
        'version': '1.0'
    }
)
```

## 错误处理

```python
try:
    result = await storage.save("BTC_USDT", data, metadata={})
    if not result:
        logger.error("数据保存失败")
        
except Exception as e:
    logger.error(f"操作失败: {str(e)}")
    
    # 数据验证错误
    if "validation" in str(e).lower():
        logger.error("数据格式错误")
    
    # 数据库连接错误
    elif "connection" in str(e).lower():
        logger.error("数据库连接失败")
```

## 性能优化

### 自动创建的索引

```sql
-- OHLCV查询优化
CREATE INDEX idx_ohlcv_symbol ON ohlcv (symbol, exchange, timeframe);
CREATE INDEX idx_ohlcv_ts ON ohlcv (ts DESC);

-- Tickers查询优化
CREATE INDEX idx_tickers_symbol ON tickers (symbol, exchange);
CREATE INDEX idx_tickers_ts ON tickers (ts DESC);

-- Futures查询优化
CREATE INDEX idx_futures_symbol ON futures_metrics (symbol, exchange);
CREATE INDEX idx_futures_ts ON futures_metrics (ts DESC);
```

### 连接池配置

```python
# 大量并发操作时，复用连接
warehouse = await storage.get_warehouse()
conn = warehouse._get_connection()
```

## 模块导出

```python
from chronoforge.storage import (
    # DuckDB存储核心组件
    FinancialDataWarehouse,
    DUCKDBStorage,
    SmartIncrementalManager,
    DataValidator,
    DataQualityMonitor,

    # 本地文件存储
    LocalFileStorage,

    # 基础接口（向后兼容）
    StorageBase,
    verify_storage_instance,

    # 数据标准化器
    DataFrameNormalizer
)
```
