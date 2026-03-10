# 使用方法文档

## 1. 安装指南

ChronoForge 提供了多种安装方式，您可以根据自己的需求选择合适的方法。

### 1.1 从源码安装

如果您下载了源代码，可以使用以下命令安装：

```bash
# 从源码安装（开发模式）
pip install -e .

# 或使用 requirements.txt 安装依赖
pip install -r requirements.txt
```

### 1.2 从PyPI安装（未来支持）

```bash
# 从PyPI安装（未来支持）
# pip install chronoforge
```

### 1.3 系统要求

- Python 3.8 或更高版本
- 推荐使用虚拟环境
- 对于特定存储插件，需要安装相应的依赖：
  - DuckDB存储：`pip install duckdb`
  - Redis存储：`pip install redis`

## 2. 快速开始

### 2.1 嵌入模式

以下是一个简单的示例，展示如何在嵌入模式下使用 ChronoForge 获取加密货币数据：

```python
import asyncio
from chronoforge.scheduler import Scheduler
from chronoforge.utils import TimeSlot, TimeRange
from chronoforge.task_manager_ultimate import ultimate_task_manager, Task

async def main():
    # 创建调度器
    scheduler = Scheduler()

    # 定义时间槽（每小时30分到59分执行）
    time_slot = TimeSlot(start="00:30", end="59:00")

    # 定义时间范围（从2024年1月1日到现在）
    timerange = TimeRange.parse_timerange("20240101-")

    # 创建任务对象
    task = Task(
        name="btc_spot_data",
        data_source_name="CryptoSpotDataSource",
        storage_name="DUCKDBStorage",
        time_slot=time_slot,
        symbols=["binance:BTC/USDT"],
        timeframe="1h",
        timerange=timerange,
        data_source_config={},
        storage_config={"db_path": "./data/chronoforge.db"}
    )

    # 添加任务
    ultimate_task_manager.add_task(name="btc_spot_data", task=task)

    # 启动调度器（异步方法，需要 await）
    await scheduler.start()

    # 运行5秒后停止
    await asyncio.sleep(5)

    # 查看任务状态
    task_state = ultimate_task_manager.task_states.get("btc_spot_data", {})
    print(f"任务状态: {task_state.get('status', 'unknown')}")

    # 停止调度器（异步方法，需要 await）
    await scheduler.stop()

# 运行
asyncio.run(main())
```

> ⚠️ **注意**：上述示例使用时间槽调度，任务仅在每天的 00:30-59:59 执行。如果需要立即获取数据（用于测试），可以使用立即执行模式（见下文）。

### 2.1.1 立即执行模式（用于测试）

如果您需要立即获取数据而不等待时间槽，可以使用 `run_task_now` 方法：

```python
import asyncio
from chronoforge.scheduler import Scheduler
from chronoforge.utils import TimeSlot, TimeRange
from chronoforge.task_manager_ultimate import ultimate_task_manager, Task

async def main():
    scheduler = Scheduler()

    timerange = TimeRange.parse_timerange("20240101-")

    task = Task(
        name="btc_spot_data",
        data_source_name="CryptoSpotDataSource",
        storage_name="DUCKDBStorage",
        time_slot=TimeSlot(start="00:30", end="59:00"),
        symbols=["binance:BTC/USDT"],
        timeframe="1h",
        timerange=timerange,
        data_source_config={},
        storage_config={"db_path": "./data/chronoforge.db"}
    )

    ultimate_task_manager.add_task(name="btc_spot_data", task=task)

    await scheduler.start()

    # 立即执行任务（跳过时间槽限制）
    result = await scheduler.run_task_now("btc_spot_data")
    print(f"执行结果: {result}")

    # 获取数据
    storage = ultimate_task_manager.storage_instances.get("btc_spot_data")
    if storage:
        task = ultimate_task_manager.tasks.get("btc_spot_data")
        data_name = f"{task.symbols[0]}_{task.timeframe}"
        data = await storage.load(id=data_name, sub=task.sub)
        if data is not None:
            print(f"数据形状: {data.shape}")
            print(data.head())

    await scheduler.stop()

asyncio.run(main())
```

### 2.2 自运行模式

ChronoForge 还支持作为独立服务运行，通过 RESTful API 提供访问。

#### 2.2.1 启动服务

##### 2.2.1.1 安装包后启动服务

如果您已经通过 `pip install -e .` 或 `pip install chronoforge` 安装了 ChronoForge，可以直接使用 `chronoforge` 命令启动服务：

```bash
# 基本用法（默认主机：127.0.0.1，默认端口：8000）
chronoforge serve

# 自定义主机和端口
chronoforge serve --host 0.0.0.0 --port 8000

# 开发模式（代码修改时自动重载）
chronoforge serve --reload

# 指定工作进程数
chronoforge serve --workers 4
```

##### 2.2.1.2 下载源代码后启动服务

如果您下载了源代码但尚未安装，可以使用以下方式启动服务：

```bash
# 使用 python -m 方式启动服务
python -m chronoforge.cli serve --host 0.0.0.0 --port 8000

# 或直接运行 cli.py 文件
python chronoforge/cli.py serve --host 0.0.0.0 --port 8000
```

##### 2.2.1.3 服务启动参数

| 参数 | 描述 | 默认值 |
|------|------|--------|
| `--host` | 服务绑定的主机地址 | `127.0.0.1` |
| `--port` | 服务绑定的端口 | `8000` |
| `--reload` | 开发模式，代码修改时自动重载 | `False` |
| `--workers` | 工作进程数 | `1` |

## 3. 配置指南

### 3.1 数据源配置

#### 3.1.1 CryptoSpotDataSource 配置

| 配置项 | 类型 | 描述 | 是否必需 | 默认值 |
|--------|------|------|----------|--------|
| `api_key` | str | 交易所API密钥 | 否 | `""` |
| `api_secret` | str | 交易所API密钥密码 | 否 | `""` |
| `api_passphrase` | str | 交易所API密码短语（如OKX） | 否 | `""` |
| `rate_limit` | int | API请求速率限制 | 否 | `10` |

#### 3.1.2 FREDDataSource 配置

| 配置项 | 类型 | 描述 | 是否必需 | 默认值 |
|--------|------|------|----------|--------|
| `api_key` | str | FRED API密钥 | 是 | - |

### 3.1.3 CryptoUMFutureDataSource 配置

| 配置项 | 类型 | 描述 | 是否必需 | 默认值 |
|--------|------|------|----------|--------|
| `api_key` | str | 交易所API密钥 | 否 | `""` |
| `api_secret` | str | 交易所API密钥密码 | 否 | `""` |
| `api_passphrase` | str | 交易所API密码短语（如OKX） | 否 | `""` |
| `rate_limit` | int | API请求速率限制 | 否 | `10` |
| `default_type` | str | 默认合约类型 | 否 | `"swap"` |

#### 3.1.4 GlobalMarketDataSource 配置

| 配置项 | 类型 | 描述 | 是否必需 | 默认值 |
|--------|------|------|----------|--------|
| `api_key` | str | 数据源API密钥 | 否 | `""` |

#### 3.1.5 CoinGeckoDataSource 配置

| 配置项 | 类型 | 描述 | 是否必需 | 默认值 |
|--------|------|------|----------|--------|
| `api_key` | str | CoinGecko API密钥（可选，免费版不需要） | 否 | `""` |

#### 3.1.6 AlthernativeDataSource 配置

| 配置项 | 类型 | 描述 | 是否必需 | 默认值 |
|--------|------|------|----------|--------|
| `api_key` | str | Althernative.me API密钥 | 是 | - |

### 3.2 存储配置

#### 3.2.1 LocalFileStorage 配置

| 配置项 | 类型 | 描述 | 是否必需 | 默认值 |
|--------|------|------|----------|--------|
| `base_path` | str | 数据存储的基础路径 | 否 | `"./data"` |

#### 3.2.2 DUCKDBStorage 配置

| 配置项 | 类型 | 描述 | 是否必需 | 默认值 |
|--------|------|------|----------|--------|
| `db_path` | str | DuckDB数据库文件路径 | 否 | `"./data/chronoforge.db"` |
| `memory_limit` | str | DuckDB内存限制 | 否 | `"2GB"` |
| `threads` | int | 并行线程数 | 否 | `4` |
| `checkpoint_threshold` | str | 自动检查点阈值 | 否 | `"100GB"` |

#### 3.2.2.1 DuckDB 存储高级特性

DUCKDBStorage 是 ChronoForge 推荐的生产环境存储方案，提供以下高级特性：

1. **OLAP 分析能力**：支持复杂的 SQL 查询和分析
2. **列式存储**：高效的列式存储格式，压缩率高
3. **零拷贝读取**：高效的读性能
4. **ACID 事务**：确保数据完整性

```python
# DuckDB 高级配置示例
storage_config = {
    "db_path": "./data/financial_data.db",
    "memory_limit": "8GB",
    "threads": 8,
    "checkpoint_threshold": "50GB"
}
```

#### 3.2.2.2 DuckDB 数据表结构

ChronoForge 使用 DuckDB 存储时，会自动创建以下数据表：

- `ohlcv`: K线数据表（时间、开盘、最高、最低、收盘、成交量）
- `metrics`: 指标数据表（时间、指标名称、值）
- `markets`: 市场数据表（时间、市场信息、值）

#### 3.2.2.3 DuckDB 查询示例

```python
import asyncio
from chronoforge.storage import DUCKDBStorage

async def query_data():
    storage = DUCKDBStorage({"db_path": "./data/financial_data.db"})
    
    # 使用 DuckDB 原生 SQL 查询
    result = await storage.execute("""
        SELECT * FROM ohlcv 
        WHERE symbol = 'BTC/USDT' 
        AND time >= '2024-01-01' 
        ORDER BY time DESC 
        LIMIT 100
    """)
    print(result)
    
asyncio.run(query_data())
```

#### 3.2.3 RedisStorage 配置

| 配置项 | 类型 | 描述 | 是否必需 | 默认值 |
|--------|------|------|----------|--------|
| `host` | str | Redis服务器主机地址 | 否 | `"localhost"` |
| `port` | int | Redis服务器端口 | 否 | `6379` |
| `db` | int | Redis数据库编号 | 否 | `0` |
| `password` | str | Redis服务器密码 | 否 | `None` |
| `socket_timeout` | int | Socket超时时间（秒） | 否 | `5` |
| `socket_connect_timeout` | int | Socket连接超时时间（秒） | 否 | `5` |
| `max_connections` | int | 最大连接数 | 否 | `10` |

#### 3.2.3.1 Redis 存储配置示例

```python
# Redis 存储配置示例
storage_config = {
    "host": "localhost",
    "port": 6379,
    "db": 0,
    "password": "your_password",  # 如果需要认证
    "socket_timeout": 10,
    "max_connections": 20
}
```

#### 3.2.3.2 Redis 数据结构

ChronoForge 使用 Redis 的以下数据结构存储数据：

- **Hash**: 存储 OHLCV 数据，键名为 `ohlcv:{symbol}:{timeframe}`
- **Sorted Set**: 存储时间序列索引，用于快速范围查询
- **String**: 存储元数据和配置信息

#### 3.2.3.3 Redis 使用场景

Redis 存储适合以下场景：
- 需要极高读写性能的场景
- 实时数据处理和流式计算
- 缓存热点数据
- 分布式部署场景

## 4. 任务管理

### 4.1 创建任务

#### 4.1.1 使用代码创建任务

```python
from chronoforge.scheduler import Scheduler
from chronoforge.utils import TimeSlot, TimeRange
from chronoforge.task_manager_ultimate import ultimate_task_manager, Task

# 创建调度器
scheduler = Scheduler()

# 定义时间槽
time_slot = TimeSlot(start="00:00", end="23:59")

# 定义时间范围
timerange = TimeRange.parse_timerange("20240101-")

# 创建任务对象
task = Task(
    name="crypto_data",
    data_source_name="CryptoSpotDataSource",
    data_source_config={"api_key": "your_key", "api_secret": "your_secret"},
    storage_name="DUCKDBStorage",
    storage_config={"db_path": "./crypto_data.db"},
    time_slot=time_slot,
    symbols=["binance:BTC/USDT", "binance:ETH/USDT"],
    timeframe="1d",
    timerange=timerange
)

# 添加任务
ultimate_task_manager.add_task(name="crypto_data", task=task)
```

#### 4.1.2 使用API创建任务

```bash
curl -X POST http://localhost:8000/api/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "name": "test_task",
    "data_source_name": "CryptoSpotDataSource",
    "data_source_config": {},
    "storage_name": "LocalFileStorage",
    "storage_config": {},
    "time_slot": {
      "start": "00:00",
      "end": "23:59"
    },
    "symbols": ["binance:BTC/USDT"],
    "timeframe": "1d",
    "timerange_str": "20240101-"
  }'
```

### 4.2 启动任务

#### 4.2.1 使用代码启动任务

```python
# 启动调度器（异步方法，需要 await）
await scheduler.start()

# 运行一段时间后停止
import asyncio
await asyncio.sleep(10)
await scheduler.stop()
```

#### 4.2.2 使用API启动任务

```bash
curl -X POST http://localhost:8000/api/tasks/{task_name}/start
```

### 4.3 停止任务

#### 4.3.1 使用代码停止任务

```python
# 停止调度器（异步方法，需要 await）
await scheduler.stop()
```

#### 4.3.2 使用API停止任务

```bash
curl -X POST http://localhost:8000/api/tasks/{task_name}/stop
```

### 4.4 删除任务

#### 4.4.1 使用代码删除任务

```python
scheduler.delete_task("crypto_data")
```

#### 4.4.2 使用API删除任务

```bash
curl -X DELETE http://localhost:8000/api/tasks/{task_name}
```

### 4.5 查看任务状态

#### 4.5.1 使用代码查看任务状态

```python
from chronoforge.task_manager_ultimate import ultimate_task_manager

task_state = ultimate_task_manager.task_states.get("crypto_data", {})
print(f"任务状态: {task_state.get('status', 'idle')}")
```

#### 4.5.2 使用API查看任务状态

```bash
curl http://localhost:8000/api/tasks/{task_name}/status
```

## 5. 数据管理

### 5.1 获取任务数据

#### 5.1.1 使用代码获取数据

```python
import asyncio
from chronoforge.task_manager_ultimate import ultimate_task_manager

async def get_data():
    task_name = "crypto_data"
    
    # 获取任务对应的存储实例
    storage = ultimate_task_manager.storage_instances.get(task_name)
    if not storage:
        print("存储实例未找到")
        return
    
    # 获取数据
    task = ultimate_task_manager.tasks.get(task_name)
    if not task:
        print("任务未找到")
        return
    
    # 遍历任务的所有symbols
    for symbol in task.symbols:
        # 构建数据名称
        data_name = f"{symbol}_{task.timeframe}"
        
        # 从存储中加载数据
        data = await storage.load(id=data_name, sub=task.sub)
        if data is not None:
            print(f"{symbol} 数据形状: {data.shape}")
            print(f"时间范围: {data['time'].min()} 到 {data['time'].max()}")

# 运行
asyncio.run(get_data())
```

#### 5.1.2 使用API获取数据

```bash
# 获取任务下的所有数据信息
curl http://localhost:8000/api/tasks/{task_name}/data_info

# 获取特定数据
curl "http://localhost:8000/api/tasks/{task_name}/data?symbol=binance:BTC/USDT"

# 获取指定时间范围的数据
curl "http://localhost:8000/api/tasks/{task_name}/data?symbol=binance:BTC/USDT&start_time=2024-01-01&end_time=2024-01-31"
```

### 5.2 数据格式

ChronoForge 获取的数据格式为 pandas DataFrame，包含以下列：

- `time`：时间戳（datetime类型）
- `open`：开盘价（适用于加密货币等）
- `high`：最高价（适用于加密货币等）
- `low`：最低价（适用于加密货币等）
- `close`：收盘价（适用于加密货币等）
- `volume`：成交量（适用于加密货币等）
- `value`：值（适用于FRED等经济数据）

具体列名可能因数据源而异。

## 6. 插件管理

### 6.1 列出支持的插件

#### 6.1.1 使用代码列出插件

```python
from chronoforge.scheduler import Scheduler

scheduler = Scheduler()

# 列出所有支持的数据源插件
print("支持的数据源插件:")
print(scheduler.list_supported_plugins("data_source"))

# 列出所有支持的存储插件
print("支持的存储插件:")
print(scheduler.list_supported_plugins("storage"))
```

#### 6.1.2 使用API列出插件

```bash
# 列出所有支持的插件
curl http://localhost:8000/api/plugins

# 按类型列出插件
curl http://localhost:8000/api/plugins/data_source
curl http://localhost:8000/api/plugins/storage
```

### 6.2 开发自定义插件

#### 6.2.1 开发自定义数据源插件

要开发自定义数据源插件，需要继承 `DataSourceBase` 抽象基类并实现必要的方法：

```python
from chronoforge.data_source import DataSourceBase
import pandas as pd
from datetime import datetime

class CustomDataSource(DataSourceBase):
    def __init__(self, config=None):
        super().__init__(config)
    
    @property
    def name(self):
        return "CustomDataSource"
    
    async def fetch(self, symbol, timeframe, start_ts_ms, end_ts_ms=None):
        # 实现从您的数据源获取数据的逻辑
        # 这里是一个示例实现
        
        # 生成示例数据
        start_date = datetime.fromtimestamp(start_ts_ms / 1000)
        end_date = datetime.fromtimestamp(end_ts_ms / 1000) if end_ts_ms else datetime.now()
        
        # 生成时间序列
        date_range = pd.date_range(start=start_date, end=end_date, freq=timeframe)
        
        # 生成示例数据
        data = {
            'time': date_range,
            'value': range(len(date_range))
        }
        
        return pd.DataFrame(data)
```

#### 6.2.2 开发自定义存储插件

要开发自定义存储插件，需要继承 `StorageBase` 抽象基类并实现所有必要的方法：

```python
from chronoforge.storage import StorageBase
import pandas as pd
import os

class CustomStorage(StorageBase):
    def __init__(self, config=None):
        super().__init__(config)
        self.base_path = self.config.get("base_path", "./data/custom")
        os.makedirs(self.base_path, exist_ok=True)
    
    @property
    def name(self):
        return "CustomStorage"
    
    async def save(self, id, data, sub=None):
        # 实现保存数据的逻辑
        # 这里是一个示例实现
        path = os.path.join(self.base_path, sub) if sub else self.base_path
        os.makedirs(path, exist_ok=True)
        
        file_path = os.path.join(path, f"{id}.csv")
        data.to_csv(file_path, index=False)
        return True
    
    async def load(self, id, sub=None):
        # 实现加载数据的逻辑
        # 这里是一个示例实现
        path = os.path.join(self.base_path, sub) if sub else self.base_path
        file_path = os.path.join(path, f"{id}.csv")
        
        if not os.path.exists(file_path):
            return None
        
        return pd.read_csv(file_path, parse_dates=['time'])
    
    async def delete(self, id, sub=None):
        # 实现删除数据的逻辑
        # 这里是一个示例实现
        path = os.path.join(self.base_path, sub) if sub else self.base_path
        file_path = os.path.join(path, f"{id}.csv")
        
        if os.path.exists(file_path):
            os.remove(file_path)
            return True
        return False
    
    async def exists(self, id, sub=None):
        # 实现检查数据是否存在的逻辑
        # 这里是一个示例实现
        path = os.path.join(self.base_path, sub) if sub else self.base_path
        file_path = os.path.join(path, f"{id}.csv")
        return os.path.exists(file_path)
    
    async def lists(self, sub=None):
        # 实现列出所有数据的逻辑
        # 这里是一个示例实现
        path = os.path.join(self.base_path, sub) if sub else self.base_path
        data_info = []
        
        if os.path.exists(path):
            for file in os.listdir(path):
                if file.endswith('.csv'):
                    data_id = file[:-4]
                    data_info.append({"id": data_id})
        
        return data_info
    
    async def get_time_range(self, id, sub=None):
        # 实现获取数据时间范围的逻辑
        # 这里是一个示例实现
        data = await self.load(id, sub)
        if data is None or data.empty:
            return None
        
        return {
            "start_time": data['time'].min(),
            "end_time": data['time'].max()
        }
```

#### 6.2.3 注册自定义插件

```python
from chronoforge.scheduler import Scheduler
from custom_plugins import CustomDataSource, CustomStorage

# 创建调度器
scheduler = Scheduler()

# 注册自定义数据源插件
scheduler.register_plugin(CustomDataSource)

# 注册自定义存储插件
scheduler.register_plugin(CustomStorage)

# 现在可以使用自定义插件了
print("支持的数据源插件:")
print(scheduler.list_supported_plugins("data_source"))

print("支持的存储插件:")
print(scheduler.list_supported_plugins("storage"))
```

## 7. 高级功能

### 7.1 时间槽配置

TimeSlot 类支持多种配置方式：

```python
from chronoforge.utils import TimeSlot

# 方式1：使用时间字符串（HH:MM 格式 - 每天定时执行）
slot1 = TimeSlot(start="00:00", end="23:59")  # 全天

# 方式2：使用时间字符串（HH:MM:SS 格式 - 每天定时执行）
slot2 = TimeSlot(start="09:00:00", end="17:00:00")  # 每天9点到17点

# 方式3：使用时间字符串（MM:SS 格式 - 每小时内的分钟段）
slot3 = TimeSlot(start="30:00", end="59:59")  # 每小时的30分到59分

# 方式4：精确时间点（开始和结束相同）
slot4 = TimeSlot(start="00:00", end="00:00")  # 每小时整点
```

### 7.2 任务依赖管理

ChronoForge 支持通过时间槽配置来管理任务之间的依赖关系：

```python
from chronoforge.scheduler import Scheduler
from chronoforge.utils import TimeSlot

# 创建调度器
scheduler = Scheduler()

# 定义时间槽
# 任务1：每天00:00执行
time_slot1 = TimeSlot(start="00:00", end="00:00")

# 任务2：每天00:30执行，依赖于任务1
time_slot2 = TimeSlot(start="00:30", end="00:30")

# 添加任务1
scheduler.add_task(
    name="task1",
    data_source_name="CryptoSpotDataSource",
    data_source_config={},
    storage_name="LocalFileStorage",
    storage_config={},
    time_slot=time_slot1,
    symbols=["binance:BTC/USDT"],
    timeframe="1d",
    timerange_str="20240101-"
)

# 添加任务2
scheduler.add_task(
    name="task2",
    data_source_name="FREDDataSource",
    data_source_config={"api_key": "your_api_key"},
    storage_name="DUCKDBStorage",
    storage_config={},
    time_slot=time_slot2,
    symbols=["GDP", "UNRATE"],
    timeframe="1d",
    timerange_str="20240101-"
)
```

### 7.3 批量任务管理

```python
from chronoforge.scheduler import Scheduler
from chronoforge.utils import TimeSlot

# 创建调度器
scheduler = Scheduler()

# 定义时间槽
time_slot = TimeSlot(start="00:00", end="23:59")

# 批量添加加密货币数据获取任务
crypto_symbols = ["binance:BTC/USDT", "binance:ETH/USDT", "binance:ADA/USDT"]

for symbol in crypto_symbols:
    # 生成任务名称
    task_name = f"crypto_{symbol.split(':')[1].replace('/', '_')}"
    
    # 添加任务
    scheduler.add_task(
        name=task_name,
        data_source_name="CryptoSpotDataSource",
        data_source_config={},
        storage_name="DUCKDBStorage",
        storage_config={},
        time_slot=time_slot,
        symbols=[symbol],
        timeframe="1d",
        timerange_str="20240101-"
    )

# 启动所有任务
scheduler.start()
```

## 8. 监控和日志

### 8.1 日志配置

ChronoForge 使用 Python 的标准 logging 模块。您可以根据需要配置日志级别：

```python
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# 现在导入并使用 ChronoForge
from chronoforge.scheduler import Scheduler
```

### 8.2 任务监控

以下是一个任务监控的示例：

```python
import time
from chronoforge.scheduler import Scheduler
from chronoforge.utils import TimeSlot

def monitor_tasks():
    scheduler = Scheduler()
    
    # 添加任务
    # ...
    
    # 启动调度器
    scheduler.start()
    
    # 监控任务状态
    try:
        while True:
            print("\n任务状态监控:")
            for task_name, task_state in scheduler.task_states.items():
                status = task_state.get('status', 'idle')
                last_run = task_state.get('last_run_time')
                run_count = task_state.get('run_count', 0)
                print(f"任务 {task_name}: 状态={status}, 运行次数={run_count}, 最后运行时间={last_run}")
            time.sleep(5)
    except KeyboardInterrupt:
        print("监控中断")
        scheduler.stop()

# 运行
monitor_tasks()
```

## 9. 常见问题和解决方案

### 9.1 问题：FREDDataSource 初始化失败

**解决方案**：确保提供了有效的 FRED API 密钥。

```python
# 正确的配置方式
scheduler.add_task(
    name="fred_data",
    data_source_name="FREDDataSource",
    data_source_config={"api_key": "your_valid_api_key"},  # 必须提供有效的API密钥
    storage_name="LocalFileStorage",
    storage_config={},
    time_slot=time_slot,
    symbols=["GDP", "UNRATE"],
    timeframe="1d",
    timerange_str="20240101-"
)
```

### 9.2 问题：任务执行失败，提示API请求限制

**解决方案**：增加任务的时间间隔，或减少并发任务数。

```python
# 减少并发任务数
# Scheduler 默认配置已优化，无需手动设置

# 或调整时间槽配置
time_slot = TimeSlot(start="00:00", end="23:59")  # 每天执行
```

### 9.3 问题：存储插件初始化失败

**解决方案**：确保安装了相应的依赖。

```bash
# 安装DuckDB依赖
pip install duckdb

# 安装Redis依赖
pip install redis
```

### 9.4 问题：数据获取不完整

**解决方案**：检查时间范围配置和API权限。

```python
# 确保时间范围配置正确
timerange_str="20240101-"  # 从2024年1月1日到现在

# 对于加密货币数据，确保API密钥有足够的权限
```

### 9.5 问题：服务启动失败

**解决方案**：检查端口是否被占用，以及依赖是否安装完整。

```bash
# 检查端口是否被占用
lsof -i :8000

# 确保所有依赖都已安装
pip install -r requirements.txt
```

## 10. 性能优化

### 10.1 提高数据获取效率

1. **合理配置并发数**：根据系统资源和API限制设置合适的并发数。

```python
# 根据系统资源设置合适的并发数
scheduler = Scheduler(max_workers=5)  # 适中的并发数
```

2. **使用增量更新**：ChronoForge 默认使用增量更新，只获取缺失的数据段。

3. **优化时间范围**：避免设置过大的时间范围，特别是对于高频数据。

### 10.2 提高存储性能

1. **选择合适的存储插件**：根据数据量和查询需求选择合适的存储插件。
   - 小数据量：LocalFileStorage
   - 中等数据量：DUCKDBStorage
   - 大数据量或需要快速查询：RedisStorage

2. **优化存储配置**：根据实际情况优化存储配置。

```python
# 优化DuckDB存储配置
storage_config={"db_path": "./data/chronoforge.db", "memory_limit": "4GB"}
```

### 10.3 减少API请求

1. **合理配置时间槽**：避免过于频繁的任务执行。

2. **批量获取数据**：尽量在一个任务中获取多个符号的数据。

```python
# 批量获取多个加密货币数据
symbols=["binance:BTC/USDT", "binance:ETH/USDT", "binance:ADA/USDT"]
```

## 11. 最佳实践

1. **使用虚拟环境**：始终在虚拟环境中安装和运行 ChronoForge。

2. **合理规划任务**：根据数据更新频率和API限制，合理规划任务的执行时间和频率。

3. **监控任务执行**：定期检查任务执行状态，确保数据获取正常。

4. **备份数据**：定期备份存储的数据，防止数据丢失。

5. **使用合适的存储插件**：根据数据量和查询需求选择合适的存储插件。

6. **遵循API使用规范**：遵守各数据源的API使用规范，避免过度请求。

7. **定期更新依赖**：定期更新项目依赖，确保使用最新的功能和安全修复。

8. **编写测试**：为自定义插件和功能编写测试，确保稳定性。

通过遵循以上最佳实践，您可以充分发挥 ChronoForge 的功能，高效地管理和处理时间序列数据。

## 12. 时间范围格式详解

### 12.1 时间范围字符串格式

ChronoForge 支持多种时间范围字符串格式：

```
# 完整格式：开始日期_开始时间 - 结束日期_结束时间
20240101_000000 - 20240630_235959

# 简写格式：仅日期（时间默认为 00:00:00 - 23:59:59）
20240101 - 20240630

# 仅开始日期：获取从该日期到当前的所有数据
20240101 -

# 仅结束日期：获取从项目支持的最早日期到指定日期的数据
- 20240630

# 相对时间格式（高级用法）
-1d    # 1天前到现在
-7d    # 7天前到现在
+1h    # 当前时间1小时后（仅用于计算结束时间）
```

### 12.2 时间戳毫秒格式

ChronoForge 内部统一使用毫秒级时间戳：

```python
# 时间戳示例
1704067200000  # 2024-01-01 00:00:00 UTC

# 转换为日期
from datetime import datetime
datetime.fromtimestamp(1704067200000 / 1000, tz=datetime.timezone.utc)
```

### 12.3 TimeRange 类用法

```python
from chronoforge.utils import TimeRange

# 方式1：从字符串解析
timerange = TimeRange.parse_timerange("20240101-20240630")

# 方式2：直接指定时间戳（毫秒）
timerange = TimeRange(
    start_ts_ms=1704067200000,  # 2024-01-01
    end_ts_ms=1717200000000     # 2024-06-01
)

# 访问时间范围
print(f"开始时间: {timerange.start_dt}")
print(f"结束时间: {timerange.end_dt}")
print(f"开始时间戳: {timerange.start_ts_ms}")
print(f"结束时间戳: {timerange.end_ts_ms}")
```

## 13. 支持的时间框架

### 13.1 时间框架格式

ChronoForge 支持以下时间框架格式：

| 时间框架 | 说明 | 分钟数 |
|----------|------|--------|
| `1m` | 1分钟 | 1 |
| `5m` | 5分钟 | 5 |
| `15m` | 15分钟 | 15 |
| `30m` | 30分钟 | 30 |
| `1h` | 1小时 | 60 |
| `2h` | 2小时 | 120 |
| `4h` | 4小时 | 240 |
| `6h` | 6小时 | 360 |
| `8h` | 8小时 | 480 |
| `12h` | 12小时 | 720 |
| `1d` | 1天 | 1440 |
| `3d` | 3天 | 4320 |
| `1w` | 1周 | 10080 |
| `1M` | 1月（30天） | 43200 |

### 13.2 时间框架转换函数

```python
from chronoforge.utils import (
    parse_timeframe_to_minutes,
    parse_timeframe_to_seconds,
    parse_timeframe_to_milliseconds
)

# 转换为不同单位
minutes = parse_timeframe_to_minutes("1h")     # 60
seconds = parse_timeframe_to_seconds("1h")      # 3600
milliseconds = parse_timeframe_to_milliseconds("1h")  # 3600000
```

## 14. 交易对格式说明

### 14.1 现货交易对格式

```
交易所:基础货币/报价货币
例如：
- binance:BTC/USDT      # 币安比特币/泰达币现货
- okx:ETH/USDT         # OKX以太坊/泰达币现货
- coinbase:ADA/USDC    # Coinbase ADA/USDC现货
```

### 14.2 合约交易对格式

```
# 永续合约
交易所:基础货币/报价货币:结算货币
例如：
- binance:BTC/USDT:USDT     # 币安BTC永续合约
- okx:ETH/USDT:USDT        # OKX ETH永续合约

# 期货合约（带到期日）
交易所:基础货币/报价货币:结算货币-到期日
例如：
- binance:BTC/USDT:USDT-20240329  # 币安2024年3月BTC期货
```

### 14.3 经济数据符号格式

```
# FRED 经济数据
FRED:指标代码
例如：
- FRED:GDP         # 美国GDP
- FRED:UNRATE      # 美国失业率
- FRED:CPIAUCSL   # 美国CPI

# 全局市场数据
交易所:市场代码
例如：
- tv:FNG           # Fear & Greed Index
```

## 15. 数据源详细说明

### 15.1 数据源能力

ChronoForge 的数据源管理器提供了丰富的数据获取能力：

```python
from chronoforge.data_source import DataSourceManager, RetryConfig

# 配置数据源管理器
manager = DataSourceManager(max_workers=10)

# 配置重试策略
retry_config = RetryConfig(
    max_retries=3,
    base_delay=1.0,
    exponential_base=2.0,
    max_delay=30.0
)

# 使用数据源
result = await manager.fetch(
    data_source=CryptoSpotDataSource,
    config={"api_key": "xxx", "api_secret": "xxx"},
    symbol="BTC/USDT",
    timeframe="1h",
    start_ts_ms=1704067200000,
    end_ts_ms=1717200000000,
    retry_config=retry_config
)
```

### 15.2 数据源错误处理

ChronoForge 定义了多种数据源错误类型：

```python
from chronoforge.data_source import (
    DataSourceError,
    DataSourceConnectionError,
    DataSourceRateLimitError,
    DataSourceAuthenticationError,
    DataSourceNotFoundError,
    DataSourceDataError,
    DataSourceTimeoutError
)

# 捕获特定错误
try:
    data = await data_source.fetch(symbol, timeframe, start_ts_ms, end_ts_ms)
except DataSourceRateLimitError as e:
    print(f"API请求频率受限，等待 {e.retry_after} 秒后重试")
except DataSourceAuthenticationError as e:
    print(f"认证失败：{e}")
except DataSourceError as e:
    print(f"数据源错误：{e}")
```

### 15.3 数据源缓存

ChronoForge 支持数据源缓存功能：

```python
from chronoforge.data_source import cached_fetch, DataSourceCacheConfig

# 配置缓存
cache_config = DataSourceCacheConfig(
    enabled=True,
    ttl=3600,           # 缓存有效期（秒）
    max_size=100,       # 最大缓存条目数
)

# 使用缓存装饰器
@cached_fetch(cache_config)
async def fetch_with_cache(data_source, symbol, timeframe, start_ts_ms, end_ts_ms):
    return await data_source.fetch(symbol, timeframe, start_ts_ms, end_ts_ms)
```

## 16. API 端点完整参考

### 16.1 状态 API

| 方法 | 端点 | 描述 |
|------|------|------|
| GET | /api/status | 获取服务状态 |

**响应示例：**
```json
{
  "status": "running",
  "version": "0.1.0",
  "tasks_count": 5,
  "scheduler_status": "started"
}
```

### 16.2 插件 API

| 方法 | 端点 | 描述 |
|------|------|------|
| GET | /api/plugins | 获取所有支持的插件 |
| GET | /api/plugins/{plugin_type} | 按类型获取插件 |

### 16.3 任务 API

| 方法 | 端点 | 描述 |
|------|------|------|
| GET | /api/tasks | 获取所有任务列表 |
| POST | /api/tasks | 创建新任务 |
| GET | /api/tasks/{task_name} | 获取任务详情 |
| DELETE | /api/tasks/{task_name} | 删除任务 |
| POST | /api/tasks/{task_name}/start | 启动任务 |
| POST | /api/tasks/{task_name}/stop | 停止任务 |
| GET | /api/tasks/{task_name}/status | 获取任务状态 |
| GET | /api/tasks/{task_name}/data_info | 获取任务数据信息 |
| GET | /api/tasks/{task_name}/data | 获取任务数据 |

### 16.4 请求/响应格式详解

#### 创建任务请求格式

```json
{
  "name": "task_name",
  "data_source_name": "CryptoSpotDataSource",
  "data_source_config": {
    "api_key": "xxx",
    "api_secret": "xxx"
  },
  "storage_name": "DUCKDBStorage",
  "storage_config": {
    "db_path": "./data/crypto.db"
  },
  "time_slot": {
    "start": "00:00",
    "end": "23:59"
  },
  "symbols": ["binance:BTC/USDT", "binance:ETH/USDT"],
  "timeframe": "1h",
  "timerange_str": "20240101-",
  "inplace": false
}
```

#### 获取数据响应格式

```json
{
  "task_name": "crypto_data",
  "data": [
    {
      "time": "2024-01-01 00:00:00",
      "open": 42000.0,
      "high": 42500.0,
      "low": 41800.0,
      "close": 42300.0,
      "volume": 1234.56
    }
  ],
  "total": 1000,
  "limit": 1000
}
```

## 17. 部署指南

### 17.1 Docker 部署

创建 Dockerfile：

```dockerfile
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["python", "-m", "chronoforge.cli", "serve", "--host", "0.0.0.0", "--port", "8000"]
```

### 17.2 Docker Compose 部署

创建 docker-compose.yml：

```yaml
version: '3.8'

services:
  chronoforge:
    build: .
    ports:
      - "8000:8000"
    volumes:
      - ./data:/app/data
    environment:
      - LOG_LEVEL=INFO

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data

volumes:
  redis_data:
```

### 17.3 生产环境配置

```python
# 生产环境推荐配置
storage_config = {
    "db_path": "/data/chronoforge.db",
    "memory_limit": "8GB",
    "threads": 16,
}

# 服务配置
# chronoforge serve --host 0.0.0.0 --port 8000 --workers 4

# 使用 Nginx 反向代理
# nginx.conf 中配置：
# location / {
#     proxy_pass http://127.0.0.1:8000;
#     proxy_set_header Host $host;
#     proxy_set_header X-Real-IP $remote_addr;
# }
```

## 18. 附录

### 18.1 环境变量

| 变量名 | 描述 | 默认值 |
|--------|------|--------|
| `CHRONOFORGE_DATA_PATH` | 数据存储路径 | `./data` |
| `CHRONOFORGE_LOG_LEVEL` | 日志级别 | `INFO` |
| `CHRONOFORGE_DB_PATH` | DuckDB 数据库路径 | `./data/chronoforge.db` |

### 18.2 依赖列表

核心依赖：
- pandas >= 1.3.0
- fastapi >= 0.68.0
- uvicorn >= 0.15.0
- ccxt >= 4.0.0
- python-dateutil >= 2.8.0

可选依赖：
- duckdb >= 0.5.0
- redis >= 4.0.0
- fredapi >= 0.5.0

### 18.3 常见术语表

| 术语 | 说明 |
|------|------|
| OHLCV | 开盘价(Open)、最高价(High)、最低价(Low)、收盘价(Close)、成交量(Volume) |
| 时间框架 | 数据的时间粒度，如1小时、1天等 |
| 时间槽 | 任务执行的时间窗口 |
| 数据源 | 获取数据的来源，如交易所、API等 |
| 存储后端 | 数据存储的位置，如本地文件、数据库等 |
| 增量更新 | 只获取缺失的数据，避免重复下载 |
| 毫秒时间戳 | 毫秒级 Unix 时间戳 |