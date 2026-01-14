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
from chronoforge import Scheduler
from chronoforge.utils import TimeSlot

async def main():
    # 创建调度器
    scheduler = Scheduler(max_workers=5)
    
    # 定义时间槽（每天00:00执行）
    time_slot = TimeSlot(hour=0, minute=0)
    
    # 添加任务：获取比特币现货数据
    scheduler.add_task(
        name="btc_spot_data",
        data_source_name="CryptoSpotDataSource",
        data_source_config={"api_key": "your_api_key", "api_secret": "your_api_secret"},
        storage_name="DUCKDBStorage",
        storage_config={"db_path": "./data/chronoforge.db"},
        time_slot=time_slot,
        symbols=["binance:BTC/USDT"],
        timeframe="1h",
        timerange_str="20240101-"
    )
    
    # 启动调度器
    scheduler.start()
    
    # 运行5秒后停止
    await asyncio.sleep(5)
    scheduler.stop()

# 运行
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

#### 3.1.3 其他数据源配置

其他数据源插件的配置项请参考相应的插件文档。

### 3.2 存储配置

#### 3.2.1 LocalFileStorage 配置

| 配置项 | 类型 | 描述 | 是否必需 | 默认值 |
|--------|------|------|----------|--------|
| `base_path` | str | 数据存储的基础路径 | 否 | `"./data"` |

#### 3.2.2 DUCKDBStorage 配置

| 配置项 | 类型 | 描述 | 是否必需 | 默认值 |
|--------|------|------|----------|--------|
| `db_path` | str | DuckDB数据库文件路径 | 否 | `"./data/chronoforge.db"` |

#### 3.2.3 RedisStorage 配置

| 配置项 | 类型 | 描述 | 是否必需 | 默认值 |
|--------|------|------|----------|--------|
| `host` | str | Redis服务器主机地址 | 否 | `"localhost"` |
| `port` | int | Redis服务器端口 | 否 | `6379` |
| `db` | int | Redis数据库编号 | 否 | `0` |
| `password` | str | Redis服务器密码 | 否 | `None` |

## 4. 任务管理

### 4.1 创建任务

#### 4.1.1 使用代码创建任务

```python
from chronoforge import Scheduler
from chronoforge.utils import TimeSlot

# 创建调度器
scheduler = Scheduler()

# 定义时间槽
time_slot = TimeSlot(start="00:00", end="23:59")

# 添加任务
scheduler.add_task(
    name="crypto_data",
    data_source_name="CryptoSpotDataSource",
    data_source_config={"api_key": "your_key", "api_secret": "your_secret"},
    storage_name="DUCKDBStorage",
    storage_config={"db_path": "./crypto_data.db"},
    time_slot=time_slot,
    symbols=["binance:BTC/USDT", "binance:ETH/USDT"],
    timeframe="1d",
    timerange_str="20240101-"
)
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
# 启动所有任务
scheduler.start()

# 运行一段时间后停止
import time
time.sleep(10)
scheduler.stop()
```

#### 4.2.2 使用API启动任务

```bash
curl -X POST http://localhost:8000/api/tasks/{task_name}/start
```

### 4.3 停止任务

#### 4.3.1 使用代码停止任务

```python
scheduler.stop()
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
task_state = scheduler.task_states.get("crypto_data", {})
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
from chronoforge import Scheduler

async def get_data():
    scheduler = Scheduler()
    
    # 假设已经添加了任务
    task_name = "crypto_data"
    
    # 获取任务对应的存储实例
    storage = scheduler.storage_instances.get(task_name)
    if not storage:
        print("存储实例未找到")
        return
    
    # 获取数据
    task = scheduler.tasks.get(task_name)
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
from chronoforge import Scheduler

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
from chronoforge import Scheduler
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

# 方式1：使用小时和分钟
slot1 = TimeSlot(hour=0, minute=0)  # 每天00:00

# 方式2：使用时间字符串
slot2 = TimeSlot(start="09:00", end="17:00")  # 每天9点到17点

# 方式3：使用完整的时间字符串
slot3 = TimeSlot(start="09:00:00", end="17:00:00")  # 每天9点到17点

# 方式4：指定工作日
slot4 = TimeSlot(start="09:00", end="17:00", weekdays=[0, 1, 2, 3, 4])  # 工作日9点到17点
```

### 7.2 任务依赖管理

ChronoForge 支持通过时间槽配置来管理任务之间的依赖关系：

```python
from chronoforge import Scheduler
from chronoforge.utils import TimeSlot

# 创建调度器
scheduler = Scheduler()

# 定义时间槽
# 任务1：每天00:00执行
time_slot1 = TimeSlot(hour=0, minute=0)

# 任务2：每天00:30执行，依赖于任务1
time_slot2 = TimeSlot(hour=0, minute=30)

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
from chronoforge import Scheduler
from chronoforge.utils import TimeSlot

# 创建调度器
scheduler = Scheduler()

# 定义时间槽
time_slot = TimeSlot(hour=0, minute=0)

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
from chronoforge import Scheduler
```

### 8.2 任务监控

以下是一个任务监控的示例：

```python
import time
from chronoforge import Scheduler
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
scheduler = Scheduler(max_workers=3)  # 减少最大工作线程数

# 或增加时间槽间隔
time_slot = TimeSlot(hour=0, minute=0, interval_hours=6)  # 每6小时执行一次
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