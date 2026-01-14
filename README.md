# 🕒 ChronoForge

ChronoForge 是一个异步、插件式的时间序列数据处理框架，专注于高效获取、存储和管理各种时间序列数据。

## 🚀 主要特性

- 🔌 **插件架构** — 可自由扩展自定义数据源和存储插件，内置多种常用数据源
- ⚙️ **异步调度** — 高效管理并行读取和序列化写入，支持多任务并发执行
- 🧩 **关注点分离** — 每个组件专注于自己的职责，确保清晰性和模块化
- 🧠 **自动分页和增量更新** — 只获取缺失的数据段，避免重复下载
- ⏱️ **统一时间戳管理** — 避免时区不一致问题，所有时间戳统一为毫秒级
- 📊 **多种数据源支持** — 内置加密货币、FRED经济数据、全球市场数据等数据源
- 💾 **灵活存储选项** — 支持本地文件、DuckDB、Redis等多种存储方式
- 🎯 **任务调度** — 支持基于时间槽的任务调度，可配置执行时间

## 🏗️ 技术架构

### 系统架构

ChronoForge 采用分层架构设计，主要包含以下核心组件：

1. **调度器层**：中央控制器，负责任务管理和调度
2. **数据源层**：负责从各种来源获取时间序列数据
3. **存储层**：负责数据的持久化和管理
4. **服务层**：提供HTTP API接口，支持远程访问和管理

### 核心组件

| 组件 | 职责 | 说明 |
|------|------|------|
| Scheduler | 中央控制器 | 管理任务、调度执行、协调插件 |
| DataSourceBase | 数据源基类 | 定义数据源插件接口，处理数据获取 |
| StorageBase | 存储基类 | 定义存储插件接口，处理数据持久化 |
| TimeSlotManager | 时间槽管理 | 管理任务的执行时间窗口 |
| FastAPI服务 | HTTP接口 | 提供RESTful API，支持远程管理 |

### 技术栈

| 技术/库 | 用途 | 版本要求 |
|---------|------|----------|
| Python | 核心开发语言 | 3.8+ |
| pandas | 数据处理 | 1.3.0+ |
| FastAPI | HTTP服务 | 0.68.0+ |
| asyncio | 异步编程 | 内置 |
| DuckDB | 可选存储 | 0.5.0+ |
| Redis | 可选存储 | 4.0.0+ |

### 数据流

1. **任务创建**：用户通过API或代码创建数据获取任务
2. **任务调度**：调度器根据时间槽配置，在适当时间触发任务
3. **数据获取**：数据源插件从外部API获取数据
4. **数据处理**：对获取的数据进行处理和验证
5. **数据存储**：将处理后的数据保存到指定存储介质
6. **状态更新**：更新任务状态，记录执行结果

## 📈 应用场景

### 加密货币数据分析

- **实时行情监控**：定期获取加密货币价格数据
- **历史数据回测**：积累历史数据用于策略回测
- **多交易所数据整合**：统一获取多个交易所的数据

### 经济数据分析

- **宏观经济指标**：获取FRED等经济数据
- **行业趋势分析**：跟踪特定行业的经济指标
- **预测模型输入**：为预测模型提供数据支持

### 全球市场监控

- **跨市场数据整合**：统一获取股票、债券、商品等市场数据
- **市场联动分析**：分析不同市场之间的相关性
- **投资组合管理**：为投资组合提供数据支持

### 自定义数据采集

- **企业内部数据**：采集企业内部系统的时间序列数据
- **IoT设备数据**：收集物联网设备产生的时序数据
- **Web数据抓取**：定期抓取网页中的时间序列数据

## 🌟 优势与特点

### 技术优势

- **高性能**：异步编程和并行处理，提高数据获取效率
- **可靠性**：增量更新和错误处理机制，确保数据完整性
- **可扩展性**：插件架构，易于扩展和定制
- **易用性**：简洁的API设计和详细的文档

### 业务价值

- **降低开发成本**：统一的数据获取和管理框架
- **提高数据质量**：标准化的数据处理流程
- **加速数据驱动决策**：实时、准确的数据支持
- **增强系统集成能力**：灵活的API接口和插件机制

## 🛣️ 发展路线

### 近期规划

- **扩展数据源**：增加更多数据源插件
- **优化存储**：改进存储插件性能
- **增强API**：扩展API功能，支持更多操作
- **完善文档**：提供更详细的使用文档和示例

### 长期愿景

- **智能化**：引入机器学习，实现智能数据预测和异常检测
- **分布式**：支持分布式部署，处理更大规模数据
- **生态系统**：构建完整的时间序列数据处理生态系统
- **标准化**：推动时间序列数据处理的行业标准

## 📦 安装

### 1. 从源码安装

如果您下载了源代码，可以使用以下命令安装：

```bash
# 从源码安装（开发模式）
pip install -e .

# 或使用 requirements.txt 安装依赖
pip install -r requirements.txt
```

### 2. 从PyPI安装（未来支持）

```bash
# 从PyPI安装（未来支持）
# pip install chronoforge
```

## 🧱 核心架构

### 1. `Scheduler`

**调度器** 是 ChronoForge 的中央控制器，负责工作流程编排和任务管理。

**主要职责：**

- 管理所有插件实例（数据源和存储）
- 支持基于时间槽的任务调度
- 处理并行读取和序列化写入
- 管理任务状态和执行结果
- 内置线程池，支持多任务并发执行

### 2. `DataSourceBase` (数据源基类)

每个数据源插件继承自 `DataSourceBase`，负责从特定源获取数据。

**内置数据源插件：**

- `CryptoSpotDataSource` — 加密货币现货数据
- `CryptoUMFutureDataSource` — 加密货币永续合约数据
- `FREDDataSource` — FRED经济数据
- `GlobalMarketDataSource` — 全球市场数据
- `BitcoinFGIDataSource` — 比特币恐惧与贪婪指数

**核心方法：**

- `fetch()` — 从数据源获取数据（基类必需实现）

**可选方法（特定实现类可能提供）：**

- `validate_data()` — 验证数据完整性
- `close_all_connections()` — 关闭数据源连接

### 3. `StorageBase` (存储基类)

存储插件继承自 `StorageBase`，负责数据的持久化和更新。

**内置存储插件：**

- `LocalFileStorage` — 本地文件存储
- `DUCKDBStorage` — DuckDB数据库存储
- `RedisStorage` — Redis数据库存储

**核心方法：**

- `save()` — 保存数据
- `load()` — 加载数据
- `delete()` — 删除数据
- `exists()` — 检查数据是否存在
- `lists()` — 列出存储介质中的所有数据
- `get_time_range()` — 获取数据的时间范围

## 🚦 快速开始

### 1. 嵌入模式

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

### 2. 自运行模式

ChronoForge 还支持作为独立服务运行，通过 RESTful API 提供访问。

#### 2.1 启动服务

##### 2.1.1 安装包后启动服务

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

##### 2.1.2 下载源代码后启动服务

如果您下载了源代码但尚未安装，可以使用以下方式启动服务：

```bash
# 使用 python -m 方式启动服务
python -m chronoforge.cli serve --host 0.0.0.0 --port 8000

# 或直接运行 cli.py 文件
python chronoforge/cli.py serve --host 0.0.0.0 --port 8000
```

##### 2.1.3 服务启动参数

| 参数 | 描述 | 默认值 |
|------|------|--------|
| `--host` | 服务绑定的主机地址 | `127.0.0.1` |
| `--port` | 服务绑定的端口 | `8000` |
| `--reload` | 开发模式，代码修改时自动重载 | `False` |
| `--workers` | 工作进程数 | `1` |

#### 2.2 API 访问

服务启动后，可以通过以下地址访问 API：

- **服务地址**: http://localhost:8000
- **API文档**: http://localhost:8000/docs （Swagger UI）
- **ReDoc文档**: http://localhost:8000/redoc

#### 2.3 核心 API 端点

| 方法 | 端点 | 描述 |
|------|------|------|
| GET | /api/status | 获取服务状态 |
| GET | /api/tasks | 列出所有任务 |
| POST | /api/tasks | 创建新任务 |
| GET | /api/tasks/{task_name} | 获取任务详情 |
| DELETE | /api/tasks/{task_name} | 删除任务 |
| POST | /api/tasks/{task_name}/start | 启动任务 |
| POST | /api/tasks/{task_name}/stop | 停止任务 |
| GET | /api/plugins | 列出所有支持的插件 |
| GET | /api/plugins/{plugin_type} | 按类型列出插件 |

#### 2.4 API 示例

##### 获取服务状态

```bash
curl http://localhost:8000/api/status
```

##### 列出所有插件

```bash
curl http://localhost:8000/api/plugins
```

##### 创建新任务

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

##### 列出所有任务

```bash
curl http://localhost:8000/api/tasks
```

#### 2.4 使用示例

ChronoForge 提供了多个示例文件，展示如何使用不同功能：

```bash
# 向服务器添加任务示例
python examples/add_tasks_to_server.py

# 嵌入模式使用示例
python examples/embeded.py

# 获取任务数据示例
python examples/get_task_data.py

# 获取任务数据信息示例
python examples/get_task_data_info.py

# 插件函数使用示例
python examples/plugin_functions_example.py

# 任务监控示例
python examples/task_monitor.py
```

这些示例文件展示了如何：

- 与 ChronoForge 服务交互
- 使用嵌入模式运行 ChronoForge
- 获取和管理任务数据
- 使用插件函数
- 监控任务执行状态

## 📁 项目结构

```
ChronoForge/
├── chronoforge/          # 主包
│   ├── __init__.py       # 包初始化
│   ├── scheduler.py      # 调度器实现
│   ├── cli.py            # 命令行工具
│   ├── utils.py          # 工具函数
│   ├── data_source/      # 数据源插件
│   │   ├── __init__.py   # 数据源包初始化
│   │   ├── base.py       # 数据源基类
│   │   ├── crypto_spot.py # 加密货币现货数据源
│   │   ├── fred.py       # FRED经济数据源
│   │   └── ...           # 其他数据源
│   ├── storage/          # 存储插件
│   │   ├── __init__.py   # 存储包初始化
│   │   ├── base.py       # 存储基类
│   │   ├── localfile.py  # 本地文件存储
│   │   ├── duckdb.py     # DuckDB存储
│   │   └── redisdb.py    # Redis存储
│   └── server/           # HTTP服务
│       ├── __init__.py   # 服务包初始化
│       ├── main.py       # FastAPI应用入口
│       ├── dependencies.py # 依赖管理
│       ├── api/          # API路由
│       │   ├── __init__.py
│       │   ├── tasks.py  # 任务管理API
│       │   ├── plugins.py # 插件管理API
│       │   └── status.py # 状态查询API
│       └── models/       # Pydantic模型
│           ├── __init__.py
│           ├── task.py   # 任务相关模型
│           └── plugin.py # 插件相关模型
├── examples/             # 示例代码
├── tests/                # 测试代码
├── data/                 # 数据目录
├── requirements.txt      # 项目依赖
├── pyproject.toml        # 项目配置
├── setup.py              # 安装配置
├── README.md             # 项目文档
└── LICENSE               # 许可证
```

## 🧪 运行测试

```bash
python -m unittest discover tests
```

## 🔧 开发自定义插件

### 开发数据源插件

继承 `DataSourceBase` 类并实现必需的方法：

```python
from chronoforge.data_source import DataSourceBase

class CustomDataSource(DataSourceBase):
    def __init__(self, config=None):
        super().__init__(config)
    
    async def fetch(self, symbol, timeframe, start_ts_ms, end_ts_ms=None):
        # 实现从您的数据源获取数据的逻辑
        # 返回包含 'time' 列的 pandas DataFrame
        pass
    
    def validate_data(self, data):
        # 实现数据验证逻辑
        pass
    
    async def close_all_connections(self):
        # 关闭数据源连接
        pass
```

### 开发存储插件

继承 `StorageBase` 类并实现必需的方法：

```python
from chronoforge.storage import StorageBase

class CustomStorage(StorageBase):
    def __init__(self, config=None):
        super().__init__(config)
    
    async def save(self, id, data, sub=None):
        # 实现数据保存逻辑
        pass
    
    async def load(self, id, sub=None):
        # 实现数据加载逻辑
        pass
    
    async def exists(self, id, sub=None):
        # 实现数据存在检查逻辑
        pass
```

## 🎯 支持的时间框架

ChronoForge 支持以下时间框架：

- `1w` — 周线
- `1d` — 日线
- `4h` — 4小时线
- `1h` — 1小时线

## 📊 示例用法

### 获取加密货币数据

```python
from chronoforge import Scheduler
from chronoforge.utils import TimeSlot

# 创建调度器
scheduler = Scheduler()

# 定义时间槽（每天执行一次）
time_slot = TimeSlot(hour=0, minute=0)

# 添加加密货币数据获取任务
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

# 启动调度器
scheduler.start()
```

### 获取FRED经济数据

```python
from chronoforge import Scheduler
from chronoforge.utils import TimeSlot

# 创建调度器
scheduler = Scheduler()

# 定义时间槽（每周一执行）
time_slot = TimeSlot(weekday=0, hour=8, minute=0)

# 添加FRED数据获取任务
scheduler.add_task(
    name="fred_data",
    data_source_name="FREDDataSource",
    data_source_config={"api_key": "your_fred_api_key"},
    storage_name="LocalFileStorage",
    storage_config={"base_path": "./fred_data"},
    time_slot=time_slot,
    symbols=["GDP", "UNRATE", "CPIAUCSL"],
    timeframe="1d",
    timerange_str="20200101-"
)

# 启动调度器
scheduler.start()
```

## 🧩 架构图

```mermaid
graph TD
    Scheduler --> DataSourceBase
    Scheduler --> StorageBase
    Scheduler --> Task[Task Management]
    Task --> TimeSlot[Time Slot Scheduling]
    DataSourceBase --> CryptoSpot[CryptoSpotDataSource]
    DataSourceBase --> CryptoFuture[CryptoUMFutureDataSource]
    DataSourceBase --> FRED[FREDDataSource]
    DataSourceBase --> GlobalMarket[GlobalMarketDataSource]
    DataSourceBase --> BitcoinFGI[BitcoinFGIDataSource]
    StorageBase --> LocalFile[LocalFileStorage]
    StorageBase --> DuckDB[(DUCKDBStorage)]
    StorageBase --> Redis[(RedisStorage)]
    CryptoSpot --> StorageBase
    CryptoFuture --> StorageBase
    FRED --> StorageBase
    GlobalMarket --> StorageBase
    BitcoinFGI --> StorageBase
```

## 🤝 贡献指南

欢迎贡献代码！请按照以下步骤进行：

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 打开 Pull Request

## 📝 许可证

本项目采用 MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

## 📧 联系方式

- 作者: Daboooooo
- 邮箱: [horsen666@gmail.com](mailto:horsen666@gmail.com)
- 项目地址: [https://github.com/daboooooo/ChronoForge](https://github.com/daboooooo/ChronoForge)
