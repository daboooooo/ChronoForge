# 代码结构文档

## 1. 项目目录结构

ChronoForge 项目采用模块化设计，目录结构清晰合理，便于维护和扩展。以下是项目的主要目录结构：

```
ChronoForge/
├── chronoforge/          # 主包目录
│   ├── __init__.py       # 包初始化文件
│   ├── scheduler.py      # 调度器实现
│   ├── cli.py            # 命令行工具
│   ├── utils.py          # 工具函数
│   ├── decorators.py     # 装饰器定义
│   ├── data_source/      # 数据源插件目录
│   │   ├── __init__.py   # 数据源包初始化
│   │   ├── base.py       # 数据源基类
│   │   ├── crypto_spot.py # 加密货币现货数据源
│   │   ├── crypto_umfuture.py # 加密货币永续合约数据源
│   │   ├── fred.py       # FRED经济数据源
│   │   ├── global_market.py # 全球市场数据源
│   │   └── bitcoin_fgi.py # 比特币恐惧与贪婪指数数据源
│   ├── storage/          # 存储插件目录
│   │   ├── __init__.py   # 存储包初始化
│   │   ├── base.py       # 存储基类
│   │   ├── localfile.py  # 本地文件存储
│   │   ├── duckdb.py     # DuckDB存储
│   │   └── redisdb.py    # Redis存储
│   └── server/           # HTTP服务目录
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
├── examples/             # 示例代码目录
├── tests/                # 测试代码目录
├── docs/                 # 文档目录
├── data/                 # 数据目录
├── requirements.txt      # 项目依赖
├── pyproject.toml        # 项目配置
├── setup.py              # 安装配置
├── README.md             # 项目文档
└── LICENSE               # 许可证
```

## 2. 核心模块说明

### 2.1 主包模块 (chronoforge/)

#### 2.1.1 scheduler.py

**功能**：调度器是ChronoForge的中央控制器，负责任务管理和调度执行。

**核心组件**：
- `Scheduler` 类：管理所有插件实例、任务调度和执行
- `Task` 类：封装任务相关信息
- `_load_data_for_updating` 函数：加载缓存数据并计算需要更新的时间范围
- `_update_data` 函数：下载单个交易对的单个时间周期的K线数据

**主要方法**：
- `add_task()`：添加新任务
- `start()`：启动调度器
- `stop()`：停止调度器
- `run()`：运行调度器，检查时间槽并执行任务
- `list_supported_plugins()`：列出所有支持的插件
- `delegate_call()`：动态调用插件的指定函数

#### 2.1.2 cli.py

**功能**：命令行工具，提供服务启动和管理功能。

**核心命令**：
- `serve`：启动HTTP服务
- 支持的参数：`--host`、`--port`、`--reload`、`--workers`

#### 2.1.3 utils.py

**功能**：工具函数集合，提供时间处理和其他辅助功能。

**核心类和函数**：
- `TimeSlot` 类：时间槽定义和管理
- `TimeSlotManager` 类：管理多个时间槽
- `TimeRange` 类：时间范围定义和解析
- `parse_timeframe_to_milliseconds()`：将时间框架解析为毫秒

### 2.2 数据源模块 (chronoforge/data_source/)

#### 2.2.1 base.py

**功能**：定义数据源插件的基类和验证函数。

**核心组件**：
- `DataSourceBase` 抽象基类：定义数据源插件的接口
- `verify_datasource_instance()` 函数：验证数据源插件是否符合要求
- `ParsedSymbol` 类：解析交易对符号

**数据源基类核心方法**：
- `fetch()`：从数据源获取数据（抽象方法）
- `name` 属性：返回数据源名称（抽象属性）

#### 2.2.2 内置数据源插件

| 数据源插件 | 功能 | 文件 |
|-----------|------|------|
| CryptoSpotDataSource | 加密货币现货数据 | crypto_spot.py |
| CryptoUMFutureDataSource | 加密货币永续合约数据 | crypto_umfuture.py |
| FREDDataSource | FRED经济数据 | fred.py |
| GlobalMarketDataSource | 全球市场数据 | global_market.py |
| BitcoinFGIDataSource | 比特币恐惧与贪婪指数 | bitcoin_fgi.py |

### 2.3 存储模块 (chronoforge/storage/)

#### 2.3.1 base.py

**功能**：定义存储插件的基类和验证函数。

**核心组件**：
- `StorageBase` 抽象基类：定义存储插件的接口
- `verify_storage_instance()` 函数：验证存储插件是否符合要求

**存储基类核心方法**：
- `save()`：保存数据
- `load()`：加载数据
- `delete()`：删除数据
- `exists()`：检查数据是否存在
- `lists()`：列出存储介质中的所有数据
- `get_time_range()`：获取数据的时间范围
- `name` 属性：返回存储插件名称

#### 2.3.2 内置存储插件

| 存储插件 | 功能 | 文件 |
|---------|------|------|
| LocalFileStorage | 本地文件存储 | localfile.py |
| DUCKDBStorage | DuckDB数据库存储 | duckdb.py |
| RedisStorage | Redis数据库存储 | redisdb.py |

### 2.4 服务模块 (chronoforge/server/)

#### 2.4.1 main.py

**功能**：FastAPI应用入口，配置和启动HTTP服务。

**核心组件**：
- `app`：FastAPI应用实例
- 路由注册：注册API路由
- 启动配置：服务启动和配置

#### 2.4.2 dependencies.py

**功能**：管理服务依赖，提供共享资源。

**核心函数**：
- `get_scheduler()`：获取调度器实例

#### 2.4.3 api/ 目录

**功能**：API路由定义，处理HTTP请求。

| API模块 | 功能 | 文件 |
|---------|------|------|
| tasks.py | 任务管理API | tasks.py |
| plugins.py | 插件管理API | plugins.py |
| status.py | 状态查询API | status.py |

#### 2.4.4 models/ 目录

**功能**：定义Pydantic模型，用于API请求和响应的数据验证。

| 模型文件 | 功能 | 文件 |
|---------|------|------|
| task.py | 任务相关模型 | task.py |
| plugin.py | 插件相关模型 | plugin.py |

## 3. 示例代码目录 (examples/)

**功能**：提供使用示例，帮助用户快速上手。

| 示例文件 | 功能 |
|---------|------|
| add_tasks_to_server.py | 向服务器添加任务的示例 |
| embeded.py | 嵌入模式使用示例 |
| get_task_data.py | 获取任务数据的示例 |
| get_task_data_info.py | 获取任务数据信息的示例 |
| plugin_functions_example.py | 插件函数使用示例 |
| task_monitor.py | 任务监控示例 |

## 4. 测试代码目录 (tests/)

**功能**：单元测试和集成测试代码。

| 测试文件 | 测试功能 |
|---------|----------|
| test_cli.py | 测试命令行工具 |
| test_data_source.py | 测试数据源插件 |
| test_periodic_task.py | 测试周期性任务 |
| test_plugin_verification.py | 测试插件验证 |
| test_scheduler.py | 测试调度器 |
| test_scheduler_core.py | 测试调度器核心功能 |
| test_server_api.py | 测试服务器API |
| test_storage.py | 测试存储插件 |
| test_utils.py | 测试工具函数 |

## 5. 配置文件

| 配置文件 | 功能 |
|---------|------|
| pyproject.toml | 项目配置文件 |
| setup.py | 安装配置文件 |
| requirements.txt | 项目依赖文件 |
| .gitignore | Git忽略文件配置 |
| LICENSE | 许可证文件 |

## 6. 代码组织原则

ChronoForge 项目的代码组织遵循以下原则：

1. **模块化设计**：将功能分解为独立的模块，每个模块专注于自己的职责
2. **抽象基类**：通过抽象基类定义接口，实现插件的标准化
3. **依赖注入**：通过依赖注入实现组件间的解耦
4. **异步编程**：使用asyncio实现高效的异步操作
5. **错误处理**：完善的错误处理机制，确保系统稳定性
6. **代码规范**：遵循Python代码规范，保持代码风格一致
7. **测试覆盖**：完善的测试用例，确保代码质量

## 7. 扩展和定制

### 7.1 开发自定义数据源插件

要开发自定义数据源插件，需要：
1. 继承 `DataSourceBase` 抽象基类
2. 实现 `fetch()` 方法和 `name` 属性
3. 确保符合数据源插件的验证要求

### 7.2 开发自定义存储插件

要开发自定义存储插件，需要：
1. 继承 `StorageBase` 抽象基类
2. 实现所有抽象方法和 `name` 属性
3. 确保符合存储插件的验证要求

### 7.3 扩展API功能

要扩展API功能，需要：
1. 在 `server/api/` 目录中创建新的路由模块
2. 定义新的API端点
3. 在 `server/main.py` 中注册新的路由

## 8. 代码维护指南

1. **添加新功能**：遵循现有代码结构和设计模式
2. **修复bug**：确保修复不会影响其他功能，添加相应的测试用例
3. **优化性能**：分析性能瓶颈，提出合理的优化方案
4. **更新文档**：代码变更后及时更新相关文档
5. **代码审查**：确保代码符合项目规范和质量要求

## 9. 技术栈依赖

| 技术/库 | 用途 | 版本要求 | 依赖文件 |
|---------|------|----------|----------|
| Python | 核心开发语言 | 3.8+ | pyproject.toml |
| pandas | 数据处理 | 1.3.0+ | requirements.txt |
| FastAPI | HTTP服务 | 0.68.0+ | requirements.txt |
| uvicorn | ASGI服务器 | 0.15.0+ | requirements.txt |
| asyncio | 异步编程 | 内置 | - |
| duckdb | 可选存储 | 0.5.0+ | requirements.txt |
| redis | 可选存储 | 4.0.0+ | requirements.txt |
| ccxt | 加密货币API | 1.90.0+ | requirements.txt |
| fredapi | FRED经济数据API | 0.5.0+ | requirements.txt |

## 10. 代码质量保证

1. **类型注解**：使用类型注解提高代码可读性和IDE支持
2. **文档字符串**：为所有公共方法和类添加详细的文档字符串
3. **单元测试**：为核心功能编写单元测试
4. **集成测试**：测试组件间的交互
5. **代码审查**：定期进行代码审查，确保代码质量
6. **静态分析**：使用静态分析工具检查代码质量

通过以上代码结构设计，ChronoForge 项目实现了高度模块化、可扩展的架构，为用户提供了一个强大而灵活的时间序列数据处理框架。