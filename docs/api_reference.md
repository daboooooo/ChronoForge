# API 接口文档

## 1. 概述

ChronoForge 提供了完整的 RESTful API 接口，支持远程管理和监控任务、插件和数据。API 基于 FastAPI 实现，提供了自动生成的 Swagger 文档和 ReDoc 文档。

### 1.1 API 访问地址

- **服务地址**: http://localhost:8000
- **API文档**: http://localhost:8000/docs （Swagger UI）
- **ReDoc文档**: http://localhost:8000/redoc

### 1.2 API 版本

当前 API 版本为 v1，所有 API 端点都以 `/api` 前缀开始。

### 1.3 认证

当前版本的 API 不需要认证，所有端点都可以直接访问。未来版本将支持 API 密钥认证。

## 2. 核心 API 端点

### 2.1 服务状态 API

#### 2.1.1 获取服务状态

- **方法**: GET
- **端点**: `/api/status`
- **描述**: 获取 ChronoForge 服务的当前状态

**请求参数**: 无

**请求示例**:

```bash
curl http://localhost:8000/api/status
```

**响应示例**:

```json
{
  "service": "ChronoForge Scheduler",
  "version": "1.0.0",
  "status": "running",
  "tasks_count": 5,
  "running_tasks_count": 0,
  "supported_data_sources": ["CryptoSpotDataSource", "FREDDataSource"],
  "supported_storages": ["LocalFileStorage", "DUCKDBStorage"],
  "task_states": [
    {
      "name": "task1",
      "status": "idle",
      "created_at": 1704067200.0,
      "last_updated_at": 1704067200.0,
      "run_count": 0,
      "last_run_time": null,
      "last_run_status": null,
      "error_message": null,
      "message": "Task is idle"
    }
  ],
  "connectivity": {
    "status": true,
    "last_test": 1704067200.0,
    "test_url": "https://www.google.com",
    "cache_expiry": 60
  }
}
```

#### 2.1.2 获取所有任务状态

- **方法**: GET
- **端点**: `/api/status/tasks`
- **描述**: 获取所有任务的详细状态信息

**请求参数**: 无

**请求示例**:

```bash
curl http://localhost:8000/api/status/tasks
```

**响应示例**:

```json
{
  "task1": {
    "name": "task1",
    "status": "idle",
    "created_at": 1704067200.0,
    "last_updated_at": 1704067200.0,
    "run_count": 0,
    "last_run_time": null,
    "last_run_status": null,
    "error_message": null,
    "message": "Task is idle"
  }
}
```

#### 2.1.3 获取系统统计信息

- **方法**: GET
- **端点**: `/api/status/stats`
- **描述**: 获取系统统计信息，包括 CPU 使用率、内存使用率、磁盘使用率、任务状态等

**请求参数**: 无

**请求示例**:

```bash
curl http://localhost:8000/api/status/stats
```

**响应示例**:

```json
{
  "system": {
    "cpu_usage": 15.3,
    "memory_usage": 65.5,
    "disk_usage": 23.2,
    "uptime_seconds": 81943.59136104584,
    "python_version": "3.12.2",
    "timestamp": "2026-01-12T17:07:16.591374"
  },
  "tasks": {
    "total": 9,
    "running": 4,
    "idle": 5,
    "failed": 0
  },
  "storage": {
    "total_size": "Unknown",
    "used_size": "Unknown",
    "data_count": 0
  },
  "api": {
    "requests_count": 0,
    "requests_per_minute": 0
  }
}
```

#### 2.1.4 获取系统统计信息（兼容端点）

- **方法**: GET
- **端点**: `/api/stats`
- **描述**: 获取系统统计信息（兼容端点，与 `/api/status/stats` 功能相同）

**请求参数**: 无

**请求示例**:

```bash
curl http://localhost:8000/api/stats
```

**响应示例**:

```json
{
  "system": {
    "cpu_usage": 15.3,
    "memory_usage": 65.5,
    "disk_usage": 23.2,
    "uptime_seconds": 81943.59136104584,
    "python_version": "3.12.2",
    "timestamp": "2026-01-12T17:07:16.591374"
  },
  "tasks": {
    "total": 9,
    "running": 4,
    "idle": 5,
    "failed": 0
  },
  "storage": {
    "total_size": "Unknown",
    "used_size": "Unknown",
    "data_count": 0
  },
  "api": {
    "requests_count": 0,
    "requests_per_minute": 0
  }
}
```

### 2.2 插件管理 API

#### 2.2.1 列出所有支持的插件

- **方法**: GET
- **端点**: `/api/plugins`
- **描述**: 列出 ChronoForge 支持的所有插件

**请求参数**: 无

**请求示例**:

```bash
curl http://localhost:8000/api/plugins
```

**响应示例**:

```json
{
  "data_source": ["CryptoSpotDataSource", "FREDDataSource", "BitcoinFGIDataSource", "CryptoUMFutureDataSource", "GlobalMarketDataSource"],
  "storage": ["LocalFileStorage", "DUCKDBStorage", "RedisStorage"]
}
```

#### 2.2.2 按类型列出插件

- **方法**: GET
- **端点**: `/api/plugins/{plugin_type}`
- **描述**: 按类型列出 ChronoForge 支持的插件

**请求参数**:

- `plugin_type`: 插件类型，可选值为 `data_source` 或 `storage`

**请求示例**:

```bash
curl http://localhost:8000/api/plugins/data_source
```

**响应示例**:

```json
{
  "plugins": ["CryptoSpotDataSource", "FREDDataSource", "BitcoinFGIDataSource", "CryptoUMFutureDataSource", "GlobalMarketDataSource"],
  "total": 5
}
```

#### 2.2.3 列出数据源函数

- **方法**: GET
- **端点**: `/api/plugins/data_source/{data_source_name}/functions`
- **描述**: 列出指定数据源的所有公共函数

**请求参数**:

- `data_source_name`: 数据源名称

**请求示例**:

```bash
curl http://localhost:8000/api/plugins/data_source/CryptoSpotDataSource/functions
```

**响应示例**:

```json
{
  "data_source_name": "CryptoSpotDataSource",
  "functions": [
    {
      "name": "fetch",
      "docstring": "获取指定时间范围内的数据",
      "parameters": [
        {
          "name": "symbol",
          "type": "str",
          "default": "None"
        },
        {
          "name": "timeframe",
          "type": "str",
          "default": "None"
        },
        {
          "name": "start_ts_ms",
          "type": "int",
          "default": "None"
        },
        {
          "name": "end_ts_ms",
          "type": "Optional[int]",
          "default": "None"
        }
      ],
      "return_type": "DataFrame"
    }
  ]
}
```

#### 2.2.4 代理调用插件函数

- **方法**: POST
- **端点**: `/api/plugins/delegate-call`
- **描述**: 代理调用插件函数

**请求体**:

| 字段 | 类型 | 描述 | 是否必需 |
|------|------|------|----------|
| `plugin_type` | str | 插件类型，可选值为 `data_source` 或 `storage` | 是 |
| `plugin_name` | str | 插件名称 | 是 |
| `function_name` | str | 函数名称 | 是 |
| `parameters` | dict | 函数参数 | 否 |

**请求示例**:

```bash
curl -X POST http://localhost:8000/api/plugins/delegate-call \
  -H "Content-Type: application/json" \
  -d '{
    "plugin_type": "data_source",
    "plugin_name": "CryptoSpotDataSource",
    "function_name": "fetch",
    "parameters": {
      "symbol": "binance:BTC/USDT",
      "timeframe": "1d",
      "start_ts_ms": 1609459200000,
      "end_ts_ms": 1612137600000
    }
  }'
```

**响应示例**:

```json
{
  "success": true,
  "result": {
    "data": [
      {
        "time": "2021-01-01 00:00:00",
        "open": 29000.0,
        "high": 33000.0,
        "low": 28000.0,
        "close": 32000.0,
        "volume": 20000.0
      }
    ],
    "metadata": {
      "columns": ["time", "open", "high", "low", "close", "volume"],
      "shape": [31, 6],
      "dtypes": {
        "time": "datetime64[ns]",
        "open": "float64",
        "high": "float64",
        "low": "float64",
        "close": "float64",
        "volume": "float64"
      }
    }
  }
}
```

### 2.3 任务管理 API

#### 2.3.1 列出所有任务

- **方法**: GET
- **端点**: `/api/tasks`
- **描述**: 列出所有已添加的任务

**请求参数**: 无

**请求示例**:

```bash
curl http://localhost:8000/api/tasks
```

**响应示例**:

```json
{
  "tasks": [
    {
      "name": "btc_spot_data",
      "data_source_name": "CryptoSpotDataSource",
      "storage_name": "DUCKDBStorage",
      "time_slot": {
        "start": "00:00",
        "end": "23:59"
      },
      "symbols": ["binance:BTC/USDT"],
      "timeframe": "1h",
      "timerange_str": "1609459200000-",
      "status": "idle"
    }
  ],
  "total": 1
}
```

#### 2.3.2 创建新任务

- **方法**: POST
- **端点**: `/api/tasks`
- **描述**: 创建一个新的任务

**请求体**:

| 字段 | 类型 | 描述 | 是否必需 | 默认值 |
|------|------|------|----------|--------|
| `name` | str | 任务名称 | 是 | - |
| `data_source_name` | str | 数据源名称 | 是 | - |
| `data_source_config` | dict | 数据源配置 | 否 | `{}` |
| `storage_name` | str | 存储名称 | 是 | - |
| `storage_config` | dict | 存储配置 | 否 | `{}` |
| `time_slot` | object | 时间槽配置 | 是 | - |
| `time_slot.start` | str | 时间槽开始时间 | 是 | - |
| `time_slot.end` | str | 时间槽结束时间 | 是 | - |
| `symbols` | array | 交易对或数据符号列表 | 是 | - |
| `timeframe` | str | 时间框架 | 否 | `"1d"` |
| `timerange_str` | str | 时间范围字符串 | 否 | `"20220101-"` |
| `inplace` | bool | 是否覆盖已存在的任务 | 否 | `false` |

**请求示例**:

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

**响应示例**:

```json
{
  "name": "test_task",
  "data_source_name": "CryptoSpotDataSource",
  "storage_name": "LocalFileStorage",
  "time_slot": {
    "start": "00:00",
    "end": "23:59"
  },
  "symbols": ["binance:BTC/USDT"],
  "timeframe": "1d",
  "timerange_str": "1704067200000-",
  "status": "idle"
}
```

#### 2.3.3 获取任务详情

- **方法**: GET
- **端点**: `/api/tasks/{task_name}`
- **描述**: 获取指定任务的详细信息

**请求参数**:

- `task_name`: 任务名称

**请求示例**:

```bash
curl http://localhost:8000/api/tasks/btc_spot_data
```

**响应示例**:

```json
{
  "name": "btc_spot_data",
  "data_source_name": "CryptoSpotDataSource",
  "storage_name": "DUCKDBStorage",
  "time_slot": {
    "start": "00:00",
    "end": "23:59"
  },
  "symbols": ["binance:BTC/USDT"],
  "timeframe": "1h",
  "timerange_str": "1609459200000-",
  "status": "idle"
}
```

#### 2.3.4 删除任务

- **方法**: DELETE
- **端点**: `/api/tasks/{task_name}`
- **描述**: 删除指定的任务

**请求参数**:

- `task_name`: 任务名称

**请求示例**:

```bash
curl -X DELETE http://localhost:8000/api/tasks/test_task
```

**响应示例**:

- 成功: `204 No Content`
- 失败: `404 Not Found` (任务不存在)

#### 2.3.5 启动任务

- **方法**: POST
- **端点**: `/api/tasks/{task_name}/start`
- **描述**: 立即启动指定的任务

**请求参数**:

- `task_name`: 任务名称

**请求示例**:

```bash
curl -X POST http://localhost:8000/api/tasks/btc_spot_data/start
```

**响应示例**:

```json
{
  "name": "btc_spot_data",
  "status": "running",
  "start_time": "2024-01-01T12:00:00Z",
  "message": "Task started successfully"
}
```

#### 2.3.6 停止任务

- **方法**: POST
- **端点**: `/api/tasks/{task_name}/stop`
- **描述**: 停止正在运行的任务

**请求参数**:

- `task_name`: 任务名称

**请求示例**:

```bash
curl -X POST http://localhost:8000/api/tasks/btc_spot_data/stop
```

**响应示例**:

```json
{
  "name": "btc_spot_data",
  "status": "stopped",
  "start_time": "2024-01-01T12:00:00Z",
  "message": "Task stopped successfully"
}
```

#### 2.3.7 获取任务状态

- **方法**: GET
- **端点**: `/api/tasks/{task_name}/status`
- **描述**: 获取指定任务的当前状态

**请求参数**:

- `task_name`: 任务名称

**请求示例**:

```bash
curl http://localhost:8000/api/tasks/btc_spot_data/status
```

**响应示例**:

```json
{
  "name": "btc_spot_data",
  "status": "idle",
  "start_time": null,
  "message": "Task is idle"
}
```

### 2.4 数据管理 API

#### 2.4.1 获取任务数据信息

- **方法**: GET
- **端点**: `/api/tasks/{task_name}/data_info`
- **描述**: 获取任务下的所有数据名称以及数据起始和结束时间

**请求参数**:

- `task_name`: 任务名称

**请求示例**:

```bash
curl http://localhost:8000/api/tasks/btc_spot_data/data_info
```

**响应示例**:

```json
{
  "task_name": "btc_spot_data",
  "data_info": [
    {
      "data_name": "binance:BTC/USDT_1h",
      "symbol": "binance:BTC/USDT",
      "timeframe": "1h",
      "start_time": "2024-01-01 00:00:00",
      "end_time": "2024-01-31 23:00:00"
    }
  ],
  "total": 1
}
```

#### 2.4.2 获取任务数据

- **方法**: GET
- **端点**: `/api/tasks/{task_name}/data`
- **描述**: 获取任务的数据

**请求参数**:

- `task_name`: 任务名称
- `data_name`: 数据名称，如 "binance:BTC/USDT_1d"（可选）
- `symbol`: 交易对，如 "binance:BTC/USDT"（可选）
- `start_time`: 起始时间，格式为 "YYYY-MM-DD HH:MM:SS"（可选）
- `end_time`: 结束时间，格式为 "YYYY-MM-DD HH:MM:SS"（可选）
- `limit`: 返回数据的最大条数，默认 1000（可选）

**请求示例**:

```bash
curl "http://localhost:8000/api/tasks/btc_spot_data/data?symbol=binance:BTC/USDT&start_time=2024-01-01&end_time=2024-01-02"
```

**响应示例**:

```json
{
  "task_name": "btc_spot_data",
  "data": [
    {
      "time": "2024-01-01 00:00:00",
      "open": 42000.0,
      "high": 42500.0,
      "low": 41800.0,
      "close": 42200.0,
      "volume": 12000.0
    },
    {
      "time": "2024-01-01 01:00:00",
      "open": 42200.0,
      "high": 42600.0,
      "low": 42000.0,
      "close": 42400.0,
      "volume": 10500.0
    }
  ],
  "total": 2,
  "limit": 1000
}
```

## 3. 请求和响应格式

### 3.1 请求格式

所有 POST 请求都需要设置 `Content-Type: application/json` 头，并以 JSON 格式发送请求体。

### 3.2 响应格式

所有 API 响应都以 JSON 格式返回，包含以下字段：

- **成功响应**: 直接返回请求的数据
- **错误响应**: 返回包含 `detail` 字段的对象

**错误响应示例**:

```json
{
  "detail": "Task name test_task already exists"
}
```

### 3.3 HTTP 状态码

| 状态码 | 描述 |
|--------|------|
| 200 OK | 请求成功 |
| 201 Created | 资源创建成功 |
| 204 No Content | 请求成功但无内容返回 |
| 400 Bad Request | 请求参数错误 |
| 404 Not Found | 资源不存在 |
| 500 Internal Server Error | 服务器内部错误 |

## 4. 错误处理

### 4.1 常见错误及解决方案

| 错误信息 | 原因 | 解决方案 |
|---------|------|----------|
| `Task name {task_name} already exists` | 任务名称已存在 | 使用不同的任务名称，或设置 `inplace=true` |
| `Data source {data_source_name} not supported` | 数据源不支持 | 检查数据源名称是否正确 |
| `Storage {storage_name} not supported` | 存储不支持 | 检查存储名称是否正确 |
| `timeframe must be one of ['1w', '1d', '4h', '1h']` | 时间框架不支持 | 使用支持的时间框架 |
| `FREDDataSource 必须包含有效的 api_key 配置` | FRED API 密钥缺失 | 提供有效的 FRED API 密钥 |
| `Task {task_name} not found` | 任务不存在 | 检查任务名称是否正确 |
| `Storage instance not found for task {task_name}` | 存储实例未找到 | 确保任务已正确创建 |

### 4.2 重试机制

对于网络不稳定的环境，建议实现重试机制：

```python
import requests
from time import sleep

def request_with_retry(url, method='GET', json=None, max_retries=3):
    retries = 0
    while retries < max_retries:
        try:
            if method == 'GET':
                response = requests.get(url)
            elif method == 'POST':
                response = requests.post(url, json=json)
            elif method == 'DELETE':
                response = requests.delete(url)
            else:
                raise ValueError(f"Unsupported method: {method}")
            
            if response.status_code < 500:
                return response
            
        except requests.exceptions.RequestException as e:
            print(f"请求失败: {e}")
        
        retries += 1
        sleep(2 ** retries)  # 指数退避
    
    raise Exception(f"达到最大重试次数 ({max_retries})")
```

## 5. API 性能优化

### 5.1 减少请求次数

- **合理缓存**: 缓存频繁访问的数据，如任务列表和插件信息
- **异步请求**: 使用异步请求并行获取数据

### 5.2 优化数据传输

- **使用适当的查询参数**: 使用 `limit` 参数限制返回数据量
- **指定时间范围**: 使用 `start_time` 和 `end_time` 参数缩小数据范围
- **只请求必要数据**: 只获取需要的数据字段

### 5.3 服务端优化

- **增加工作进程数**: 使用 `--workers` 参数增加服务工作进程数
- **优化存储配置**: 根据数据量选择合适的存储插件
- **监控资源使用**: 定期监控服务器资源使用情况

## 6. 未来 API 计划

### 6.1 计划中的 API 端点

- **批量操作 API**: 支持批量添加和删除任务
- **认证 API**: 支持 API 密钥认证
- **用户管理 API**: 支持多用户和权限管理
- **数据导出 API**: 支持导出数据为 CSV、JSON 等格式
- **数据可视化 API**: 支持生成数据图表
- **告警 API**: 支持设置任务执行告警
- **插件管理 API**: 支持上传和管理自定义插件

### 6.2 API 版本控制

未来版本将实现 API 版本控制，通过 URL 路径或请求头指定 API 版本：

- **路径版本控制**: `/api/v2/tasks`
- **请求头版本控制**: `X-API-Version: 2`

## 7. 总结

ChronoForge 提供了完整、强大的 RESTful API 接口，支持远程管理和监控任务、插件和数据。API 设计遵循 RESTful 最佳实践，提供了清晰、一致的接口风格。

通过这些 API 接口，您可以：

- 远程管理任务的创建、启动、停止和删除
- 监控任务执行状态和系统统计信息
- 管理和使用各种数据源和存储插件
- 获取和分析任务数据
- 集成 ChronoForge 到其他系统中

API 文档自动生成，通过 Swagger UI 和 ReDoc 提供了交互式的使用指南，方便开发者快速上手和测试。
