# ChronoForge自运行模式实现方案

## 1. 方案概述

为ChronoForge添加自运行模式，使其可以作为独立服务运行，通过RESTful API提供对调度器功能的访问。

## 2. 技术选型

* **Web框架**：FastAPI（支持异步、自动生成API文档、易于集成）

* **API风格**：RESTful

* **命令行工具**：Python标准库`argparse`

* **服务管理**：使用异步方式管理Scheduler实例生命周期

## 3. 实现内容

### 3.1 目录结构扩展

```
chronoforge/
├── server/
│   ├── __init__.py
│   ├── main.py          # FastAPI应用入口
│   ├── api/             # API路由
│   │   ├── __init__.py
│   │   ├── tasks.py     # 任务管理API
│   │   ├── plugins.py   # 插件管理API
│   │   └── status.py    # 状态查询API
│   └── models/          # Pydantic模型
│       ├── __init__.py
│       ├── task.py      # 任务相关模型
│       └── plugin.py    # 插件相关模型
└── cli.py               # 命令行工具
```

### 3.2 核心功能实现

#### 3.2.1 命令行工具 (`cli.py`)

* 提供`serve`命令启动HTTP服务

* 支持配置服务端口、主机等参数

* 管理服务生命周期

#### 3.2.2 FastAPI应用 (`server/main.py`)

* 创建FastAPI实例

* 集成CORS支持

* 注册API路由

* 管理Scheduler实例

#### 3.2.3 API路由

**任务管理API (`/api/tasks`)**

* `GET /api/tasks` - 列出所有任务

* `POST /api/tasks` - 创建新任务

* `GET /api/tasks/{task_name}` - 获取任务详情

* `DELETE /api/tasks/{task_name}` - 删除任务

**插件管理API (`/api/plugins`)**

* `GET /api/plugins` - 列出所有支持的插件

* `GET /api/plugins/{plugin_type}` - 按类型列出插件

**状态查询API (`/api/status`)**

* `GET /api/status` - 获取服务状态

* `GET /api/status/tasks` - 获取所有任务状态

### 3.3 依赖管理

* 添加`fastapi`和`uvicorn`到项目依赖

* 确保异步兼容性

## 4. 使用方式

### 4.1 启动服务

```bash
python -m chronoforge serve --host 0.0.0.0 --port 8000
```

### 4.2 API访问

* 服务启动后，可通过`http://localhost:8000/docs`访问自动生成的API文档

* 使用HTTP客户端（如curl、Postman）调用API

### 4.3 示例API调用

```bash
# 创建任务
curl -X POST http://localhost:8000/api/tasks \
  -H "Content-Type: application/json" \
  -d '{"name": "test_task", "data_source_name": "CryptoSpotDataSource", "storage_name": "LocalFileStorage", "time_slot": {"start_time": "00:00", "end_time": "23:59"}, "symbols": ["binance:BTC/USDT"], "timeframe": "1d"}'

# 列出任务
curl http://localhost:8000/api/tasks
```

## 5. 实现步骤

1. 添加FastAPI和uvicorn依赖
2. 创建server目录结构
3. 实现Pydantic模型
4. 实现API路由
5. 实现命令行工具
6. 测试服务启动和API调用
7. 更新文档

## 6. 预期效果

* ChronoForge可以作为独立服务运行

* 提供完整的RESTful API访问调度器功能

* 自动生成API文档，方便使用和调试

* 支持异步操作，性能优秀

* 易于部署和管理

