# ChronoForge 示例程序

本目录包含了展示ChronoForge框架各种功能的示例程序。这些示例涵盖了从基础使用到高级功能的完整演示。

## 📋 示例列表

### 🚀 基础示例

#### 1. [auto_periodic_tasks_example.py](auto_periodic_tasks_example.py)
**自动创建周期性任务功能演示**
- 展示如何配置数据源以自动创建周期性任务
- 支持多种数据源类型（加密货币、经济数据、情绪指标）
- 实时监控任务执行情况
- 详细的配置选项说明

#### 2. [complete_workflow_example.py](complete_workflow_example.py)
**完整工作流综合演示**
- 多样化数据源创建和配置
- 自动任务生成和手动任务创建
- 自定义数据源和装饰器任务
- 全面的任务执行监控
- 数据处理管道演示
- 系统性能分析和总结

#### 3. [create_task_example.py](create_task_example.py)
**使用@create_task装饰器创建任务**
- 展示如何使用@create_task装饰器
- 周期性任务和时间槽任务的创建
- 任务配置和参数说明
- 向后兼容性说明

### 🔌 API和插件示例

#### 4. [plugin_functions_example.py](plugin_functions_example.py)
**插件函数API演示**
- 展示如何调用数据源的插件函数
- 获取数据源支持的函数列表
- 代理调用插件函数
- 结果展示和错误处理

#### 5. [add_tasks_to_server.py](add_tasks_to_server.py)
**通过RESTful API添加任务**
- 启动ChronoForge服务
- 通过API添加多种类型的任务
- 任务状态监控和管理
- 支持加密货币、经济数据、全球市场数据

### 📊 数据和存储示例

#### 6. [get_task_data.py](get_task_data.py)
**获取任务数据**
- 展示如何获取任务执行的数据
- 数据查询和过滤
- 结果展示和格式化

#### 7. [get_task_data_info.py](get_task_data_info.py)
**获取任务数据信息**
- 获取任务数据的元信息
- 数据范围和时间信息
- 存储位置和数据格式

#### 8. [storage_performance_comparison.py](storage_performance_comparison.py)
**存储性能对比**
- 比较不同存储后端的性能
- 读写速度测试
- 内存使用和效率分析

### 🛠️ 系统管理示例

#### 9. [task_monitor.py](task_monitor.py)
**任务监控工具**
- 实时监控任务执行状态
- 性能指标和错误统计
- 可视化任务状态变化

#### 10. [embeded.py](embeded.py)
**嵌入式使用示例**
- 在应用程序中嵌入ChronoForge
- 最小化配置和启动
- 资源管理和清理

## 🎯 快速开始

### 环境准备

确保已安装ChronoForge及其依赖：

```bash
pip install -e .
```

### 运行示例

每个示例都可以独立运行：

```bash
# 自动周期性任务演示
python examples/auto_periodic_tasks_example.py

# 完整工作流演示
python examples/complete_workflow_example.py

# 插件函数API演示
python examples/plugin_functions_example.py
```

## 📖 示例详解

### 自动周期性任务示例

这个示例展示了ChronoForge最强大的功能之一：**自动创建周期性任务**。

```python
# 配置数据源启用自动任务创建
crypto_config = {
    'exchange_name': 'binance',
    'default_config': {
        'auto_create_periodic_tasks': True,  # 启用自动创建
        'periodic_task_config': {
            'interval': 60,      # 60秒间隔
            'symbols': ['BTC/USDT', 'ETH/USDT'],
            'timeframe': '1h',
            'exchange_name': 'binance'
        }
    }
}

# 创建数据源
crypto_data_source = CryptoSpotDataSource(crypto_config)

# 注册数据源（自动创建周期性任务）
ultimate_task_manager.register_data_source_instance("crypto_auto", crypto_data_source)
```

### 完整工作流示例

这个示例展示了ChronoForge的完整生态系统：

1. **多样化数据源**：加密货币、经济数据、情绪指标、全球市场
2. **混合任务类型**：自动任务、手动任务、装饰器任务
3. **多存储后端**：本地文件、DuckDB、Redis、MongoDB
4. **实时监控**：任务状态、执行统计、性能指标
5. **数据处理**：技术指标、数据清洗、聚合分析

### 插件函数示例

展示如何通过API调用数据源的插件函数：

```python
# 获取数据源函数列表
functions = get_data_source_functions("CryptoSpotDataSource")

# 代理调用插件函数
result = delegate_call_plugin_function(
    plugin_name="CryptoSpotDataSource",
    plugin_type="data_source",
    function_name="top_volume_symbols",
    exchange_name="binance",
    quote="USDT",
    top_n=10
)
```

## 🔧 高级功能

### 自定义数据源

可以通过继承`DataSourceBase`创建自定义数据源：

```python
class MyDataSource(DataSourceBase):
    @property
    def name(self):
        return "MyDataSource"
    
    async def fetch(self, symbol, timeframe, start_ts_ms, end_ts_ms=None):
        # 实现数据获取逻辑
        return pd.DataFrame()
    
    @api_callable
    def my_custom_function(self, param1, param2):
        # 自定义API函数
        return {"result": "success"}
    
    @create_task(
        interval=300,
        symbols=['CUSTOM_SYMBOL'],
        timeframe='1h',
        storage_name="LocalFileStorage"
    )
    async def my_periodic_task(self):
        # 周期性任务
        return pd.DataFrame()
```

### 任务配置

支持多种任务配置方式：

```python
# 时间槽任务
scheduler.add_task(
    name="my_task",
    data_source_name="CryptoSpotDataSource",
    storage_name="DuckDBStorage",
    time_slot=TimeSlot('09:00:00', '17:00:00'),  # 交易时段
    symbols=['BTC/USDT'],
    timeframe='1h'
)

# 周期性任务（通过装饰器）
@create_task(
    interval=3600,  # 每小时
    symbols=['BTC/USDT'],
    timeframe='1h',
    storage_name="LocalFileStorage"
)
async def my_periodic_task(self):
    return await self.fetch('BTC/USDT', '1h', start_time, end_time)
```

### 存储配置

支持多种存储后端配置：

```python
# DuckDB存储
duckdb_config = {
    'db_path': './data/market_data.db',
    'read_only': False
}

# MongoDB存储
mongodb_config = {
    'connection_string': 'mongodb://localhost:27017/',
    'database': 'chronoforge',
    'collection': 'market_data'
}

# Redis存储
redis_config = {
    'host': 'localhost',
    'port': 6379,
    'db': 0
}
```

## 📊 性能优化建议

### 1. 连接池优化
- 合理设置最大连接数（默认20个）
- 启用连接复用以提高性能
- 监控连接使用情况

### 2. 任务调度优化
- 合理设置任务执行间隔
- 避免过于频繁的任务调度
- 使用时间槽任务减少资源浪费

### 3. 存储优化
- 选择合适的存储后端
- 启用数据压缩减少存储空间
- 定期清理过期数据

### 4. 内存管理
- 合理设置缓存大小
- 及时清理不需要的数据
- 监控内存使用情况

## 🔍 故障排除

### 常见问题

1. **服务启动失败**
   - 检查端口是否被占用
   - 验证配置文件格式
   - 查看日志获取详细信息

2. **任务执行失败**
   - 检查数据源配置
   - 验证API密钥和网络连接
   - 查看任务状态和错误信息

3. **性能问题**
   - 监控系统资源使用情况
   - 优化任务调度配置
   - 考虑使用更高效的存储后端

### 调试技巧

- 启用详细日志记录：`logging.basicConfig(level=logging.DEBUG)`
- 使用任务监控工具查看实时状态
- 检查API响应和错误信息
- 验证数据源连接和认证

## 📚 更多资源

- [ChronoForge文档](../docs/)
- [API参考](../docs/api.md)
- [架构设计](../docs/architecture.md)
- [贡献指南](../CONTRIBUTING.md)

## 🤝 贡献

欢迎提交新的示例程序和改进建议！请遵循以下步骤：

1. Fork项目仓库
2. 创建特性分支：`git checkout -b feature/new-example`
3. 提交更改：`git commit -am 'Add new example'`
4. 推送到分支：`git push origin feature/new-example`
5. 提交Pull Request

## 📄 许可证

本示例代码遵循与ChronoForge相同的许可证条款。详见[LICENSE](../LICENSE)文件。