# ChronoForge 故障排查手册

## 📋 故障排查流程

```
┌─────────────────────────────────────────────────────────────┐
│                      故障排查流程                              │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  1. 问题识别 ──> 2. 信息收集 ──> 3. 根因分析                  │
│       │              │              │                         │
│       ▼              ▼              ▼                         │
│  ┌─────────┐   ┌─────────┐   ┌─────────┐                    │
│  │监控告警 │   │日志分析 │   │问题定位 │                    │
│  │用户报告 │   │指标检查 │   │假设验证 │                    │
│  └─────────┘   └─────────┘   └─────────┘                    │
│                                    │                          │
│                                    ▼                          │
│  4. 解决方案 ──> 5. 实施修复 ──> 6. 验证和复盘               │
│       │              │              │                         │
│       ▼              ▼              ▼                         │
│  ┌─────────┐   ┌─────────┐   ┌─────────┐                    │
│  │制定方案 │   │执行修复 │   │验证效果 │                    │
│  │评估影响 │   │监控变化 │   │文档记录 │                    │
│  └─────────┘   └─────────┘   └─────────┘                    │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

---

## 🚨 常见问题FAQ

### 1. 服务启动问题

#### Q: 服务无法启动，提示端口被占用

**症状**：
```
OSError: [Errno 48] Address already in use
```

**诊断步骤**：
```bash
# 1. 检查端口占用
lsof -i :8000
netstat -tulnp | grep 8000

# 2. 查找进程
ps aux | grep chronoforge

# 3. 检查Docker容器
docker ps | grep chronoforge
```

**解决方案**：
```bash
# 方案1：停止占用端口的进程
kill -9 <PID>

# 方案2：更换端口
chronoforge serve --port 8001

# 方案3：停止Docker容器
docker stop chronoforge
```

#### Q: 服务启动失败，提示权限不足

**症状**：
```
PermissionError: [Errno 13] Permission denied: '/app/data/chronoforge.duckdb'
```

**诊断步骤**：
```bash
# 1. 检查文件权限
ls -la /app/data/

# 2. 检查目录所有者
stat /app/data/

# 3. 检查当前用户
whoami
id
```

**解决方案**：
```bash
# 方案1：修改权限
chmod 755 /app/data
chmod 644 /app/data/chronoforge.duckdb

# 方案2：修改所有者
chown -R chronoforge:chronoforge /app/data

# 方案3：使用正确的用户运行
sudo -u chronoforge chronoforge serve
```

#### Q: 服务启动后立即退出

**症状**：
```
服务启动后无错误信息直接退出
```

**诊断步骤**：
```bash
# 1. 查看详细日志
journalctl -u chronoforge -n 100 --no-pager

# 2. 检查配置文件
cat /app/config/config.yaml

# 3. 检查环境变量
env | grep CHRONOFORGE

# 4. 手动启动查看错误
python -m chronoforge.server.main
```

**解决方案**：
```bash
# 方案1：修复配置文件错误
# 检查YAML语法
python -c "import yaml; yaml.safe_load(open('config.yaml'))"

# 方案2：检查依赖
pip install -r requirements.txt

# 方案3：检查Python版本
python --version  # 需要3.8+
```

---

### 2. 数据源问题

#### Q: 数据源连接超时

**症状**：
```
DataSourceTimeoutError: Connection to binance timed out after 30s
```

**诊断步骤**：
```bash
# 1. 测试网络连接
curl -I https://api.binance.com/api/v3/ping
ping api.binance.com

# 2. 检查DNS解析
nslookup api.binance.com
dig api.binance.com

# 3. 检查防火墙
sudo iptables -L -n | grep 443
sudo ufw status

# 4. 检查代理设置
env | grep -i proxy
```

**解决方案**：
```bash
# 方案1：增加超时时间
# 在配置中设置
data_source_config:
  timeout: 60
  retry_count: 3

# 方案2：配置代理
export HTTP_PROXY=http://proxy.example.com:8080
export HTTPS_PROXY=http://proxy.example.com:8080

# 方案3：检查网络限制
# 联系网络管理员确认是否有防火墙规则

# 方案4：使用备用API端点
# 在数据源配置中指定备用域名
```

#### Q: API密钥认证失败

**症状**：
```
DataSourceAuthenticationError: Invalid API key for binance
```

**诊断步骤**：
```bash
# 1. 验证API密钥格式
echo $BINANCE_API_KEY | wc -c
echo $BINANCE_API_SECRET | wc -c

# 2. 测试API密钥
curl -H "X-MBX-APIKEY: $BINANCE_API_KEY" \
  "https://api.binance.com/api/v3/account?timestamp=$(date +%s)000&signature=$(echo -n "timestamp=$(date +%s)000" | openssl dgst -sha256 -hmac "$BINANCE_API_SECRET")"

# 3. 检查环境变量
printenv | grep BINANCE

# 4. 检查密钥权限
# 登录交易所账户确认API密钥权限
```

**解决方案**：
```bash
# 方案1：重新生成API密钥
# 登录交易所重新创建API密钥

# 方案2：检查密钥权限
# 确保API密钥有读取权限

# 方案3：验证环境变量
export BINANCE_API_KEY="your_correct_key"
export BINANCE_API_SECRET="your_correct_secret"

# 方案4：检查IP白名单
# 在交易所API设置中添加服务器IP
```

#### Q: 频繁触发速率限制

**症状**：
```
DataSourceRateLimitError: Rate limit exceeded for binance
```

**诊断步骤**：
```bash
# 1. 检查请求频率
grep "rate limit" /app/logs/chronoforge.log | tail -20

# 2. 查看API使用统计
curl http://localhost:8000/api/status/stats

# 3. 检查并发任务数
curl http://localhost:8000/api/status/tasks | jq '.[] | select(.status=="running")'

# 4. 查看数据源配置
cat /app/config/config.yaml | grep -A 10 "binance"
```

**解决方案**：
```bash
# 方案1：降低请求频率
# 在配置中调整
scheduler:
  max_concurrent_tasks: 5  # 降低并发数

# 方案2：增加请求间隔
# 在任务配置中增加间隔
tasks:
  spot_tickers:
    interval_seconds: 60  # 从30秒增加到60秒

# 方案3：使用多个API密钥
# 配置多个API密钥轮询使用

# 方案4：升级API套餐
# 购买交易所的高级API套餐
```

---

### 3. 存储问题

#### Q: DuckDB数据库文件损坏

**症状**：
```
duckdb.IOException: IO Error: Cannot open file "chronoforge.duckdb" for reading: corrupted file
```

**诊断步骤**：
```bash
# 1. 检查文件完整性
ls -lh /app/data/chronoforge.duckdb
file /app/data/chronoforge.duckdb

# 2. 检查磁盘空间
df -h /app/data

# 3. 检查文件权限
stat /app/data/chronoforge.duckdb

# 4. 尝试修复
duckdb /app/data/chronoforge.duckdb "PRAGMA integrity_check;"
```

**解决方案**：
```bash
# 方案1：从备份恢复
cp /backup/chronoforge_backup.duckdb /app/data/chronoforge.duckdb

# 方案2：导出数据并重建
duckdb /app/data/chronoforge.duckdb "COPY (SELECT * FROM ohlcv) TO '/tmp/ohlcv.csv';"
rm /app/data/chronoforge.duckdb
# 重启服务会自动创建新数据库

# 方案3：使用DuckDB修复工具
duckdb /app/data/chronoforge.duckdb "PRAGMA force_checkpoint;"

# 方案4：联系DuckDB支持
# 如果数据重要，寻求专业帮助
```

#### Q: 磁盘空间不足

**症状**：
```
OSError: [Errno 28] No space left on device
```

**诊断步骤**：
```bash
# 1. 检查磁盘使用
df -h
du -sh /app/data/*
du -sh /app/logs/*

# 2. 查找大文件
find /app/data -type f -size +100M -exec ls -lh {} \;

# 3. 检查数据库大小
duckdb /app/data/chronoforge.duckdb "SELECT table_name, estimated_size FROM duckdb_tables();"

# 4. 检查日志文件大小
ls -lh /app/logs/
```

**解决方案**：
```bash
# 方案1：清理旧日志
find /app/logs -name "*.log" -mtime +30 -delete
gzip /app/logs/*.log

# 方案2：清理旧数据
# 删除90天前的数据
duckdb /app/data/chronoforge.duckdb "DELETE FROM ohlcv WHERE time < NOW() - INTERVAL 90 DAY;"

# 方案3：压缩数据库
duckdb /app/data/chronoforge.duckdb "VACUUM;"

# 方案4：扩容磁盘
# 在云平台上扩容磁盘
# 或迁移到更大的磁盘
```

#### Q: 数据写入性能缓慢

**症状**：
```
任务执行时间过长，数据写入缓慢
```

**诊断步骤**：
```bash
# 1. 检查磁盘I/O
iostat -x 1 10

# 2. 检查系统负载
top -p $(pgrep -f chronoforge)

# 3. 检查数据库性能
duckdb /app/data/chronoforge.duckdb "EXPLAIN ANALYZE SELECT * FROM ohlcv LIMIT 1000;"

# 4. 检查索引
duckdb /app/data/chronoforge.duckdb "SELECT * FROM duckdb_indexes();"
```

**解决方案**：
```bash
# 方案1：优化数据库配置
# 在配置中调整
storage_config:
  memory_limit: "8GB"
  threads: 8
  checkpoint_threshold: "50GB"

# 方案2：创建索引
duckdb /app/data/chronoforge.duckdb "CREATE INDEX idx_ohlcv_time ON ohlcv(time);"

# 方案3：批量插入
# 修改代码使用批量插入而不是单条插入

# 方案4：使用SSD
# 迁移到SSD磁盘以提高I/O性能
```

---

### 4. 任务调度问题

#### Q: 任务未按预期执行

**症状**：
```
任务配置了但从未执行
```

**诊断步骤**：
```bash
# 1. 检查任务状态
curl http://localhost:8000/api/status/tasks

# 2. 检查任务配置
curl http://localhost:8000/api/tasks

# 3. 查看调度器日志
grep "scheduler" /app/logs/chronoforge.log | tail -50

# 4. 检查时间槽配置
# 确认当前时间是否在时间槽范围内
```

**解决方案**：
```bash
# 方案1：检查任务是否启用
# 在配置中确认
tasks:
  spot_tickers:
    enabled: true  # 确保为true

# 方案2：检查时间槽配置
# 确保时间槽包含当前时间
time_slot:
  start: "00:00"
  end: "23:59"

# 方案3：手动触发任务
curl -X POST http://localhost:8000/api/tasks/spot_tickers/start

# 方案4：重启调度器
sudo systemctl restart chronoforge
```

#### Q: 任务执行失败

**症状**：
```
Task failed with error: ...
```

**诊断步骤**：
```bash
# 1. 查看任务错误详情
curl http://localhost:8000/api/status/tasks | jq '.[] | select(.status=="failed")'

# 2. 查看错误日志
grep "ERROR" /app/logs/chronoforge.log | tail -20

# 3. 检查任务执行历史
curl http://localhost:8000/api/tasks/spot_tickers/history

# 4. 检查依赖服务
# 确认数据源、存储等服务正常
```

**解决方案**：
```bash
# 方案1：根据错误信息修复
# 查看具体错误信息并针对性修复

# 方案2：增加重试次数
# 在任务配置中
tasks:
  spot_tickers:
    retry_count: 3
    retry_delay_seconds: 5

# 方案3：检查资源限制
# 确认内存、CPU等资源充足

# 方案4：联系支持
# 如果问题持续，查看GitHub Issues或联系支持
```

#### Q: 任务队列堆积

**症状**：
```
大量任务等待执行，队列持续增长
```

**诊断步骤**：
```bash
# 1. 检查队列大小
curl http://localhost:8000/api/status/stats | jq '.tasks'

# 2. 检查正在运行的任务
curl http://localhost:8000/api/status/tasks | jq '.[] | select(.status=="running")'

# 3. 检查系统资源
top -p $(pgrep -f chronoforge)

# 4. 检查任务执行时长
grep "duration" /app/logs/chronoforge.log | tail -20
```

**解决方案**：
```bash
# 方案1：增加并发数
# 在配置中调整
scheduler:
  max_concurrent_tasks: 20  # 增加并发数

# 方案2：优化任务执行
# 检查是否有慢任务，优化代码

# 方案3：清理队列
# 删除不必要的任务
curl -X DELETE http://localhost:8000/api/tasks/unnecessary_task

# 方案4：扩容资源
# 增加CPU和内存资源
```

---

### 5. API问题

#### Q: API响应缓慢

**症状**：
```
API请求响应时间超过5秒
```

**诊断步骤**：
```bash
# 1. 测试API响应时间
curl -w "@curl-format.txt" -o /dev/null -s http://localhost:8000/api/status

# 2. 检查系统负载
top -p $(pgrep -f chronoforge)

# 3. 检查数据库查询
duckdb /app/data/chronoforge.duckdb "EXPLAIN ANALYZE SELECT * FROM ohlcv LIMIT 100;"

# 4. 检查网络延迟
ping localhost
```

**解决方案**：
```bash
# 方案1：优化查询
# 添加索引
duckdb /app/data/chronoforge.duckdb "CREATE INDEX idx_time ON ohlcv(time);"

# 方案2：增加缓存
# 在API层添加缓存

# 方案3：增加worker数量
chronoforge serve --workers 8

# 方案4：使用负载均衡
# 部署多个实例并使用负载均衡
```

#### Q: API返回500错误

**症状**：
```
HTTP 500 Internal Server Error
```

**诊断步骤**：
```bash
# 1. 查看详细错误
curl -v http://localhost:8000/api/tasks

# 2. 检查日志
tail -100 /app/logs/chronoforge.log

# 3. 检查异常堆栈
grep "Traceback" /app/logs/chronoforge.log | tail -50

# 4. 检查依赖服务
# 确认数据库、Redis等服务正常
```

**解决方案**：
```bash
# 方案1：根据错误信息修复
# 查看具体错误并针对性修复

# 方案2：检查配置
# 确认配置文件正确

# 方案3：重启服务
sudo systemctl restart chronoforge

# 方案4：回滚版本
# 如果是新版本问题，回滚到上一个稳定版本
```

---

## 🔍 诊断工具

### 1. 日志分析工具

#### 日志搜索脚本

```bash
#!/bin/bash
# log_search.sh

LOG_FILE="/app/logs/chronoforge.log"

# 搜索错误日志
search_errors() {
    grep -E "ERROR|CRITICAL" $LOG_FILE | tail -50
}

# 搜索特定任务
search_task() {
    local task_name=$1
    grep "task_name=$task_name" $LOG_FILE | tail -20
}

# 搜索特定时间段
search_time_range() {
    local start_time=$1
    local end_time=$2
    awk -v start="$start_time" -v end="$end_time" \
        '$1 " " $2 >= start && $1 " " $2 <= end' $LOG_FILE
}

# 统计错误类型
error_stats() {
    grep "ERROR" $LOG_FILE | \
        awk '{print $NF}' | \
        sort | uniq -c | sort -rn
}

# 使用示例
case "$1" in
    errors)
        search_errors
        ;;
    task)
        search_task $2
        ;;
    time)
        search_time_range $2 $3
        ;;
    stats)
        error_stats
        ;;
    *)
        echo "Usage: $0 {errors|task <name>|time <start> <end>|stats}"
        ;;
esac
```

### 2. 性能分析工具

#### 性能监控脚本

```bash
#!/bin/bash
# performance_monitor.sh

# 监控CPU和内存
monitor_resources() {
    while true; do
        echo "=== $(date) ==="
        ps aux | grep chronoforge | grep -v grep | \
            awk '{printf "CPU: %.1f%%, MEM: %.1f%%, RSS: %dMB\n", $3, $4, $6/1024}'
        sleep 5
    done
}

# 监控网络连接
monitor_connections() {
    while true; do
        echo "=== $(date) ==="
        netstat -an | grep :8000 | wc -l
        sleep 5
    done
}

# 监控磁盘I/O
monitor_disk_io() {
    iostat -x 5
}

# 使用示例
case "$1" in
    resources)
        monitor_resources
        ;;
    connections)
        monitor_connections
        ;;
    disk)
        monitor_disk_io
        ;;
    *)
        echo "Usage: $0 {resources|connections|disk}"
        ;;
esac
```

### 3. 健康检查脚本

```bash
#!/bin/bash
# health_check.sh

# 检查服务状态
check_service() {
    echo "Checking service status..."
    response=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/api/status)
    if [ $response -eq 200 ]; then
        echo "✅ Service is healthy"
    else
        echo "❌ Service is unhealthy (HTTP $response)"
    fi
}

# 检查数据库
check_database() {
    echo "Checking database..."
    if duckdb /app/data/chronoforge.duckdb "SELECT 1;" > /dev/null 2>&1; then
        echo "✅ Database is accessible"
    else
        echo "❌ Database is not accessible"
    fi
}

# 检查磁盘空间
check_disk_space() {
    echo "Checking disk space..."
    usage=$(df -h /app/data | awk 'NR==2 {print $5}' | sed 's/%//')
    if [ $usage -lt 80 ]; then
        echo "✅ Disk usage is OK ($usage%)"
    else
        echo "⚠️  Disk usage is high ($usage%)"
    fi
}

# 检查内存
check_memory() {
    echo "Checking memory..."
    total=$(free -m | awk 'NR==2 {print $2}')
    used=$(free -m | awk 'NR==2 {print $3}')
    usage=$((used * 100 / total))
    if [ $usage -lt 80 ]; then
        echo "✅ Memory usage is OK ($usage%)"
    else
        echo "⚠️  Memory usage is high ($usage%)"
    fi
}

# 运行所有检查
run_all_checks() {
    echo "=== ChronoForge Health Check ==="
    echo ""
    check_service
    check_database
    check_disk_space
    check_memory
    echo ""
    echo "=== Check Complete ==="
}

# 使用示例
run_all_checks
```

---

## 🚨 应急响应手册

### 1. 服务宕机

**响应流程**：
```bash
# 1. 确认服务状态
sudo systemctl status chronoforge

# 2. 查看错误日志
journalctl -u chronoforge -n 100 --no-pager

# 3. 尝试重启服务
sudo systemctl restart chronoforge

# 4. 如果重启失败，检查配置
chronoforge serve --config /app/config/config.yaml

# 5. 如果仍然失败，回滚版本
git checkout <previous_stable_version>
sudo systemctl restart chronoforge
```

### 2. 数据丢失

**响应流程**：
```bash
# 1. 立即停止服务
sudo systemctl stop chronoforge

# 2. 评估数据丢失范围
ls -lh /app/data/
duckdb /app/data/chronoforge.duckdb "SELECT COUNT(*) FROM ohlcv;"

# 3. 从备份恢复
cp /backup/chronoforge_backup.duckdb /app/data/chronoforge.duckdb

# 4. 验证数据完整性
duckdb /app/data/chronoforge.duckdb "PRAGMA integrity_check;"

# 5. 重启服务
sudo systemctl start chronoforge
```

### 3. 性能严重下降

**响应流程**：
```bash
# 1. 检查系统资源
top -p $(pgrep -f chronoforge)
iostat -x 1

# 2. 检查慢查询
duckdb /app/data/chronoforge.duckdb "SELECT * FROM duckdb_queries();"

# 3. 临时降低负载
# 减少并发任务数
curl -X PUT http://localhost:8000/api/config \
  -d '{"max_concurrent_tasks": 5}'

# 4. 清理资源
duckdb /app/data/chronoforge.duckdb "VACUUM;"

# 5. 如果必要，重启服务
sudo systemctl restart chronoforge
```

---

## 📊 性能调优指南

### 1. 系统级优化

```bash
# 增加文件描述符限制
ulimit -n 65536

# 优化TCP参数
sudo sysctl -w net.core.somaxconn=65535
sudo sysctl -w net.ipv4.tcp_max_syn_backlog=65535

# 优化内存管理
sudo sysctl -w vm.swappiness=10
sudo sysctl -w vm.dirty_ratio=15
```

### 2. 应用级优化

```python
# 优化配置
config = {
    "scheduler": {
        "max_concurrent_tasks": 20,  # 根据CPU核心数调整
        "default_timeout": 300,
    },
    "storage": {
        "memory_limit": "8GB",  # 根据可用内存调整
        "threads": 8,  # 根据CPU核心数调整
    },
    "data_source": {
        "timeout": 60,
        "retry_count": 3,
        "retry_delay": 5,
    }
}
```

### 3. 数据库优化

```sql
-- 创建索引
CREATE INDEX idx_ohlcv_time ON ohlcv(time);
CREATE INDEX idx_ohlcv_symbol ON ohlcv(symbol);

-- 分析表
ANALYZE ohlcv;

-- 清理和优化
VACUUM;
CHECKPOINT;
```

---

## ✅ 故障排查检查清单

### 服务启动问题
- [ ] 检查端口是否被占用
- [ ] 检查文件权限
- [ ] 检查配置文件语法
- [ ] 检查依赖是否安装
- [ ] 检查Python版本

### 数据源问题
- [ ] 检查网络连接
- [ ] 检查API密钥有效性
- [ ] 检查速率限制
- [ ] 检查代理配置
- [ ] 检查防火墙规则

### 存储问题
- [ ] 检查磁盘空间
- [ ] 检查文件权限
- [ ] 检查数据库完整性
- [ ] 检查索引
- [ ] 检查I/O性能

### 任务调度问题
- [ ] 检查任务配置
- [ ] 检查时间槽设置
- [ ] 检查任务状态
- [ ] 检查系统资源
- [ ] 检查依赖服务

### API问题
- [ ] 检查服务状态
- [ ] 检查日志错误
- [ ] 检查数据库查询
- [ ] 检查系统负载
- [ ] 检查网络延迟

---

## 📞 获取帮助

### 日志收集

```bash
# 收集诊断信息
./scripts/collect_diagnostics.sh > diagnostics_$(date +%Y%m%d_%H%M%S).tar.gz
```

### 联系支持

1. **GitHub Issues**: [https://github.com/your-org/chronoforge/issues](https://github.com/your-org/chronoforge/issues)
2. **文档**: [https://chronoforge.readthedocs.io](https://chronoforge.readthedocs.io)
3. **社区**: [Slack频道](https://chronoforge.slack.com)

### 提交Issue时请提供

- 系统信息（OS、Python版本）
- ChronoForge版本
- 完整的错误日志
- 复现步骤
- 配置文件（去除敏感信息）

---

## 📚 参考资料

- [DuckDB故障排查](https://duckdb.org/docs/operations/overview)
- [FastAPI调试指南](https://fastapi.tiangolo.com/tutorial/debugging/)
- [Python异步编程调试](https://docs.python.org/3/library/asyncio-dev.html)
- [系统性能分析](https://www.brendangregg.com/linuxperf.html)
