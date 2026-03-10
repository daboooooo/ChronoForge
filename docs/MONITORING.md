# ChronoForge 监控和可观测性指南

## 📊 监控架构

ChronoForge采用现代化的可观测性架构，包含三大支柱：

```
┌─────────────────────────────────────────────────────────────┐
│                      可观测性架构                              │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │   指标监控    │  │   日志聚合    │  │   分布式追踪   │     │
│  │  (Metrics)   │  │   (Logs)     │  │  (Traces)    │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
│         │                  │                  │              │
│         ▼                  ▼                  ▼              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │  Prometheus  │  │ Elasticsearch│  │    Jaeger    │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
│         │                  │                  │              │
│         └──────────────────┼──────────────────┘              │
│                            ▼                                  │
│                    ┌──────────────┐                          │
│                    │   Grafana    │                          │
│                    │  (可视化)     │                          │
│                    └──────────────┘                          │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

---

## 🎯 监控指标

### 核心指标

ChronoForge暴露以下核心指标：

#### 1. 系统指标

| 指标名称 | 类型 | 描述 | 标签 |
|---------|------|------|------|
| `chronoforge_system_cpu_usage_percent` | Gauge | CPU使用率 | - |
| `chronoforge_system_memory_usage_bytes` | Gauge | 内存使用量 | - |
| `chronoforge_system_disk_usage_bytes` | Gauge | 磁盘使用量 | path |
| `chronoforge_system_uptime_seconds` | Gauge | 服务运行时间 | - |

#### 2. 任务指标

| 指标名称 | 类型 | 描述 | 标签 |
|---------|------|------|------|
| `chronoforge_tasks_total` | Counter | 任务总数 | status |
| `chronoforge_tasks_running` | Gauge | 正在运行的任务数 | - |
| `chronoforge_tasks_completed_total` | Counter | 已完成任务数 | status |
| `chronoforge_tasks_failed_total` | Counter | 失败任务数 | error_type |
| `chronoforge_task_duration_seconds` | Histogram | 任务执行时长 | task_name, status |
| `chronoforge_task_queue_size` | Gauge | 任务队列大小 | priority |

#### 3. 数据源指标

| 指标名称 | 类型 | 描述 | 标签 |
|---------|------|------|------|
| `chronoforge_datasource_requests_total` | Counter | 数据源请求总数 | datasource, status |
| `chronoforge_datasource_request_duration_seconds` | Histogram | 请求时长 | datasource |
| `chronoforge_datasource_rate_limit_hits_total` | Counter | 速率限制命中次数 | datasource |
| `chronoforge_datasource_records_fetched_total` | Counter | 获取的记录数 | datasource |
| `chronoforge_datasource_errors_total` | Counter | 数据源错误数 | datasource, error_type |

#### 4. 存储指标

| 指标名称 | 类型 | 描述 | 标签 |
|---------|------|------|------|
| `chronoforge_storage_operations_total` | Counter | 存储操作总数 | storage, operation, status |
| `chronoforge_storage_operation_duration_seconds` | Histogram | 存储操作时长 | storage, operation |
| `chronoforge_storage_records_stored_total` | Counter | 存储的记录数 | storage |
| `chronoforge_storage_size_bytes` | Gauge | 存储大小 | storage |
| `chronoforge_storage_errors_total` | Counter | 存储错误数 | storage, error_type |

#### 5. API指标

| 指标名称 | 类型 | 描述 | 标签 |
|---------|------|------|------|
| `chronoforge_http_requests_total` | Counter | HTTP请求总数 | method, endpoint, status |
| `chronoforge_http_request_duration_seconds` | Histogram | HTTP请求时长 | method, endpoint |
| `chronoforge_http_requests_in_flight` | Gauge | 正在处理的请求数 | - |

---

## 🔧 Prometheus配置

### 1. 安装Prometheus

```bash
# 使用Docker
docker run -d \
  --name prometheus \
  -p 9091:9090 \
  -v $(pwd)/prometheus.yml:/etc/prometheus/prometheus.yml \
  prom/prometheus:latest
```

### 2. 配置文件

```yaml
# prometheus.yml
global:
  scrape_interval: 15s
  evaluation_interval: 15s
  external_labels:
    monitor: 'chronoforge-monitor'

# 告警规则文件
rule_files:
  - 'alert_rules.yml'

# 抓取配置
scrape_configs:
  # ChronoForge应用
  - job_name: 'chronoforge'
    static_configs:
      - targets: ['chronoforge:9090']
    metrics_path: '/metrics'
    scrape_interval: 10s
    scrape_timeout: 5s

  # Prometheus自身
  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9090']

  # Node Exporter（系统指标）
  - job_name: 'node'
    static_configs:
      - targets: ['node-exporter:9100']

  # Redis监控
  - job_name: 'redis'
    static_configs:
      - targets: ['redis-exporter:9121']
```

### 3. 告警规则

```yaml
# alert_rules.yml
groups:
  - name: chronoforge_alerts
    interval: 30s
    rules:
      # 服务可用性告警
      - alert: ChronoForgeDown
        expr: up{job="chronoforge"} == 0
        for: 5m
        labels:
          severity: critical
          team: platform
        annotations:
          summary: "ChronoForge服务不可用"
          description: "ChronoForge实例 {{ $labels.instance }} 已经宕机超过5分钟"
          runbook_url: "https://wiki.example.com/runbooks/chronoforge-down"

      # 高错误率告警
      - alert: HighErrorRate
        expr: |
          sum(rate(chronoforge_tasks_failed_total[5m])) by (error_type)
          /
          sum(rate(chronoforge_tasks_total[5m])) 
          > 0.1
        for: 10m
        labels:
          severity: warning
          team: platform
        annotations:
          summary: "ChronoForge错误率过高"
          description: "过去10分钟错误率为 {{ $value | humanizePercentage }}"

      # 任务执行缓慢告警
      - alert: SlowTaskExecution
        expr: |
          histogram_quantile(0.95, 
            sum(rate(chronoforge_task_duration_seconds_bucket[5m])) by (le, task_name)
          ) > 300
        for: 15m
        labels:
          severity: warning
          team: platform
        annotations:
          summary: "任务执行缓慢"
          description: "任务 {{ $labels.task_name }} 的P95执行时间超过300秒"

      # 数据源请求失败告警
      - alert: DataSourceFailures
        expr: |
          sum(rate(chronoforge_datasource_errors_total[5m])) by (datasource, error_type)
          > 0.05
        for: 10m
        labels:
          severity: warning
          team: data
        annotations:
          summary: "数据源请求失败率过高"
          description: "数据源 {{ $labels.datasource }} 的错误类型 {{ $labels.error_type }} 失败率过高"

      # 存储空间不足告警
      - alert: StorageSpaceLow
        expr: |
          chronoforge_storage_size_bytes / (1024 * 1024 * 1024) > 400
        for: 5m
        labels:
          severity: warning
          team: platform
        annotations:
          summary: "存储空间不足"
          description: "存储大小已达到 {{ $value | humanize }}GB"

      # 内存使用过高告警
      - alert: HighMemoryUsage
        expr: |
          chronoforge_system_memory_usage_bytes / (1024 * 1024 * 1024) > 12
        for: 10m
        labels:
          severity: warning
          team: platform
        annotations:
          summary: "内存使用过高"
          description: "内存使用已达到 {{ $value | humanize }}GB"

      # 任务队列堆积告警
      - alert: TaskQueueBacklog
        expr: chronoforge_task_queue_size > 100
        for: 15m
        labels:
          severity: warning
          team: platform
        annotations:
          summary: "任务队列堆积"
          description: "任务队列中有 {{ $value }} 个任务等待执行"

      # API响应缓慢告警
      - alert: SlowAPIResponse
        expr: |
          histogram_quantile(0.95,
            sum(rate(chronoforge_http_request_duration_seconds_bucket[5m])) by (le, endpoint)
          ) > 5
        for: 10m
        labels:
          severity: warning
          team: platform
        annotations:
          summary: "API响应缓慢"
          description: "端点 {{ $labels.endpoint }} 的P95响应时间超过5秒"
```

---

## 📈 Grafana仪表板

### 1. 安装Grafana

```bash
# 使用Docker
docker run -d \
  --name grafana \
  -p 3000:3000 \
  -v grafana-storage:/var/lib/grafana \
  grafana/grafana:latest
```

### 2. 数据源配置

```yaml
# datasources.yml
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: false
```

### 3. 仪表板JSON

创建 `grafana-dashboard.json` 文件（内容较长，见 `monitoring/grafana-dashboard.json`）

### 4. 关键面板

#### 系统概览面板

```json
{
  "title": "系统概览",
  "panels": [
    {
      "title": "CPU使用率",
      "type": "gauge",
      "targets": [
        {
          "expr": "chronoforge_system_cpu_usage_percent"
        }
      ]
    },
    {
      "title": "内存使用",
      "type": "gauge",
      "targets": [
        {
          "expr": "chronoforge_system_memory_usage_bytes / (1024*1024*1024)"
        }
      ]
    },
    {
      "title": "运行时间",
      "type": "stat",
      "targets": [
        {
          "expr": "chronoforge_system_uptime_seconds"
        }
      ]
    }
  ]
}
```

#### 任务执行面板

```json
{
  "title": "任务执行",
  "panels": [
    {
      "title": "任务成功率",
      "type": "graph",
      "targets": [
        {
          "expr": "sum(rate(chronoforge_tasks_completed_total[5m])) / sum(rate(chronoforge_tasks_total[5m])) * 100"
        }
      ]
    },
    {
      "title": "任务执行时长分布",
      "type": "heatmap",
      "targets": [
        {
          "expr": "sum(rate(chronoforge_task_duration_seconds_bucket[5m])) by (le)"
        }
      ]
    },
    {
      "title": "正在运行的任务",
      "type": "stat",
      "targets": [
        {
          "expr": "chronoforge_tasks_running"
        }
      ]
    }
  ]
}
```

---

## 📝 日志管理

### 1. 结构化日志配置

```python
# logging_config.py
import logging
import json
from datetime import datetime

class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }
        
        if hasattr(record, 'task_name'):
            log_entry['task_name'] = record.task_name
        if hasattr(record, 'datasource'):
            log_entry['datasource'] = record.datasource
        if hasattr(record, 'duration_ms'):
            log_entry['duration_ms'] = record.duration_ms
            
        if record.exc_info:
            log_entry['exception'] = self.formatException(record.exc_info)
            
        return json.dumps(log_entry)

# 配置日志
def setup_logging():
    handler = logging.StreamHandler()
    handler.setFormatter(JSONFormatter())
    
    logger = logging.getLogger('chronoforge')
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    
    return logger
```

### 2. ELK Stack配置

#### Logstash配置

```ruby
# logstash.conf
input {
  tcp {
    port => 5000
    codec => json_lines
  }
}

filter {
  if [logger] =~ "chronoforge" {
    grok {
      match => {
        "message" => "%{GREEDYDATA:log_message}"
      }
    }
    
    date {
      match => ["timestamp", "ISO8601"]
      target => "@timestamp"
    }
    
    if [task_name] {
      mutate {
        add_field => { "task" => "%{task_name}" }
      }
    }
  }
}

output {
  elasticsearch {
    hosts => ["elasticsearch:9200"]
    index => "chronoforge-%{+YYYY.MM.dd}"
  }
}
```

#### Elasticsearch索引模板

```json
{
  "index_patterns": ["chronoforge-*"],
  "mappings": {
    "properties": {
      "@timestamp": { "type": "date" },
      "level": { "type": "keyword" },
      "logger": { "type": "keyword" },
      "message": { "type": "text" },
      "task_name": { "type": "keyword" },
      "datasource": { "type": "keyword" },
      "duration_ms": { "type": "float" },
      "exception": { "type": "text" }
    }
  }
}
```

### 3. 日志聚合查询

#### Kibana查询示例

```
# 查询错误日志
level: ERROR

# 查询特定任务
task_name: "spot_tickers"

# 查询慢任务
duration_ms: >1000

# 查询特定数据源的错误
datasource: "binance" AND level: ERROR
```

---

## 🔍 分布式追踪

### 1. OpenTelemetry集成

```python
# tracing.py
from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

def setup_tracing(app):
    # 配置Jaeger导出器
    jaeger_exporter = JaegerExporter(
        agent_host_name="jaeger",
        agent_port=6831,
    )
    
    # 创建追踪提供者
    provider = TracerProvider()
    processor = BatchSpanProcessor(jaeger_exporter)
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)
    
    # 自动埋点FastAPI
    FastAPIInstrumentor.instrument_app(app)
    
    return trace.get_tracer(__name__)
```

### 2. Jaeger部署

```yaml
# docker-compose.yml
version: '3.8'

services:
  jaeger:
    image: jaegertracing/all-in-one:latest
    container_name: jaeger
    ports:
      - "5775:5775/udp"
      - "6831:6831/udp"
      - "6832:6832/udp"
      - "5778:5778"
      - "16686:16686"  # UI
      - "14268:14268"
      - "14250:14250"
      - "9411:9411"
    environment:
      - COLLECTOR_ZIPKIN_HOST_PORT=:9411
```

### 3. 追踪示例

```python
from opentelemetry import trace

tracer = trace.get_tracer(__name__)

async def fetch_data(symbol: str):
    with tracer.start_as_current_span("fetch_data") as span:
        span.set_attribute("symbol", symbol)
        span.set_attribute("datasource", "binance")
        
        # 执行数据获取
        data = await datasource.fetch(symbol)
        
        span.set_attribute("records_count", len(data))
        return data
```

---

## 🚨 告警配置

### 1. AlertManager配置

```yaml
# alertmanager.yml
global:
  resolve_timeout: 5m
  smtp_smarthost: 'smtp.example.com:587'
  smtp_from: 'alertmanager@example.com'
  smtp_auth_username: 'alertmanager@example.com'
  smtp_auth_password: 'password'

route:
  group_by: ['alertname', 'severity']
  group_wait: 10s
  group_interval: 10s
  repeat_interval: 12h
  receiver: 'team-platform'
  
  routes:
    - match:
        severity: critical
      receiver: 'team-platform-critical'
      
    - match:
        severity: warning
      receiver: 'team-platform'

receivers:
  - name: 'team-platform'
    email_configs:
      - to: 'platform-team@example.com'
        send_resolved: true
        
  - name: 'team-platform-critical'
    email_configs:
      - to: 'platform-team@example.com'
    slack_configs:
      - api_url: 'https://hooks.slack.com/services/YOUR/WEBHOOK/URL'
        channel: '#critical-alerts'
        send_resolved: true
```

### 2. 告警通知渠道

#### Slack集成

```yaml
receivers:
  - name: 'slack-notifications'
    slack_configs:
      - api_url: 'https://hooks.slack.com/services/YOUR/WEBHOOK/URL'
        channel: '#chronoforge-alerts'
        send_resolved: true
        title: '{{ .Status | toUpper }}: {{ .CommonLabels.alertname }}'
        text: >-
          {{ range .Alerts }}
          *Alert:* {{ .Labels.alertname }}
          *Severity:* {{ .Labels.severity }}
          *Description:* {{ .Annotations.description }}
          *Details:*
          {{ range .Labels.SortedPairs }} • *{{ .Name }}:* {{ .Value }}
          {{ end }}
          {{ end }}
```

#### PagerDuty集成

```yaml
receivers:
  - name: 'pagerduty'
    pagerduty_configs:
      - service_key: 'YOUR_PAGERDUTY_SERVICE_KEY'
        severity: '{{ .Labels.severity }}'
        description: '{{ .Annotations.summary }}'
        details:
          firing: '{{ template "pagerduty.default.instances" .Alerts.Firing }}'
          resolved: '{{ template "pagerduty.default.instances" .Alerts.Resolved }}'
          num_firing: '{{ .Alerts.Firing | len }}'
          num_resolved: '{{ .Alerts.Resolved | len }}'
```

---

## 📊 性能监控

### 1. 性能基线

建立性能基线，用于对比和异常检测：

| 指标 | 正常范围 | 警告阈值 | 严重阈值 |
|------|---------|---------|---------|
| CPU使用率 | < 50% | 50-70% | > 70% |
| 内存使用 | < 8GB | 8-12GB | > 12GB |
| 磁盘I/O | < 100MB/s | 100-200MB/s | > 200MB/s |
| API响应时间(P95) | < 1s | 1-5s | > 5s |
| 任务成功率 | > 99% | 95-99% | < 95% |
| 任务队列大小 | < 50 | 50-100 | > 100 |

### 2. 性能分析工具

#### cProfile集成

```python
import cProfile
import pstats
from io import StringIO

def profile_task(func):
    """任务性能分析装饰器"""
    async def wrapper(*args, **kwargs):
        pr = cProfile.Profile()
        pr.enable()
        
        result = await func(*args, **kwargs)
        
        pr.disable()
        s = StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('cumulative')
        ps.print_stats(20)
        
        logger.info(f"Performance profile for {func.__name__}:\n{s.getvalue()}")
        return result
    
    return wrapper
```

---

## ✅ 监控检查清单

部署监控后，请验证以下项目：

### Prometheus
- [ ] Prometheus UI可访问：`http://prometheus:9090`
- [ ] 目标状态正常：`Status > Targets`
- [ ] 指标可查询：`Graph` 标签页
- [ ] 告警规则加载：`Status > Rules`

### Grafana
- [ ] Grafana UI可访问：`http://grafana:3000`
- [ ] 数据源配置正确：`Configuration > Data Sources`
- [ ] 仪表板导入成功：`Dashboards > Manage`
- [ ] 面板数据正常显示

### AlertManager
- [ ] AlertManager UI可访问：`http://alertmanager:9093`
- [ ] 告警规则触发正常
- [ ] 通知渠道配置正确
- [ ] 测试告警发送成功

### 日志
- [ ] 日志正常输出到文件
- [ ] 日志格式为JSON
- [ ] ELK Stack正常接收日志
- [ ] Kibana可查询日志

### 追踪
- [ ] Jaeger UI可访问：`http://jaeger:16686`
- [ ] 追踪数据正常收集
- [ ] 服务依赖图正常显示

---

## 📞 故障排查

### 监控系统故障

#### Prometheus无法抓取指标

```bash
# 检查ChronoForge指标端点
curl http://chronoforge:9090/metrics

# 检查Prometheus配置
promtool check config prometheus.yml

# 查看Prometheus日志
docker logs prometheus
```

#### Grafana仪表板无数据

```bash
# 检查数据源连接
curl http://prometheus:9090/api/v1/query?query=up

# 检查Grafana日志
docker logs grafana

# 验证查询语句
# 在Grafana的Explore页面测试查询
```

#### 告警未触发

```bash
# 检查告警规则
curl http://prometheus:9090/api/v1/rules

# 检查AlertManager状态
curl http://alertmanager:9093/api/v2/status

# 手动触发测试告警
curl -X POST http://alertmanager:9093/api/v2/alerts -d '[{
  "labels": {"alertname": "TestAlert", "severity": "warning"},
  "annotations": {"summary": "Test alert"}
}]'
```

---

## 📚 参考资源

- [Prometheus最佳实践](https://prometheus.io/docs/practices/)
- [Grafana仪表板最佳实践](https://grafana.com/docs/grafana/latest/best-practices/)
- [OpenTelemetry文档](https://opentelemetry.io/docs/)
- [Jaeger文档](https://www.jaegertracing.io/docs/)
- [SRE书籍](https://sre.google/books/)
