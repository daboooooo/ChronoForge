# ChronoForge 生产环境部署指南

## 📋 部署前检查清单

### 系统要求

| 组件 | 最低要求 | 推荐配置 | 生产环境 |
|------|---------|---------|---------|
| Python | 3.8+ | 3.11+ | 3.12+ |
| CPU | 2核 | 4核 | 8核+ |
| 内存 | 4GB | 8GB | 16GB+ |
| 磁盘 | 20GB | 100GB SSD | 500GB+ NVMe SSD |
| 网络 | 10Mbps | 100Mbps | 1Gbps |

### 依赖服务

- [ ] PostgreSQL 14+ (可选，用于元数据存储)
- [ ] Redis 7+ (可选，用于缓存和队列)
- [ ] DuckDB 1.4+ (默认存储后端)
- [ ] Nginx (反向代理和负载均衡)

### 外部API密钥

- [ ] Binance API Key (如果使用Binance数据源)
- [ ] OKX API Key (如果使用OKX数据源)
- [ ] FRED API Key (如果使用FRED数据源)
- [ ] CoinGecko API Key (可选，提升速率限制)

---

## 🚀 快速部署

### 方式一：Docker部署（推荐）

#### 1. 构建镜像

```bash
# 克隆仓库
git clone https://github.com/your-org/chronoforge.git
cd chronoforge

# 构建镜像
docker build -t chronoforge:latest .
```

#### 2. 创建配置文件

```bash
# 创建配置目录
mkdir -p config data logs

# 创建环境变量文件
cat > .env <<EOF
# 服务配置
CHRONOFORGE_HOST=0.0.0.0
CHRONOFORGE_PORT=8000
CHRONOFORGE_WORKERS=4

# 数据库配置
DUCKDB_PATH=/app/data/chronoforge.duckdb

# API密钥（使用环境变量，不要硬编码）
BINANCE_API_KEY=your_binance_api_key
BINANCE_API_SECRET=your_binance_api_secret
OKX_API_KEY=your_okx_api_key
OKX_API_SECRET=your_okx_api_secret
FRED_API_KEY=your_fred_api_key

# 日志配置
LOG_LEVEL=INFO
LOG_FORMAT=json

# 监控配置
ENABLE_METRICS=true
METRICS_PORT=9090
EOF
```

#### 3. 运行容器

```bash
# 单容器运行
docker run -d \
  --name chronoforge \
  --restart unless-stopped \
  -p 8000:8000 \
  -p 9090:9090 \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/logs:/app/logs \
  -v $(pwd)/config:/app/config \
  --env-file .env \
  chronoforge:latest

# 查看日志
docker logs -f chronoforge
```

#### 4. 健康检查

```bash
# 检查服务状态
curl http://localhost:8000/api/status

# 预期响应
{
  "service": "ChronoForge Scheduler",
  "version": "1.0.0",
  "status": "running",
  "tasks_count": 0,
  "running_tasks_count": 0
}
```

### 方式二：Docker Compose部署

#### 1. 创建docker-compose.yml

```yaml
version: '3.8'

services:
  chronoforge:
    image: chronoforge:latest
    container_name: chronoforge
    restart: unless-stopped
    ports:
      - "8000:8000"
      - "9090:9090"
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
      - ./config:/app/config
    env_file:
      - .env
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/api/status"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 40s
    networks:
      - chronoforge-network
    depends_on:
      - redis
    deploy:
      resources:
        limits:
          cpus: '4'
          memory: 8G
        reservations:
          cpus: '2'
          memory: 4G

  redis:
    image: redis:7-alpine
    container_name: chronoforge-redis
    restart: unless-stopped
    ports:
      - "6379:6379"
    volumes:
      - redis-data:/data
    command: redis-server --appendonly yes --maxmemory 2gb --maxmemory-policy allkeys-lru
    networks:
      - chronoforge-network

  nginx:
    image: nginx:alpine
    container_name: chronoforge-nginx
    restart: unless-stopped
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf:ro
      - ./ssl:/etc/nginx/ssl:ro
    depends_on:
      - chronoforge
    networks:
      - chronoforge-network

networks:
  chronoforge-network:
    driver: bridge

volumes:
  redis-data:
```

#### 2. 启动服务

```bash
# 启动所有服务
docker-compose up -d

# 查看服务状态
docker-compose ps

# 查看日志
docker-compose logs -f chronoforge
```

### 方式三：Kubernetes部署

#### 1. 创建命名空间

```yaml
# namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: chronoforge
```

#### 2. 创建ConfigMap

```yaml
# configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: chronoforge-config
  namespace: chronoforge
data:
  CHRONOFORGE_HOST: "0.0.0.0"
  CHRONOFORGE_PORT: "8000"
  CHRONOFORGE_WORKERS: "4"
  DUCKDB_PATH: "/app/data/chronoforge.duckdb"
  LOG_LEVEL: "INFO"
  LOG_FORMAT: "json"
  ENABLE_METRICS: "true"
  METRICS_PORT: "9090"
```

#### 3. 创建Secret

```yaml
# secret.yaml
apiVersion: v1
kind: Secret
metadata:
  name: chronoforge-secrets
  namespace: chronoforge
type: Opaque
stringData:
  BINANCE_API_KEY: "your_binance_api_key"
  BINANCE_API_SECRET: "your_binance_api_secret"
  OKX_API_KEY: "your_okx_api_key"
  OKX_API_SECRET: "your_okx_api_secret"
  FRED_API_KEY: "your_fred_api_key"
```

#### 4. 创建Deployment

```yaml
# deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: chronoforge
  namespace: chronoforge
spec:
  replicas: 3
  selector:
    matchLabels:
      app: chronoforge
  template:
    metadata:
      labels:
        app: chronoforge
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9090"
        prometheus.io/path: "/metrics"
    spec:
      containers:
      - name: chronoforge
        image: chronoforge:latest
        imagePullPolicy: IfNotPresent
        ports:
        - containerPort: 8000
          name: http
        - containerPort: 9090
          name: metrics
        envFrom:
        - configMapRef:
            name: chronoforge-config
        - secretRef:
            name: chronoforge-secrets
        resources:
          limits:
            cpu: "4"
            memory: "8Gi"
          requests:
            cpu: "2"
            memory: "4Gi"
        livenessProbe:
          httpGet:
            path: /api/status
            port: 8000
          initialDelaySeconds: 30
          periodSeconds: 10
          timeoutSeconds: 5
          failureThreshold: 3
        readinessProbe:
          httpGet:
            path: /api/status
            port: 8000
          initialDelaySeconds: 5
          periodSeconds: 5
          timeoutSeconds: 3
          failureThreshold: 3
        volumeMounts:
        - name: data
          mountPath: /app/data
        - name: logs
          mountPath: /app/logs
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: chronoforge-data-pvc
      - name: logs
        emptyDir: {}
```

#### 5. 创建Service

```yaml
# service.yaml
apiVersion: v1
kind: Service
metadata:
  name: chronoforge
  namespace: chronoforge
spec:
  type: LoadBalancer
  selector:
    app: chronoforge
  ports:
  - name: http
    port: 80
    targetPort: 8000
    protocol: TCP
  - name: metrics
    port: 9090
    targetPort: 9090
    protocol: TCP
```

#### 6. 部署到Kubernetes

```bash
# 应用所有配置
kubectl apply -f namespace.yaml
kubectl apply -f configmap.yaml
kubectl apply -f secret.yaml
kubectl apply -f deployment.yaml
kubectl apply -f service.yaml

# 检查部署状态
kubectl get pods -n chronoforge
kubectl get services -n chronoforge

# 查看日志
kubectl logs -f deployment/chronoforge -n chronoforge
```

---

## ⚙️ 生产环境配置

### Nginx反向代理配置

```nginx
# nginx.conf
user nginx;
worker_processes auto;
error_log /var/log/nginx/error.log warn;
pid /var/run/nginx.pid;

events {
    worker_connections 4096;
    use epoll;
    multi_accept on;
}

http {
    include /etc/nginx/mime.types;
    default_type application/octet-stream;

    log_format json_combined escape=json '{'
        '"time_local":"$time_local",'
        '"remote_addr":"$remote_addr",'
        '"remote_user":"$remote_user",'
        '"request":"$request",'
        '"status":"$status",'
        '"body_bytes_sent":"$body_bytes_sent",'
        '"request_time":"$request_time",'
        '"http_referrer":"$http_referer",'
        '"http_user_agent":"$http_user_agent",'
        '"http_x_forwarded_for":"$http_x_forwarded_for"'
    '}';

    access_log /var/log/nginx/access.log json_combined;

    sendfile on;
    tcp_nopush on;
    tcp_nodelay on;
    keepalive_timeout 65;
    types_hash_max_size 2048;

    # Gzip压缩
    gzip on;
    gzip_vary on;
    gzip_proxied any;
    gzip_comp_level 6;
    gzip_types text/plain text/css text/xml text/javascript application/json application/javascript application/xml+rss;

    # 速率限制
    limit_req_zone $binary_remote_addr zone=api:10m rate=10r/s;
    limit_conn_zone $binary_remote_addr zone=conn:10m;

    # 上游服务器
    upstream chronoforge_backend {
        least_conn;
        server chronoforge:8000 max_fails=3 fail_timeout=30s;
        keepalive 32;
    }

    # HTTP服务器（重定向到HTTPS）
    server {
        listen 80;
        server_name your-domain.com;
        return 301 https://$server_name$request_uri;
    }

    # HTTPS服务器
    server {
        listen 443 ssl http2;
        server_name your-domain.com;

        # SSL配置
        ssl_certificate /etc/nginx/ssl/cert.pem;
        ssl_certificate_key /etc/nginx/ssl/key.pem;
        ssl_protocols TLSv1.2 TLSv1.3;
        ssl_ciphers HIGH:!aNULL:!MD5;
        ssl_prefer_server_ciphers on;
        ssl_session_cache shared:SSL:10m;
        ssl_session_timeout 10m;

        # 安全头
        add_header Strict-Transport-Security "max-age=31536000; includeSubDomains" always;
        add_header X-Frame-Options "SAMEORIGIN" always;
        add_header X-Content-Type-Options "nosniff" always;
        add_header X-XSS-Protection "1; mode=block" always;

        # API代理
        location /api/ {
            limit_req zone=api burst=20 nodelay;
            limit_conn conn 10;

            proxy_pass http://chronoforge_backend;
            proxy_http_version 1.1;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
            proxy_set_header Connection "";

            proxy_connect_timeout 60s;
            proxy_send_timeout 60s;
            proxy_read_timeout 60s;

            proxy_buffering on;
            proxy_buffer_size 4k;
            proxy_buffers 8 4k;
        }

        # 健康检查
        location /health {
            access_log off;
            proxy_pass http://chronoforge_backend/api/status;
        }

        # API文档
        location /docs {
            proxy_pass http://chronoforge_backend;
            proxy_http_version 1.1;
            proxy_set_header Host $host;
        }
    }
}
```

### Systemd服务配置

```ini
# /etc/systemd/system/chronoforge.service
[Unit]
Description=ChronoForge Data Scheduler Service
Documentation=https://github.com/your-org/chronoforge
After=network.target postgresql.service redis.service
Wants=postgresql.service redis.service

[Service]
Type=notify
User=chronoforge
Group=chronoforge
WorkingDirectory=/opt/chronoforge
Environment="PATH=/opt/chronoforge/.venv/bin"
EnvironmentFile=/opt/chronoforge/.env

ExecStart=/opt/chronoforge/.venv/bin/uvicorn chronoforge.server.main:app \
    --host 0.0.0.0 \
    --port 8000 \
    --workers 4 \
    --loop uvloop \
    --http httptools

ExecReload=/bin/kill -HUP $MAINPID
Restart=on-failure
RestartSec=10
TimeoutStartSec=60
TimeoutStopSec=60

# 安全加固
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/opt/chronoforge/data /opt/chronoforge/logs
ReadOnlyPaths=/opt/chronoforge/config

# 资源限制
LimitNOFILE=65536
LimitNPROC=4096

# 日志
StandardOutput=journal
StandardError=journal
SyslogIdentifier=chronoforge

[Install]
WantedBy=multi-user.target
```

---

## 🔐 安全配置

### 1. 密钥管理

#### 使用环境变量（推荐）

```bash
# 创建.env文件（不要提交到Git）
cat > .env <<EOF
BINANCE_API_KEY=${BINANCE_API_KEY}
BINANCE_API_SECRET=${BINANCE_API_SECRET}
OKX_API_KEY=${OKX_API_KEY}
OKX_API_SECRET=${OKX_API_SECRET}
FRED_API_KEY=${FRED_API_KEY}
EOF

# 设置权限
chmod 600 .env
```

#### 使用HashiCorp Vault

```python
# config.py
import hvac
import os

def get_secret_from_vault(secret_path):
    """从Vault获取密钥"""
    client = hvac.Client(
        url=os.getenv('VAULT_ADDR'),
        token=os.getenv('VAULT_TOKEN')
    )
    secret = client.secrets.kv.v2.read_secret_version(path=secret_path)
    return secret['data']['data']

# 使用
api_keys = get_secret_from_vault('chronoforge/api-keys')
BINANCE_API_KEY = api_keys['binance_api_key']
```

### 2. HTTPS/TLS配置

```bash
# 使用Let's Encrypt获取免费证书
sudo apt-get install certbot python3-certbot-nginx
sudo certbot --nginx -d your-domain.com

# 自动续期
sudo crontab -e
# 添加以下行
0 12 * * * /usr/bin/certbot renew --quiet
```

### 3. 防火墙配置

```bash
# UFW配置
sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw allow ssh
sudo ufw allow http
sudo ufw allow https
sudo ufw enable

# 检查状态
sudo ufw status verbose
```

---

## 📊 性能优化

### 1. 数据库优化

```python
# DuckDB配置优化
storage_config = {
    "db_path": "/app/data/chronoforge.duckdb",
    "memory_limit": "8GB",  # 根据可用内存调整
    "threads": 8,  # 根据CPU核心数调整
    "checkpoint_threshold": "50GB"
}
```

### 2. 连接池配置

```python
# Redis连接池
import redis

redis_pool = redis.ConnectionPool(
    host='localhost',
    port=6379,
    db=0,
    max_connections=100,
    socket_timeout=5,
    socket_connect_timeout=5,
    retry_on_timeout=True
)
```

### 3. 并发配置

```python
# 调度器配置
scheduler_config = {
    "max_concurrent_tasks": 20,  # 根据系统资源调整
    "default_timeout": 300,
    "enable_metrics": True
}
```

---

## 🔄 备份和恢复

### 自动备份脚本

```bash
#!/bin/bash
# backup.sh

BACKUP_DIR="/backup/chronoforge"
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="chronoforge_backup_${DATE}.tar.gz"

# 创建备份目录
mkdir -p ${BACKUP_DIR}

# 备份数据库
cp /app/data/chronoforge.duckdb ${BACKUP_DIR}/chronoforge_${DATE}.duckdb

# 备份配置
tar -czf ${BACKUP_DIR}/${BACKUP_FILE} \
    /app/data \
    /app/config \
    /app/.env

# 删除30天前的备份
find ${BACKUP_DIR} -name "*.tar.gz" -mtime +30 -delete

echo "Backup completed: ${BACKUP_FILE}"
```

### 恢复流程

```bash
# 停止服务
sudo systemctl stop chronoforge

# 恢复数据
tar -xzf chronoforge_backup_20240101_120000.tar.gz -C /

# 启动服务
sudo systemctl start chronoforge
```

---

## 📈 监控和告警

### Prometheus配置

```yaml
# prometheus.yml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'chronoforge'
    static_configs:
      - targets: ['chronoforge:9090']
    metrics_path: '/metrics'
```

### Grafana仪表板

导入预配置的Grafana仪表板（见 `monitoring/grafana-dashboard.json`）

### 告警规则

```yaml
# alert_rules.yml
groups:
  - name: chronoforge
    rules:
      - alert: ChronoForgeDown
        expr: up{job="chronoforge"} == 0
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "ChronoForge服务不可用"
          description: "ChronoForge服务已经宕机超过5分钟"

      - alert: HighErrorRate
        expr: rate(chronoforge_errors_total[5m]) > 0.1
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "ChronoForge错误率过高"
          description: "过去10分钟错误率超过10%"
```

---

## 🚨 故障排查

### 常见问题

#### 1. 服务无法启动

```bash
# 检查日志
journalctl -u chronoforge -n 100

# 检查端口占用
sudo lsof -i :8000

# 检查权限
ls -la /app/data /app/logs
```

#### 2. 数据库连接失败

```bash
# 检查DuckDB文件
ls -lh /app/data/chronoforge.duckdb

# 检查磁盘空间
df -h /app/data

# 检查权限
stat /app/data/chronoforge.duckdb
```

#### 3. API响应缓慢

```bash
# 检查系统资源
top -p $(pgrep -f chronoforge)

# 检查网络连接
netstat -an | grep 8000

# 检查日志中的慢查询
grep "slow" /app/logs/chronoforge.log
```

---

## ✅ 部署验证清单

部署完成后，请验证以下项目：

- [ ] 服务健康检查通过：`curl http://localhost:8000/api/status`
- [ ] API文档可访问：`http://localhost:8000/docs`
- [ ] 监控指标可访问：`curl http://localhost:9090/metrics`
- [ ] 日志正常输出：`tail -f /app/logs/chronoforge.log`
- [ ] 数据持久化正常：检查数据目录
- [ ] 备份脚本运行正常：`./backup.sh`
- [ ] HTTPS证书有效：`curl -vI https://your-domain.com`
- [ ] 防火墙规则正确：`sudo ufw status`
- [ ] 系统资源充足：`top`, `free -h`, `df -h`

---

## 📞 支持

如遇问题，请参考：
- [故障排查手册](./TROUBLESHOOTING.md)
- [监控配置指南](./MONITORING.md)
- [GitHub Issues](https://github.com/your-org/chronoforge/issues)
