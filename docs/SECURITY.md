# ChronoForge 安全最佳实践指南

## 🔐 安全架构

ChronoForge采用多层安全架构，确保数据和系统的安全性：

```
┌─────────────────────────────────────────────────────────────┐
│                      安全架构层次                              │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  第7层：应用安全                                               │
│  ├─ 输入验证和清理                                            │
│  ├─ 输出编码                                                  │
│  └─ 安全编码实践                                              │
│                                                               │
│  第6层：API安全                                                │
│  ├─ 认证和授权                                                │
│  ├─ 速率限制                                                  │
│  └─ API密钥管理                                               │
│                                                               │
│  第5层：网络安全                                                │
│  ├─ HTTPS/TLS加密                                             │
│  ├─ 防火墙规则                                                │
│  └─ 网络隔离                                                  │
│                                                               │
│  第4层：数据安全                                                │
│  ├─ 数据加密                                                  │
│  ├─ 访问控制                                                  │
│  └─ 数据脱敏                                                  │
│                                                               │
│  第3层：基础设施安全                                            │
│  ├─ 容器安全                                                  │
│  ├─ 主机加固                                                  │
│  └─ 补丁管理                                                  │
│                                                               │
│  第2层：身份和访问管理                                          │
│  ├─ 用户认证                                                  │
│  ├─ 角色权限                                                  │
│  └─ 审计日志                                                  │
│                                                               │
│  第1层：安全治理                                                │
│  ├─ 安全策略                                                  │
│  ├─ 合规性                                                    │
│  └─ 风险管理                                                  │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

---

## 🔑 密钥管理

### 1. 环境变量管理（推荐）

#### 最佳实践

```bash
# 1. 创建.env文件（不要提交到Git）
cat > .env <<EOF
# ChronoForge配置
CHRONOFORGE_HOST=0.0.0.0
CHRONOFORGE_PORT=8000

# 数据库配置
DUCKDB_PATH=/app/data/chronoforge.duckdb

# API密钥（从环境变量读取）
BINANCE_API_KEY=${BINANCE_API_KEY}
BINANCE_API_SECRET=${BINANCE_API_SECRET}
OKX_API_KEY=${OKX_API_KEY}
OKX_API_SECRET=${OKX_API_SECRET}
FRED_API_KEY=${FRED_API_KEY}

# 监控配置
ENABLE_METRICS=true
METRICS_PORT=9090
EOF

# 2. 设置权限（仅所有者可读写）
chmod 600 .env

# 3. 添加到.gitignore
echo ".env" >> .gitignore
echo "*.env" >> .gitignore
```

#### 在代码中使用

```python
import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 读取密钥
BINANCE_API_KEY = os.getenv('BINANCE_API_KEY')
BINANCE_API_SECRET = os.getenv('BINANCE_API_SECRET')

# 验证密钥存在
if not BINANCE_API_KEY:
    raise ValueError("BINANCE_API_KEY environment variable is not set")
```

### 2. HashiCorp Vault集成

#### 安装和配置

```bash
# 安装Vault客户端
pip install hvac

# 启动Vault服务器（开发模式）
vault server -dev -dev-root-token-id="root"

# 配置环境变量
export VAULT_ADDR='http://127.0.0.1:8200'
export VAULT_TOKEN='root'
```

#### 在代码中使用

```python
import hvac
import os

class VaultSecretManager:
    def __init__(self):
        self.client = hvac.Client(
            url=os.getenv('VAULT_ADDR'),
            token=os.getenv('VAULT_TOKEN')
        )
    
    def get_secret(self, path):
        """从Vault获取密钥"""
        try:
            response = self.client.secrets.kv.v2.read_secret_version(
                path=path
            )
            return response['data']['data']
        except Exception as e:
            raise RuntimeError(f"Failed to retrieve secret: {e}")
    
    def store_secret(self, path, data):
        """存储密钥到Vault"""
        try:
            self.client.secrets.kv.v2.create_or_update_secret(
                path=path,
                secret=data
            )
        except Exception as e:
            raise RuntimeError(f"Failed to store secret: {e}")

# 使用示例
vault = VaultSecretManager()

# 获取API密钥
api_keys = vault.get_secret('chronoforge/api-keys')
BINANCE_API_KEY = api_keys['binance_api_key']
BINANCE_API_SECRET = api_keys['binance_api_secret']
```

#### Vault配置

```bash
# 启用KV引擎
vault secrets enable -path=chronoforge kv-v2

# 存储密钥
vault kv put chronoforge/api-keys \
    binance_api_key="your_binance_key" \
    binance_api_secret="your_binance_secret" \
    okx_api_key="your_okx_key" \
    okx_api_secret="your_okx_secret"

# 创建访问策略
cat > chronoforge-policy.hcl <<EOF
path "chronoforge/data/api-keys" {
  capabilities = ["read"]
}
EOF

vault policy write chronoforge-policy chronoforge-policy.hcl

# 创建应用令牌
vault token create -policy=chronoforge-policy
```

### 3. AWS Secrets Manager集成

```python
import boto3
import base64
from botocore.exceptions import ClientError

class AWSSecretManager:
    def __init__(self, region_name="us-east-1"):
        self.client = boto3.client(
            service_name='secretsmanager',
            region_name=region_name
        )
    
    def get_secret(self, secret_name):
        """从AWS Secrets Manager获取密钥"""
        try:
            response = self.client.get_secret_value(SecretId=secret_name)
            
            if 'SecretString' in response:
                return eval(response['SecretString'])
            else:
                return base64.b64decode(response['SecretBinary'])
        except ClientError as e:
            raise RuntimeError(f"Failed to retrieve secret: {e}")

# 使用示例
aws_sm = AWSSecretManager()
api_keys = aws_sm.get_secret('chronoforge/api-keys')
```

### 4. Kubernetes Secrets

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

```yaml
# deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: chronoforge
spec:
  template:
    spec:
      containers:
      - name: chronoforge
        envFrom:
        - secretRef:
            name: chronoforge-secrets
```

---

## 🛡️ API安全

### 1. API认证

#### JWT认证实现

```python
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from datetime import datetime, timedelta
from typing import Optional

app = FastAPI()
security = HTTPBearer()

# JWT配置
SECRET_KEY = "your-secret-key-keep-it-secret"  # 从环境变量读取
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    """创建JWT令牌"""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """验证JWT令牌"""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(
            credentials.credentials, 
            SECRET_KEY, 
            algorithms=[ALGORITHM]
        )
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        return username
    except JWTError:
        raise credentials_exception

# 使用认证保护API
@app.get("/api/protected")
async def protected_endpoint(current_user: str = Depends(get_current_user)):
    return {"message": f"Hello, {current_user}"}
```

#### API密钥认证

```python
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import APIKeyHeader

app = FastAPI()
api_key_header = APIKeyHeader(name="X-API-Key")

# 有效的API密钥（从数据库或配置中读取）
VALID_API_KEYS = {
    "api-key-1": {"user": "user1", "permissions": ["read", "write"]},
    "api-key-2": {"user": "user2", "permissions": ["read"]},
}

async def verify_api_key(api_key: str = Depends(api_key_header)):
    """验证API密钥"""
    if api_key not in VALID_API_KEYS:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key"
        )
    return VALID_API_KEYS[api_key]

@app.get("/api/data")
async def get_data(api_key_info: dict = Depends(verify_api_key)):
    if "read" not in api_key_info["permissions"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions"
        )
    return {"data": "sensitive information"}
```

### 2. 速率限制

#### 使用slowapi实现

```python
from fastapi import FastAPI, Request
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

app = FastAPI()

# 配置速率限制
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# 应用速率限制
@app.get("/api/data")
@limiter.limit("10/minute")  # 每分钟10次请求
async def get_data(request: Request):
    return {"data": "response"}

# 不同端点不同限制
@app.get("/api/heavy")
@limiter.limit("5/minute")  # 每分钟5次请求
async def heavy_operation(request: Request):
    return {"data": "heavy operation result"}
```

#### 使用Redis实现分布式速率限制

```python
import redis
from fastapi import FastAPI, HTTPException, Request
from functools import wraps
import time

app = FastAPI()
redis_client = redis.Redis(host='localhost', port=6379, db=0)

def rate_limit(limit: int, window: int):
    """速率限制装饰器
    
    Args:
        limit: 时间窗口内允许的最大请求数
        window: 时间窗口（秒）
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(request: Request, *args, **kwargs):
            # 获取客户端IP
            client_ip = request.client.host
            
            # 构建Redis键
            key = f"rate_limit:{client_ip}:{func.__name__}"
            
            # 获取当前计数
            current = redis_client.get(key)
            
            if current is None:
                # 第一次请求，设置计数和过期时间
                redis_client.setex(key, window, 1)
            elif int(current) >= limit:
                # 超过限制
                raise HTTPException(
                    status_code=429,
                    detail=f"Rate limit exceeded. Try again in {redis_client.ttl(key)} seconds."
                )
            else:
                # 增加计数
                redis_client.incr(key)
            
            return await func(request, *args, **kwargs)
        return wrapper
    return decorator

@app.get("/api/data")
@rate_limit(limit=10, window=60)  # 每分钟10次请求
async def get_data(request: Request):
    return {"data": "response"}
```

### 3. 输入验证

```python
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, validator, constr
from typing import List
import re

app = FastAPI()

class TaskCreate(BaseModel):
    name: constr(min_length=1, max_length=100)  # 限制长度
    symbols: List[str]
    timeframe: str
    
    @validator('name')
    def validate_name(cls, v):
        # 只允许字母、数字、下划线和连字符
        if not re.match(r'^[a-zA-Z0-9_-]+$', v):
            raise ValueError('Name can only contain letters, numbers, underscores, and hyphens')
        return v
    
    @validator('symbols')
    def validate_symbols(cls, v):
        # 验证交易对格式
        for symbol in v:
            if not re.match(r'^[a-z]+:[A-Z]+/[A-Z]+$', symbol):
                raise ValueError(f'Invalid symbol format: {symbol}')
        return v
    
    @validator('timeframe')
    def validate_timeframe(cls, v):
        # 只允许特定的时间框架
        allowed = ['1m', '5m', '15m', '1h', '4h', '1d']
        if v not in allowed:
            raise ValueError(f'Invalid timeframe. Allowed: {allowed}')
        return v

@app.post("/api/tasks")
async def create_task(task: TaskCreate):
    # Pydantic会自动验证输入
    return {"task": task.dict()}
```

---

## 🔒 HTTPS/TLS配置

### 1. Let's Encrypt证书

```bash
# 安装Certbot
sudo apt-get update
sudo apt-get install certbot python3-certbot-nginx

# 获取证书
sudo certbot --nginx -d your-domain.com -d www.your-domain.com

# 自动续期
sudo crontab -e
# 添加以下行
0 12 * * * /usr/bin/certbot renew --quiet
```

### 2. Nginx HTTPS配置

```nginx
server {
    listen 443 ssl http2;
    server_name your-domain.com;

    # SSL证书
    ssl_certificate /etc/letsencrypt/live/your-domain.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/your-domain.com/privkey.pem;

    # SSL配置
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers 'ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384';
    ssl_prefer_server_ciphers on;
    ssl_session_cache shared:SSL:10m;
    ssl_session_timeout 10m;

    # 安全头
    add_header Strict-Transport-Security "max-age=31536000; includeSubDomains" always;
    add_header X-Frame-Options "SAMEORIGIN" always;
    add_header X-Content-Type-Options "nosniff" always;
    add_header X-XSS-Protection "1; mode=block" always;
    add_header Content-Security-Policy "default-src 'self'" always;

    # 反向代理
    location / {
        proxy_pass http://chronoforge:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}

# HTTP重定向到HTTPS
server {
    listen 80;
    server_name your-domain.com;
    return 301 https://$server_name$request_uri;
}
```

### 3. 自签名证书（仅用于开发）

```bash
# 生成自签名证书
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout private.key \
  -out certificate.pem \
  -subj "/C=US/ST=State/L=City/O=Organization/CN=localhost"

# 使用证书启动服务
uvicorn chronoforge.server.main:app \
  --ssl-keyfile=private.key \
  --ssl-certfile=certificate.pem
```

---

## 🚧 网络安全

### 1. 防火墙配置

#### UFW配置

```bash
# 启用UFW
sudo ufw enable

# 默认策略
sudo ufw default deny incoming
sudo ufw default allow outgoing

# 允许SSH
sudo ufw allow ssh

# 允许HTTP和HTTPS
sudo ufw allow http
sudo ufw allow https

# 允许特定端口
sudo ufw allow 8000/tcp

# 限制连接速率
sudo ufw limit ssh

# 查看状态
sudo ufw status verbose
```

#### iptables配置

```bash
# 清除现有规则
sudo iptables -F

# 默认策略
sudo iptables -P INPUT DROP
sudo iptables -P FORWARD DROP
sudo iptables -P OUTPUT ACCEPT

# 允许本地回环
sudo iptables -A INPUT -i lo -j ACCEPT

# 允许已建立的连接
sudo iptables -A INPUT -m state --state ESTABLISHED,RELATED -j ACCEPT

# 允许SSH
sudo iptables -A INPUT -p tcp --dport 22 -j ACCEPT

# 允许HTTP和HTTPS
sudo iptables -A INPUT -p tcp --dport 80 -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 443 -j ACCEPT

# 防止SYN洪水攻击
sudo iptables -A INPUT -p tcp --syn -m limit --limit 1/s --limit-burst 3 -j ACCEPT

# 防止端口扫描
sudo iptables -A INPUT -m recent --name portscan --rcheck --seconds 300 -j DROP

# 保存规则
sudo iptables-save > /etc/iptables/rules.v4
```

### 2. 网络隔离

#### Docker网络隔离

```yaml
# docker-compose.yml
version: '3.8'

networks:
  frontend:
    driver: bridge
  backend:
    driver: bridge
    internal: true  # 内部网络，无法访问外网

services:
  nginx:
    networks:
      - frontend
      - backend
  
  chronoforge:
    networks:
      - backend
  
  redis:
    networks:
      - backend
```

#### Kubernetes网络策略

```yaml
# network-policy.yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: chronoforge-network-policy
  namespace: chronoforge
spec:
  podSelector:
    matchLabels:
      app: chronoforge
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          name: nginx-ingress
    ports:
    - protocol: TCP
      port: 8000
  egress:
  - to:
    - namespaceSelector:
        matchLabels:
          name: chronoforge
    ports:
    - protocol: TCP
      port: 6379  # Redis
```

---

## 📝 审计日志

### 1. 审计日志配置

```python
import logging
import json
from datetime import datetime
from typing import Dict, Any

class AuditLogger:
    def __init__(self, log_file: str = "/var/log/chronoforge/audit.log"):
        self.logger = logging.getLogger('audit')
        self.logger.setLevel(logging.INFO)
        
        handler = logging.FileHandler(log_file)
        formatter = logging.Formatter('%(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
    
    def log_event(
        self,
        event_type: str,
        user: str,
        action: str,
        resource: str,
        status: str,
        details: Dict[str, Any] = None
    ):
        """记录审计事件"""
        event = {
            "timestamp": datetime.utcnow().isoformat(),
            "event_type": event_type,
            "user": user,
            "action": action,
            "resource": resource,
            "status": status,
            "details": details or {}
        }
        self.logger.info(json.dumps(event))

# 使用示例
audit = AuditLogger()

# 记录API访问
audit.log_event(
    event_type="api_access",
    user="user123",
    action="GET",
    resource="/api/tasks",
    status="success",
    details={"ip": "192.168.1.1"}
)

# 记录配置变更
audit.log_event(
    event_type="config_change",
    user="admin",
    action="UPDATE",
    resource="scheduler_config",
    status="success",
    details={"changes": {"max_concurrent_tasks": 20}}
)
```

### 2. 敏感操作监控

```python
from functools import wraps
from typing import Callable

def audit_sensitive_operation(operation_name: str):
    """审计敏感操作装饰器"""
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # 记录操作开始
            audit.log_event(
                event_type="sensitive_operation",
                user=get_current_user(),
                action=operation_name,
                resource=str(kwargs),
                status="started"
            )
            
            try:
                # 执行操作
                result = await func(*args, **kwargs)
                
                # 记录成功
                audit.log_event(
                    event_type="sensitive_operation",
                    user=get_current_user(),
                    action=operation_name,
                    resource=str(kwargs),
                    status="success"
                )
                
                return result
            except Exception as e:
                # 记录失败
                audit.log_event(
                    event_type="sensitive_operation",
                    user=get_current_user(),
                    action=operation_name,
                    resource=str(kwargs),
                    status="failed",
                    details={"error": str(e)}
                )
                raise
        
        return wrapper
    return decorator

# 使用示例
@app.delete("/api/tasks/{task_name}")
@audit_sensitive_operation("delete_task")
async def delete_task(task_name: str):
    # 删除任务
    pass
```

---

## 🔍 安全扫描

### 1. 依赖漏洞扫描

```bash
# 使用Safety检查依赖漏洞
pip install safety
safety check -r requirements.txt

# 使用pip-audit
pip install pip-audit
pip-audit

# 使用Trivy扫描容器镜像
trivy image chronoforge:latest
```

### 2. 代码安全扫描

```bash
# 使用Bandit进行Python代码安全扫描
pip install bandit
bandit -r chronoforge/

# 使用Semgrep
pip install semgrep
semgrep --config=auto chronoforge/
```

### 3. 容器安全扫描

```dockerfile
# Dockerfile安全最佳实践

# 使用最小化基础镜像
FROM python:3.12-slim

# 创建非root用户
RUN useradd -m -u 1000 chronoforge

# 设置工作目录
WORKDIR /app

# 复制依赖文件
COPY requirements.txt .

# 安装依赖
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用代码
COPY --chown=chronoforge:chronoforge . .

# 切换到非root用户
USER chronoforge

# 健康检查
HEALTHCHECK --interval=30s --timeout=3s \
  CMD curl -f http://localhost:8000/api/status || exit 1

# 启动服务
CMD ["uvicorn", "chronoforge.server.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

---

## ✅ 安全检查清单

### 部署前检查

- [ ] 所有密钥使用环境变量或密钥管理服务
- [ ] 启用HTTPS/TLS加密
- [ ] 配置防火墙规则
- [ ] 启用速率限制
- [ ] 配置审计日志
- [ ] 进行依赖漏洞扫描
- [ ] 进行代码安全扫描
- [ ] 配置备份和恢复

### 运行时检查

- [ ] 监控异常访问模式
- [ ] 定期审查审计日志
- [ ] 定期更新依赖
- [ ] 定期进行安全扫描
- [ ] 定期测试备份恢复
- [ ] 监控安全告警

### 合规性检查

- [ ] 符合GDPR要求（如适用）
- [ ] 符合SOC2要求（如适用）
- [ ] 符合PCI-DSS要求（如适用）
- [ ] 符合HIPAA要求（如适用）
- [ ] 定期进行安全审计

---

## 📚 安全资源

- [OWASP Top 10](https://owasp.org/www-project-top-ten/)
- [Python安全最佳实践](https://python.readthedocs.io/en/stable/library/security_warnings.html)
- [FastAPI安全指南](https://fastapi.tiangolo.com/tutorial/security/)
- [Docker安全最佳实践](https://docs.docker.com/engine/security/)
- [Kubernetes安全最佳实践](https://kubernetes.io/docs/concepts/security/)

---

## 🚨 安全事件响应

### 发现安全漏洞时

1. **立即隔离**：隔离受影响的系统
2. **评估影响**：确定漏洞影响范围
3. **修复漏洞**：应用安全补丁
4. **通知相关方**：通知受影响的用户
5. **文档记录**：记录事件和响应过程
6. **改进措施**：更新安全策略和流程

### 报告安全漏洞

如果您发现ChronoForge的安全漏洞，请负责任地披露：

- 邮箱：security@chronoforge.example.com
- PGP公钥：[链接]
- 响应时间：24小时内确认，72小时内初步评估

---

## 📞 安全支持

如有安全问题或疑问，请联系：
- 安全团队：security@chronoforge.example.com
- 安全文档：https://chronoforge.example.com/security
- 安全公告：https://chronoforge.example.com/security/advisories
