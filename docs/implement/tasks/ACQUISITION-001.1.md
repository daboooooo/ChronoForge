# ACQUISITION-001.1 — DataConnector 协议 + 错误映射

## 派发信息

- 任务单：D10 §3 ACQUISITION-001 拆分（子任务 1/3）
- 设计依据：D04 §1（协议）、D01 §3（错误层级）、D04 §3（HTTP→错误映射表）
- Agent：待定
- 派发时间：2026-09-12
- 状态：READY
- 依赖：MODEL-002（CanonicalType 枚举用于 CapabilityMatrix）

## file_ownership

- `src/chronoforge/connectors/base.py`（新建，协议定义）
- `src/chronoforge/connectors/errors.py`（新建，错误类层级 + HTTP 映射）
- `tests/unit/test_connector_protocol.py`（新建）
- `tests/unit/test_error_mapping.py`（新建）

## 交付物契约摘要

### FetchRequest 数据类（connectors/base.py）

```python
@dataclass(frozen=True)
class FetchRequest:
    dataset_id: str
    params: Mapping[str, Any]        # {"symbol":"BTCUSDT","interval":"1m",...}
    start: datetime | None
    end: datetime | None
    cursor: str | None               # 增量续传（源语义，各源定义）
```

### RawBatch 数据类（connectors/base.py）

```python
@dataclass
class RawBatch:
    endpoint: str
    payload: Any                     # 源响应反序列化结果（原样，不清洗）
    raw_meta: dict                   # {"http_status","headers_subset","fetched_at","url"}
```

- `payload` 保持源响应原样（下游 fixture 对比的基准，D04 §1）
- `raw_meta.fetched_at = datetime.now(UTC).replace(tzinfo=None)`

### CapabilityMatrix 数据类（connectors/base.py）

```python
@dataclass(frozen=True)
class CapabilityMatrix:
    canonical_types: frozenset[CanonicalType]
    intervals: frozenset[Interval]
    supports_revision: bool
    supports_websocket: bool
    max_history_days: int | None
```

### DataConnector Protocol（connectors/base.py）

```python
class DataConnector(Protocol):
    source_id: str

    def capabilities(self) -> CapabilityMatrix: ...
    def health(self) -> HealthStatus: ...          # {'ok':bool,'latency_ms':int,'detail':str}
    def discover(self) -> list[InstrumentRef]: ... # 可选实体/合约枚举（Deribit 必须）
    def fetch(self, request: FetchRequest) -> Iterator[RawBatch]: ...   # 分页由实现内聚
    def normalize(self, raw: RawBatch) -> list[BaseRecord]: ...         # -> 具体子类
    def validate(self, records: list[BaseRecord]) -> "QualityReport": ...  # 委托 quality.rules
    def checkpoint_from(self, raw: RawBatch | list[BaseRecord]) -> str | None: ...
```

- connector **不落盘、不写库**（D04 §1 边界规则）
- `normalize` 输出必含 provenance 字段（raw_record_id 由 pipeline 注入回调 `raw_ref_provider`）
- `discover` 返回 `InstrumentRef = {"entity_id": str, "instrument_id": str, "market_id": str}`

### HealthStatus 类型

```python
@dataclass
class HealthStatus:
    ok: bool
    latency_ms: int
    detail: str = ""
```

### 错误类层级（connectors/errors.py）

严格继承 D01 §3 层级，全部继承 `ChronoForgeError`：

```python
class ChronoForgeError(Exception):
    """顶层异常，所有自定义错误继承此基类"""
    def __init__(self, message: str, context: dict | None = None):
        super().__init__(message)
        self.context = context or {}

class TransportError(ChronoForgeError):
    """传输层错误（网络超时、5xx）→ 可重试"""
    pass

class RateLimitError(TransportError):
    """限流错误（429）→ 可重试，退避尊重 Retry-After"""
    def __init__(self, message: str, context: dict | None = None, retry_after: int | None = None):
        super().__init__(message, context)
        self.retry_after = retry_after

class AuthError(ChronoForgeError):
    """认证错误（401/403）→ 不重试，run FAILED"""
    pass

class ProviderError(ChronoForgeError):
    """提供商错误（4xx≠429）→ 不重试"""
    pass

class SchemaError(ChronoForgeError):
    """响应结构不符合契约 → 不重试，dataset 熔断"""
    pass

class QualityError(ChronoForgeError):
    """质量校验失败 → 由配置决定阻断/放行"""
    pass

class StorageError(ChronoForgeError):
    """存储错误"""
    pass

class ConfigError(ChronoForgeError):
    """配置错误 → 启动 fail fast"""
    pass
```

- **错误必须携带 `context: dict`**（source/dataset/endpoint/status），禁止裸 raise 字符串
- 捕获规则：仅 pipeline 顶层捕获并归类入 run_log；模块内只抛出，不吞（禁 `except: pass`）

### HTTP→错误类映射表（connectors/errors.py）

```python
def map_http_status(status_code: int, response_body: Any = None) -> type[ChronoForgeError]:
    """HTTP 状态码 → 错误类（D04 §3 映射表）"""
    match status_code:
        case 401 | 403:
            return AuthError
        case 429:
            return RateLimitError
        case 400 | 404 | 422:
            return ProviderError
        case status if status >= 500:
            return TransportError
        case _:
            if 400 <= status_code < 500:
                return ProviderError
            return TransportError
```

## 测试要求（D09 TC-C 组）

- **TC-C-008**：429 → RateLimitError（携带 retry_after）
- **TC-C-009**：401 → AuthError（不重试）
- **TC-C-010**：4xx≠429 → ProviderError（不重试）
- **unit**：FetchRequest 不可变（frozen=True）
- **unit**：RawBatch.payload 可任意类型（Any）
- **unit**：CapabilityMatrix 不可变（frozen=True）
- **unit**：map_http_status 全覆盖（401/403/429/400/404/422/500/502/503/自定义4xx）
- **unit**：错误类 context 字典携带 source/dataset/endpoint

## acceptance（GWT）

- [x] Given HTTP 429 When map_http_status Then 返回 RateLimitError 且携带 retry_after
- [x] Given HTTP 401 When map_http_status Then 返回 AuthError
- [x] Given valid FetchRequest When 构造 Then dataset_id/params/start/end/cursor 全部保留

## 验收清单（验收时填写）

DoD 16 项 + GWT 逐条勾选。

测试输出摘要：71 passed in 0.35s（pytest -v）｜接管性抽查：通过——所有实现均可凭任务单 + design_refs 理解｜git commit：待 Orchestrator 统一提交

## 执行记录

| 时间 | 事件 |
|---|---|
| 2026-09-14 | 实现完成：创建 base.py（协议+数据类）、更新 errors.py（RateLimitError.retry_after + map_http_status）、更新 exceptions.py（消除循环导入）、创建 test_connector_protocol.py（14 tests）和 test_error_mapping.py（35 tests）|
| 2026-09-14 | 验收完成：49 passed in 0.27s；协议+数据类/错误映射/HTTP映射表全部实现闭环；ruff + mypy 通过 |

## Deferred Acceptance

无。本子任务独立闭环，限流器（ACQUISITION-001.2）和重试装饰器（ACQUISITION-001.3）分别由子任务 2/3 实现。
