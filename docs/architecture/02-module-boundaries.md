# 02 — Module Boundaries

依据：约束 §36/§37/§38/§82.2；需求 §37/§43

## 1. 模块清单

| 模块 | Purpose / Responsibilities | 依赖（仅允许以下） | Public Interfaces |
|---|---|---|---|
| `connectors/` | 数据源接入：auth、请求、分页、限流、重试、原始响应返回 | `models`(协议), `config` | `DataConnector` 协议；按源子模块（binance/spot、binance/futures、deribit、ccxt、yahoo、fred、sec、cftc、bls、bea、federal_reserve、polymarket、gdelt、rss） |
| `models/` | Canonical 数据模型：全部 Canonical Type 的 Pydantic schema、时间语义、provenance 字段 | 无（最底层） | 各 Canonical Type 类 + 校验器 |
| `storage/` | raw/canonical/derived 三层读写、分区管理、元数据库访问 | `models` | `RawStore` / `CanonicalStore` / `DerivedStore` / `MetaStore` 协议 |
| `quality/` | schema/时间戳/重复/缺口/null/范围校验；quality flag 生成 | `models` | `validate(records, canonical_type) -> QualityReport` |
| `registry/` | source_registry、dataset_registry 管理 | `storage` | `SourceRegistry` / `DatasetRegistry` |
| `pipeline/` | 编排：fetch→raw→validate→normalize→canonical→quality→run_log | `connectors`, `storage`, `quality`, `registry`, `config` | `run(dataset_id)` / `replay(layer, dataset_id)` |
| `features/` | Derived 指标与 Feature 计算（returns/vol/surface/sentiment 等） | `storage`, `models` | `FeatureEngine` 协议 |
| `research/` | 查询与分析工具（notebook/agent 复用；非 notebook 内实现）；研究结果快照 ResearchSnapshot（约束 §23/§52 可复现性） | `storage`, `features` | `QueryService` / `ResearchSnapshot` |
| `cli/` | 薄层：参数解析、配置加载、调用 pipeline/research、渲染结果 | `pipeline`, `research`, `config` | typer 命令组 |
| `config/` | pydantic-settings 配置定义，显式传递；多环境 default/development/test/production（约束 §25） | 无 | `Settings` |

## 2. 关键协议（Interface Before Implementation，约束 §37）

```python
class DataConnector(Protocol):
    def discover(self) -> list[MarketRef]: ...
    def fetch(self, request: FetchRequest) -> RawBatch: ...
    def normalize(self, raw: RawBatch) -> list[CanonicalRecord]: ...
    def validate(self, data: list[CanonicalRecord]) -> QualityReport: ...
    def checkpoint(self) -> Checkpoint: ...
    def health(self) -> HealthStatus: ...
    def capabilities(self) -> CapabilityMatrix: ...  # 需求 §38
```

- 扩展方法（fetch_ohlcv/fetch_options/...）为可选能力，通过 `capabilities()` 声明，不强制实现
- Connector **不负责**：投资逻辑、因子计算、策略（约束 §8）

## 3. 模块边界规则

1. 禁止 `models` 依赖任何其他业务模块（零依赖底层）
2. 禁止 `quality` 依赖 `connectors`（校验基于 Canonical schema，不基于源）
3. `cli` 不含业务逻辑/SQL（约束 §62）
4. `research` 不修改数据，只读（约束 §64）
5. 新增数据源 = 新增 `connectors/{source}/` 子模块 + registry 注册，**不修改核心模块**（Open-Closed，约束 §38）
6. **写职责独占**：connector 只返回 `RawBatch`，不写任何存储层；raw/canonical/derived 的写入由 pipeline 独占执行（保证幂等与对账边界单一）

## 4. 失败模式（模块级）

| 模块 | 主要失败模式 | 处理 |
|---|---|---|
| connectors | 网络/限流/认证/schema 变更 | 错误分类 + 有上限重试（08-failure-model.md） |
| pipeline | 部分源失败 | PARTIAL_SUCCESS 状态 + 按 dataset 粒度恢复 |
| quality | 校验不过 | 写 quality flag，不阻断整批（可配置） |
| storage | 写入中断 | 分区原子写（temp + rename），幂等重放 |

## 5. QueryService 接口（研究者 / AI Agent 唯一数据出口）

```python
class QueryService(Protocol):
    def query(
        self,
        dataset_id: str,
        columns: list[str] | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        asof: datetime | None = None,          # 点时视图：过滤 release_time/revision（防 look-ahead）
        filters: Mapping[str, Any] | None = None,
    ) -> QueryResult: ...
```

- `asof` 缺省为当前时间；实现语义见 04-storage-design.md「Revision / Vintage 查询」
- `QueryResult` = 数据帧（Pandas/Polars，按 04 §4 规则选择）+ `dataset_version` + `schema_version` 元信息
- research 层其余分析函数只经 QueryService 取数，不直连存储
- `ResearchSnapshot`：研究产出时记录 `{datasets: [(dataset_id, version)], code_version, parameters, output_hash}`——同输入 + 同代码 → 可复现比对（约束 §23/§52，机制与 03 §7 dependencies 同构）
