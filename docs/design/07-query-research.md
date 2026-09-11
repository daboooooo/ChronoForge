# D07 — 查询与研究服务详细设计

依据：架构 02 §5、04 §4、03 §7。定义 QueryService 实现、FeatureEngine、ResearchSnapshot——研究者与 AI Agent 的唯一数据出口。

## 1. QueryService（research/query.py）

```python
class DuckDBQueryService:
    def __init__(self, con: duckdb.DuckDBPyConnection, registry: DatasetRegistry): ...

    def query(
        self,
        dataset_id: str,
        columns: list[str] | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        asof: datetime | None = None,       # 缺省=now()；点时视图语义见下
        filters: Mapping[str, Any] | None = None,   # {"market_id": "..."} 相等过滤
    ) -> QueryResult: ...
```

实现规则（按序执行，生成单条 SQL）：

1. `dataset_id` → registry 查 canonical_type → 视图名（type 小写）
2. 时间列选择：OHLCV/TRADE… = event_time；NUMBER/FILING = observation_time；start/end → `WHERE ts >= ? AND ts < ?`（分区裁剪由 DuckDB hive 过滤自动下推）
3. **as-of 语义**：
   - revision_supported=false → 过滤 `release_time`（若有）或直接放行
   - revision_supported=true → 切换 `{type}_asof` 视图（D03 §4）+ `SET VARIABLE asof`
4. filters 白名单列（视图列存在性校验，防注入：列名来自 introspection，值全部参数化）
5. columns 投影；**恒附加** dataset_version/schema_version 元信息
6. **默认排序（审计 F-16）**：按主时间列升序（时间列选择同规则 2），并列按 natural key 次列稳定排序；不可配置关闭（保证结果确定性/可复现）

```python
@dataclass
class QueryResult:
    frame: pl.DataFrame                   # polars（批量语义，架构 04 §4）；.to_pandas() 惰性供研究
    dataset_id: str; dataset_version: str; schema_version: str
    row_count: int; elapsed_ms: int
```

- dataset_version 语义：该 dataset 最近一次 SUCCESS run 的 code_version+schema_version 复合（存 dataset_registry 扩展列 `current_version`，迁移 0003）
- 只读：QueryService 持有的 DuckDB 连接以 read_only 模式打开数据目录（架构 02 规则 4 的执行点）

## 2. FeatureEngine（features/engine.py）

```python
class FeatureEngine(Protocol):
    name: str
    dependencies: list[dataset_id]        # 声明式（架构 03 §7）
    def compute(self, q: QueryService, params: Mapping) -> pl.DataFrame: ...
    def register(self) -> None: ...       # 写 dataset_registry（FEATURE 类型）+ 版本
```

内置 P0 特征（需求 §27 子集，全部声明 dependencies）：

| name | dependencies | 公式（参数化） |
|---|---|---|
| returns | {ohlcv} | log(c_t/c_{t-1}) |
| realized_vol | {ohlcv} | rolling std(returns, window) |
| iv_surface | {implied_volatility} | 每 expiry 按 moneyness=k/S 聚合 → ATM/OTM 层级标注 |
| funding_oi_divergence | {funding, open_interest} | Δfunding·ΔOI 符号背离标记 |
| btc_market_stress（需求 §27） | {realized_vol, funding, open_interest, liquidation_aggregate, implied_volatility, prediction_price} | z-score 加权合成（weights=参数） |

规则：特征计算只经 QueryService 取数；输出写 DerivedStore（dependencies + params + code_version 随行存储）；replay 确定性 = dependencies 版本 + code_version → output hash 一致（D05 §4 验收）。

## 3. ResearchSnapshot（research/snapshot.py，SQLite 迁移 0002）

```sql
CREATE TABLE research_snapshot(
  snapshot_id TEXT PRIMARY KEY,           -- uuid hex 12
  created_at TEXT NOT NULL,
  datasets_json TEXT NOT NULL,            -- [{"dataset_id","dataset_version"}]
  code_version TEXT NOT NULL,             -- chronoforge.__version__
  params_json TEXT NOT NULL,              -- 研究参数
  output_hash TEXT NOT NULL,              -- sha256(结果序列化)
  notebook_ref TEXT, query_text TEXT      -- 可选溯源
);
```

```python
@contextmanager
def research_snapshot(qs, datasets: list[str], params: dict):
    # 进入：锁定各 dataset 版本；退出：计算 output_hash 入库
    # 复现：snapshot_reproduce(snapshot_id) → 重跑并比对 output_hash（架构 02 §5 契约）
```

## 4. CLI 查询面（详见 D08 命令树）

`chronoforge query --dataset X [--asof ...] [--start --end] [--json|--csv]` 输出（AI Agent 消费 JSON：QueryResult 元信息 + 行数据）；`chronoforge research reproduce --snapshot ID`。

## 5. 测试依据（D09 TC-R 组）

- as-of：两 vintage 构造（D03 §7 用例复用）→ asof=T1 取 v1，asof=T2 取 v2，未 release 不可见
- 注入防御：filters 含 `; DROP` 与未知列 → ValueError，连接无恙
- read_only：QueryService 连接上尝试写 → 异常
- snapshot 复现：同 snapshot 重复 reproduce → hash 一致；改数据后 reproduce → hash 不一致且报告 dataset_version 变化
- 特征链：returns→realized_vol 依赖注册 + replay 字节一致（架构 07 验收）
