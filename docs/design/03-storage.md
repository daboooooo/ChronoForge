# D03 — 存储详细设计

依据：架构 04。本文档给出 SQLite DDL、Parquet 布局、merge-rewrite 算法、DuckDB 视图 SQL——可直接照抄实施。

## 1. SQLite 元数据库（meta/chronoforge.db，WAL 模式，foreign_keys=ON）

```sql
-- migrations/0001_init.py 按序执行（架构 04 §7）
CREATE TABLE source_registry(
  source_id TEXT PRIMARY KEY,            -- 'binance_spot'
  display_name TEXT NOT NULL,
  access_type TEXT NOT NULL CHECK(access_type IN ('PUBLIC','PUBLIC_WITH_KEY','AUTHENTICATED')),
  base_url TEXT NOT NULL,
  rate_limit_json TEXT NOT NULL,         -- {"req_per_min":1200,"weight_per_min":6000}
  historical_limit_days INT,             -- NULL=无限制/未知
  license TEXT NOT NULL,                 -- 访问条款摘要+URL
  enabled INT NOT NULL DEFAULT 1
);
CREATE TABLE dataset_registry(
  dataset_id TEXT PRIMARY KEY,           -- 'binance_spot.btcusdt.ohlcv_1m'（映射声明见 DDL 后）
  source_id TEXT NOT NULL REFERENCES source_registry(source_id),
  canonical_type TEXT NOT NULL,          -- CanonicalType 值
  entity_id TEXT NOT NULL,
  params_json TEXT NOT NULL,             -- {"symbol":"BTCUSDT","interval":"1m"}
  frequency TEXT,
  continuity_model TEXT NOT NULL CHECK(continuity_model IN
    ('ALWAYS_OPEN','TRADING_CALENDAR','EVENT_BASED','RELEASE_SCHEDULE')),  -- 审计 F-06
  status TEXT NOT NULL DEFAULT 'UNKNOWN' CHECK(status IN
    ('UNKNOWN','COMPLETE','PARTIAL','INCOMPLETE','QUARANTINED','STALE','REVISION_PENDING')),  -- 审计 F-10
  available_from TEXT, available_to TEXT,
  revision_supported INT NOT NULL DEFAULT 0,
  enabled INT NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL
);
CREATE TABLE checkpoints(
  source_id TEXT NOT NULL, dataset_id TEXT NOT NULL,
  last_cursor TEXT NOT NULL,             -- 源语义 cursor（D04 各源定义）
  last_success_time TEXT NOT NULL,
  PRIMARY KEY(source_id, dataset_id)
);
CREATE TABLE run_log(
  run_id TEXT PRIMARY KEY,
  source_id TEXT NOT NULL, dataset_id TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('PENDING','RUNNING','SUCCESS','PARTIAL_SUCCESS','FAILED','CANCELLED')),
  started_at TEXT NOT NULL, ended_at TEXT,
  input_count INT DEFAULT 0, output_count INT DEFAULT 0,
  error_count INT DEFAULT 0, warning_count INT DEFAULT 0,
  request_count INT DEFAULT 0, retry_count INT DEFAULT 0,          -- 审计 F-11
  duplicate_count INT DEFAULT 0, missing_count INT DEFAULT 0,
  latency_ms INT DEFAULT 0,
  checkpoint_before TEXT, checkpoint_after TEXT,
  chunk_success INT DEFAULT 0, chunk_failed INT DEFAULT 0,         -- 审计 F-01
  error_summary TEXT,                    -- 截断 2000 字符
  schema_version TEXT NOT NULL, code_version TEXT NOT NULL,
  ingest_batch_id TEXT NOT NULL
);
CREATE INDEX idx_run_log_ds_time ON run_log(dataset_id, started_at DESC);
CREATE TABLE quality_flags(
  record_key TEXT NOT NULL,              -- natural key（架构 04 §2）
  dataset_id TEXT NOT NULL,
  rule_id TEXT NOT NULL,                 -- Q-xx-nnn（D06）
  severity TEXT NOT NULL CHECK(severity IN ('ERROR','WARNING','INFO')),
  detail TEXT NOT NULL,
  raw_ref TEXT NOT NULL,                 -- jsonl 路径+行号（审计 F-08 quarantine 定位）
  payload_digest TEXT NOT NULL,          -- sha256(原始 payload)（重处理比对）
  run_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY(record_key, rule_id, run_id)
);
CREATE TABLE schema_versions(
  version TEXT PRIMARY KEY, applied_at TEXT NOT NULL,
  description TEXT NOT NULL              -- 仅 SQLite DDL 迁移
);
```

**分层映射声明（审计 F-17）**：howto §15 的 Raw/Validated/Canonical/Derived 四层在本设计中映射为——Validated 层职责由 ValidateStage（结构校验执行）+ quality_flags（结果记录）承担，不单独落盘；物理分层 = Raw → Canonical → Derived 三层 + quality_flags 表，语义等价。

**dataset_id 映射声明（审计 F-18）**：`dataset_id = {source_id}.{instrument}.{dataset_type[_timeframe]}`——howto §6.2 要求的 instrument/timeframe 维度编码于主键，checkpoints 以 (source_id, dataset_id) 定位。

**dataset 状态推导规则（审计 F-10，RunLogStage 终态时执行，按序判定）**：存在未处理 ERROR finding→`INCOMPLETE`；存在 QUARANTINED 记录→`QUARANTINED`；revision_supported 且修订扫描待处理→`REVISION_PENDING`；now − last_success_time > frequency×2→`STALE`；最新 run SUCCESS 且无 ERROR finding→`COMPLETE`；其余→`PARTIAL`。

**dataset 级锁**（架构 04 §3 并发约束）：`run_log` 插入 PENDING 行时 `BEGIN IMMEDIATE`；若存在该 dataset 的 RUNNING/PENDING 行 → 抛 `StorageError("dataset locked")` 快速失败。run 结束（任意终态）释放。

**ResearchSnapshot 表**：见 D07 §3（迁移 0002）。

## 2. Raw 层（storage/raw.py）

- 路径：`data/raw/{source}/{dataset}/ingest_date=YYYY-MM-DD/{HHmmss}-{seq}.jsonl`
- 每行 = `{"ingest_batch_id","fetched_at","url","payload": <原始响应 JSON>}`；**只追加**，append 后不可变
- 接口：

```python
class RawStore(Protocol):
    def append(self, source: str, dataset: str, batches: list[RawBatch]) -> list[RawRef]: ...
    # RawRef = jsonl 相对路径+行号区间 → 供 raw_record_id 构造
    def iter_refs(self, source: str, dataset: str, date: date | None = None) -> Iterator[RawRef]: ...
```

## 3. Canonical 层（storage/canonical.py）

- 路径：`data/canonical/{type}/entity={id}/year=YYYY/month=MM/part-{seq}.parquet`
- Parquet：pyarrow，`compression="zstd"`，`timestamp[us]`；写全字段 + parquet metadata 存 schema_version

**merge-rewrite upsert 算法**（架构 04 §2，函数 `CanonicalStore.upsert(records, type, entity) -> UpsertStats`）：

```
1. 按 (year, month) 将 records 分组 → 得到受影响分区集合 P
2. FOR p in P:
   a. new_df = 该分区新记录 → DataFrame（按 natural key 去重，保留最后）
   b. IF 分区无旧文件: 直接 temp 写 → rename（即 D05 原子写协议）
   c. ELSE: old = 读全部旧 parquet
      drift = new_df ⋈ old（natural key 相等 且 任一值列不同）     # 审计 F-04：值漂移检测
      IF drift 非空（非 revision 类）: 生成 Q-DRIFT-001 findings（record_key + 旧值/新值 digest 随行），
        经 QualityStage 写 quality_flags —— 禁止静默覆盖；P0 不做多版本保留
        （PROVISIONAL：漂移频次数据积累后评估版本化存储，触发时按约束 §42 补 ADR）
      merged = concat(old, new_df).sort_values("_revision_seq")
               .drop_duplicates(natural_key_cols, keep="last")
               .drop("_revision_seq")
      d. 写 p.tmp-{uuid}/ → fsync → rename 替换 p 的文件集；旧文件先 mv 至 p.old-{uuid} 再整体删除
3. 返回 stats{inserted, updated, drifted, rewritten_partitions}   # drifted = Q-DRIFT-001 计数（审计 F-04）
```

- `natural_key_cols` 由 `models` 按类型提供（`natural_key(CanonicalType) -> tuple[str,...]`，实现 D02 各"身份键"）
- **revision 类**（NUMBER/FLOW/POSITION/POSITION_AGGREGATE）：natural key 含 `revision_time`，天然多版本追加，不走去重路径的"keep last"合并冲突（同一 (nk, revision_time) 重复才合并）

**身份键逐类型对照表（冻结附件，审计 C-1 / §8）**：

`storage/base.py::_NATURAL_KEY_MAP` 为唯一实现，下表为其冻结镜像；与模型层 `natural_key()` 的一致性由架构测试 `tests/architecture/test_nk_mapping_consistency.py` 守卫。基准：D02 §2 显式声明身份的类型（OHLCV/LIQUIDATION_EVENT/NUMBER/FLOW）以声明为准，其余以模型层 `natural_key()` 实现为冻结基准。**任何变更必须走冻结契约变更流程（A-2），禁止在实现层单方面偏离。**

| CanonicalType | natural key 列 |
| --- | --- |
| OHLCV | market_id, event_time, interval |
| TRADE | market_id, trade_id |
| TICKER | market_id, event_time |
| FUNDING | market_id, event_time |
| OPEN_INTEREST | market_id, event_time |
| ORDERBOOK | market_id, event_time, transaction_time |
| OPTION | instrument_id, event_time |
| IMPLIED_VOLATILITY | instrument_id, event_time |
| GREEKS | instrument_id, event_time |
| LIQUIDATION_EVENT | market_id, order_id |
| LIQUIDATION_AGGREGATE | market_id, event_time, interval |
| NUMBER † | source_id, observation_time, revision_time |
| FLOW † | source_id, observation_time, revision_time |
| MACRO_EVENT | event_ref, scheduled_time |
| FUNDAMENTAL | entity_id, concept, observation_time |
| FILING | cik, accession_number |
| DOCUMENT | url, publication_time |
| POSITION † | contract, report_date, participant_type, revision_time |
| POSITION_AGGREGATE † | contract, report_date, participant_type, revision_time |
| PREDICTION_MARKET | source_id, event_id |
| PREDICTION_PRICE | market_id, outcome_id, event_time |
| TEXT_MESSAGE | source_id, event_time, author |
| TEXT_EVENT | event_time, gkg_themes |
| ENTITY | entity_id |
| INSTRUMENT | instrument_id |
| DERIVED | name, computed_at |
| FEATURE | name, computed_at |

† revision 类：natural key 含 `revision_time`。

**DerivedStore**：同布局 `data/derived/{name}/...`，接口 `write(rebuild=True)` 整体替换；`features/` 由 FeatureEngine 使用。

## 4. DuckDB 视图层（storage/views.py，`register_views(con: duckdb.DuckDBPyConnection)`）

```sql
CREATE OR REPLACE VIEW ohlcv AS
SELECT * FROM read_parquet('data/canonical/OHLCV/**/*.parquet', hive_partitioning=1);
-- 同模式建 view: trade/ticker/funding/open_interest/liquidation_event/
--   option/implied_volatility/number/position/prediction_price/feature ...
-- 点时（as-of）视图模板（NUMBER；其余 release 类同构）：
CREATE OR REPLACE VIEW number_asof AS
SELECT * EXCLUDE (rn) FROM (
  SELECT *, row_number() OVER (
    PARTITION BY source_id, observation_time
    ORDER BY revision_time DESC) AS rn
  FROM number
  WHERE release_time <= getvariable('asof')
) WHERE rn = 1;
```

- `QueryService` 用 `con.execute("SET VARIABLE asof = ?", [asof])` 绑定（D07）
- 视图名 = canonical type 小写，与 dataset_registry 对齐；新增 type 时必须同步注册视图（architecture test 断言两者集合一致）

## 5. 写入原子性协议（跨存储一致性，架构 08 §5）

1. 数据写入唯一完成边界 = **Parquet 分区 rename 成功**
2. rename 前 crash → temp/p.old 目录启动时清理（`storage.meta.cleanup_orphans()`）
3. rename 后 run_log 写失败 → 下次启动 reconciliation：扫描 ingest_batch_id 无终态 run_log 的分区 mtime，补记终态（SUCCESS 或 FAILED+备注）
4. 顺序恒为：raw append → canonical upsert → quality flags → run_log 终态
5. **checkpoint 损坏恢复（审计 F-14）**：SQLite 文件损坏 → 以 raw 层为事实源重建：`meta.rebuild_checkpoints(dataset_id)` = 重放该 dataset 全部 raw 分片（复用 D05 §4 replay 基础设施）→ 以 durable 数据边界反推 cursor；重建期间 dataset 持锁

## 6. 接口汇总（storage/base.py）

```python
class MetaStore(Protocol):
    def migrate(self) -> None: ...                    # 执行未应用迁移
    def try_lock_dataset(self, ds: str) -> RunRow: ...   # PENDING 插入或抛锁错误
    def finish_run(self, run_id: str, status: str, counts: RunCounts) -> None: ...
    def save_checkpoint(self, source: str, ds: str, cursor: str) -> None: ...
    def get_checkpoint(self, source: str, ds: str) -> str | None: ...
    def add_quality_flags(self, flags: list[QualityFlagRow]) -> None: ...
```

## 7. 测试依据（D09 TC-S 组）

- upsert 幂等：同批次连写两次 → 第二次 stats.inserted=0
- merge-rewrite 保留旧行：旧分区 3 行 + 新 1 行（同 nk）→ 3 行（1 更新 2 原样）
- 锁：两个线程 try_lock_dataset 同 dataset → 后者抛 StorageError
- 崩溃恢复：模拟 rename 后 run_log 缺失 → reconciliation 补记
- 视图 as-of：构造两 vintage，SET VARIABLE asof 断言取到正确版本
- 值漂移：旧分区 close=100，重拉同 nk close=101 → 记录被更新 + Q-DRIFT-001 finding 含旧/新值 digest（审计 F-04）

## 8. Compaction（PROVISIONAL，审计 F-12）

P0 不实现。触发条件（任一）：单分区 parquet 文件数 > 64 / raw 单日目录 jsonl 分片 > 256。届时按约束 §42 补 ADR：parquet 分区内合并重写（复用 §3 merge-rewrite 协议）、raw jsonl 按日归档压缩。禁止为 compaction 牺牲 append-only 语义。
