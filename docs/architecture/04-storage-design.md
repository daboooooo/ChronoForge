# 04 — Storage Design

依据：需求 §26/§27；约束 §15/§16/§17

## 1. 总体布局

```
data/
├── raw/{source}/...            # Layer 1：不可变原始数据
├── canonical/{type}/...        # Layer 2：统一模型
├── derived/{domain}/...        # Layer 3：可重建派生数据
└── features/{entity}/...       # Feature（不污染 canonical）
meta/
└── chronoforge.db              # SQLite：元数据 + 运行状态
```

## 2. 各层设计

### Raw（不可变）
- 格式：JSONL（原始 API 响应逐行），按 `{source}/{dataset}/ingest_date=YYYY-MM-DD/` 分区
- 只追加，永不更新；保留 `raw_payload` 全文供审计与重放
- Raw 不可被 Canonical transformation 覆盖（需求 §26）

### Canonical
- 格式：Parquet（列式，约束 §17），Hive 风格分区 `{type}/entity={id}/year=YYYY/month=MM/`
- 写入模式：temp 目录写全量分片 → 原子 rename（幂等、可重放）
- 写入与幂等（upsert 机制）：Parquet 不支持原地更新，upsert = **分区级 merge-rewrite**——读目标分区旧文件 → 按 natural key 合并新批次 → temp 写 → 原子 rename 替换。写放大为每次更新重写整个分区文件；月度分区粒度下可控（个人研究写入量级），性能不达标时按约束 §42 补 ADR
- natural key 规范（同时即 quality_flags.record_key）：`canonical_type + market_id（或 entity_id）+ 事件主时间（按类型定义：OHLCV=区间起点、LIQUIDATION=event_id、NUMBER=observation_time+series_id）+ source_id`
- 每记录携带 provenance 五字段 + schema_version（见 03-data-model.md）

### Derived / Features
- 格式：Parquet，分区同上
- **可随时删除重建**（由 Canonical 重放生成）；出问题不修复，直接 replay（约束 §12）

## 3. 元数据（OLTP — SQLite）

单文件 SQLite，零部署，覆盖元数据/状态类低写入量数据（OLTP/OLAP 分离，约束 §16）：

| 表 | 内容 |
|---|---|
| source_registry | source_id、access_type、authentication、rate_limit、historical_limit、license、enabled（需求 §41） |
| dataset_registry | dataset_id、source_id、canonical_type、frequency、available_from/to、revision_supported（需求 §42） |
| checkpoints | (source, dataset) → last_cursor / last_success_time |
| run_log | 见 08-failure-model.md §4 |
| quality_flags | (record_key, quality_status, quality_reason) |
| schema_versions | schema_version → 迁移记录（仅 SQLite DDL；Parquet 见 §7） |

**并发约束**：SQLite 为单写者——pipeline 数据写入与元数据更新串行于单进程执行；同一 dataset 禁止并发 run（dataset 级锁 + 事务实现，违反即快速失败）。多任务并行需求触发演进路径见 10-evolution-strategy.md §6。

### Revision / Vintage 查询（as-of，防 look-ahead 落地）

- 修订类数据（FRED/CFTC/SEC）按 `(natural_key, revision_time)` 追加写入，历史版本永不覆盖（需求 §15）
- as-of 查询经 DuckDB 视图实现：`release_time <= asof`，且同一 natural key 取 `revision_time <= asof` 的最新版本
- 第一阶段不建二级索引，依赖分区裁剪 + 列扫描（数据量级支持）；性能不达标时按约束 §42 补 ADR

## 4. 查询路径（OLAP — DuckDB）

- DuckDB 作为**查询引擎**直接读 Parquet（无数据复制），对外暴露 `research.QueryService`
- 提供 canonical 视图层：统一 SQL 视图屏蔽物理分区细节
- Pandas/Polars 按任务选择（约束 §35）：研究探索用 Pandas，批量转换用 Polars/DuckDB SQL

## 5. 技术决策（Alternatives / Evidence / Confidence）

**Parquet + DuckDB（OLAP）** — Confidence: HIGH
- Alternatives：PostgreSQL/TimescaleDB（需常驻服务、单机研究场景过重）、ClickHouse（运维复杂度高）
- Evidence：约束 §15 明确推荐；Apache Parquet 列式+Row Group 裁剪、DuckDB 本地分析 SQL 均为官方文档支持
- Trade-off：并发写入弱 → 本项目单进程 pipeline 写入，可接受

**SQLite（元数据）** — Confidence: HIGH
- Alternatives：PostgreSQL（部署成本）、DuckDB 存元数据（OLAP 引擎不适合高频小事务/唯一约束）
- Evidence：约束 §16 OLTP/OLAP 分离；SQLite 为 OLTP 场景成熟方案
- Trade-off：单写者 → 写入串行化于 pipeline 单进程，符合单机架构

## 6. Retention 与容量

- raw / canonical：永久保留（个人研究规模，磁盘可承受）
- derived / features：可重建，可按需清理
- 增量处理：禁止全量载入内存，DuckDB predicate pushdown + 列裁剪（约束 §32）

## 7. Schema 迁移机制（两类分离）

- **SQLite（元数据）**：手写迁移函数按序号注册执行（表数量少，不引入 alembic 等框架——约束 §44，见 06 D11）
- **Parquet（数据）**：schema 变更 = 新 schema_version 写入新分区，旧数据由 replay 重写生成；不实现通用原地迁移工具

## 8. Retention 修订（文本类）

文本类源（GDELT / RSS，P2 级）全文量级未验证，retention 标记 **UNKNOWN**（约束 §79：Unknown 不等于 False，不猜测）；引入前必须按约束 §42 补 ADR，禁止默认「全文永久保留」假设。
