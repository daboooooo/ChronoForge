# 01 — Architecture Overview

依据：`docs/ChronoForge.md`（需求）、`docs/ArchitectureConstraints.md`（约束 §5/§7/§45/§46/§82.1）

## 1. 系统定位（System Context）

ChronoForge 是**本地优先（Local-first）、单机模块化（Modular Monolith）**的个人金融数据基础设施，服务于量化研究、回测与 AI Agent。

- 外部系统：各数据源官方 API（Binance Spot/Futures、Deribit、CCXT、Yahoo、FRED、SEC EDGAR、CFTC、BLS、BEA、Fed、Polymarket、GDELT、RSS）
- 用户角色：研究者（CLI / Notebook / 查询 API）、AI Agent（同一查询 API）
- 明确不在范围内：交易执行、实时生产系统、分布式部署

## 2. 分层架构（Layered）

```
┌─────────────────────────────────────────┐
│ Applications: CLI / Research / AI Agent  │
├─────────────────────────────────────────┤
│ Analytics: Features / Factors / Backtest │
├─────────────────────────────────────────┤
│ Query: DuckDB 查询服务 / Domain Services │
├─────────────────────────────────────────┤
│ Canonical Data Model（Pydantic Schema）  │
├─────────────────────────────────────────┤
│ Data Quality / Validation               │
├─────────────────────────────────────────┤
│ Ingestion: Connectors / Normalization   │
├─────────────────────────────────────────┤
│ Storage: Raw / Canonical / Derived      │
│         + Metadata (Registry/RunLog)    │
├─────────────────────────────────────────┤
│ External Data Sources（官方 API）        │
└─────────────────────────────────────────┘
```

## 3. 数据流（单向、可重建）

```
SOURCE → RAW → CANONICAL → DERIVED → FEATURE → RESEARCH/AI
```

- 每层只消费上一层输出；`Raw→Canonical`、`Canonical→Derived` 必须可重复执行（幂等）
- 层间只通过 storage 接口读写，禁止跨层直写（如 Derived 直改 Raw）
- 质量问题以 quality flag 表达，禁止静默修正（约束 §50）

## 4. 依赖方向（Dependency Flow）

```
Application → Analytics → Canonical Model → Storage Abstraction → Concrete Provider
```

- 只允许向下依赖；Domain/Analytics 禁止 import 任何具体 connector（约束 §7）
- Provider 隔离全部 vendor 细节：auth、分页、限流、重试、source-specific 转换（约束 §8）
- 通过 import-linter 强制执行（见 07-testing-architecture.md）

## 5. 核心架构原则

1. **Source 可替换，Canonical Type 稳定**——替换 Yahoo 不影响下游（需求 §1）
2. **Raw 不可变**——异常数据打 quality flag，不覆盖（需求 §32）
3. **全链路 provenance**——Derived→Canonical→Raw→Source 四级可追溯（需求 §45）
4. **时间语义显式建模**——8 种时间字段，禁用单一 timestamp（需求 §28）
5. **observation_time ≠ release_time**——revision/vintage 不覆盖历史事实，防 look-ahead bias（需求 §15，约束 §11）

## 6. 关键架构决策摘要

| 决策 | 置信度 | 详见 |
|---|---|---|
| 单机 Modular Monolith，不做微服务/分布式 | HIGH | 约束 §45/§72 |
| Parquet + DuckDB 作 OLAP 存储 | HIGH | 06-technology-decisions.md |
| SQLite 作元数据 OLTP 存储 | HIGH | 04-storage-design.md |
| Pydantic v2 定义 Canonical Schema | HIGH | 03-data-model.md |
| Python 3.12+ 单语言实现 | HIGH | 约束 §4 |

## 7. 第一阶段验收标准（需求 §45 四原则的可验证化）

| 原则 | 验收测试（见 07-testing-architecture.md §3） |
|---|---|
| 新增数据源不破坏已有模型 | provider 可替换性契约测试 |
| 新增 Canonical Type 不迫使 Connector 重写 | connector 契约测试只依赖既有 type |
| Raw 永远可追溯 | provenance 字段完备性断言（Raw → Source） |
| Derived/Canonical 可逐级追溯与重建 | replay 逐字节一致性测试 |
