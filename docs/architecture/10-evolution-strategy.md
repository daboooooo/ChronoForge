# 10 — Evolution Strategy

依据：约束 §38/§65/§66/§70/§72/§80/§82.10；需求 §45/§47

## 1. 演进原则

当前需求 + 已知可预见需求 + 清晰扩展点（约束 §70）；重大变更须走 ADR 流程 + Architecture Freeze Rule（约束 §85）。

## 2. 新增数据源（需求 §45 目标：不破坏已有模型）

```
验证官方 API 文档 → 注册 source_registry → 实现 connectors/{source}/ 
→ 补 fixture 契约测试 → 接入 pipeline
```

- 核心模块零修改（Open-Closed，约束 §38）；normalize 映射到既有 Canonical Type
- 若源提供新数据语义 → 先评估是否需要新 Canonical Type，而非塞进现有类型

## 3. 新增 Canonical Type（需求 §45 目标：不迫使 Connector 重写）

- 在 `models/` 定义 schema → storage 路由（canonical/{type}/）→ 各 connector 按需声明 capability 映射
- 既有 connector 不受影响；schema 变更走版本迁移（见 §4）

## 4. Schema 迁移（约束 §66）

- schema_version 字段 + `schema_versions` 迁移记录表
- 变更规则：新增字段（兼容，默认值）；改语义/删字段 → 新 schema_version + 迁移脚本 + 旧版本数据保留
- 禁止静默改变字段语义；Breaking change 必须 document + version + migration + test（约束 §65）

## 5. Provider 替换（约束 §80）

- 可替换边界：DataConnector 协议、Storage 协议、QueryService
- 替换 Yahoo → 新 connector 映射到同一 OHLCV canonical schema，下游 features/research 零改动（需求 §1 验收标准）

## 6. 存储与规模演进

| 触发条件 | 演进路径 |
|---|---|
| 数据量接近单机磁盘/查询性能极限（TB 级） | DuckDB → ClickHouse（列式 OLAP 平滑迁移：Parquet 导入）；触发时按约束 §42 补 ADR |
| 元数据需多进程并发写 | SQLite → PostgreSQL（MetaStore 协议不变） |
| 需要实时流摄入 | 直连 connector 增加 WebSocket 通道（已预留：raw ingest_batch_id + event/ingest 时间分离） |

## 7. 明确不做（约束 §72/§74）

- 不提前拆微服务/消息队列/分布式——仅当出现明确的数据规模、并发、故障隔离需求并经 ADR 评估后考虑
- 不引入 speculative abstraction（BaseProviderFactoryManager 类空抽象）
- 不引入 CoinGecko / 商业付费源 / 未验证 scraping（需求 §44）

## 8. 第一阶段交付边界

P0 connector（Binance Spot/Futures、Deribit、CCXT、Yahoo、FRED、SEC EDGAR）+ 三层存储 + 质量门槛 + registry + 查询服务；P1（CFTC/BLS/BEA/Fed/Polymarket）与 P2（GDELT/RSS）按需求 §36 顺序扩展，架构无需变更。

**P0 完成定义（DoD）**：每个 connector 具备——官方文档核对记录、source_registry 注册、契约测试（fixture）通过、增量 checkpoint 冒烟通过；基础设施具备——01 §7 四条验收测试全部通过。
