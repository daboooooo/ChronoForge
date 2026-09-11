# 03 — Data Model

依据：需求 §4/§12/§28/§30/§33/§34；约束 §9/§10/§20/§57/§66

## 1. Canonical Types（核心数据模型）

以 Canonical Type 为中心，不以 Source 为中心（需求 §47 约束 1/2）。全部类型定义于 `models/`，Pydantic v2。

| 域 | Type | 说明 |
|---|---|---|
| Market | TICKER / TRADE / OHLCV / ORDERBOOK / FUNDING / OPEN_INTEREST | 行情基础 |
| Derivatives | FUTURE / PERPETUAL / OPTION / IMPLIED_VOLATILITY / GREEKS | OPTION 独立于 TICKER；须拆解 underlying/expiry/strike/option_type（需求 §12） |
| Liquidation | LIQUIDATION_EVENT / LIQUIDATION_AGGREGATE | EVENT 独立于 TRADE；event_time 用交易所时间（需求 §9） |
| Macro | NUMBER / FLOW / MACRO_EVENT | 必须保留 observation/release/revision 三时间（需求 §14/§15） |
| Fundamental | FUNDAMENTAL / FILING / DOCUMENT | 保留 filing_date、accepted_datetime、accession_number（需求 §16/§17） |
| Positioning | POSITION / POSITION_AGGREGATE | participant_type 分类；COT net_position ≠ Binance OI（需求 §18） |
| Prediction | PREDICTION_MARKET / PREDICTION_PRICE | 同时保存 price 与 implied_probability + 原始值（需求 §23） |
| Text | TEXT_MESSAGE / TEXT_EVENT / SENTIMENT | SENTIMENT 属 Derived，不属 Raw（需求 §25） |
| Reference | STRUCTURED / ENTITY / INSTRUMENT | 实体与工具主数据 |
| Derived | DERIVED / FEATURE | 从上游计算，永远可重建 |

## 2. Instrument Identity（三级结构，需求 §34；约束 §20）

```
entity_id   — 标准化金融实体（BTC、AAPL）
instrument_id — 实体+工具类型+合约要素（BTC-PERP、BTC-2026-09-26-100000-C）
market_id   — 具体交易场所挂牌（BINANCE:BTCUSDT、DERIBIT:BTC-26SEP26-100000-C）
```

- 禁止用 ticker string 作全局唯一 ID
- OPTION instrument_name 必须解析为结构化字段：`underlying / expiry / strike / option_type / settlement_asset`
- `models/reference` 提供统一解析器；connector 只负责源格式→结构化字段映射

## 3. 时间语义（约束 §10；需求 §28）

统一时间字段词表，全部 UTC 存储（ISO 8601），禁止裸 `timestamp`：

| 字段 | 语义 | 适用域 |
|---|---|---|
| event_time | 事件实际发生时间 | 行情/强平/成交 |
| observation_time | 数据观测的时间区间 | 宏观/基本面 |
| effective_time | 数据生效时间 | 修订/合约变更 |
| release_time | 数据对外发布时间 | 宏观/财报 |
| publication_time | 文档公开时间 | filing/新闻 |
| transaction_time | 交易所事务时间 | 行情 |
| ingest_time | 本系统接收时间 | 全部（延迟测量） |
| revision_time | 修订版本时间 | 宏观/COT |

**Look-ahead 防护**：研究查询层强制 `available_time = release_time` 过滤——任何数据只能在其 release_time 之后被研究逻辑读取（约束 §11）。

## 4. Provenance（最小字段，需求 §30）

每条 Canonical 记录必须携带：

```
source, source_id, source_timestamp, ingest_timestamp, raw_record_id
```

源特定字段按类型附加：SEC→accession_number；FRED→series_id+vintage；Binance→exchange+symbol+market_type；Deribit→instrument_name；Polymarket→market_id+event_id+outcome_id。

## 5. Adjustment Policy（需求 §33）

任何调整保留四元组：`original_value / adjusted_value / adjustment_type / adjustment_timestamp`。股票 split/dividend 调整按此执行；Crypto 默认不做 corporate-action 调整；FRED/SEC/COT 修订走 version 追加，不覆盖。

## 6. Versioning（约束 §14/§66）

- 每条 Canonical 记录含 `schema_version`；每数据集含 `dataset_version`
- 同一历史数据变化 → 新版本追加（v1、v2 并存），禁止静默覆盖
- Revision 类数据（FRED vintage、CFTC 修订）按 (natural_key, revision_time) 多版本存储

## 7. Derived / Feature 依赖声明

DERIVED / FEATURE 记录除 provenance 外，必须携带 `dependencies: list[(dataset_id, dataset_version)]`：

- 多源 Feature（如 BTC_MARKET_STRESS = 波动率 + funding + OI + 爆仓失衡 + Deribit IV + Polymarket 概率，需求 §27）通过 dependencies 显式表达来源
- 重放确定性：同 dependencies 版本 + 同代码版本 → 相同输出（约束 §23）
- 与 provenance 双层配合，覆盖「Feature → Canonical → Raw → Source」全链追溯（需求 §45）
