# MODEL-002.4 — reference/derived 类型 + natural_key() 完整实现

## 派发信息

- 任务单：D10 §2 MODEL-002 拆分（子任务 4/4）
- 设计依据：D02 §2 reference.py/derived.py 字段表、D02 §2 身份键定义、D02 §5 校验器
- Agent：Agent-C
- 派发时间：2026-09-11
- 状态：DONE
- 依赖：MODEL-001（BaseRecord、CanonicalType 枚举）
- 完成时间：2026-09-13
- 完成依据：reference/derived 类型 + natural_key() 完整实现验收通过

## file_ownership

- `src/chronoforge/models/reference.py`（新建，ENTITY/INSTRUMENT 数据模型部分，Resolver 逻辑属 MODEL-003）
- `src/chronoforge/models/derived.py`（新建）
- `src/chronoforge/models/__init__.py`（追加所有导出）
- `tests/unit/test_reference_types.py`（新建）
- `tests/unit/test_derived_types.py`（新建）

## 交付物契约摘要

### reference.py

1. **ENTITY**：entity_id(str 标准化大写)、canonical_name(str)、entity_type(EntityType枚举)、aliases(list[str])
   - EntityType.CRYPTO、EQUITY、INDEX、FOREX、COMMODITY、BOND、CRYPTOCURRENCY 等
   - natural_key() → ("entity_id",)

2. **INSTRUMENT**：instrument_id(str)、entity_id(str)、instrument_type(InstrumentType枚举)、expiry(date|None)、strike(float|None)、option_type(OptionType|None)、settlement_asset(str|None)
   - InstrumentType.SPOT、PERP、FUTURE、OPTION
   - 校验：option_type 存在时 strike > 0
   - natural_key() → ("instrument_id",)

### derived.py

3. **DERIVED**：name(str 如 BTC_MARKET_STRESS)、computed_at(datetime)、dependencies(list[tuple[str, str]] dataset_id+version)、value(float)、params(dict)、source_type(str)
   - 校验：value isfinite、len(dependencies) > 0
   - natural_key() → ("name", "computed_at")

4. **FEATURE**：同 DERIVED 结构、name(特征名)、computed_at、dependencies、value(float)、params(dict)、feature_engine_version(str)
   - 校验：value isfinite
   - natural_key() → ("name", "computed_at")

### natural_key() 完整实现

所有 Type 必须实现 `natural_key() -> tuple` 方法，返回 D02 §2 身份键列元组：

| Type | natural_key() |
|---|---|
| OHLCV | ("market_id", "event_time") |
| TRADE | ("market_id", "trade_id") |
| TICKER | ("market_id", "event_time") |
| FUNDING | ("market_id", "event_time") |
| OPEN_INTEREST | ("market_id", "event_time") |
| ORDERBOOK | ("market_id", "event_time", "transaction_time") |
| OPTION | ("instrument_id", "event_time") |
| IMPLIED_VOLATILITY | ("instrument_id", "event_time") |
| GREEKS | ("instrument_id", "event_time") |
| LIQUIDATION_EVENT | ("market_id", "order_id") |
| LIQUIDATION_AGGREGATE | ("market_id", "event_time", "interval") |
| NUMBER | ("source_id", "observation_time", "revision_time") |
| FLOW | ("source_id", "observation_time", "revision_time") |
| MACRO_EVENT | ("event_ref", "scheduled_time") |
| FILING | ("cik", "accession_number") |
| FUNDAMENTAL | ("entity_id", "concept", "observation_time") |
| DOCUMENT | ("url", "publication_time") |
| POSITION | ("contract", "report_date", "participant_type") |
| PREDICTION_MARKET | ("source_id", "event_id") |
| PREDICTION_PRICE | ("market_id", "outcome_id", "event_time") |
| TEXT_MESSAGE | ("source_id", "event_time", "author") |
| TEXT_EVENT | ("event_time", "gkg_themes") |
| ENTITY | ("entity_id",) |
| INSTRUMENT | ("instrument_id",) |
| DERIVED | ("name", "computed_at") |
| FEATURE | ("name", "computed_at") |

### 校验规则（D02 §5）

- 全部 float：math.isfinite()
- NUMBER value=None 合法（缺失）
- PREDICTION_PRICE price ∈ [0, 1]
- DOCUMENT content_hash 长度 64（sha256）
- 模块零依赖：仅 pydantic + stdlib + models.base

## 测试要求

- ENTITY：entity_type 枚举遍历合法值
- INSTRUMENT：option 型 expiry/strike/option_type 全提供合法、SPOT 型这些字段为空合法
- DERIVED：dependencies 为空拒绝、value=NaN 拒绝
- FEATURE：feature_engine_version 非空
- natural_key()：每个 Type 的 natural_key() 返回值与 D02 身份键声明逐列一致（架构测试）
- 边界：INSTRUMENT option_type 有但 strike=None（拒绝）、strike 有但 option_type=None（SPOT 合法）

## acceptance（GWT）

- [ ] Given INSTRUMENT type=SPOT When expiry/strike/option_type 为 None Then 成功
- [ ] Given INSTRUMENT type=OPTION When strike=None Then ValidationError
- [ ] Given DERIVED dependencies=[] When 实例化 Then ValidationError
- [ ] Given 全部 26 个 Type When natural_key() Then 返回值与 D02 身份键声明一致

## 验收清单（验收时填写）

DoD 16 项 + GWT 逐条勾选。

测试输出摘要：（待填）｜接管性抽查：（待填）｜git commit：（待填）

## Deferred Acceptance

无。本子任务独立闭环。
