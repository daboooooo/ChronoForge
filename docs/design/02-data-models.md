# D02 — 数据模型详细设计

依据：架构 03。本文件字段级定义全部 Canonical Type，实施者据此编写 `models/`，测试者据此构造合法/非法样例。

## 1. 基础结构（models/base.py）

```python
class BaseRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)
    schema_version: str                        # "1.0"
    # provenance 五字段（架构 03 §4）
    source: str                                # source_registry.source_id
    source_id: str                             # 源侧唯一标识（symbol/series_id/cik...）
    source_timestamp: datetime | None          # 源侧时间，无则 None
    ingest_timestamp: datetime
    raw_record_id: str                         # "{source}:{dataset}:{jsonl_file}:{line_no}"
    # 质量（架构 05 §4）
    quality_status: QualityStatus = "VALID"    # VALID|SUSPECT|INVALID
    quality_reason: str | None = None
```

时间字段：不进 BaseRecord，由各 Type 显式声明（防"裸 timestamp"约束）。

```python
class CanonicalType(str, Enum):
    TICKER="TICKER"; TRADE="TRADE"; OHLCV="OHLCV"; ORDERBOOK="ORDERBOOK"
    FUNDING="FUNDING"; OPEN_INTEREST="OPEN_INTEREST"; OPTION="OPTION"
    IMPLIED_VOLATILITY="IMPLIED_VOLATILITY"; GREEKS="GREEKS"
    LIQUIDATION_EVENT="LIQUIDATION_EVENT"; LIQUIDATION_AGGREGATE="LIQUIDATION_AGGREGATE"
    NUMBER="NUMBER"; FLOW="FLOW"; MACRO_EVENT="MACRO_EVENT"
    FUNDAMENTAL="FUNDAMENTAL"; FILING="FILING"; DOCUMENT="DOCUMENT"
    POSITION="POSITION"; POSITION_AGGREGATE="POSITION_AGGREGATE"
    PREDICTION_MARKET="PREDICTION_MARKET"; PREDICTION_PRICE="PREDICTION_PRICE"
    TEXT_MESSAGE="TEXT_MESSAGE"; TEXT_EVENT="TEXT_EVENT"
    ENTITY="ENTITY"; INSTRUMENT="INSTRUMENT"
    DERIVED="DERIVED"; FEATURE="FEATURE"
```

## 2. 各 Type 字段表（Δ = BaseRecord 之外新增；金额 float64 USD 或标注币种字段；价格/数量 decimal 由 Pydantic `float` 承载并 range 校验）

### market.py

**OHLCV**（时间 = `event_time` 区间起点 + `interval: Interval` 枚举 1m/5m/1h/1d；身份 = market_id+event_time+interval）

| 字段 | 类型 | 校验 |
|---|---|---|
| market_id | str | 非空，`VENUE:SYMBOL:MKT` 格式 |
| open/high/low/close/volume | float | high≥max(o,c), low≤min(o,c), 全部>0 校验规则见 D06 Q-RANGE-001 |

**TRADE**：market_id、event_time、price、quantity、side(BUY/SELL)、trade_id(str，源侧)
**TICKER**：market_id、event_time、last_price、bid、ask、volume_24h、quote_volume_24h
**FUNDING**：market_id、event_time(结算时间)、funding_rate、next_funding_time
**OPEN_INTEREST**：market_id、event_time、open_interest(float, 币本位或张数→`unit: str`)
**ORDERBOOK**：market_id、event_time、transaction_time、bids/asks: list[[price, qty]]（top-N 快照，N≤1000；P0 仅快照，增量 P1）

### derivatives.py

**OPTION**：market_id、instrument_id、event_time、underlying(BTC/ETH…)、expiry(date)、strike(float)、option_type(C/P)、settlement_asset、mark_price、bid、ask
**IMPLIED_VOLATILITY**：instrument_id、event_time、iv(float 0~5)、mark_iv|bid_iv|ask_iv、implied_forward、data_tier：enum(OTM|ATM|FULL)——OTM/ATM 插值标注数据层级（需求：标注插值）
**GREEKS**：instrument_id、event_time、delta/gamma/vega/theta/rho(float)
**LIQUIDATION_EVENT**：market_id、event_time(交易所时间)、side(BUY=空头爆仓/SELL=多头爆仓，按交易所语义映射)、price、quantity、order_id(源侧)；身份键 = market_id+order_id
**LIQUIDATION_AGGREGATE**：market_id、event_time(区间起点)、interval、buy_vol/sell_vol/total_notional

### macro.py

**NUMBER**（存量/序列指标，FRED）：source_id=series_id、observation_time、release_time、revision_time(默认=release_time)、value(float|None 可为缺失)、units、seasonal_adjustment、vintage_date。身份键 = series_id+observation_time+revision_time
**FLOW**：同 NUMBER + period_start/period_end
**MACRO_EVENT**：event_ref(CPI/FOMC…)、scheduled_time、actual_time、actual/forecast/previous

### fundamental.py

**FILING**：entity_id、cik、accession_number、form_type(10-K/10-Q/8-K…)、filing_date、accepted_datetime、report_period_end、primary_doc_url、raw_record_id 必须指向 raw JSONL 行
**FUNDAMENTAL**：entity_id、cik、concept(Assets/Revenues/NetIncomeLoss…)、taxon(us-gaap/ifrs-full)、unit、observation_time(=period end)、value、fiscal_year、fiscal_period、frame
**DOCUMENT**：url、publication_time、content_hash、mime；content 本体存 raw 层

### positioning.py

**POSITION / POSITION_AGGREGATE**（COT）：report_date、release_time(周五 15:00 ET→UTC)、contract(MARKET 商品代码)、participant_type(enum: COMMERCIAL|NON_COMMERCIAL|NON_REPORTABLE|…)、long_positions、short_positions、spreading、net_position(计算=long-short)、revision_time

### prediction.py

**PREDICTION_MARKET**：source_id=market slug、event_id、question、outcomes: list[str]、close_time、status(OPEN/CLOSED/RESOLVED)
**PREDICTION_PRICE**：market_id+outcome_id、event_time、price(0~1)、implied_probability(=price 当以概率计价时保存原始值)、volume、liquidity

### text.py

**TEXT_MESSAGE**：source_id、event_time、author、text_hash、language；**TEXT_EVENT**：event_time、gkg_themes(仅 GDELT)、entities: list[entity_id]

### reference.py / derived.py

**ENTITY**：entity_id、canonical_name、entity_type(CRYPTO/EQUITY/INDEX/…)、aliases
**INSTRUMENT**：instrument_id、entity_id、instrument_type(SPOT/PERP/FUTURE/OPTION…)、合约要素(expiry/strike/option_type 可空)
**DERIVED/FEATURE**：name(如 BTC_MARKET_STRESS)、computed_at、dependencies: list[(dataset_id, dataset_version)]、value: float、params: dict

## 3. Instrument Identity 生成规则（models/reference.py）

| 层级 | 规则 | 示例 |
|---|---|---|
| entity_id | 标准化大写 ticker（映射表处理 WBTC→BTC 类别名） | `BTC`、`AAPL` |
| instrument_id | `{entity}-{TYPE}[-{expiry}-{strike}-{C\|P}]` | `BTC-SPOT`、`BTC-PERP`、`BTC-2026-09-26-100000-C` |
| market_id | `{VENUE}:{SYMBOL}:{MKT_TYPE}` | `BINANCE:BTCUSDT:SPOT`、`BINANCE:BTCUSDT:USDT-FUT`、`DERIBIT:BTC-26SEP26-100000-C:OPTION` |

- 解析函数：`parse_binance(symbol, market_type) -> (entity_id, instrument_id, market_id)`；`parse_deribit("BTC-26SEP26-100000-C")` → underlying/expiry(2026-09-26)/strike/option_type/settlement_asset——Deribit 月份码 J F M A M J J A S O N D + 2 位年 + 日，实施时以官方文档核对月份表（架构 05 §3）
- Yahoo：`AAPL` → `AAPL-SPOT` / `YAHOO:AAPL:SPOT`
- 全部经 `InstrumentResolver` 单点实现，connector 只调用不实现（架构 02 边界规则 6 同理）

## 4. 时间字段适用矩阵

| Type | event | observation | release | revision | ingest |
|---|---|---|---|---|---|
| OHLCV/TRADE/TICKER/FUNDING/OI/ORDERBOOK/LIQ.* | ● | | | | ● |
| OPTION/IV/GREEKS | ● | | | | ● |
| NUMBER/FLOW | | ● | ● | ● | ● |
| MACRO_EVENT | ●(scheduled) | | actual_time | | ● |
| FILING | | ●(period end) | ●(filing_date) | | ● |
| POSITION | | ●(report_date) | ● | ● | ● |
| PREDICTION_* | ● | | | | ● |
| TEXT_* | ●(publication) | | | | ● |
| DERIVED/FEATURE | ●(computed_at) | | | | ● |

## 5. 校验器（随模型内联，mypy strict）

- `@field_validator` 强制：价格>0、iv∈[0,5]、probability∈[0,1]、strike>0、全部数值字段 `math.isfinite()`（显式拒绝 NaN/Inf，审计 F-15）、expiry>今天的容差（过期合约不报错，仅 quality SUSPECT）
- `event_time <= ingest_timestamp + 5min` 容差，违反 → `QualityError` 上下文（由 pipeline 决定 flag 或阻断）

## 6. 对应测试依据（详见 D09 TC-M 组）

非法样例必须构造：负价格、high<low、裸 timestamp（extra="forbid" 生效）、未知 source_id、Deribit 月份码解析错、vintage 缺 release_time。
