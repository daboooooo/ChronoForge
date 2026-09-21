# MODEL-002.2 — derivatives.py 类型 schema（OPTION/IV/GREEKS/LIQUIDATION）

## 派发信息

- 任务单：D10 §2 MODEL-002 拆分（子任务 2/4）
- 设计依据：D02 §2 derivatives.py 字段表、D02 §5 校验器、D01 §4（UTC naive datetime）
- Agent：Agent-C
- 派发时间：2026-09-11
- 状态：DONE
- 依赖：MODEL-001（BaseRecord、CanonicalType 枚举）

## file_ownership

- `src/chronoforge/models/derivatives.py`（新建）
- `src/chronoforge/models/__init__.py`（追加 derivatives 类型导出）
- `tests/unit/test_derivatives_types.py`（新建）

## 交付物契约摘要

### derivatives.py 实现

按 D02 §2 derivatives.py 字段表逐字段实现以下 Pydantic 模型（均继承 `BaseRecord`）：

1. **OPTION**：market_id、instrument_id、event_time、underlying(str BTC/ETH)、expiry(date)、strike(float)、option_type(OptionType枚举)、settlement_asset(str)、mark_price(float)、bid(float)、ask(float)
   - 校验：strike > 0、mark_price ≥ 0、bid ≥ 0、ask ≥ 0、expiry 可 ≤ 今天（不拒绝，SUSPECT）
   - OptionType.CALL = "CALL"、OptionType.PUT = "PUT"
   - natural_key() → ("instrument_id", "event_time")

2. **IMPLIED_VOLATILITY**：instrument_id、event_time、iv(float 0~5)、mark_iv/bid_iv/ask_iv(float)、implied_forward(float)、data_tier(DataTier枚举)
   - 校验：iv ∈ [0, 5]、mark_iv/bid_iv/ask_iv ≥ 0、isfinite、iv ≤ 5（D02 §5 显式要求）
   - DataTier.OTM = "OTM"、DataTier.ATM = "ATM"、DataTier.FULL = "FULL"
   - natural_key() → ("instrument_id", "event_time")

3. **GREEKS**：instrument_id、event_time、delta/gamma/vega/theta/rho(float)
   - 校验：全部 isfinite、delta ∈ [-1, 1]、gamma ≥ 0（希腊字母数学约束）
   - natural_key() → ("instrument_id", "event_time")

4. **LIQUIDATION_EVENT**：market_id、event_time(datetime 交易所时间)、side(Side枚举: BUY=空头爆仓/SELL=多头爆仓)、price(float)、quantity(float)、order_id(str)
   - 校验：price > 0、quantity > 0、isfinite
   - natural_key() → ("market_id", "order_id")

5. **LIQUIDATION_AGGREGATE**：market_id、event_time(datetime 区间起点)、interval(Interval枚举)、buy_vol/sell_vol/total_notional(float)
   - 校验：全部 ≥ 0、isfinite
   - natural_key() → ("market_id", "event_time", "interval")

### 时间字段（D02 §4 矩阵第 2 行）

- OPTION/IV/GREEKS：event_time + ingest_timestamp
- LIQUIDATION_EVENT/AGGREGATE：event_time + ingest_timestamp

### 校验规则（D02 §5）

- OPTION：iv > 5 拒绝（对 IMPLIED_VOLATILITY 的 iv 字段）
- 全部 float：math.isfinite()
- expiry 过期不拒绝（容许历史合约）
- 模块零依赖：仅 pydantic + stdlib + models.base

## 测试要求（D09 TC-M 组）

- **TC-M-005**：OPTION 完整解析（underlying/expiry/strike/option_type 全字段）
- **TC-M-006**：过期合约 OPTION（expiry < today）实例化通过
- **TC-M-009**：IMPLIED_VOLATILITY iv 边界 0/5 校验
- **TC-M-010**：GREEKS delta ∈ [-1, 1] 校验
- **边界**：iv=0（合法）、iv=5（边界合法）、iv=5.1（拒绝）、gamma=-1（拒绝）
- **失败**：option_type="INVALID" → ValidationError、order_id="" → ValidationError
- **property**：GREEKS 6 字段全 isfinite 对随机 float 生成的拒绝率=100%

## acceptance（GWT）

- [x] Given OPTION with expiry=2020-01-01 When 实例化 Then 成功（过期不拒绝）
- [x] Given IMPLIED_VOLATILITY iv=6.0 When 实例化 Then ValidationError
- [x] Given GREEKS delta=1.5 When 实例化 Then ValidationError

## 验收清单

DoD 16 项 + GWT 逐条勾选。

测试输出摘要：84 passed in 0.58s ｜接管性抽查：全部通过 ｜git commit：待提交

## Deferred Acceptance

无。本子任务独立闭环。
