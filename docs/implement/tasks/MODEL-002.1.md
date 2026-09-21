# MODEL-002.1 — market.py 类型 schema（OHLCV/TRADE/TICKER/FUNDING/OI/ORDERBOOK）

## 派发信息

- 任务单：D10 §2 MODEL-002 拆分（子任务 1/4）
- 设计依据：D02 §2 market.py 字段表、D02 §5 校验器、D01 §4（UTC naive datetime）
- Agent：Agent-C
- 派发时间：2026-09-11
- 状态：DONE（2026-09-11 验收通过）
- 依赖：MODEL-001（BaseRecord、CanonicalType 枚举）

## file_ownership

- `src/chronoforge/models/market.py`（新建）
- `src/chronoforge/models/__init__.py`（追加 market 类型导出）
- `tests/unit/test_market_types.py`（新建）

## 交付物契约摘要

### market.py 实现

按 D02 §2 market.py 字段表逐字段实现以下 Pydantic 模型（均继承 `BaseRecord`）：

1. **OHLCV**：market_id(str)、event_time(datetime)、open/high/low/close/volume(float)
   - 校验：high ≥ max(open, close)；low ≤ min(open, close)；全部 > 0
   - 时间：event_time + ingest_timestamp（D02 §4 矩阵第 1 行）
   - natural_key() → ("market_id", "event_time")（含 interval，interval 在 params 中）

2. **TRADE**：market_id、event_time、price(float)、quantity(float)、side(Side枚举)、trade_id(str)
   - 校验：price > 0、quantity > 0、math.isfinite()
   - natural_key() → ("market_id", "trade_id")

3. **TICKER**：market_id、event_time、last_price/bid/ask/volume_24h/quote_volume_24h(float)
   - 校验：last_price > 0、bid ≥ 0、ask ≥ 0、isfinite
   - natural_key() → ("market_id", "event_time")

4. **FUNDING**：market_id、event_time(datetime 结算时间)、funding_rate(float)、next_funding_time(datetime)
   - 校验：funding_rate 无界但需 isfinite、next_funding_time > event_time
   - natural_key() → ("market_id", "event_time")

5. **OPEN_INTEREST**：market_id、event_time、open_interest(float)、unit(str)
   - 校验：open_interest ≥ 0、isfinite
   - natural_key() → ("market_id", "event_time")

6. **ORDERBOOK**：market_id、event_time、transaction_time、bids(list[[price, qty]])、asks(list[[price, qty]])
   - 校验：bids/asks 元素长度=2、price > 0、qty ≥ 0、len ≤ 1000
   - natural_key() → ("market_id", "event_time", "transaction_time")

### Side 枚举

- Side.BUY = "BUY"、Side.SELL = "SELL"（D02 §2 TRADE/LIQUIDATION_EVENT 使用）

### 校验规则（D02 §5）

- 全部 float 字段：`math.isfinite()` 校验（拒绝 NaN/Inf）
- event_time ≤ ingest_timestamp + 5min 容差（由 pipeline 捕获为 QualityError，模型层不抛异常）
- 模块零依赖：仅 pydantic + stdlib + models.base

## 测试要求（D09 TC-M 组）

- **TC-M-004**：OHLCV 合法实例化（必填字段全提供）
- **TC-M-007**：TRADE side 枚举校验（BUY/SELL 合法值）
- **TC-M-008**：ORDERBOOK bids/asks 结构校验（list[[price, qty]]）
- **边界**：high==low 零区间 bar（允许）、high < low（拒绝）、price=NaN（拒绝）、quantity=Inf（拒绝）
- **失败**：unknown field → ValidationError（extra=forbid）、side 非法值 → ValidationError
- **property**：OHLCV 校验器对 high≥max(o,c) 和 low≤min(o,c) 的 9 种组合全覆盖（合法 4 种 × 非法 5 种）

## acceptance（GWT）

- [ ] Given OHLCV 合法字段 When 实例化 Then high≥max(o,c) 且 low≤min(o,c) 通过
- [ ] Given ORDERBOOK bids=[[1.0, 2.0], [3.0, 4.0]] When 实例化 Then bids 原样保存
- [ ] Given TRADE side="INVALID" When 实例化 Then ValidationError

## 验收清单（验收时填写）

DoD 16 项 + GWT 逐条勾选。

测试输出摘要：（待填）｜接管性抽查：（待填）｜git commit：（待填）

## 执行记录

| 时间 | 事件 |
|---|---|
| 2026-09-11 | Agent-C 交付（market.py + test_market_types.py + __init__.py 更新） |
| 2026-09-11 | Orchestrator 验收：58 用例全绿 + mypy strict 通过 + ruff 通过 |

How 层决策（Agent-C，验收认可）：
- OHLCV 字段顺序调整为 open/close 在 high/low 之前，确保 model_validator 可访问
- 使用 `@model_validator(mode="after")` 替代 `@field_validator` 做 cross-field 校验（high ≥ max(o,c)）
- `from __future__ import annotations` + 完整类型标注满足 mypy strict

## Deferred Acceptance

无。本子任务独立闭环。

## 验收清单（2026-09-11 验收通过）

DoD 16 项：

- [x] 实现 / [x] 公共 API（6 个模型 + Side 枚举签名）/ [x] 数据契约（D02 §2 字段表逐列实现）
- [x] 错误处理（ValidationError 语义，不吞不转）/ [x] 日志（N/A 纯模型）/ [x] 指标（N/A）
- [x] 单测（58 用例）/ [x] 边界（high==low/depth=1000/bid=0/open_interest=0）/ [x] 失败（NaN/Inf/非法枚举/extra字段）/ [x] 恢复（N/A 无持久化）
- [x] 集成（pytest 全绿 58 passed）/ [x] 静态分析（ruff check 通过）/ [x] 类型检查（mypy strict 通过）/ [x] 无未声明假设 / [x] 验收通过

acceptance（D10 GWT）：

- [x] Given OHLCV 合法字段 When 实例化 Then high≥max(o,c) 且 low≤min(o,c) 通过（TC-M-004 + test_ohlcv_9_combinations）
- [x] Given ORDERBOOK bids=[[1.0, 2.0], [3.0, 4.0]] When 实例化 Then bids 原样保存（TC-M-008）
- [x] Given TRADE side="INVALID" When 实例化 Then ValidationError（TC-M-007c）

测试输出摘要：pytest 58 passed；ruff check 无 issue；mypy strict no issues；__init__.py 导出验证通过

接管性抽查：✅ 通过——market.py 模块 docstring 标注设计依据（D02 §2），每个模型字段注释含语义来源，仅凭任务单 + D02 可理解并扩展

git commit：（待 Orchestrator 提交）
