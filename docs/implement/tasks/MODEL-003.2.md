# MODEL-003.2 — parse_deribit + parse_yahoo + InstrumentResolver 完整

## 派发信息

- 任务单：D10 §3 MODEL-003 拆分（子任务 2/2）
- 设计依据：D02 §3 表（三级 ID 生成规则）、D02 §3 解析函数规范、D02 §3 Deribit 月份码表
- Agent：Agent-C
- 派发时间：2026-09-11
- 状态：DONE
- 完成时间：2026-09-13
- 完成依据：38 passed in 0.73s；parse_deribit + parse_yahoo + InstrumentResolver 完整验收通过
- 依赖：MODEL-001（BaseRecord）、MODEL-002（ENTITY/INSTRUMENT 模型）

## file_ownership

- `src/chronoforge/models/reference.py`（追加 parse_deribit/parse_yahoo + InstrumentResolver 完整类）
- `tests/unit/test_parser_deribit.py`（新建）
- `tests/unit/test_parser_yahoo.py`（新建）
- `tests/unit/test_instrument_resolver.py`（新建）

## 交付物契约摘要

### parse_deribit(instrument_name: str) -> dict

按 D02 §3 实现 Deribit instrument_name → 解析结果：

**输入格式**：`{UNDERLYING}-{DD}{MON}{YY}-{STRIKE}-{C\|P}`
示例：`BTC-26SEP26-100000-C`

**输出**：
```python
{
    "underlying": "BTC",
    "expiry": date(2026, 9, 26),  # 解析为 date
    "strike": 100000.0,
    "option_type": "CALL",  # C→CALL, P→PUT
    "settlement_asset": "USD",  # Deribit 默认 USD
    "instrument_id": "BTC-2026-09-26-100000-C",  # 标准化格式
    "market_id": "DERIBIT:BTC-26SEP26-100000-C:OPTION",
}
```

**月份码表（D02 §3）**：
- J=Jan, F=Feb, M=Mar, A=Apr, M=May, J=Jun, J=Jul, A=Aug, S=Sep, O=Oct, N=Nov, D=Dec
- 注意：M=Mar, M=May（需上下文区分：A后M=May，无A前M=Mar）
- 实现策略：按位置解析（2位年 + 1位月份码 + 2位日）

**解析规则**：
1. 按 `-` 分割为 [underlying, date_str, strike, option_type]
2. date_str = "26SEP26" → 日(26) + 月份码(SEP) + 年(26) → date(2026, 9, 26)
3. 年 2 位：≥50 → 19xx，<50 → 20xx
4. strike → float
5. C → CALL, P → PUT

**非法月份码拒绝**：月份码不在 {J,F,M,A,S,O,N,D} 中 → ProviderError

**过期合约**：expiry < today → 不拒绝，返回结果中标注 "expired": True

### parse_yahoo(symbol: str) -> tuple[str, str, str]

**输入**：Yahoo Finance ticker，如 "AAPL"、"MSFT"、"SPY"
**输出**：(entity_id, instrument_id, market_id)

**实现规则**：
1. entity_id = symbol.upper().strip()
2. instrument_id = f"{entity_id}-SPOT"
3. market_id = f"YAHOO:{entity_id}:SPOT"
4. 简单映射，无复杂解析

### InstrumentResolver 完整类

```python
class InstrumentResolver:
    def resolve(self, source: str, identifier: str, market_type: str = None) -> dict:
        """统一入口，按 source 分发到 parse_* 函数。"""
        ...

    def resolve_binance(self, symbol: str, market_type: str) -> tuple[str, str, str]:
        ...

    def resolve_deribit(self, instrument_name: str) -> dict:
        ...

    def resolve_yahoo(self, symbol: str) -> tuple[str, str, str]:
        ...
```

**约束**：connector 只调用 InstrumentResolver，不直接调用 parse_* 函数（单点实现原则）。

## 测试要求

- **TC-M-005**：BTC-26SEP26-100000-C → underlying=BTC/expiry=2026-09-26/strike=100000/CALL
- **TC-M-006**：expiry < today 的合约解析成功（不拒绝）
- **TC-M-013**：BTC-32FOO26-100000-C 非法月份码 → ProviderError
- **TC-M-014**：AAPL → ("AAPL", "AAPL-SPOT", "YAHOO:AAPL:SPOT")
- **边界**：Deribit 26JAN26（Jan合法）、26XAN26（X非法拒绝）
- **失败**：instrument_name="" → ProviderError、strike="abc" → ProviderError
- **property**：InstrumentResolver.resolve 按 source 正确分发

## acceptance（GWT）

- [x] Given "BTC-26SEP26-100000-C" When parse_deribit Then expiry=date(2026, 9, 26)
- [x] Given "BTC-32FOO26-100000-C" When parse_deribit Then ProviderError
- [x] Given "AAPL" When parse_yahoo Then ("AAPL", "AAPL-SPOT", "YAHOO:AAPL:SPOT")
- [x] Given 过期合约 When parse_deribit Then 返回结果含 "expired": True

## 验收清单（验收时填写）

DoD 16 项 + GWT 逐条勾选。

测试输出摘要：38 passed in 0.73s ｜接管性抽查：（待填）｜git commit：（待填）

## Deferred Acceptance

无。本子任务独立闭环。
