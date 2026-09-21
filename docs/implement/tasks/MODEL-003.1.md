# MODEL-003.1 — parse_binance 解析器

## 派发信息

- 任务单：D10 §3 MODEL-003 拆分（子任务 1/2）
- 设计依据：D02 §3 表（三级 ID 生成规则）、D02 §3 解析函数规范
- Agent：Agent-C
- 派发时间：2026-09-11
- 状态：DONE
- 依赖：MODEL-001（BaseRecord）、MODEL-002（ENTITY/INSTRUMENT 模型）

## file_ownership

- `src/chronoforge/models/reference.py`（追加 parse_binance 函数 + EntityResolver 类部分）
- `tests/unit/test_parser_binance.py`（新建）

## 交付物契约摘要

### parse_binance(symbol: str, market_type: str) -> tuple[str, str, str]

按 D02 §3 表实现 Binance symbol → (entity_id, instrument_id, market_id) 解析：

**输入**：
- symbol：Binance 交易对，如 "BTCUSDT"、"ETHUSDT"、"BTC-USDT"
- market_type：SPOT、USDT-FUT、COIN-FUT

**输出**：
- entity_id：标准化大写 ticker，"BTCUSDT" → "BTC"、"ETHUSDT" → "ETH"、"BNBUSDT" → "BNB"
- instrument_id：`{entity}-{TYPE}`，如 "BTC-SPOT"、"BTC-PERP"
- market_id：`{VENUE}:{SYMBOL}:{MKT_TYPE}`，如 "BINANCE:BTCUSDT:SPOT"、"BINANCE:BTCUSDT:USDT-FUT"

**实现规则**：
1. venue 固定 "BINANCE"
2. symbol 去后缀：USDT/BTC/ETH/BUSD → 剩余部分为 entity（大写）
3. market_type 映射：SPOT→SPOT、USDT-FUT/COIN-FUT→PERP
4. entity_id 校验：长度 ≥ 2（防 "USDT" → ""）

### 错误处理

- 无法识别 symbol 格式 → ProviderError("无法解析 Binance symbol: {symbol}")
- symbol 长度 < 4 → ProviderError

### EntityResolver 类（部分）

- `resolve(source: str, symbol: str, market_type: str)` → (entity_id, instrument_id, market_id)
- 单点实现，connector 只调用不实现解析逻辑

## 测试要求

- **TC-M-004**：BTCUSDT SPOT → ("BTC", "BTC-SPOT", "BINANCE:BTCUSDT:SPOT")
- **TC-M-011**：ETHUSDT USDT-FUT → ("ETH", "ETH-PERP", "BINANCE:ETHUSDT:USDT-FUT")
- **TC-M-012**：BNBUSDT SPOT → ("BNB", "BNB-SPOT", "BINANCE:BNBUSDT:SPOT")
- **边界**：symbol="AUSDT"（2字母entity合法）、symbol="USDT"（无entity拒绝）
- **失败**：symbol="" → ProviderError、symbol="INVALID" → ProviderError
- **property**：parse_binance → EntityResolver.resolve 结果一致

## acceptance（GWT）

- [x] Given "BTCUSDT" SPOT When parse_binance Then ("BTC", "BTC-SPOT", "BINANCE:BTCUSDT:SPOT")
- [x] Given "ETHUSDT" USDT-FUT When parse_binance Then ("ETH", "ETH-PERP", "BINANCE:ETHUSDT:USDT-FUT")
- [x] Given "USDT" When parse_binance Then ProviderError

## 验收清单（验收时填写）

DoD 16 项：

- [x] 实现 / [x] 公共 API / [x] 数据契约 / [x] 错误处理（ProviderError 语义）
- [x] 日志（N/A 解析函数）/ [x] 指标（N/A）/ [x] 单测（13 用例）/ [x] 边界（hyphenated/short/empty/invalid）
- [x] 失败（空串/无效symbol/无效market_type）/ [x] 恢复（N/A 无持久化）/ [x] 集成（EntityResolver.resolve 一致）
- [x] 静态分析（ruff 通过）/ [x] 类型检查（mypy strict 通过）/ [x] 无未声明假设 / [x] 验收通过

测试输出摘要：pytest 13 passed（全库 293 passed）；ruff All checks passed；mypy no issues

接管性抽查：✅ 通过——reference.py 新增 parse_binance/EntityResolver/docstring 含设计依据（D02 §3），异常定义清晰，仅凭任务单 + D02 可理解并扩展

git commit：待提交

## Deferred Acceptance

无。本子任务独立闭环。
