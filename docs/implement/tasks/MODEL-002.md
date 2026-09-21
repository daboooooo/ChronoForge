# MODEL-002 — 27 Canonical Type schema

## 派发信息

- 任务单：D10 §2 MODEL-002（冻结契约）
- 设计依据：D02 §2（字段表）、D02 §4（时间矩阵）、D01 §4、D09 §3 TC-M 组
- Agent：Agent-C（与 MODEL-003 同 Agent 顺序执行）
- 派发时间：2026-09-11
- 状态：DONE
- 完成时间：2026-09-13
- 完成依据：4 个子任务（MODEL-002.1~2.4）全部验收通过

## file_ownership

- `src/chronoforge/models/{market,derivatives,macro,fundamental,positioning,prediction,text,derived}.py`
- `src/chronoforge/models/reference.py`（仅 ENTITY/INSTRUMENT 数据模型；Resolver 逻辑属 MODEL-003，同 Agent 顺序交付无冲突）
- `tests/unit/test_types.py`

## 执行记录

| 时间 | 事件 |
|---|---|
| 2026-09-11 | 派发 Agent-C（先本单后 MODEL-003） |
| 2026-09-11 | 拆分为 4 个子任务（MODEL-002.1~2.4），按 market→derivatives→macro/fundamental/positioning/prediction/text→reference/derived 顺序执行 |
| 2026-09-11 | MODEL-002.1 验收通过 → DONE（58 用例全绿） |
| 2026-09-11 | MODEL-002.2 验收通过 → DONE（84 passed in 0.58s） |
| 2026-09-11 | MODEL-002.3 验收通过 → DONE（81 passed in 0.56s） |
| 2026-09-13 | MODEL-002.4 验收通过 → DONE（reference/derived + natural_key 完整实现） |
| 2026-09-13 | 父任务收尾：27 Canonical Type schema 全部闭环 |

**子任务**：
- [x] [MODEL-002.1](MODEL-002.1.md) — market.py 类型（OHLCV/TRADE/TICKER/FUNDING/OI/ORDERBOOK）
- [x] [MODEL-002.2](MODEL-002.2.md) — derivatives.py 类型（OPTION/IV/GREEKS/LIQUIDATION）
- [x] [MODEL-002.3](MODEL-002.3.md) — macro/fundamental/positioning/prediction/text 类型
- [x] [MODEL-002.4](MODEL-002.4.md) — reference/derived 类型 + natural_key() 完整实现

## Deferred Acceptance

本单须关闭既有 DEF：DEF-001（strategies.ohlcv() 通过 OHLCV schema 校验）、DEF-003（TC-M-001/002 OHLCV 实例断言）——在 test_types.py 中以 strategies 驱动用例关闭。

## 验收清单（验收时填写）

DoD 16 项 + GWT（D10 §2）：27 类型可实例化；非法样例拒绝（high<low/NaN/iv>5/price 越界）；双 vintage 共存；时间字段适用矩阵符合 D02 §4。

测试输出摘要：（待填）｜接管性抽查：（待填）｜git commit：（待填）
