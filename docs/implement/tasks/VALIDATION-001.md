# VALIDATION-001 — 质量规则引擎

## 派发信息

- 任务单：D10 §5 VALIDATION-001（冻结契约）
- 设计依据：D06 §1（规则引擎）、D06 §2（规则清单）、D06 §3（处置策略）
- Agent：待定
- 派发时间：2026-09-12
- 状态：READY
- 依赖：MODEL-002（BaseRecord、CanonicalType）、STORAGE-001（MetaStore.add_quality_flags 协议）

## file_ownership

- `src/chronoforge/quality/rules.py`（QualityRule 协议 + 规则引擎 + 17 条规则）
- `src/chronoforge/quality/report.py`（QualityReport + QualityFinding + GapContext）

## 执行记录

| 时间 | 事件 |
|---|---|
| 2026-09-12 | 拆分为 2 个子任务：引擎框架→17 条规则实现 |

**子任务**：
- [ ] [VALIDATION-001.1](VALIDATION-001.1.md) — 规则引擎框架（协议/注册表/run/report）（READY）
- [ ] [VALIDATION-001.2](VALIDATION-001.2.md) — 17 条质量规则实现（READY）

## Deferred Acceptance

无。本子任务为 DATA-SOURCE 和 PIPELINE 的质量门禁依赖。
