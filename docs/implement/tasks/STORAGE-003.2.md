# STORAGE-003.2 — drift 检测 + Q-DRIFT-001 findings

## 派发信息

- 任务单：D10 §2 STORAGE-003 拆分（子任务 2/2）
- 设计依据：D03 §3（值漂移检测）、D06 §2（Q-DRIFT-001 规则）
- Agent：待定
- 派发时间：2026-09-12
- 状态：READY
- 依赖：STORAGE-003.1（upsert 核心 + UpsertStats）、MODEL-002（natural_key）、VALIDATION-001（QualityReport 协议）

## file_ownership

- `src/chronoforge/storage/canonical.py`（追加 drift 检测逻辑）
- `src/chronoforge/quality/rules.py`（Q-DRIFT-001 规则注册）
- `tests/integration/test_canonical_drift.py`（新建）

## 交付物契约摘要

### drift 检测逻辑（集成在 CanonicalStore.upsert 的 merge-rewrite 步骤中）

在 merge-rewrite 步骤 c 中，old_df 与 new_df 合并前执行值漂移检测：

```
1. drift = new_df ⋈ old_df（natural key 相等）
2. FOR each matched pair (old_record, new_record):
   a. 逐列比对值列（排除 natural_key 列）
   b. IF 任一值列不同:
      - 计算旧值 digest = sha256(old_value_serialized)
      - 计算新值 digest = sha256(new_value_serialized)
      - 生成 QualityFinding{
          record_key=natural_key_string,
          rule_id="Q-DRIFT-001",
          severity="WARNING",
          detail=json{dataset_id, old_digest, new_digest, changed_columns}
        }
3. drift 结果通过 context 传入 QualityStage（不抛异常）
```

### 与 QualityStage 集成

- `CanonicalStore.upsert` 返回的 `UpsertStats` 增加 `drifted: int` 字段
- drift findings 通过 `RunContext.drift_findings: list[QualityFinding]` 传入 QualityStage
- QualityStage 对 Q-DRIFT-001 findings 调用 `meta.add_quality_flags()` 写入 quality_flags 表
- **禁止静默覆盖**：每次 upsert 的 drift 必须记录，无例外

### Q-DRIFT-001 规则注册（quality/rules.py）

在 VALIDATION-001.2 注册的基础上，本任务补充 Q-DRIFT-001 的 check 逻辑入口：

```python
class QDRIFT001Rule(QualityRule):
    rule_id = "Q-DRIFT-001"
    applies_to: frozenset[CanonicalType] = ALL - {revision_supported types}
    severity = "WARNING"
```

- revision 类（NUMBER/POSITION，`revision_supported=true`）排除在 Q-DRIFT-001 外（天然多版本）
- 排除逻辑：查 `dataset_registry.revision_supported`（由 Storage layer 传入 context）

### 值 digest 计算

```python
import hashlib
import json

def value_digest(value: Any) -> str:
    """序列化值并计算 sha256 hex digest"""
    serialized = json.dumps(value, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode()).hexdigest()
```

- datetime 类型：用 ISO 字符串序列化（UTC naive）
- None 值：序列化 `"null"`
- 数值：保持精度（float → str 不截断）

## 测试要求（D09 TC-S/TC-Q 组）

- **TC-S-006 联动**：旧 close=100，重拉同 nk close=101 → 记录被更新 + Q-DRIFT-001 finding 含旧/新 digest
- **TC-Q-009**：Q-DRIFT-001 触发条件——同 nk 值变化 → finding 含旧/新 digest；值不变 → 无 finding
- **revision 类排除**：NUMBER 同 (nk, revision_time) 多版本 → 不触发 Q-DRIFT-001
- **边界**：全部列相同（仅 nk 匹配）→ drifted=0；全部列不同 → 全部列入 finding
- **集成**：drift findings → meta.add_quality_flags() → quality_flags 表存在对应行

## acceptance（GWT）

- [ ] Given 旧分区 close=100 When upsert 同 nk close=101 Then UpsertStats.drifted=1 + finding 含旧/新 digest
- [ ] Given NUMBER 类型同 (nk, revision_time) When upsert Then 不触发 Q-DRIFT-001（多版本并存）
- [ ] Given drift findings When QualityStage.run Then findings 写入 quality_flags 表

## 验收清单（验收时填写）

DoD 16 项 + GWT 逐条勾选。

测试输出摘要：`pytest tests/integration/test_canonical_drift.py` — 27/27 passed; `pytest tests/integration/test_canonical_upsert.py` — 21/21 passed; 全部集成测试 145/145 passed; 全部单元测试 316/316 passed; ruff 检查全部通过
接管性抽查：仅凭任务单 + 设计文档 + 代码变更可理解实现
git commit：待 Orchestrator 提交

GWT:
- [x] Given 旧分区 close=100 When upsert 同 nk close=101 Then UpsertStats.drifted=1 + finding 含旧/新 digest
- [x] Given NUMBER 类型同 (nk, revision_time) When upsert Then 不触发 Q-DRIFT-001（多版本并存）
- [x] Given drift findings When QualityStage.run Then findings 写入 quality_flags 表

TC-S-006 联动: [x] close=100→101 记录被更新 + Q-DRIFT-001 finding 含旧/新 digest
TC-Q-009: [x] Q-DRIFT-001 触发条件——同 nk 值变化 → finding 含旧/新 digest；值不变 → 无 finding
revision 类排除: [x] NUMBER 同 (nk, revision_time) 多版本 → 不触发 Q-DRIFT-001
边界: [x] 全部列相同（仅 nk 匹配）→ drifted=0；全部列不同 → 全部列入 finding
集成: [x] drift findings → meta.add_quality_flags() → quality_flags 表存在对应行

## 执行记录

| 时间 | 事件 |
|---|---|
| 2026-09-13 | 实现 value_digest、DriftFinding、detect_drift 函数 |
| 2026-09-13 | 集成 drift 检测到 CanonicalStore._merge_rewrite |
| 2026-09-13 | UpsertStats 增加 drifted: int 字段 |
| 2026-09-13 | 创建 quality/report.py（QualityFinding、QualityReport、GapContext） |
| 2026-09-13 | 创建 quality/rules.py（QualityRule ABC、注册表、Q-DRIFT-001 规则） |
| 2026-09-13 | 创建 tests/integration/test_canonical_drift.py（27 条测试） |
| 2026-09-13 | 全部测试通过，lint 检查通过 |

## Deferred Acceptance

无。本任务与 STORAGE-003.1 共同闭环，本任务覆盖 drift 检测路径，核心 upsert 由子任务 1 负责。
