# MODEL-001 — BaseRecord 基座

## 派发信息

- 任务单：D10 §1 MODEL-001（冻结契约）
- 设计依据：D02 §1（BaseRecord/CanonicalType）、D01 §4（全局约定）、D09 §3 TC-M 组
- Agent：Agent-A
- 派发时间：2026-09-11
- 状态：DONE（2026-09-11 验收通过）

## file_ownership

- `src/chronoforge/models/base.py`
- `src/chronoforge/models/enums.py`
- `tests/unit/test_base.py`

## 交付物契约摘要（详见 D02 §1，以设计文档为准）

- `BaseRecord(BaseModel)`：`extra="forbid"`、`validate_assignment=True`；schema_version + provenance 五字段（source/source_id/source_timestamp/ingest_timestamp/raw_record_id）+ quality 两字段（quality_status 默认 VALID、quality_reason 默认 None）
- `CanonicalType(str, Enum)`：27 值（TICKER…FEATURE，见 D02 §1 逐行列出）
- `QualityStatus(str, Enum)`：VALID/SUSPECT/INVALID
- datetime 一律 UTC naive（D01 §4）；类型特定枚举（Interval/Side/OptionType…）不在本任务（属 MODEL-002 各类型模块）
- 模块零依赖：仅 pydantic + stdlib

## 执行记录

| 时间 | 事件 |
|---|---|
| 2026-09-11 | 派发 Agent-A |
| 2026-09-11 | Agent-A 交付（base.py/enums.py/test_base.py 共 12 用例） |
| 2026-09-11 | Orchestrator 验收：复跑全绿 + 契约逐字比对（27 枚举/8 字段/config 三项）通过 |

How 层决策（Agent-A，验收认可）：
- raw_record_id 以 `Field(min_length=1)` 落实非空；source_timestamp 必填可空（显式传 None）
- ruff UP042（建议 StrEnum）与 D02 冻结代码块 `(str, Enum)` 冲突 → 契约优先，`noqa: UP042` 保留（StrEnum 功能等价，如切换属受控变更，不立项）

## Deferred Acceptance

| ID | 验收项 | 依赖 | 关闭条件 |
|---|---|---|---|
| DEF-003 | TC-M-001/002 涉及 OHLCV 实例的断言（本任务以 BaseRecord 最小子类替代） | MODEL-002 | MODEL-002 DONE 后其 test_types.py 覆盖，本文件勾销 |

## 验收清单（2026-09-11 验收通过）

DoD 16 项：

- [x] 实现 / [x] 公共 API / [x] 数据契约 / [x] 错误处理（ValidationError 语义，不吞不转）
- [x] 日志（N/A 纯模型）/ [x] 指标（N/A）/ [x] 单测（12 用例）/ [x] 边界（extra=forbid/全枚举遍历）
- [x] 失败（非法枚举/缺字段/赋值校验）/ [ ] 恢复（N/A 无持久化）/ [ ] 集成（N/A，纯模型）
- [x] 静态分析（ruff 通过）/ [x] 类型检查（mypy strict 通过）/ [x] 无未声明假设 / [x] 验收通过

acceptance（D10 GWT）：

- [x] Given 合法样例 When 子类实例化 Then provenance 字段全部必填通过
- [x] Given 含未知字段的记录 Then ValidationError（extra=forbid）
- [x] Given NaN 数值 Then 拒绝——以测试预演，正式落点 MODEL-002（见 DEF-003）

测试输出摘要：pytest 12 passed（全库 24 passed）；ruff All checks passed；mypy no issues（14 files）；lint-imports 2 kept 0 broken

接管性抽查：✅ 通过——base.py/enums.py 模块 docstring 标注设计依据（D02 §1），字段注释含语义来源，仅凭任务单 + D02 可理解并扩展

git commit：b23820b（feat(model)）
