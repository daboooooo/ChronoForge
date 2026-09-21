# VALIDATION-001.1 — 规则引擎框架（协议/注册表/run/report）

## 派发信息

- 任务单：D10 §5 VALIDATION-001 拆分（子任务 1/2）
- 设计依据：D06 §1（规则引擎协议）、D06 §3（处置策略/Settings 配置）
- Agent：待定
- 派发时间：2026-09-12
- 状态：READY
- 依赖：MODEL-002（BaseRecord、CanonicalType）、STORAGE-001（MetaStore.add_quality_flags 协议）

## file_ownership

- `src/chronoforge/quality/rules.py`（新建，QualityRule 协议 + 规则注册表 + run 编排）
- `src/chronoforge/quality/report.py`（新建，QualityReport + QualityFinding）
- `src/chronoforge/quality/__init__.py`（新建，__all__ 导出）
- `tests/unit/test_rules_engine.py`（新建）

## 交付物契约摘要

### QualityFinding 数据类（quality/report.py）

```python
@dataclass(frozen=True)
class QualityFinding:
    record_key: str           # natural key 字符串表示
    rule_id: str              # Q-xx-nnn
    severity: Literal["ERROR", "WARNING", "INFO"]
    detail: str               # JSON 字符串（含 detail 信息）
    raw_ref: str | None = None    # jsonl 路径+行号（可选，由调用方注入）
    payload_digest: str | None = None  # sha256(原始 payload，可选)
```

- `detail` 为 JSON 字符串，结构由具体规则定义（如 Q-GAP-001 含缺失区间列表）
- `raw_ref` 和 `payload_digest` 由 QualityStage 在写入 quality_flags 前补充

### QualityReport 数据类（quality/report.py）

```python
@dataclass
class QualityReport:
    findings: list[QualityFinding]
    rule_id: str  # 触发阻断的规则 ID（如果 block_on 命中）

    @property
    def error_count(self) -> int: ...
    @property
    def warning_count(self) -> int: ...
    @property
    def info_count(self) -> int: ...

    @property
    def block_on_triggered(self) -> bool: ...
    """是否命中 block_on 策略"""
```

### QualityRule Protocol（quality/rules.py）

```python
from abc import ABC, abstractmethod

class QualityRule(ABC):
    @property
    @abstractmethod
    def rule_id(self) -> str: ...         # Q-xx-nnn

    @property
    @abstractmethod
    def applies_to(self) -> frozenset[CanonicalType] | Literal["ALL"]: ...

    @property
    @abstractmethod
    def severity(self) -> Literal["ERROR", "WARNING", "INFO"]: ...

    @abstractmethod
    def check(self, records: Sequence[BaseRecord], *, context: dict | None = None) -> list[QualityFinding]: ...
```

### 规则注册表（quality/rules.py）

```python
# 全局注册表
_rule_registry: dict[str, QualityRule] = {}

def register_rule(rule: QualityRule) -> None:
    """注册质量规则（单例，模块加载时调用）"""
    if rule.rule_id in _rule_registry:
        raise ValueError(f"Duplicate rule_id: {rule.rule_id}")
    _rule_registry[rule.rule_id] = rule

def get_all_rules() -> list[QualityRule]:
    """返回全部注册规则（testing 用）"""
    return list(_rule_registry.values())

def get_rule(rule_id: str) -> QualityRule | None:
    """按 rule_id 获取规则"""
    return _rule_registry.get(rule_id)

def validate_registry() -> None:
    """架构测试用：校验 rule_id 无重复、severity 合法"""
    ...
```

- **rule_id 全局唯一**（architecture test 断言）
- 注册时机：模块加载时（各规则类定义后通过装饰器或手动调用注册）
- VALIDATION-001.2 负责注册全部 17 条规则

### run 函数（quality/rules.py）

```python
def run(
    records: Sequence[BaseRecord],
    canonical_type: CanonicalType,
    *,
    context: GapContext | None = None,
    block_on: list[str] | None = None,
) -> QualityReport:
    """
    执行质量规则检查（D06 §1）。

    Args:
        records: 待检查记录
        canonical_type: 记录类型
        context: 上下文（GapContext 含 dataset_id/continuity_model 等）
        block_on: 阻断规则列表（来自 Settings.quality_block_on）
    Returns:
        QualityReport（含 findings + block_on_triggered）
    Raises:
        QualityError: block_on 命中
    """
    # 1. 筛选适用于 canonical_type 的规则
    applicable_rules = [
        rule for rule in get_all_rules()
        if rule.applies_to == "ALL" or canonical_type in rule.applies_to
    ]

    # 2. 逐个执行规则
    all_findings: list[QualityFinding] = []
    for rule in applicable_rules:
        try:
            findings = rule.check(records, context=context)
            all_findings.extend(findings)
        except Exception as exc:
            # 规则内部异常 → ERROR finding（不中断）
            all_findings.append(QualityFinding(
                record_key=f"RULE_ERROR:{rule.rule_id}",
                rule_id=rule.rule_id,
                severity="ERROR",
                detail=json.dumps({"exception": str(exc)}),
            ))

    # 3. 构建 report
    report = QualityReport(findings=all_findings, rule_id=None)

    # 4. 检查 block_on
    if block_on:
        for finding in all_findings:
            if finding.rule_id in block_on and finding.severity == "ERROR":
                report.rule_id = finding.rule_id
                raise QualityError(
                    f"Block on rule {finding.rule_id}",
                    context={"rule_id": finding.rule_id, "finding_count": len(all_findings)}
                )

    return report
```

### GapContext 数据类（quality/report.py）

```python
@dataclass
class GapContext:
    dataset_id: str
    continuity_model: str  # ALWAYS_OPEN / TRADING_CALENDAR / EVENT_BASED / RELEASE_SCHEDULE
    frequency: str | None = None  # 如 "1m", "1d"（可选，Q-GAP-002 用）
```

### 处置策略（Settings 配置，D06 §3）

- `quality_block_on`：默认 `["Q-SCHEMA-001", "Q-PROV-001"]`（ERROR 级默认阻断）
- `normalize_error_threshold`：默认 `0.10`（NormalizeStage 失败率上限）
- 配置由 Settings 加载，传递给 `run()` 的 `block_on` 参数

## 测试要求（D09 TC-Q 组）

- **TC-Q-005**：每规则触发/不触发成对（17 规则 × 2，由 VALIDATION-001.2 实现规则时补充）
- **TC-Q-006**：quality --json 报告 schema（QualityReport 序列化）
- **unit**：run([]) 空记录集 → 无 findings
- **unit**：rule_id 重复注册 → ValueError
- **unit**：规则内部抛异常 → ERROR finding（不中断其余规则）
- **unit**：block_on 命中 → QualityError 抛出
- **unit**：block_on 未命中 → QualityReport 正常返回
- **property**：规则纯函数性（同输入同输出，无状态依赖）

## acceptance（GWT）

- [ ] Given 空记录 When run Then QualityReport.findings 为空列表
- [ ] Given block_on=["Q-SCHEMA-001"] When Q-SCHEMA-001 命中 Then 抛 QualityError
- [ ] Given block_on=[] When 任意规则命中 Then 正常返回 QualityReport

## 验收清单（验收时填写）

DoD 16 项 + GWT 逐条勾选。

测试输出摘要：（待填）｜接管性抽查：（待填）｜git commit：（待填）

## 执行记录

| 时间 | 事件 |
|---|---|
| （执行时填写） | |

## Deferred Acceptance

无。本子任务与 VALIDATION-001.2 共同闭环，本任务覆盖引擎框架（协议/注册表/run/report），规则实现由子任务 2 负责。
