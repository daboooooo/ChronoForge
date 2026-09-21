"""QualityFinding / QualityReport / GapContext（D06 §1, VALIDATION-001.1）。

职责：
- QualityFinding：单条质量发现
- QualityReport：规则检查结果报告
- GapContext：Gap 算法上下文
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Literal


@dataclass(frozen=True)
class QualityFinding:
    """单条质量发现。

    Args:
        record_key: natural key 字符串表示。
        rule_id: Q-xx-nnn。
        severity: ERROR / WARNING / INFO。
        detail: JSON 字符串（含 detail 信息，结构由具体规则定义）。
        raw_ref: jsonl 路径+行号（可选，由调用方注入）。
        payload_digest: sha256(原始 payload)（可选）。
    """

    record_key: str
    rule_id: str
    severity: Literal["ERROR", "WARNING", "INFO"]
    detail: str
    raw_ref: str | None = None
    payload_digest: str | None = None


@dataclass
class QualityReport:
    """规则检查结果报告。

    Args:
        findings: 所有发现列表。
        rule_id: 触发阻断的规则 ID（如果 block_on 命中）。
    """

    findings: list[QualityFinding]
    rule_id: str | None = None

    @property
    def error_count(self) -> int:
        """ERROR 级发现数。"""
        return sum(1 for f in self.findings if f.severity == "ERROR")

    @property
    def warning_count(self) -> int:
        """WARNING 级发现数。"""
        return sum(1 for f in self.findings if f.severity == "WARNING")

    @property
    def info_count(self) -> int:
        """INFO 级发现数。"""
        return sum(1 for f in self.findings if f.severity == "INFO")

    @property
    def block_on_triggered(self) -> bool:
        """是否命中 block_on 策略。"""
        return self.rule_id is not None

    def to_json(self) -> str:
        """序列化为 JSON（D09 TC-Q-006：quality --json 报告 schema）。

        schema（稳定契约，CLI --json 输出）：
        {"findings": [{record_key, rule_id, severity, detail, raw_ref,
                       payload_digest}, ...],
         "rule_id": str | null,
         "error_count": int, "warning_count": int, "info_count": int}
        """
        return json.dumps(
            {
                "findings": [asdict(f) for f in self.findings],
                "rule_id": self.rule_id,
                "error_count": self.error_count,
                "warning_count": self.warning_count,
                "info_count": self.info_count,
            },
            ensure_ascii=False,
        )


@dataclass
class GapContext:
    """Gap 算法上下文（D06 §2, Q-GAP-001/002）。

    Args:
        dataset_id: 数据集 ID。
        continuity_model: ALWAYS_OPEN / TRADING_CALENDAR / EVENT_BASED / RELEASE_SCHEDULE。
        frequency: 如 "1m", "1d"（可选，Q-GAP-002 用）。
    """

    dataset_id: str
    continuity_model: str
    frequency: str | None = None
