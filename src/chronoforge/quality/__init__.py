"""质量规则引擎（D06：17 条规则，Q-xx-nnn）。

导出：
- QualityFinding / QualityReport / GapContext
- QualityRule, register_rule, get_all_rules, get_rule, validate_registry, run
- gap_detection 公共函数（Q-GAP-001/002 核心算法）
- ContinuityModel 枚举（VALIDATION-002）
- 17 条规则类（Q-DRIFT-001, Q-SCHEMA-001, Q-PROV-001, Q-NULL-001, Q-TS-001/002/003,
  Q-DUP-001, Q-SEQ-001, Q-RANGE-001/002/003, Q-GAP-001/002, Q-OHLC-001,
  Q-CROSS-001, Q-REV-001）
"""

from .continuity import ContinuityModel
from .report import GapContext, QualityFinding, QualityReport
from .rules import (
    QCross001Rule,
    QDRIFT001Rule,
    QDup001Rule,
    QGap001Rule,
    QGap002Rule,
    QNull001Rule,
    QOHLC001Rule,
    QProv001Rule,
    QRange001Rule,
    QRange002Rule,
    QRange003Rule,
    QRev001Rule,
    QSchema001Rule,
    QSeq001Rule,
    QTS001Rule,
    QTS002Rule,
    QTS003Rule,
    QualityRule,
    gap_detection,
    get_all_rules,
    get_rule,
    register_rule,
    run,
    validate_registry,
)

__all__ = [
    "QCross001Rule",
    "QDRIFT001Rule",
    "QDup001Rule",
    "QGap001Rule",
    "QGap002Rule",
    "QNull001Rule",
    "QOHLC001Rule",
    "QProv001Rule",
    "QRange001Rule",
    "QRange002Rule",
    "QRange003Rule",
    "QRev001Rule",
    "QSchema001Rule",
    "QSeq001Rule",
    "QTS001Rule",
    "QTS002Rule",
    "QTS003Rule",
    "ContinuityModel",
    "GapContext",
    "QualityFinding",
    "QualityReport",
    "QualityRule",
    "gap_detection",
    "get_all_rules",
    "get_rule",
    "register_rule",
    "run",
    "validate_registry",
]
