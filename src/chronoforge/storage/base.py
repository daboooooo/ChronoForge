"""Canonical 存储层基元（D03 §3，STORAGE-003.1）。

职责：
- CanonicalStore Protocol：upsert 接口契约
- UpsertStats：upsert 结果统计
- natural_key() / REVISION_TYPES / partition_time_field()：自
  models.identity re-export（审计 SR-05：映射冻结契约属模型层，
  quality 与 storage 同层互禁 import，故真身下沉 models.identity）
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Protocol

from chronoforge.models.enums import CanonicalType

# 审计 SR-05：真身位于 models/identity.py，此处 re-export 保持既有
# 导入路径（storage.canonical / pipeline.runner / tests 等）不变。
from chronoforge.models.identity import (
    _NATURAL_KEY_MAP,  # noqa: F401  # re-export（canonical.py 内部引用）
    _TIME_FIELD_MAP,  # noqa: F401  # re-export
)
from chronoforge.models.identity import (
    REVISION_TYPES as REVISION_TYPES,
)
from chronoforge.models.identity import (
    natural_key as natural_key,
)
from chronoforge.models.identity import (
    partition_time_field as partition_time_field,
)

# ── DriftFinding（D03 §3, D06 §1）──────────────────────────────────────


@dataclass(frozen=True)
class DriftFinding:
    """单条 drift finding，对应 QualityFinding 结构（D06 §1）。

    由 upsert 路径生成并经 UpsertStats.drift_findings 返回给调用方，
    调用方（Runner）负责经 meta.add_quality_flags 落盘（审计 H-3）。
    """

    record_key: str
    rule_id: str = "Q-DRIFT-001"
    severity: str = "WARNING"
    # detail 为 dict：dataset_id, old_digest, new_digest, changed_columns
    detail: dict[str, object] = field(default_factory=dict)


# ── UpsertStats ────────────────────────────────────────────────────────


@dataclass(frozen=True)
class UpsertStats:
    """upsert 操作的结果统计。"""

    inserted: int           # 新增记录数
    updated: int            # 更新记录数
    rewritten_partitions: int  # 被重写分区数
    total_records: int      # 本次处理总记录数
    drifted: int = 0        # Q-DRIFT-001 检测到的值漂移记录数（审计 F-04）
    # 审计 H-3：drift findings 随 stats 返回（不再只计数丢弃），
    # 恒有 drifted == len(drift_findings)
    drift_findings: tuple[DriftFinding, ...] = ()


# ── CanonicalStore Protocol ───────────────────────────────────────────


class CanonicalStore(Protocol):
    """Canonical 层存储接口（D03 §3）。"""

    def upsert(
        self,
        records: Sequence[dict[str, object]],
        canonical_type: CanonicalType,
        entity_id: str,
    ) -> UpsertStats:
        """将 records upsert 到 canonical 层。

        Args:
            records: 待写入的记录，每个记录为 dict，必须包含 base record 字段
                     (schema_version, source, source_id, ingest_timestamp, raw_record_id)
                     以及 canonical type 特定的业务字段。
            canonical_type: 记录的 canonical type。
            entity_id: 实体标识，路径中用于 entity= 分区。

        Returns:
            UpsertStats 统计信息。
        """

