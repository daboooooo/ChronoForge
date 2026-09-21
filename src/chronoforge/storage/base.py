"""Canonical 存储层基元（D03 §3，STORAGE-003.1）。

职责：
- CanonicalStore Protocol：upsert 接口契约
- UpsertStats：upsert 结果统计
- natural_key()：按 CanonicalType 返回身份键列元组
- REVISION_TYPES：revision 类类型集合（natural_key 含 revision_time）
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Protocol

from chronoforge.models.enums import CanonicalType

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


# ── natural_key 映射 ──────────────────────────────────────────────────

# D02 §2 定义的身份键（natural key），按 CanonicalType 分组。
# D02 §2 显式声明身份的类型以其声明为唯一基准（OHLCV/LIQUIDATION_EVENT/NUMBER/FLOW）；
# 其余类型以模型层 natural_key() 实现为冻结基准（审计 2026-09-19 C-1）。
# POSITION/POSITION_AGGREGATE 依 D03 §3 含 revision_time（天然多版本）。
# 一致性由 tests/architecture/test_nk_mapping_consistency.py 架构测试守卫。

_NATURAL_KEY_MAP: dict[CanonicalType, tuple[str, ...]] = {
    # market.py
    CanonicalType.OHLCV: ("market_id", "event_time", "interval"),
    CanonicalType.TRADE: ("market_id", "trade_id"),
    CanonicalType.TICKER: ("market_id", "event_time"),
    CanonicalType.FUNDING: ("market_id", "event_time"),
    CanonicalType.OPEN_INTEREST: ("market_id", "event_time"),
    CanonicalType.ORDERBOOK: ("market_id", "event_time", "transaction_time"),
    # derivatives.py
    CanonicalType.OPTION: ("instrument_id", "event_time"),
    CanonicalType.IMPLIED_VOLATILITY: ("instrument_id", "event_time"),
    CanonicalType.GREEKS: ("instrument_id", "event_time"),
    CanonicalType.LIQUIDATION_EVENT: ("market_id", "order_id"),
    CanonicalType.LIQUIDATION_AGGREGATE: ("market_id", "event_time", "interval"),
    # macro.py
    CanonicalType.NUMBER: ("source_id", "observation_time", "revision_time"),
    CanonicalType.FLOW: ("source_id", "observation_time", "revision_time"),
    CanonicalType.MACRO_EVENT: ("event_ref", "scheduled_time"),
    # fundamental.py
    CanonicalType.FUNDAMENTAL: ("entity_id", "concept", "observation_time"),
    CanonicalType.FILING: ("cik", "accession_number"),
    CanonicalType.DOCUMENT: ("url", "publication_time"),
    # positioning.py（D03 §3：natural_key 含 revision_time，天然多版本）
    CanonicalType.POSITION: (
        "contract",
        "report_date",
        "participant_type",
        "revision_time",
    ),
    CanonicalType.POSITION_AGGREGATE: (
        "contract",
        "report_date",
        "participant_type",
        "revision_time",
    ),
    # prediction.py
    CanonicalType.PREDICTION_MARKET: ("source_id", "event_id"),
    CanonicalType.PREDICTION_PRICE: ("market_id", "outcome_id", "event_time"),
    # text.py
    CanonicalType.TEXT_MESSAGE: ("source_id", "event_time", "author"),
    CanonicalType.TEXT_EVENT: ("event_time", "gkg_themes"),
    # reference.py / derived.py
    CanonicalType.ENTITY: ("entity_id",),
    CanonicalType.INSTRUMENT: ("instrument_id",),
    CanonicalType.DERIVED: ("name", "computed_at"),
    CanonicalType.FEATURE: ("name", "computed_at"),
}


def natural_key(canonical_type: CanonicalType) -> tuple[str, ...]:
    """获取 canonical type 的身份键列名元组。

    Args:
        canonical_type: canonical type。

    Returns:
        身份键列名元组。

    Raises:
        ValueError: 未知 canonical type。
    """
    cols = _NATURAL_KEY_MAP.get(canonical_type)
    if cols is None:
        raise ValueError(f"Unknown canonical type for natural_key: {canonical_type}")
    return cols


# ── Revision 类类型 ──────────────────────────────────────────────────

# D03 §3：NUMBER/POSITION 的 natural_key 含 revision_time，天然多版本，
# upsert 时不走"keep last"合并路径（同一 (nk, revision_time) 重复才合并）。
REVISION_TYPES: frozenset[CanonicalType] = frozenset({
    CanonicalType.NUMBER,
    CanonicalType.FLOW,
    CanonicalType.POSITION,
    CanonicalType.POSITION_AGGREGATE,
})


# ── 时间字段解析 ──────────────────────────────────────────────────────

# D02 §4 时间字段适用矩阵：从 CanonicalType 到记录中时间字段的映射。
# 用于提取 year/month 分区。
_TIME_FIELD_MAP: dict[CanonicalType, str] = {
    # 有 event_time 的
    CanonicalType.OHLCV: "event_time",
    CanonicalType.TRADE: "event_time",
    CanonicalType.TICKER: "event_time",
    CanonicalType.FUNDING: "event_time",
    CanonicalType.OPEN_INTEREST: "event_time",
    CanonicalType.ORDERBOOK: "event_time",
    CanonicalType.OPTION: "event_time",
    CanonicalType.IMPLIED_VOLATILITY: "event_time",
    CanonicalType.GREEKS: "event_time",
    CanonicalType.LIQUIDATION_EVENT: "event_time",
    CanonicalType.LIQUIDATION_AGGREGATE: "event_time",
    CanonicalType.MACRO_EVENT: "event_time",
    CanonicalType.PREDICTION_PRICE: "event_time",
    CanonicalType.TEXT_MESSAGE: "event_time",
    CanonicalType.TEXT_EVENT: "event_time",
    # computed_at
    CanonicalType.DERIVED: "computed_at",
    CanonicalType.FEATURE: "computed_at",
    # observation_time
    CanonicalType.NUMBER: "observation_time",
    CanonicalType.FLOW: "observation_time",
    CanonicalType.FUNDAMENTAL: "observation_time",
    # report_date
    CanonicalType.POSITION: "report_date",
    CanonicalType.POSITION_AGGREGATE: "report_date",
    # filing_date
    CanonicalType.FILING: "filing_date",
    # publication_time
    CanonicalType.DOCUMENT: "publication_time",
    # 其他
    CanonicalType.PREDICTION_MARKET: "close_time",
    # 审计 M-5：ENTITY/INSTRUMENT 无可用时间字段（原映射的 canonical_name/
    # instrument_id 是标识符非时间，注定解析失败），参考数据按
    # ingest_timestamp 分区（canonical 层 fallback 行为，不告警）
}


def partition_time_field(canonical_type: CanonicalType) -> str | None:
    """获取 canonical type 对应的时间字段名（用于分区提取）。

    Args:
        canonical_type: canonical type。

    Returns:
        时间字段名，无法确定时返回 None。
    """
    return _TIME_FIELD_MAP.get(canonical_type)
