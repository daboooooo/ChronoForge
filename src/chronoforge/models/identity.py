"""身份键与时间字段映射（D02 §2 / D03 §3，审计 SR-05 下沉）。

职责（自 storage/base.py 原样下沉，冻结契约不变）：
- _NATURAL_KEY_MAP / natural_key()：按 CanonicalType 返回身份键列元组
- REVISION_TYPES：revision 类类型集合（natural_key 含 revision_time）
- _TIME_FIELD_MAP / partition_time_field()：CanonicalType → 分区时间字段

下沉原因：quality 层（rules.py）与 storage 层为同层兄弟（.importlinter
layers 契约互禁 import），而身份键/时间字段本质是 D02 §2/§4 的模型层
冻结映射，属共享数据结构；置于最底层 models 后两层均可合法依赖。

storage/base.py 对以上名称做 re-export，既有导入路径不受影响；
一致性仍由 tests/architecture/test_nk_mapping_consistency.py 守卫。
"""

from __future__ import annotations

from chronoforge.models.enums import CanonicalType

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


__all__ = [
    "REVISION_TYPES",
    "_NATURAL_KEY_MAP",
    "_TIME_FIELD_MAP",
    "natural_key",
    "partition_time_field",
]
