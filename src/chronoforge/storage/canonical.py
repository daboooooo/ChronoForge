"""CanonicalStore — Canonical 层 Parquet 存储（D03 §3，STORAGE-003.1）。

职责：
- merge-rewrite upsert：按 (year, month) 分区读取旧文件 + 新记录，
  按 natural_key 去重合并后原子写回分区目录
- 值漂移检测（D03 §3, 审计 F-04）：merge-rewrite 前逐列比对，生成 Q-DRIFT-001 findings，
  经 UpsertStats.drift_findings 返回调用方落盘并结构化日志上报（审计 H-3）
- 路径布局：{data_dir}/canonical/{type}/entity={id}/year={YYYY}/month={MM}/
- 原子写：temp 目录 → fsync → rename-swap（.old-* 备份交换，D03 §3）
"""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import TypedDict

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]
import structlog

from chronoforge.exceptions import StorageError
from chronoforge.models.enums import CanonicalType
from chronoforge.storage.base import (
    REVISION_TYPES,
    DriftFinding,
    UpsertStats,
    natural_key,
    partition_time_field,
)

logger = structlog.get_logger()


class _PartitionResult(TypedDict):
    """单分区 upsert 结果（审计 H-3：findings 随行返回）。"""

    inserted: int
    updated: int
    rewritten: int
    drifted: int
    findings: list[DriftFinding]


# ── 记录转换辅助 ───────────────────────────────────────────────────────


# ── 冻结契约 Arrow schema（审计 H-4）────────────────────────────────
#
# 按冻结契约（D02 §2 模型层字段类型）为每个 CanonicalType 显式声明
# Arrow schema，写盘类型不随批内值漂移（int64/float64/timestamp[us]）。
# 与模型层字段的一致性由 tests/architecture/test_nk_mapping_consistency.py
# 守卫；任何变更必须走冻结契约变更流程（A-2）。

_TS = pa.timestamp("us")
_DATE = pa.date32()
_F64 = pa.float64()
_S = pa.string()

# BaseRecord 公共字段（models/base.py，D02 §1）
_BASE_FIELDS: list[tuple[str, pa.DataType]] = [
    ("schema_version", _S),
    ("source", _S),
    ("source_id", _S),
    ("source_timestamp", _TS),
    ("ingest_timestamp", _TS),
    ("raw_record_id", _S),
    ("quality_status", _S),
    ("quality_reason", _S),
]

# 各 CanonicalType 业务字段（models/market|derivatives|macro|fundamental|
# positioning|prediction|text|reference|derived.py）
_TYPE_FIELDS: dict[CanonicalType, list[tuple[str, pa.DataType]]] = {
    # market.py
    CanonicalType.OHLCV: [
        ("market_id", _S), ("event_time", _TS), ("interval", _S),
        ("open", _F64), ("high", _F64), ("low", _F64),
        ("close", _F64), ("volume", _F64),
    ],
    CanonicalType.TRADE: [
        ("market_id", _S), ("event_time", _TS), ("price", _F64),
        ("quantity", _F64), ("side", _S), ("trade_id", _S),
    ],
    CanonicalType.TICKER: [
        ("market_id", _S), ("event_time", _TS), ("last_price", _F64),
        ("bid", _F64), ("ask", _F64), ("volume_24h", _F64),
        ("quote_volume_24h", _F64),
    ],
    CanonicalType.FUNDING: [
        ("market_id", _S), ("event_time", _TS), ("funding_rate", _F64),
        ("next_funding_time", _TS),
    ],
    CanonicalType.OPEN_INTEREST: [
        ("market_id", _S), ("event_time", _TS), ("open_interest", _F64),
        ("unit", _S),
    ],
    CanonicalType.ORDERBOOK: [
        ("market_id", _S), ("event_time", _TS), ("transaction_time", _TS),
        ("bids", pa.list_(pa.list_(_F64))), ("asks", pa.list_(pa.list_(_F64))),
    ],
    # derivatives.py
    CanonicalType.OPTION: [
        ("market_id", _S), ("instrument_id", _S), ("event_time", _TS),
        ("underlying", _S), ("expiry", _DATE), ("strike", _F64),
        ("option_type", _S), ("settlement_asset", _S),
        ("mark_price", _F64), ("bid", _F64), ("ask", _F64),
    ],
    CanonicalType.IMPLIED_VOLATILITY: [
        ("instrument_id", _S), ("event_time", _TS), ("iv", _F64),
        ("mark_iv", _F64), ("bid_iv", _F64), ("ask_iv", _F64),
        ("implied_forward", _F64), ("data_tier", _S),
    ],
    CanonicalType.GREEKS: [
        ("instrument_id", _S), ("event_time", _TS), ("delta", _F64),
        ("gamma", _F64), ("vega", _F64), ("theta", _F64), ("rho", _F64),
    ],
    CanonicalType.LIQUIDATION_EVENT: [
        ("market_id", _S), ("event_time", _TS), ("side", _S),
        ("price", _F64), ("quantity", _F64), ("order_id", _S),
    ],
    CanonicalType.LIQUIDATION_AGGREGATE: [
        ("market_id", _S), ("event_time", _TS), ("interval", _S),
        ("buy_vol", _F64), ("sell_vol", _F64), ("total_notional", _F64),
    ],
    # macro.py
    CanonicalType.NUMBER: [
        ("source_id", _S), ("observation_time", _TS), ("release_time", _TS),
        ("revision_time", _TS), ("value", _F64), ("units", _S),
        ("seasonal_adjustment", _S), ("vintage_date", _S),
    ],
    CanonicalType.FLOW: [
        ("source_id", _S), ("observation_time", _TS), ("release_time", _TS),
        ("revision_time", _TS), ("value", _F64), ("units", _S),
        ("seasonal_adjustment", _S), ("vintage_date", _S),
        ("period_start", _TS), ("period_end", _TS),
    ],
    CanonicalType.MACRO_EVENT: [
        ("event_ref", _S), ("scheduled_time", _TS), ("actual_time", _TS),
        ("actual", _S), ("forecast", _S), ("previous", _S),
    ],
    # fundamental.py
    CanonicalType.FUNDAMENTAL: [
        ("entity_id", _S), ("cik", _S), ("concept", _S), ("taxon", _S),
        ("unit", _S), ("observation_time", _TS), ("value", _F64),
        ("fiscal_year", pa.int64()), ("fiscal_period", _S), ("frame", _S),
    ],
    CanonicalType.FILING: [
        ("entity_id", _S), ("cik", _S), ("accession_number", _S),
        ("form_type", _S), ("filing_date", _DATE),
        ("accepted_datetime", _TS), ("report_period_end", _DATE),
        ("primary_doc_url", _S),
    ],
    CanonicalType.DOCUMENT: [
        ("url", _S), ("publication_time", _TS), ("content_hash", _S),
        ("mime", _S),
    ],
    # positioning.py（D03 §3：含 revision_time，天然多版本）
    CanonicalType.POSITION: [
        ("report_date", _DATE), ("release_time", _TS), ("contract", _S),
        ("participant_type", _S), ("long_positions", _F64),
        ("short_positions", _F64), ("spreading", _F64),
        ("net_position", _F64), ("revision_time", _TS),
    ],
    CanonicalType.POSITION_AGGREGATE: [
        ("report_date", _DATE), ("release_time", _TS), ("contract", _S),
        ("participant_type", _S), ("long_positions", _F64),
        ("short_positions", _F64), ("spreading", _F64),
        ("net_position", _F64), ("revision_time", _TS),
    ],
    # prediction.py
    CanonicalType.PREDICTION_MARKET: [
        ("source_id", _S), ("event_id", _S), ("question", _S),
        ("outcomes", pa.list_(_S)), ("close_time", _TS), ("status", _S),
    ],
    CanonicalType.PREDICTION_PRICE: [
        ("market_id", _S), ("outcome_id", _S), ("event_time", _TS),
        ("price", _F64), ("implied_probability", _F64),
        ("volume", _F64), ("liquidity", _F64),
    ],
    # text.py
    CanonicalType.TEXT_MESSAGE: [
        ("source_id", _S), ("event_time", _TS), ("author", _S),
        ("text_hash", _S), ("language", _S),
    ],
    CanonicalType.TEXT_EVENT: [
        ("event_time", _TS), ("gkg_themes", pa.list_(_S)),
        ("entities", pa.list_(_S)),
    ],
    # reference.py / derived.py
    CanonicalType.ENTITY: [
        ("entity_id", _S), ("canonical_name", _S), ("entity_type", _S),
        ("aliases", pa.list_(_S)),
    ],
    CanonicalType.INSTRUMENT: [
        ("instrument_id", _S), ("entity_id", _S), ("instrument_type", _S),
        ("expiry", _DATE), ("strike", _F64), ("option_type", _S),
        ("settlement_asset", _S),
    ],
    # params: dict[str, Any] → JSON 文本（确定性序列化，Reproducibility §55）
    CanonicalType.DERIVED: [
        ("name", _S), ("computed_at", _TS),
        ("dependencies", pa.list_(pa.list_(_S))), ("value", _F64),
        ("params", _S), ("source_type", _S),
    ],
    CanonicalType.FEATURE: [
        ("name", _S), ("computed_at", _TS),
        ("dependencies", pa.list_(pa.list_(_S))), ("value", _F64),
        ("params", _S), ("feature_engine_version", _S),
    ],
}

# CanonicalType → 冻结 Arrow schema
# （业务字段与 BaseRecord 同名时以基础字段为准去重，如 NUMBER.source_id）
_SCHEMAS: dict[CanonicalType, pa.Schema] = {}
for _ct, _fields in _TYPE_FIELDS.items():
    _seen = {name for name, _ in _BASE_FIELDS}
    _merged = list(_BASE_FIELDS)
    for _name, _typ in _fields:
        if _name not in _seen:
            _merged.append((_name, _typ))
            _seen.add(_name)
    _SCHEMAS[_ct] = pa.schema(_merged)


def _typed_array(values: list[object], target_type: pa.DataType) -> pa.Array:
    """按声明目标类型构建 Array（枚举归一化、dict → JSON、去时区）。"""
    norm: list[object] = []
    for v in values:
        if isinstance(v, Enum):
            v = v.value
        elif isinstance(v, dict):
            v = json.dumps(v, sort_keys=True, default=str)
        elif isinstance(v, datetime) and v.tzinfo is not None:
            v = v.replace(tzinfo=None)
        norm.append(v)
    return pa.array(norm, type=target_type)


def _records_to_df(
    records: list[dict[str, object]], canonical_type: CanonicalType
) -> pa.Table:
    """将 dict 列表转为 PyArrow Table（审计 H-4：按冻结契约显式 schema）。

    列集合与类型完全由 _SCHEMAS[canonical_type] 声明，不随批内值推断；
    缺失字段补 null。声明外的额外字段（含 _revision_seq）按首次出现
    顺序追加在尾部（保真，不丢弃）。
    """
    if not records:
        return pa.table({})

    schema = _SCHEMAS[canonical_type]
    columns: dict[str, pa.Array] = {}
    for field in schema:
        values: list[object] = []
        for rec in records:
            v = rec.get(field.name)
            if v is not None and pa.types.is_timestamp(field.type) and isinstance(v, str):
                # 兼容 ISO 字符串时间（维持既有写入接受域）
                v = datetime.fromisoformat(v)
            values.append(v)
        columns[field.name] = _typed_array(values, field.type)
    table = pa.Table.from_pydict(columns, schema=schema)

    # 声明外的额外字段按首次出现顺序追加
    declared = set(schema.names)
    extras: list[str] = []
    for rec in records:
        for k in rec:
            if k not in declared and k not in extras:
                extras.append(k)
    for k in extras:
        vals = [rec.get(k) for rec in records]
        table = table.append_column(
            pa.field(k, _to_arrow_array(vals).type), _to_arrow_array(vals)
        )
    return table


def _cast_to_declared(table: pa.Table, schema: pa.Schema) -> pa.Table:
    """将旧分区表对齐到声明 schema（审计 H-4：legacy 兼容路径）。

    缺失列补 null；类型不一致时 cast，cast 失败熔断（StorageError），
    禁止静默降级。声明外既有列原样保留。
    """
    cols: dict[str, pa.Array | pa.ChunkedArray] = {}
    for field in schema:
        if field.name in table.column_names:
            col = table.column(field.name)
            if col.type != field.type:
                try:
                    col = col.cast(field.type)
                except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as e:
                    raise StorageError(
                        f"legacy column {field.name} cannot be cast to "
                        f"declared type {field.type}: {e}"
                    ) from e
            cols[field.name] = col
        else:
            cols[field.name] = pa.nulls(table.num_rows, type=field.type)
    aligned = pa.table(cols)

    declared = set(schema.names)
    for name in table.column_names:
        if name not in declared:
            aligned = aligned.append_column(
                table.schema.field(name), table.column(name)
            )
    return aligned


def _to_arrow_array(values: list[object]) -> pa.Array:
    """将 Python 值列表转为 PyArrow Array，尝试推断类型。"""
    if not values:
        return pa.array([])

    # 枚举值归一化为 .value（str(枚举成员) 为 "Class.MEMBER" 形态，
    # 会使 interval 等身份字段以错误值落盘，违反 D02 §2 值契约）
    values = [v.value if isinstance(v, Enum) else v for v in values]

    # 检查是否都是 datetime（含时区）
    non_none = [v for v in values if v is not None]
    if non_none and all(isinstance(v, datetime) for v in non_none):
        # datetime → timestamp[us]
        timestamps = [
            v.replace(tzinfo=None) if isinstance(v, datetime) else v
            for v in values
        ]
        return pa.array(timestamps, type=pa.timestamp("us"))

    # 检查是否是 ISO datetime 字符串
    if non_none and all(
        isinstance(v, str) and _is_iso_datetime(v) for v in non_none
    ):
        timestamps = []
        for v in values:
            if v is None:
                timestamps.append(None)
            else:
                try:
                    dt = datetime.fromisoformat(str(v))
                    timestamps.append(dt.replace(tzinfo=None) if dt.tzinfo else dt)
                except (ValueError, TypeError):
                    timestamps.append(None)
        return pa.array(timestamps, type=pa.timestamp("us"))

    # 检查是否都是 int
    if all(isinstance(v, (int, type(None))) for v in values):
        non_none = [v for v in values if v is not None]
        if all(isinstance(v, int) for v in non_none):
            return pa.array(values, type=pa.int64())

    # 检查是否都是 float
    if all(isinstance(v, (int, float, type(None))) for v in values):
        non_none = [v for v in values if v is not None]
        if all(isinstance(v, float) for v in non_none):
            return pa.array(values, type=pa.float64())

    # 默认字符串
    return pa.array([str(v) if v is not None else None for v in values])


def _is_iso_datetime(s: str) -> bool:
    """检查字符串是否为 ISO datetime 格式。"""
    try:
        datetime.fromisoformat(s)
        return True
    except (ValueError, TypeError):
        return False


# ── 值漂移检测（D03 §3, 审计 F-04）─────────────────────────────────


def value_digest(value: object) -> str:
    """序列化值并计算 sha256 hex digest。

    - datetime: ISO 字符串（UTC naive）
    - None: 序列化 ``"null"``
    - 数值: 保持精度（float → str 不截断）
    - 其他: 统一 str() 序列化
    """
    if value is None:
        serialized = "null"
    elif isinstance(value, datetime):
        dt = value
        if dt.tzinfo is not None:
            dt = dt.replace(tzinfo=None)
        serialized = dt.isoformat()
    else:
        serialized = json.dumps(value, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode()).hexdigest()


def detect_drift(
    old_table: pa.Table,
    new_table: pa.Table,
    nk_cols: tuple[str, ...],
    canonical_type: CanonicalType,
    entity_id: str,
) -> list[DriftFinding]:
    """检测值漂移：old_df ⋈ new_df（natural key 相等），逐列比对值列。

    返回 DriftFinding 列表（每条对应一个被更新的 natural key）。

    排除非业务值列（provenance 元数据如 raw_record_id、ingest_timestamp
    等天然会变化，不参与 drift 判断）。

    Args:
        old_table: 旧分区数据。
        new_table: 新记录数据（已含 _revision_seq）。
        nk_cols: natural key 列名元组。
        canonical_type: canonical type。
        entity_id: 实体 ID（用于 finding detail）。

    Returns:
        DriftFinding 列表（同 nk 值变化的记录）。
    """
    if old_table.num_rows == 0 or new_table.num_rows == 0:
        return []

    # 排除非业务值列（provenance 元数据）
    _EXCLUDED_VALUE_COLS = frozenset({
        "_revision_seq",
        "raw_record_id",
        "ingest_timestamp",
        "source_timestamp",
        "schema_version",
    })

    # 获取值列（排除 natural_key 列和元数据列）
    value_cols = [
        col for col in old_table.column_names
        if col not in nk_cols
        and col not in _EXCLUDED_VALUE_COLS
    ]
    if not value_cols:
        return []

    # 构建旧数据的 nk → row 映射（取最后一条，即已去重后的）
    old_nk_map: dict[tuple[object, ...], dict[str, object]] = {}
    for i in range(old_table.num_rows):
        nk_key = tuple(old_table[col][i].as_py() for col in nk_cols)
        row_vals = {col: old_table[col][i].as_py() for col in value_cols}
        old_nk_map[nk_key] = row_vals

    findings: list[DriftFinding] = []
    for i in range(new_table.num_rows):
        nk_key = tuple(new_table[col][i].as_py() for col in nk_cols)
        if nk_key not in old_nk_map:
            continue  # 新记录，无旧值比对

        old_vals = old_nk_map[nk_key]
        new_vals: dict[str, object] = {}
        for col in value_cols:
            if col in new_table.column_names:
                new_vals[col] = new_table[col][i].as_py()

        # 逐列比对
        changed_columns: list[str] = []
        for col in value_cols:
            old_v = old_vals.get(col)
            new_v = new_vals.get(col)
            if old_v != new_v:
                changed_columns.append(col)

        if not changed_columns:
            continue  # 值未变化

        # 生成 finding
        old_digest = value_digest(old_vals)
        new_digest = value_digest(new_vals)
        nk_str = ",".join(str(nk_key[i]) for i in range(len(nk_cols)))
        findings.append(DriftFinding(
            record_key=nk_str,
            detail={
                "dataset_id": entity_id,
                "old_digest": old_digest,
                "new_digest": new_digest,
                "changed_columns": changed_columns,
            },
        ))

    return findings


# ── CanonicalStore 实现 ──────────────────────────────────────────────


@dataclass
class CanonicalStoreImpl:
    """Canonical 层 Parquet 存储（D03 §3 merge-rewrite upsert）。

    路径布局：
        {data_dir}/canonical/{type}/entity={id}/year={YYYY}/month={MM}/part-{seq}.parquet
    """

    data_dir: str

    def __post_init__(self) -> None:
        """确保数据目录存在。"""
        # 审计 M-6：_revision_seq 实例级单调游标（保证批内唯一 + 全局单调）
        self._last_revision_seq = 0
        try:
            Path(self.data_dir).mkdir(parents=True, exist_ok=True)
        except OSError as e:
            raise StorageError(
                f"Failed to create data directory {self.data_dir}"
            ) from e

    # ── 原子写辅助 ────────────────────────────────────────────────────

    def _atomic_rename(self, tmp_dir: Path, target: Path) -> None:
        """原子写 rename-swap（D03 §3，审计 C-3）。

        1. target 存在时先 rename 为同级 .old-* 备份（原子操作）
        2. rename tmp_dir → target（原子操作）
        3. 成功后删除 .old-* 备份；第二步失败则回滚 .old-* → target

        任意崩溃窗口内 target 要么是完整旧数据（.old-*）要么是完整新数据，
        不存在"分区已删、新数据未就位"的中间态（No partial commit）。
        """
        old_dir: Path | None = None
        if target.exists():
            old_dir = target.parent / f".old-{uuid.uuid4().hex[:8]}"
            target.rename(old_dir)
        try:
            tmp_dir.rename(target)
        except Exception:
            # 第二步失败：回滚旧分区，保证 target 始终指向完整数据
            if old_dir is not None and not target.exists():
                old_dir.rename(target)
            raise
        if old_dir is not None:
            import shutil
            shutil.rmtree(old_dir, ignore_errors=True)
        # fsync 父目录，确保 rename 的目录条目持久化到磁盘
        try:
            fd = os.open(str(target.parent), os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        except OSError:
            pass  # 非必须，忽略失败

    # ── 路径构造 ────────────────────────────────────────────────────

    def _partition_path(
        self,
        canonical_type: CanonicalType,
        entity_id: str,
        year: str,
        month: str,
    ) -> Path:
        """生成分区目录路径。

        格式：{data_dir}/canonical/{type}/entity={id}/year={YYYY}/month={MM}/
        """
        return Path(self.data_dir) / "canonical" / canonical_type.value / (
            f"entity={entity_id}"
        ) / f"year={year}" / f"month={month}"

    def _partition_files(self, partition_path: Path) -> list[Path]:
        """获取分区目录中现有的 parquet 文件。"""
        if not partition_path.exists():
            return []
        all_files = [
            f for f in partition_path.glob("*.parquet")
            if not f.name.startswith(".")
        ]
        return sorted(all_files)

    # ── 时间提取 ────────────────────────────────────────────────────

    def _extract_partition_key(
        self, record: dict[str, object], canonical_type: CanonicalType
    ) -> tuple[str, str] | None:
        """从记录中提取 (year, month) 分区键。

        Returns:
            (year, month) 元组，或 None（无法提取时）。
        """
        time_field = partition_time_field(canonical_type)
        if not time_field:
            return None

        time_val = record.get(time_field)
        if time_val is None:
            return None

        # 尝试解析为 datetime（date32 字段如 report_date/filing_date 为 date 对象）
        if isinstance(time_val, datetime):
            dt = time_val
        elif isinstance(time_val, date):
            dt = datetime(time_val.year, time_val.month, time_val.day)
        elif isinstance(time_val, str):
            try:
                dt = datetime.fromisoformat(time_val)
            except (ValueError, TypeError):
                return None
        else:
            return None

        # 处理时区
        if dt.tzinfo is not None:
            dt = dt.replace(tzinfo=None)

        return (dt.strftime("%Y"), dt.strftime("%m"))

    def _fallback_partition_key(
        self, record: dict[str, object], canonical_type: CanonicalType
    ) -> tuple[str, str]:
        """审计 M-5：声明时间字段不可用时回退 ingest_timestamp 提取分区键。

        - 类型无声明时间字段（参考数据 ENTITY/INSTRUMENT 等）：按
          ingest_timestamp 分区，属设计行为，不告警。
        - 有声明时间字段但缺失/解析失败：回退 + warning（暴露数据质量问题）。
        - ingest_timestamp 也不可用：熔断 StorageError，不允许静默丢数据。
        """
        time_field = partition_time_field(canonical_type)
        if time_field:
            logger.warning(
                "storage.partition_time_fallback",
                canonical_type=canonical_type.value,
                time_field=time_field,
            )
        it = record.get("ingest_timestamp")
        dt: datetime | None = None
        if isinstance(it, datetime):
            dt = it.replace(tzinfo=None)
        elif isinstance(it, str):
            try:
                dt = datetime.fromisoformat(it).replace(tzinfo=None)
            except (ValueError, TypeError):
                dt = None
        if dt is None:
            raise StorageError(
                f"cannot determine partition for {canonical_type.value}: "
                f"time field {time_field!r} unusable and ingest_timestamp missing"
            )
        return (dt.strftime("%Y"), dt.strftime("%m"))

    # ── 核心 upsert ─────────────────────────────────────────────────

    def upsert(
        self,
        records: Sequence[dict[str, object]],
        canonical_type: CanonicalType,
        entity_id: str,
    ) -> UpsertStats:
        """merge-rewrite upsert 核心算法（D03 §3）。

        Args:
            records: 待写入记录。
            canonical_type: canonical type。
            entity_id: 实体 ID。

        Returns:
            UpsertStats 统计。
        """
        input_records = list(records)
        if not input_records:
            return UpsertStats(
                inserted=0, updated=0, rewritten_partitions=0, total_records=0
            )

        nk_cols = natural_key(canonical_type)
        is_revision = canonical_type in REVISION_TYPES

        # 步骤 1：按 (year, month) 分组
        # 审计 M-5：每条记录必须落入某个分区——声明时间字段缺失/解析失败时
        # 回退 ingest_timestamp（含告警），不可回退则熔断，禁止静默丢弃
        partition_groups: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
        for rec in input_records:
            key = self._extract_partition_key(rec, canonical_type)
            if key is None:
                key = self._fallback_partition_key(rec, canonical_type)
            partition_groups[key].append(rec)

        inserted = 0
        updated = 0
        rewritten_partitions = 0
        drifted = 0
        all_findings: list[DriftFinding] = []

        # 步骤 2：处理每个分区
        for (year, month), partition_records in partition_groups.items():
            part_path = self._partition_path(
                canonical_type, entity_id, year, month
            )

            stats = self._process_partition(
                part_path,
                partition_records,
                nk_cols,
                is_revision,
                canonical_type,
                entity_id,
            )
            inserted += stats["inserted"]
            updated += stats["updated"]
            rewritten_partitions += stats["rewritten"]
            drifted += stats["drifted"]
            all_findings.extend(stats["findings"])

        return UpsertStats(
            inserted=inserted,
            updated=updated,
            rewritten_partitions=rewritten_partitions,
            total_records=len(input_records),
            drifted=drifted,
            # 审计 H-3：findings 随 stats 返回，由调用方落盘（meta.add_quality_flags）
            drift_findings=tuple(all_findings),
        )

    # ── 分区处理 ────────────────────────────────────────────────────

    def _process_partition(
        self,
        part_path: Path,
        new_records: list[dict[str, object]],
        nk_cols: tuple[str, ...],
        is_revision: bool,
        canonical_type: CanonicalType,
        entity_id: str,
    ) -> _PartitionResult:
        """处理单个分区：merge-rewrite 或原子写。"""
        existing_files = self._partition_files(part_path)
        has_old_data = len(existing_files) > 0

        if not has_old_data:
            # 无旧数据：直接原子写（传 nk_cols 做正确去重）
            return self._write_new_partition(
                part_path, new_records, nk_cols, canonical_type
            )

        # merge-rewrite（含 drift 检测）
        return self._merge_rewrite(
            part_path, new_records, nk_cols, is_revision,
            canonical_type, entity_id,
        )

    @staticmethod
    def _empty_stats() -> _PartitionResult:
        """构造全零分区统计（findings 为空列表）。"""
        return {
            "inserted": 0,
            "updated": 0,
            "rewritten": 0,
            "drifted": 0,
            "findings": [],
        }

    def _write_new_partition(
        self,
        part_path: Path,
        records: list[dict[str, object]],
        nk_cols: tuple[str, ...] = (),
        canonical_type: CanonicalType = CanonicalType.OHLCV,
    ) -> _PartitionResult:
        """无旧数据时：原子写分区（temp → rename）。

        part_path 已经是 month 级目录，直接在该目录下写文件。
        """
        if not records:
            return self._empty_stats()

        # 添加 _revision_seq
        records_with_seq = self._add_revision_seq(records)

        # 转为 PyArrow Table（审计 H-4：按冻结契约显式 schema）
        table = _records_to_df(records_with_seq, canonical_type)
        if table.num_rows == 0:
            return self._empty_stats()

        # 按 natural_key 去重（保留最后一条，_revision_seq 仅用于排序）
        if nk_cols and "_revision_seq" in table.column_names:
            dedup_cols = list(nk_cols)
            table = self._dedup_by_columns(table, dedup_cols)

        # 写 parquet 文件（不含 _revision_seq）
        cols_to_keep = [c for c in table.column_names if c != "_revision_seq"]
        table = table.select(cols_to_keep)

        if table.num_rows == 0:
            return self._empty_stats()

        # 原子写：temp 目录（与 part_path 同级）→ rename
        tmp_dir = part_path.parent / f".tmp-{uuid.uuid4().hex[:8]}"
        try:
            tmp_dir.mkdir(parents=True, exist_ok=True)
            if not part_path.parent.exists():
                part_path.parent.mkdir(parents=True, exist_ok=True)

            # 写 parquet 文件到 temp 目录
            part_seq = self._next_part_seq(tmp_dir)
            output_path = tmp_dir / f"part-{part_seq:04d}.parquet"
            pq.write_table(table, str(output_path), compression="zstd", write_statistics=True)

            # fsync
            fd = os.open(str(output_path), os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)

            # 原子 rename（清理可能残留的目标目录）
            self._atomic_rename(tmp_dir, part_path)
        except Exception:
            # 清理孤儿 temp 目录
            if tmp_dir.exists():
                import shutil
                shutil.rmtree(tmp_dir, ignore_errors=True)
            raise

        return {
            "inserted": table.num_rows,
            "updated": 0,
            "rewritten": 1,
            "drifted": 0,
            "findings": [],
        }

    def _merge_rewrite(
        self,
        part_path: Path,
        new_records: list[dict[str, object]],
        nk_cols: tuple[str, ...],
        is_revision: bool,
        canonical_type: CanonicalType,
        entity_id: str,
    ) -> _PartitionResult:
        """merge-rewrite：读取旧文件 + 新记录，去重合并后替换分区。

        在合并前执行值漂移检测（D03 §3, 审计 F-04）。
        """
        # 读取旧 parquet 文件
        old_table = self._read_all_old(part_path)
        # 审计 H-4：旧分区块表（含 legacy 推断 schema）对齐到冻结声明 schema，
        # 保证与新表 concat/dedup 全程类型一致；cast 失败熔断
        if old_table.num_rows > 0:
            old_table = _cast_to_declared(old_table, _SCHEMAS[canonical_type])

        # 转换新记录为 DataFrame（按冻结契约显式 schema）
        records_with_seq = self._add_revision_seq(new_records)
        new_table = _records_to_df(records_with_seq, canonical_type)

        # 统计 inserted/updated
        inserted_count, updated_count = self._count_changes(
            old_table, new_table, nk_cols, is_revision
        )

        if old_table.num_rows == 0:
            # 旧分区无实际数据（文件为空或 schema 不兼容），等同于新写
            # 审计 M-7：批内重复 NK 只计一次（与 _count_changes 语义一致）
            unique_nk = {
                tuple(new_table[col][i].as_py() for col in nk_cols)
                for i in range(new_table.num_rows)
            }
            return {
                "inserted": len(unique_nk),
                "updated": 0,
                "rewritten": 1,
                "drifted": 0,
                "findings": [],
            }

        if new_table.num_rows == 0:
            # 无新数据，返回旧数据
            return self._empty_stats()

        # 值漂移检测（merge-rewrite 前，D03 §3, 审计 F-04）
        # 排除 revision 类（天然多版本）
        drift_findings: list[DriftFinding] = []
        if not is_revision:
            drift_findings = detect_drift(
                old_table, new_table, nk_cols, canonical_type, entity_id
            )
            # 审计 H-3：findings 结构化上报（落盘由调用方经
            # UpsertStats.drift_findings → meta.add_quality_flags 完成）
            for f in drift_findings:
                logger.warning(
                    "storage.drift_detected",
                    canonical_type=canonical_type.value,
                    entity_id=entity_id,
                    record_key=f.record_key,
                    changed_columns=f.detail.get("changed_columns", []),
                )

        # 合并去重
        merged = self._merge_dedup(
            old_table, new_table, nk_cols, is_revision
        )

        # 删除 _revision_seq 列
        if "_revision_seq" in merged.column_names:
            merged = merged.drop_columns(["_revision_seq"])

        # 原子写：temp 目录 → rename（part_path 已是 month 级目录）
        tmp_dir = part_path.parent / f".tmp-{uuid.uuid4().hex[:8]}"
        try:
            tmp_dir.mkdir(parents=True, exist_ok=True)
            if not part_path.parent.exists():
                part_path.parent.mkdir(parents=True, exist_ok=True)

            # 写 parquet 文件到 temp 目录
            part_seq = self._next_part_seq(tmp_dir)
            output_path = tmp_dir / f"part-{part_seq:04d}.parquet"
            pq.write_table(merged, str(output_path), compression="zstd", write_statistics=True)

            # fsync
            fd = os.open(str(output_path), os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)

            # 原子 rename（清理可能残留的目标目录）
            self._atomic_rename(tmp_dir, part_path)
        except Exception:
            # 清理
            if tmp_dir.exists():
                import shutil
                shutil.rmtree(tmp_dir, ignore_errors=True)
            raise

        return {
            "inserted": inserted_count,
            "updated": updated_count,
            "rewritten": 1,
            "drifted": len(drift_findings),
            "findings": drift_findings,
        }

    def _read_all_old(self, part_path: Path) -> pa.Table:
        """读取分区中所有旧 parquet 文件并合并。

        审计 C-2：任何文件读取失败必须熔断（raise StorageError），
        禁止静默跳过——否则 merge-rewrite 会以仅含新数据的表整体替换
        分区，导致静默数据丢失。
        """
        old_files = self._partition_files(part_path)
        if not old_files:
            return pa.table({})

        tables = []
        for f in old_files:
            try:
                table = pq.read_table(str(f))
            except Exception as e:
                raise StorageError(
                    f"Failed to read existing parquet file {f}: {e}"
                ) from e
            tables.append(table)

        return pa.concat_tables(tables)

    def _write_merged_to_dir(
        self, target_dir: Path, merged: pa.Table, use_subdirs: bool = True
    ) -> None:
        """将合并后的数据写入目标目录，保持分区目录结构。

        将数据按 year/month 分组写入 partition 目录结构。
        如果数据没有 year/month 列（新写场景），直接写文件到 target_dir。
        如果 use_subdirs=False，即使有 year/month 列也直接写文件。
        """
        if merged.num_rows == 0:
            return

        # 需要 year 和 month 列来构建分区目录
        has_year = "year" in merged.column_names
        has_month = "month" in merged.column_names

        if not has_year or not has_month or not use_subdirs:
            # 没有分区列或不需要子目录，直接写文件
            part_seq = self._next_part_seq(target_dir)
            output_path = target_dir / f"part-{part_seq:04d}.parquet"
            pq.write_table(
                merged, str(output_path), compression="zstd", write_statistics=True
            )
            return

        # 按 year/month 分组写入
        df = merged.to_pandas()
        grouped = df.groupby(["year", "month"], dropna=False)

        for (year_val, month_val), group in grouped:
            if year_val is None or month_val is None:
                continue
            year_str = str(year_val).zfill(4)
            month_str = str(month_val).zfill(2)

            partition_subdir = target_dir / f"year={year_str}" / f"month={month_str}"
            partition_subdir.mkdir(parents=True, exist_ok=True)

            part_seq = self._next_part_seq(partition_subdir)
            output_path = partition_subdir / f"part-{part_seq:04d}.parquet"
            sub_table = pa.Table.from_pandas(group, preserve_index=False)
            pq.write_table(
                sub_table, str(output_path), compression="zstd", write_statistics=True
            )

    def _add_revision_seq(self, records: list[dict[str, object]]) -> list[dict[str, object]]:
        """为记录附加 _revision_seq 字段（审计 M-6：批内唯一 + 全局单调）。

        以当前时间（微秒）为基准，但不小于上一批最大值 + 1；批内逐条递增，
        保证同批多条记录的 _revision_seq 不重复（keep-last 去重确定性）。
        """
        now_us = int(datetime.now().replace(tzinfo=None).timestamp() * 1_000_000)
        base = max(now_us, self._last_revision_seq + 1)
        self._last_revision_seq = base + len(records) - 1
        return [
            {**rec, "_revision_seq": base + i}
            for i, rec in enumerate(records)
        ]

    def _count_changes(
        self,
        old_table: pa.Table,
        new_table: pa.Table,
        nk_cols: tuple[str, ...],
        is_revision: bool,
    ) -> tuple[int, int]:
        """统计 inserted 和 updated 数量。

        Returns:
            (inserted_count, updated_count)
        """
        inserted = 0
        updated = 0

        if old_table.num_rows == 0:
            # 无旧数据，全部为新插入
            return (new_table.num_rows, 0)

        # 构建旧数据的 natural_key 集合
        old_nk_set: set[tuple[object, ...]] = set()
        for i in range(old_table.num_rows):
            nk_key = tuple(
                old_table[col][i].as_py() for col in nk_cols
            )
            old_nk_set.add(nk_key)

        # 审计 M-7：同批次内相同 NK 只计一次（去重后真正写入一行），
        # 避免批内重复记录重复计入 inserted/updated
        seen_new: set[tuple[object, ...]] = set()
        for i in range(new_table.num_rows):
            nk_key = tuple(
                new_table[col][i].as_py() for col in nk_cols
            )
            if nk_key in seen_new:
                continue
            seen_new.add(nk_key)
            if nk_key in old_nk_set:
                updated += 1
            else:
                inserted += 1

        return inserted, updated

    def _values_changed(
        self, old_vals: dict[str, object], new_vals: dict[str, object]
    ) -> bool:
        """检查值列是否有变化。"""
        all_cols = set(old_vals.keys()) | set(new_vals.keys())
        for col in all_cols:
            old_v = old_vals.get(col)
            new_v = new_vals.get(col)
            if old_v != new_v:
                return True
        return False

    def _align_schemas(
        self, table1: pa.Table, table2: pa.Table
    ) -> tuple[pa.Table, pa.Table]:
        """对齐两个 PyArrow Table 的 schema。

        处理 pyarrow 读取 parquet 时产生的 large_string vs string 差异，
        timestamp[us] vs string 差异（旧数据读为 timestamp，新数据可能写为 string），
        以及新表多出的 _revision_seq 列。
        """
        # 审计 H-4：声明 schema 下两侧本就一致，直接返回（保住声明列序）
        if table1.schema.equals(table2.schema, check_metadata=False):
            return table1, table2

        # 找出共同的列名
        common_cols = sorted(set(table1.column_names) & set(table2.column_names))
        # 对齐共同列的 schema
        aligned_cols_1 = []
        aligned_cols_2 = []
        for col in common_cols:
            t1_type = table1.schema.field(col).type
            t2_type = table2.schema.field(col).type
            if t1_type != t2_type:
                # 统一 timestamp[us] 和 string：都转为 timestamp[us]
                if (
                    pa.types.is_timestamp(t1_type)
                    and pa.types.is_string(t2_type)
                ) or (
                    pa.types.is_timestamp(t2_type)
                    and pa.types.is_string(t1_type)
                ):
                    cast_to = pa.timestamp("us")
                elif pa.types.is_string(t1_type) and pa.types.is_large_string(t2_type):
                    cast_to = pa.large_string()
                elif pa.types.is_large_string(t1_type) and pa.types.is_string(t2_type):
                    cast_to = pa.large_string()
                else:
                    # 不兼容类型，跳过对齐
                    aligned_cols_1.append(col)
                    aligned_cols_2.append(col)
                    continue

                # 对两个 table 都转为统一类型
                if cast_to != table1.schema.field(col).type:
                    table1 = table1.cast(
                        pa.schema([
                            pa.field(c, cast_to) if c == col else table1.schema.field(c)
                            for c in table1.column_names
                        ])
                    )
                if cast_to != table2.schema.field(col).type:
                    table2 = table2.cast(
                        pa.schema([
                            pa.field(c, cast_to) if c == col else table2.schema.field(c)
                            for c in table2.column_names
                        ])
                    )
            aligned_cols_1.append(col)
            aligned_cols_2.append(col)

        return table1.select(aligned_cols_1), table2.select(aligned_cols_2)

    def _merge_dedup(
        self,
        old_table: pa.Table,
        new_table: pa.Table,
        nk_cols: tuple[str, ...],
        is_revision: bool,
    ) -> pa.Table:
        """合并旧数据和新数据，按 natural_key 去重。

        使用 _revision_seq 排序，keep last（最新的保留）。
        """
        # 合并旧+新：对齐 schema 后处理
        aligned_old, aligned_new = self._align_schemas(old_table, new_table)
        merged = pa.concat_tables([aligned_old, aligned_new], promote_options="default")

        if is_revision:
            # revision 类：natural_key + revision_time 组合唯一
            # 按 _revision_seq 排序，同一 (nk, revision_time) 去重
            return self._revision_dedup(merged, nk_cols)

        # 非 revision 类：按 _revision_seq 排序，natural_key 去重（keep last）
        return self._dedup_by_nk(merged, nk_cols)

    def _revision_dedup(
        self,
        table: pa.Table,
        nk_cols: tuple[str, ...],
    ) -> pa.Table:
        """revision 类去重：(natural_key + revision_time) 组合唯一。"""
        # 找出 revision_time 列
        rev_time_col = None
        for col in nk_cols:
            if col == "revision_time" and col in table.column_names:
                rev_time_col = col
                break

        if rev_time_col is None:
            # 退化为普通去重
            return self._dedup_by_nk(table, nk_cols)

        # 按 _revision_seq 排序 → 相同 (nk, revision_time) 的保留最后一条
        dedup_keys = list(nk_cols) + [rev_time_col]
        return self._dedup_by_columns(table, dedup_keys)

    def _dedup_by_nk(
        self, table: pa.Table, nk_cols: tuple[str, ...]
    ) -> pa.Table:
        """按 natural_key 列去重（keep last）。"""
        return self._dedup_by_columns(table, list(nk_cols))

    def _dedup_by_columns(
        self, table: pa.Table, dedup_cols: list[str]
    ) -> pa.Table:
        """按指定列去重，保留每组最后一条。"""
        if table.num_rows == 0:
            return table

        # 使用 pandas 做去重（pyarrow 原生去重支持有限）
        df = table.to_pandas()

        # 按 dedup_cols + _revision_seq 排序（如果有 _revision_seq）
        sort_cols = dedup_cols
        if "_revision_seq" in df.columns:
            sort_cols = dedup_cols + ["_revision_seq"]

        # 审计 M-6：stable 排序保证相同排序键下输入顺序确定（keep-last 可复现）
        df = df.sort_values(sort_cols, ignore_index=True, kind="stable")
        df = df.drop_duplicates(subset=dedup_cols, keep="last")
        df = df.sort_values(sort_cols, ignore_index=True, kind="stable")

        # 审计 H-4：pandas 往返会重推类型（timestamp[us]→[ns]、date32→object），
        # 必须按原 schema 还原，保证写盘类型稳定
        return pa.Table.from_pandas(df, schema=table.schema, preserve_index=False)

    def _next_part_seq(self, partition_dir: Path) -> int:
        """获取下一个分区文件序列号。"""
        existing = list(partition_dir.glob("part-*.parquet"))
        if not existing:
            return 1

        max_seq = 0
        for f in existing:
            try:
                fname = f.stem  # part-{seq}
                seq_str = fname[5:]  # 去掉 "part-"
                seq = int(seq_str)
                max_seq = max(max_seq, seq)
            except (ValueError, IndexError):
                continue

        return max_seq + 1
