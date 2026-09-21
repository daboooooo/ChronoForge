"""DuckDBQueryService — 研究查询服务（D07 §1，QUERY-001）。

query() 六规则（按序执行，生成单条 SQL）：
1. dataset_id → registry 查 canonical_type → 视图名（type 小写）
2. 时间列选择：D02 §4 时间字段矩阵（storage.base.partition_time_field）；
   无独立时间业务字段的参考类型（ENTITY/INSTRUMENT，审计 M-5）回退 ingest_timestamp
3. as-of 语义：revision_supported=true → 切换 {type}_asof 点时视图 + SET VARIABLE "asof"
   （asof 缺省=now()）；false → 直接放行（现 27 类型中无 revision 外的 release_time 载体）
4. filters/columns 白名单校验：列名来自视图 introspection（防注入），
   值全部参数化（? 占位符）
5. columns 投影；恒附加 dataset_version/schema_version 元信息到 QueryResult
6. 默认排序：主时间列升序 + natural key 次列稳定排序；不可配置关闭（确定性/可复现）

只读契约：本服务持有的 DuckDB 连接由调用方以 read_only 模式打开（架构 02 规则 4
的执行点），服务自身不产生任何写路径。

dataset_version 语义（D07 §1）：该 dataset 最近一次 SUCCESS run 的
code_version+schema_version 复合（"{code}+{schema}" 格式，无 SUCCESS run 时为 ""）。
语义经 run_log 查询实现（D07 §1 提及的 dataset_registry.current_version 扩展列
对应迁移 0003 未派发；run_log 为同一事实源，无需 schema 变更）。
"""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import duckdb
import polars as pl

from chronoforge.models.enums import CanonicalType
from chronoforge.storage.base import natural_key, partition_time_field

# 审计 M-5：ENTITY/INSTRUMENT 无独立时间业务字段，时间列回退（canonical 层
# 同以此字段做分区 fallback）
_NO_TIME_FIELD_FALLBACK = "ingest_timestamp"


@dataclass(frozen=True)
class QueryResult:
    """查询结果（D07 §1，元信息三字段恒附加）。"""

    frame: pl.DataFrame  # polars（批量语义）；.to_pandas() 惰性供研究
    dataset_id: str
    dataset_version: str
    schema_version: str
    row_count: int
    elapsed_ms: int


@dataclass(frozen=True)
class DatasetEntry:
    """dataset_registry 行的只读投影（查询所需最小字段集）。"""

    dataset_id: str
    canonical_type: CanonicalType
    revision_supported: bool


@dataclass(frozen=True)
class DatasetVersionInfo:
    """最近一次 SUCCESS run 的版本信息（dataset_version 的事实源）。"""

    code_version: str
    schema_version: str


class DatasetRegistry:
    """dataset_registry 表的只读查询接口（D07 §1 构造参数 `registry`）。

    连接来自 MetaStore.connection（SQLite，只读使用；与 pipeline/runner.py
    查询 dataset_registry 同一模式）。完整注册表服务按 D01 §2 规划于
    registry/service.py（未派发），QUERY-001 依赖读侧最小接口。
    """

    def __init__(self, connection: sqlite3.Connection) -> None:
        """Args:
            connection: SQLite 连接（MetaStore.connection，meta 已 migrate）。
        """
        self._conn = connection

    def get(self, dataset_id: str) -> DatasetEntry | None:
        """按 dataset_id 查询数据集登记项；不存在返回 None。"""
        row = self._conn.execute(
            "SELECT dataset_id, canonical_type, revision_supported "
            "FROM dataset_registry WHERE dataset_id = ?",
            (dataset_id,),
        ).fetchone()
        if row is None:
            return None
        return DatasetEntry(
            dataset_id=str(row[0]),
            canonical_type=CanonicalType(str(row[1])),
            revision_supported=bool(row[2]),
        )

    def version_info(self, dataset_id: str) -> DatasetVersionInfo | None:
        """最近一次 SUCCESS run 的版本信息；无 SUCCESS run 返回 None。"""
        row = self._conn.execute(
            "SELECT code_version, schema_version FROM run_log "
            "WHERE dataset_id = ? AND status = 'SUCCESS' "
            "ORDER BY started_at DESC LIMIT 1",
            (dataset_id,),
        ).fetchone()
        if row is None:
            return None
        return DatasetVersionInfo(
            code_version=str(row[0]), schema_version=str(row[1])
        )


def _naive_utc(dt: datetime) -> datetime:
    """规范化为 naive-UTC（canonical 层 timestamp[us] 存储约定，D03 §3）。"""
    if dt.tzinfo is not None:
        return dt.astimezone(UTC).replace(tzinfo=None)
    return dt


class DuckDBQueryService:
    """DuckDB 只读查询服务（研究者与 AI Agent 的唯一数据出口，D07 §1）。"""

    def __init__(self, con: duckdb.DuckDBPyConnection, registry: DatasetRegistry):
        """
        Args:
            con: read_only DuckDB 连接（视图经 STORAGE-004 register_views 预注册）。
            registry: DatasetRegistry 实例（dataset_registry 表只读查询接口）。
        """
        self.con = con
        self.registry = registry

    def query(
        self,
        dataset_id: str,
        columns: list[str] | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        asof: datetime | None = None,
        filters: Mapping[str, Any] | None = None,
    ) -> QueryResult:
        """查询入口（D07 §1 六规则按序执行）。

        Args:
            dataset_id: 数据集 ID（dataset_registry 主键）。
            columns: 投影列（白名单校验）；None = 全列。
            start: 主时间列下界（含）；naive 视为 UTC。
            end: 主时间列上界（不含）。
            asof: 点时语义时刻；缺省 now()。仅 revision_supported 类型生效。
            filters: 相等过滤（列名白名单校验，值参数化）。

        Returns:
            QueryResult（frame + 恒附加的元信息）。

        Raises:
            ValueError: dataset_id 不存在，或 columns/filters 含视图外列名。
        """
        start_ts = time.monotonic()

        # 规则 1：dataset_id → canonical_type → 视图名
        entry = self.registry.get(dataset_id)
        if entry is None:
            raise ValueError(f"Dataset not found: {dataset_id}")
        view_name = entry.canonical_type.value.lower()

        # 规则 2：时间列选择（D02 §4 矩阵）
        time_col = self._resolve_time_column(entry.canonical_type)

        # 规则 3：as-of 语义（revision_supported → 点时视图，缺省 now()）
        if entry.revision_supported:
            view_name = f"{view_name}_asof"
            asof_value = (
                _naive_utc(asof) if asof is not None
                else datetime.now(UTC).replace(tzinfo=None)
            )
            # datetime.isoformat() 仅含 [0-9T:.-]，无注入面（与 STORAGE-004 测试同模式）
            self.con.execute(f'SET VARIABLE "asof" = \'{asof_value.isoformat()}\'')

        # 规则 4：视图列白名单（列名来自 introspection）
        view_columns = self._get_view_columns(view_name)
        if view_columns is None:
            # 视图未注册（数据未写入，STORAGE-004 惰性语义）→ 空结果
            return self._empty_result(entry, start_ts)
        if columns:
            unknown = [c for c in columns if c not in view_columns]
            if unknown:
                raise ValueError(f"Unknown column(s): {unknown}")
        if filters:
            for col in filters:
                if col not in view_columns:
                    raise ValueError(f"Unknown column: {col}")

        # 规则 5+6：投影/过滤（值参数化）+ 默认稳定排序
        sql, params = self._build_sql(
            view_name=view_name,
            time_col=time_col,
            canonical_type=entry.canonical_type,
            view_columns=view_columns,
            columns=columns,
            start=start,
            end=end,
            filters=filters,
        )
        frame = self.con.execute(sql, params or None).pl()

        elapsed_ms = int((time.monotonic() - start_ts) * 1000)
        return QueryResult(
            frame=frame,
            dataset_id=entry.dataset_id,
            dataset_version=self._get_dataset_version(entry.dataset_id),
            schema_version=self._get_schema_version(entry.dataset_id),
            row_count=frame.height,
            elapsed_ms=elapsed_ms,
        )

    # ── 规则 2：时间列选择 ────────────────────────────────────────────

    def _resolve_time_column(self, canonical_type: CanonicalType) -> str:
        """主时间列（D02 §4 矩阵；无独立时间字段的参考类型回退 ingest_timestamp）。"""
        field = partition_time_field(canonical_type)
        if field is None:
            return _NO_TIME_FIELD_FALLBACK
        return field

    # ── 规则 4：视图列白名单 ──────────────────────────────────────────

    def _get_view_columns(self, view_name: str) -> set[str] | None:
        """视图列 introspection；视图未注册返回 None。

        列名来自 DuckDB catalog（非用户输入），这是防注入的第一道防线：
        白名单之外的任何列名在规则 4 即被 ValueError 拒绝。
        """
        try:
            rows = self.con.execute(f'DESCRIBE "{view_name}"').fetchall()
        except duckdb.Error:
            return None
        return {str(r[0]) for r in rows}

    # ── 规则 5+6：SQL 构建（单条） ────────────────────────────────────

    def _build_sql(
        self,
        *,
        view_name: str,
        time_col: str,
        canonical_type: CanonicalType,
        view_columns: set[str],
        columns: list[str] | None,
        start: datetime | None,
        end: datetime | None,
        filters: Mapping[str, Any] | None,
    ) -> tuple[str, list[Any]]:
        """构建单条 SQL；列名经白名单+引号包裹，值全部 ? 参数化。"""
        projection = ", ".join(f'"{c}"' for c in columns) if columns else "*"
        sql = f'SELECT {projection} FROM "{view_name}"'

        params: list[Any] = []
        clauses: list[str] = []
        if start is not None:
            clauses.append(f'"{time_col}" >= ?')
            params.append(_naive_utc(start))
        if end is not None:
            clauses.append(f'"{time_col}" < ?')
            params.append(_naive_utc(end))
        if filters:
            for col, value in filters.items():
                clauses.append(f'"{col}" = ?')
                params.append(value)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)

        # 规则 6：主时间列升序 + natural key 次列稳定排序（不可关闭）。
        # nk 列不在视图列集中时跳过（schema 缺列防御，正常 canonical 数据恒有）
        order_cols = [time_col]
        order_cols += [
            c for c in natural_key(canonical_type)
            if c != time_col and c in view_columns
        ]
        sql += " ORDER BY " + ", ".join(f'"{c}" ASC' for c in order_cols)
        return sql, params

    # ── 规则 5：恒附加元信息 ──────────────────────────────────────────

    def _get_dataset_version(self, dataset_id: str) -> str:
        """最近一次 SUCCESS run 的 code_version+schema_version 复合（D07 §1）。"""
        info = self.registry.version_info(dataset_id)
        if info is None:
            return ""
        return f"{info.code_version}+{info.schema_version}"

    def _get_schema_version(self, dataset_id: str) -> str:
        """canonical schema 版本：最近 SUCCESS run 的 schema_version，缺省 "1.0"（D02 §1）。"""
        info = self.registry.version_info(dataset_id)
        if info is None or not info.schema_version:
            return "1.0"
        return info.schema_version

    def _empty_result(self, entry: DatasetEntry, start_ts: float) -> QueryResult:
        """视图未注册（数据未写入）时的空结果（元信息仍恒附加）。"""
        return QueryResult(
            frame=pl.DataFrame(),
            dataset_id=entry.dataset_id,
            dataset_version=self._get_dataset_version(entry.dataset_id),
            schema_version=self._get_schema_version(entry.dataset_id),
            row_count=0,
            elapsed_ms=int((time.monotonic() - start_ts) * 1000),
        )
