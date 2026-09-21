"""DuckDB 视图注册（D03 §4，STORAGE-004）。

职责：
- register_views：为所有 canonical type 注册 DuckDB 视图（read_parquet hive_partitioning）
- as-of 视图：为 revision_supported 类型注册点时视图（row_number + release_time <= asof）
- get_registered_view_names：测试辅助，获取已注册视图名集合

视图命名约定：
- 基础视图名 = canonical type 小写（如 ohlcv, trade, number, position）
- as-of 视图名 = {type}_asof（如 number_asof, position_asof）
"""

from __future__ import annotations

import duckdb

from chronoforge.models.enums import CanonicalType

# ── revision_supported 类型（D02 §2 身份键含 revision_time）─────────────

# 这些类型的 natural_key 含 revision_time，需要 as-of 点时视图。
REVISION_TYPES: frozenset[CanonicalType] = frozenset({
    CanonicalType.NUMBER,
    CanonicalType.FLOW,
    CanonicalType.POSITION,
    CanonicalType.POSITION_AGGREGATE,
})


# ── partition_time_field 映射（复用 canonical 层的分区时间字段）─────────

# D02 §4 时间字段适用矩阵：从 CanonicalType 到记录中时间字段的映射。
# 用于 as-of 视图中 partition by 子句。
_PARTITION_TIME_FIELD_MAP: dict[CanonicalType, str] = {
    # revision 类：partition by observation_time 或 report_date
    CanonicalType.NUMBER: "source_id, observation_time",
    CanonicalType.FLOW: "source_id, observation_time",
    CanonicalType.POSITION: "contract, report_date",
    CanonicalType.POSITION_AGGREGATE: "contract, report_date",
}


# ── 视图注册 ───────────────────────────────────────────────────────────

def register_views(con: duckdb.DuckDBPyConnection, data_dir: str) -> None:
    """注册全部 canonical type 的 DuckDB 视图（D03 §4）。

    对所有 CanonicalType 注册基础视图（read_parquet hive_partitioning），
    对 revision_supported 类型额外注册 as-of 点时视图。

    视图惰性：分区目录不存在时，该视图跳过注册（数据写入后重新调用 register_views 即可）。

    Args:
        con: DuckDB 连接（需为 write 模式以注册视图）。
        data_dir: canonical 数据目录路径，视图 SQL 中使用绝对路径。

    Raises:
        duckdb.Error: parquet 格式错误等 DuckDB 原生错误（不捕获）。
    """
    canonical_base = f"{data_dir}/canonical"

    # 注册基础视图（所有 CanonicalType）
    for ct in CanonicalType:
        _try_register_view(con, ct.value.lower(), canonical_base, ct.value)

    # 注册 as-of 视图（revision_supported 类型）
    _register_asof_views(con, canonical_base)


def _try_register_view(
    con: duckdb.DuckDBPyConnection,
    view_name: str,
    canonical_base: str,
    canonical_type_name: str,
) -> None:
    """尝试注册单个视图，找不到 parquet 文件时跳过（惰性加载）。"""
    parquet_path = f"{canonical_base}/{canonical_type_name}/**/*.parquet"
    sql = (
        f"CREATE OR REPLACE VIEW {view_name} AS "
        f"SELECT * FROM read_parquet('{parquet_path}', hive_partitioning=1)"
    )
    try:
        con.execute(sql)
    except duckdb.Error:
        # 找不到 parquet 文件 → 跳过（惰性，数据写入后重新注册）
        pass


def _register_asof_views(
    con: duckdb.DuckDBPyConnection, canonical_base: str
) -> None:
    """注册 revision_supported 类型的 as-of 点时视图。

    SQL 模式（以 NUMBER 为例）：
    ```sql
    CREATE OR REPLACE VIEW number_asof AS
    SELECT * EXCLUDE (rn) FROM (
      SELECT *, row_number() OVER (
        PARTITION BY source_id, observation_time
        ORDER BY revision_time DESC) AS rn
      FROM number
      WHERE release_time <= getvariable('asof')
    ) WHERE rn = 1;
    ```

    使用 `SET VARIABLE asof = ?` 绑定参数。
    `release_time <= asof` 确保仅显示发布前可见的数据（防 look-ahead）。
    """
    for ct in REVISION_TYPES:
        view_name = f"{ct.value.lower()}_asof"
        partition_cols = _PARTITION_TIME_FIELD_MAP.get(ct, "source_id")
        parquet_path = f"{canonical_base}/{ct.value}/**/*.parquet"

        sql = (
            f"CREATE OR REPLACE VIEW {view_name} AS "
            f"SELECT * EXCLUDE (rn) FROM ("
            f"  SELECT *, row_number() OVER ("
            f"    PARTITION BY {partition_cols} "
            f"    ORDER BY revision_time DESC"
            f"  ) AS rn"
            f"  FROM read_parquet('{parquet_path}', hive_partitioning=1)"
            f"  WHERE release_time <= getvariable('asof')"
            f") WHERE rn = 1"
        )
        try:
            con.execute(sql)
        except duckdb.Error:
            pass


def get_registered_view_names(con: duckdb.DuckDBPyConnection) -> set[str]:
    """获取已注册视图名（测试用）。

    查询 DuckDB 的 SHOW TABLES（视图在 DuckDB 中显示为表）。

    Args:
        con: DuckDB 连接。

    Returns:
        已注册视图名的集合（仅字符串类型）。
    """
    result = con.execute("SHOW TABLES").fetchall()
    return {row[0] for row in result if isinstance(row[0], str)}


def get_all_view_names() -> set[str]:
    """获取应注册的全部视图名（基础视图 + as-of 视图）。

    Returns:
        视图名集合。
    """
    views = {ct.value.lower() for ct in CanonicalType}
    views |= {f"{ct.value.lower()}_asof" for ct in REVISION_TYPES}
    return views


def get_base_view_names() -> set[str]:
    """获取基础视图名集合（不含 as-of 视图）。

    Returns:
        基础视图名 = CanonicalType 小写集合。
    """
    return {ct.value.lower() for ct in CanonicalType}


def get_asof_view_names() -> set[str]:
    """获取 as-of 视图名集合。

    Returns:
        as-of 视图名 = {type}_asof 集合。
    """
    return {f"{ct.value.lower()}_asof" for ct in REVISION_TYPES}
