"""DuckDB 视图注册（D03 §4，STORAGE-004）。

职责：
- register_views：为所有 canonical type 注册 DuckDB 视图（read_parquet hive_partitioning）
- as-of 视图：为 revision_supported 类型注册点时视图（row_number + release_time <= asof）
- missing_views：只读探测「应注册未注册」视图集合（READY-005 方案 A 快路径判定）
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

def _base_view_sql(canonical_base: str, type_name: str) -> str:
    """基础视图 SELECT 语句（注册与只读探测共用，单一事实源）。"""
    parquet_path = f"{canonical_base}/{type_name}/**/*.parquet"
    return f"SELECT * FROM read_parquet('{parquet_path}', hive_partitioning=1)"


def _asof_view_sql(canonical_base: str, ct: CanonicalType) -> str:
    """as-of 点时视图 SELECT 语句（注册与只读探测共用）。

    SQL 模式（以 NUMBER 为例）：
    ```sql
    SELECT * EXCLUDE (rn) FROM (
      SELECT *, row_number() OVER (
        PARTITION BY source_id, observation_time
        ORDER BY revision_time DESC) AS rn
      FROM number
      WHERE release_time <= getvariable('asof')
    ) WHERE rn = 1;
    ```

    `release_time <= asof` 确保仅显示发布前可见的数据（防 look-ahead）；
    getvariable 未 SET 时求值为 NULL（不报错，只读探测安全）。
    """
    partition_cols = _PARTITION_TIME_FIELD_MAP.get(ct, "source_id")
    parquet_path = f"{canonical_base}/{ct.value}/**/*.parquet"
    return (
        f"SELECT * EXCLUDE (rn) FROM ("
        f"  SELECT *, row_number() OVER ("
        f"    PARTITION BY {partition_cols} "
        f"    ORDER BY revision_time DESC"
        f"  ) AS rn"
        f"  FROM read_parquet('{parquet_path}', hive_partitioning=1)"
        f"  WHERE release_time <= getvariable('asof')"
        f") WHERE rn = 1"
    )


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
        _try_register_view(
            con, ct.value.lower(), _base_view_sql(canonical_base, ct.value)
        )

    # 注册 as-of 视图（revision_supported 类型）
    _register_asof_views(con, canonical_base)


def _try_register_view(
    con: duckdb.DuckDBPyConnection, view_name: str, select_sql: str
) -> None:
    """尝试注册单个视图，绑定失败（分区目录不存在等）时跳过（惰性加载）。"""
    try:
        con.execute(f"CREATE OR REPLACE VIEW {view_name} AS {select_sql}")
    except duckdb.Error:
        # 找不到 parquet 文件 → 跳过（惰性，数据写入后重新注册）
        pass


def _register_asof_views(
    con: duckdb.DuckDBPyConnection, canonical_base: str
) -> None:
    """注册 revision_supported 类型的 as-of 点时视图（SQL 见 _asof_view_sql）。"""
    for ct in REVISION_TYPES:
        _try_register_view(
            con, f"{ct.value.lower()}_asof", _asof_view_sql(canonical_base, ct)
        )


def _view_bindable(con: duckdb.DuckDBPyConnection, select_sql: str) -> bool:
    """只读探测视图 SELECT 能否绑定（与 CREATE VIEW 的绑定校验同源）。"""
    try:
        con.execute(f"SELECT * FROM ({select_sql}) LIMIT 0")
    except duckdb.Error:
        return False
    return True


def missing_views(con: duckdb.DuckDBPyConnection, data_dir: str) -> set[str]:
    """只读探测：register_views 将注册而 catalog 尚缺的视图名（READY-005 方案 A）。

    判定与 register_views 的惰性注册同源（共用 _base_view_sql/_asof_view_sql，
    同一 read_parquet 绑定校验）：返回空集 ⇒ catalog 视图已齐全，
    open_query_service 可跳过写模式注册，消除并发 CLI 的 query.duckdb
    文件锁窗口（审计 SR-16）。

    PROVISIONAL：视图定义由 (data_dir, type) 决定性生成，同名视图视作最新；
    升级修改视图 SQL 模板后需删除 query.duckdb 重建（纯派生物，零成本）。

    Args:
        con: 只读 DuckDB 连接（探测不写 catalog）。
        data_dir: canonical 数据目录路径（与 register_views 参数一致）。

    Returns:
        待注册视图名集合；空集表示无需写模式。
    """
    canonical_base = f"{data_dir}/canonical"
    registered = get_registered_view_names(con)
    missing: set[str] = set()
    for ct in CanonicalType:
        name = ct.value.lower()
        if name not in registered and _view_bindable(
            con, _base_view_sql(canonical_base, ct.value)
        ):
            missing.add(name)
    for ct in REVISION_TYPES:
        name = f"{ct.value.lower()}_asof"
        if name not in registered and _view_bindable(
            con, _asof_view_sql(canonical_base, ct)
        ):
            missing.add(name)
    return missing


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
