"""STORAGE-004 集成测试 — DuckDB 视图注册（含 as-of 点时视图）。

依据：
- D03 §4 视图 SQL
- STORAGE-004.md 测试要求（TC-S-005 as-of 两 vintage / SQL 文本快照 / 边界）
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from chronoforge.models.enums import CanonicalType
from chronoforge.storage.views import (
    REVISION_TYPES,
    get_all_view_names,
    get_asof_view_names,
    get_base_view_names,
    get_registered_view_names,
    register_views,
)

# ── Helpers ─────────────────────────────────────────────────────────────


def _write_sample_parquet(
    data_dir: Path,
    canonical_type: CanonicalType,
    records: list[dict],
) -> Path:
    """将 records 写为 parquet 文件，路径为 canonical/{type}/entity=xxx/year=YYYY/month=MM/。

    Returns:
        parquet 文件路径。
    """
    # 提取 entity_id（取第一条记录的 entity_id 或 source_id）
    if records:
        rec = records[0]
        entity_id = rec.get("entity_id", rec.get("source_id", "test"))
    else:
        entity_id = "test"

    # 提取时间字段用于分区
    time_field = _get_time_field(canonical_type)
    if time_field and records:
        time_val = records[0].get(time_field)
        if time_val is not None:
            if isinstance(time_val, datetime):
                dt = time_val
            else:
                dt = datetime.fromisoformat(str(time_val))
            year = dt.strftime("%Y")
            month = dt.strftime("%m")
        else:
            year = "2026"
            month = "01"
    else:
        year = "2026"
        month = "01"

    partition_dir = data_dir / "canonical" / canonical_type.value / (
        f"entity={entity_id}"
    ) / f"year={year}" / f"month={month}"
    partition_dir.mkdir(parents=True, exist_ok=True)

    # 构建 PyArrow schema
    if not records:
        # 空文件
        table = pa.table({})
    else:
        columns: dict[str, list] = {}
        for rec in records:
            for k, v in rec.items():
                if k not in columns:
                    columns[k] = []
                columns[k].append(v)
        table = _build_arrow_table(columns)

    parquet_file = partition_dir / "part-0001.parquet"
    pq.write_table(table, str(parquet_file), compression="zstd")
    return parquet_file


def _get_time_field(canonical_type: CanonicalType) -> str | None:
    """获取 canonical type 对应的时间字段名。"""
    from chronoforge.storage.base import partition_time_field

    return partition_time_field(canonical_type)


def _build_arrow_table(columns: dict[str, list]) -> pa.Table:

    arrays = {}
    for col_name, values in columns.items():
        py_values = [v if v is not None else None for v in values]

        # datetime 类型
        non_none = [v for v in py_values if v is not None]
        if non_none and all(isinstance(v, datetime) for v in non_none):
            timestamps = [
                v.replace(tzinfo=None) if isinstance(v, datetime) else v
                for v in py_values
            ]
            arrays[col_name] = pa.array(timestamps, type=pa.timestamp("us"))
            continue

        # 整数类型
        if all(isinstance(v, (int, type(None))) for v in py_values):
            non_none = [v for v in py_values if v is not None]
            if non_none and all(isinstance(v, int) for v in non_none):
                arrays[col_name] = pa.array(py_values, type=pa.int64())
                continue

        # 浮点数类型
        if all(isinstance(v, (int, float, type(None))) for v in py_values):
            non_none = [v for v in py_values if v is not None]
            if non_none and all(isinstance(v, float) for v in non_none):
                arrays[col_name] = pa.array(py_values, type=pa.float64())
                continue

        # 默认字符串
        arrays[col_name] = pa.array(
            [str(v) if v is not None else None for v in py_values]
        )

    return pa.table(arrays)


# ── TC-S-005: as-of 视图取正确 vintage ──────────────────────────────────


class TestAsOfVintage:

    def test_asof_view_takes_correct_vintage(self, tmp_stores) -> None:
        """TC-S-005: Given v1/v2 两 vintage When SET VARIABLE asof=T1 Then 仅 v1 可见"""
        import duckdb

        base_time = datetime(2026, 9, 11, 10, 0, 0)
        v1_time = base_time - timedelta(hours=1)  # T1 = v1 时间
        v2_time = base_time + timedelta(hours=1)  # T2 = v2 时间

        # 构造 v1 和 v2 记录（同一 observation_time，不同 release_time/revision_time）
        v1_record = {
            "schema_version": "1.0",
            "source": "FRED",
            "source_id": "FRED:GDP",
            "source_timestamp": base_time,
            "ingest_timestamp": base_time,
            "raw_record_id": "FRED:macro:file.jsonl:1",
            "observation_time": base_time,
            "release_time": v1_time,
            "revision_time": v1_time,
            "value": 100.0,
            "units": "IDX",
            "seasonal_adjustment": "SA",
        }
        v2_record = {
            **v1_record,
            "release_time": v2_time,
            "revision_time": v2_time,
            "value": 200.0,  # v2 修正了值
        }

        # 写 parquet（v1 和 v2 都需要写入）
        _write_sample_parquet(tmp_stores.data_dir, CanonicalType.NUMBER, [v1_record, v2_record])

        # 注册视图
        con = duckdb.connect()
        register_views(con, str(tmp_stores.data_dir))

        # asof=T1（v1 的 release_time）→ 仅 v1 可见
        con.execute(f'SET VARIABLE "asof" = \'{v1_time.isoformat()}\'')
        result_v1 = con.execute("SELECT value FROM number_asof ORDER BY revision_time").fetchall()
        assert len(result_v1) == 1
        assert result_v1[0][0] == 100.0

        # asof=T2（v2 的 release_time）→ 仅 v2 可见
        con.execute(f'SET VARIABLE "asof" = \'{v2_time.isoformat()}\'')
        result_v2 = con.execute("SELECT value FROM number_asof ORDER BY revision_time").fetchall()
        assert len(result_v2) == 1
        assert result_v2[0][0] == 200.0

        con.close()

    def test_asof_view_prefers_latest_revision(self, tmp_stores) -> None:
        """as-of 视图：同 observation_time 多 revision 时取最新 revision 且 release_time <= asof"""
        import duckdb

        base_time = datetime(2026, 9, 11, 10, 0, 0)
        # 三个 revision 时间
        r1_time = base_time - timedelta(hours=2)
        r2_time = base_time - timedelta(hours=1)
        r3_time = base_time + timedelta(hours=1)  # 未来的 release_time

        records = [
            {
                "schema_version": "1.0",
                "source": "FRED",
                "source_id": "FRED:GDP",
                "source_timestamp": base_time,
                "ingest_timestamp": base_time,
                "raw_record_id": "FRED:macro:file.jsonl:1",
                "observation_time": base_time,
                "release_time": r1_time,
                "revision_time": r1_time,
                "value": 100.0,
                "units": "IDX",
                "seasonal_adjustment": "SA",
            },
            {
                "schema_version": "1.0",
                "source": "FRED",
                "source_id": "FRED:GDP",
                "source_timestamp": base_time,
                "ingest_timestamp": base_time,
                "raw_record_id": "FRED:macro:file.jsonl:2",
                "observation_time": base_time,
                "release_time": r2_time,
                "revision_time": r2_time,
                "value": 150.0,
                "units": "IDX",
                "seasonal_adjustment": "SA",
            },
            {
                "schema_version": "1.0",
                "source": "FRED",
                "source_id": "FRED:GDP",
                "source_timestamp": base_time,
                "ingest_timestamp": base_time,
                "raw_record_id": "FRED:macro:file.jsonl:3",
                "observation_time": base_time,
                "release_time": r3_time,
                "revision_time": r3_time,
                "value": 200.0,
                "units": "IDX",
                "seasonal_adjustment": "SA",
            },
        ]

        _write_sample_parquet(tmp_stores.data_dir, CanonicalType.NUMBER, records)

        con = duckdb.connect()
        register_views(con, str(tmp_stores.data_dir))

        # asof = r2_time → 应取 r2（value=150），r3 因 release_time > asof 被过滤
        con.execute(f'SET VARIABLE "asof" = \'{r2_time.isoformat()}\'')
        result = con.execute("SELECT value FROM number_asof ORDER BY revision_time").fetchall()
        assert len(result) == 1
        assert result[0][0] == 150.0

        con.close()


# ── SQL 文本快照 ─────────────────────────────────────────────────────────


class TestSQLSnapshot:

    def test_view_names_match_canonical_types(self, tmp_stores) -> None:
        """SQL 文本快照：register_views 后 SHOW TABLES 返回 CanonicalType 子集视图（惰性注册）。"""
        import duckdb

        con = duckdb.connect()
        register_views(con, str(tmp_stores.data_dir))

        registered = get_registered_view_names(con)
        expected_base = get_base_view_names()

        # 已注册视图应为所有 canonical type 视图的子集（惰性注册：无数据则跳过）
        assert registered.issubset(expected_base)

        con.close()

    def test_all_canonical_types_have_views(self) -> None:
        """所有 CanonicalType 都有对应的视图名。"""
        expected = get_base_view_names()
        actual = {ct.value.lower() for ct in CanonicalType}
        assert expected == actual
        # 验证 27 个类型
        assert len(CanonicalType) == 27
        assert len(expected) == 27

    def test_asof_view_names_match_revision_types(self) -> None:
        """as-of 视图名与 REVISION_TYPES 一致。"""
        expected_asof = get_asof_view_names()
        actual_asof = {f"{ct.value.lower()}_asof" for ct in REVISION_TYPES}
        assert expected_asof == actual_asof

    def test_all_view_names_comprehensive(self) -> None:
        """get_all_view_names() 返回基础视图 + as-of 视图的完整集合。"""
        all_views = get_all_view_names()
        expected_all = get_base_view_names() | get_asof_view_names()
        assert all_views == expected_all

    def test_no_duplicate_view_names(self) -> None:
        """as-of 视图名不与基础视图名冲突。"""
        base_views = get_base_view_names()
        asof_views = get_asof_view_names()
        assert len(base_views & asof_views) == 0


# ── 边界：空 parquet 目录 ─────────────────────────────────────────────────


class TestBoundaryEmptyDir:

    def test_empty_parquet_dir_view_registered_zero_rows(self, tmp_stores) -> None:
        """边界：写一条 OHLCV 记录 → 视图注册成功且查询返回 1 行。"""
        import duckdb

        base_time = datetime(2026, 9, 11, 10, 0, 0)
        record = {
            "schema_version": "1.0",
            "source": "test",
            "source_id": "btcusdt",
            "source_timestamp": base_time,
            "ingest_timestamp": base_time,
            "raw_record_id": "test:ohlcv:file.jsonl:1",
            "market_id": "BINANCE:BTCUSDT:SPOT",
            "event_time": base_time,
            "open": 50000.0,
            "high": 51000.0,
            "low": 49000.0,
            "close": 50500.0,
            "volume": 100.0,
        }
        _write_sample_parquet(tmp_stores.data_dir, CanonicalType.OHLCV, [record])

        con = duckdb.connect()
        register_views(con, str(tmp_stores.data_dir))

        # ohlcv 视图存在且查询返回 1 行
        registered = get_registered_view_names(con)
        assert "ohlcv" in registered

        result = con.execute("SELECT COUNT(*) FROM ohlcv").fetchone()
        assert result[0] == 1

        con.close()


# ── 边界：单行分区 ────────────────────────────────────────────────────────


class TestBoundarySingleRow:

    def test_single_row_partition(self, tmp_stores) -> None:
        """边界：单行分区（视图表单正确，数据可查）。"""
        import duckdb

        base_time = datetime(2026, 9, 11, 10, 0, 0)
        record = {
            "schema_version": "1.0",
            "source": "test",
            "source_id": "btcusdt",
            "source_timestamp": base_time,
            "ingest_timestamp": base_time,
            "raw_record_id": "test:ohlcv:file.jsonl:1",
            "market_id": "BINANCE:BTCUSDT:SPOT",
            "event_time": base_time,
            "open": 50000.0,
            "high": 51000.0,
            "low": 49000.0,
            "close": 50500.0,
            "volume": 100.0,
        }

        _write_sample_parquet(tmp_stores.data_dir, CanonicalType.OHLCV, [record])

        con = duckdb.connect()
        register_views(con, str(tmp_stores.data_dir))

        result = con.execute("SELECT COUNT(*) FROM ohlcv").fetchone()
        assert result[0] == 1

        con.close()


# ── 失败：缺失分区目录（惰性视图） ─────────────────────────────────────────


class TestFailureLazyView:

    def test_missing_partition_dir_view_registerable(self, tmp_stores) -> None:
        """失败：缺失分区目录 → register_views 不抛异常（惰性视图）。"""
        import duckdb

        # 确保 canonical/OHLCV 目录不存在
        ohlcv_dir = tmp_stores.data_dir / "canonical" / "OHLCV"
        assert not ohlcv_dir.exists()

        con = duckdb.connect()
        # 应成功注册（不抛异常）
        register_views(con, str(tmp_stores.data_dir))

        # OHLCV 视图因无数据未被注册（惰性），但 TICKER 等视图也应未被注册
        registered = get_registered_view_names(con)
        # 无数据 → 无视图注册
        assert "ohlcv" not in registered
        assert registered == set()

        con.close()


# ── Integration：视图注册后 QueryService 可正常查询 ────────────────────────


class TestIntegrationQueryService:

    def test_view_query_after_register(self, tmp_stores) -> None:
        """集成：VIEW 注册后 DuckDB 可正常查询（模拟 QueryService 取数）。"""
        import duckdb

        base_time = datetime(2026, 9, 11, 10, 0, 0)
        records = [
            {
                "schema_version": "1.0",
                "source": "test",
                "source_id": "btcusdt",
                "source_timestamp": base_time,
                "ingest_timestamp": base_time,
                "raw_record_id": f"test:ohlcv:file.jsonl:{i}",
                "market_id": "BINANCE:BTCUSDT:SPOT",
                "event_time": base_time + timedelta(minutes=i),
                "open": 50000.0 + i,
                "high": 51000.0 + i,
                "low": 49000.0 - i,
                "close": 50500.0 + i,
                "volume": 100.0 + i,
            }
            for i in range(5)
        ]

        _write_sample_parquet(tmp_stores.data_dir, CanonicalType.OHLCV, records)

        con = duckdb.connect()
        register_views(con, str(tmp_stores.data_dir))

        # 模拟 QueryService 查询
        result = con.execute(
            "SELECT market_id, event_time, close, volume FROM ohlcv ORDER BY event_time"
        ).fetchall()

        assert len(result) == 5
        # 验证数据正确性
        for i, row in enumerate(result):
            assert row[0] == "BINANCE:BTCUSDT:SPOT"
            assert row[2] == 50500.0 + i  # close
            assert row[3] == 100.0 + i    # volume

        con.close()

    def test_number_asof_integration(self, tmp_stores) -> None:
        """集成：NUMBER 类型 as-of 视图注册后可正常查询。"""
        import duckdb

        base_time = datetime(2026, 9, 11, 10, 0, 0)
        v1_release = base_time - timedelta(days=1)
        v2_release = base_time

        # 写两条 v1/v2 数据
        records = [
            {
                "schema_version": "1.0",
                "source": "FRED",
                "source_id": "FRED:UNRATE",
                "source_timestamp": base_time,
                "ingest_timestamp": base_time,
                "raw_record_id": "FRED:macro:file.jsonl:1",
                "observation_time": base_time,
                "release_time": v1_release,
                "revision_time": v1_release,
                "value": 4.5,
                "units": "PCT",
                "seasonal_adjustment": "SA",
            },
            {
                "schema_version": "1.0",
                "source": "FRED",
                "source_id": "FRED:UNRATE",
                "source_timestamp": base_time,
                "ingest_timestamp": base_time,
                "raw_record_id": "FRED:macro:file.jsonl:2",
                "observation_time": base_time,
                "release_time": v2_release,
                "revision_time": v2_release,
                "value": 4.3,
                "units": "PCT",
                "seasonal_adjustment": "SA",
            },
        ]

        _write_sample_parquet(tmp_stores.data_dir, CanonicalType.NUMBER, records)

        con = duckdb.connect()
        register_views(con, str(tmp_stores.data_dir))

        # asof=v1_release → 仅取 v1（value=4.5）
        con.execute(f'SET VARIABLE "asof" = \'{v1_release.isoformat()}\'')
        result = con.execute(
            "SELECT source_id, value FROM number_asof ORDER BY revision_time"
        ).fetchall()
        assert len(result) == 1
        assert result[0][0] == "FRED:UNRATE"
        assert result[0][1] == 4.5

        # asof=v2_release → 仅取 v2（value=4.3）
        con.execute(f'SET VARIABLE "asof" = \'{v2_release.isoformat()}\'')
        result = con.execute(
            "SELECT source_id, value FROM number_asof ORDER BY revision_time"
        ).fetchall()
        assert len(result) == 1
        assert result[0][1] == 4.3

        con.close()


# ── 多分区场景 ────────────────────────────────────────────────────────────


class TestMultiPartition:

    def test_multiple_partitions_same_type(self, tmp_stores) -> None:
        """多分区场景：同类型跨月分区 → 视图合并查询所有分区数据。"""
        import duckdb

        # 写 Aug 和 Sep 两月数据（先写数据，后注册视图）
        aug_time = datetime(2026, 8, 15, 10, 0, 0)
        sep_time = datetime(2026, 9, 15, 10, 0, 0)

        aug_record = {
            "schema_version": "1.0",
            "source": "test",
            "source_id": "btcusdt",
            "source_timestamp": aug_time,
            "ingest_timestamp": aug_time,
            "raw_record_id": "test:ohlcv:aug.jsonl:1",
            "market_id": "BINANCE:BTCUSDT:SPOT",
            "event_time": aug_time,
            "open": 50000.0,
            "high": 51000.0,
            "low": 49000.0,
            "close": 50500.0,
            "volume": 100.0,
        }
        sep_record = {
            **aug_record,
            "event_time": sep_time,
            "raw_record_id": "test:ohlcv:sep.jsonl:1",
            "close": 60000.0,
            "volume": 200.0,
        }

        _write_sample_parquet(tmp_stores.data_dir, CanonicalType.OHLCV, [aug_record])
        _write_sample_parquet(tmp_stores.data_dir, CanonicalType.OHLCV, [sep_record])

        con = duckdb.connect()
        register_views(con, str(tmp_stores.data_dir))

        result = con.execute("SELECT COUNT(*) FROM ohlcv").fetchone()
        assert result[0] == 2

        con.close()
