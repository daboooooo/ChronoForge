"""QUERY-001 集成测试 — DuckDBQueryService（D07 §1 六规则）。

依据：
- D07 §1 QueryService 六规则（视图选择/时间列/as-of/白名单/投影/稳定排序）
- QUERY-001.md 测试要求（TC-R-001/002/003 + 边界 + 默认排序）

测试环境：SQLite meta（STORAGE-001 MetaStore）登记 dataset_registry/run_log；
canonical parquet 手工写入（同 test_views.py 惯例）；DuckDB catalog 文件先以
write 模式注册视图（STORAGE-004 register_views），再以 read_only 重开供
QueryService 使用（架构 02 规则 4 的执行点）。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from chronoforge.models.enums import CanonicalType
from chronoforge.research import DatasetRegistry, DuckDBQueryService
from chronoforge.storage.meta import MetaStore
from chronoforge.storage.views import register_views

# ── 固定基准时刻（全部早于当前时间，保证 asof 缺省=now() 语义确定性）───────

BASE_TIME = datetime(2026, 9, 11, 10, 0, 0)
T1 = BASE_TIME - timedelta(hours=1)   # v1 release/revision 时刻
T2 = BASE_TIME + timedelta(hours=1)   # v2 release/revision 时刻

DATASET_NUMBER = "FRED:MACRO"          # revision_supported=1
DATASET_OHLCV = "BINANCE:BTCUSDT:OHLCV"  # revision_supported=0
DATASET_NO_DATA = "EMPTY:DOC"          # 已登记但无 parquet（视图未注册）


# ── 测试环境装配 ────────────────────────────────────────────────────────


@dataclass(frozen=True)
class QueryEnv:
    """测试环境：service + 依赖句柄（meta 保持存活以维持 SQLite 连接）。"""

    service: DuckDBQueryService
    con: duckdb.DuckDBPyConnection
    meta: MetaStore
    data_dir: Path


def _number_records() -> list[dict[str, Any]]:
    """NUMBER 两 vintage + 跨 observation_time 数据（4 条）。

    - A: source_id=FRED:GDP,   obs=BASE,       rel/rev=T1, value=100
    - B: source_id=FRED:GDP,   obs=BASE,       rel/rev=T2, value=200（A 的修正版）
    - C: source_id=FRED:GDP,   obs=BASE+1day,  rel/rev=T1, value=300
    - D: source_id=FRED:UNRATE, obs=BASE,      rel/rev=T1, value=9
    """
    base = {
        "schema_version": "1.0",
        "source": "FRED",
        "source_timestamp": BASE_TIME,
        "ingest_timestamp": BASE_TIME,
    }
    return [
        {**base, "source_id": "FRED:GDP", "raw_record_id": "FRED:macro:f.jsonl:1",
         "observation_time": BASE_TIME, "release_time": T1, "revision_time": T1,
         "value": 100.0, "units": "IDX", "seasonal_adjustment": "SA"},
        {**base, "source_id": "FRED:GDP", "raw_record_id": "FRED:macro:f.jsonl:2",
         "observation_time": BASE_TIME, "release_time": T2, "revision_time": T2,
         "value": 200.0, "units": "IDX", "seasonal_adjustment": "SA"},
        {**base, "source_id": "FRED:GDP", "raw_record_id": "FRED:macro:f.jsonl:3",
         "observation_time": BASE_TIME + timedelta(days=1),
         "release_time": T1, "revision_time": T1,
         "value": 300.0, "units": "IDX", "seasonal_adjustment": "SA"},
        {**base, "source_id": "FRED:UNRATE", "raw_record_id": "FRED:macro:f.jsonl:4",
         "observation_time": BASE_TIME, "release_time": T1, "revision_time": T1,
         "value": 9.0, "units": "PCT", "seasonal_adjustment": "SA"},
    ]


def _ohlcv_records() -> list[dict[str, Any]]:
    """OHLCV 5 条（event_time 递增 1 分钟，含 nk 次列 market_id/interval）。"""
    return [
        {
            "schema_version": "1.0",
            "source": "BINANCE",
            "source_id": "btcusdt",
            "source_timestamp": BASE_TIME + timedelta(minutes=i),
            "ingest_timestamp": BASE_TIME,
            "raw_record_id": f"BINANCE:ohlcv:f.jsonl:{i}",
            "market_id": "BINANCE:BTCUSDT:SPOT",
            "event_time": BASE_TIME + timedelta(minutes=i),
            "interval": "1m",
            "open": 50000.0 + i,
            "high": 51000.0 + i,
            "low": 49000.0 - i,
            "close": 50500.0 + i,
            "volume": 100.0 + i,
        }
        for i in range(5)
    ]


def _write_parquet(
    data_dir: Path, canonical_type: CanonicalType, records: list[dict[str, Any]]
) -> None:
    """records 写为 canonical/{TYPE}/entity=…/year=…/month=…/part-0001.parquet。"""
    first = records[0]
    entity = first.get("entity_id", first.get("source_id", "test"))
    obs = first.get("observation_time", first.get("event_time", BASE_TIME))
    partition_dir = (
        data_dir / "canonical" / canonical_type.value
        / f"entity={entity}" / f"year={obs.strftime('%Y')}" / f"month={obs.strftime('%m')}"
    )
    partition_dir.mkdir(parents=True, exist_ok=True)

    columns: dict[str, list[Any]] = {}
    for rec in records:
        for k, v in rec.items():
            columns.setdefault(k, []).append(v)

    arrays: dict[str, pa.Array] = {}
    for name, values in columns.items():
        non_none = [v for v in values if v is not None]
        if non_none and all(isinstance(v, datetime) for v in non_none):
            arrays[name] = pa.array(
                [v.replace(tzinfo=None) if v.tzinfo else v for v in values],
                type=pa.timestamp("us"),
            )
        elif non_none and all(isinstance(v, str) for v in non_none):
            arrays[name] = pa.array(values, type=pa.string())
        else:
            arrays[name] = pa.array(values, type=pa.float64())
    table = pa.table(arrays)
    pq.write_table(table, str(partition_dir / "part-0001.parquet"), compression="zstd")


def _register_dataset(
    meta: MetaStore,
    *,
    dataset_id: str,
    source_id: str,
    canonical_type: str,
    revision_supported: bool,
) -> None:
    """登记 source_registry + dataset_registry 行（幂等）。"""
    meta.connection.execute(
        "INSERT OR IGNORE INTO source_registry"
        "(source_id, display_name, access_type, base_url, rate_limit_json, "
        "historical_limit_days, license, enabled) "
        "VALUES (?, ?, 'PUBLIC', 'https://example.com', '{}', NULL, 'public', 1)",
        (source_id, source_id),
    )
    meta.connection.execute(
        "INSERT OR IGNORE INTO dataset_registry"
        "(dataset_id, source_id, canonical_type, entity_id, params_json, frequency, "
        "continuity_model, status, revision_supported, enabled, created_at) "
        "VALUES (?, ?, ?, 'test', '{}', NULL, 'ALWAYS_OPEN', 'UNKNOWN', ?, 1, datetime('now'))",
        (dataset_id, source_id, canonical_type, int(revision_supported)),
    )
    meta.connection.commit()


def _record_success_run(
    meta: MetaStore,
    dataset_id: str,
    source_id: str,
    *,
    code_version: str = "0.1.0",
    schema_version: str = "1.0",
) -> None:
    """经 try_lock/finish_run 真实路径写一条 SUCCESS run（版本元信息事实源）。"""
    row = meta.try_lock_dataset(dataset_id, source_id=source_id)
    meta.finish_run(
        row.run_id,
        "SUCCESS",
        code_version=code_version,
        schema_version=schema_version,
    )


def _make_env(tmp_stores: Any) -> QueryEnv:
    """装配完整测试环境：meta 登记 → parquet → write 注册视图 → read_only 重开。"""
    meta = MetaStore(str(tmp_stores.meta_dir))
    meta.migrate()
    _register_dataset(
        meta, dataset_id=DATASET_NUMBER, source_id="FRED",
        canonical_type="NUMBER", revision_supported=True,
    )
    _register_dataset(
        meta, dataset_id=DATASET_OHLCV, source_id="BINANCE",
        canonical_type="OHLCV", revision_supported=False,
    )
    _register_dataset(
        meta, dataset_id=DATASET_NO_DATA, source_id="FRED",
        canonical_type="DOCUMENT", revision_supported=False,
    )
    _record_success_run(meta, DATASET_NUMBER, "FRED")

    _write_parquet(tmp_stores.data_dir, CanonicalType.NUMBER, _number_records())
    _write_parquet(tmp_stores.data_dir, CanonicalType.OHLCV, _ohlcv_records())

    catalog_path = tmp_stores.data_dir / "query.duckdb"
    con_write = duckdb.connect(str(catalog_path))
    register_views(con_write, str(tmp_stores.data_dir))
    # TC-R-003 专用：write 模式预建实体表，read_only 下 INSERT 的失败原因无歧义
    con_write.execute("CREATE TABLE sandbox(a INT)")
    con_write.close()

    con_ro = duckdb.connect(str(catalog_path), read_only=True)
    service = DuckDBQueryService(con_ro, DatasetRegistry(meta.connection))
    return QueryEnv(service=service, con=con_ro, meta=meta, data_dir=tmp_stores.data_dir)


# ── TC-R-001: as-of 两 vintage ──────────────────────────────────────────


class TestAsOfVintage:

    def test_asof_t1_returns_v1_stable_ordered(self, tmp_stores) -> None:
        """TC-R-001/GWT-1: Given 两 vintage When asof=T1 Then 仅 v1 且按 observation_time 升序。"""
        env = _make_env(tmp_stores)
        result = env.service.query(DATASET_NUMBER, asof=T1)

        # 仅 v1（release_time <= T1）可见：A(100)、C(300)、D(9)；v2(200) 不可见
        assert result.row_count == 3
        assert 200.0 not in result.frame["value"].to_list()
        # 默认排序：observation_time 升序（BASE < BASE+1day）
        obs = result.frame["observation_time"].to_list()
        assert obs == sorted(obs)
        # obs=BASE 并列两点按 nk 次列 source_id 排序：GDP(100) < UNRATE(9)
        assert result.frame["value"].to_list() == [100.0, 9.0, 300.0]

    def test_asof_t2_returns_v2(self, tmp_stores) -> None:
        """TC-R-001: asof=T2 → 同 nk 取最新 revision（B 替代 A）。"""
        env = _make_env(tmp_stores)
        result = env.service.query(DATASET_NUMBER, asof=T2)

        assert result.row_count == 3
        # obs=BASE 的 nk 点位取 v2（200），v1（100）被 row_number 过滤
        assert result.frame["value"].to_list() == [200.0, 9.0, 300.0]

    def test_asof_default_now_takes_latest_released(self, tmp_stores) -> None:
        """asof 缺省=now()：两 vintage 均已 release → 同 D07 §1 取最新 revision。"""
        env = _make_env(tmp_stores)
        result = env.service.query(DATASET_NUMBER)

        assert result.row_count == 3
        assert result.frame["value"].to_list() == [200.0, 9.0, 300.0]

    def test_asof_earliest_moment_empty(self, tmp_stores) -> None:
        """边界：asof=最早时刻（早于全部 release_time）→ 空结果。"""
        env = _make_env(tmp_stores)
        result = env.service.query(DATASET_NUMBER, asof=T1 - timedelta(seconds=1))

        assert result.row_count == 0
        assert result.frame.height == 0
        # 元信息仍恒附加
        assert result.dataset_id == DATASET_NUMBER
        assert result.dataset_version == "0.1.0+1.0"


# ── TC-R-002: SQL 注入防御 ──────────────────────────────────────────────


class TestInjectionDefense:

    def test_injection_in_filter_key_valueerror(self, tmp_stores) -> None:
        """TC-R-002/GWT-2: Given filters 含 \"; DROP\"（列名位）Then ValueError 且连接无恙。"""
        env = _make_env(tmp_stores)
        payload = 'x"; DROP TABLE number_asof; --'

        with pytest.raises(ValueError, match="Unknown column"):
            env.service.query(DATASET_NUMBER, asof=T1, filters={payload: "1"})

        # 连接无恙：as-of 视图仍可正常查询
        result = env.service.query(DATASET_NUMBER, asof=T1)
        assert result.row_count == 3

    def test_injection_in_filter_value_parameterized(self, tmp_stores) -> None:
        """TC-R-002: 注入串位于过滤值 → 参数化执行不报错、不产生副作用、零匹配。"""
        env = _make_env(tmp_stores)
        payload = "x'; DROP TABLE number_asof; --"

        result = env.service.query(
            DATASET_NUMBER, asof=T1, filters={"source_id": payload}
        )
        assert result.row_count == 0

        # 连接无恙 + 视图未被 DROP
        follow_up = env.service.query(DATASET_NUMBER, asof=T1)
        assert follow_up.row_count == 3


# ── TC-R-003: read_only 连接写失败 ──────────────────────────────────────


class TestReadOnly:

    def test_write_attempt_raises(self, tmp_stores) -> None:
        """TC-R-003: QueryService 连接（read_only）上尝试写 → 异常。"""
        env = _make_env(tmp_stores)
        with pytest.raises(duckdb.Error):
            env.service.con.execute("CREATE TABLE t_inject(a INT)")
        # 连接仍可用（只读查询不受影响）
        assert env.service.query(DATASET_OHLCV).row_count == 5

    def test_insert_attempt_raises(self, tmp_stores) -> None:
        """TC-R-003：对实体表 INSERT → 仅因 read_only 模式失败（write 模式可行）。"""
        env = _make_env(tmp_stores)
        with pytest.raises(duckdb.Error):
            env.service.con.execute("INSERT INTO sandbox VALUES (1)")


# ── 规则 1/4/5: 错误路径与白名单 ────────────────────────────────────────


class TestErrorsAndWhitelist:

    def test_unknown_dataset_id_valueerror(self, tmp_stores) -> None:
        """规则 1：未知 dataset_id → ValueError。"""
        env = _make_env(tmp_stores)
        with pytest.raises(ValueError, match="Dataset not found"):
            env.service.query("NOPE:MISSING")

    def test_unknown_filter_column_valueerror(self, tmp_stores) -> None:
        """规则 4：filters 未知列 → ValueError。"""
        env = _make_env(tmp_stores)
        with pytest.raises(ValueError, match="Unknown column"):
            env.service.query(DATASET_NUMBER, filters={"not_a_column": "x"})

    def test_unknown_projection_column_valueerror(self, tmp_stores) -> None:
        """规则 5：columns 投影未知列 → ValueError（同白名单防御）。"""
        env = _make_env(tmp_stores)
        with pytest.raises(ValueError, match="Unknown column"):
            env.service.query(DATASET_NUMBER, columns=["source_id", "evil_col"])

    def test_columns_projection(self, tmp_stores) -> None:
        """规则 5：合法 columns 投影仅返回所列列。"""
        env = _make_env(tmp_stores)
        result = env.service.query(DATASET_NUMBER, asof=T1, columns=["source_id", "value"])

        assert result.frame.columns == ["source_id", "value"]
        assert result.row_count == 3


# ── 规则 6: 默认稳定排序 ────────────────────────────────────────────────


class TestStableOrdering:

    def test_same_input_same_order_twice(self, tmp_stores) -> None:
        """默认排序：同输入两次查询结果顺序一致（确定性/可复现）。"""
        env = _make_env(tmp_stores)
        r1 = env.service.query(DATASET_NUMBER, asof=T2)
        r2 = env.service.query(DATASET_NUMBER, asof=T2)

        assert r1.frame.rows() == r2.frame.rows()

    def test_nk_tiebreak_ordering(self, tmp_stores) -> None:
        """规则 6：observation_time 并列时按 natural key 次列（source_id）稳定排序。

        asof=T2 可见三点：obs=BASE 的 GDP(200)/UNRATE(9) 与 obs=BASE+1d 的 GDP(300)。
        主时间列升序优先；obs=BASE 并列两点内 GDP < UNRATE（nk 次列）。
        """
        env = _make_env(tmp_stores)
        result = env.service.query(DATASET_NUMBER, asof=T2)

        assert result.frame["source_id"].to_list() == ["FRED:GDP", "FRED:UNRATE", "FRED:GDP"]
        assert result.frame["value"].to_list() == [200.0, 9.0, 300.0]

    def test_ordering_not_configurable(self, tmp_stores) -> None:
        """规则 6：排序不可关闭——即使 ORDER BY 语义下结果仍恒有序。"""
        env = _make_env(tmp_stores)
        result = env.service.query(DATASET_OHLCV)
        event_times = result.frame["event_time"].to_list()
        assert event_times == sorted(event_times)


# ── 规则 2: 时间列选择与非 revision 类型 ────────────────────────────────


class TestTimeColumnAndRange:

    def test_ohlcv_event_time_range_filter(self, tmp_stores) -> None:
        """规则 2：OHLCV 主时间列=event_time；start 含界/end 不含界。"""
        env = _make_env(tmp_stores)
        result = env.service.query(
            DATASET_OHLCV,
            start=BASE_TIME + timedelta(minutes=1),
            end=BASE_TIME + timedelta(minutes=3),
        )
        # [BASE+1m, BASE+3m) → 第 1、2 分钟两条
        assert result.row_count == 2
        assert result.frame["close"].to_list() == [50501.0, 50502.0]

    def test_empty_result_set(self, tmp_stores) -> None:
        """边界：空结果集（时间范围无匹配）→ row_count=0，元信息照常。"""
        env = _make_env(tmp_stores)
        result = env.service.query(
            DATASET_OHLCV, start=BASE_TIME + timedelta(days=365)
        )
        assert result.row_count == 0
        assert result.dataset_version == ""  # OHLCV 数据集无 SUCCESS run
        assert result.schema_version == "1.0"

    def test_naive_and_aware_datetime_equivalent(self, tmp_stores) -> None:
        """时间参数 naive 视为 UTC；tz-aware 规范化为 naive-UTC 后等价。"""
        env = _make_env(tmp_stores)
        start_naive = BASE_TIME + timedelta(minutes=1)
        start_aware = start_naive.replace(tzinfo=UTC)
        r1 = env.service.query(DATASET_OHLCV, start=start_naive)
        r2 = env.service.query(DATASET_OHLCV, start=start_aware)
        assert r1.frame.rows() == r2.frame.rows()


# ── 规则 5: 元信息恒附加 ────────────────────────────────────────────────


class TestMetadata:

    def test_metadata_from_success_run(self, tmp_stores) -> None:
        """dataset_version=最近 SUCCESS run 的 code+schema 复合；schema_version 同源。"""
        env = _make_env(tmp_stores)
        result = env.service.query(DATASET_NUMBER, asof=T1)

        assert result.dataset_id == DATASET_NUMBER
        assert result.dataset_version == "0.1.0+1.0"
        assert result.schema_version == "1.0"
        assert result.row_count == result.frame.height
        assert result.elapsed_ms >= 0

    def test_metadata_without_success_run(self, tmp_stores) -> None:
        """无 SUCCESS run → dataset_version 为空串、schema_version 回退 "1.0"（D02 §1）。"""
        env = _make_env(tmp_stores)
        result = env.service.query(DATASET_OHLCV)

        assert result.dataset_version == ""
        assert result.schema_version == "1.0"


# ── 边界: 视图未注册（数据未写入） ──────────────────────────────────────


class TestLazyView:

    def test_registered_dataset_without_data_returns_empty(self, tmp_stores) -> None:
        """已登记 dataset 无 parquet（STORAGE-004 惰性语义）→ 空结果不抛异常。"""
        env = _make_env(tmp_stores)
        result = env.service.query(DATASET_NO_DATA)

        assert result.row_count == 0
        assert result.frame.height == 0
        assert result.dataset_id == DATASET_NO_DATA

    def test_registered_dataset_without_data_filters_validated(self, tmp_stores) -> None:
        """视图未注册 → 无白名单可校验，空结果先于列校验返回。"""
        env = _make_env(tmp_stores)
        result = env.service.query(DATASET_NO_DATA, filters={"any_col": 1})
        assert result.row_count == 0
