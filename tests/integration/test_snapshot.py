"""QUERY-003 集成测试 — ResearchSnapshot 与复现（D07 §3）。

依据：
- D07 §3 research_snapshot DDL + contextmanager + reproduce
- QUERY-003.md 测试要求（TC-R-004 + 边界空 datasets + 失败报告版本变化）

测试环境：SQLite meta（迁移含 0003 research_snapshot）登记 dataset_registry/
run_log；canonical parquet 手工写入（同 test_query.py 惯例）；DuckDB catalog
先 write 注册视图（STORAGE-004），再 read_only 重开供 QueryService 使用。
数据更新通过向分区目录追加 parquet 文件实现（视图 per-query glob，无需重注册）。
"""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from chronoforge import __version__
from chronoforge.models.enums import CanonicalType
from chronoforge.research import (
    DatasetRegistry,
    DuckDBQueryService,
    get_snapshot,
    research_snapshot,
    snapshot_reproduce,
)
from chronoforge.storage.meta import MetaStore
from chronoforge.storage.views import register_views

# ── 固定基准时刻 ────────────────────────────────────────────────────────

BASE_TIME = datetime(2026, 9, 11, 10, 0, 0)

DATASET_OHLCV = "BINANCE:BTCUSDT:OHLCV"    # 有 SUCCESS run（0.1.0+1.0）+ 5 行数据
DATASET_NO_RUN = "BINANCE:ETHUSDT:OHLCV"   # 已登记但无 run 无数据（锁定版本 ""）


# ── 测试环境装配 ────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SnapshotEnv:
    """测试环境：query service + meta 句柄（meta 保持存活维持 SQLite 连接）。"""

    qs: DuckDBQueryService
    meta: MetaStore
    data_dir: Path


def _ohlcv_records(offset_minutes: range) -> list[dict[str, Any]]:
    """OHLCV 记录（event_time = BASE + i 分钟，nk 次列 market_id/interval）。"""
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
        for i in offset_minutes
    ]


def _write_parquet(
    data_dir: Path,
    records: list[dict[str, Any]],
    *,
    filename: str = "part-0001.parquet",
) -> None:
    """records 写为 canonical/OHLCV/entity=…/year=…/month=…/{filename}。"""
    obs = records[0]["event_time"]
    entity = records[0]["source_id"]
    partition_dir = (
        data_dir / "canonical" / CanonicalType.OHLCV.value
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
    pq.write_table(
        pa.table(arrays), str(partition_dir / filename), compression="zstd"
    )


def _register_dataset(
    meta: MetaStore, *, dataset_id: str, source_id: str
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
        "VALUES (?, ?, 'OHLCV', 'test', '{}', NULL, 'ALWAYS_OPEN', 'UNKNOWN', 0, 1, "
        "datetime('now'))",
        (dataset_id, source_id),
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
        row.run_id, "SUCCESS", code_version=code_version, schema_version=schema_version
    )


def _make_env(tmp_stores: Any) -> SnapshotEnv:
    """装配环境：meta 迁移 → 登记 → parquet → write 注册视图 → read_only 重开。"""
    meta = MetaStore(str(tmp_stores.meta_dir))
    meta.migrate()
    _register_dataset(meta, dataset_id=DATASET_OHLCV, source_id="BINANCE")
    _register_dataset(meta, dataset_id=DATASET_NO_RUN, source_id="BINANCE")
    _record_success_run(meta, DATASET_OHLCV, "BINANCE")

    _write_parquet(tmp_stores.data_dir, _ohlcv_records(range(5)))

    catalog_path = tmp_stores.data_dir / "query.duckdb"
    con_write = duckdb.connect(str(catalog_path))
    register_views(con_write, str(tmp_stores.data_dir))
    con_write.close()

    con_ro = duckdb.connect(str(catalog_path), read_only=True)
    qs = DuckDBQueryService(con_ro, DatasetRegistry(meta.connection))
    return SnapshotEnv(qs=qs, meta=meta, data_dir=tmp_stores.data_dir)


def _snapshot_row(meta: MetaStore, snapshot_id: str) -> sqlite3.Row:
    """读取 research_snapshot 原始行。"""
    meta.connection.row_factory = sqlite3.Row
    row = meta.connection.execute(
        "SELECT * FROM research_snapshot WHERE snapshot_id = ?", (snapshot_id,)
    ).fetchone()
    assert row is not None
    return row


def _query_ohlcv(qs: DuckDBQueryService) -> pl.DataFrame:
    """研究查询函数：OHLCV 全量（query 层默认稳定排序）。"""
    return qs.query(DATASET_OHLCV).frame


# ── 迁移 0003：research_snapshot 表 ─────────────────────────────────────


class TestMigration:

    def test_research_snapshot_table_created(self, tmp_stores) -> None:
        """Given 空 meta_dir When migrate() Then research_snapshot 表按 DDL 创建。"""
        meta = MetaStore(str(tmp_stores.meta_dir))
        meta.migrate()
        columns = {
            row[1]: row[2]
            for row in meta.connection.execute(
                "PRAGMA table_info(research_snapshot)"
            ).fetchall()
        }
        assert columns == {
            "snapshot_id": "TEXT",
            "created_at": "TEXT",
            "datasets_json": "TEXT",
            "code_version": "TEXT",
            "params_json": "TEXT",
            "output_hash": "TEXT",
            "notebook_ref": "TEXT",
            "query_text": "TEXT",
        }

    def test_migration_idempotent_and_registered(self, tmp_stores) -> None:
        """Given 已迁移库 When 再 migrate() Then 幂等且 schema_versions 仅一条 0003。"""
        for _ in range(2):
            meta = MetaStore(str(tmp_stores.meta_dir))
            meta.migrate()
            meta.close()

        meta = MetaStore(str(tmp_stores.meta_dir))
        count = meta.connection.execute(
            "SELECT COUNT(*) FROM schema_versions WHERE version = '0003'"
        ).fetchone()[0]
        assert count == 1


# ── 进入：锁定数据集版本 ────────────────────────────────────────────────


class TestSnapshotEntry:

    def test_empty_datasets_valueerror(self, tmp_stores) -> None:
        """边界：空 datasets → ValueError（进入即拒绝）。"""
        env = _make_env(tmp_stores)
        with pytest.raises(ValueError, match="datasets must not be empty"):
            with research_snapshot(env.qs, [], {}, env.meta):
                pass

    def test_unknown_dataset_valueerror(self, tmp_stores) -> None:
        """未登记 dataset_id → ValueError（进入即拒绝，无副作用）。"""
        env = _make_env(tmp_stores)
        with pytest.raises(ValueError, match="Dataset not found"):
            with research_snapshot(env.qs, ["NOPE:MISSING:OHLCV"], {}, env.meta):
                pass

    def test_entry_locks_dataset_versions(self, tmp_stores) -> None:
        """进入锁定各 dataset 当前 dataset_version（最近 SUCCESS run 复合）。"""
        env = _make_env(tmp_stores)
        with research_snapshot(env.qs, [DATASET_OHLCV, DATASET_NO_RUN], {}, env.meta) as ctx:
            assert ctx.datasets == [
                {"dataset_id": DATASET_OHLCV, "dataset_version": "0.1.0+1.0"},
                {"dataset_id": DATASET_NO_RUN, "dataset_version": ""},
            ]


# ── compute：落库与不可变 ───────────────────────────────────────────────


class TestCompute:

    def test_compute_persists_record(self, tmp_stores) -> None:
        """compute 执行研究并落库：字段完整、参数按 key 排序、hash 64 位 hex。"""
        env = _make_env(tmp_stores)
        params = {"window": 20, "asset": "BTC"}
        with research_snapshot(
            env.qs, [DATASET_OHLCV], params, env.meta
        ) as ctx:
            ctx.notebook_ref = "nb/research-01.ipynb"
            ctx.query_text = "SELECT 1"
            result = ctx.compute(_query_ohlcv)

        assert result.snapshot_id == ctx.snapshot_id
        assert re.fullmatch(r"[0-9a-f]{64}", result.output_hash)
        assert result.datasets == [
            {"dataset_id": DATASET_OHLCV, "dataset_version": "0.1.0+1.0"}
        ]
        assert result.params == params

        row = _snapshot_row(env.meta, ctx.snapshot_id)
        assert row["code_version"] == __version__
        assert json.loads(row["datasets_json"]) == ctx.datasets
        assert json.loads(row["params_json"]) == params
        # params_json 按 key 排序（确定性序列化）
        assert row["params_json"] == json.dumps(params, sort_keys=True)
        assert row["output_hash"] == result.output_hash
        assert row["notebook_ref"] == "nb/research-01.ipynb"
        assert row["query_text"] == "SELECT 1"
        # 落库记录经 get_snapshot 读回一致
        record = get_snapshot(env.meta, ctx.snapshot_id)
        assert record is not None
        assert record.output_hash == result.output_hash
        assert record.params == params

    def test_compute_twice_rejected(self, tmp_stores) -> None:
        """快照不可变：同一 snapshot 第二次 compute → ValueError，记录不重复。"""
        env = _make_env(tmp_stores)
        with research_snapshot(env.qs, [DATASET_OHLCV], {}, env.meta) as ctx:
            ctx.compute(_query_ohlcv)
            with pytest.raises(ValueError, match="already exists"):
                ctx.compute(_query_ohlcv)

        count = env.meta.connection.execute(
            "SELECT COUNT(*) FROM research_snapshot"
        ).fetchone()[0]
        assert count == 1

    def test_compute_on_empty_view(self, tmp_stores) -> None:
        """边界：无数据视图（空结果）→ 空帧 hash 确定且可复现。"""
        env = _make_env(tmp_stores)
        with research_snapshot(env.qs, [DATASET_NO_RUN], {}, env.meta) as ctx:
            result = ctx.compute(lambda qs: qs.query(DATASET_NO_RUN).frame)

        assert re.fullmatch(r"[0-9a-f]{64}", result.output_hash)
        reproduced = snapshot_reproduce(
            ctx.snapshot_id, env.meta, env.qs, lambda qs: qs.query(DATASET_NO_RUN).frame
        )
        assert reproduced.hash_match is True


# ── TC-R-004：复现 ──────────────────────────────────────────────────────


class TestReproduce:

    def test_same_snapshot_reproduce_hash_consistent(self, tmp_stores) -> None:
        """TC-R-004/GWT-1: Given 同 snapshot 两次 reproduce Then hash 一致。"""
        env = _make_env(tmp_stores)
        with research_snapshot(env.qs, [DATASET_OHLCV], {"w": 20}, env.meta) as ctx:
            created = ctx.compute(_query_ohlcv)

        r1 = snapshot_reproduce(ctx.snapshot_id, env.meta, env.qs, _query_ohlcv)
        r2 = snapshot_reproduce(ctx.snapshot_id, env.meta, env.qs, _query_ohlcv)

        assert r1.hash_match is True
        assert r2.hash_match is True
        assert r1.new_hash == created.output_hash
        assert r2.new_hash == created.output_hash
        assert r1.original_hash == created.output_hash
        assert r1.version_changes is None
        assert r2.version_changes is None
        assert r1.snapshot_id == r2.snapshot_id == ctx.snapshot_id

    def test_reproduce_after_data_update_reports_version_change(
        self, tmp_stores
    ) -> None:
        """TC-R-004/GWT-2: Given 数据更新后 reproduce Then hash 不一致且报告版本变化。"""
        env = _make_env(tmp_stores)
        with research_snapshot(env.qs, [DATASET_OHLCV], {}, env.meta) as ctx:
            created = ctx.compute(_query_ohlcv)

        # 数据更新：追加一条新 event_time 行（视图 per-query glob，无需重注册）
        _write_parquet(
            env.data_dir, _ohlcv_records(range(5, 6)), filename="part-0002.parquet"
        )
        # 版本推进：新 SUCCESS run（code_version 0.1.1）
        _record_success_run(env.meta, DATASET_OHLCV, "BINANCE", code_version="0.1.1")

        result = snapshot_reproduce(ctx.snapshot_id, env.meta, env.qs, _query_ohlcv)

        assert result.hash_match is False
        assert result.new_hash != created.output_hash
        assert result.original_hash == created.output_hash
        assert result.version_changes == {
            DATASET_OHLCV: {
                "snapshot_version": "0.1.0+1.0",
                "current_version": "0.1.1+1.0",
            }
        }

    def test_version_bump_alone_keeps_hash_match(self, tmp_stores) -> None:
        """仅版本推进、数据未变 → hash 一致且 version_changes 不报告。"""
        env = _make_env(tmp_stores)
        with research_snapshot(env.qs, [DATASET_OHLCV], {}, env.meta) as ctx:
            ctx.compute(_query_ohlcv)

        _record_success_run(env.meta, DATASET_OHLCV, "BINANCE", code_version="0.1.1")
        result = snapshot_reproduce(ctx.snapshot_id, env.meta, env.qs, _query_ohlcv)

        assert result.hash_match is True
        assert result.version_changes is None

    def test_snapshot_not_found_valueerror(self, tmp_stores) -> None:
        """失败路径：未知 snapshot_id → ValueError。"""
        env = _make_env(tmp_stores)
        with pytest.raises(ValueError, match="Snapshot not found"):
            snapshot_reproduce("deadbeef0000", env.meta, env.qs, _query_ohlcv)
