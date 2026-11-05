"""跨存储一致性测试（STORAGE-005）。

依据：STORAGE-005.md + D03 §5
测试用例：
- TC-S-007: temp/p.old 孤儿清理
- cleanup: 模拟 crash 留下 p.tmp-xxx → cleanup_orphans 后目录恢复干净
- 边界: 全部 temp 无 rename 的极端态
- recovery: 启动即修复（startup_repair 完整路径）
- M2: 启动修复只做清理与恢复，不调用对账补记
"""

from __future__ import annotations

import os
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from chronoforge.models.enums import CanonicalType
from chronoforge.storage.canonical import CanonicalStoreImpl
from chronoforge.storage.consistency import cleanup_orphans, startup_repair
from chronoforge.storage.meta import MetaStore
from chronoforge.storage.raw import RawStore

# ── Helper: setup fixture data ─────────────────────────────────────


def _setup_source_and_dataset(store: MetaStore, source_id: str, dataset_id: str) -> None:
    """插入 source_registry 和 dataset_registry 记录。"""
    conn = store.connection
    conn.execute(
        "INSERT OR IGNORE INTO source_registry "
        "(source_id, display_name, access_type, base_url, "
        "rate_limit_json, license) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (source_id, "Test", "PUBLIC", "https://test.com", "{}", "MIT"),
    )
    conn.execute(
        "INSERT OR IGNORE INTO dataset_registry "
        "(dataset_id, source_id, canonical_type, entity_id, "
        "params_json, continuity_model, status, "
        "created_at, revision_supported) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (dataset_id, source_id, "OHLCV", "BTCUSDT", "{}",
         "ALWAYS_OPEN", "UNKNOWN", "2024-01-01", 0),
    )
    conn.commit()


def _insert_run_log(
    store: MetaStore, run_id: str, dataset_id: str,
    source_id: str, status: str, ingest_batch_id: str | None = None,
    checkpoint_before: str | None = None,
    checkpoint_after: str | None = None,
) -> None:
    """插入一条 run_log 记录。"""
    store.connection.execute(
        "INSERT INTO run_log "
        "(run_id, source_id, dataset_id, status, started_at, "
        "schema_version, code_version, ingest_batch_id, "
        "checkpoint_before, checkpoint_after, ended_at) "
        "VALUES (?, ?, ?, ?, datetime('now'), '', '', ?, ?, ?, "
        "CASE WHEN ? IS NOT NULL THEN datetime('now') ELSE NULL END)",
        (run_id, source_id, dataset_id, status,
         ingest_batch_id or "", checkpoint_before, checkpoint_after,
         checkpoint_after),
    )
    store.connection.commit()


def _append_raw_batch(
    raw_store: RawStore, source_id: str, dataset_id: str, batch_id: str
) -> None:
    """向 raw 层写入一条携带指定 ingest_batch_id 的记录（真实 RawStore 路径）。"""
    raw_store.append(source_id, dataset_id, [{
        "url": f"https://test.example/{dataset_id}",
        "payload": b'{"test": "data"}',
        "fetched_at": datetime.now(UTC).replace(tzinfo=None),
        "ingest_batch_id": batch_id,
        "ingest_timestamp": datetime.now(UTC).replace(tzinfo=None),
    }])


# ── TC-S-007: cleanup_orphans ─────────────────────────────────────


class TestCleanupOrphans:
    """TC-S-007: temp/p.old 孤儿清理

    审计 C-4：.tmp-*/.old-* 实际创建于 year 层（part_path.parent），
    即 month=* 分区目录的同级。测试布局与实现布局保持一致。
    """

    def test_cleanup_removes_tmp_dirs(self, tmp_stores) -> None:
        """Given 真实分区 year 层遗留 .tmp-xxx When cleanup_orphans Then 孤儿清除、分区完好"""
        data_dir = str(tmp_stores.data_dir)
        store = CanonicalStoreImpl(data_dir)

        # 通过真实 upsert 建立权威分区布局
        et = datetime(2026, 9, 11, 10, 0, 0)
        record = {
            "schema_version": "1.0",
            "source": "test_source",
            "source_id": "btcusdt",
            "source_timestamp": et,
            "ingest_timestamp": et,
            "raw_record_id": "test_source:1",
            "market_id": "BINANCE:BTCUSDT:SPOT",
            "event_time": et,
            "interval": "1m",
            "open": 1.0,
            "high": 1.0,
            "low": 1.0,
            "close": 1.0,
            "volume": 1.0,
        }
        store.upsert([record], CanonicalType.OHLCV, "BTC")

        # 模拟 crash：在 year 层（part_path.parent）遗留 .tmp-* 孤儿
        part_path = store._partition_path(CanonicalType.OHLCV, "BTC", "2026", "09")
        year_dir = part_path.parent
        tmp_orphan = year_dir / ".tmp-abc123"
        tmp_orphan.mkdir()
        (tmp_orphan / "part-0001.parquet").write_text("dummy")

        orphans = cleanup_orphans(data_dir)
        assert len(orphans) == 1
        assert str(tmp_orphan) in orphans
        assert not tmp_orphan.exists()

        # 正常分区数据不受影响
        part_files = list(part_path.glob("part-*.parquet"))
        assert len(part_files) == 1
        assert part_files[0].exists()

    def test_cleanup_removes_old_dirs(self, tmp_stores) -> None:
        """Given year 层 .old-* 孤儿 When cleanup_orphans Then 目录被删除"""
        data_dir = str(tmp_stores.data_dir)
        year_dir = Path(data_dir) / "canonical" / "OHLCV" / "entity=BTC" / "year=2026"
        year_dir.mkdir(parents=True)
        # month 分区为孤儿的同级目录
        month_dir = year_dir / "month=01"
        month_dir.mkdir()
        (month_dir / "part-0000.parquet").write_text("dummy")

        old_orphan = year_dir / ".old-def456"
        old_orphan.mkdir()
        (old_orphan / "part-0001.parquet").write_text("dummy")

        orphans = cleanup_orphans(data_dir)
        assert len(orphans) == 1
        assert not old_orphan.exists()
        # month 分区不受影响
        assert (month_dir / "part-0000.parquet").exists()

    def test_cleanup_preserves_parquet_files(self, tmp_stores) -> None:
        """Given 正常 part-*.parquet 文件 When cleanup_orphans Then 文件不被删除"""
        data_dir = str(tmp_stores.data_dir)
        canonical_dir = (
            Path(data_dir) / "canonical" / "OHLCV" / "entity=BTC" / "year=2026" / "month=01"
        )
        canonical_dir.mkdir(parents=True)

        normal_file = canonical_dir / "part-0001.parquet"
        normal_file.write_text("normal data")

        orphans = cleanup_orphans(data_dir)
        assert len(orphans) == 0
        assert normal_file.exists()
        assert normal_file.read_text() == "normal data"

    def test_cleanup_idempotent(self, tmp_stores) -> None:
        """cleanup_orphans 幂等：多次调用不报错"""
        data_dir = str(tmp_stores.data_dir)
        year_dir = Path(data_dir) / "canonical" / "OHLCV" / "entity=BTC" / "year=2026"
        year_dir.mkdir(parents=True)

        tmp_orphan = year_dir / ".tmp-abc123"
        tmp_orphan.mkdir()

        # 第一次调用
        orphans1 = cleanup_orphans(data_dir)
        assert len(orphans1) == 1

        # 第二次调用（孤儿已删除）
        orphans2 = cleanup_orphans(data_dir)
        assert len(orphans2) == 0

    def test_cleanup_no_canonical_dir(self, tmp_stores) -> None:
        """Given 无 canonical 目录 When cleanup_orphans Then 返回空列表"""
        data_dir = str(tmp_stores.data_dir)
        orphans = cleanup_orphans(data_dir)
        assert orphans == []


# ── startup_repair 完整路径 ──────────────────────────────────────


class TestStartupRepair:
    """recovery: 启动即修复（startup_repair 完整路径）"""

    def test_startup_repair_full_path(self, tmp_stores) -> None:
        """Given 超龄孤儿目录 + 超时孤儿 run（raw 已落盘）When startup_repair
        Then cleanup + 释放超时锁执行：孤儿目录清除、孤儿 run 置 CANCELLED。

        R2-03 护栏语义：cleanup_orphans 需目录年龄超 stale_run_timeout_seconds；
        超时孤儿 run 由 release_stale_locks 先行置 CANCELLED（终态，审计 M2：
        启动路径不再对账补记 SUCCESS，数据可经 pipeline replay 重建）。
        """
        data_dir = str(tmp_stores.data_dir)
        meta_dir = str(tmp_stores.meta_dir)

        # 设置孤儿目录（审计 C-4：孤儿位于 year 层，month 分区的同级）
        canonical_dir = (
            Path(data_dir) / "canonical" / "OHLCV" / "entity=BTC" / "year=2026" / "month=01"
        )
        canonical_dir.mkdir(parents=True)
        year_dir = canonical_dir.parent
        tmp_orphan = year_dir / ".tmp-abc123"
        tmp_orphan.mkdir()
        old_orphan = year_dir / ".old-def456"
        old_orphan.mkdir()
        # R2-03① 年龄护栏：回拨 mtime 2 小时（超默认 3600s）才会被清理
        stale_ts = time.time() - 2 * 3600
        for orphan in (tmp_orphan, old_orphan):
            os.utime(orphan, (stale_ts, stale_ts))

        with MetaStore(meta_dir) as meta:
            meta.migrate()
            _setup_source_and_dataset(meta, "test_source", "test_dataset")

            # 模拟崩溃孤儿 run：数据已落盘（raw），终态未写；
            # started_at 回拨 2 小时 → 超 release_stale_locks 租约
            row = meta.try_lock_dataset("test_dataset", source_id="test_source")
            stale_start = (
                datetime.now(UTC) - timedelta(hours=2)
            ).replace(tzinfo=None).isoformat() + "Z"
            meta.connection.execute(
                "UPDATE run_log SET started_at = ? WHERE run_id = ?",
                (stale_start, row.run_id),
            )
            meta.connection.commit()
            raw_store = RawStore(data_dir)
            _append_raw_batch(raw_store, "test_source", "test_dataset", row.ingest_batch_id)

            # 执行启动修复
            startup_repair(meta, data_dir)

            # 验证超龄孤儿已清理
            assert not tmp_orphan.exists()
            assert not old_orphan.exists()

            # 验证超时孤儿锁已释放（H-7：置 CANCELLED 终态，数据集解除死锁）
            cursor = meta.connection.execute(
                "SELECT status, source_id FROM run_log WHERE run_id = ?",
                (row.run_id,),
            )
            db_row = cursor.fetchone()
            assert db_row is not None
            assert db_row[0] == "CANCELLED"
            assert db_row[1] == "test_source"

    def test_startup_repair_no_orphans_keeps_run_log(self, tmp_stores) -> None:
        """Given 无孤儿且全部 run 已终态 When startup_repair Then 无异常且 run_log 不变"""
        data_dir = str(tmp_stores.data_dir)
        meta_dir = str(tmp_stores.meta_dir)

        # 创建干净的 canonical 目录
        canonical_dir = (
            Path(data_dir) / "canonical" / "OHLCV" / "entity=BTC" / "year=2026" / "month=01"
        )
        canonical_dir.mkdir(parents=True)
        (canonical_dir / "part-0001.parquet").write_text("normal")

        with MetaStore(meta_dir) as meta:
            meta.migrate()
            _setup_source_and_dataset(meta, "test_source", "test_dataset")

            _insert_run_log(
                meta, "run_001", "test_dataset", "test_source", "SUCCESS",
                ingest_batch_id="batch_001", checkpoint_after="cursor",
            )
            count_before = meta.connection.execute(
                "SELECT COUNT(*) FROM run_log"
            ).fetchone()[0]

            # 应不报错且不新增行
            startup_repair(meta, data_dir)
            count_after = meta.connection.execute(
                "SELECT COUNT(*) FROM run_log"
            ).fetchone()[0]
            assert count_after == count_before

    def test_startup_repair_does_not_invoke_reconcile(
        self, tmp_stores, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """M2：启动修复只做清理与恢复，不做对账补记。

        审计 M2：release_stale_locks 用同一 cutoff 先把超时 RUNNING/PENDING
        置 CANCELLED，紧随其后的 reconcile 再查非终态行恒空（死代码）。
        修复前本用例红：reconcile 仍被 startup_repair 调用。
        """
        data_dir = str(tmp_stores.data_dir)
        meta_dir = str(tmp_stores.meta_dir)
        calls: list[int] = []

        def _spy(*args: object, **kwargs: object) -> list[dict[str, object]]:
            calls.append(1)
            return []

        monkeypatch.setattr(
            "chronoforge.storage.consistency.reconcile", _spy, raising=False
        )

        with MetaStore(meta_dir) as meta:
            meta.migrate()
            _setup_source_and_dataset(meta, "test_source", "test_dataset")
            row = meta.try_lock_dataset("test_dataset", source_id="test_source")
            stale_start = (
                datetime.now(UTC) - timedelta(hours=2)
            ).replace(tzinfo=None).isoformat() + "Z"
            meta.connection.execute(
                "UPDATE run_log SET started_at = ? WHERE run_id = ?",
                (stale_start, row.run_id),
            )
            meta.connection.commit()

            raw_store = RawStore(data_dir)
            _append_raw_batch(
                raw_store, "test_source", "test_dataset", row.ingest_batch_id
            )
            startup_repair(
                meta, data_dir, stale_run_timeout_seconds=3600.0,
            )

        # 启动路径不得再调用对账逻辑
        assert calls == []

    def test_startup_repair_releases_stale_locks(self, tmp_stores) -> None:
        """审计 H-7：startup_repair 第 0 步释放超时孤儿锁 → 数据集解除死锁"""
        data_dir = str(tmp_stores.data_dir)
        meta_dir = str(tmp_stores.meta_dir)

        with MetaStore(meta_dir) as meta:
            meta.migrate()
            _setup_source_and_dataset(meta, "test_source", "test_dataset")

            # 模拟崩溃残留的 PENDING 行（started_at 回拨 2 小时）
            row = meta.try_lock_dataset("test_dataset")
            stale_start = (
                datetime.now(UTC) - timedelta(hours=2)
            ).replace(tzinfo=None).isoformat() + "Z"
            meta.connection.execute(
                "UPDATE run_log SET started_at = ? WHERE run_id = ?",
                (stale_start, row.run_id),
            )
            meta.connection.commit()

            startup_repair(meta, data_dir)

            # 孤儿 run 已置终态
            status = meta.connection.execute(
                "SELECT status FROM run_log WHERE run_id = ?", (row.run_id,)
            ).fetchone()[0]
            assert status == "CANCELLED"

            # 死锁解除：可重新加锁
            new_row = meta.try_lock_dataset("test_dataset")
            assert new_row.run_id != row.run_id


# ── GWT acceptance tests ─────────────────────────────────────────


class TestGWT:

    def test_given_crash_leaves_tmp_when_cleanup_orphans_then_dirs_clean(
        self, tmp_stores
    ) -> None:
        """Given crash 留下 p.tmp-xxx When cleanup_orphans Then 目录干净且 canonical 完整"""
        data_dir = str(tmp_stores.data_dir)
        canonical_dir = (
            Path(data_dir) / "canonical" / "OHLCV" / "entity=BTC" / "year=2026" / "month=01"
        )
        canonical_dir.mkdir(parents=True)

        # crash 遗留（审计 C-4：.tmp-* 位于 year 层）
        tmp_dir = canonical_dir.parent / ".tmp-crash"
        tmp_dir.mkdir()
        (tmp_dir / "part-0001.parquet").write_text("temp data")

        # 正常数据
        normal_file = canonical_dir / "part-0002.parquet"
        normal_file.write_text("normal data")

        orphans = cleanup_orphans(data_dir)
        assert len(orphans) == 1
        assert not tmp_dir.exists()
        assert normal_file.exists()


# ── R2-03: startup_repair 并发护栏（审计 2026-09-22）──────────────


class TestStartupRepairGuards:
    """R2-03：启动修复的租约护栏。

    防止 startup_repair（每写命令执行）与在途 run 重叠时误伤：
    新鲜 .tmp-*（在途 merge-rewrite 分片）不删，租约内活跃 run
    不被置为 CANCELLED。
    """

    @staticmethod
    def _make_tmp_orphan(data_dir: str, name: str) -> Path:
        year_dir = Path(data_dir) / "canonical" / "OHLCV" / "entity=BTC" / "year=2026"
        year_dir.mkdir(parents=True, exist_ok=True)
        tmp_orphan = year_dir / name
        tmp_orphan.mkdir()
        (tmp_orphan / "part-0001.parquet").write_text("in-flight")
        return tmp_orphan

    def test_cleanup_orphans_min_age_skips_fresh_tmp(self, tmp_stores) -> None:
        """Given 新鲜 .tmp-* When cleanup_orphans(min_age_seconds=3600)
        Then 跳过；mtime 回拨 2 小时后正常清理。"""
        data_dir = str(tmp_stores.data_dir)
        tmp_orphan = self._make_tmp_orphan(data_dir, ".tmp-fresh123")

        assert cleanup_orphans(data_dir, min_age_seconds=3600.0) == []
        assert tmp_orphan.exists()

        old_time = time.time() - 2 * 3600
        os.utime(tmp_orphan, (old_time, old_time))
        orphans = cleanup_orphans(data_dir, min_age_seconds=3600.0)
        assert len(orphans) == 1
        assert not tmp_orphan.exists()

    def test_cleanup_orphans_min_age_zero_keeps_old_behavior(self, tmp_stores) -> None:
        """min_age_seconds=0（默认）保持旧行为：新鲜孤儿立即清理。"""
        data_dir = str(tmp_stores.data_dir)
        tmp_orphan = self._make_tmp_orphan(data_dir, ".tmp-justnow")

        orphans = cleanup_orphans(data_dir)
        assert len(orphans) == 1
        assert not tmp_orphan.exists()

    def test_startup_repair_passes_timeout_to_guards(self, tmp_stores) -> None:
        """startup_repair 将 stale_run_timeout_seconds 传给清理与锁释放：
        新鲜 .tmp-* 不删、活跃 run 不被置为 CANCELLED（在途 run 不被误伤）。"""
        data_dir = str(tmp_stores.data_dir)
        meta_dir = str(tmp_stores.meta_dir)
        tmp_orphan = self._make_tmp_orphan(data_dir, ".tmp-inflight")

        with MetaStore(meta_dir) as meta:
            meta.migrate()
            _setup_source_and_dataset(meta, "test_source", "test_dataset")
            row = meta.try_lock_dataset("test_dataset", source_id="test_source")

            startup_repair(
                meta, data_dir, stale_run_timeout_seconds=3600.0,
            )

            assert tmp_orphan.exists()  # 新鲜孤儿未删
            status = meta.connection.execute(
                "SELECT status FROM run_log WHERE run_id = ?", (row.run_id,)
            ).fetchone()[0]
            assert status == "PENDING"  # 活跃 run 未被释放
