"""跨存储一致性测试（STORAGE-005）。

依据：STORAGE-005.md + D03 §5
测试用例：
- TC-S-004: 孤儿 run（数据落盘但终态写失败）→ reconciliation 补记
- TC-S-007: temp/p.old 孤儿清理
- cleanup: 模拟 crash 留下 p.tmp-xxx → cleanup_orphans 后目录恢复干净
- reconcile: raw 层有对应 ingest_batch_id 数据 → 补记 SUCCESS（保留 lineage）
- reconcile: raw 层无数据 → 补记 FAILED（保留 lineage）
- reconcile: 无孤儿 run → 不产生任何伪造行
- 边界: 全部 temp 无 rename 的极端态
- recovery: 启动即修复（startup_repair 完整路径）
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from chronoforge.models.enums import CanonicalType
from chronoforge.storage.canonical import CanonicalStoreImpl
from chronoforge.storage.consistency import (
    cleanup_orphans,
    reconcile,
    startup_repair,
)
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


# ── TC-S-004: reconcile ──────────────────────────────────────────


class TestReconcile:
    """TC-S-004: 孤儿 run（数据落盘但终态写失败）→ reconciliation 补记。

    审计 H-5：对账源 = run_log 孤儿行（PENDING/RUNNING），匹配键 =
    ingest_batch_id（try_lock_dataset 写入 = run_id），判定依据 =
    raw 层 JSONL 逐行携带的 ingest_batch_id。
    """

    def test_reconcile_orphan_run_raw_landed_then_success(self, tmp_stores) -> None:
        """Given 孤儿 PENDING run 且 raw 层有对应 batch When reconcile
        Then 补记 SUCCESS 且 source_id/dataset_id lineage 保留"""
        data_dir = str(tmp_stores.data_dir)
        meta_dir = str(tmp_stores.meta_dir)

        with MetaStore(meta_dir) as meta:
            meta.migrate()
            _setup_source_and_dataset(meta, "test_source", "test_dataset")

            # 模拟崩溃：try_lock 后未 finish_run（PENDING 孤儿行）
            row = meta.try_lock_dataset("test_dataset", source_id="test_source")
            assert row.status == "PENDING"

            # 数据已落盘（raw 行携带 ingest_batch_id = run_id）
            raw_store = RawStore(data_dir)
            _append_raw_batch(raw_store, "test_source", "test_dataset", row.ingest_batch_id)

            canonical_store = CanonicalStoreImpl(data_dir)
            reconciled = reconcile(meta, raw_store, canonical_store, data_dir)

            assert len(reconciled) == 1
            assert reconciled[0]["run_id"] == row.run_id
            assert reconciled[0]["status"] == "SUCCESS"
            assert reconciled[0]["error_summary"] is None

            # lineage 保留（H-5 核心：禁止伪造 source_id=''/dataset_id='' 行）
            db_row = meta.connection.execute(
                "SELECT status, source_id, dataset_id, ingest_batch_id "
                "FROM run_log WHERE run_id = ?",
                (row.run_id,),
            ).fetchone()
            assert db_row[0] == "SUCCESS"
            assert db_row[1] == "test_source"
            assert db_row[2] == "test_dataset"
            assert db_row[3] == row.ingest_batch_id

    def test_reconcile_orphan_run_raw_missing_then_failed(self, tmp_stores) -> None:
        """Given 孤儿 PENDING run 且 raw 层无对应 batch When reconcile
        Then 补记 FAILED（no raw data found for batch）且 lineage 保留"""
        data_dir = str(tmp_stores.data_dir)
        meta_dir = str(tmp_stores.meta_dir)

        with MetaStore(meta_dir) as meta:
            meta.migrate()
            _setup_source_and_dataset(meta, "test_source", "test_dataset")

            row = meta.try_lock_dataset("test_dataset", source_id="test_source")

            raw_store = RawStore(data_dir)
            canonical_store = CanonicalStoreImpl(data_dir)
            reconciled = reconcile(meta, raw_store, canonical_store, data_dir)

            assert len(reconciled) == 1
            assert reconciled[0]["status"] == "FAILED"
            assert reconciled[0]["error_summary"] == "no raw data found for batch"

            db_row = meta.connection.execute(
                "SELECT status, source_id, dataset_id FROM run_log WHERE run_id = ?",
                (row.run_id,),
            ).fetchone()
            assert db_row[0] == "FAILED"
            assert db_row[1] == "test_source"
            assert db_row[2] == "test_dataset"

    def test_reconcile_orphan_run_missing_batch_id_then_failed(self, tmp_stores) -> None:
        """Given 孤儿 run 无 ingest_batch_id（legacy 行）When reconcile
        Then 补记 FAILED（orphaned run: missing ingest_batch_id）"""
        data_dir = str(tmp_stores.data_dir)
        meta_dir = str(tmp_stores.meta_dir)

        with MetaStore(meta_dir) as meta:
            meta.migrate()
            _setup_source_and_dataset(meta, "test_source", "test_dataset")

            _insert_run_log(
                meta, "run_legacy", "test_dataset", "test_source", "PENDING",
                ingest_batch_id="",
            )

            raw_store = RawStore(data_dir)
            canonical_store = CanonicalStoreImpl(data_dir)
            reconciled = reconcile(meta, raw_store, canonical_store, data_dir)

            assert len(reconciled) == 1
            assert reconciled[0]["status"] == "FAILED"
            assert reconciled[0]["error_summary"] == "orphaned run: missing ingest_batch_id"

    def test_reconcile_no_orphans_no_fabricated_rows(self, tmp_stores) -> None:
        """Given 仅有终态 run_log When reconcile Then 返回空且不新增任何行
        （H-5：禁止伪造 run_log 行污染审计痕迹）"""
        data_dir = str(tmp_stores.data_dir)
        meta_dir = str(tmp_stores.meta_dir)

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

            raw_store = RawStore(data_dir)
            canonical_store = CanonicalStoreImpl(data_dir)
            reconciled = reconcile(meta, raw_store, canonical_store, data_dir)

            assert reconciled == []
            count_after = meta.connection.execute(
                "SELECT COUNT(*) FROM run_log"
            ).fetchone()[0]
            assert count_after == count_before

    def test_reconcile_running_orphan_reconciled(self, tmp_stores) -> None:
        """Given RUNNING 孤儿 run（finish_run 前崩溃）When reconcile Then 补记终态"""
        data_dir = str(tmp_stores.data_dir)
        meta_dir = str(tmp_stores.meta_dir)

        with MetaStore(meta_dir) as meta:
            meta.migrate()
            _setup_source_and_dataset(meta, "test_source", "test_dataset")

            row = meta.try_lock_dataset("test_dataset", source_id="test_source")
            meta.connection.execute(
                "UPDATE run_log SET status = 'RUNNING' WHERE run_id = ?",
                (row.run_id,),
            )
            meta.connection.commit()

            raw_store = RawStore(data_dir)
            _append_raw_batch(raw_store, "test_source", "test_dataset", row.ingest_batch_id)
            canonical_store = CanonicalStoreImpl(data_dir)

            reconciled = reconcile(meta, raw_store, canonical_store, data_dir)
            assert len(reconciled) == 1
            assert reconciled[0]["status"] == "SUCCESS"


# ── startup_repair 完整路径 ──────────────────────────────────────


class TestStartupRepair:
    """recovery: 启动即修复（startup_repair 完整路径）"""

    def test_startup_repair_full_path(self, tmp_stores) -> None:
        """Given 孤儿目录 + 孤儿 run（raw 已落盘）When startup_repair
        Then cleanup + reconcile 执行，孤儿 run 补记 SUCCESS"""
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

        with MetaStore(meta_dir) as meta:
            meta.migrate()
            _setup_source_and_dataset(meta, "test_source", "test_dataset")

            # 模拟崩溃孤儿 run：数据已落盘（raw），终态未写
            row = meta.try_lock_dataset("test_dataset", source_id="test_source")
            raw_store = RawStore(data_dir)
            _append_raw_batch(raw_store, "test_source", "test_dataset", row.ingest_batch_id)

            canonical_store = CanonicalStoreImpl(data_dir)

            # 执行启动修复
            startup_repair(meta, raw_store, canonical_store, data_dir)

            # 验证孤儿已清理
            assert not tmp_orphan.exists()
            assert not old_orphan.exists()

            # 验证孤儿 run 已补记 SUCCESS（真实 batch_id 对账，非伪造行）
            cursor = meta.connection.execute(
                "SELECT status, source_id FROM run_log WHERE run_id = ?",
                (row.run_id,),
            )
            db_row = cursor.fetchone()
            assert db_row is not None
            assert db_row[0] == "SUCCESS"
            assert db_row[1] == "test_source"

    def test_startup_repair_no_orphans_no_reconcile_needed(self, tmp_stores) -> None:
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

            raw_store = RawStore(data_dir)
            canonical_store = CanonicalStoreImpl(data_dir)

            # 应不报错且不新增行
            startup_repair(meta, raw_store, canonical_store, data_dir)
            count_after = meta.connection.execute(
                "SELECT COUNT(*) FROM run_log"
            ).fetchone()[0]
            assert count_after == count_before

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
                datetime.utcnow() - timedelta(hours=2)
            ).isoformat() + "Z"
            meta.connection.execute(
                "UPDATE run_log SET started_at = ? WHERE run_id = ?",
                (stale_start, row.run_id),
            )
            meta.connection.commit()

            raw_store = RawStore(data_dir)
            canonical_store = CanonicalStoreImpl(data_dir)

            startup_repair(meta, raw_store, canonical_store, data_dir)

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

    def test_given_rename_ok_run_log_write_failed_when_reconcile_then_supplement(
        self, tmp_stores
    ) -> None:
        """Given 数据 rename 成功但 run_log 终态写失败（孤儿 PENDING 行）
        When reconcile Then 按真实 batch_id 对账补记 SUCCESS"""
        data_dir = str(tmp_stores.data_dir)
        meta_dir = str(tmp_stores.meta_dir)

        with MetaStore(meta_dir) as meta:
            meta.migrate()
            _setup_source_and_dataset(meta, "test_src", "test_ds")

            row = meta.try_lock_dataset("test_ds", source_id="test_src")
            raw_store = RawStore(data_dir)
            _append_raw_batch(raw_store, "test_src", "test_ds", row.ingest_batch_id)

            canonical_store = CanonicalStoreImpl(data_dir)
            reconciled = reconcile(meta, raw_store, canonical_store, data_dir)

            assert len(reconciled) == 1
            assert reconciled[0]["status"] == "SUCCESS"

    def test_given_no_raw_when_reconcile_then_failed_no_raw_data_found(self, tmp_stores) -> None:
        """Given 孤儿 run 且 raw 层无对应数据 When reconcile Then 补记 FAILED"""
        data_dir = str(tmp_stores.data_dir)
        meta_dir = str(tmp_stores.meta_dir)

        with MetaStore(meta_dir) as meta:
            meta.migrate()
            _setup_source_and_dataset(meta, "test_src", "test_ds")

            # 产生孤儿 PENDING 行（无需引用返回值）
            meta.try_lock_dataset("test_ds", source_id="test_src")

            raw_store = RawStore(data_dir)
            canonical_store = CanonicalStoreImpl(data_dir)
            reconciled = reconcile(meta, raw_store, canonical_store, data_dir)

            assert len(reconciled) == 1
            assert reconciled[0]["status"] == "FAILED"
            assert reconciled[0]["error_summary"] == "no raw data found for batch"
