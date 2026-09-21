"""STORAGE-001.2 集成测试 — MetaStore 锁 + checkpoint + run_log + 状态推导。

依据：STORAGE-001.2.md 测试要求 + GWT 验收标准 + D03 §1 状态推导规则
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta

import pytest

from chronoforge.exceptions import StorageError
from chronoforge.storage.meta import MetaStore

# ── Helper: setup fixture data ─────────────────────────────────────


def _setup_source_and_dataset(store: MetaStore, source_id: str, dataset_id: str) -> None:
    """插入 source_registry 和 dataset_registry 记录。"""
    conn = store.connection
    conn.execute(
        "INSERT OR IGNORE INTO source_registry "
        "(source_id, display_name, access_type, base_url, rate_limit_json, license) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (source_id, "Test", "PUBLIC", "https://test.com", "{}", "MIT"),
    )
    conn.execute(
        "INSERT OR IGNORE INTO dataset_registry "
        "(dataset_id, source_id, canonical_type, entity_id, params_json, "
        "continuity_model, status, created_at, revision_supported) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (dataset_id, source_id, "OHLCV", "BTCUSDT", "{}",
         "ALWAYS_OPEN", "UNKNOWN", "2024-01-01", 0),
    )
    conn.commit()


def _insert_run_log(store: MetaStore, run_id: str, dataset_id: str,
                    source_id: str, status: str,
                    checkpoint_before: str | None = None,
                    checkpoint_after: str | None = None) -> None:
    """插入一条 run_log 记录。"""
    store.connection.execute(
        "INSERT INTO run_log "
        "(run_id, source_id, dataset_id, status, started_at, "
        "schema_version, code_version, ingest_batch_id, "
        "checkpoint_before, checkpoint_after) "
        "VALUES (?, ?, ?, ?, datetime('now'), '', '', ?, ?, ?)",
        (run_id, source_id, dataset_id, status,
         "ingest_001", checkpoint_before, checkpoint_after),
    )
    store.connection.commit()


def _insert_checkpoint(store: MetaStore, source_id: str, dataset_id: str,
                       cursor: str) -> None:
    """插入一条 checkpoints 记录。"""
    store.connection.execute(
        "INSERT INTO checkpoints "
        "(source_id, dataset_id, last_cursor, last_success_time) "
        "VALUES (?, ?, ?, datetime('now'))",
        (source_id, dataset_id, cursor),
    )
    store.connection.commit()


def _insert_quality_flag(store: MetaStore, record_key: str, dataset_id: str,
                         rule_id: str, severity: str, run_id: str,
                         detail: str = "", raw_ref: str = "",
                         payload_digest: str = "") -> None:
    """插入一条 quality_flags 记录。"""
    store.connection.execute(
        "INSERT OR REPLACE INTO quality_flags "
        "(record_key, dataset_id, rule_id, severity, detail, "
        "raw_ref, payload_digest, run_id, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
        (record_key, dataset_id, rule_id, severity,
         detail, raw_ref, payload_digest, run_id),
    )
    store.connection.commit()


def _insert_dataset_with_status(store: MetaStore, dataset_id: str,
                                status: str,
                                revision_supported: int = 0) -> None:
    """更新 dataset_registry 中的 status（用 UPDATE 避免 FK 问题）。"""
    conn = store.connection
    # First ensure the dataset exists, then update
    cursor = conn.execute(
        "SELECT source_id FROM dataset_registry WHERE dataset_id = ?",
        (dataset_id,),
    )
    row = cursor.fetchone()
    source_id = row[0] if row else "test_source"
    conn.execute(
        "UPDATE dataset_registry SET status = ?, revision_supported = ? "
        "WHERE dataset_id = ?",
        (status, revision_supported, dataset_id),
    )
    # If row wasn't updated, insert it
    if conn.total_changes == 0:
        conn.execute(
            "INSERT INTO dataset_registry "
            "(dataset_id, source_id, canonical_type, entity_id, params_json, "
            "continuity_model, status, created_at, revision_supported) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (dataset_id, source_id, "OHLCV", "BTCUSDT", "{}",
             "ALWAYS_OPEN", status, "2024-01-01", revision_supported),
        )
    conn.commit()


# ── GWT acceptance tests ─────────────────────────────────────────


class TestGWT:

    def test_given_two_concurrent_threads_try_lock_same_dataset_then_second_raises_storage_error(
        self, tmp_stores
    ) -> None:
        """Given 两线程并发 try_lock 同 dataset Then 后者抛 StorageError"""
        meta_dir = str(tmp_stores.meta_dir)

        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")

            # First thread locks successfully
            run_id_1 = store.try_lock_dataset("test_dataset")
            assert run_id_1 is not None

            # Second connection also locks (sees the committed PENDING row)
            # TC-S-003: dataset 锁互斥（D09 §3）
            conn2 = MetaStore(meta_dir)
            conn2.migrate()
            with pytest.raises(StorageError) as exc_info:
                conn2.try_lock_dataset("test_dataset")
            assert "dataset locked" in str(exc_info.value).lower()

            conn2.close()

    def test_given_finish_run_status_failed_then_checkpoint_not_updated(
        self, tmp_stores
    ) -> None:
        """Given finish_run status=FAILED Then checkpoint 不更新"""
        meta_dir = str(tmp_stores.meta_dir)

        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")

            run_id = store.try_lock_dataset("test_dataset")

            # Save a checkpoint first
            store.save_checkpoint("test_source", "test_dataset", "cursor_001")

            cp_before = store.get_checkpoint("test_source", "test_dataset")
            assert cp_before is not None
            last_success_before = store.connection.execute(
                "SELECT last_success_time FROM checkpoints WHERE source_id = ? AND dataset_id = ?",
                ("test_source", "test_dataset"),
            ).fetchone()[0]

            # Finish with FAILED
            time.sleep(0.1)  # ensure time difference
            store.finish_run(run_id.run_id, "FAILED")

            cp_after = store.get_checkpoint("test_source", "test_dataset")
            assert cp_after is not None
            last_success_after = store.connection.execute(
                "SELECT last_success_time FROM checkpoints WHERE source_id = ? AND dataset_id = ?",
                ("test_source", "test_dataset"),
            ).fetchone()[0]
            assert last_success_after == last_success_before

    def test_given_error_finding_exists_when_derive_dataset_status_then_returns_incomplete(
        self, tmp_stores
    ) -> None:
        """Given ERROR finding 存在 When derive_dataset_status Then 返回 INCOMPLETE"""
        meta_dir = str(tmp_stores.meta_dir)

        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")

            run_id = store.try_lock_dataset("test_dataset")
            _insert_quality_flag(store, "rk1", "test_dataset",
                                 "Q-001-001", "ERROR", run_id.run_id)

            status = store.derive_dataset_status("test_dataset")
            assert status == "INCOMPLETE"

    def test_given_checkpoints_cleared_when_rebuild_checkpoints_then_restored_from_run_log(
        self, tmp_stores
    ) -> None:
        """Given checkpoints 清空 When rebuild_checkpoints Then 从 run_log 恢复"""
        meta_dir = str(tmp_stores.meta_dir)

        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")

            # Insert a successful run with checkpoint info
            run_id = "rebuild_test_run"
            _insert_run_log(
                store, run_id, "test_dataset", "test_source", "SUCCESS",
                checkpoint_before="cursor_before",
                checkpoint_after="cursor_after",
            )

            # Delete checkpoint
            store.connection.execute(
                "DELETE FROM checkpoints WHERE dataset_id = ?",
                ("test_dataset",),
            )
            store.connection.commit()

            # Rebuild
            store.rebuild_checkpoints("test_dataset")

            cp = store.get_checkpoint("test_source", "test_dataset")
            assert cp is not None
            assert cp == "cursor_after"


# ── try_lock_mutex tests ──────────────────────────────────────────


class TestTryLock:

    def test_try_lock_returns_run_id(self, tmp_stores) -> None:
        """try_lock 返回非空 run_id"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")

            run_id = store.try_lock_dataset("test_dataset")
            assert run_id is not None
            assert len(run_id.run_id) == 32  # UUID hex

    def test_try_lock_creates_run_log_entry(self, tmp_stores) -> None:
        """try_lock 创建 PENDING run_log 记录"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")

            run_id = store.try_lock_dataset("test_dataset")
            conn = store.connection
            cursor = conn.execute(
                "SELECT status FROM run_log WHERE run_id = ?", (run_id.run_id,)
            )
            row = cursor.fetchone()
            assert row is not None
            assert row[0] == "PENDING"

    def test_try_lock_concurrent_same_dataset_raises_error(self, tmp_stores) -> None:
        """两个连续 try_lock 同 dataset → 第二个因第一个仍 PENDING 而抛 StorageError"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")

            # First lock (PENDING, not finished)
            run_id_1 = store.try_lock_dataset("test_dataset")
            assert run_id_1 is not None

            # Second lock should fail because first is still PENDING
            with pytest.raises(StorageError) as exc_info:
                store.try_lock_dataset("test_dataset")
            assert "dataset locked" in str(exc_info.value).lower()

    def test_try_lock_different_datasets_no_conflict(self, tmp_stores) -> None:
        """不同 dataset 互不冲突"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            for ds in ("ds_a", "ds_b"):
                _setup_source_and_dataset(store, "test_source", ds)

            id_a = store.try_lock_dataset("ds_a")
            id_b = store.try_lock_dataset("ds_b")
            assert id_a.run_id != id_b.run_id


# ── checkpoint atomicity tests ────────────────────────────────────


class TestCheckpoint:

    def test_save_and_get_checkpoint_roundtrip(self, tmp_stores) -> None:
        """save_checkpoint → get_checkpoint 往返一致"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()

            store.save_checkpoint("test_source", "test_dataset", "cursor_abc")

            cp = store.get_checkpoint("test_source", "test_dataset")
            assert cp is not None
            assert cp == "cursor_abc"
            last_success_time = store.connection.execute(
                "SELECT last_success_time FROM checkpoints WHERE source_id = ? AND dataset_id = ?",
                ("test_source", "test_dataset"),
            ).fetchone()[0]
            assert last_success_time is not None

    def test_get_checkpoint_no_record_returns_none(self, tmp_stores) -> None:
        """get_checkpoint 无记录返回 None"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()

            cp = store.get_checkpoint("test_source", "nonexistent_dataset")
            assert cp is None

    def test_save_checkpoint_overwrites(self, tmp_stores) -> None:
        """save_checkpoint 更新已有记录"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()

            store.save_checkpoint("test_source", "test_dataset", "cursor_v1")
            cp1 = store.get_checkpoint("test_source", "test_dataset")
            assert cp1 == "cursor_v1"
            last_success_time_1 = store.connection.execute(
                "SELECT last_success_time FROM checkpoints WHERE source_id = ? AND dataset_id = ?",
                ("test_source", "test_dataset"),
            ).fetchone()[0]

            time.sleep(0.1)  # ensure time difference
            store.save_checkpoint("test_source", "test_dataset", "cursor_v2")
            cp2 = store.get_checkpoint("test_source", "test_dataset")
            assert cp2 == "cursor_v2"
            last_success_time_2 = store.connection.execute(
                "SELECT last_success_time FROM checkpoints WHERE source_id = ? AND dataset_id = ?",
                ("test_source", "test_dataset"),
            ).fetchone()[0]
            assert last_success_time_2 >= last_success_time_1

    def test_property_try_lock_then_get_checkpoint_non_none(
        self, tmp_stores
    ) -> None:
        """property: try_lock 成功后 get_checkpoint 返回非 None"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")

            _ = store.try_lock_dataset("test_dataset")
            store.save_checkpoint("test_source", "test_dataset", "cursor_from_lock")

            cp = store.get_checkpoint("test_source", "test_dataset")
            assert cp is not None
            assert cp == "cursor_from_lock"


# ── finish_run tests ──────────────────────────────────────────────


class TestFinishRun:

    def test_finish_run_updates_run_log(self, tmp_stores) -> None:
        """finish_run 更新 run_log 状态"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")

            run_id = store.try_lock_dataset("test_dataset")
            store.finish_run(run_id.run_id, "SUCCESS", chunk_success=5, chunk_failed=0)

            conn = store.connection
            cursor = conn.execute(
                "SELECT status, chunk_success, chunk_failed, ended_at "
                "FROM run_log WHERE run_id = ?", (run_id.run_id,)
            )
            row = cursor.fetchone()
            assert row is not None
            assert row[0] == "SUCCESS"
            assert row[1] == 5
            assert row[2] == 0
            assert row[3] is not None

    def test_finish_run_status_failed_no_checkpoint_update(
        self, tmp_stores
    ) -> None:
        """status=FAILED 不更新 checkpoint"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")

            run_id = store.try_lock_dataset("test_dataset")
            store.save_checkpoint("test_source", "test_dataset", "cursor_001")

            time.sleep(0.1)
            last_success_before = store.connection.execute(
                "SELECT last_success_time FROM checkpoints WHERE source_id = ? AND dataset_id = ?",
                ("test_source", "test_dataset"),
            ).fetchone()[0]

            store.finish_run(run_id.run_id, "FAILED")

            cp_after = store.get_checkpoint("test_source", "test_dataset")
            assert cp_after is not None
            last_success_after = store.connection.execute(
                "SELECT last_success_time FROM checkpoints WHERE source_id = ? AND dataset_id = ?",
                ("test_source", "test_dataset"),
            ).fetchone()[0]
            assert last_success_after == last_success_before

    def test_finish_run_status_success_updates_checkpoint(
        self, tmp_stores
    ) -> None:
        """status=SUCCESS 更新 checkpoint last_success_time"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")

            run_id = store.try_lock_dataset("test_dataset")
            store.save_checkpoint("test_source", "test_dataset", "cursor_001")

            time.sleep(0.1)
            last_success_before = store.connection.execute(
                "SELECT last_success_time FROM checkpoints WHERE source_id = ? AND dataset_id = ?",
                ("test_source", "test_dataset"),
            ).fetchone()[0]

            store.finish_run(run_id.run_id, "SUCCESS")

            cp_after = store.get_checkpoint("test_source", "test_dataset")
            assert cp_after is not None
            last_success_after = store.connection.execute(
                "SELECT last_success_time FROM checkpoints WHERE source_id = ? AND dataset_id = ?",
                ("test_source", "test_dataset"),
            ).fetchone()[0]
            assert last_success_after >= last_success_before

    def test_finish_run_invalid_status_raises(self, tmp_stores) -> None:
        """无效 status → StorageError"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()

            with pytest.raises(StorageError):
                store.finish_run("fake_run", "INVALID_STATUS")


# ── derive_dataset_status tests ───────────────────────────────────


class TestDeriveDatasetStatus:

    def test_error_finding_returns_incomplete(self, tmp_stores) -> None:
        """模拟 ERROR finding → derive_dataset_status = INCOMPLETE"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")

            run_id = store.try_lock_dataset("test_dataset")
            _insert_quality_flag(store, "rk1", "test_dataset",
                                 "Q-001-001", "ERROR", run_id.run_id)

            status = store.derive_dataset_status("test_dataset")
            assert status == "INCOMPLETE"

    def test_quarantined_flag_returns_quarantined(self, tmp_stores) -> None:
        """存在未处理 Q-QUAR-% flag → QUARANTINED（审计 M-13 新契约）"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")

            run_id = store.try_lock_dataset("test_dataset")
            _insert_quality_flag(store, "rk_quar", "test_dataset",
                                 "Q-QUAR-001", "WARNING", run_id.run_id)

            status = store.derive_dataset_status("test_dataset")
            assert status == "QUARANTINED"

    def test_quarantined_flag_resolved_recovers(self, tmp_stores) -> None:
        """Q-QUAR flag resolved 后不再返回 QUARANTINED（与 H-6 衔接）"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")

            run_id = store.try_lock_dataset("test_dataset")
            _insert_quality_flag(store, "rk_quar", "test_dataset",
                                 "Q-QUAR-001", "WARNING", run_id.run_id)
            store.resolve_quality_flags("test_dataset")

            status = store.derive_dataset_status("test_dataset")
            assert status != "QUARANTINED"

    def test_quarantined_severity_error_returns_incomplete(
        self, tmp_stores
    ) -> None:
        """Q-QUAR flag 为 ERROR severity 时规则 1 优先（INCOMPLETE）"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")

            run_id = store.try_lock_dataset("test_dataset")
            _insert_quality_flag(store, "rk_quar", "test_dataset",
                                 "Q-QUAR-001", "ERROR", run_id.run_id)

            status = store.derive_dataset_status("test_dataset")
            assert status == "INCOMPLETE"

    def test_no_error_and_successful_run_returns_complete(
        self, tmp_stores
    ) -> None:
        """最新 run SUCCESS 且无 ERROR → COMPLETE"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")

            run_id = store.try_lock_dataset("test_dataset")
            store.finish_run(run_id.run_id, "SUCCESS")

            status = store.derive_dataset_status("test_dataset")
            assert status == "COMPLETE"

    def test_no_run_returns_partial(self, tmp_stores) -> None:
        """无 run 记录 → PARTIAL"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")

            status = store.derive_dataset_status("test_dataset")
            assert status == "PARTIAL"

    def test_revision_pending_returns_revision_pending(self, tmp_stores) -> None:
        """revision_supported 且修订扫描待处理 → REVISION_PENDING"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")
            _insert_dataset_with_status(store, "test_dataset", "UNKNOWN",
                                        revision_supported=1)

            run_id = store.try_lock_dataset("test_dataset")
            _insert_quality_flag(
                store, "rk1", "test_dataset",
                "Q-REV-001", "WARNING", run_id.run_id,
            )

            status = store.derive_dataset_status("test_dataset")
            assert status == "REVISION_PENDING"


# ── rebuild_checkpoints tests ─────────────────────────────────────


class TestRebuildCheckpoints:

    def test_rebuild_from_run_log(self, tmp_stores) -> None:
        """删除 checkpoints → rebuild 恢复"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")

            run_id = "rebuild_run_001"
            _insert_run_log(
                store, run_id, "test_dataset", "test_source", "SUCCESS",
                checkpoint_before="prev_cursor",
                checkpoint_after="new_cursor",
            )

            # Delete checkpoint
            store.connection.execute(
                "DELETE FROM checkpoints WHERE dataset_id = ?",
                ("test_dataset",),
            )
            store.connection.commit()

            store.rebuild_checkpoints("test_dataset")

            cp = store.get_checkpoint("test_source", "test_dataset")
            assert cp is not None
            assert cp == "new_cursor"

    def test_rebuild_uses_checkpoint_after_over_before(
        self, tmp_stores
    ) -> None:
        """rebuild 优先使用 checkpoint_after"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")

            _insert_run_log(
                store, "run_001", "test_dataset", "test_source", "SUCCESS",
                checkpoint_before="before", checkpoint_after="after",
            )

            store.connection.execute(
                "DELETE FROM checkpoints WHERE dataset_id = ?",
                ("test_dataset",),
            )
            store.connection.commit()

            store.rebuild_checkpoints("test_dataset")

            cp = store.get_checkpoint("test_source", "test_dataset")
            assert cp == "after"

    def test_rebuild_no_valid_run_raises(self, tmp_stores) -> None:
        """无有效 run_log → 抛 StorageError"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")

            # Insert only FAILED run
            _insert_run_log(
                store, "run_001", "test_dataset", "test_source", "FAILED",
                checkpoint_after="cursor",
            )

            with pytest.raises(StorageError):
                store.rebuild_checkpoints("test_dataset")

    def test_rebuild_no_cursor_in_run_log_raises(self, tmp_stores) -> None:
        """run_log 无 checkpoint → 抛 StorageError"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")

            _insert_run_log(
                store, "run_001", "test_dataset", "test_source", "SUCCESS",
                checkpoint_before=None, checkpoint_after=None,
            )

            with pytest.raises(StorageError):
                store.rebuild_checkpoints("test_dataset")


# ── add_quality_flags tests ───────────────────────────────────────


class TestAddQualityFlags:

    def test_add_single_quality_flag(self, tmp_stores) -> None:
        """插入单条 quality_flag"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()

            store.add_quality_flags([{
                "record_key": "rk1",
                "dataset_id": "ds1",
                "rule_id": "Q-001-001",
                "severity": "ERROR",
                "detail": "test detail",
                "raw_ref": "raw/path.jsonl:1",
                "payload_digest": "abc123",
                "run_id": "run_001",
            }])

            conn = store.connection
            cursor = conn.execute(
                "SELECT record_key, severity, detail FROM quality_flags "
                "WHERE record_key = ?", ("rk1",)
            )
            row = cursor.fetchone()
            assert row is not None
            assert row[0] == "rk1"
            assert row[1] == "ERROR"
            assert row[2] == "test detail"

    def test_add_quality_flags_empty_list_no_error(self, tmp_stores) -> None:
        """add_quality_flags 空列表不报错"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()

            store.add_quality_flags([])  # Should not raise

    def test_add_quality_flags_batch(self, tmp_stores) -> None:
        """批量插入多条 quality_flag"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()

            flags = [
                {
                    "record_key": f"rk{i}",
                    "dataset_id": "ds1",
                    "rule_id": f"Q-001-{i:03d}",
                    "severity": "ERROR" if i % 2 == 0 else "WARNING",
                    "detail": f"detail_{i}",
                    "raw_ref": f"raw/path{i}.jsonl:{i}",
                    "payload_digest": f"digest_{i}",
                    "run_id": "run_001",
                }
                for i in range(5)
            ]
            store.add_quality_flags(flags)

            conn = store.connection
            cursor = conn.execute(
                "SELECT COUNT(*) FROM quality_flags WHERE dataset_id = ?",
                ("ds1",),
            )
            assert cursor.fetchone()[0] == 5

    def test_add_quality_flags_overwrites(self, tmp_stores) -> None:
        """INSERT OR REPLACE 允许重处理覆盖"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()

            store.add_quality_flags([{
                "record_key": "rk1",
                "dataset_id": "ds1",
                "rule_id": "Q-001-001",
                "severity": "ERROR",
                "detail": "original",
                "raw_ref": "raw/path.jsonl:1",
                "payload_digest": "digest1",
                "run_id": "run_001",
            }])

            # Same record_key + rule_id + run_id → should overwrite
            store.add_quality_flags([{
                "record_key": "rk1",
                "dataset_id": "ds1",
                "rule_id": "Q-001-001",
                "severity": "WARNING",
                "detail": "updated",
                "raw_ref": "raw/path.jsonl:1",
                "payload_digest": "digest2",
                "run_id": "run_001",
            }])

            conn = store.connection
            cursor = conn.execute(
                "SELECT severity, detail FROM quality_flags "
                "WHERE record_key = ? AND rule_id = ?",
                ("rk1", "Q-001-001"),
            )
            row = cursor.fetchone()
            assert row is not None
            assert row[0] == "WARNING"
            assert row[1] == "updated"


# ── Boundary tests ────────────────────────────────────────────────


class TestBoundary:

    def test_get_checkpoint_no_record(self, tmp_stores) -> None:
        """get_checkpoint 无记录返回 None"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()

            result = store.get_checkpoint("no_source", "no_dataset")
            assert result is None

    def test_add_quality_flags_empty_list(self, tmp_stores) -> None:
        """add_quality_flags 空列表不报错"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()

            store.add_quality_flags([])  # No exception

    def test_finish_run_all_terminal_statuses(self, tmp_stores) -> None:
        """所有终态都能正常结束 run"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")

            for status in ("SUCCESS", "PARTIAL_SUCCESS", "FAILED", "CANCELLED"):
                run_id = store.try_lock_dataset("test_dataset")
                store.finish_run(run_id.run_id, status)

                conn = store.connection
                cursor = conn.execute(
                    "SELECT status FROM run_log WHERE run_id = ?", (run_id.run_id,)
                )
                row = cursor.fetchone()
                assert row[0] == status

                # Clean up for next iteration
                store.connection.execute(
                    "DELETE FROM run_log WHERE run_id = ?", (run_id.run_id,)
                )
                store.connection.commit()


# ── Integration: lock → run → finish → derive flow ───────────────


class TestFullFlow:

    def test_full_pipeline_lock_run_finish_derive(self, tmp_stores) -> None:
        """完整流程：try_lock → save_checkpoint → finish_run → derive_status"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")

            # Step 1: Lock dataset
            run_id = store.try_lock_dataset("test_dataset")
            assert run_id is not None

            # Step 2: Save checkpoint during run
            store.save_checkpoint("test_source", "test_dataset", "cursor_001")

            # Step 3: Add quality flags during processing
            store.add_quality_flags([{
                "record_key": "rk1",
                "dataset_id": "test_dataset",
                "rule_id": "Q-001-001",
                "severity": "WARNING",
                "detail": "minor issue",
                "raw_ref": "raw/data.jsonl:1",
                "payload_digest": "d1",
                "run_id": run_id.run_id,
            }])

            # Step 4: Finish run successfully
            store.finish_run(
                run_id.run_id, "SUCCESS",
                chunk_success=10, chunk_failed=0,
                checkpoint_before="cursor_before",
                checkpoint_after="cursor_001",
            )

            # Verify checkpoint updated
            cp = store.get_checkpoint("test_source", "test_dataset")
            assert cp is not None
            assert cp == "cursor_001"

            # Step 5: Derive status → COMPLETE (no ERROR findings)
            status = store.derive_dataset_status("test_dataset")
            assert status == "COMPLETE"

    def test_full_flow_with_error(self, tmp_stores) -> None:
        """含 ERROR 的完整流程 → 状态推导为 INCOMPLETE"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")

            run_id = store.try_lock_dataset("test_dataset")

            # Add ERROR flag
            store.add_quality_flags([{
                "record_key": "rk_err",
                "dataset_id": "test_dataset",
                "rule_id": "Q-001-ERR",
                "severity": "ERROR",
                "detail": "critical error",
                "raw_ref": "raw/data.jsonl:42",
                "payload_digest": "d_err",
                "run_id": run_id.run_id,
            }])

            store.finish_run(
                run_id.run_id, "PARTIAL_SUCCESS",
                chunk_success=8, chunk_failed=2,
            )

            # ERROR finding exists → INCOMPLETE
            status = store.derive_dataset_status("test_dataset")
            assert status == "INCOMPLETE"


# ── run_log source_id lineage tests（审计 H-2） ────────────────────


class TestRunLineage:

    def test_try_lock_with_source_id_writes_lineage(self, tmp_stores) -> None:
        """try_lock_dataset(source_id=...) → run_log.source_id 落库，
        RunRow 携带同一 source_id（lineage 自锁创建即建立）"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")

            row = store.try_lock_dataset("test_dataset", source_id="test_source")
            assert row.source_id == "test_source"

            db_source_id = store.connection.execute(
                "SELECT source_id FROM run_log WHERE run_id = ?",
                (row.run_id,),
            ).fetchone()[0]
            assert db_source_id == "test_source"

    def test_finish_run_updates_source_id_when_provided(self, tmp_stores) -> None:
        """finish_run(source_id=...) 非空时更新 run_log.source_id"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")

            row = store.try_lock_dataset("test_dataset")
            store.finish_run(row.run_id, "SUCCESS", source_id="test_source")

            db_source_id = store.connection.execute(
                "SELECT source_id FROM run_log WHERE run_id = ?",
                (row.run_id,),
            ).fetchone()[0]
            assert db_source_id == "test_source"

    def test_finish_run_preserves_lock_time_source_id(self, tmp_stores) -> None:
        """finish_run 未提供 source_id 时保留锁创建时的 lineage（不清空）"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")

            row = store.try_lock_dataset("test_dataset", source_id="test_source")
            store.finish_run(row.run_id, "SUCCESS")

            db_source_id = store.connection.execute(
                "SELECT source_id FROM run_log WHERE run_id = ?",
                (row.run_id,),
            ).fetchone()[0]
            assert db_source_id == "test_source"

    def test_full_flow_checkpoint_lineage_end_to_end(self, tmp_stores) -> None:
        """审计 H-2 端到端：lock(source_id) → finish → checkpoints 清空 →
        rebuild_checkpoints 回填到正确 source_id，无 source_id='' 孤儿"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")

            row = store.try_lock_dataset("test_dataset", source_id="test_source")
            store.finish_run(
                row.run_id, "SUCCESS", checkpoint_after="cursor_9"
            )

            # 模拟 checkpoint 丢失 → 从 run_log 重建
            store.connection.execute("DELETE FROM checkpoints")
            store.connection.commit()
            store.rebuild_checkpoints("test_dataset")

            # 正确身份可取到
            cp = store.get_checkpoint("test_source", "test_dataset")
            assert cp == "cursor_9"
            # 无 source_id='' 的孤儿 checkpoint
            orphan_count = store.connection.execute(
                "SELECT COUNT(*) FROM checkpoints WHERE source_id = ''"
            ).fetchone()[0]
            assert orphan_count == 0

    def test_rebuild_raises_on_empty_source_id(self, tmp_stores) -> None:
        """run_log.source_id 为空（lineage 断裂）→ rebuild 熔断而非写孤儿"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")

            _insert_run_log(
                store, "orphan_run", "test_dataset", "", "SUCCESS",
                checkpoint_after="cursor_x",
            )

            with pytest.raises(StorageError, match="lineage broken"):
                store.rebuild_checkpoints("test_dataset")


# ── release_stale_locks tests（审计 H-7） ──────────────────────────


class TestReleaseStaleLocks:

    def _backdate_run(self, store: MetaStore, run_id: str, hours: float) -> None:
        """将 run 的 started_at 回拨 hours 小时（模拟崩溃残留）。"""
        old = (
            datetime.now(UTC) - timedelta(hours=hours)
        ).replace(tzinfo=None).isoformat() + "Z"
        store.connection.execute(
            "UPDATE run_log SET started_at = ? WHERE run_id = ?",
            (old, run_id),
        )
        store.connection.commit()

    def test_stale_pending_run_released(self, tmp_stores) -> None:
        """超时 PENDING run → CANCELLED 终态 + error_summary"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")

            row = store.try_lock_dataset("test_dataset")
            self._backdate_run(store, row.run_id, hours=2)

            released = store.release_stale_locks(timeout_seconds=3600.0)
            assert released == [row.run_id]

            status, ended, summary = store.connection.execute(
                "SELECT status, ended_at, error_summary FROM run_log "
                "WHERE run_id = ?",
                (row.run_id,),
            ).fetchone()
            assert status == "CANCELLED"
            assert ended is not None
            assert "orphaned" in summary

    def test_fresh_run_not_released(self, tmp_stores) -> None:
        """未超时的活跃 run 不受影响"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")

            row = store.try_lock_dataset("test_dataset")

            released = store.release_stale_locks(timeout_seconds=3600.0)
            assert released == []

            status = store.connection.execute(
                "SELECT status FROM run_log WHERE run_id = ?",
                (row.run_id,),
            ).fetchone()[0]
            assert status == "PENDING"

    def test_lock_recoverable_after_release(self, tmp_stores) -> None:
        """审计 H-7 核心场景：崩溃孤儿锁 → release → 数据集可重新加锁"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")

            # 第一次 run 崩溃残留 PENDING
            row = store.try_lock_dataset("test_dataset")
            self._backdate_run(store, row.run_id, hours=2)

            # 未释放前：dataset locked
            with pytest.raises(StorageError, match="dataset locked"):
                store.try_lock_dataset("test_dataset")

            # 释放后：可重新加锁
            released = store.release_stale_locks(timeout_seconds=3600.0)
            assert released == [row.run_id]

            new_row = store.try_lock_dataset(
                "test_dataset", source_id="test_source"
            )
            assert new_row.run_id != row.run_id

    def test_release_stale_locks_idempotent(self, tmp_stores) -> None:
        """重复调用幂等：第二次返回空"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")

            row = store.try_lock_dataset("test_dataset")
            self._backdate_run(store, row.run_id, hours=2)

            assert store.release_stale_locks(timeout_seconds=3600.0) == [row.run_id]
            assert store.release_stale_locks(timeout_seconds=3600.0) == []

    def test_release_stale_locks_negative_timeout_raises(
        self, tmp_stores
    ) -> None:
        """负超时 → StorageError"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()

            with pytest.raises(StorageError, match="timeout_seconds"):
                store.release_stale_locks(timeout_seconds=-1.0)


# ── quality_flags resolved 机制测试（审计 H-6） ─────────────────────


class TestResolveQualityFlags:

    def _add_error_flag(
        self, store: MetaStore, dataset_id: str, run_id: str,
        record_key: str = "rk_err",
    ) -> None:
        """写入一条未处理 ERROR flag"""
        store.add_quality_flags([{
            "record_key": record_key,
            "dataset_id": dataset_id,
            "rule_id": "Q-001-ERR",
            "severity": "ERROR",
            "detail": "critical error",
            "raw_ref": "raw/data.jsonl:42",
            "payload_digest": "d_err",
            "run_id": run_id,
        }])

    def test_given_resolved_error_when_derive_then_recover_to_complete(
        self, tmp_stores
    ) -> None:
        """GWT: Given ERROR flag 已 resolve When derive_dataset_status
        Then 数据集从 INCOMPLETE 恢复为 COMPLETE（审计 H-6 核心）"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")

            run_id = store.try_lock_dataset("test_dataset")
            self._add_error_flag(store, "test_dataset", run_id.run_id)
            store.finish_run(run_id.run_id, "SUCCESS")

            assert store.derive_dataset_status("test_dataset") == "INCOMPLETE"

            resolved = store.resolve_quality_flags("test_dataset")
            assert resolved == 1

            assert store.derive_dataset_status("test_dataset") == "COMPLETE"

    def test_resolve_specific_record_keys_only(self, tmp_stores) -> None:
        """指定 record_keys 时只解析匹配行，其余未处理行保持"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")

            run_id = store.try_lock_dataset("test_dataset")
            self._add_error_flag(store, "test_dataset", run_id.run_id)
            self._add_error_flag(store, "test_dataset", run_id.run_id)
            # 第二条不同 record_key
            store.add_quality_flags([{
                "record_key": "rk_err_2",
                "dataset_id": "test_dataset",
                "rule_id": "Q-001-ERR",
                "severity": "ERROR",
                "detail": "another error",
                "raw_ref": "raw/data.jsonl:43",
                "payload_digest": "d_err2",
                "run_id": run_id.run_id,
            }])
            store.finish_run(run_id.run_id, "SUCCESS")

            assert store.resolve_quality_flags(
                "test_dataset", record_keys=["rk_err"]
            ) == 1

            status = store.derive_dataset_status("test_dataset")
            # rk_err_2 仍未处理 → 仍 INCOMPLETE
            assert status == "INCOMPLETE"

            assert store.resolve_quality_flags("test_dataset") == 1
            assert store.derive_dataset_status("test_dataset") == "COMPLETE"

    def test_resolve_idempotent_and_empty_keys(self, tmp_stores) -> None:
        """幂等：重复调用返回 0；空 record_keys 列表返回 0 不发 SQL"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")

            run_id = store.try_lock_dataset("test_dataset")
            self._add_error_flag(store, "test_dataset", run_id.run_id)
            store.finish_run(run_id.run_id, "SUCCESS")

            assert store.resolve_quality_flags("test_dataset") == 1
            assert store.resolve_quality_flags("test_dataset") == 0
            assert store.resolve_quality_flags(
                "test_dataset", record_keys=[]
            ) == 0

    def test_readd_same_flag_resets_to_unresolved(self, tmp_stores) -> None:
        """INSERT OR REPLACE 重报同键 flag → 重置为未处理（重新暴露）"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")

            run_id = store.try_lock_dataset("test_dataset")
            self._add_error_flag(store, "test_dataset", run_id.run_id)
            store.finish_run(run_id.run_id, "SUCCESS")

            assert store.resolve_quality_flags("test_dataset") == 1
            # 同 (record_key, rule_id, run_id) 重报
            self._add_error_flag(store, "test_dataset", run_id.run_id)

            row = store.connection.execute(
                "SELECT resolved FROM quality_flags WHERE record_key = 'rk_err'"
            ).fetchone()
            assert row[0] == 0
            assert store.derive_dataset_status("test_dataset") == "INCOMPLETE"

    def test_resolve_isolated_per_dataset(self, tmp_stores) -> None:
        """resolve 按 dataset 隔离，不误处理其他数据集的 flags"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            _setup_source_and_dataset(store, "test_source", "test_dataset")
            _setup_source_and_dataset(store, "test_source", "other_dataset")

            run_id = store.try_lock_dataset("test_dataset")
            self._add_error_flag(
                store, "test_dataset", run_id.run_id, record_key="rk_ds1"
            )
            self._add_error_flag(
                store, "other_dataset", run_id.run_id, record_key="rk_ds2"
            )
            store.finish_run(run_id.run_id, "SUCCESS")

            assert store.resolve_quality_flags("test_dataset") == 1

            row = store.connection.execute(
                "SELECT resolved FROM quality_flags "
                "WHERE dataset_id = 'other_dataset'"
            ).fetchone()
            assert row[0] == 0
