"""STORAGE-001.1 集成测试 — MetaStore DDL + 迁移 0001。

依据：STORAGE-001.1.md 测试要求 + GWT 验收标准 + D03 §1 DDL
"""

from __future__ import annotations

import os
import sqlite3
import stat

import pytest

from chronoforge.exceptions import StorageError
from chronoforge.storage.meta import MetaStore

EXPECTED_TABLES = {
    "source_registry",
    "dataset_registry",
    "checkpoints",
    "run_log",
    "quality_flags",
    "schema_versions",
}


def _get_tables(conn: sqlite3.Connection) -> set[str]:
    """获取数据库中所有表名。"""
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    )
    return {row[0] for row in cursor.fetchall()}


# ── GWT acceptance tests ──────────────────────────────────────────


class TestGWT:

    def test_given_empty_meta_dir_when_migrate_then_6_tables_created(
        self, tmp_stores
    ) -> None:
        """Given 空 meta_dir When migrate() Then 6 张表全部创建成功"""
        meta_dir = str(tmp_stores.meta_dir)
        # Ensure meta_dir exists but is empty
        assert not (tmp_stores.meta_dir / "chronoforge.db").exists()

        with MetaStore(meta_dir) as store:
            store.migrate()

            conn = store.connection
            tables = _get_tables(conn)
            assert EXPECTED_TABLES.issubset(tables)

    def test_given_migrated_db_when_migrate_then_no_error(
        self, tmp_stores
    ) -> None:
        """Given 已迁移数据库 When migrate() Then 不报错（幂等）"""
        meta_dir = str(tmp_stores.meta_dir)

        # First migration
        with MetaStore(meta_dir) as store:
            store.migrate()

        # Second migration (idempotent)
        with MetaStore(meta_dir) as store:
            store.migrate()  # Should not raise

    def test_given_connection_when_check_pragma_journal_mode_then_wal(
        self, tmp_stores
    ) -> None:
        """Given WAL 模式 When 查询 PRAGMA journal_mode Then 返回 WAL"""
        meta_dir = str(tmp_stores.meta_dir)

        with MetaStore(meta_dir) as store:
            store.migrate()
            conn = store.connection
            cursor = conn.execute("PRAGMA journal_mode")
            mode = cursor.fetchone()[0]
            assert mode == "wal"


# ── Migration idempotency tests ──────────────────────────────────


class TestMigrationIdempotency:

    def test_migrate_twice_no_duplicate_schema_versions(
        self, tmp_stores
    ) -> None:
        """连续调用 migrate() 两次，schema_versions 中只有一条 0001 记录"""
        meta_dir = str(tmp_stores.meta_dir)

        with MetaStore(meta_dir) as store:
            store.migrate()
            store.migrate()

            conn = store.connection
            cursor = conn.execute(
                "SELECT COUNT(*) FROM schema_versions WHERE version = '0001'"
            )
            count = cursor.fetchone()[0]
            assert count == 1


# ── DDL integrity tests ──────────────────────────────────────────


class TestDDLIntegrity:

    def test_all_6_tables_exist(self, tmp_stores) -> None:
        """6 张表全部存在"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            conn = store.connection
            tables = _get_tables(conn)
            assert tables == EXPECTED_TABLES

    def test_source_registry_columns(self, tmp_stores) -> None:
        """source_registry 字段校验"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            conn = store.connection

            cursor = conn.execute("PRAGMA table_info(source_registry)")
            columns = {row[1]: row[2] for row in cursor.fetchall()}

            assert set(columns.keys()) == {
                "source_id", "display_name", "access_type",
                "base_url", "rate_limit_json", "historical_limit_days",
                "license", "enabled",
            }
            assert columns["source_id"] == "TEXT"
            assert columns["enabled"] == "INT"

    def test_dataset_registry_columns(self, tmp_stores) -> None:
        """dataset_registry 字段校验"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            conn = store.connection

            cursor = conn.execute("PRAGMA table_info(dataset_registry)")
            columns = {row[1]: row[2] for row in cursor.fetchall()}

            assert set(columns.keys()) == {
                "dataset_id", "source_id", "canonical_type",
                "entity_id", "params_json", "frequency",
                "continuity_model", "status", "available_from",
                "available_to", "revision_supported", "enabled",
                "created_at",
            }

    def test_checkpoints_composite_pk(self, tmp_stores) -> None:
        """checkpoints PK 为 (source_id, dataset_id)"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            conn = store.connection

            cursor = conn.execute("PRAGMA table_info(checkpoints)")
            columns = {row[1]: row[2] for row in cursor.fetchall()}

            assert set(columns.keys()) == {
                "source_id", "dataset_id", "last_cursor", "last_success_time",
            }

    def test_run_log_columns(self, tmp_stores) -> None:
        """run_log 字段校验"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            conn = store.connection

            cursor = conn.execute("PRAGMA table_info(run_log)")
            columns = {row[1]: row[2] for row in cursor.fetchall()}

            assert set(columns.keys()) == {
                "run_id", "source_id", "dataset_id", "status",
                "started_at", "ended_at", "input_count", "output_count",
                "error_count", "warning_count", "request_count",
                "retry_count", "duplicate_count", "missing_count",
                "latency_ms", "checkpoint_before", "checkpoint_after",
                "chunk_success", "chunk_failed", "error_summary",
                "schema_version", "code_version", "ingest_batch_id",
            }

    def test_run_log_index_exists(self, tmp_stores) -> None:
        """run_log 索引 idx_run_log_ds_time 存在"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            conn = store.connection

            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index' "
                "AND name='idx_run_log_ds_time'"
            )
            assert cursor.fetchone() is not None

    def test_quality_flags_columns(self, tmp_stores) -> None:
        """quality_flags 字段校验"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            conn = store.connection

            cursor = conn.execute("PRAGMA table_info(quality_flags)")
            columns = {row[1]: row[2] for row in cursor.fetchall()}

            # 审计 H-6：新增 resolved/resolved_at 列（迁移 0002）
            assert set(columns.keys()) == {
                "record_key", "dataset_id", "rule_id", "severity",
                "detail", "raw_ref", "payload_digest", "run_id",
                "created_at", "resolved", "resolved_at",
            }
            # resolved 默认 0（未处理），resolved_at 可空
            assert columns["resolved"] == "INT"
            assert columns["resolved_at"] == "TEXT"

    def test_schema_versions_columns(self, tmp_stores) -> None:
        """schema_versions 字段校验"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            conn = store.connection

            cursor = conn.execute("PRAGMA table_info(schema_versions)")
            columns = {row[1]: row[2] for row in cursor.fetchall()}

            assert set(columns.keys()) == {"version", "applied_at", "description"}


# ── Foreign keys test ────────────────────────────────────────────


class TestForeignKey:

    def test_foreign_keys_enabled(self, tmp_stores) -> None:
        """PRAGMA foreign_keys 返回 1"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            conn = store.connection
            cursor = conn.execute("PRAGMA foreign_keys")
            value = cursor.fetchone()[0]
            assert value == 1

    def test_foreign_key_constraint_enforced(self, tmp_stores) -> None:
        """dataset_registry.source_id 必须引用 source_registry.source_id"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            conn = store.connection

            # Insert valid source first
            conn.execute(
                "INSERT INTO source_registry(source_id, display_name, "
                "access_type, base_url, rate_limit_json, license) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ("test_source", "Test", "PUBLIC", "https://test.com", "{}", "MIT"),
            )
            conn.commit()

            # Valid insert
            conn.execute(
                "INSERT INTO dataset_registry(dataset_id, source_id, "
                "canonical_type, entity_id, params_json, continuity_model, "
                "status, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                ("test_ds", "test_source", "OHLCV", "BTCUSDT", "{}",
                 "ALWAYS_OPEN", "UNKNOWN", "2024-01-01"),
            )
            conn.commit()

            # Invalid source_id should fail
            with pytest.raises(sqlite3.IntegrityError):
                conn.execute(
                    "INSERT INTO dataset_registry(dataset_id, source_id, "
                    "canonical_type, entity_id, params_json, continuity_model, "
                    "status, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    ("bad_ds", "nonexistent_source", "OHLCV", "BTCUSDT", "{}",
                     "ALWAYS_OPEN", "UNKNOWN", "2024-01-01"),
                )
                conn.commit()


# ── Edge case tests ──────────────────────────────────────────────


class TestEdgeCases:

    def test_meta_dir_already_exists(self, tmp_stores) -> None:
        """meta_dir 已存在（不报错）"""
        meta_dir = str(tmp_stores.meta_dir)
        # tmp_stores fixture already creates the dir
        assert tmp_stores.meta_dir.exists()

        with MetaStore(meta_dir) as store:
            store.migrate()

    def test_meta_dir_with_subdirs(self, tmp_stores) -> None:
        """meta_dir 含子目录（创建成功）"""
        meta_dir = str(tmp_stores.meta_dir / "nested" / "deep")

        with MetaStore(meta_dir) as store:
            store.migrate()

        assert (tmp_stores.meta_dir / "nested" / "deep" / "chronoforge.db").exists()

    def test_meta_dir_permission_denied(self, tmp_path) -> None:
        """meta_dir 权限不足 → StorageError"""
        # Create a read-only directory
        readonly_dir = tmp_path / "readonly"
        readonly_dir.mkdir(parents=True)
        os.chmod(str(readonly_dir), stat.S_IRUSR | stat.S_IXUSR)

        # This should fail when trying to create the db file inside
        with pytest.raises(StorageError):
            with MetaStore(str(readonly_dir)) as store:
                store.migrate()

        os.chmod(str(readonly_dir), stat.S_IRWXU)  # restore for cleanup


# ── Schema version tracking test ─────────────────────────────────


class TestSchemaVersionTracking:

    def test_schema_versions_recorded_after_migrate(self, tmp_stores) -> None:
        """迁移执行后 schema_versions 表中有记录"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            conn = store.connection

            cursor = conn.execute(
                "SELECT version, applied_at, description FROM schema_versions "
                "WHERE version = '0001'"
            )
            row = cursor.fetchone()
            assert row is not None
            assert row[0] == "0001"
            assert row[1] is not None  # applied_at is set
            assert "Initial DDL" in row[2]
