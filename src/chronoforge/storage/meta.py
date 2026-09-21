"""MetaStore — SQLite 元数据库基础类（D03 §1）。

职责：
- 初始化 SQLite 连接（WAL 模式 + foreign_keys=ON）
- 执行迁移（调用 migrations 模块）
- schema 版本管理
- 数据集锁、run 生命周期、checkpoint、质量标志、状态推导、checkpoint 重建
"""

from __future__ import annotations

import sqlite3
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import structlog

from chronoforge.exceptions import StorageError
from chronoforge.storage.migrations import _all_migrations

logger = structlog.get_logger()


@dataclass(frozen=True)
class RunRow:
    """run_log 行数据（D03 §6，IMP-004）。"""

    run_id: str
    source_id: str
    dataset_id: str
    status: str
    started_at: str
    ended_at: str | None = None
    input_count: int = 0
    output_count: int = 0
    error_count: int = 0
    warning_count: int = 0
    request_count: int = 0
    retry_count: int = 0
    duplicate_count: int = 0
    missing_count: int = 0
    latency_ms: int = 0
    checkpoint_before: str | None = None
    checkpoint_after: str | None = None
    chunk_success: int = 0
    chunk_failed: int = 0
    error_summary: str | None = None
    schema_version: str = ""
    code_version: str = ""
    ingest_batch_id: str = ""


# Valid run_log terminal statuses
_RUN_STATUSES = frozenset(
    {"PENDING", "RUNNING", "SUCCESS", "PARTIAL_SUCCESS", "FAILED", "CANCELLED"}
)
# Valid dataset statuses (for derive_dataset_status output)
_DATASET_STATUSES = frozenset(
    {"UNKNOWN", "COMPLETE", "PARTIAL", "INCOMPLETE", "QUARANTINED", "STALE", "REVISION_PENDING"}
)


class MetaStore:
    """SQLite 元数据库封装。"""

    def __init__(self, meta_dir: str) -> None:
        """初始化 MetaStore。

        Args:
            meta_dir: 元数据库目录，不存在时自动创建。

        Raises:
            StorageError: meta_dir 创建失败或 SQLite 连接失败。
        """
        meta_path = Path(meta_dir)
        try:
            meta_path.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            raise StorageError(f"Failed to create meta directory {meta_dir}: {e}") from e

        self._db_path = str(meta_path / "chronoforge.db")
        self._conn: sqlite3.Connection | None = None

        self._connect()

    def _connect(self) -> None:
        """建立 SQLite 连接，配置 WAL 模式和 foreign_keys。"""
        try:
            self._conn = sqlite3.connect(self._db_path)
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.execute("BEGIN")
        except sqlite3.Error as e:
            raise StorageError(f"Failed to connect to SQLite database: {e}") from e

    def migrate(self) -> None:
        """执行未应用的迁移。

        迁移按注册顺序执行，每个迁移幂等。
        """
        if self._conn is None:
            raise StorageError("Connection is closed")

        # Sort by version to ensure execution order
        sorted_migrations = sorted(_all_migrations, key=lambda m: m["version"])

        for migration in sorted_migrations:
            version = migration["version"]
            if self._is_migration_applied(version):
                continue

            sql = migration["sql"]
            description = migration["description"]
            try:
                self._conn.executescript(sql)
                self._conn.execute(
                    "INSERT INTO schema_versions(version, applied_at, description) "
                    "VALUES (?, datetime('now'), ?)",
                    (version, description),
                )
                self._conn.commit()
            except sqlite3.Error as e:
                self._conn.rollback()
                raise StorageError(f"Migration {version} failed: {e}") from e

    def _is_migration_applied(self, version: str) -> bool:
        """检查迁移是否已应用。

        如果 schema_versions 表不存在（首次迁移），返回 False。
        """
        if self._conn is None:
            return False

        try:
            cursor = self._conn.execute(
                "SELECT 1 FROM schema_versions WHERE version = ?", (version,)
            )
            return cursor.fetchone() is not None
        except sqlite3.OperationalError:
            # schema_versions table doesn't exist yet (first run)
            return False

    def _check_schema_version(self) -> None:
        """校验 schema_versions 表是否存在。"""
        if self._conn is None:
            raise StorageError("Connection is closed")

        cursor = self._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_versions'"
        )
        if cursor.fetchone() is None:
            raise StorageError("schema_versions table does not exist")

    def close(self) -> None:
        """关闭连接。"""
        if self._conn is not None:
            try:
                self._conn.close()
            except sqlite3.Error:
                pass
            finally:
                self._conn = None

    @property
    def connection(self) -> sqlite3.Connection:
        """暴露底层连接（供内部使用）。"""
        if self._conn is None:
            raise StorageError("Connection is closed")
        return self._conn

    def __enter__(self) -> MetaStore:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    # ── Dataset lock ────────────────────────────────────────────────

    def _begin_immediate(self) -> None:
        """Commit current transaction and start an IMMEDIATE one.

        Needed because __init__ opens a transaction; we must close it
        before BEGIN IMMEDIATE (SQLite does not allow nested transactions
        of different types).
        """
        if self._conn is None:
            raise StorageError("Connection is closed")
        try:
            try:
                self._conn.execute("COMMIT")
            except sqlite3.OperationalError:
                # No active transaction (e.g., after migrate() commits)
                pass
            self._conn.execute("BEGIN IMMEDIATE")
        except sqlite3.Error as e:
            raise StorageError(f"Failed to begin immediate transaction: {e}") from e

    def _reconnect(self) -> None:
        """Reconnect for fresh transaction (after commit outside context)."""
        if self._conn is not None:
            try:
                self._conn.close()
            except sqlite3.Error:
                pass
        self._connect()

    def try_lock_dataset(self, dataset_id: str, *, source_id: str = "") -> RunRow:
        """尝试获取数据集锁并创建 run_log 条目。

        使用 BEGIN IMMEDIATE 事务 + run_log 检查 RUNNING/PENDING 行。
        存在活跃运行 → 抛 StorageError；否则插入 PENDING 行返回 RunRow。

        审计 H-2：source_id 在锁创建时即写入 run_log（Data Lineage，§26），
        保证 rebuild_checkpoints 能从 run_log 回填正确的 checkpoint 身份。
        进程崩溃残留的孤儿锁可通过 release_stale_locks 释放（审计 H-7）。

        Args:
            dataset_id: 数据集 ID。
            source_id: 数据源 ID（可选，建立 run 级 lineage）。

        Returns:
            RunRow（含 run_id），供 finish_run 使用。

        Raises:
            StorageError: dataset locked（有活跃运行）。
        """
        if self._conn is None:
            raise StorageError("Connection is closed")

        run_id = uuid.uuid4().hex
        now = datetime.utcnow().isoformat() + "Z"

        try:
            self._begin_immediate()
            # Check for active runs on this dataset
            cursor = self._conn.execute(
                "SELECT run_id, status FROM run_log "
                "WHERE dataset_id = ? AND status IN ('RUNNING', 'PENDING') "
                "ORDER BY started_at DESC LIMIT 1",
                (dataset_id,),
            )
            row = cursor.fetchone()
            if row is not None:
                self._conn.execute("ROLLBACK")
                raise StorageError(
                    f"dataset locked: active run {row[0]} (status={row[1]})"
                )

            # Insert PENDING run_log entry
            self._conn.execute(
                "INSERT INTO run_log "
                "(run_id, source_id, dataset_id, status, started_at, "
                "schema_version, code_version, ingest_batch_id) "
                "VALUES (?, ?, ?, 'PENDING', ?, '', '', ?)",
                (run_id, source_id, dataset_id, now, run_id),
            )
            self._conn.commit()
            # Re-establish initial transaction for subsequent operations
            self._conn.execute("BEGIN")
        except StorageError:
            raise
        except sqlite3.Error as e:
            if self._conn is not None:
                self._conn.rollback()
            raise StorageError(f"Failed to lock dataset: {e}") from e

        return RunRow(
            run_id=run_id,
            source_id=source_id,
            dataset_id=dataset_id,
            status="PENDING",
            started_at=now,
            ingest_batch_id=run_id,
        )

    def release_stale_locks(self, timeout_seconds: float) -> list[str]:
        """将超过租约超时仍处于 RUNNING/PENDING 的孤儿 run 置为 CANCELLED。

        审计 H-7：进程崩溃后 run_log 残留 RUNNING/PENDING 行，
        try_lock_dataset 会永久抛 "dataset locked"，数据集死锁且无恢复路径。
        本方法提供启动清理（startup_repair）与运维恢复入口：超过 timeout
        仍非终态的 run 视为孤儿，标记 CANCELLED（终态）解除死锁。

        started_at 由 try_lock_dataset 以 ISO-8601 UTC 写入，同格式字符串
        按字典序比较即时间序。幂等：已终态行不受影响，重复调用返回空。

        Args:
            timeout_seconds: 租约超时（秒），started_at 距今超过该值的
                活跃 run 被视为孤儿。

        Returns:
            被释放的 run_id 列表。

        Raises:
            StorageError: timeout_seconds 为负或 SQL 执行失败。
        """
        if self._conn is None:
            raise StorageError("Connection is closed")
        if timeout_seconds < 0:
            raise StorageError("timeout_seconds must be >= 0")

        now = datetime.utcnow()
        cutoff = (now - timedelta(seconds=timeout_seconds)).isoformat() + "Z"
        ended = now.isoformat() + "Z"

        try:
            cursor = self._conn.execute(
                "SELECT run_id FROM run_log "
                "WHERE status IN ('RUNNING', 'PENDING') AND started_at <= ?",
                (cutoff,),
            )
            run_ids = [str(row[0]) for row in cursor.fetchall()]
            if not run_ids:
                return []

            self._conn.executemany(
                "UPDATE run_log SET status = 'CANCELLED', ended_at = ?, "
                "error_summary = 'orphaned: stale lock exceeded timeout' "
                "WHERE run_id = ?",
                [(ended, rid) for rid in run_ids],
            )
            self._conn.commit()
            return run_ids
        except sqlite3.Error as e:
            if self._conn is not None:
                self._conn.rollback()
            raise StorageError(f"Failed to release stale locks: {e}") from e

    def finish_run(
        self,
        run_id: str,
        status: str,
        *,
        source_id: str = "",
        input_count: int = 0,
        output_count: int = 0,
        error_count: int = 0,
        warning_count: int = 0,
        request_count: int = 0,
        retry_count: int = 0,
        duplicate_count: int = 0,
        missing_count: int = 0,
        latency_ms: int = 0,
        checkpoint_before: str | None = None,
        checkpoint_after: str | None = None,
        chunk_success: int = 0,
        chunk_failed: int = 0,
        error_summary: str | None = None,
        schema_version: str = "",
        code_version: str = "",
        ingest_batch_id: str = "",
    ) -> None:
        """结束 run 并更新 run_log 终态。

        Args:
            run_id: 要结束的 run ID。
            status: 终态值（SUCCESS/PARTIAL_SUCCESS/FAILED/CANCELLED）。
            chunk_success: 成功 chunk 数。
            chunk_failed: 失败 chunk 数。
            **kwargs: 其他可选参数。

        Raises:
            StorageError: run 不存在或 status 无效。
        """
        if status not in _RUN_STATUSES:
            raise StorageError(f"Invalid run status: {status}")

        if self._conn is None:
            raise StorageError("Connection is closed")

        error_summary_trunc = (error_summary or "")[:2000]

        try:
            self._conn.execute(
                "UPDATE run_log SET "
                "status = ?, ended_at = datetime('now'), "
                # 审计 H-2：source_id 非空时更新（lineage 可在 finish 时补正），
                # 为空时保留 try_lock_dataset 写入的值，避免清空 lineage
                "source_id = CASE WHEN ? = '' THEN source_id ELSE ? END, "
                "input_count = ?, output_count = ?, error_count = ?, "
                "warning_count = ?, request_count = ?, retry_count = ?, "
                "duplicate_count = ?, missing_count = ?, latency_ms = ?, "
                "checkpoint_before = ?, checkpoint_after = ?, "
                "chunk_success = ?, chunk_failed = ?, "
                "error_summary = ?, schema_version = ?, code_version = ?, "
                "ingest_batch_id = ? "
                "WHERE run_id = ?",
                (
                    status,
                    source_id, source_id,
                    input_count, output_count, error_count, warning_count,
                    request_count, retry_count,
                    duplicate_count, missing_count, latency_ms,
                    checkpoint_before, checkpoint_after,
                    chunk_success, chunk_failed,
                    error_summary_trunc, schema_version, code_version,
                    ingest_batch_id,
                    run_id,
                ),
            )

            # Update checkpoints only on SUCCESS/PARTIAL_SUCCESS
            if status in ("SUCCESS", "PARTIAL_SUCCESS"):
                self._conn.execute(
                    "UPDATE checkpoints SET last_success_time = datetime('now') "
                    "WHERE dataset_id = (SELECT dataset_id FROM run_log WHERE run_id = ?)",
                    (run_id,),
                )

            self._conn.commit()
        except sqlite3.Error as e:
            if self._conn is not None:
                self._conn.rollback()
            raise StorageError(f"Failed to finish run {run_id}: {e}") from e

    # ── Checkpoint ─────────────────────────────────────────────────

    def save_checkpoint(self, source_id: str, dataset_id: str, cursor: str) -> None:
        """UPSERT checkpoint：写入 last_cursor + last_success_time。

        Args:
            source_id: 数据源 ID。
            dataset_id: 数据集 ID。
            cursor: 游标值。
        """
        if self._conn is None:
            raise StorageError("Connection is closed")

        try:
            self._conn.execute(
                "INSERT INTO checkpoints "
                "(source_id, dataset_id, last_cursor, last_success_time) "
                "VALUES (?, ?, ?, datetime('now')) "
                "ON CONFLICT(source_id, dataset_id) "
                "DO UPDATE SET "
                "last_cursor = excluded.last_cursor, "
                "last_success_time = datetime('now')",
                (source_id, dataset_id, cursor),
            )
            self._conn.commit()
        except sqlite3.Error as e:
            if self._conn is not None:
                self._conn.rollback()
            raise StorageError(f"Failed to save checkpoint: {e}") from e

    def get_checkpoint(self, source_id: str, dataset_id: str) -> str | None:
        """查询 checkpoint。

        Returns:
            last_cursor 字符串，或 None（无记录）。
        """
        if self._conn is None:
            raise StorageError("Connection is closed")

        cursor = self._conn.execute(
            "SELECT last_cursor FROM checkpoints "
            "WHERE source_id = ? AND dataset_id = ?",
            (source_id, dataset_id),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return str(row[0])

    # ── Quality flags ──────────────────────────────────────────────

    def add_quality_flags(self, flags: list[dict[str, object]]) -> None:
        """批量 INSERT quality_flags。

        使用 INSERT OR REPLACE（允许重处理覆盖）。

        Args:
            flags: 每条 flag 包含 record_key, dataset_id, rule_id,
                   severity, detail, raw_ref, payload_digest, run_id 等字段。
        """
        if not flags:
            return

        if self._conn is None:
            raise StorageError("Connection is closed")

        now = datetime.utcnow().isoformat() + "Z"
        rows = []
        for flag in flags:
            rows.append((
                flag["record_key"],
                flag["dataset_id"],
                flag["rule_id"],
                flag["severity"],
                flag.get("detail", ""),
                flag.get("raw_ref", ""),
                flag.get("payload_digest", ""),
                flag["run_id"],
                now,
            ))

        try:
            self._conn.executemany(
                "INSERT OR REPLACE INTO quality_flags "
                "(record_key, dataset_id, rule_id, severity, detail, "
                "raw_ref, payload_digest, run_id, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )
            self._conn.commit()
        except sqlite3.Error as e:
            if self._conn is not None:
                self._conn.rollback()
            raise StorageError(f"Failed to add quality flags: {e}") from e

    def resolve_quality_flags(
        self,
        dataset_id: str,
        record_keys: Sequence[str] | None = None,
    ) -> int:
        """将 quality_flags 标记为已处理（审计 H-6 可运维性）。

        数据集一旦产生 ERROR flag 即永久 INCOMPLETE、无法恢复——本方法
        提供处理入口：人工/自动核验后将 flags 置 resolved=1，
        derive_dataset_status 规则 1 只统计未处理（resolved=0）ERROR。

        INSERT OR REPLACE 重报同键 flag 会重置为未处理（重新暴露），
        语义为"最新一次检测仍存在问题"。

        Args:
            dataset_id: 数据集 ID。
            record_keys: 仅解析指定 record_key 的 flags；None 解析该
                         dataset 全部未处理 flags（运维恢复入口）。

        Returns:
            更新行数。

        Raises:
            StorageError: 连接关闭或 SQL 执行失败。
        """
        if self._conn is None:
            raise StorageError("Connection is closed")

        now = datetime.utcnow().isoformat() + "Z"
        try:
            if record_keys is None:
                cursor = self._conn.execute(
                    "UPDATE quality_flags SET resolved = 1, resolved_at = ? "
                    "WHERE dataset_id = ? AND resolved = 0",
                    (now, dataset_id),
                )
            else:
                keys = list(record_keys)
                if not keys:
                    return 0
                placeholders = ",".join("?" * len(keys))
                cursor = self._conn.execute(
                    "UPDATE quality_flags SET resolved = 1, resolved_at = ? "
                    "WHERE dataset_id = ? AND resolved = 0 "
                    f"AND record_key IN ({placeholders})",
                    (now, dataset_id, *keys),
                )
            self._conn.commit()
            return int(cursor.rowcount)
        except sqlite3.Error as e:
            if self._conn is not None:
                self._conn.rollback()
            raise StorageError(f"Failed to resolve quality flags: {e}") from e

    # ── Dataset status derivation (D03 §1, 审计 F-10) ──────────────

    def derive_dataset_status(self, dataset_id: str) -> str:
        """按 D03 §1 规则推导数据集状态。

        判定顺序：
        1. 存在未处理 ERROR finding → "INCOMPLETE"
        2. 存在未处理隔离记录（Q-QUAR-% flag）→ "QUARANTINED"
        3. revision_supported 且修订扫描待处理 → "REVISION_PENDING"
        4. now − last_success_time > frequency × 2 → "STALE"
        5. 最新 run SUCCESS 且无 ERROR finding → "COMPLETE"
        6. 其余 → "PARTIAL"

        Args:
            dataset_id: 数据集 ID。

        Returns:
            推导出的状态字符串。
        """
        if self._conn is None:
            raise StorageError("Connection is closed")

        try:
            # 1. Check for unprocessed ERROR findings（审计 H-6：只统计
            # 未处理 flags，历史 ERROR 经 resolve_quality_flags 处理后可恢复）
            cursor = self._conn.execute(
                "SELECT COUNT(*) FROM quality_flags "
                "WHERE dataset_id = ? AND severity = 'ERROR' AND resolved = 0",
                (dataset_id,),
            )
            if cursor.fetchone()[0] > 0:
                return "INCOMPLETE"

            # 2. Check for quarantined records (审计 M-13：真实写入路径)
            # 隔离记录的载体 = quality_flags 中 rule_id 前缀 Q-QUAR- 的 flag
            # （severity=WARNING，避免规则 1 拦截）。原实现读
            # dataset_registry.status 自身，全库无写入路径，为死代码。
            # 与 H-6 衔接：resolve 后数据集可恢复推导。
            cursor = self._conn.execute(
                "SELECT COUNT(*) FROM quality_flags "
                "WHERE dataset_id = ? AND rule_id LIKE 'Q-QUAR-%' "
                "AND resolved = 0",
                (dataset_id,),
            )
            if cursor.fetchone()[0] > 0:
                return "QUARANTINED"

            # 3. Revision pending check
            cursor = self._conn.execute(
                "SELECT revision_supported FROM dataset_registry "
                "WHERE dataset_id = ?",
                (dataset_id,),
            )
            ds_row = cursor.fetchone()
            if ds_row is not None:
                revision_supported = ds_row[0]
                if revision_supported and revision_supported != 0:
                    # Check if there are revision-related quality flags pending
                    cursor = self._conn.execute(
                        "SELECT COUNT(*) FROM quality_flags "
                        "WHERE dataset_id = ? AND rule_id LIKE 'Q-REV-%'",
                        (dataset_id,),
                    )
                    if cursor.fetchone()[0] > 0:
                        return "REVISION_PENDING"

            # 4. Staleness check: now - last_success_time > frequency × 2
            cursor = self._conn.execute(
                "SELECT c.last_success_time, d.frequency "
                "FROM checkpoints c "
                "LEFT JOIN dataset_registry d ON c.dataset_id = d.dataset_id "
                "WHERE c.dataset_id = ?",
                (dataset_id,),
            )
            cp_row = cursor.fetchone()
            if cp_row is not None and cp_row[0] is not None and cp_row[1] is not None:
                last_success = cp_row[0]
                frequency = cp_row[1]
                # 契约声明（审计 M-11）：dataset_registry.frequency 以数值
                # "小时" 表示（如 "24" = 每日更新一次），与 quality 层的
                # D06 间隔简写（"1m"/"1d"）语义不同。非数值无法参与 STALE
                # 判定，显式告警（不再静默跳过）。
                try:
                    freq_hours = float(frequency)
                    last_dt = datetime.fromisoformat(
                        last_success.replace("Z", "+00:00")
                    )
                    now_dt = datetime.utcnow().replace(tzinfo=last_dt.tzinfo)
                    elapsed_hours = (now_dt - last_dt).total_seconds() / 3600
                    if elapsed_hours > freq_hours * 2:
                        return "STALE"
                except (ValueError, TypeError):
                    logger.warning(
                        "storage.staleness_frequency_unparsed",
                        dataset_id=dataset_id,
                        frequency=frequency,
                    )

            # 5. Latest run SUCCESS with no ERROR findings → COMPLETE
            cursor = self._conn.execute(
                "SELECT status FROM run_log "
                "WHERE dataset_id = ? AND status IN ('SUCCESS', 'PARTIAL_SUCCESS') "
                "ORDER BY started_at DESC LIMIT 1",
                (dataset_id,),
            )
            run_row = cursor.fetchone()
            if run_row is not None:
                return "COMPLETE"

            # 6. Fallback → PARTIAL
            return "PARTIAL"

        except sqlite3.Error as e:
            raise StorageError(f"Failed to derive dataset status: {e}") from e

    # ── Checkpoint rebuild (D03 §5.5) ─────────────────────────────

    def rebuild_checkpoints(self, dataset_id: str) -> None:
        """从 run_log 重建 checkpoints。

        按 D03 §5.5：扫描 run_log WHERE dataset_id = ? ORDER BY started_at DESC，
        取最新 SUCCESS/PARTIAL_SUCCESS 记录的 checkpoint_before/after。

        Args:
            dataset_id: 数据集 ID。

        Raises:
            StorageError: 无有效 run_log 记录。
        """
        if self._conn is None:
            raise StorageError("Connection is closed")

        try:
            # Find the source_id and latest valid checkpoint info
            cursor = self._conn.execute(
                "SELECT source_id, checkpoint_before, checkpoint_after, started_at "
                "FROM run_log "
                "WHERE dataset_id = ? AND status IN ('SUCCESS', 'PARTIAL_SUCCESS') "
                "ORDER BY started_at DESC LIMIT 1",
                (dataset_id,),
            )
            row = cursor.fetchone()
            if row is None:
                raise StorageError(f"No valid run_log records found for dataset {dataset_id}")

            source_id, checkpoint_before, checkpoint_after, started_at = row

            # 审计 H-2：source_id 为空说明 lineage 断裂（旧数据或写入缺陷），
            # 回填会产生 source_id='' 的孤儿 checkpoint（get_checkpoint 永远
            # 取不到），必须熔断而非静默写坏
            if not source_id:
                raise StorageError(
                    f"Cannot rebuild checkpoint for dataset {dataset_id}: "
                    "run_log.source_id is empty (lineage broken)"
                )

            # Use the checkpoint_after if available, otherwise checkpoint_before
            last_cursor = checkpoint_after if checkpoint_after else checkpoint_before
            if last_cursor is None:
                raise StorageError(
                    f"No cursor found in run_log for dataset {dataset_id}"
                )

            # Upsert checkpoint
            self._conn.execute(
                "INSERT INTO checkpoints "
                "(source_id, dataset_id, last_cursor, last_success_time) "
                "VALUES (?, ?, ?, datetime('now')) "
                "ON CONFLICT(source_id, dataset_id) "
                "DO UPDATE SET last_cursor = excluded.last_cursor, "
                "last_success_time = excluded.last_success_time",
                (source_id, dataset_id, last_cursor),
            )
            self._conn.commit()

        except StorageError:
            raise
        except sqlite3.Error as e:
            if self._conn is not None:
                self._conn.rollback()
            raise StorageError(f"Failed to rebuild checkpoints: {e}") from e
