"""跨存储一致性：孤儿清理 + 对账（D03 §5.5）。

职责：
- cleanup_orphans：启动时清理 temp/.old-* 孤儿目录
- reconcile：对账孤儿 run（数据落盘但终态写失败），按 ingest_batch_id 补记
- startup_repair：完整启动修复流程（cleanup → reconcile）
"""

from __future__ import annotations

import json
import logging
import shutil
import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING

from chronoforge.exceptions import StorageError

if TYPE_CHECKING:
    from chronoforge.storage.base import CanonicalStore
    from chronoforge.storage.meta import MetaStore
    from chronoforge.storage.raw import RawStore

logger = logging.getLogger(__name__)


# ── cleanup_orphans（D03 §5.5）─────────────────────────────────────


def cleanup_orphans(data_dir: str) -> list[str]:
    """清理 canonical 层 temp/p.old 孤儿目录（D03 §5.5 原子性协议）。

    启动时调用。幂等：多次调用不报错，不删除正常的 part-*.parquet 文件。

    审计 C-4：.tmp-*（未完成 rename）与 .old-*（rename-swap 备份）均创建于
    year 层（canonical.py 中 part_path.parent / target.parent），
    即 month=* 分区目录的同级，因此扫描 year 目录的直接子目录。

    Args:
        data_dir: 数据存储根目录。

    Returns:
        被清理的目录路径列表。
    """
    orphan_dirs: list[str] = []
    canonical_dir = Path(data_dir) / "canonical"
    if not canonical_dir.exists():
        return orphan_dirs

    for type_dir in canonical_dir.iterdir():
        if not type_dir.is_dir():
            continue
        for entity_dir in type_dir.iterdir():
            if not entity_dir.is_dir():
                continue
            for year_dir in entity_dir.iterdir():
                if not year_dir.is_dir():
                    continue
                # 清理 year 层的 .tmp-*（未完成的重命名）和 .old-*（rename-swap 备份）
                for subdir in year_dir.iterdir():
                    if not subdir.is_dir():
                        continue
                    if subdir.name.startswith(".tmp-") or subdir.name.startswith(".old-"):
                        orphan_dirs.append(str(subdir))
                        shutil.rmtree(subdir, ignore_errors=True)

    return orphan_dirs


# ── reconcile（D03 §5.5，架构 08 §5 对账）─────────────────────────


def _raw_batch_exists(
    raw_store: RawStore, source_id: str, dataset_id: str, batch_id: str
) -> bool:
    """检查 raw 层是否存在携带指定 ingest_batch_id 的记录。

    RawStore.append 将 ingest_batch_id 逐行写入 JSONL（raw.py §2），
    因此以「raw 行的 ingest_batch_id == run 的 ingest_batch_id」作为
    数据已落盘的判定依据（架构 08 §5：以 ingest_batch_id 对账）。
    """
    if not source_id or not dataset_id or not batch_id:
        return False
    raw_base = raw_store._raw_dir / source_id / dataset_id
    if not raw_base.exists():
        return False
    for jsonl_file in raw_base.glob("ingest_date=*/*.jsonl"):
        try:
            with open(jsonl_file, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if data.get("ingest_batch_id") == batch_id:
                        return True
        except OSError:
            continue
    return False


def reconcile(
    meta: MetaStore,
    raw_store: RawStore,
    canonical_store: CanonicalStore,  # noqa: ARG001  # Protocol 引用，用于类型检查
    data_dir: str,  # noqa: ARG001  # 签名契约（STORAGE-005）保留
) -> list[dict[str, object]]:
    """对账：补记孤儿 run 的缺失终态（D03 §5.5，架构 08 §5）。

    审计 H-5 重写：不再用 parquet mtime 反推 batch_id（与真实 uuid
    ingest_batch_id 永不匹配，且伪造 source_id=''/dataset_id='' 行污染
    run_log 审计痕迹）。改为以 run_log 自身的孤儿行为对账源：

    算法：
    1. 查询 run_log 中所有非终态行（PENDING/RUNNING）——即崩溃残留的
       孤儿 run（进程内单写者模型；启动时经 startup_repair 调用，
       须先 release_stale_locks 或保证无并发运行）
    2. 对每个孤儿 run，按其 ingest_batch_id（try_lock_dataset 写入，
       = run_id）在 raw 层 JSONL 逐行检索数据是否落盘
    3. raw 存在 → 补记 SUCCESS；不存在 → 补记 FAILED
       （UPDATE 原行，保留 source_id/dataset_id lineage，禁止伪造行）
    4. 更新涉及 dataset 的状态

    Args:
        meta: MetaStore 实例。
        raw_store: RawStore 实例。
        canonical_store: CanonicalStore 实例（Protocol 类型）。
        data_dir: 数据存储根目录（签名契约保留）。

    Returns:
        补记的 dict 列表（含 run_id, status, error_summary）。
    """
    if meta._conn is None:
        raise StorageError("Connection is closed")

    reconciled: list[dict[str, object]] = []

    cursor = meta._conn.execute(
        "SELECT run_id, source_id, dataset_id, ingest_batch_id FROM run_log "
        "WHERE status IN ('PENDING', 'RUNNING')"
    )
    orphan_runs = cursor.fetchall()

    for run_id, source_id, dataset_id, batch_id in orphan_runs:
        run_id = str(run_id)
        batch_id = str(batch_id or "")
        if not batch_id:
            status = "FAILED"
            error_summary: str | None = "orphaned run: missing ingest_batch_id"
        elif _raw_batch_exists(raw_store, str(source_id), str(dataset_id), batch_id):
            status = "SUCCESS"
            error_summary = None
        else:
            status = "FAILED"
            error_summary = "no raw data found for batch"

        try:
            meta._conn.execute(
                "UPDATE run_log SET status = ?, ended_at = datetime('now'), "
                "error_summary = ? "
                "WHERE run_id = ? AND status IN ('PENDING', 'RUNNING')",
                (status, error_summary, run_id),
            )
            meta._conn.commit()
        except sqlite3.Error:
            # 单条补记失败不影响其余 run
            continue
        reconciled.append({
            "run_id": run_id,
            "status": status,
            "error_summary": error_summary,
        })

    # 步骤 4：更新涉及 dataset 的状态
    if reconciled:
        _update_dataset_statuses(meta)

    return reconciled


def _update_dataset_statuses(meta: MetaStore) -> None:
    """reconcile 后，对所有涉及 dataset 调用 derive_dataset_status。

    按 D03 §1 推导规则更新 dataset_registry 的 status 列。
    """
    if meta._conn is None:
        raise StorageError("Connection is closed")

    try:
        cursor = meta._conn.execute(
            "SELECT DISTINCT dataset_id FROM run_log WHERE dataset_id != ''"
        )
        for (dataset_id,) in cursor.fetchall():
            try:
                new_status = meta.derive_dataset_status(dataset_id)
                meta._conn.execute(
                    "UPDATE dataset_registry SET status = ? WHERE dataset_id = ?",
                    (new_status, dataset_id),
                )
                meta._conn.commit()
            except Exception:  # noqa: BLE001
                # 单个 dataset 更新失败不影响整体
                logger.warning(
                    "consistency.update_dataset_status_failed",
                    "dataset_id=%s",
                    dataset_id,
                )
    except sqlite3.Error:
        pass  # 无 dataset 或查询失败


# ── startup_repair（D03 §5.5）──────────────────────────────────────


def startup_repair(
    meta: MetaStore,
    raw_store: RawStore,
    canonical_store: CanonicalStore,  # noqa: ARG001
    data_dir: str,
    stale_run_timeout_seconds: float = 3600.0,
) -> None:
    """启动时修复流程（D03 §5.5）。

    顺序：release_stale_locks → cleanup_orphans → reconcile。
    先释放超时孤儿锁（审计 H-7，解除数据集死锁），再清理 temp/.old 孤儿，
    最后对账补记缺失终态。

    Args:
        meta: MetaStore 实例。
        raw_store: RawStore 实例。
        canonical_store: CanonicalStore 实例（Protocol 类型）。
        data_dir: 数据存储根目录。
        stale_run_timeout_seconds: 活跃 run 租约超时（秒），超过视为孤儿锁。
    """
    # 0. 释放超时孤儿锁（审计 H-7）
    released = meta.release_stale_locks(stale_run_timeout_seconds)
    if released:
        logger.info(
            "storage.release_stale_locks",
            "released=%d run_ids=%s",
            len(released),
            released,
        )

    # 1. 清理孤儿
    orphans = cleanup_orphans(data_dir)
    if orphans:
        logger.info(
            "storage.cleanup_orphans",
            "cleaned=%d paths=%s",
            len(orphans),
            orphans,
        )

    # 2. 对账补记
    reconciled = reconcile(meta, raw_store, canonical_store, data_dir)
    if reconciled:
        logger.info(
            "storage.reconcile",
            "reconciled=%d",
            len(reconciled),
        )
