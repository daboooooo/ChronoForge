"""跨存储一致性：孤儿清理 + 对账（D03 §5.5）。

职责：
- cleanup_orphans：启动时清理 temp/.old-* 孤儿目录
- reconcile：对账孤儿 run（数据落盘但终态写失败），按 ingest_batch_id 补记
- startup_repair：完整启动修复流程（cleanup → reconcile）
"""

from __future__ import annotations

import json
import shutil
import sqlite3
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from chronoforge.exceptions import StorageError
from chronoforge.logging import get_logger

if TYPE_CHECKING:
    from chronoforge.storage.base import CanonicalStore
    from chronoforge.storage.meta import MetaStore
    from chronoforge.storage.raw import RawStore

# D01 §4：统一经 structlog 入口（旧代码误用 stdlib logger + % 位置参数，
# 桥接 RichHandler 后会触发 LogRecord 格式化错误）。
logger = get_logger()


# ── cleanup_orphans（D03 §5.5）─────────────────────────────────────


def cleanup_orphans(data_dir: str, *, min_age_seconds: float = 0.0) -> list[str]:
    """清理 canonical 层 temp/p.old 孤儿目录（D03 §5.5 原子性协议）。

    启动时调用。幂等：多次调用不报错，不删除正常的 part-*.parquet 文件。

    审计 C-4：.tmp-*（未完成 rename）与 .old-*（rename-swap 备份）均创建于
    year 层（canonical.py 中 part_path.parent / target.parent），
    即 month=* 分区目录的同级，因此扫描 year 目录的直接子目录。

    R2-03①（审计 2026-09-22）年龄护栏：min_age_seconds > 0 时仅清理
    mtime 距今超过该值的目录——并发 run 在途的新鲜 .tmp-* 不删，防止
    startup_repair 与在途 merge-rewrite 重叠时误伤（rename-swap 失败）。

    Args:
        data_dir: 数据存储根目录。
        min_age_seconds: 孤儿目录最小年龄（秒），0 = 立即清理（旧行为）。

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
                        if min_age_seconds > 0:
                            try:
                                age = time.time() - subdir.stat().st_mtime
                            except OSError:
                                age = float("inf")  # stat 失败视为足够老
                            if age <= min_age_seconds:
                                continue  # 新鲜目录：可能是在途 run 的临时分片
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
    *,
    stale_run_timeout_seconds: float = 0.0,
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

    R2-03②（审计 2026-09-22）租约护栏：stale_run_timeout_seconds > 0 时
    仅对账 started_at 距今超过该值的行——并发活跃 run（租约内）不补记
    中间假终态，防 run_log 审计痕迹被真实 finish_run 之外的写入污染。

    Args:
        meta: MetaStore 实例。
        raw_store: RawStore 实例。
        canonical_store: CanonicalStore 实例（Protocol 类型）。
        data_dir: 数据存储根目录（签名契约保留）。
        stale_run_timeout_seconds: 孤儿租约下限（秒），0 = 全部对账（旧行为）。

    Returns:
        补记的 dict 列表（含 run_id, status, error_summary）。
    """
    if meta._conn is None:
        raise StorageError("Connection is closed")

    reconciled: list[dict[str, object]] = []

    sql = (
        "SELECT run_id, source_id, dataset_id, ingest_batch_id FROM run_log "
        "WHERE status IN ('PENDING', 'RUNNING')"
    )
    params: list[object] = []
    if stale_run_timeout_seconds > 0:
        # started_at 为 ISO-8601 UTC 字符串（try_lock_dataset 写入），
        # 同格式字典序即时间序（与 release_stale_locks 同一比较约定）
        cutoff = (
            datetime.now(UTC).replace(tzinfo=None)
            - timedelta(seconds=stale_run_timeout_seconds)
        ).isoformat() + "Z"
        sql += " AND started_at <= ?"
        params.append(cutoff)
    cursor = meta._conn.execute(sql, params)
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

    R2-03（审计 2026-09-22）：cleanup_orphans 与 reconcile 共用
    stale_run_timeout_seconds 作为护栏——前者为孤儿目录最小年龄
    （并发 run 在途的 .tmp-* 不删），后者为孤儿 run 租约下限
    （活跃 run 不补记中间假终态）。

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
            released=len(released),
            run_ids=released,
        )

    # 1. 清理孤儿（R2-03①：年龄护栏，在途 run 的临时目录不删）
    orphans = cleanup_orphans(data_dir, min_age_seconds=stale_run_timeout_seconds)
    if orphans:
        logger.info(
            "storage.cleanup_orphans",
            cleaned=len(orphans),
            paths=orphans,
        )

    # 2. 对账补记（R2-03②：租约护栏，活跃 run 不补记）
    reconciled = reconcile(
        meta,
        raw_store,
        canonical_store,
        data_dir,
        stale_run_timeout_seconds=stale_run_timeout_seconds,
    )
    if reconciled:
        logger.info(
            "storage.reconcile",
            reconciled=len(reconciled),
        )
