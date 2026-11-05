"""跨存储一致性：孤儿清理 + 启动修复（D03 §5.5）。

职责：
- cleanup_orphans：启动时清理/前滚 temp/.old-* 孤儿目录
- startup_repair：启动修复流程（release_stale_locks → cleanup_orphans）

审计 M2（2026-11-05）：启动只做清理与恢复，不做对账补记。
release_stale_locks 已把超时 RUNNING/PENDING 置 CANCELLED（终态），
随后按 raw 对账补记在启动路径恒空，属不可达死代码，已删除。
真需补记历史账目时应另立显式命令（TODO），不复用启动路径。
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from chronoforge.logging import get_logger
from chronoforge.storage.canonical import recover_partition_orphans

if TYPE_CHECKING:
    from chronoforge.storage.meta import MetaStore

# D01 §4：统一经 structlog 入口（旧代码误用 stdlib logger + % 位置参数，
# 桥接 RichHandler 后会触发 LogRecord 格式化错误）。
logger = get_logger()


# ── cleanup_orphans（D03 §5.5）─────────────────────────────────────


def cleanup_orphans(data_dir: str, *, min_age_seconds: float = 0.0) -> list[str]:
    """恢复/清理 canonical 层 temp/old 孤儿目录（D03 §5.5 原子性协议）。

    启动时调用。幂等：多次调用不报错，不删除正常的 part-*.parquet 文件。

    审计 C-4：.tmp-*（未完成 rename）与 .old-*（rename-swap 备份）均创建于
    year 层（canonical.py 中 part_path.parent / target.parent），
    即 month=* 分区目录的同级，因此扫描 year 目录的直接子目录。

    审计 H6：孤儿不再"只删不合并"。target 缺失（两次 rename 之间崩溃）
    时优先前滚已提交的 .tmp-*、否则回滚 .old-*，已提交数据不丢失；
    详见 `canonical.recover_partition_orphans`。

    R2-03①（审计 2026-09-22）年龄护栏：min_age_seconds > 0 时仅清理
    mtime 距今超过该值的目录——并发 run 在途的新鲜 .tmp-* 不删，防止
    startup_repair 与在途 merge-rewrite 重叠时误伤（rename-swap 失败）。
    前滚（target 缺失）不受该护栏限制：恢复已提交数据不会覆盖有效文件。

    Args:
        data_dir: 数据存储根目录。
        min_age_seconds: 孤儿目录最小年龄（秒），0 = 立即清理（旧行为）。

    Returns:
        被恢复或清理的目录路径列表。
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
                # 审计 H6：不再"只删不合并"——先尝试把崩溃窗口残留的
                # .old-*/.tmp-* 还原到目标分区，无法还原的才按年龄清理。
                orphan_dirs.extend(
                    recover_partition_orphans(
                        year_dir, min_age_seconds=min_age_seconds
                    )
                )

    return orphan_dirs


# ── startup_repair（D03 §5.5）──────────────────────────────────────


def startup_repair(
    meta: MetaStore,
    data_dir: str,
    stale_run_timeout_seconds: float = 3600.0,
) -> None:
    """启动时修复流程（D03 §5.5）。

    顺序：release_stale_locks → cleanup_orphans。
    先释放超时孤儿锁（审计 H-7，解除数据集死锁），再清理/前滚 temp/.old
    孤儿目录。

    审计 M2（2026-11-05）：启动只做清理与恢复，不做对账补记。
    release_stale_locks 已以同一 cutoff 把超时 RUNNING/PENDING 置
    CANCELLED，随后再查非终态行恒空——按 raw 对账补记在启动路径不可达，
    已删除。若未来需要补记历史账目，应另立显式命令，不复用启动路径。

    R2-03（审计 2026-09-22）：stale_run_timeout_seconds 同时作为
    孤儿锁租约与孤儿目录最小年龄——租约内活跃 run 不释放、在途 .tmp-*
    不删。

    Args:
        meta: MetaStore 实例。
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
