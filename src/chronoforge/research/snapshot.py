"""ResearchSnapshot — 研究快照与复现（D07 §3，QUERY-003）。

契约（架构 02 §5）：
- 快照不可变：research_snapshot 仅追加（本模块无 UPDATE/DELETE 路径；
  同一 snapshot_id 重复 compute 触发主键冲突 → StorageError）
- 复现确定性：同 snapshot 两次 reproduce → output_hash 一致
- 版本变更报告：数据更新后 reproduce → hash 不一致且报告 dataset_version 变化

output_hash 语义：sha256(结果序列化)。序列化 = polars DataFrame 按全部列
升序排序后写 Arrow IPC（write_ipc）字节——同 schema+数据字节确定，且
null 与空串可区分（CSV 文本序列化无法区分）。

dataset_version 语义（D07 §1 / QUERY-001 决策 D-2）：最近一次 SUCCESS run
的 code_version+schema_version 复合（无 SUCCESS run 为 ""），事实源 run_log，
经 DatasetRegistry 只读查询（与 DuckDBQueryService._get_dataset_version 同源）。

入库时机：compute() 执行研究并落库（使用者显式触发）；contextmanager 退出
无强制动作（异常安全：研究过程抛异常时不产生任何 snapshot 记录）。

迁移编号说明：D07 §3/D10 原编号 0002 已被审计 H-6（quality_flags.resolved）
占用，research_snapshot 迁移顺延注册为 0003（DDL 不变）。

错误模型：不导入 chronoforge.exceptions（其 →connectors.errors 存量断裂链会
经传递闭包破坏 research 禁入 connectors 契约，同 features/engine.py 模式）——
领域错误 ValueError、SQLite 失败 RuntimeError；meta 参数经 MetaStoreLike
结构协议（同层协议模式，同 QUERY-002 QueryServiceLike）。
"""

from __future__ import annotations

import hashlib
import io
import json
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol, cast
from uuid import uuid4

import polars as pl
import structlog

from chronoforge import __version__
from chronoforge.research.query import DatasetRegistry, DuckDBQueryService

logger = structlog.get_logger()

# 查询函数契约：接收 DuckDBQueryService，返回 polars DataFrame（D07 §3）。
QueryFunc = Callable[["DuckDBQueryService"], pl.DataFrame]


class MetaStoreLike(Protocol):
    """MetaStore 最小结构协议（同层协议模式，同 QUERY-002 QueryServiceLike）。

    调用方传入 MetaStore 实例（结构化满足本协议）。不运行时导入
    chronoforge.storage.meta：其 →exceptions→connectors.errors 存量断裂链
    会经传递闭包破坏 research 禁入 connectors 契约（.importlinter）。
    """

    @property
    def connection(self) -> sqlite3.Connection: ...


@dataclass(frozen=True)
class ResearchSnapshot:
    """compute() 的返回：已入库 snapshot 的句柄（D07 §3）。"""

    snapshot_id: str
    output_hash: str
    datasets: list[dict[str, str]]
    params: dict[str, Any]


@dataclass(frozen=True)
class SnapshotRecord:
    """research_snapshot 行的只读投影（get_snapshot 返回）。"""

    snapshot_id: str
    created_at: str
    datasets: list[dict[str, str]]
    code_version: str
    params: dict[str, Any]
    output_hash: str
    notebook_ref: str | None
    query_text: str | None


@dataclass(frozen=True)
class ReproduceResult:
    """snapshot_reproduce 结果（D07 §3 复现契约）。"""

    snapshot_id: str
    hash_match: bool
    original_hash: str
    new_hash: str
    version_changes: dict[str, dict[str, str]] | None


def _utc_now_iso() -> str:
    """naive-UTC ISO 时间戳（canonical 层 timestamp 存储约定，D03 §3）。"""
    return datetime.now(UTC).replace(tzinfo=None).isoformat()


def _version_string(registry: DatasetRegistry, dataset_id: str) -> str:
    """当前 dataset_version（D07 §1：最近 SUCCESS run 的 code+schema 复合）。"""
    info = registry.version_info(dataset_id)
    if info is None:
        return ""
    return f"{info.code_version}+{info.schema_version}"


def _frame_output_hash(frame: pl.DataFrame) -> str:
    """结果序列化哈希（sha256）：按全部列升序排序后写 Arrow IPC 字节。

    排序消除行序随机性；IPC 二进制保证同 schema+数据字节确定，且
    null 与空串可区分（查询层默认稳定排序之外的二次防线）。
    """
    ordered = frame.sort(pl.all())
    buf = io.BytesIO()
    ordered.write_ipc(buf)
    return hashlib.sha256(buf.getvalue()).hexdigest()


class ResearchSnapshotContext:
    """snapshot 执行上下文（research_snapshot yield 的对象）。"""

    def __init__(
        self,
        *,
        snapshot_id: str,
        datasets: list[dict[str, str]],
        params: dict[str, Any],
        qs: DuckDBQueryService,
        meta: MetaStoreLike,
    ) -> None:
        self.snapshot_id = snapshot_id
        self.datasets = datasets
        self.params = params
        self.qs = qs
        self.meta = meta
        self.notebook_ref: str | None = None
        self.query_text: str | None = None

    def compute(self, query_func: QueryFunc) -> ResearchSnapshot:
        """执行研究并创建 snapshot 记录（不可变：同一 snapshot 仅可落库一次）。

        Args:
            query_func: 研究查询函数（接收 qs，返回 polars DataFrame）。

        Returns:
            ResearchSnapshot（snapshot_id + output_hash + 锁定版本 + 参数）。

        Raises:
            TypeError: params 含 JSON 不可序列化对象（json.dumps 抛出）。
            ValueError: snapshot 已存在（重复 compute，快照不可变）。
            RuntimeError: SQLite 写入失败（同 features/engine.py 模式，
                不导入 chronoforge.exceptions 以避免触发存量
                exceptions→connectors.errors 层级断裂链）。
        """
        result = query_func(self.qs)
        output_hash = _frame_output_hash(result)

        try:
            self.meta.connection.execute(
                "INSERT INTO research_snapshot("
                "snapshot_id, created_at, datasets_json, code_version, "
                "params_json, output_hash, notebook_ref, query_text) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    self.snapshot_id,
                    _utc_now_iso(),
                    json.dumps(self.datasets),
                    __version__,
                    json.dumps(self.params, sort_keys=True),
                    output_hash,
                    self.notebook_ref,
                    self.query_text,
                ),
            )
            self.meta.connection.commit()
        except sqlite3.IntegrityError as e:
            self.meta.connection.rollback()
            raise ValueError(
                f"Snapshot already exists (immutable): {self.snapshot_id}"
            ) from e
        except sqlite3.Error as e:
            self.meta.connection.rollback()
            raise RuntimeError(
                f"Failed to insert snapshot {self.snapshot_id}: {e}"
            ) from e

        logger.info(
            "research.snapshot_created",
            snapshot_id=self.snapshot_id,
            output_hash=output_hash,
            datasets=[d["dataset_id"] for d in self.datasets],
        )
        return ResearchSnapshot(
            snapshot_id=self.snapshot_id,
            output_hash=output_hash,
            datasets=self.datasets,
            params=self.params,
        )


@contextmanager
def research_snapshot(
    qs: DuckDBQueryService,
    datasets: list[str],
    params: dict[str, Any],
    meta: MetaStoreLike,
) -> Iterator[ResearchSnapshotContext]:
    """研究快照上下文（D07 §3）。

    进入：校验 datasets 并锁定各 dataset 当前版本（锁定 = 登记 dataset_version
    快照，非 pipeline 层数据集运行锁）；退出：无强制动作（落库由
    ctx.compute() 触发，研究抛异常时不产生记录）。

    Args:
        qs: 只读查询服务（compute 时传给 query_func）。
        datasets: 参与 snapshot 的 dataset_id 列表（非空）。
        params: 研究参数（JSON 可序列化；入库按 key 排序保证确定性）。
        meta: MetaStore 实例（版本锁定 + snapshot 落库）。

    Yields:
        ResearchSnapshotContext。

    Raises:
        ValueError: datasets 为空，或任一 dataset_id 未在 dataset_registry 登记。
    """
    if not datasets:
        raise ValueError("datasets must not be empty")

    registry = DatasetRegistry(meta.connection)
    locked_datasets: list[dict[str, str]] = []
    for ds_id in datasets:
        if registry.get(ds_id) is None:
            raise ValueError(f"Dataset not found: {ds_id}")
        locked_datasets.append({
            "dataset_id": ds_id,
            "dataset_version": _version_string(registry, ds_id),
        })

    yield ResearchSnapshotContext(
        snapshot_id=uuid4().hex[:12],
        datasets=locked_datasets,
        params=params,
        qs=qs,
        meta=meta,
    )


def get_snapshot(meta: MetaStoreLike, snapshot_id: str) -> SnapshotRecord | None:
    """按 ID 读取 snapshot 记录；不存在返回 None。"""
    row = meta.connection.execute(
        "SELECT snapshot_id, created_at, datasets_json, code_version, "
        "params_json, output_hash, notebook_ref, query_text "
        "FROM research_snapshot WHERE snapshot_id = ?",
        (snapshot_id,),
    ).fetchone()
    if row is None:
        return None
    return SnapshotRecord(
        snapshot_id=str(row[0]),
        created_at=str(row[1]),
        datasets=cast(list[dict[str, str]], json.loads(str(row[2]))),
        code_version=str(row[3]),
        params=cast(dict[str, Any], json.loads(str(row[4]))),
        output_hash=str(row[5]),
        notebook_ref=str(row[6]) if row[6] is not None else None,
        query_text=str(row[7]) if row[7] is not None else None,
    )


def snapshot_reproduce(
    snapshot_id: str,
    meta: MetaStoreLike,
    qs: DuckDBQueryService,
    query_func: QueryFunc,
) -> ReproduceResult:
    """复现 snapshot（D07 §3 / 架构 02 §5 复现契约）。

    重跑 query_func 并比对 output_hash；不一致时对比 snapshot 锁定版本与
    当前 dataset_version，报告变化项（已注销 dataset 跳过）。

    Raises:
        ValueError: snapshot_id 不存在。
    """
    snapshot = get_snapshot(meta, snapshot_id)
    if snapshot is None:
        raise ValueError(f"Snapshot not found: {snapshot_id}")

    result = query_func(qs)
    new_hash = _frame_output_hash(result)
    hash_match = new_hash == snapshot.output_hash

    version_changes: dict[str, dict[str, str]] | None = None
    if not hash_match:
        registry = DatasetRegistry(meta.connection)
        changes: dict[str, dict[str, str]] = {}
        for ds_info in snapshot.datasets:
            ds_id = ds_info["dataset_id"]
            if registry.get(ds_id) is None:
                continue
            current = _version_string(registry, ds_id)
            if current != ds_info["dataset_version"]:
                changes[ds_id] = {
                    "snapshot_version": ds_info["dataset_version"],
                    "current_version": current,
                }
        version_changes = changes

    return ReproduceResult(
        snapshot_id=snapshot_id,
        hash_match=hash_match,
        original_hash=snapshot.output_hash,
        new_hash=new_hash,
        version_changes=version_changes,
    )
