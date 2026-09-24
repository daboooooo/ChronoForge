"""CLI 组合根（架构 02 规则 3：cli 层零业务逻辑）。

职责边界：仅做「构造 service 依赖 + 只读渲染投影」，不含任何业务规则：
- wire_*：Settings → MetaStore/RawStore/CanonicalStore/connector/PipelineRunner
- open_query_service：DuckDB 目录（meta_dir/query.duckdb）只读探测 +
  惰性写模式注册（READY-005 方案 A，稳态零写窗口）+ read_only 查询
- list_runs / list_quality_flags：run_log / quality_flags 的只读投影
  （渲染用 SELECT，经 MetaStore.connection 公共 API；偏差登记见 CLI-001.md）
- build_connector：source_id → connector 工厂（fred 缺 key → ConfigError，TC-X-002）

业务规则一律在 pipeline/research/quality/registry 各 service 内。
"""

from __future__ import annotations

import contextlib
import json
import time
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import duckdb

from chronoforge.config.settings import Settings
from chronoforge.connectors.base import DataConnector
from chronoforge.connectors.ccxt_bridge import CcxtBridgeConnector
from chronoforge.connectors.deribit import DeribitConnector
from chronoforge.connectors.errors import ConfigError
from chronoforge.connectors.fred import FREDConnector
from chronoforge.connectors.sec_edgar import SECEdgarConnector
from chronoforge.pipeline.runner import PipelineRunner, RunContext
from chronoforge.registry.service import get_dataset
from chronoforge.storage.canonical import CanonicalStoreImpl
from chronoforge.storage.consistency import startup_repair
from chronoforge.storage.meta import MetaStore
from chronoforge.storage.raw import RawStore
from chronoforge.storage.views import missing_views, register_views

__all__ = [
    "build_connector",
    "build_runner",
    "enter_closeable",
    "list_quality_flags",
    "list_runs",
    "open_meta",
    "open_query_service",
    "parse_dt",
    "to_jsonable",
]


def open_meta(settings: Settings, *, repair: bool = False) -> MetaStore:
    """打开 MetaStore 并确保迁移（CLI 各命令共用入口）；repair 时执行启动修复。

    审计 SR-01：startup_repair（release_stale_locks → cleanup_orphans →
    reconcile，D03 §5 顺序）实现完整但此前生产路径从未调用——进程 crash
    后 dataset 锁永久滞留。

    R2-03③（审计 2026-09-22）：startup_repair 收敛到写入口（pipeline
    run/replay 显式传 repair=True）——读命令（status/query/registry/
    quality/research）默认免修复，避免 cron 重叠期间对账/清理误伤在途
    run（此前 docstring 自认的「已知权衡」由此收敛）。护栏细节见
    consistency.startup_repair（年龄护栏 + 租约下限，R2-03①②）。
    """
    meta = MetaStore(str(settings.meta_dir))
    meta.migrate()
    if repair:
        startup_repair(
            meta,
            RawStore(str(settings.data_dir)),
            CanonicalStoreImpl(str(settings.data_dir)),
            str(settings.data_dir),
        )
    return meta


def build_connector(
    source_id: str, settings: Settings, params: dict[str, Any]
) -> DataConnector:
    """按 source_id 构造 connector（D04 §4；CLI 不直依赖具体源逻辑）。

    Raises:
        ConfigError: 未知 source_id，或必需凭证缺失（FRED 无 key，TC-X-002）。
    """
    if source_id == "binance_spot":
        from chronoforge.connectors.binance_spot import BinanceSpotConnector

        return BinanceSpotConnector(settings=settings)
    if source_id == "binance_futures":
        from chronoforge.connectors.binance_futures import BinanceFuturesConnector

        return BinanceFuturesConnector(settings=settings)
    if source_id == "deribit":
        return DeribitConnector(settings=settings)
    if source_id == "ccxt":
        exchange = str(params.get("exchange", "binance"))
        return CcxtBridgeConnector(settings=settings, exchange=exchange)
    if source_id == "yahoo":
        from chronoforge.connectors.yahoo import YahooConnector

        return YahooConnector(settings=settings)
    if source_id == "fred":
        # D08 §1：缺失且启用 FRED → ConfigError 启动失败（TC-X-002）
        if settings.fred_api_key.is_empty():
            raise ConfigError(
                "FRED_API_KEY is required for source 'fred' (D08 §1)",
                context={"source": "fred"},
            )
        return FREDConnector(settings=settings)
    if source_id == "sec_edgar":
        # SECEdgarConnector 需要 str 型 sec_contact_email（_SECSettings 结构）
        sec_settings = SimpleNamespace(
            sec_contact_email=settings.sec_contact_email.get_secret_value(),
            http_timeout_s=settings.http_timeout_s,
        )
        return SECEdgarConnector(sec_settings)  # type: ignore[arg-type]
    raise ConfigError(
        f"Unknown source_id: {source_id}", context={"source": source_id}
    )


def build_runner(
    settings: Settings,
    meta: MetaStore,
    connector: DataConnector,
    source_id: str,
    stack: contextlib.ExitStack | None = None,
) -> PipelineRunner:
    """装配 PipelineRunner（ctx_factory 构造 RunContext，run_id 由 runner 回填）。

    R2-07（审计 2026-09-22）：stack 提供时把 runner 内部常驻句柄的
    RawStore（READY-002，append 有 close()）注册进 ExitStack——CLI 每
    dataset 迭代的 job_stack 关闭时释放，进程常驻化前不依赖退出兜底。
    """
    raw = RawStore(str(settings.data_dir))
    canonical = CanonicalStoreImpl(str(settings.data_dir))
    if stack is not None:
        enter_closeable(stack, raw)

    def _ctx_factory(job: Any) -> RunContext:
        return RunContext(
            run_id="",
            ingest_batch_id="",
            source_id=source_id,
            dataset_id=job.dataset_id,
            connector=connector,
            raw_store=raw,
            canonical_store=canonical,
            meta=meta,
            settings=settings,
        )

    return PipelineRunner(_ctx_factory)


def enter_closeable(stack: contextlib.ExitStack, resource: Any) -> None:
    """把 resource.close() 注册进 stack（审计 SR-11 资源统一管理）。

    close() 是 DataConnector 协议外的可选能力（各实现均有、协议未声明），
    经 getattr 防御注册：缺失则跳过（如测试以 object() 桩替换 connector）。
    """
    close = getattr(resource, "close", None)
    if close is not None:
        stack.callback(close)


# READY-005（SR-16）：写模式注册窗口的文件锁冲突有界退避
# （仅首次创建/新增类型才进入写路径；0.1s 起指数退避，最长 ~1.5s）
_WRITE_OPEN_ATTEMPTS = 5
_WRITE_OPEN_BACKOFF_S = 0.1


def _register_and_reopen(
    catalog_path: Path, data_dir: str
) -> duckdb.DuckDBPyConnection:
    """READY-005 写路径：幂等注册视图后 read_only 重开（SR-16）。

    仅在快路径探测到视图缺失/文件缺失时进入。写打开遇文件锁冲突
    （DuckDB 单文件单写连接）时按指数退避重试，且每次冲突后**只读复探**：
    冲突持有者若已代为完成注册（并发首次创建的典型时序），直接复用其
    成果返回，无需再抢写锁——注册幂等，收敛语义为「视图齐全即可用」。
    重试耗尽仍无法注册/复探 → IOException 有界上抛。
    """
    delay = _WRITE_OPEN_BACKOFF_S
    for attempt in range(1, _WRITE_OPEN_ATTEMPTS + 1):
        try:
            con_write = duckdb.connect(str(catalog_path))
        except duckdb.IOException:
            # 他进程持锁（注册中/查询中）→ 复探视图是否已被其注册齐全
            con_ro = _open_ready_readonly(catalog_path, data_dir)
            if con_ro is not None:
                return con_ro
            if attempt == _WRITE_OPEN_ATTEMPTS:
                raise
            time.sleep(delay)
            delay *= 2
        else:
            register_views(con_write, data_dir)
            con_write.close()
            break
    return duckdb.connect(str(catalog_path), read_only=True)


def _open_ready_readonly(
    catalog_path: Path, data_dir: str
) -> duckdb.DuckDBPyConnection | None:
    """READY-005 方案 A 快路径：只读打开 + 视图完备性探测（SR-16）。

    返回可用的 read_only 连接（视图齐全，稳态零写窗口，多进程只读连接
    天然并发安全）；文件缺失、打开遇锁冲突（他进程注册窗口）或视图缺失
    时返回 None，由调用方进入写路径。
    """
    if not catalog_path.exists():
        return None
    con: duckdb.DuckDBPyConnection | None = None
    try:
        con = duckdb.connect(str(catalog_path), read_only=True)
        if not missing_views(con, data_dir):
            return con
    except duckdb.IOException:
        pass  # 他进程持写锁（注册窗口）→ 调用方走写路径退避
    if con is not None:
        con.close()
    return None


def open_query_service(
    settings: Settings, meta: MetaStore
) -> tuple[Any, duckdb.DuckDBPyConnection]:
    """构造只读 QueryService（D07 §1）。

    DuckDB 目录文件位于 meta_dir/query.duckdb（架构 02 规则 4）。
    READY-005（SR-16）方案 A——只读探测优先：视图已齐全时直接 read_only
    使用（不进写模式）；仅 query.duckdb 缺失（首次创建）或新增 canonical
    类型（视图缺失）时以写模式幂等注册，锁冲突经「退避 + 只读复探」收敛。
    """
    from chronoforge.research.query import DatasetRegistry, DuckDBQueryService

    catalog_path = settings.meta_dir / "query.duckdb"
    data_dir = str(settings.data_dir)
    con_ro = _open_ready_readonly(catalog_path, data_dir)
    if con_ro is None:
        con_ro = _register_and_reopen(catalog_path, data_dir)

    # READY-003：query_max_rows 为可调项（env CHRONOFORGE_QUERY_MAX_ROWS），
    # 组合根注入使其对 CLI 生效（偏差登记：file_ownership 外 1 行接线）
    service = DuckDBQueryService(
        con_ro, DatasetRegistry(meta.connection), max_rows=settings.query_max_rows
    )
    return service, con_ro


def list_runs(
    meta: MetaStore, dataset_id: str | None = None, last: int = 10
) -> list[dict[str, Any]]:
    """run_log 只读投影（pipeline status 渲染用，新→旧）。"""
    sql = "SELECT * FROM run_log"
    params: list[Any] = []
    if dataset_id is not None:
        sql += " WHERE dataset_id = ?"
        params.append(dataset_id)
    sql += " ORDER BY started_at DESC LIMIT ?"
    params.append(last)
    cur = meta.connection.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]


def list_quality_flags(
    meta: MetaStore, dataset_id: str | None = None
) -> list[dict[str, Any]]:
    """quality_flags 只读投影（quality report 渲染用，新→旧）。"""
    sql = "SELECT * FROM quality_flags"
    params: list[Any] = []
    if dataset_id is not None:
        sql += " WHERE dataset_id = ?"
        params.append(dataset_id)
    sql += " ORDER BY created_at DESC"
    cur = meta.connection.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]


def parse_dt(value: str | None) -> datetime | None:
    """ISO 字符串 → naive-UTC datetime（D01 §4 存储约定；None 透传）。"""
    if value is None:
        return None
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is not None:
        dt = dt.astimezone(UTC).replace(tzinfo=None)
    return dt


def to_jsonable(obj: Any) -> Any:
    """JSON 安全化（QueryResult/RunRow/flags 渲染用；datetime→ISO、Decimal→str）。"""
    if isinstance(obj, dict):
        return {k: to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [to_jsonable(v) for v in obj]
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, bytes):
        return obj.hex()
    return obj


def load_dataset_params(meta: MetaStore, dataset_id: str) -> dict[str, Any]:
    """读取 dataset_registry.params_json 为 dict（pipeline run 装配参数用）。"""
    row = get_dataset(meta, dataset_id)
    if row is None:
        raise ConfigError(
            f"Dataset not found in registry: {dataset_id}",
            context={"dataset_id": dataset_id},
        )
    raw = row.get("params_json") or "{}"
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ConfigError(
            f"Invalid params_json for dataset {dataset_id}: {exc}",
            context={"dataset_id": dataset_id},
        ) from exc
    return parsed if isinstance(parsed, dict) else {}
