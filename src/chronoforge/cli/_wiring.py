"""CLI 组合根（架构 02 规则 3：cli 层零业务逻辑）。

职责边界：仅做「构造 service 依赖 + 只读渲染投影」，不含任何业务规则：
- wire_*：Settings → MetaStore/RawStore/CanonicalStore/connector/PipelineRunner
- open_query_service：DuckDB 目录（meta_dir/query.duckdb）+ 视图注册 + read_only 重开
- list_runs / list_quality_flags：run_log / quality_flags 的只读投影
  （渲染用 SELECT，经 MetaStore.connection 公共 API；偏差登记见 CLI-001.md）
- build_connector：source_id → connector 工厂（fred 缺 key → ConfigError，TC-X-002）

业务规则一律在 pipeline/research/quality/registry 各 service 内。
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal
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
from chronoforge.storage.views import register_views

__all__ = [
    "build_connector",
    "build_runner",
    "list_quality_flags",
    "list_runs",
    "open_meta",
    "open_query_service",
    "parse_dt",
    "to_jsonable",
]


def open_meta(settings: Settings) -> MetaStore:
    """打开 MetaStore 并确保迁移 + 启动修复（CLI 各命令共用入口）。

    审计 SR-01：startup_repair（release_stale_locks → cleanup_orphans →
    reconcile，D03 §5 顺序）实现完整但此前生产路径从未调用——进程 crash
    后 dataset 锁永久滞留。接线于此，每次 CLI 打开元数据时执行启动修复。

    注：reconcile 无租约护栏，若另一进程正在运行长任务，本命令会将其
    PENDING/RUNNING 行误判为孤儿并补记终态（单机单写者模型下的已知
    权衡；finish_run 会以真实终态覆盖，数据层经幂等 upsert 收敛）。
    """
    meta = MetaStore(str(settings.meta_dir))
    meta.migrate()
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
) -> PipelineRunner:
    """装配 PipelineRunner（ctx_factory 构造 RunContext，run_id 由 runner 回填）。"""
    raw = RawStore(str(settings.data_dir))
    canonical = CanonicalStoreImpl(str(settings.data_dir))

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


def open_query_service(
    settings: Settings, meta: MetaStore
) -> tuple[Any, duckdb.DuckDBPyConnection]:
    """构造只读 QueryService（D07 §1）。

    DuckDB 目录文件位于 meta_dir/query.duckdb：首次（或文件缺失视图时）
    write 模式注册视图后关闭，再以 read_only 重开供查询（架构 02 规则 4）。
    """
    from chronoforge.research.query import DatasetRegistry, DuckDBQueryService

    catalog_path = settings.meta_dir / "query.duckdb"
    con_write = duckdb.connect(str(catalog_path))
    register_views(con_write, str(settings.data_dir))
    con_write.close()

    con_ro = duckdb.connect(str(catalog_path), read_only=True)
    service = DuckDBQueryService(con_ro, DatasetRegistry(meta.connection))
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
