"""注册表服务（D04 §5）：source_registry 引导 + dataset_registry 写入/查询。

bootstrap_defaults()：将 D04 §4 P0 源规格写入 source_registry
（含 license 摘要、rate_limit_json），幂等（ON CONFLICT UPDATE）。
CLI `chronoforge registry sync` 暴露。

同层协议模式（同 research/snapshot.py MetaStoreLike）：importlinter layers
契约下 registry 与 storage 同层互斥导入，故以结构化协议引用 MetaStore
（调用方传实例，运行时不导入 chronoforge.storage.meta）。
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any, Protocol

__all__ = [
    "DEFAULT_SOURCES",
    "SourceSpec",
    "add_dataset",
    "bootstrap_defaults",
    "get_dataset",
    "list_datasets",
    "list_sources",
]


class RegistryStoreLike(Protocol):
    """注册表存储最小结构协议（MetaStore 结构化满足）。"""

    @property
    def connection(self) -> sqlite3.Connection: ...


@dataclass(frozen=True)
class SourceSpec:
    """source_registry 默认行（D04 §4 P0 源规格表）。"""

    source_id: str
    display_name: str
    access_type: str  # PUBLIC / PUBLIC_WITH_KEY / AUTHENTICATED
    base_url: str
    rate_limit_json: dict[str, int]
    license: str
    historical_limit_days: int | None = None


# D04 §4 源规格（rate_limit 与 connectors/* 实现常量一致）
DEFAULT_SOURCES: tuple[SourceSpec, ...] = (
    SourceSpec(
        "binance_spot", "Binance Spot", "PUBLIC", "https://api.binance.com",
        {"req_per_min": 1200}, "Binance 公开市场数据（公开 REST 行情，署名来源）",
    ),
    SourceSpec(
        "binance_futures", "Binance USDT-M Futures", "PUBLIC", "https://fapi.binance.com",
        {"req_per_min": 1200}, "Binance 公开市场数据（公开 REST 行情，署名来源）",
    ),
    SourceSpec(
        "deribit", "Deribit", "PUBLIC", "https://www.deribit.com/api/v2",
        {"req_per_min": 100}, "Deribit public API（公开行情）",
    ),
    SourceSpec(
        "ccxt", "CCXT Bridge", "PUBLIC", "ccxt://exchange",
        {"req_per_s": 60}, "随底层交易所公开条款（ccxt 抽象层）",
    ),
    SourceSpec(
        "yahoo", "Yahoo Finance", "PUBLIC", "https://query1.finance.yahoo.com",
        {"req_per_min": 30}, "Yahoo Finance 公开行情（非官方条款，延迟约 15 分钟）",
    ),
    SourceSpec(
        "fred", "FRED (St. Louis Fed)", "PUBLIC_WITH_KEY", "https://api.stlouisfed.org/fred",
        {"req_per_min": 120}, "FRED 公开数据（API key 必填，署名来源）",
    ),
    SourceSpec(
        "sec_edgar", "SEC EDGAR", "PUBLIC", "https://data.sec.gov/submissions",
        {"req_per_s": 10}, "SEC 公开数据（fair-access：UA 必填，≤10 req/s）",
    ),
)

_UPSERT_SOURCE_SQL = """
INSERT INTO source_registry(
  source_id, display_name, access_type, base_url, rate_limit_json,
  historical_limit_days, license, enabled)
VALUES (?, ?, ?, ?, ?, ?, ?, 1)
ON CONFLICT(source_id) DO UPDATE SET
  display_name = excluded.display_name,
  access_type = excluded.access_type,
  base_url = excluded.base_url,
  rate_limit_json = excluded.rate_limit_json,
  historical_limit_days = excluded.historical_limit_days,
  license = excluded.license,
  enabled = 1
"""


def bootstrap_defaults(store: RegistryStoreLike) -> int:
    """将 D04 §4 源规格写入 source_registry（幂等，D04 §5）。

    Returns:
        写入的源数量。
    """
    conn = store.connection
    for spec in DEFAULT_SOURCES:
        conn.execute(
            _UPSERT_SOURCE_SQL,
            (
                spec.source_id,
                spec.display_name,
                spec.access_type,
                spec.base_url,
                json.dumps(spec.rate_limit_json),
                spec.historical_limit_days,
                spec.license,
            ),
        )
    conn.commit()
    return len(DEFAULT_SOURCES)


_ADD_DATASET_SQL = """
INSERT INTO dataset_registry(
  dataset_id, source_id, canonical_type, entity_id, params_json, frequency,
  continuity_model, status, available_from, available_to,
  revision_supported, enabled, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?, 'UNKNOWN', ?, ?, ?, 1,
  datetime('now'))
ON CONFLICT(dataset_id) DO UPDATE SET
  source_id = excluded.source_id,
  canonical_type = excluded.canonical_type,
  entity_id = excluded.entity_id,
  params_json = excluded.params_json,
  frequency = excluded.frequency,
  continuity_model = excluded.continuity_model,
  available_from = excluded.available_from,
  available_to = excluded.available_to,
  revision_supported = excluded.revision_supported,
  enabled = 1
"""


def add_dataset(
    store: RegistryStoreLike,
    *,
    dataset_id: str,
    source_id: str,
    canonical_type: str,
    entity_id: str,
    params: dict[str, Any],
    frequency: str | None = None,
    continuity_model: str = "ALWAYS_OPEN",
    revision_supported: bool = False,
    available_from: str | None = None,
    available_to: str | None = None,
) -> None:
    """登记/更新数据集（dataset_registry 写侧，D08 §2 `dataset add`）。

    Raises:
        sqlite3.IntegrityError: source_id 外键缺失或 continuity_model 非法。
    """
    store.connection.execute(
        _ADD_DATASET_SQL,
        (
            dataset_id,
            source_id,
            canonical_type,
            entity_id,
            json.dumps(params),
            frequency,
            continuity_model,
            available_from,
            available_to,
            int(revision_supported),
        ),
    )
    store.connection.commit()


def get_dataset(store: RegistryStoreLike, dataset_id: str) -> dict[str, Any] | None:
    """按 dataset_id 读取注册行；不存在返回 None（pipeline 装配用）。"""
    return _first_row(
        store.connection.execute(
            "SELECT * FROM dataset_registry WHERE dataset_id = ?", (dataset_id,)
        )
    )


def list_sources(store: RegistryStoreLike) -> list[dict[str, Any]]:
    """source_registry 全量（enabled 含在内）。"""
    return _all_rows(store.connection.execute(
        "SELECT * FROM source_registry ORDER BY source_id"
    ))


def list_datasets(
    store: RegistryStoreLike, *, source_id: str | None = None, enabled_only: bool = False
) -> list[dict[str, Any]]:
    """dataset_registry 列表（--source 过滤；enabled_only 过滤启用项）。"""
    sql = "SELECT * FROM dataset_registry"
    conds: list[str] = []
    params: list[Any] = []
    if source_id is not None:
        conds.append("source_id = ?")
        params.append(source_id)
    if enabled_only:
        conds.append("enabled = 1")
    if conds:
        sql += " WHERE " + " AND ".join(conds)
    sql += " ORDER BY dataset_id"
    return _all_rows(store.connection.execute(sql, params))


def _first_row(cur: sqlite3.Cursor) -> dict[str, Any] | None:
    row = cur.fetchone()
    if row is None:
        return None
    return dict(zip([d[0] for d in cur.description], row, strict=True))


def _all_rows(cur: sqlite3.Cursor) -> list[dict[str, Any]]:
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]
