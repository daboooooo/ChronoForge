"""DataConnector 协议定义（D04 §1）。

包含：
- FetchRequest：获取请求（frozen）
- RawBatch：原始响应批次
- CapabilityMatrix：连接器能力矩阵（frozen）
- HealthStatus：健康检查状态
- DataConnector：协议基类（Protocol）

连接器不落盘、不写库（D04 §1 边界规则）。
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol

from chronoforge.models.derivatives import Interval
from chronoforge.models.enums import CanonicalType
from chronoforge.models.quality import QualityReport


@dataclass(frozen=True)
class FetchRequest:
    """获取请求（D04 §1，frozen 保证不可变）。

    Args:
        dataset_id: 数据集唯一标识
        params: 源侧查询参数字典（如 {"symbol":"BTCUSDT","interval":"1m"}）
        start: 起始时间（UTC naive）
        end: 结束时间（UTC naive）
        cursor: 增量续传游标（源语义，由各源定义）
    """

    dataset_id: str
    params: Mapping[str, Any]
    start: datetime | None
    end: datetime | None
    cursor: str | None


@dataclass
class RawBatch:
    """原始响应批次（D04 §1）。

    payload 保持源响应原样（不清洗），下游 fixture 对比的基准。
    raw_meta.fetched_at = datetime.now(UTC).replace(tzinfo=None)。
    """

    endpoint: str
    payload: Any
    raw_meta: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """自动注入 fetched_at 时间戳（UTC naive）。"""
        if "fetched_at" not in self.raw_meta:
            object.__setattr__(
                self,
                "raw_meta",
                {**self.raw_meta, "fetched_at": datetime.now(UTC).replace(tzinfo=None)},
            )


@dataclass(frozen=True)
class CapabilityMatrix:
    """连接器能力矩阵（D04 §1，frozen 保证不可变）。

    Args:
        canonical_types: 支持的 Canonical Type 集合
        intervals: 支持的时间区间粒度
        supports_revision: 是否支持数据修订
        supports_websocket: 是否支持 WebSocket 流式
        max_history_days: 最大历史数据天数（None=无限制）
    """

    canonical_types: frozenset[CanonicalType]
    intervals: frozenset[Interval]
    supports_revision: bool
    supports_websocket: bool
    max_history_days: int | None


@dataclass
class HealthStatus:
    """健康检查状态（D04 §1）。

    Args:
        ok: 是否健康
        latency_ms: 响应延迟（毫秒）
        detail: 详细诊断信息
    """

    ok: bool
    latency_ms: int
    detail: str = ""


# InstrumentRef type alias（D04 §1：{"entity_id", "instrument_id", "market_id"}）
InstrumentRef = dict[str, str]


class DataConnector(Protocol):
    """数据连接器协议（D04 §1）。

    Protocol 边界规则：
    - 连接器不落盘、不写库
    - normalize 输出必含 provenance 字段（raw_record_id 由 pipeline 注入回调 raw_ref_provider）
    - discover 返回 InstrumentRef 字典
    - validate 委托 quality.rules（QualityReport）
    """

    source_id: str

    def capabilities(self) -> CapabilityMatrix: ...

    def health(self) -> HealthStatus: ...

    def discover(self) -> list[InstrumentRef]: ...

    def fetch(self, request: FetchRequest) -> Iterator[RawBatch]: ...

    def normalize(self, raw: RawBatch) -> list[Any]: ...

    def validate(
        self, records: list[Any]
    ) -> QualityReport: ...

    def checkpoint_from(
        self, raw: RawBatch | list[Any]
    ) -> str | None: ...
