"""FRED (Federal Reserve Economic Data) connector（D04 §4.6，DATA-SOURCE-006）。

通过 FRED REST API 获取宏观经济指标：
- `/series/observations` — 系列观测值（NUMBER 类型）
- `/release/dates` — 发布日历（用于 release_time）
- 支持修订追踪（revision_supported=true）
- 保守限流：120 req/min（FRED 官方限制）

时间语义（审计 F-09）：
- observation `date` 日期型 → T00:00:00Z
- release 日期无时刻 → T23:59:59Z（防 look-ahead）
- vintage = realtime_date
- continuity_model: RELEASE_SCHEDULE

"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from chronoforge.config.settings import Settings
from chronoforge.connectors.base import (
    CapabilityMatrix,
    DataConnector,
    FetchRequest,
    HealthStatus,
    InstrumentRef,
    RawBatch,
)
from chronoforge.connectors.errors import (
    ProviderError,
    RateLimitError,
    TransportError,
)
from chronoforge.connectors.ratelimit import RateLimiter
from chronoforge.models.base import BaseRecord
from chronoforge.models.enums import CanonicalType, QualityStatus
from chronoforge.models.macro import NUMBER
from chronoforge.models.quality import QualityFinding, QualityReport
from chronoforge.security import SecretStr

# FRED 官方限流：120 req/min
_DEFAULT_RATE = 120.0  # requests per minute
_DEFAULT_BURST = 120


@dataclass(frozen=True)
class _FREDSettings:
    """FRED 连接器最小配置（向后兼容）。"""

    api_key: str
    http_timeout_s: float = 30.0


class FREDConnector(DataConnector):
    """FRED 数据连接器（D04 §4.6）。

    Args:
        settings: 配置对象。可以是 Settings（主入口）或 _FREDSettings（向后兼容）。
                  如果为 None，则调用 Settings.load() 自动加载。
    """

    source_id = "fred"

    def __init__(self, settings: Settings | _FREDSettings | None = None) -> None:
        if settings is None:
            settings = Settings.load()

        if isinstance(settings, Settings):
            self._api_key = settings.fred_api_key.get_secret_value()
            self._http_timeout_s = settings.http_timeout_s
        else:
            # _FREDSettings backward compat
            key = settings.api_key
            # SecretStr 防误传：httpx 会把 SecretStr 序列化成 "******" 发出
            self._api_key = (
                key.get_secret_value() if isinstance(key, SecretStr) else key
            )
            self._http_timeout_s = settings.http_timeout_s

        self._client = self._init_client()
        self._rate_limiter = RateLimiter(
            rate=_DEFAULT_RATE / 60.0,
            burst=_DEFAULT_BURST,
        )

    def _init_client(self) -> httpx.Client:
        """初始化 httpx Client。"""
        return httpx.Client(
            base_url="https://api.stlouisfed.org/fred",
            timeout=self._http_timeout_s,
        )

    def capabilities(self) -> CapabilityMatrix:
        """能力矩阵（D04 §4.6）。"""
        return CapabilityMatrix(
            canonical_types=frozenset({
                CanonicalType.NUMBER,
                CanonicalType.FLOW,
                CanonicalType.MACRO_EVENT,
            }),
            intervals=frozenset(),  # FRED 无 interval 概念
            supports_revision=True,
            supports_websocket=False,
            max_history_days=None,  # 无限制
        )

    def health(self) -> HealthStatus:
        """健康检查（D04 §1）。"""
        import time

        start = time.monotonic()
        try:
            # 简单检查：用已知 series_id 查询
            self._fetch_observations("UNRATE", obs_start=None, obs_end=None)
            elapsed_ms = int((time.monotonic() - start) * 1000)
            return HealthStatus(ok=True, latency_ms=elapsed_ms)
        except Exception as exc:
            elapsed_ms = int((time.monotonic() - start) * 1000)
            return HealthStatus(
                ok=False, latency_ms=elapsed_ms, detail=str(exc)
            )

    def discover(self) -> list[InstrumentRef]:
        """FRED discover 可选（P0 不实现，系列静态注册）。"""
        return []

    def fetch(self, request: FetchRequest) -> Iterator[RawBatch]:
        """获取 FRED 观测值数据（D04 §4.6）。

        Args:
            request: 包含 series_id 参数，start/end 为观测时间窗口。

        Yields:
            RawBatch 迭代器。
        """
        series_id = request.params.get("series_id", "")
        if not series_id:
            raise ValueError("fred: series_id is required in request.params")

        obs_start = (
            request.start.strftime("%Y-%m-%d") if request.start else None
        )
        obs_end = (
            request.end.strftime("%Y-%m-%d") if request.end else None
        )

        yield self._fetch_observations(series_id, obs_start, obs_end)

    def _fetch_observations(
        self,
        series_id: str,
        obs_start: str | None = None,
        obs_end: str | None = None,
    ) -> RawBatch:
        """请求 FRED observations 端点（D04 §4.6）。

        Args:
            series_id: FRED 系列 ID（如 CPIAUCSL, UNRATE）。
            obs_start: 观测起始日期（YYYY-MM-DD）。
            obs_end: 观测结束日期（YYYY-MM-DD）。

        Returns:
            RawBatch 原始响应。
        """
        params: dict[str, Any] = {
            "series_id": series_id,
            "api_key": self._api_key,
            "file_type": "json",
        }
        if obs_start:
            params["observation_start"] = obs_start
        if obs_end:
            params["observation_end"] = obs_end

        self._rate_limiter.acquire(1)

        try:
            response = self._client.get(
                "/series/observations", params=params
            )
        except httpx.TransportError as e:
            raise TransportError(
                f"fred: observations request failed: {e}",
                context={
                    "source": "fred",
                    "endpoint": "/series/observations",
                    "series_id": series_id,
                },
            ) from e

        status_code = response.status_code

        # 错误映射（D04 §3）
        if status_code == 429:
            raise RateLimitError(
                f"fred: rate limited on /series/observations/{series_id}",
                context={
                    "source": "fred",
                    "endpoint": "/series/observations",
                    "series_id": series_id,
                },
                retry_after=1,
            )
        if status_code in (401, 403):
            raise ProviderError(
                f"fred: auth failed for series {series_id}",
                context={
                    "source": "fred",
                    "endpoint": "/series/observations",
                    "status": status_code,
                    "series_id": series_id,
                },
            )
        if status_code >= 400:
            raise ProviderError(
                f"fred: HTTP {status_code} on observations endpoint",
                context={
                    "source": "fred",
                    "endpoint": "/series/observations",
                    "status": status_code,
                    "series_id": series_id,
                },
            )

        data = response.json()
        return RawBatch(
            endpoint="/series/observations",
            payload=data,
            raw_meta={
                "http_status": status_code,
                "series_id": series_id,
            },
        )

    def _fetch_release_dates(self) -> list[dict[str, Any]]:
        """获取 FRED 发布日历（D04 §4.6）。

        Returns:
            发布日历列表。
        """
        self._rate_limiter.acquire(1)

        response = self._client.get(
            "/release/dates",
            params={
                "api_key": self._api_key,
                "file_type": "json",
            },
        )

        if response.status_code != 200:
            raise ProviderError(
                f"fred: HTTP {response.status_code} on release/dates",
                context={
                    "source": "fred",
                    "endpoint": "/release/dates",
                    "status": response.status_code,
                },
            )

        return response.json().get("releasedates", [])  # type: ignore[no-any-return]

    def normalize(self, raw: RawBatch) -> list[BaseRecord]:
        """将 FRED observations 响应转为 NUMBER 记录（D04 §4.6）。

        时间语义（审计 F-09）：
        - observation date → T00:00:00Z
        - release 日期 → T23:59:59Z（保守，防 look-ahead）
        - vintage = realtime_date

        Args:
            raw: RawBatch from fetch().

        Returns:
            list of NUMBER records.
        """
        if raw.endpoint != "/series/observations":
            raise ValueError(f"fred: unknown endpoint: {raw.endpoint}")

        payload = raw.payload
        if not isinstance(payload, dict):
            return []

        observations = payload.get("observations")
        if not observations or not isinstance(observations, list):
            return []

        # FRED observations 响应不回显 series_id → 从 raw_meta 回退
        series_id = payload.get("series_id") or raw.raw_meta.get("series_id", "")
        units = payload.get("units", "")
        seasonal_adjustment = payload.get("seasonal_adjustment", "")

        records: list[NUMBER] = []
        now = datetime.now(UTC).replace(tzinfo=None)

        for obs in observations:
            date_str = obs.get("date", "")
            if not date_str:
                continue

            value_str = obs.get("value", "")
            realtime_date = obs.get("realtime_start", "")

            # 解析 observation 日期 → T00:00:00Z（审计 F-09）
            try:
                year = int(date_str[:4])
                month = int(date_str[5:7])
                day = int(date_str[8:10])
                observation_time = datetime(
                    year=year, month=month, day=day,
                    hour=0, minute=0, second=0
                )
            except (ValueError, IndexError):
                continue

            # release_time：T23:59:59Z 保守（防 look-ahead）
            if realtime_date and len(realtime_date) >= 10:
                try:
                    r_year = int(realtime_date[:4])
                    r_month = int(realtime_date[5:7])
                    r_day = int(realtime_date[8:10])
                    release_time = datetime(
                        year=r_year, month=r_month, day=r_day,
                        hour=23, minute=59, second=59
                    )
                except (ValueError, IndexError):
                    release_time = observation_time + timedelta(days=1)
            else:
                # 无 realtime_date → 观察日次日
                release_time = observation_time + timedelta(days=1)

            revision_time = release_time

            # value: "." = missing → None
            if value_str == "." or value_str == "":
                value: float | None = None
            else:
                try:
                    value = float(value_str)
                except (ValueError, TypeError):
                    continue

            record = NUMBER(
                schema_version="1.0",
                source="fred",
                source_id=series_id,
                source_timestamp=observation_time,
                ingest_timestamp=now,
                raw_record_id=f"fred:{series_id}:{date_str}",
                quality_status=QualityStatus.VALID,
                quality_reason=None,
                observation_time=observation_time,
                release_time=release_time,
                revision_time=revision_time,
                value=value,
                units=units,
                seasonal_adjustment=seasonal_adjustment,
                vintage_date=realtime_date or "",
            )
            records.append(record)

        return list[BaseRecord](records)

    def validate(self, records: list[BaseRecord]) -> QualityReport:
        """验证 NUMBER 记录（D04 §1，委托 quality.rules）。

        P0：基础校验（release_time >= observation_time）。
        """
        findings: list[QualityFinding] = []

        for record in records:
            if not isinstance(record, NUMBER):
                continue

            key = str(record.natural_key())

            # release_time >= observation_time
            if record.release_time < record.observation_time:
                findings.append(QualityFinding(
                    record_key=key,
                    rule_id="Q-TS-001",
                    severity="ERROR",
                    detail=(
                        f"release_time {record.release_time} "
                        f"< observation_time {record.observation_time}"
                    ),
                ))

            # revision_time >= observation_time
            if record.revision_time < record.observation_time:
                findings.append(QualityFinding(
                    record_key=key,
                    rule_id="Q-TS-002",
                    severity="ERROR",
                    detail=(
                        f"revision_time {record.revision_time} "
                        f"< observation_time {record.observation_time}"
                    ),
                ))

        return QualityReport(findings=findings)

    def checkpoint_from(
        self, raw: RawBatch | list[BaseRecord]
    ) -> str | None:
        """从 raw batch 或 records 中提取 checkpoint cursor（D04 §1）。

        返回最后一个观测值的日期字符串（YYYY-MM-DD）。
        """
        if isinstance(raw, RawBatch):
            payload = raw.payload
            if isinstance(payload, dict):
                observations = payload.get("observations", [])
                if observations:
                    last_obs = observations[-1]
                    date_str = last_obs.get("date", "")
                    if date_str:
                        return date_str  # type: ignore[no-any-return]

        elif isinstance(raw, list) and raw and len(raw) > 0:
            last_record = raw[-1]
            if isinstance(last_record, NUMBER):
                return last_record.observation_time.strftime("%Y-%m-%d")

        return None

    def close(self) -> None:
        """关闭 HTTP 客户端。"""
        try:
            self._client.close()
        except Exception:
            pass

    def __del__(self) -> None:
        """确保资源清理。"""
        try:
            self.close()
        except Exception:
            pass
