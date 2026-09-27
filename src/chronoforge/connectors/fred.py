"""FRED (Federal Reserve Economic Data) connector（D04 §4.6，DATA-SOURCE-006）。

通过 FRED REST API 获取宏观经济指标：
- `/series/observations` — 系列观测值（NUMBER 类型）
- `/release/dates` — 发布日历（备用，当前快照语义直接用观测 vintage）
- 保守限流：120 req/min（FRED 官方限制）

快照语义（revision_supported=0，2026-09-26 修订）：
- observations 固定以 realtime_start=1776-07-04 请求全 vintage 历史；
  默认 realtime 窗口（=采集日）会把每条观测的 realtime_start 钳制成
  采集日，导致 revision_time 随跑批漂移、自然键永不撞键 → 全量复制
- normalize 对同一 observation 日期仅保留 vintage（realtime_start）
  最新的一行（= 当前值），revision_time 取该真实 vintage 日：
  跨重跑稳定，重跑即 upsert；真实修订发生时才产生新版本行
- FRED 真实修订稀少，历史旧版本由 number_asof 视图（rn=1）屏蔽

时间语义（审计 F-09）：
- observation `date` 日期型 → T00:00:00Z
- vintage 日期无时刻 → T23:59:59Z（防 look-ahead）
- vintage = realtime_date；无 vintage → 观察日次日
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

# FRED realtime 窗口最早允许日（ALFRED vintage 下界）。固定下限请求
# observations，使每条观测回传其真实 vintage（realtime_start），
# 而非被钳制成采集日——快照语义下 revision_time 必须跨重跑稳定。
_REALTIME_START_FLOOR = "1776-07-04"

# FRED 限制：单个 realtime 窗口内 vintage 日期 ≤2000（超出 HTTP 400）；
# /series/vintagedates 单页 ≤1000。长历史序列按前者切窗分页。
_OBS_VINTAGE_WINDOW = 2000
_VINTAGEDATES_PAGE_SIZE = 1000


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
            # 简单检查：用已知 series_id 查询近 60 天（月频，响应轻量）。
            # 不传 realtime 下限 → 默认窗口（=今天）的当前快照即可。
            recent_start = (
                datetime.now(UTC).date() - timedelta(days=60)
            ).strftime("%Y-%m-%d")
            self._fetch_observations("UNRATE", obs_start=recent_start)
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

        yield self._fetch_observations(
            series_id,
            obs_start,
            obs_end,
            realtime_start=_REALTIME_START_FLOOR,
        )

    def _fetch_observations(
        self,
        series_id: str,
        obs_start: str | None = None,
        obs_end: str | None = None,
        realtime_start: str | None = None,
    ) -> RawBatch:
        """请求 FRED observations 端点（D04 §4.6）。

        全 vintage 模式（realtime_start 非空）下，FRED observations 端点
        限制单个 realtime 窗口内 vintage 日期数 ≤ _OBS_VINTAGE_WINDOW（否则
        HTTP 400；该端点会把非 vintage 日的右端也计一个隐式 vintage，故右端
        必须正好落在 vintage 日上）。先取系列 vintage 日期，按上限切窗：
        每窗 [v[i], v[i+N-1]] 含恰 N 个 vintage 日，末窗口右端不封。逐页
        拉取后合并为单个 RawBatch，合并按 (date, realtime_start) 去重，
        交给 normalize 做快照折叠。

        Args:
            series_id: FRED 系列 ID（如 CPIAUCSL, UNRATE）。
            obs_start: 观测起始日期（YYYY-MM-DD）。
            obs_end: 观测结束日期（YYYY-MM-DD）。
            realtime_start: vintage 窗口下界（YYYY-MM-DD）。生产路径固定
                传 _REALTIME_START_FLOOR 以取回每条观测的真实 vintage；
                None 时用 FRED 默认（=采集日，仅健康检查等轻量场景）。

        Returns:
            RawBatch 合并后的原始响应。
        """
        windows: list[tuple[str | None, str | None]]
        if realtime_start:
            vintage_dates = self._fetch_vintage_dates(
                series_id, realtime_start
            )
            if len(vintage_dates) <= _OBS_VINTAGE_WINDOW:
                windows = [(realtime_start, None)]
            else:
                windows = []
                for i in range(0, len(vintage_dates), _OBS_VINTAGE_WINDOW):
                    rt_start = vintage_dates[i]
                    last_idx = min(
                        i + _OBS_VINTAGE_WINDOW, len(vintage_dates)
                    ) - 1
                    # 右端取本窗最后一个 vintage 日（端点隐式 vintage 计数）
                    rt_end = (
                        vintage_dates[last_idx]
                        if last_idx + 1 < len(vintage_dates)
                        else None
                    )
                    windows.append((rt_start, rt_end))
        else:
            windows = [(None, None)]

        merged_payload: dict[str, Any] | None = None
        seen_rows: set[tuple[str, str]] = set()
        merged_rows: list[dict[str, Any]] = []

        for rt_start, rt_end in windows:
            params: dict[str, Any] = {
                "series_id": series_id,
                "api_key": self._api_key,
                "file_type": "json",
            }
            if obs_start:
                params["observation_start"] = obs_start
            if obs_end:
                params["observation_end"] = obs_end
            if rt_start:
                # 全 vintage 历史：同 observation 日期可回多行（每次修订一行），
                # normalize 折叠为当前值快照
                params["realtime_start"] = rt_start
            if rt_end:
                params["realtime_end"] = rt_end

            data = self._get_json("/series/observations", params, series_id)
            if merged_payload is None:
                merged_payload = data
            for row in data.get("observations") or []:
                key = (str(row.get("date", "")), str(row.get("realtime_start", "")))
                if key not in seen_rows:
                    seen_rows.add(key)
                    merged_rows.append(row)

        assert merged_payload is not None  # 至少一个窗口
        merged_payload["observations"] = merged_rows
        return RawBatch(
            endpoint="/series/observations",
            payload=merged_payload,
            raw_meta={
                "http_status": 200,
                "series_id": series_id,
                "realtime_windows": len(windows),
            },
        )

    def _fetch_vintage_dates(
        self, series_id: str, realtime_start: str
    ) -> list[str]:
        """分页取回系列在 realtime_start 之后的全部 vintage 日期（升序）。

        /series/vintagedates 单页最多 1000 条，用 offset 翻页。
        """
        dates: list[str] = []
        offset = 0
        while True:
            data = self._get_json(
                "/series/vintagedates",
                {
                    "series_id": series_id,
                    "api_key": self._api_key,
                    "file_type": "json",
                    "realtime_start": realtime_start,
                    "limit": _VINTAGEDATES_PAGE_SIZE,
                    "offset": offset,
                },
                series_id,
            )
            page = data.get("vintage_dates") or []
            dates.extend(page)
            total = int(data.get("count", len(dates)))
            offset += _VINTAGEDATES_PAGE_SIZE
            if not page or offset >= total:
                break
        return dates

    def _get_json(
        self, endpoint: str, params: dict[str, Any], series_id: str = ""
    ) -> dict[str, Any]:
        """执行单次 FRED GET：统一限流、错误映射，返回 JSON dict。"""
        self._rate_limiter.acquire(1)

        try:
            response = self._client.get(endpoint, params=params)
        except httpx.TransportError as e:
            raise TransportError(
                f"fred: {endpoint} request failed: {e}",
                context={
                    "source": "fred",
                    "endpoint": endpoint,
                    "series_id": series_id,
                },
            ) from e

        status_code = response.status_code

        # 错误映射（D04 §3）
        if status_code == 429:
            raise RateLimitError(
                f"fred: rate limited on {endpoint}/{series_id}",
                context={
                    "source": "fred",
                    "endpoint": endpoint,
                    "series_id": series_id,
                },
                retry_after=1,
            )
        if status_code in (401, 403):
            raise ProviderError(
                f"fred: auth failed on {endpoint} series {series_id}",
                context={
                    "source": "fred",
                    "endpoint": endpoint,
                    "status": status_code,
                    "series_id": series_id,
                },
            )
        if status_code >= 400:
            raise ProviderError(
                f"fred: HTTP {status_code} on {endpoint}",
                context={
                    "source": "fred",
                    "endpoint": endpoint,
                    "status": status_code,
                    "series_id": series_id,
                },
            )

        return response.json()

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

        快照语义（revision_supported=0）：全 vintage 窗口响应中同一
        observation 日期可能有多行，仅归一化 realtime_start 最新的一行；
        输出按 observation 日期升序。

        时间语义（审计 F-09）：
        - observation date → T00:00:00Z
        - vintage 日期 → T23:59:59Z（保守，防 look-ahead）
        - vintage = realtime_date；无 vintage → 观察日次日
        - 个别序列（如 IORB）ALFRED 预置的 realtime_start 可早于观测日，
          此时 release/revision 钳制为 observation_time（vintage_date 保留真值）

        Args:
            raw: RawBatch from fetch().

        Returns:
            list of NUMBER records（每 observation 日期恰一条）。
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

        # 快照折叠（revision_supported=0）：全 vintage 窗口下同一 observation
        # 日期会返回多行（每次修订一行），仅保留 realtime_start 最新的当前值。
        # 空 realtime_start 视为最低优先；同日期同 vintage 保留先出现的行。
        # 折叠后 revision_time=真实 vintage 日 → 跨重跑自然键稳定。
        latest_by_date: dict[str, dict[str, Any]] = {}
        for obs in observations:
            date_key = str(obs.get("date", ""))
            if not date_key:
                continue
            incumbent = latest_by_date.get(date_key)
            if incumbent is None or str(obs.get("realtime_start", "")) > str(
                incumbent.get("realtime_start", "")
            ):
                latest_by_date[date_key] = obs

        records: list[NUMBER] = []
        now = datetime.now(UTC).replace(tzinfo=None)

        for date_str in sorted(latest_by_date):
            obs = latest_by_date[date_str]

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

            # vintage 早于观测日（ALFRED 预置历史）→ 最早可用于观测时点
            if release_time < observation_time:
                release_time = observation_time

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
