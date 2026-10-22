"""Alternative.me connector（Crypto Fear & Greed Index，DATA-SOURCE alternative）。

通过 alternative.me 公开 API 获取加密市场恐慌贪婪指数（当前仅 BTC 口径）：
- `GET /fng/?limit=0&format=json` — limit=0 一次返回全历史
  （2018-02-01 起每日一行），免费无 key
- 官方限流：10 分钟窗口 60 次（≈6 req/min）；日频任务实际 1 请求/天

时间语义（对齐 sosovalue 日频 NUMBER 的 T+1 结算模式）：
- observation `timestamp`（Unix 秒，UTC 00:00）→ T00:00:00（UTC naive）
- T+1 结算：release = revision = 次日 T00:00:00（固定值，非墙钟——
  full-window 重拉下用墙钟会为同一 observation 无限铸造新 revision）
- 仅当天未冻结行带 `time_until_update`（滚动值，次日才冻结）→ normalize
  跳过；fetch 起点回退 1 天，保证冻结后的值次日重采不漏
- continuity_model: ALWAYS_OPEN（每日均有产出，无发布日历）

响应为倒序（最新在前），normalize 输出按 observation 升序。
"""

from __future__ import annotations

import math
import os
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx
import structlog

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
    SchemaError,
    TransportError,
)
from chronoforge.connectors.ratelimit import RateLimiter
from chronoforge.models.base import BaseRecord
from chronoforge.models.enums import CanonicalType, QualityStatus
from chronoforge.models.macro import NUMBER
from chronoforge.models.quality import QualityFinding, QualityReport

# 官方限流：10 分钟窗口 60 次 ≈ 6 req/min
_DEFAULT_RATE = 6.0  # requests per minute
_DEFAULT_BURST = 6

_BASE_URL = "https://api.alternative.me"
_ENDPOINT_FNG = "/fng/"

# 全历史起点（2018-02-01 UTC）：窗口整体早于该日时不发请求
_OLDEST_DATE = date(2018, 2, 1)

_UNITS = "INDEX_POINTS"
_SEASONAL_ADJUSTMENT = "NOT_SEASONALLY_ADJUSTED"

# 当前唯一支持的指标/标的（alternative.me 仅有加密 F&G，口径为 BTC 市场）
_METRIC = "fear_greed_index"
_SYMBOL = "BTC"
_SERIES_ID = "fng_btc"

logger = structlog.get_logger()


@dataclass(frozen=True)
class _AlternativeSettings:
    """Alternative 连接器最小配置（向后兼容）。"""

    http_timeout_s: float = 30.0


def _row_ts_seconds(row: Any) -> int | None:
    """提取行的 Unix 秒时间戳；非法/缺失 → None。"""
    if not isinstance(row, dict):
        return None
    try:
        return int(str(row.get("timestamp", "")))
    except (ValueError, TypeError):
        return None


class AlternativeConnector(DataConnector):
    """Alternative.me 恐慌贪婪指数连接器。

    Args:
        settings: 配置对象。可以是 Settings（主入口）或 _AlternativeSettings
                  （向后兼容）。如果为 None，则调用 Settings.load() 自动加载。
    """

    source_id = "alternative"

    def __init__(self, settings: Settings | _AlternativeSettings | None = None) -> None:
        if settings is None:
            settings = Settings.load()
        self._http_timeout_s = getattr(settings, "http_timeout_s", 30.0)

        # 支持 HTTP_PROXY / HTTPS_PROXY 环境变量（ccxt 不读环境变量的教训：
        # httpx 也必须显式传 proxy，部分被墙网络环境仅代理可达）
        proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")
        self._client = httpx.Client(
            base_url=_BASE_URL,
            timeout=self._http_timeout_s,
            headers={"User-Agent": "ChronoForge/alternative"},
            proxy=proxy or None,
        )
        self._rate_limiter = RateLimiter(
            rate=_DEFAULT_RATE / 60.0,
            burst=_DEFAULT_BURST,
        )

    def capabilities(self) -> CapabilityMatrix:
        """能力矩阵（日频 NUMBER，全历史无限制）。"""
        return CapabilityMatrix(
            canonical_types=frozenset({CanonicalType.NUMBER}),
            intervals=frozenset(),  # 日频，无 interval 概念
            supports_revision=False,
            supports_websocket=False,
            max_history_days=None,
        )

    def health(self) -> HealthStatus:
        """健康检查：limit=1 拉最新一行（公开端点，轻量探活）。"""
        import time

        start = time.monotonic()
        try:
            self._get_json(_ENDPOINT_FNG, {"limit": 1, "format": "json"})
            elapsed_ms = int((time.monotonic() - start) * 1000)
            return HealthStatus(ok=True, latency_ms=elapsed_ms)
        except Exception as exc:
            elapsed_ms = int((time.monotonic() - start) * 1000)
            return HealthStatus(ok=False, latency_ms=elapsed_ms, detail=str(exc))

    def discover(self) -> list[InstrumentRef]:
        """静态注册（同 fred），discover 不实现。"""
        return []

    # ------------------------------------------------------------------
    # fetch
    # ------------------------------------------------------------------

    def fetch(self, request: FetchRequest) -> Iterator[RawBatch]:
        """获取 F&G 全历史并按窗口过滤。

        API 不支持日期参数，limit=0 单请求返回全历史；窗口过滤在客户端
        完成（起点回退 1 天重采 T+1 冻结值）。

        Args:
            request: params 固定 metric=fear_greed_index / symbol=BTC；
                     start/end 为观测时间窗口（UTC naive）。
        """
        metric = request.params.get("metric", "")
        symbol = request.params.get("symbol", "")
        if metric != _METRIC or symbol != _SYMBOL:
            raise ValueError(
                "alternative: only metric=fear_greed_index symbol=BTC "
                f"is supported (got metric={metric!r} symbol={symbol!r})"
            )

        today = datetime.now(UTC).replace(tzinfo=None).date()
        end_date = request.end.date() if request.end else today
        start_date = request.start.date() if request.start else _OLDEST_DATE
        if end_date < _OLDEST_DATE:
            # 回填窗口早于全历史起点 → 不发请求不产出
            # （0 batch → chunk success → checkpoint 照常推进）
            return

        # T+1 结算回补：起点回退 1 天，重采上一窗口时仍为滚动值的行。
        lo_date = max(start_date - timedelta(days=1), _OLDEST_DATE)

        body = self._get_json(_ENDPOINT_FNG, {"limit": 0, "format": "json"})
        rows = body.get("data")
        if not isinstance(rows, list):
            raise SchemaError(
                "alternative: unexpected response shape (data is not a list)",
                context={"source": "alternative", "endpoint": _ENDPOINT_FNG},
            )

        filtered = [
            row
            for row in rows
            if (ts := _row_ts_seconds(row)) is not None
            and lo_date <= datetime.fromtimestamp(ts, UTC).date() <= end_date
        ]
        # 保持源结构（倒序），仅替换 data 为窗口内子集
        payload = {**body, "data": filtered}

        yield RawBatch(
            endpoint=_ENDPOINT_FNG,
            payload=payload,
            raw_meta={
                "http_status": 200,
                "series_id": _SERIES_ID,
                "metric": metric,
                "symbol": symbol,
                "window_start": lo_date.isoformat(),
                "window_end": end_date.isoformat(),
            },
        )

    # ------------------------------------------------------------------
    # HTTP 与错误映射
    # ------------------------------------------------------------------

    def _get_json(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        """执行单次 GET：统一限流、错误映射，返回 JSON dict。"""
        self._rate_limiter.acquire(1)

        try:
            response = self._client.get(path, params=params)
        except httpx.TransportError as e:
            raise TransportError(
                f"alternative: request failed on {path}: {e}",
                context={"source": "alternative", "endpoint": path},
            ) from e

        status_code = response.status_code

        try:
            body = response.json()
        except ValueError as e:
            if status_code >= 400:
                raise ProviderError(
                    f"alternative: HTTP {status_code} on {path} (non-JSON body)",
                    context={"source": "alternative", "endpoint": path, "status": status_code},
                ) from e
            raise SchemaError(
                f"alternative: invalid JSON on {path}",
                context={"source": "alternative", "endpoint": path, "status": status_code},
            ) from e

        if not isinstance(body, dict):
            raise SchemaError(
                f"alternative: unexpected response shape on {path}",
                context={"source": "alternative", "endpoint": path, "status": status_code},
            )

        if status_code == 429:
            retry_after = _parse_retry_after(response.headers.get("retry-after"))
            self._rate_limiter.on_rate_limited(retry_after)
            raise RateLimitError(
                f"alternative: rate limited on {path}",
                context={"source": "alternative", "endpoint": path, "status": status_code},
                retry_after=retry_after,
            )
        if status_code in (401, 403):
            # 公开端点正常不会触发（多为 CDN/WAF 拦截）→ 不重试
            raise ProviderError(
                f"alternative: access denied on {path} (HTTP {status_code})",
                context={"source": "alternative", "endpoint": path, "status": status_code},
            )
        if status_code >= 400:
            raise ProviderError(
                f"alternative: HTTP {status_code} on {path}",
                context={"source": "alternative", "endpoint": path, "status": status_code},
            )

        metadata = body.get("metadata")
        if isinstance(metadata, dict) and metadata.get("error"):
            raise ProviderError(
                f"alternative: API error: {metadata.get('error')}",
                context={"source": "alternative", "endpoint": path},
            )

        return body

    # ------------------------------------------------------------------
    # normalize
    # ------------------------------------------------------------------

    def normalize(self, raw: RawBatch) -> list[BaseRecord]:
        """将 F&G 响应转为 NUMBER 记录。

        - value 为字符串 "0".."100" → float 强转，越界/非数值行跳过
        - 带 time_until_update 的当天滚动行（未冻结）→ skip
        - 输出按 observation 日期升序；同日多行保留先出现的一条
        """
        if raw.endpoint != _ENDPOINT_FNG:
            raise ValueError(f"alternative: unknown endpoint: {raw.endpoint}")

        payload = raw.payload
        if not isinstance(payload, dict):
            return []
        rows = payload.get("data")
        if not isinstance(rows, list):
            return []

        series_id = raw.raw_meta.get("series_id", _SERIES_ID)
        now = datetime.now(UTC).replace(tzinfo=None)

        # 按日期收敛（升序）：源为倒序，先排序再 setdefault 保证同日期
        # 保留 timestamp 最小的一条且输出确定
        indexed: list[tuple[int, dict[str, Any]]] = []
        for row in rows:
            ts = _row_ts_seconds(row)
            if ts is not None:
                indexed.append((ts, row))
        indexed.sort(key=lambda item: item[0])

        records: list[BaseRecord] = []
        skipped: dict[str, int] = {}
        seen_dates: set[date] = set()

        for ts, row in indexed:
            obs_date = datetime.fromtimestamp(ts, UTC).date()
            if obs_date in seen_dates:
                continue

            # 当天未冻结行（滚动值，带 time_until_update）→ 等次日重采
            if row.get("time_until_update"):
                continue

            try:
                value = float(str(row.get("value")))
            except (ValueError, TypeError):
                skipped["non_numeric_value"] = skipped.get("non_numeric_value", 0) + 1
                continue
            if not math.isfinite(value) or not 0.0 <= value <= 100.0:
                skipped["value_out_of_range"] = skipped.get("value_out_of_range", 0) + 1
                continue

            seen_dates.add(obs_date)
            observation_time = datetime.combine(obs_date, datetime.min.time())
            release_time = observation_time + timedelta(days=1)
            records.append(
                NUMBER(
                    schema_version="1.0",
                    source="alternative",
                    source_id=series_id,
                    source_timestamp=observation_time,
                    ingest_timestamp=now,
                    raw_record_id=(f"alternative:{series_id}:{obs_date.isoformat()}"),
                    quality_status=QualityStatus.VALID,
                    quality_reason=None,
                    observation_time=observation_time,
                    release_time=release_time,
                    revision_time=release_time,
                    value=value,
                    units=_UNITS,
                    seasonal_adjustment=_SEASONAL_ADJUSTMENT,
                    vintage_date="",
                )
            )

        if skipped:
            logger.warning(
                "connector.normalize_summary",
                source="alternative",
                kept=len(records),
                skipped=sum(skipped.values()),
                reasons=skipped,
            )

        return records

    def validate(self, records: list[BaseRecord]) -> QualityReport:
        """验证 NUMBER 记录（P0：release/revision >= observation）。"""
        findings: list[QualityFinding] = []

        for record in records:
            if not isinstance(record, NUMBER):
                continue

            key = str(record.natural_key())

            if record.release_time < record.observation_time:
                findings.append(
                    QualityFinding(
                        record_key=key,
                        rule_id="Q-TS-001",
                        severity="ERROR",
                        detail=(
                            f"release_time {record.release_time} "
                            f"< observation_time {record.observation_time}"
                        ),
                    )
                )

            if record.revision_time < record.observation_time:
                findings.append(
                    QualityFinding(
                        record_key=key,
                        rule_id="Q-TS-002",
                        severity="ERROR",
                        detail=(
                            f"revision_time {record.revision_time} "
                            f"< observation_time {record.observation_time}"
                        ),
                    )
                )

        return QualityReport(findings=findings)

    def checkpoint_from(self, raw: RawBatch | list[BaseRecord]) -> str | None:
        """提取 checkpoint cursor：最新观测日期（YYYY-MM-DD）。

        RawBatch 口径包含当天未冻结行（同 sosovalue：raw max 含 T+0 行），
        次日窗口经起点回退 1 天重采冻结值。
        """
        if isinstance(raw, RawBatch):
            payload = raw.payload
            if isinstance(payload, dict) and isinstance(payload.get("data"), list):
                timestamps = [
                    ts for row in payload["data"] if (ts := _row_ts_seconds(row)) is not None
                ]
                if timestamps:
                    return datetime.fromtimestamp(max(timestamps), UTC).date().isoformat()

        elif isinstance(raw, list):
            obs = [r.observation_time for r in raw if isinstance(r, NUMBER)]
            if obs:
                return max(obs).strftime("%Y-%m-%d")

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


def _parse_retry_after(value: str | None, default: int = 60) -> int:
    """Retry-After 头（delta-seconds 形式）→ int；非法取默认值。"""
    if not value:
        return default
    try:
        seconds = int(float(value))
        return seconds if seconds > 0 else default
    except (ValueError, TypeError):
        return default
