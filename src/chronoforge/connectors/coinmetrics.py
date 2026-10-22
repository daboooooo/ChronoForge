"""Coin Metrics connector（社区层公开 API，DATA-SOURCE coinmetrics）。

通过 Coin Metrics 社区层 API 获取加密资产日频指标：
- `GET /v4/timeseries/asset-metrics`
  query: `assets={asset}&metrics={metric}&frequency=1d`
         `&start_time={YYYY-MM-DD}&end_time={YYYY-MM-DD}&page_size=10000`
- base_url `https://community-api.coinmetrics.io`，**社区层免费无 key**
- 设置了 COINMETRICS_API_KEY 时自动带 `X-CM-API-KEY` 头（付费层），
  用于社区层受限指标（如余额分层地址数 AdrBalNtv*）
- 社区层限流 30 req/min → 保守取 20 req/min

覆盖场景：
- USDT/USDC 流通量（SplyCur）与市值（CapMrktCurUSD）
- BTC 有余额地址数（AdrBalCnt）与活跃地址数（AdrActCnt）

分页语义（实测踩坑，务必保留）：
- **必须同时传 start_time 与 end_time**：只传 start_time 时 API 返回的是
  全历史最新一页（而非窗口起点），会静默丢数据
- 首页返回的是**窗口尾页**，`next_page_token` 指向**更早**的页；
  需按 token 逐页向前回溯才能取全窗口
- `page_size` 上限 10000

时间语义（对齐 alternative 日频 NUMBER 的 T+1 结算模式）：
- observation = 观测日 T00:00:00（UTC naive）
- T+1 结算：release = revision = 次日 T00:00:00（固定值，非墙钟——
  full-window 重拉下用墙钟会为同一 observation 无限铸造新 revision）
- 当天行尚未冻结 → normalize 跳过；fetch 起点回退 1 天，次日重采
- continuity_model: ALWAYS_OPEN（每日均有产出，无发布日历）

响应为 `{"data":[{"asset","time","<Metric>":"<值字符串>"}], "next_page_token":...}`，
指标值均为字符串，缺失表示该日该指标不可得。
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
    AuthError,
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
from chronoforge.security import SecretStr

# 社区层限流 30 req/min → 保守取 20 req/min
_DEFAULT_RATE = 20.0 / 60.0  # requests per second
_DEFAULT_BURST = 20

_BASE_URL = "https://community-api.coinmetrics.io"
_ENDPOINT_ASSET_METRICS = "/v4/timeseries/asset-metrics"

# 单请求最大行数（API 硬上限 10000）
_PAGE_SIZE = 10000
# 回溯分页上限（防 token 异常导致死循环）
_MAX_PAGES = 50

# 指标单位映射（canonical NUMBER.units）
_METRIC_UNITS: dict[str, str] = {
    "SplyCur": "Tokens",
    "CapMrktCurUSD": "USD",
    "AdrBalCnt": "Addresses",
    "AdrActCnt": "Addresses",
}
_DEFAULT_UNITS = "Units"

# 资产最早可用日（Bitcoin 创世块）；早于此日的窗口不发请求
_OLDEST_DATE = date(2009, 1, 1)

_UNITS = "Tokens"
_SEASONAL_ADJUSTMENT = "NOT_SEASONALLY_ADJUSTED"

# 默认频率（社区层 asset-metrics 支持 1d/1h 等；本连接器仅日频口径）
_DEFAULT_FREQUENCY = "1d"

logger = structlog.get_logger()


@dataclass(frozen=True)
class _CoinMetricsSettings:
    """Coin Metrics 连接器最小配置（向后兼容）。"""

    api_key: str
    http_timeout_s: float = 30.0


def _row_date(row: Any) -> date | None:
    """提取行的 UTC 观测日期；非法/缺失 → None。

    源时间格式为 ISO 8601（"2017-01-01T00:00:00.000000000Z"），
    统一取其 UTC 日期部分。
    """
    if not isinstance(row, dict):
        return None
    raw = row.get("time")
    if not isinstance(raw, str) or not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(UTC).date()
    except ValueError:
        return None


def _parse_retry_after(value: str | None, default: int = 60) -> int:
    """Retry-After 头（delta-seconds 形式）→ int；非法取默认值。"""
    if not value:
        return default
    try:
        seconds = int(float(value))
        return seconds if seconds > 0 else default
    except (ValueError, TypeError):
        return default


class CoinMetricsConnector(DataConnector):
    """Coin Metrics 社区层 asset-metrics 连接器。

    Args:
        settings: 配置对象。可以是 Settings（主入口）或 _CoinMetricsSettings
                  （向后兼容）。如果为 None，则调用 Settings.load() 自动加载。
    """

    source_id = "coinmetrics"

    def __init__(self, settings: Settings | _CoinMetricsSettings | None = None) -> None:
        if settings is None:
            settings = Settings.load()

        if isinstance(settings, Settings):
            self._api_key = settings.coinmetrics_api_key.get_secret_value()
            self._http_timeout_s = settings.http_timeout_s
        else:
            # _CoinMetricsSettings backward compat
            key = settings.api_key
            # SecretStr 防误传：httpx 会把 SecretStr 序列化成 "******" 发出
            self._api_key = key.get_secret_value() if isinstance(key, SecretStr) else key
            self._http_timeout_s = settings.http_timeout_s

        headers = {"User-Agent": "ChronoForge/coinmetrics"}
        if self._api_key:
            # 付费层：社区层受限指标（如 AdrBalNtv* 余额分层）需带 key
            headers["X-CM-API-KEY"] = self._api_key

        # 本机直连被墙：httpx 显式传 proxy（ccxt 不读环境变量的教训）
        proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")
        self._client = httpx.Client(
            base_url=_BASE_URL,
            timeout=self._http_timeout_s,
            headers=headers,
            proxy=proxy or None,
        )
        self._rate_limiter = RateLimiter(
            rate=_DEFAULT_RATE,
            burst=_DEFAULT_BURST,
        )

    def capabilities(self) -> CapabilityMatrix:
        """能力矩阵（日频 NUMBER，全历史无已知跨度限制）。"""
        return CapabilityMatrix(
            canonical_types=frozenset({CanonicalType.NUMBER}),
            intervals=frozenset(),  # 日频，无 interval 概念
            supports_revision=False,
            supports_websocket=False,
            max_history_days=None,
        )

    def health(self) -> HealthStatus:
        """健康检查：拉最近 1 行 BTC 地址数（轻量探活）。"""
        import time

        start = time.monotonic()
        try:
            today = datetime.now(UTC).replace(tzinfo=None).date()
            self._fetch_asset_metrics(
                asset="btc",
                metric="AdrBalCnt",
                start_date=today - timedelta(days=1),
                end_date=today,
            )
            elapsed_ms = int((time.monotonic() - start) * 1000)
            return HealthStatus(ok=True, latency_ms=elapsed_ms)
        except Exception as exc:
            elapsed_ms = int((time.monotonic() - start) * 1000)
            return HealthStatus(ok=False, latency_ms=elapsed_ms, detail=str(exc))

    def discover(self) -> list[InstrumentRef]:
        """静态注册（资产/指标在 dataset params 中显式声明），discover 不实现。"""
        return []

    # ------------------------------------------------------------------
    # fetch
    # ------------------------------------------------------------------

    def fetch(self, request: FetchRequest) -> Iterator[RawBatch]:
        """获取窗口内的 asset-metrics 日频序列。

        Args:
            request: params 必含 asset（如 usdt/usdc/btc）与 metric
                     （如 SplyCur/CapMrktCurUSD/AdrBalCnt/AdrActCnt）；
                     可选 series_id（默认 f"{asset}_{metric}"）与
                     frequency（默认 1d）；start/end 为观测时间窗口
                     （UTC naive）。
        """
        asset = str(request.params.get("asset", "")).strip().lower()
        metric = str(request.params.get("metric", "")).strip()
        if not asset or not metric:
            raise ValueError(
                "coinmetrics: params must contain 'asset' and 'metric' "
                f"(got asset={asset!r} metric={metric!r})"
            )
        series_id = str(request.params.get("series_id", "") or f"{asset}_{metric}")
        frequency = str(request.params.get("frequency", "") or _DEFAULT_FREQUENCY)

        today = datetime.now(UTC).replace(tzinfo=None).date()
        end_date = request.end.date() if request.end else today
        start_date = request.start.date() if request.start else _OLDEST_DATE
        if end_date < _OLDEST_DATE:
            # 回填窗口早于全历史起点 → 不发请求不产出
            # （0 batch → chunk success → checkpoint 照常推进）
            return

        # T+1 结算回补：起点回退 1 天，重采上一窗口时仍为滚动值的行。
        lo_date = max(start_date - timedelta(days=1), _OLDEST_DATE)

        rows = self._fetch_asset_metrics(
            asset=asset,
            metric=metric,
            start_date=lo_date,
            end_date=end_date,
            frequency=frequency,
        )

        filtered = [
            row
            for row in rows
            if (d := _row_date(row)) is not None and lo_date <= d <= end_date
        ]

        yield RawBatch(
            endpoint=_ENDPOINT_ASSET_METRICS,
            payload={"data": filtered},
            raw_meta={
                "http_status": 200,
                "series_id": series_id,
                "asset": asset,
                "metric": metric,
                "frequency": frequency,
                "window_start": lo_date.isoformat(),
                "window_end": end_date.isoformat(),
            },
        )

    # ------------------------------------------------------------------
    # HTTP 与分页
    # ------------------------------------------------------------------

    def _fetch_asset_metrics(
        self,
        asset: str,
        metric: str,
        start_date: date,
        end_date: date,
        frequency: str = _DEFAULT_FREQUENCY,
    ) -> list[dict[str, Any]]:
        """拉取窗口内全部数据行（自动按 next_page_token 向前回溯分页）。

        API 首页返回窗口**尾页**，token 指向更早的页；逐页回溯直到
        token 消失即取全窗口。行按观测日期去重（保留首次出现）。
        """
        params: dict[str, Any] = {
            "assets": asset,
            "metrics": metric,
            "frequency": frequency,
            "start_time": start_date.isoformat(),
            "end_time": end_date.isoformat(),
            "page_size": _PAGE_SIZE,
        }

        rows: list[dict[str, Any]] = []
        seen: set[date] = set()
        token: str | None = None

        for _ in range(_MAX_PAGES):
            page_params = dict(params)
            if token:
                page_params["next_page_token"] = token

            body = self._get_json(_ENDPOINT_ASSET_METRICS, page_params)
            page = body.get("data")
            if not isinstance(page, list):
                raise SchemaError(
                    "coinmetrics: unexpected response shape (data is not a list)",
                    context={
                        "source": "coinmetrics",
                        "endpoint": _ENDPOINT_ASSET_METRICS,
                        "asset": asset,
                        "metric": metric,
                    },
                )

            for row in page:
                if not isinstance(row, dict):
                    continue
                d = _row_date(row)
                if d is None or d in seen:
                    continue
                seen.add(d)
                rows.append(row)

            next_token = body.get("next_page_token")
            if not isinstance(next_token, str) or not next_token:
                break
            token = next_token
        else:
            logger.warning(
                "connector.fetch_truncated",
                source="coinmetrics",
                asset=asset,
                metric=metric,
                max_pages=_MAX_PAGES,
            )

        return rows

    def _get_json(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        """执行单次 GET：统一限流、错误映射，返回 JSON dict。

        状态码先于 body 解析判定（429/401/403 的错误体可能非 JSON，
        先解析会把鉴权失败误判为 ProviderError）。
        """
        self._rate_limiter.acquire(1)

        try:
            response = self._client.get(path, params=params)
        except httpx.TransportError as e:
            raise TransportError(
                f"coinmetrics: request failed on {path}: {e}",
                context={"source": "coinmetrics", "endpoint": path},
            ) from e

        status_code = response.status_code
        detail = self._error_detail(response)

        if status_code == 429:
            retry_after = _parse_retry_after(response.headers.get("retry-after"))
            self._rate_limiter.on_rate_limited(retry_after)
            raise RateLimitError(
                f"coinmetrics: rate limited on {path}",
                context={"source": "coinmetrics", "endpoint": path, "status": status_code},
                retry_after=retry_after,
            )
        if status_code in (401, 403):
            # 缺/错 key 或付费墙 → 不重试
            raise AuthError(
                f"coinmetrics: access denied on {path} (HTTP {status_code}): {detail}".rstrip(),
                context={"source": "coinmetrics", "endpoint": path, "status": status_code},
            )
        if status_code >= 400:
            # 社区层对不受支持指标返回 400（如 "Value 'AdrBalNtv10' is not supported"）
            raise ProviderError(
                f"coinmetrics: HTTP {status_code} on {path}: {detail}".rstrip(),
                context={
                    "source": "coinmetrics",
                    "endpoint": path,
                    "status": status_code,
                    "params": {k: v for k, v in params.items() if k != "next_page_token"},
                },
            )

        try:
            body = response.json()
        except ValueError as e:
            raise SchemaError(
                f"coinmetrics: invalid JSON on {path}",
                context={"source": "coinmetrics", "endpoint": path, "status": status_code},
            ) from e

        if not isinstance(body, dict):
            raise SchemaError(
                f"coinmetrics: unexpected response shape on {path}",
                context={"source": "coinmetrics", "endpoint": path, "status": status_code},
            )

        error = body.get("error")
        if error:
            raise ProviderError(
                f"coinmetrics: API error on {path}: {error}",
                context={"source": "coinmetrics", "endpoint": path},
            )

        return body

    @staticmethod
    def _error_detail(response: httpx.Response) -> str:
        """从错误响应体提取可读信息（非 JSON 或无 error 字段 → 空串）。"""
        try:
            body = response.json()
        except ValueError:
            return ""
        if isinstance(body, dict):
            error = body.get("error")
            if isinstance(error, dict):
                return str(error.get("message", ""))[:200]
            if error:
                return str(error)[:200]
        return ""

    # ------------------------------------------------------------------
    # normalize
    # ------------------------------------------------------------------

    def normalize(self, raw: RawBatch) -> list[BaseRecord]:
        """将 asset-metrics 响应转为 NUMBER 记录。

        - time ISO 8601 → UTC 观测日
        - 指标值为字符串 → float 强转；缺失/非数值/非有限 → skip
        - 当天行（尚未冻结）→ skip（T+1，次日经 fetch 回退重采）
        - 输出按 observation 日期升序
        """
        if raw.endpoint != _ENDPOINT_ASSET_METRICS:
            raise ValueError(f"coinmetrics: unknown endpoint: {raw.endpoint}")

        payload = raw.payload
        if not isinstance(payload, dict):
            return []
        rows = payload.get("data")
        if not isinstance(rows, list):
            return []

        series_id = str(raw.raw_meta.get("series_id", ""))
        metric = str(raw.raw_meta.get("metric", ""))
        units = _METRIC_UNITS.get(metric, _DEFAULT_UNITS)
        now = datetime.now(UTC).replace(tzinfo=None)
        today = now.date()

        indexed: list[tuple[date, dict[str, Any]]] = []
        for row in rows:
            d = _row_date(row)
            if d is not None:
                indexed.append((d, row))
        indexed.sort(key=lambda item: item[0])

        records: list[BaseRecord] = []
        skipped: dict[str, int] = {}
        seen_dates: set[date] = set()

        for obs_date, row in indexed:
            if obs_date in seen_dates:
                continue

            # 当天行未冻结 → 等次日重采
            if obs_date >= today:
                continue

            raw_value = row.get(metric)
            if raw_value is None or raw_value == "":
                skipped["missing_metric_value"] = skipped.get("missing_metric_value", 0) + 1
                continue
            try:
                value = float(str(raw_value))
            except (ValueError, TypeError):
                skipped["non_numeric_value"] = skipped.get("non_numeric_value", 0) + 1
                continue
            if not math.isfinite(value):
                skipped["non_finite_value"] = skipped.get("non_finite_value", 0) + 1
                continue

            seen_dates.add(obs_date)
            observation_time = datetime.combine(obs_date, datetime.min.time())
            release_time = observation_time + timedelta(days=1)
            records.append(
                NUMBER(
                    schema_version="1.0",
                    source="coinmetrics",
                    source_id=series_id,
                    source_timestamp=observation_time,
                    ingest_timestamp=now,
                    raw_record_id=(f"coinmetrics:{series_id}:{obs_date.isoformat()}"),
                    quality_status=QualityStatus.VALID,
                    quality_reason=None,
                    observation_time=observation_time,
                    release_time=release_time,
                    revision_time=release_time,
                    value=value,
                    units=units,
                    seasonal_adjustment=_SEASONAL_ADJUSTMENT,
                    vintage_date="",
                )
            )

        if skipped:
            logger.warning(
                "connector.normalize_summary",
                source="coinmetrics",
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
                dates = [d for row in payload["data"] if (d := _row_date(row)) is not None]
                if dates:
                    return max(dates).isoformat()

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
