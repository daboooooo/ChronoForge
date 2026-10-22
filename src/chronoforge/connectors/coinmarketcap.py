"""CoinMarketCap connector（全市场指标，DATA-SOURCE coinmarketcap）。

通过 CoinMarketCap 公开的 global-metrics 端点获取加密市场日频快照：
- `GET /data-api/v3/global-metrics/quotes/historical`
  query: `timeStart={YYYY-MM-DD}&timeEnd={YYYY-MM-DD}&interval=1d`
- 免费无 key（网页端数据接口，非官方文档化 API）；响应带 `status` 信封，
  `status.error_code != 0` 表示业务错误（HTTP 仍为 200）
- 单次请求上限 2200 条 → 连接器内部按 5 年分片拉取，片间重叠 1 天后按
  timestamp 去重

每日一行，字段（USDT 口径 quote[]）：
- `btcDominance` — BTC 占全市场总市值百分比（直接给 dominance，无需自算）
- `ethDominance` — ETH 市占率
- `quote[].totalMarketCap` / `quote[].altcoinMarketCap` — 全市场总市值

口径说明（为何不自己用 CoinMetrics 市值自算）：CM 社区层 `SplyCur` 对
XRP/XLM 采用最大供应量（~100B），会把 2017-2018 的全市场总市值高估约
18%，导致自算 dominance 同期偏低约 6pp；CMC 的 btcDominance 为其官方口径，
直接取用可避免该偏差。

时间语义（对齐 alternative 日频 NUMBER 的 T+1 结算模式）：
- observation = 观测日 T00:00:00（UTC naive）
- T+1 结算：release = revision = 次日 T00:00:00（固定值，非墙钟——
  full-window 重拉下用墙钟会为同一 observation 无限铸造新 revision）
- 当天行仍在盘中滚动更新 → normalize 跳过；fetch 起点回退 1 天，次日重采
- continuity_model: ALWAYS_OPEN（每日均有产出，无发布日历）
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

# 保守限流（端点无公开配额说明；日频任务实际 1 次全量拉取）
_DEFAULT_RATE = 30.0 / 60.0  # requests per second
_DEFAULT_BURST = 5

_BASE_URL = "https://api.coinmarketcap.com"
_ENDPOINT_GLOBAL_HISTORY = "/data-api/v3/global-metrics/quotes/historical"

# 单请求最多 2200 条 → 分片跨度取 5 年（≈1827 天），留足余量
_MAX_ITEMS_PER_REQUEST = 2200
_CHUNK_DAYS = 1827

# 全历史起点早于此（端点首行 2013-04-29）；窗口整体早于该日时不发请求
_OLDEST_DATE = date(2013, 1, 1)

_SEASONAL_ADJUSTMENT = "NOT_SEASONALLY_ADJUSTED"

# 指标 → (源字段, canonical units, 是否取自 quote[] 报价数组)
_METRICS: dict[str, tuple[str, str, bool]] = {
    "btc_dominance": ("btcDominance", "Percent", False),
    "eth_dominance": ("ethDominance", "Percent", False),
    "total_market_cap": ("totalMarketCap", "USD", True),
    "altcoin_market_cap": ("altcoinMarketCap", "USD", True),
}
_DEFAULT_METRIC = "btc_dominance"
# 市值类指标取 quote[] 中的 USD 报价
_QUOTE_CURRENCY = "USD"

logger = structlog.get_logger()


@dataclass(frozen=True)
class _CoinMarketCapSettings:
    """CoinMarketCap 连接器最小配置（向后兼容）。"""

    http_timeout_s: float = 30.0


def _quote_date(row: Any) -> date | None:
    """提取行的 UTC 观测日期；非法/缺失 → None。

    源时间格式为 ISO 8601（"2013-04-29T00:00:00.000Z"），统一取 UTC 日期。
    """
    if not isinstance(row, dict):
        return None
    raw = row.get("timestamp")
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


class CoinMarketCapConnector(DataConnector):
    """CoinMarketCap 全市场指标连接器。

    Args:
        settings: 配置对象。可以是 Settings（主入口）或 _CoinMarketCapSettings
                  （向后兼容）。如果为 None，则调用 Settings.load() 自动加载。
    """

    source_id = "coinmarketcap"

    def __init__(self, settings: Settings | _CoinMarketCapSettings | None = None) -> None:
        if settings is None:
            settings = Settings.load()
        self._http_timeout_s = getattr(settings, "http_timeout_s", 30.0)

        # 本机直连被墙：httpx 显式读 HTTPS_PROXY/HTTP_PROXY（同 binance_spot）
        proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")
        self._client = httpx.Client(
            base_url=_BASE_URL,
            timeout=self._http_timeout_s,
            headers={"User-Agent": "ChronoForge/coinmarketcap"},
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
        """健康检查：拉最近 2 天（轻量探活）。"""
        import time

        start = time.monotonic()
        try:
            today = datetime.now(UTC).replace(tzinfo=None).date()
            self._fetch_quotes(today - timedelta(days=1), today)
            elapsed_ms = int((time.monotonic() - start) * 1000)
            return HealthStatus(ok=True, latency_ms=elapsed_ms)
        except Exception as exc:
            elapsed_ms = int((time.monotonic() - start) * 1000)
            return HealthStatus(ok=False, latency_ms=elapsed_ms, detail=str(exc))

    def discover(self) -> list[InstrumentRef]:
        """静态注册（指标在 dataset params 中显式声明），discover 不实现。"""
        return []

    # ------------------------------------------------------------------
    # fetch
    # ------------------------------------------------------------------

    def fetch(self, request: FetchRequest) -> Iterator[RawBatch]:
        """获取窗口内的全市场日频快照。

        Args:
            request: params 可选 metric（默认 btc_dominance，见 _METRICS）
                     与 series_id（默认同 metric 名）；start/end 为观测时间
                     窗口（UTC naive）。
        """
        metric = str(request.params.get("metric", "") or _DEFAULT_METRIC)
        if metric not in _METRICS:
            raise ValueError(
                f"coinmarketcap: unsupported metric {metric!r} "
                f"(supported: {sorted(_METRICS)})"
            )
        series_id = str(request.params.get("series_id", "") or metric)

        today = datetime.now(UTC).replace(tzinfo=None).date()
        end_date = request.end.date() if request.end else today
        start_date = request.start.date() if request.start else _OLDEST_DATE
        if end_date < _OLDEST_DATE:
            # 回填窗口早于全历史起点 → 不发请求不产出
            # （0 batch → chunk success → checkpoint 照常推进）
            return

        # T+1 结算回补：起点回退 1 天，重采上一窗口时仍在修正的行。
        lo_date = max(start_date - timedelta(days=1), _OLDEST_DATE)

        quotes = self._fetch_quotes(lo_date, end_date)

        filtered = [
            row
            for row in quotes
            if (d := _quote_date(row)) is not None and lo_date <= d <= end_date
        ]

        yield RawBatch(
            endpoint=_ENDPOINT_GLOBAL_HISTORY,
            payload={"data": {"quotes": filtered}},
            raw_meta={
                "http_status": 200,
                "series_id": series_id,
                "metric": metric,
                "window_start": lo_date.isoformat(),
                "window_end": end_date.isoformat(),
            },
        )

    # ------------------------------------------------------------------
    # HTTP 与分片
    # ------------------------------------------------------------------

    def _fetch_quotes(self, start_date: date, end_date: date) -> list[dict[str, Any]]:
        """按 5 年分片拉取窗口内全部快照行（按 timestamp 去重、升序）。

        端点单请求上限 2200 条（约 6 年），超限时 status.error_code 会返回
        "500 / Too many items requested"（HTTP 仍 200），故分片由连接器完成。
        片间重叠 1 天，避免边界日被两侧同时丢弃。
        """
        merged: dict[str, dict[str, Any]] = {}
        chunk_start = start_date
        while chunk_start <= end_date:
            chunk_end = min(chunk_start + timedelta(days=_CHUNK_DAYS), end_date)
            body = self._get_json(
                _ENDPOINT_GLOBAL_HISTORY,
                {
                    "timeStart": chunk_start.isoformat(),
                    "timeEnd": chunk_end.isoformat(),
                    "interval": "1d",
                },
            )
            data = body.get("data")
            if not isinstance(data, dict) or not isinstance(data.get("quotes"), list):
                raise SchemaError(
                    "coinmarketcap: unexpected response shape (data.quotes is not a list)",
                    context={
                        "source": "coinmarketcap",
                        "endpoint": _ENDPOINT_GLOBAL_HISTORY,
                    },
                )
            for row in data["quotes"]:
                if not isinstance(row, dict):
                    continue
                ts = row.get("timestamp")
                if isinstance(ts, str) and ts:
                    merged[ts] = row
            if chunk_end >= end_date:
                break
            chunk_start = chunk_end  # 重叠 1 天（边界日由两侧共同覆盖）

        return sorted(merged.values(), key=lambda row: str(row.get("timestamp", "")))

    def _get_json(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        """执行单次 GET：统一限流、错误映射，返回 JSON dict。

        CMC 的业务错误在 200 响应里（status.error_code != 0），故状态码判定
        与信封判定都要处理。
        """
        self._rate_limiter.acquire(1)

        try:
            response = self._client.get(path, params=params)
        except httpx.TransportError as e:
            raise TransportError(
                f"coinmarketcap: request failed on {path}: {e}",
                context={"source": "coinmarketcap", "endpoint": path},
            ) from e

        status_code = response.status_code

        # 状态码先于 body 解析判定（429/401/403 的错误体可能非 JSON，
        # 先解析会把鉴权/限流失败误判为 ProviderError）
        if status_code == 429:
            retry_after = _parse_retry_after(response.headers.get("retry-after"))
            self._rate_limiter.on_rate_limited(retry_after)
            raise RateLimitError(
                f"coinmarketcap: rate limited on {path}",
                context={"source": "coinmarketcap", "endpoint": path, "status": status_code},
                retry_after=retry_after,
            )
        if status_code in (401, 403):
            raise AuthError(
                f"coinmarketcap: access denied on {path} (HTTP {status_code})",
                context={"source": "coinmarketcap", "endpoint": path, "status": status_code},
            )

        try:
            body = response.json()
        except ValueError as e:
            if status_code >= 400:
                raise ProviderError(
                    f"coinmarketcap: HTTP {status_code} on {path} (non-JSON body)",
                    context={"source": "coinmarketcap", "endpoint": path, "status": status_code},
                ) from e
            raise SchemaError(
                f"coinmarketcap: invalid JSON on {path}",
                context={"source": "coinmarketcap", "endpoint": path, "status": status_code},
            ) from e

        if not isinstance(body, dict):
            raise SchemaError(
                f"coinmarketcap: unexpected response shape on {path}",
                context={"source": "coinmarketcap", "endpoint": path, "status": status_code},
            )

        if status_code >= 400:
            raise ProviderError(
                f"coinmarketcap: HTTP {status_code} on {path}",
                context={"source": "coinmarketcap", "endpoint": path, "status": status_code},
            )

        # 200 但业务错误信封（error_code 非 0）
        status = body.get("status")
        if isinstance(status, dict):
            error_code = str(status.get("error_code", "0"))
            if error_code not in ("0", "None"):
                raise ProviderError(
                    "coinmarketcap: API error "
                    f"{error_code}: {status.get('error_message', '')}".strip(),
                    context={
                        "source": "coinmarketcap",
                        "endpoint": path,
                        "error_code": error_code,
                    },
                )

        return body

    # ------------------------------------------------------------------
    # normalize
    # ------------------------------------------------------------------

    def _extract_value(self, row: dict[str, Any], metric: str) -> float | None:
        """取指定指标的数值；缺失/非数值 → None。"""
        field, _units, from_quote = _METRICS[metric]
        if not from_quote:
            raw = row.get(field)
        else:
            quotes = row.get("quote")
            if not isinstance(quotes, list):
                return None
            entry = next(
                (q for q in quotes if isinstance(q, dict) and q.get("name") == _QUOTE_CURRENCY),
                None,
            )
            if entry is None:
                return None
            raw = entry.get(field)
        if raw is None:
            return None
        try:
            value = float(raw)
        except (ValueError, TypeError):
            return None
        if not math.isfinite(value):
            return None
        return value

    def normalize(self, raw: RawBatch) -> list[BaseRecord]:
        """将 global-metrics 快照转为 NUMBER 记录。

        - observation = 行 timestamp 的 UTC 日期 T00:00:00
        - 当天行（盘中滚动）→ skip；次日经 fetch 起点回退重采
        - dominance 须 0..100，超出 → skip
        - 输出按 observation 日期升序
        """
        if raw.endpoint != _ENDPOINT_GLOBAL_HISTORY:
            raise ValueError(f"coinmarketcap: unknown endpoint: {raw.endpoint}")

        payload = raw.payload
        if not isinstance(payload, dict):
            return []
        data = payload.get("data")
        if not isinstance(data, dict) or not isinstance(data.get("quotes"), list):
            return []

        metric = str(raw.raw_meta.get("metric", "") or _DEFAULT_METRIC)
        if metric not in _METRICS:
            metric = _DEFAULT_METRIC
        field, units, from_quote = _METRICS[metric]
        series_id = str(raw.raw_meta.get("series_id", "") or metric)

        today = datetime.now(UTC).replace(tzinfo=None).date()
        now = datetime.now(UTC).replace(tzinfo=None)

        by_date: dict[date, float] = {}
        skipped: dict[str, int] = {}

        for row in data["quotes"]:
            obs_date = _quote_date(row)
            if obs_date is None:
                skipped["invalid_timestamp"] = skipped.get("invalid_timestamp", 0) + 1
                continue
            # 当天行仍在盘中滚动 → 等次日重采冻结值
            if obs_date >= today:
                continue

            value = self._extract_value(row, metric)
            if value is None:
                skipped["missing_or_non_numeric_value"] = (
                    skipped.get("missing_or_non_numeric_value", 0) + 1
                )
                continue
            if not from_quote:
                if value <= 0.0:
                    # 源端缺失以 0 占位（实测 2019-12-20 / 2020-02-02 /
                    # 2020-05-18~20 出现该形态），真实市占率不会为 0 → 视为缺失
                    skipped["non_positive_value"] = (
                        skipped.get("non_positive_value", 0) + 1
                    )
                    continue
                if value > 100.0:
                    skipped["value_out_of_range"] = skipped.get("value_out_of_range", 0) + 1
                    continue

            by_date[obs_date] = value

        records: list[BaseRecord] = []
        for obs_date in sorted(by_date):
            observation_time = datetime.combine(obs_date, datetime.min.time())
            release_time = observation_time + timedelta(days=1)
            records.append(
                NUMBER(
                    schema_version="1.0",
                    source="coinmarketcap",
                    source_id=series_id,
                    source_timestamp=observation_time,
                    ingest_timestamp=now,
                    raw_record_id=(f"coinmarketcap:{series_id}:{obs_date.isoformat()}"),
                    quality_status=QualityStatus.VALID,
                    quality_reason=None,
                    observation_time=observation_time,
                    release_time=release_time,
                    revision_time=release_time,
                    value=by_date[obs_date],
                    units=units,
                    seasonal_adjustment=_SEASONAL_ADJUSTMENT,
                    vintage_date="",
                )
            )

        if skipped:
            logger.warning(
                "connector.normalize_summary",
                source="coinmarketcap",
                metric=metric,
                field=field,
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

        RawBatch 口径包含当天未冻结行（同 alternative：raw max 含 T+0 行），
        次日窗口经起点回退 1 天重采冻结值。
        """
        if isinstance(raw, RawBatch):
            payload = raw.payload
            if isinstance(payload, dict):
                data = payload.get("data")
                if isinstance(data, dict) and isinstance(data.get("quotes"), list):
                    dates = [
                        d for row in data["quotes"] if (d := _quote_date(row)) is not None
                    ]
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
