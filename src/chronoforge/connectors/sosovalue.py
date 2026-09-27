"""SoSoValue connector（ETF 资金流，DATA-SOURCE sosovalue）。

通过 SoSoValue OpenAPI 获取美国现货加密 ETF 资金流数据：
- `GET /etfs` — ETF 清单（ticker 发现）
- `GET /etfs/summary-history` — 汇总日频流向（FLOW/NUMBER）
- `GET /etfs/{ticker}/history` — 分 ticker 日频流向（FLOW）

时间语义（对齐 FRED 审计 F-09 的日期型处理）：
- observation date（YYYY-MM-DD）→ T00:00:00（UTC naive）
- T+1 结算：release = revision = 次日 T00:00:00（固定值，非墙钟——
  full-window 重拉下用墙钟会为同一 observation 无限铸造新 revision）
- FLOW period = 当日右开区间 [D 00:00, D+1 00:00)
- continuity_model: ALWAYS_OPEN（每日均有产出，无发布日历）

API 限制（官方文档 + demo 计划实测）：
- 历史仅回看最近 1 个月，实测边界 start_date ≥ today-30d（更早直接
  HTTP 403）→ max_history_days=30，fetch 内做 30 天钳制
- demo 计划 summary 每响应实测 ≤20 行 → 跨度 >15 天拆分为多个
  ≤15 天子请求（各约 ≤11 个交易日），保证窗口全覆盖
- T+1 结算：history 最新一行 flow 字段为 null → normalize 跳过；
  fetch 起点回退 1 天重采，保证 T+1 结算值不漏采
- 已知 bug：history 的 volume 字段返回 value_traded 的字符串值 → 禁采
- 限流 20 req/min
"""

from __future__ import annotations

import math
import re
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
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
    AuthError,
    ProviderError,
    RateLimitError,
    SchemaError,
    TransportError,
)
from chronoforge.connectors.ratelimit import RateLimiter
from chronoforge.models.base import BaseRecord
from chronoforge.models.enums import CanonicalType, QualityStatus
from chronoforge.models.macro import FLOW, NUMBER
from chronoforge.models.quality import QualityFinding, QualityReport
from chronoforge.security import SecretStr

# SoSoValue 官方限流：20 req/min
_DEFAULT_RATE = 20.0  # requests per minute
_DEFAULT_BURST = 20

_BASE_URL = "https://openapi.sosovalue.com/openapi/v1"

# API 仅回看最近 1 个月；demo 计划实测 start_date 早于 today-30d 即 403
_MAX_HISTORY_DAYS = 30

# 每个子请求的最大跨度（天）：demo 计划 summary 每响应实测 ≤20 行，
# 15 天 ≈ 11 个交易日，远低于分页上限
_SUB_RANGE_DAYS = 15

# 文档口径单请求 limit 上限 300（demo 计划实际按 20 行截断，
# 由 _SUB_RANGE_DAYS 拆分兜底）
_SUMMARY_LIMIT = 300

_UNITS = "USD"
_SEASONAL_ADJUSTMENT = "NOT_SEASONALLY_ADJUSTED"

# 指标 → canonical 类型映射（日流量 → FLOW，累计/存量 → NUMBER）
_FLOW_METRICS = frozenset({"total_net_inflow", "total_value_traded", "net_inflow"})
_NUMBER_METRICS = frozenset({"total_net_assets", "cum_net_inflow"})

# ticker URL path 白名单（防注入，API ticker 形如 IBIT/FBTC）
_TICKER_PATTERN = re.compile(r"^[A-Za-z0-9.\-]{1,10}$")

_ENDPOINT_SUMMARY = "/etfs/summary-history"
_ENDPOINT_LIST = "/etfs"


@dataclass(frozen=True)
class _SoSoValueSettings:
    """SoSoValue 连接器最小配置（向后兼容）。"""

    api_key: str
    http_timeout_s: float = 30.0


class SoSoValueConnector(DataConnector):
    """SoSoValue ETF 资金流数据连接器。

    Args:
        settings: 配置对象。可以是 Settings（主入口）或 _SoSoValueSettings
                  （向后兼容）。如果为 None，则调用 Settings.load() 自动加载。
    """

    source_id = "sosovalue"

    def __init__(self, settings: Settings | _SoSoValueSettings | None = None) -> None:
        if settings is None:
            settings = Settings.load()

        if isinstance(settings, Settings):
            self._api_key = settings.sosovalue_api_key.get_secret_value()
            self._http_timeout_s = settings.http_timeout_s
        else:
            # _SoSoValueSettings backward compat
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
        """初始化 httpx Client（API key 走 header）。"""
        return httpx.Client(
            base_url=_BASE_URL,
            timeout=self._http_timeout_s,
            headers={"x-soso-api-key": self._api_key},
        )

    def capabilities(self) -> CapabilityMatrix:
        """能力矩阵（API 历史仅回看最近 1 个月）。"""
        return CapabilityMatrix(
            canonical_types=frozenset({
                CanonicalType.NUMBER,
                CanonicalType.FLOW,
            }),
            intervals=frozenset(),  # 日频，无 interval 概念
            supports_revision=False,
            supports_websocket=False,
            max_history_days=_MAX_HISTORY_DAYS,
        )

    def health(self) -> HealthStatus:
        """健康检查：用已知的 BTC+US 组合调 /etfs。"""
        start = time.monotonic()
        try:
            self._request(_ENDPOINT_LIST, {"symbol": "BTC", "country_code": "US"})
            elapsed_ms = int((time.monotonic() - start) * 1000)
            return HealthStatus(ok=True, latency_ms=elapsed_ms)
        except Exception as exc:
            elapsed_ms = int((time.monotonic() - start) * 1000)
            return HealthStatus(
                ok=False, latency_ms=elapsed_ms, detail=str(exc)
            )

    def discover(self) -> list[InstrumentRef]:
        """发现 US-BTC 现货 ETF ticker 清单。"""
        etfs = self.list_etfs("BTC", "US")
        return [
            {
                "entity_id": str(item.get("ticker", "")),
                "instrument_id": str(item.get("ticker", "")),
                "market_id": "US_ETF",
            }
            for item in etfs
            if item.get("ticker")
        ]

    def list_etfs(self, symbol: str, country_code: str) -> list[dict[str, Any]]:
        """获取 ETF 清单（注册脚本与 discover 复用）。

        Args:
            symbol: 标的资产（如 BTC）。
            country_code: 上市国家（US/HK）。

        Returns:
            [{"ticker", "name", "exchange"}, ...]
        """
        body = self._request(
            _ENDPOINT_LIST,
            {"symbol": symbol, "country_code": country_code},
        )
        data = body.get("data") or []
        return list(data)  # type: ignore[arg-type]

    # ------------------------------------------------------------------
    # fetch
    # ------------------------------------------------------------------

    def fetch(self, request: FetchRequest) -> Iterator[RawBatch]:
        """获取 ETF 流向数据。

        Args:
            request: params 必含 endpoint（"summary-history" | "etf-history"）、
                     symbol、country_code、metric；etf-history 另需 ticker。
                     start/end 为观测时间窗口（UTC naive）。
        """
        endpoint = request.params.get("endpoint", "")
        metric = request.params.get("metric", "")
        symbol = request.params.get("symbol", "")
        country_code = request.params.get("country_code", "")
        if not endpoint or not metric or not symbol or not country_code:
            raise ValueError(
                "sosovalue: endpoint/metric/symbol/country_code are "
                "required in request.params"
            )

        # 日期换算 + 30 天钳制（demo 计划实测：start_date 早于 today-30d
        # 即 HTTP 403；文档口径「最近 1 个月」）
        today = datetime.now(UTC).replace(tzinfo=None).date()
        oldest = today - timedelta(days=_MAX_HISTORY_DAYS)
        end_date = request.end.date() if request.end else today
        start_date = request.start.date() if request.start else oldest
        start_date = max(start_date, oldest)
        if start_date > end_date:
            # 回填窗口早于 API 可回看范围 → 不发请求不产出
            # （0 batch → chunk success → checkpoint 照常推进）
            return

        # T+1 结算回补：起点回退 1 天，重采上一窗口结算时仍为 null 的行。
        # pipeline cursor = 窗口右界（连续窗口无重叠），若无回退，边界日期
        # 的 T+1 结算值将永久漏采；natural key upsert + revision_time 固定
        # 为 D+1，重采幂等不铸造新 revision。
        start_date = max(start_date - timedelta(days=1), oldest)

        if endpoint not in ("summary-history", "etf-history"):
            raise ValueError(f"sosovalue: unknown endpoint: {endpoint!r}")

        # 跨度 > _SUB_RANGE_DAYS 拆为多个子请求：demo 计划 summary 每响应
        # 实测 ≤20 行，30 天窗口约 21-22 个交易日，单请求会截断最老行
        sub_ranges: list[tuple[date, date]] = []
        cursor_d = start_date
        while cursor_d <= end_date:
            sub_end = min(cursor_d + timedelta(days=_SUB_RANGE_DAYS), end_date)
            sub_ranges.append((cursor_d, sub_end))
            cursor_d = sub_end + timedelta(days=1)

        for sub_start, sub_end in sub_ranges:
            if endpoint == "summary-history":
                series_id = self._entity_id(
                    endpoint, symbol, country_code, metric
                )
                raw_endpoint = _ENDPOINT_SUMMARY
                body = self._request(
                    _ENDPOINT_SUMMARY,
                    {
                        "symbol": symbol,
                        "country_code": country_code,
                        "start_date": sub_start.isoformat(),
                        "end_date": sub_end.isoformat(),
                        "limit": _SUMMARY_LIMIT,
                    },
                )
                raw_meta = {
                    "http_status": 200,
                    "series_id": series_id,
                    "metric": metric,
                    "symbol": symbol,
                    "country_code": country_code,
                }
            else:
                ticker = request.params.get("ticker", "")
                if not ticker or not _TICKER_PATTERN.fullmatch(ticker):
                    raise ValueError(f"sosovalue: invalid ticker: {ticker!r}")
                series_id = self._entity_id(
                    endpoint, symbol, country_code, metric, ticker
                )
                raw_endpoint = f"/etfs/{ticker}/history"
                body = self._request(
                    raw_endpoint,
                    {
                        "start_date": sub_start.isoformat(),
                        "end_date": sub_end.isoformat(),
                    },
                )
                raw_meta = {
                    "http_status": 200,
                    "series_id": series_id,
                    "metric": metric,
                    "symbol": symbol,
                    "country_code": country_code,
                    "ticker": ticker,
                }

            yield RawBatch(
                endpoint=raw_endpoint,
                payload=body,
                raw_meta=raw_meta,
            )

    @staticmethod
    def _entity_id(
        endpoint: str, symbol: str, country_code: str, metric: str,
        ticker: str = "",
    ) -> str:
        """构造 entity_id（与 dataset 注册的 entity_id 严格一致）。"""
        prefix = f"etf_{country_code.lower()}_{symbol.lower()}"
        if endpoint == "summary-history":
            return f"{prefix}_summary_{metric}"
        return f"{prefix}_{ticker}_{metric}"

    # ------------------------------------------------------------------
    # HTTP 与错误映射
    # ------------------------------------------------------------------

    def _request(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        """统一请求：限流 + 错误映射 + 包装 code 解析。

        SoSoValue 是「HTTP 200 + body code 非 0」风格：先解析 body code，
        再看 HTTP status。

        Returns:
            完整包装响应 {code: 0, message, data}（code==0 才返回）。
        """
        self._rate_limiter.acquire(1)

        try:
            response = self._client.get(path, params=params)
        except httpx.TransportError as e:
            raise TransportError(
                f"sosovalue: request failed on {path}: {e}",
                context={"source": "sosovalue", "endpoint": path},
            ) from e

        status_code = response.status_code

        if status_code in (401, 403):
            raise AuthError(
                f"sosovalue: auth failed on {path} (HTTP {status_code})",
                context={"source": "sosovalue", "endpoint": path,
                         "status": status_code},
            )

        # 解析 body（429 时 body 可能仍带 code=42901 与 retry_after）
        try:
            body = response.json()
        except ValueError as e:
            if status_code >= 400:
                # 5xx 网关/服务器错误常返回非 JSON 页面 → ProviderError
                raise ProviderError(
                    f"sosovalue: HTTP {status_code} on {path} (non-JSON body)",
                    context={"source": "sosovalue", "endpoint": path,
                             "status": status_code},
                ) from e
            raise SchemaError(
                f"sosovalue: invalid JSON on {path}",
                context={"source": "sosovalue", "endpoint": path,
                         "status": status_code},
            ) from e
        if not isinstance(body, dict):
            raise SchemaError(
                f"sosovalue: unexpected response shape on {path}",
                context={"source": "sosovalue", "endpoint": path,
                         "status": status_code},
            )

        code = body.get("code")

        if code == 42901 or status_code == 429:
            details = body.get("details") or {}
            retry_after = int(details.get("retry_after") or 60)
            # 全局冷却 + runner 层 retry() 自动尊重 retry_after
            self._rate_limiter.on_rate_limited(retry_after)
            raise RateLimitError(
                f"sosovalue: rate limited on {path}",
                context={"source": "sosovalue", "endpoint": path,
                         "status": status_code},
                retry_after=retry_after,
            )

        if isinstance(code, int) and code != 0:
            # 400001（无效 symbol+country 组合）/ 400003（参数非法）等
            # 均为提供方错误，不重试
            raise ProviderError(
                f"sosovalue: provider error on {path}: "
                f"code={code} message={body.get('message', '')}",
                context={"source": "sosovalue", "endpoint": path,
                         "code": code, "status": status_code},
            )

        if status_code >= 400:
            raise ProviderError(
                f"sosovalue: HTTP {status_code} on {path}",
                context={"source": "sosovalue", "endpoint": path,
                         "status": status_code},
            )

        if code != 0:
            raise SchemaError(
                f"sosovalue: unexpected code on {path}: {code!r}",
                context={"source": "sosovalue", "endpoint": path},
            )

        return body

    # ------------------------------------------------------------------
    # normalize
    # ------------------------------------------------------------------

    def normalize(self, raw: RawBatch) -> list[BaseRecord]:
        """将 SoSoValue 响应转为 NUMBER/FLOW 记录。

        时间语义：
        - observation date → D T00:00:00（UTC naive）
        - T+1 结算：release = revision = (D+1) T00:00:00（固定值）
        - FLOW period = [D 00:00, D+1 00:00) 右开日界
        - metric 值为 null（T+0 未结算行）→ skip
        - 禁采 volume 字段（官方已知 bug：返回 value_traded 的字符串值）
        """
        if raw.endpoint != _ENDPOINT_SUMMARY and not raw.endpoint.endswith(
            "/history"
        ):
            raise ValueError(f"sosovalue: unknown endpoint: {raw.endpoint}")

        payload = raw.payload
        if not isinstance(payload, dict):
            return []
        if payload.get("code") != 0:
            return []  # fetch 层已保证 code==0，防御性跳过

        rows = payload.get("data") or []
        if not isinstance(rows, list):
            return []

        metric = raw.raw_meta.get("metric", "")
        series_id = raw.raw_meta.get("series_id", "")
        if metric in _FLOW_METRICS:
            record_type: type[FLOW] | type[NUMBER] = FLOW
        elif metric in _NUMBER_METRICS:
            record_type = NUMBER
        else:
            raise ValueError(f"sosovalue: unknown metric: {metric!r}")

        records: list[BaseRecord] = []
        now = datetime.now(UTC).replace(tzinfo=None)

        for row in rows:
            if not isinstance(row, dict):
                continue
            date_str = row.get("date", "")
            if not date_str:
                continue

            # 解析 observation 日期 → T00:00:00（对齐 fred 审计 F-09）
            try:
                observation_time = datetime(
                    year=int(date_str[:4]),
                    month=int(date_str[5:7]),
                    day=int(date_str[8:10]),
                )
            except (ValueError, IndexError):
                continue

            # T+1 结算：最新一行 flow 字段为 null → skip（非数据缺口）
            raw_value = row.get(metric)
            if raw_value is None:
                continue

            # value 强转（源侧可能回字符串，含千分位逗号）
            try:
                value = float(str(raw_value).replace(",", ""))
            except (ValueError, TypeError):
                continue
            if not math.isfinite(value):
                continue

            release_time = observation_time + timedelta(days=1)
            record = record_type(
                schema_version="1.0",
                source="sosovalue",
                source_id=series_id,
                source_timestamp=observation_time,
                ingest_timestamp=now,
                raw_record_id=f"sosovalue:{series_id}:{date_str}",
                quality_status=QualityStatus.VALID,
                quality_reason=None,
                observation_time=observation_time,
                release_time=release_time,
                revision_time=release_time,
                value=value,
                units=_UNITS,
                seasonal_adjustment=_SEASONAL_ADJUSTMENT,
                vintage_date="",
                **(
                    {"period_start": observation_time,
                     "period_end": release_time}
                    if record_type is FLOW else {}
                ),
            )
            records.append(record)

        return records

    def validate(self, records: list[BaseRecord]) -> QualityReport:
        """验证 NUMBER/FLOW 记录（P0：release/revision >= observation）。"""
        findings: list[QualityFinding] = []

        for record in records:
            if not isinstance(record, (NUMBER, FLOW)):
                continue

            key = str(record.natural_key())

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
        """提取 checkpoint cursor：最新观测值的日期（YYYY-MM-DD）。

        源行序不保证升序（summary/history 均为倒序），必须取 max 而非末元素。
        """
        if isinstance(raw, RawBatch):
            payload = raw.payload
            if isinstance(payload, dict):
                rows = payload.get("data") or []
                dates = [
                    row.get("date", "")
                    for row in rows
                    if isinstance(row, dict) and row.get("date")
                ]
                if dates:
                    return max(dates)  # type: ignore[no-any-return]

        elif isinstance(raw, list):
            obs = [
                r.observation_time for r in raw if isinstance(r, (NUMBER, FLOW))
            ]
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
