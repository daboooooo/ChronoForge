"""Yahoo Finance connector（D04 §4.5，DATA-SOURCE-005）。

通过 Yahoo Finance chart 端点获取 OHLCV 数据：
- `/v8/finance/chart/{symbol}` — 区间查询（period1/period2 + interval）
- 支持 splits/dividends 事件（adjustment 四元组）
- 保守限流：30 req/min（无官方限流文档）
- 429 退避 ≥60s

时间语义（审计 F-09）：
- period1/period2: epoch **秒**（含端点）→ us ×1e6
- 返回 timestamp[]: epoch 秒 → us ×1e6
- continuity_model: TRADING_CALENDAR（周末/节假日缺 K = EXPECTED_GAP）

market_id 格式：YAHOO:{SYMBOL}:SPOT
"""

from __future__ import annotations

import math
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx

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
from chronoforge.models.derivatives import Interval
from chronoforge.models.enums import CanonicalType, QualityStatus
from chronoforge.models.market import OHLCV
from chronoforge.models.quality import QualityFinding, QualityReport

# Yahoo Finance 保守限流配置（无官方限流文档）
_DEFAULT_RATE = 30.0  # requests per minute
_DEFAULT_BURST = 5


@dataclass(frozen=True)
class _YahooSettings:
    """Yahoo 连接器最小配置。"""

    http_timeout_s: float = 30.0
    crumb: str = ""
    cookie: str = ""
    # 短 UA：Yahoo getcrumb 对完整 Chrome UA 做机器人指纹拦截（429），
    # 简化 UA 实测可稳定通过（完整 UA + 同 IP 同时刻对比验证）
    user_agent: str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"


class YahooConnector(DataConnector):
    """Yahoo Finance 数据连接器（D04 §4.5）。

    Args:
        settings: 配置对象或 None（使用默认值）。
    """

    source_id = "yahoo"

    def __init__(self, settings: Any | None = None) -> None:
        self._settings = settings or _YahooSettings()
        self._client = self._init_client(self._settings)
        self._rate_limiter = RateLimiter(
            rate=self._settings.http_timeout_s * _DEFAULT_RATE / 60.0,
            burst=_DEFAULT_BURST,
        )
        self._crumb: str | None = None

    @staticmethod
    def _init_client(settings: _YahooSettings) -> httpx.Client:
        """初始化 httpx Client。"""
        # Settings 对象可能没有 user_agent 属性（用 sec_user_agent），
        # 兼容处理：getattr 回退到 _YahooSettings 默认值
        user_agent = getattr(settings, "user_agent", None) or _YahooSettings.user_agent
        return httpx.Client(
            base_url="https://query1.finance.yahoo.com",
            timeout=settings.http_timeout_s,
            headers={
                "User-Agent": user_agent,
            },
        )

    def _ensure_crumb(self) -> None:
        """获取/缓存 crumb（Yahoo cookie 认证，D04 §4.5）。

        完整流程（缺一不可）：
        1. 访问 fc.yahoo.com 获取 A3 cookie（返回 404 但会 Set-Cookie，
           httpx client 自动持久化）
        2. 用**同一 client** 请求 getcrumb（自动携带 cookie）
        3. crumb 为空视为认证流程失败（显式抛 SchemaError，
           避免 health 误报 OK 而 fetch 失败）
        """
        if self._crumb is not None:
            return

        # Step 1: 获取 cookie（fc.yahoo.com 预期 404，cookie 由 client 持久化）
        try:
            self._client.get("https://fc.yahoo.com")
        except httpx.TransportError:
            pass  # cookie 获取失败不在此处阻断，getcrumb 阶段统一报错

        # Step 2: 同一 client 请求 getcrumb（携带 cookie）
        try:
            resp = self._client.get(
                "https://query1.finance.yahoo.com/v1/test/getcrumb"
            )
        except httpx.TransportError as e:
            raise TransportError(
                f"yahoo: getcrumb request failed: {e}",
                context={
                    "source": "yahoo",
                    "endpoint": "/v1/test/getcrumb",
                },
            ) from e

        # 429 → RateLimitError（D04 §3，getcrumb 端点按 IP 限流）
        if resp.status_code == 429:
            raise RateLimitError(
                "yahoo: rate limited on /v1/test/getcrumb",
                context={
                    "source": "yahoo",
                    "endpoint": "/v1/test/getcrumb",
                },
                retry_after=60,
            )

        crumb = resp.text.strip() if resp.status_code == 200 else ""
        if not crumb:
            raise SchemaError(
                "yahoo: failed to obtain crumb "
                "(empty or non-200, cookie flow may have changed)",
                context={
                    "source": "yahoo",
                    "endpoint": "/v1/test/getcrumb",
                    "status": resp.status_code,
                },
            )
        self._crumb = crumb

    def capabilities(self) -> CapabilityMatrix:
        """能力矩阵（D04 §4.5）。"""
        # Yahoo 支持的间隔粒度（部分不在 Interval 枚举中）
        return CapabilityMatrix(
            canonical_types=frozenset({CanonicalType.OHLCV}),
            intervals=frozenset({
                Interval._1M, Interval._5M, Interval._15M, Interval._30M,
                Interval._1H, Interval._2H, Interval._4H, Interval._6H,
                Interval._1D, Interval._1W,
            }),
            supports_revision=False,
            supports_websocket=False,
            max_history_days=30 * 365,  # ~30 years
        )

    def health(self) -> HealthStatus:
        """健康检查（D04 §1）。"""
        import time

        start = time.monotonic()
        try:
            self._ensure_crumb()
            elapsed_ms = int((time.monotonic() - start) * 1000)
            return HealthStatus(ok=True, latency_ms=elapsed_ms)
        except Exception as exc:
            elapsed_ms = int((time.monotonic() - start) * 1000)
            return HealthStatus(
                ok=False, latency_ms=elapsed_ms, detail=str(exc)
            )

    def discover(self) -> list[InstrumentRef]:
        """Yahoo discover 可选（P0 不实现）。"""
        return []

    def fetch(self, request: FetchRequest) -> Iterator[RawBatch]:
        """获取 Yahoo chart 数据（D04 §4.5）。

        通过 /v8/finance/chart/{symbol} 端点获取 OHLCV 数据。
        period1/period2 为 epoch 秒（含端点）。
        由实现内聚分页（D04 §1）。

        Args:
            request: 包含 symbol、interval、start、end 参数。

        Yields:
            RawBatch 迭代器。
        """
        symbol = request.params.get("symbol", "")
        if not symbol:
            raise ValueError("yahoo: symbol is required in request.params")

        interval = request.params.get("interval", "1d")
        # Yahoo interval 格式转换
        yahoo_interval = self._normalize_interval(interval)

        # epoch 秒（D04 §4.5：period1/period2 为 epoch 秒）
        start_epoch = int(request.start.timestamp()) if request.start else None
        end_epoch = int(request.end.timestamp()) if request.end else None

        if start_epoch is None:
            raise ValueError("yahoo: start is required")
        if end_epoch is None:
            raise ValueError("yahoo: end is required")

        yield self._fetch_chart(symbol, start_epoch, end_epoch, yahoo_interval, interval)

    def _fetch_chart(
        self,
        symbol: str,
        start: int,
        end: int,
        interval: str,
        canonical_interval: str = "1d",
    ) -> RawBatch:
        """请求 Yahoo chart 端点（D04 §4.5）。

        Args:
            symbol: Yahoo 交易对符号（如 AAPL）。
            start: 起始时间 epoch 秒（含端点）。
            end: 结束时间 epoch 秒（含端点）。
            interval: Yahoo 格式间隔（1m, 5d, 1wk 等）。
            canonical_interval: Interval 枚举值格式间隔（写入 raw_meta 供
                normalize 构造 OHLCV 身份键，D02 §2 L43）。

        Returns:
            RawBatch 原始响应。
        """
        params: dict[str, Any] = {
            "symbol": symbol,
            "period1": start,
            "period2": end,
            "interval": interval,
            "events": "split,dividends",
        }

        # 需要 crumb 认证（_ensure_crumb 成功后保证非空）
        self._ensure_crumb()
        params["crumb"] = self._crumb

        self._rate_limiter.acquire(1)

        try:
            response = self._client.get(
                f"/v8/finance/chart/{symbol}", params=params
            )
        except httpx.TransportError as e:
            raise TransportError(
                f"yahoo: chart request failed: {e}",
                context={
                    "source": "yahoo",
                    "endpoint": "/v8/finance/chart",
                    "symbol": symbol,
                },
            ) from e

        status_code = response.status_code

        # 错误映射（D04 §3）
        if status_code == 429:
            retry_after = int(response.headers.get("retry-after", 60))
            raise RateLimitError(
                f"yahoo: rate limited on /v8/finance/chart/{symbol}",
                context={
                    "source": "yahoo",
                    "endpoint": "/v8/finance/chart",
                },
                retry_after=retry_after,
            )
        if status_code in (401, 403):
            raise SchemaError(
                "yahoo: auth failed "
                "(crumb/cookie may have changed)",
                context={
                    "source": "yahoo",
                    "endpoint": "/v8/finance/chart",
                    "status": status_code,
                },
            )

        if status_code == 404 or (status_code >= 400 < 500):
            raise ProviderError(
                f"yahoo: HTTP {status_code} on chart endpoint",
                context={
                    "source": "yahoo",
                    "endpoint": "/v8/finance/chart",
                    "status": status_code,
                    "symbol": symbol,
                },
            )
        if status_code >= 500:
            raise TransportError(
                f"yahoo: HTTP {status_code} on chart endpoint",
                context={
                    "source": "yahoo",
                    "endpoint": "/v8/finance/chart",
                    "status": status_code,
                },
            )

        # 解析响应
        data = response.json()

        # 检查 chart 是否返回空结果（不是错误，是正常 0 行）
        chart = data.get("chart")
        if chart is not None and chart.get("result") is None:
            error_result = chart.get("error")
            if error_result:
                raise ProviderError(
                    f"yahoo: chart error for "
                    f"{symbol}: {error_result}",
                    context={
                        "source": "yahoo",
                        "endpoint": "/v8/finance/chart",
                        "symbol": symbol,
                        "error": error_result,
                    },
                )
            # 空结果 = 正常 0 行
            return RawBatch(
                endpoint="/v8/finance/chart",
                payload={"chart": {"result": []}},
                raw_meta={
                    "http_status": status_code,
                    "fetched_at": datetime.now(UTC).replace(tzinfo=None),
                    "url": str(response.url),
                    "symbol": symbol,
                    "interval": canonical_interval,
                },
            )

        return RawBatch(
            endpoint="/v8/finance/chart",
            payload=data,
            raw_meta={
                "http_status": status_code,
                "fetched_at": datetime.now(UTC).replace(tzinfo=None),
                "url": str(response.url),
                "symbol": symbol,
                "interval": canonical_interval,
            },
        )

    @staticmethod
    def _normalize_interval(interval: str) -> str:
        """将 Interval 枚举值转换为 Yahoo 格式。

        Yahoo chart 端点支持的 interval 格式与 Interval 枚举不完全一致：
        - Yahoo: 1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo
        - Interval: 1m, 5m, 15m, 30m, 1H, 2H, 4H, 6H, 8H, 12H, 1D, 1W

        Args:
            interval: Interval 枚举值字符串。

        Returns:
            Yahoo 格式间隔字符串。
        """
        mapping = {
            "1m": "1m",
            "5m": "5m",
            "15m": "15m",
            "30m": "30m",
            "1h": "1h",
            "2h": "2h",
            "4h": "4h",
            "6h": "6h",
            "8h": "8h",
            "12h": "12h",
            "1d": "1d",
            "1w": "1wk",
            "5d": "5d",
            "3mo": "3mo",
            "1mo": "1mo",
            "90m": "90m",
            "60m": "60m",
        }
        result = mapping.get(interval)
        if result is None:
            # 回退：尝试直接使用（Yahoo 可能接受未映射的值）
            if interval in ("1m", "5m", "15m", "30m", "1d", "1h", "1wk", "1mo"):
                return interval
            raise ValueError(f"yahoo: unsupported interval '{interval}'")
        return result

    def normalize(self, raw: RawBatch) -> list[BaseRecord]:
        """将 Yahoo chart 响应转为 OHLCV 记录（D04 §4.5）。

        处理 chart 响应中的 OHLCV 数据，包含 split/dividend 事件。
        时间语义（审计 F-09）：timestamp[] 为 epoch 秒 → us ×1e6。

        Args:
            raw: RawBatch from fetch().

        Returns:
            list of OHLCV records.
        """
        if raw.endpoint != "/v8/finance/chart":
            raise ValueError(f"yahoo: unknown endpoint: {raw.endpoint}")

        payload = raw.payload
        if not isinstance(payload, dict):
            return []

        chart = payload.get("chart")
        if not chart or not isinstance(chart, dict):
            return []

        result_list = chart.get("result")
        if not result_list or not isinstance(result_list, list) or len(result_list) == 0:
            return []

        result = result_list[0]
        symbol = raw.raw_meta.get("symbol", "UNKNOWN")

        # interval 是 OHLCV 身份键组成部分（D02 §2 L43），缺失/非法必须熔断
        interval_raw = raw.raw_meta.get("interval")
        if not interval_raw:
            raise ProviderError("yahoo: interval missing in raw_meta for chart data")
        try:
            interval_enum = Interval(interval_raw)
        except ValueError as e:
            raise ProviderError(
                f"yahoo: unsupported chart interval: {interval_raw}"
            ) from e

        # 提取数据数组（D04 §4.5）
        # 真实响应形状：OHLCV 数组在 indicators.quote[0]（result.quote 不存在）
        timestamps = result.get("timestamp", [])
        quote = result.get("quote", {})
        if not quote and isinstance(result.get("indicators"), dict):
            quote_list = result["indicators"].get("quote", [])
            if quote_list and isinstance(quote_list[0], dict):
                quote = quote_list[0]
        meta = result.get("meta", {})
        events = result.get("events", {})
        # splits/dividends 保留用于后续 adjustment 四元组（D04 §4.5）
        # (unused — stored in raw_meta for downstream use)
        _splits = events.get("splits", {})  # noqa: F841
        _dividends = events.get("dividends", {})  # noqa: F841

        records: list[OHLCV] = []
        now = datetime.now(UTC).replace(tzinfo=None)

        for i, ts in enumerate(timestamps):
            try:
                # ts 是 epoch 秒 → datetime UTC（审计 F-09；R2-06：替换
                # 已弃用的 utcfromtimestamp，Python 3.14 移除）
                event_time = datetime.fromtimestamp(ts, UTC).replace(tzinfo=None)

                open_list = quote.get("open", [])
                high_list = quote.get("high", [])
                low_list = quote.get("low", [])
                close_list = quote.get("close", [])
                volume_list = quote.get("volume", [])

                open_val = (
                    float(open_list[i])
                    if i < len(open_list) else None
                )
                high_val = (
                    float(high_list[i])
                    if i < len(high_list) else None
                )
                low_val = (
                    float(low_list[i])
                    if i < len(low_list) else None
                )
                close_val = (
                    float(close_list[i])
                    if i < len(close_list) else None
                )
                volume_val = (
                    float(volume_list[i])
                    if i < len(volume_list) else 0.0
                )

                # 跳过全空记录
                if any(v is None for v in [open_val, high_val, low_val, close_val]):
                    continue

                # 验证数值有限性
                values = [open_val, high_val, low_val, close_val, volume_val]
                if not all(math.isfinite(v) for v in values):  # type: ignore[arg-type]
                    continue

                market_id = f"YAHOO:{meta.get('symbol', symbol.upper())}:SPOT"

                record = OHLCV(
                    schema_version="1.0",
                    source="yahoo",
                    source_id=meta.get("symbol", symbol),
                    source_timestamp=event_time,
                    ingest_timestamp=now,
                    raw_record_id=f"yahoo:ohlcv:{symbol}:{ts}",
                    quality_status=QualityStatus.VALID,
                    quality_reason=None,
                    market_id=market_id,
                    event_time=event_time,
                    interval=interval_enum,
                    open=open_val,  # type: ignore[arg-type]
                    close=close_val,  # type: ignore[arg-type]
                    high=high_val,  # type: ignore[arg-type]
                    low=low_val,  # type: ignore[arg-type]
                    volume=volume_val if volume_val > 0 else 0.0,
                )
                records.append(record)
            except (ValueError, IndexError, TypeError):
                # 时间戳转换失败或数组索引越界 → 跳过
                continue

        return records  # type: ignore[return-value]

    def validate(self, records: list[BaseRecord]) -> QualityReport:
        """验证记录（D04 §1，委托 quality.rules）。

        P0：基础 OHLCV 校验。
        """
        findings: list[QualityFinding] = []

        for record in records:
            if not isinstance(record, OHLCV):
                continue

            key = str(getattr(record, "natural_key", lambda: ("unknown",))())

            if record.low > record.open or record.low > record.close:
                findings.append(QualityFinding(
                    record_key=key,
                    rule_id="Q-RANGE-001",
                    severity="ERROR",
                    detail=(
                        f"low={record.low} should be <= "
                        f"min(open={record.open}, close={record.close})"
                    ),
                ))
            if record.high < record.open or record.high < record.close:
                findings.append(QualityFinding(
                    record_key=key,
                    rule_id="Q-RANGE-001",
                    severity="ERROR",
                    detail=(
                        f"high={record.high} should be >= "
                        f"max(open={record.open}, close={record.close})"
                    ),
                ))

        return QualityReport(findings=findings)

    def checkpoint_from(
        self, raw: RawBatch | list[BaseRecord]
    ) -> str | None:
        """从 raw batch 或 records 中提取 checkpoint cursor（D04 §1）。

        返回最后一个记录的 event_time（epoch 秒）。
        """
        if isinstance(raw, RawBatch):
            payload = raw.payload
            if isinstance(payload, dict):
                chart = payload.get("chart")
                if chart and isinstance(chart, dict):
                    result_list = chart.get("result")
                    if result_list and isinstance(result_list, list) and len(result_list) > 0:
                        result = result_list[0]
                        timestamps = result.get("timestamp", [])
                        if timestamps:
                            last_ts = timestamps[-1]
                            return str(last_ts)

        elif isinstance(raw, list) and raw and len(raw) > 0:
            last_record = raw[-1]
            if isinstance(last_record, OHLCV):
                return str(int(last_record.event_time.timestamp()))

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
