"""Binance Spot connector（D04 §4.1，DATA-SOURCE-001）。

实现 DataConnector 协议，对接 Binance Spot REST API：
- /api/v3/klines → OHLCV
- /api/v3/aggTrades → TRADE
- /api/v3/ticker/24hr → TICKER

时间语义（审计 F-09）：源侧 ms epoch → 存储 us ×1000（无损）。
"""

from __future__ import annotations

import math
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx
import structlog

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
    ChronoForgeError,
    ProviderError,
    RateLimitError,
    TransportError,
    map_http_status,
)
from chronoforge.connectors.ratelimit import RateLimiter
from chronoforge.models.base import BaseRecord
from chronoforge.models.derivatives import Interval
from chronoforge.models.enums import CanonicalType, QualityStatus
from chronoforge.models.market import OHLCV, TICKER, TRADE, Side, interval_seconds
from chronoforge.models.quality import QualityFinding, QualityReport
from chronoforge.models.reference import parse_binance

logger = structlog.get_logger()

# Binance Spot 限流配置（D04 §4.1）
_BINFREQ_PER_MIN = 1200
# Binance klines endpoint weight
_KLINE_WEIGHT = 2
_TRADE_WEIGHT = 2
_TICKER_WEIGHT = 1

# Supported intervals (subset of Interval enum that maps to D04 §4.1)
_SUPPORTED_INTERVALS = frozenset({
    Interval._1M, Interval._5M, Interval._1H, Interval._1D,
})


@dataclass
class _Settings:
    """Minimal settings for BinanceSpotConnector（DEF-002 延后至 CLI-001）。"""

    http_timeout_s: float = 30.0
    base_url: str = "https://api.binance.com"


class BinanceSpotConnector(DataConnector):
    """Binance Spot REST connector（D04 §4.1）。

    Args:
        settings: 连接配置（http 超时、base_url）。可传入 Settings 实例或 _Settings。
    """

    source_id = "binance_spot"

    def __init__(self, settings: Any | None = None) -> None:
        if settings is None:
            settings = _Settings()

        self._settings: Any = settings
        timeout = getattr(settings, "http_timeout_s", 30.0)
        base_url = getattr(settings, "base_url", "https://api.binance.com")
        # 支持 HTTP_PROXY / HTTPS_PROXY 环境变量
        import os
        proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")
        self._client = httpx.Client(
            base_url=base_url,
            timeout=timeout,
            headers={"User-Agent": "ChronoForge/binance_spot"},
            proxy=proxy or None,
        )
        self._rate_limiter = RateLimiter(
            rate=_BINFREQ_PER_MIN / 60.0,
            burst=_BINFREQ_PER_MIN,
        )
        # InstrumentResolver 单点实现（connector 只调用不实现解析逻辑）

    def capabilities(self) -> CapabilityMatrix:
        """返回连接器能力矩阵（D04 §4.1）。"""
        return CapabilityMatrix(
            canonical_types=frozenset({
                CanonicalType.OHLCV, CanonicalType.TRADE, CanonicalType.TICKER
            }),
            intervals=_SUPPORTED_INTERVALS,
            supports_revision=False,
            supports_websocket=False,
            max_history_days=None,
        )

    def health(self) -> HealthStatus:
        """健康检查（D04 §1）。"""
        import time
        start = time.monotonic()
        try:
            resp = self._client.get("/api/v3/ping", timeout=5.0)
            elapsed_ms = int((time.monotonic() - start) * 1000)
            if resp.status_code == 200:
                return HealthStatus(
                    ok=True, latency_ms=elapsed_ms
                )
            return HealthStatus(
                ok=False, latency_ms=elapsed_ms,
                detail=f"status={resp.status_code}"
            )
        except Exception as exc:
            elapsed_ms = int((time.monotonic() - start) * 1000)
            return HealthStatus(
                ok=False, latency_ms=elapsed_ms, detail=str(exc)
            )

    def discover(self) -> list[InstrumentRef]:
        """返回支持的交易对列表（P0 返回硬编码常用交易对）。"""
        # P0：返回 BTCUSDT 和 ETHUSDT 作为默认交易对
        # P1：通过 /api/v3/exchangeInfo 动态获取
        common_symbols = ["BTCUSDT", "ETHUSDT"]
        result: list[InstrumentRef] = []
        for symbol in common_symbols:
            try:
                entity_id, instrument_id, market_id = parse_binance(symbol, "SPOT")
                result.append({
                    "entity_id": entity_id,
                    "instrument_id": instrument_id,
                    "market_id": market_id,
                })
            except ChronoForgeError:
                continue
        return result

    def fetch(self, request: FetchRequest) -> Iterator[RawBatch]:
        """获取数据（D04 §1，分页由实现内聚）。

        Args:
            request: FetchRequest 包含 symbol、interval 等参数。

        Yields:
            RawBatch 迭代器。
        """
        symbol = request.params.get("symbol", "")
        interval = request.params.get("interval", "")

        if not symbol:
            raise ProviderError("binance_spot: symbol is required in request.params")

        # Determine endpoint based on canonical type implied by params
        if "aggregate" in request.params.get("data_type", ""):
            yield from self._fetch_agg_trades(symbol, request)
        elif "ticker" in request.params.get("data_type", ""):
            yield from self._fetch_ticker(symbol)
        else:
            # Default: klines (OHLCV)
            if not interval:
                raise ProviderError("binance_spot: interval is required for klines")
            yield from self._fetch_klines(symbol, interval, request)

    def _fetch_klines(
        self,
        symbol: str,
        interval: str,
        request: FetchRequest,
    ) -> Iterator[RawBatch]:
        """Fetch klines endpoint with pagination（D04 §4.1）。

        Pagination: startTime/endTime scrolling window, limit=1000.
        cursor = next window startTime.
        """
        params: dict[str, Any] = {
            "symbol": symbol,
            "interval": interval,
            "limit": 1000,
        }

        if request.start is not None:
            # 起点回退一根 K 线：增量窗口按收线推进，当 K 线收线时刻落在
            # 窗口内但 openTime 在窗口起点之前时，startTime 过滤会令其
            # 永久漏采（1d K 线在小时级增量调度下必漏）。回退后未收盘
            # K 线由 normalize 过滤，重复 K 线由 natural key upsert 幂等收敛。
            rewind_ms = interval_seconds(interval) * 1000
            params["startTime"] = int(request.start.timestamp() * 1000) - rewind_ms
        if request.end is not None:
            params["endTime"] = int(request.end.timestamp() * 1000)

        while True:
            # Rate limit
            self._rate_limiter.acquire(_KLINE_WEIGHT)

            response = self._client.get("/api/v3/klines", params=params)
            self._handle_response(response)

            data = response.json()
            if not isinstance(data, list):
                raise ProviderError(f"binance_spot: unexpected klines response type: {type(data)}")

            # Don't yield empty payloads
            if not data:
                break

            yield RawBatch(
                endpoint="/api/v3/klines",
                payload=data,
                raw_meta={
                    "http_status": response.status_code,
                    "headers_subset": {
                        k: response.headers[k]
                        for k in ("x-mbx-used-weight", "x-mbx-use-mine-weight")
                        if k in response.headers
                    },
                    "url": str(response.url),
                    "interval": interval,
                },
            )

            # Pagination check
            # If end is set and last candle's closeTime >= end, stop
            if request.end is not None:
                last_close_time = data[-1][6]  # closeTime in ms
                if last_close_time >= int(request.end.timestamp() * 1000):
                    break
            # Advance cursor: next window start = last closeTime + 1ms
            params["startTime"] = data[-1][6] + 1

    def _fetch_agg_trades(
        self,
        symbol: str,
        request: FetchRequest,
    ) -> Iterator[RawBatch]:
        """Fetch aggTrades endpoint with pagination（D04 §4.1）。

        Pagination: fromId scrolling. Cursor = fromId (aggregate first trade ID).
        Q-SEQ-001: adjacent records must satisfy prev.l+1 == curr.a.
        """
        # Binance aggTrades 约束：fromId 与 startTime/endTime 组合非法（-1128）。
        # cursor 为纯数字时走 fromId 分页（时间边界客户端截断）；
        # 否则（ISO cursor/None）走时间窗口；翻页统一改 fromId-only。
        end_ms = (
            int(request.end.timestamp() * 1000) if request.end is not None else None
        )
        params: dict[str, Any] = {"symbol": symbol, "limit": 1000}
        if request.cursor is not None and request.cursor.isdigit():
            params["fromId"] = request.cursor
        else:
            if request.start is not None:
                params["startTime"] = int(request.start.timestamp() * 1000)
            if end_ms is not None:
                params["endTime"] = end_ms

        while True:
            self._rate_limiter.acquire(_TRADE_WEIGHT)

            response = self._client.get("/api/v3/aggTrades", params=params)
            self._handle_response(response)

            data = response.json()
            if not isinstance(data, list):
                raise ProviderError(
                    f"binance_spot: unexpected aggTrades "
                    f"response type: {type(data)}"
                )

            # Don't yield empty payloads
            if not data:
                break

            yield RawBatch(
                endpoint="/api/v3/aggTrades",
                payload=data,
                raw_meta={
                    "http_status": response.status_code,
                    "headers_subset": {
                        k: response.headers[k]
                        for k in ("x-mbx-used-weight",)
                        if k in response.headers
                    },
                    "url": str(response.url),
                },
            )
            # 时间边界（客户端截断，兼容 fromId-only 翻页）
            if end_ms is not None and int(data[-1]["T"]) >= end_ms:
                break
            # Advance cursor: next fromId = last aggTradeId + 1
            # aggTradeId is field 'a' in response
            last_agg_id = int(data[-1]["a"])
            # fromId-only 翻页（与 startTime/endTime 组合非法），并移除时间参数
            params = {"symbol": symbol, "limit": 1000, "fromId": str(last_agg_id + 1)}

    def _fetch_ticker(self, symbol: str) -> Iterator[RawBatch]:
        """Fetch ticker24hr endpoint（单次请求，不分页）。"""
        params = {"symbol": symbol}

        self._rate_limiter.acquire(_TICKER_WEIGHT)

        response = self._client.get("/api/v3/ticker/24hr", params=params)
        self._handle_response(response)

        data = response.json()
        if not isinstance(data, dict):
            raise ProviderError(f"binance_spot: unexpected ticker24hr response type: {type(data)}")

        yield RawBatch(
            endpoint="/api/v3/ticker/24hr",
            payload=data,
            raw_meta={
                "http_status": response.status_code,
                "headers_subset": {},
                "url": str(response.url),
            },
        )

    def _handle_response(self, response: httpx.Response) -> None:
        """处理 HTTP 响应，转换错误（D04 §3 HTTP映射）。"""
        status = response.status_code

        # Parse Retry-After header for 429
        retry_after = None
        if status == 429:
            retry_after_hdr = response.headers.get("retry-after")
            if retry_after_hdr:
                try:
                    retry_after = int(retry_after_hdr)
                except (ValueError, TypeError):
                    pass

        error_cls = map_http_status(status)
        url = getattr(response.url, "path", str(response.url)) if response.url else ""
        error_ctx = {"source": "binance_spot", "endpoint": url}

        # 2xx success - no error
        if 200 <= status < 300:
            return

        if error_cls == RateLimitError:
            raise RateLimitError(
                f"binance_spot: rate limited (status={status})",
                context=error_ctx,
                retry_after=retry_after,
            )
        if error_cls == AuthError:
            raise AuthError(
                f"binance_spot: auth failed (status={status})",
                context=error_ctx,
            )
        if error_cls == ProviderError:
            raise ProviderError(
                f"binance_spot: provider error (status={status}, body={response.text[:500]})",
                context=error_ctx,
            )
        if error_cls == TransportError:
            raise TransportError(
                f"binance_spot: transport error (status={status})",
                context=error_ctx,
            )

    def normalize(self, raw: RawBatch) -> list[BaseRecord]:
        """将原始响应转为 Canonical Type 记录（D04 §4.1）。

        Args:
            raw: RawBatch from fetch().

        Returns:
            list of BaseRecord subclasses (OHLCV, TRADE, TICKER).
        """
        match raw.endpoint:
            case "/api/v3/klines":
                return self._normalize_klines(
                    raw.payload, raw.raw_meta
                )  # type: ignore[return-value]
            case "/api/v3/aggTrades":
                return self._normalize_agg_trades(
                    raw.payload, raw.raw_meta
                )  # type: ignore[return-value]
            case "/api/v3/ticker/24hr":
                return self._normalize_ticker(
                    raw.payload, raw.raw_meta
                )  # type: ignore[return-value]
            case _:
                raise ProviderError(f"binance_spot: unknown endpoint: {raw.endpoint}")

    def _normalize_klines(
        self,
        payload: list[Any],
        raw_meta: dict[str, Any],
    ) -> list[OHLCV]:
        """Normalize klines payload to OHLCV records（D04 §4.1）。

        kline row format:
        [openTime, open, high, low, close, volume, closeTime, ...]

        Time semantics (audit F-09): source ms epoch → storage us ×1000.
        Discard unfinished klines: event_time + interval > now.
        """
        records: list[OHLCV] = []
        now = datetime.now(UTC).replace(tzinfo=None)

        # interval 是 OHLCV 身份键组成部分（D02 §2 L43），缺失/非法必须熔断
        interval_raw = raw_meta.get("interval")
        if not interval_raw:
            raise ProviderError("binance_spot: interval missing in raw_meta for klines")
        try:
            interval_enum = Interval(interval_raw)
        except ValueError as e:
            raise ProviderError(
                f"binance_spot: unsupported kline interval: {interval_raw}"
            ) from e

        for row_idx, row in enumerate(payload):
            if not isinstance(row, (list, tuple)) or len(row) < 7:
                # 审计 M-12：malformed 行显式告警（不再静默丢弃）
                logger.warning(
                    "connector.normalize_skip",
                    source="binance_spot",
                    reason="malformed_row",
                    index=row_idx,
                )
                continue

            open_time_ms = int(row[0])
            close_time_ms = int(row[6])

            # ms → us ×1000 for storage
            event_time = datetime.fromtimestamp(open_time_ms / 1000, tz=UTC).replace(tzinfo=None)

            # Discard unfinished kline: check if openTime + some reasonable interval > now
            # Since we don't know the exact interval here, we check if closeTime > now
            # (a kline that hasn't closed yet has closeTime in the future)
            if close_time_ms > int(now.timestamp() * 1000):
                # 未完成 K 线是增量获取的常态（最后一根），debug 级即可
                logger.debug(
                    "connector.normalize_skip",
                    source="binance_spot",
                    reason="unfinished_kline",
                    index=row_idx,
                )
                continue

            try:
                open_val = float(row[1])
                high_val = float(row[2])
                low_val = float(row[3])
                close_val = float(row[4])
                volume_val = float(row[5])
            except (ValueError, TypeError):
                logger.warning(
                    "connector.normalize_skip",
                    source="binance_spot",
                    reason="non_numeric_value",
                    index=row_idx,
                )
                continue

            # Validate price/volume are finite
            values = [open_val, high_val, low_val, close_val, volume_val]
            if not all(math.isfinite(v) for v in values):
                logger.warning(
                    "connector.normalize_skip",
                    source="binance_spot",
                    reason="non_finite_value",
                    index=row_idx,
                )
                continue

            # market_id = BINANCE:{symbol}:{SPOT}
            # symbol is extracted from the raw_meta URL or stored in payload meta
            market_id = self._extract_market_id(raw_meta)

            record = OHLCV(
                schema_version="1.0",
                source="binance_spot",
                source_id=str(row[0]),  # openTime as source identifier
                source_timestamp=datetime.fromtimestamp(
                    open_time_ms / 1000, tz=UTC
                ).replace(tzinfo=None),
                ingest_timestamp=now,
                raw_record_id=f"binance_spot:kline:{row[0]}",
                quality_status=QualityStatus.VALID,
                quality_reason=None,
                market_id=market_id,
                event_time=event_time,
                interval=interval_enum,
                open=open_val,
                close=close_val,
                high=high_val,
                low=low_val,
                volume=volume_val,
            )
            records.append(record)

        return records

    def _normalize_agg_trades(
        self,
        payload: list[Any],
        raw_meta: dict[str, Any],
    ) -> list[TRADE]:
        """Normalize aggTrades payload to TRADE records（D04 §4.1）。

        aggTrades fields:
        - a: aggTradeId
        - P: price
        - Q: quantity
        - T: tradeTime
        - m: isBuyerIsMaker
        - B: bestBuyOrderAggTradeId
        """
        records: list[TRADE] = []
        now = datetime.now(UTC).replace(tzinfo=None)
        market_id = self._extract_market_id(raw_meta)

        for trade in payload:
            try:
                trade_time_ms = int(trade["T"])
                price = float(trade["P"])
                quantity = float(trade["Q"])
                is_maker = trade.get("m", False)

                # ms → us ×1000
                event_time = datetime.fromtimestamp(
                    trade_time_ms / 1000, tz=UTC
                ).replace(tzinfo=None)

                # side: m=true → SELL (maker is seller), m=false → BUY
                side = Side.SELL if is_maker else Side.BUY

                # trade_id = aggTradeId (source-side unique id)
                trade_id = str(trade["a"])

                record = TRADE(
                    schema_version="1.0",
                    source="binance_spot",
                    source_id=trade_id,
                    source_timestamp=event_time,
                    ingest_timestamp=now,
                    raw_record_id=f"binance_spot:trade:{trade_id}",
                    quality_status=QualityStatus.VALID,
                    quality_reason=None,
                    market_id=market_id,
                    event_time=event_time,
                    price=price,
                    quantity=quantity,
                    side=side,
                    trade_id=trade_id,
                )
                records.append(record)
            except (KeyError, ValueError, TypeError):
                continue

        return records

    def _normalize_ticker(
        self,
        payload: dict[str, Any],
        raw_meta: dict[str, Any],
    ) -> list[TICKER]:
        """Normalize ticker24hr payload to TICKER record（D04 §4.1）。"""
        now = datetime.now(UTC).replace(tzinfo=None)
        market_id = self._extract_market_id(raw_meta)

        try:
            # Binance ticker24hr uses "openTime" (not "T")
            event_time = datetime.fromtimestamp(
                int(payload["openTime"]) / 1000, tz=UTC
            ).replace(tzinfo=None)

            record = TICKER(
                schema_version="1.0",
                source="binance_spot",
                source_id=payload.get("symbol", ""),
                source_timestamp=event_time,
                ingest_timestamp=now,
                raw_record_id=f"binance_spot:ticker:{payload.get('symbol', '')}",
                quality_status=QualityStatus.VALID,
                quality_reason=None,
                market_id=market_id,
                event_time=event_time,
                last_price=float(payload["lastPrice"]),
                bid=float(payload["bidPrice"]),
                ask=float(payload["askPrice"]),
                volume_24h=float(payload["volume"]),
                quote_volume_24h=float(payload["quoteVolume"]),
            )
            return [record]
        except (KeyError, ValueError, TypeError):
            return []

    def _extract_market_id(self, raw_meta: dict[str, Any]) -> str:
        """从 raw_meta 中提取 market_id。

        从 URL 或 endpoint 路径推断 symbol，再通过 parse_binance 生成 market_id。
        对于测试场景，raw_meta 可能包含 "symbol" 字段。
        """
        # Try to get symbol from various sources
        url = raw_meta.get("url", "")
        symbol = None

        # Extract symbol from URL query params
        if url:
            # URL format: https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m
            if "symbol=" in url:
                symbol = url.split("symbol=")[1].split("&")[0]

        if symbol:
            try:
                _, _, market_id = parse_binance(symbol, "SPOT")
                return market_id
            except ChronoForgeError:
                pass

        # Fallback: return a generic market_id
        return "BINANCE:UNKNOWN:SPOT"

    def validate(
        self, records: list[BaseRecord]
    ) -> QualityReport:
        """验证记录（D04 §1，委托 quality.rules）。

        P0：实现基本校验，P1：集成 quality.rules 引擎。
        """
        findings: list[QualityFinding] = []

        for record in records:
            key = str(getattr(record, "natural_key", lambda: ("unknown",))())
            # Basic validation: check OHLCV constraints
            if isinstance(record, OHLCV):
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

        For klines: return closeTime of last batch as ISO string.
        For aggTrades: return aggTradeId of last record.
        """
        if isinstance(raw, RawBatch):
            payload = raw.payload
            endpoint = raw.endpoint

            if endpoint == "/api/v3/klines" and payload:
                # Return last closeTime as cursor
                last_close_time = payload[-1][6]  # closeTime in ms
                return str(last_close_time)
            if endpoint == "/api/v3/aggTrades" and payload:
                # Return last aggTradeId as cursor
                last_agg_id = payload[-1]["a"]
                return str(last_agg_id)

        elif isinstance(raw, list) and raw:
            last_record = raw[-1]
            if isinstance(last_record, OHLCV):
                return str(int(last_record.event_time.timestamp() * 1000))
            if isinstance(last_record, TRADE):
                return last_record.trade_id

        return None

    def close(self) -> None:
        """关闭 httpx client。"""
        self._client.close()

    def __del__(self) -> None:
        """确保资源清理。"""
        try:
            self.close()
        except Exception:
            pass
