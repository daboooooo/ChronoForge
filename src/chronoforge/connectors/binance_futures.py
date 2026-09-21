"""Binance Futures (USDT-M) connector（D04 §4.2，DATA-SOURCE-002）。

实现 DataConnector 协议，对接 Binance Futures USDT-M REST API：
- /fapi/v1/klines → OHLCV
- /fapi/v1/fundingRate → FUNDING
- /fapi/v1/openInterest → OPEN_INTEREST

时间语义（审计 F-09）：源侧 ms epoch → 存储 us ×1000（无损）。
funding 结算时间 = fundingTime（含端点）。
OI 快照 event_time = fetched_at（无源侧时间）。
"""

from __future__ import annotations

import math
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
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
from chronoforge.models.market import FUNDING, OHLCV, OPEN_INTEREST
from chronoforge.models.reference import parse_binance
from chronoforge.quality.report import QualityFinding, QualityReport

# Binance Futures USDT-M 限流配置（D04 §4.2）
_BINFREQ_PER_MIN = 1200
_KLINE_WEIGHT = 2
_FUNDING_WEIGHT = 1
_OI_WEIGHT = 2

# Supported intervals (D04 §4.2)
_SUPPORTED_INTERVALS = frozenset({
    Interval._1M, Interval._5M, Interval._15M, Interval._30M,
    Interval._1H, Interval._2H, Interval._4H, Interval._6H,
    Interval._8H, Interval._12H, Interval._1D, Interval._1W,
})


@dataclass
class _Settings:
    """Minimal settings for BinanceFuturesConnector."""

    http_timeout_s: float = 30.0
    base_url: str = "https://fapi.binance.com"


class BinanceFuturesConnector(DataConnector):
    """Binance Futures (USDT-M) REST connector（D04 §4.2）。

    Args:
        settings: 连接配置（http 超时、base_url）。
    """

    source_id = "binance_futures"

    def __init__(self, settings: Any | None = None) -> None:
        if settings is None:
            settings = _Settings()

        self._settings: Any = settings
        timeout = getattr(settings, "http_timeout_s", 30.0)
        base_url = getattr(settings, "base_url", "https://fapi.binance.com")
        self._client = httpx.Client(
            base_url=base_url,
            timeout=timeout,
            headers={"User-Agent": "ChronoForge/binance_futures"},
        )
        self._rate_limiter = RateLimiter(
            rate=_BINFREQ_PER_MIN / 60.0,
            burst=_BINFREQ_PER_MIN,
        )

    def capabilities(self) -> CapabilityMatrix:
        """返回连接器能力矩阵（D04 §4.2）。"""
        return CapabilityMatrix(
            canonical_types=frozenset({
                CanonicalType.OHLCV, CanonicalType.FUNDING,
                CanonicalType.OPEN_INTEREST,
            }),
            intervals=_SUPPORTED_INTERVALS,
            supports_revision=False,
            supports_websocket=False,  # liquidation WebSocket = P1
            max_history_days=None,
        )

    def health(self) -> HealthStatus:
        """健康检查（D04 §1）。"""
        import time
        start = time.monotonic()
        try:
            resp = self._client.get("/fapi/v1/ping", timeout=5.0)
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
        common_symbols = ["BTCUSDT", "ETHUSDT"]
        result: list[InstrumentRef] = []
        for symbol in common_symbols:
            try:
                entity_id, instrument_id, market_id = parse_binance(
                    symbol, "USDT-FUT"
                )
                result.append({
                    "entity_id": entity_id,
                    "instrument_id": instrument_id,
                    "market_id": market_id,
                })
            except ChronoForgeError:
                continue
        return result

    def fetch(self, request: FetchRequest) -> Iterator[RawBatch]:
        """获取数据（D04 §4.2，分页由实现内聚）。

        Args:
            request: FetchRequest 包含 symbol、interval 等参数。

        Yields:
            RawBatch 迭代器。
        """
        symbol = request.params.get("symbol", "")
        data_type = request.params.get("data_type", "")

        if not symbol:
            raise ProviderError(
                "binance_futures: symbol is required in request.params"
            )

        if data_type == "funding":
            yield from self._fetch_funding(symbol, request)
        elif data_type == "open_interest":
            yield from self._fetch_oi(symbol, request)
        else:
            # Default: klines (OHLCV)
            interval = request.params.get("interval", "")
            if not interval:
                raise ProviderError(
                    "binance_futures: interval is required for klines"
                )
            yield from self._fetch_klines(symbol, interval, request)

    def _fetch_klines(
        self,
        symbol: str,
        interval: str,
        request: FetchRequest,
    ) -> Iterator[RawBatch]:
        """Fetch klines endpoint with pagination（D04 §4.2）。

        Pagination: startTime/endTime scrolling window, limit=1000.
        cursor = next window startTime.
        """
        params: dict[str, Any] = {
            "symbol": symbol,
            "interval": interval,
            "limit": 1000,
        }

        if request.start is not None:
            params["startTime"] = int(request.start.timestamp() * 1000)
        if request.end is not None:
            params["endTime"] = int(request.end.timestamp() * 1000)

        while True:
            self._rate_limiter.acquire(_KLINE_WEIGHT)

            response = self._client.get("/fapi/v1/klines", params=params)
            self._handle_response(response)

            data = response.json()
            if not isinstance(data, list):
                raise ProviderError(
                    f"binance_futures: unexpected klines response type: "
                    f"{type(data)}"
                )

            if not data:
                break

            yield RawBatch(
                endpoint="/fapi/v1/klines",
                payload=data,
                raw_meta={
                    "http_status": response.status_code,
                    "headers_subset": {
                        k: response.headers[k]
                        for k in ("x-mbx-used-weight", "x-mbx-use-mine-weight")
                        if k in response.headers
                    },
                    "url": str(response.url),
                    "symbol": symbol,
                    "interval": interval,
                },
            )

            # Pagination check
            if request.end is not None:
                last_close_time = data[-1][6]  # closeTime in ms
                if last_close_time >= int(request.end.timestamp() * 1000):
                    break
            # Advance cursor: next window start = last closeTime + 1ms
            params["startTime"] = data[-1][6] + 1

    def _fetch_funding(
        self,
        symbol: str,
        request: FetchRequest,
    ) -> Iterator[RawBatch]:
        """Fetch fundingRate endpoint with pagination（D04 §4.2）。

        cursor = startTime (fundingTime) scrolling.
        Funding settlement cycle = 8h.
        """
        params: dict[str, Any] = {"symbol": symbol}

        if request.start is not None:
            params["startTime"] = int(request.start.timestamp() * 1000)
        if request.cursor is not None:
            params["startTime"] = int(request.cursor)

        while True:
            self._rate_limiter.acquire(_FUNDING_WEIGHT)

            response = self._client.get("/fapi/v1/fundingRate", params=params)
            self._handle_response(response)

            data = response.json()
            if not isinstance(data, list):
                raise ProviderError(
                    f"binance_futures: unexpected fundingRate "
                    f"response type: {type(data)}"
                )

            if not data:
                break

            yield RawBatch(
                endpoint="/fapi/v1/fundingRate",
                payload=data,
                raw_meta={
                    "http_status": response.status_code,
                    "url": str(response.url),
                    "symbol": symbol,
                },
            )

            # Advance cursor: next startTime = last fundingTime + 1ms
            params["startTime"] = str(int(data[-1]["fundingTime"]) + 1)

    def _fetch_oi(
        self,
        symbol: str,
        request: FetchRequest,
    ) -> Iterator[RawBatch]:
        """Fetch openInterest endpoint（单次请求，轮询快照）。

        OI is a snapshot with no pagination.
        cursor = last event_time on retry.
        """
        params: dict[str, Any] = {"symbol": symbol}

        self._rate_limiter.acquire(_OI_WEIGHT)

        response = self._client.get("/fapi/v1/openInterest", params=params)
        self._handle_response(response)

        data = response.json()
        if not isinstance(data, dict):
            raise ProviderError(
                f"binance_futures: unexpected openInterest "
                f"response type: {type(data)}"
            )

        yield RawBatch(
            endpoint="/fapi/v1/openInterest",
            payload=data,
            raw_meta={
                "http_status": response.status_code,
                "url": str(response.url),
                "symbol": symbol,
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
        error_ctx = {"source": "binance_futures", "endpoint": url}

        if 200 <= status < 300:
            return

        if error_cls == RateLimitError:
            raise RateLimitError(
                f"binance_futures: rate limited (status={status})",
                context=error_ctx,
                retry_after=retry_after,
            )
        if error_cls == AuthError:
            raise AuthError(
                f"binance_futures: auth failed (status={status})",
                context=error_ctx,
            )
        if error_cls == ProviderError:
            raise ProviderError(
                f"binance_futures: provider error "
                f"(status={status}, body={response.text[:500]})",
                context=error_ctx,
            )
        if error_cls == TransportError:
            raise TransportError(
                f"binance_futures: transport error (status={status})",
                context=error_ctx,
            )

    def normalize(self, raw: RawBatch) -> list[BaseRecord]:
        """将原始响应转为 Canonical Type 记录（D04 §4.2）。

        Args:
            raw: RawBatch from fetch().

        Returns:
            list of BaseRecord subclasses (OHLCV, FUNDING, OPEN_INTEREST).
        """
        match raw.endpoint:
            case "/fapi/v1/klines":
                return self._normalize_klines(
                    raw.payload, raw.raw_meta
                )  # type: ignore[return-value]
            case "/fapi/v1/fundingRate":
                return self._normalize_funding(
                    raw.payload, raw.raw_meta
                )  # type: ignore[return-value]
            case "/fapi/v1/openInterest":
                return self._normalize_oi(
                    raw.payload, raw.raw_meta
                )  # type: ignore[return-value]
            case _:
                raise ProviderError(
                    f"binance_futures: unknown endpoint: {raw.endpoint}"
                )

    def _normalize_klines(
        self,
        payload: list[Any],
        raw_meta: dict[str, Any],
    ) -> list[OHLCV]:
        """Normalize klines payload to OHLCV records（D04 §4.2）。

        kline row format:
        [openTime, open, high, low, close, volume, closeTime, ...]

        Time semantics (audit F-09): source ms epoch → storage us ×1000.
        """
        records: list[OHLCV] = []
        now = datetime.now(UTC).replace(tzinfo=None)

        # interval 是 OHLCV 身份键组成部分（D02 §2 L43），缺失/非法必须熔断
        interval_raw = raw_meta.get("interval")
        if not interval_raw:
            raise ProviderError(
                "binance_futures: interval missing in raw_meta for klines"
            )
        try:
            interval_enum = Interval(interval_raw)
        except ValueError as e:
            raise ProviderError(
                f"binance_futures: unsupported kline interval: {interval_raw}"
            ) from e

        symbol = raw_meta.get("symbol", "")
        if symbol:
            try:
                _, _, market_id = parse_binance(symbol, "USDT-FUT")
            except ChronoForgeError:
                market_id = "BINANCE:UNKNOWN:USDT-FUT"
        else:
            market_id = "BINANCE:UNKNOWN:USDT-FUT"

        for row in payload:
            if not isinstance(row, (list, tuple)) or len(row) < 7:
                continue

            open_time_ms = int(row[0])
            close_time_ms = int(row[6])

            # ms → us ×1000 for storage
            event_time = datetime.fromtimestamp(
                open_time_ms / 1000, tz=UTC
            ).replace(tzinfo=None)

            # Discard unfinished kline: closeTime in future
            if close_time_ms > int(now.timestamp() * 1000):
                continue

            try:
                open_val = float(row[1])
                high_val = float(row[2])
                low_val = float(row[3])
                close_val = float(row[4])
                volume_val = float(row[5])
            except (ValueError, TypeError):
                continue

            values = [open_val, high_val, low_val, close_val, volume_val]
            if not all(math.isfinite(v) for v in values):
                continue

            record = OHLCV(
                schema_version="1.0",
                source="binance_futures",
                source_id=str(open_time_ms),
                source_timestamp=datetime.fromtimestamp(
                    open_time_ms / 1000, tz=UTC
                ).replace(tzinfo=None),
                ingest_timestamp=now,
                raw_record_id=f"binance_futures:kline:{open_time_ms}",
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

    def _normalize_funding(
        self,
        payload: list[Any],
        raw_meta: dict[str, Any],
    ) -> list[FUNDING]:
        """Normalize fundingRate payload to FUNDING records（D04 §4.2）。

        fundingRate response fields:
        - symbol: str
        - fundingRate: float (decimal, e.g. 0.00001 = 0.001%)
        - fundingTime: int (ms epoch)
        - markPrice: float

        Time semantics (audit F-09): fundingTime = event_time (结算时间，含端点)。
        """
        records: list[FUNDING] = []
        now = datetime.now(UTC).replace(tzinfo=None)

        symbol = raw_meta.get("symbol", "")
        if symbol:
            try:
                _, _, market_id = parse_binance(symbol, "USDT-FUT")
            except ChronoForgeError:
                market_id = "BINANCE:UNKNOWN:USDT-FUT"
        else:
            market_id = "BINANCE:UNKNOWN:USDT-FUT"

        for item in payload:
            try:
                funding_time_ms = int(item["fundingTime"])
                funding_rate = float(item["fundingRate"])

                # ms → us ×1000
                event_time = datetime.fromtimestamp(
                    funding_time_ms / 1000, tz=UTC
                ).replace(tzinfo=None)

                # nextFundingTime is optional (only available for recent records)
                next_funding_ms = item.get("nextFundingTime")
                if next_funding_ms is not None:
                    next_funding_time = datetime.fromtimestamp(
                        int(next_funding_ms) / 1000, tz=UTC
                    ).replace(tzinfo=None)
                else:
                    # Fallback: nextFundingTime must be > event_time per FUNDING model
                    next_funding_time = event_time + timedelta(seconds=1)

                record = FUNDING(
                    schema_version="1.0",
                    source="binance_futures",
                    source_id=str(funding_time_ms),
                    source_timestamp=event_time,
                    ingest_timestamp=now,
                    raw_record_id=(
                        f"binance_futures:funding:{funding_time_ms}"
                    ),
                    quality_status=QualityStatus.VALID,
                    quality_reason=None,
                    market_id=market_id,
                    event_time=event_time,
                    funding_rate=funding_rate,
                    next_funding_time=next_funding_time,
                )
                records.append(record)
            except (KeyError, ValueError, TypeError):
                continue

        return records

    def _normalize_oi(
        self,
        payload: dict[str, Any],
        raw_meta: dict[str, Any],
    ) -> list[OPEN_INTEREST]:
        """Normalize openInterest payload to OPEN_INTEREST record（D04 §4.2）。

        openInterest response format:
        {
            "symbol": "BTCUSDT",
            "openInterest": "0.123",
            "timestamp": 1234567890 (ms epoch),
            "time": 1234567890 (ms epoch, same as timestamp)
        }

        Time semantics (audit F-09): OI snapshot event_time = fetched_at.
        """
        now = datetime.now(UTC).replace(tzinfo=None)

        symbol = raw_meta.get("symbol", "")
        if symbol:
            try:
                _, _, market_id = parse_binance(symbol, "USDT-FUT")
            except ChronoForgeError:
                market_id = "BINANCE:UNKNOWN:USDT-FUT"
        else:
            market_id = "BINANCE:UNKNOWN:USDT-FUT"

        try:
            oi_value = float(payload["openInterest"])

            # OI snapshot: use timestamp if available, else fetched_at
            ts_ms = payload.get("timestamp") or payload.get("time")
            if ts_ms is not None:
                event_time = datetime.fromtimestamp(
                    int(ts_ms) / 1000, tz=UTC
                ).replace(tzinfo=None)
            else:
                event_time = now

            record = OPEN_INTEREST(
                schema_version="1.0",
                source="binance_futures",
                source_id=symbol or "UNKNOWN",
                source_timestamp=event_time,
                ingest_timestamp=now,
                raw_record_id=(
                    f"binance_futures:oi:{symbol}:{int(ts_ms)}"
                    if ts_ms
                    else f"binance_futures:oi:{symbol}"
                ),
                quality_status=QualityStatus.VALID,
                quality_reason=None,
                market_id=market_id,
                event_time=event_time,
                open_interest=oi_value,
                unit="contracts",
            )
            return [record]
        except (KeyError, ValueError, TypeError):
            return []

    def validate(
        self, records: list[BaseRecord]
    ) -> QualityReport:
        """验证记录（D04 §1，委托 quality.rules）。"""
        findings: list[QualityFinding] = []

        for record in records:
            key = str(
                getattr(record, "natural_key", lambda: ("unknown",))()
            )

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

        For klines: return closeTime of last batch as string.
        For fundingRate: return fundingTime of last batch as string.
        For OI: return timestamp of the snapshot.
        """
        if isinstance(raw, RawBatch):
            payload = raw.payload
            endpoint = raw.endpoint

            if endpoint == "/fapi/v1/klines" and payload:
                last_close_time = payload[-1][6]  # closeTime in ms
                return str(last_close_time)

            if endpoint == "/fapi/v1/fundingRate" and payload:
                last_funding_time = payload[-1]["fundingTime"]
                return str(int(last_funding_time))

            if endpoint == "/fapi/v1/openInterest" and isinstance(payload, dict):
                ts = payload.get("timestamp") or payload.get("time")
                if ts:
                    return str(int(ts))

        elif isinstance(raw, list) and raw:
            last_record = raw[-1]
            if isinstance(last_record, OHLCV):
                return str(
                    int(last_record.event_time.timestamp() * 1000)
                )
            if isinstance(last_record, FUNDING):
                return str(
                    int(last_record.event_time.timestamp() * 1000)
                )
            if isinstance(last_record, OPEN_INTEREST):
                return str(
                    int(last_record.event_time.timestamp() * 1000)
                )

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
