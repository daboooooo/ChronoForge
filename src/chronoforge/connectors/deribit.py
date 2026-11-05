"""Deribit connector（D04 §4.3，DATA-SOURCE-003）。

实现 DataConnector 协议，对接 Deribit REST API：
- public/get_instruments → INSTRUMENT（discover 必须实现）
- public/get_book_summary_by_currency → OPTION 快照
- public/get_tradingview_chart_data → OHLCV/IV 历史

时间语义（审计 F-09）：
- timestamp ms → us ×1000
- expiration（"26SEP26" 月份码）解析为 UTC 日期；源即 UTC 无 DST 歧义
- continuity_model = ALWAYS_OPEN（7×24）；到期后 instrument 标 SUSPECT（Q-RANGE-003）
"""

from __future__ import annotations

import calendar
import math
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any, cast

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
    ProviderError,
    RateLimitError,
    TransportError,
    map_http_status,
)
from chronoforge.connectors.ratelimit import RateLimiter
from chronoforge.models.base import BaseRecord
from chronoforge.models.derivatives import (
    OPTION,
    Interval,
)
from chronoforge.models.enums import CanonicalType, QualityStatus
from chronoforge.models.market import OHLCV, interval_seconds
from chronoforge.models.quality import QualityFinding, QualityReport
from chronoforge.models.reference import (
    INSTRUMENT,
    InstrumentResolver,
    InstrumentType,
    OptionType,
    parse_deribit,
)

# Deribit 限流配置（D04 §4.3）
# Deribit: 100 req/min for public endpoints, 2000 for authenticated
_DERIBIT_REQ_PER_MIN = 100
# Deribit endpoint weights (conservative)
_INSTRUMENT_WEIGHT = 1
_BOOK_SUMMARY_WEIGHT = 1
_CHART_DATA_WEIGHT = 1

# chart 分页护栏：continuation token 重复或页数超限即失败，
# 避免源端分页异常导致无限请求/重复采集（审计 P2，2026-11-05）
_MAX_CHART_PAGES = 1000

# Supported intervals mapped from D04 §4.3 resolution values (in minutes)
# Deribit get_tradingview_chart_data uses: 1, 5, 15, 30, 60, 240, 1440
_DERIBIT_RESOLUTION_MAP: dict[str, Interval] = {
    "1": Interval._1M,
    "5": Interval._5M,
    "15": Interval._15M,
    "30": Interval._30M,
    "60": Interval._1H,
    "240": Interval._4H,
    "1440": Interval._1D,
}
# Reverse map for lookup
_DERIBIT_INTERVALS: frozenset[Interval] = frozenset(_DERIBIT_RESOLUTION_MAP.values())

logger = structlog.get_logger()


def _dt_to_ms(dt: datetime) -> int:
    """UTC naive datetime → epoch ms（D04 §1：FetchRequest 时间为 UTC naive）。"""
    return calendar.timegm(dt.timetuple()) * 1000 + dt.microsecond // 1000


def _quote_float(value: Any) -> float | None:
    """报价字段（mark/bid/ask）转 float。

    源端显式 null（或字段缺失）→ None，表示该侧无报价；不可降级为
    0.0——0.0 是真实价格。
    """
    return None if value is None else float(value)


@dataclass
class _Settings:
    """DeribitConnector 配置。"""

    http_timeout_s: float = 30.0
    base_url: str = "https://www.deribit.com"


class DeribitConnector(DataConnector):
    """Deribit REST connector（D04 §4.3）。

    Args:
        settings: 连接配置（http 超时、base_url）。
    """

    source_id = "deribit"

    def __init__(self, settings: Any | None = None) -> None:
        if settings is None:
            settings = _Settings()

        self._settings: Any = settings
        timeout = getattr(settings, "http_timeout_s", 30.0)
        base_url = getattr(settings, "base_url", "https://www.deribit.com")
        # 请求路径已含 /api/v2 前缀；注册表 base_url 可能带同样后缀，剥掉防双重前缀
        base_url = base_url.removesuffix("/api/v2").rstrip("/")
        self._client = httpx.Client(
            base_url=base_url,
            timeout=timeout,
            headers={"User-Agent": "ChronoForge/deribit"},
        )
        self._rate_limiter = RateLimiter(
            rate=_DERIBIT_REQ_PER_MIN / 60.0,
            burst=_DERIBIT_REQ_PER_MIN,
        )
        self._resolver = InstrumentResolver()

    def capabilities(self) -> CapabilityMatrix:
        """返回连接器能力矩阵（D04 §4.3）。"""
        return CapabilityMatrix(
            canonical_types=frozenset({
                CanonicalType.OHLCV,
                CanonicalType.OPTION,
                CanonicalType.IMPLIED_VOLATILITY,
            }),
            intervals=_DERIBIT_INTERVALS,
            supports_revision=False,
            supports_websocket=False,
            max_history_days=365,
        )

    def health(self) -> HealthStatus:
        """健康检查（D04 §1）。"""
        import time
        start = time.monotonic()
        try:
            # public/test 是 Deribit 官方探活方法（无参数，返回版本号）；
            # 原实现的 public/get_currency 不是有效公开方法（返回 400）
            resp = self._client.post("/api/v2", json={
                "method": "public/test",
                "params": {},
            })
            elapsed_ms = int((time.monotonic() - start) * 1000)
            if resp.status_code == 200:
                return HealthStatus(ok=True, latency_ms=elapsed_ms)
            return HealthStatus(
                ok=False, latency_ms=elapsed_ms,
                detail=f"status={resp.status_code}",
            )
        except Exception as exc:
            elapsed_ms = int((time.monotonic() - start) * 1000)
            return HealthStatus(ok=False, latency_ms=elapsed_ms, detail=str(exc))

    def discover(self) -> list[InstrumentRef]:
        """全量 instrument 发现（D04 §4.3 必须实现）。

        调用 get_instruments 端点返回 INSTRUMENT canonical 记录。
        由于 Deribit API 按 currency 分页，这里返回 BTC 和 ETH 的 instruments
        作为 P0 实现（覆盖主要交易对）。
        """
        result: list[InstrumentRef] = []

        for currency in ("BTC", "ETH"):
            try:
                payload = self._fetch_instruments(currency)
                for item in payload.get("result", []):
                    instrument_name = item.get("instrument_name", "")
                    if not instrument_name:
                        continue

                    try:
                        parsed = parse_deribit(instrument_name)
                        result.append({
                            "entity_id": cast(str, parsed["underlying"]),
                            "instrument_id": cast(str, parsed["instrument_id"]),
                            "market_id": cast(str, parsed["market_id"]),
                        })
                    except ProviderError:
                        continue
            except (RateLimitError, TransportError):
                continue

        return result

    def _fetch_instruments(self, currency: str) -> dict[str, Any]:
        """调用 get_instruments API。"""
        self._rate_limiter.acquire(_INSTRUMENT_WEIGHT)

        response = self._client.post("/api/v2", json={
            "method": "public/get_instruments",
            "params": {"currency": currency, "expired": False},
        })
        self._handle_response(response)
        data = response.json()

        if not isinstance(data, dict):
            raise ProviderError(f"deribit: unexpected get_instruments response type: {type(data)}")
        return data

    def fetch(self, request: FetchRequest) -> Iterator[RawBatch]:
        """获取数据（D04 §4.3，分页由实现内聚）。

        Args:
            request: FetchRequest 包含 instrument_name、resolution、start/end 等参数。

        Yields:
            RawBatch 迭代器。
        """
        data_type = request.params.get("data_type", "")
        instrument_name = request.params.get("instrument_name", "")
        currency = request.params.get("currency", "")

        if data_type == "option_summary":
            if not currency:
                raise ProviderError("deribit: currency is required for option_summary")
            yield from self._fetch_option_summary(currency)
        elif data_type == "chart":
            if not instrument_name:
                raise ProviderError("deribit: instrument_name is required for chart data")
            resolution = request.params.get("resolution", "60")
            yield from self._fetch_chart_data(instrument_name, resolution, request)
        else:
            # Default: discover instruments
            currency = currency or "BTC"
            yield from self._fetch_instruments_as_batches(currency)

    def _fetch_instruments_as_batches(self, currency: str) -> Iterator[RawBatch]:
        """将 get_instruments 结果包装为 RawBatch。"""
        payload = self._fetch_instruments(currency)
        yield RawBatch(
            endpoint="/public/get_instruments",
            payload=payload,
            raw_meta={
                "http_status": 200,
                "currency": currency,
            },
        )

    def _fetch_option_summary(self, currency: str) -> Iterator[RawBatch]:
        """OPTION 快照（D04 §4.3，单次请求，返回该货币全部活跃期权）。"""
        self._rate_limiter.acquire(_BOOK_SUMMARY_WEIGHT)

        response = self._client.post("/api/v2", json={
            "method": "public/get_book_summary_by_currency",
            "params": {"currency": currency, "kind": "option"},
        })
        self._handle_response(response)
        data = response.json()

        if not isinstance(data, dict):
            raise ProviderError(f"deribit: unexpected get_book_summary response type: {type(data)}")

        yield RawBatch(
            endpoint="/public/get_book_summary_by_currency",
            payload=data,
            raw_meta={
                "http_status": response.status_code,
                "currency": currency,
            },
        )

    def _fetch_chart_data(
        self,
        instrument_name: str,
        resolution: str,
        request: FetchRequest,
    ) -> Iterator[RawBatch]:
        """OHLCV/IV 历史（D04 §4.3）。

        resolution: 1, 5, 15, 30, 60, 240, 1440（分钟）
        """
        if resolution not in _DERIBIT_RESOLUTION_MAP:
            raise ProviderError(f"deribit: unsupported resolution: {resolution}")

        params: dict[str, Any] = {
            "instrument_name": instrument_name,
            "resolution": resolution,
        }

        # Deribit API 参数名为 start_timestamp / end_timestamp（毫秒）。
        # start 回退一个 resolution：增量窗口按收线推进，边界 bar 的
        # openTime 落在窗口起点之前时会被 start_timestamp 过滤永久漏采；
        # 未收盘/重复 bar 由 normalize 跳过 + natural key upsert 幂等收敛。
        rewind_ms = interval_seconds(_DERIBIT_RESOLUTION_MAP[resolution]) * 1000
        if request.start is not None:
            params["start_timestamp"] = _dt_to_ms(request.start) - rewind_ms
        if request.end is not None:
            params["end_timestamp"] = _dt_to_ms(request.end)

        # 分页由 result.continuation 驱动（Deribit v2 语义）；记录已用 token，
        # 出现重复 token 或页数超限时显式失败，禁止静默无限分页。
        seen_continuations: set[str] = set()
        pages_requested = 0

        while True:
            self._rate_limiter.acquire(_CHART_DATA_WEIGHT)

            # 每次请求使用独立 dict：httpx 会序列化，但测试/日志中保留
            # 调用当时的参数快照，避免 append continuation 后回写历史请求。
            page_params = dict(params)
            response = self._client.post("/api/v2", json={
                "method": "public/get_tradingview_chart_data",
                "params": page_params,
            })
            self._handle_response(response)
            data = response.json()

            if not isinstance(data, dict):
                raise ProviderError(f"deribit: unexpected chart response type: {type(data)}")

            # 真实响应为 JSON-RPC envelope；result 才是图表数据（并行数组）
            result = data.get("result")
            if not isinstance(result, dict):
                raise ProviderError(
                    "deribit: chart response missing JSON-RPC result object"
                )

            ticks = result.get("ticks")
            if not isinstance(ticks, list) or not ticks:
                break

            continuation = result.get("continuation")
            token: str | None = None
            if continuation not in (None, ""):
                token = str(continuation)
                if token in seen_continuations:
                    raise ProviderError(
                        "deribit: repeated chart continuation token: "
                        f"{token} (instrument={instrument_name})"
                    )
                seen_continuations.add(token)

            yield RawBatch(
                endpoint="/public/get_tradingview_chart_data",
                payload=data,
                raw_meta={
                    "http_status": response.status_code,
                    "instrument_name": instrument_name,
                    "resolution": resolution,
                },
            )

            # 双重收敛条件 1：end 边界（末根 tick 已到达/越过 end 即停止）
            if request.end is not None:
                end_ms = _dt_to_ms(request.end)
                if int(ticks[-1]) >= end_ms:
                    break

            # 双重收敛条件 2：continuation 缺失/null 表示无更多数据
            if token is None:
                break

            pages_requested += 1
            if pages_requested >= _MAX_CHART_PAGES:
                raise ProviderError(
                    "deribit: chart pagination exceeded "
                    f"{_MAX_CHART_PAGES} pages (instrument={instrument_name})"
                )
            params["continuation"] = token

    def normalize(self, raw: RawBatch) -> list[BaseRecord]:
        """将原始响应转为 Canonical Type 记录（D04 §4.3）。

        Args:
            raw: RawBatch from fetch().

        Returns:
            list of BaseRecord subclasses (INSTRUMENT, OPTION, OHLCV, IMPLIED_VOLATILITY).
        """
        match raw.endpoint:
            case "/public/get_instruments":
                return self._normalize_instruments(raw.payload)
            case "/public/get_book_summary_by_currency":
                return self._normalize_options(raw.payload)
            case "/public/get_tradingview_chart_data":
                return self._normalize_chart(raw.payload, raw.raw_meta)
            case _:
                raise ProviderError(f"deribit: unknown endpoint: {raw.endpoint}")

    def _normalize_instruments(self, payload: dict[str, Any]) -> list[BaseRecord]:
        """Normalize get_instruments payload to INSTRUMENT records（D04 §4.3）。

        expiry 优先使用 instrument_name 解析结果（D02 §3 是唯一事实源），
        expiration_timestamp 仅作兜底。
        """
        records: list[INSTRUMENT] = []
        now = datetime.now(UTC).replace(tzinfo=None)

        for item in payload.get("result", []):
            instrument_name = item.get("instrument_name", "")
            if not instrument_name:
                continue

            try:
                parsed = parse_deribit(instrument_name)
            except ProviderError:
                continue

            kind = item.get("kind", "")
            is_option = kind == "option"

            # expiry: prefer instrument_name parsing (D02 §3 canonical source),
            # fallback to expiration_timestamp only when name parsing gives None
            expiration = cast("date | None", parsed.get("expiry"))
            if expiration is None:
                expiration_str = item.get("expiration_timestamp")
                if expiration_str is not None:
                    try:
                        exp_str = str(expiration_str)
                        exp_str = exp_str.replace("Z", "+00:00").replace("+00:00", "")
                        expiration = date.fromisoformat(exp_str)
                    except (ValueError, TypeError):
                        try:
                            exp_ts = int(expiration_str)
                            if exp_ts > 9999999999:  # ms epoch
                                expiration = datetime.fromtimestamp(
                                    exp_ts / 1000, tz=UTC
                                ).date()
                            else:
                                expiration = datetime.fromtimestamp(
                                    exp_ts, tz=UTC
                                ).date()
                        except (ValueError, TypeError):
                            pass

            # Deribit: strike is per-option, use the strike from the response
            strike = float(item.get("strike", 0)) if item.get("strike") else None
            if strike is not None and strike <= 0:
                strike = None

            settlement_asset = item.get("settlement_currency", "USD")

            try:
                instrument_type = InstrumentType.OPTION if is_option \
                    else InstrumentType.PERP
                record = INSTRUMENT(
                    schema_version="1.0",
                    source="deribit",
                    source_id=instrument_name,
                    source_timestamp=None,
                    ingest_timestamp=now,
                    raw_record_id=f"deribit:instrument:{instrument_name}",
                    quality_status=QualityStatus.VALID,
                    quality_reason=None,
                    instrument_id=cast(str, parsed["instrument_id"]),
                    entity_id=cast(str, parsed["underlying"]),
                    instrument_type=instrument_type,
                    expiry=expiration,
                    strike=strike,
                    option_type=cast(Any, parsed["option_type"]),
                    settlement_asset=settlement_asset,
                )
                records.append(record)
            except Exception:
                continue

        return list[BaseRecord](records)

    def _normalize_options(self, payload: dict[str, Any]) -> list[BaseRecord]:
        """Normalize get_book_summary payload to OPTION records（D04 §4.3）。

        真实响应为 JSON-RPC envelope，result 是 BookSummary 列表
        （2026-11-05 依据 Deribit v2 get_book_summary_by_currency 文档 /
        docs.rs deribit-http 0.7 BookSummary）：字段含 instrument_name、
        base_currency、mark_price、bid_price、ask_price、open_interest、
        mark_iv、last_update_time。
        """
        records: list[OPTION] = []
        skipped: dict[str, int] = defaultdict(int)
        now = datetime.now(UTC).replace(tzinfo=None)

        result = payload.get("result")
        if not isinstance(result, list):
            raise ProviderError(
                "deribit: get_book_summary response missing JSON-RPC result list"
            )

        for opt in result:
            if not isinstance(opt, dict):
                skipped["item_not_object"] += 1
                continue

            currency = str(opt.get("base_currency") or "")
            instrument_name = opt.get("instrument_name", "<unknown>")
            try:
                if not instrument_name or instrument_name == "<unknown>":
                    raise ProviderError("missing instrument_name")

                parsed = parse_deribit(instrument_name)
                market_id = cast(str, parsed["market_id"])

                # Deribit book summary fields（显式 null → None，不伪造 0）
                mark_price = _quote_float(opt.get("mark_price"))
                bid = _quote_float(opt.get("bid_price"))
                ask = _quote_float(opt.get("ask_price"))
                # 未平仓合约数：源端恒为数值；null/缺失按 0 处理
                open_interest = float(opt.get("open_interest") or 0.0)

                # event_time: use last_update_time if available, else now
                last_update_ms = opt.get("last_update_time")
                if last_update_ms is not None:
                    event_time = datetime.fromtimestamp(
                        int(last_update_ms) / 1000, tz=UTC
                    ).replace(tzinfo=None)
                else:
                    event_time = now

                # IV fields
                parsed_ot = parsed.get("option_type")
                if parsed_ot:
                    option_type = OptionType(parsed_ot)
                else:
                    option_type = OptionType.CALL

                record = OPTION(
                    schema_version="1.0",
                    source="deribit",
                    source_id=instrument_name,
                    source_timestamp=event_time,
                    ingest_timestamp=now,
                    raw_record_id=f"deribit:option:{instrument_name}:{event_time.isoformat()}",
                    quality_status=QualityStatus.VALID,
                    quality_reason=None,
                    market_id=market_id,
                    instrument_id=cast(str, parsed["instrument_id"]),
                    event_time=event_time,
                    underlying=cast(str, parsed["underlying"]),
                    expiry=cast(date, parsed["expiry"]),
                    strike=cast(float, parsed["strike"]),
                    option_type=option_type,
                    settlement_asset="USD",
                    mark_price=mark_price,
                    bid=bid,
                    ask=ask,
                    open_interest=open_interest,
                )
                records.append(record)
            except (KeyError, ValueError, TypeError, ProviderError) as exc:
                reason = f"{type(exc).__name__}: {exc}"
                logger.warning(
                    "connector.normalize_skip",
                    source="deribit",
                    currency=currency,
                    instrument_name=instrument_name,
                    reason=reason,
                )
                skipped[reason] += 1
                continue

        if skipped:
            logger.warning(
                "connector.normalize_summary",
                source="deribit",
                kept=len(records),
                skipped=sum(skipped.values()),
                reasons=dict(skipped),
            )

        return list[BaseRecord](records)

    def _normalize_chart(
        self, payload: dict[str, Any], raw_meta: dict[str, Any] | None = None
    ) -> list[BaseRecord]:
        """Normalize get_tradingview_chart_data payload to OHLCV records.

        真实响应为 JSON-RPC envelope（2026-11-05 依据 Deribit v2
        get_tradingview_chart_data 文档 / docs.rs deribit-http 0.7
        TradingViewChartData）：

        {
          "jsonrpc": "2.0",
          "result": {
            "status": "ok",
            "ticks": [1672444800000, ...],
            "open": [...], "high": [...], "low": [...],
            "close": [...], "volume": [...], "cost": [...],
            "continuation": null,
          },
          "usIn": ..., "usOut": ..., "testnet": false,
        }

        result 内是并行数组而非 candles 列表；instrument_name 不在 result 中，
        必须由 raw_meta 带入。chart 响应不含 IV。
        """
        records: list[BaseRecord] = []
        skipped: dict[str, int] = defaultdict(int)
        now = datetime.now(UTC).replace(tzinfo=None)

        instrument_name = str((raw_meta or {}).get("instrument_name") or "")
        if not instrument_name:
            raise ProviderError(
                "deribit: instrument_name missing in raw_meta for chart data"
            )

        result = payload.get("result")
        if not isinstance(result, dict):
            raise ProviderError(
                "deribit: chart response missing JSON-RPC result object"
            )

        # interval 是 OHLCV 身份键组成部分（D02 §2 L43），缺失/非法必须熔断
        resolution = (raw_meta or {}).get("resolution")
        if not resolution:
            raise ProviderError(
                "deribit: resolution missing in raw_meta for chart data"
            )
        interval_enum = _DERIBIT_RESOLUTION_MAP.get(str(resolution))
        if interval_enum is None:
            raise ProviderError(
                f"deribit: unsupported chart resolution: {resolution}"
            )
        bar_seconds = interval_seconds(interval_enum)

        parsed = parse_deribit(instrument_name)
        market_id = cast(str, parsed["market_id"])

        ticks = result.get("ticks")
        if not isinstance(ticks, list):
            raise ProviderError(
                "deribit: chart result missing ticks array"
            )
        if not ticks:
            return records

        arrays = {
            "open": result.get("open"),
            "high": result.get("high"),
            "low": result.get("low"),
            "close": result.get("close"),
            "volume": result.get("volume"),
        }

        for i, ts_raw in enumerate(ticks):
            try:
                ts_ms = int(ts_raw)
                event_time = datetime.fromtimestamp(
                    ts_ms / 1000, tz=UTC
                ).replace(tzinfo=None)

                # 未收盘 bar（close_time 在未来）跳过并记数；已收盘 bar
                # 正常入库，重复采集由 natural key upsert 幂等收敛。
                if event_time + timedelta(seconds=bar_seconds) > now:
                    logger.debug(
                        "connector.normalize_skip",
                        source="deribit",
                        instrument_name=instrument_name,
                        reason="unclosed_bar",
                        index=i,
                    )
                    skipped["unclosed_bar"] += 1
                    continue

                values: dict[str, float] = {}
                for name, arr in arrays.items():
                    if not isinstance(arr, list) or i >= len(arr):
                        raise KeyError(f"{name} array shorter than ticks")
                    values[name] = float(arr[i])

                # Validate finite
                if not all(math.isfinite(v) for v in values.values()):
                    raise ValueError("non-finite OHLCV value")

                # Create OHLCV record
                record = OHLCV(
                    schema_version="1.0",
                    source="deribit",
                    source_id=f"{instrument_name}:{ts_ms}",
                    source_timestamp=event_time,
                    ingest_timestamp=now,
                    raw_record_id=f"deribit:ohlcv:{instrument_name}:{ts_ms}",
                    quality_status=QualityStatus.VALID,
                    quality_reason=None,
                    market_id=market_id,
                    event_time=event_time,
                    interval=interval_enum,
                    open=values["open"],
                    high=values["high"],
                    low=values["low"],
                    close=values["close"],
                    volume=values["volume"],
                )
                records.append(record)

            except (KeyError, ValueError, TypeError) as exc:
                reason = f"{type(exc).__name__}: {exc}"
                logger.warning(
                    "connector.normalize_skip",
                    source="deribit",
                    instrument_name=instrument_name,
                    index=i,
                    reason=reason,
                )
                skipped[reason] += 1

        if skipped:
            # 未收盘 bar 是增量采集常态 → debug；其他跳过原因仍为 warning
            log = (
                logger.debug
                if set(skipped) == {"unclosed_bar"}
                else logger.warning
            )
            log(
                "connector.normalize_summary",
                source="deribit",
                instrument_name=instrument_name,
                kept=len(records),
                skipped=sum(skipped.values()),
                reasons=dict(skipped),
            )

        return records

    def validate(
        self, records: list[BaseRecord]
    ) -> QualityReport:
        """验证记录（D04 §1，委托 quality.rules）。"""
        findings: list[QualityFinding] = []

        for record in records:
            key = str(getattr(record, "natural_key", lambda: ("unknown",))())

            if isinstance(record, OHLCV):
                if record.low > min(record.open, record.close):
                    findings.append(QualityFinding(
                        record_key=key,
                        rule_id="Q-RANGE-001",
                        severity="ERROR",
                        detail=(
                            f"low={record.low} should be <= "
                            f"min(open={record.open}, close={record.close})"
                        ),
                    ))
                if record.high < max(record.open, record.close):
                    findings.append(QualityFinding(
                        record_key=key,
                        rule_id="Q-RANGE-001",
                        severity="ERROR",
                        detail=(
                            f"high={record.high} should be >= "
                            f"max(open={record.open}, close={record.close})"
                        ),
                    ))

            elif isinstance(record, OPTION):
                if record.expiry is not None and record.expiry < date.today():
                    findings.append(QualityFinding(
                        record_key=key,
                        rule_id="Q-RANGE-003",
                        severity="WARNING",
                        detail=f"option expired: {record.expiry}",
                    ))

        return QualityReport(findings=findings)

    def checkpoint_from(
        self, raw: RawBatch | list[BaseRecord]
    ) -> str | None:
        """从 raw batch 或 records 中提取 checkpoint cursor（D04 §1）。"""
        if isinstance(raw, RawBatch):
            payload = raw.payload
            endpoint = raw.endpoint

            if endpoint == "/public/get_instruments" and isinstance(payload, dict):
                # Instruments: no time-based cursor, use a marker
                result_list = payload.get("result", [])
                if result_list:
                    return f"deribit:instruments:{len(result_list)}"
                return None

            if endpoint == "/public/get_book_summary_by_currency" and isinstance(payload, dict):
                # Option summary: use a marker based on number of instruments
                result = payload.get("result")
                total = len(result) if isinstance(result, list) else 0
                return f"deribit:options:{total}" if total > 0 else None

            if endpoint == "/public/get_tradingview_chart_data" and isinstance(payload, dict):
                result = payload.get("result")
                ticks = result.get("ticks") if isinstance(result, dict) else None
                if isinstance(ticks, list) and ticks:
                    return str(int(ticks[-1]))
                return None

        elif isinstance(raw, list) and raw:
            last_record = raw[-1]
            if hasattr(last_record, "source_timestamp") and last_record.source_timestamp:
                return str(int(last_record.source_timestamp.timestamp() * 1000))

        return None

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
        url = getattr(response.url, "path", str(response.url)) \
            if response.url else ""
        error_ctx = {"source": "deribit", "endpoint": url}

        if 200 <= status < 300:
            return

        if error_cls == RateLimitError:
            raise RateLimitError(
                f"deribit: rate limited (status={status})",
                context=error_ctx,
                retry_after=retry_after,
            )
        if error_cls == AuthError:
            raise AuthError(
                f"deribit: auth failed (status={status})",
                context=error_ctx,
            )
        if error_cls == ProviderError:
            raise ProviderError(
                f"deribit: provider error "
                f"(status={status}, body={response.text[:500]})",
                context=error_ctx,
            )
        if error_cls == TransportError:
            raise TransportError(
                f"deribit: transport error (status={status})",
                context=error_ctx,
            )

    def close(self) -> None:
        """关闭 httpx client。"""
        self._client.close()

    def __del__(self) -> None:
        """确保资源清理。"""
        try:
            self.close()
        except Exception:
            pass
