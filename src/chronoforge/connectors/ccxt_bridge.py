"""CcxtBridge connector（D04 §4.4，DATA-SOURCE-004）。

通过 ccxt 库抽象层对接任意支持的交易所：
- fetchOHLCV → OHLCV
- fetchTrades → TRADE
- fetchTicker → TICKER

能力检测由 exchange.has 字典驱动，禁止静态假设（D04 §4.4，需求 §38）。
时间语义（审计 F-09）：ccxt 统一 ms epoch → us ×1000。
continuity_model：随底层市场（加密源 ALWAYS_OPEN）。

market_id 格式：CCXT-{EX}:{symbol}:{type}
示例：CCXT-BINANCE:BTCUSDT:SPOT
"""

from __future__ import annotations

import math
import os
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import ccxt
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
from chronoforge.connectors.normalize_stats import note_skip
from chronoforge.connectors.ratelimit import RateLimiter
from chronoforge.exceptions import (
    ProviderError,
    RateLimitError,
    SchemaError,
    TransportError,
)
from chronoforge.models.base import BaseRecord
from chronoforge.models.derivatives import Interval
from chronoforge.models.enums import CanonicalType, QualityStatus
from chronoforge.models.market import OHLCV, TICKER, TRADE, Side, interval_seconds
from chronoforge.models.quality import QualityFinding, QualityReport
from chronoforge.timeutil import to_epoch_milliseconds

logger = structlog.get_logger()

# 保守默认限流配置（不同交易所差异大，按 exchange 调整）
_DEFAULT_RATE = 60.0  # requests per second
_DEFAULT_BURST = 60

# trades 分页护栏：连续翻页上限，防止源侧游标异常时无限请求
_MAX_TRADE_PAGES = 1000


def _dt_to_ms(dt: datetime) -> int:
    """UTC naive datetime → epoch ms（统一委托 timeutil，避免主机时区偏移）。"""
    return to_epoch_milliseconds(dt)


def _trade_cursor_to_ms(cursor: str) -> int:
    """ISO-8601 游标 → epoch ms；非法游标给出可诊断的 ProviderError。"""
    try:
        dt = datetime.fromisoformat(cursor)
    except ValueError as e:
        raise ProviderError(
            f"ccxt_bridge: invalid trades cursor {cursor!r}: expected a numeric "
            "trade id or an ISO-8601 timestamp"
        ) from e
    return to_epoch_milliseconds(dt)


def _map_ccxt_error(op: str, exc: Exception) -> TransportError | ProviderError:
    """ccxt 异常 → 领域错误层级（架构 08 §1 错误分类，审计 SR-12）。

    此前映射为 Python 内建 ConnectionError，runner 的 chunk 级失败语义
    （TransportError/RateLimitError/ProviderError）无法识别 → ccxt 源的
    瞬时网络故障被当作不可重试异常直接终态 FAILED。

    映射（ccxt ≥4 异常层级：RateLimitExceeded ⊂ NetworkError，
    ExchangeError 与 NetworkError 同级，故判定顺序不可调换；
    旧版 ccxt 中限流异常名为 RateLimitError ⊂ DDoSProtection）：
    - ccxt.RateLimitExceeded → RateLimitError（可重试 + 退避）
    - ccxt.NetworkError      → TransportError（可重试）
    - ccxt.ExchangeError     → ProviderError（4xx 参数/资源错误，不重试）
    """
    if isinstance(exc, ccxt.RateLimitExceeded):
        return RateLimitError(f"ccxt_bridge: {op} failed: {exc}")
    if isinstance(exc, ccxt.NetworkError):
        return TransportError(f"ccxt_bridge: {op} failed: {exc}")
    return ProviderError(f"ccxt_bridge: {op} failed: {exc}")


@dataclass
class _Settings:
    """CcxtBridge 最小配置。"""

    http_timeout_s: float = 30.0
    binance_api_key: str = ""
    binance_secret: str = ""
    okx_api_key: str = ""
    okx_secret: str = ""
    okx_passphrase: str = ""


class CcxtBridgeConnector(DataConnector):
    """通过 ccxt 库抽象层对接交易所的连接器（D04 §4.4）。

    Args:
        exchange: ccxt exchange ID（如 "binance", "okx"）。
    """

    source_id = "ccxt"

    def __init__(
        self,
        settings: Settings | _Settings | None = None,
        exchange: str = "binance",
    ) -> None:
        self.exchange_id = exchange.lower()
        resolved_settings = self._resolve_settings(settings)
        self.exchange = self._init_exchange(resolved_settings, self.exchange_id)
        self._rate_limiter = RateLimiter(
            rate=self._guess_rate(self.exchange_id),
            burst=_DEFAULT_BURST,
        )
        self._settings = resolved_settings

    @staticmethod
    def _resolve_settings(settings: Settings | _Settings | None) -> _Settings:
        """将 Settings 或 _Settings 统一为 _Settings。

        如果 settings 为 None，调用 Settings.load() 并从
        Settings 中提取凭证。
        """
        if settings is None:
            gs = Settings.load()
            return _Settings(
                http_timeout_s=gs.http_timeout_s,
                binance_api_key=gs.get_secret("ccxt_binance_api_key")
                .get_secret_value(),
                binance_secret=gs.get_secret("ccxt_binance_secret")
                .get_secret_value(),
                okx_api_key=gs.get_secret("ccxt_okx_api_key")
                .get_secret_value(),
                okx_secret=gs.get_secret("ccxt_okx_secret")
                .get_secret_value(),
                okx_passphrase=gs.get_secret("ccxt_okx_passphrase")
                .get_secret_value(),
            )

        if isinstance(settings, Settings):
            return _Settings(
                http_timeout_s=settings.http_timeout_s,
                binance_api_key=settings.get_secret("ccxt_binance_api_key")
                .get_secret_value(),
                binance_secret=settings.get_secret("ccxt_binance_secret")
                .get_secret_value(),
                okx_api_key=settings.get_secret("ccxt_okx_api_key")
                .get_secret_value(),
                okx_secret=settings.get_secret("ccxt_okx_secret")
                .get_secret_value(),
                okx_passphrase=settings.get_secret("ccxt_okx_passphrase")
                .get_secret_value(),
            )

        return settings  # _Settings

    @staticmethod
    def _init_exchange(settings: _Settings, exchange_id: str) -> ccxt.Exchange:
        """初始化 ccxt Exchange 实例。

        Args:
            settings: 配置对象。
            exchange_id: ccxt exchange ID。

        Returns:
            配置好的 ccxt Exchange 实例。
        """
        import ccxt

        config: dict[str, Any] = {
            "enableRateLimit": True,
            "timeout": settings.http_timeout_s * 1000,  # ccxt 使用毫秒
        }

        # 按交易所注入 API 凭证
        if exchange_id == "binance":
            config.update({
                "apiKey": getattr(settings, "binance_api_key", ""),
                "secret": getattr(settings, "binance_secret", ""),
            })
        elif exchange_id == "okx":
            config.update({
                "apiKey": getattr(settings, "okx_api_key", ""),
                "secret": getattr(settings, "okx_secret", ""),
                "password": getattr(settings, "okx_passphrase", ""),
            })

        exchange_class = getattr(ccxt, exchange_id, None)
        if exchange_class is None:
            raise ValueError(f"ccxt: unknown exchange '{exchange_id}'")

        # 代理支持：ccxt 不读环境代理变量，需显式 proxies
        # （okx 等需代理才能访问的源依赖 HTTPS_PROXY/HTTP_PROXY）
        proxy = (
            os.environ.get("HTTPS_PROXY")
            or os.environ.get("https_proxy")
            or os.environ.get("HTTP_PROXY")
            or os.environ.get("http_proxy")
        )
        if proxy:
            config["proxies"] = {"http": proxy, "https": proxy}

        return exchange_class(config)

    @staticmethod
    def _guess_rate(exchange_id: str) -> float:
        """根据交易所 ID 猜测保守限流速率（req/s）。"""
        # Binance: 1200 weight/min ≈ 20 req/s（保守）
        # OKX: 60 req/s
        # 默认保守值
        rates = {
            "binance": 20.0,
            "okx": 10.0,
        }
        return rates.get(exchange_id, _DEFAULT_RATE)

    def capabilities(self) -> CapabilityMatrix:
        """能力检测：由 exchange.has 字典驱动（D04 §4.4）。

        禁止静态假设——不硬编码支持哪些端点。
        """
        canonical_types: set[CanonicalType] = set()

        # OHLCV
        if self.exchange.has.get("fetchOHLCV"):
            canonical_types.add(CanonicalType.OHLCV)

        # TRADE
        if self.exchange.has.get("fetchTrades"):
            canonical_types.add(CanonicalType.TRADE)

        # TICKER
        if self.exchange.has.get("fetchTicker"):
            canonical_types.add(CanonicalType.TICKER)

        # ORDERBOOK
        if self.exchange.has.get("fetchOrderBook"):
            canonical_types.add(CanonicalType.ORDERBOOK)

        # 从 exchange.timeframes 推断支持的间隔
        intervals: set[Interval] = set()
        if self.exchange.has.get("fetchOHLCV"):
            # ccxt timeframes 可能是 dict（如 {'1m': '1m', ...}）或 list
            tf_map = self.exchange.timeframes
            if isinstance(tf_map, dict):
                tf_keys = list(tf_map.keys())
            elif isinstance(tf_map, list):
                tf_keys = tf_map
            else:
                tf_keys = []

            for tf in tf_keys:
                # 标准化为 Interval 枚举值
                normalized = self._normalize_interval(tf)
                if normalized is not None:
                    intervals.add(normalized)

        return CapabilityMatrix(
            canonical_types=frozenset(canonical_types),
            intervals=frozenset(intervals),
            supports_revision=False,
            supports_websocket=bool(self.exchange.has.get("watchOHLCV")),
            max_history_days=None,
        )

    @staticmethod
    def _normalize_interval(tf: str) -> Interval | None:
        """将 ccxt timeframe 字符串映射到 Interval 枚举。

        ccxt 返回的 timeframe 可能与 Interval 枚举值不完全匹配，
        需要标准化处理。
        """
        tf_lower = tf.lower() if isinstance(tf, str) else str(tf).lower()
        mapping = {
            "1s": Interval._1M,  # 1秒不在 Interval 中，映射为最小单位
            "1m": Interval._1M,
            "3m": Interval._1M,
            "5m": Interval._5M,
            "15m": Interval._15M,
            "30m": Interval._30M,
            "1h": Interval._1H,
            "2h": Interval._2H,
            "4h": Interval._4H,
            "6h": Interval._6H,
            "8h": Interval._8H,
            "12h": Interval._12H,
            "1d": Interval._1D,
            "3d": Interval._1D,
            "1w": Interval._1W,
            "1M": Interval._1D,  # 月度映射为日线
        }
        return mapping.get(tf_lower)

    def health(self) -> HealthStatus:
        """健康检查（D04 §1）。"""
        import time

        start = time.monotonic()
        try:
            # ccxt ping 方法
            self.exchange.load_markets()
            elapsed_ms = int((time.monotonic() - start) * 1000)
            return HealthStatus(ok=True, latency_ms=elapsed_ms)
        except Exception as exc:
            elapsed_ms = int((time.monotonic() - start) * 1000)
            return HealthStatus(
                ok=False, latency_ms=elapsed_ms, detail=str(exc)
            )

    def discover(self) -> list[InstrumentRef]:
        """返回交易所支持的合约/交易对列表。

        P0：返回前 N 个常见交易对。
        """
        result: list[InstrumentRef] = []

        # Load markets if not already loaded
        if self.exchange.markets is None:
            try:
                self.exchange.load_markets()
            except Exception:
                return result

        # 从市场中筛选常见交易对（按 type 和 active 状态）
        symbols = []
        for symbol, market in (self.exchange.markets or {}).items():
            if not market.get("active", False):
                continue
            market_type = market.get("type", "spot")
            if market_type in ("spot", "swap"):
                symbols.append(symbol)
                if len(symbols) >= 10:
                    break

        # Build market_id format: CCXT-{EX}:{symbol}:{type}

        for symbol in symbols:
            result.append({
                "entity_id": symbol.upper(),
                "instrument_id": symbol.upper(),
                "market_id": f"CCXT-{self.exchange_id.upper()}:{symbol}:SPOT",
            })

        return result

    def fetch(self, request: FetchRequest) -> Iterator[RawBatch]:
        """获取数据（D04 §1，分页由实现内聚）。

        Args:
            request: FetchRequest 包含 symbol、interval 等参数。

        Yields:
            RawBatch 迭代器。
        """
        symbol = request.params.get("symbol", "")
        if not symbol:
            raise ValueError("ccxt_bridge: symbol is required in request.params")

        data_type = request.params.get("type", "ohlcv")

        if data_type == "ohlcv":
            yield from self._fetch_ohlcv(symbol, request)
        elif data_type == "trades":
            yield from self._fetch_trades(symbol, request)
        elif data_type == "ticker":
            yield from self._fetch_ticker(symbol, request)
        else:
            raise ValueError(f"ccxt_bridge: unknown type '{data_type}'")

    def _fetch_ohlcv(self, symbol: str, request: FetchRequest) -> Iterator[RawBatch]:
        """通过 ccxt fetchOHLCV 获取 K 线数据。

        ccxt fetchOHLCV 签名：
            fetchOHLCV(symbol, timeframe, since=None, limit=500, params={})

        返回格式：[[timestamp, datetime_str, o, h, l, c, v], ...]
        """
        timeframe = request.params.get("interval", "1m")
        limit = int(request.params.get("limit", 500))

        since = _dt_to_ms(request.start) if request.start else None
        until = _dt_to_ms(request.end) if request.end else None

        # 起点回退一根 K 线：增量窗口按收线推进，bar 的 openTime 可能落在
        # 窗口起点之前（如 1d bar 在小时级增量调度下），不满足 since 过滤
        # 会被永久漏采。回退后重复/未收盘 bar 由 natural key upsert 幂等
        # 收敛，未收盘 bar 由 normalize 显式跳过。
        if since is not None:
            try:
                rewind_ms = interval_seconds(timeframe) * 1000
            except ValueError as e:
                raise ValueError(
                    f"ccxt_bridge: unsupported fetchOHLCV timeframe: {timeframe}"
                ) from e
            since -= rewind_ms

        # ccxt fetchOHLCV 可能只支持 since，不支持 until
        # 需要在客户端做窗口切分
        if since is not None:
            # 按 limit 分页
            cursor_ms = since
            end_ms_check = _dt_to_ms(request.end) if request.end else None
            while True:
                if end_ms_check is not None and cursor_ms >= end_ms_check:
                    break

                try:
                    self._rate_limiter.acquire(1)
                    ohlcv_data = self.exchange.fetchOHLCV(
                        symbol,
                        timeframe,
                        since=int(cursor_ms),
                        limit=limit,
                    )
                except (ccxt.NetworkError, ccxt.ExchangeError) as e:
                    raise _map_ccxt_error("fetchOHLCV", e) from e

                if not ohlcv_data:
                    break

                # 防止无限循环：返回数据最晚时间不晚于 cursor 说明已拉完
                last_ts = ohlcv_data[-1][0]
                if last_ts <= cursor_ms:
                    break

                yield RawBatch(
                    endpoint="fetchOHLCV",
                    payload=ohlcv_data,
                    raw_meta={
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "fetched_since": cursor_ms,
                        "candle_count": len(ohlcv_data),
                    },
                )

                # 检查是否达到 until
                last_candle_ts = ohlcv_data[-1][0]
                if last_candle_ts >= end_ms_check:
                    break

                # 下一页：从最后一个 candle 之后开始
                cursor_ms = last_candle_ts + 1

        elif until is not None:
            # 没有 since，只有 until：从很早开始到 until
            try:
                self._rate_limiter.acquire(1)
                ohlcv_data = self.exchange.fetchOHLCV(
                    symbol, timeframe, limit=limit
                )
            except (ccxt.NetworkError, ccxt.ExchangeError) as e:
                raise _map_ccxt_error("fetchOHLCV", e) from e

            # 过滤到 until 之前的数据
            cutoff = int(until)
            filtered = [c for c in ohlcv_data if c[0] <= cutoff]

            if filtered:
                yield RawBatch(
                    endpoint="fetchOHLCV",
                    payload=filtered,
                    raw_meta={
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "candle_count": len(filtered),
                    },
                )
        else:
            # 无时间范围：拉取最近的 limit 条
            try:
                self._rate_limiter.acquire(1)
                ohlcv_data = self.exchange.fetchOHLCV(
                    symbol, timeframe, limit=limit
                )
            except (ccxt.NetworkError, ccxt.ExchangeError) as e:
                raise _map_ccxt_error("fetchOHLCV", e) from e

            if ohlcv_data:
                yield RawBatch(
                    endpoint="fetchOHLCV",
                    payload=ohlcv_data,
                    raw_meta={
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "candle_count": len(ohlcv_data),
                    },
                )

    def _fetch_trades(self, symbol: str, request: FetchRequest) -> Iterator[RawBatch]:
        """通过 ccxt fetchTrades 获取成交数据（分页由实现内聚）。

        ccxt fetchTrades 签名：
            fetchTrades(symbol, since=None, limit=100, params={})

        返回格式：[{id, timestamp, price, amount, ...}, ...]

        分页语义（对齐 Binance aggTrades 教训，项目记忆）：
        - ``fromId`` 与 ``startTime/endTime`` 组合非法（-1128/-1100），
          故游标只走单一模式：数字游标 = fromId-only；时间游标 = since。
        - ISO 字符串不能当 fromId（也不能当 order），必须先归一化为 ms。
        - ``order`` 是翻页方向（asc/desc），不是游标，必须显式校验并透传。
        - candlestick 之外的 end 截断在客户端完成（源侧 until 不可靠）。
        """
        limit = int(request.params.get("limit", 100))
        order = str(request.params.get("order", "asc")).lower()
        if order not in ("asc", "desc"):
            raise ProviderError(
                f"ccxt_bridge: invalid trades order {order!r}: "
                "expected 'asc' or 'desc'"
            )

        start_ms = _dt_to_ms(request.start) if request.start else None
        end_ms = _dt_to_ms(request.end) if request.end else None

        # 游标类型归一化：纯数字 = 成交 id（fromId）；其余按 ISO 时间解析。
        # 二者互斥，禁止把 fromId 与 since 同时传给交易所。
        since_ms: int | None = None
        from_id: str | None = None
        if request.cursor is not None:
            raw_cursor = str(request.cursor).strip()
            if raw_cursor.isdigit():
                from_id = raw_cursor
            else:
                since_ms = _trade_cursor_to_ms(raw_cursor)
        else:
            since_ms = start_ms

        seen_ids: set[str] = set()
        pages = 0

        while True:
            params: dict[str, Any] = {"order": order}
            if from_id is not None:
                params["fromId"] = from_id

            try:
                self._rate_limiter.acquire(1)
                trades_data = self.exchange.fetchTrades(
                    symbol,
                    since=None if from_id is not None else since_ms,
                    limit=limit,
                    params=params,
                )
            except (ccxt.NetworkError, ccxt.ExchangeError) as e:
                raise _map_ccxt_error("fetchTrades", e) from e

            if not trades_data:
                break

            page: list[dict[str, Any]] = []
            raw_timestamps: list[int] = []
            for trade in trades_data:
                if not isinstance(trade, dict):
                    continue
                ts_raw = trade.get("timestamp")
                if ts_raw is None:
                    continue
                ts = int(ts_raw)
                raw_timestamps.append(ts)
                if start_ms is not None and ts < start_ms:
                    continue
                if end_ms is not None and ts > end_ms:
                    continue
                trade_id = str(trade.get("id", ""))
                if trade_id and trade_id in seen_ids:
                    continue
                if trade_id:
                    seen_ids.add(trade_id)
                page.append(trade)

            if page:
                yield RawBatch(
                    endpoint="fetchTrades",
                    payload=page,
                    raw_meta={
                        "symbol": symbol,
                        "trade_count": len(page),
                        "order": order,
                    },
                )

            # 收敛条件：asc 末条已到/越过 end；desc 首条已到/越过 start。
            if (
                end_ms is not None
                and order == "asc"
                and raw_timestamps
                and raw_timestamps[-1] >= end_ms
            ):
                break
            if (
                start_ms is not None
                and order == "desc"
                and raw_timestamps
                and raw_timestamps[0] <= start_ms
            ):
                break

            # 短页表示源侧已无更多数据
            if len(trades_data) < limit:
                break

            pages += 1
            if pages >= _MAX_TRADE_PAGES:
                raise ProviderError(
                    "ccxt_bridge: fetchTrades pagination exceeded "
                    f"{_MAX_TRADE_PAGES} pages (symbol={symbol})"
                )

            # 下一页游标：id 必须为数字才能用 fromId 续拉；asc 取 max+1，
            # desc 取 min-1。无进展时显式失败，禁止死循环或静默漏采。
            ids = [
                str(t.get("id"))
                for t in trades_data
                if isinstance(t, dict) and t.get("id") not in (None, "")
            ]
            numeric_ids = [int(i) for i in ids if i.isdigit()]
            if len(numeric_ids) != len(ids):
                raise ProviderError(
                    "ccxt_bridge: fetchTrades page returned non-numeric trade ids; "
                    "cannot continue fromId pagination "
                    f"(symbol={symbol}, cursor={from_id or since_ms})"
                )
            if not numeric_ids:
                raise ProviderError(
                    "ccxt_bridge: fetchTrades page returned no trade ids; "
                    f"cannot paginate (symbol={symbol})"
                )
            next_id = (
                max(numeric_ids) + 1 if order == "asc" else min(numeric_ids) - 1
            )
            next_from_id = str(next_id)
            if next_from_id == from_id:
                raise ProviderError(
                    "ccxt_bridge: fetchTrades cursor did not advance "
                    f"(symbol={symbol}, fromId={from_id})"
                )
            from_id = next_from_id

    def _fetch_ticker(self, symbol: str, request: FetchRequest) -> Iterator[RawBatch]:
        """通过 ccxt fetchTicker 获取 Ticker 数据。

        ccxt fetchTicker 签名：
            fetchTicker(symbol, params={})

        返回格式：{symbol, last, bid, ask, baseVolume, quoteVolume, ...}
        """
        try:
            self._rate_limiter.acquire(1)
            ticker_data = self.exchange.fetchTicker(symbol)
        except (ccxt.NetworkError, ccxt.ExchangeError) as e:
            raise _map_ccxt_error("fetchTicker", e) from e

        if ticker_data:
            yield RawBatch(
                endpoint="fetchTicker",
                payload=ticker_data,
                raw_meta={
                    "symbol": symbol,
                },
            )

    def normalize(self, raw: RawBatch) -> list[BaseRecord]:
        """将原始响应转为 Canonical Type 记录（D04 §4.4）。

        Args:
            raw: RawBatch from fetch().

        Returns:
            list of BaseRecord subclasses (OHLCV, TRADE, TICKER).
        """
        match raw.endpoint:
            case "fetchOHLCV":
                return self._normalize_ohlcv(
                    raw.payload, raw.raw_meta
                )  # type: ignore[return-value]
            case "fetchTrades":
                return self._normalize_trades(raw.payload)  # type: ignore[return-value]
            case "fetchTicker":
                return self._normalize_ticker(raw.payload)  # type: ignore[return-value]
            case _:
                raise ValueError(f"ccxt_bridge: unknown endpoint: {raw.endpoint}")

    def _normalize_ohlcv(
        self,
        payload: list[list[Any]],
        raw_meta: dict[str, Any] | None = None,
    ) -> list[OHLCV]:
        """Normalize fetchOHLCV payload to OHLCV records。

        ccxt fetchOHLCV 返回格式：
        [
          [timestamp, datetime_str, open, high, low, close, volume],
          ...
        ]

        时间语义（审计 F-09）：ccxt 统一 ms epoch → us ×1000。
        """
        records: list[OHLCV] = []
        symbol = self._extract_symbol(payload, raw_meta or {})

        now = datetime.now(UTC).replace(tzinfo=None)

        # interval 是 OHLCV 身份键组成部分（D02 §2 L43），缺失/非法必须熔断
        # replay 场景 raw_meta 由 dataset params 补齐，键名为 interval
        timeframe_raw = (raw_meta or {}).get("timeframe") or (
            raw_meta or {}
        ).get("interval")
        if not timeframe_raw:
            raise ValueError("ccxt_bridge: timeframe missing in raw_meta for fetchOHLCV")
        try:
            interval_enum = Interval(timeframe_raw)
        except ValueError as e:
            raise ValueError(
                f"ccxt_bridge: unsupported fetchOHLCV timeframe: {timeframe_raw}"
            ) from e
        bar_seconds = interval_seconds(interval_enum)

        for candle in payload:
            if not isinstance(candle, (list, tuple)):
                note_skip("malformed_row")
                continue

            # ccxt 统一格式为 6 元素: [ts, open, high, low, close, volume]
            # 兼容 7 元素变体（部分交易所在 ts 后附带 datetime 字符串）
            if len(candle) == 6:
                timestamp_ms = int(candle[0])
                open_val = float(candle[1])
                high_val = float(candle[2])
                low_val = float(candle[3])
                close_val = float(candle[4])
                volume_val = float(candle[5])
            elif len(candle) >= 7:
                timestamp_ms = int(candle[0])
                # ccxt format: [timestamp, datetime_str, open, high, low, close, volume]
                open_val = float(candle[2])
                high_val = float(candle[3])
                low_val = float(candle[4])
                close_val = float(candle[5])
                volume_val = float(candle[6])
            else:
                note_skip("malformed_row")
                continue

            # ms → us ×1000 for storage
            event_time = datetime.fromtimestamp(
                timestamp_ms / 1000, tz=UTC
            ).replace(tzinfo=None)

            # 未收盘 bar（close_time 在未来）显式跳过并留痕；已收盘 bar 正常
            # 入库，重复采集由 natural key upsert 幂等收敛。
            if event_time + timedelta(seconds=bar_seconds) > now:
                logger.debug(
                    "connector.normalize_skip",
                    source="ccxt",
                    reason="unclosed_bar",
                    symbol=symbol,
                    event_time=str(event_time),
                )
                note_skip("unclosed_bar")
                continue

            # Validate price/volume are finite
            values = [open_val, high_val, low_val, close_val, volume_val]
            if not all(math.isfinite(v) for v in values):
                note_skip("non_finite_value")
                continue

            # Validate positive
            if any(v <= 0 for v in values):
                note_skip("non_positive_value")
                continue

            ex_upper = self.exchange_id.upper()
            if symbol:
                # ccxt 原生符号含 "/"（BTC/USDT），会破坏 canonical 的
                # entity={market_id} 单路径段分区约定；与项目跨源命名
                # （BINANCE:BTCUSDT:SPOT）对齐，统一去掉斜杠
                market_id = (
                    f"CCXT-{ex_upper}:{symbol.replace('/', '')}:SPOT"
                )
            else:
                market_id = f"CCXT-{ex_upper}:UNKNOWN:SPOT"

            record = OHLCV(
                schema_version="1.0",
                source="ccxt",
                source_id=str(timestamp_ms),
                source_timestamp=datetime.fromtimestamp(
                    timestamp_ms / 1000, tz=UTC
                ).replace(tzinfo=None),
                ingest_timestamp=now,
                raw_record_id=f"ccxt:ohlcv:{symbol}:{timestamp_ms}",
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

    def _normalize_trades(self, payload: list[dict[str, Any]]) -> list[TRADE]:
        """Normalize fetchTrades payload to TRADE records。

        ccxt fetchTrades 返回格式（每个 trade dict）：
        {
          'id': str,
          'timestamp': int (ms),
          'datetime': str,
          'symbol': str,
          'price': float,
          'amount': float,
          'side': 'buy' | 'sell',
          ...
        }
        """
        records: list[TRADE] = []
        now = datetime.now(UTC).replace(tzinfo=None)
        symbol = ""

        for trade in payload:
            if not isinstance(trade, dict):
                note_skip("malformed_row")
                continue

            try:
                timestamp_ms = int(trade["timestamp"])
                price = float(trade["price"])
                amount = float(trade["amount"])
                side_str = trade.get("side", "")

                if side_str == "buy":
                    side = Side.BUY
                elif side_str == "sell":
                    side = Side.SELL
                else:
                    note_skip("invalid_trade_side")
                    continue

                trade_id = str(trade.get("id", f"{timestamp_ms}_{price}_{amount}"))

                # ms → us ×1000
                event_time = datetime.fromtimestamp(
                    timestamp_ms / 1000, tz=UTC
                ).replace(tzinfo=None)

                symbol = trade.get("symbol", "")
                market_id = (
                    f"CCXT-{self.exchange_id.upper()}:"
                    f"{symbol.replace('/', '')}:SPOT"
                    if symbol
                    else ""
                )

                record = TRADE(
                    schema_version="1.0",
                    source="ccxt",
                    source_id=trade_id,
                    source_timestamp=event_time,
                    ingest_timestamp=now,
                    raw_record_id=f"ccxt:trade:{trade_id}",
                    quality_status=QualityStatus.VALID,
                    quality_reason=None,
                    market_id=market_id,
                    event_time=event_time,
                    price=price,
                    quantity=amount,
                    side=side,
                    trade_id=trade_id,
                )
                records.append(record)
            except (KeyError, ValueError, TypeError):
                note_skip("malformed_row")
                continue

        return records

    def _normalize_ticker(self, payload: dict[str, Any]) -> list[TICKER]:
        """Normalize fetchTicker payload to TICKER record。"""
        try:
            now = datetime.now(UTC).replace(tzinfo=None)
            symbol = payload.get("symbol", "")
            market_id = (
                f"CCXT-{self.exchange_id.upper()}:"
                f"{symbol.replace('/', '')}:SPOT"
                if symbol
                else ""
            )

            last_price = float(payload.get("last", 0))
            bid = float(payload.get("bid", 0))
            ask = float(payload.get("ask", 0))
            base_volume = float(payload.get("baseVolume", 0))
            quote_volume = float(payload.get("quoteVolume", 0))

            # ccxt ticker timestamp is ms
            timestamp_ms = int(payload.get("timestamp", 0))
            if timestamp_ms == 0:
                note_skip("malformed_row")
                return []

            event_time = datetime.fromtimestamp(
                timestamp_ms / 1000, tz=UTC
            ).replace(tzinfo=None)

            record = TICKER(
                schema_version="1.0",
                source="ccxt",
                source_id=symbol,
                source_timestamp=event_time,
                ingest_timestamp=now,
                raw_record_id=f"ccxt:ticker:{symbol}",
                quality_status=QualityStatus.VALID,
                quality_reason=None,
                market_id=market_id,
                event_time=event_time,
                last_price=last_price,
                bid=bid,
                ask=ask,
                volume_24h=base_volume,
                quote_volume_24h=quote_volume,
            )
            return [record]
        except (KeyError, ValueError, TypeError):
            note_skip("malformed_row")
            return []

    @staticmethod
    def _extract_symbol(
        payload: list[Any], raw_meta: dict[str, Any] | None = None
    ) -> str:
        """尝试从 payload 或 raw_meta 中推断 symbol。"""
        # 优先从 raw_meta 获取（测试场景）
        if raw_meta and "symbol" in raw_meta:
            return raw_meta["symbol"]  # type: ignore[no-any-return]
        if not payload:
            return ""
        first = payload[0]
        if isinstance(first, dict):
            return first.get("symbol", "") or ""
        return ""

    def validate(self, records: list[BaseRecord]) -> QualityReport:
        """验证记录（D04 §1，委托 quality.rules）。

        P0：实现基本校验。
        """
        findings: list[QualityFinding] = []

        for record in records:
            key = str(getattr(record, "natural_key", lambda: ("unknown",))())

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

    def row_units(self, raw: RawBatch) -> int | None:
        """OHLCV/trades 为行数组；ticker 为单快照（1 行）。"""
        payload = raw.payload
        if isinstance(payload, (list, tuple)):
            return len(payload)
        if isinstance(payload, dict):
            return 0 if not payload else 1
        raise SchemaError(
            "ccxt_bridge: payload is neither list nor dict",
            context={"source": "ccxt_bridge", "endpoint": raw.endpoint},
        )

    def checkpoint_from(
        self, raw: RawBatch | list[BaseRecord]
    ) -> str | None:
        """从 raw batch 或 records 中提取 checkpoint cursor（D04 §1）。

        For OHLCV: return last timestamp as cursor.
        For TRADE: return last trade_id as cursor.
        """
        if isinstance(raw, RawBatch):
            payload = raw.payload
            endpoint = raw.endpoint

            if endpoint == "fetchOHLCV" and payload:
                last_ts = payload[-1][0]
                return str(last_ts)
            if endpoint == "fetchTrades" and payload:
                last_id = payload[-1].get("id")
                return str(last_id) if last_id else None

        elif isinstance(raw, list) and raw:
            last_record = raw[-1]
            if isinstance(last_record, OHLCV):
                return str(_dt_to_ms(last_record.event_time))
            if isinstance(last_record, TRADE):
                return last_record.trade_id

        return None

    def close(self) -> None:
        """关闭 ccxt exchange 资源。"""
        try:
            self.exchange.close()
        except Exception:
            pass

    def __del__(self) -> None:
        """确保资源清理。"""
        try:
            self.close()
        except Exception:
            pass
