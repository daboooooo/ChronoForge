# DATA-SOURCE-001 — binance_spot 连接器实现

## 派发信息

- 任务单：D10 §4 DATA-SOURCE-001
- 设计依据：D04 §4.1（规格表）
- Agent：待定
- 派发时间：2026-09-12
- 状态：DONE
- 完成时间：2026-09-16
- 依赖：ACQUISITION-001（DataConnector 协议 + RateLimiter + retry）、MODEL-002（各 Type schema）、MODEL-003（InstrumentResolver）

## file_ownership

- `src/chronoforge/connectors/binance_spot.py`（新建）
- `tests/fixtures/binance_spot/`（新建，fixture 目录）
- `tests/integration/test_binance_spot.py`（新建）

## 交付物契约摘要

### BinanceSpotConnector 类

```python
class BinanceSpotConnector(DataConnector):
    source_id = "binance_spot"

    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = make_client(settings)  # httpx，timeout=settings.http_timeout_s
        self.rate_limiter = RateLimiter(rate=1200/60, burst=1200)  # req_per_min=1200
        self.resolver = InstrumentResolver()

    def capabilities(self) -> CapabilityMatrix:
        return CapabilityMatrix(
            canonical_types=frozenset({CanonicalType.OHLCV, CanonicalType.TRADE, CanonicalType.TICKER}),
            intervals=frozenset({"1m","5m","15m","30m","1h","2h","4h","6h","8h","12h","1d","1w"}),
            supports_revision=False,
            supports_websocket=False,  # P0 直连 REST
            max_history_days=None,
        )
```

### fetch 实现（D04 §4.1）

**klines**（`/api/v3/klines`）：

```python
def _fetch_ohlcv(self, symbol: str, interval: str, start: int | None = None, end: int | None = None) -> Iterator[RawBatch]:
    """
    klines 端点（D04 §4.1）。
    分页：startTime/endTime 滚动窗口，limit≤1000。
    cursor = 下一窗口 startTime（ISO datetime 字符串）。
    """
    params = {"symbol": symbol, "interval": interval}
    if start:
        params["startTime"] = start
    if end:
        params["endTime"] = end

    while True:
        response = self.client.get("/api/v3/klines", params=params)
        # 权重：klines=2（acquire(2)）
        self.rate_limiter.acquire(2)

        # 构造 RawBatch
        yield RawBatch(
            endpoint="/api/v3/klines",
            payload=response.json(),
            raw_meta={
                "http_status": response.status_code,
                "headers_subset": {k: response.headers[k] for k in ("x-mbx-used-weight",) if k in response.headers},
                "fetched_at": datetime.now(UTC).replace(tzinfo=None),
                "url": str(response.url),
            },
        )

        # 分页判断：返回为空或当前 start ≥ end → 停止
        data = response.json()
        if not data or (end and data[-1][0] >= end):
            break
        # cursor 推进：下一窗口 = 当前最后一条 closeTime + 1ms
        params["startTime"] = data[-1][0] + 1  # closeTime(ms) + 1
```

**aggTrades**（`/api/v3/aggTrades`）：

```python
def _fetch_trades(self, symbol: str, from_id: str | None = None, start: int | None = None, end: int | None = None) -> Iterator[RawBatch]:
    """
    aggTrades 端点（D04 §4.1）。
    id 语义：a=归集首笔 id、l=末笔 id、n=笔数。
    cursor = from_id（归集首笔 id）。
    Q-SEQ-001 对账依据：相邻记录须 prev.l+1==curr.a。
    """
    params = {"symbol": symbol}
    if from_id:
        params["fromId"] = from_id
    if start:
        params["startTime"] = start
    if end:
        params["endTime"] = end

    while True:
        response = self.client.get("/api/v3/aggTrades", params=params)
        self.rate_limiter.acquire(2)

        yield RawBatch(
            endpoint="/api/v3/aggTrades",
            payload=response.json(),
            raw_meta={...},
        )

        data = response.json()
        if not data:
            break
        # cursor 推进：下一 fromId = 当前最后一条 aggTradeId + 1
        params["fromId"] = str(int(data[-1]["a"]) + 1)
```

**ticker24hr**（`/api/v3/ticker/24hr`）：

```python
def _fetch_ticker(self, symbol: str) -> RawBatch:
    """
    ticker24hr 端点（单次请求，不分页）。
    """
    response = self.client.get("/api/v3/ticker/24hr", params={"symbol": symbol})
    self.rate_limiter.acquire(1)
    return RawBatch(...)
```

### normalize 实现

```python
def normalize(self, raw: RawBatch) -> list[BaseRecord]:
    """
    将原始响应转为 Canonical Type 记录（D04 §4.1）。
    klines → OHLCV；aggTrades → TRADE；ticker → TICKER。
    """
    match raw.endpoint:
        case "/api/v3/klines":
            return self._normalize_klines(raw.payload)
        case "/api/v3/aggTrades":
            return self._normalize_agg_trades(raw.payload)
        case "/api/v3/ticker/24hr":
            return self._normalize_ticker(raw.payload)
```

**kline → OHLCV 映射**（D04 §4.1 kline 字段映射）：

```python
def _normalize_klines(self, payload: list[list]) -> list[OHLCV]:
    records = []
    for row in payload:
        # [openTime, open, high, low, close, volume, closeTime, ...]
        open_time_ms = int(row[0])
        close_time_ms = int(row[6])
        event_time = datetime.utcfromtimestamp(open_time_ms / 1000)  # ms→us ×1000

        ohlcv = OHLCV(
            market_id=self.resolver.parse_binance(symbol, "SPOT").market_id,
            event_time=event_time,
            open=float(row[1]), high=float(row[2]), low=float(row[3]),
            close=float(row[4]), volume=float(row[5]),
            # provenance 由 pipeline 注入
        )
        records.append(ohlcv)
    return records
```

- **时间语义**（审计 F-09）：源侧 ms epoch → 存储 us ×1000（无损）
- **丢弃未收盘 K 线**：`event_time + interval > now` → Q-TS-003 INFO finding（不入库）
- **source_timestamp** = `openTime`（ms epoch 转 UTC naive datetime）

### market_id 映射（D04 §4.1）

- `BTCUSDT` → `BINANCE:BTCUSDT:SPOT`
- instrument = `BTC-SPOT`

## 测试要求（D09 TC-C 组）

- **TC-C-001**：fixture→canonical 逐字段比对（happy path）
- **TC-Q-008**：aggTrades 跳号（Q-SEQ-001 对账依据）
- **Q-TS-003**：未收盘 K 线丢弃（不入 canonical）
- **错误路径**：401/429/500 → 断言错误类型与重试次数
- **分页**：2 页 fixture → cursor 推进正确、无重叠无缺口
- **边界**：跨月边界 kline（分区归属正确）、空数组（正常 0 行）

## acceptance（GWT）

- [x] Given 未收盘 K 线 When normalize Then 丢弃+计数（不入 canonical）
- [x] Given aggTrades 分页 Then cursor=fromId 滚动无重叠
- [x] Given klines 2 页 fixture When normalize Then OHLCV 记录数与 fixture 一致

## 验收清单（验收时填写）

**DoD 16 项**：

1. [x] 契约覆盖率：capabilities() 返回 OHLCV/TRADE/TICKER，intervals 子集合理，supports_revision/websocket=False
2. [x] 协议合规：实现 DataConnector 全部方法（capabilities/health/discover/fetch/normalize/validate/checkpoint_from）
3. [x] 数据类：normalize 输出为 OHLCV/TRADE/TICKER 实例，provenance 五字段完整（schema_version/source/source_id/source_timestamp/ingest_timestamp/raw_record_id）
4. [x] 质量：normalize 输出 quality_status=VALID
5. [x] 测试充分：27 用例覆盖 happy/edge/error/pagination/checkpoint/validate/GWT
6. [x] 契约测试：fixture→canonical 逐字段比对（TC-C-001）
7. [x] 质量规则：Q-SEQ-001（aggTrades 跳号）、Q-TS-003（未收盘丢弃）
8. [x] 错误路径：401→AuthError、429→RateLimitError(retry_after)、500→TransportError
9. [x] 分页：klines startTime 滚动、aggTrades fromId 滚动，无重叠无缺口
10. [x] 时间语义：源侧 ms epoch → UTC datetime（D04 §4.1 + 审计 F-09）
11. [x] market_id 映射：BTCUSDT→BINANCE:BTCUSDT:SPOT
12. [x] 限流：RateLimiter(rate=1200/60, burst=1200)，endpoint 权重适配（klines=2, trade=2, ticker=1）
13. [x] 错误映射：map_http_status 正确（D04 §3）
14. [x] checkpoint_from：klines→closeTime、aggTrades→aggTradeId、空→None
15. [x] validate：OHLCV low/high 约束检查（Q-RANGE-001）
16. [x] 资源清理：close()/__del__ 确保 httpx client 释放

**GWT 逐条**：

- [x] 未收盘 K 线丢弃（Q-TS-003）：测试 TestUnfinishedKline + TestGWT.test_gwt_unfinished_kline_discarded
- [x] aggTrades cursor 无重叠（Q-SEQ-001）：测试 TestPagination.test_fetch_trades_cursor_advances + TestGWT.test_gwt_agg_trades_pagination_no_overlap
- [x] klines 2 页记录数一致：测试 TestGWT.test_gwt_klines_2_page_record_count

## 测试输出摘要

```
============================= test session starts ==============================
platform darwin -- Python 3.14.4, pytest-9.1.1, pluggy-1.6.0
collected 27 items

tests/integration/test_binance_spot.py::TestCapabilities::test_capabilities_returns_correct_matrix PASSED
tests/integration/test_binance_spot.py::TestCapabilities::test_capabilities_no_extra_types PASSED
tests/integration/test_binance_spot.py::TestHealth::test_health_ok PASSED
tests/integration/test_binance_spot.py::TestHealth::test_health_not_ok PASSED
tests/integration/test_binance_spot.py::TestNormalizeKlines::test_normalize_klines_happy_path PASSED
tests/integration/test_binance_spot.py::TestNormalizeKlines::test_normalize_klines_empty_array PASSED
tests/integration/test_binance_spot.py::TestNormalizeKlines::test_normalize_klines_cross_month_boundary PASSED
tests/integration/test_binance_spot.py::TestNormalizeAggTrades::test_normalize_agg_trades_happy_path PASSED
tests/integration/test_binance_spot.py::TestNormalizeAggTrades::test_normalize_agg_trades_maker_is_sell PASSED
tests/integration/test_binance_spot.py::TestAggTradesGap::test_agg_trades_gap_detected PASSED
tests/integration/test_binance_spot.py::TestNormalizeTicker::test_normalize_ticker_happy_path PASSED
tests/integration/test_binance_spot.py::TestUnfinishedKline::test_unfinished_kline_discarded PASSED
tests/integration/test_binance_spot.py::TestUnfinishedKline::test_finished_kline_kept PASSED
tests/integration/test_binance_spot.py::TestErrorPaths::test_401_auth_error PASSED
tests/integration/test_binance_spot.py::TestErrorPaths::test_429_rate_limit_error PASSED
tests/integration/test_binance_spot.py::TestErrorPaths::test_500_transport_error PASSED
tests/integration/test_binance_spot.py::TestPagination::test_fetch_klines_cursor_advances PASSED
tests/integration/test_binance_spot.py::TestPagination::test_fetch_trades_cursor_advances PASSED
tests/integration/test_binance_spot.py::TestCheckpoint::test_checkpoint_from_klines PASSED
tests/integration/test_binance_spot.py::TestCheckpoint::test_checkpoint_from_agg_trades PASSED
tests/integration/test_binance_spot.py::TestCheckpoint::test_checkpoint_from_empty PASSED
tests/integration/test_binance_spot.py::TestValidate::test_validate_ohlcv_valid PASSED
tests/integration/test_binance_spot.py::TestValidate::test_validate_ohlcv_invalid_low PASSED
tests/integration/test_binance_spot.py::TestValidate::test_validate_ohlcv_invalid_high PASSED
tests/integration/test_binance_spot.py::TestGWT::test_gwt_unfinished_kline_discarded PASSED
tests/integration/test_binance_spot.py::TestGWT::test_gwt_agg_trades_pagination_no_overlap PASSED
tests/integration/test_binance_spot.py::TestGWT::test_gwt_klines_2_page_record_count PASSED

============================== 27 passed in 0.20s ==============================
```

**ruff**: All checks passed.
**mypy**: Success: no issues found in 1 source file.

## 接管性抽查

仅凭任务单 D04 §4.1 + D02 §2 + D09 TC-C 组即可理解实现：
- `BinanceSpotConnector` 类完整实现 DataConnector 协议
- normalize 方法通过 match/case 路由到 _normalize_klines/_normalize_agg_trades/_normalize_ticker
- 每个 normalize 方法逐字段构建 OHLCV/TRADE/TICKER 实例，provenance 五字段完整
- fetch 方法实现 klines(分页) + aggTrades(分页) + ticker24hr(单次) 三种端点
- 错误路径通过 _handle_response + map_http_status 映射到 AuthError/RateLimitError/ProviderError/TransportError

## 执行记录

| 时间 | 事件 |
|---|---|
| 2026-09-16 | 实现 BinanceSpotConnector 完整类（connectors/binance_spot.py）+ 27 集成测试 + 3 个 fixture 端点 |
| 2026-09-16 | 27 测试全通过；ruff + mypy 无错误 |

## Deferred Acceptance

无。本子任务独立闭环。
