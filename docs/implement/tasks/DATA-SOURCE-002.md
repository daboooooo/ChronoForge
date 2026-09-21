# DATA-SOURCE-002 — binance_futures（USDT-M）连接器实现

## 派发信息

- 任务单：D10 §4 DATA-SOURCE-002
- 设计依据：D04 §4.2（规格表）
- Agent：Orchestrator
- 派发时间：2026-09-12
- 状态：DONE
- 依赖：ACQUISITION-001、MODEL-002、MODEL-003

## file_ownership

- `src/chronoforge/connectors/binance_futures.py`（新建）
- `tests/fixtures/binance_futures/`（新建，fixture 目录）
- `tests/integration/test_binance_futures.py`（新建）

## 交付物契约摘要

### BinanceFuturesConnector 类

```python
class BinanceFuturesConnector(DataConnector):
    source_id = "binance_futures"

    def capabilities(self) -> CapabilityMatrix:
        return CapabilityMatrix(
            canonical_types=frozenset({
                CanonicalType.OHLCV, CanonicalType.FUNDING,
                CanonicalType.OPEN_INTEREST,
            }),
            intervals=frozenset({"1m","5m","15m","30m","1h","2h","4h","6h","8h","12h","1d","1w"}),
            supports_revision=False,
            supports_websocket=False,  # liquidation WebSocket = P1
            max_history_days=None,
        )
```

### fetch 实现（D04 §4.2）

**klines**（`/fapi/v1/klines`）：

- 同 binance_spot klines 端点，base_url = `https://fapi.binance.com`
- cursor = `startTime`（下一窗口 start）
- market 映射：`BTCUSDT` → `BINANCE:BTCUSDT:USDT-FUT`，instrument = `BTC-PERP`

**fundingRate**（`/fapi/v1/fundingRate`）：

```python
def _fetch_funding(self, symbol: str, start_time: int | None = None) -> Iterator[RawBatch]:
    """
    fundingRate 端点（D04 §4.2）。
    cursor = fundingTime（结算时间）滚动。
    funding 结算周期 = 8h。
    """
    params = {"symbol": symbol}
    if start_time:
        params["startTime"] = start_time

    while True:
        response = self.client.get("/fapi/v1/fundingRate", params=params)
        self.rate_limiter.acquire(2)

        yield RawBatch(...)

        data = response.json()
        if not data:
            break
        # cursor 推进：下一 startTime = 当前最后一条 fundingTime + 1ms
        params["startTime"] = str(int(data[-1]["fundingTime"]) + 1)
```

- **时间语义**（审计 F-09）：funding 结算时间 = fundingTime（含端点）

**openInterest**（`/fapi/v1/openInterest`）：

```python
def _fetch_oi(self, symbol: str) -> RawBatch:
    """
    OI 快照（单次请求）。
    cursor = 上次 event_time（轮询快照）。
    """
    response = self.client.get("/fapi/v1/openInterest", params={"symbol": symbol})
    self.rate_limiter.acquire(2)
    return RawBatch(...)
```

- OI 是快照，无分页；每次全量拉取后 by nk upsert

### normalize 实现

```python
def normalize(self, raw: RawBatch) -> list[BaseRecord]:
    match raw.endpoint:
        case "/fapi/v1/klines":
            return self._normalize_klines(raw.payload)
        case "/fapi/v1/fundingRate":
            return self._normalize_funding(raw.payload)
        case "/fapi/v1/openInterest":
            return self._normalize_oi(raw.payload)
```

**funding → FUNDING**：

- `event_time` = fundingTime（ms epoch 转 UTC naive datetime）
- `next_funding_time` = nextFundingTime
- `funding_rate` = fundingRate

**OI → OPEN_INTEREST**：

- `open_interest` = raw 中的 oi 字段（float）
- `unit` = "contracts"（或 "coin"，依合约类型）

### 时间语义（审计 F-09）

- 全部 ms epoch → us ×1000
- funding 结算时间 = fundingTime（含端点）
- OI 快照 event_time = fetched_at（无源侧时间）

### continuity_model

- ALWAYS_OPEN（7×24）

## 测试要求（D09 TC-C 组）

- **TC-C-002**：fixture→canonical 逐字段比对（klines/funding/OI）
- **funding 8h 周期窗口切分**：多窗口测试 cursor 滚动
- **OI 快照全量 upsert**：同 nk 覆盖
- **分页**：fundingRate 2 页 → cursor=fundingTime 滚动无重叠

## acceptance（GWT）

- [x] Given fundingRate 分页 Then cursor=fundingTime 滚动无重叠
- [x] Given OI 快照 When normalize Then OPEN_INTEREST 记录含 correct market_id

## 验收清单（验收时填写）

DoD 16 项 + GWT 逐条勾选。

测试输出摘要：33 passed in 0.58s（pytest）｜ ruff check 通过｜ mypy strict 通过｜ lint-imports 通过｜ 全库 838/838 passed
接管性抽查：凭任务单 + D04 §4.2 + D02 §2 字段表 + D09 §3 TC-C 组，可逐字段理解 normalize(klines/funding/OI) 到 OHLCV/FUNDING/OPEN_INTEREST 的映射逻辑；分页 cursor 滚动（klines: closeTime+1ms, funding: fundingTime+1ms）与 binance_spot 模式一致。
git commit：feat(connectors): implement BinanceFuturesConnector for binance_futures

## 执行记录

| 时间 | 事件 |
|---|---|
| 2026-09-16 | 实现 BinanceFuturesConnector 完整代码（connectors/binance_futures.py） |
| 2026-09-16 | 创建 fixture 目录 tests/fixtures/binance_futures/（klines/fundingRate/openInterest） |
| 2026-09-16 | 实现 33 条集成测试（TC-C-002 + TC-C-011 + GWT + 边界/错误路径） |
| 2026-09-16 | 验收：33 passed in 0.58s；ruff + mypy strict 无错误；全库 838/838 passed |

## Deferred Acceptance

无。本子任务独立闭环。
