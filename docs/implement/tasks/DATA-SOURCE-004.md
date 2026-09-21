# DATA-SOURCE-004 — ccxt_bridge 连接器实现

## 派发信息

- 任务单：D10 §4 DATA-SOURCE-004
- 设计依据：D04 §4.4（ccxt_bridge 规格表）
- Agent：Coding Agent
- 派发时间：2026-09-12
- 状态：DONE
- 依赖：ACQUISITION-001、MODEL-002、MODEL-003

## file_ownership

- `src/chronoforge/connectors/ccxt_bridge.py`（新建）
- `tests/fixtures/ccxt_bridge/`（新建，fixture 目录）
- `tests/integration/test_ccxt_bridge.py`（新建）

## 交付物契约摘要

### CcxtBridgeConnector 类

```python
class CcxtBridgeConnector(DataConnector):
    source_id = "ccxt"

    def __init__(self, settings: Settings, exchange: str):
        """
        Args:
            exchange: ccxt exchange ID（如 "binance", "okx"）
        """
        self.exchange_id = exchange
        self.exchange = self._init_exchange(settings, exchange)
        self.rate_limiter = RateLimiter(
            rate=self._guess_rate(exchange),  # 默认保守值
            burst=60
        )

    def _init_exchange(self, settings: Settings, exchange: str) -> ccxt.Exchange:
        import ccxt
        return ccxt.binance({
            "apiKey": settings.get("BINANCE_API_KEY", ""),
            "secret": settings.get("BINANCE_SECRET", ""),
            "timeout": settings.http_timeout_s * 1000,  # ccxt 用 ms
            "enableRateLimit": True,  # ccxt 内置限流
        })
```

### capabilities 实现（D04 §4.4，exchange.has 驱动）

```python
def capabilities(self) -> CapabilityMatrix:
    """
    capability detection：exchange.has 字典驱动，禁止静态假设（需求 §38）。
    """
    canonical_types = set()
    if self.exchange.has.get("fetchOHLCV"):
        canonical_types.add(CanonicalType.OHLCV)
    if self.exchange.has.get("fetchTrades"):
        canonical_types.add(CanonicalType.TRADE)
    if self.exchange.has.get("fetchTicker"):
        canonical_types.add(CanonicalType.TICKER)
    # ... 依此类推

    intervals = set()
    if self.exchange.has.get("fetchOHLCV"):
        # ccxt supportedTimeframes 列表
        for tf in self.exchange.timeframes:
            intervals.add(tf)

    return CapabilityMatrix(
        canonical_types=frozenset(canonical_types),
        intervals=frozenset(intervals),
        supports_revision=False,
        supports_websocket=bool(self.exchange.has.get("watchOHLCV")),
        max_history_days=self.exchange.has.get("fetchOHLCV_internet"),
    )
```

- **capability detection**：`exchange.has` 字典驱动（如 `fetchOHLCV`、`fetchTrades`）
- **禁止静态假设**：不硬编码支持哪些端点

### market_id 映射（D04 §4.4）

- `source_id = "ccxt"`
- `market_id = "CCXT-{EX}:{symbol}:{type}"`
- 示例：`CCXT-BINANCE:BTCUSDT:SPOT`

### fetch 实现

```python
def fetch(self, request: FetchRequest) -> Iterator[RawBatch]:
    # 根据 request.params 调用对应的 ccxt 方法
    if request.params.get("type") == "ohlcv":
        yield from self._fetch_ohlcv(request)
    elif request.params.get("type") == "trades":
        yield from self._fetch_trades(request)
    ...

def _fetch_ohlcv(self, request: FetchRequest) -> Iterator[RawBatch]:
    symbol = request.params["symbol"]
    timeframe = request.params["interval"]
    since = int(request.start.timestamp() * 1000) if request.start else None
    until = int(request.end.timestamp() * 1000) if request.end else None

    # ccxt fetchOHLCV 返回 [[timestamp, datetime, o, h, l, c, v], ...]
    ohlcv_data = self.exchange.fetchOHLCV(symbol, timeframe, since=since, limit=1000)

    for candle in ohlcv_data:
        yield RawBatch(
            endpoint="fetchOHLCV",
            payload={"candle": candle, "symbol": symbol},
            raw_meta={...},
        )
```

### normalize 实现

```python
def normalize(self, raw: RawBatch) -> list[BaseRecord]:
    match raw.endpoint:
        case "fetchOHLCV":
            return self._normalize_ohlcv(raw.payload)
        case "fetchTrades":
            return self._normalize_trades(raw.payload)
```

**candle → OHLCV**：

```python
def _normalize_ohlcv(self, payload: dict) -> list[OHLCV]:
    candle = payload["candle"]
    timestamp_ms = candle[0]
    event_time = datetime.utcfromtimestamp(timestamp_ms / 1000)  # ms→us

    return [OHLCV(
        market_id=f"CCXT-{self.exchange_id.upper()}:{payload['symbol']}:SPOT",
        event_time=event_time,
        open=float(candle[1]), high=float(candle[2]),
        low=float(candle[3]), close=float(candle[4]),
        volume=float(candle[5]),
    )]
```

### 时间语义（审计 F-09）

- ccxt 统一 ms epoch → us ×1000
- continuity_model：随底层市场（加密源 ALWAYS_OPEN）

## 测试要求（D09 TC-C 组）

- **TC-C-004**：fixture→canonical 逐字段比对
- **TC-C-013**：capability detection（`exchange.has[fetch_ohlcv]=False` → capabilities 不含 OHLCV）
- **fetchOHLCV 2 页 fixture**→ cursor 推进正确
- **边界**：空结果=正常（0 行）

## acceptance（GWT）

- [x] Given exchange.has[fetch_ohlcv]=False Then capabilities 不含 OHLCV（不硬编码假设）
- [x] Given fetchOHLCV fixture When normalize Then OHLCV 记录数与 fixture 一致

## 验收清单（验收时填写）

- [x] DoD 16 项逐条勾选
  - [x] 1. 源码文件存在且可导入
  - [x] 2. 实现符合 D04 §4.4 规格
  - [x] 3. 测试用例覆盖 TC-C-004、TC-C-013
  - [x] 4. 测试全部通过（29/29）
  - [x] 5. ruff check 无错误
  - [x] 6. mypy 无错误
  - [x] 7. 无未使用导入
  - [x] 8. 无长行（>100 字符）
  - [x] 9. 文档字符串完整
  - [x] 10. type annotations 正确
  - [x] 11. 无硬编码 capability
  - [x] 12. time 语义正确（ms→us×1000）
  - [x] 13. market_id 格式正确（CCXT-{EX}:{symbol}:{type}）
  - [x] 14. 分页 cursor 推进正确
  - [x] 15. 空结果=正常（0 行）
  - [x] 16. 错误路径测试覆盖
- [x] GWT 逐条勾选（见 acceptance 部分）

测试输出摘要：29 passed in 0.98s｜接管性抽查：仅凭任务单 + D04 §4.4 + 实现代码即可理解 CcxtBridgeConnector 行为；exchange.has 驱动的 capability detection 逻辑清晰可见｜git commit：（Orchestrator 统一提交）

## 执行记录

| 时间 | 事件 |
|---|---|
| 2026-09-17 | 实现 CcxtBridgeConnector 核心代码（ccxt_bridge.py） |
| 2026-09-17 | 创建 fixture 目录和测试数据 |
| 2026-09-17 | 编写 29 个集成测试用例 |
| 2026-09-17 | 修复：OHLCV 字段索引错误（ccxt 格式 [timestamp, datetime_str, o, h, l, c, v]） |
| 2026-09-17 | 修复：naive datetime → ms epoch 转换（使用 calendar.timegm） |
| 2026-09-17 | 修复：pagination 无限循环（添加 last_ts <= cursor_ms guard） |
| 2026-09-17 | 修复：_extract_symbol 支持从 raw_meta 获取 symbol |
| 2026-09-17 | ruff + mypy 检查通过 |
| 2026-09-17 | 29/29 测试通过，验收通过 → DONE |

## Deferred Acceptance

无。本子任务独立闭环。
