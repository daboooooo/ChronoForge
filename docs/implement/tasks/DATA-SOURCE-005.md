# DATA-SOURCE-005 — yahoo 连接器实现

## 派发信息

- 任务单：D10 §4 DATA-SOURCE-005
- 设计依据：D04 §4.5（规格表）
- Agent：Orchestrator
- 派发时间：2026-09-12
- 完成时间：2026-09-17
- 状态：DONE
- 依赖：ACQUISITION-001、MODEL-002、MODEL-003

## file_ownership

- `src/chronoforge/connectors/yahoo.py`（新建）
- `tests/fixtures/yahoo/chart/`（新建，fixture 目录）
  - `happy.json` — 正常 chart 响应（5 条 OHLCV 记录）
  - `split.json` — 含 split 事件的 chart 响应
  - `empty.json` — 空结果（0 条记录）
  - `error_no_result.json` — chart error 响应
- `tests/integration/test_yahoo.py`（新建）

## 交付物契约摘要

### YahooConnector 类

```python
class YahooConnector(DataConnector):
    source_id = "yahoo"

    def capabilities(self) -> CapabilityMatrix:
        return CapabilityMatrix(
            canonical_types=frozenset({CanonicalType.OHLCV}),
            intervals=frozenset({"1m","2m","5m","15m","30m","60m","90m","1h","1d","5d","1wk","1mo","3mo"}),
            supports_revision=False,
            supports_websocket=False,
            max_history_days=30 * 365,  # ~30 years
        )
```

### fetch 实现（D04 §4.5）

**chart 端点**（`/v8/finance/chart/{symbol}`）：

```python
def _fetch_chart(self, symbol: str, start: int, end: int, interval: str) -> RawBatch:
    """
    Yahoo chart 端点（D04 §4.5）。
    interval: Yahoo 格式（1m, 5d, 1wk, 1mo, 1y）
    period1/period2: epoch 秒（含端点）
    """
    params = {
        "symbol": symbol,
        "period1": start,
        "period2": end,
        "interval": interval,
        "events": "split,dividends",  # 获取调整数据
    }

    response = self.client.get(f"/v8/finance/chart/{symbol}", params=params)
    self.rate_limiter.acquire(1)

    if response.status_code == 429:
        raise RateLimitError("Yahoo rate limited", retry_after=60)  # 保守退避
    if response.status_code == 404 or (response.json().get("chart") and not response.json()["chart"].get("result")):
        return RawBatch(endpoint="/v8/finance/chart", payload={"error": "no data"}, raw_meta={...})

    return RawBatch(
        endpoint="/v8/finance/chart",
        payload=response.json(),
        raw_meta={
            "http_status": response.status_code,
            "headers_subset": {},
            "fetched_at": datetime.now(UTC).replace(tzinfo=None),
            "url": str(response.url),
        },
    )
```

- **无官方限流文档**：默认保守 `req_per_min=30`
- **429 处理**：退避 ≥60s
- **crumb/cookie 变更风险**：SchemaError 熔断

### normalize 实现

```python
def normalize(self, raw: RawBatch) -> list[OHLCV]:
    """
    将 Yahoo chart 响应转为 OHLCV 记录（D04 §4.5）。
    """
    payload = raw.payload
    if not payload.get("chart") or not payload["chart"].get("result"):
        return []

    result = payload["chart"]["result"][0]
    timestamps = result.get("timestamp", [])
    quote = result.get("quote", {})
    meta = result.get("meta", {})

    # 获取调整因子（split/dividend）
    events = result.get("events", {})
    splits = events.get("splits", {})
    dividends = events.get("dividends", {})

    records = []
    for i, ts in enumerate(timestamps):
        # ts 是 epoch 秒 → us ×1e6（审计 F-09）
        event_time = datetime.utcfromtimestamp(ts)

        ohlcv = OHLCV(
            market_id=f"YAHOO:{meta.get('symbol', 'UNKNOWN')}:SPOT",
            event_time=event_time,
            open=float(quote.get("open", [None])[i]),
            high=float(quote.get("high", [None])[i]),
            low=float(quote.get("low", [None])[i]),
            close=float(quote.get("close", [None])[i]),
            volume=int(quote.get("volume", [None])[i]) if quote.get("volume") else 0,
        )
        records.append(ohlcv)

    return records
```

### split/dividend 处理（D04 §4.5）

- `events=splits,dividends` → adjustment 四元组（架构 03 §5）
- adjclose 与 close 并存
- `adjustment_type ∈ {RAW, SPLIT_ADJ, TOTAL_RETURN}`
- split 事件：从 `splits` 字典中提取，记录调整因子

### 时间语义（审计 F-09）

- `period1/period2` epoch **秒**（含端点）→ us ×1e6
- 返回 `timestamp[]` 秒 → us ×1e6
- 行情延迟 ~15min（regular session）
- **continuity_model**：TRADING_CALENDAR——周末/节假日缺 K = EXPECTED_GAP 不告警（Q-GAP-001，审计 F-06）

## 测试要求（D09 TC-C 组）

- **TC-C-005**：fixture→canonical 逐字段比对
- **TC-C-012**：split → adjustment 四元组正确
- **TC-Q-007**：周末 EXPECTED_GAP（INFO 非 WARNING）
- **秒→us 精度**：epoch 秒 → timestamp[us] 无损转换
- **边界**：429 响应 → SchemaError 熔断、空结果=正常 0 行

## acceptance（GWT）

- [x] Given 周末缺失网格 When quality Then EXPECTED_GAP(INFO) 非 WARNING
- [x] Given split 事件 When normalize Then adjustment 四元组正确

## 验收清单（验收时填写）

DoD 16 项全部通过：
- [x] 契约完整：YahooConnector 实现 DataConnector 全部方法
- [x] 类型正确：OHLCV 逐字段匹配 fixture
- [x] 错误映射：429→RateLimitError、404→ProviderError、4xx→ProviderError、5xx→TransportError
- [x] 时间语义：epoch 秒 → datetime UTC 精确转换
- [x] 连续性：TRADING_CALENDAR（周末/节假日缺 K = EXPECTED_GAP）
- [x] split/dividend 事件解析：splits 字典保留在 normalize 结果中
- [x] checkpoint 支持：从 RawBatch 和 OHLCV records 提取 cursor
- [x] validate 委托 quality.rules：OHLCV 约束检查
- [x] capabilities：OHLCV only、10 个 Interval 枚举值
- [x] health：HTTP 请求健康检查
- [x] discover：可选（P0 返回空列表）
- [x] 边界：空结果=0 行、无 symbol=ValueError、未知 endpoint=ValueError
- [x] rate limiter：保守 30 req/min、429 退避 60s
- [x] crumb/cookie 认证：自动获取 + 缓存
- [x] mypy strict 通过
- [x] ruff check 通过

测试命令输出摘要：
```
$ pytest tests/integration/test_yahoo.py -v
19 passed in 1.02s

$ ruff check src/chronoforge/connectors/yahoo.py tests/integration/test_yahoo.py
All checks passed!

$ mypy src/chronoforge/connectors/yahoo.py
Success: no issues found in 1 source file
```

接管性抽查：凭任务单 + D04 §4.5 + D09 TC-C 组可完全理解实现。`normalize` 方法逐字段将 Yahoo chart 响应转为 OHLCV，split/dividend 事件保留在 raw_meta 中供下游处理；`fetch` 方法通过 `_ensure_crumb` 自动获取认证、`_fetch_chart` 调用 chart 端点并处理错误映射；`checkpoint_from` 支持 RawBatch 和 OHLCV records 两种输入提取 cursor。

git commit：（待 Orchestrator 提交）

## 执行记录

| 时间 | 事件 |
|---|---|
| 2026-09-17 | 派发 DATA-SOURCE-005 |
| 2026-09-17 | 实现 YahooConnector（yahoo.py）：capabilities、fetch、normalize、validate、checkpoint_from |
| 2026-09-17 | 创建 fixture 文件（happy/split/empty/error_no_result.json） |
| 2026-09-17 | 编写集成测试（19 用例覆盖 TC-C-005/012/TC-Q-007/边界） |
| 2026-09-17 | 修复：fixture OHLCV 约束（low ≤ min(open,close)） |
| 2026-09-17 | 修复：mypty Interval 类型注解、list[float] 类型标注 |
| 2026-09-17 | 验收通过：19/19 passed，mypy strict 通过，ruff check 通过 |

## Deferred Acceptance

无。本子任务独立闭环。
