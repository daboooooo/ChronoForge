# D04 — Connector 详细设计

依据：架构 02/05/06。本文档定义连接器协议、限流/重试实现、各 P0 源规格。实施前必须逐条核对官方 API 文档并记录到 source_registry（架构 05 §3，需求 §44）。

## 1. 协议（connectors/base.py）

```python
@dataclass(frozen=True)
class FetchRequest:
    dataset_id: str
    params: Mapping[str, Any]        # {"symbol":"BTCUSDT","interval":"1m",...}
    start: datetime | None
    end: datetime | None
    cursor: str | None               # 增量续传（源语义，见 §4 各表）

@dataclass
class RawBatch:
    endpoint: str
    payload: Any                     # 源响应反序列化结果（原样，不清洗）
    raw_meta: dict                   # {"http_status","headers_subset","fetched_at","url"}

@dataclass(frozen=True)
class CapabilityMatrix:
    canonical_types: frozenset[CanonicalType]
    intervals: frozenset[Interval]
    supports_revision: bool
    supports_websocket: bool
    max_history_days: int | None

class DataConnector(Protocol):
    source_id: str
    def capabilities(self) -> CapabilityMatrix: ...
    def health(self) -> HealthStatus: ...          # {'ok':bool,'latency_ms':int,'detail':str}
    def discover(self) -> list[InstrumentRef]: ... # 可选实体/合约枚举（Deribit 必须）
    def fetch(self, request: FetchRequest) -> Iterator[RawBatch]: ...   # 分页由实现内聚
    def normalize(self, raw: RawBatch) -> list[BaseRecord]: ...         # -> 具体子类
    def validate(self, records: list[BaseRecord]) -> "QualityReport": ...  # 委托 quality.rules
    def checkpoint_from(self, raw: RawBatch | records) -> str | None: ...
```

- connector **不落盘、不写库**（架构 02 边界规则 6）；`normalize` 输出必含 provenance 字段（raw_record_id 由 pipeline 注入回调 `raw_ref_provider`）

## 2. 限流器（connectors/ratelimit.py）

令牌桶：`RateLimiter(rate: float, burst: int)`，`await/调用 acquire(n=1)` 阻塞至可用。Binance 按 endpoint 权重表计权（`acquire(weight)`）；429 → 全局冷却 `Retry-After` 秒并记录 warning。实现须可注入时钟（测试）。

## 3. 重试（connectors/errors.py + base）

```
retry(fetch) : 最多 5 次，指数退避 1s→2s→4s→8s→16s + jitter(0~0.5s)
仅重试: TransportError / RateLimitError / HTTP>=500
不重试: AuthError / ProviderError(4xx≠429) / SchemaError
每次尝试记 structlog event="connector.retry" {source,endpoint,attempt,exc}
```

HTTP 映射：401/403→AuthError；429→RateLimitError；400/404/422→ProviderError；5xx/超时→TransportError；响应结构不符录制契约→SchemaError（对比 fixture 的 JSON Schema，见 D09）。

## 4. 源规格表

§4.1~4.7 为 P0 源（原始需求范围）；§4.8~4.11 为后续追加的源（ETF 资金流 / 情绪指数 / 链上供给 / 全市场指标），规格以已落地实现为准。

### 4.1 binance_spot（直连，REST）

| 项 | 规格 |
|---|---|
| endpoints | `/api/v3/klines`(OHLCV,limit≤1000)、`/api/v3/aggTrades`(TRADE)、`/api/v3/ticker/24hr`(TICKER) |
| 分页/cursor | klines：`startTime/endTime` 滚动窗口；cursor = 下一窗口 startTime（ISO） |
| 限流 | 权重制（klines=2 等），实现前核对当前权重表 |
| market 映射 | `BTCUSDT` → `BINANCE:BTCUSDT:SPOT`，instrument `BTC-SPOT` |
| kline 字段映射 | `[openTime,o,h,l,c,v,closeTime,...]` → event_time=openTime(UTC)；closeTime→quality 附注（区间闭合校验用） |
| 时间语义（审计 F-09） | 源侧全部 ms epoch → 存储 us：×1000（无损）；源即 UTC，无 tz 后缀 |
| aggTrades id 语义 | `a`=归集首笔 id、`l`=末笔 id、`n`=笔数；相邻记录须 prev.l+1==curr.a（Q-SEQ-001 对账依据，审计 F-05） |
| continuity_model | ALWAYS_OPEN（7×24） |

### 4.2 binance_futures（USDT-M）

| 项 | 规格 |
|---|---|
| endpoints | `/fapi/v1/klines`、`/fapi/v1/fundingRate`(FUNDING, ≤1000)、`/fapi/v1/openInterest`(OI 快照)、`/futures/data/takerlongshortRatio` 等统计端点按需 |
| cursor | fundingRate：`startTime` 滚动；OI：轮询快照 cursor=上次 event_time |
| liquidation | **P1**：`!forceOrder@arr` WebSocket → 走 D05 §7 流式路径；P0 不做 |
| market 映射 | `BINANCE:BTCUSDT:USDT-FUT`，instrument `BTC-PERP` |
| 时间语义（审计 F-09） | ms epoch → us ×1000；funding 结算时间=fundingTime（含端点） |
| continuity_model | ALWAYS_OPEN（7×24） |

### 4.3 deribit（期权）

| 项 | 规格 |
|---|---|
| endpoints | `public/get_instruments`(discover)、`public/get_book_summary_by_currency`(OPTION 快照)、`public/get_tradingview_chart_data`(OHLCV/IV 历史) |
| discover | 必须实现：全量 instrument → INSTRUMENT canonical 记录 + 本地 instrument 表缓存（models InstrumentResolver） |
| 解析 | `parse_deribit`（D02 §3）；期权 OHLCV 的 IV 推导属 derived 层，connector 只存 OPTION 价格快照与源侧 IV 字段 |
| market 映射 | `DERIBIT:BTC-26SEP26-100000-C:OPTION` |
| 时间语义（审计 F-09） | `timestamp` ms → us ×1000；`expiration`（"26SEP26" 月份码）解析为 UTC 日期；源即 UTC 无 DST 歧义 |
| continuity_model | ALWAYS_OPEN（7×24）；到期后 instrument 标 SUSPECT（Q-RANGE-003） |

### 4.4 ccxt_bridge（抽象层，架构 06 D6）

- `source_id = "ccxt"`，构造参数 `exchange: str`（如 binance/okx）；market_id = `CCXT-{EX}:{symbol}:{type}`
- 仅用于 ccxt 已抽象的统一端点（fetch_ohlcv/fetch_trades）；交易所特有数据一律直连 connector
- capability detection：`exchange.has` 字典驱动，禁止静态假设（需求 §38）
- 时间语义（审计 F-09）：ccxt 统一 ms epoch → us ×1000；continuity_model 随底层市场（加密源 ALWAYS_OPEN）

### 4.5 yahoo

| 项 | 规格 |
|---|---|
| endpoints | `/v8/finance/chart/{symbol}`（区间+interval 参数） |
| 无官方限流文档 | 默认保守 `req_per_min=30`；429 处理同上；crumb/cookie 变更风险 → SchemaError 熔断 |
| 调整语义 | `events=splits,dividends` → adjustment 四元组（架构 03 §5）；adjclose 与 close 并存，adjustment_type ∈ {RAW, SPLIT_ADJ, TOTAL_RETURN} |
| 时间语义（审计 F-09） | `period1/period2` epoch **秒**（含端点）→ us ×1e6；返回 `timestamp[]` 秒 → us；行情延迟 ~15min（regular session） |
| continuity_model | TRADING_CALENDAR——周末/节假日缺 K = EXPECTED_GAP 不告警（Q-GAP-001，审计 F-06） |

### 4.6 fred（PUBLIC_WITH_KEY）

| 项 | 规格 |
|---|---|
| endpoints | `/fred/series/observations`（params: series_id, observation_start/end, file_type=json）；系列清单静态注册（CPIAUCSL/UNRATE/DGS10/…按需求清单） |
| 修订 | `revision_supported=true`：每次全窗口拉取 → 与库内 (nk, revision_time) 对比，新增 revision 追加；release_time 取 FRED 发布日历（`/fred/release/dates`），无则=观察日次日并在 quality 附注 |
| key | `FRED_API_KEY` env（D08） |
| 时间语义（审计 F-09） | observation `date` 日期型 → `T00:00:00Z`；release 日期无时刻 → 保守取 `T23:59:59Z`（防 look-ahead，宁可晚可见）；vintage=realtime_date |
| continuity_model | RELEASE_SCHEDULE（无网格 gap 概念，仅 Q-GAP-002 节奏对比） |

### 4.7 sec_edgar（PUBLIC，fair-access）

| 项 | 规格 |
|---|---|
| headers | `User-Agent: ChronoForge/{version} research (contact@example.com)` **必填**（官方 fair-access 要求，联系邮箱由 Settings 注入） |
| 限流 | ≤10 req/s；串行 + 令牌桶双保险 |
| endpoints | `submissions/CIK{cik}.json`(FILING 索引)、`companyfacts/CIK{cik}.json`(FUNDAMENTAL)、full-text search 按需 |
| cursor | submissions `filing-recent.json` 增量；companyfacts 全量 JSON 按 accession 缓存，response ETag/Last-Modified 有则条件请求 |
| 时间语义（审计 F-09） | `acceptanceDateTime` 带 EDT/EST 后缀 → 先转 UTC 再去 tzinfo；`filingDate` 日期型 → 保守 `T23:59:59Z` |
| continuity_model | EVENT_BASED（无 gap 概念） |

### 4.8 sosovalue（PUBLIC_WITH_KEY，US 现货加密 ETF 资金流）

| 项 | 规格 |
|---|---|
| endpoints | `/etfs`（ticker 清单，供 discover/注册脚本复用）、`/etfs/summary-history`（汇总日频）、`/etfs/{ticker}/history`（单 ticker 日频） |
| key | `SOSOVALUE_API_KEY` env；启用该源时缺失 → ConfigError（组合根 fail fast） |
| 限流 | 20 req/min（burst=20） |
| 指标 | FLOW 类：`total_net_inflow` / `total_value_traded` / `net_inflow`；NUMBER 类：`total_net_assets` / `cum_net_inflow` |
| 历史边界 | 回看起点 ≥ today−30d（更早被 Cloudflare 拦截 403）；summary 单响应 ≤20 行，跨度 >15 天须拆子请求 |
| 时间语义 | observation=D 00:00:00；release=revision=D+1 00:00:00（T+1 结算） |
| continuity_model | RELEASE_SCHEDULE |

### 4.9 alternative（PUBLIC，Fear & Greed Index）

| 项 | 规格 |
|---|---|
| endpoints | `/fng/`（params：`limit`、`format=json`；请求固定 `metric=fear_greed_index` + `symbol=BTC`） |
| 限流 | 6 req/min（burst=6） |
| 历史 | `limit=0` 单请求取全历史（2018-02-01 起），窗口过滤在客户端完成 |
| 时间语义 | observation=D 00:00:00（源为 unix 秒）；release=revision=D+1 00:00:00（T+1 结算） |
| normalize 过滤 | 同一观测日取 timestamp 最小的一条；当天未冻结行（带 `time_until_update`）跳过等次日重采；value 越界 [0,100] 跳过并计数 |
| canonical | NUMBER，units=`INDEX_POINTS`；entity=`fng_btc` |
| continuity_model | ALWAYS_OPEN |

### 4.10 coinmetrics（PUBLIC，社区层免 key；链上/供给指标）

| 项 | 规格 |
|---|---|
| endpoints | `/v4/timeseries/asset-metrics`（params：`assets`、`metrics`、`frequency`、`start_time`、`end_time`、`page_size`、`paging_from`） |
| key | `COINMETRICS_API_KEY` **可选**：无 key 走社区层公开接口；有 key 自动带请求头 `X-CM-API-KEY` 走付费层（余额分层等社区层受限指标） |
| 限流 | 社区层 20 req/min（burst=20） |
| 分页 | 游标 `paging_from` 推进，上限 50 页 |
| 代理 | 显式读取 `HTTPS_PROXY`/`HTTP_PROXY`（httpx 不自动继承） |
| 时间语义 | T+1 结算：observation=D 00:00:00；release=revision=D+1 00:00:00；fetch 起点回退 1 天（重采边界日）；当天未冻结行跳过 → 重采幂等 |
| 指标 | `SplyCur`（流通量）、`CapMrktCurUSD`（市值）、`AdrBalCnt`（有余额地址数）、`AdrActCnt`（活跃地址数）；资产维度 usdt/usdc/btc |
| 历史边界 | 按资产实际可得日（实测）：btc 2009-01-03、usdt 2014-10-06、usdc 2018-08-03（市值 2018-09-28）；注册脚本的 `available_from` 与之一一对齐 |
| continuity_model | ALWAYS_OPEN |

### 4.11 coinmarketcap（PUBLIC，全市场指标）

| 项 | 规格 |
|---|---|
| endpoints | `/data-api/v3/global-metrics/quotes/historical`（公开端点，免 key） |
| 限流 | 30 req/min（burst=5） |
| 分片 | 单请求 ≤2200 条 → 5 年（1827 天）一片，客户端跨片去重；窗口整体早于 2013-01-01 时不发请求 |
| 指标 | `btcDominance` / `ethDominance`（units=Percent，取自 quotes 直字段）；`totalMarketCap` / `altcoinMarketCap`（units=USD，取自 `quotes[].quotes[].value` 的 USD 报价） |
| 时间语义 | 源为 ISO 8601（`2013-04-29T00:00:00.000Z`）→ observation=D 00:00:00；release=revision=D+1 00:00:00 |
| normalize 过滤 | 市占率类 value ≤0 或 >100 跳过并计数（防脏值） |
| 错误映射 | 429/401/403 先于 body 解析判定（错误页非 JSON） |
| continuity_model | ALWAYS_OPEN |

## 5. 注册表初始化

`registry/service.py` 提供 `bootstrap_defaults()`：将 §4 各源规格写入 source_registry（含 license 摘要、rate_limit_json），幂等（ON CONFLICT UPDATE）。CLI `chronoforge registry sync` 暴露。

当前 `DEFAULT_SOURCES` 共 **11 条**：`binance_spot` / `binance_futures` / `deribit` / `ccxt` / `yahoo` / `fred` / `sec_edgar` / `sosovalue` / `alternative` / `coinmetrics` / `coinmarketcap`。

> **下线源不会自动清理**：`bootstrap_defaults` 是 upsert，已下线源的 `source_registry` 残留行需手工 `DELETE`（如 `livecoinwatch` 清理时即如此处理）。

数据集（dataset_registry）侧的登记不走 `registry sync`，而由各源的注册脚本完成（幂等、含 `available_from` 与窗口预检）——`scripts/register_{sosovalue_etfs,alternative_fng,coinmetrics_metrics,coinmarketcap_dominance}.py`。

## 6. 测试依据（D09 TC-C 组）

每源必备 fixture（`tests/fixtures/{source}/{endpoint}/*.json`，脱敏真实响应）：
- happy path：normalize 输出与手写 canonical 期望逐字段一致（契约测试）
- 错误路径：401/429/429 带 Retry-After/500/422 → 断言错误类型与重试次数
- 分页：2 页 fixture → cursor 推进正确、无重叠无缺口
- deribit：instrument_name 解析全字段；过期合约 → SUSPECT 不 INVALID
- fred：同 observation 两个 vintage → 两条记录共存
- yahoo：split 事件 → 四元组 + adjustment_type 正确
