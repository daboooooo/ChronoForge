ChronoForge 金融数据基础设施

Data Sources & Canonical Data Types Specification v1.0

本文定义 ChronoForge 第一阶段金融数据基础设施的数据源、数据类型、标准化边界、时间语义和数据分层原则。

本文是后续 Coding Agent 开发 Data Connector、Canonical Schema、Raw Storage、Data Quality、Derived Feature 和 Research Layer 的基础技术规范。

⸻

1. 设计目标

ChronoForge 不是一个简单的行情抓取程序，而是一个面向个人金融研究、量化分析和 AI Agent 的金融数据基础设施。

系统需要统一接入：

* Crypto Spot
* Crypto Futures / Perpetual
* Crypto Options
* 股票 / ETF / 指数 / FX
* 宏观经济
* 公司基本面
* Futures Positioning
* Prediction Market
* 新闻与文本
* 金融事件
* 派生指标

不同数据源的数据结构、时间频率、修订机制和语义差异很大。

因此 ChronoForge 不应以“每个数据源一个数据库表”为核心设计，而应采用：

Data Source
    ↓
Connector
    ↓
Raw Data
    ↓
Canonical Data Model
    ↓
Derived Data
    ↓
Features
    ↓
Research / Strategy / AI

核心原则：

Source 是可替换的；Canonical Data Type 是稳定的。

例如 Yahoo Finance 将来被其他股票数据源替换时，下游 OHLCV、指标、回测和 AI 分析代码不应该需要修改。

⸻

2. 总体数据架构

                         DATA SOURCES
                              │
 ┌──────────────┬─────────────┼──────────────┬──────────────┐
 │              │             │              │              │
Market         Macro       Fundamental    Prediction      Text
 │              │             │              │              │
CCXT           FRED          SEC            Polymarket      GDELT
Binance        BLS           EDGAR          │              RSS
Deribit        BEA           XBRL           │              News
Yahoo          Fed                          │
 │              │             │              │              │
 └──────────────┴─────────────┴──────────────┴──────────────┘
                              │
                              ▼
                         RAW LAYER
                              │
                              ▼
                    CANONICAL DATA LAYER
                              │
          ┌───────────────────┼──────────────────┐
          │                   │                  │
        MARKET             MACRO             EVENT
          │                   │                  │
       ticker             number            event
       trade              flow              prediction
       OHLCV              position
       orderbook
       derivatives
       options
       liquidation
          │                   │                  │
          └───────────────────┼──────────────────┘
                              ▼
                       DERIVED LAYER
                              │
                              ▼
                       FEATURE LAYER
                              │
                 ┌────────────┼────────────┐
                 ▼            ▼            ▼
              Research     Backtest       AI

⸻

3. 数据源白名单

第一阶段正式纳入以下数据源。

数据源	类型	主要用途	优先级
CCXT	Exchange abstraction	多交易所统一访问	P0
Binance Spot	Crypto exchange	Spot 行情	P0
Binance Futures	Crypto derivatives	Futures / Perpetual / Funding / OI / Liquidation	P0
Deribit	Crypto derivatives	Options / Futures / IV / Greeks / OI	P0
Yahoo Finance	Traditional market	股票 / ETF / 指数 / FX	P0
FRED	Macro	美国宏观经济	P0
SEC EDGAR	Fundamental	美国上市公司财报 / XBRL / filings	P0
CFTC COT	Positioning	Futures positioning	P1
BLS	Macro	CPI / Employment / Wages 等	P1
BEA	Macro	GDP / PCE / Income / Corporate Profits	P1
Federal Reserve	Macro	Monetary / Banking / Fed data	P1
Polymarket	Prediction Market	市场隐含概率 / 事件预测	P1
GDELT	News / Text	新闻与事件文本	P2
RSS / 官方新闻源	News / Text	新闻 / 公告 / 新闻稿	P2

明确排除：

CoinGecko

第一阶段不作为 ChronoForge 数据源。

⸻

4. Canonical Data Type

ChronoForge 第一阶段定义以下标准数据类型。

4.1 Market

Type	含义
TICKER	某时刻的市场报价快照
TRADE	单笔成交
OHLCV	时间区间聚合 K 线
ORDERBOOK	某时刻订单簿快照或增量
FUNDING	永续合约资金费率
OPEN_INTEREST	未平仓合约数量/价值

⸻

4.2 Derivatives

Type	含义
FUTURE	交割合约
PERPETUAL	永续合约
OPTION	期权合约及其市场数据
IMPLIED_VOLATILITY	隐含波动率
GREEKS	Delta / Gamma / Vega / Theta / Rho 等

⸻

4.3 Liquidation

Type	含义
LIQUIDATION_EVENT	强平/爆仓事件
LIQUIDATION_AGGREGATE	按时间窗口聚合后的爆仓统计

LIQUIDATION_EVENT 必须独立于 TRADE。

原因：

普通成交 != 强平成交

强平事件具有不同的市场语义，应保留原始事件属性。

⸻

4.4 Macro

Type	含义
NUMBER	单值宏观时间序列
FLOW	资金、货币、库存等流量数据
MACRO_EVENT	CPI/FOMC/GDP 等经济事件

宏观数据必须保留：

observation_time
release_time
revision_time

三者不能混为一个 timestamp。

⸻

4.5 Fundamental

Type	含义
FUNDAMENTAL	财务指标 / XBRL facts
FILING	SEC filing 等申报文件
DOCUMENT	原始报告、公告、PDF、HTML 等

⸻

4.6 Positioning

Type	含义
POSITION	某类市场参与者的持仓
POSITION_AGGREGATE	净仓、Long/Short 等聚合结果

例如 CFTC：

Dealer
Asset Manager
Leveraged Money
Other Reportables
Non-Reportables

应作为 participant classification，而不是简单的 long/short 数字。

⸻

4.7 Prediction

Type	含义
PREDICTION_MARKET	预测市场中的事件结果及其市场价格
PREDICTION_PRICE	某 outcome 的市场价格/隐含概率

Polymarket 中 outcome price 可以作为隐含概率，但必须保存原始 price，而不能只保存 probability。

例如：

YES price = 0.72
NO price  = 0.28

Canonical 层保存：

price = 0.72
implied_probability = 0.72

同时保留 source raw value。

⸻

4.8 Text

Type	含义
TEXT_MESSAGE	新闻、公告、社交媒体、文章等
TEXT_EVENT	从文本中识别出的事件
SENTIMENT	从文本派生的情绪指标

SENTIMENT 不属于 Raw Data。

它属于：

Raw Text
    ↓
NLP
    ↓
Derived Sentiment

⸻

4.9 Reference

Type	含义
STRUCTURED	公司、交易所、市场、合约、Token 等 metadata
ENTITY	标准化金融实体
INSTRUMENT	标准化金融工具

⸻

4.10 Derived

Type	含义
DERIVED	从原始数据计算出的指标
FEATURE	面向策略/模型/AI 的特征

例如：

OHLCV
  ↓
Return
Volatility
ATR
RSI
Drawdown
Z-score

这些都不应该作为 Raw 数据源直接存储。

⸻

5. Data Source × Data Type 总表

数据源	TICKER	TRADE	OHLCV	ORDERBOOK	FUNDING	OI	LIQUIDATION	FUTURE	PERPETUAL	OPTION	IV/Greeks	POSITION	EVENT	TEXT	FUNDAMENTAL
CCXT	✓	✓	✓	✓	✓*	✓*	✓*	✓*	✓*	✓*	✓*	—	—	—	—
Binance Spot	✓	✓	✓	✓	—	—	—	—	—	—	—	—	✓	—	—
Binance Futures	✓	✓	✓	✓	✓	✓	✓✓	✓	✓	—	—	△	✓	—	—
Deribit	✓	✓	✓	✓	✓	✓✓	—	✓	✓	✓✓	✓✓	—	✓	—	—
Yahoo Finance	✓	—	✓	—	—	—	—	—	—	△	△	—	△	—	✓
FRED	—	—	—	—	—	—	—	—	—	—	—	—	✓	—	—
SEC EDGAR	—	—	—	—	—	—	—	—	—	—	—	—	✓✓	✓	✓✓
CFTC COT	—	—	—	—	—	✓	—	✓	—	✓*	—	✓✓	✓	—	—
BLS	—	—	—	—	—	—	—	—	—	—	—	—	✓	—	—
BEA	—	—	—	—	—	—	—	—	—	—	—	—	✓	—	✓
Federal Reserve	—	—	—	—	—	—	—	—	—	—	—	—	✓	—	—
Polymarket	✓	✓	△	✓	—	✓	—	—	—	—	—	✓	✓✓	△	—
GDELT	—	—	—	—	—	—	—	—	—	—	—	—	✓	✓✓	—
RSS / News	—	—	—	—	—	—	—	—	—	—	—	—	✓	✓✓	—

说明：

* ✓✓：该数据源的核心能力。
* ✓：明确支持/适合作为该类数据源。
* △：可能存在或可以间接获得，但不是该数据源的核心用途。
* —：不应将其作为该数据源的标准能力。
* *：CCXT 是统一 API 抽象层，具体能力取决于底层交易所，不应假定所有 exchange 都支持该接口。

CCXT 官方文档明确列出了 unified API 的 order book、ticker、OHLCV、trades、funding、open interest、liquidations、Greeks、option chain 等能力，但实际 exchange capability 必须通过 market/exchange capability 检测，而不能静态假定。

⸻

6. CCXT 的定位

CCXT 不应该被视为一个独立交易所。

它应该被定义为：

EXCHANGE ABSTRACTION LAYER

架构：

                   CCXT
                    │
        ┌───────────┼───────────┐
        ↓           ↓           ↓
    Binance      OKX/...     Other
        │           │           │
        └───────────┼───────────┘
                    ↓
             Unified Interface
                    ↓
              Canonical Layer

因此：

source = ccxt
exchange = binance

和：

source = binance

必须能够同时存在。

Direct Binance Connector 用于：

* 交易所特有字段
* 高频 WebSocket
* liquidation stream
* exchange-specific market data
* 无法通过 CCXT unified API 完整表达的数据

CCXT 用于：

* 多交易所统一接口
* 标准化市场发现
* 跨交易所研究
* fallback connector

⸻

7. Binance Spot

Binance Spot 主要用于：

TICKER
TRADE
OHLCV
ORDERBOOK
MARKET_METADATA

典型市场：

BTCUSDT
ETHUSDT
SOLUSDT

Canonical instrument 应包含：

exchange
market_type
symbol
base_asset
quote_asset
instrument_id
status

⸻

8. Binance Futures

Binance Futures 是 ChronoForge 的一级数据源。

建议明确拆分：

Binance
├── Spot
├── USDⓈ-M Futures
└── COIN-M Futures

不要把 Futures 数据和 Spot 混在同一 connector 中。

Binance 官方 Futures API 提供 market data，包括 order book、trades、funding、open interest 等；官方文档也提供 futures market-data endpoints。

⸻

9. Binance Futures Liquidation

爆仓数据必须单独设计：

LIQUIDATION_EVENT

建议字段：

event_id
source
exchange
market_type
symbol
contract_type
event_time
transaction_time
side
order_side
price
quantity
notional
raw_payload
ingest_time

最重要的是：

event_time

必须使用交易所事件时间，而不是本地接收时间。

同时保存：

ingest_time

用于测量：

market_event_latency

⸻

10. Liquidation Aggregate

原始：

LIQUIDATION_EVENT

可以派生：

LIQUIDATION_AGGREGATE

例如：

timestamp
symbol
window
long_liquidation_volume
short_liquidation_volume
total_liquidation_volume
long_liquidation_notional
short_liquidation_notional
total_liquidation_notional
liquidation_imbalance

可进一步计算：

long_short_ratio
liquidation_zscore
liquidation_percentile
market_stress_score

这些属于 DERIVED，不能覆盖 Raw liquidation data。

⸻

11. Deribit

Deribit 是 ChronoForge Crypto Options 数据的核心数据源。

主要保存：

INSTRUMENT
TICKER
TRADE
OHLCV
ORDERBOOK
FUTURE
PERPETUAL
OPTION
OPEN_INTEREST
IMPLIED_VOLATILITY
GREEKS

Deribit API 的 instrument/book summary 数据能够提供期权及其市场摘要；Options 数据应作为独立 canonical type，而不能简单当成 ticker。

⸻

12. OPTION Canonical Model

期权 instrument 必须拆解：

underlying
expiry
strike
option_type
settlement_asset
instrument_id

例如：

BTC-26SEP26-100000-C

应解析为：

underlying = BTC
expiry = 2026-09-26
strike = 100000
option_type = CALL

Option market data：

timestamp
bid
ask
mark_price
index_price
volume
open_interest
mark_iv
bid_iv
ask_iv
delta
gamma
vega
theta
rho

不要只保存一个：

price

因为期权研究的核心价值之一就是 volatility surface 和 Greeks。

⸻

13. Deribit Options Derived Layer

Deribit Raw Data：

OPTION
+
IV
+
GREEKS
+
OI

可以进一步生成：

VOLATILITY_SURFACE

例如：

underlying
timestamp
expiry
days_to_expiry
strike
moneyness
iv
delta
call_iv
put_iv
atm_iv
25d_call_iv
25d_put_iv
25d_rr
25d_butterfly

进一步形成：

TERM_STRUCTURE
VOLATILITY_SKEW
VOLATILITY_SURFACE

这些全部属于 Derived Layer。

⸻

14. FRED

FRED 是 ChronoForge 宏观数据的核心来源之一。

FRED API 支持：

* series
* observations
* categories
* releases
* search
* real-time periods
* vintage dates

因此 FRED connector 不应该只保存：

date
value

而应该至少保存：

series_id
observation_date
value
realtime_start
realtime_end
release_id
source
units
frequency
seasonal_adjustment
retrieved_at

FRED 官方 API 明确支持 realtime_start、realtime_end、vintage_dates 以及 initial-release 等数据访问方式。

这是金融回测中避免 look-ahead bias 的关键。

⸻

15. Macro Time Semantics

宏观数据至少存在三个时间：

observation_time
release_time
revision_time

例如：

GDP Q2

可能：

observation:
2026-Q2
first release:
2026-07-30
revision:
2026-08-28

因此：

observation_date != release_date

任何回测系统不得使用未来 revision 覆盖历史当时可获得的数据。

⸻

16. SEC EDGAR

SEC EDGAR 是美国股票 Fundamental Data 的核心来源。

官方 data.sec.gov API 提供：

Submissions
XBRL Company Facts
XBRL Frames

覆盖包括：

10-K
10-Q
8-K
20-F
40-F
6-K

等文件及其 XBRL 数据。

Canonical Data：

FUNDAMENTAL

建议保存：

entity_id
cik
ticker
concept
taxonomy
unit
value
period_start
period_end
filing_date
accepted_datetime
form
accession_number
frame
source

特别重要：

filing_date
accepted_datetime

不能只保存财务报表的 period_end。

⸻

17. SEC Document Layer

原始文件必须独立保存：

DOCUMENT

包括：

filing
filing_html
filing_xbrl
exhibit
document_text

Canonical Fundamental：

Revenue
EPS
Assets
Debt
FCF

应该可以追溯到：

SEC Filing
    ↓
XBRL Fact
    ↓
Canonical Fundamental

即：

每一个重要 Fundamental Value 都必须具有 provenance。

⸻

18. CFTC COT

CFTC COT 是 positioning 数据源。

它是：

weekly positioning

而不是实时市场数据。

CFTC 的 COT 报告包括 Legacy、Disaggregated、Traders in Financial Futures 等分类，并提供 futures-only 和 futures-and-options combined 数据。官方 Public Reporting Environment 支持 API 和历史数据查询。

Canonical：

POSITION

建议字段：

report_date
week_date
market
contract
participant_type
long
short
spreading
open_interest
long_change
short_change
net_position
net_change
source

不能把 COT：

net_position

和 Binance：

open_interest

视为同一种 OI。

⸻

19. BLS

BLS 主要提供：

CPI
Employment
Unemployment
Wages
PPI
Productivity

等经济时间序列。

BLS Public Data API 支持单序列和多序列查询，并提供 preliminary 等 footnotes。

Canonical：

NUMBER

必须保留：

series_id
period
value
unit
footnote
status

特别是：

preliminary

等状态不能丢失。

⸻

20. BEA

BEA 用于：

GDP
PCE
Personal Income
Corporate Profits
NIPA
Industry Data
Regional Data

等。

BEA 官方 API 提供经济统计数据及其 metadata。

Canonical：

NUMBER
FUNDAMENTAL
EVENT

具体类型根据数据语义决定。

⸻

21. Federal Reserve

Federal Reserve 数据作为独立 source 保留。

主要用于：

Monetary Policy
Balance Sheet
Banking
Interest Rate
Liquidity
Financial Conditions

但如果数据已经通过 FRED 以标准化形式获取，不应重复存储为完全相同的数据。

原则：

Original Source
    ↓
FRED

与：

Federal Reserve Direct

必须通过：

source
source_series_id

进行区分。

⸻

22. Polymarket

Polymarket 是独立的：

PREDICTION MARKET

数据源。

不能归类为：

NEWS

也不能简单归类为：

NUMBER

Polymarket 官方文档目前将 API 能力分成：

Gamma API
CLOB API
Data API

其中：

Gamma

市场和事件：

events
markets
series
tags
sports
teams

CLOB

市场价格和订单簿：

price
prices
book
prices-history
midpoint
spread

Data API

positions
closed positions
activity
value
open interest
holders
trades

因此 ChronoForge 的 Polymarket Connector 不应只有一个 endpoint。

⸻

23. Polymarket Canonical Model

建议：

PREDICTION_MARKET

字段：

market_id
event_id
question
description
outcome_id
outcome
market_status
created_at
start_time
end_time
resolution_time
timestamp
price
implied_probability
volume
open_interest
liquidity
spread
source
source_url
raw_payload

例如：

question:
Will BTC reach $100k?
outcome:
YES
price:
0.72
implied_probability:
0.72

必须同时保存：

price

和：

implied_probability

因为二者在不同数据处理阶段具有不同语义。

⸻

24. Prediction Market 与 Event 的关系

Prediction Market 的核心不是价格，而是：

EVENT
    ↓
PREDICTION MARKET
    ↓
OUTCOME
    ↓
MARKET PRICE
    ↓
IMPLIED PROBABILITY

因此建议：

event_id

成为重要关联字段。

例如：

EVENT
└── Fed decision
       │
       ├── Polymarket probability
       ├── economist forecast
       ├── previous decision
       └── actual decision

这样可以进一步计算：

prediction_surprise
forecast_surprise
event_surprise

⸻

25. GDELT / RSS / News

News 数据必须归入：

TEXT_MESSAGE

而不是：

NUMBER

建议 Raw Text 保存：

message_id
source
author
published_at
discovered_at
title
body
language
url
entities
raw_payload

然后通过 NLP 产生：

SENTIMENT
TOPIC
ENTITY
TEXT_EVENT
EMBEDDING

这些全部进入 Derived Layer。

GDELT DOC 2.0 提供新闻全文检索能力，并支持 JSON 等机器可读输出；其覆盖范围和历史窗口需要按具体 API 当前能力处理，不能假设其提供无限历史全文。

⸻

26. 数据分层

ChronoForge 必须采用三层核心存储模型。

Layer 1 — RAW

保存数据源原始返回。

raw/
├── binance/
├── deribit/
├── polymarket/
├── yahoo/
├── fred/
├── sec/
├── cftc/
└── ...

原则：

Raw 数据不可被 Canonical transformation 覆盖。

⸻

Layer 2 — CANONICAL

统一数据模型：

canonical/
├── ticker/
├── trade/
├── ohlcv/
├── orderbook/
├── funding/
├── open_interest/
├── liquidation/
├── future/
├── perpetual/
├── option/
├── greeks/
├── macro/
├── fundamental/
├── position/
├── prediction/
├── event/
├── text/
└── document/

⸻

Layer 3 — DERIVED

派生指标：

derived/
├── returns/
├── volatility/
├── technical/
├── funding/
├── basis/
├── liquidation/
├── option_surface/
├── positioning/
├── sentiment/
├── prediction/
└── macro/

⸻

27. Feature Layer

Feature 不应污染 Canonical Layer。

例如：

feature/
├── btc/
│   ├── market_state
│   ├── derivatives_state
│   ├── liquidation_state
│   ├── options_state
│   └── macro_state
│
├── equity/
│   └── market_state
│
└── macro/
    └── liquidity_state

一个 Feature 可以依赖多个数据源。

例如：

BTC_MARKET_STRESS
=
BTC volatility
+
funding
+
open interest change
+
liquidation imbalance
+
Deribit IV
+
Polymarket probability

这属于：

FEATURE

而不是任何单一数据源的数据。

⸻

28. 时间模型

所有时间序列必须明确区分：

event_time
observation_time
effective_time
release_time
publication_time
transaction_time
ingest_time
revision_time

不要统一使用：

timestamp

作为唯一时间字段。

⸻

29. Market Data 时间

对于交易所数据：

event_time
transaction_time
ingest_time

例如：

Binance liquidation

应保存：

transaction_time
event_time
ingest_time

这样可以分析：

exchange event
    ↓
network
    ↓
collector

之间的延迟。

⸻

30. Data Provenance

所有 Canonical 数据都必须能够回答：

这个数从哪里来的？

最低要求：

source
source_id
source_timestamp
ingest_timestamp
raw_record_id

对于 SEC：

accession_number

对于 FRED：

series_id
vintage

对于 Binance：

exchange
symbol
market_type

对于 Deribit：

instrument_name

对于 Polymarket：

market_id
event_id
outcome_id

⸻

31. 数据质量要求

每个 Connector 必须实现至少：

schema validation
timestamp validation
duplicate detection
gap detection
null validation
range validation
source availability
rate-limit handling
retry
checkpoint

对于时间序列：

expected interval
actual interval
missing interval

必须能够检测。

⸻

32. 不允许静默修正原始数据

例如：

BTC price = 100000

如果发现异常，不允许直接覆盖为：

BTC price = 10000

必须：

RAW
 ↓
QUALITY FLAG
 ↓
CANONICAL

例如：

quality_status = suspect
quality_reason = price_outlier

⸻

33. Adjustment Policy

不同市场的数据调整规则必须明确。

例如：

股票：
split adjustment
dividend adjustment
Crypto：
通常不进行股票式 corporate-action adjustment
FRED：
revision / vintage
SEC：
filing amendment
CFTC：
report revision
Polymarket：
market resolution

任何 adjustment 都必须保留：

original_value
adjusted_value
adjustment_type
adjustment_timestamp

⸻

34. Symbol 与 Instrument Identity

不能直接把：

BTCUSDT

作为全球唯一资产 ID。

建议：

entity_id
instrument_id
market_id

三级结构。

例如：

Entity:
BTC
Instrument:
BTC Spot
Market:
BINANCE:BTCUSDT

Deribit：

Entity:
BTC
Instrument:
BTC Option 2026-09-26 100000 Call
Market:
DERIBIT:BTC-26SEP26-100000-C

Polymarket：

Event:
BTC reaches 100k
Market:
POLYMARKET:<market_id>
Outcome:
YES

⸻

35. Frequency 分类

统一定义：

TICK
SECOND
MINUTE
HOURLY
DAILY
WEEKLY
MONTHLY
QUARTERLY
ANNUAL
EVENT
IRREGULAR

不要把：

CPI monthly

和：

BTC 1m OHLCV

仅仅当作不同的 interval。

宏观数据还具有：

release schedule
revision schedule

⸻

36. Connector 开发优先级

P0

必须优先完成：

1. Binance Spot
2. Binance Futures
3. Deribit
4. CCXT
5. Yahoo Finance
6. FRED
7. SEC EDGAR

P1

随后完成：

8. CFTC COT
9. BLS
10. BEA
11. Federal Reserve
12. Polymarket

P2

最后：

13. GDELT
14. RSS / News

⸻

37. Connector 接口

每个 Connector 应实现统一接口：

class DataConnector:
    def discover(self):
        ...
    def fetch(self, request):
        ...
    def normalize(self, raw):
        ...
    def validate(self, data):
        ...
    def checkpoint(self):
        ...
    def health(self):
        ...

不同 Connector 可以提供额外能力：

fetch_trades()
fetch_orderbook()
fetch_ohlcv()
fetch_funding()
fetch_open_interest()
fetch_liquidations()
fetch_options()
fetch_greeks()
fetch_events()
fetch_documents()

但公共接口不应强行要求所有 source 实现所有方法。

⸻

38. Capability Discovery

Connector 必须支持 capability discovery。

例如：

connector.capabilities()

返回：

{
  "ticker": true,
  "trade": true,
  "ohlcv": true,
  "orderbook": true,
  "funding": true,
  "open_interest": true,
  "liquidation": true,
  "option": false,
  "greeks": false
}

原因：

不同交易所和数据源的 API 能力并不相同。

Coding Agent 不得根据“交易所通常提供什么”自行假设 endpoint 存在。

⸻

39. API Key / Access Policy

所有数据源分为：

PUBLIC
PUBLIC_WITH_KEY
AUTHENTICATED

第一阶段尽量使用 public market-data endpoints。

例如：

SEC

data.sec.gov 的部分数据 API 不要求 API key，但要求合法 User-Agent，并遵守访问频率和 fair-access 规则。

FRED

FRED API 使用 API key。

BLS

Public Data API 可用于程序化获取时间序列。

CFTC

Public Reporting Environment 提供 API；CFTC 当前说明其公共 API 通常无需 token，但应避免过度访问。

Binance / Deribit / Polymarket

优先使用其公开 market-data API。

⸻

40. 免费数据源不等于无限制数据源

“免费”必须定义为：

在不购买商业数据订阅的情况下，可以通过官方公开 API / public endpoint 获取。

不能理解为：

unlimited
no rate limit
commercial redistribution allowed
historical data unlimited

Coding Agent 必须分别记录：

access_type
authentication
rate_limit
historical_limit
retention_limit
license
redistribution_policy

⸻

41. Source Registry

建议建立：

source_registry

字段：

source_id
source_name
source_type
base_url
api_version
access_type
authentication_type
rate_limit
historical_limit
timezone
license
terms_url
enabled

例如：

binance_futures
deribit
fred
sec_edgar
cftc_cot
polymarket

⸻

42. Dataset Registry

再建立：

dataset_registry

字段：

dataset_id
source_id
canonical_type
entity_type
instrument_type
frequency
available_from
available_to
timezone
update_frequency
revision_supported
raw_storage
canonical_storage

这样以后新增数据源时不需要修改核心数据库结构。

⸻

43. 推荐目录结构

chronoforge/
│
├── connectors/
│   ├── ccxt/
│   ├── binance/
│   │   ├── spot/
│   │   └── futures/
│   ├── deribit/
│   ├── yahoo/
│   ├── fred/
│   ├── sec/
│   ├── cftc/
│   ├── bls/
│   ├── bea/
│   ├── federal_reserve/
│   ├── polymarket/
│   ├── gdelt/
│   └── rss/
│
├── models/
│   ├── market/
│   ├── derivatives/
│   ├── liquidation/
│   ├── macro/
│   ├── fundamental/
│   ├── positioning/
│   ├── prediction/
│   ├── text/
│   └── reference/
│
├── storage/
│   ├── raw/
│   ├── canonical/
│   └── derived/
│
├── features/
│
├── quality/
│
├── registry/
│
└── research/

⸻

44. 第一阶段不要做的事情

Coding Agent 在第一阶段不得擅自增加以下内容：

❌ CoinGecko
❌ 商业付费数据源
❌ 未验证的 scraping source
❌ 不明确授权的数据源
❌ 自行推测的 API endpoint
❌ 未验证的数据字段
❌ 将 derived indicator 当作 raw source
❌ 将不同时间语义的数据强行统一为 timestamp

如果需要增加数据源：

先验证官方 API 文档
→ 验证访问方式
→ 验证数据类型
→ 验证历史范围
→ 验证频率
→ 再加入 source registry

⸻

45. 第一阶段最终目标

第一阶段不是实现所有数据源。

而是建立稳定的数据基础设施：

                   ChronoForge
                       │
          ┌────────────┴────────────┐
          │                         │
       Connector                 Canonical
          │                         │
     ┌────┼────┐              ┌─────┼─────┐
     ↓    ↓    ↓              ↓     ↓     ↓
 Binance Deribit FRED       Market Macro Event
     │    │    │              │     │     │
     └────┴────┴──────────────┴─────┴─────┘
                       │
                       ▼
                     DuckDB
                       │
                       ▼
                    Parquet
                       │
            ┌──────────┴──────────┐
            ↓                     ↓
        Research              AI Agent

最终达到：

新增一个数据源，不应该破坏已有的数据模型。

新增一种 Canonical Data Type，不应该迫使已有 Connector 重写。

Raw 数据永远可追溯到原始来源。

Derived 数据永远可以追溯到 Canonical 数据。

Canonical 数据永远可以追溯到 Raw 数据。

这四条是 ChronoForge 数据层的核心架构原则。

⸻

46. 官方资料与依据

1. CCXT — Unified Exchange API / Market Data / Derivatives API
    官方文档明确列出统一的 ticker、order book、OHLCV、trades、funding、open interest、liquidations、Greeks、option chain 等接口。
2. Binance Developer Documentation — Futures Market Data
    Binance Futures 官方 API 提供 order book、market trades、funding、open interest 等数据；Futures WebSocket 用于实时市场事件。
3. Deribit API Documentation
    Deribit 官方 API 提供期权 instrument、book summary、市场数据及衍生品相关字段。
4. Polymarket Documentation
    官方文档将 API 分为 Gamma API、CLOB API 和 Data API，分别负责 events/markets、prices/order books 和 positions/trades/OI 等数据。
5. FRED API Documentation
    FRED 官方 API 支持 series、observations、release、search、real-time periods 和 vintage data。
6. SEC EDGAR API Documentation
    SEC 官方 API 提供 submissions 和 XBRL Company Facts / Frames 等数据，并明确规定 User-Agent 和 fair-access 要求。
7. CFTC Commitments of Traders
    CFTC 官方 Public Reporting Environment 提供 COT 数据、历史数据和 API 访问。
8. BLS Public Data API
    BLS 官方 API 提供单序列、多序列以及带元数据和计算参数的数据访问。
9. BEA Data API
    BEA 官方 API 提供 GDP、NIPA、Industry、Regional 等经济统计数据及 metadata。
10. GDELT DOC 2.0
    GDELT 官方文档介绍其新闻全文搜索 API 以及机器可读的数据访问能力。

⸻

47. 给 Coding Agent 的最终约束

Coding Agent 在实现 ChronoForge 时必须遵守：

1. 不要以 Data Source 作为核心数据模型。
2. 必须以 Canonical Data Type 作为核心数据模型。
3. Source 必须可以替换。
4. Raw 数据必须保留。
5. Raw → Canonical 必须可重复执行。
6. Canonical → Derived 必须可重复执行。
7. 所有数据必须保存 provenance。
8. 所有时间序列必须明确时间语义。
9. observation_time 与 release_time 不得混淆。
10. revision/vintage 数据不得覆盖历史事实。
11. Liquidation 必须独立于 Trade。
12. Option 必须独立于 Ticker。
13. Prediction Market 必须独立于 News。
14. Position 必须独立于 Open Interest。
15. Sentiment 必须属于 Derived，而不是 Raw。
16. CCXT 是 abstraction layer，不是单一数据源。
17. Binance Spot 与 Binance Futures 必须独立建模。
18. Deribit 必须作为 Crypto Options 一级数据源。
19. Polymarket 必须作为 Prediction Market 一级数据源。
20. CoinGecko 不属于当前数据源白名单。
21. 未验证的 API endpoint 不得实现。
22. 不得根据模型记忆臆测 API 字段。
23. 开发 Connector 前必须核对官方 API 文档。
24. API rate limit、authentication 和历史数据限制必须进入 source metadata。
25. 所有 Connector 必须具备 health、checkpoint、retry、validation 能力。

核心原则最终归纳为：

SOURCE
   ↓
RAW
   ↓
CANONICAL
   ↓
DERIVED
   ↓
FEATURE
   ↓
RESEARCH / STRATEGY / AI

这条数据链必须保持单向、可追溯、可重建。