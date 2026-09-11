Architecture Constraints

Document Type: Architecture Governance / Project Constraints
Primary Audience: System Architect, Coding Agent, Senior Developer
Primary Language: Python
Status: Mandatory
Scope: Entire project

⸻

1. Purpose

本文件定义本项目的架构约束、设计原则、技术选型原则、工程边界和决策规则。

本文件不是具体实现方案，也不是功能需求文档。

它的作用是约束架构师：

* 如何分析问题
* 如何设计系统
* 如何划分模块
* 如何选择技术
* 如何组织数据
* 如何处理不同金融数据源
* 如何保证数据质量
* 如何保证研究结果可重复
* 如何避免隐含的设计假设
* 如何评估架构演进

所有重大架构设计必须符合本文件。

⸻

2. Fundamental Principle

2.1 Evidence-Based Architecture

任何架构设计都必须有明确依据。

禁止根据：

* 随机猜测
* 个人偏好
* “通常大家都这么做”
* LLM 的概率性推测
* 未验证的经验
* 当前目录中已有代码的偶然结构

直接决定架构。

架构决策必须至少基于以下一种依据：

1. 官方技术文档
2. 成熟开源项目的架构
3. 行业标准
4. 经典软件工程模式
5. 数据库 / 分布式系统 / 数据工程的成熟实践
6. 金融数据平台的成熟设计
7. 明确的性能、可靠性或维护性需求
8. 可验证的实验结果
9. 明确的业务约束

⸻

2.2 Architecture Decision Record

任何重要设计都必须能够回答：

为什么这样设计？
有哪些候选方案？
为什么没有选择其他方案？
这个设计解决什么问题？
它引入什么成本？
未来如何替换？

重大设计不得只有：

"我们决定使用 X。"

必须能够说明：

Problem
→ Constraints
→ Alternatives
→ Evidence
→ Decision
→ Trade-offs
→ Consequences

⸻

3. Existing Code Is NOT an Architecture Reference

3.1 Absolute Rule

不要参考当前项目目录中的现有代码来决定目标架构。

当前目录中的代码可能：

* 不完整
* 过时
* 存在历史包袱
* 是实验代码
* 是错误设计
* 是临时代码
* 与最终架构目标不同

因此：

Existing Code ≠ Architecture Specification

⸻

3.2 Initial Architecture Phase

在架构设计阶段：

禁止：

* 根据现有文件结构推导模块边界
* 根据现有 class 推导领域模型
* 根据已有 API 推导数据模型
* 因为“代码已经这么写了”而保留错误架构
* 为兼容现有实现而牺牲目标架构

如果需要了解当前代码，只能在明确进入：

Implementation / Migration / Compatibility Analysis

阶段之后进行。

⸻

4. Primary Programming Language

4.1 Python First

本项目主要编程语言：

Python

默认使用 Python 实现：

* 数据采集
* 数据清洗
* 数据标准化
* 数据验证
* 数据存储接口
* 数据查询
* 数据分析
* 因子计算
* 特征工程
* 研究工具
* 回测相关组件
* CLI
* 调度任务
* 测试
* 数据质量检查
* 元数据管理

⸻

4.2 Secondary Languages

只有在存在明确工程理由时，才允许引入其他语言。

例如：

* 极端性能关键路径
* 特定数据库扩展
* GPU kernel
* 系统级组件
* 已有成熟 native library
* 第三方系统强制要求

任何非 Python 核心组件必须说明：

Why Python is insufficient
Why the alternative is justified
Interface boundary
Operational cost
Maintenance cost
Fallback strategy

⸻

5. Architecture Style

系统必须采用：

Modular + Layered + Domain-Oriented Architecture

禁止建立：

Monolithic Script Architecture

避免：

download.py
    ↓
clean.py
    ↓
analysis.py
    ↓
strategy.py

这种依赖链逐渐演化为无法维护的脚本系统。

推荐形成清晰的层次：

Data Sources
     ↓
Ingestion
     ↓
Raw Data
     ↓
Normalization
     ↓
Canonical Data Model
     ↓
Data Quality
     ↓
Storage / Query
     ↓
Research / Analytics
     ↓
Features / Factors
     ↓
Backtesting
     ↓
Reporting / Applications

⸻

6. Layering Rules

推荐至少区分以下层：

┌───────────────────────────────┐
│ Applications / CLI / Research │
├───────────────────────────────┤
│ Analytics / Factors / Signals │
├───────────────────────────────┤
│ Query / Domain Services       │
├───────────────────────────────┤
│ Canonical Data Model          │
├───────────────────────────────┤
│ Data Quality / Validation     │
├───────────────────────────────┤
│ Normalization / Transformation│
├───────────────────────────────┤
│ Ingestion / Connectors        │
├───────────────────────────────┤
│ Storage / Metadata            │
├───────────────────────────────┤
│ External Data Sources         │
└───────────────────────────────┘

实际项目可以根据需求调整层数。

但是必须保持：

高层依赖抽象，而不是依赖具体数据源。

⸻

7. Dependency Direction

依赖方向必须尽量单向：

Application
    ↓
Domain / Analytics
    ↓
Canonical Data Model
    ↓
Infrastructure Abstraction
    ↓
Concrete Data Provider

禁止：

Domain
  ↓
Binance API

或：

Factor Calculation
  ↓
Specific CSV File

业务逻辑不得直接绑定具体数据提供商。

⸻

8. Data Source Abstraction

所有外部金融数据源必须通过统一的数据源抽象接入。

例如：

DataProvider
├── Binance
├── Deribit
├── Polymarket
├── FRED
├── Equity Provider
├── Macro Provider
└── Other Providers

具体 Provider 负责：

* API authentication
* API request
* pagination
* rate limiting
* retry
* error handling
* source-specific transformation
* source metadata

Provider 不负责：

* 投资逻辑
* 因子计算
* 策略逻辑
* 高层业务分析

⸻

9. Canonical Data Model

不同数据源的数据不得直接混合。

必须区分：

Source Schema
      ↓
Source Adapter
      ↓
Canonical Schema

例如：

Binance OHLCV
Deribit OHLCV
Other Exchange OHLCV
        ↓
     Bar Model

Canonical Model 应表达金融领域语义，而不是某个 API 的字段结构。

⸻

10. Financial Data Must Be Time-Aware

金融数据系统最重要的约束之一是：

时间语义必须明确。

所有时间序列数据必须明确区分：

event_time
ingest_time
available_time
effective_time

具体项目不一定全部使用，但如果这些概念可能影响研究结果，则必须显式建模。

⸻

11. No Look-Ahead Bias

研究和回测系统必须避免：

Look-Ahead Bias

任何数据只能在其：

available_time

之后被研究逻辑使用。

禁止：

Future Data
    ↓
Historical Signal

允许：

Data Available at T
        ↓
Signal at T or T+1

研究系统必须明确：

event time
knowledge time
decision time
execution time

金融数据的时间语义应参考成熟量化平台对于 streaming/event-based analysis 的设计，而不是简单地把整个 DataFrame 当作无时间边界的数据集合。LEAN 的设计明确将历史分析建模为时间流，以减少 look-ahead bias。(量子连接)

⸻

12. Raw Data Must Be Preserved

原则：

Raw data should be immutable.

原始数据不得直接覆盖。

推荐：

Raw
 ↓
Normalized
 ↓
Validated
 ↓
Derived

而不是：

Raw
 ↓
Clean in place
 ↓
Overwrite

必须能够追溯：

Derived Data
    ↓
Transformation
    ↓
Normalized Data
    ↓
Raw Source

⸻

13. Data Lineage

重要数据必须能够回答：

Where did this data come from?
When was it retrieved?
Which provider?
Which endpoint?
Which version?
Which transformation?
Which normalization?
Which code version?

因此建议设计：

Data
+
Metadata
+
Lineage
+
Version

而不是只保存一个最终 DataFrame。

⸻

14. Data Versioning

对于可能影响研究结果的数据，必须考虑版本化。

例如：

Dataset
Dataset Version
Schema Version
Provider Version
Transformation Version

如果同一历史数据发生变化，系统必须能够区分：

Dataset v1
Dataset v2

而不是静默覆盖。

⸻

15. Storage Architecture

对于大量历史时间序列数据，优先考虑：

Parquet + DuckDB

而不是默认使用传统 OLTP 数据库存储全部历史分析数据。

推荐：

Parquet
   ↓
DuckDB
   ↓
Python / Pandas / Polars
   ↓
Research / Analytics

Parquet 的列式存储适合分析型读取，并允许通过 Row Group / Column Chunk 等机制减少不必要的数据读取；DuckDB 则适合直接对本地分析数据进行 SQL 查询。(Apache Arrow)

但：

不得因为“Parquet + DuckDB 很流行”而机械采用。

必须根据：

* 数据规模
* 查询模式
* 写入模式
* 并发
* 更新频率
* 数据生命周期
* 部署复杂度

做技术决策。

⸻

16. OLAP vs OLTP

必须明确区分：

OLTP

适合：

* 元数据
* 配置
* 用户信息
* 任务状态
* 事务状态

OLAP

适合：

* OHLCV
* Tick
* Order Book
* Funding Rate
* Options
* Macro Time Series
* Large Historical Dataset

不要因为某个数据库“万能”而把所有数据放进去。

⸻

17. Data Format Rules

默认优先：

Parquet
Arrow

用于分析型时间序列数据。

文本格式：

CSV
JSON

主要用于：

* 外部交换
* API 原始响应
* 小型配置
* 调试

不应该默认将 CSV 作为核心长期分析存储格式。

⸻

18. Schema First

任何重要数据集必须首先定义：

Schema

然后再实现：

Ingestion
Transformation
Storage
Analytics

禁止：

先抓数据
↓
看到字段以后再决定模型

应当：

Domain Requirement
↓
Canonical Schema
↓
Source Mapping
↓
Implementation

⸻

19. Data Quality Is a First-Class Component

数据质量不是下载完成后的附加检查。

必须作为独立架构能力。

至少考虑：

Completeness
Uniqueness
Validity
Consistency
Timeliness
Continuity
Range
Precision
Temporal Integrity

金融时间序列还必须关注：

duplicate timestamps
missing intervals
out-of-order events
invalid OHLC relationships
abnormal price jumps
volume anomalies
timezone errors
symbol changes
contract changes
corporate actions

⸻

20. Financial Instrument Identity

不得简单使用 ticker string 作为唯一金融资产身份。

例如：

BTCUSDT
BTC-PERP
BTC-20261225
AAPL
AAPL.US

必须考虑：

Asset
Venue
Instrument Type
Contract
Currency
Expiration
Underlying
Settlement

必要时建立：

Instrument
Instrument Identity
Instrument Mapping

⸻

21. Corporate Actions and Contract Changes

对于股票、期货、期权等数据：

不得假设：

symbol == permanent identity

必须考虑：

* Split
* Dividend
* Delisting
* Merger
* Symbol change
* Futures rollover
* Contract expiration
* Option expiration
* Settlement changes

成熟量化系统通常将数据标准化、公司行为和合约连续性作为核心数据问题，而不是简单的价格清洗。LEAN 的数据模型就是一个可参考的成熟案例。(虚拟交易引擎 - QuantConnect.com)

⸻

22. Research / Backtest Separation

研究环境和生产/回测环境必须保持明确边界。

推荐：

Research
    ↓
Hypothesis
    ↓
Statistical Validation
    ↓
Backtest
    ↓
Out-of-Sample Validation
    ↓
Production

不得：

看到历史收益很好
        ↓
直接进入生产

成熟量化研究流程通常先在研究环境验证假设，再进入回测；研究和回测的数据访问方式也应该尽量保持一致。(量子连接)

⸻

23. Analytics Must Be Reproducible

任何重要分析必须能够重新运行。

应尽可能记录：

Dataset Version
Query
Parameters
Code Version
Environment
Timestamp
Output

目标：

Same Input + Same Code + Same Parameters → Same Result

如果无法保证完全确定性，必须明确记录：

Source of Non-determinism

⸻

24. No Hidden Global State

禁止核心模块依赖：

* global mutable variables
* implicit environment state
* hidden singleton
* working-directory assumptions
* undeclared configuration

配置必须显式传递。

⸻

25. Configuration

配置与代码分离。

推荐：

config/
├── default
├── development
├── test
└── production

敏感信息不得写入：

source code
Git
configuration committed to repository
logs

使用：

Environment Variables
Secret Store
External Credential Provider

⸻

26. API Client Design

每个外部 API Client 应负责：

Authentication
Connection
Request
Retry
Rate Limit
Pagination
Response Validation
Error Mapping

不得让上层业务代码直接处理：

HTTP response
HTTP status
JSON parsing
API pagination
rate-limit details

⸻

27. Retry Policy

禁止无条件：

while True:
    retry()

Retry 必须：

* 有上限
* 有 backoff
* 区分错误类型
* 区分 transient / permanent failure
* 可观察
* 可配置

对于：

401
403
invalid request
schema mismatch

通常不得无限重试。

⸻

28. Rate Limiting

金融 API 经常存在：

request limits
weight limits
burst limits
IP limits
account limits

Rate limiting 必须作为 Provider / Infrastructure 层能力，而不是散落在业务代码中。

⸻

29. Error Handling

错误必须分层。

至少区分：

Transport Error
Authentication Error
Authorization Error
Rate Limit Error
Provider Error
Schema Error
Data Quality Error
Storage Error
Domain Error
Configuration Error

禁止：

except Exception:
    pass

或：

except Exception:
    return None

除非有明确的、记录充分的容错策略。

⸻

30. Observability

重要任务必须能够回答：

What happened?
When?
Why?
How long?
How many records?
Which source?
Which dataset?
Which version?
What failed?

至少考虑：

Structured Logging
Metrics
Task Status
Error Reporting
Data Quality Reports

⸻

31. Idempotency

数据采集和转换任务应尽可能支持：

Idempotent Execution

重复运行：

same input
+
same parameters

不应该产生：

duplicate records
corrupted state
inconsistent output

尤其适用于：

* Historical Downloads
* Incremental Updates
* ETL
* Data Normalization
* Feature Generation

⸻

32. Incremental Processing

对于大型数据集，禁止默认：

Load Everything Into RAM

优先考虑：

Partition
Chunk
Stream
Incremental Update
Predicate Pushdown
Column Projection

具体实现根据数据规模决定。

⸻

33. Performance Principles

性能优化必须建立在 profiling / measurement 上。

禁止：

“我觉得这个算法更快。”

必须尽可能提供：

Baseline
Measurement
Bottleneck
Optimization
Measurement After Optimization

优化顺序：

Correctness
↓
Profiling
↓
Algorithm
↓
I/O
↓
Memory
↓
Concurrency
↓
Micro-optimization

⸻

34. Concurrency

不要默认使用：

threads
asyncio
multiprocessing
distributed computing

必须根据 workload 判断。

例如：

Network-bound → async / concurrent I/O
CPU-bound → multiprocessing / vectorization / native code
Analytical SQL → DuckDB
Large distributed workload → distributed engine

⸻

35. Pandas / Polars / DuckDB

不得规定“所有数据必须使用某一个框架”。

根据任务选择：

Pandas

适合：

* 中小规模研究
* notebook
* 传统金融分析
* 广泛生态兼容

Polars

适合：

* 高性能 dataframe processing
* columnar workloads
* parallel transformations

DuckDB

适合：

* analytical SQL
* Parquet querying
* large local datasets
* joins / aggregations

技术选择必须有依据。

⸻

36. Module Design

模块必须：

Single Responsibility

一个模块不应该同时负责：

API
Storage
Transformation
Analytics
Reporting

推荐：

provider/
ingestion/
normalization/
quality/
storage/
query/
analytics/
features/
backtest/
reporting/
cli/

具体目录名称可根据最终架构调整。

⸻

37. Interface Before Implementation

重要模块应先定义：

Interface
Input
Output
Error
Contract

再实现具体 Provider。

例如：

class MarketDataProvider(Protocol):
    ...

而不是先写：

BinanceClient

再倒推整个系统接口。

⸻

38. Plugin Architecture

对于可扩展组件，优先考虑 Plugin / Adapter 模式。

例如：

DataProvider
├── BinanceProvider
├── DeribitProvider
├── FREDProvider
└── PolymarketProvider

核心系统不应该因为增加一个 Provider 而修改大量已有业务逻辑。

目标：

Open for Extension, Closed for Modification

⸻

39. Testing Strategy

必须采用分层测试。

Unit Tests
    ↓
Integration Tests
    ↓
Data Quality Tests
    ↓
End-to-End Tests

关键领域逻辑必须有 deterministic tests。

金融数据系统特别需要测试：

timezone
timestamp
sorting
duplicates
missing data
corporate actions
contract rollover
normalization
look-ahead prevention

⸻

40. Architecture Testing

必须测试架构约束本身。

例如：

domain must not import provider
provider must not import strategy
storage must not import analytics

可以通过：

* import rules
* dependency checks
* static analysis
* architecture tests

保证架构不会随着项目增长逐渐腐化。

⸻

41. Documentation

重要模块必须包含：

Purpose
Responsibilities
Inputs
Outputs
Dependencies
Invariants
Failure Modes
Examples

架构决策必须记录：

ADR

⸻

42. Architecture Decision Record

重大架构决策建议采用：

ADR-XXXX-title.md

标准结构：

# ADR-XXXX: Title
## Context
## Problem
## Constraints
## Alternatives
### Alternative A
### Alternative B
### Alternative C
## Decision
## Evidence
## Trade-offs
## Consequences
## Rejected Alternatives
## Future Reconsideration

⸻

43. Technology Selection Rule

选择技术时必须至少比较：

Correctness
Maturity
Community
Performance
Operational Complexity
Python Integration
Data Compatibility
Maintenance Cost
Lock-in
Extensibility

不要因为：

latest
popular
fast
AI recommended

而采用技术。

⸻

44. Prefer Boring Technology

核心基础设施优先选择：

成熟、稳定、可维护、可替换的技术。

除非有明确理由，否则不要为了“先进”而引入：

* 新数据库
* 新消息系统
* 新分布式框架
* 新编程语言
* 新编排系统

⸻

45. Avoid Premature Distributed Architecture

默认优先：

Single-node modular architecture

只有当实际需求证明单机架构无法满足：

* 数据规模
* 计算规模
* 并发
* 可用性
* 可靠性

时，才考虑：

Distributed Processing
Message Queue
Distributed Database
Microservices
Kubernetes

不要因为“金融平台最终应该分布式”而提前复杂化。

⸻

46. Local-First Architecture

本项目优先支持：

Local-first

核心研究和数据分析能力不得强依赖：

Cloud SaaS
External Notebook
Vendor-specific platform

外部服务可以作为：

Data Source
Optional Service
Deployment Target

但核心数据模型和分析逻辑应保持独立。

⸻

47. Vendor Neutrality

核心 Domain 不得绑定：

Binance
Deribit
FRED
Polymarket
Specific Cloud
Specific Database

Vendor-specific logic 必须隔离在：

Adapter / Connector / Infrastructure

⸻

48. Financial Data Source Independence

不同数据源的数据质量和语义可能不同。

不得默认：

Provider A == Provider B

必须明确：

Source
Coverage
Latency
Granularity
Definition
Timezone
Adjustment
Reliability

⸻

49. Data Semantics Before Data Volume

系统设计优先级：

Correct Semantics
        ↓
Correct Schema
        ↓
Correct Time Model
        ↓
Correct Data Quality
        ↓
Correct Storage
        ↓
Performance
        ↓
Scale

不要：

先收集大量数据，再考虑数据是什么意思。

⸻

50. No Silent Data Transformation

任何可能影响金融分析结果的转换必须明确记录。

例如：

timezone conversion
resampling
forward fill
backward fill
price adjustment
contract rollover
missing value handling
outlier removal
deduplication

禁止静默执行：

df.fillna(...)
df.drop_duplicates(...)

而不说明原因和规则。

⸻

51. Financial Calculation Correctness

所有金融计算必须优先保证：

Precision
Time Alignment
Units
Currency
Scaling
Rounding
Sign Convention

例如：

0.01 BTC
1000000 USDT
1 contract
1 share
1 option

不能仅依靠变量名称判断单位。

必要时明确：

unit
currency
contract_multiplier
tick_size
lot_size

⸻

52. Research Reproducibility

任何研究结果必须尽量可以回答：

What data?
Which version?
What period?
What query?
What parameters?
What transformations?
What code?
What environment?

否则结果只能视为：

exploratory result

而不是：

reproducible research result

⸻

53. Backtesting Integrity

回测系统必须明确：

Data Timestamp
Signal Timestamp
Order Timestamp
Execution Timestamp
Settlement Timestamp

必须避免：

future information
survivorship bias
selection bias
data snooping
incorrect corporate actions
incorrect contract rollover
unrealistic execution

⸻

54. Research vs Production Data

研究数据和生产数据不得无条件混用。

至少需要能够区分：

Historical
Realtime
Delayed
Simulated
Derived
Synthetic

数据状态必须可追踪。

⸻

55. Configuration and Environment Reproducibility

项目必须尽量能够通过：

configuration
environment specification
dependency lock

恢复运行环境。

推荐：

pyproject.toml
lock file
.env.example

敏感 .env 不得提交。

⸻

56. Python Engineering Standards

推荐使用现代 Python 工程实践：

pyproject.toml
type hints
dataclasses / Pydantic where appropriate
Protocol / ABC where appropriate
pytest
ruff
mypy / pyright where justified
pre-commit where useful

但：

工具不是强制堆砌。

每个工具都必须有工程价值。

⸻

57. Type Safety

核心领域模型应尽可能使用：

type hints

重要数据对象应避免完全依赖：

dict[str, Any]

核心 Domain Model 应尽可能表达：

what the object means
what fields are required
what invariants exist

⸻

58. Dependency Management

禁止：

random pip install

所有生产依赖必须：

* 明确声明
* 可重复安装
* 有版本策略
* 评估安全性
* 评估维护状态

⸻

59. Security

必须遵循：

Least Privilege
Credential Isolation
Secret Redaction
Input Validation
Dependency Security
Network Security

API credentials：

绝对不能进入 Git、日志、异常信息或研究结果。

⸻

60. Secrets

禁止：

API_KEY = "..."

禁止：

print(headers)
print(environment)
print(api_response)

如果可能包含 credential。

⸻

61. Logging

日志不得包含：

API keys
Access tokens
Passwords
Private credentials
Signed URLs
Session tokens

错误日志也必须遵循相同规则。

⸻

62. CLI Design

CLI 应作为 Application Layer。

CLI 不应该包含：

核心业务逻辑
数据模型
Provider implementation
复杂 SQL

CLI 负责：

parse arguments
load configuration
invoke application service
render result

⸻

63. Notebook Rule

Notebook 可以用于：

Exploration
Visualization
Hypothesis Testing
Research

但：

Notebook 不应该成为核心生产逻辑的唯一实现。

成熟研究环境也通常允许研究代码复用项目中的正式代码，而不是让 notebook 成为孤立代码库。(量子连接)

⸻

64. Visualization

Visualization 层不得修改核心数据。

应采用：

Data
 ↓
Analytics
 ↓
Visualization

而不是：

Plot Function
 ↓
modify DataFrame
 ↓
Analysis

⸻

65. API Stability

核心接口一旦被多个模块依赖：

不得随意修改。

Breaking Change 必须：

document
version
migration
test

⸻

66. Backward Compatibility

数据 schema 发生变化时必须考虑：

Schema Version
Migration
Compatibility
Deprecation

禁止静默改变字段语义。

⸻

67. Observability of Data Pipelines

每次重要 pipeline 执行建议记录：

run_id
source
dataset
start_time
end_time
input_count
output_count
error_count
warning_count
schema_version
code_version
status

⸻

68. Pipeline State

数据任务必须能够区分：

PENDING
RUNNING
SUCCESS
PARTIAL_SUCCESS
FAILED
CANCELLED

不得简单使用：

True / False

表达复杂 pipeline 状态。

⸻

69. Failure Recovery

重要数据任务必须考虑：

retry
resume
checkpoint
idempotency
partial failure
reconciliation

但不得通过无限 retry 掩盖真正的系统故障。

⸻

70. Architecture Simplicity

架构必须遵循：

最小充分复杂度。

不要设计：

future-proof architecture

而应该设计：

current requirements
+
known foreseeable requirements
+
clear extension points

⸻

71. No Speculative Abstraction

禁止为了“以后可能会用到”而创建大量抽象。

例如：

BaseProviderFactoryManager
AbstractDataEngine
UniversalFinancialRepository
GenericStrategyFramework

如果没有真实需求和明确边界，不得创建。

⸻

72. No Speculative Microservices

默认：

Modular Monolith

优先于：

Microservices

只有当存在明确的：

independent deployment
independent scaling
failure isolation
organizational boundary

需求时才考虑服务拆分。

⸻

73. Architecture Review Gate

每个重大架构设计必须通过以下检查：

[ ] Problem clearly defined
[ ] Constraints identified
[ ] Alternatives considered
[ ] Evidence collected
[ ] Decision documented
[ ] Trade-offs documented
[ ] Dependency direction valid
[ ] Data semantics defined
[ ] Failure modes considered
[ ] Security considered
[ ] Test strategy defined
[ ] Migration strategy defined if needed

⸻

74. Architecture Anti-Patterns

禁止以下架构：

God Module

一个模块负责整个系统。

God DataFrame

所有数据都塞进一个巨大 DataFrame。

Provider Leakage

业务逻辑直接调用某个 Provider API。

Hidden Transformation

数据在没有记录的情况下被自动修改。

Global State

模块依赖不可见的全局状态。

Script Chain

通过大量 shell/python scripts 串联核心业务。

Magic Configuration

配置隐藏在代码内部。

Silent Failure

错误被吞掉。

Infinite Retry

无限重试。

Premature Microservices

没有需求就拆微服务。

Speculative Abstraction

没有实际需求就建立复杂抽象。

Vendor Lock-in

Domain Logic 依赖具体供应商。

Notebook Production

生产逻辑只存在于 notebook。

⸻

75. Architecture Decision Hierarchy

发生设计冲突时，按照以下优先级处理：

1. Correctness
2. Data Integrity
3. Security
4. Reproducibility
5. Maintainability
6. Testability
7. Observability
8. Performance
9. Scalability
10. Convenience

不得为了：

performance

牺牲：

correctness
data integrity
reproducibility

除非经过明确的 Architecture Decision。

⸻

76. Evidence Hierarchy

架构证据优先级：

1. Official Documentation
2. Standards / Specifications
3. Mature Open-Source Implementations
4. Peer-reviewed / authoritative technical literature
5. Established industry architecture
6. Reproducible benchmark
7. Expert engineering judgment
8. Personal preference
9. LLM-generated assumption

特别注意：

LLM 的默认回答不能作为架构依据。

⸻

77. Mandatory Design Evidence

如果架构师提出：

Database X
Framework Y
Storage Z
Architecture Pattern A

必须说明至少：

Decision
Reason
Evidence
Trade-off
Alternative

如果没有足够证据：

Decision Status = PROVISIONAL

不得伪装成最终架构。

⸻

78. Architecture Confidence

重大架构设计应标记：

CONFIDENCE:
HIGH
MEDIUM
LOW

其中：

HIGH

有明确官方文档、成熟案例或充分实验支持。

MEDIUM

有成熟模式支持，但项目-specific evidence 不完整。

LOW

主要依赖推断或尚未验证。

⸻

79. Unknown Is Not False

如果信息不足：

UNKNOWN

而不是：

FALSE

例如：

Provider supports feature X

如果没有证据：

UNKNOWN

而不是：

NO

⸻

80. Architecture Must Be Reversible Where Practical

优先选择可替换的架构边界。

例如：

Provider Interface
Storage Interface
Query Interface
Execution Interface

使：

Provider A

未来可以替换为：

Provider B

而不需要重写 Domain。

⸻

81. Final Architecture Principle

最终架构应该满足：

Simple
Modular
Layered
Observable
Testable
Reproducible
Evidence-Based
Vendor-Neutral
Data-Semantic-Aware
Time-Aware
Extensible
Locally Operable

但：

不要为了满足这些形容词而人为增加复杂度。

⸻

82. Mandatory Architect Output

在正式进入 Coding Phase 之前，架构师必须输出：

82.1 Architecture Overview

System Context
Component Diagram
Data Flow
Dependency Flow

82.2 Module Boundaries

每个模块说明：

Purpose
Responsibilities
Inputs
Outputs
Dependencies
Public Interfaces

82.3 Data Model

必须说明：

Canonical Entities
Identifiers
Time Semantics
Schema
Relationships
Versioning

82.4 Storage Design

说明：

Raw Storage
Normalized Storage
Derived Storage
Metadata
Indexes / Partitioning
Retention

82.5 Data Pipeline

说明：

Source
Ingestion
Validation
Normalization
Storage
Query
Analytics

82.6 Technology Decisions

每项重大技术必须说明：

Decision
Alternatives
Evidence
Trade-offs
Confidence

82.7 Testing Architecture

说明：

Unit
Integration
Data Quality
End-to-End
Architecture Tests

82.8 Failure Model

说明：

Failure Classes
Retry
Recovery
Partial Failure
Observability

82.9 Security Model

说明：

Credentials
Permissions
Secrets
Network
Logging

82.10 Evolution Strategy

说明：

How modules can evolve
How providers can be replaced
How schemas can migrate
How storage can evolve
How the architecture scales

⸻

83. Architect Must Not Start Coding Immediately

架构设计阶段禁止直接进入大规模 Coding。

正确顺序：

Requirements
      ↓
Constraints
      ↓
Domain Model
      ↓
Architecture
      ↓
Alternatives
      ↓
Evidence
      ↓
Architecture Decisions
      ↓
Interfaces
      ↓
Data Model
      ↓
Test Strategy
      ↓
Implementation Plan
      ↓
Coding

⸻

84. Final Constraint

本项目不是：

“让 AI 猜一个看起来合理的金融数据系统。”

而是：

建立一个有明确领域模型、数据语义、时间语义、模块边界、工程依据和可验证设计依据的金融数据分析基础设施。

任何无法回答：

Why?
Based on what?
What are the alternatives?
What are the trade-offs?
How can we verify it?

的重大设计：

不得直接进入最终架构。

⸻

85. Architecture Freeze Rule

一旦架构设计完成并进入 Implementation Phase：

重大架构变更必须重新经过：

Problem
→ Evidence
→ Alternatives
→ Decision
→ Impact Analysis
→ Migration Plan
→ Test Plan
→ Approval

不得在 Coding Phase 中通过“顺手重构”的方式改变架构。

⸻

86. Architect Final Checklist

在提交架构方案之前，必须确认：

[ ] 没有参考当前目录代码来决定目标架构
[ ] Python 是主要实现语言
[ ] 模块边界明确
[ ] Layer boundaries 明确
[ ] Dependency direction 明确
[ ] Data source abstraction 明确
[ ] Canonical data model 明确
[ ] Time semantics 明确
[ ] Look-ahead bias 已考虑
[ ] Raw data 与 derived data 分离
[ ] Data lineage 已考虑
[ ] Data versioning 已考虑
[ ] Data quality 是独立能力
[ ] Instrument identity 已考虑
[ ] Corporate actions / contract changes 已考虑
[ ] Storage architecture 有依据
[ ] OLTP / OLAP 边界明确
[ ] Research / Backtest 边界明确
[ ] Reproducibility 已考虑
[ ] Idempotency 已考虑
[ ] Failure model 已考虑
[ ] Retry policy 已考虑
[ ] Observability 已考虑
[ ] Security 已考虑
[ ] Testing architecture 已定义
[ ] Architecture tests 已考虑
[ ] Technology decisions 有证据
[ ] Alternatives 已分析
[ ] Trade-offs 已记录
[ ] 没有 speculative abstraction
[ ] 没有 premature microservices
[ ] 没有 vendor lock-in
[ ] 没有未经证实的架构假设

⸻

87. Governing Rule

当本文件与个人偏好冲突时：

遵守本文件。

当本文件与实际需求冲突时：

明确指出冲突，不得默默违反约束。

当本文件不足以决定设计时：

收集证据并建立 Architecture Decision。

当没有足够证据时：

标记 UNKNOWN / PROVISIONAL，而不是猜测。

当多个方案都合理时：

比较 Alternatives 和 Trade-offs，而不是随机选择。

⸻

Final Principle

Architecture is a set of justified decisions, not a collection of guesses.

先理解问题，再建立模型；先建立边界，再选择技术；先验证假设，再实现系统。
