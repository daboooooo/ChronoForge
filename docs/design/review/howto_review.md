多源金融数据系统设计文档审计要求

1. 审计目标

本审计用于判断一个复杂、多源、长期运行的金融数据系统设计文档，是否已经达到可直接进入并行工程实施阶段的标准。

设计文档必须不仅能够说明“系统应该做什么”，还必须能够使多个 Coding Agent 在低耦合、低歧义、可独立验证的条件下分别完成指定模块。

审计的最终目标是确认：

1. 每个模块具有明确、稳定、可执行的职责边界。
2. 每个模块可以被独立分配给 Coding Agent 实施。
3. Coding Agent 无需自行猜测关键架构决策。
4. 每个模块具有明确输入、输出、依赖、异常和状态定义。
5. 每个模块具有完整的功能测试与边界测试要求。
6. 模块可以在不依赖其他未完成模块的情况下进行单元测试和契约测试。
7. 多模块最终能够通过明确的接口契约完成集成。
8. 数据获取、处理、存储链路能够支持增量获取、断点续传、重复执行、失败恢复和数据校验。
9. 系统能够检测并防止金融时间序列出现静默缺失、重复、错位、篡改或错误覆盖。
10. 所有重要设计决策均具有明确依据，而不是基于“看起来合理”的随机设计。

⸻

2. 审计原则

设计文档必须遵循以下原则。

2.1 Design Before Code

任何重要模块在进入 Coding Agent 实施阶段之前，必须已经明确：

* 模块职责
* 数据模型
* API / 接口
* 状态机
* 错误处理
* 数据一致性规则
* 并发模型
* 持久化规则
* 测试要求

如果 Coding Agent 需要自行决定上述事项，则设计文档视为不完整。

⸻

2.2 Contract First

模块之间必须通过明确的 Contract 进行协作。

Contract 至少包括：

* 输入
* 输出
* 数据类型
* 必填字段
* 可选字段
* 字段语义
* 单位
* 时区
* 精度
* 排序规则
* 唯一键
* 错误类型
* 错误处理方式
* 幂等性要求
* 版本兼容策略

不得依赖：

“开发人员根据上下文理解即可。”

⸻

2.3 Single Responsibility

每个模块必须具有明确的单一职责。

例如：

Data Source Adapter
        ↓
Acquisition Engine
        ↓
Validation Engine
        ↓
Normalization Engine
        ↓
Storage Engine
        ↓
Dataset / Query Layer

不得将：

* 数据获取
* 数据清洗
* 数据标准化
* 数据校验
* 数据存储
* 数据查询

混合在一个模块中。

⸻

3. Coding Agent 可独立实施性审计

设计文档必须能够自然拆分成多个独立实施任务。

每个可交付模块必须至少定义：

项目	必须明确
Module ID	是
Module Name	是
Responsibility	是
Scope	是
Non-goals	是
Inputs	是
Outputs	是
Dependencies	是
Public API	是
Data Contract	是
State Model	必要时
Error Model	是
Persistence	必要时
Concurrency	必要时
Configuration	是
Observability	是
Tests	是
Acceptance Criteria	是

⸻

3.1 Non-goals 必须存在

每个模块必须明确：

这个模块不负责什么。

例如：

Market Data Downloader
负责：
- 请求交易所 REST API
- 分页
- 限速
- 重试
- checkpoint
- 增量获取
不负责：
- K线指标计算
- 数据库存储
- 技术指标计算
- 投资策略

这样可以防止不同 Coding Agent 在实现过程中产生职责重叠。

⸻

4. 模块依赖审计

设计文档必须提供完整的依赖 DAG。

例如：

Source Definition
       │
       ▼
Source Adapter
       │
       ▼
Acquisition Engine
       │
       ├────────► Raw Storage
       │
       ▼
Validation
       │
       ▼
Normalization
       │
       ▼
Canonical Storage
       │
       ▼
Query Layer
       │
       ▼
Analytics

审计必须确认：

* 不存在循环依赖。
* 下游模块不会反向依赖上层业务模块。
* 数据获取模块不依赖分析模块。
* Storage 不依赖具体 Source Adapter。
* Query Layer 不直接依赖外部 API。
* Analytics 不直接访问原始 API。
* 每个模块都可以通过 Mock / Fixture 替代其外部依赖。

⸻

5. 数据源抽象审计

多源金融数据系统不得针对每个数据源重复设计完整业务逻辑。

必须区分：

Source-specific logic

和

Generic acquisition logic

例如：

Binance Adapter
Deribit Adapter
Polymarket Adapter
FRED Adapter
        │
        ▼
Generic Acquisition Engine
        │
        ▼
Generic Validation / Storage

Source Adapter 应主要负责：

* API endpoint
* authentication
* request parameters
* pagination semantics
* source-specific response parsing
* source-specific rate limits
* source-specific cursor semantics
* source-specific error mapping

通用系统负责：

* checkpoint
* retry
* backoff
* deduplication
* continuity checking
* persistence
* recovery
* observability

⸻

6. 增量数据获取审计 —— 最高优先级

对于金融时间序列系统，设计文档必须默认采用：

Incremental First

而不是：

Full Download First

除非数据源本身只能提供全量数据。

⸻

6.1 每个数据集必须定义增量游标

必须明确至少一种：

timestamp
sequence_id
cursor
page_token
trade_id
update_id
block_number
event_id

必须定义：

* cursor 类型
* cursor 的单调性
* cursor 是否连续
* cursor 是否稳定
* cursor 是否可能重复
* cursor 是否可能跳跃
* cursor 是否可能回退
* cursor 是否跨 API 请求保持一致

⸻

6.2 增量获取必须支持 Checkpoint

必须设计：

Dataset
    ↓
Acquisition State
    ↓
Checkpoint

Checkpoint 至少记录：

source
dataset
instrument
timeframe
partition
last_successful_cursor
last_successful_timestamp
last_successful_sequence
watermark
status
updated_at

如果数据源支持 sequence ID，则优先使用 sequence ID，而不是仅使用 timestamp。

⸻

7. 增量下载必须具备幂等性

重复执行同一个任务不得导致数据重复或损坏。

必须满足：

download(A → B)
download(A → B)

最终结果必须与：

download(A → B)

一致。

即：

Idempotent Acquisition

测试必须覆盖：

* 完全重复下载
* 部分重复下载
* 重叠时间窗口
* checkpoint 回退
* checkpoint 丢失
* checkpoint 损坏
* 任务中途退出
* 网络断开后重试

⸻

8. 断点续传审计

下载任务必须能够在任意位置失败后恢复。

例如：

T0 ─────────────── T10000
                  ↑
                failure

重新启动后必须：

T10001 ────────── T20000

而不是重新下载整个：

T0 ────────────── T20000

除非数据源没有可靠的增量机制。

⸻

9. 增量窗口设计审计

禁止简单采用：

last_timestamp → now

作为唯一设计。

必须考虑：

* API 时间边界包含/不包含
* 时间精度
* 数据延迟
* 数据回补
* 数据修正
* late-arriving data
* source clock skew
* API pagination boundary
* 相邻请求重复记录

推荐明确规定：

fetch_start = last_successful_cursor - overlap_window

然后通过：

primary key / natural key
+
deduplication

消除 overlap。

Overlap window 必须具有设计依据，而不能随意设置。

⸻

10. 数据连续性审计 —— 最高优先级

对于具有理论连续时间结构的数据集，必须定义：

Continuity Contract

例如 OHLCV：

T0
T1
T2
T3
...
Tn

必须定义：

Δt = timeframe

对于正常交易时段，应满足：

timestamp[i+1] - timestamp[i] = Δt

但审计不得简单要求所有金融数据都“无间隙”。

必须区分：

Expected Gap

例如：

* 周末
* 节假日
* 市场关闭
* 交易品种未上市
* 合约到期
* API 数据源明确不存在数据

Unexpected Gap

例如：

* 下载失败
* API pagination bug
* 网络中断
* checkpoint 错误
* 数据解析错误
* 存储失败

系统必须能够区分两者。

⸻

11. 连续性规则必须数据集特定

不同数据类型不得使用统一的连续性判断。

例如：

OHLCV

检查：

expected_interval
missing_bar
duplicate_bar
out_of_order

Trades

可能检查：

trade_id
sequence
timestamp ordering
duplicate trade

Order Book

必须考虑：

snapshot
delta
sequence
checksum

FRED

不能简单要求每日连续，因为：

* 周期可能不同
* 发布日不同
* revision 存在

Polymarket

事件型数据通常不存在固定时间间隔。

因此设计文档必须为每种 dataset 定义自己的：

Continuity Model

⸻

12. 数据正确性审计

数据进入 Canonical Storage 之前必须经过 Validation。

至少检查：

Schema

required fields
data types
nullability
enum

Temporal

timestamp validity
timezone
ordering
duplicate
future timestamp

Numerical

NaN
Inf
negative price
negative volume
invalid precision
overflow

Domain

例如 OHLC：

Low <= Open <= High
Low <= Close <= High
High >= Low
Volume >= 0

并检查：

High >= max(Open, Close)
Low <= min(Open, Close)

⸻

13. 数据重复审计

必须明确 Dataset 的：

Natural Key / Primary Key

例如：

symbol + timeframe + timestamp

或：

exchange + symbol + trade_id

或：

market_id + event_id

必须避免仅使用数据库自动生成 ID 作为数据唯一性依据。

⸻

14. 数据覆盖与误覆盖审计

这是金融数据系统中的高风险问题。

设计文档必须明确：

Append-only

与：

Upsert

以及：

Revision

的使用场景。

不得让一个普通下载任务：

DELETE historical data
INSERT new data

而没有明确的数据版本策略。

⸻

15. Source Raw Data 与 Canonical Data 必须分层

建议设计至少包含：

Raw Layer
    ↓
Validated Layer
    ↓
Canonical Layer
    ↓
Derived Layer

其中：

Raw

尽量保存数据源原始数据。

Validated

保存经过结构验证的数据。

Canonical

统一数据模型。

Derived

指标、聚合、特征等计算结果。

这样才能在发现：

parser bug
normalization bug
source correction

时重新构建下游数据。

⸻

16. Storage 增量写入审计

存储系统必须支持：

* append
* partitioned write
* atomic write
* deduplication
* compaction
* checkpoint
* recovery

不得要求每次任务：

read entire dataset
→ rewrite entire dataset

除非数据量或存储格式明确证明这种方式合理。

⸻

17. Partition Strategy 审计

必须明确：

partition key

例如：

source
dataset
exchange
asset
symbol
date

同时避免过度 partitioning。

设计文档必须说明 partition strategy 的依据，包括：

* 查询模式
* 数据量
* 增量写入模式
* 文件数量
* 并发访问
* compaction 成本

⸻

18. 原子性审计

一次数据写入不能出现：

50% data written
checkpoint = SUCCESS

这种状态。

必须保证：

data commit
      +
checkpoint commit

具有一致性。

推荐：

Write temporary partition
        ↓
Validate
        ↓
Atomic commit
        ↓
Update checkpoint

而不是：

update checkpoint
        ↓
write data

⸻

19. Checkpoint 与 Data Commit 一致性

必须明确：

checkpoint 到底代表什么？

推荐定义为：

最后一个已经成功持久化、通过验证、可以被下游读取的数据边界。

因此：

checkpoint <= durable_valid_data

绝不能：

checkpoint > durable_valid_data

这是增量系统最重要的不变量之一。

⸻

20. Failure Recovery 审计

设计文档必须定义每一种失败后的行为。

至少覆盖：

Failure	Expected Behavior
DNS failure	retry
Connection timeout	retry
HTTP 429	rate-limit backoff
HTTP 5xx	retry
HTTP 4xx	classify
malformed response	quarantine
partial response	retry
process crash	resume
disk full	fail safely
database unavailable	retry
checkpoint corrupted	recover
duplicate data	deduplicate
gap detected	mark incomplete
checksum failure	reject

⸻

21. Quarantine 机制

错误数据不得直接写入 Canonical Dataset。

必须设计：

Invalid Data
      ↓
Quarantine
      ↓
Diagnostic Information

Quarantine 至少保留：

source
dataset
request
timestamp
raw payload / reference
validation error
detected_at

这样可以进行事后分析和重新处理。

⸻

22. 数据源 Revision 审计

设计必须考虑：

数据源可能修改过去的数据。

例如：

T100

今天得到：

price = 100

未来可能变成：

price = 101

因此必须明确：

* 是否允许历史修正
* 如何发现 revision
* 如何重新下载
* 如何覆盖
* 是否保留版本
* 是否记录 revision timestamp

⸻

23. Backfill 与 Incremental 必须统一

不得设计两套完全不同的数据处理逻辑：

Backfill Engine
Incremental Engine

导致：

Backfill data ≠ Incremental data

推荐：

Acquisition Engine
       │
       ├── Backfill Mode
       └── Incremental Mode

二者最终使用相同：

Validation
Normalization
Storage
Deduplication

⸻

24. 数据获取必须支持时间范围任务

每一个 Acquisition Job 应能够表达：

source
dataset
instrument
start
end
mode
priority

例如：

BINANCE
BTCUSDT
1m
2026-01-01
2026-09-01
incremental

任务必须能够被拆成：

Chunk 1
Chunk 2
Chunk 3
...

每个 chunk 独立成功/失败。

⸻

25. Pagination 审计

设计文档必须明确每个 API 的 pagination 类型：

page number
offset
cursor
timestamp
sequence
ID

并定义：

* 下一页如何计算
* 边界是否包含
* 是否可能重复
* 是否可能漏数据
* 最大 page size
* API 返回空页时行为
* cursor 失效时行为

必须测试：

exact page boundary
empty page
duplicate page
overlapping page
missing page
cursor expiration

⸻

26. Rate Limit 审计

每个 Source Adapter 必须明确：

request limit
weight
burst
concurrency
retry-after

通用 Acquisition Engine 不得假定所有数据源具有相同限速模型。

⸻

27. 时区与时间精度审计

金融数据系统必须明确：

internal timezone = UTC

除非有充分理由使用其他标准。

必须明确：

* timestamp storage type
* timezone
* milliseconds / microseconds / nanoseconds
* source timestamp precision
* conversion rule
* rounding rule

禁止隐式转换。

⸻

28. 测试覆盖要求

每个 Coding Agent 必须同时提交：

Implementation
+
Unit Tests
+
Integration Tests
+
Boundary Tests
+
Failure Tests

不能仅提交功能代码。

⸻

29. 测试必须覆盖正常路径与异常路径

最低测试集合：

Normal

* empty dataset
* single record
* normal batch
* large batch
* normal incremental update

Boundary

* first record
* last record
* exact page boundary
* exact partition boundary
* midnight
* month boundary
* year boundary
* leap day
* DST-related source behavior
* maximum API page size
* minimum API page size

Failure

* network timeout
* connection reset
* malformed JSON
* invalid schema
* missing field
* null value
* duplicate
* out-of-order
* gap
* corrupted checkpoint
* process interruption
* disk failure

Recovery

必须测试：

failure
→ restart
→ resume
→ verify

⸻

30. Property-Based Testing

对于核心数据处理模块，应优先考虑 Property-Based Testing。

例如 OHLCV：

Low <= min(Open, Close)
High >= max(Open, Close)
Volume >= 0

对于 Deduplication：

dedup(dedup(X)) == dedup(X)

对于 Incremental Acquisition：

acquire(A,B)
+
acquire(B,C)

最终应与：

acquire(A,C)

在 Canonical Dataset 上具有一致结果。

⸻

31. 数据完整性测试

必须设计能够自动验证：

expected records
actual records
missing records
duplicate records
unexpected records

例如：

Expected:
2026-01-01 00:00
2026-01-01 00:01
2026-01-01 00:02
2026-01-01 00:03
Actual:
2026-01-01 00:00
2026-01-01 00:01
2026-01-01 00:03

系统必须明确识别：

MISSING: 2026-01-01 00:02

而不是认为任务成功。

⸻

32. Silent Data Loss 审计

必须重点审查系统是否存在：

任务成功，但数据实际缺失

的可能。

例如：

HTTP 200
↓
返回 500 records
↓
程序认为成功
↓
checkpoint 更新

如果实际应该有：

1000 records

则这是严重错误。

设计必须尽可能通过：

* expected range
* pagination verification
* sequence verification
* continuity verification
* checksum
* record count
* source metadata

检测 silent data loss。

⸻

33. 数据质量状态必须显式化

Dataset 不应该只有：

exists / doesn't exist

至少应支持：

COMPLETE
PARTIAL
INCOMPLETE
VALIDATED
INVALID
QUARANTINED
STALE
REVISION_PENDING

状态定义必须写入设计文档。

⸻

34. Observability 审计

每一个 Acquisition Job 必须产生结构化日志和 metrics。

至少包括：

source
dataset
instrument
start_time
end_time
request_count
record_count
duplicate_count
missing_count
retry_count
error_count
latency
checkpoint_before
checkpoint_after
status

这样才能诊断：

“系统运行成功，但数据是否真的完整？”

⸻

35. 可重放性审计

重要数据处理流程必须支持：

Replay

即：

Raw Data
   ↓
Parser Version X
   ↓
Canonical Dataset

如果 Parser 出现 bug：

Raw Data
   ↓
Parser Version Y
   ↓
Canonical Dataset v2

不应该要求重新从外部 API 下载全部历史数据。

⸻

36. Schema Versioning

所有重要数据 Contract 必须具有版本概念。

例如：

OHLCV v1
OHLCV v2

必须明确：

* schema version
* compatibility
* migration
* backward compatibility
* forward compatibility

禁止 Coding Agent 随意修改字段名称或类型。

⸻

37. API Contract 测试

模块之间必须存在 Contract Test。

例如：

Acquisition Engine
        ↓
Storage Engine

测试：

given valid AcquisitionRecord
when write()
then canonical storage contains expected record

并测试非法输入。

⸻

38. Mock 与真实数据测试必须同时存在

Coding Agent 不得只依赖真实 API 测试。

必须：

Unit Test
    ↓
Mock Source

以及：

Integration Test
    ↓
Real / Recorded Fixture

真实 API 测试应尽量使用固定 fixture / cassette，避免：

* 网络不稳定
* API 变化
* rate limit
* 测试不可重复

⸻

39. Acceptance Criteria 必须可机器验证

禁止：

系统运行稳定
数据获取可靠
性能良好

这种不可验证描述。

必须转换为：

Given ...
When ...
Then ...

例如：

Given checkpoint = T1000
And source returns T1000..T1100
When acquisition runs
Then records T1001..T1100 are persisted
And no duplicate records exist
And checkpoint = T1100

⸻

40. Coding Agent Task Specification

设计文档中的每个模块必须最终能够转换成类似以下任务：

Task ID: DATA-ACQ-001
Implement:
Incremental Market Data Acquisition Engine
Inputs:
- SourceAdapter
- DatasetDefinition
- AcquisitionCheckpoint
Outputs:
- AcquisitionBatch
- UpdatedCheckpoint
Must support:
- incremental download
- pagination
- retry
- checkpoint
- idempotency
- deduplication
- continuity detection
Must NOT implement:
- analytics
- indicator calculation
- strategy logic
Required tests:
- normal acquisition
- duplicate page
- missing page
- timeout
- retry
- checkpoint recovery
- process interruption
- duplicate records
- gap detection
- boundary timestamps
Acceptance:
1. ...
2. ...
3. ...

如果设计文档无法自然产生这样的 Task Specification，则设计文档尚未达到实施级别。

⸻

41. 并行开发审计

多个 Coding Agent 并行工作时：

Agent A → Source Adapters
Agent B → Acquisition Engine
Agent C → Validation
Agent D → Storage
Agent E → Query
Agent F → Tests

必须避免共享未定义的内部实现。

Agent 之间只能依赖：

Public Interface
Data Contract
Schema
Test Contract

禁止：

Agent A directly imports Agent B's private module

⸻

42. Git 边界审计

每个模块必须能够形成独立 Git Commit。

推荐：

feat(data): implement source adapter
feat(acquisition): implement incremental engine
feat(validation): implement market data validator
feat(storage): implement partition writer
test(acquisition): add recovery tests

设计文档必须明确哪些文件属于哪个模块，避免多个 Agent 同时修改同一个核心文件。

⸻

43. Definition of Done

一个模块只有同时满足以下条件才算完成：

[ ] Implementation complete
[ ] Public API complete
[ ] Data contract implemented
[ ] Error handling implemented
[ ] Logging implemented
[ ] Metrics implemented
[ ] Unit tests complete
[ ] Boundary tests complete
[ ] Failure tests complete
[ ] Recovery tests complete
[ ] Integration tests complete
[ ] Static analysis passes
[ ] Type checking passes
[ ] No undocumented assumptions
[ ] Acceptance criteria satisfied

⸻

44. 设计文档最终审计等级

审计结果必须分级。

PASS

设计已经达到：

Coding Agent 可以直接开始实施。

不存在关键架构歧义。

⸻

PASS WITH WARNINGS

可以开始实施，但存在非阻塞问题。

必须列出：

Warning ID
Risk
Impact
Recommended Action

⸻

FAIL

存在以下任意问题时必须 FAIL：

* 核心模块职责不明确
* 模块无法独立实施
* 数据 Contract 不明确
* 增量机制未定义
* checkpoint 未定义
* 数据连续性未定义
* 数据唯一性未定义
* 失败恢复未定义
* 存储原子性未定义
* 测试边界未定义
* 关键设计依赖 Coding Agent 自行决策
* 数据可能发生 silent loss
* 数据可能被错误覆盖
* Backfill 与 Incremental 行为不一致
* 无法证明数据正确性

⸻

45. 审计报告格式

最终审计报告必须输出：

A. Overall Result

PASS / PASS WITH WARNINGS / FAIL

B. Architecture Findings

ID	Severity	Finding	Impact	Required Action

C. Module Independence

Module	Independent Implementation	Contract Complete	Testable	Status

D. Data Acquisition Audit

Capability	Required	Designed	Test Defined	Status
Incremental	✓			
Checkpoint	✓			
Resume	✓			
Idempotency	✓			
Pagination	✓			
Deduplication	✓			
Continuity	✓			
Retry	✓			
Rate Limit	✓			
Revision	✓			

E. Data Integrity Audit

Check	Designed	Tested	Status
Schema			
Primary Key			
Duplicate			
Missing			
Ordering			
Gap			
OHLC consistency			
Timestamp			
Precision			
Atomicity			
Checkpoint consistency			

F. Test Coverage Audit

必须明确：

Normal Path
Boundary
Failure
Recovery
Concurrency
Idempotency
Data Integrity
Integration

是否全部覆盖。

⸻

46. 最重要的系统级不变量

设计文档审计时，必须寻找并明确系统级 Invariants。

至少包括：

Invariant 1 — Checkpoint Safety

checkpoint <= durable_valid_data_boundary

Invariant 2 — Idempotency

F(F(X)) = F(X)

Invariant 3 — No Silent Loss

successful_job ⇒ verified_data_boundary

Invariant 4 — No Duplicate Canonical Records

∀ key, count(key) ≤ 1

Invariant 5 — Temporal Ordering

对于要求有序的数据：

t[i+1] >= t[i]

Invariant 6 — Continuity

对于定义为连续的数据：

expected_next(t[i]) = t[i+1]

除非该 gap 被明确标记为：

EXPECTED_GAP

Invariant 7 — Reproducibility

same raw input
+
same schema/parser version
=
same canonical output

⸻

47. 审计的核心判断标准

最终不要问：

“这份设计文档看起来是否合理？”

而必须问：

“如果把其中一个模块单独交给一个完全不了解项目背景的 Coding Agent，它是否能够仅依据设计文档，在不自行做关键架构决策的情况下完成实现，并通过完整的边界、失败、恢复和数据正确性测试？”

进一步必须问：

“如果该 Coding Agent 明天停止工作，另一个 Agent 能否仅根据 Contract、测试和设计文档接管该模块？”

如果答案为否，则设计文档不能通过审计。

⸻

48. 数据获取与存储的最高优先级原则

对于金融数据基础设施：

数据完整性优先于下载速度，数据可恢复性优先于实现简单性，增量获取优先于重复全量获取。

系统必须优先保证：

Correctness
    ↓
Continuity
    ↓
Durability
    ↓
Recoverability
    ↓
Idempotency
    ↓
Observability
    ↓
Performance

而不是：

Performance
    ↓
Implementation simplicity
    ↓
Correctness

尤其禁止为了提高下载速度而牺牲：

* checkpoint 安全
* 数据连续性
* 去重
* 原子提交
* 失败恢复
* 数据验证

⸻

49. 审计结论

只有当设计文档能够同时回答以下问题时，才允许进入 Coding Agent 并行实施阶段：

1. 谁负责？
2. 谁不负责？
3. 输入是什么？
4. 输出是什么？
5. 接口是什么？
6. 数据 Contract 是什么？
7. 状态是什么？
8. 失败怎么办？
9. 如何恢复？
10. 如何增量获取？
11. 如何断点续传？
12. 如何避免重复？
13. 如何检测缺失？
14. 如何证明数据连续？
15. 如何证明数据正确？
16. 如何保证 checkpoint 不超过实际数据？
17. 如何处理历史数据修订？
18. 如何保证 Backfill 与 Incremental 一致？
19. 如何独立测试？
20. 如何证明测试覆盖了全部边界？
21. 另一个 Coding Agent 能否无缝接管？

任何一个核心问题没有明确答案，都必须在设计阶段解决，而不能将其留给 Coding Agent 在编码阶段“自行判断”。

设计文档的目标不是描述一个漂亮的系统，而是将架构师的决策转换成可以被多个独立 Coding Agent 精确执行、验证和集成的工程契约。
