# D10 — Coding Agent Task Manifest

依据：`review/00-design-review-and-task-decomposition-spec.md` Part B；任务清单源自 `review/01-design-audit-report.md` §H，全部任务已随 F-01~F-16 修复解除 blocked（见 `review/02-reaudit.md`）。
用法：每单派发给一个 Coding Agent；Agent 仅可依赖 `dependencies` 声明的 Public API 与 D01 公共约定。

## 0. 公共约定（所有任务单继承，不逐单重复）

- **DoD**：review spec §B.5 的 16 项清单（实现/公共 API/数据契约/错误处理/日志/指标/单测/边界/失败/恢复/集成/静态分析/类型检查/无未声明假设/验收通过）
- **error_model**：全部异常定义于 D01 §3 层级，禁止新增顶层异常类
- **configuration**：全部经 Settings（D08 §1），禁止模块内直读 env
- **observability**：structlog 事件词表 D08 §3；新增事件名须先改词表（architecture test 校验）
- **git**：每单独立分支 + 约定式提交（如 `feat(storage): implement canonical merge-rewrite`）
- **测试**：用例 ID 即 pytest docstring 首行，与 D09 §3 一一对应；标签按 D09 §1
- **验收格式**：全部 Given/When/Then，机器可验证（review spec §C）

### 0.2 Coding Agent 决策边界（冻结项，审计要求 §42/§61：Design Frozen, Implementation Open）

Agent **可以**决定（How to implement，无需评审）：

- private helper 函数的拆分与命名；模块内部局部实现细节
- 变量名、内部常量组织、代码排版
- 测试 fixture 的组织方式（文件内）、参数化写法、断言风格
- 循环/向量化等实现策略；pandas vs polars 的模块内选择（接口契约不变时）
- 日志调用的具体位置（事件名与字段仍须遵守词表）

Agent **不可以**决定（What the system means，任何改动必须回到 Design Change Process，架构约束 §85）：

- Data Contract：字段名/类型/单位/精度/nullable/natural key/schema_version（D02 全部）
- API Contract：Public API 签名/参数语义/错误类型/幂等性（D03–D08 各接口）
- Storage Semantics：分区布局/merge-rewrite 算法/原子性协议/compaction 触发（D03）
- Incremental Semantics：cursor 语义八属性/overlap/boundary 包含性（D05 §5）
- Checkpoint Semantics：推进不变量/回退防护/恢复路径（D05 §3、D03 §5）
- Continuity Rules：continuity_model 枚举值与判定逻辑（D06）
- Error Semantics：错误类层级/重试矩阵/终态映射（D01 §3、D04 §3、D05 §1）
- Module Boundaries：file_ownership 与依赖方向（D01 §1、.importlinter）
- 质量规则 ID/severity/block_on 默认值（D06）

**冲突处理**：实现中发现 Contract 本身有问题 → 停止实现、开 Design Issue、走受控变更（架构约束 §85）；只有 Implementation Bug / Test Bug 才由 Agent 自行修复。

---

## 1. MODEL 域

### MODEL-001 BaseRecord 基座

```yaml
task_id: MODEL-001
title: BaseRecord、枚举与 provenance 基座
design_refs: [D02 §1, D01 §3]
responsibility: 定义全部 Canonical Type 的公共基座（BaseRecord/provenance 五字段/quality 字段/CanonicalType 枚举）
scope: [src/chronoforge/models/base.py, src/chronoforge/models/enums.py, tests/unit/test_base.py]
non_goals: [具体 Type schema（MODEL-002）, InstrumentResolver（MODEL-003）, 任何存储/IO]
inputs: []                                   # 零依赖底层
outputs: [{name: BaseRecord, contract: D02 §1}, {name: CanonicalType, contract: D02 §1}]
public_api: D02 §1 代码块逐行实现
data_contract: D02 §1 + D02 §5 校验器（含 isfinite，审计 F-15）
error_model: 校验失败 → pydantic ValidationError（由 VALIDATION-001 捕获转 finding，不在本层吞）
file_ownership: [src/chronoforge/models/base.py, src/chronoforge/models/enums.py, tests/unit/test_base.py]
dependencies: []
tests: {unit: [TC-M-001/002/003], boundary: [extra=forbid 未知字段, 全枚举值遍历], failure: [非法枚举, NaN/Inf 数值]}
acceptance:
  - Given 合法样例 When 子类实例化 Then provenance 字段全部必填通过
  - Given 含未知字段的记录 Then ValidationError（extra=forbid）
  - Given NaN 价格 Then 拒绝（isfinite）
```

### MODEL-002 27 Canonical Type schema

```yaml
task_id: MODEL-002
title: 全部 Canonical Type 字段级 schema + natural_key()
design_refs: [D02 §2/§4/§5]
responsibility: 实现 27 个 Canonical Type（market/derivatives/macro/fundamental/positioning/prediction/text/reference/derived）
scope: [src/chronoforge/models/{market,derivatives,macro,fundamental,positioning,prediction,text,reference,derived}.py, tests/unit/test_types.py]
non_goals: [InstrumentResolver 解析逻辑（MODEL-003）, 质量规则（VALIDATION 域）, 存储序列化]
inputs: [{name: BaseRecord, from: MODEL-001}]
outputs: [{name: 各 Type 类, contract: D02 §2 字段表}, {name: natural_key(CanonicalType), contract: D02 §2 各身份键}]
public_api: 各字段表直接转 pydantic；natural_key() 返回 D02 声明的列元组
data_contract: D02 §2 全部字段表 + §4 时间矩阵（每 Type 的时间字段组合不可偏离）
error_model: 同 MODEL-001
file_ownership: [src/chronoforge/models/*.py（除 base/enums/reference 解析器）, tests/unit/test_types.py]
dependencies: [MODEL-001]
tests: {unit: [TC-M-004/007/008], boundary: [FRED value=None 合法, iv 边界 0/5], failure: [high<low, iv>5, probability<0, provenance 缺字段(Q-PROV-001 语义)]}
acceptance:
  - Given parse 后的 OPTION 样例 Then 含 underlying/expiry/strike/option_type/settlement_asset
  - Given NUMBER 同 observation 两 vintage Then 两实例 natural_key 不同（revision_time 参与键）
  - Given 任意 Type When natural_key() Then 与 D02 身份键声明逐列一致
```

### MODEL-003 InstrumentResolver

```yaml
task_id: MODEL-003
title: 三级 Instrument Identity 解析器
design_refs: [D02 §3]
responsibility: entity_id/instrument_id/market_id 生成与解析（含 Deribit 月份码表、别名映射）
scope: [src/chronoforge/models/reference.py（解析函数部分）, tests/unit/test_resolver.py]
non_goals: [discover 拉取（DATA-SOURCE-003）, market_id 的存储]
inputs: [{name: 源侧 symbol/instrument_name, type: str}]
outputs: [{name: parse_binance/parse_deribit/parse_yahoo, contract: D02 §3 表}]
public_api: D02 §3 三个函数 + InstrumentResolver 类（单点实现，connector 只调用）
data_contract: D02 §3 ID 规则表（格式即契约，违例=bug）
error_model: 无法解析 → ProviderError（携带原始字符串）
file_ownership: [src/chronoforge/models/reference.py, tests/unit/test_resolver.py]
dependencies: [MODEL-001]
tests: {unit: [TC-M-004/005/006], boundary: [过期合约, 最小月份码], failure: [非法月份码, 未知 strike 格式]}
acceptance:
  - Given "BTC-26SEP26-100000-C" Then (BTC, BTC-2026-09-26-100000-C, DERIBIT:…:OPTION) 全字段正确
  - Given 过期合约 Then 解析成功 + 调用方获知过期（不抛异常）
  - Given "BTC-32FOO26-100000-C" Then ProviderError
```

## 2. STORAGE 域

### STORAGE-001 MetaStore + 迁移

```yaml
task_id: STORAGE-001
title: SQLite 元数据库（DDL/迁移/锁/checkpoint/run_log/状态推导）
design_refs: [D03 §1/§6, D04 §5]
responsibility: 迁移 0001 全部 6 表 + try_lock_dataset + checkpoints + run_log 读写 + dataset 状态推导 + rebuild_checkpoints
scope: [src/chronoforge/storage/meta.py, src/chronoforge/storage/migrations/0001_init.py, tests/integration/test_meta.py]
non_goals: [Parquet 读写（STORAGE-003）, 状态推导的业务触发（PIPELINE-001）]
inputs: [{name: Settings.meta_dir, from: D08}]
outputs: [{name: MetaStore, contract: D03 §6 接口}]
public_api: D03 §6 代码块（migrate/try_lock_dataset/finish_run/save_checkpoint/get_checkpoint/add_quality_flags + rebuild_checkpoints）
data_contract: D03 §1 DDL 逐列实现（含 continuity_model/status/raw_ref/payload_digest/观测计数的全部扩列）
state_model: run 状态机 D05 §3；dataset 状态推导 D03 §1（按序判定）
error_model: 锁冲突→StorageError("dataset locked")；损坏→rebuild_checkpoints 路径（D03 §5.5）
concurrency: 单写者；BEGIN IMMEDIATE；dataset 级锁
file_ownership: [src/chronoforge/storage/meta.py, src/chronoforge/storage/migrations/, tests/integration/test_meta.py]
dependencies: [MODEL-001]
tests: {unit: [迁移幂等], integration: [TC-S-003/004], boundary: [空库首次迁移, 迁移重入], failure: [锁互斥, SQLite 损坏→rebuild], recovery: [TC-S-004]}
acceptance:
  - Given 两线程并发 try_lock 同 dataset Then 后者抛 StorageError
  - Given rename 后 run_log 缺失的分区 Then reconciliation 补记终态
  - Given 损坏的 meta.db When rebuild_checkprints(ds) Then cursor 与 durable 数据边界一致
```

### STORAGE-002 RawStore

```yaml
task_id: STORAGE-002
title: Raw 层 JSONL 只追加存储
design_refs: [D03 §2]
responsibility: raw JSONL 分区写（ingest_date 分区）+ iter_refs 遍历 + RawRef 构造
scope: [src/chronoforge/storage/raw.py, tests/integration/test_raw.py]
non_goals: [解析/校验 raw 内容, canonical 写入]
inputs: [{name: RawBatch, from: ACQUISITION-001}]
outputs: [{name: RawStore, contract: D03 §2}]
public_api: append(source, dataset, batches) -> list[RawRef]; iter_refs(...)
data_contract: 行结构 {"ingest_batch_id","fetched_at","url","payload"}；只追加不可变
persistence: jsonl，分片 {HHmmss}-{seq}.jsonl
error_model: StorageError（磁盘/权限）；无静默丢行
file_ownership: [src/chronoforge/storage/raw.py, tests/integration/test_raw.py]
dependencies: [MODEL-001]
tests: {unit: [行序列化], integration: [append→iter_refs 往返], boundary: [空批次, 跨日分区归属], failure: [磁盘写满→StorageError]}
acceptance:
  - Given append 100 行 When iter_refs Then 逐行可回读且 raw_record_id 可定位
  - Given 同一文件二次 append Then 只追加不覆盖（文件行数单调）
```

### STORAGE-003 CanonicalStore（merge-rewrite + 漂移）

```yaml
task_id: STORAGE-003
title: Parquet CanonicalStore（分区级 merge-rewrite + 值漂移检测）
design_refs: [D03 §3]
responsibility: upsert 算法（含 drift 检测→Q-DRIFT-001 findings）、分区布局、UpsertStats
scope: [src/chronoforge/storage/canonical.py, tests/integration/test_canonical.py]
non_goals: [质量规则判定（VALIDATION 域）, 视图（STORAGE-004）, 元数据（STORAGE-001）]
inputs: [{name: records: list[BaseRecord 子类], from: MODEL-002}]
outputs: [{name: CanonicalStore, contract: D03 §3}, {name: UpsertStats{inserted,updated,drifted,rewritten_partitions}}]
public_api: upsert(records, type, entity) -> UpsertStats（算法逐步照抄 D03 §3 伪代码）
data_contract: 路径 {type}/entity=/year=/month=/；zstd；timestamp[us]；natural_key 来自 MODEL-002
error_model: StorageError；drift 不抛异常→findings 交 QualityStage
persistence: temp→fsync→rename；p.old 先 mv 再删
concurrency: 由 pipeline 串行调用；本模块不自身加锁
file_ownership: [src/chronoforge/storage/canonical.py, tests/integration/test_canonical.py]
dependencies: [MODEL-002]
tests: {unit: [分组/排序], integration: [TC-S-001/002, 值漂移用例（D03 §7 末行）], boundary: [空分区首写, 跨月分片], failure: [rename 中断→孤儿清理], property: [TC-PROP-004 upsert 交换律]}
acceptance:
  - Given 同批次 upsert 两次 Then 第二次 inserted=0（幂等）
  - Given 旧 close=100 重拉同 nk close=101 Then 记录更新 + Q-DRIFT-001 finding 含旧/新 digest
  - Given revision 类同 (nk, revision_time) Then 不触发 drift（多版本并存）
```

### STORAGE-004 DuckDB 视图层

```yaml
task_id: STORAGE-004
title: DuckDB 视图注册（含 as-of 点时视图）
design_refs: [D03 §4]
responsibility: register_views 全部 canonical type 视图 + {type}_asof 模板 + 视图集合一致性
scope: [src/chronoforge/storage/views.py, tests/integration/test_views.py]
non_goals: [查询编排（QUERY-001）, 视图内业务计算]
inputs: [{name: canonical parquet 目录, from: STORAGE-003}]
outputs: [{name: register_views(con), contract: D03 §4 SQL}]
public_api: D03 §4 SQL 逐条实现（read_parquet hive_partitioning + row_number as-of 模板）
data_contract: 视图名 = type 小写；as-of 经 SET VARIABLE asof
file_ownership: [src/chronoforge/storage/views.py, tests/integration/test_views.py]
dependencies: [STORAGE-003]
tests: {unit: [SQL 文本快照], integration: [TC-S-005 as-of 两 vintage], boundary: [空 parquet 目录, 单行分区], failure: [缺失分区目录→视图仍可注册（惰性）]}
acceptance:
  - Given v1/v2 两 vintage When SET VARIABLE asof=T1 Then 仅 v1 可见
  - Given CanonicalType 全集 When register_views Then 视图集合 == 类型集合（architecture test 断言）
```

### STORAGE-005 原子性协议 + reconciliation

```yaml
task_id: STORAGE-005
title: 跨存储一致性（孤儿清理 + 对账）
design_refs: [D03 §5]
responsibility: cleanup_orphans（temp/p.old 清理）+ reconciliation（无终态 run 的 ingest_batch_id 补记）
scope: [src/chronoforge/storage/consistency.py, tests/integration/test_consistency.py]
non_goals: [正常路径写入（各 Store 自身）]
inputs: [{name: 文件系统扫描, }, {name: run_log, from: STORAGE-001}]
outputs: [{name: cleanup_orphans()/reconcile(), contract: D03 §5}]
public_api: 启动时调用：cleanup → reconcile 顺序固定
error_model: 对账冲突记 ERROR 日志并标记 dataset INCOMPLETE，不自动改写数据
file_ownership: [src/chronoforge/storage/consistency.py, tests/integration/test_consistency.py]
dependencies: [STORAGE-001, STORAGE-002, STORAGE-003]
tests: {integration: [TC-S-004/007], boundary: [全部 temp 无 rename 的极端态], failure: [rename 后 run_log 缺失], recovery: [启动即修复]}
acceptance:
  - Given 模拟 crash 留下 p.tmp-xxx When cleanup_orphans Then 目录恢复干净且 canonical 完整
  - Given rename 成功但 run_log 无终态 When reconcile Then 补记 SUCCESS/FAILED
```

## 3. ACQUISITION 域

### ACQUISITION-001 Connector 协议 + 限流 + 重试

```yaml
task_id: ACQUISITION-001
title: DataConnector 协议、令牌桶限流器、重试与错误映射
design_refs: [D04 §1–3, D01 §3]
responsibility: 协议类型定义（FetchRequest/RawBatch/CapabilityMatrix）、RateLimiter（可注入时钟）、retry 装饰器、HTTP→错误类映射
scope: [src/chronoforge/connectors/base.py, src/chronoforge/connectors/ratelimit.py, src/chronoforge/connectors/errors.py, tests/unit/test_ratelimit.py]
non_goals: [任何具体源实现（DATA-SOURCE-*）, normalize 逻辑（各源自带）, 落盘]
inputs: [{name: Settings（timeout/retry_max/rate_overrides）, from: D08}]
outputs: [{name: DataConnector 等 5 类型, contract: D04 §1}]
public_api: D04 §1 代码块 + §2 限流器 + §3 重试表
data_contract: RawBatch.payload 保持源响应原样（不清洗）——下游 fixture 对比的基准
error_model: D01 §3 全层级 + D04 §3 HTTP 映射表逐行实现
configuration: retry_max/http_timeout_s/rate_overrides_json
observability: connector.retry 事件 {source,endpoint,attempt,exc}
file_ownership: [src/chronoforge/connectors/{base,ratelimit,errors}.py, tests/unit/test_ratelimit.py]
dependencies: [MODEL-002]
tests: {unit: [令牌桶时钟注入, 退避序列 1/2/4/8/16s+jitter], failure: [TC-C-008/009/010], boundary: [Retry-After 超大值, burst=1]}
acceptance:
  - Given 429+Retry-After=5 When fetch Then 冷却 5s 后重试且计数
  - Given 401 When fetch Then 立即 AuthError 不重试
  - Given 固定时钟 When acquire(n) 连续调用 Then 阻塞时长符合令牌桶公式
```

### ACQUISITION-002 AcquisitionJob + 窗口/overlap 引擎

```yaml
task_id: ACQUISITION-002
title: 窗口拆分、overlap、cursor 推进与回退防护（获取引擎核心）
design_refs: [D05 §1 FetchStage/§2/§3/§5（修复后）]
responsibility: AcquisitionJob 数据类、窗口计算（mode×overlap×boundary 语义表驱动）、chunk 序列生成、durable cursor 推进、回退防护
scope: [src/chronoforge/pipeline/windows.py, src/chronoforge/pipeline/cursor.py, tests/unit/test_windows.py]
non_goals: [HTTP 请求（ACQUISITION-001）, stage 编排（PIPELINE-001）, 存储]
inputs: [{name: cursor(get_checkpoint), from: STORAGE-001}, {name: §5.2 语义表, from: D05}]
outputs: [{name: plan_chunks(job, cursor, now) -> list[Chunk], }, {name: advance(cursor, chunk_result) -> NewCursor|Skip}]
public_api: 窗口/overlap/boundary 全部表驱动（D05 §5.2 六行数据即配置，禁止硬编码）
data_contract: Chunk{start,end,request}；cursor 一律 ISO datetime 字符串
state_model: cursor 推进不变量（D05 §3）+ 回退防护
error_model: 表外数据集→ConfigError（fail fast）
configuration: overlap 值随语义表，不可运行时随意改
observability: checkpoint_before/after 入 StageResult
file_ownership: [src/chronoforge/pipeline/{windows,cursor}.py, tests/unit/test_windows.py]
dependencies: [ACQUISITION-001, STORAGE-001]
tests: {unit: [TC-P-010, 六数据集×两 mode 窗口快照], boundary: [闰日/年边界窗口切分, 00:00 归属], failure: [cursor 回退→跳过+WARNING], property: [TC-PROP-002 区间拼接律]}
acceptance:
  - Given checkpoint=T1000 And 3-chunk 窗口第 2 chunk 失败 Then cursor=第 1 chunk 右界、chunk_failed=1、终态 PARTIAL
  - Given klines cursor=T1000 Then 下窗口 start=T1000−1×interval（overlap 生效）
  - Given 新 cursor≤旧 cursor Then 跳过更新 + WARNING
```

## 4. DATA-SOURCE 域（DATA-SOURCE-001~007 共用模板）

**共用模板（各单继承）**：responsibility=单源接入（endpoint/auth/分页/限流/cursor/错误映射/normalize 到 canonical）；non_goals=[通用重试/限流（ACQUISITION-001）, 落盘, 质量判定, 指标计算]；inputs=[AcquisitionJob chunk（ACQUISITION-002）, MODEL-002/003]；outputs=[RawBatch → normalize → list[具体 Type]]；public_api=DataConnector 协议全方法；error_model=D04 §3 映射；file_ownership=[src/chronoforge/connectors/{source}.py, tests/fixtures/{source}/, tests/integration/test_{source}.py]；dependencies=[ACQUISITION-001, MODEL-002, MODEL-003]；通用测试=每 endpoint happy/edge/error fixture 契约 + TC-C-008~011 通用路径；通用验收=fixture→canonical 逐字段一致 + 分页无重叠无缺口 + 官方文档核对记录入 source_registry。

各单差异项：

```yaml
DATA-SOURCE-001 binance_spot:
  spec: D04 §4.1（klines/aggTrades/ticker24hr；aggTrades id 语义=Q-SEQ-001 对账）
  tests_extra: [TC-Q-008 aggTrades 跳号, Q-TS-003 未收盘 K 线丢弃]
  acceptance_extra: [Given 未收盘 K 线 When normalize Then 丢弃+计数（不入 canonical）]
DATA-SOURCE-002 binance_futures:
  spec: D04 §4.2（klines/fundingRate/openInterest；liquidation=P1 不做）
  tests_extra: [funding 8h 周期窗口切分, OI 快照全量 upsert]
  acceptance_extra: [Given fundingRate 分页 Then cursor=fundingTime 滚动无重叠]
DATA-SOURCE-003 deribit:
  spec: D04 §4.3（get_instruments discover 必须实现 + book_summary + tvchart）
  tests_extra: [discover 全量→INSTRUMENT 记录, 月份码联动 TC-M-004/005]
  acceptance_extra: [Given discover Then INSTRUMENT 记录数=源返回数且三级 ID 正确]
DATA-SOURCE-004 ccxt_bridge:
  spec: D04 §4.4（exchange.has 驱动 capability；仅统一端点）
  tests_extra: [TC-C-013 capability detection, fetch_ohlcv 2 页 fixture]
  acceptance_extra: [Given exchange.has[fetch_ohlcv]=False Then capabilities 不含该端点（不硬编码假设）]
DATA-SOURCE-005 yahoo:
  spec: D04 §4.5（chart 端点；保守限流 30/min；split/dividend 四元组；TRADING_CALENDAR）
  tests_extra: [TC-C-012 split 四元组, TC-Q-007 周末 EXPECTED_GAP, 秒→us 精度]
  acceptance_extra: [Given 周末缺失网格 When quality Then INFO(EXPECTED_GAP) 非 WARNING]
DATA-SOURCE-006 fred:
  spec: D04 §4.6（observations 全窗口 diff；vintage 多版本追加；发布日历 release_time）
  tests_extra: [TC-M-007 双 vintage 并存, release 无日历→次日+附注, T23:59:59Z 保守规则]
  acceptance_extra: [Given 同 observation 修订 Then (nk,revision_time) 追加不覆盖]
DATA-SOURCE-007 sec_edgar:
  spec: D04 §4.7（submissions/companyfacts；User-Agent 必填；ETag 条件请求；≤10req/s）
  tests_extra: [TC-C-014 UA 注入, ETag 304 处理, acceptanceDatetime EDT→UTC]
  acceptance_extra: [Given 无 UA 配置 When 构造 client Then ConfigError（fail fast）]
```

## 5. VALIDATION 域

### VALIDATION-001 质量规则引擎

```yaml
task_id: VALIDATION-001
title: 规则引擎 + 全部 17 条规则（14 原有 + Q-TS-003/Q-SEQ-001/Q-DRIFT-001）
design_refs: [D06 §1–3（修复后）]
responsibility: QualityRule 协议、规则注册表、run() 编排、QualityReport、block_on 策略
scope: [src/chronoforge/quality/rules.py, src/chronoforge/quality/report.py, tests/unit/test_rules.py]
non_goals: [drift 检测本身（STORAGE-003 提供 findings）, 交易日历数据（VALIDATION-002）, run 状态]
inputs: [{name: records, from: MODEL-002}, {name: drift findings, from: STORAGE-003}]
outputs: [{name: QualityReport/findings, contract: D06 §1}]
public_api: run(records, canonical_type, context) -> QualityReport；规则表 = D06 §2 全表
data_contract: finding 字段 = quality_flags DDL 列（含 raw_ref/payload_digest）
error_model: block_on 命中 → QualityError（run FAILED，不回滚已写分区）
configuration: quality_block_on / normalize_error_threshold
file_ownership: [src/chronoforge/quality/, tests/unit/test_rules.py]
dependencies: [MODEL-002]
tests: {unit: [每规则触发/不触发成对, TC-Q-001~004/008/009], boundary: [空记录集, 单记录], property: [规则纯函数性]}
acceptance:
  - Given 1m 网格缺 3 连续+2 分散 When Q-GAP-001 Then [(10:05,10:08),(10:20,10:22)]
  - Given Q-SCHEMA-001 命中（block_on）Then run FAILED 且已写分区保留
  - Given 规则注册表 Then rule_id 无重复（architecture test）
```

### VALIDATION-002 Continuity Model + 简化交易日历

```yaml
task_id: VALIDATION-002
title: continuity_model 驱动的 gap 判定 + P0 简化日历
design_refs: [D06 §2 Q-GAP-001（修复后）, D03 §1 continuity_model 列]
responsibility: 四种 continuity_model 的 gap 语义 + 内置美股简化日历（周末+固定节假日）+ EXPECTED_GAP 标记
scope: [src/chronoforge/quality/continuity.py, src/chronoforge/quality/calendar.py, tests/unit/test_continuity.py]
non_goals: [正式交易日历库引入（P1 评估）, dataset 状态推导（STORAGE-001）]
inputs: [{name: dataset.continuity_model, from: STORAGE-001}]
outputs: [{name: expected_grid(market, interval, model, range), }]
public_api: is_trading_day(date) -> bool（周末+固定节假日表）；grid 生成含 EXPECTED_GAP 标注
data_contract: TRADING_CALENDAR 之外的三种 model 不产生网格 gap finding
file_ownership: [src/chronoforge/quality/{continuity,calendar}.py, tests/unit/test_continuity.py]
dependencies: [VALIDATION-001]
tests: {unit: [TC-Q-007, 闰日/节假日/周末矩阵, ALWAYS_OPEN 全网格], boundary: [年末感恩节固定表, 2/29]}
acceptance:
  - Given yahoo 周六无 K 线 Then EXPECTED_GAP(INFO) 无 WARNING
  - Given 交易日缺失 Then Unexpected Gap WARNING
  - Given ALWAYS_OPEN 数据集周末缺失 Then 仍 WARNING（无豁免）
```

## 6. PIPELINE 域

### PIPELINE-001 Runner 七阶段 + 状态机 + replay

```yaml
task_id: PIPELINE-001
title: PipelineRunner（七阶段编排/锁/熔断/PARTIAL/replay/观测计数）
design_refs: [D05 §1–4/§7（修复后）]
responsibility: Stage 装配与顺序保证、终态判定、熔断、run_many 聚合、replay、run_log 全字段记账
scope: [src/chronoforge/pipeline/{runner,state,replay}.py, tests/integration/test_pipeline.py]
non_goals: [窗口计算（ACQUISITION-002）, 规则判定（VALIDATION-001）, 各 Store 实现]
inputs: [{name: AcquisitionJob, from: ACQUISITION-002}, 全部上游 Public API]
outputs: [{name: PipelineRunner.run/replay, contract: D05 §2/§4}]
public_api: run(job)->RunRow; run_many(jobs); replay(layer, dataset_id)
state_model: D05 §3 转换矩阵 + 熔断（3 连败）+ cursor 推进不变量
error_model: 各错误类→终态映射表逐行实现（D05 §1 表）
concurrency: 单线程；dataset 锁经 STORAGE-001
observability: pipeline.stage/run.finish 事件；run_log 全部观测列（含 chunk/计数/checkpoint 前后）
file_ownership: [src/chronoforge/pipeline/{runner,state,replay}.py, tests/integration/test_pipeline.py]
dependencies: [ACQUISITION-002, STORAGE-001/002/003/005, VALIDATION-001, DATA-SOURCE-001~007（接口依赖）]
tests: {integration: [TC-P-001~011 全组], boundary: [空结果, 单 chunk], failure: [各阶段注入各错误类], recovery: [TC-P-009 三态恢复]}
acceptance:
  - Given 各错误类注入 When run Then 终态与 D05 §1 映射表逐行一致
  - Given 3 次 FAILED When 第 4 次 run Then CANCELLED（熔断）
  - Given 删除 derived When replay Then 输出与原快照逐字节一致
```

## 7. QUERY 域

### QUERY-001 QueryService

```yaml
task_id: QUERY-001
title: DuckDBQueryService（as-of/防注入/只读/默认排序）
design_refs: [D07 §1（修复后）]
responsibility: query() 六规则实现（视图选择/时间列/as-of/白名单/投影/稳定排序）
scope: [src/chronoforge/research/query.py, tests/integration/test_query.py]
non_goals: [特征计算（QUERY-002）, 视图 DDL（STORAGE-004）]
inputs: [{name: DuckDB 连接(read_only), }, {name: dataset_registry, from: STORAGE-001}]
outputs: [{name: QueryResult, contract: D07 §1}]
public_api: D07 §1 签名 + 六规则（排序规则 6 不可关闭）
data_contract: QueryResult.frame=polars；元信息三字段恒附加
error_model: 未知列/非法 filters → ValueError；连接只读
file_ownership: [src/chronoforge/research/query.py, tests/integration/test_query.py]
dependencies: [STORAGE-001, STORAGE-004]
tests: {integration: [TC-R-001/002/003], boundary: [空结果集, asof=最早时刻], failure: [注入防御, 写入尝试]}
acceptance:
  - Given 两 vintage When asof=T1 Then 仅 v1 且默认按 observation_time 升序稳定排序
  - Given filters 含 "; DROP" Then ValueError 且连接无恙
```

### QUERY-002 FeatureEngine

```yaml
task_id: QUERY-002
title: FeatureEngine 协议 + 5 个 P0 特征
design_refs: [D07 §2]
responsibility: 协议 + returns/realized_vol/iv_surface/funding_oi_divergence/btc_market_stress（依赖声明式）
scope: [src/chronoforge/features/engine.py, src/chronoforge/features/builtin.py, tests/integration/test_features.py]
non_goals: [取数（只经 QUERY-001）, 存储（DerivedStore 由 STORAGE-003 布局）, 研究快照（QUERY-003）]
inputs: [{name: QueryService, from: QUERY-001}]
outputs: [{name: FeatureEngine 实现集, contract: D07 §2 表（公式列即契约）}]
public_api: compute(q, params)->DataFrame; register(); dependencies 声明
data_contract: 输出随行携带 dependencies+params+code_version（D02 §7）
file_ownership: [src/chronoforge/features/, tests/integration/test_features.py]
dependencies: [QUERY-001]
tests: {integration: [特征链依赖注册, 黄金值（固定输入→固定输出）], property: [确定性：同输入两次计算逐字节一致]}
acceptance:
  - Given btc_market_stress 声明 Then dependencies 含 6 项 D07 §2 表所列 dataset
  - Given 同 dependencies 版本+同代码 When 重算 Then output hash 一致
```

### QUERY-003 ResearchSnapshot

```yaml
task_id: QUERY-003
title: 研究快照与复现（迁移 0002）
design_refs: [D07 §3]
responsibility: research_snapshot 表迁移 + contextmanager + reproduce 比对
scope: [src/chronoforge/research/snapshot.py, src/chronoforge/storage/migrations/0002_snapshot.py, tests/integration/test_snapshot.py]
non_goals: [研究逻辑本身, 数据变更回滚]
inputs: [{name: datasets+params, }, {name: MetaStore, from: STORAGE-001}]
outputs: [{name: ResearchSnapshot/reproduce, contract: D07 §3}]
public_api: D07 §3 SQL + contextmanager 签名
data_contract: snapshot_id/datasets_json/output_hash 语义（架构 02 §5）
file_ownership: [src/chronoforge/research/snapshot.py, src/chronoforge/storage/migrations/0002_snapshot.py, tests/integration/test_snapshot.py]
dependencies: [STORAGE-001, QUERY-001]
tests: {integration: [TC-R-004], boundary: [空 datasets], failure: [hash 不一致报告版本变化]}
acceptance:
  - Given 同 snapshot 两次 reproduce Then hash 一致
  - Given 数据更新后 reproduce Then hash 不一致且报告 dataset_version 变化
```

## 8. CLI 域

### CLI-001 命令树 + Settings + 脱敏

```yaml
task_id: CLI-001
title: typer 命令树、Settings 多环境、structlog + redact
design_refs: [D08 全文, D01 §4]
responsibility: 8 组命令（薄层）、Settings 全字段表、日志词表、redact 递归
scope: [src/chronoforge/cli/, src/chronoforge/config/settings.py, src/chronoforge/logging.py, tests/unit/test_cli.py]
non_goals: [任何业务逻辑/SQL（架构 02 规则 3）, connector 直依赖]
inputs: [各 service Public API]
outputs: [{name: chronoforge CLI, contract: D08 §2 命令树}]
public_api: 命令树逐条；--json 输出 = QueryResult 直序列化
data_contract: Settings 字段表 D08 §1（env 名即契约）
configuration: .env.example 全键名；test 环境强制 tmp 目录
observability: 事件词表 D08 §3（architecture test 校验）
security: redact 键表 D08 §4；SEC 只读 User-Agent
file_ownership: [src/chronoforge/cli/, src/chronoforge/config/, src/chronoforge/logging.py, tests/unit/test_cli.py, .env.example]
dependencies: [全部 service 任务（运行时）；编译期仅类型引用]
tests: {unit: [TC-X-001~004, TC-SEC-001/002/003], boundary: [空参数默认值, --json 空结果], failure: [FRED 无 key→ConfigError, 非法 env 值]}
acceptance:
  - Given .env < 环境变量冲突 When Settings.load Then 环境变量胜出
  - Given 含 api_key 的日志上下文 When 输出 Then 掩码为 ***
  - Given pipeline run --dry-run Then 仅打印 job 计划不落盘
```

## 9. INFRA 域

### INFRA-001 测试脚手架 + fixture + hypothesis + CI

```yaml
task_id: INFRA-001
title: 测试基础设施（conftest/fixture 样例/hypothesis 策略/CI/coverage）
design_refs: [D09 全文（修复后）, review spec §A G7]
responsibility: tests/ 骨架、settings fixture（test 环境 tmp）、fixture 目录规范、hypothesis 策略库（OHLCV/记录生成器）、CI 5 步、覆盖率门槛
scope: [tests/conftest.py, tests/strategies.py, .github/workflows/ci.yml, tests/fixtures/README]
non_goals: [具体业务用例（各任务自带）]
inputs: [D09 §5 fixture 清单]
outputs: [{name: 共享 fixtures/strategies, }]
public_api: conftest 的 settings/tmp_stores fixture；strategies.ohlcv()/records()/trade_seq()
data_contract: fixture 三件套规范（happy/edge/error JSON，脱敏）
file_ownership: [tests/conftest.py, tests/strategies.py, .github/workflows/ci.yml, tests/fixtures/README]
dependencies: []
tests: {self: [策略生成合法性 property, CI 本身跑通]}
acceptance:
  - Given CI When push Then 5 步全绿（ruff/mypy/pytest/lint-imports/pip-audit 周期）
  - Given strategies.ohlcv() 生成 1000 例 Then 全部通过 MODEL-002 校验或显式非法标记
  - Given pytest -m "not smoke" Then 本地与 CI 结果一致（无网络依赖）
```

## 10. 派发顺序（依赖 DAG 拓扑层，同层可并行）

```
L0: MODEL-001, INFRA-001
L1: MODEL-002, MODEL-003, STORAGE-001, STORAGE-002
L2: STORAGE-003, ACQUISITION-001, VALIDATION-001
L3: STORAGE-004, STORAGE-005, ACQUISITION-002, VALIDATION-002, DATA-SOURCE-001~007
L4: PIPELINE-001, QUERY-001
L5: QUERY-002, QUERY-003
L6: CLI-001（集成联调 + architecture test 全量生效）
```

接手测试（review spec §C 判断标准）：任一任务单的 Agent 中途停止，另一 Agent 可凭该单 + design_refs 所引文档 + 已有契约测试无缝接管。
