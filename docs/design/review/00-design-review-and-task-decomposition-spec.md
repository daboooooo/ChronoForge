# Design Review Checklist + Agent Task Decomposition Specification

版本：1.0（2026-09-11）
来源：`docs/design/review/howto_review.md`（49 条审计要求）的结构化转写。
用途：① 作为 `docs/design/` 文档的审计清单；② 定义"设计文档 → Coding Agent 任务单"的自动拆解规范。审计通过（或 findings 修复）后，任务单即可直接派发。

---

## Part A — Design Review Checklist

条目 ID：`C-{组}-{序号}`。级别：**CORE**（缺失即 FAIL）、REQ（缺失记 Finding，须修复）、REC（建议）。

### G1 模块可实施性（howto §1/§3/§47）

| ID | 级别 | 要求 |
|---|---|---|
| C-G1-01 | CORE | 每模块具备 18 要素：Module ID/Name/Responsibility/Scope/**Non-goals**/Inputs/Outputs/Dependencies/Public API/Data Contract/State Model*/Error Model/Persistence*/Concurrency*/Configuration/Observability/Tests/Acceptance Criteria（*必要时） |
| C-G1-02 | CORE | Non-goals 显式成文，防止 Agent 职责重叠 |
| C-G1-03 | CORE | 无需 Agent 自行决策关键架构事项（"仅依据设计文档能否实现"测试） |
| C-G1-04 | REQ | 模块可被另一 Agent 接管（contract + 测试 + 文档自足） |

### G2 契约完备性（howto §2.2/§27）

| ID | 级别 | 要求 |
|---|---|---|
| C-G2-01 | CORE | 每契约定义：输入/输出/类型/必填/可选/字段语义/单位/时区/精度/排序规则/唯一键/错误类型/错误处理/幂等性/版本兼容（16 要素） |
| C-G2-02 | CORE | 内部时区 = UTC；timestamp 存储类型与精度（ms/us/ns）明确；禁止隐式转换 |
| C-G2-03 | REQ | 逐源定义源侧时间精度与转换/舍入规则 |
| C-G2-04 | REQ | 查询/写入默认排序规则明确 |

### G3 增量获取（howto §6–§9/§23–§26，最高优先级）

| ID | 级别 | 要求 |
|---|---|---|
| C-G3-01 | CORE | 每数据集定义增量 cursor，且声明 8 项语义属性：类型/单调性/连续性/稳定性/可重复/可跳跃/可回退/跨请求一致性 |
| C-G3-02 | CORE | Checkpoint 记录：source/dataset/instrument/timeframe/partition/last_cursor/last_success_ts/watermark/status/updated_at（允许编码进 dataset_id，但须声明映射） |
| C-G3-03 | CORE | 序列可用时优先 sequence ID 而非 timestamp |
| C-G3-04 | CORE | 幂等：download(A→B)×2 ≡ ×1；覆盖 8 场景（全重复/部分重复/重叠窗口/cursor 回退/cursor 丢失/cursor 损坏/中途退出/断网重试） |
| C-G3-05 | CORE | 断点续传：任意点失败后从 durable 边界续传，不重下全量 |
| C-G3-06 | CORE | 增量窗口含 overlap 设计且 overlap 有依据；API 时间边界包含性逐源声明 |
| C-G3-07 | CORE | Backfill 与 Incremental 统一引擎（同 Validation/Normalization/Storage/Dedup），仅模式不同 |
| C-G3-08 | CORE | Acquisition Job 可表达 source/dataset/instrument/start/end/mode/priority，可拆 chunk，chunk 级独立成败 |
| C-G3-09 | REQ | Pagination 逐源定义：类型/下一页计算/边界包含/重复可能/漏数据可能/max page size/空页行为/cursor 失效行为 |
| C-G3-10 | REQ | Rate limit 逐源定义：limit/weight/burst/concurrency/retry-after；通用引擎不假定统一限速模型 |

### G4 数据完整性（howto §10–§14/§21/§22/§31–§33）

| ID | 级别 | 要求 |
|---|---|---|
| C-G4-01 | CORE | Continuity Model 逐数据集定义（网格/事件/发布节奏），区分 **Expected Gap**（周末/节假日/停市/到期/未上市）与 **Unexpected Gap**（下载失败/pagination bug/解析错误） |
| C-G4-02 | CORE | 正确性 4 类检查：Schema（类型/必填/null/enum）、Temporal（有效性/时区/顺序/重复/未来值）、Numerical（NaN/Inf/负值/精度/溢出）、Domain（OHLC 关系等） |
| C-G4-03 | CORE | Natural Key 逐类型定义；禁止仅用自增 ID |
| C-G4-04 | CORE | Append-only / Upsert / Revision 三者使用场景分界明确；普通任务不得无版本策略地覆盖历史 |
| C-G4-05 | CORE | 非 revision 数据的值漂移（同 key 重拉值不同）必须可检测（flag/告警），禁止静默覆盖 |
| C-G4-06 | CORE | Quarantine 机制：INVALID 数据不入 canonical，保留 source/dataset/request/ts/raw ref/validation error/detected_at |
| C-G4-07 | CORE | Revision 策略：是否允许/如何发现/如何重下/是否保留版本/revision timestamp |
| C-G4-08 | CORE | Silent data loss 防线：expected vs actual（gap/sequence/count/checksum 至少其一） |
| C-G4-09 | REQ | Dataset 级质量状态显式化（COMPLETE/PARTIAL/INCOMPLETE/VALIDATED/INVALID/QUARANTINED/STALE/REVISION_PENDING） |

### G5 存储与原子性（howto §15–§19）

| ID | 级别 | 要求 |
|---|---|---|
| C-G5-01 | CORE | Raw 与 Canonical 分层（Raw/Validated/Canonical/Derived 或等价映射须声明） |
| C-G5-02 | CORE | 原子提交顺序：write temp → validate → atomic commit → update checkpoint；禁止 checkpoint 先于数据 |
| C-G5-03 | CORE | 不变量：checkpoint ≤ durable_valid_data_boundary（含 PARTIAL_SUCCESS 语义） |
| C-G5-04 | REQ | 增量写入不需全数据集重写；分区级重写须有量级论证 |
| C-G5-05 | REQ | Partition strategy 有依据（查询模式/数据量/文件数/compaction 成本） |
| C-G5-06 | REC | Compaction 策略（或 PROVISIONAL + 触发条件） |

### G6 失败恢复（howto §20）

| ID | 级别 | 要求 |
|---|---|---|
| C-G6-01 | CORE | 14 类失败→行为映射：DNS/timeout/429/5xx/4xx/malformed/partial/crash/disk full/db unavailable/checkpoint corrupted/duplicate/gap/checksum |
| C-G6-02 | REQ | checkpoint 损坏的恢复路径（从 Raw 重建） |

### G7 测试（howto §28–§32/§37–§39）

| ID | 级别 | 要求 |
|---|---|---|
| C-G7-01 | CORE | 每任务交付：Implementation + Unit + Integration + Boundary + Failure + Recovery 测试 |
| C-G7-02 | CORE | Boundary 清单覆盖：first/last/exact page & partition boundary/midnight/month/year/leap day/DST/max & min page size |
| C-G7-03 | REQ | 核心模块 Property-Based Testing：dedup 幂等律、acquire(A,B)+acquire(B,C)≡acquire(A,C)、OHLC 不变量 |
| C-G7-04 | CORE | 完整性测试：expected/actual/missing/duplicate/unexpected 记录可自动判别 |
| C-G7-05 | CORE | 模块间 Contract Test（含非法输入）；Mock/fixture 与真实测试并存 |
| C-G7-06 | CORE | Acceptance 为 Given/When/Then 可机器验证格式 |

### G8 可观测性与版本（howto §34–§36）

| ID | 级别 | 要求 |
|---|---|---|
| C-G8-01 | CORE | 每 Job 结构化记录：source/dataset/instrument/start/end/request_count/record_count/duplicate_count/missing_count/retry_count/error_count/latency/checkpoint_before/checkpoint_after/status |
| C-G8-02 | CORE | Replay：Parser 升级后从 Raw 重建 Canonical，不需重新下载 |
| C-G8-03 | REQ | Schema versioning：版本/兼容/迁移策略，禁止 Agent 擅改字段 |

### G9 任务可派发性（howto §4/§40–§42）

| ID | 级别 | 要求 |
|---|---|---|
| C-G9-01 | CORE | 依赖 DAG 无环；下游不反向依赖；Storage 不依赖具体 Source；Query 不依赖外部 API |
| C-G9-02 | CORE | 每模块可经 Mock/Fixture 替代外部依赖独立测试 |
| C-G9-03 | CORE | 文件归属唯一：每个源文件恰属一个任务单，无共享编辑冲突 |
| C-G9-04 | REQ | Git 边界：任务单 → 独立 commit 序列（约定式提交） |

**FAIL 判定**：任一 CORE 条目缺失，或触发 howto §44 所列 15 项之一（含 silent loss / 错误覆盖 / Backfill-Incremental 不一致）。

---

## Part B — Agent Task Decomposition Specification

### 1. 任务命名空间

| 前缀 | 域 | 典型内容 |
|---|---|---|
| `MODEL-*` | 数据模型 | Canonical schema、枚举、InstrumentResolver |
| `DATA-SOURCE-*` | 源适配 | 每个数据源一个任务单（endpoint/auth/分页/限流/cursor/错误映射/normalize） |
| `ACQUISITION-*` | 获取引擎 | AcquisitionJob/chunk/overlap、pipeline 阶段、状态机、replay、限流器、重试 |
| `VALIDATION-*` | 校验 | 质量规则引擎、Continuity Model、quarantine、数据集状态 |
| `STORAGE-*` | 存储 | RawStore、CanonicalStore(merge-rewrite)、MetaStore、视图、原子性/reconciliation |
| `QUERY-*` | 查询研究 | QueryService(as-of)、FeatureEngine、ResearchSnapshot |
| `CLI-*` | 交互 | 命令树、Settings、日志/脱敏 |
| `INFRA-*` | 工程设施 | 测试脚手架、fixture、CI、property-based 套件 |

### 2. 任务单 Schema（每单必填，与 C-G1-01 的 18 要素一一对应）

```yaml
task_id: ACQUISITION-002            # 命名空间-序号
title: 增量获取引擎（chunk 化 + overlap 窗口）
design_refs: [docs/design/05-pipeline.md §5, docs/design/04-connectors.md §2-3]  # 设计依据
responsibility: |                   # 职责（一句话）
scope: |                            # 交付物清单（函数/类/文件级）
non_goals:                          # 明确不做（C-G1-02）
  - 指标计算
  - 存储实现
inputs:  [{name, type, contract_ref}]   # 依赖的上游契约
outputs: [{name, type, contract_ref}]
public_api: |                       # 签名（照抄设计文档）
data_contract: |                    # 引用 D02/D03 字段表或内联
state_model: |                      # 必要时
error_model: |                      # 错误类 + 处理策略
persistence: |                      # 必要时
concurrency: |                      # 必要时
configuration: |                    # Settings 字段引用
observability: |                    # 日志事件 + run_log 字段
file_ownership: [src/..., tests/...]   # 独占文件清单（C-G9-03）
dependencies: [STORAGE-003, DATA-SOURCE-001]  # 仅 Public API 依赖
tests:                              # 对应 D09 TC 编号 + 新增
  unit: [...]; integration: [...]; boundary: [...]; failure: [...]; recovery: [...]
acceptance:                         # GWT 机器可验证（C-G7-06）
  - Given checkpoint=T1000 And source returns T1000..T1100
    When acquisition runs
    Then T1001..T1100 persisted, no duplicates, checkpoint=T1100
definition_of_done: [ ]             # §43 16 项清单
```

### 3. 派发规则

1. **顺序**：按依赖 DAG 拓扑序派发；同层任务单并行（不同 Agent）。
2. **隔离**：Agent 只可 import 依赖任务单声明的 Public API 与 `models`/`config`；禁止触碰他人 `file_ownership`（code review + CI 路径检查）。
3. **状态**：`ready`（可派发）/ `blocked-by:<finding-id>`（审计发现未修复）/ `done`。
4. **Git**：每任务单独立分支与 commit 序列：`feat(storage): implement canonical merge-rewrite`。
5. **DoD**（howto §43，全项满足才算完成）：实现/公共 API/数据契约/错误处理/日志/指标/单测/边界/失败/恢复/集成测试/静态分析/类型检查/无未声明假设/验收通过。

---

## Part C — 审计流程与报告格式

流程：① 按 Part A 逐组核查（证据 = 设计文档章节）；② 产出 Findings（ID/Severity/Finding/Impact/Required Action）；③ 生成 Task Manifest（任务单清单 + 状态）；④ 定级。

报告格式（howto §45 + 扩展）：

- **A. Overall Result**：PASS / PASS WITH WARNINGS / FAIL
- **B. Architecture Findings** 表
- **C. Module Independence** 表（Module/独立实施/契约完备/可测试/状态）
- **D. Data Acquisition Audit** 表（10 能力：Incremental/Checkpoint/Resume/Idempotency/Pagination/Dedup/Continuity/Retry/RateLimit/Revision × Required/Designed/TestDefined/Status）
- **E. Data Integrity Audit** 表（11 检查 × Designed/Tested/Status）
- **F. Test Coverage Audit**（8 类覆盖判定）
- **G. 系统不变量映射**（howto §46 七不变量）
- **H. Task Manifest**（可派发任务单 + blocked 状态）
- **I. 结论与修复路径**

核心判断（howto §47）：陌生 Agent 仅凭设计文档 + 契约 + 测试能否完成并可通过接管测试。
