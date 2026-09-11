# Design Audit Report — docs/design/ D01–D09

- 日期：2026-09-11
- 依据：`00-design-review-and-task-decomposition-spec.md`（Part A 清单 / Part C 格式）
- 对象：docs/design/01–09 全部九份文档
- 方法：逐组核查 Part A CORE/REQ 条目，证据指向具体设计文档章节

---

## A. Overall Result

**FAIL** —— 存在 4 项 BLOCKER（F-01~F-04），均集中在增量获取链路（Spec G3/G4/G5），按 howto §44 属必须 FAIL 项（增量机制不完整 / 数据可能被错误覆盖 / checkpoint 语义缺口）。**所有问题均有明确、局部的修复路径，无需重构**；修复后预计可达 PASS WITH WARNINGS。

## B. Architecture Findings

| ID | 严重度 | Finding | 证据 | Impact | Required Action |
|---|---|---|---|---|---|
| F-01 | **BLOCKER**（C-G3-07/08） | Backfill 与 Incremental 未统一：无 AcquisitionJob 的 start/end/mode 表达，无 chunk 拆分与 chunk 级成败记账；run 是 dataset 级整体 | D05 §1-2 | 历史 backfill 无法表达为任务；大窗口失败重试粒度过粗 | D05 增加 `AcquisitionJob{dataset_id, start, end, mode}`；fetch 内滚动窗口即 chunk，run_log 增 chunk 计数（成功/失败 chunk 分列），单 chunk 失败→PARTIAL+cursor 推进至最后 durable chunk |
| F-02 | **BLOCKER**（C-G3-06） | 增量窗口无 overlap 设计；API 时间边界包含性未逐源声明（Binance startTime 是否含端点、未收盘 K 线边界） | D05 §5；D04 §4.1 | 边界记录可能重复或丢失（silent gap） | D05 §5 表增 `overlap_window` 列（默认 1×interval，依据：交易所最后 K 线未闭合会重写）与 `boundary_semantics` 列（含/不含）；重叠由 natural key upsert 消除（已有） |
| F-03 | **BLOCKER**（C-G5-03） | PARTIAL_SUCCESS 时 cursor 推进语义未定义："FetchStage 完成"在部分失败时含义模糊，可能 checkpoint > durable_valid_data | D05 §3 | 违反 Invariant 1，断点续传产生缺口 | 定义：cursor = 最后一个**已落盘且通过校验** chunk 的右边界（exclusive）；PARTIAL 亦按此推进；失败 chunk 之后窗口不推进 |
| F-04 | **BLOCKER**（C-G4-04/05） | 非 revision 类型（OHLCV 等）值漂移被 upsert keep-last **静默覆盖**：无检测、无告警、无版本保留 | D03 §3 算法 | 历史数据被源修正后旧值无痕丢失，违反 Invariant 7 可追溯 | merge-rewrite 增 value-diff 检测：同 natural key 字段值变化 → 新规则 `Q-DRIFT-001`（WARNING）+ 旧值/新值入 finding detail；P0 不做多版本存储（记录为 PROVISIONAL） |
| F-05 | HIGH（C-G4-08） | TRADE 类 sequence 连续性校验缺失：aggTrades 的 id 为归集首笔 id（跳号正常），当前无任何对账规则 → silent loss 不可检测 | D04 §4.1；D06 | HTTP 200 但少页/漏记录时任务仍 SUCCESS | D06 增 `Q-SEQ-001`：aggTrades 按「窗口内首末 id + 归集聚合数」对账；D04 §4.1 声明该端点 id 语义，或改用 `/api/v3/trades`（raw id 连续）并说明选择依据 |
| F-06 | HIGH（C-G4-01） | Expected/Unexpected Gap 未区分：Q-GAP-001 对 yahoo（周末/节假日休市）会持续误报 WARNING；合约到期、未上市无表达 | D06 §2；D02 | 告警噪音淹没真实缺口；违反 Invariant 6 | dataset_registry 增 `continuity_model` 列（enum: ALWAYS_OPEN_24_7 / TRADING_CALENDAR / EVENT_BASED / RELEASE_SCHEDULE）；TRADING_CALENDAR 类引入交易日历依赖（P0 仅美股周末日历，简化版）；expected gap 标记 `EXPECTED_GAP` 不产生 WARNING |
| F-07 | HIGH（C-G1-01/02） | 模块未按 18 要素任务单格式成文（Non-goals 仅 connectors 有） | 全部 | 无法直接派发 Agent | 按 Spec Part B 生成 Task Manifest（本报告 §H），每单补齐 Non-goals/Inputs/Outputs 等 |
| F-08 | MEDIUM（C-G4-06） | Quarantine 未显式设计：INVALID 记录仅写 quality_flags，detail 无 request/raw 引用字段（raw_record_id 在 canonical 记录上，被拒记录无载体） | D06 §3 | 被拒数据事后无法定位原始请求上下文 | quality_flags 增列 `raw_ref`（jsonl 路径+行号）与 `payload_digest`；明确 quarantine = raw 层原文 + flags 记录的组合语义 |
| F-09 | MEDIUM（C-G2-03） | 源侧时间精度与转换规则未逐源定义：Binance ms→us 截断/舍入、FRED 日期型 observation→UTC 时刻、SEC accepted_datetime 带 EDT 后缀、Yahoo epoch 秒 | D01 §4；D02 | 隐式转换 = Agent 自行决策（违反 C-G1-03） | D04 §4 各源表增 `time_semantics` 行；统一规则：ms 一律 ×1000 无舍入损失；FRED 日期→`T00:00:00Z`；带 tz 后缀字符串→先转 UTC 再去 tzinfo |
| F-10 | MEDIUM（C-G4-09） | Dataset 级状态机缺失（COMPLETE/PARTIAL/STALE/...），仅有 run 级状态 | D03 §1；D05 §3 | 消费者无法判断数据可用性/新鲜度 | dataset_registry 增 `status` 列；推导规则：最新 run SUCCESS 且无 ERROR finding→COMPLETE；超 frequency 周期无成功 run→STALE；其余→PARTIAL |
| F-11 | MEDIUM（C-G8-01） | run_log 缺 7 字段：request_count/retry_count/duplicate_count/missing_count/latency_ms/checkpoint_before/checkpoint_after | D03 §1 | 无法回答"任务成功但数据是否完整"（howto §34） | run_log DDL 扩列（仍在 0001 迁移阶段，零成本）；StageResult 增对应计数；checkpoint 前后值由 FetchStage 记录 |
| F-12 | REC（C-G5-06） | Compaction 未设计：长期运行 raw JSONL 分片与 parquet 小文件累积 | D03 | 文件数膨胀影响查询与备份 | 记录 PROVISIONAL：触发条件 = 单分区文件数 > 64 时合并；P0 不实现 |
| F-13 | MEDIUM（C-G7-02/03） | Boundary 测试缺 leap day/DST/year boundary（D09 仅"跨月边界"）；Property-Based Testing 完全缺失 | D09 §3/§5 | 不变量未用性质测试锚定 | D09 增 hypothesis 组：dedup 幂等律、acquire(A,B)+acquire(B,C)≡acquire(A,C)、OHLC 四不等式；boundary 用例补齐 howto §29 清单 |
| F-14 | MEDIUM（C-G6-02） | checkpoint 损坏（SQLite 文件损坏）恢复路径未写明 | D03 §5；D05 | 元数据损坏后系统停摆 | 恢复路径：从 raw 层按 dataset 重放重建 checkpoints 表（replay 已有基础设施，补文档一段） |
| F-15 | LOW（C-G4-02） | Q-RANGE 未显式拒绝 NaN/Inf（Pydantic float 默认接受 NaN，`>0` 校验对 NaN 恒 False 可拦截但未声明） | D02 §5；D06 §2 | NaN 可能入 canonical | D02 校验器显式 `math.isfinite()`；D06 Q-RANGE-001 描述补 NaN/Inf |
| F-16 | LOW（C-G2-04） | 查询默认排序规则未声明（QueryService 未定义 ORDER BY） | D07 §1 | Agent 自行决定排序，结果不稳定 | QueryService 规则 5 增：默认按主时间列升序，稳定排序 |
| F-17 | INFO（C-G5-01） | 四层建议（Raw/Validated/Canonical/Derived）映射为三层 + quality_flags，Validated 职责由 ValidateStage+flags 承担 | 架构 04；D03 | 无（设计等价，需声明） | 在 D03 §1 增一段映射声明（本次审计确认等价性成立） |
| F-18 | INFO（C-G3-02） | checkpoints 表未按 howto §6.2 全字段展开（instrument/timeframe 编码于 dataset_id） | D03 §1 | 无（编码已定且可解析） | D03 增映射声明：`dataset_id = {source}.{instrument}.{dataset_type+timeframe}` |

## C. Module Independence

| Module | 独立实施 | 契约完备 | 可测试 | 状态 |
|---|---|---|---|---|
| models | ✓ | ✓（D02 字段级） | ✓（TC-M） | ready |
| config/logging | ✓ | ✓（D08 全字段表） | ✓（TC-X） | ready |
| storage/raw | ✓ | ✓（D03 §2） | ✓ | ready |
| storage/meta | ✓ | ◐（缺 F-10/F-11 列） | ✓ | blocked-by F-10/F-11 |
| storage/canonical | ✓ | ◐（F-04 值漂移） | ✓（TC-S） | blocked-by F-04 |
| storage/views | ✓ | ✓（D03 §4 SQL） | ✓ | ready |
| connectors/base+ratelimit | ✓ | ✓（D04 §1-3 签名级） | ✓ | ready |
| connectors/{7 源} | ✓ | ◐（F-09 时间语义） | ✓（fixture 契约） | blocked-by F-05/F-06/F-09 |
| quality | ✓ | ◐（F-06/F-08/F-15） | ✓（TC-Q） | blocked-by F-05/F-06/F-08 |
| pipeline | ◐（F-01/02/03） | ◐ | ✓（TC-P） | blocked-by F-01/02/03 |
| registry | ✓ | ✓ | ✓ | ready |
| features/research | ✓ | ✓（D07） | ✓（TC-R） | ready |
| cli | ✓ | ✓（命令树） | ✓（TC-X-003） | ready |

## D. Data Acquisition Audit

| Capability | Required | Designed | Test Defined | Status |
|---|---|---|---|---|
| Incremental | ✓ | ◐ cursor 有，语义属性 8 项未声明（C-G3-01） | ◐ | **GAP**（F-02/F-09） |
| Checkpoint | ✓ | ◐ 表有，PARTIAL 推进语义缺口 | ◐（缺回退/丢失/损坏场景） | **GAP**（F-03/F-14） |
| Resume | ✓ | ✓（cursor 续传） | ◐（TC-P-005 部分） | PARTIAL |
| Idempotency | ✓ | ✓（natural key upsert） | ◐（缺 8 场景中 5 项，D09） | PARTIAL（F-13） |
| Pagination | ✓ | ◐（滚动窗口有，边界包含性无） | ✓（TC-C-011） | **GAP**（F-02） |
| Deduplication | ✓ | ✓（merge-rewrite） | ✓（TC-S-001/002） | PASS |
| Continuity | ✓ | ◐（Q-GAP-001 有，expected/unexpected 不分） | ◐ | **GAP**（F-06） |
| Retry | ✓ | ✓（退避+分类，D04 §3） | ✓（TC-C-008~010） | PASS |
| Rate Limit | ✓ | ✓（令牌桶+权重+保守 yahoo） | ◐ | PASS（补 retry_count 记账 F-11） |
| Revision | ✓ | ◐（FRED/COT 有；非 revision 漂移无检测） | ✓（TC-M-007） | **GAP**（F-04） |
| Backfill 统一 | ✓ | ✗（无 mode/chunk 表达） | ✗ | **GAP**（F-01） |

## E. Data Integrity Audit

| Check | Designed | Tested | Status |
|---|---|---|---|
| Schema | ✓（Pydantic + Q-SCHEMA-001） | ✓（TC-M-002/003） | PASS |
| Primary/Natural Key | ✓（D02 身份键 + D03 §3） | ✓（TC-S-006） | PASS |
| Duplicate | ✓（Q-DUP-001 + upsert） | ✓ | PASS |
| Missing | ✓（Q-GAP-001/002） | ✓（TC-Q-001~003） | PARTIAL（F-06 误报） |
| Ordering | ✓（Q-TS-002） | ◐ | PASS |
| OHLC consistency | ✓（Q-RANGE-001） | ✓ | PASS（补 NaN/Inf F-15） |
| Timestamp | ◐（UTC 定，源侧转换规则缺） | ◐ | GAP（F-09） |
| Precision | ◐（us 定，源侧精度未声明） | ✗ | GAP（F-09） |
| Atomicity | ✓（temp→rename 协议 D03 §5） | ✓（TC-S-004/007） | PASS |
| Checkpoint consistency | ◐（顺序对，PARTIAL 语义缺） | ✗ | **GAP**（F-03） |
| Silent loss 防线 | ◐（OHLCV gap 有；TRADE 序列无） | ✗ | **GAP**（F-05） |

## F. Test Coverage Audit

| 类别 | 覆盖 | 缺口 |
|---|---|---|
| Normal Path | ✓（TC-P/M/C happy） | — |
| Boundary | ◐ | leap day/DST/year boundary/max-min page size（F-13） |
| Failure | ✓（错误类映射 + 注入） | — |
| Recovery | ✓（TC-S-004、TC-P-004/006） | cursor 回退/丢失/损坏 3 场景（F-13/F-14） |
| Concurrency | ✓（TC-S-003 锁） | — |
| Idempotency | ◐（upsert 幂等） | acquire 区间拼接律（property-based，F-13） |
| Data Integrity | ✓（TC-Q 组 + quality marker） | Q-SEQ-001（F-05） |
| Integration/Contract | ✓（TC-C 契约 + TC-R） | — |

## G. 系统不变量映射（howto §46）

| Invariant | 设计落点 | 判定 |
|---|---|---|
| 1 Checkpoint Safety | D03 §5 顺序 + D05 §3 | ◐ PARTIAL 语义缺口（F-03） |
| 2 Idempotency | natural key upsert | ✓ |
| 3 No Silent Loss | Q-GAP + 计数对账 | ◐ TRADE 序列缺（F-05） |
| 4 No Duplicate Canonical | natural key + Q-DUP-001 | ✓ |
| 5 Temporal Ordering | Q-TS-002 | ✓ |
| 6 Continuity + EXPECTED_GAP | Q-GAP-001 | ◐ expected gap 未区分（F-06） |
| 7 Reproducibility | replay + dependencies + snapshot | ✓（值漂移破坏历史可溯，F-04） |

## H. Task Manifest（按 Spec Part B 生成；状态 = ready / blocked-by）

| Task ID | Title | 设计依据 | Deps | 状态 |
|---|---|---|---|---|
| MODEL-001 | BaseRecord + 枚举 + provenance | D02 §1 | — | ready |
| MODEL-002 | 27 Canonical Type schema | D02 §2 | MODEL-001 | ready |
| MODEL-003 | InstrumentResolver 三级 ID | D02 §3 | MODEL-001 | ready |
| STORAGE-001 | MetaStore + 迁移 0001（含 F-10/F-11 扩列） | D03 §1 | MODEL-001 | blocked-by F-10/F-11 |
| STORAGE-002 | RawStore JSONL | D03 §2 | MODEL-001 | ready |
| STORAGE-003 | CanonicalStore merge-rewrite（含 F-04 漂移检测） | D03 §3 | MODEL-002 | blocked-by F-04 |
| STORAGE-004 | DuckDB 视图层 | D03 §4 | STORAGE-003 | ready |
| STORAGE-005 | 原子性协议 + reconciliation | D03 §5 | STORAGE-002/003 | ready |
| ACQUISITION-001 | Connector 协议 + 限流器 + 重试 | D04 §1–3 | MODEL-002 | ready |
| ACQUISITION-002 | AcquisitionJob + chunk + overlap + cursor 语义 | D05（修复后） | ACQUISITION-001, STORAGE-001 | blocked-by F-01/F-02/F-03 |
| DATA-SOURCE-001~007 | binance_spot / binance_futures / deribit / ccxt_bridge / yahoo / fred / sec_edgar | D04 §4 | ACQUISITION-001 | yahoo blocked-by F-06；binance_spot blocked-by F-05；其余 blocked-by F-09 |
| VALIDATION-001 | 规则引擎 + 14+N 规则（含 Q-SEQ/Q-DRIFT） | D06（修复后） | MODEL-002 | blocked-by F-05/F-08/F-15 |
| VALIDATION-002 | Continuity Model + 交易日历 + dataset 状态 | F-06/F-10 方案 | VALIDATION-001 | blocked-by F-06 |
| PIPELINE-001 | Runner 七阶段 + 状态机 + replay | D05 | ACQUISITION-002, STORAGE-005, VALIDATION-001 | blocked-by F-01~03 |
| QUERY-001 | QueryService（as-of + 排序规则） | D07 §1 | STORAGE-004 | blocked-by F-16 |
| QUERY-002 | FeatureEngine + 5 特征 | D07 §2 | QUERY-001 | ready |
| QUERY-003 | ResearchSnapshot | D07 §3 | STORAGE-001 | ready |
| CLI-001 | 命令树 + Settings + 脱敏 | D08 | 各 service | ready |
| INFRA-001 | 测试脚手架 + fixture + hypothesis + CI | D09（修复后） | — | blocked-by F-13 |

共 25 单（DATA-SOURCE 拆 7 单）；ready 12，blocked 13（全部 blocked 项的解锁条件 = 对应 finding 修复落图）。

## I. 结论与修复路径

1. **判定 FAIL**：F-01~F-04 四项 BLOCKER 位于同一链路（AcquisitionJob → 窗口语义 → cursor 语义 → upsert 安全），**集中修复 D03 §3 / D05 §1-3-5 / D04 §4.1 / D06 §2 四处即可整体解锁**，不涉及架构层变更（architecture/ 文档无需改动——四项均为设计层细化，未违背架构决议）。
2. 修复顺序建议：F-02/F-03（窗口与 cursor 语义，其他项依赖它）→ F-01（Job/chunk）→ F-04（漂移检测）→ F-05/F-06（校验规则）→ F-07~F-16（MEDIUM/LOW 批量）。
3. 修复完成后按本 spec 复审（预计 PASS WITH WARNINGS：F-12 compaction、值漂移多版本保留两项 PROVISIONAL 遗留），即可按 §H Manifest 拓扑序派发全部任务单。
4. 接管性测试（howto §47）：models/storage-raw/views/registry/features/cli 七模块当前即满足"陌生 Agent 仅凭文档可实施"；connectors/pipeline/quality 须待对应 finding 修复。
