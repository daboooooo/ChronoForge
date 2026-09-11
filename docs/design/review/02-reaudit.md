# Design Re-Audit Record — F-01~F-16 修复确认

- 日期：2026-09-11
- 依据：`01-design-audit-report.md` §I 修复路径，按序执行完毕后的验证性复审
- 方法：逐 finding 核对修复落点（文件/章节），并复核 FAIL 判定条件是否全部解除

## 1. Findings 修复确认表

| ID | 严重度 | 修复落点 | 状态 |
|---|---|---|---|
| F-01 | BLOCKER | D05 §2 AcquisitionJob（mode/priority）+ FetchStage chunk 化 + run_log chunk 计数（D03 §1）+ TC-P-010 | ✅ FIXED |
| F-02 | BLOCKER | D05 §5.2 boundary_semantics + overlap_window 列（含依据）+ Q-TS-003 未收盘 K 线丢弃（D06 §2） | ✅ FIXED |
| F-03 | BLOCKER | D05 §3 cursor 推进不变量（durable chunk 右界）+ 回退防护 + RunLogStage 语义 | ✅ FIXED |
| F-04 | BLOCKER | D03 §3 drift 检测算法 + UpsertStats.drifted + Q-DRIFT-001（D06 §2）+ TC-Q-009/值漂移用例 | ✅ FIXED（多版本保留 PROVISIONAL） |
| F-05 | HIGH | Q-SEQ-001（D06 §2）+ aggTrades id 语义声明（D04 §4.1）+ §5.1 cursor 八属性表 + TC-Q-008 | ✅ FIXED |
| F-06 | HIGH | dataset_registry.continuity_model 列（D03 §1）+ Q-GAP-001 按 model 判定 + EXPECTED_GAP（D06 §2）+ VALIDATION-002 任务单（简化日历）+ 各源 continuity_model 行（D04 §4）+ TC-Q-007 | ✅ FIXED（正式日历库 P1 评估） |
| F-07 | HIGH | D10 任务清单：25 单全部按 Spec Part B 18 要素成文 + 派发 DAG | ✅ FIXED |
| F-08 | MEDIUM | quality_flags.raw_ref/payload_digest 列（D03 §1）+ Quarantine 语义段（D06 §3） | ✅ FIXED |
| F-09 | MEDIUM | 七源时间语义行（ms→us / 秒→us / 日期→T00:00:00Z / 保守 T23:59:59Z / EDT→UTC，D04 §4）+ §5.1 精度声明 | ✅ FIXED |
| F-10 | MEDIUM | dataset_registry.status 列 + 状态推导规则（D03 §1，RunLogStage 执行） | ✅ FIXED |
| F-11 | MEDIUM | run_log 7 观测列（D03 §1）+ StageResult 对应字段（D05 §1） | ✅ FIXED |
| F-12 | REC | D03 §8 Compaction PROVISIONAL（触发条件明确） | ✅ FIXED（PROVISIONAL） |
| F-13 | MEDIUM | TC-PROP-001~004（hypothesis）+ TC-C-016 边界 + TC-P-009/011 恢复与幂等 8 场景（D09 §3）+ INFRA-001 strategies | ✅ FIXED |
| F-14 | MEDIUM | D03 §5.5 rebuild_checkpoints 恢复路径 + TC-P-009 | ✅ FIXED |
| F-15 | LOW | D02 §5 isfinite 显式校验 + Q-RANGE-001 NaN/Inf（D06 §2） | ✅ FIXED |
| F-16 | LOW | D07 §1 规则 6 默认稳定排序（不可关闭） | ✅ FIXED |
| F-17 | INFO | D03 §1 分层映射声明（四层→三层+flags 等价） | ✅ DECLARED |
| F-18 | INFO | D03 §1 dataset_id 映射声明 | ✅ DECLARED |

## 2. 复审结论

**PASS WITH WARNINGS** —— howto §44 的 15 项 FAIL 条件全部解除：增量机制（cursor 八属性/overlap/boundary）、checkpoint 语义（durable 边界不变量）、连续性（continuity_model 四态）、唯一性（natural key + drift 检测）、失败恢复（14 类映射 + rebuild）、存储原子性（temp→rename→对账）、Backfill/Incremental 统一（AcquisitionJob 单引擎）、silent loss 防线（Q-GAP/Q-SEQ/观测计数）、错误覆盖（Q-DRIFT-001）均有设计与测试锚点。

**Warnings（PROVISIONAL 遗留，非阻塞，均已声明触发条件与升级路径）**：

| ID | 遗留项 | 触发条件 |
|---|---|---|
| W-01 | 值漂移多版本保留（F-04 关联） | 漂移频次数据积累后评估版本化存储 |
| W-02 | 正式交易日历库（F-06 关联） | P1：简化日历（周末+固定节假日）出现误判时 |
| W-03 | Compaction（F-12） | 单分区文件数 >64 / raw 日分片 >256 |
| W-04 | APScheduler（架构 06 D10） | P1 调度需求明确时补 ADR |

## 3. Task Manifest 状态更新

`01-design-audit-report.md` §H 的 13 个 blocked 任务单**全部解锁**（修复落图后其设计依据完整）。最终清单见 `docs/design/10-task-manifest.md`：25 单、7 层派发 DAG、每单含 Non-goals/契约/测试/验收（Spec Part B 18 要素）。

## 4. 接管性判定（howto §47）

- 陌生 Agent 仅凭任务单 + design_refs 所引章节 + D01 公共约定：**可实现、可通过全部边界/失败/恢复测试**（各单 acceptance 均为机器可验证 GWT）
- 中途换 Agent 接管：契约 = Public API + D09 用例 ID + fixture 三件套规范，**可无缝接管**

设计文档集（D01–D10）自此进入可派发状态；后续变更走 review spec §A/架构 §85 冻结规则。
