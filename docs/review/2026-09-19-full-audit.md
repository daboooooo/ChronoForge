# 完整审计报告 — 文档 / 实现代码 / 测试代码

- **日期**: 2026-09-19
- **审计依据**: `docs/编码钱最终整体审计审计要求.md`(§ 编号均指该文档)
- **审计对象**: docs/(architecture + design D01–D10)、src/chronoforge(已实现 L0–L3 全部任务)、tests/
- **审计方法**: 源码逐文件精读(存储层/pipeline/connectors 基础设施/security)+ 3 个并行子审计(测试覆盖、文档一致性、quality/connectors 逐源抽查)+ 实测基线(pytest/ruff/mypy)

---

## 1. Executive Summary

**Overall Status: NOT READY**

基线实测:**1033 passed / 3 failed**(128s);ruff **109 errors**(60 可自动修复);mypy **37 errors**(7 文件)。这与派发台账多任务声称的"全绿 / ruff+mypy 无错误"不符——存在验收后改动未回归的情况。

发现 **4 项 CRITICAL**(其中 3 项集中在 CanonicalStore,构成一条"静默数据丢失"链)、**7 项 HIGH**。存储层的身份键映射与冻结设计(D02)存在系统性偏离,是最优先修复项。PIPELINE-001(未实现)在当前存储契约下直接开工会把 C-1/H-2 固化成生产数据丢失。

| 严重度 | 数量 |
|---|---|
| CRITICAL | 4 |
| HIGH | 7 |
| MEDIUM | 13 |
| LOW | 若干(未逐条列出) |

---

## 2. Critical Findings

| ID | 区域 | Finding | 影响 | 必要动作 |
|----|------|---------|------|----------|
| **C-1** | 存储契约 | `storage/base.py:64-110` `_NATURAL_KEY_MAP` 与冻结设计/模型层身份键系统性不一致:① OHLCV 缺 `interval`(D02 §2 L43 明确"身份 = market_id+event_time+**interval**");② TRADE 用 `event_time` 而非 `trade_id`(models/market.py:92 为 `(market_id, trade_id)`);③ ORDERBOOK 缺 `transaction_time`;④ LIQUIDATION_AGGREGATE 缺 `interval`;⑤ TEXT_MESSAGE 缺 `author`;⑥ TEXT_EVENT 缺 `gkg_themes`;⑦ DERIVED/FEATURE 缺 `computed_at` | 非 revision 类 upsert 走 keep-last 去重(storage/canonical.py:834-859)。同一 event_time 的 1m/1h/1d K 线互相合并;同毫秒多笔成交只留最后一笔;同名 DERIVED 指标不同计算时刻被覆盖——**系统性静默数据丢失**(§19/§55) | 以 D02 §2 为唯一基准重写 `_NATURAL_KEY_MAP`,并为该映射建立"模型层 natural_key ↔ 存储层映射"一致性架构测试 |
| **C-2** | Canonical 原子性 | `storage/canonical.py:613-619` `_read_all_old` 读旧 parquet 失败时 `except Exception: continue`,损坏文件被静默跳过后分区被 merge-rewrite **整体替换** | HTTP 200 + 正常退出 + checkpoint 推进 + 数据实际缺失的典型 Silent Data Loss 路径(§19 CRITICAL 定义) | 读取失败必须 raise StorageError 熔断,禁止静默降级 |
| **C-3** | Canonical 原子性 | `storage/canonical.py:273-278` `_atomic_rename` 先 `rmtree(target)` 再 `rename(tmp→target)`。两步之间存在崩溃窗口:分区已删、新数据尚未就位 | 崩溃后分区数据为空;D03 §3 声明的 "p.old 先 mv 再删" 交换协议未实现(§15/§24 No partial commit) | 实现 rename-swap:target→`.old-*`,tmp→target,成功后删 `.old-*` |
| **C-4** | 恢复机制 | `storage/consistency.py:58-67` `cleanup_orphans` 只扫描 **month 目录的子目录**;而 `.tmp-*` 实际创建在 **year 层**(canonical.py:478/573 `part_path.parent`) | 孤儿目录永远清不到;startup_repair 的清理步骤完全无效,与 C-3 叠加后崩溃数据无自动恢复路径(§14/§36) | 修正扫描层级,并补"留下 .tmp 后跑 cleanup"的集成测试(现有 TC-S-007 测试的是 temp 文件在 month 层的错误布局,测试与实现一起错了) |

---

## 3. High Findings

| ID | 区域 | Finding | 证据 |
|----|------|---------|------|
| **H-1** | 流程/回归 | 当前 HEAD 3 个失败测试:`test_derivatives_types.py:64`(Interval 枚举新增 15m/30m/2h…1w 后断言未更新)、`test_windows.py:85/86`(frozen dataclass 抛 `FrozenInstanceError`,测试期望 `TypeError`——实现由 pydantic 改 dataclass 后未回归) | pytest 实测;说明验收后代码被改动且未重跑测试,验收门失效 |
| **H-2** | Checkpoint Lineage | `storage/meta.py:313-334` `finish_run` 接受 `source_id` 参数但 SQL **不更新 source_id**;且 `try_lock_dataset:245` 插入 `source_id=''` → run_log.source_id 恒为空;`rebuild_checkpoints:564` 从 run_log 读 source_id 回填 checkpoints → 写出 `source_id=''` 的孤儿 checkpoint,`get_checkpoint(真实source_id,…)` 永远取不到 | 违反 Data Lineage(§26)与 Checkpoint Safety(§14) |
| **H-3** | Drift 可观测 | `storage/canonical.py:557-604` upsert 路径中 `drift_findings` 只保留 `len()`,DriftFinding(old/new digest、changed_columns)被丢弃,无任何落盘/上报 | Q-DRIFT-001 检测结果不可用,违反 D06 findings 流程与 §31 |
| **H-4** | Schema 契约 | `storage/canonical.py:38-107` Arrow schema 按批内值推断而非按冻结契约声明:int64/float64/string/timestamp 随批次值漂移;`_align_schemas:733-788` 的 cast 补丁即此问题的症状 | 违反 Data Contract Freeze(§9)、Reproducibility(§55);concat 不兼容类型时 upsert 直接失败 |
| **H-5** | 对账语义 | `storage/consistency.py:75-83,146-223` reconcile 用 parquet mtime 推 `HHmmss` 当 ingest_batch_id,与真实 uuid batch_id(meta.py:223)**永不匹配**;并写入 `source_id=''、dataset_id=''` 的伪造 SUCCESS/FAILED run_log 行 | run_log 审计痕迹被污染(§26/§31);对账机制语义性失效 |
| **H-6** | 状态机 | `storage/meta.py:401-443` quality_flags 无 resolved/处理机制(INSERT OR REPLACE 累积),`derive_dataset_status:469-475` 规则 1 统计全部历史 ERROR flags | 数据集一旦产生 ERROR flag 永远 INCOMPLETE,无法恢复(§48 可运维性) |
| **H-7** | 锁恢复 | `storage/meta.py:229-238` `try_lock_dataset` 检查 RUNNING/PENDING 行;进程崩溃后 PENDING 行永久残留 → 数据集**永久锁死**;无租约/超时/启动清理,reconcile 亦不处理孤儿 run | §48 错误任务可停止/可恢复要求;PIPELINE-001 实现后将首当其冲 |

---

## 4. Medium Findings(摘要)

| ID | Finding | 证据 |
|----|---------|------|
| M-1 | ruff 109 errors / mypy 37 errors 与台账"无错误"矛盾,验收门未执行 | 实测 |
| M-2 | `pipeline/windows.py:107-165` WINDOW_CONFIG 与已实现数据集不一致:`binance_spot_funding` 不存在(spot 无 funding)、`binance_futures_open_interest` 缺失、ccxt 数据集未注册 → plan_chunks 抛 unknown dataset_id | windows.py vs connectors |
| M-3 | `plan_chunks` 构造的 FetchRequest.params 恒为 `{}`,AcquisitionJob 无 params 字段 → dataset_registry.params_json(symbol/interval)无处传递 | windows.py:293,317 |
| M-4 | WINDOW_CONFIG 的 `"boundary": "both_ends"` 为死配置,代码从不读取 | grep 全库仅定义处 |
| M-5 | 无时间字段/解析失败的记录静默落入硬编码默认分区 `("2026","01")`(canonical.py:386-388);ENTITY/INSTRUMENT 的 partition_time_field 为 `canonical_name`/`instrument_id`(base.py:180-181)注定解析失败 | 错误分区静默写入 |
| M-6 | `_add_revision_seq` 批内同一时间戳 + pandas 非稳定排序 → 同批重复 NK 的 keep-last 结果不确定 | canonical.py:671-677,840-859 |
| M-7 | `_count_changes` 按行统计,批内重复 NK 重复计数;`new_nk_set` 构建后未使用 | canonical.py:699-719 |
| M-8 | retry 不消费 `RateLimitError.retry_after`(Retry-After 未整合进退避);无 timeout/cancellation | retry.py:64 vs D04 §3 |
| M-9 | tests/e2e、tests/quality、tests/architecture 全为空;D09 的 61 个 TC-ID 中 26 个无对应测试(TC-P-001~011/TC-R-*/TC-X-* 属未实现任务,但 TC-S-003、TC-M-002、TC-Q-004/005/006、TC-PROP-001/003 等属当前阶段应覆盖) | 实测目录 + TC 清单比对 |
| M-10 | 缺真实进程崩溃→重启→resume 测试;缺 disk full、checkpoint 损坏注入测试(§35/§36) | 测试扫描 |
| M-11 | STALE 判定仅支持纯数字 frequency,非数字静默跳过(契约未声明) | meta.py:513-525 |
| M-12 | binance_spot normalize 多处静默 `continue`(malformed/未完成 K 线/非有限值)无日志无计数 | binance_spot.py:395-422 |
| M-13 | dataset_registry.status 仅 reconcile 路径更新;QUARANTINED 无任何写入路径,derive 规则 2 为死代码 | consistency.py:232-260, meta.py:479-486 |

---

## 5. 数据完整性就绪矩阵(§59.4)

| 区域 | 状态 | 说明 |
|------|------|------|
| Incremental | ⚠️ | cursor 推进/回退防护逻辑正确(cursor.py),但依存 C-1(overlap 区去重键错误会误删) |
| Checkpoint | ⚠️ | checkpoint≤durable 不变量由 Runner 保证(未实现);H-2 使 rebuild 语义破坏 |
| Resume | ✗ | H-7 孤儿锁 + C-4 恢复失效 |
| Idempotency | ⚠️ | TC-PROP-004 顺序无关性已测,但 M-6 批内不确定性未测 |
| Deduplication | ✗ | C-1 身份键错误 |
| Continuity | ✅ | EXPECTED/UNEXPECTED GAP 区分正确,Q-GAP-001 实现符合"不得假定连续" |
| Completeness | ⚠️ | 依赖 Runner(未实现) |
| Revision | ✅ | REVISION_TYPES 四类 as-of/多版本语义正确 |
| Atomic Commit | ✗ | C-2/C-3/C-4 |
| Recovery | ✗ | C-4/H-5/H-7 |

## 6. 编码代理就绪(§59.5)

L0–L3 任务(MODEL/STORAGE/ACQUISITION/VALIDATION/DATA-SOURCE-001~007)实现与 D02/D03/D04/D06 逐项一致性总体良好(先前 2026-09-14 审计 7 项 finding 全部已修复:IMP-001 POSITION_AGGREGATE ✅、IMP-002 errors.py 9 类 ✅、IMP-003 get_checkpoint→str ✅、IMP-004 try_lock→RunRow ✅、IMP-005 iter_refs 扩展已文档化 ✅、IMP-006 len<2 ✅、IMP-007 ParticipantType ✅)。但 **PIPELINE-001 当前不应派发**:其全部上游依赖中存在 C-1~C-4/H-2/H-5/H-7,M-3(params 传递缺口)更是 PIPELINE-001 的直接前置契约缺口,必须先修或随 PIPELINE-001 一并设计。

## 7. 测试就绪(§59.6)

| 类型 | 覆盖 | 状态 |
|------|------|------|
| Unit | 充分(617 项),含边界(闰年/年界/午夜分区) | ✅(除 3 个失效断言) |
| Integration | 充分(416 项),7 数据源 fixture→canonical 逐字段比对 | ✅ |
| Contract | 模型/connector 协议有;**存储 NK 映射无一致性架构测试** | ⚠️ |
| Failure | 429/404/500/malformed/空结果覆盖好;disk full/checkpoint 损坏缺 | ⚠️ |
| Recovery | temp 残留清理/reconcile 有,但断言的是错误布局(C-4);真实 crash-restart 缺 | ✗ |
| Property | dedup 幂等/顺序无关(TC-PROP-004)有;TC-PROP-001/003 缺 | ⚠️ |
| E2E / Architecture / Quality 层 | 目录为空 | ✗ |

## 8. 文档审计结论

- D03 §6 接口表已与实现对齐(先前 MISMATCH 已闭环),Design Issue 登记表保持为空 ✅。
- 主要文档缺口:**D02 身份键声明(D02 §2)与 storage/base.py 的偏离未走 Design Change Process**——这正是审计要求 §63 禁止的"边写代码边改契约";C-1 修复后需在 D03 §3 补一份 `_NATURAL_KEY_MAP` 的逐类型对照表作为冻结附件。
- 派发台账(01-dispatch-log.md)与实际状态不符(H-1/M-1):验收记录不可信,需补一轮回归并修正记录。

## 9. Final Gate 结论

**NOT READY**(Critical=4, High=7)。

修复优先级建议:
1. **P0(阻塞 PIPELINE-001 派发)**: C-1 → C-2/C-3/C-4 → H-2/H-7(存储正确性 + 恢复链);
2. **P1**: H-1(修复 3 个失败测试并重跑全量)、H-3/H-5、M-2/M-3(WINDOW_CONFIG/params 契约,PIPELINE-001 前置);
3. **P2**: H-4(schema 显式化,可与 C-1 同批)、H-6、其余 M 项建 Issue 指派责任模块。

修复完成并全量回归(含新增 NK 一致性架构测试、crash-restart 恢复测试)后,可复审进入 READY WITH WARNINGS。
