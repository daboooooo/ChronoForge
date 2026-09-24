# READY-001 — Run 流式化 / 分窗口 backfill（内存治理）

## 派发信息

- 任务单：2026-09-21 服务就绪审计 SR-04（P1，唯一需独立设计的工程项）
- 设计依据：D05 §1（FetchStage「流式，不积内存」）、架构 05；方案对比见本单 §候选方案
- Agent：Orchestrator 主会话直执（沿用 QUERY-001~003 / READY-002~005 惯例）
- 派发时间：2026-09-21
- 状态：DONE（2026-09-22 验收通过）
- 依赖：PIPELINE-001（Runner 七阶段）、STORAGE-002（iter_refs）、ACQUISITION-002（窗口/cursor）

## 审计证据（SR-04 原文要点）

- [runner.py](../../../src/chronoforge/pipeline/runner.py) RunContext 以 `_batches`/`_raw_refs`/`_records`/`_canonical_groups` 四个列表**全量持有**整个 run 的数据；FetchStage 逐 chunk fetch（正确）但结果 append 进 `ctx._batches`；RawAppendStage 同时持有 payload 对象与序列化 bytes（约 2× 峰值）。
- 后果：backfill 数年 1m K 线 / aggTrades（亿级行）必然 OOM；小数据量测试全绿掩盖该问题。

## file_ownership

- `src/chronoforge/pipeline/runner.py`（RunContext 数据流改造）
- `src/chronoforge/pipeline/windows.py`（若采纳方案 B，分窗口循环）
- `tests/integration/test_pipeline.py`（回归 + 新增内存断言用例）

## 候选方案（执行前先出设计对比，择一实施）

| 方案 | 内容 | 代价 |
| --- | --- | --- |
| A. 阶段间流式 | RawAppend 已逐批落 JSONL；Validate/Normalize/Canonical/Quality 改为从 `RawStore.iter_refs` 流式读，RunContext 只持 chunk 级游标与聚合计数 | 数据流改造量大；StageResult 聚合语义、drift 收集、质量 flag 缓冲需重新设计；七阶段签名可能变更 |
| B. 分窗口 run | 每次 run 只回填一个时间窗（窗口上限可配），外层循环推进 cursor；单 run 内存即 O(窗口) | run_log 语义变化：一次逻辑 backfill 产生 N 个 run 行；CLI `pipeline run` 需新增窗口参数；熔断（D05 §3）按 run 计数语义需复核 |

约束（无论何方案）：

1. SR-03 熔断持久化（checkpoints `circuit_open`，迁移 0004）与 SR-06 finish_run 写回语义保持不变。
2. 锁语义不变：一次 try_lock 对应一个 run_id、一次 finish_run；方案 B 的窗口循环在锁外层还是内层需入档决策。
3. 失败恢复语义不变：单窗口失败 → FAILED + cursor 不推进该窗口；已有 checkpoint 对齐。

**方案裁决（2026-09-22）**：采纳 **方案 B（分窗口 run）**。

- A 需重构七阶段数据流：跨 chunk 全有全无失败语义、normalize 阈值语义、replay 路径、TC-P-001 spy 断言全部改变——触及 D05 冻结的 Error Semantics 与 PIPELINE-001 已验收契约，改造面与回归风险大。
- B 七阶段数据流零改动，全部冻结语义逐 run 保持；单 run 内存 O(窗口)；run_log 语义变化（一次逻辑 backfill = N 行 run 行）已由任务单列明可接受；TC-P 全绿不动。

## 测试要求

- 内存断言：构造大数据集（`sys.setrecursionlimit` 不可用，用 tracemalloc 或 RSS 采样）Given 数十万行窗口 When run Then 峰值内存 O(单 chunk/单窗口)，不随总量线性增长。
- 回归：TC-P-001~011 全部保持绿（尤其 TC-P-004 多 run 连跑、DEC-F0 首 chunk 失败）。
- 方案 B 追加：窗口边界对齐（不重不漏）、循环中断（SR-02 CANCELLED）后剩余窗口可恢复。

## acceptance（GWT）

- [x] Given 大窗口 backfill When run Then 峰值内存有界且 run_log/数据结果与改造前逐字节等价（test_ready001_peak_memory_bounded_by_window（tracemalloc + FakeConnector pad_bytes=200KB：单窗 2 天峰值 > 300KB 且 12 天多窗循环峰值 < 单窗 × 2.5，不随总量线性增长）+ test_ready001_windowed_matches_single_run（分窗与整段单 run 的 canonical 值列逐字节一致，run_log 3 行））
- [x] Given 窗口中途失败 When 下次 run Then 从 checkpoint 精确续传，无重复无遗漏（test_ready001_resume_after_window_failure（窗口失败 → FAILED + cursor 停留失败 chunk，重入续传从 checkpoint 起且首请求 start=2024-01-01 23:00，重叠行经 natural key upsert 收敛）+ test_ready001_completed_backfill_rerun_returns_empty（checkpoint 已覆盖请求跨度 → 幂等返回 []））

## 执行记录

### 交付实现（file_ownership 内）

1. **windows.py**：新增 `resolve_window_span(dataset_id, window_seconds)`（窗口跨度 = window_seconds 向上对齐到 chunk_size 整数倍，保证窗口边界与 chunk 边界对齐；`chunk_size == "full_window"` 返回 None）+ `windowed_base(job, cursor)`（续传起点：cursor 优先，backfill 模式下不低于 `job.start`；无 cursor 且无 start → None）。模块 docstring 补充两函数职责说明。
2. **runner.py**：新增 `_WINDOW_CONTINUE = _CURSOR_TERMINAL`（= {SUCCESS, PARTIAL_SUCCESS}，DEC-W4 注释）+ `PipelineRunner.run_windowed(job, *, window_seconds) -> list[RunRow]`（docstring 含 DEC-W1~W4 全文与「一次逻辑 backfill = N 行 run_log」语义变化说明）。循环体：`window_seconds <= 0 → ConfigError`；span=None（DEC-W2）与空任务（无 cursor 且无 start）退化为单 `run(job)` 保持既有语义；`base >= final_end → []`（DEC-W4 幂等）；每窗 `replace(job, start=base, end=window_end, mode="backfill")` 子任务独立 run()，终态不在 _WINDOW_CONTINUE / cursor 无 / 无推进（next_base ≤ base 防死循环）即停。
3. **test_pipeline.py**：新增 `_harness_mem`（非保留 ctx 工厂——不持窗口 ctx 引用，与生产组合根 `_wiring.build_runner` 同构，保证内存断言有效性）+ FakeConnector `pad_bytes` 参数（payload 注入垫片字节放大单 chunk 内存）+ READY-001 测试组 9 用例：窗口覆盖与状态聚合（含 window_seconds=0 → ConfigError）、分窗与单 run 逐字节等价、失败续传（GWT-2）、PARTIAL 链式重试、KeyboardInterrupt→CANCELLED 后恢复、熔断终止循环、full_window 不拆分、完成重跑幂等 []、tracemalloc 峰值 O(窗口)。

### 决策入档（How 自由度内）

| ID | 决策 | 理由 |
|---|---|---|
| 方案裁决 | 采纳 **方案 B（分窗口 run）**，弃 A（阶段间流式） | A 触及 D05 冻结 Error Semantics 与 PIPELINE-001 已验收契约（七阶段签名/聚合语义/replay/TC-P-001 spy 断言全变），改造面与回归风险大；B 七阶段零改动、逐 run 语义全等、单 run 内存 O(窗口)，run_log N 行语义变化任务单已列明可接受 |
| DEC-W1 | 窗口循环在**锁外层**：每个窗口是一次独立 `run()`（一次 try_lock / 一个 run_id / 一次 finish_run） | 保持任务约束 2 的锁不变量（一次 try_lock ↔ 一个 run_id ↔ 一次 finish_run）零改动；SR-03 熔断按 run 计数、SR-06 finish_run 写回逐窗口自然成立；锁持有时间缩小到单窗口，中断（SR-02）在窗口边界即收敛 |
| DEC-W2 | 全窗口 diff 类（fred/sec，`chunk_size=full_window`）不拆分：span=None → 直接 `run(job)` | 其增量语义依赖全窗口 diff，拆窗会破坏正确性；此类数据源行数规模小，内存治理无需求 |
| DEC-W3 | 窗口统一以 `mode="backfill"` 的显式 `[start, end)` 子任务执行；窗口边界对齐 chunk 边界（span = window_seconds 向上取整到 chunk 整数倍） | 与全跨度 plan_chunks 的 chunk 序列逐 chunk 一致（不重不漏）；缝间仅标准 overlap 重复（≤1 chunk），与增量 overlap 防御同源，由 natural key upsert 幂等收敛——「不重不漏」以「canonical 值列逐字节等价 + fetch 覆盖区间连续无缝」证明（等价性测试） |
| DEC-W4 | 续传/停止规则：窗口 SUCCESS/PARTIAL_SUCCESS 且 cursor 有推进 → 继续下一窗口（PARTIAL 的失败 chunk 由下一窗口自然重试）；FAILED/CANCELLED/无推进（next_base ≤ base）→ 停止；重入时 checkpoint 已覆盖请求跨度 → 幂等返回 [] | 失败恢复语义与约束 3 逐条一致；FAILED 交熔断计数（SR-03）、CANCELLED 含熔断打开（SR-06），停止即恢复点；无推进保护防死循环 |

补充：`windowed_base` 返回 None（无 cursor 且无 start 的空任务）→ 退化为单 `run(job)` 保持既有空 SUCCESS 语义（runner.py 注释）。

### 偏差登记（file_ownership 外的必要联动）

| ID | 偏差 | 理由 |
|---|---|---|
| DEV-1 | `cli/pipeline_cmd.py`（file_ownership 外）新增 `--window-seconds` 选项（≤0 → typer.BadParameter）与执行分支（传参走 `run_windowed`，不传保持单 run；多结果逐行渲染） | 任务单 §候选方案 B 明示「CLI `pipeline run` 需新增窗口参数」——属方案 B 交付面的必然联动；CLI 薄层，窗口规则全部位于 runner 层（架构 02 规则 3） |

### 测试命令输出摘要（2026-09-22 复跑）

- `uv run pytest`（全量）：**1385 passed / 0 failed**（基线 1376 + 新增 9：READY-001 测试组），in 143.94s
- 目标组：test_pipeline.py 46 passed（TC-P-001~011 全绿 + 新增 9）
- `uv run ruff check src tests`：All checks passed
- `uv run mypy src/chronoforge`：Success: no issues found in 72 source files
- `uv run lint-imports`：Contracts: 2 kept, 0 broken
- CLI 冒烟：`.venv/bin/chronoforge pipeline run --help` 显示 `--window-seconds`；`--window-seconds 0` → BadParameter
- git commit：PENDING（随 READY-003/004/005 待统一提交）

### DoD 逐项核对（howto §43）

- [x] 实现（resolve_window_span / windowed_base / run_windowed 窗口循环）
- [x] 公共 API（`PipelineRunner.run_windowed(job, *, window_seconds)`；既有 `run()`/`run_many()` 零改动）
- [x] 数据契约（DEC-W1~W4 入档 run_windowed docstring；「一次逻辑 backfill = N 行 run_log」语义变化明示；约束 1~3 逐条满足）
- [x] 错误处理（window_seconds ≤ 0 → ConfigError；CLI 层 BadParameter；无推进/无 cursor/已覆盖边界防御）
- [x] 日志（无新增事件名，EVENT_VOCABULARY 不变；每窗口复用既有 pipeline.run 事件链）
- [x] 指标（RunRow 聚合计数逐窗口保留；返回 rows 列表即窗口序观测面）
- [x] 单测（9 用例：覆盖与状态/等价/续传/PARTIAL 链式重试/中断恢复/熔断终止/full_window 不拆/幂等重跑/内存峰值）
- [x] 边界（window_seconds=0、checkpoint 已覆盖 → []、base ≥ final_end、next_base ≤ base 防死循环、cursor 为 None、full_window）
- [x] 失败（FAILED/CANCELLED → 停止循环；DEC-F0 首 chunk 失败 → FAILED 逐窗口保持；SR-02 KeyboardInterrupt → CANCELLED 由 run() 捕获后循环停止）
- [x] 恢复（PARTIAL → 从新 checkpoint 续传、失败 chunk 下一窗口自然重试；重入幂等返回 []）
- [x] 集成测试（TC-P-001~011 全绿 46 passed；CLI `--window-seconds` 冒烟通过）
- [x] 静态分析（ruff All checks passed）
- [x] 类型检查（mypy Success，72 文件）
- [x] 无未声明假设（方案裁决 + DEC-W1~W4 + DEV-1 入档）
- [x] 验收通过（GWT 2/2 + 复跑记录如上）

### 接管性抽查

仅凭本任务单 + run_windowed docstring（DEC-W1~W4 全文）+ windows.py 模块说明，可完整理解窗口跨度对齐规则（resolve_window_span 向上取整到 chunk 整数倍）、锁外层语义、续传/停止规则与 CLI 参数；无需读 _wiring 或存储细节。抽查通过。

## Deferred Acceptance

（无 Deferred——任务范围全部闭环）

范围注记：replay.py 的全量缓冲（重放路径一次性物化全表）为相邻已知限制，属 D05 §4 replay 语义的独立改造面，不在 SR-04/本任务单范围。
