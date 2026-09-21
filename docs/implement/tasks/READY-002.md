# READY-002 — RawStore 批量 fsync（吞吐治理）

## 派发信息

- 任务单：2026-09-21 服务就绪审计 SR-09（P2）
- 设计依据：D03 §2（Raw JSONL 持久性契约）、架构 08 §2（durable boundary）；现有不变量已支持按批 fsync
- Agent：Orchestrator 主会话直执（沿用 QUERY-001~003 / CLI-001 惯例）
- 派发时间：2026-09-21
- 状态：DONE（2026-09-21 验收通过）
- 依赖：STORAGE-002.1（append + iter_refs）、test_failure_injection.py 既有 crash 注入基线

## 审计证据（SR-09 原文要点）

- [raw.py](../../../src/chronoforge/storage/raw.py) 每 append 一行 JSONL 即 `open("a") + flush + fsync`。
- 持久性正确性无可挑剔（crash 注入测试证明可恢复），但 backfill 百万行时 fsync 次数 = 行数，I/O 放大严重。

## file_ownership

- `src/chronoforge/storage/raw.py`
- `tests/integration/test_raw_store.py`
- `tests/integration/test_failure_injection.py`（crash 注入回归）

## 交付物契约摘要

改造为「常驻句柄 + 按批 fsync」：

1. 常驻文件句柄（按分区/文件切分规则打开，10000 行滚动新建文件时同步切换句柄）。
2. fsync 粒度：每 chunk 边界（`flush_batch()` 显式调用点）或每 N 行（N 可配），run 结束（含 FAILED/CANCELLED 异常路径，SR-02 语义）强制 flush + fsync。
3. **durable boundary 不变量**：cursor 推进（save_checkpoint）只允许越过已 fsync 的数据边界。实现上由调用序保证——chunk 内全部 append → flush_batch() → checkpoint 写入；RawStore 需暴露「是否已 durably 落盘」的判定点供 runner 依赖（或在 runner 侧以显式调用序入档契约）。
4. crash 窗口语义：未 fsync 批次允许丢失，但 cursor 不得越过；重放部分由上游 chunk 重试覆盖（raw 允许重复行，幂等由 canonical upsert 收敛——此语义入档）。

## 测试要求

- crash 注入：子进程 append N 批 + `os._exit(1)`（在部分批次 fsync 前/后各采样）→ 已 fsync 批次完整可读，行数与 durable boundary 一致。
- 吞吐断言：百万行 append 墙钟时间较逐行 fsync 显著下降（数量级验证即可）。
- 回归：test_raw_store / test_raw_cleanup / test_failure_injection / test_consistency 全绿；分区滚动（ingest_date + 10000 行切文件）与新句柄管理交互覆盖。

## acceptance（GWT）

- [x] Given chunk 内 append 完成 When flush_batch 后 crash Then 该 chunk 数据完整且 cursor 可安全推进（test_failure_injection.py::TestRawFsyncCrash::test_crash_after_flush_batch_data_complete：子进程 append 25 行 + flush_batch + os._exit(1) → 25 行全批可读、可解析、无 torn line，新实例读回全部）
- [x] Given fsync 前崩溃 Then 丢失行不越过 durable boundary，重放收敛（test_crash_before_fsync_boundary_consistent：子进程在第 1 个 fsync 点 os._exit → 可读行数 = boundary = 10 行、无 torn line；test_crash_then_replay_duplicates_converge：重放同批 → 重复行允许落盘、35 行可回读，幂等由 canonical upsert 收敛）

## 执行记录

### 交付实现（raw.py，file_ownership 内，未改 runner.py）

1. **常驻句柄**：`_FileHandle` 增加 `file_obj`（惰性打开的追加句柄）与 `unsynced_lines`（crash 窗口计量）；`_get_handle` 在 10000 行滚动新建文件时同步关闭并切换旧句柄；新增 `close()` 供显式释放。
2. **fsync 粒度**：写入经句柄缓冲；`append()` 返回前对本批全部脏句柄 flush + fsync（per-call 边界）；`fsync_every_n`（默认 0）> 0 时批内每 N 行追加中间 fsync；`flush_batch()` 为幂等显式持久化点。
3. **durable boundary 不变量**：契约入档模块 docstring——`append()` 返回 ⇒ 本批全部行已 durable；runner 现有调用序（RawAppendStage.append → … → RunLogStage.save_checkpoint）天然满足「cursor 只推进到已 fsync 边界」。
4. **crash 窗口语义**：append 执行期间未 fsync 的行允许丢失但 cursor 不越过 boundary；raw 允许重复行，重放由上游 chunk 重试覆盖、幂等由 canonical upsert 按 natural key 收敛（D03 §3）。

### 决策入档（How 自由度内）

| ID | 决策 | 理由 |
|---|---|---|
| DEC-1 | fsync 时机采用「append() 返回即 durable」而非「append 缓冲 + flush_batch 提交」 | runner（ownership 外）每 run 仅一次 append 且 checkpoint 在 Stage 7 才写；存储层自身保证不变量，无需修改 runner.py、不扩 ownership；flush_batch() 保留为幂等显式点（GWT-1 语义成立） |
| DEC-2 | 「每 N 行（N 可配）」落地为构造参数 fsync_every_n（默认 0） | 缩小 backfill 大批量单次 append 的 crash 窗口；默认仅调用边界 |
| DEC-3 | run 结束（含 FAILED/CANCELLED，SR-02 语义）无需补偿 flush | append() 返回 ⇒ durable，异常路径不写 cursor（TC-P-005），不存在滞留未落盘数据 |
| DEC-4 | 吞吐断言以 fsync 次数为数量级证据，墙钟仅 sanity（< 60s） | 20 万行单次 append = 1 次 fsync vs 逐行基线 20 万次（≥5 个数量级削减）；避免定时断言 flaky |
| 偏差裁决 | 任务单「每 chunk 边界（flush_batch() 显式调用点）」字面语义需 runner 逐 chunk 调 flush_batch；实际 runner 每 run 一次性 append 全部 chunk（PIPELINE-001 既定流式缓冲设计），fsync 边界落在 append() 调用边界（= per-run） | fsync 次数从 行数 → run 数，达成 SR-09 吞吐治理目标；调用序契约以模块 docstring 入档（任务单 §3 允许的「判定点」选项） |

### 测试命令输出摘要（2026-09-21 复跑）

- `uv run pytest`（全量）：**1348 passed / 0 failed**（基线 1336 + 新增 12：TestBatchFsync 9 + TestRawFsyncCrash 3），in 125.80s
- 目标组：test_raw_store / test_failure_injection / test_raw_cleanup / test_consistency → 76 passed
- `uv run ruff check .`：All checks passed
- `uv run mypy src`：Success: no issues found in 72 source files（storage.* strict）
- `uv run lint-imports`：Contracts: 2 kept, 0 broken
- git commit：ccb08d7（feat(storage)）

### DoD 逐项核对（howto §43）

- [x] 实现（常驻句柄 + 按批 fsync）
- [x] 公共 API（`RawStore(data_dir, fsync_every_n=0)` / `flush_batch()` / `close()`；append/iter_refs 签名不变）
- [x] 数据契约（durable boundary 契约入档模块 docstring；行格式/路径布局/10000 行滚动不变）
- [x] 错误处理（open/write/fsync/close 的 OSError → StorageError，不吞）
- [x] 日志（存储层低频操作，无新增事件需求，EVENT_VOCABULARY 不变）
- [x] 指标（run_log 观测列无变化）
- [x] 单测（TestBatchFsync 9 用例：fsync 计数/分区句柄/N 行中间点/幂等/新读者可见/滚动切换/close 重开/重复行/吞吐）
- [x] 边界（10000 行滚动句柄切换、跨分区 dirty 句柄、close 后重开行号延续、空批次短路）
- [x] 失败（crash 注入 3 用例：fsync 前 boundary 一致 / flush 后全批完整 / 崩溃后重放收敛）
- [x] 恢复（重放重复行收敛语义验证；canonical 幂等由 STORAGE-003 既有测试覆盖）
- [x] 集成测试（test_pipeline / test_consistency / test_raw_cleanup / test_replay 随全量回归全绿）
- [x] 静态分析（ruff All checks passed）
- [x] 类型检查（mypy strict storage.* Success，72 文件）
- [x] 无未声明假设（DEC-1~4 + 偏差裁决入档）
- [x] 验收通过（GWT 2/2 + 复跑记录如上）

### 接管性抽查

仅凭本任务单 + raw.py 模块 docstring 持久性契约 + 测试命名/docstring，可完整理解 fsync 时机、crash 窗口语义与 durable boundary 调用序契约，无需读 runner 实现细节。抽查通过。

## Deferred Acceptance

无（本任务验收项无跨任务依赖；「幂等由 canonical upsert 收敛」所依赖的 STORAGE-003 已 DONE 且测试存量覆盖）。
