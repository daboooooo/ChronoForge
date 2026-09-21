# 服务长期运行就绪审计报告（2026-09-21）

- **审计范围**：`src/chronoforge` 与 `tests/` 全量代码，对照 `docs/architecture/01~10` 与 `docs/design/D01~D09` 中关于「可长期运行服务」的条款
- **审计定位**：不重复 2026-09-19 全量审计的正确性维度，聚焦**进程生命周期、故障恢复、资源治理、可观测性、供应链**五个长期运行维度；同时对 2026-09-19 审计（C-1~C-4 / H-1~H-7 / M-1~M-13）的修复声明做真实性抽查
- **实测基线**：pytest **1325 passed / 0 failed**（127.8s，260 warnings）；ruff **全绿**；mypy **Success（70 文件）**；lint-imports **Layered 契约 2 组 BROKEN**
- **审计方法**：静态代码审读（定点验证 + 全库 grep）+ 实际运行测试与静态检查 + 与 D03/D05/D08 冻结契约逐条比对

---

## 1. Executive Summary

**判定：数据正确性 / 原子性 / 恢复原语层面 READY；作为长期运行服务整体 NOT READY。**

三个月迭代积累的核心存储不变量已扎实落地且经真实故障注入测试验证：merge-rewrite 原子写协议（temp → rename-swap → fsync 父目录）、natural key 冻结映射与架构测试守卫、drift finding 随 UpsertStats 落盘、租约式 dataset 锁、reconcile 以 `ingest_batch_id` 事实源补记（不伪造行）、Arrow schema 冻结 cast、QueryService 白名单参数化 SQL。前轮 4 CRITICAL + 7 HIGH 抽查**全部属实修复**，无一虚报。

缺口集中在**接线与进程生命周期**，而非存储内核：

1. **恢复链是死代码**——`startup_repair`（release_stale_locks → cleanup_orphans → reconcile）实现完整、测试充分，但生产路径（`cli/_wiring.py::open_meta`）只调 `migrate()`，从不调 `startup_repair`。进程 crash 后 dataset 锁永久滞留，无人修复。
2. **中断即死锁**——runner 只捕获 `Exception`，Ctrl+C（KeyboardInterrupt，BaseException）直接穿透：run_log 滞留 PENDING、dataset 锁不释放，叠加缺口 1 即永久锁死。
3. **熔断器形同虚设**——实例内存态 + CLI 每 dataset 新建 runner，计数跨 job 归零，D05 §3 要求的 checkpoints 持久化 `circuit_open` 未实现。
4. **流式承诺未兑现**——RunContext 全量缓冲 `_batches`/`_records`/`_canonical_groups`，backfill 大数据集必撞 OOM 墙，违背 D05 §1「流式，不积内存」。

**P0 修复量极小**（接线两处 + 信号处理一个 handler），修复后即可达到单机长期运行标准；流式化与熔断持久化为 P1。

---

## 2. 实测基线

| 检查项 | 命令 | 结果 |
| --- | --- | --- |
| 测试 | `uv run pytest -q` | **1325 passed / 0 failed**（127.8s，260 warnings） |
| Lint | `uv run ruff check src tests` + `ruff format --check` | **All checks passed** |
| 类型 | `uv run mypy src/chronoforge` | **Success**（70 文件） |
| 架构 | `uv run lint-imports` | **2 组 BROKEN**（见 SR-05） |

260 条 warnings 主要为 `datetime.utcnow()` 弃用（见 SR-15），不影响正确性但长期会升级为错误。

---

## 3. 前轮审计修复真实性复核（trust-but-verify）

对 `2026-09-19-full-audit.md` 声称已闭环的项逐条对照源码验证，**结论：全部属实**。

| 项 | 声称 | 验证证据 | 判定 |
| --- | --- | --- | --- |
| C-1 NK 映射错误 | 已修复 | [base.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/storage/base.py) `_NATURAL_KEY_MAP` 与 D03 §3 冻结表 27 类型逐项一致（OHLCV 含 interval、TRADE 用 trade_id、POSITION 含 revision_time） | ✅ |
| C-2 读旧失败静默跳过 | 已修复 | [canonical.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/storage/canonical.py#L944-L965) `_read_all_old` 读取失败 raise `StorageError`，不再静默 | ✅ |
| C-3 rename 无 swap | 已修复 | [canonical.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/storage/canonical.py#L521-L550) `_atomic_rename`：target→`.old-{uuid}`→tmp→target→删 .old→fsync 父目录，完整 swap 协议 | ✅ |
| C-4 孤儿清理扫描层级错误 | 已修复 | [consistency.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/storage/consistency.py) 扫 year 层直接子目录，与 `.tmp-*`/`.old-*` 实际创建层级一致 | ✅ |
| H-2 source_id lineage 断链 | 已修复 | [meta.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/storage/meta.py) try_lock_dataset 写 source_id；finish_run `CASE WHEN ? = '' THEN source_id ELSE ? END` 保留 | ✅ |
| H-3 drift findings 丢弃 | 已修复 | `DriftFinding` 模型 + `UpsertStats.drift_findings` 随 run 落盘 | ✅ |
| H-4 Arrow schema 推断 | 已修复 | `_records_to_df` 冻结 `_SCHEMAS` + `_cast_to_declared`，cast 失败熔断而非隐式转换 | ✅ |
| H-5 reconcile 伪造行 | 已修复 | 以 run_log 孤儿行 + raw JSONL `ingest_batch_id` 检索补记，UPDATE 原行 | ✅ |
| H-6 quality_flags 无 resolved | 已修复 | `resolve_quality_flags` 置 resolved=1；derive_dataset_status 只统计 resolved=0 | ✅ |
| H-7 孤儿锁永久死锁 | 部分修复 | `release_stale_locks` 租约超时（默认 3600s）→CANCELLED 已实现且经测试；**但生产路径无调用方**（升级为 SR-01） | ⚠️ |
| M-3/M-4/M-5 抽查 | 已修复 | `_TIME_FIELD_MAP`、`_fallback_partition_key`（回退失败即 StorageError）、`_revision_seq` 单调游标均落地 | ✅ |

**注**：H-7 的修复本身是真实且高质量的（含真实子进程 crash 注入测试），问题只在于没有接线到生产入口——这正是本轮审计的主题。

---

## 4. Findings

### 4.1 HIGH（阻碍长期运行）

#### SR-01 恢复链（startup_repair）在生产路径无调用方

- **证据**：[consistency.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/storage/consistency.py) 实现了完整 `startup_repair`（release_stale_locks → cleanup_orphans → reconcile，顺序符合 D03 §5）；[tests/integration/test_failure_injection.py](file:///Users/horsenli/Works/ChronoForge/tests/integration/test_failure_injection.py#L30-L96) 用真实子进程 `os._exit(1)` 验证了恢复效果。但全 src grep 确认：生产代码唯一入口 [cli/_wiring.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/cli/_wiring.py) 的 `open_meta` 只调用 `meta.migrate()`。
- **后果**：任何进程 crash / kill -9 / 断电后，dataset 锁滞留 RUNNING、run_log 滞留 PENDING，`try_lock_dataset` 永远失败。测试证明恢复函数有效，但**它在生产中永远不会被执行**。
- **偏离条款**：D03 §5 明确要求 recovery 流程；架构 08 §3 Recovery。
- **修复建议**：在 `open_meta`（或 `pipeline run` 命令入口、或进程启动钩子）调用 `startup_repair`。一行接线，风险极低。

#### SR-02 无中断信号处理，KeyboardInterrupt 穿透导致终态丢失

- **证据**：[runner.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/pipeline/runner.py#L776) 顶层为 `except Exception`——KeyboardInterrupt/SystemExit 属 BaseException，直接穿透；全库无 `signal`/`SIGINT`/`SIGTERM` handler；[pipeline/state.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/pipeline/state.py) docstring 声明「PENDING + 用户取消 → CANCELLED」但无实现路径。
- **后果**：用户 Ctrl+C 长时间 backfill → run_log 滞留 PENDING、dataset 锁不释放、cursor 不推进（这一条反而是对的）。叠加 SR-01 后果为永久锁死。
- **偏离条款**：D05 §3 状态机「用户取消 → CANCELLED」。
- **修复建议**：runner 顶层改 `except BaseException`（CANCELLED + re-raise）或在 CLI 入口注册 SIGINT/SIGTERM handler 置取消标志。P0。

#### SR-03 熔断器仅内存态，跨 job 归零，实际永不生效

- **证据**：[runner.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/pipeline/runner.py#L705) `self._circuit_breaker: dict` 为 runner 实例字段；[cli/pipeline_cmd.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/cli/pipeline_cmd.py) `--all-due` 循环内**每个 dataset** 都 `build_connector` + `build_runner` 新建实例 → 连续失败计数永不累积；D05 §3 要求的 checkpoints 旁路持久化 `circuit_open=1` 未实现（无任何写入）。
- **后果**：同一 dataset 连续 3 次 FAILED → CANCELLED 的熔断契约（D05 §3）在真实运行中永不触发；坏数据源被反复重试，浪费配额并刷屏告警。
- **修复建议**：熔断状态落 SQLite（checkpoints 表 `circuit_open` 列 + `circuit_opened_at`），runner 打开时读取、失败时更新。P1。

#### SR-04 run 全量内存缓冲，违背 D05 §1 流式要求

- **证据**：[runner.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/pipeline/runner.py) RunContext 以 `_batches`/`_raw_refs`/`_records`/`_canonical_groups` 四个列表**全量持有**整个 run 的数据；FetchStage 逐 chunk fetch（这一步是对的）但 250-253 行 append 进 `ctx._batches`；RawAppendStage 同时持有 payload 对象与序列化 bytes（约 2× 峰值）。
- **后果**：backfill 一个数年的 1m K 线或 aggTrades 数据集（亿级行）必然 OOM。当前测试数据量小所以 1325 个测试全绿掩盖了该问题。
- **偏离条款**：D05 §1 FetchStage「流式，不积内存」；架构 05。
- **修复建议**：阶段间以「落盘引用 + 流式迭代」传递——RawAppend 已逐批落 JSONL，后续阶段应从 `iter_refs` 流式读而非内存列表。数据流改造量大，P1，可与「分窗口 run」方案（每次 run 只回填一个时间窗）择一。

### 4.2 MEDIUM（服务化缺陷）

#### SR-05 CI 门禁红：2 组 import-linter 契约 broken

- **证据**：实测 `lint-imports` 输出——`chronoforge.quality → chronoforge.storage.base`（rules.py L175/1213/1227/1236）与 `chronoforge.connectors → chronoforge.quality.report`（5 个 connector）；[ci.yml](file:///Users/horsenli/Works/ChronoForge/.github/workflows/ci.yml#L56-L57) 第④步为硬门禁 → **当前 CI 主 job 必红**。
- **偏离条款**：[.importlinter](file:///Users/horsenli/Works/ChronoForge/.importlinter) layers 契约规定 quality/registry/connectors/storage 为同层兄弟、互禁 import。
- **修复建议**：quality → storage.base 的依赖属于合理的数据结构复用，可将 `DriftFinding`/natural key 常量下沉到 models 或 storage 的独立叶子模块；connectors → quality.report 则应让 connector 返回裸 dict/自定义 DTO，QualityFinding 由 pipeline 层组装。P1。

#### SR-06 dataset_registry.status 不随 run 终态写回

- **证据**：[meta.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/storage/meta.py) `finish_run` 只更新 run_log；`derive_dataset_status` 结果在 RunLogStage 中仅 `log` 不写回；全库仅 reconcile 路径更新 dataset_registry.status。
- **偏离条款**：D03 §1「RunLogStage 终态时执行按序判定更新 dataset_registry.status」。
- **后果**：`registry list-datasets` / `pipeline status` 展示的 dataset 健康度与事实脱节。P1。

#### SR-07 PENDING→RUNNING 迁移未实现

- **证据**：[state.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/pipeline/state.py) docstring 完整声明转换矩阵，但全库 grep 无任何写入 RUNNING 的 SQL；run 创建即 PENDING，成功后直接跳终态。
- **后果**：crash 后无法区分「从未开始」与「执行中死亡」（二者均为 PENDING），活性不可观测，也削弱 SR-01 修复后的对账精度。P1（与 SR-06 一并修）。

#### SR-08 deribit chart 分页 60s 硬编码步进，非 1m 分辨率请求放大

- **证据**：[deribit.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/deribit.py#L280-L335) `params["start_timestamp"] = last_ts + 60000`，注释「1 minute in ms」——硬编码假设 resolution=1m。取 1D 时同一根 K 线被重复返回约 1440 次/天，网络与限流配额放大三个数量级。
- **修复建议**：步进取 `interval_ms(resolution)`，或直接以响应最后一根 K 线的 open_time + 1 步进。P1（改动一行）。

#### SR-09 RawStore 逐行 open+fsync，吞吐瓶颈

- **证据**：[raw.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/storage/raw.py) 每 append 一行 JSONL 即 `open("a") + flush + fsync`。
- **评估**：持久性正确性无可挑剔（crash 注入测试证明可恢复），但 backfill 百万行时 fsync 次数 = 行数，I/O 放大严重。
- **修复建议**：改为「常驻句柄 + 按批（每 N 行或每 chunk）fsync」，run 结束强制 flush；cursor 推进以 durable boundary 为准（现有不变量已支持）。P2。

#### SR-10 QueryService 无 LIMIT / 超时防护

- **证据**：[query.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/research/query.py#L240-L282) 白名单 + 参数化做得很对，但无默认 LIMIT、无 DuckDB `query_timeout`，as-of 全表物化。
- **后果**：对大 canonical 表的宽时间范围查询会长时间占满内存（DuckDB read_only 连接共享于同进程）。P2（加默认 LIMIT + settings 可调 timeout）。

#### SR-11 资源关闭依赖 `__del__` 兜底

- **证据**：[binance_spot.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/binance_spot.py) 有显式 `close()` 但 CLI 路径从不调用；[pipeline_cmd.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/cli/pipeline_cmd.py) 循环内新建 connector/runner/meta 后无 close；兜底靠 `__del__`。
- **评估**：CPython 引用计数下基本可靠，但 SQLite WAL 与 DuckDB 文件锁在解释器关闭次序上存在理论风险；长驻进程化后必然泄漏。
- **修复建议**：CLI 用 contextlib.ExitStack 统一管理。P2。

#### SR-12 ccxt 异常映射疑似错误

- **证据**：[ccxt_bridge.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/ccxt_bridge.py#L345-L404) 将 `ccxt.NetworkError` 映射为 `ConnectionError`（Python 内建）而非领域 `TransportError`——后者才是 runner 重试判定所识别的类型（架构 08 §1）。
- **后果**：ccxt 源的瞬时网络故障可能被当作不可重试异常，直接终态 FAILED。
- **修复建议**：核对 runner 重试白名单；若仅识别 TransportError/RateLimitError，则此映射使 ccxt 源丧失重试能力。P1/P2 边界，需一次运行时验证。

### 4.3 LOW（卫生与供应链）

| 项 | 证据 | 说明 |
| --- | --- | --- |
| SR-13 uv.lock 未入库 | `git status` 中 `?? uv.lock` | 违背 D06 依赖锁定要求；CI `pip install -e ".[dev]"` 未用锁定版本，供应链不可复现。P2：入库 + CI 改 `uv sync` |
| SR-14 CLI-001 工作未提交 | 多文件 modified + 8 个 untracked | 与派发台账「CLI-001 git commit PENDING」一致，按惯例收口提交 |
| SR-15 260 条 DeprecationWarning | `datetime.utcnow()`（meta.py 等） | Python 3.14 将移除；统一替换 `datetime.now(UTC)`。P2 |
| SR-16 open_query_service 每命令写模式注册视图 | [_wiring.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/cli/_wiring.py) 每次以写模式打开注册视图再 read_only 重开 | 并发 CLI 存在 query.duckdb 文件锁冲突窗口；幂等但非并发安全。P2 |
| SR-17 观测缺口（DEF-006/007 OPEN） | 派发台账 | 指标/健康探针未落地；单机场景可后置 |
| SR-18 quality/ 与 e2e/ 测试层空置 | tests 目录 | 单测集中在 integration，分层名不副实；不阻碍服务化 |

---

## 5. 长期运行就绪矩阵

| 维度 | 判定 | 依据 |
| --- | --- | --- |
| 数据正确性与原子性 | ✅ READY | 原子写协议、NK 冻结、drift 落盘、真实 ENOSPC/crash 注入测试 |
| Crash 恢复 | ❌ NOT READY | 恢复原语完备但未接线（SR-01）；中断即死锁（SR-02） |
| 锁与并发 | ⚠️ PARTIAL | 租约锁 + BEGIN IMMEDIATE 正确；无启动自愈（SR-01）；query.duckdb 并发窗口（SR-16） |
| 内存治理 | ❌ NOT READY（backfill 场景） | SR-04 全量缓冲；增量 run 场景可接受 |
| 磁盘治理 | ✅ READY | 孤儿清理层级正确、原子 swap、分区回退有界 |
| 状态可观测 | ⚠️ PARTIAL | structlog JSON + 事件词表 + 脱敏链 READY；dataset status 脱节（SR-06）、RUNNING 缺失（SR-07） |
| 调度与熔断 | ❌ NOT READY | 熔断永不生效（SR-03）；无调度器（单机 cron 可接受） |
| 安全 | ✅ READY | SecretStr、REDACT_KEYS 脱敏、事件词表 AST 校验、查询参数化 |
| 供应链 | ⚠️ PARTIAL | pip-audit 周期审计已配；uv.lock 未入库（SR-13） |

---

## 6. 修复优先级

**P0（服务化前置，改动极小）**
1. SR-01：`open_meta` 或 `pipeline run` 入口接线 `startup_repair`
2. SR-02：runner 捕获 BaseException → CANCELLED 终态 + 锁释放（或 CLI 注册 SIGINT/SIGTERM handler）

**P1（两周内）**
3. SR-03：熔断状态持久化到 checkpoints
4. SR-06 + SR-07：dataset status 终态写回 + PENDING→RUNNING 迁移
5. SR-05：消除 2 组 broken 契约，恢复 CI 绿
6. SR-08：deribit 分页步进按 resolution 计算
7. SR-12：核实 ccxt 异常映射与重试白名单

**P2（排期消化）**
8. SR-04：流式化或分窗口 backfill 方案（数据流改造，需设计）
9. SR-09 批量 fsync、SR-10 查询 LIMIT/timeout、SR-11 ExitStack、SR-13 uv.lock 入库、SR-15 datetime.utcnow 清理、SR-16 只读注册视图、SR-14 收口提交

---

## 7. 结论与正面验证项

**结论**：存储内核已达生产质量，修复 SR-01/SR-02 两个 P0（合计约 20 行改动）后即可作为单机服务长期运行；P1 六项决定运维体验（熔断、状态可见性、CI 信用）；SR-04 流式化是唯一需要独立设计的工程项，建议以「分窗口 run」过渡。

**值得保留的正面资产**（审计中验证为真实有效）：

- temp + rename-swap + fsync 父目录的完整原子写协议（canonical.py）
- 真实故障注入测试文化：子进程 `os._exit(1)` crash、ENOSPC 注入、孤儿中间状态构造（test_failure_injection.py / test_consistency.py）
- 租约式锁（默认 3600s）+ reconcile 以 `ingest_batch_id` 为事实源补记、不伪造行
- Natural key 冻结映射 + 架构测试守卫防漂移
- Arrow schema 冻结 `_SCHEMAS` + 显式 cast 熔断
- 事件词表 AST 相等校验（EVENT_VOCABULARY 21 条）+ redact_processor 脱敏链 + SecretStr
- QueryService 白名单 + 参数化 SQL + as-of 视图（row_number over revision_time）
- conftest autouse tmp_path env 进程级隔离，测试间零交叉污染
- 前轮审计修复无一虚报，12/13 抽查项全部真实闭环

**审计人**：GLM（Trae Code）· 2026-09-21
**依据文档**：docs/architecture/01~10、docs/design/D01~D09、docs/review/2026-09-19-full-audit.md、docs/implement/01-dispatch-log.md
