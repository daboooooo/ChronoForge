# READY-005 — open_query_service 视图注册并发窗口消除

## 派发信息

- 任务单：2026-09-21 服务就绪审计 SR-16（P2）
- 设计依据：D07 §1 / STORAGE-004（视图注册惰性语义）、架构 02 规则 4；单机多 CLI 进程并发场景
- Agent：Orchestrator 主会话直执（沿用 QUERY-001~003 / READY-002~004 惯例）
- 派发时间：2026-09-21
- 状态：DONE（2026-09-22 验收通过）
- 依赖：CLI-001（open_query_service）、STORAGE-004（register_views）

## 审计证据（SR-16 原文要点）

- [_wiring.py](../../../src/chronoforge/cli/_wiring.py) `open_query_service` 每次以**写模式**打开 query.duckdb 注册视图再 read_only 重开；并发 CLI 存在 query.duckdb 文件锁冲突窗口；幂等但非并发安全。

## file_ownership

- `src/chronoforge/cli/_wiring.py`
- `src/chronoforge/storage/views.py`（若探测/注册逻辑下沉）
- `tests/unit/test_cli.py`

## 候选方案（执行前设计对比，已择一实施）

| 方案 | 内容 | 代价 | 裁决 |
| --- | --- | --- | --- |
| A. 只读探测优先 | 先 read_only 打开并 introspect 视图集合；目标视图齐全 → 直接使用；缺失才写模式注册（缩小窗口至「首次/新增类型」） | 探测开销小；仍保留极小写窗口 | **采纳**（写窗口仅剩首次创建/新增类型，配合有界退避+只读复探收敛） |
| B. 注册时机前移 | 视图注册并入 open_meta 的启动修复/迁移阶段（每命令入口已单点执行），open_query_service 纯 read_only | 语义集中但 open_meta 承担查询层职责，分层需入档论证 | 拒：open_meta 为元数据层入口，承担 query.duckdb 注册越层；且把写窗口扩大到**全部** CLI 命令（run/status 等也会抢 query.duckdb 写锁） |
| C. 写窗口重试 | 保留现状 + 写模式打开加文件锁冲突重试（有界退避） | 改动最小；未消除窗口只缓解 | 单独否；其退避机制**吸收进 A 的写路径**（冲突时退避 + 只读复探收敛） |

约束核对：视图注册保持幂等 ✓（register_views 未改语义，快路径跳过的重复注册本为幂等）；read_only 连接返回语义（service, con 元组）不变 ✓；query.duckdb 不存在时首次创建路径仍可用 ✓（exists() False → 直接写路径）。

## 测试要求

- 并发冒烟：两个子进程同时执行 `query --dataset ...`（视图已注册 / 全新 query.duckdb 两态）→ 无锁死、无异常退出（有界重试语义按所选方案断言）✓
- 幂等：连续 N 次 open_query_service → 视图集合稳定 ✓
- 回归：test_cli.py Query 组全量绿 ✓

## acceptance（GWT）

- [x] Given 两进程并发 query When 同时打开 query.duckdb Then 双方均正常返回或按重试策略收敛，无 unhandled 文件锁异常（test_cli.py::TestOpenQueryServiceConcurrency::test_concurrent_query_fresh_catalog_smoke（全新 catalog 态，两子进程 `python -m` 等价入口并发，row_count=5 双收敛）+ test_concurrent_query_registered_views_smoke（预热注册态，双 fast path，dataset_version=0.1.0+1.0）；stderr 均无 lock 异常字样。进程内有界重试语义另由 test_write_path_lock_retry_converges（2 次注入冲突 → 收敛）与 test_write_path_lock_retry_exhausted（持续冲突 → IOException 有界上抛）断言）
- [x] Given 视图已注册 When 打开 Then 不进入写模式（方案 A/B 下）（test_ready_views_skip_write_mode_and_idempotent：预热后 monkeypatch `_register_and_reopen` 为必失败桩，连续 3 次 open_query_service 视图集合稳定且 query 正常；另断言返回连接 read_only（CREATE TABLE → duckdb.Error，D07 §5））

## 执行记录

### 交付实现（file_ownership 内）

1. **views.py**（探测逻辑下沉）：
   - SQL 生成重构为单一事实源：`_base_view_sql(canonical_base, type_name)` / `_asof_view_sql(canonical_base, ct)`；`_try_register_view` 改收 `(con, view_name, select_sql)`，`_register_asof_views` 复用（对外 `register_views` 签名与语义零变化）。
   - 新增 `missing_views(con, data_dir) -> set[str]`：只读探测「register_views 将注册而 catalog 尚缺」的视图集合；判定与惰性注册**同源**（同一 SQL 构造 + 同一 read_parquet 绑定校验 `_view_bindable` = `SELECT * FROM (…) LIMIT 0`）。PROVISIONAL 注记：视图定义由 (data_dir, type) 决定性生成，升级改模板需删 query.duckdb 重建（纯派生物）。
2. **_wiring.py**（方案 A 接线）：
   - `_open_ready_readonly(catalog_path, data_dir)`：快路径——文件存在则 read_only 打开 + `missing_views` 空集判定；文件缺失/IOException 锁冲突/视图缺失 → None。
   - `_register_and_reopen(catalog_path, data_dir)`：写路径——写模式幂等注册 + read_only 重开；**写打开锁冲突时指数退避（0.1s 起 ×2，5 次 ≈1.5s 上限）且每次冲突后只读复探**：冲突持有者若已代为完成注册（并发首次创建的典型时序）直接复用其成果返回，无需抢写锁；重试耗尽 → IOException 有界上抛。
   - `open_query_service`：快路径命中 → 直接构造 service（稳态零写窗口，多进程只读连接并发安全）；未命中 → 写路径。返回 (service, con) 语义、`max_rows` 注入（READY-003 偏差）原样保留。

### 决策入档（How 自由度内）

| ID | 决策 | 理由 |
|---|---|---|
| DEC-1 | 探测判定不做文件系统镜像（glob parquet 存在性），而是与注册共用同一 SQL 生成 + read_parquet 绑定校验（`SELECT * FROM (…) LIMIT 0`） | 绑定校验与 CREATE VIEW 的绑定语义严格同源：分区目录缺失、corrupt parquet、NUMBER 缺 release_time 列（as-of 视图引用）等场景下「probe 结果 = register 行为」，杜绝「probe True 但注册失败 → missing 永不收敛 → 每次进写窗口」的缝隙；getvariable('asof') 未 SET 求值为 NULL（运行时已核实 duckdb 1.5.5），只读探测安全 |
| DEC-2 | 写路径锁冲突采用「退避 + 只读复探」而非纯等待重试写锁 | 冒烟实测发现：并发首次创建时，后到方 1.5s 重试窗口可能整体落入围栏内前到方的 read_only 查询持锁期而耗尽。改为冲突后复探「视图是否已被持有者注册齐全」，齐全即复用（注册幂等，收敛语义 = 视图齐全即可用），消除对该时序的等待依赖；复探仅 O(type 数) 次 read_parquet 绑定，代价可忽略 |
| DEC-3 | 视图 SQL 模板升级不走自动刷新（同名视图视作最新） | 视图定义是 (data_dir, type) 的决定性纯函数；refresh 需每次写模式打开，与 SR-16 目标相悖。query.duckdb 为纯派生物，删除重建零成本，PROVISIONAL 注记入 `missing_views` docstring |
| DEC-4 | 重试参数为模块常量 `_WRITE_OPEN_ATTEMPTS=5` / `_WRITE_OPEN_BACKOFF_S=0.1`（非 Settings 项） | 写窗口属实现细节而非用户可调契约；常量便于测试 monkeypatch（重试用例均置 0.01s），避免 Settings 面扩张 |

### 偏差登记

- 无（改动全部落在 file_ownership 三文件内；`register_views` 对外签名与语义零变化，test_views.py/test_query.py 存量用例零改写全绿）。

### 测试命令输出摘要（2026-09-22 复跑）

- `.venv/bin/pytest tests/unit/test_cli.py::TestOpenQueryServiceConcurrency -q`：**6 passed**（连续 3 轮复跑均 6 passed，并发冒烟无 flake）
- `.venv/bin/pytest -q`（全量）：**1376 passed / 0 failed**（基线 1370 + 新增 6），in 139.46s
- `.venv/bin/ruff check`（改动三文件）：All checks passed
- `.venv/bin/mypy src/chronoforge`：Success: no issues found in 72 source files
- `.venv/bin/lint-imports`：Contracts: 2 kept, 0 broken
- git commit：PENDING（READY-003/004 变更尚未提交且与本任务共享 test_cli.py / _wiring.py，随二者待 Orchestrator 统一提交）

### DoD 逐项核对（howto §43）

- [x] 实现（方案 A：快路径只读探测 + 惰性写路径「退避 + 只读复探」收敛；稳态零写窗口）
- [x] 公共 API（新增 `views.missing_views`；`open_query_service` / `register_views` 签名与返回语义零变化）
- [x] 数据契约（探测与惰性注册同源单一事实源：`_base_view_sql` / `_asof_view_sql`；PROVISIONAL 模板升级注记入档）
- [x] 错误处理（重试耗尽 IOException 有界上抛；非锁冲突 duckdb.Error 不吞、语义与既有一致）
- [x] 日志（无新增事件，EVENT_VOCABULARY 不变）
- [x] 指标（无观测面变化）
- [x] 单测（TestOpenQueryServiceConcurrency 6 用例：快路径 spy+幂等+read_only 断言、重试收敛/耗尽、missing_views 迟到类型检出、子进程并发冒烟两态）
- [x] 边界（全新 catalog 首次创建、新增 canonical 类型检出补注册、空数据目录探测空集快路径、同文件 read_only 多进程共存）
- [x] 失败（持续锁冲突 → 有界上抛不死等；两态冒烟 stderr 无 lock 异常）
- [x] 恢复（catalog 为派生物，删除即重建；复探复用他进程注册成果，冲突后状态收敛）
- [x] 集成测试（全量 1376 passed；test_views/test_query 存量零改写回归绿）
- [x] 静态分析（ruff All checks passed）
- [x] 类型检查（mypy Success，72 文件）
- [x] 无未声明假设（DEC-1~4 + PROVISIONAL 注记入档）
- [x] 验收通过（GWT 2/2 + 复跑记录如上）

### 接管性抽查

仅凭本任务单 + `_wiring._register_and_reopen` / `_open_ready_readonly` docstring + `views.missing_views` docstring，可完整理解方案 A 语义（快路径判据、写窗口仅剩首次/新增类型、退避+复探收敛理由、模板升级 PROVISIONAL），无需读调用点以外实现。抽查通过。

## Deferred Acceptance

（无——两态并发冒烟、幂等、回归均已闭环；查询超时防护为本任务前置任务 READY-003 的 DEF-008，非本任务验收依赖。）
