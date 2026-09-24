# READY-004 — CLI 资源生命周期 ExitStack 统一管理

## 派发信息

- 任务单：2026-09-21 服务就绪审计 SR-11（P2）
- 设计依据：架构 02（CLI 层零业务逻辑 + 资源管理）、D08 §2（命令树）；CPython `__del__` 兜底的关闭次序风险
- Agent：Orchestrator 主会话直执（沿用 QUERY-001~003 / READY-002~003 惯例）
- 派发时间：2026-09-21
- 状态：DONE（2026-09-22 验收通过）
- 依赖：CLI-001（命令组/组合根）、SR-01 接线后的 `open_meta`（已含 startup_repair）

## 审计证据（SR-11 原文要点）

- [binance_spot.py](../../../src/chronoforge/connectors/binance_spot.py) 有显式 `close()` 但 CLI 路径从不调用；[pipeline_cmd.py](../../../src/chronoforge/cli/pipeline_cmd.py) `--all-due` 循环内新建 connector/runner/meta 后无 close，兜底靠 `__del__`。
- CPython 引用计数下基本可靠，但 SQLite WAL 与 DuckDB 文件锁在解释器关闭次序上存在理论风险；长驻进程化后必然泄漏。

## file_ownership

- `src/chronoforge/cli/pipeline_cmd.py`
- `src/chronoforge/cli/query_cmd.py`
- `src/chronoforge/cli/quality_cmd.py`
- `src/chronoforge/cli/registry_cmd.py`
- `src/chronoforge/cli/research_cmd.py`
- `src/chronoforge/cli/_wiring.py`（若需让 open_meta/build 路径适配资源注册）
- `tests/unit/test_cli.py`

## 交付物契约摘要

1. 各命令以 `contextlib.ExitStack` 统一管理 open_meta / build_connector / open_query_service 产生的资源；`finally` 语义由 ExitStack 保证（含 Typer 异常路径与 `typer.Exit`）。
2. `--all-due` 循环：**每个 dataset** 的 connector 在单次迭代结束即 close（循环体内注册进 stack 后随迭代释放，避免 N 个连接器同时存活）。
3. MetaStore.close()（连接关闭）纳入 stack；注意 SR-01：startup_repair 已在 open_meta 内完成，关闭次序不影响修复结果。
4. 不改变现有 CLI 输出与退出码语义（test_cli.py 全量回归为准）。

## 测试要求

- spy 断言：monkeypatch connector.close / MetaStore.close → Given 正常路径、抛异常路径、typer.Exit 路径 When 命令执行 Then close 均被调用且次数正确（--all-due 2 个 dataset → 每个 connector close 1 次）。
- 回归：test_cli.py 全量绿。

## acceptance（GWT）

- [x] Given 命令执行完成或中途异常 When 退出 Then 全部打开的 connector/MetaStore/DuckDB 连接显式关闭（test_cli.py::TestResourceLifecycle：run 正常/异常/typer.Exit 三路径断言 connector close 1 次 + MetaStore close 1 次；replay、query 成功/异常路径断言 con + meta close；meta-only 四命令参数化断言 meta close）
- [x] Given --all-due 多 dataset Then 不存在跨 dataset 存活的未关闭连接器（test_run_all_due_releases_connector_per_iteration：events == ["build", "close", "build", "close"]——connector#1 close 先于 connector#2 build，每连接器 close 恰 1 次）

## 执行记录

### 交付实现（file_ownership 内）

1. **_wiring.py**：新增 `enter_closeable(stack, resource)`（入 `__all__`）——把 `resource.close()` 注册进 ExitStack；close 为 DataConnector 协议外可选能力，经 `getattr` 防御注册、缺失跳过。
2. **pipeline_cmd.py**：`run` / `replay` / `status` 外层 `with ExitStack()`（meta.close 纳入）；`run` 的 job 循环体内层 `with ExitStack()` 包裹 connector 构造与 `runner.run`，迭代结束即释放；dry-run 提前 return 经 stack 正常 unwind。
3. **query_cmd.py / research_cmd.py**：meta + DuckDB `con` 一并纳入 stack（LIFO：先 con 后 meta）；移除原内层 `try/finally con.close()`；渲染移入 with 块内（结果已物化，无可观测差异）。
4. **quality_cmd.py / registry_cmd.py**：meta-only 三命令（report / sync / list-sources / list-datasets）meta.close 纳入 stack。

### 决策入档（How 自由度内）

| ID | 决策 | 理由 |
|---|---|---|
| DEC-1 | connector close 经 `getattr` 防御注册（`_wiring.enter_closeable`），**不给 DataConnector 协议追加 close()** | 协议定义于架构 02 §2 / D04（What 级冻结契约），base.py 不在本任务 file_ownership；7 个实现均有 close() 但协议未声明，getattr 缺失跳过同时兼容既有测试的 `object()` 桩（零测试改写）。MetaStore.close / DuckDB con.close 为确定存在，直接 `stack.callback` |
| DEC-2 | --all-due 每迭代释放 = job 循环体**内层 `with ExitStack()`**，非外层 stack 迭代末手动 `stack.close()` | 词法作用域天然保证 LIFO 与异常安全（runner.run 抛错即 unwind），且 close 先于下一次 build 可被 events 序断言直接验证；外层 stack 仅持有 meta，职责单一 |
| DEC-3 | query/research 的 con 关闭时机由「渲染前（原 try/finally）」改为「渲染后随 stack 释放」 | service.query 返回时结果已物化（frame 在内存），渲染不触 con；CLI 输出与退出码零变化（DEC 对应契约项 4，test_cli 快照回归验证） |
| DEC-4 | replay 路径的 RawStore / CanonicalStoreImpl 不注册 close | 任务契约项 1 限定「open_meta / build_connector / open_query_service 三工厂产物」；GWT 亦仅约束 connector/MetaStore/DuckDB 连接。RawStore 常驻句柄 close()（READY-002）在本路径为纯读打开，留待后续资源面任务统一收口 |

### 范围注记（非偏差）

- `dataset_cmd.py`（dataset add）同样经 open_meta 打开 MetaStore，但**不在本任务 file_ownership**（D10 冻结），未改动；SR-11 的覆盖范围以任务单 ownership 为准。建议随后续 CLI 资源面任务（如 READY-005 派发时的 cli 域联动或独立豁免单）补齐。

### 测试命令输出摘要（2026-09-22 复跑）

- `.venv/bin/pytest -q`（全量）：**1370 passed / 0 failed**（基线 1359 + 新增 11：TestResourceLifecycle），in 127.02s
- 目标组：test_cli.py **45 passed**（34 存量全绿含 TC-X-002~004 / GWT / object() 桩映射测试 + 11 新增）
- `.venv/bin/ruff check .`：All checks passed
- `.venv/bin/mypy src`：Success: no issues found in 72 source files
- `.venv/bin/lint-imports`：Contracts: 2 kept, 0 broken
- git commit：PENDING（随 READY-003 待 Orchestrator 统一提交）

### DoD 逐项核对（howto §43）

- [x] 实现（9 命令资源生命周期 ExitStack 统一管理：pipeline run/replay/status、query、quality report、registry sync/list-sources/list-datasets、research reproduce）
- [x] 公共 API（新增 `_wiring.enter_closeable`（入 `__all__`）；既有命令签名/输出/退出码零变化）
- [x] 数据契约（SR-11 生命周期语义入档代码注释：LIFO 次序 con→meta、迭代内释放）
- [x] 错误处理（异常路径与 typer.Exit 路径释放由 ExitStack finally 语义保证，三路径 spy 测试覆盖）
- [x] 日志（无新增事件，EVENT_VOCABULARY 不变；bind_context/clear_context 既有配对不变）
- [x] 指标（无观测面变化）
- [x] 单测（TestResourceLifecycle 11 用例：run 正常/异常/typer.Exit/all-due 迭代序/replay/query 成功/query 异常/meta-only 4 参数化）
- [x] 边界（typer.Exit 穿透 with 块、dry-run 提前 return 经 unwind、BadParameter 早于资源打开无需释放、close 为可选能力的缺失跳过）
- [x] 失败（runner.run 抛 ChronoForgeError → close 均调用且退出码 1；service.query 抛 ValueError → con+meta 关闭且退出码 1）
- [x] 恢复（SIGTERM→KeyboardInterrupt（SR-02）路径随内层/外层 stack 释放，run_log 终态与锁释放机制不受影响；open_meta 内 startup_repair 先于关闭，SR-01 修复结果不受关闭次序影响）
- [x] 集成测试（test_cli 全量回归绿；既有 build_connector→object() 桩测试零改写通过，验证 DEC-1 防御注册）
- [x] 静态分析（ruff All checks passed）
- [x] 类型检查（mypy Success，72 文件）
- [x] 无未声明假设（DEC-1~4 + 范围注记入档）
- [x] 验收通过（GWT 2/2 + 复跑记录如上）

### 接管性抽查

仅凭本任务单 + pipeline_cmd.py SR-11 注释 + `_wiring.enter_closeable` docstring，可完整理解生命周期语义（外层 stack 管 meta、内层 per-job stack 管 connector、LIFO 先 con 后 meta、getattr 防御注册理由、dataset_cmd 范围注记），无需读 ExitStack 调用点以外的实现。抽查通过。

## Deferred Acceptance

（无——验收项均已闭环；dataset_cmd 范围注记见执行记录，属所有权边界而非验收依赖。）
