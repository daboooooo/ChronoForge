# READY-003 — QueryService LIMIT / 超时防护

## 派发信息

- 任务单：2026-09-21 服务就绪审计 SR-10（P2）
- 设计依据：D07 §1（QueryService 契约扩展：默认 LIMIT）；架构 02 规则 4（read_only 共享连接的资源治理）
- Agent：Orchestrator 主会话直执（沿用 QUERY-001~003 / READY-002 惯例）
- 派发时间：2026-09-21
- 状态：DONE（2026-09-22 验收通过）
- 依赖：QUERY-001（DuckDBQueryService）、STORAGE-004（视图注册）

## 审计证据（SR-10 原文要点）

- [query.py](../../../src/chronoforge/research/query.py) 白名单 + 参数化做得很对，但无默认 LIMIT、无超时防护，as-of 全表物化。
- 后果：大 canonical 表宽时间范围查询会长时间占满内存（DuckDB read_only 连接共享于同进程）。

## file_ownership

- `src/chronoforge/research/query.py`
- `src/chronoforge/cli/query_cmd.py`（`--limit` 透传）
- `src/chronoforge/config/settings.py`（默认值与可调项）
- `tests/integration/test_query.py`

## 交付物契约摘要

1. 默认行数上限 `query_max_rows`（Settings 新增，缺省值执行时定，建议 100k~1M）；`query()` 恒附加 `LIMIT ?`（用户显式 `limit` 参数可调低不可调高，或超限报错——二选一入档决策）。
2. 超限行为二选一（执行时决策并入档）：
   - A. 截断返回 + `QueryResult` 新增 `truncated: bool` 元信息（CLI 输出提示）；
   - B. 直接 `ValueError` 提示缩小时间范围（保守，无歧义）。
3. **超时防护注意**（执行前核实，不得臆造）：DuckDB **无内建 SQL 级 query_timeout**；可行路径为 Python 侧执行线程 + `con.interrupt()`，或 `SET memory_limit` 约束。若 interrupt 方案复杂度不可接受，超时可登记 Deferred，仅落地 LIMIT——在决策表记录理由即可。
4. as-of 全表物化问题随 LIMIT 一并缓解；`{type}_asof` 视图的 row_number 窗口不受影响。

## 测试要求

- 超限：构造 > 上限行数（可小上限注入测试）→ 触发约定行为（截断标志 / ValueError）。
- 边界：恰好等于上限、limit=0、负数 → 参数校验。
- 回归：TC-R-001~003 全绿（as-of 两 vintage、注入防御、read_only）。

## acceptance（GWT）

- [x] Given 宽时间范围大表查询 When 执行 Then 行数受上限约束且元信息/报错符合入档决策（test_query.py::TestRowLimit::test_over_cap_truncated_with_stable_order（max_rows=2 → 2 行 + truncated=True + 稳定排序前缀）、test_asof_query_respects_limit（点时视图同受约束）、test_explicit_limit_above_cap_clamped（limit 上调被钳制）、test_default_max_rows_from_settings_constant（缺省=Settings 单一事实源））
- [x] Given 用户显式 limit When 合法 Then 结果行数 ≤ limit（test_explicit_limit_below_cap（limit=2 → 2 行 + truncated=True）、test_explicit_limit_equal_row_count_not_truncated（limit=行数 → truncated=False））

## 执行记录

### 交付实现（file_ownership 内）

1. **settings.py**：新增 `DEFAULT_QUERY_MAX_ROWS = 200_000` 模块常量（单一事实源）+ `query_max_rows` 字段（`ge=1`，default 引用常量）+ env 映射 `CHRONOFORGE_QUERY_MAX_ROWS` + `dict_for_logging` 纳入（与其他运行时字段一致性）。
2. **query.py**：
   - `QueryResult` 新增 `truncated: bool = False`（frozen dataclass 尾部带默认值，既有构造点/消费方零破坏）；
   - `DuckDBQueryService.__init__` 新增 `max_rows: int = DEFAULT_QUERY_MAX_ROWS`（构造注入，`< 1` 抛 ValueError）；
   - `query()` 新增 `limit: int | None = None` 参数，有效上限 = `min(limit, max_rows)`；SQL 构建恒附加参数化 `LIMIT ?`（取 effective+1 探测截断，恰好等于上限时 truncated=False）；`_build_sql` 增加 `row_limit` 参数。
3. **query_cmd.py**：`--limit` 透传 `service.query(limit=...)`；`--json` payload 增加 `truncated` 字段；截断时 stderr 黄色提示（不污染 stdout 的 JSON/CSV 数据流）。

### 决策入档（How 自由度内）

| ID | 决策 | 理由 |
|---|---|---|
| DEC-1 | 超限行为选 **A：截断返回 + `truncated` 元信息**（CLI stderr 提示 + JSON 字段），不抛错 | 研究探索式查询不应在上限处硬失败；元信息双通道（机器读 JSON / 人读 stderr）保证无歧义；选项 B 要求用户预知数据规模，可用性差且与「默认 LIMIT 保护」目的相悖 |
| DEC-2 | 用户显式 `limit` 语义 = **可调低不可调高**：effective = min(limit, max_rows)，超上限不报错按上限截断 | 与 DEC-1 自洽（截断语义下钳制是唯一无歧义解释）；GWT-2「结果行数 ≤ limit」仍成立 |
| DEC-3 | 截断探测 = `LIMIT effective+1` 取数，行数 > effective 时截断至 effective 并标 `truncated=True` | 精确语义：恰好等于上限时 `truncated=False`（避免「==上限即截断」的假阳性）；代价仅多物化 1 行 |
| DEC-4 | 缺省值 **200_000**（任务单建议区间 100k~1M 取保守端） | 宽表（OHLCV ~15 列）20 万行 polars 物化 ≈ 数十 MB，共享 read_only 连接（架构 02 规则 4）内存压力可控；更大数据量应按 D07 语义以时间窗口分页。单一事实源 = `settings.DEFAULT_QUERY_MAX_ROWS`，Settings 字段与构造缺省同源引用，无漂移 |
| DEC-5 | **超时防护登记 Deferred（DEF-008），仅落地 LIMIT**（任务单明示允许） | 执行前核实：duckdb 1.5.5 无 SQL 级 query_timeout；`DuckDBPyConnection.interrupt()` 存在（Python 侧线程 + interrupt 可行）。但每查询需工作线程 + 超时 join + InterruptException 传播，且共享连接上中断未及时响应时连接仍被占用——线程化故障面引入共享读路径，D09 亦无慢查询注入测试基建（人为慢查询 → CI flaky）。复杂度/收益比不可接受，理由入档 |
| DEC-6 | LIMIT 追加于 ORDER BY 之后、值参数化（`LIMIT ?`） | DuckDB 支持 LIMIT 参数绑定；ORDER BY + LIMIT 走 TOP-K 排序，排序内存随上限有界（SR-10 内存治理主路径） |

### 偏差登记（file_ownership 外的必要联动）

| ID | 偏差 | 理由 |
|---|---|---|
| DEV-1 | `_wiring.py`（ownership 外）1 处：`open_query_service` 构造服务时注入 `max_rows=settings.query_max_rows` | 契约要求 `query_max_rows` 为「可调项」（env `CHRONOFORGE_QUERY_MAX_ROWS`）；组合根不接线则 env 为死配置。缺省同源（DEFAULT_QUERY_MAX_ROWS）无行为漂移；改动 1 行。⚠️ READY-005（SR-16 open_query_service 改造）派发时需保留此注入 |
| DEV-2 | `tests/unit/test_cli.py` 1 行：`test_json_output_snapshot` 期望 payload 补 `"truncated": False` | QueryResult 元信息扩展的必然涟漪（--json 直序列化快照断言）；与 QUERY-003 联动维护 EXPECTED_TABLES 同例 |

### 测试命令输出摘要（2026-09-22 复跑）

- `uv run pytest`（全量）：**1359 passed / 0 failed**（基线 1348 + 新增 11：TestRowLimit 11），in 126.70s
- 目标组：test_query.py 33 passed（22 存量含 TC-R-001~003 全绿 + 11 新增）、test_cli.py 34 passed
- `uv run ruff check src tests`：All checks passed
- `uv run mypy src/chronoforge`：Success: no issues found in 72 source files
- `uv run lint-imports`：Contracts: 2 kept, 0 broken
- git commit：PENDING（待 Orchestrator 统一提交）

### DoD 逐项核对（howto §43）

- [x] 实现（恒附加 LIMIT + 截断元信息 + 参数校验）
- [x] 公共 API（`query(..., limit=None)`；`DuckDBQueryService(con, registry, max_rows=DEFAULT_QUERY_MAX_ROWS)`；`QueryResult.truncated`；既有调用零破坏）
- [x] 数据契约（规则 7 入档模块 docstring；LIMIT 恒附加、truncated 语义、单一事实源常量）
- [x] 错误处理（limit ≤ 0 / max_rows < 1 → ValueError；JSON/CSV 数据流不受 stderr 提示污染）
- [x] 日志（查询路径无新增事件需求，EVENT_VOCABULARY 不变；query_max_rows 纳入 dict_for_logging）
- [x] 指标（QueryResult.elapsed_ms/row_count 既有观测不变；truncated 为新增元信息）
- [x] 单测（TestRowLimit 11 用例：超限截断/恰好上限/显式 limit/钳制/limit=0/负数/max_rows<1/as-of 受限/空结果/未注册视图/缺省同源）
- [x] 边界（恰好等于上限 truncated=False、limit=0、负数、空结果、视图未注册）
- [x] 失败（limit 非法 → ValueError 且连接无恙；超上限 → 钳制不报错）
- [x] 恢复（截断为元信息标注，无状态残留；重复查询确定性由存量 TestStableOrdering 保证）
- [x] 集成测试（test_cli 快照联动 DEV-2；test_features/test_snapshot 经 QueryServiceLike 协议消费不受影响，随全量回归全绿）
- [x] 静态分析（ruff All checks passed）
- [x] 类型检查（mypy Success，72 文件）
- [x] 无未声明假设（DEC-1~6 + DEV-1~2 入档）
- [x] 验收通过（GWT 2/2 + 复跑记录如上）

### 接管性抽查

仅凭本任务单 + query.py 模块 docstring 规则 7 + 决策表，可完整理解上限来源（DEFAULT_QUERY_MAX_ROWS 单一事实源）、截断探测语义（LIMIT effective+1）、limit 钳制规则与超时 Deferred 理由，无需读 _wiring 细节。抽查通过。

## Deferred Acceptance

| ID | 验收项 | 依赖/关闭条件 |
|---|---|---|
| DEF-008 | DuckDB 查询超时防护（执行线程 + `con.interrupt()` 或等效资源约束；DEC-5 已核实 duckdb 1.5.5 无 SQL 级 query_timeout、interrupt() 存在） | 立后续专项任务（建议与 READY-005 open_query_service 改造统筹，避免共享连接线程语义二次返工）；LIMIT + TOP-K 已使返回行内存有界，主风险面已覆盖 |
