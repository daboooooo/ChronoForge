# ChronoForge 全面审计报告（文档 + 代码）

| 项目 | 内容 |
|---|---|
| 审计日期 | 2026-11-05 |
| 审计对象 | `/Users/horsenli/Works/ChronoForge`（Python 3.12，uv + polars + DuckDB + Parquet） |
| 审计范围 | `src/chronoforge`（82 文件 / 23,026 行）、`scripts`（26 文件 / 12,848 行）、`tests`（69 文件 / 31,552 行 / 71 fixtures）、`docs`（94 篇 / 24,702 行）、`config/`、README、git 工作区 |
| 审计性质 | 初始审计为只读；后续按 §8.2 批次实施修复（含源码、测试、fixture、配置与文档），每批附实跑验证结果 |
| 最终判定 | **READY（按 G1–G7 门禁判定，2026-11-05 复评）：G1–G7 全部 ✅**——A-2、A-8 均已关闭；另有 2 项 BLOCKED 取证（sosovalue/coinmarketcap 未当日重新抓包），详见 §9.1 |

---

## 0. 结论速览

### 0.1 总体判定：READY（G1–G7 全部通过）

架构分层、类型安全、测试独立性和日频源的幂等回退实现质量扎实：mypy strict 全绿、import-linter 两个架构契约均 KEPT、1972 个测试全过。本轮最初发现的**数据正确性核心不变量破口已于 2026-11-05 全部关闭**：

1. ~~管道层 `normalize` 全失败/零产出时仍推进 checkpoint（C1）~~ —— **异常路径与逐行静默跳过路径均已修复（2026-11-05）**：全失败/高丢弃率现在触发 `QualityError` 并阻止 checkpoint 推进；新增行级丢弃率门禁（A-8，阈值 `normalize_drop_threshold=0.5`），合法跳过按 reason 白名单排除（见 §5.2）；
2. ~~三个连接器按**想象出来的 API 结构**编码（deribit chart、sec_edgar companyfacts、ccxt trades），生产中静默产出 0 行~~ —— deribit 与 sec_edgar **已修复（C2/C3/A-1，2026-11-05）**；ccxt trades 的分页/`order` 参数问题（报告编号 **H3**，由用户任务中的"H1"指代）**已于 2026-11-05 修复**；
3. ~~TGA 主模型的组件聚合去重键缺少 subcategory，导致同类多子类的基线/事件现金流被**静默坍缩**（TGA-1），直接污染主模型输出~~ —— **已修复（2026-11-05）**：仅事件行去重且键含 detail，基线行不再去重；去重前后月度守恒断言锁死组件丢失；
4. ~~`scripts/fdam_attention.py` 存在语法错误，FDAM 日报流程完全不可用（B-1）~~ —— **已修复（2026-11-05）**：删除重复 `__main__` 入口与 281 行死代码，`py_compile` 通过；
5. ~~整个 TGA 子系统（`scripts/tga_*.py`、`config/tga/`、treasury 连接器、迁移 0005、TGA 测试）**全部未纳入版本控制**，且 38 个已跟踪文件处于修改态，部分提交会直接 ImportError。~~ —— **已修复（2026-11-05）**：以单次成套提交 `5590163` 纳入 101 个文件（27,774 insertions / 454 deletions），提交前全量 1782 passed、ruff/mypy/lint-imports 通过，提交后工作区干净；`reports/` 已入 `.gitignore`。

### 0.2 严重级别统计

| 级别 | 数量 | 编号 |
|---|---|---|
| CRITICAL | **6**（现存 0，C1/C2/C3/A-1/TGA-1/B-1 均已修复） | C1、C2、C3、A-1、TGA-1、B-1 |
| HIGH | **14**（现存 0；H1–H7、A-2、A-3、A-8 均已修复并关闭） | H1–H7、A-2、A-3、A-8、G-1、D-1、D-2、D-3 |
| MEDIUM | **20**（现存 0；M1–M10、A-4、A-5、A-6、A-9、C-2、C-3、C-4、D-4、D-5、TGA-3 全部已修复） | M1–M10、A-4–A-6、C-2、C-3、C-4、D-4、D-5、TGA-3 |
| LOW | **11**（现存 10，D-7 已修复；D-6 按历史留痕不处理） | L1–L6、A-7、C-6、C-7、D-6、D-7 |
| 合计 | **51** | |

### 0.3 最需要先修的事项（按顺序）

1. ~~**C1 — normalize 零产出仍推进 checkpoint**（[runner.py#L459-L515](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/pipeline/runner.py#L459-L515)）~~ —— **异常路径与行级跳过路径均已修复（2026-11-05）**。它是所有连接器缺陷的"静默放大器"，两条出口均已堵住：批量异常走 C1 错误率门禁，逐行跳过走 A-8 行级丢弃率门禁（详见 §3.1 C1 与 §5.2）。
2. ~~**C2 / C3 / A-1 — 连接器响应契约与 fixture 同源**（[deribit.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/deribit.py)、[sec_edgar.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/sec_edgar.py)）~~ —— **已修复（2026-11-05）**：两连接器已按真实响应解包并换真实 fixture，补契约与负向测试（详见 §3.1 C2/C3、§5.1 A-1）。
3. ~~**TGA-1 — 组件聚合去重键缺 subcategory**~~ —— **已修复（2026-11-05）**：事件行去重键纳入 `detail`，基线行不再参与去重；新增去重前原始月度守恒、去重后基线/月度/身份集合三重核对，任何组件丢失显式抛 `Q-TGA-012`（详见 §4.2）。
4. ~~**B-1 — `fdam_attention.py` 语法错误**，FDAM 日报流程完全不可用~~ —— **已修复（2026-11-05）**：死代码已删除、入口恢复，详见 §6.1。

> 另需在 P0 批次内同步处理 **G-1**（TGA 全部未跟踪文件的成套提交）。

---

## 1. 审计方案

### 1.1 审计目标

1. 判定当前代码库是否满足进入下一阶段（发布/扩大数据源/对外使用 TGA 输出）的质量门禁；
2. 找出会导致**静默数据丢失、数据损坏、不可恢复状态**的缺陷，而不止于代码风格；
3. 核验项目既定的硬性约束（PIT、幂等重采、熔断冷却、日志规范、TGA 口径）是否在代码/配置/文档中一致落地；
4. 核验文档承诺与实际实现是否一致（测试层级、数据源数量、类型数量、环境变量、TGA 定位）；
5. 输出可执行的分级问题清单与修复批次，作为后续迭代基线。

### 1.2 审计范围与规模

| 模块 | 文件数 | 行数 | 审计深度 |
|---|---|---|---|
| `src/chronoforge/pipeline` | 6 | 2,159 | 深度（逐函数） |
| `src/chronoforge/storage` | 11 | 3,734 | 深度（事务/原子性/迁移） |
| `src/chronoforge/connectors` | 18 | 8,729 | 深度（全部连接器窗口语义与解析契约） |
| `src/chronoforge/quality` | 5 | 1,861 | 深度（规则有效性与日历） |
| `src/chronoforge/models` | 15 | 2,189 | 中（schema/枚举/身份） |
| `src/chronoforge/cli` | 9 | 1,145 | 中（入口隔离与错误码） |
| `src/chronoforge/registry` | 2 | 286 | 中（源规格与幂等注册） |
| `src/chronoforge/config` | 3 | 512 | 中（settings 与环境变量） |
| `src/chronoforge/features` / `research` / `security` | 3 / 3 / 3 | 700 / 699 / 260 | 浅（分层契约与依赖方向） |
| `src/chronoforge/ui.py` 等根模块 | — | 含于总计 | 浅 |
| `scripts/` | 26 | 12,848 | 深度（TGA 七个脚本全读；其余抽读） |
| `tests/` | 69 | 31,552 | 深度（测试契约、fixture、失败恢复、独立性） |
| `docs/` | 94 | 24,702 | 中（architecture/design/strategy/implement/review 交叉核对） |
| `config/` | 4 个 TGA YAML | 327 | 深度（键与代码读取一一对应） |
| `README.md` | 1 | — | 深度（数字与表格核对） |

**明确排除**：`data/`、`meta/`、`var/`、`output/`、`reports/` 等运行时产物内容（仅检查其版本控制与 .gitignore 覆盖）；第三方依赖实现；历史审计报告正文（仅核对其中 CRITICAL/HIGH 项是否已被修复）。

### 1.3 审计方法与证据要求

| 方法 | 具体做法 | 证据要求 |
|---|---|---|
| 静态检查 | `mypy`、`ruff`、`lint-imports`、`pytest`（默认排除 smoke） | 记录命令、版本、退出码与摘要 |
| 分区深度审读 | 四个独立分区（核心源码 / TGA / 文档一致性 / 测试·脚本·配置）并行审读 | 每个发现必须给出 `file:line` 与关键代码行 |
| 契约核对 | 将项目记忆中的硬性约束逐条映射到代码/配置/文档位置 | 每条约束给出"已落实 / 部分落实 / 违反 / 需复核" |
| trust-but-verify | 对代理报告的 CRITICAL 与 HIGH 项，主审计回读源码逐行复核 | 复核通过的标注 ✅ 已核实；未能复核的标注"需复核" |
| 反证测试 | 对"测试全绿"的结论，检查 fixture 是否与生产解析路径同形 | 给出 fixture 路径与连接器读取路径对比 |
| 只读验证 | `py_compile`、`yaml.safe_load`、`grep` 统计，不写入任何数据 | 记录命令与输出 |

**证据分级**：本报告中标注 ✅ 已核实的条目由主审计直接回读源码确认；未标注者为分区代理提供证据、主审计未逐行复读（涉及 H1–H6 与部分 M/L 项，建议修复前再确认行号漂移）。

### 1.4 检查项清单

| # | 检查项 | 判定关注点 |
|---|---|---|
| 1 | 正确性 | 计算/映射/时间语义是否符合口径（UTC、T+1、PIT、交易日历） |
| 2 | 静默数据丢失 | HTTP 200 + 正常退出 + checkpoint 推进，但 canonical 实际缺数 |
| 3 | 崩溃一致性 | 写入与 checkpoint 顺序、原子换名、fsync、迁移幂等 |
| 4 | 幂等与重采 | 自然键 upsert、起点回退重采、重复 run 不产生重复数据 |
| 5 | 错误处理与熔断 | 错误分类、重试范围、熔断冷却与 half-open、失败隔离 |
| 6 | 连接器契约 | 响应结构与解析路径同形、分页完整、未收盘数据过滤 |
| 7 | 日志规范 | 结构化字段、profile、跳过/丢弃是否留痕 |
| 8 | 配置一致性 | YAML 键是否被读取、默认值语义、config_hash 覆盖 |
| 9 | 文档一致性 | 数量/名称/流程/口径是否与代码一致；过期与矛盾 |
| 10 | 测试有效性 | 覆盖真实失败路径、fixture 真实性、层级承诺是否兑现 |
| 11 | 架构契约 | 分层依赖、research 不依赖 connectors、TGA 边界守卫 |
| 12 | 工程卫生 | 未提交变更成套性、生成物是否入版本库、脚本可执行 |

### 1.5 严重级别定义

| 级别 | 判定标准（满足任一条） |
|---|---|
| **CRITICAL** | 静默数据丢失或损坏；不可恢复状态；核心流程完全不可用；违反核心不变量且无告警；生产环境必然产出错误结果 |
| **HIGH** | 会造成返工或接口不兼容；测试无法验证核心行为；约束明文违反；关键路径存在可触发崩溃窗口；版本库状态会导致构建/导入失败 |
| **MEDIUM** | 局部功能缺陷或配置陷阱；文档与实现显著不一致；可测但缺测试；不立即致错但会误导使用者 |
| **LOW** | 代码/文档质量瑕疵、日志口径不统一、冗余维护、命名与格式问题 |

### 1.6 准入门禁与通过标准

| 门禁 | 标准 | 本次结果 |
|---|---|---|
| G1 静态质量 | `mypy src` 0 error；`ruff check src` 0 error；`lint-imports` 全 KEPT | ✅ 通过（ruff 仅 scripts 有错） |
| G2 测试 | 全量 `pytest` 通过；无真实网络依赖；失败恢复测试为真实链路 | ✅ 通过（1972 passed，含 C1/C2/C3/A-1/A-2/A-8/A-9/TGA-1、G3 全批次回归及 MEDIUM 批 M1–M10/A-4/A-5/A-6/C-4；`TZ=Asia/Shanghai` 下同为 1972 passed；M2 删除 7 条仅覆盖已删 reconcile 的用例，M9/M7/M10/C-4 新增契约与矩阵用例） |
| G3 契约测试 | 每条连接器有"真实响应样本 → fetch → normalize"全链路测试，禁止仅直喂内层结构 | ✅ 通过（2026-11-05：13 个连接器均有 fetch→normalize 全链路 + 负向用例 + `SOURCE.md`；真实抓包：treasury_fiscal/treasury_refunding/alternative/coinmetrics；文档/客户端模型：binance_spot/binance_futures/yahoo/fred/ccxt_bridge/deribit/sec_edgar；历史实测形状：sosovalue/coinmarketcap） |
| G4 静默丢失 | 任何"有输入但零产出"必须置为失败或阻止 checkpoint 推进 | ✅ 通过（2026-11-05：C1 覆盖批量异常路径，A-8 覆盖连接器逐行跳过路径——非白名单丢弃率 ≥0.5 即 `QualityError`，checkpoint 不推进；白名单外零产出已不可静默） |
| G5 约束核验 | 项目硬性约束逐条有代码/配置/文档三方证据 | ✅ 通过（2026-11-05：H4 起点回退、C-2 死键均已修复；TGA-1 亦已修复；A-8 配置项已写入 `.env.example`/README/设计文档） |
| G6 版本库 | 工作区可构建；生成物不入库；变更成套 | ✅ 通过（2026-11-05 提交 `5590163` 成套纳入 101 文件；`reports/` 已入 `.gitignore`；提交后工作区干净；A-9 已提交 `4d28b93`/`4840f28`） |
| G7 文档一致 | README/设计文档/TGA 文档与实现一致 | ✅ 通过（2026-11-05：D-1/D-2/D-3/D-4/D-5/TGA-3 已修复，D-7 重命名；新增 README 源数/类型数一致性测试；A-8 门禁已同步 06-quality/08-cli-config-security） |

**通过标准**：G1–G7 全部为 ✅ 方判定 READY。本次 **G1–G7 全部 ✅**，按门禁标准判定 **READY（2026-11-05 复评）**。A-2、A-8、A-9 已关闭，H6 已修复并提交；sosovalue/coinmarketcap 未能当日重新抓包（依据为历史实测形状），详见 §9.1 残余风险。

### 1.7 审计执行顺序

1. 基线采集（静态检查 + 全量测试 + git 状态）→ 建立客观证据；
2. 四个分区并行深度审读（核心源码 / TGA / 文档 / 测试·脚本·配置）；
3. 主审计对 CRITICAL 逐条 `trust-but-verify` 回读；
4. 约束矩阵逐条映射；
5. 发现合并去重、定级、排批次；
6. 写入报告（本文件）。

### 1.8 风险与局限

- **动态行为未验证**：审计以静态阅读与只读命令为主，未对真实外部 API 做联网调用（受时间/配额/合规限制）。连接器"生产恒 0 行"的判定基于响应结构比对，修复时应用真实响应样本回归。
- **未做性能压测**：L4 的 O(N²) 判定基于请求/重写语义分析，未实测（H1 的 endTime 截断已消除其对 backfill 的放大）。
- **未覆盖运行时数据内容质量**：`data/`、`meta/` 中历史数据是否已被既有缺陷污染（尤其 deribit/sec_edgar/TGA 三条链路）**需要单独的数据回溯核对**，本报告只能确认代码路径存在缺陷。
- **代理证据的行号漂移**：工作区有 38 个未提交修改，部分代理给出的行号可能与最终提交版本有偏移，修复前应重新定位。
- **范围外**：未审计第三方依赖（ccxt/polars/duckdb）自身行为与供应链安全；未做渗透测试。

---

## 2. 基线验证（命令与结果）

| 检查 | 命令 | 结果 |
|---|---|---|
| 类型检查 | `uv run mypy src` | ✅ Success，82 files（models/storage/pipeline strict） |
| Lint | `uv run ruff check src scripts tests` | ⚠️ 89 errors，**src 为 0**，全部集中在 `scripts/`（F821×32、E501×20、UP031×12、E702×10、F541×6、F706×1 等） |
| 架构契约 | `uv run lint-imports` | ✅ 2 contracts KEPT，0 broken |
| 全量测试 | `uv run pytest -q` | ✅ 1770 passed，11 warnings，87.33s |
| 脚本可编译 | `py_compile scripts/fdam_attention.py` | ❌ `SyntaxError: 'return' outside function`（line 1518） |
| 枚举核对 | AST 统计 `CanonicalType` | 32 个（README 写 27） |
| 源规格核对 | 读 `DEFAULT_SOURCES` | 13 个（README 写 11） |
| 工作区 | `git status --porcelain` | 38 个 M + 30 个 untracked（含整个 TGA 子系统与 `reports/`） |

### 2.1 最终验证（2026-11-05，G5 + G3 Batch 1–4 完成后）

| 检查 | 实跑命令 | 结果 |
|---|---|---|
| 全量测试 | `timeout 600 uv run pytest -q` | ✅ **1862 passed, 10 warnings**（exit 0；含 H1/H3/H2/M6/H7/H5/H6 回归） |
| 跨时区全量 | `TZ=Asia/Shanghai uv run pytest -q` | ✅ **1862 passed, 10 warnings**（与默认 TZ 结果一致；H7 修复前同一用例在 UTC+8 下失败） |
| Lint | `uv run ruff check src tests` | ✅ All checks passed（exit 0） |
| 类型检查 | `uv run mypy src` | ✅ Success: no issues found in 83 source files（exit 0） |
| 架构契约 | `uv run lint-imports` | ✅ 2 kept, 0 broken（83 files / 283 dependencies，exit 0） |
| Batch 3 定向 | `uv run pytest tests/integration/{test_sosovalue,test_coinmarketcap,test_coinmetrics,test_alternative}.py -q` | ✅ 111 passed in 0.43s |
| Batch 4 定向 | `uv run pytest tests/integration/{test_ccxt_bridge,test_deribit,test_sec_edgar}.py -q` | ✅ 123 passed in 0.63s |

> 说明：审计期间 `deribit` 测试曾因连接器未读取 `result.continuation` 在多页 mock 下挂死（进程残留）；该缺陷已于 2026-11-05 修复（continuation 驱动 + 重复 token/页数上限护栏），见 §8.2 P2 与 §9.1(d)。

---

## 3. 核心源码审计（`src/chronoforge`）

### 3.1 CRITICAL

#### C1. normalize 全失败/零产出时 checkpoint 照常推进 —— 静默数据丢失 ✅ 已核实 ✅ 已修复（2026-11-05）

- **位置**：[runner.py#L459-L515](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/pipeline/runner.py#L459-L515)
- **证据（修复前）**：
  - 旧 L447-453：`connector.normalize(batch)` 抛异常时只累加 `error_count` 后 `continue`；
  - 旧 L454-455：`input_count` 仅在 normalize **成功返回记录**时累加；
  - 旧 L473-479：`total = result.input_count`，`if total > 0 and result.error_count / total > threshold` —— 当所有 batch 都抛异常时 `total == 0`，短路，**永不触发 QualityError**；
  - 下游 `_decide_status` 将空产出判为 PARTIAL_SUCCESS，而 cursor 属终态，`advance_chunks` 只依据 fetch 成功推进。
- **影响**：raw 有数据但 canonical 永远缺失，窗口不再重拉。这正是审计要求中定义的 Silent Data Loss，也违反 [cursor.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/pipeline/cursor.py) 的"checkpoint ≤ 已验证持久化数据边界"不变量。它是 C2/C3/A-1 能造成长期损害的直接原因。
- **修复实现**：
  - 新增 [\_payload_units()](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/pipeline/runner.py#L433-L446)：失败调用无法回传记录数，按 payload 单元数（list 长度 / 常见行数组键）给出下界，未知形状返回 0、调用方以 `max(..., 1)` 兜底；
  - 失败批次计入 `failed_units`（与成功记录同口径）；provenance 注入失败单独计 `record_errors`，避免与批次失败重复计数；
  - 阈值判定改为 `attempted = input_count + failed_units`、`attempted_errors = record_errors + failed_units`，分母不再可能退化为 0：全失败时错误率 = 100% → `QualityError` → run FAILED，cursor 不推进；合法空批次（normalize 正常返回 `[]`）仍 `attempted = 0`，不误报。
  - **残余（A-8，已于 2026-11-05 关闭）**：C1 只覆盖"批量抛异常"；连接器对畸形/非数值行记 `note_skip` 后返回 `[]` 时 `attempted` 仍为 0。实测 binance_spot 两行 `open="bad"` → normalize 0 行、canonical 0 行、run **SUCCESS**、checkpoint 已推进。A-8 新增"行级丢弃率"门禁（`normalize_drop_threshold=0.5`，白名单外跳过计入分子），该路径现已 `QualityError`（详见 §5.2 A-8）。
- **回归测试**：
  - [test_normalize_all_failures_blocks_checkpoint](file:///Users/horsenli/Works/ChronoForge/tests/integration/test_pipeline.py#L550-L568)：3/3 chunk 全失败 → `QualityError(72/72)`、run `FAILED`、`checkpoint_after=None`、`meta.get_checkpoint=None`、canonical 0 行；
  - [test_normalize_partial_failure_within_threshold_advances](file:///Users/horsenli/Works/ChronoForge/tests/integration/test_pipeline.py#L571-L594)：12 chunk 中 1 个失败（24/288 = 8.3% < 10%）→ `PARTIAL_SUCCESS` 且 checkpoint 推进到终点，入库 264 行（防止过度阻断）。
- **验证结果（2026-11-05）**：`pytest tests/integration/test_pipeline.py` 55 passed；`tests/unit + test_pipeline.py` 977 passed；全量 `pytest -q` **1772 passed**（原 1770 + 2 新增）；`ruff check src` 无问题；`mypy src` Success（82 files）。
- **兼容性影响**：阈值默认值与异常类型未变；历史 run 记录不受影响（新逻辑仅作用于后续 run 的阈值判定）；已落盘 checkpoint 不回退、不重算。行为变化覆盖"normalize 抛异常导致零产出"（C1）与"连接器逐行跳过导致高丢弃率"（A-8）。
- **剩余风险**：`_payload_units` 对未知形状 payload 返回 0（不再兜底推断），行级丢弃检测此时完全依赖连接器的 `note_skip` 结构化上报；若新增连接器漏报跳过原因，门禁覆盖会退化。约束：新增连接器必须调用 `note_skip`/`normalize_skip` 并在契约测试中验证丢弃原因可达（A-8 白名单测试锁定）。

#### C2. deribit chart 响应结构解析错误 —— OHLCV/IV 生产恒 0 行 ✅ 已核实 ✅ 已修复（2026-11-05）

- **位置**：[deribit.py#L302-L325](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/deribit.py#L302-L325)；normalize 侧约 L446、L554
- **证据**：L307 `data = response.json()` 后 L313 直接 `candles = data.get("candles", [])`。Deribit 是 JSON-RPC，数据在 `result` 下；同文件 instruments 路径（约 L366）正确使用 `payload.get("result")`，chart 路径未解包 —— **同一文件内两种口径**。`_normalize_options` 也直接迭代 `payload.items()` 期望内层 `{"BTC":[...]}`。而 `tests/fixtures/deribit/get_tradingview_chart_data/happy.json` 正是内层形状（见 A-1）。
- **影响**：deribit OHLCV/IV 每次 run 均为 0 batch、HTTP 200、checkpoint 推进、canonical 永远为空；且由于 C1，不会有任何告警。
- **修复建议**：统一按 JSON-RPC envelope 解包（`payload.get("result", {})`，再取 `candles`）；用真实响应样本重建 fixture；补"fetch→normalize"全链路用例。
- **真实形状依据（2026-11-05）**：
  - Deribit v2 公共接口响应为 JSON-RPC envelope：`{"jsonrpc":"2.0","result":...}`；
  - `public/get_tradingview_chart_data` 的 `result` 为**并行数组**：`{status, ticks:[u64], open:[f64], high:[f64], low:[f64], close:[f64], volume:[f64], cost:[f64], continuation}`（docs.rs `deribit-http` 0.7 `TradingViewChartData`），**不是** `candles` 对象列表，也不含 `instrument_name` / IV；
  - `public/get_book_summary_by_currency` 的 `result` 是 **BookSummary 列表**（docs.rs `BookSummary`：instrument_name、base_currency、mark_price、bid_price、ask_price、open_interest、mark_iv、last_update_time 等），不是按币种键控的 dict；
  - 当前环境无法直连 Deribit（curl 无响应），形状以官方 v2 接口与客户端模型定义为准，已在 `tests/fixtures/deribit/SOURCE.md` 记录。
- **修复实现**：
  - [\_fetch_chart_data](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/deribit.py#L273-L336)：先解 `data["result"]`（必须为 dict），空 `ticks` 即止；分页取 `result["ticks"][-1]` 作为下一页 `start_timestamp`（保留原 resolution × 60_000 步进修复）；
  - [\_normalize_chart](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/deribit.py#L542-L700)：`instrument_name`/`resolution` 从 `raw_meta` 取（真实 result 无 instrument_name）；按 `ticks/open/high/low/close/volume` 并行数组逐行构造 OHLCV；任一数组短于 ticks、非数值或非有限值 → `normalize_skip` 计数；真实 chart 无 IV，删除伪造 IV 分支（同步移除未用导入）；
  - [\_normalize_options](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/deribit.py#L440-L540)：显式遍历 `payload["result"]` 列表，币种取每项的 `base_currency`（不再猜 `payload.items()`）；
  - [checkpoint_from](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/deribit.py#L716-L770)：chart 取最后一个 tick，options 取 result 长度；
  - **零行防御**：非空 envelope 缺 `result`（或 chart `result` 非 dict）→ 抛 `ProviderError`；`ticks` 非空但所有行被跳过 → `connector.normalize_skip` + `connector.normalize_summary(kept=0, skipped=N)` 告警，不再静默空产出。
- **测试（A-1 联动）**：
  - fixture 换为真实 envelope（`tests/fixtures/deribit/get_tradingview_chart_data/{happy,empty}.json`、`get_book_summary/happy.json`），来源与日期见 `tests/fixtures/deribit/SOURCE.md`；
  - 契约：[test_contract_chart_real_envelope](file:///Users/horsenli/Works/ChronoForge/tests/integration/test_deribit.py) 断言 2 条 OHLCV 及事件时间/收盘价/成交量/`source_id`/`market_id`；
  - 负向：`test_negative_chart_short_arrays_warn_not_silent`（非空 ticks + 短数组 → 0 行但必须 `normalize_summary` 告警）、`test_negative_chart_missing_result_raises`、`test_negative_book_summary_old_dict_shape_raises`。
- **验证结果（2026-11-05）**：`pytest tests/integration/test_deribit.py tests/integration/test_sec_edgar.py` 83 passed；全量 `pytest -q` **1778 passed**；`ruff check src tests` 通过；`mypy src` Success（82 files）；`lint-imports` 2 kept / 0 broken。
- **对已落盘数据的影响**：`data/raw` 与 `data/canonical` 中均无 deribit 分区（chart 因 `candles` 恒空从未 yield 任何 batch；options 亦无 canonical），因此**无需回放/清洗**；但历史 run 若对 deribit chart 记过成功，应视为无效并重采。建议对 `dataset_id` 含 deribit 的 checkpoint 做一次审计后重拉所需窗口。
- **剩余风险**：Deribit 真实抓包未能取得（网络不可达），形状证据为官方客户端模型定义；若官方新增字段不影响并行数组解析。options 的 IV 指标仍由 `mark_iv` 提供，但 canonical 仅落 OPTION 快照，IMPLIED_VOLATILITY 历史仍未落盘（能力矩阵声明与实现存在缺口，见 M 级后续项）。

#### C3. sec_edgar companyfacts 结构解析错误 —— FUNDAMENTAL 恒空 ✅ 已核实 ✅ 已修复（2026-11-05）

- **位置**：[sec_edgar.py#L416-L455](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/sec_edgar.py#L416-L455)
- **证据**：L421-425 按 `facts.items() → concept_data.get("units", [])` 把 `units` 当作 **list** 迭代。真实 companyfacts 为 `facts → {taxonomy → {concept → {units: {USD: [ {end, val, fy, fp, frame} ... ]}}}}` 四层结构，`units` 是 **dict**，值键为 `val` / `end`。代码读取的 `i` / `f` / `$` 是 frames API 的形状。结果：`units` 在错误层级取不到 → 循环空转 → 返回 `[]`。另 L446-449 观测日**硬编码 `day=28`**，即使结构修好日期也会错。
- **影响**：FUNDAMENTAL 数据集静默 0 行且 checkpoint 推进。
- **修复建议**：按 companyfacts 真实 schema 重写解析（taxonomy→concept→units→unit）；观测日取 `end` 字段；未收盘/未修订语义按 `filed` 日期设 release_time。补真实响应 fixture。
- **真实形状依据（2026-11-05，SEC 官方 API 实测）**：
  - 正确 endpoint：`https://data.sec.gov/api/xbrl/companyfacts/CIK##########.json`；
  - 结构：`facts[taxonomy][concept]["units"][unit] = [observation, ...]`；`units` 是**以单位名为键的 object**（`USD`/`shares`），不是 list；
  - observation 字段：`end`（期间结束日）、`val`（数值）、`accn`、`fy`、`fp`、`form`、`filed`，`frame` 可选；`i`/`f`/`$`/`taxonomy.entry.url` 均不存在。
- **修复实现**：
  - [\_fetch_companyfacts](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/sec_edgar.py#L195-L230)：endpoint 由实测 404 的 `/submissions/CIK{cik}/companyfacts.json` 改为真实 `/api/xbrl/companyfacts/CIK{cik}.json`；因 client `base_url` 指向 `/submissions`，改用**绝对 URL** 请求（相对路径会被拼成 `/submissions/api/xbrl/...` 而 404）；
  - [\_normalize_fundamentals](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/sec_edgar.py#L414-L560)：按 taxonomy→concept→units→unit 四层遍历；`unit` 取键名；`observation_time = end` 的 T23:59:59；`value = val`；`taxon` 取外层 taxonomy；`fiscal_year = fy`（缺失时回退 `end` 年份）；`fiscal_period = fp`；`frame` 可选；`raw_record_id` 含 `taxon/concept/unit/end/accn` 保证 raw 唯一；
  - [checkpoint_from](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/sec_edgar.py#L658-L720)：按真实四层结构取最后一个观测的 `end`，cursor 为 `YYYY-MM`；
  - **零行防御**：payload 缺 `facts` 或 `facts` 非 object → `ProviderError`；`facts` 非空但全部观测被跳过 → 逐 reason 记 `connector.normalize_skip` + `connector.normalize_summary` 后抛 `ProviderError("parsed 0 rows from non-empty facts")`，交由 C1 门禁；`facts:{}` 视作合法空返回 0。
- **测试（A-1 联动）**：
  - fixture 换为真实 schema（`tests/fixtures/sec_edgar/companyfacts/happy.json`，5 条观测，taxonomy=`us-gaap`，unit=`USD`），来源与日期见 `tests/fixtures/sec_edgar/SOURCE.md`；
  - 契约：[test_contract_companyfacts_real_schema](file:///Users/horsenli/Works/ChronoForge/tests/integration/test_sec_edgar.py) 断言 5 条及 `observation_time`（2024-09-28 23:59:59）、`value`、`taxon`、`unit`、`fiscal_year/period`、`frame`、`cik`；
  - 负向：`test_negative_companyfacts_missing_fields_raises`（非空 facts + observation 缺 `end/val` → 告警 + `ProviderError`）；`test_gwt_fundamental_count_matches_fixture` 改为按真实四层结构统计观测数。
- **验证结果（2026-11-05）**：
  - 单测：`pytest tests/integration/test_deribit.py tests/integration/test_sec_edgar.py` → **83 passed**；
  - 全量：`pytest -q` → **1778 passed**（81.8s，11 条既有 DeprecationWarning）；
  - `ruff check src tests` → All checks passed；`mypy src` → Success（82 files）；`lint-imports` → **2 kept / 0 broken**；
  - **真实端到端（SEC 实测）**：`fetch(CIK 320193, data_type=fundamental) → normalize` → **25,598 条记录**，首条 `EntityCommonStockSharesOutstanding / shares / 2009-06-27 23:59:59 / 895816758.0`，checkpoint `2026-09`（证明真实形状可解析且非 0 行）。
- **对已落盘数据的影响**：`data/raw` 与 `data/canonical` 中无 `sec_edgar` 分区，无历史 FUNDAMENTAL 数据；原 endpoint 404 使 fetch 直接失败，旧解析即使拿到 payload 也恒 0 行，故**无需回放/清洗**。历史 run 若对 `sec_fund` 记过成功/零行，应按无效处理并对目标 CIK 重采。
- **剩余风险**：companyfacts 同一 `(concept, unit, end)` 可能有多条 `accn`（修订/多表），当前 raw 允许重复，canonical 自然键 `(entity_id, concept, observation_time)` 会在下游去重时按写入顺序保留其一；如需严格 PIT/修订语义，应显式按 `filed` 择最新。`filed` 字段已随 payload 可用但未落 release_time（避免扩大本次改动面），建议后续单列。large CIK（如 Apple）单次 2.5 万条，normalize 为纯 CPU，未见性能问题。

### 3.2 HIGH

| ID | 位置 | 问题 | 影响 |
|---|---|---|---|
| H1 | [binance_futures.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/binance_futures.py) `_fetch_funding`（约 L280-L325） | 只设 `startTime` 无 `endTime`，分页仅当返回空才停 | 窗口语义被破坏；backfill 时每 chunk 拉到"现在"，请求量 O(N²)。**✅ 已修复（2026-11-05）**：`endTime` 显式透传（UTC naive → ms），末条 `fundingTime >= end` 即停；新增 `test_fetch_funding_includes_end_time_and_stops`（1 次请求 / 2 条记录） |
| H2 | [ccxt_bridge.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/ccxt_bridge.py)（约 L400-L470、L720-L790） | OHLCV 无起点 rewind、无未收盘过滤（对照 [binance_spot.py#L216](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/binance_spot.py#L216) 有 `rewind_ms`） | 跨窗口边界 K 线永久漏拉；live 末根部分值永不修正。**✅ 已修复（2026-11-05）**：`since -= interval_seconds`；normalize 对 `event_time + interval > now` 的 bar 记 `connector.normalize_skip`（reason=`unclosed_bar`）并跳过；新增 rewind 与未收盘/幂等回归 |
| H3 | ccxt_bridge.py `fetchTrades`（约 L512-L655） | 单次 `fetchTrades(limit=100)` 无分页；把 ISO cursor 传给期望 `asc/desc` 的 `order` 参数 | 窗口内成交 >100 笔静默截断；参数误用。**✅ 已修复（2026-11-05）**：数字游标 → `fromId`-only、ISO 游标 → `since`（先归一化 ms），`order` 独立校验并透传；asc 取 max(id)+1、desc 取 min(id)-1 翻页；客户端按 `[start,end]` 截断并去重；非法 order/cursor/非数字 id/游标无进展均抛可诊断 `ProviderError`。新增 6 条分页用例（多页顺序、无重复、ISO 游标、desc、end 截断、异常参数） |
| H4 | [treasury_fiscal.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/treasury_fiscal.py) fetch 窗口过滤（L354-L362） | 原按 `record_date >= start` 过滤，**无 T+1 起点回退**（sosovalue/alternative/coinmetrics/coinmarketcap 均有回退 1 天） | DTS T+1 发布语义下边界日行永久漏采，违反项目硬性约束。**✅ 已修复（2026-11-05）**：`fetch` 在计算窗口后统一 `start_date -= 1 day`（下游 natural key upsert 幂等），新增回归测试 `test_fetch_rewinds_start_by_one_day_for_t_plus_1` 断言实际请求 filter 为 `record_date:gte:2026-09-30` |
| H5 | [yahoo.py#L59-L85](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/yahoo.py#L59-L85)、[yahoo.py#L112-L115](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/yahoo.py#L112-L115) | `RateLimiter(rate=http_timeout_s * 30/60)`：超时 30s 时 15 req/s = 900 req/min（本意 30 req/min）；429 未调用 `on_rate_limited`；getcrumb 硬编码 `retry_after=60`；非数字 `Retry-After` 触发 `int()` ValueError | 限流形同失效，429 风暴与 getcrumb 封禁风险。**✅ 已修复并提交（2026-11-05，`0f5583f`/`d2425e5`）**：速率固定 `_DEFAULT_RATE_PER_SECOND = 30/60 = 0.5 req/s`（不再乘超时，burst=5 不变）；新增 `_parse_retry_after` 支持秒数与 HTTP-date（缺失/非法回退 60s）；chart 与 getcrumb 429 均先 `on_rate_limited()` 清空令牌再抛 `RateLimitError`，重试沿用既有 `connectors.retry`（1→2→4→8→16s+jitter、Retry-After 下限）。**实测（2026-11-05 08:45 UTC）**：`fc.yahoo.com` 404、`getcrumb`/`chart` 连续小批量后出现 **429**，响应无 `Retry-After`/`X-RateLimit-*`，约 30s 后回落 403；无法取得 crumb（本机 IP 403），未能复现精确阈值，按公开行为（约 2000 req/h/IP ≈ 33/min）取 30/min 保守值。回归：`tests/unit/test_yahoo_ratelimit.py`（8 条；修复前 5 条失败，含实际并发 26 次/1.2s） |
| H6 | [canonical.py#L615-L709](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/storage/canonical.py#L615-L709) `recover_partition_orphans`、[canonical.py#L753-L787](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/storage/canonical.py#L753-L787) `_atomic_rename`、[canonical.py#L961-L975](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/storage/canonical.py#L961-L975) `_process_partition`、[consistency.py#L34-L81](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/storage/consistency.py#L34-L81) `cleanup_orphans` | `target→.old-*` 与 `tmp→target` 两次 rename 之间进程被强杀（无 `except` 机会）→ target 缺失、`.old-*`/`.tmp-*` 残留；`cleanup_orphans` 只删不合并；下次 upsert 视 `has_old_data=False` 直接覆盖 | 该月历史数据丢失（原子提交契约破口）。**✅ 已修复并提交（2026-11-05，`0b0f4d0`/`d21a8c3`）**：根因确认——异常路径回滚有效，但 SIGKILL/断电跳过回滚；启动清理按年龄直接 `rmtree`，写入路径把缺失分区当空分区。修复：①`.old-*`/`.tmp-*` 名内嵌目标分区名（`.old-{month}-{uuid}` / `.tmp-{month}-{uuid}`）可配对；②`_atomic_rename` 在交换前向 tmp 写 `._commit` 标记并 fsync，只有完整快照才可前滚；③新增 `recover_partition_orphans`：target 缺失时优先前滚已提交 tmp（merge 快照含旧+新）、否则回滚 `.old-*`，无完整快照才删半成品；④`cleanup_orphans` 改为"先恢复后清理"，`_process_partition` 在 `has_old_data` 判断前内联自愈（防启动清理未跑/年龄护栏跳过时覆盖历史）；⑤前滚不受年龄护栏限制，删除仍保留 R2-03 护栏。新增 6 条用例（崩溃窗口前滚、仅备份回滚、半成品不提升、无启动清理时下次 upsert 自愈、正常路径无标记残留、并发不同分区互不干扰）；修复前 4 条红（崩溃后分区缺失、下次 upsert 仅写新批次 1 行），修复后 6/6 绿 |
| H7 | [binance_spot.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/binance_spot.py)、[binance_futures.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/binance_futures.py)、`checkpoint_from`（spot/futures/ccxt/deribit/yahoo）、[canonical.py#L1260](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/storage/canonical.py#L1260)、[raw.py#L384](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/storage/raw.py#L384) | 对 **naive UTC** datetime 直接调用 `.timestamp()`：被解释为主机本地时区；`datetime.now()` 取本地时间；`_cursor_to_ms` ISO 分支同样本地化 | 在 UTC+8 生产主机上整体偏移 **-8h**：klines/aggTrades 的 `endTime` 提前 8h（尾段 8h 静默缺失且 cursor 照常推进 → 不可自愈的空洞）、未收盘判定阈值提前 8h（已收盘 bar 被误删）、checkpoint 游标回退 8h。**✅ 已修复并提交（2026-11-05，`ecdbe38`/`d27b5dd`/`0d71036`）**：新增唯一换算助手 [timeutil.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/timeutil.py)（naive 一律 `calendar.timegm`，aware 按自身时偏移），binance_spot/futures 窗口与未收盘判定、五个 `checkpoint_from`、canonical `_revision_seq`、raw 保留截止日全部改走该助手；新增 `tests/integration/test_timezone_utc.py`（25 条，按 `TZ=UTC` / `TZ=Asia/Shanghai` 参数化）。**数据影响核验（2026-11-05）**：`meta/chronoforge.db` 的 `checkpoints`/`run_log` 无任何 `binance_spot`/`binance_futures` 记录，`data/raw` 亦无对应目录（本机仅 `ccxt` 采集，其 `_dt_to_ms` 修复前已为 UTC 安全）；扫描 248 个 OHLCV entity / 92,693 行，Binance 源零缺口（72 处缺口全部为 CCXT-OKX / YAHOO 股票周末休市），**本机无历史污染，无需重采** |

### 3.3 MEDIUM

| ID | 位置 | 问题 |
|---|---|---|
| M1 | [raw.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/storage/raw.py)（约 L130-L143） | 滚动切文件时旧句柄 `close()` 前未 `fsync`；append 末尾 dirty 循环跳过 `file_obj=None` 句柄 —— 违反 docstring "append 返回 ⇒ 本批已 durable"。**✅ 已修复（2026-11-05，已提交 `60576bc`）**：滚动前先 `_sync_handle` 再 `close`；新增 `TestBatchFsync::test_rollover_fsyncs_full_handle_before_close`（保存 1 次 → 2 次），吞吐用例改为断言 200k 行 20 次 fsync（仍为有界，非逐行） |
| M2 | [consistency.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/storage/consistency.py) | startup_repair 先 `release_stale_locks` 把超时 RUNNING/PENDING 置 CANCELLED，reconcile 再查 PENDING/RUNNING 恒空 —— 按 raw 对账补记逻辑为死代码。**✅ 已修复（2026-11-05，已提交 `4c7ad07`）**：删除 `reconcile`/`_raw_batch_exists`/`_update_dataset_statuses`；`startup_repair` 收敛为 `release_stale_locks → cleanup_orphans`，去掉未使用的 raw/canonical 参数；设计文档明确"启动只做清理与恢复，不做对账补记"，真需补账另立显式命令（仅记 TODO，未实现）。回归 `test_startup_repair_does_not_invoke_reconcile` 先红（reconcile 被调用 1 次）后绿；删除 7 条仅覆盖已删函数的用例 |
| M3 | canonical.py `merge_rewrite`（约 L923-L936） | `old_table.num_rows == 0` 分支直接 `return`，却声称 inserted/rewritten=1，未实际写盘 —— 静默丢失 + run_log 计数虚报。**✅ 已修复（2026-11-05，已提交 `1ace9ac`）**：0 行分支改走 `_write_new_partition` 真正落盘，统计按批内唯一 NK 计数；新增 `TestEmptyPartitionUpsert::test_upsert_into_zero_row_partition_writes_new_rows`（修复前写入 0 行） |
| M4 | [ratelimit.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/ratelimit.py)（约 L125-L135） | `on_rate_limited(retry_after)` 忽略 `retry_after`，docstring 承诺的"429 全局冷却"未实现（原描述"多数连接器从不调用"已与现状不符：yahoo/treasury_fiscal/alternative/coinmarketcap/coinmetrics/sosovalue 等 6 处均已调用，实际缺陷是忽略 `retry_after`）。**✅ 已修复（2026-11-05，已提交 `9d465a3`）**：记录 `_cooldown_until`，`_wait_for_tokens` 冷却期阻塞并计入 `total_wait`；回归 `Test429Cooldown::test_on_rate_limited_forces_wait`（修复前 wait=0，修复后 ≈4.5s） |
| M5 | yahoo.py 约 L293 | `status_code >= 400 < 500` 链式比较恒等于 `>= 400`；5xx 被误分类为不可重试 ProviderError，L303 的 TransportError 分支为死代码。**✅ 已修复（2026-11-05，已提交 `9d465a3`）**：改为 `400 <= status_code < 500`，5xx 落到 `TransportError`；回归 `TestEdgeCases::test_5xx_raises_transport_error`（503，修复前 ProviderError） |
| M6 | deribit.py L301-L309、L660-L732、yahoo.py L219-L237、L477-L567 | start 无回退 / 无 `closeTime > now` 过滤 —— live 末根未收盘 K 线永久入库且不修正（与 binance_spot 口径不一致）。**✅ 已修复（2026-11-05）**：deribit 起点回退一个 resolution、yahoo `period1` 回退一个 interval；两者 normalize 对 `event_time + interval > now` 的 bar 记 `normalize_skip`（reason=`unclosed_bar`，debug）并跳过，未收盘计数进入 summary（debug 级，避免每轮 warning 噪声） |
| M7 | [fred.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/fred.py) | 观测日早于窗口起点的历史修订永不回补（vintage 机制对增量失效）；`units`/`seasonal_adjustment` 读响应回显而真实端点不回显 → canonical 恒 `""`，Q-NULL-001 只查 None 不查空串。**✅ 已修复（2026-11-05，已提交 `da88958`）**：增量回补窗口改为 `FRED_REVISION_WINDOW_DAYS`（默认 60 天），显式历史回补仍取真实 vintage；实测确认 FRED 将窗口前 vintage 钳制到窗口起点（UNRATE 2023 观测 → 窗口起点），normalize 对钳制行改用观察日次日且 `vintage_date=""`，自然键跨重跑稳定。units/seasonal_adjustment 改由 `/series` 元数据一次获取并缓存（真实样本 `tests/fixtures/fred/series/UNRATE.json`），不再从 observation 推断；既有 NUMBER 字段语义不变。6 条新用例先红后绿（临时回退源码复现） |
| M8 | [treasury_refunding.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/treasury_refunding.py)（约 L155-L160） | httpx 传输异常未包装为 TransportError（裸异常 → run FAILED 而非 chunk 重试）；非 200 一律 SchemaError → 5xx 触发熔断而非重试。**✅ 已修复（2026-11-05，已提交 `9d465a3`）**：两次 GET 的 `httpx.TransportError` 统一包装为 chronoforge `TransportError`；非 200 改走 `map_http_status`（429 → `RateLimitError` 并写入全局冷却、5xx → `TransportError`、其他 4xx → `ProviderError`）；回归 `test_fetch_transport_exception_wrapped`、`test_fetch_release_http_error_raises_transport_error` |
| M9 | [calendar.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/quality/calendar.py) | `US_MARKET_HOLIDAYS` 为 `(month, day)` 近似表且浮动假日写死，跨年份应用 → 真假日判 REGULAR、真实交易日判 EXPECTED_GAP（掩盖真缺失）。**✅ 已修复（2026-11-05，已提交 `9e4404b`）**：改为按年份显式假日表（2026/2027 实际 NYSE 日期，含 MLK/总统日/Good Friday/感恩节/observed 规则）；查询未配置年份抛 `ConfigError`（消息列出已知年份），不做规则推算；同时修正 2026-07-03、2027-07-05、2027-12-24 被误列为早闭日的问题。回归用例 18 条先红后绿（2026/2027 假日各 10、早闭 4、超年份 2、配置化 2）。**2026-11-06 扩展（`a5dd383`）**：假日表补齐 2021–2025（9/9/10/10/10 个全日休市日 + 2021–2025 早闭日，依据 NYSE Group 官方公告逐日期落表，含 Juneteenth 2022 起生效与各年 observed）；超年份显式失败语义不变；新增 2021–2025 假日/早闭/已配置年份用例（先红后绿） |
| M10 | sec_edgar.py 约 L336、L527 | 只读 `filings.recent`（≈1000 条上限），忽略 `filings.files` 分页 → 老 filing 永久缺失；`_extract_doc_url` 缺 CIK 路径段，primary_doc_url 恒 404。**✅ 已修复（2026-11-06，`6106ced`）**：按真实抓包（`CIK0000320193.json` 的 `filings.files` + `CIK0000320193-submissions-001.json` 扁平并行数组）实现逐页抓取与归一化；URL 改为 `data/{cik 去零}/{accession 去连字符}/{primaryDocument}`（实测 200，旧形式实测 404）；3 条用例先红后绿 |

### 3.4 LOW

- **L1**：binance_spot / binance_futures / deribit / yahoo / sec_edgar 多处畸形行 `continue` 无 warning（alternative/coinmetrics 的 skipped 计数日志可作范本，口径不一致）。
- **L2**：[rules.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/quality/rules.py) 中 Q-OHLC-001 读 `open_time`/`close_time`、Q-CROSS-001 读 `low_24h`/`high_24h`，canonical schema 无这些字段 → 两条规则永不触发（空转）。
- **L3**：ccxt_bridge `"1M"` 因 `tf.lower()` 恒不可达，月 K 线误标 `"1m"`；ccxt/yahoo/sec_edgar/fred 多处 `raise ValueError` 而非 ProviderError/ConfigError，破坏 chunk 级失败语义。
- **L4**：`replay` 全量读 raw 重放整个数据集；canonical merge-rewrite 每次重写全月分区 —— backfill 的 O(N²) 放大已随 H1 `endTime` 截断减少一层放大。
- **L5**：`_revision_dedup` 仅当自然键恰含 `revision_time` 才生效；新增 REVISION 类型漏配将静默退化为 keep-last、折叠多版本。
- **L6**：meta.py `ended_at` 用 `datetime('now')`（无 Z/T，与 `started_at` 的 ISO+Z 不一致）；迁移 0002 的 ALTER 非幂等；`MIGRATION_0001_SQL` 双份维护。

### 3.5 项目硬性约束核对（核心源码）

| 约束 | 结论 | 证据 |
|---|---|---|
| 熔断冷却（默认 1800s）+ half-open 探测 | ✅ 已落实 | [settings.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/config/settings.py) `circuit_cooldown_s=1800`；runner half-open 标记 |
| chunk 重试使用 `retry(max_retries=settings.retry_max)` | ✅ 已落实 | runner 调用 retry 装饰器 |
| startup repair 有 `min_age_seconds` / `stale_run_timeout_seconds` | ✅ 防护已落实（年龄护栏 + 租约下限）；启动不做对账补记（死代码已删） | M2（已关闭） |
| FRED 稳定 revision_time / `realtime_start=1776` / vintage 分页 | ✅ 已落实 | fred.py vintage 日切分 |
| T+1 起点回退（sosovalue/alternative/coinmetrics/coinmarketcap/treasury_fiscal） | ✅（treasury_fiscal 于 2026-11-05 修复；ccxt_bridge/deribit/yahoo 于 2026-11-05 补齐 rewind） | H2/M6（H4 已关闭，H2 已关闭） |
| ccxt 显式传 `proxies` | ✅ 已落实 | ccxt_bridge `_init_exchange` |
| 日志统一走 ui.py / structlog + RichHandler | ✅ 已落实 | logging.py、ui.py |

---

## 4. TGA 链路审计（`scripts/tga_*`、`config/tga`、`docs/tga`、`docs/strategy`）

审读范围：`tga_forecast.py`（1,519 行）、`tga_backtest.py`（820）、`tga_report.py`（706）、`tga_store.py`（692）、`tga_reconstruct.py`（1,179）、`tga_common.py`（228）、`tga_probe.py`（351）、`config/tga/*.yaml`（327）、`docs/strategy/TGA前瞻模型*.md`（4,025）、`docs/tga/*.md`（708）。

### 4.1 硬性约束核对矩阵

| # | 约束 | 结论 | 证据（file:line） |
|---|---|---|---|
| 1 | PIT：拍卖 `release_time <= cutoff`，现金流 `date <= cutoff` | ✅ 已落实 | [pit_auctions L236-L238](file:///Users/horsenli/Works/ChronoForge/scripts/tga_forecast.py#L236-L238)；`hist()` 过滤 `date <= through`（L195-L200）；公告层 L241-L259 |
| 2 | 情景不得修改已知事件（KNOWN_* 因子恒 1.0） | ✅ 已落实 | `apply_to_known` 为 true 时直接抛 D-TGA-11（L1041-L1043） |
| 3 | 回测与生产预测同源组件逻辑 | ✅ 已落实 | [tga_backtest.py L36-L45](file:///Users/horsenli/Works/ChronoForge/scripts/tga_backtest.py#L36-L45) 直接 import `compute_base_components` / `aggregate_forecast` / `apply_scenario`，无简化副本 |
| 4 | 校准只用 cutoff 前信息；`seasonal_window` 仅为可选，默认 `none` | ✅ 已落实 | [model.yaml L63-L66](file:///Users/horsenli/Works/ChronoForge/config/tga/model.yaml#L63-L66) `calibration.mode: none` |
| 5 | 历史公告时间戳来自官方 `announcement_date`，不伪造、缺口如实记录 | ✅ 已落实 | `announced_auctions` 对缺失公告日的未出结果条目**直接剔除**，不回填（L241-L259） |
| 6 | 日历层只改月内时点分布、不改月度总量；停更子类保护 | ✅ 已落实 | 月度守恒 L1008-L1013；停更保护 `STALE_SUBCATEGORY_DAYS=90`（L75、L940-L947）；TGA-1 修复后聚合层另有去重前后月度核对 |
| 7 | 定位为"情景与事件解释"；输出 `output_type=scenario` 等标签；日报为情景表+归因+切换条件 | ✅ 已落实 | [tga_forecast.py L1248-L1256](file:///Users/horsenli/Works/ChronoForge/scripts/tga_forecast.py#L1248-L1256)；[tga_report.py L588-L647](file:///Users/horsenli/Works/ChronoForge/scripts/tga_report.py#L588-L647) 十个章节；多处显式"非点位预测"（L310-L319、L534-L540） |
| 8 | DM 检验 HAC/Newey-West + Bonferroni/FDR；能力边界如实声明 | ✅ 已落实 | [tga_backtest.py L289-L430](file:///Users/horsenli/Works/ChronoForge/scripts/tga_backtest.py#L289-L430)；日报第九节 |
| 9 | 到期偿还按 CUSIP 去重；35 年回看；entity `US_TREASURY` | ✅ 已落实 | `seen_maturity`（L584-L591）、`_principal_by_cusip`（L503-L513）、`AUCTION_LOOKBACK_YEARS=35`（L81、L222）；[register_treasury_datasets.py L80/L88](file:///Users/horsenli/Works/ChronoForge/scripts/register_treasury_datasets.py#L80-L88) |
| 10 | run 按 `config_hash + cutoff` 幂等复用；回测记录终态 | ✅ 已落实 | [tga_store.py L198-L228](file:///Users/horsenli/Works/ChronoForge/scripts/tga_store.py#L198-L228) 唯一键含 origin/cutoff/model_version/config_hash/scenarios/horizons；回测状态字段 L398-L410 |
| 11 | 组件日期落在预测网格内、非工作日顺延；缺发行规模跳过不伪造 | ✅ 已落实 | 越界断言 L1159-L1164；顺延 `next_business_day`（L357、L398）；`offering is None` 跳过（L559-L560、L595-L596） |

### 4.2 发现

#### TGA-1 CRITICAL —— 组件去重键缺 subcategory，同类多子类现金流被静默坍缩 ✅ 已修复（2026-11-05）

- **位置**：[tga_forecast.py#L74-L78](file:///Users/horsenli/Works/ChronoForge/scripts/tga_forecast.py#L74-L78)、[aggregate_forecast L1163-L1300](file:///Users/horsenli/Works/ChronoForge/scripts/tga_forecast.py#L1163-L1300)
- **根因（已复核并量化）**：
  1. 原实现 `unique(subset=["target_date", "category", "event_key"], keep="first")`：基线行 `event_key=None`，同类别不同子类落在同一 `(date, category, None)` 键；事件行 label（`TAX:<id>`、`OUTLAY:first_of_month`）跨子类共享；
  2. 守恒断言原在去重**之后**比较 `comps` 与其按日汇总，恒等成立，无法发现丢弃；
  3. 实测（origin=2026-11-05、cutoff=2026-11-02、90 天网格）：原始 1,813 行组件，旧键仅保留 633 行，**丢失 1,180 行**（基线 1,527→347），组件总额偏移 **+584,402.85 USD Million（≈+584B）**——负向流出优先被丢，TGA 被系统性高估；
  4. 复核还发现第二层同质缺陷：同一子类同日存在**流入/流出两条基线**（如 `OTHER/agency_misc` 同时 `+2,246` 与 `−6,889`），仅补 `detail` 仍会被合并（1,527→1,409，丢 118 行）。因此修复必须同时把基线行移出去重。
- **修复（最小 diff）**：
  - [L55](file:///Users/horsenli/Works/ChronoForge/scripts/tga_forecast.py#L55)：`MODEL_VERSION` `0.3.0` → `0.4.0`（阻断旧逻辑 run 复用，见下）；
  - [L74-L78](file:///Users/horsenli/Works/ChronoForge/scripts/tga_forecast.py#L74-L78)：去重键改为 `["target_date", "category", "detail", "event_key"]`，并明确**仅用于事件行**；
  - [L1196-L1224](file:///Users/horsenli/Works/ChronoForge/scripts/tga_forecast.py#L1196-L1224)：拆分基线行与事件行——基线行（`event_key is null`）**不参与去重**（逐子类/逐方向独立生成）；仅事件行按新键、`confidence` 降序 `keep="first"`；
  - [L1201-L1215](file:///Users/horsenli/Works/ChronoForge/scripts/tga_forecast.py#L1201-L1215)：新增冲突防御——同一去重键下若 `predicted/lower/upper` 出现多种取值，抛 `Q-TGA-012 dedupe key collides distinct event components`，不再静默 `keep="first"`。
- **守恒校验的新位置与断言**（[L1188-L1300](file:///Users/horsenli/Works/ChronoForge/scripts/tga_forecast.py#L1188-L1300)）：
  1. **去重前**：原始组件按自然月汇总，`Σmonthly == Σcomponents`（±0.5）；
  2. **去重后**：基线行数量不得减少（实测 1,527→1,527）、基线逐月合计与去重前逐月相等；
  3. **去重后月度核对**：`原始月度合计 − 完全重复事件的重复部分 == 去重后月度合计`（逐月，±0.5）。任何无法解释为“完全重复事件”的丢失（跨子类、跨方向、整月整组）都会显式抛 `Q-TGA-012`；
  4. **身份集合核对**：去重前后 `(年, 月, category, detail)` 集合不得减少；
  5. 原有“去重后组件 == 按日聚合”的整体与逐月守恒保留。口径未变：只允许月内时点分布变化，月度总量不变。
- **幂等与历史 run**：`find_success_run` / `create_run` 的身份键已包含 `model_version`（[tga_store.py#L155-L191](file:///Users/horsenli/Works/ChronoForge/scripts/tga_store.py#L155-L191)），因此提升 `MODEL_VERSION` 即可阻断旧逻辑 run 复用，无需把逻辑版本塞进 `config_hash`（最小可行方案；`config_hash` 只覆盖 taxonomy/calendar/model/scenarios，不含代码逻辑）。影响：
  - 现存 **14 条 `tga-forecast-0.3.0` SUCCESS forecast run** 由旧逻辑生成，组件坍缩，**不可信，需以 0.4.0 重跑**。需重跑窗口（origin / cutoff）：`2026-11-02/2026-10-29`、`2026-11-03/2026-10-30`、`2026-11-04/2026-10-30`、`2026-11-04/2026-11-02`；
  - **19 条 backtest run 记录（16 SUCCESS / 3 FAILED）** 的 `model_versions_json` 均含 `tga-forecast-0.3.0`，第九节回测/DM 结论在重跑前不应引用；重跑窗口为 `2026-09-01→2026-10-29`（stride 1）、`2023-11-01→2026-10-29`（stride 5）、`2023-11-01→2026-11-02`（stride 5 及各校准变体）；
  - 旧 0.3.0 run 与旧回测分区**保留不删**（审计可追溯），但 `latest_success_run` 将取到新版本；报告按 `MODEL_VERSION` 过滤，旧回测数据自动不进入新报告；
  - 0.4.0 新 run 已在 origin=2026-11-05、cutoff=2026-11-02 实跑成功（见验证）。
- **测试（先失败后修复）**：
  - 失败复现（修复前均 `assert 1 == 2` 失败）：`test_aggregate_preserves_multi_subcategory_baseline_rows`、`test_aggregate_preserves_shared_event_label_across_subcategories`；
  - 真实数据第二层缺陷回归：`test_aggregate_preserves_inflow_outflow_baseline_rows`；
  - 冲突身份负向：`test_aggregate_rejects_conflicting_event_identity`；
  - 保留原事件去重用例 `test_aggregate_dedupes_same_event_and_conserves`。
- **验证（2026-11-05 实跑）**：
  - TGA 单测：`uv run pytest tests/unit/test_tga_forecast.py tests/unit/test_tga_common.py tests/unit/test_tga_backtest.py tests/unit/test_tga_report.py tests/unit/test_tga_reconstruct.py tests/unit/test_tga_leakage.py -q` → **56 passed**；
  - 全量：`uv run pytest -q` → **1782 passed**；
  - `uv run ruff check src tests scripts/tga_forecast.py` → **All checks passed**；
  - `uv run mypy src` → **Success: no issues found in 82 source files**；
  - `uv run lint-imports` → **2 kept, 0 broken**；
  - 端到端 `uv run python scripts/tga_forecast.py` → 成功，`run=3349f9d395b9dd56-r1`，组件 **5,439 行**（1,813×3 情景）、预测 **177 行**（59 工作日×3 情景），T+7/T+14/T+30/T+60/T+90 三情景均正常输出。
- **剩余风险**：历史 0.3.0 产物不可信（需按上述窗口重跑）；`_seasonal_flows` 的月度水平校准仍以 0.5–2.0 倍率截断，极端月份仍可能有偏差；本次只锁定“组件不丢”，不校正既有 0.3.0 数据。

#### C-2 MEDIUM —— `config/tga` 存在 7 个死键，其中 `same_month_years` 与实现直接矛盾 ✅ 已修复（2026-11-05）

- **位置（修复前）**：[model.yaml L18-L19、L44-L48、L54-L55](file:///Users/horsenli/Works/ChronoForge/config/tga/model.yaml)、[calendar.yaml L61、L78](file:///Users/horsenli/Works/ChronoForge/config/tga/calendar.yaml)
- **证据**：在 `scripts/tga_*.py` 全量 grep，以下键**无读取处**（仅出现在测试 fixture 字典中）：
  - `windows.monthly_median_weeks`、`windows.same_month_years`
  - `issuance.layer2_confidence`、`issuance.layer3_confidence`（实际置信度来自 `model["confidence"]` 映射）
  - `components.debt_events/tax/outlays/other` 四个开关
  - `calendar.refund_season_months`、`outlay_cycles.federal_pay`
- **直接矛盾**：`same_month_years` 注释写"回看年数 3"，而 [run_rate_and_month_rate L328-L332](file:///Users/horsenli/Works/ChronoForge/scripts/tga_forecast.py#L328-L332) 硬编码 `through - 365` 到 `through - 427`，只回看 **1 年**。
- **影响**：调参者修改这些键会误以为行为已变，实际 no-op；但 YAML 变更会改变 `config_hash` → 触发一次无意义的新 forecast run，造成"配置生效"的错觉与历史 run 膨胀。
- **修复建议**：要么让代码读取（`same_month_years` 改为按年循环），要么从 YAML 删除并注释说明；`components` 开关应接入 `compute_base_components` 或删除。
- **修复（2026-11-05）**：复核后 7 个键在 `scripts/tga_*.py` 中仍无读取处，采用"删除 + 注释说明"（不改变预测行为）：
  - [model.yaml](file:///Users/horsenli/Works/ChronoForge/config/tga/model.yaml)：删除 `windows.monthly_median_weeks` / `same_month_years`（注释明确同月基准固定回看去年同月，见 `run_rate_and_month_rate` 的 `through−365..−427`）、删除 `components` 四个开关（`compute_base_components` 恒计算五组件）、删除 `issuance.layer2/3_confidence`（置信度来自 `model["confidence"]` 映射，`_confidence` L292-L293）；
  - [calendar.yaml](file:///Users/horsenli/Works/ChronoForge/config/tga/calendar.yaml)：删除 `refund_season_months`（REFUND 走运行率/同月率基线）、删除 `outlay_cycles.federal_pay`（双周规则未实现）；
  - [DECISIONS.md#L17](file:///Users/horsenli/Works/ChronoForge/docs/tga/DECISIONS.md#L17) 的 D-TGA-10 依据同步改为实际读取的 `tax_events` / `monthly_events`；
  - 新增 `test_tga_config_has_no_dead_keys` 锁定上述键不再回流。
- **影响与兼容性**：删除 YAML 键会改变 `config_hash`，下一次 `tga_forecast` 将新建 run 而不会复用旧 run（幂等键含 config_hash），历史 run 保持可读；预测输出与删除前逐值一致（被删键本就不参与计算）。
- **验证**：`pytest tests/unit/test_tga_common.py` → 8 passed；全量 `uv run pytest -q` → **1786 passed**；`uv run ruff check src tests` → All checks passed；`uv run mypy src` → Success（82 files）；`uv run lint-imports` → 2 kept / 0 broken。

#### TGA-3 MEDIUM —— `docs/tga/DECISIONS.md` 状态标签严重过期

- **位置**：[DECISIONS.md L12-L25](file:///Users/horsenli/Works/ChronoForge/docs/tga/DECISIONS.md#L12-L25)
- **证据**：
  - D-TGA-05 写 `treasury_refunding` Phase 4 —— 实际 `src/chronoforge/connectors/treasury_refunding.py` 已存在并被 `_wiring` 引用（未跟踪）；
  - D-TGA-06 写"读写脚本待 Phase 2/3" —— `scripts/tga_store.py`（692 行）已实现；
  - D-TGA-10 写"Phase 3" —— `config/tga/calendar.yaml`（81 行）已存在；
  - D-TGA-18 写"taxonomy.yaml 待 Phase 2" —— `config/tga/taxonomy.yaml` 已存在（281 行）。
- **影响**：决策记录是 TGA 的权威口径来源，过期状态会让后续维护者误判"未实现"，甚至重复开发。
- **修复建议**：按当前代码逐条更新状态列，并把 DECISIONS 的版本依据从 v1.2 更新到实现方案现版本。

#### 其余正面结论（TGA）

- 预测 → 回测 → 报告复用同一 `load_forecast_context` 与 `compute_base_components`，PIT 语义一致（L1275-L1296）；
- `reports/` 与 `output/` 职责分离符合 D-TGA-17；
- 日报已彻底改为情景口径，无残留"点位预测"结论（全文均为"情景锚点 / 条件路径 / 非点位预测"）；
- 能力边界声明完整（"不优于 Naive、不构成交易建议、仅 T+7 档差距不显著"）；
- 停更子类保护、月度守恒、CUSIP 去重、35 年回看、缺失公告剔除等此前踩过的坑均已按记忆中的教训实现。

---

## 5. 测试体系审计（`tests/`）

### 5.1 CRITICAL

#### A-1. deribit fixture 形状与生产 fetch 路径不一致 —— 重演 yahoo 教训 ✅ 已修复（2026-11-05）

- **证据**：`_fetch_option_summary` / `_fetch_chart_data` 把**全量 JSON-RPC envelope** 存入 `RawBatch.payload`，但 `_normalize_options` 期望内层形状；`tests/fixtures/deribit/get_book_summary/happy.json` 与 `get_tradingview_chart_data/happy.json` 均为内层形状。`tests/integration/test_deribit.py`（约 L303-L312、L459）把 fixture 直喂 normalize，fetch 级 mock（约 L768-L837）也返回内层形状。
- **影响**：生产 fetch→normalize 链路可静默产出 0 行而测试全绿（与 C2 同源）。历史上 yahoo 正因 fixture 用假形状导致"测试全过、normalize 静默丢全部数据"。
- **修复建议**：fixture 存 **原始响应**（含 envelope），测试走 `fetch → normalize` 全链路；并对每条 canonical 断言**行数 > 0**。
- **修复实现（2026-11-05）**：
  - deribit fixtures 全部改为真实 JSON-RPC envelope：
    - `get_tradingview_chart_data/{happy,empty}.json`：`result` 为 `ticks/open/high/low/close/volume/cost/continuation` 并行数组；
    - `get_book_summary/happy.json`：`result` 为 BookSummary 列表；
    - `get_instruments/{happy,empty}.json` 原本即 `result` 数组，保持；
  - sec_edgar fixture 改为真实 companyfacts 四层 schema（`facts[taxon][concept].units[unit]` 观测列表）；
  - 两个 fixture 目录新增 `SOURCE.md`，注明形状依据、核对日期（2026-11-05）与最小裁剪说明；
  - 契约测试：`test_contract_chart_real_envelope`（deribit，2 条 + 时间/价格/标识）、`test_contract_companyfacts_real_schema`（sec，5 条 + 时间/数值/标识）；
  - 负向测试：deribit 2 条（短数组告警、缺 `result` 抛错）+ book summary 旧形状抛错；sec_edgar 1 条（非空 facts 缺 `end/val` → 告警 + 抛错）。
- **验证结果（2026-11-05）**：目标测试 83 passed；全量 1778 passed；SEC 真实端到端解析 25,598 行。其余连接器的 fixture 同源问题当时仍属 **A-3**，该批已于 2026-11-05 分批全部关闭（见 §5.2）。
- **剩余风险**：Deribit 无法直连抓包，fixture 依据官方客户端模型而非逐字节抓包；建议后续在可联网环境补一次真实响应归档（脱敏后可入库）并加入 CI 契约门禁。

### 5.2 HIGH

| ID | 问题 | 证据 / 影响 |
|---|---|---|
| A-2 | e2e / quality 两层承诺存在但为空 | 原问题：`tests/e2e/`、`tests/quality/` 仅 `.gitkeep`；`quality` / `smoke` marker 已注册但全仓无人使用；[09-testing-plan.md](file:///Users/horsenli/Works/ChronoForge/docs/design/09-testing-plan.md)、[07-testing-architecture.md](file:///Users/horsenli/Works/ChronoForge/docs/architecture/07-testing-architecture.md) 承诺的两层从未存在 → 发布门禁缺位。**✅ 已关闭（2026-11-05，已提交 `0bbada0`/`8965770`）**：新增 `tests/e2e/test_binance_spot_pipeline.py`（2 条：真实 `BinanceSpotConnector` + `httpx.MockTransport` 回放 fixture，断言 fetch 分页参数、raw 批次/行、canonical 2/3 行及价格字段与年/月分区、checkpoint、run_log 计数；增量 run 从 checkpoint 回退 overlap 后 merge-rewrite）、`tests/quality/test_quality_gate.py`（2 条：normalize 3/3 全失败 → `QualityError`、run FAILED、canonical 0、cursor 不推进；重复 natural key + `block_on=["Q-DUP-001"]` → `QualityError`、cursor 不推进、flags 0）、共享 harness `tests/real_pipeline_harness.py`；`quality` marker 首次实际使用。验证：4 passed；对"抑制 upsert / 禁用错误率门禁 / 禁用 block_on / 删除 save_checkpoint"4 处临时注入后 4 条全红，撤回后全绿。残余：e2e 为录制 fixture 回放（默认 CI 运行），真实源 smoke 仍手动 |
| A-8 | 非空 payload 被连接器逐行静默跳过 → normalize 零产出仍 SUCCESS 并推进 checkpoint（C1 残余） | **复核 A-2 时实测发现**：`NormalizeStage` 只把批量异常计入错误率；连接器对 malformed/非数值行仅 `normalize_skip` 并返回 `[]`，`attempted=0` 不触发门禁。复现：binance_spot 两行 `open="bad"` 的 kline（len≥7、closeTime 已过）→ fetch/raw 1 批次、normalize 0 行、canonical 0 行、run `SUCCESS`、`checkpoint_after=2017-07-03T01:00:00` 已写入。**✅ 已修复（2026-11-05，已提交 `9758771`）**：① 新增 [normalize_stats.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/normalize_stats.py)（ContextVar 结构化计数：`begin_normalize`/`end_normalize`/`note_skip`，不解析日志）；② 12 个连接器在全部逐行跳过分支补 `note_skip(reason)`（binance_spot/futures、yahoo、ccxt_bridge、deribit、sec_edgar、coinmetrics、coinmarketcap、alternative、fred、sosovalue、treasury_fiscal）；③ [runner.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/pipeline/runner.py#L477-L546) 按 chunk 计算 `drop_invalid/drop_attempted`，`drop_attempted = 返回行 + 上报跳过 + 未上报行`，`drop_invalid = 未上报行 + 非白名单跳过`，`>= normalize_drop_threshold(0.5)` → `QualityError`；④ 合法跳过白名单 `LEGIT_SKIP_REASONS`（`unclosed_bar`/`unfinished_kline`/`unfrozen_today`/`pending_settlement`/`source_missing_value`/`non_target_account`/`duplicate_observation`），由测试锁定；⑤ 新增配置 `CHRONOFORGE_NORMALIZE_DROP_THRESHOLD`（默认 0.5，无 DB 迁移），同步 `.env.example`/README/06-quality/08-cli-config-security。**口径边界（不与 C1 重复计数）**：批量抛异常 → `failed_units`（错误率门禁）；正常返回但逐行跳过 → kept/skipped 比率（本门禁）。**验证**：6 条新用例（全畸形 2/2、丢弃率 2/3、单条未收盘、整批合法跳过、空 payload、白名单锁定）先红后绿；全量 1872 passed（提交当次，默认 TZ 与 `TZ=Asia/Shanghai` 各一次）；ruff/mypy(84 files)/lint-imports 通过。**数据影响**：本机 `data/` 只读核对，无"非空 raw 且 canonical 0 行"的数据集；`run_log` 中 `output_count=0` 的 SUCCESS run 均未追加任何 raw 批次（确认为空窗口），无需重采 |
| A-9 | 并发 `cf query` 偶发退出码 1：注册完成后的只读重开未做锁冲突退避 | **2026-11-05 复现（`TZ=Asia/Shanghai`，4 并发 pytest 压测 20 次 worker 运行 3 次失败（15%）；隔离单跑 0/45）**，捕获子进程 stderr：`_duckdb.IOException: Could not set lock on file`，exit code 1。根因：[`_wiring.py:240`](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/cli/_wiring.py#L240-L253) 在 `register_views` 后 `close()` 写连接，紧接着无条件 `duckdb.connect(read_only=True)`；此窗口内并发进程可能刚抢到写锁，只读打开抛 `IOException` 并直接冒泡。**✅ 已修复（2026-11-05，已提交 `4d28b93`/`4840f28`）**：释放写锁后的只读重开改为复用 `_open_ready_readonly` 的退避 + 复探循环（`_WRITE_OPEN_ATTEMPTS` 上限），耗尽后才裸连以保留原错误语义；新增确定性回归用例 `test_reopen_after_register_retries_transient_lock`（注入两次只读锁冲突 → 第三次成功，修复前红、修复后绿）。**验证**：该两用例单跑循环 50/50 绿（159s）；并发压测 40/40 worker 运行绿；`TZ=Asia/Shanghai` 全量 3 次 + 默认 TZ 1 次均 1873 passed。与 TZ 无关，TZ=Asia/Shanghai 只是暴露窗口的触发条件之一。无数据侧动作 |
| A-3 | fixtures 完整性与 README 承诺严重不符 | 原问题：`tests/fixtures/README.md` 承诺 malformed / out-of-order / duplicated / late-arriving / corrected / large-volume 六种形态，实际**无任何文件**；多源缺 error fixture，异常路径回归能力接近零。**✅ 已关闭（2026-11-05）**：13 个连接器全部补齐 fetch→normalize 全链路 + 缺字段/非空零行/异常状态码负向用例 + `SOURCE.md`（来源类型与核对日期）；修正 yahoo `split.json` 假形状、fred 缺 `realtime_end`、清理 binance_futures 非法 fixture；新增真实抓包 fixture（alternative `fng_live_20261105.json`、coinmetrics `live_20261105.json`）。残余：sosovalue（当日 403）与 coinmarketcap（当日超时）fixture 未重新抓包，依据标注为历史实测形状 |

### 5.3 MEDIUM

- **A-4**：TC-C-015（SchemaError 触发熔断）无直接测试，仅有 `test_retry` 的"不重试"语义。**✅ 已修复（2026-11-05，已提交 `f12dd5c`）**：新增 `test_tcp004_schema_error_counts_toward_circuit`（3 次 SchemaError → FAILED，第 4 次 `CANCELLED` 且 `error_summary="circuit open"`，fetch 不再调用）。
- **A-5**：~~Q-GAP-001~~ 实际为 **Q-GAP-002** 的"分散缺失 → 区间合并"分支（`gap_detection`，[rules.py#L972](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/quality/rules.py#L972)）无测试，现有用例只覆盖连续缺失（Q-GAP-001 走 `expected_grid` 逐区间列出，原描述已修正）。**✅ 已修复（2026-11-05，已提交 `f12dd5c`）**：新增 `test_dispersed_gaps_split_and_adjacent_merged`（10:00/10:03/10:06 → 单 finding、`gap_ranges` 长度 2）。
- **A-6**：DST 切换日、分页上界 1000 / 下界 1 无边界测试（闰日、跨年界已有覆盖）。**✅ 已修复（2026-11-05，已提交 `f12dd5c`）**：新增 DST 切换日用例（`test_acceptance_datetime_dst_switch_days`）、`acquire(0/-1)` ValueError 用例、deribit `_MAX_CHART_PAGES == 1000` 上界用例。

### 5.4 LOW

- **A-7**：TC-Q-001~003 存在测试函数但未标注 TC-ID，无法与对照表机器校验。

### 5.5 已核实无问题（正面结论）

| 项 | 结论 |
|---|---|
| 失败恢复测试真实性 | ✅ 为真实 failure→restart→resume→validate：subprocess `os._exit(1)`、真实 SQLite/文件系统、ENOSPC 注入、fsync 边界 |
| checkpoint 不超前 | ✅ TC-P-010 / TC-P-005 在真实 store 验证；C1 异常路径与 A-8 逐行跳过路径均已拦截（非白名单丢弃率 ≥0.5 → 失败） |
| 测试独立性 | ✅ connector 测试全部 mock httpx，无真实网络依赖 |
| yahoo / fred fixture | ✅ 形状与解析路径一致，yahoo 教训已修复（`test_yahoo.py:220` 有 `len == 5` 防静默丢数断言） |
| TC-Q-005 | ✅ 17 条规则成对覆盖满足 |

---

## 6. 脚本与配置审计

### 6.1 CRITICAL

#### B-1. `scripts/fdam_attention.py` 启动即死（SyntaxError）✅ 已修复（2026-11-05）

- **证据（修复前）**：`py_compile` 实测 `SyntaxError: 'return' outside function`（line 1518）。文件 1,524 行；`if __name__ == "__main__"` 出现在 **L1240 与 L1521 两处**；L1242-L1518 为旧版内联卡片绘制残留死代码段，含 32 个 F821 未定义名（`fig` / `last_date` / `inf_idx` / `emp_idx` / `res_a` / `bt_full_summary` / `out_path` 等）。
- **死代码核实依据（删除前逐项确认）**：
  1. L1240-L1518 全部位于第一个 `if __name__ == "__main__": main()` 之后的 4 空格缩进块内，区间内**无任何顶层语句**（脚本检查 1,240–1,518 行的非注释、非缩进行仅 L1240 的 `if` 本身）；
  2. 块内引用的 `fig` / `last_date` / `inf_idx` / `emp_idx` / `res_a` / `bt_full_summary` / `out_path` 在模块作用域均无定义（ruff 32 个 F821）；块内唯一函数 `ax_of`（L1258）与字典 `S`（L1249）也仅在块内定义/使用，文件其余部分无引用；
  3. `main()` 在 L1117 调用 `build_card_payload`（L1135-L1237），L1128-L1132 把 payload JSON 交给 `card_renderer.render_card`；旧内联绘图无调用方，功能已被完整覆盖；
  4. L1518 的 `return out_path` 处于非函数作用域，即 SyntaxError 的直接根因。
- **删除范围**：L1240-L1520（重复的 `if __name__` + `main()` + L1242-L1518 死代码 + 多余空行），保留 L1521 起的正式入口；删除后文件 **1,524 → 1,243 行（净删 281 行）**。
- **验证（2026-11-05 实跑）**：
  - `python3 -m py_compile scripts/fdam_attention.py` → **exit 0**；
  - `uv run ruff check scripts` → **F821 0、F706 0、E702 1**（[fdam_attention.py#L912](file:///Users/horsenli/Works/ChronoForge/scripts/fdam_attention.py#L912) 既有分号写法，在删除范围外）；`scripts/fdam_attention.py` 自身剩 **17 项既有风格问题**（E501×7、F541×6、I001、F401、E741、E702），全部位于删除范围外，属既有非本次范围；
  - AST 结构检查：顶层 `return` 0 个、`__main__` 入口 1 个；以 matplotlib 桩执行模块导入 → **import OK，`main` 可调用**；
  - 无 fdam 专项测试；全量 `uv run pytest -q` → **1782 passed**；`uv run mypy src` → **Success: no issues found in 82 source files**；`uv run lint-imports` → **2 kept, 0 broken**。
- **功能回退风险**：无。删除段在语法上不可达且无外部引用；`main()` / `build_card_payload` / `card_renderer` 路径未改动。
- **剩余风险（独立于 B-1）**：`matplotlib` 未在 [pyproject.toml](file:///Users/horsenli/Works/ChronoForge/pyproject.toml) 声明，项目环境直接运行 `python scripts/fdam_attention.py` 仍会 `ModuleNotFoundError: No module named 'matplotlib'`；需补依赖或移入可选 extra。fdam 仍无自动化测试，后续改动无回归保护。

### 6.2 HIGH

#### G-1. 整个 TGA 子系统未纳入版本控制，且修改态文件依赖未跟踪文件 —— 已修复

- **证据（修复前）**：`git status --porcelain` = 38 个 `M` + 30 个 `??`。全部 **untracked** 的关键项包括：
  - `scripts/tga_*.py`（7 个）、`scripts/register_treasury_datasets.py`、`scripts/tga_howto.md`
  - `config/`（整个 TGA 配置目录）
  - `src/chronoforge/connectors/treasury_fiscal.py`、`treasury_refunding.py`、`src/chronoforge/models/treasury.py`、`storage/migrations/0005_forecast_metadata.py`
  - `docs/strategy/`、`docs/tga/`、TGA 全部单元/架构测试（7 个）、`tests/fixtures/treasury_fiscal/`
  - `reports/`（每日生成的报告）
- **影响（修复前）**：已修改的 `registry/service.py`（L107-L118 引用 treasury 源）与 `cli/_wiring.py` 依赖上述未跟踪文件；**只提交 M 而不提交 ?? 会直接 ImportError**，主干不可构建。同时 TGA 全部工作存在丢失风险。
- **修复（2026-11-05）**：`reports/` 先入 `.gitignore`（C-3），随后按模块分 6 组显式 `git add`（不使用 `git add -A`），以单次语义完整提交 `5590163`（`feat(tga): 接入 TGA 预测链路并修复审计发现的静默数据丢失`）纳入 **101 文件 / +27,774 / −454**，TGA 脚本、`config/tga`、treasury 连接器/模型、迁移 0005、`docs/strategy`+`docs/tga`、TGA 测试与 fixtures 与 `registry/service.py`、`cli/_wiring.py` 同批落地。
- **验证**：提交前 `uv run pytest -q` → **1782 passed**；`ruff check src tests`、`uv run mypy src`（82 files）、`uv run lint-imports`（2 kept）通过；提交后 `git status --porcelain` **无输出**；`git show --stat` 文件数与清单一致。

### 6.3 MEDIUM

- **C-2**：见 §4.2（TGA 死键，**已修复（2026-11-05）**：删除 7 个未被读取的键并加注释，新增回归测试）。
- **C-3. `reports/` 未被 .gitignore 覆盖 —— 已修复（2026-11-05）**：修复前 `output/` 已命中（[.gitignore#L56](file:///Users/horsenli/Works/ChronoForge/.gitignore#L56)），但 `reports/` 仍为 `??`，内含 `reports/tga/TGA_MODEL0_RECONSTRUCTION_2026-11-0{2,3,4}.md` 与 `reports/daily/TGA_Forecast_2026-11-0{2,3,4}.md` 6 份生成物 → 误提交风险。现已把 `reports/` 加入 [.gitignore](file:///Users/horsenli/Works/ChronoForge/.gitignore)（`git check-ignore` 验证命中）；确认 `reports/` 下均为可再生产物，无需纳管文件；`data/`、`meta/`、`var/`、`*.db` 沿用既有忽略规则。
- **C-4. `config_hash` 对 `taxonomy.unmatched` 覆盖不全**：[tga_common.py#L189-L192](file:///Users/horsenli/Works/ChronoForge/scripts/tga_common.py#L189-L192) 仅哈希 `category` + `include_in_net`；修改 `flow_type` / `subcategory` / `confidence` / `is_subtotal` 不改变 hash → 违反 `model.yaml` 头部"任何变更 → 新 run"的约定。**✅ 已修复（2026-11-06，`317a262`）**：新增 `config_hash_v2()`（返回 `v2:<digest>`，规则含 confidence、unmatched 含全字段）；旧 `config_hash` 原样保留；迁移 0006 为 `forecast_runs`/`backtest_runs` 增加 `config_hash_v2` 列，预测/回测同时记录两者，`find_success_run` 仅在 v2 一致时复用（历史 NULL 行不静默复用）。先红后绿 3 组用例。

### 6.4 LOW

- **B-2 / B-3 正面结论**：`hourly_update.sh` 与 `backfill_datasets.py` 符合 READY-006（默认 `WINDOW_SECONDS=21600`、do_round 传 `--window-seconds`、loop 模式 `--due-only`、mkdir 锁 + trap 释放）；6 个 `register_*.py` 全部幂等（`get_dataset` 预检 → `[SKIP]`）；scripts 无硬编码密钥或绝对路径。
- **C-6**：`.gitignore` 全局 `*.txt` / `*.png` / `*.db` 可能吞掉未来需要的文档类 txt。
- **C-7**：`tests/conftest.py` 无 settings fixture（文件内 DEF-002 注释自认）。

---

## 7. 文档一致性审计

### 7.1 HIGH

| ID | 位置 | 文档原文 | 代码事实 | 处理 |
|---|---|---|---|---|
| D-1 | [README.md#L80-L81](file:///Users/horsenli/Works/ChronoForge/README.md#L80-L81) 与 L84-L99 | "registry sync: 11 sources"，表格 11 行 | [service.py#L51-L119](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/registry/service.py#L51-L119) `DEFAULT_SOURCES` 为 **13** 个，缺 `treasury_fiscal`、`treasury_refunding` 两行 | ✅ 已修复：改为 13 并补两行（`treasury_fiscal` / `treasury_refunding`），新增 `tests/architecture/test_docs_consistency.py` 守卫数量与逐源出现 |
| D-2 | [README.md#L518](file:///Users/horsenli/Works/ChronoForge/README.md#L518) | "Canonical type system — 27 types across 7 asset classes" | [enums.py#L19-L54](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/models/enums.py#L19-L54) `CanonicalType` 实测 **32** 个。**复核修正报告原描述**：Prediction/Text/Entity/Instrument/Derived/Feature 原本已在分类表；实际缺的是 **Treasury (TGA) 5 个**，且表内已有 8 行而文字写 7 | ✅ 已修复：改为 "32 types (9 groups)" 并新增 Treasury 行；测试同步断言 32 与逐类型出现 |
| D-3 | [TGA前瞻模型.md](file:///Users/horsenli/Works/ChronoForge/docs/strategy/TGA前瞻模型.md)（2,150 行） | **复核修正报告原描述**：并非"全文无情景"——第九章已有三情景定义；实际缺的是**定位变更声明、非目标、使用边界、`output_type`**，且正文仍以"预测周期"表述 | 实现方案 v1.6 已改为"情景与事件解释"；代码输出 `output_type=scenario` / `usage` / `confidence` / `drivers`（[tga_forecast.py#L1370-L1375](file:///Users/horsenli/Works/ChronoForge/scripts/tga_forecast.py#L1370-L1375)） | ✅ 已修复：首部加"定位变更"声明（定位/非目标/使用边界/output_type）；第九节输出样例补四个字段并注明非点位预测 |

### 7.2 MEDIUM

- **D-4. README 环境变量表缺项** —— ✅ 已修复（2026-11-05）：以 [settings.py#L365-L396](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/config/settings.py#L365-L396) 的 `_read_env_fields` 映射为准，补齐 `CHRONOFORGE_LOG_FORMAT` / `LOG_FILE` / `CIRCUIT_COOLDOWN_S` / `NORMALIZE_ERROR_THRESHOLD` / `QUERY_MAX_ROWS` / `OHLCV_CHUNK_BARS` / `VERSION` / `RATE_OVERRIDES` 与 CCXT 凭证；并把 `CHRONOFORGE_SEC_CONTACT` 标注为必填（缺失时 `ConfigError`）。
- **D-5. 任务状态漂移** —— ✅ 已修复（2026-11-05）：`docs/implement/01-dispatch-log.md` 的 TGA-ACQ-002 / RECON / STORE / FCST-SCEN-BT-REPORT 改为 DONE（附代码证据），仅 OPS/EXPORT 保持 PENDING（`scripts/tga_daily.py`、`export_liquidity.py` 尚未落地）。历史 2026-09 审计的 HIGH 项（熔断 half-open 恢复、`--all-due` 隔离）则已确认修复（[runner.py#L747-L818](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/pipeline/runner.py#L747-L818)、[pipeline_cmd.py#L121-L178](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/cli/pipeline_cmd.py#L121-L178)）。
- **TGA-3** —— ✅ 已修复（2026-11-05）：`docs/tga/DECISIONS.md` 依据版本更新为 v1.6；D-TGA-05（treasury_refunding）、06（ForecastStore/BacktestStore）、10（calendar.yaml）、18（taxonomy.yaml）状态改为已落地；D-TGA-09 如实标注 `tga_daily.py` 待 Phase 6。

### 7.3 LOW

- **D-6**：`docs/` 下 20 处 TBD/TODO，分布于 17 个文件，集中在 `implement/tasks` 历史任务卡与 `docs/final-design-audit.md`，非核心行为阻塞项。**不处理（2026-11-05）**：均为历史任务卡/审计留痕，改写会破坏历史记录；不影响 G7 的运行一致性。
- **D-7**：文件名错别字 `docs/编码钱最终整体审计审计要求.md`（应为"编码前"，且"审计审计"重复）—— **已修复（2026-11-05）**：重命名为 `docs/编码前最终整体审计要求.md`，并更新 4 处引用。

### 7.4 已核实一致（正面结论）

| 项 | 结论 |
|---|---|
| Pipeline 七阶段 | ✅ README、[05-pipeline.md](file:///Users/horsenli/Works/ChronoForge/docs/design/05-pipeline.md#L33-L43) 与 [runner.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/pipeline/runner.py#L747-L818) 三方一致（fetch→raw_append→validate→normalize→canonical→quality→runlog） |
| 历史审计 HIGH 项 | ✅ 熔断冷却/half-open、`--all-due` 逐 dataset 隔离 + 失败汇总退出码均已修复 |
| docs/design 与 docs/architecture | ✅ 抽查未发现直接矛盾表述 |
| 未提交的 docs/design 改动 | ⚠️ 5 篇（02-data-models、03-storage、04-connectors、08-cli-config-security、10-task-manifest）+ `docs/final-design-audit.md` 处于修改态，需与 G-1 同批提交，否则文档与代码版本错位 |
| TGA 日报口径 | ✅ 十个章节与"非点位预测"表述完整一致 |

---

## 8. 汇总统计与修复批次

### 8.1 统计表

| 级别 | 数量 | 分布 |
|---|---|---|
| CRITICAL | 6（现存 0） | 核心源码 3（C1/C2/C3 已修复）、测试 1（A-1 已修复）、脚本 1（B-1 已修复）、TGA 1（TGA-1 已修复） |
| HIGH | 14（现存 0） | 核心源码 8（H1–H7 已修复并提交；A-8 已修复）、测试 2（A-2、A-3，均已修复）、工程卫生 1（G-1，已修复）、文档 3（D-1–D-3，已修复） |
| MEDIUM | 20（现存 0） | 核心源码 10（M1–M10，全部已修复）、测试 3（A-4–A-6，已修复）、配置/卫生/并发 4（C-2、C-3、C-4、A-9 全部已修复）、文档 3（D-4、D-5、TGA-3，已修复） |
| LOW | 11 | L1–L6、A-7、C-6、C-7、D-6、D-7 |
| 合计 | 51 | |

> 说明：C-7 归入 LOW，MEDIUM 实为 20 = 10 + 3 + 3 + 3 + 1（A-9）。A-8 为 2026-11-05 复核 A-2 时实测发现，计入 HIGH，同日修复关闭；A-9 为 2026-11-05 并发压测发现，计入 MEDIUM，同日修复关闭。**2026-11-05 MEDIUM 批量处理**：本轮修复 8 项（M1 `60576bc`、M3 `1ace9ac`、M4/M5/M8 `9d465a3`、A-4/A-5/A-6 `f12dd5c`），现存由 14 降至 5；随后 M2 经用户授权按"删除死代码、启动只做清理与恢复"方向修复（`4c7ad07`），M9 按"配置化假日表 + 超年份显式失败"修复（`9e4404b`），M7 按"60 天可配置回补窗口 + `/series` 元数据"修复（`da88958`），M10 按真实样本实现 `filings.files` 分页与 primary_doc_url（`6106ced`），C-4 以版本前缀 v2 哈希覆盖全字段（`317a262`）关闭，**MEDIUM 现存 0**。

### 8.2 优先级与修复批次

#### P0（阻塞，必须先做；建议单个 PR 内按序完成）

| 顺序 | 项 | 动作 | 验证 |
|---|---|---|---|
| 1 | ~~**C1**~~ | ~~normalize 非空批次产出 0 记录 → 抛错并阻止 cursor 推进~~ **已完成（2026-11-05）** | 2 条回归测试 + 全量 pytest 1772 passed + ruff/mypy 通过 |
| 2 | ~~**TGA-1**~~ | ~~去重键加入 subcategory（或仅对 event_key 非空去重）~~ **已完成（2026-11-05）** | 4 条新增用例（2 条失败复现 + 流入/流出 + 冲突身份）+ TGA 56 passed + 全量 1782 passed + 端到端 5,439 组件行 |
| 3 | ~~**C2 + A-1**~~ | ~~deribit 统一 envelope 解包；fixture 换真实响应；补 fetch→normalize 全链路断言~~ **已完成（2026-11-05）** | 契约 1 条 + 负向 3 条；deribit 集成测试 2 行 > 0 |
| 4 | ~~**C3**~~ | ~~sec_edgar 按 companyfacts 真实 schema 重写；观测日取 `end`~~ **已完成（2026-11-05）** | 契约 1 条 + 负向 1 条；SEC 真实端到端 25,598 行 |
| 5 | ~~**B-1**~~ | ~~删除 fdam 死代码段~~ **已完成（2026-11-05）** | `py_compile` 通过；`ruff check scripts` F821/F706 = 0、E702 = 1（既有）；全量 1782 passed |
| 6 | ~~**G-1 + C-3**~~ | ~~`reports/` 入 .gitignore；38 个 M 与 30 个 ?? 成套提交~~ **已完成（2026-11-05）** | 提交 `5590163`（101 文件 / +27,774 / −454）；提交前全量 1782 passed、ruff/mypy/lint-imports 通过；提交后 `git status --porcelain` 干净 |

#### P1（正确性缺口，紧随其后）

| 项 | 动作 |
|---|---|
| ~~**H4**~~ | ~~treasury_fiscal 加 T+1 起点回退（与 sosovalue 一致）~~ **已完成（2026-11-05）** |
| ~~**H2 + M6**~~ | ~~ccxt / deribit / yahoo 统一 rewind + 未收盘过滤~~ **已完成（2026-11-05）**：ccxt OHLCV `since -= interval_seconds`；deribit start 回退一个 resolution；yahoo `period1` 回退一个 interval；三者 normalize 均跳过 `close_time > now` 的 bar 并记 `normalize_skip`/summary（deribit/yahoo）。回归：`TestOHLCVRewind`、`TestChartRewindAndUnclosed`、`TestRewindAndUnclosed` |
| ~~**H5**~~ | ~~yahoo 限流公式改为 `_DEFAULT_RATE/60`（不乘超时）~~ **已完成并提交（2026-11-05，`0f5583f`/`d2425e5`）**：速率固定 0.5 req/s；429 解析 `Retry-After`（秒/HTTP-date，缺省 60s）并调用 `on_rate_limited`；重试沿用 `connectors.retry`。回归：`tests/unit/test_yahoo_ratelimit.py`（8 条） |
| ~~**H6**~~ | ~~canonical 换名用"先写新分区再交换"或保留旧目录可回滚~~ **已完成并提交（2026-11-05，`0b0f4d0`/`d21a8c3`）**：`.old-*`/`.tmp-*` 名内嵌目标分区名可配对；`_atomic_rename` 交换前写 `._commit` 标记并 fsync；`recover_partition_orphans` 在启动清理与每次 `_process_partition` 前前滚已提交 tmp / 回滚 `.old-*`，无完整快照才删半成品；`cleanup_orphans` 从"只删"改为"先恢复后清理"。回归：`TestRenameSwapCrashRecovery`（6 条，修复前 4 条红）。M1/M3 已于 2026-11-05 另行修复（`60576bc`/`1ace9ac`） |
| ~~**H1 / H3**~~ | ~~funding 补 `endTime`；trades 补分页并修正 `order` 参数~~ **已完成（2026-11-05）**：funding `endTime` + 边界停止；trades fromId-only/ISO→since 归一化、`order` 校验透传、asc/desc 双向翻页、客户端 end 截断与去重。回归：`TestFetchTradesPagination`（6 条）+ `test_fetch_funding_includes_end_time_and_stops` |
| ~~**H7**~~ | ~~统一 UTC 时间助手，消除 naive `.timestamp()` 的主机时区偏移~~ **已完成并提交（2026-11-05，`ecdbe38`/`d27b5dd`/`0d71036`）**：新增 `timeutil.py` 单一换算入口；binance_spot/futures 窗口上下界与 `close_time > now`、spot/futures/ccxt/deribit/yahoo `checkpoint_from`、`_cursor_to_ms`、canonical `_revision_seq`、raw 保留截止日全部改走 UTC 助手。回归：`tests/integration/test_timezone_utc.py`（25 条，TZ 参数化；修复前 UTC+8 下 12 条失败）。数据核验：本机无 binance_spot/futures 采集记录，Binance 源 OHLCV 零缺口，无需重采 |
| ~~**M1 / M3**~~ | ~~raw 滚动前 fsync；0 行分支不虚报计数~~ **已完成（2026-11-05，已提交 `60576bc`/`1ace9ac`）**：滚动前 `_sync_handle` 再 close；0 行分区改为真正落盘并按批内唯一 NK 计数 |
| ~~**A-2**~~ | ~~补 e2e/quality 两层最小骨架~~ **已完成（2026-11-05，已提交 `0bbada0`/`8965770`）**：e2e 2 条（真实 connector + MockTransport 全链路、增量 checkpoint）+ quality 2 条（错误率门禁、block_on 门禁）+ 共享 harness；4 条用例在 4 处临时注入下全红、撤回后全绿。A-3 fixtures 六形态与 13 连接器契约覆盖已于 2026-11-05 完成 |
| ~~**A-8**~~ | ~~连接器逐行静默跳过导致"非空 payload → 0 行产出"仍 SUCCESS 并推进 checkpoint~~ **已完成（2026-11-05，已提交 `9758771`）**：`note_skip` 结构化计数 + `normalize_drop_threshold=0.5` 行级丢弃率门禁；6 条新用例先红后绿；全量 1872 passed 双时区（当次验证） |
| ~~**A-9**~~ | ~~并发 `cf query` 偶发退出码 1（注册后只读重开撞锁）~~ **已完成（2026-11-05，已提交 `4d28b93`/`4840f28`）**：只读重开复用退避 + 复探；确定性回归用例先红后绿；单跑 50/50、并发压测 40/40、双时区全量 4 次全绿 |

#### P2（一致性与可维护性）

| 项 | 动作 |
|---|---|
| ~~M4 / M5 / M8~~ | **已完成（2026-11-05，已提交 `9d465a3`）**：429 全局冷却按 `retry_after` 生效；yahoo 5xx 归 `TransportError`；treasury_refunding 传输异常包装 + 非 200 按 `map_http_status` 分类 |
| ~~M2~~ / ~~M9~~ / ~~M7~~ / ~~M10~~ / ~~C-4~~ | **M2（`4c7ad07`）**：删除启动路径恒空的对账死代码；**M9（`9e4404b`）**：按年份显式假日表 + 超年份显式失败；**M7（`da88958`）**：修订回补 60 天窗口 + `/series` 元数据；**M10（`6106ced`）**：按真实样本实现 `filings.files` 分页与 primary_doc_url；**C-4（`317a262`）**：v2 配置哈希覆盖 taxonomy 全字段，旧 hash 兼容，历史 run 不再静默复用 |
| ~~A-4 / A-5 / A-6~~ / A-7 | **A-4/A-5/A-6 已完成（2026-11-05，已提交 `f12dd5c`）**：TC-C-015 熔断、Q-GAP-002 分散区间合并、DST/分页上下界边界；A-7 仍待标注 TC-ID |
| **新发现（2026-11-05，已修复）** | deribit chart fetch 未读取 `result.continuation`：当 `end` 缺失且接口持续返回非空 ticks 时存在无限分页/重复采集风险。已改为 continuation 驱动（null 终止）+ `end` 边界双重收敛；重复 token 或页数 > 1000 抛 `ProviderError`（[deribit.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/deribit.py#L301-L370)），并补 5 条分页契约测试 |
| ~~C-2~~ / ~~C-4~~ | **C-2 已完成（2026-11-05）**：删除 7 个死键 + 注释 + 回归测试；**C-4 已完成（`317a262`）**：v2 配置哈希覆盖 taxonomy 全字段，旧 hash 保留 |
| ~~D-1 ~ D-5、TGA-3~~ | **已完成（2026-11-05）**：README 源数/类型数/环境变量表、TGA 概念文档定位声明、DECISIONS 状态、dispatch-log 状态均已对齐代码；新增 `test_docs_consistency.py` 守卫 |
| L1 ~ L6、C-6、D-6、D-7 | 日志口径统一、空转规则、错误类型、性能与迁移幂等、文件名修订（D-7 已完成；D-6 为历史留痕不处理） |

### 8.3 建议的修复-验证闭环

每批修复后统一执行：

```bash
uv run ruff check src scripts tests
uv run mypy src
uv run lint-imports
uv run pytest -q
```

对 C1/C2/C3/TGA-1 另需增加**数据回溯核对**：修复后按 `market_id` / `dataset_id` / `forecast_run` 统计 canonical 行数与数据库观测日数，确认历史缺口被重新采集且不产生重复。

---

## 9. 结论与后续动作

### 9.1 结论

ChronoForge 的基础工程质量（分层、类型、测试独立性、幂等回退、TGA 的 PIT 与情景口径改造）达到较高水准，本轮审计未发现架构性返工需求。**6 项 CRITICAL 已全部修复，现存 CRITICAL 为 0**；G-1/C-3（版本库/生成物忽略）已于 2026-11-05 以提交 `5590163` 关闭（G6 ✅）；文档一致性 D-1/D-2/D-3/D-4/D-5/TGA-3 已修复、D-7 重命名（G7 ✅）；H4/C-2 修复并复核（G5 ✅）。**G3 于 2026-11-05 关闭**：13 个连接器全部具备"可溯源响应样本 → fetch → normalize → 行数与关键字段断言"全链路用例，并配有缺字段、非空零行、异常状态码等负向用例与 `SOURCE.md` 取证说明；全量 `pytest` 1972 passed，`ruff` / `mypy` / `lint-imports` 均通过；**A-2 已关闭**（e2e/quality 两层落地，4 条用例在临时注入下全红、撤回后全绿）；**A-8 已关闭**（`note_skip` 结构化计数 + 行级丢弃率门禁，6 条用例先红后绿）；**A-9 已关闭**（并发 query 只读重开撞锁，确定性用例先红后绿，单跑 50/50、并发压测 40/40 绿）。据此，§1.6 的 **G1–G7 全部为 ✅，按门禁标准判定 READY（2026-11-05 复评）**。

**READY（G1–G7 全部通过）**。残余风险综合等级 **MEDIUM**，须在下一迭代清除：(a) **H1（funding `endTime`）、H2（ccxt/deribit/yahoo rewind 与未收盘过滤）、H3（ccxt trades 分页与 `order` 参数）已于 2026-11-05 修复**——用户任务中的"H1（ccxt trades）"对应本报告 **H3**，此处已按实际编号修正；(b) **H6 已于 2026-11-05 阶段 B 修复并提交（`0b0f4d0`/`d21a8c3`）**——两次 rename 之间的强杀窗口改为"提交标记 + 前滚/回滚自愈"，新增 6 条崩溃恢复用例（修复前 4 条红）；A-2 已于 2026-11-05 关闭；**A-8 已于 2026-11-05 修复（已提交 `9758771`）**——连接器逐行跳过改为结构化计数 + 行级丢弃率门禁，G4 因此通过；**A-9 已于 2026-11-05 修复（已提交 `4d28b93`/`4840f28`）**——并发 query 注册后只读重开复用锁退避，消除偶发退出码 1；(c) **A-3 取证分层**——真实抓包为 treasury_fiscal、treasury_refunding、alternative、coinmetrics（其中 alternative/coinmetrics 当日 HTTP 200 并归档 live fixture），官方文档/客户端模型为 binance_spot、binance_futures、yahoo、fred、ccxt_bridge、deribit、sec_edgar，**sosovalue（当日 403 Cloudflare）与 coinmarketcap（未文档化网页端点，当日超时）为依据历史实测形状、未当日重新抓包的 BLOCKED 项**（已在各自 `SOURCE.md` 如实记录尝试方式与状态码）；(d) **deribit chart 分页缺陷已修复（2026-11-05）**：改为 `result.continuation` 驱动并以 `end` 边界双重收敛，重复 token / 页数 > 1000 抛 `ProviderError`；`raw_meta` 语义不变，`checkpoint_from` 仍取末页末根 tick。历史上 yahoo fixture 假形状造成过同类事故，说明测试契约层（真实响应 → fetch → normalize）是制度性缺口，本次 13/13 覆盖即为该缺口的制度化闭环，后续新增连接器必须沿用同一门槛。**(e) MEDIUM 现存 0 项**：C-4 已按"版本前缀 v2 哈希覆盖全字段、旧 hash 保留"关闭（`317a262`）；M2 已按"删除死代码、启动只做清理与恢复"关闭（`4c7ad07`），M9 已按"配置化假日表 + 超年份显式失败"关闭（`9e4404b`），M7 已按"60 天可配置回补窗口 + `/series` 元数据"关闭（`da88958`），M10 已按真实抓包关闭（`6106ced`）；本轮已完成 M1/M3/M4/M5/M8 与 A-4/A-5/A-6（见 §3.3/§5.3 与 §8.2 P1/P2）。

TGA 方面值得肯定：11 条硬性约束均已有落实证据，日报已彻底转为情景口径且能力边界如实声明；TGA-1 修复同时揭示并修正了"同子类流入/流出基线被合并"的第二层缺陷。

**0.4.0 重跑结果（2026-11-05）**：当日预测 `3349f9d395b9dd56-r2`（**5,439 组件行** / 177 预测行，base T+7/T+14/T+30/T+60/T+90 = 846.80 / 667.06 / 828.77 / 1,000.80 / 633.24 B）；短回测 `de0ff140cf2560b7`（2026-09-01→10-29，stride 1，15.8s，SUCCESS）；长回测 `fe7a9c621dc3dc4b`（2023-11-01→2026-10-29，stride 5，51.7s，SUCCESS）。同日同 cutoff（origin 2026-11-04 / cutoff 2026-11-02）新旧对比：T+7 −48.17B（−5.3%）、T+14 −108.37B（−13.6%）、T+30 −240.83B（−22.9%）、T+60 −457.58B（−49.2%）、T+90 −710.46B（−56.7%），方向与 +584,402.85 USD Million 高估修正一致。0.4.0 回测显示全档显著劣于 Naive（T+7 p_Bonf=0.0138，T+14/30/60/90 p_Bonf=0.0000），日报 §九 已恢复 0.4.0 证据并声明"不得宣称有效"；0.3.0 历史 run 仍保留在库，报告按 `MODEL_VERSION=0.4.0` 自动排除。

**v2 配置哈希重跑结果（2026-11-06，C-4 关闭后）**：在 M9 提交（`a5dd383`）与审计同步（`7cf7a3c`）之后，按 `config_hash_v2` 重跑 0.4.0 的当日预测与短/长两档回测：

- 当日预测 `0148b1d017abf160`（origin 2026-11-06 / cutoff 2026-11-02；**5,451 组件行**（1,817×3）/ 177 预测行（59 工作日×3）；base T+7/T+14/T+30/T+60/T+90 = 846.80 / 667.06 / 839.26 / 996.49 / 730.97 B；宽松 811.44 / 589.90 / 675.14 / 646.91 / 209.78 B，收紧 882.15 / 744.22 / 1,003.37 / 1,346.08 / 1,252.16 B。T+30/T+60/T+90 随 origin 前移 1 个工作日移动，T+7/T+14 与 11-05 同值。
- 短回测 `537156e73d3b1530`（2026-09-01→2026-10-29，stride 1，41 origins，485 评估行）；长回测 `ad190f7cc4217f79`（2023-11-01→2026-10-29，stride 5，150 origins，3,635 评估行）。两窗口 MAE/DM 与 0.4.0 原值逐项一致：长窗口 T+7 MAE 81.514 / DM 3.39 / p_Bonf=0.0138，T+14/30/60/90 的 MAE 138.035 / 260.299 / 383.381 / 498.973 与旧 run 相同；短窗口各模型各周期 MAE 与旧 `de0ff140cf2560b7` 逐值相同。
- 配置与幂等：`config_hash` 由 `bc0d79afd758c189` 变为 `193daeb9c5869c03`（`config_hash_v2=v2:708d391e20e4bb81`，已写入 `forecast_runs` / `backtest_runs`）。旧 run 的 `config_hash_v2` 为 NULL，`find_success_run` 不参与复用；同日复跑返回 `已存在同配置 SUCCESS run：0148b1d017abf160`，验证 v2 幂等生效。哈希变化的唯一来源是 `b8e8505` 删除 7 个死键（C-2），行为不变，故数值逐项一致。


### 9.2 后续动作清单

1. P0 的 6 项 CRITICAL 已全部关闭；G-1 成套提交与 `reports/` 忽略已完成（提交 `5590163`，G6 ✅）；文档一致性 D 类已关闭、G7 ✅；H4/C-2 已修复、G5 ✅；**G3 已于 2026-11-05 关闭（A-3 完成）**；H5 已提交，H6 已于阶段 B 修复并提交（`0b0f4d0`/`d21a8c3`）；A-2 与 **A-8** 已于 2026-11-05 关闭（e2e/quality 两层落地、行级丢弃率门禁落地）；**M1/M3 已于 2026-11-05 修复（`60576bc`/`1ace9ac`），M4/M5/M8 与 A-4/A-5/A-6 同批关闭（`9d465a3`/`f12dd5c`），M2 已删除启动路径死对账（`4c7ad07`），M9 已换配置化假日表并超年份显式失败（`9e4404b`），M10 已按真实样本补齐 `filings.files` 分页并修正 primary_doc_url（`6106ced`），C-4 已新增 v2 配置哈希并覆盖 taxonomy 全字段（`317a262`），**MEDIUM 现存 0 项**；
2. 连接器契约门禁落地：每个连接器必须有一条 `raw 响应样本 → fetch → normalize → 行数断言` 的用例，纳入 CI（**13 个连接器已于 2026-11-05 全覆盖**；新增连接器须沿用）；
3. 对既有数据做一次完整性回溯（treasury_fiscal、TGA forecast 产物，以及历史 run 中曾记成功的 deribit/sec_edgar checkpoint），量化历史缺口；**TGA 0.4.0 已重跑当日预测与 2 个回测窗口（`de0ff140cf2560b7`、`fe7a9c621dc3dc4b`），并在 C-4 后以 `config_hash_v2` 重跑当前 origin 与同一短/长两档（`0148b1d017abf160`、`537156e73d3b1530`、`ad190f7cc4217f79`）；审计原列的其余 forecast 窗口（2026-11-02/10-29、11-03/10-30、11-04/10-30）与 backtest 窗口（2023-11-01→11-02）尚未重跑**；
4. 重新运行本报告 §2 的基线命令与 §4.1 约束矩阵，逐项勾销后复评 READY/NOT READY；
5. ~~文档侧统一"单一口径来源"~~ —— **已完成（2026-11-05）**：TGA 以实现方案为准，概念文档首部加定位声明并以链接指向实现方案，避免双源漂移；
6. **A-3 残余两项（BLOCKED）**：在可联网、无 Cloudflare 拦截的环境补抓 sosovalue（历史回看 ≥ today-30d 返回 403；summary 单响应 ≤20 行，与文档 `limit=300` 不符）与 coinmarketcap（未文档化网页端点）真实响应并归档，替换现有历史实测形状 fixture；deribit continuation 分页缺陷已于 2026-11-05 修复。

### 9.3 复评建议

完成 P0 + P1 后复评。复评通过条件：§1.6 的 G1–G7 全部为 ✅，且 P0/P1 条目全部关闭并有对应回归测试。

---

## 附录 A：关键文件索引

| 主题 | 文件 |
|---|---|
| 审计要求 | [编码前最终整体审计要求.md](file:///Users/horsenli/Works/ChronoForge/docs/编码前最终整体审计要求.md) |
| 管道入口 | [runner.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/pipeline/runner.py)、[cursor.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/pipeline/cursor.py) |
| 存储 | [canonical.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/storage/canonical.py)、[raw.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/storage/raw.py)、[consistency.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/storage/consistency.py) |
| 连接器 | [deribit.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/deribit.py)、[sec_edgar.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/sec_edgar.py)、[ccxt_bridge.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/ccxt_bridge.py)、[treasury_fiscal.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/treasury_fiscal.py)、[yahoo.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/yahoo.py) |
| TGA | [tga_forecast.py](file:///Users/horsenli/Works/ChronoForge/scripts/tga_forecast.py)、[tga_backtest.py](file:///Users/horsenli/Works/ChronoForge/scripts/tga_backtest.py)、[tga_report.py](file:///Users/horsenli/Works/ChronoForge/scripts/tga_report.py)、[model.yaml](file:///Users/horsenli/Works/ChronoForge/config/tga/model.yaml)、[DECISIONS.md](file:///Users/horsenli/Works/ChronoForge/docs/tga/DECISIONS.md) |
| 文档 | [README.md](file:///Users/horsenli/Works/ChronoForge/README.md)、[05-pipeline.md](file:///Users/horsenli/Works/ChronoForge/docs/design/05-pipeline.md)、[09-testing-plan.md](file:///Users/horsenli/Works/ChronoForge/docs/design/09-testing-plan.md) |
| 脚本 | [fdam_attention.py](file:///Users/horsenli/Works/ChronoForge/scripts/fdam_attention.py)、[backfill_datasets.py](file:///Users/horsenli/Works/ChronoForge/scripts/backfill_datasets.py) |

## 附录 B：复现命令

```bash
# 静态质量
uv run ruff check src scripts tests --statistics
uv run mypy src
uv run lint-imports

# 测试
uv run pytest -q

# 关键核实
uv run python -c "import py_compile; py_compile.compile('scripts/fdam_attention.py', doraise=True)"
uv run python -c "import ast; t=ast.parse(open('src/chronoforge/models/enums.py').read()); print(len([n for c in t.body if isinstance(c, ast.ClassDef) and c.name=='CanonicalType' for n in c.body if isinstance(n, ast.Assign)]))"
git status --porcelain
```
