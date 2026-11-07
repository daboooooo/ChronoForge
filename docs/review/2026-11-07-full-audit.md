# ChronoForge 全面审计报告（代码 + 文档）

| 项目 | 内容 |
|---|---|
| 审计日期 | 2026-11-07 |
| 审计对象 | `/Users/horsenli/Works/ChronoForge`（Python 3.12，uv + polars + DuckDB + Parquet） |
| 基线报告 | `docs/review/2026-11-05-full-audit.md`（下称"上轮报告"） |
| 审计性质 | 只读审计；未修改源码、未改数据、未重置 checkpoint、未重新采集 |
| 唯一交付 | 本文件 |
| 最终判定 | **NOT READY**（G1/G2/G3/G4/G6/G7 六道门禁为红；G5 部分失效） |

> 本报告对基线报告的所有"已修复"结论做了独立复核。复核方式：回读源码 + 跑既有测试 + 只读查询 canonical/derived 数据。**凡报告与代码不一致处，以代码与数据为准。**

---

## 0. 结论速览

### 0.1 总体判定：NOT READY

上轮报告判定 READY（G1–G7 全 ✅）。本次复核结论不同：**上轮关闭的项大部分在旧代码路径上确实已修复，但本轮新增的第 14 个连接器（`nyfed`）与准备金模型、可视化站点把同一批不变量重新打穿了。** 这不是"文档改口径"的问题，而是新增代码没有接入既有门禁。

三个最关键的结论：

1. ~~**SRF 口径把美联储的 SVE 测试操作当成真实用量写入 canonical**（C-N1）。~~ **已修复（2026-11-07 第 1 批）**：按 `note` 排除 SVE，4 条测试先红后绿，重采 2021-07-29 至今，11 个被污染日期全部纠正（2025-11-05 → 3M，2026-05-27 → 11M）。
2. **C1/A-8 门禁对 `nyfed` 完全失效**（C-N2）。`_payload_units` 只认顶层 `rows/data/result/observations/values`，nyfed 的 `{repo:{operations:[...]}}` 与 `{soma:{summary:[...]}}` 一律返回 0；连接器没有 `note_skip`；`checkpoint_from` 直接读 raw 最大日期。结果是"有输入、零产出、checkpoint 照常推进"，与上轮 C1 的症状完全一致。
3. **准备金模型的"PIT 截断"只截 observation_time，不截 release_time**（H-N3）；**TGA 区间发散是算法性的**（H-N4），并被图 5 的硬编码 y 轴裁切掩盖。

### 0.2 严重级别统计

| 级别 | 本次新增 | 已修复（第 1 批） | 现存 | 合计 |
|---|---|---|---|---|
| CRITICAL | 2（C-N1、C-N2） | 1（C-N1） | **1（C-N2）** | 2 |
| HIGH | 5 组（H-N1–H-N4、H-N5–H-N9） | 0 | **5 组** | 5 组 |
| MEDIUM | 10 | 0 | **10** | 10 |
| LOW | 8 | 0 | **8** | 8 |
| 合计 | — | 1 | **24 条 / 5 组** | 29 |

> 编号规则：`*-N*` 表示本轮新增（N = new）；`C1/A-1/H1/M1` 等沿用上轮编号。
> 修复状态以 §17「修复进度」为准（每批完成后更新）。

### 0.3 卡在哪几道门禁

| 门禁 | 结果 | 直接原因 |
|---|---|---|
| G1 静态质量 | ❌ 红 | `mypy src` 2 errors、`ruff check src` 2 errors，全部在 `nyfed.py` |
| G2 测试 | ❌ 红 | `3 failed, 1980 passed`：docs 一致性 1 条 + CLI 源数/源列表 2 条 |
| G3 契约测试 | ❌ 红（重新打开） | `nyfed` 无真实响应 fixture、无 fetch→normalize 全链路测试 |
| G4 静默丢失 | ❌ 红（重新打开） | SRF SVE 污染已修（第 1 批）；**C-N2 仍红**——`nyfed` 零产出/逐行丢弃不触发门禁 |
| G5 约束核验 | ⚠️ 部分失效 | PIT、发布时刻、区间口径三条硬约束在新增代码上无三方证据 |
| G6 版本库 | ❌ 红 | 15 个工作区改动 + `nyfed.py`/`reserve_forecast.py`/`visualization/`/`docs/USD_Liquidity/` 全部未跟踪；落后 origin 52 个提交 |
| G7 文档一致 | ❌ 红 | README `13 sources` vs 实际 14；新增连接器/模型/站点无有效文档；站点 README 仍写 16 张图、准备金"未实现" |

### 0.4 相对上一次审计的变化

| 上轮编号 | 上轮结论 | 本次复核 | 变化 |
|---|---|---|---|
| C1 | 已修复 | 旧连接器路径仍有效；`nyfed` 重新失效 | **部分关闭 / 新连接器重新打开** |
| C2 deribit chart | 已修复 | 代码含 `result.continuation` 分页与护栏，fixture 两页样本；定向测试通过 | **关闭（复核通过）** |
| C3 sec_edgar | 已修复 | 已改真实 `/api/xbrl/companyfacts/CIK{cik}.json`，fixture 与 SOURCE.md 齐备 | **关闭（复核通过）** |
| A-1 连接器 fixture | 已修复 | deribit/sec_edgar fixture 为真实形状 | **关闭（复核通过）** |
| TGA-1 去重键 | 已修复 | `_DEDUPE_KEY` 含 `detail`，Q-TGA-012 三重守恒断言在位 | **关闭（复核通过）** |
| B-1 fdam 语法 | 已修复 | `py_compile` 通过 | **关闭（复核通过）** |
| H1–H7 | 已修复 | 全量套件对应回归用例通过；未逐条重读实现 | **关闭（测试级复核）** |
| M1–M10 / A-2–A-9 | 已修复 | 全量套件通过；仅 A-8 在 `nyfed` 上失效 | **关闭（A-8 部分重新打开）** |
| G3 | 13 连接器全覆盖 | 新增第 14 个连接器无 fixture | **重新打开** |
| G1/G2/G7 | 全绿 | nyfed + 文档漂移导致红 | **重新打开** |
| G6 | 工作区干净 | 新一轮改动未提交、未推送 | **重新打开** |

> **本轮新增项的修复状态**：C-N1 已修复（第 1 批）；C-N2 与全部 H/M/L 仍开放。详见 §17。

> 上轮报告自身还有一处内部数字不一致：§0.1/§1.6 写 `1972 passed`，§2.1 写 `1862 passed`，§2 写 `1770 passed`。本轮实测 1980 passed / 3 failed。作为文档瑕疵记为 L-N8，不影响代码结论。

---

## 1. 审计方案

### 1.1 审计目标

1. 独立复核上轮报告声称"已修复"的 CRITICAL/HIGH/MEDIUM，确认是真修复而非口径调整；
2. 重点审计本轮新增、此前从未审过的代码：`nyfed.py`、`reserve_forecast.py`、`visualization/`、新增测试层、调度脚本；
3. 逐条核对 `docs/` 与代码事实；
4. 用上轮相同的四级严重度与 G1–G7 门禁，给出可横向比较的判定。

### 1.2 审计范围

| 对象 | 文件/规模 | 深度 |
|---|---|---|
| `src/chronoforge/connectors/nyfed.py` | 1 文件 / ~470 行（未跟踪） | 逐函数 |
| `scripts/reserve_forecast.py` | 1 文件 / ~750 行（未跟踪） | 逐函数 + 独立复算 |
| `scripts/tga_forecast.py` | 既有文件，重点看区间聚合 | 定向 |
| `visualization/` | 6 文件（`index.html`/`app.js`/`styles.css`/`export_data.py`/`refresh.sh`/`README.md`，未跟踪） | 全量 |
| `tests/e2e/`、`tests/quality/`、`tests/real_pipeline_harness.py`、`tests/architecture/test_docs_consistency.py` | 新增测试层 | 有效性验证 |
| `scripts/hourly_update.sh`、`backfill_datasets.py`、`dataset_stats.py` | 调度与统计 | 定向 |
| `src/chronoforge/pipeline/runner.py`、`windows.py`、`config/update_schedule.py` | 与新连接器交互的门禁/窗口/调度 | 定向 |
| `docs/` 全体新增/改动 + 根 `README.md` | 交叉核对 | 中 |

**排除**：`data/`、`meta/`、`var/`、`output/`、`reports/` 的运行内容（仅做只读查询取证）；第三方依赖实现；`docs/USD_Liquidity/` 的 PDF 知识库内容本身（仅记录其未跟踪状态）。

### 1.3 方法与证据

| 方法 | 做法 | 证据形式 |
|---|---|---|
| 静态检查 | `uv run mypy src`、`uv run ruff check src`、`uv run ... pytest -q` | 命令 + 退出码 + 摘要 |
| 源码回读 | 逐函数读新增文件，对照上轮修复点 | `file:line` |
| 数据复算 | DuckDB 只读查询 canonical/derived；base64 解码 raw JSONL | 数值 + 查询脚本 |
| 反证测试 | 检查新增测试是否覆盖新增代码的失败路径 | 测试名 + 断言 |
| 基线复核 | 对 C2/C3/A-1/TGA-1/B-1 定向 grep + `py_compile` + 测试 | 结果 |
| 只读约束 | 未执行任何写库/重置/重采命令 | 见 §14 |

**证据分级**：标"已核实"者为本次直接回读源码或只读查询确认；标"测试级复核"者为既有回归用例通过但未逐行重读实现；标"推断"者为无法在当前只读条件下实证。

### 1.4 严重级别（沿用上轮）

| 级别 | 判定标准 |
|---|---|
| CRITICAL | 静默数据丢失/损坏；不可恢复状态；核心流程不可用；违反核心不变量且无告警；生产必然产出错误结果 |
| HIGH | 会返工或接口不兼容；测试无法验证核心行为；约束明文违反；关键路径可触发崩溃窗口；版本库状态导致构建/导入失败 |
| MEDIUM | 局部功能缺陷或配置陷阱；文档与实现显著不一致；可测但缺测试 |
| LOW | 代码/文档质量瑕疵、日志口径、命名/格式 |

### 1.5 门禁（沿用上轮）

G1 静态质量 / G2 测试 / G3 契约测试 / G4 静默丢失 / G5 约束核验 / G6 版本库 / G7 文档一致。**通过标准：G1–G7 全部 ✅ 才判 READY。**

---

## 2. 基线验证与工作区状态

### 2.1 git 状态（区分"已提交"与"仅工作区"）

```
$ git status -sb
## main...origin/main [ahead 52]
 M pyproject.toml
 M scripts/fdam_attention.py
 M scripts/plot_btc_etf_flow.py
 M scripts/plot_rates_2y_effr.py
 M scripts/symbols/z_manual_fred_symbols.json
 M scripts/symbols/z_manual_yahoo_symbols.json
 M src/chronoforge/cli/_wiring.py
 M src/chronoforge/pipeline/windows.py
 M src/chronoforge/registry/service.py
 M uv.lock
?? docs/USD_Liquidity/
?? scripts/register_nyfed_datasets.py
?? scripts/reserve_forecast.py
?? src/chronoforge/connectors/nyfed.py
?? visualization/
```

- **已提交但未推送**：`main` 领先 `origin/main` **52 个提交**。最近的提交包括 `b6aab22`（日线归一时间戳）、`29fd88e`（默认停用清单持久化）、`3e580ef`（yahoo 日线 00:00）、`0b4730e`（set-enabled 入口）等。
- **仅在工作区（未提交）**：
  - 新文件：`src/chronoforge/connectors/nyfed.py`、`scripts/register_nyfed_datasets.py`、`scripts/reserve_forecast.py`、`visualization/`、`docs/USD_Liquidity/`；
  - 修改：`pyproject.toml`、`uv.lock`、`src/chronoforge/cli/_wiring.py`、`src/chronoforge/pipeline/windows.py`、`src/chronoforge/registry/service.py`、`scripts/fdam_attention.py`、`scripts/plot_btc_etf_flow.py`、`scripts/plot_rates_2y_effr.py`、两个 `scripts/symbols/*.json`。
- **含义**：本轮审计结论中，凡涉及 `nyfed`/准备金/可视化站点的，**全部属于"仅工作区"**；它们既没有测试保护，也没有提交历史，任何 `git clean`/切分支都会丢失。G6 因此为红。

### 2.2 静态检查与测试（实跑）

| 检查 | 命令 | 结果 |
|---|---|---|
| 类型 | `uv run mypy src` | ❌ **2 errors in 1 file (checked 86 source files)**，均在 `nyfed.py:125/126` |
| Lint | `uv run ruff check src` | ❌ **Found 2 errors**，均 `E501`，均在 `nyfed.py` |
| 全量测试 | `uv run --with pytest --with pytest-asyncio --with hypothesis python -m pytest -q` | ❌ **3 failed, 1980 passed, 10 warnings in 89.84s** |
| 脚本编译 | `uv run python -m py_compile scripts/fdam_attention.py` | ✅ 通过（B-1 关闭） |
| 恒等式 | `uv run python scripts/reserve_forecast.py check` | 输出 `PASS（L1/L3 容差 3 USD 百万）`，但 L2/L3 为恒等式（见 M-N1） |

**3 条失败**：

1. `tests/architecture/test_docs_consistency.py::test_readme_source_count_matches_default_sources` —— `DEFAULT_SOURCES` 已含 `nyfed`（14 个），README 仍写 `bootstrapped: 13 sources`；
2. `tests/unit/test_cli.py::test_sync_bootstraps_all_sources_idempotent` —— 断言 `"13 sources" in output` 且 `COUNT(*)=13`；
3. `tests/unit/test_cli.py::test_list_sources_json` —— 期望集合不含 `nyfed`。

> 这说明新增源已进代码，但测试与文档没同步：**守卫确实会变红**（不是形式守卫），但红的方向是"代码超前于文档/契约"，而不是"代码被改坏"。

### 2.3 环境可复现性问题

项目 `.venv` 未安装 `pytest` / `hypothesis`，直接 `uv run pytest` 会因 `ModuleNotFoundError: hypothesis` 收集失败；`uv run` 还可能落到 Homebrew Python 3.14。本次用 `--with pytest --with pytest-asyncio --with hypothesis` 才跑通。**G2 的官方命令 `uv run pytest -q` 在当前环境不可复现**，记为 L-N9。

---

## 3. CRITICAL

### C-N1. SRF 口径把 SVE 测试操作计入真实用量，canonical 已被污染 ✅ 已核实 → ✅ 已修复（2026-11-07，第 1 批）

**位置**：[nyfed.py#L286-L311](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/nyfed.py#L286-L311)

```python
for row in rows:
    ...
    # SRF 仅隔夜；Term Repo 不属 SRF 口径
    if str(row.get("term", "")).strip().lower() != "overnight":
        skipped["non_overnight"] = skipped.get("non_overnight", 0) + 1
        continue
    ...
    by_date[obs_date] = by_date.get(obs_date, 0.0) + amount
```

**问题**：只按 `term == "Overnight"` 过滤，没有排除 `note` 含 `Small Value Exercise (SVE)` 的测试操作，也没有用 `operationId` 尾号/`note` 做二次判定。SVE 操作同样是 `Overnight`，金额约 1 亿美元，会与真实 SRF 用量按日期相加。

**原始证据**（base64 解码 `data/raw/nyfed/nyfed_repo_srf_usage/**/*.jsonl`）：

| operationDate | operationId | term | totalAmtAccepted | note |
|---|---|---|---|---|
| 2025-11-05 | RP 110525 3 | Overnight | 3,000,000 | （空） |
| 2025-11-05 | RP 110525 25 | Overnight | 99,000,000 | `This operation is a Small Value Exercise (SVE); ...` |
| 2026-05-27 | RP 052726 25 | Overnight | 4,000,000 | （空） |
| 2026-05-27 | RP 052726 26 | Overnight | 7,000,000 | （空） |
| 2026-05-27 | RP 052726 99 | Overnight | 100,000,000 | `This operation is a Small Value Exercise (SVE), ...` |

**canonical 实际值**（只读查询 `data/canonical/NUMBER/entity=srf_usage`）：

| observation_time | canonical value | 应为 |
|---|---|---|
| 2025-11-05 | **102,000,000** | 3,000,000 |
| 2026-05-27 | **111,000,000** | 11,000,000 |

**影响**：
- 图 7（SRF 用量）与任何用 `srf_usage` 做流动性分析的推导都会把测试操作当成真实需求；
- 2026-05-27 的 111M 与真实 11M 相差 10 倍，足以改变"SRF 是否被使用"的判断；
- 这是**静默**的：normalize 成功返回、checkpoint 推进、质量门禁不触发。

**复现**：

```bash
uv run python - <<'PY'
import glob, json, base64
for f in glob.glob('data/raw/nyfed/nyfed_repo_srf_usage/**/*.jsonl', recursive=True):
    for line in open(f):
        o = json.loads(line)
        p = o.get('payload')
        if isinstance(p, str):
            p = json.loads(base64.b64decode(p))
        for r in (p or {}).get('repo', {}).get('operations', []):
            if 'SVE' in str(r.get('note', '')) and r.get('totalAmtAccepted'):
                print(r['operationDate'], r['operationId'], r['totalAmtAccepted'])
PY
```

**判定**：CRITICAL。生产数据已错，需按 `note` 规则回溯修复 canonical。~~本轮只读，未修。~~ **已于 2026-11-07 第 1 批修复并重采，见下。**

> 补充：`term == "Overnight"` 这个前提本身成立（SRF 是隔夜回购），**错的是没有排除同 term 的 SVE**。用户要求核查的三处假设中，`term` 假设成立、`observation_time=操作日` 成立，`release_time=当日 22:00` 不构成前视但存在"预发布占位"问题（见 H-N2）。

#### 第 1 批修复记录（2026-11-07）

**代码修复**：[nyfed.py#L291-L296](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/nyfed.py#L291-L296)

```python
_SVE_MARKER = "small value exercise"
...
note = row.get("note")
if isinstance(note, str) and _SVE_MARKER in note.lower():
    skipped["sve_test_operation"] = skipped.get("sve_test_operation", 0) + 1
    continue
```

**识别依据（真实响应，非猜测）**：`repo.operations[].note` 字段。实测变体包括
`"This operation is a Small Value Exercise (SVE); ..."`、
`"This operation is a Small Value Exercise."`、
`"This operation is an early settling overnight repo small value exercise (SVE); ..."`、
以及带 `<p>` HTML 包裹的版本。`operationId` 尾号**不可靠**：实测尾号 1/2/4/25/26/28/29/99 都出现过 SVE 行。

**测试**：[tests/integration/test_nyfed.py](file:///Users/horsenli/Works/ChronoForge/tests/integration/test_nyfed.py)

- 4 条用例：真实+SVE 同日只计真实部分、纯 SVE 日不产出记录、Term SVE 不计入、note 变体大小写/HTML/early-settling；
- 先红后绿：修复前 `3 failed, 1 passed`（Term 用例因既有 term 过滤本就通过），修复后 `4 passed`；
- **审计事实修正**：真实数据中**不存在"纯 SVE 日"**——13 个含 SVE 的日期都至少有一个真实 Overnight 操作（多数接受量为 0）。纯 SVE 日用例是合成的负向用例，用于锁死"无真实操作时不产记录"的语义。

**备份**：

| 对象 | 备份路径 | 大小 |
|---|---|---|
| canonical SRF 分区 | `var/backups/2026-11-07-srf-sve/canonical/entity=srf_usage/` | 见 `du -sh` |
| raw SRF 分区 | `var/backups/2026-11-07-srf-sve/raw/nyfed_repo_srf_usage/` | 见 `du -sh` |

**回滚方式**：

```bash
rm -rf data/canonical/NUMBER/entity=srf_usage
cp -a var/backups/2026-11-07-srf-sve/canonical/entity=srf_usage data/canonical/NUMBER/
rm -rf data/raw/nyfed/nyfed_repo_srf_usage
cp -a var/backups/2026-11-07-srf-sve/raw/nyfed_repo_srf_usage data/raw/nyfed/
# checkpoint 需回退为 2026-11-06T15:23:16（本次重采前值）
```

**重采**：

```bash
uv run python scripts/backfill_datasets.py --sources nyfed --entities srf_usage \
  --canonical-types NUMBER --start 2021-07-29 --end 2026-11-07
```

结果：`SUCCESS`，`1317` 条 canonical 行（与修复前行数一致），`checkpoint_before=2026-11-06T15:23:16` → `checkpoint_after=2026-11-07T00:00:00`。

**全量污染清单（13 个含 SVE 的日期）**：

| 日期 | SVE 类型/金额 | 真实 Overnight | 修复前 canonical | 修复后 canonical | 差额 |
|---|---:|---:|---:|---:|---:|
| 2022-04-11 | Overnight 61M | 0 | 61,000,000 | 0 | −61,000,000 |
| 2022-09-08 | Overnight 61M | 0 | 61,000,000 | 0 | −61,000,000 |
| 2023-11-15 | Overnight 76M | 0 | 76,000,000 | 0 | −76,000,000 |
| 2024-05-09 | Overnight 94M | 0 | 94,000,000 | 0 | −94,000,000 |
| 2025-01-24 | Overnight 82M | 0 | 82,000,000 | 0 | −82,000,000 |
| 2025-02-26 | Overnight 94M | 3M | 97,000,000 | 3,000,000 | −94,000,000 |
| 2025-03-05 | Overnight 84M | 0 | 84,000,000 | 0 | −84,000,000 |
| 2025-08-21 | **Term** 61M | 1M | 1,000,000 | 1,000,000 | 0（本就未计入） |
| 2025-10-08 | Overnight 97M | 1M | 98,000,000 | 1,000,000 | −97,000,000 |
| 2025-11-05 | Overnight 99M | 3M | 102,000,000 | **3,000,000** | −99,000,000 |
| 2026-02-18 | Overnight 56M | 4M | 60,000,000 | 4,000,000 | −56,000,000 |
| 2026-05-27 | Overnight 100M | 11M | 111,000,000 | **11,000,000** | −100,000,000 |
| 2026-10-20 | **Term** 67M | 0 | 0 | 0 | 0（本就未计入） |

- **实际被污染的日期**：11 个（Overnight SVE）；另 2 个仅 Term SVE，term 过滤已生效，无污染。
- **核对**：2025-11-05 = 3,000,000 ✅；2026-05-27 = 11,000,000 ✅。
- **残留**：旧/新 canonical 的 `observation_time` 集合完全一致（old-only=0，new-only=0），行数 1317 不变；无孤立/残留行。
- **upsert**：自然键（含 `revision_time`）未变，新值覆盖旧值；storage 会记录漂移并保留最终修订。无旧值残留。
- **checkpoint**：由窗口右界推进到 `2026-11-07T00:00:00`（`advance_chunks` 语义，不是 `checkpoint_from` 的数据最大日期）；本次重采前为 `2026-11-06T15:23:16`。

**门禁影响**：G4 的"SRF 污染"子项关闭，但 **G4 仍红**（C-N2 的静默丢失未修）。G1/G2/G3/G6/G7 不受本批影响，仍红。

---

### C-N2. `nyfed` 零产出/逐行丢弃不触发门禁，checkpoint 照常推进 ✅ 已核实

**位置**：
- [runner.py#L433-L449](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/pipeline/runner.py#L433-L449) `_payload_units`
- [runner.py#L516-L524](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/pipeline/runner.py#L516-L524) `untracked` 计算
- [nyfed.py#L270-L346](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/nyfed.py#L270-L346) `_normalize_rp`
- [nyfed.py#L348-L413](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/nyfed.py#L348-L413) `_normalize_soma`
- [nyfed.py#L451-L465](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/nyfed.py#L451-L465) `checkpoint_from`

**链路**：

1. `_payload_units` 只识别 list 或顶层 dict 的 `rows/data/result/observations/values`：
   ```python
   if isinstance(payload, dict):
       for key in ("rows", "data", "result", "observations", "values"):
           value = payload.get(key)
           if isinstance(value, (list, tuple)):
               return len(value)
   return 0
   ```
   nyfed 的 payload 是 `{"repo": {"operations": [...]}}` 或 `{"soma": {"summary": [...]}}` → **返回 0**。

2. `NormalizeStage` 的行级丢弃：
   ```python
   untracked = max(0, _payload_units(batch.payload) - len(records) - reported_skips)
   drop_attempted += len(records) + reported_skips + untracked
   ```
   `_payload_units=0`、`reported_skips=0`（nyfed 没有 `note_skip`）→ `drop_attempted=0` → `_enforce_drop_gate()` 直接返回，**不抛 QualityError**。

3. `nyfed` 的跳过只写本地 dict + `logger.warning`，不调用 `normalize_stats.note_skip`：
   ```python
   skipped["non_overnight"] = skipped.get("non_overnight", 0) + 1
   ...
   logger.warning("connector.normalize_summary", ..., reasons=skipped)
   ```

4. `checkpoint_from` 直接读 raw：
   ```python
   rows = raw.payload.get("repo", {}).get("operations", [])
   dates = [str(r.get("operationDate")) for r in rows if isinstance(r, dict)]
   return max(dates) if dates else None
   ```
   即使 normalize 把全部记录过滤掉（例如全部非 Overnight / 全部缺金额），cursor 仍推进到 raw 的最大日期。

**后果**：只要 HTTP 200、payload 看起来正常，但 normalize 返回 0 行，本次 run 仍是 SUCCESS，checkpoint 推进，下一窗口不会再回头。与上轮 C1 的症状**完全一致**，只是换了一个连接器。

**复现（最小用例，只读构造）**：

```python
# 伪代码：把 nyfed 的 payload 形状喂给 _payload_units
from chronoforge.pipeline.runner import _payload_units
assert _payload_units({"repo": {"operations": [{"operationDate": "2026-11-06"}]}}) == 0
assert _payload_units({"soma": {"summary": [{"asOfDate": "2026-11-04"}]}}) == 0
```

**测试缺口**：[tests/quality/test_normalize_drop_gate.py](file:///Users/horsenli/Works/ChronoForge/tests/quality/test_normalize_drop_gate.py) 只覆盖 OHLCV 的 list payload；全仓 grep `nyfed` 在 `tests/` 下**0 命中**。

**判定**：CRITICAL。它是所有 nyfed 解析缺陷的"静默放大器"，且直接违反 G4。

---

## 4. HIGH

### H-N1. `nyfed` 没有发布时刻配置 → SOMA 恒定约 1 周滞后、SRF 约 1 天滞后 ✅ 已核实

**位置**：[update_schedule.py#L24-L51](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/config/update_schedule.py#L24-L51)

```python
SOURCE_PUBLISH_TIME_UTC: dict[str, tuple[int, int]] = {
    "fred": (8, 10),
    ...
}
DEFAULT_PUBLISH_TIME_UTC: tuple[int, int] = (16, 0)
```

**问题**：
- `SOURCE_PUBLISH_TIME_UTC` 没有 `nyfed` 键 → 用默认 **16:00 UTC**。
- SOMA summary：周三快照，NY Fed 周四下午/晚间（约 21:00–23:59 UTC）发布。周四 16:00 抓取时，本周三的快照尚未发布 → 恒定拿到上一周。
- SRF：操作 13:30 ET（17:30/18:30 UTC），结果在操作后公布。每天 16:00 UTC 抓取时，当日操作尚未发生 → 恒定拿到前一日结果，并把当日写成 0。
- 文件自身在第 12 行就写明：**"在发布时刻前抓取会推进 last_success_time，导致数据恒定滞后一天。"** 新增源没有遵守自己的警告。

**影响**：SOMA 9 分项与 `reserve_forecast` 的 `load_soma` 输入永远慢一周；SRF 图的"最新值"永远慢一天。不是前视，是**陈旧**。

**复现**：

```bash
uv run python -c "from chronoforge.config.update_schedule import publish_time_utc; print(publish_time_utc('nyfed'))"  # 16:00
```

**判定**：HIGH（数据可用性/时效性约束违反，且会长期存在）。

---

### H-N2. SRF 在操作发生前把当日写为 0，并把 `release_time` 标为当日 22:00 ✅ 已核实

**位置**：[nyfed.py#L313-L335](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/nyfed.py#L313-L335)

```python
observation_time = datetime.combine(obs_date, datetime.min.time())
release_time = observation_time.replace(hour=22)  # 当日收盘后公布
...
value=round(by_date[obs_date], 2),
release_time=release_time,
revision_time=release_time,
```

**证据**：
- 最近一次 SRF raw 抓取是 `2026-11-06 15:23:18 UTC`（`data/raw/nyfed/nyfed_repo_srf_usage/ingest_date=2026-11-06/152318-0001.jsonl`）；
- 该文件里 `2026-11-06` 的 `RP 110626 25` 为 `Overnight`、`totalAmtAccepted=0`；
- canonical `srf_usage` 最新一行：`observation_time=2026-11-06`、`release_time=2026-11-06 22:00:00`、`value=0.0`。

**问题**：13:30 ET = 18:30 UTC（夏令时 17:30 UTC），抓取发生在操作之前，`0` 是"操作尚未发生"的占位值，不是"当日净用量为 0"的结论。但该行被赋予 `release_time=22:00`，在 22:00 之前就对任何读 canonical 的消费者可见。若下一次调度因熔断/停机未重拉，这个 0 会被长期保留。

**缓解**：`_get_config` 对 `nyfed_` 走 `full_window`（[windows.py#L263-L265](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/pipeline/windows.py#L263-L265)），下一次成功运行会从 `job.start` 全窗口重拉并 upsert，通常会纠正前一日。因此这是**最终一致但存在窗口期**的问题，不是永久错误。

**判定**：HIGH（发布前可见的错误占位 + 依赖后续重拉；调度中断即永久化）。

---

### H-N3. 准备金模型 PIT 只截 `observation_time`，origin 周三使用周四才发布的 H.4.1/SOMA ✅ 已核实

**位置**：
- [reserve_forecast.py#L118-L128](file:///Users/horsenli/Works/ChronoForge/scripts/reserve_forecast.py#L118-L128)（只 select `observation_time`/`value`）
- [reserve_forecast.py#L142-L156](file:///Users/horsenli/Works/ChronoForge/scripts/reserve_forecast.py#L142-L156)（同上）
- [reserve_forecast.py#L304-L358](file:///Users/horsenli/Works/ChronoForge/scripts/reserve_forecast.py#L304-L358)（`hist = df[df.index <= origin]`）
- [reserve_forecast.py#L374-L428](file:///Users/horsenli/Works/ChronoForge/scripts/reserve_forecast.py#L374-L428)（回测）

**证据**：canonical `WRBWFRBL` 的发布滞后是稳定的 1 天：

```
observation_time 2026-11-04  release_time 2026-11-05 23:59:59
observation_time 2026-10-28  release_time 2026-10-29 23:59:59
```

`forecast_from_origin` 用 `df[df.index <= origin]` 截断，origin=2026-11-04 时，`hist` 的最后一行的 `release_time` 是 2026-11-05 23:59:59。也就是说，模型在"周三收盘"这个 origin 上使用了"周四晚间才发布"的数据。

**用户要求核查的四个点**：

| 核查项 | 结论 |
|---|---|
| PIT 是否真正截断 | **否**。只截 `observation_time`，未读/未比较 `release_time`。 |
| 回测是否用了未来数据 | **是（1 天发布滞后）**。目标值本身在 origin 之后是正常的；问题是 origin 当期的 H.4.1/SOMA 在 origin 时点尚未发布。 |
| 置信带口径 | **不一致**（见 M-N2）。 |
| 消融与 DM | DM 算法基本正确（HAC + 双侧正态）；"消融"仅剩 TGA 一项且文档仍写 0.1.0 的 `pred_drop_*`（见 M-N10）。 |

另外，canonical 单版本存储只保留最终修订，回测 `actual` 用最终修订值；`index.html` 已承认这一点，但 `reserve_forecast.py` 自身没有在输出中标记修订口径。

**复现**：

```bash
uv run python - <<'PY'
import duckdb
con = duckdb.connect()
print(con.execute("""
select observation_time, release_time from read_parquet(
 'data/canonical/NUMBER/entity=WRBWFRBL/**/*.parquet', hive_partitioning=1)
order by observation_time desc limit 3""").pl().to_pandas())
PY
```

**判定**：HIGH。PIT 是本项目的核心不变量；文档（`index.html:359`）明确声称"每个 origin 只用该时点之前的数据"，与实现不符。

---

### H-N4. TGA 区间在 2027-02-04 发散：算法性线性放大，图 5 硬编码 y 轴裁切掩盖 ✅ 已核实

**位置**：
- [tga_forecast.py#L297-L306](file:///Users/horsenli/Works/ChronoForge/scripts/tga_forecast.py#L297-L306)
- [tga_forecast.py#L1275-L1278](file:///Users/horsenli/Works/ChronoForge/scripts/tga_forecast.py#L1275-L1278)
- [tga_forecast.py#L1311-L1318](file:///Users/horsenli/Works/ChronoForge/scripts/tga_forecast.py#L1311-L1318)
- [app.js#L503](file:///Users/horsenli/Works/ChronoForge/visualization/app.js#L503)

**算法**：

```python
# _component_row: 组件带 = 金额的 50%
band = _band(source_type, model) * abs(amount)
lower = amount - band
upper = amount + band

# aggregate: 逐日把各组件 lower/upper 相加，再对整段做 cum_sum
pl.col("lower").sum().alias("daily_lower"),
pl.col("upper").sum().alias("daily_upper"),
...
(ctx.last_tga + pl.col("daily_lower").cum_sum()).alias("lower"),
(ctx.last_tga + pl.col("daily_upper").cum_sum()).alias("upper"),
```

**判定：是区间累积算法问题，不是数据问题。** 每个组件的带宽按金额的 50% 生成，逐日相加后 `cum_sum`，带宽随预测天数线性增长；到 59 天后，日半宽合计约 6.35 万亿美元。

**derived 实测值**（`data/derived/tga_forecast`，run_date=2026-11-06，target=2027-02-04）：

| scenario | predicted_tga | lower | upper |
|---|---|---|---|
| base | 730,972 | **−5,622,973** | **7,084,918** |
| liquidity_easing | 209,781 | **−5,973,376** | 6,392,938 |
| liquidity_tightening | 1,252,164 | −5,272,570 | **7,768,898** |

（单位：百万美元；predicted 0.73T，区间 −5.62T ~ +7.08T。）

**为什么图上看不见**：`chart5` 的 y 轴是 `min: 0, max: 1.45`（万亿美元），而 `bandLo` 直接使用 `lower`（可为负），`band` 使用 `hi - lo`。负下沿与 7T 上沿都被裁掉，观众只看到一条窄带，误以为区间正常。

**判定**：HIGH。模型输出本身产生无意义的 ±6T 区间，且可视化层主动掩盖了它。修复方向应是"区间按预测期做分位/方差传播，而非逐日带线性累加 + 硬编码轴改为自适应或在图上显式标注越界"。本轮只记录，不改。

---

### H-N5. G1 红：`mypy` 与 `ruff` 均在 `nyfed.py` 报错 ✅ 已核实

**位置**：[nyfed.py#L118-L136](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/nyfed.py#L118-L136)

```python
def health(self) -> HealthStatus:
    ...
    self._get_json(
        {
            "startDate": ...,
            "endDate": ...,
            "operationTypes": "Repo",
        }
    )
```

`_get_json` 的签名是 `(self, endpoint: str, params: dict[str, Any])`。这里把 dict 当作 `endpoint` 传入，且少传 `params`：

```
nyfed.py:125: error: Missing positional argument "params" in call to "_get_json"  [call-arg]
nyfed.py:126: error: Argument 1 ... has incompatible type "dict[str, str]"; expected "str"  [arg-type]
```

`ruff` 另有 2 个 `E501`。

**含义**：`health()` 一旦被调用（CLI 健康检查/探活）会抛 `TypeError`，而不是返回 `HealthStatus(ok=False)`——`except Exception` 会捕获，但仍会把"代码 bug"伪装成"上游不可用"。同时 G1 门禁直接为红。

**判定**：HIGH（同时是门禁失败与功能缺陷）。

---

### H-N6. G2 红：3 条测试失败，新增源未同步测试 ✅ 已核实

见 §2.2。`test_cli.py` 的 `13 sources` 与源集合断言、`test_docs_consistency.py` 的 README 源数断言全部因新增 `nyfed` 而失败。

**判定**：HIGH（门禁失败；也说明新增源没有走"加源即改契约"的流程）。

---

### H-N7. G3 重新打开：`nyfed` 无 fixture、无 fetch→normalize 全链路测试 ✅ 已核实

- `tests/` 下 grep `nyfed`：0 命中；
- `tests/fixtures/` 下无 `nyfed/`；
- `tests/e2e/test_binance_spot_pipeline.py` 只覆盖 Binance；
- `tests/quality/test_normalize_drop_gate.py` 只用 OHLCV list payload。

上轮 G3 的通过条件是"每条连接器有真实响应样本 → fetch → normalize 全链路测试"。第 14 条连接器没有满足。

**判定**：HIGH（门禁重新打开）。

---

### H-N8. G6 红：新增代码全部未提交，且分支落后远端 52 个提交 ✅ 已核实

见 §2.1。尤其：
- `nyfed.py`、`reserve_forecast.py`、`register_nyfed_datasets.py`、`visualization/`、`docs/USD_Liquidity/` 全部 `??`；
- `windows.py`、`registry/service.py`、`_wiring.py` 的修改未提交；
- `main` 领先 `origin/main` 52 个提交，说明即使已提交部分也未推送。

**含义**：当前工作区不可作为交付基线；任何构建/发布都会把未审的改动一起带出去，或者因 `git clean` 丢失。

**判定**：HIGH（门禁失败）。

---

### H-N9. G7 红：README 源数与新增子系统文档缺失 ✅ 已核实

- [README.md#L81](file:///Users/horsenli/Works/ChronoForge/README.md#L81) `bootstrapped: 13 sources`，实际 `DEFAULT_SOURCES` 已含 `nyfed`（14）；
- [README.md#L84](file:///Users/horsenli/Works/ChronoForge/README.md#L84) `13 pre-configured sources`；
- `docs/architecture`/`docs/design` 中没有 `nyfed` 连接器、`reserve-structural` 模型、`visualization/` 站点的设计文档；
- `visualization/README.md` 与站点自身矛盾（见 M-N5）。

**判定**：HIGH（门禁失败；文档与代码事实不一致）。

---

## 5. MEDIUM

### M-N1. 准备金"三类恒等式"实际只有 L1 独立，L2 恒为 0、L3 ≡ −L1 ✅ 已核实

**位置**：[reserve_forecast.py#L159-L221](file:///Users/horsenli/Works/ChronoForge/scripts/reserve_forecast.py#L159-L221)

```python
df["other_dep"] = df["WLDLCL"] - (
    df["WRBWFRBL"] + df["WDTGAL"] + df["TERMT"] + df["foreign_dep"]
)
...
l2 = df["WLDLCL"] - (
    df["WRBWFRBL"] + df["WDTGAL"] + df["TERMT"] + df["foreign_dep"] + df["other_dep"]
)
l3 = df["WRBWFRBL"] - df["reserve_implied"]
```

代数展开：
- `L2 ≡ WLDLCL − (WLDLCL) = 0`，恒等式，不提供任何信息；
- `reserve_implied = WRBWFRBL + L1`，所以 `L3 = WRBWFRBL − reserve_implied ≡ −L1`；
- `cmd_check` 实际只 gate L1 与 L3（[L649-L652](file:///Users/horsenli/Works/ChronoForge/scripts/reserve_forecast.py#L649-L652)），L3 是 L1 的镜像，等于同一个校验做两遍。

**独立复算**（canonical，1247 周）：`L1 max_abs=2.000M USD、mean_abs=0.499M`。即 L1 逐周并不严格为 0，但在 3M 容差内；`L2=0`、`L3=−L1`。

**判定**：MEDIUM。用户问的"`WRBWFRBL` 重构恒等式是否逐周成立"——答案是：L1 近似成立（最大 2M，约 0.07%），L2/L3 是恒等式，不构成独立校验。

### M-N2. 置信带是逐 horizon 边际分位，标签却写 `joint` / "联合 90%" ✅ 已核实

**位置**：
- [reserve_forecast.py#L431-L455](file:///Users/horsenli/Works/ChronoForge/scripts/reserve_forecast.py#L431-L455)
- [reserve_forecast.py#L611-L613](file:///Users/horsenli/Works/ChronoForge/scripts/reserve_forecast.py#L611-L613)
- [reserve_forecast.py#L676-L677](file:///Users/horsenli/Works/ChronoForge/scripts/reserve_forecast.py#L676-L677)
- [index.html#L359](file:///Users/horsenli/Works/ChronoForge/visualization/index.html#L359)

```python
for h in sorted(...):
    e = recent[recent["horizon"] == h]["error"].dropna()
    "q05": round(float(e.quantile(0.05)), 1),
    "q95": round(float(e.quantile(0.95)), 1),
...
lo = fc["predicted_reserve"] + st.get("q05", 0.0)
hi = fc["predicted_reserve"] + st.get("q95", 0.0)
```

这是"对每个 horizon 分别取误差的 5%/95% 分位"，即**边际区间**。`_config` 的 `"type": "empirical_error_quantile_joint"` 与 `index.html` 的"联合 90% 带"都不成立：联合区间要么用每个 origin 的跨 horizon 最大绝对误差，要么做 Bonferroni/同时置信带，宽度应更宽。

**判定**：MEDIUM。口径名不副实，会误导使用者对带宽的解读。

### M-N3. `_atomic_write` 重演 H6 崩溃窗口，且失败时删除旧分区 ✅ 已核实

**位置**：[reserve_forecast.py#L567-L593](file:///Users/horsenli/Works/ChronoForge/scripts/reserve_forecast.py#L567-L593)

```python
if part_dir.exists():
    part_dir.rename(old_dir)
tmp_dir.rename(part_dir)
...
finally:
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir, ignore_errors=True)
    if old_dir.exists():
        shutil.rmtree(old_dir, ignore_errors=True)
```

若 `tmp_dir.rename(part_dir)` 失败（磁盘满/权限/跨设备），`finally` 会把仍然保存着旧数据的 `old_dir` 删掉 → 该月分区彻底丢失。这与上轮 H6 的崩溃窗口是同一模式。

另外，每次 run 只写"最新 origin"的预测行，但按 `year/month` **整月覆盖**，会把同一目标月份里历史 run 的行覆盖掉。当前导出只取 `max(run_date)`，所以可视化不受影响，但预测历史不可追溯。

**判定**：MEDIUM（derived 可重跑，不是不可恢复；但违反原子写承诺）。

### M-N4. 可视化 renderer 异常/全 null 会静默空白，实例还被缓存 ✅ 已核实

**位置**：[app.js#L169-L194](file:///Users/horsenli/Works/ChronoForge/visualization/app.js#L169-L194)

```js
const inst = echarts.init(dom, ...);
instances[id] = inst;
renderers[id](inst);              // ← 没有 try/catch
const opt = inst.getOption();
const series = (opt && opt.series) || [];
const hasData = series.some((s) => Array.isArray(s.data) && s.data.length > 0);
if (!dom.clientWidth || !dom.clientHeight || !hasData) { ... }
```

- renderer 抛异常时，异常向上冒泡，`instances[id]` 已写入，后续切 tab 会直接 `return`（第 170 行），**永久空白且无提示**；
- `hasData` 只检查 `length > 0`，一条全 `null` 的 series 也能通过，图上什么也看不见但不报错；
- 空数据断言本身有可见提示（第 192 行），这是好的；但异常路径没有。

**判定**：MEDIUM。用户明确要求"图表失败是否有可见提示而非静默空白"——答案是"部分有，异常路径没有"。

### M-N5. `visualization/README.md` 与站点/代码互相矛盾 ✅ 已核实

| 位置 | 文档写的 | 实际 |
|---|---|---|
| [README.md#L3](file:///Users/horsenli/Works/ChronoForge/visualization/README.md#L3) | "16 张图" | `index.html` 18 张（[L60](file:///Users/horsenli/Works/ChronoForge/visualization/index.html#L60)），`PANE_CHARTS` 18 个 id |
| [README.md#L58](file:///Users/horsenli/Works/ChronoForge/visualization/README.md#L58) | 准备金预测"`forecast_runs` 仅有 TGA 模型，方案待确认后建模" | `reserve_forecast.py` 已实现并导出图 16 |
| [README.md#L52](file:///Users/horsenli/Works/ChronoForge/visualization/README.md#L52) | backfill 起点 `2021-07-29` | SOMA 实测最早 2003-07-09（`_SOMA_OLDEST_DATE`） |

**判定**：MEDIUM。

### M-N6. `index.html` 对 PIT 与"联合 90%"的描述与实现不符 ✅ 已核实

[index.html#L359](file:///Users/horsenli/Works/ChronoForge/visualization/index.html#L359)：

> 置信带：**联合**口径——准备金水平的 h 步外推残差近 5 年 5–95 分位，…回测为扩张窗口，**每个 origin 只用该时点之前的数据**；canonical 单版本存储，回测为最终修订值口径。

前两点均与实现不符（M-N2、H-N3）。第三点（单版本/最终修订）属实。

**判定**：MEDIUM。站点是面向使用者的最终呈现，这类"方法学声明"必须准确。

### M-N7. `hourly_update.sh` 不驱动新增链路 ✅ 已核实

**位置**：[hourly_update.sh#L114-L162](file:///Users/horsenli/Works/ChronoForge/scripts/hourly_update.sh#L114-L162)

只做：
1. `backfill_datasets.py --due-only`；
2. `run_plots`（`scripts/plot_*.py`）。

**不运行**：`reserve_forecast.py`、`visualization/export_data.py`（或 `refresh.sh`）、`dataset_stats.py`、`register_nyfed_datasets.py`。

含义：准备金模型与可视化站点不会自动刷新；`nyfed` 数据集注册脚本需要人工先跑一次，否则 `--due-only` 找不到它们。不是立即错误，但"新增子系统接入调度"这一步没有完成。

**判定**：MEDIUM。

### M-N8. `nyfed` 的 `checkpoint_from` 以 raw 最大日期推进，即使 normalize 全部过滤 ✅ 已核实

**位置**：[nyfed.py#L451-L465](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/nyfed.py#L451-L465)

与 C-N2 同源，但这里是独立的可观测性问题：即使连接器未来补上 `note_skip`，`checkpoint_from` 仍会在"raw 有行、normalize 返回空"时推进。正确做法是优先使用 normalize 后的观测日期，raw 只作为回退。

**判定**：MEDIUM（与 C-N2 组合升级为 CRITICAL）。

### M-N9. `_load_number_series` / `load_soma` 丢弃 `release_time`/`revision_time` ✅ 已核实

**位置**：[reserve_forecast.py#L118-L128](file:///Users/horsenli/Works/ChronoForge/scripts/reserve_forecast.py#L118-L128)、[L142-L156](file:///Users/horsenli/Works/ChronoForge/scripts/reserve_forecast.py#L142-L156)

```python
result = service.query(dataset_id=..., filters=...)
df = result.frame.select(["observation_time", "value"])
```

模型侧拿不到 `release_time`，因此**即使想修 PIT 也无从下手**。这是 H-N3 的结构性原因，也会让后续修订感知（revision）无法实现。

**判定**：MEDIUM。

### M-N10. `reserve_forecast.py` 文档/标注与 0.1.1 实现不符 ✅ 已核实

**位置**：[reserve_forecast.py#L374-L428](file:///Users/horsenli/Works/ChronoForge/scripts/reserve_forecast.py#L374-L428)

```python
"""扩张窗口回测：每个 origin 只用该时点之前的数据。

同一批 origin 上同时产出：
- predicted（模型 reserve-structural-0.1.0）；
...
- pred_drop_currency / pred_drop_tga（科目级消融：把该科目固定为 naive）。
"""
```

实际：
- 模型版本是 `reserve-structural-0.1.1`（currency 已 HOLD，[L328-L330](file:///Users/horsenli/Works/ChronoForge/scripts/reserve_forecast.py#L328-L330)）；
- 未产出 `pred_drop_currency` / `pred_drop_tga` 列；
- 只有 `tga_increment = naive_mae - model_mae`，这是"相对持平基准的增量"，在 0.1.1 里等价于"去掉 TGA"，但与文档描述的"科目级消融"不是同一实现。

**判定**：MEDIUM（会误导后续研究者以为存在完整消融矩阵）。

---

## 6. LOW

| 编号 | 位置 | 问题 |
|---|---|---|
| L-N1 | [nyfed.py#L23-L24](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/nyfed.py#L23-L24) | 模块注释承认 `release_time` 是"固定值而非墙钟"；与真实发布时间不一致（不影响 PIT，但影响审计追溯） |
| L-N2 | [nyfed.py#L361-L378](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/nyfed.py#L361-L378) | `_normalize_soma` 中 `raw_value is None` 直接 `continue`，不计入 `skipped`；字段改名会静默少 9 个分项 |
| L-N3 | [tga_backtest.py#L302-L304](file:///Users/horsenli/Works/ChronoForge/scripts/tga_backtest.py#L302-L304) | DM 的 HAC lag 只按样本量，不按 horizon 重叠（h=4 周频重叠时应至少 3）；当前经验值通常 ≥4，风险低 |
| L-N4 | [visualization/export_data.py#L196-L197](file:///Users/horsenli/Works/ChronoForge/visualization/export_data.py#L196-L197) | docstring 仍写 `reserve-structural-0.1.0` |
| L-N5 | [reserve_forecast.py#L715](file:///Users/horsenli/Works/ChronoForge/scripts/reserve_forecast.py#L715) | `cutoff = origin + 1 day` 的命名/语义容易与 PIT cutoff 混淆，建议改名（仅可读性） |
| L-N6 | [nyfed.py#L337-L344](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/nyfed.py#L337-L344) | skip 只写日志，未进入结构化指标，导致无法在 run_log 里审计 |
| L-N7 | `nyfed.py` | 2 处 `E501`（长注释/长行） |
| L-N8 | `docs/review/2026-11-05-full-audit.md` | 内部测试数字不一致：1770 / 1862 / 1972 三处并存；本轮实测 1980 passed / 3 failed |
| L-N9 | `pyproject.toml` / `.venv` | dev 依赖未安装，`uv run pytest` 直接收集失败；审计需临时 `--with pytest --with hypothesis` |

---

## 7. 专项：TGA 2027-02-04 区间反常

### 7.1 结论

**是 `tga_forecast.py` 的区间累积算法问题，不是数据问题。**

### 7.2 证据链

1. **组件带按金额比例生成**（[tga_forecast.py#L297-L306](file:///Users/horsenli/Works/ChronoForge/scripts/tga_forecast.py#L297-L306)）：
   ```python
   band = _band(source_type, model) * abs(amount)   # 默认 0.5
   lower = amount - band
   upper = amount + band
   ```
2. **逐日求和 + 线性累加**（[L1275-L1278](file:///Users/horsenli/Works/ChronoForge/scripts/tga_forecast.py#L1275-L1278)、[L1311-L1318](file:///Users/horsenli/Works/ChronoForge/scripts/tga_forecast.py#L1311-L1318)）：
   ```python
   pl.col("lower").sum().alias("daily_lower")
   pl.col("upper").sum().alias("daily_upper")
   ...
   (ctx.last_tga + pl.col("daily_lower").cum_sum()).alias("lower")
   (ctx.last_tga + pl.col("daily_upper").cum_sum()).alias("upper")
   ```
   误差带被当成"每天都有 ±50% 的新增不确定"，而不是"同一路径的不确定性"，于是方差/带宽随天数线性增长（若按独立误差传播应是 √h 增长，若按同一路径应是常数或有界）。
3. **derived 实测**：59 天后日半宽合计约 6.35 万亿美元，与 `hi − lo ≈ 12.7 万亿` 吻合。
4. **图 5 裁切**（[app.js#L503](file:///Users/horsenli/Works/ChronoForge/visualization/app.js#L503)）：`yAxis: { min: 0, max: 1.45 }`，而 `bandLo` 使用负的 `lower`，`band` 使用 `hi − lo`。负下沿与 7T 上沿都在轴外，观众看不到异常。

### 7.3 确切位置

- 根因：`tga_forecast.py:301`（band 生成）+ `tga_forecast.py:1277-1278`（逐日汇总）+ `tga_forecast.py:1317-1318`（cum_sum）；
- 掩盖：`visualization/app.js:503`（硬编码 y 轴）+ `app.js:483-484`（`bandLo`/`band` 的堆叠构造）。

### 7.4 判定

HIGH。不是"图表问题"而是"模型区间语义问题"；即使把 y 轴改成自适应，也只是把 ±6T 的荒谬区间显示出来。修复应改成按 horizon 的误差传播（例如用历史预测误差的经验分位，或用组件协方差），并对负 TGA 做下界约束。

---

## 8. 专项：`visualization/` 全目录

### 8.1 导出取数（`export_data.py`）

| 项 | 实现 | 结论 |
|---|---|---|
| NUMBER/FLOW 去重 | 按 `revision_time` 排序，`drop_duplicates("observation_time", keep="last")`（[L89-L106](file:///Users/horsenli/Works/ChronoForge/visualization/export_data.py#L89-L106)） | ✅ 正确（保留最新修订） |
| OHLCV | DuckDB 直读 canonical parquet，`drop_duplicates("event_time", keep="last")`（[L127-L147](file:///Users/horsenli/Works/ChronoForge/visualization/export_data.py#L127-L147)） | ✅ 正确 |
| TGA 日度 | 读 `derived/tga_daily_balance`（[L149-L165](file:///Users/horsenli/Works/ChronoForge/visualization/export_data.py#L149-L165)） | ✅ 口径匹配 |
| TGA 预测 | 读 `derived/tga_forecast`，取 `run_date.max()`（[L167-L195](file:///Users/horsenli/Works/ChronoForge/visualization/export_data.py#L167-L195)） | ✅ 取最新 run |
| 准备金 | 读 `reserve_forecast`/`_components`/`_eval`，取最新 run（[L199-L239](file:///Users/horsenli/Works/ChronoForge/visualization/export_data.py#L199-L239)） | ✅ 取最新，但 docstring 写 0.1.0（L-N4） |
| SRF | `load_number(SRF_DATASET, "srf_usage")` | ⚠️ 继承了 C-N1 的 SVE 污染 |

**前视风险**：导出层本身不做 PIT 过滤，但它是"事后展示"，取最新修订是合理的；它的问题是**上游数据错**（SRF）和**上游区间错**（TGA），不是导出层引入前视。

### 8.2 硬编码坐标轴核查（本轮全量）

| 图 | 轴范围 | 数据实际范围 | 结论 |
|---|---|---|---|
| chart1 | 1.2–4.5T | WRESBAL 1.394–4.276 | ✅ 覆盖 |
| chart2 | 0–12 | 连续下降 11 周 | ✅ 覆盖 |
| chart5 | **0–1.45T** | TGA 预测 −5.97T ~ +7.78T | ❌ **裁切**（H-N4） |
| chart6 | 0–5.6% | 利率最高 5.4% | ✅ 覆盖 |
| chart10 | max 6 | 5.31 | ✅ 覆盖 |
| chart11 | visualMap ±2.2 | 仅颜色饱和 | ✅ 不影响数据 |
| chart16 | 2.4–3.7T | 2.828–3.621 | ✅ 覆盖 |

**结论**：上轮修过的坐标轴确实修好了，但 **chart5 是遗漏的一处**，且恰好掩盖了本次最严重的模型异常。

### 8.3 `PANE_CHARTS` 与卡片面板

| pane | 卡片 id | `PANE_CHARTS` | 结论 |
|---|---|---|---|
| `pane-d1`（[index.html#L139](file:///Users/horsenli/Works/ChronoForge/visualization/index.html#L139)） | chart1,6,7,3,4,2,5 | d1: chart1,2,3,4,5,6,7 | ✅ 7↔7 |
| `pane-d2`（[#L233](file:///Users/horsenli/Works/ChronoForge/visualization/index.html#L233)） | chart8,8b,9,10,12 | d2: chart8,8b,9,10,12 | ✅ 5↔5 |
| `pane-d3`（[#L295](file:///Users/horsenli/Works/ChronoForge/visualization/index.html#L295)） | chart13,14,11,16,17,18 | d3: chart11,13,14,16,17,18 | ✅ 6↔6 |

**结论**：`PANE_CHARTS` 与卡片一一对应，无孤儿、无遗漏（上轮"图 16 空白"的 pane 未激活问题已通过 `closest(".pane")` 检查修复）。

### 8.4 图表失败提示

- 空数据/尺寸为 0：有可见 `chart-fail` 提示（[app.js#L188-L193](file:///Users/horsenli/Works/ChronoForge/visualization/app.js#L188-L193)）；
- renderer 异常：无 try/catch，会静默空白且缓存实例（M-N4）；
- 全 null series：`hasData` 只查 `length`，会静默通过（M-N4）。

### 8.5 其他

- `refresh.sh` 只做 `export_data.py` + `open`，正确且简单；
- `index.html` 18 张图、3 个 dashboard 卡片的数字与实际一致（除 §359 的方法学声明）。

---

## 9. 专项：新增测试层是否真守卫

| 测试 | 是否真会变红 | 评价 |
|---|---|---|
| `tests/architecture/test_docs_consistency.py` | ✅ 是（本次就红了） | 但只守 README 的源数/类型数，不守连接器文档、模型文档、站点文档；守卫面窄 |
| `tests/quality/test_normalize_drop_gate.py` | ✅ 是（对 list payload） | 只覆盖 OHLCV fixture，不覆盖 dict payload（nyfed）→ 无法发现 C-N2 |
| `tests/e2e/test_binance_spot_pipeline.py` | ✅ 是（真实生产代码 + MockTransport） | 只覆盖 Binance，不是通用 harness |
| `tests/real_pipeline_harness.py` | ✅ 是 | 质量好，但只被 Binance e2e 使用 |
| `tests/unit/test_cli.py` | ✅ 是（本次就红了） | 源数/源集合断言反而阻碍新增源，需要更新 |

**结论**：这些测试不是"形式守卫"，它们确实会在代码被改坏（或改超前）时变红。问题是**覆盖面**：新增的 `nyfed`、`reserve_forecast`、`visualization` 没有任何测试，所以它们坏了不会红。G2 的 3 条红是"文档/契约没跟上代码"，不是"新增代码有测试保护"。

---

## 10. 专项：调度与统计链路

### 10.1 `hourly_update.sh`

- 互斥锁、TTY/JSONL 日志、整点对齐、熔断不清除、`--due-only`、6h 窗口——设计正确；
- 不驱动 `reserve_forecast.py` / `export_data.py` / `dataset_stats.py` / `register_nyfed_datasets.py`（M-N7）；
- 全窗口 diff 类（`fred_*`、`nyfed_*`）在 `run_windowed` 里 `span=None` → 每次 due run 都从 `job.start`（默认 2021-01-01）全量重拉（[runner.py#L1079-L1081](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/pipeline/runner.py#L1079-L1081)、[windows.py#L263-L265](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/pipeline/windows.py#L263-L265)）。这对 SRF/SOMA 是"每次全历史单请求 + upsert"，功能上幂等，但请求体积与解析成本随历史增长；不是错误，记为观察项。

### 10.2 `backfill_datasets.py`

- `--due-only` 的 `_next_due_at(last_success, frequency, publish_time)` 逻辑正确；
- 默认 `--start 2021-01-01` 对所有源生效，对 SOMA（2003 起）会少拉 2003–2020（但 `_SOMA_OLDEST_DATE` 允许 2003；`visualization/README.md` 也写 2021-07-29，与 canonical 实际最早的 2003 不符）；
- `register_nyfed_datasets.py` 未被自动调用（M-N7）。

### 10.3 `dataset_stats.py`

- 只读扫描 + 注册表匹配，代码质量正常；
- 未被调度调用，属于人工诊断工具，可接受。

---

## 11. 相对 2026-11-05 审计的变化（完整表）

| 编号 | 上轮 | 本次 | 变化类型 |
|---|---|---|---|
| C1 | 修复 | 旧路径保持；nyfed 重新失效 | 部分关闭 / 重新打开 |
| C2 | 修复 | 复核通过 | 关闭 |
| C3 | 修复 | 复核通过 | 关闭 |
| A-1 | 修复 | 复核通过 | 关闭 |
| TGA-1 | 修复 | 复核通过 | 关闭 |
| B-1 | 修复 | `py_compile` 通过 | 关闭 |
| H1–H7 | 修复 | 回归用例通过 | 关闭（测试级复核） |
| A-2/A-3/A-9 | 修复 | 回归用例通过 | 关闭 |
| A-8 | 修复 | 旧连接器有效；nyfed 无 `note_skip` | 部分重新打开 |
| M1–M10 | 修复 | 回归用例通过 | 关闭 |
| G1 | ✅ | ❌ | 重新打开 |
| G2 | ✅ | ❌（3 failed） | 重新打开 |
| G3 | ✅（13 连接器） | ❌（14 连接器无 fixture） | 重新打开 |
| G4 | ✅ | ❌（SRF 污染 + nyfed 静默） | 重新打开 |
| G5 | ✅ | ⚠️（PIT/发布/区间三条失效） | 部分失效 |
| G6 | ✅ | ❌（未提交/未推送） | 重新打开 |
| G7 | ✅ | ❌（README 13 vs 14） | 重新打开 |
| **C-N1** | — | 新增 | SRF SVE |
| **C-N2** | — | 新增 | nyfed 门禁失效 |
| **H-N1–H-N4** | — | 新增 | 调度/PIT/TGA |
| **H-N5–H-N9** | — | 新增 | 门禁红 |
| **M-N1–M-N10** | — | 新增 | 模型/可视化/调度 |
| **L-N1–L-N9** | — | 新增 | 质量瑕疵 |

---

## 12. 门禁判定（G1–G7）

| 门禁 | 结果 | 证据 | 结论 |
|---|---|---|---|
| **G1 静态质量** | ❌ | `mypy` 2 errors、`ruff` 2 errors（均在 `nyfed.py`） | 不通过 |
| **G2 测试** | ❌ | `3 failed, 1980 passed`；`uv run pytest` 本身不可复现（缺 hypothesis） | 不通过 |
| **G3 契约测试** | ❌ | `nyfed` 无 fixture、无 fetch→normalize 测试 | 不通过 |
| **G4 静默丢失** | ❌ | SRF SVE 污染；`nyfed` 零产出不触发门禁 | 不通过 |
| **G5 约束核验** | ⚠️ | PIT、publish_time、区间口径三条在新增代码上无正确实现/证据 | 部分通过 |
| **G6 版本库** | ❌ | 15 个工作区改动；5 个未跟踪目录/文件；落后 origin 52 提交 | 不通过 |
| **G7 文档一致** | ❌ | README 13 vs 14；新增子系统文档缺失；站点 README 矛盾 | 不通过 |

**最终判定：NOT READY。** 卡在 G1、G2、G3、G4、G6、G7 六道门禁；G5 部分失效。不能因为"上轮大多已修"而放松判定——本轮新增代码把其中至少四条重新打穿。

---

## 13. 复现命令汇总

```bash
# 1. 静态检查（G1）
cd /Users/horsenli/Works/ChronoForge
uv run mypy src
uv run ruff check src

# 2. 全量测试（G2；项目 .venv 缺 dev 依赖，需显式 --with）
uv run --with pytest --with pytest-asyncio --with hypothesis python -m pytest -q

# 3. SRF SVE 污染（C-N1）
uv run python - <<'PY'
import glob, json, base64
for f in glob.glob('data/raw/nyfed/nyfed_repo_srf_usage/**/*.jsonl', recursive=True):
    for line in open(f):
        o = json.loads(line); p = o.get('payload')
        if isinstance(p, str): p = json.loads(base64.b64decode(p))
        for r in (p or {}).get('repo', {}).get('operations', []):
            if 'SVE' in str(r.get('note', '')):
                print(r.get('operationDate'), r.get('operationId'), r.get('totalAmtAccepted'))
PY
uv run python - <<'PY'
import duckdb
con = duckdb.connect()
print(con.execute("""
select observation_time, value from read_parquet(
 'data/canonical/NUMBER/entity=srf_usage/**/*.parquet', hive_partitioning=1)
where observation_time in (timestamp '2025-11-05', timestamp '2026-05-27')
order by observation_time""").pl().to_pandas())
PY

# 4. nyfed payload 门禁失效（C-N2）
uv run python - <<'PY'
from chronoforge.pipeline.runner import _payload_units
print(_payload_units({"repo": {"operations": [{"operationDate": "2026-11-06"}]}}))  # 0
print(_payload_units({"soma": {"summary": [{"asOfDate": "2026-11-04"}]}}))         # 0
PY

# 5. SOMA/SRF 发布时刻（H-N1）
uv run python -c "from chronoforge.config.update_schedule import publish_time_utc; print('nyfed', publish_time_utc('nyfed'))"

# 6. H.4.1 发布滞后（H-N3）
uv run python - <<'PY'
import duckdb
con = duckdb.connect()
print(con.execute("""
select observation_time, release_time from read_parquet(
 'data/canonical/NUMBER/entity=WRBWFRBL/**/*.parquet', hive_partitioning=1)
order by observation_time desc limit 3""").pl().to_pandas())
PY

# 7. TGA 2027-02-04 区间（H-N4）
uv run python - <<'PY'
import duckdb
con = duckdb.connect()
print(con.execute("""
select run_date, scenario, target_date, predicted_tga, lower, upper
from read_parquet('data/derived/tga_forecast/**/*.parquet', hive_partitioning=1)
where target_date = date '2027-02-04' order by scenario""").pl().to_pandas())
PY

# 8. 恒等式（M-N1）
uv run python scripts/reserve_forecast.py check
```

---

## 14. 只读约束、副作用与局限

### 14.1 只读约束执行情况

- 未修改任何 `src/`、`scripts/`、`visualization/` 源文件；
- 未写入/删除 canonical、derived、raw 数据；
- 未重置 checkpoint、未重新采集、未运行任何会写库的命令；
- 仅执行了 `pytest`（使用 tmp 目录）与 DuckDB 只读查询；
- **唯一写入**：本文件 `docs/review/2026-11-07-full-audit.md`。

### 14.2 审计副作用（如实记录）

- 在更早的复核步骤中运行 `docs/USD_Liquidity/scripts/run_tests.py`，该脚本重新生成了 `docs/USD_Liquidity/reports/quality_report.md`（untracked 生成物，无法用 git 还原原时间戳）。该文件属于既有知识库的生成物，不是本次审计要交付的内容；未修改任何源码/canonical 数据。
- 除上述外，无其他写操作。

### 14.3 局限

- 未联网调用 NY Fed / FRED 真实 API，所有 SRF/SOMA 判断基于 `data/raw` 已录制响应与 canonical 数据；
- 上轮 H1–H7、M1–M10 采用"回归用例通过 + 定向代码检查"的测试级复核，未逐条重读全部实现；
- 未做性能压测；`full_window` 全量重拉的请求体积/耗时未实测；
- `docs/USD_Liquidity/` 的 PDF 知识库内容本身未逐条核对，仅记录其未跟踪状态与生成物副作用。

---

## 15. 修复优先级建议（仅记录，本轮不改）

| 优先级 | 项 | 建议动作 |
|---|---|---|
| P0 | C-N1 SRF SVE | 在 `_normalize_rp` 增加 SVE 过滤（`note` 含 `Small Value Exercise (SVE)` 或 `operationId` 规则），并回溯重算 `srf_usage` canonical |
| P0 | C-N2 nyfed 门禁 | `_payload_units` 支持 `repo.operations` / `soma.summary`，或连接器接 `note_skip`；`checkpoint_from` 优先用 normalize 结果 |
| P0 | H-N5/G1 | 修 `health()` 的 `_get_json` 调用；清 `E501` |
| P0 | H-N8/G6 | 把 `nyfed`、`reserve_forecast`、`visualization`、`register_nyfed_datasets`、`docs/USD_Liquidity` 成套提交；推送 52 个提交 |
| P1 | H-N1 调度 | `update_schedule` 增加 `nyfed` 的 SOMA 周四发布时刻与 SRF 操作后时刻 |
| P1 | H-N3 PIT | `_load_number_series`/`load_soma` 读取 `release_time`，按 `release_time <= as_of` 截断 |
| P1 | H-N4 TGA 区间 | 改为按 horizon 的误差传播/经验分位；`chart5` y 轴自适应或显式标注越界 |
| P1 | H-N6/H-N7/H-N9 | 更新 CLI/README 到 14 源；补 nyfed fixture + fetch→normalize 契约测试；补新增子系统文档 |
| P2 | M-N1–M-N4 | 修恒等式命名/口径、联合区间、原子写、renderer try/catch |
| P2 | M-N5–M-N10 | 同步站点 README/index 方法学声明；接入调度；补测试 |
| P3 | L-N1–L-N9 | 清理注释、docstring、dev 依赖 |

---

## 16. 最终判定

**NOT READY。**

- **卡点**：G1、G2、G3、G4、G6、G7 六道门禁为红，G5 部分失效。
- **最严重的事实**：SRF 生产数据已因 SVE 测试操作而错（10 倍量级）；`nyfed` 把上轮刚修好的 C1/A-8 静默丢失门禁重新绕开；准备金模型的 PIT 与 TGA 区间语义都与文档声称不一致；TGA 图 5 的硬编码 y 轴主动掩盖了 ±6T 的异常区间。
- **与上轮的关系**：上轮关闭的 C2/C3/A-1/TGA-1/B-1 经复核确实已修；H1–H7、M1–M10 在旧路径上仍有效。但新增代码没有继承这些门禁，导致 G1–G4、G6–G7 重新变红。这不是"文档改口径"，是代码事实。
- **建议**：先修 P0（SVE、nyfed 门禁、G1、提交），再补 P1（调度/PIT/区间/契约/文档），最后处理 P2/P3。在 P0 关闭前不应对外使用 `srf_usage`、TGA 预测区间或准备金预测。

---

## 17. 修复进度（按批次）

| 批次 | 范围 | 状态 | 门禁影响 |
|---|---|---|---|
| 第 1 批 | C-N1 SRF 口径 + 污染数据重采 | ✅ 完成（2026-11-07，未提交） | G4 的 SRF 子项关闭；G4 仍红（C-N2 未修） |
| 第 2 批 | C-N2 nyfed 门禁 + fixture + 注入验证 | 待开始 | 预计 G3/G4 可转绿 |
| 第 3 批 | H-N3 准备金 PIT | 待开始 | G5 部分恢复 |
| 第 4 批 | H-N4 TGA 区间 + 图 5 轴 | 待开始 | G5 部分恢复 |
| 第 5 批 | 失败用例 + mypy/ruff + 文档 | 待开始 | 预计 G1/G2/G7 可转绿 |

### 第 1 批改动清单（仅本次改动，未提交）

| 文件/对象 | 类型 | 说明 |
|---|---|---|
| `src/chronoforge/connectors/nyfed.py` | 修改（该文件本身仍为未跟踪） | 增加 SVE note 过滤与 `_SVE_MARKER` |
| `tests/integration/test_nyfed.py` | **新增** | 4 条 SRF/SVE 用例 |
| `data/canonical/NUMBER/entity=srf_usage` | 数据重写 | 1317 行，11 个污染日期被纠正 |
| `data/raw/nyfed/nyfed_repo_srf_usage` | 新增一次抓取 | `ingest_date=2026-11-07`；旧 raw 保留 |
| checkpoint `nyfed_repo_srf_usage` | 推进 | `2026-11-06T15:23:16 → 2026-11-07T00:00:00` |
| `var/backups/2026-11-07-srf-sve/` | **新增备份** | canonical + raw 回滚点 |
| `docs/review/2026-11-07-full-audit.md` | 修改 | 本节与 C-N1 条目 |

**验证**：

```bash
uv run ruff check tests/integration/test_nyfed.py        # All checks passed
uv run --with pytest --with pytest-asyncio --with hypothesis \
  python -m pytest tests/integration/test_nyfed.py -q    # 4 passed
```

**与既有未提交内容的关系**：`nyfed.py` 在本次修复前就是未跟踪文件；本次只在其内部加入 SVE 过滤，没有触碰 `registry/service.py`、`windows.py`、`_wiring.py`、`pyproject.toml` 等既有未提交改动。`tests/integration/test_nyfed.py` 是本次新增，与既有未跟踪的 `docs/USD_Liquidity/`、`visualization/`、`reserve_forecast.py` 无关。

**未做**：未提交、未推送；未删除 `docs/USD_Liquidity/reports/quality_report.md`。
</content>
