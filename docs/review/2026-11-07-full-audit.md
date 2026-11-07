# ChronoForge 全面审计报告（代码 + 文档）

| 项目 | 内容 |
|---|---|
| 审计日期 | 2026-11-07 |
| 审计对象 | `/Users/horsenli/Works/ChronoForge`（Python 3.12，uv + polars + DuckDB + Parquet） |
| 基线报告 | `docs/review/2026-11-05-full-audit.md`（下称"上轮报告"） |
| 审计性质 | 只读审计；未修改源码、未改数据、未重置 checkpoint、未重新采集 |
| 唯一交付 | 本文件 |
| 最终判定 | **NOT READY**（审计时基线：G1/G2/G3/G4/G6/G7 六道门禁为红，G5 部分失效）；**当前修复进度见 §0.3 与 §17**（G3/G4 已转绿） |

> 本报告对基线报告的所有"已修复"结论做了独立复核。复核方式：回读源码 + 跑既有测试 + 只读查询 canonical/derived 数据。**凡报告与代码不一致处，以代码与数据为准。**

---

## 0. 结论速览

### 0.1 总体判定：NOT READY

上轮报告判定 READY（G1–G7 全 ✅）。本次复核结论不同：**上轮关闭的项大部分在旧代码路径上确实已修复，但本轮新增的第 14 个连接器（`nyfed`）与准备金模型、可视化站点把同一批不变量重新打穿了。** 这不是"文档改口径"的问题，而是新增代码没有接入既有门禁。

三个最关键的结论：

1. ~~**SRF 口径把美联储的 SVE 测试操作当成真实用量写入 canonical**（C-N1）。~~ **已修复（2026-11-07 第 1 批）**：按 `note` 排除 SVE，4 条测试先红后绿，重采 2021-07-29 至今，11 个被污染日期全部纠正（2025-11-05 → 3M，2026-05-27 → 11M）。
2. ~~**C1/A-8 门禁对 `nyfed` 完全失效**（C-N2）。~~ **已修复（2026-11-07 第 2 批，方案 B）**：删除 runner 的 `_payload_units` 形状猜测，改为连接器契约 `DataConnector.row_units(raw)`；未声明即拒绝执行（不降级为 0）。14 个连接器逐个声明行单位，含 deribit chart（`result.ticks`）、coinmarketcap（`data.quotes`）、yahoo（`chart.result[0].timestamp`）、sec_edgar（`filings.recent` 列式 / `facts` 四层嵌套）。nyfed 同时补齐 `note_skip`（合法/非法分离）与 `checkpoint_from` 对齐。注入验证覆盖全部连接器：取消声明 / 错误行数均触发 `QualityError` 且 checkpoint 不推进，合法空批不误报。
3. ~~**准备金模型的"PIT 截断"只截 observation_time，不截 release_time**（H-N3）。~~ **已修复（2026-11-07 第 3 批）**：`AsOfFrame` 按 `max(observation_time, release_time, revision_time)` 截断；origin 周三收盘只看到上周三观测，FRED `release_time` 与 SOMA `revision_time` 两条链路都参与判断；回测同步按该时点截断，naive 基准也只用最近可知值。模型升级为 `reserve-structural-0.1.2`，回测数字如实变差（同一 703 个 origin：h=1 全样本 MAE 56,089 → 79,597，+41.9%；h=2 +12.8%；h=4 +14.0%）；对 naive / seasonal naive 仍全部显著，currency 消融的显著 horizon 由 h=2 移到 h=1。**TGA 区间发散是算法性的**（H-N4），并被图 5 的硬编码 y 轴裁切掩盖。**H-N4 已于第 6 批修复**：区间改为按 horizon 的历史残差分位（P5–P95），下沿按 0 截断，图 5 轴放开为自动缩放；2027-02-04 区间由 −5.62T ~ +7.08T 收敛为 0.06T ~ 1.70T。

### 0.2 严重级别统计

| 级别 | 本次新增 | 已修复（第 1–6 批） | 现存 | 合计 |
|---|---|---|---|---|
| CRITICAL | 2（C-N1、C-N2） | 2（C-N1、C-N2） | **0** | 2 |
| HIGH | 9 条（H-N1–H-N9） | 7（H-N1、H-N2、H-N3、H-N4、H-N5、H-N6、H-N7） | **2（H-N8、H-N9）** | 9 |
| MEDIUM | 10 | 3（M-N8、M-N9、M-N10） | **7** | 10 |
| LOW | 9 | 4（L-N1、L-N2、L-N6、L-N7） | **5** | 9 |
| 合计 | — | 16 | **14 条** | 30 |

> 编号规则：`*-N*` 表示本轮新增（N = new）；`C1/A-1/H1/M1` 等沿用上轮编号。
> 修复状态以 §17「修复进度」为准（每批完成后更新）。

### 0.3 卡在哪几道门禁

| 门禁 | 结果 | 直接原因 |
|---|---|---|
| G1 静态质量 | ✅ 绿（第 5 批关闭） | `mypy src` 0 errors；`ruff check src scripts/backfill_datasets.py` 0 errors |
| G2 测试 | ✅ 绿（第 5 批关闭） | `2111 passed`（第 1–6 批新增用例全绿） |
| G3 契约测试 | ✅ 绿（第 2 批关闭） | nyfed 已补真实形状 fixture + fetch→normalize 全链路 + 负向用例 |
| G4 静默丢失 | ✅ 绿（第 2 批方案 B 关闭） | 行单位改为连接器声明，14 个连接器逐一覆盖；取消声明即拒绝执行；错误声明触发门禁；合法空批不误报 |
| G5 约束核验 | ⚠️ 部分恢复 | 准备金 PIT（H-N3，第 3 批）、发布时刻（H-N1/H-N2，第 4 批）、TGA 区间口径（H-N4，第 6 批：历史残差分位 + 下沿 0 截断）已修复；"联合 90%"标注（M-N2）仍开放 |
| G6 版本库 | ❌ 红 | 第 5 批已提交 `a9b9b95`；第 6 批改动未提交；`register_nyfed_datasets.py`/`visualization/`/`docs/USD_Liquidity/` 未跟踪；领先 origin 57 个提交 |
| G7 文档一致 | ❌ 红 | README 源数已修为 14；但新增连接器/模型/站点仍无设计文档，`visualization/README.md` 与站点仍互相矛盾（M-N5） |

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

> **本轮新增项的修复状态**：C-N1、C-N2（第 1–2 批）、H-N7（第 2 批）、H-N3（第 3 批）、H-N1/H-N2（第 4 批）、H-N5/H-N6（第 5 批）、H-N4（第 6 批）已修复；M-N8/M-N9/M-N10 与 L-N1/L-N2/L-N6/L-N7 随之关闭。H-N8（版本库）、H-N9（文档）与其余 M/L 仍开放（M-N2 与 H-N4 同源，仍未修）。详见 §17。

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

**门禁影响**（第 1 批当时）：G4 的"SRF 污染"子项关闭；C-N2 当时尚未修。**现状态**：G4 已随第 2 批方案 B 整体转绿（见 §17），G3 亦已转绿；G1/G2/G6/G7 仍红。

---

### C-N2. `nyfed` 零产出/逐行丢弃不触发门禁，checkpoint 照常推进 ✅ 已核实 → ✅ 已修复（2026-11-07，第 2 批）

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

**判定**：CRITICAL。它是所有 nyfed 解析缺陷的"静默放大器"，且直接违反 G4。**已于 2026-11-07 第 2 批修复，见下。**

#### 第 2 批修复记录（2026-11-07）

| 修复点 | 实现 | 证据 |
|---|---|---|
| 行单位判定（方案 B） | 删除 runner 的 `_payload_units` 形状猜测；改为连接器契约 `DataConnector.row_units(raw) -> int \| None`，runner 只消费声明。基类默认抛 `NotImplementedError`；runner 捕获后抛 `QualityError`（**拒绝执行，不降级为 0**——降级会把"契约缺失"伪装成"无数据"）；显式 `None` 仅用于单文档源；负值视为非法声明 | [base.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/base.py)、[runner.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/pipeline/runner.py) |
| 14 个连接器逐一声明 | `alternative`（`data`）、`binance_spot`/`binance_futures`/`ccxt`（顶层 list；单快照 dict=1）、`coinmarketcap`（`data.quotes`）、`coinmetrics`（`data`）、`deribit`（chart `result.ticks`；其它 `result`）、`fred`（`observations`）、`nyfed`（`repo.operations` / `soma.summary`）、`sec_edgar`（`filings.recent` 或分页顶层 `accessionNumber`；companyfacts 四层累计）、`sosovalue`（`data`）、`treasury_fiscal`（`data`）、`treasury_refunding`（显式 `None`）、`yahoo`（`chart.result[0].timestamp`） | 各连接器 `row_units`；[tests/unit/test_connector_row_units.py](file:///Users/horsenli/Works/ChronoForge/tests/unit/test_connector_row_units.py) |
| `note_skip` 合法/非法分离 | `sve_test_operation`、`non_overnight_operation` 进白名单；`malformed_row` / `bad_operation_date` / `missing_amount` / `non_numeric_amount` / `negative_amount` / `non_numeric_value` / `bad_as_of_date` 计为非法丢弃 | [nyfed.py#L78-L137](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/nyfed.py#L78-L137)、[normalize_stats.py#L31-L46](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/normalize_stats.py#L31-L46) |
| SOMA 缺分项 | 逐分项上报 `source_missing_value`（合法），不再静默 `continue` | 同上 `_classify_soma_row` |
| `checkpoint_from` 对齐产出 | 只取能产出记录的日期（SRF 排除 SVE/Term/畸形；SOMA 要求至少一个可解析分项），零产出返回 `None` | [nyfed.py#L509-L544](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/nyfed.py#L509-L544) |
| 真实形状 fixture | `tests/fixtures/nyfed/`：SRF 2025-11-05 三笔真实操作、纯 SVE 单行、混合畸形、SOMA 两个真实快照；来源与核对日期见 SOURCE.md | [SOURCE.md](file:///Users/horsenli/Works/ChronoForge/tests/fixtures/nyfed/SOURCE.md) |
| 全链路 + 负向用例 | `tests/integration/test_nyfed.py`：fetch（MockTransport，真实 fixture）→ normalize；note_skip 计数；checkpoint 契约 | 12 passed |
| 注入验证（全部连接器） | `tests/quality/test_row_units_gate.py`：15 个形状 × 3 模式 = 45 用例。取消声明 → `QualityError` + checkpoint 不推进；错误行数（声明 +2）→ `QualityError` + checkpoint 不推进；合法空批 → `SUCCESS` + cursor 推进 | 45 passed |

**先红后绿**：
- 修复前（`_payload_units` 旧实现 + 无 `note_skip`）：`tests/unit/test_payload_units.py`、`tests/integration/test_nyfed.py` 新增用例与 `tests/quality/test_nyfed_gate.py` 共 **9 failed / 22 passed**，失败点分别落在"嵌套形状返回 0"“无 note_skip 计数”“checkpoint_from 取 raw 最大日期”“静默丢弃不抛 QualityError（DID NOT RAISE）”。
- 方案 B 注入验证（全部连接器）：`tests/quality/test_row_units_gate.py` **45 passed**——取消声明与错误行数均 `QualityError` + `checkpoint_after IS NULL`，合法空批 `SUCCESS` + cursor 推进。
- 修复后：定向套件（quality + pipeline + 行单位契约）**152 passed**；全量 `3 failed, 2080 passed`（3 条失败与本批无关，见 §2.2）。

**门禁影响**：
- **G3 契约测试 → ✅ 绿**：nyfed 已有真实形状 fixture + fetch→normalize 全链路 + 负向用例。
- **G4 静默丢失 → ✅ 绿**：行单位改为连接器声明后，14 个连接器全部纳入门禁口径；未声明拒绝执行；错误声明触发门禁；合法空批不误报（见 §17 覆盖表）。
- G1/G2/G6/G7 不受本批影响。

**SVE 过滤复核（2026-11-07，方案 B 前）**：对全部已落盘 SRF raw 去重后的 1684 条操作复核——13 条 SVE 全部带 `note`，不存在"note 为空但实为 SVE"的行；1554 条空 note 行中，金额 0 与 < 5M 的为真实未用量/常规操作，`operationId` 尾号在 SVE 与真实行中混用，不能作判据。非空 note 中除 SVE 外还有"额度调整/结算差额"说明，均非测试操作。SOMA `summary` 结构只有 `asOfDate` + 持仓字段，无 note/测试标记，且全量文本字段无 test 字样。**结论：已核，无需改。**

**改动边界**：`base.py`（新增 `row_units` 契约默认）、`runner.py`（删除 `_payload_units`、新增 `_declared_row_units`）、14 个连接器（`row_units` 声明）、`normalize_stats.py`（白名单）、`nyfed.py`（分类/上报/checkpoint）、`tests/unit/test_connector_row_units.py`、`tests/quality/test_row_units_gate.py`、`tests/quality/test_nyfed_gate.py`、`tests/integration/test_pipeline.py`（`FakeConnector.row_units`）、`tests/integration/test_nyfed.py`、fixture 目录 + 本报告。删除了 `tests/unit/test_payload_units.py`（测试已删除的 `_payload_units`）。未改 `pyproject.toml`、`windows.py`、`service.py`、`_wiring.py` 等既有未提交内容；未动 `data/`、`meta/`、`var/`。

**未提交**：按用户要求，第 2 批改动先不提交，等确认。

---

## 4. HIGH

### H-N1. `nyfed` 没有发布时刻配置 → SOMA 恒定约 1 周滞后、SRF 约 1 天滞后 ✅ 已核实 → ✅ 已修复（2026-11-07 第 4 批）

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

**修复（第 4 批）**：
- `update_schedule` 改为两级发布时刻：新增 `DATASET_PUBLISH_TIME_UTC`，`publish_time_utc(source_id, dataset_id)` 先查 dataset 级、再查 source 级、最后落到默认 16:00。`nyfed` 源级回退 22:00；`nyfed_soma_holdings` 覆盖为 **周四 23:00 UTC**（北京周五 07:00），`nyfed_repo_srf_usage` 为 **操作日 22:00 UTC**（北京次日 06:00）。
- `_next_due_at(last, cadence, source_id, dataset_id)` 透传 dataset_id；长周期（≥2 天）新增"同日发布时刻尚未过 → 先对齐当天发布时刻"，避免上周四抓取后因发布时刻改为 23:00 而整周漏取。
- **各源实际触发时刻（UTC）**：SOMA 周四 23:00；SRF 每日 22:00；`nyfed` 未配置数据集 22:00；FRED 08:10；Binance 系列 00:00；Alternative/CoinMetrics/CoinMarketCap 00:10；Treasury DTS 21:00；Quarterly Refunding 15:15；其余默认 16:00。
- **默认 16:00 的后果**：SOMA 周四 16:00–23:00 之间（源尚未发布）会被判定到期，抓到上一周三的旧快照并推进 `last_success_time`，下一轮要再等 7 天 → 恒定滞后一周；SRF 16:00 抓取时当日操作尚未发生，直接写入占位 0。

**验证**（`tests/unit/test_due_selection.py::TestNyfedPublishAlignment` 8 条 + `tests/unit/test_update_schedule.py::TestDatasetPublishTime` 4 条）：

```text
# 修复前（stash 掉本批源码改动）
FAILED ...test_nyfed_source_fallback_is_2200_not_default_1600
  AssertionError: assert datetime.datetime(2026, 9, 29, 16, 0) == datetime.datetime(2026, 9, 29, 22, 0)
FAILED ...test_soma_same_day_before_release_waits_for_thursday_2300
  TypeError: _next_due_at() takes 3 positional arguments but 4 were given
FAILED ...test_soma_not_due_between_1600_and_2300
  AssertionError: assert [{'dataset_id': 'nyfed_soma_holdings', ...}] == []
FAILED ...test_srf_not_due_before_release_but_due_after
  AssertionError: assert [{'dataset_id': 'nyfed_repo_srf_usage', ...}] == []
ERROR tests/unit/test_update_schedule.py
  ImportError: cannot import name 'DATASET_PUBLISH_TIME_UTC'

# 修复后
158 passed（四文件定向：due_selection / windows / update_schedule / nyfed 集成）
```

---

### H-N2. SRF 在操作发生前把当日写为 0，并把 `release_time` 标为当日 22:00 ✅ 已核实 → ✅ 已修复（2026-11-07 第 4 批）

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

**修复（第 4 批）**：
- 连接器侧：`_srf_release_time(obs_date)` 固定为操作日 22:00 UTC（重拉幂等），`_utc_now()` 独立成函数便于冻结测试。`normalize` 对 `release_time > now` 的行按 `not_yet_published` 跳过——**不写占位 0**；`checkpoint_from` 同样只把已公布日期纳入 cursor，避免 cursor 越过尚未定稿的操作日。
- 白名单：`LEGIT_SKIP_REASONS` 增加 `not_yet_published`（冻结集合，锁定测试同步）。
- 窗口侧：`nyfed_` 前缀不再复用 `fred_series`（无回看），改为新配置 `nyfed_series`（1 天回看 + 单请求全窗口），与发布时刻截断**互补而非抵消**：回看只扩大请求窗口（`request.start.date()` 保证覆盖前一日操作日），是否入 canonical 仍由连接器 `release_time` 判定；若 cursor 已过操作日 00:00（次日凌晨补跑），无回看会漏掉前一日定稿值。
- **SRF 在 TGA/准备金模型的可见时刻**：`AsOfFrame` 取 `max(observation_time, release_time, revision_time)`。SRF 观测日 T、发布 T 22:00 → 在 origin（周三 23:59:59）可见 T 日及以前的定稿值；修复前 16:00 调度写入的占位 0 也带 `release_time=T 22:00`，22:00 一过就被当成"已发布 0"，模型在周三 origin 读到错误的当日零值（真实用量需等重拉）。

**验证**（`tests/integration/test_nyfed.py` 新增 4 条 + `tests/unit/test_windows.py` 新增 2 条）：

```text
# 修复前（stash 掉本批源码改动）
FAILED test_srf_before_release_skips_unpublished_day
  AttributeError: 'module' object at chronoforge.connectors.nyfed has no attribute '_utc_now'
FAILED test_srf_before_release_checkpoint_stops_at_previous_day
  AttributeError: ...
FAILED test_srf_placeholder_zero_is_never_published
  AttributeError: ...
FAILED test_incremental_mode_creates_chunks[nyfed_series-86400-full_window]
  ValueError: unknown dataset_id: 'nyfed_series'
FAILED test_nyfed_full_window_with_one_day_lookback
  ValueError: unknown dataset_id: 'nyfed_repo_srf_usage'
（共 15 failed / 38 passed，含 due_selection 的 8 条）

# 修复后
158 passed（due_selection / windows / update_schedule / nyfed 集成 + 白名单锁）
```

> 修复后实测的可见时刻：冻结墙钟 2025-11-05 15:23 UTC 时，当日行全部跳过（含占位 0），只输出 2025-11-04（`release_time=2025-11-04 22:00`），`checkpoint_from` 停在 `2025-11-04`；冻结到 22:30 后当日行可见、`release_time` 仍为 22:00。

---

### H-N3. 准备金模型 PIT 只截 `observation_time`，origin 周三使用周四才发布的 H.4.1/SOMA ✅ 已核实 → ✅ 已修复（2026-11-07 第 3 批）

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

**修复（第 3 批）**：

- `scripts/reserve_forecast.py` 引入 `AsOfFrame`（`values` + `available`）。每个观测的可得时刻 = `max(observation_time, release_time, revision_time)`；FRED 的 `release_time` 与 SOMA 的 `revision_time` 两条链路都参与截断。
- `AsOfFrame.history(as_of)` 是唯一取数入口：把 `available > as_of` 的单元格置空后前向填充，得到"该时点最近已知值"；预测与回测都只走这条路径。
- origin 定义为周三收盘（`_asof_cutoff = 周三 23:59:59`）。H.4.1/SOMA 的当周观测在周四 23:59:59 才可得，因此被排除；`naive` 基准同样只能用上周三的可知值。
- 缺口周补齐：anchor（上周三）到 origin（本周三）之间漏掉的一周，也按季节中位变化补齐，避免预测水平系统性缺失一周季节项。
- 模型版本 `reserve-structural-0.1.1 → 0.1.2`；`config_hash` 显式记录 `pit.truncate_by=[release_time, revision_time]`。
- **不做任何归档回退**：ALFRED 对 H.4.1 的 vintage 归档起点是 2013-04-18，早于此的观测是批量回填，无法重建首次发布值；严格 PIT 下回测 origin 只能从 2013-04-18 之后开始（`_VINTAGE_ARCHIVE_FLOOR`）。曾尝试"滞后 >14 天按发布节奏回退"，但 `WCICL` 有 645 行真实的批量晚修订（如 2013-07-18），回退会偷看修订，MAE 反而从 60,891 涨到 85,995/101,322，已删除。
- **TCCF 单项**：`H41RESH4ENWW` 的 vintage 归档从 2020-05-14 才开始，严格截断会让 2013–2020 的 origin 因 `other_liabilities` 不可得而整行失效。该科目 2002–2019 实测恒为 0，故对取值为 0 的行按"观测日即已知 0"处理（常数假设，不引入未来 vintage），回测样本恢复到 703 个 origin。
- **仅此一处需要修**：`tga_forecast.py` / `tga_backtest.py` 已按 `release_time` 做 cutoff；`tga_reconstruct.py` 是历史重建、`plot_*` / `visualization/export_data.py` 是展示，均无 origin 截断语义。SOMA 不参与建模，只在 `check` 交叉校验，但其 `revision_time` 已进入 `AsOfFrame` 并在 `check` 输出"该时点最近可知观测"（`soma_latest_known_observation`）。
- `benchmark_summary` 的所有对照与消融改为在同一批有效预测行上比较（此前 model 与 naive 可能落在不同子样本上）。

**修复前 vs 修复后（USD 百万；同一 origin 子样本，全样本 n=703，近 5 年 n=258/259/261）**

"修复前"按旧口径在同一批 origin 上复算：`hist = df[df.index <= origin]`（只用观测日截断，origin 当周值直接进入），naive = origin 当周值。旧 0.1.1 原生全样本（n=1138，2003 起）作为参考。

| h | 口径 | 修复前 MAE / RMSE | 修复后 MAE / RMSE | 变化 |
|---|---|---|---:|---:|
| 1 | 近5年 | 59,561.8 / 83,654.7 | 81,024.2 / 110,141.8 | +36.0% / +31.7% |
| 1 | 全样本 | 56,089.4 / 82,205.8 | 79,596.9 / 113,520.8 | +41.9% / +38.1% |
| 2 | 近5年 | 80,842.0 / 109,949.1 | 92,083.3 / 123,177.6 | +13.9% / +12.0% |
| 2 | 全样本 | 79,719.8 / 113,581.3 | 89,894.9 / 132,533.5 | +12.8% / +16.7% |
| 4 | 近5年 | 95,533.0 / 126,851.8 | 106,391.9 / 141,948.2 | +11.4% / +11.9% |
| 4 | 全样本 | 98,397.3 / 149,106.3 | 112,138.2 / 171,555.0 | +14.0% / +15.1% |

参考（旧 0.1.1 原生全样本 n=1138）：h=1 `44,289.6 / 68,756.4`；h=2 `60,925.1 / 94,351.9`；h=4 `76,745.9 / 125,221.8`。两端样本不同，不与上表直接相减。

- **H-N3 的代价主要在 h=1**：修复后 origin 只能锚定上周三，target 距锚点 2 周，而修复前实际只隔 1 周。h=1 全样本 MAE +41.9%，h=2 +12.8%，h=4 +14.0%。
- 对 **naive** 仍全部显著（Bonferroni m=3）：h=1 `stat=-6.621, p_bonf=0.0`；h=2 `-5.722, 0.0`；h=4 `-3.987, 0.0003`。修复前为 `-7.889 / -6.650 / -4.402`，显著性结论不变，但优势幅度收窄。
- 对 **seasonal naive** 仍全部显著：`-9.468 / -9.352 / -9.067`，`p_bonf=0.0`（修复前 `-9.940 / -9.447 / -9.277`）。
- 对 **0.1.0（currency 消融）**：h=1 由不显著转为显著（`p_bonf=0.2958 → 0.0288`）；h=2 由显著转为不显著（`0.0342 → 0.4347`）；h=4 仍不显著（`0.4746 → 0.486`）。**结论变化**：currency 季节项"去掉更好"的统计支撑从 h=2 移到 h=1，h=2 不再能断言，h=4 一直不能断言。
- **TGA 增量**（naive MAE − 模型 MAE，近5年/全样本）：h=1 `5,880.3 → 9,216.7 / 3,815.7 → 5,750.1`；h=2 `9,253.3 → 10,013.5 / 5,787.2 → 6,277.9`；h=4 `9,704.8 → 10,439.5 / 5,921.0 → 6,131.5`。TGA 季节项仍是唯一有效增量，PIT 后增量更大。
- **currency 增量**（0.1.0 − 0.1.2 MAE）：h=1 `142.0 → 373.1 / 164.4 → 485.7`；h=2 `376.0 → 372.5 / 478.6 → 374.5`；h=4 `781.7 → 938.5 / 487.2 → 560.9`。h=2 的增量小幅下降，与 DM 由显著转不显著一致。
- 误差带同步变宽：h=1 近5年 `[-118,767, +107,937] → [-170,649, +137,839]`，偏差 `-2,950.1 → -5,903.6`（PIT 后模型更偏低，因为锚定在上周三、而准备金近期上行）。

**预测点（origin 2026-11-04，周三收盘）**：

| h | target | 修复前 | 修复后 | 差异 |
|---|---|---|---:|---:|
| 1 | 2026-11-11 | 2,962,804 | 2,902,147 | -60,657 |
| 2 | 2026-11-18 | 2,963,425 | 2,902,768 | -60,657 |
| 4 | 2026-12-02 | 2,963,749 | 2,903,092 | -60,657 |

修复前的 `origin_reserve` 是周三当周值 2,962,804（周四才发布，属前视）；修复后为 2026-10-28 的 2,895,236。差异 = 锚点水平 -67,568 + TGA 季节项 +6,911。**可视化**：`visualization/export_data.py` 已重新导出（`data.js`，`model_version=0.1.2`），基准文案不再写死 `0.1.1`。图 16 由 `data.js` 驱动、重新导出即刷新；同时把扇图起点改为 origin 之前最近可知的观测（2026-10-28，2,895,236），不再用周三当周已发布值作锚点，从而消除 -60.7B 的伪跳变。实际值曲线仍保留 2026-11-04 的 2,962,804（周四发布后的事实），它与扇图起点之间有约 67.6B 的缺口，这是发布时滞的真实体现。

**验证**：新增 `tests/unit/test_reserve_forecast_pit.py` 6 条（当周排除、污染不生效、revision 延后排除、回测锚点/target、release+revision 取最大、晚修订不做归档回退）；全量 `3 failed, 2086 passed`（3 条仍为第 5 批文档/CLI 范围）；`ruff check scripts/reserve_forecast.py tests/unit/test_reserve_forecast_pit.py` 通过。

---

### H-N4. TGA 区间在 2027-02-04 发散：算法性线性放大，图 5 硬编码 y 轴裁切掩盖 ✅ 已核实 → ✅ 已修复（2026-11-07 第 6 批）

**位置（修复前）**：
- [tga_forecast.py](file:///Users/horsenli/Works/ChronoForge/scripts/tga_forecast.py) `_component_row`（`band = _band(source_type, model) * abs(amount)`）
- `aggregate_forecast`（`daily_lower`/`daily_upper` 求和 + `cum_sum`）
- [app.js#L504](file:///Users/horsenli/Works/ChronoForge/visualization/app.js#L504)（修复前为 `min: 0, max: 1.45`）

**算法（修复前）**：

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

#### 根因定位（用实际数据分层验证，非仅读代码）

对 `data/derived/tga_forecast_components`（`run_date=2026-11-06/scenario=base`，59 个预测日）逐层复算：

| 层 | 量 | 实测值 |
|---|---|---|
| ① 组件带假设 | 59 天内 `Σ(upper−lower)/2`（线性累加后的带宽） | **6.354T** |
| ② 独立误差传播 | `sqrt(Σ((upper−lower)/2)²)`（RSS） | **0.453T** |
| ③ 差异倍数 | ① / ② | **14.0×** |
| ④ 50% 带本身的覆盖率 | RSS 带宽对历史误差的实际覆盖（h7/14/30/60/90） | **74.5% / 68.1% / 50.0% / 48.6% / 45.7%** |

结论：**两者叠加，但主因明确是 ②→① 的累加方式。**

- **主因（贡献 14× 放大）**：`cum_sum` 把"每日新增的独立误差"当成"完全相关的同向误差"线性相加。误差应按方差叠加（√h 量级），线性相加使带宽随天数线性膨胀。这是**统计问题**——即使组件带数值完全正确，线性累加也会发散。
- **次因**：`bands` 里的 50% / 100% 是按金额拍的比例，并非任何置信水平的分位；把它当作 90% 区间本身就不成立。RSS 带宽（0.453T）在历史误差上的覆盖只有 45.7%–74.5%，远不到 90%。这是**口径问题**。
- 因此不是数据问题，也不是渲染问题：`predicted_tga` 本身正常，只有 `lower`/`upper` 的语义错了。

#### 两类错误分开修

| 错误性质 | 表现 | 修复 |
|---|---|---|
| **(a) 区间宽度算错（统计）** | 带宽随 h 线性增长；50% 比例带不对应 90% 分位 | 改为**按 horizon 的历史残差分位**（P5–P95）。`eval_row` 中 `error = predicted − actual`，故 actual 的 90% 区间 = `[predicted − q95(error), predicted + (−q05(error))]`。校准值写入 `config/tga/model.yaml: interval_bands`，每档 n=276–376 |
| **(b) 出现物理不可能值（口径）** | `lower` 为 −5.97T，TGA 余额不可能为负 | 聚合时下沿按 `max(0.0, lower)` 截断。这是**物理约束**，不是统计分位；`upper` 不做对称截断。同时在 `model.yaml` 与 docstring 中写明这层区别 |

兜底（`model` 未配置 `interval_bands` 时）：按**独立误差方差传播** `sqrt(cum_sum(daily_var))`，不再退回旧线性累加。

**代码落点（修复后）**：
- [tga_forecast.py#L293-L301](file:///Users/horsenli/Works/ChronoForge/scripts/tga_forecast.py#L293-L301)：`_interval_offsets`（horizon → 偏移）
- [tga_forecast.py#L1320-L1335](file:///Users/horsenli/Works/ChronoForge/scripts/tga_forecast.py#L1320-L1335)：日方差 → `_fallback_half`
- [tga_forecast.py#L1341-L1358](file:///Users/horsenli/Works/ChronoForge/scripts/tga_forecast.py#L1341-L1358)：按 horizon 取偏移 + `max(0.0, lower)`
- [model.yaml](file:///Users/horsenli/Works/ChronoForge/config/tga/model.yaml)：`interval_bands`（7/14/30/60/90）+ 口径注释；`bands` 降级为"仅组件行展示用"
- [app.js#L504](file:///Users/horsenli/Works/ChronoForge/visualization/app.js#L504)：`yVal({ name: "万亿美元" })`，轴由数据自动缩放
- `MODEL_VERSION` `tga-forecast-0.4.0 → 0.5.0`（`find_success_run`/`create_run` 以 model_version 区分，阻断旧 run 复用；旧分区保留不覆盖）

#### 修复前后实测区间（origin 2026-11-06，base 情景，单位：万亿美元）

| horizon | target | predicted | 修复前 lo / hi | 修复后 lo / hi | 修复后宽度 |
|---|---|---|---|---|---|
| 7 | 2026-11-13 | 0.847 | 0.479 / 1.215 | **0.730 / 1.041** | 0.311 |
| 14 | 2026-11-20 | 0.667 | −0.137 / 1.471 | **0.524 / 1.038** | 0.514 |
| 30 | 2026-12-04 | 0.839 | −0.867 / 2.545 | **0.628 / 1.366** | 0.739 |
| 60 | 2027-01-05 | 0.996 | −3.016 / 5.009 | **0.388 / 1.761** | 1.373 |
| 90 | 2027-02-04 | 0.731 | **−5.623 / 7.085** | **0.056 / 1.699** | 1.643 |

**点预测一字未动**（0.847 / 0.667 / 0.839 / 0.996 / 0.731），变化只在区间。远端 2027-02-04 的 −5.97T 已消失，下沿 0.056T（未触发 0 截断，说明校准分位本身已落在正区）。

#### 对既有 backtest 结论与页面文案的影响（先评估后动手）

- **backtest 点预测指标不变**：MAE / RMSE / MAPE / Direction / Bias 只依赖 `predicted_tga`，本批未改预测路径，数值保持原样。
- **Coverage 会变**：旧口径带宽发散到 ±6T，覆盖恒为 100%，这个指标此前没有信息量；新口径下应接近 90%。但**校准样本与评估样本是同一批 walk-forward 残差**，因此该 90% 属**样本内**结果，不能当作独立 OOS 验证。`interval_bands` 的注释已写明"校准样本更新后需重跑 tga_backtest 并重算本表"。
- **Pinball loss 会变**：它直接取决于带宽，本批**未重跑** `tga_backtest`，因此不提供新 pinball 数字；不把"改了区间"说成"提升了 pinball"。
- **页面文案**：图 05 的"90% 区间"从"±50% 金额线性带"变为"历史残差 P5–P95"；轴放开后，若区间超出旧经验范围会真实显示，不再被 `0–1.45` 裁掉。M-N2（把逐 horizon 边际分位标成"联合 90%"）**本批未动**，仍是开放项。

#### 验证

```bash
uv run --with pytest --with pytest-asyncio --with hypothesis python -m pytest -q
# 2111 passed, 10 warnings in 90.40s（新增 2 条区间用例）
uv run ruff check src scripts/tga_forecast.py tests/unit/test_tga_forecast.py
# All checks passed!
uv run mypy src
# Success: no issues found in 86 source files
```

新增用例：
- `test_forecast_interval_uses_horizon_residual_offsets`：h7 边界严格等于 `predicted + 偏移`；h30 远端 `lower == 0`（截断生效）。
- `test_forecast_interval_fallback_propagates_independently`：无 `interval_bands` 时两天各 ±100，第二天半宽为 141.42（RSS），不是 200（线性）。

**本批未提交**（等待确认）。

---

### H-N5. G1 红：`mypy` 与 `ruff` 均在 `nyfed.py` 报错 ✅ 已核实 → ✅ 已修复（2026-11-07 第 5 批）

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

`ruff` 另有 1 个 `E501`（模块 docstring 第 6 行 URL；第 5 批处理）。

**含义**：`health()` 一旦被调用（CLI 健康检查/探活）会抛 `TypeError`，而不是返回 `HealthStatus(ok=False)`——`except Exception` 会捕获，但仍会把"代码 bug"伪装成"上游不可用"。同时 G1 门禁直接为红。

**判定**：HIGH（同时是门禁失败与功能缺陷）。

**修复（第 5 批）**：`health()` 的探活语义是"拉取最近一个月的 SRF 回购结果"，因此补上 `_ENDPOINT` 并保留原 params（不是伪造）。docstring 第 6 行长 URL 折成两行；`backfill_datasets.py:111` 的 help f-string 拆成隐式拼接。

```text
# 修复前
$ uv run ruff check src scripts/backfill_datasets.py
src/chronoforge/connectors/nyfed.py:6:96: E501 Line too long (103 > 100)
scripts/backfill_datasets.py:111:101: E501 Line too long (114 > 100)
Found 2 errors.

$ uv run mypy src
src/chronoforge/connectors/nyfed.py:213: error: Missing positional argument "params" in call to "_get_json" of "NYFedConnector"  [call-arg]
src/chronoforge/connectors/nyfed.py:214: error: Argument 1 to "_get_json" of "NYFedConnector" has incompatible type "dict[str, str]"; expected "str"  [arg-type]
Found 2 errors in 1 file (checked 86 source files)

# 修复后
$ uv run ruff check src scripts/backfill_datasets.py    # All checks passed!
$ uv run mypy src                                        # Success: no issues found in 86 source files
```

---

### H-N6. G2 红：3 条测试失败，新增源未同步测试 ✅ 已核实 → ✅ 已修复（2026-11-07 第 5 批）

见 §2.2。`test_cli.py` 的 `13 sources` 与源集合断言、`test_docs_consistency.py` 的 README 源数断言全部因新增 `nyfed` 而失败。

**修复（第 5 批）**：以代码事实（`DEFAULT_SOURCES` 14 个，含 `nyfed`）为准改文档与断言——README 的 `bootstrapped: 14 sources` / `14 pre-configured sources` 并补 `nyfed` 表格行；`test_cli.py` 改为 `14 sources`、`COUNT(*)=14`、源集合加 `nyfed`。测试断言是"锁定事实"，不是迁就文档。

```text
# 修复前
FAILED tests/architecture/test_docs_consistency.py::test_readme_source_count_matches_default_sources
  AssertionError: assert 'bootstrapped: 14 sources' in '# ChronoForge ...'
FAILED tests/unit/test_cli.py::TestRegistryCommands::test_sync_bootstraps_all_sources_idempotent
  AssertionError: assert '13 sources' in 'source_registry bootstrapped: 14 sources\n'
FAILED tests/unit/test_cli.py::TestRegistryCommands::test_list_sources_json
  AssertionError: assert {...} == {...}
  Extra items in the left set:
  'nyfed'

# 修复后
4 passed（含 canonical type 数量守卫）
```

**判定**：HIGH（门禁失败；也说明新增源没有走"加源即改契约"的流程）。

---

### H-N7. G3 重新打开：`nyfed` 无 fixture、无 fetch→normalize 全链路测试 ✅ 已核实 → ✅ 已修复（第 2 批）

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

### H-N9. G7 红：README 源数与新增子系统文档缺失 ✅ 已核实 → ⚠️ 部分修复（第 5 批：源数已修，设计文档仍缺）

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

### M-N8. `nyfed` 的 `checkpoint_from` 以 raw 最大日期推进，即使 normalize 全部过滤 ✅ 已核实 → ✅ 已修复（第 2 批，第 4 批补齐发布时间过滤）

**位置**：[nyfed.py#L451-L465](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/nyfed.py#L451-L465)

与 C-N2 同源，但这里是独立的可观测性问题：即使连接器未来补上 `note_skip`，`checkpoint_from` 仍会在"raw 有行、normalize 返回空"时推进。正确做法是优先使用 normalize 后的观测日期，raw 只作为回退。

**判定**：MEDIUM（与 C-N2 组合升级为 CRITICAL）。

### M-N9. `_load_number_series` / `load_soma` 丢弃 `release_time`/`revision_time` ✅ 已核实 → ✅ 已修复（第 3 批）

**位置**：[reserve_forecast.py#L118-L128](file:///Users/horsenli/Works/ChronoForge/scripts/reserve_forecast.py#L118-L128)、[L142-L156](file:///Users/horsenli/Works/ChronoForge/scripts/reserve_forecast.py#L142-L156)

```python
result = service.query(dataset_id=..., filters=...)
df = result.frame.select(["observation_time", "value"])
```

模型侧拿不到 `release_time`，因此**即使想修 PIT 也无从下手**。这是 H-N3 的结构性原因，也会让后续修订感知（revision）无法实现。

**判定**：MEDIUM。

### M-N10. `reserve_forecast.py` 文档/标注与 0.1.1 实现不符 ✅ 已核实 → ✅ 已修复（第 3 批，升级 0.1.2 并同步文档）

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
| L-N1 | [nyfed.py#L23-L24](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/nyfed.py#L23-L24) | 模块注释承认 `release_time` 是"固定值而非墙钟"；与真实发布时间不一致（不影响 PIT，但影响审计追溯）— ✅ 第 4 批已补"固定值保证重拉幂等 + 未公布行跳过"的语义说明 |
| L-N2 | [nyfed.py#L361-L378](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/nyfed.py#L361-L378) | `_normalize_soma` 中 `raw_value is None` 直接 `continue`，不计入 `skipped`；字段改名会静默少 9 个分项 — ✅ 第 2 批改为 `source_missing_value` 结构化计数 |
| L-N3 | [tga_backtest.py#L302-L304](file:///Users/horsenli/Works/ChronoForge/scripts/tga_backtest.py#L302-L304) | DM 的 HAC lag 只按样本量，不按 horizon 重叠（h=4 周频重叠时应至少 3）；当前经验值通常 ≥4，风险低 |
| L-N4 | [visualization/export_data.py#L196-L197](file:///Users/horsenli/Works/ChronoForge/visualization/export_data.py#L196-L197) | docstring 仍写 `reserve-structural-0.1.0`（现为 0.1.2） |
| L-N5 | [reserve_forecast.py#L715](file:///Users/horsenli/Works/ChronoForge/scripts/reserve_forecast.py#L715) | `cutoff = origin + 1 day` 的命名/语义容易与 PIT cutoff 混淆，建议改名（仅可读性） |
| L-N6 | [nyfed.py#L337-L344](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/nyfed.py#L337-L344) | skip 只写日志，未进入结构化指标，导致无法在 run_log 里审计 — ✅ 第 2 批已改为 `note_skip` 结构化计数 |
| L-N7 | `nyfed.py` | 2 处 `E501`（长注释/长行）— ✅ 第 5 批已修（含 `backfill_datasets.py:111`） |
| L-N8 | `docs/review/2026-11-05-full-audit.md` | 内部测试数字不一致：1770 / 1862 / 1972 三处并存；本轮实测 1980 passed / 3 failed |
| L-N9 | `pyproject.toml` / `.venv` | dev 依赖未安装，`uv run pytest` 直接收集失败；审计需临时 `--with pytest --with hypothesis` |

---

## 7. 专项：TGA 2027-02-04 区间反常

### 7.1 结论

**是 `tga_forecast.py` 的区间累积算法问题，不是数据问题。** 已于第 6 批修复，详见 H-N4 的"根因定位 / 两类错误分开修 / 修复前后实测"。

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

### 7.5 修复结果（第 6 批）

1. **统计口径**：区间改由 `config/tga/model.yaml: interval_bands` 提供，按 horizon 的历史残差 P5–P95 生成，不再用组件金额比例带累加；
2. **物理约束**：聚合时 `lower = max(0.0, lower)`，上沿不动；
3. **兜底路径**：缺 `interval_bands` 时按方差传播 `sqrt(cum_sum(daily_var))`，不是线性累加；
4. **可视化**：`chart5` 去掉 `min: 0, max: 1.45`，轴自适应。

修复后 2027-02-04（origin 2026-11-06）区间为 **0.056T ~ 1.699T**；修复前为 −5.623T ~ 7.085T。

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
| chart5 | ~~0–1.45T~~ → 自动缩放（第 6 批） | TGA 预测 0.056–1.699T（修复后） | ✅ 已放开（H-N4） |
| chart6 | 0–5.6% | 利率最高 5.4% | ✅ 覆盖 |
| chart10 | max 6 | 5.31 | ✅ 覆盖 |
| chart11 | visualMap ±2.2 | 仅颜色饱和 | ✅ 不影响数据 |
| chart16 | 2.4–3.7T | 2.828–3.621 | ✅ 覆盖 |

**结论**：上轮修过的坐标轴确实修好了；**chart5 是遗漏的一处**，且恰好掩盖了本次最严重的模型异常。第 6 批已去掉 `min/max` 并同时修复区间口径（H-N4），轴不再需要人工兜底。

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

### 12.1 当前状态（2026-11-07 第 6 批后，实测）

| 门禁 | 结果 | 证据 | 结论 |
|---|---|---|---|
| **G1 静态质量** | ✅ | `uv run mypy src` → `Success: no issues found in 86 source files`；`uv run ruff check src scripts/backfill_datasets.py` → `All checks passed!` | 通过 |
| **G2 测试** | ✅ | `uv run --with pytest --with pytest-asyncio --with hypothesis python -m pytest -q` → `2111 passed` | 通过 |
| **G3 契约测试** | ✅ | nyfed 真实形状 fixture + fetch→normalize 全链路 + 负向用例（第 2 批） | 通过 |
| **G4 静默丢失** | ✅ | 行单位连接器声明（方案 B）+ 注入门禁（第 2 批） | 通过 |
| **G5 约束核验** | ⚠️ | PIT（H-N3）、发布时刻（H-N1/H-N2）、TGA 区间口径（H-N4）已修；"联合 90%"标注（M-N2）仍未修 | 部分通过 |
| **G6 版本库** | ❌ | 第 5 批已提交 `a9b9b95`；第 6 批改动未提交；`register_nyfed_datasets.py`/`visualization/`/`docs/USD_Liquidity/` 未跟踪；领先 origin 57 提交 | 不通过 |
| **G7 文档一致** | ❌ | README 源数已对齐 14；新增连接器/模型/站点仍无设计文档；`visualization/README.md` 与站点互相矛盾（M-N5） | 不通过 |

**当前判定：NOT READY。** 卡在 G6（版本库未收敛）与 G7（文档缺口），G5 的区间口径已修复但"联合 90%"标注仍不实（M-N2）。G1/G2 已转绿，不能据此放宽。

### 12.2 审计基线（2026-11-07 修复前，留档）

| 门禁 | 结果 | 证据 | 结论 |
|---|---|---|---|
| **G1 静态质量** | ❌ | `mypy` 2 errors、`ruff` 2 errors（均在 `nyfed.py`） | 不通过 |
| **G2 测试** | ❌ | `3 failed, 1980 passed`；`uv run pytest` 本身不可复现（缺 hypothesis） | 不通过 |
| **G3 契约测试** | ❌ | `nyfed` 无 fixture、无 fetch→normalize 测试 | 不通过 |
| **G4 静默丢失** | ❌ | SRF SVE 污染；`nyfed` 零产出不触发门禁 | 不通过 |
| **G5 约束核验** | ⚠️ | PIT、publish_time、区间口径三条在新增代码上无正确实现/证据 | 部分通过 |
| **G6 版本库** | ❌ | 15 个工作区改动；5 个未跟踪目录/文件；落后 origin 52 提交 | 不通过 |
| **G7 文档一致** | ❌ | README 13 vs 14；新增子系统文档缺失；站点 README 矛盾 | 不通过 |

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

# 4. 行单位契约（C-N2 已修；方案 B 注入验证）
uv run --with pytest --with pytest-asyncio --with hypothesis \
  python -m pytest tests/unit/test_connector_row_units.py tests/quality/test_row_units_gate.py -q
# 期望：41 + 45 passed（取消声明/错误行数 → QualityError 且 checkpoint 不推进；空批 → SUCCESS）
uv run python -c "from chronoforge.connectors.base import DataConnector; DataConnector.row_units(object(), None)"

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
| P1 | H-N4 TGA 区间 | ✅ 第 6 批已做：按 horizon 历史残差分位 + 下沿 0 截断；`chart5` 轴自适应 |
| P1 | H-N6/H-N7/H-N9 | 更新 CLI/README 到 14 源；补 nyfed fixture + fetch→normalize 契约测试；补新增子系统文档 |
| P2 | M-N1–M-N4 | 修恒等式命名/口径、联合区间、原子写、renderer try/catch |
| P2 | M-N5–M-N10 | 同步站点 README/index 方法学声明；接入调度；补测试 |
| P3 | L-N1–L-N9 | 清理注释、docstring、dev 依赖 |

---

## 16. 最终判定

**NOT READY（截至第 6 批，2026-11-07）。**

- **已关闭**：C-N1/C-N2（CRITICAL 全部）、H-N1/N2/N3/N4/N5/N6/N7、M-N8/N9/N10、L-N1/N2/N6/N7。G1、G2、G3、G4 四道门禁转绿；全量 `2111 passed`，`mypy`/`ruff` 零错误。
- **仍红**：
  - **G6 版本库**：第 5 批已提交 `a9b9b95`，第 6 批改动未提交，`register_nyfed_datasets.py`/`visualization/`/`docs/USD_Liquidity/` 仍未跟踪，分支领先 origin 57 个提交。
  - **G7 文档一致**：README 源数已对齐 14，但 `nyfed` 连接器、`reserve-structural` 模型与 `visualization/` 站点仍无设计文档，站点 README 与实现互相矛盾（M-N5/M-N6）。
  - **G5 约束核验（部分）**：H-N4 已修（2027-02-04 区间 0.056T ~ 1.699T）；但置信带仍标为"联合 90%"，实为逐 horizon 边际分位（M-N2）。
- **不能判 READY 的理由**：测试与静态检查转绿只说明门禁 G1/G2 恢复，不代表 G5/G6/G7 关闭。"联合 90%"标注不实仍会误导使用者；版本库未收敛则当前工作区不可作为交付基线。
- **下一步建议**：确认并提交第 6 批 → 修 M-N2 区间标注（G5）→ 第 7 批 M-N1 恒等式冗余 → 补 `docs/architecture`/`docs/design` 与站点 README（G7）→ 推送（G6）。在 M-N2 关闭前，不应对外宣称准备金带宽是"联合 90%"。

---

## 17. 修复进度（按批次）

| 批次 | 范围 | 状态 | 门禁影响 |
|---|---|---|---|
| 第 1 批 | C-N1 SRF 口径 + 污染数据重采 | ✅ 完成（已提交 `4ab246a`） | G4 的 SRF 子项关闭 |
| 第 2 批 | C-N2 nyfed 门禁 + 方案 B 行单位契约 + fixture + 注入验证 | ✅ 完成（已提交 `f688c3f`） | **G3 转绿；G4 转绿** |
| 第 3 批 | H-N3 准备金 PIT | ✅ 完成（已提交 `2499eca`） | G5 部分恢复（PIT 子项关闭） |
| 第 4 批 | H-N1 发布时刻 + H-N2 SRF 未公布占位 + nyfed 窗口 | ✅ 完成（已提交 `afb4d28`） | G5 部分恢复（发布时刻子项关闭） |
| 第 5 批 | H-N5/H-N6 静态与测试红 + 文档/源数对齐 | ✅ 完成（已提交 `a9b9b95`） | **G1、G2 转绿** |
| 第 6 批 | H-N4 TGA 区间口径 + 图 5 轴硬编码 | ✅ 完成（2026-11-07，未提交） | G5 的区间子项关闭 |

### 第 1 批改动清单（已提交 `4ab246a`）

| 文件/对象 | 类型 | 说明 |
|---|---|---|
| `src/chronoforge/connectors/nyfed.py` | 修改（首次纳入版本库） | 增加 SVE note 过滤与 `_SVE_MARKER` |
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

**未做**：未推送；未删除 `docs/USD_Liquidity/reports/quality_report.md`。

### 第 2 批改动清单（2026-11-07，已提交 `f688c3f`）

| 文件/对象 | 类型 | 说明 |
|---|---|---|
| `src/chronoforge/connectors/base.py` | 修改 | 新增 `DataConnector.row_units(raw) -> int \| None` 契约；基类默认抛 `NotImplementedError`（未声明即不可运行） |
| `src/chronoforge/pipeline/runner.py` | 修改 | 删除 `_payload_units` 形状猜测；新增 `_declared_row_units`，每个 batch 在 normalize 前取声明，未声明/负值 → `QualityError`；`failed_units += max(units, 1)` |
| `src/chronoforge/connectors/*.py`（14 个） | 修改 | 逐一声明 `row_units`（见 §17 覆盖表）；形状漂移抛 `SchemaError` |
| `src/chronoforge/connectors/normalize_stats.py` | 修改 | 白名单增加 `sve_test_operation` / `non_overnight_operation` |
| `src/chronoforge/connectors/nyfed.py` | 修改 | `_classify_srf_row` / `_classify_soma_row`、`note_skip`、checkpoint 对齐 |
| `tests/unit/test_connector_row_units.py` | **新增** | 表驱动 14 连接器真声明（含空批、嵌套形状、形状漂移、基类默认） |
| `tests/unit/test_payload_units.py` | **删除** | 测试已删除的 runner `_payload_units` |
| `tests/quality/test_row_units_gate.py` | **新增** | 15 形状 × 3 模式：取消声明 / 错误行数 / 合法空批 |
| `tests/integration/test_pipeline.py` | 修改 | `FakeConnector` 增加 `row_units` 测试双 |
| `tests/integration/test_nyfed.py` | 扩充 | 12 条：SVE 口径、note_skip、checkpoint、fetch→normalize 全链路 |
| `tests/quality/test_nyfed_gate.py` | **新增** | 注入"静默丢弃"假实现 → 门禁变红；纯 SVE 合法跳过 → 绿 |
| `tests/quality/test_normalize_drop_gate.py` | 修改 | 白名单锁测试同步新增两个合法原因 |
| `tests/fixtures/nyfed/` | **新增** | 真实形状 SRF/SOMA fixture + `SOURCE.md` |
| `docs/review/2026-11-07-full-audit.md` | 修改 | C-N2 修复记录、SVE 复核、§0 统计/门禁、本节 |

**验证命令与结果**：

```bash
uv run --with pytest --with pytest-asyncio --with hypothesis \
  python -m pytest tests/unit/test_connector_row_units.py -q                          # 41 passed
uv run --with pytest --with pytest-asyncio --with hypothesis \
  python -m pytest tests/quality tests/integration/test_pipeline.py \
  tests/unit/test_connector_row_units.py -q                                           # 152 passed
uv run --with pytest --with pytest-asyncio --with hypothesis python -m pytest -q      # 3 failed, 2080 passed
uv run ruff check src tests/unit/test_connector_row_units.py \
  tests/quality/test_row_units_gate.py tests/integration/test_pipeline.py             # 1 error（nyfed docstring E501，第 5 批）
uv run mypy src                                                                       # 2 errors（nyfed health() 缺 params，第 5 批）
```

3 条失败均为 docs/CLI 漂移（README `13 sources`、CLI `13 sources`、源列表缺 `nyfed`），属第 5 批范围；本批新增用例全绿。

### 第 3 批改动清单（2026-11-07，已提交 `2499eca`）

| 文件/对象 | 类型 | 说明 |
|---|---|---|
| `scripts/reserve_forecast.py` | 修改（此前未跟踪） | `AsOfFrame`（values + available）、`_availability`、`_asof_cutoff`；预测与回测共用 PIT 历史；补缺口周；TCCF 归档起点按 0 处理；版本 `0.1.2`；新增 seasonal-naive DM 与 currency 消融输出；benchmark 统一有效子样本 |
| `tests/unit/test_reserve_forecast_pit.py` | **新增** | 6 条：当周排除、污染不生效、revision 延后排除、回测锚点/target、release+revision 取最大、晚修订不做归档回退 |
| `data/derived/reserve_forecast` | 重写 | 3 行，`model_version=reserve-structural-0.1.2`，`origin_reserve` 改为最近可知值 |
| `data/derived/reserve_forecast_components` | 重写 | 24 行 |
| `data/derived/reserve_forecast_eval` | 重写 | `2013-04-24` 起 703 origins × 3 horizons = 2109 行，新增 `anchor_date`、seasonal-naive DM 列 |
| `visualization/data.js` / `data.json` | 重新导出 | `model_version=0.1.2`，图 16 数据已刷新 |
| `docs/review/2026-11-07-full-audit.md` | 修改 | H-N3 修复记录、§0 统计/门禁、本节 |

**验证命令与结果**：

```bash
uv run --with pytest --with pytest-asyncio --with hypothesis \
  python -m pytest tests/unit/test_reserve_forecast_pit.py -q    # 6 passed
uv run --with pytest --with pytest-asyncio --with hypothesis python -m pytest -q
                                                                  # 3 failed, 2086 passed
uv run ruff check scripts/reserve_forecast.py tests/unit/test_reserve_forecast_pit.py
                                                                  # All checks passed
uv run ruff check src                                             # 1 error（nyfed E501，第 5 批）
uv run mypy src                                                   # 2 errors（nyfed，第 5 批）
uv run python scripts/reserve_forecast.py check                   # PASS；asof=周三，最新可知=上周三
uv run python scripts/reserve_forecast.py run                     # fc=3 comp=24 eval=2109
uv run python visualization/export_data.py                        # data.js 刷新
```

### 第 4 批改动清单（2026-11-07，未提交）

| 文件/对象 | 类型 | 说明 |
|---|---|---|
| `src/chronoforge/config/update_schedule.py` | 修改 | 新增 `DATASET_PUBLISH_TIME_UTC`（SOMA 23:00 / SRF 22:00）；`publish_time_utc(source_id, dataset_id)` 两级覆盖；`nyfed` 源级回退 22:00 |
| `scripts/backfill_datasets.py` | 修改 | `_next_due_at` 接收 `dataset_id`；长周期新增"同日发布时刻未过先对齐当天"；调用处透传 dataset_id |
| `src/chronoforge/connectors/nyfed.py` | 修改 | `_utc_now()` / `_srf_release_time()`；SRF 未公布行按 `not_yet_published` 跳过（不写占位 0）；`checkpoint_from` 只统计已公布日期 |
| `src/chronoforge/connectors/normalize_stats.py` | 修改 | 白名单新增 `not_yet_published` |
| `src/chronoforge/pipeline/windows.py` | 修改 | 新增 `nyfed_series`（1 天回看 + 单请求全窗口），`nyfed_` 前缀不再复用 `fred_series` |
| `tests/unit/test_update_schedule.py` | 扩充 | `TestDatasetPublishTime` 4 条 + `nyfed` 北京时刻 |
| `tests/unit/test_due_selection.py` | 扩充 | `TestNyfedPublishAlignment` 8 条（同日对齐/跨周/到期窗口） |
| `tests/unit/test_windows.py` | 扩充 | `nyfed_series` 快照 + 1 天回看交互 1 条 |
| `tests/integration/test_nyfed.py` | 扩充 | 4 条：冻结墙钟的未发布跳过 / checkpoint 不推进 / 22:00 后可见 / 纯占位 0 不产出 |
| `tests/quality/test_normalize_drop_gate.py` | 修改 | 白名单锁同步 `not_yet_published` |
| `docs/review/2026-11-07-full-audit.md` | 修改 | H-N1/H-N2 修复记录、§0 统计/门禁、本节 |

**验证命令与结果**：

```bash
uv run --with pytest --with pytest-asyncio --with hypothesis \
  python -m pytest tests/unit/test_due_selection.py tests/unit/test_windows.py \
  tests/unit/test_update_schedule.py tests/integration/test_nyfed.py \
  tests/quality/test_normalize_drop_gate.py -q               # 158 passed
uv run --with pytest --with pytest-asyncio --with hypothesis python -m pytest -q
                                                             # 3 failed, 2106 passed
uv run ruff check src/chronoforge/config/update_schedule.py src/chronoforge/connectors/nyfed.py \
  src/chronoforge/connectors/normalize_stats.py src/chronoforge/pipeline/windows.py \
  scripts/backfill_datasets.py tests/unit/test_update_schedule.py \
  tests/unit/test_due_selection.py tests/unit/test_windows.py \
  tests/integration/test_nyfed.py                            # 2 errors（均为既有，第 5 批）
```

**修复前实测**：见 H-N1/H-N2 各条的"验证"块（stash 本批源码改动后 15 failed / 38 passed；`test_update_schedule.py` 因缺少新符号整体导入失败）。

**3 条全量失败**均为 docs/CLI 漂移（第 5 批已修复）：`test_readme_source_count_matches_default_sources`（README `13 sources`）、`test_sync_bootstraps_all_sources_idempotent`（CLI `13 sources`）、`test_list_sources_json`（源列表缺 `nyfed`）。

### 第 5 批改动清单（2026-11-07，已提交 `a9b9b95`）

| 文件/对象 | 类型 | 说明 |
|---|---|---|
| `src/chronoforge/connectors/nyfed.py` | 修改 | `health()` 补 `_ENDPOINT` 参数（按"最近一月 SRF 结果"的真实语义）；docstring 长 URL 折行 |
| `scripts/backfill_datasets.py` | 修改 | `--intervals` help f-string 拆为隐式拼接（E501） |
| `README.md` | 修改 | `13 → 14 sources`（两处），源表补 `nyfed` 行 |
| `tests/unit/test_cli.py` | 修改 | `13 → 14 sources`、`COUNT(*)=14`、源集合补 `nyfed` |
| `docs/review/2026-11-07-full-audit.md` | 修改 | §0 统计/门禁、H-N5/H-N6/H-N7/H-N9、M-N8/N9/N10、L-N1/N2/N6/N7 状态，§12/§16/§17 |

**验证命令与结果**：

```bash
uv run ruff check src scripts/backfill_datasets.py            # All checks passed!
uv run mypy src                                               # Success: no issues found in 86 source files
uv run --with pytest --with pytest-asyncio --with hypothesis python -m pytest -q
                                                              # 2109 passed, 10 warnings in 92.67s
```

**未做**：H-N4（TGA 区间算法）与 M-N2（"联合"标注）、M-N1/N3–N7、M-N9 之外的可视化/调度项、L-N3/N4/N5/N8/N9 均未改；`docs/architecture`/`docs/design` 缺文档未补。这些是真正的剩余缺口，不计入本批。

### 第 6 批改动清单（2026-11-07，未提交）

| 文件/对象 | 类型 | 说明 |
|---|---|---|
| `config/tga/model.yaml` | 修改 | 新增 `interval_bands`（7/14/30/60/90，USD 百万，P5–P95 残差偏移）+ 口径注释；`bands` 降级为"仅组件行展示用，不参与区间合成" |
| `scripts/tga_forecast.py` | 修改 | `MODEL_VERSION → tga-forecast-0.5.0`；新增 `_interval_offsets`；`aggregate_forecast` 删除 `daily_lower`/`daily_upper` 与 `cum_sum`，改为日方差 RSS 兜底 + 按 horizon 取偏移 + `max(0.0, lower)` |
| `tests/unit/test_tga_forecast.py` | 扩充 | 2 条：horizon 偏移命中 + 下沿 0 截断；无 `interval_bands` 时方差传播（RSS≠线性） |
| `visualization/app.js` | 修改 | `chart5` 去掉 `yAxis.min/max`（原 0/1.45），轴自适应 |
| `docs/review/2026-11-07-full-audit.md` | 修改 | H-N4 修复记录（根因实测/两类错误/前后数值/backtest 影响）、§0 统计/门禁、§7、§8.2、§12/§15/§16/§17 |
| `data/derived/tga_forecast` | 新增 run | `36e223cae0c29e10`（origin 2026-11-07，60 行）、`ff1148f6576d7dd4`（origin 2026-11-06，59 行，用于前后对比）；旧 run 保留、不覆盖 |

**验证命令与结果**：

```bash
uv run --with pytest --with pytest-asyncio --with hypothesis python -m pytest -q
                                                 # 2111 passed, 10 warnings in 90.40s
uv run ruff check src scripts/tga_forecast.py tests/unit/test_tga_forecast.py
                                                 # All checks passed!
uv run mypy src                                  # Success: no issues found in 86 source files
```

**未做**：M-N2（"联合 90%"标注）未动；未重跑 `tga_backtest`（pinball/coverage 需单独评估）；未提交、未推送。

### G4 行单位覆盖表（✅ 17 个形状全部声明、15 个形状注入验证，第 2 批方案 B）

runner 不再猜测 payload 形状：每个连接器必须实现 `row_units`，返回值等于 normalize 候选行数（产出 + 上报跳过）；`0` 表示源端确实无数据；`None` 仅用于无行级粒度的单文档源；形状漂移抛 `SchemaError`；未声明由基类抛 `NotImplementedError`，runner 转为 `QualityError` 拒绝执行。

| 连接器 | 端点/形状 | 声明的行单位 | 契约测试 |
|---|---|---|---|
| alternative | `{"data": [...]}` | `len(data)` | ✅ |
| binance_spot | 顶层 list / ticker dict | `len(list)`；单快照 dict = `1` | ✅ |
| binance_futures | 顶层 list / ticker dict | 同上 | ✅ |
| ccxt_bridge | 顶层 list（OHLCV/trades） | `len(list)` | ✅ |
| coinmarketcap | `{"data": {"quotes": [...]}}` | `len(data["quotes"])` | ✅（盲区已关） |
| coinmetrics | `{"data": [...]}` | `len(data)` | ✅ |
| deribit（chart） | `{"result": {"ticks": [...], ...}}`（并行数组） | `len(result["ticks"])`（不是数组条数） | ✅（盲区已关） |
| deribit（ticker/funding） | `{"result": [...]}` | `len(result)` | ✅ |
| fred | `{"observations": [...]}` | `len(observations)` | ✅ |
| nyfed（SRF） | `{"repo": {"operations": [...]}}` | `len(repo["operations"])` | ✅（盲区已关） |
| nyfed（SOMA） | `{"soma": {"summary": [...]}}` | `len(soma["summary"])` | ✅（盲区已关） |
| sec_edgar（submissions） | `{"filings": {"recent": {并行数组列}}}` | `len(recent["accessionNumber"])`（分页文件用顶层键） | ✅（盲区已关） |
| sec_edgar（companyfacts） | `{"facts": {taxonomy: {concept: {units: {...}}}}}` | 四层遍历后的观测条目总数 | ✅（盲区已关） |
| sosovalue | `{"code": 0, "data": [...]}` | `len(data)` | ✅ |
| treasury_fiscal | `{"data": [...]}` | `len(data)` | ✅ |
| treasury_refunding | `{"release_html": "..."}`（单文档） | 显式 `None`（无行级粒度） | ✅ |
| yahoo | `{"chart": {"result": [{"timestamp": [...]}]}}` | `0`（空 result）或 `len(result[0]["timestamp"])` | ✅（盲区已关） |

**注入验证（`tests/quality/test_row_units_gate.py`，45 passed）**：

| 注入 | 结果 |
|---|---|
| 取消声明（`row_units` 抛 `NotImplementedError`） | 15/15 `QualityError`，`checkpoint_after IS NULL` |
| 错误行数（声明真实值 +2） | 15/15 `QualityError: normalize drop rate`，`checkpoint_after IS NULL` |
| 合法空批（真实声明 `0` / `None`） | 15/15 `SUCCESS`，cursor 推进到窗口右界 |

**契约测试（`tests/unit/test_connector_row_units.py`，41 passed）**：覆盖上述全部形状的真声明、空批、`None` 语义、形状漂移 `SchemaError`，以及基类默认抛 `NotImplementedError`。

**残余风险（明确边界，不掩盖）**：门禁只能发现"声明 > 产出"，无法发现"连接器把有数据的批次错报为 0"。该风险由契约测试的真实形状覆盖 + 形状漂移抛错来约束，不再依赖 runner 猜测；新增连接器若忘写 `row_units` 会直接拒绝执行，而不是静默为 0。
</content>
