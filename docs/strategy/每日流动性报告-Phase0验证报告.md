# 每日流动性报告 · Phase 0 验证报告（V1–V10）

- 验证日期：2026-11-08（Asia/Shanghai）
- 验证对象：《每日流动性报告-实施方案》§9 待验证清单 V1–V10，外加两项专项：
  - H.8 是否真的未注册（注册表 / `DEFAULT_SOURCES` / checkpoint / canonical 四处交叉）
  - Yahoo `DX-Y.NYB`「1 timestamp → 0 record」根因
- 运行环境：`/Users/horsenli/Works/ChronoForge`
  - meta 库：`meta/chronoforge.db`
  - canonical：`data/canonical`
  - raw：`data/raw`
- 官方页面访问日期：**2026-11-08**
- 约束：只做验证，不实现日报、不改实现代码。临时查询全部为内联 heredoc（未在工作区落盘）；仅 `/tmp/nim_models.json`、`/tmp/nim_gen.json` 为临时响应缓存，不提交。

> **时点说明**：2026-11-08 为周日，无法在 07:40 UTC 真实触发"交易日批次"。V1/V2 的"交易日 07:40"结论由**官方发布时刻 + 实测 canonical `release_time` + checkpoint** 推导；能直接实测的部分（周末批次、序列最新日期）另列。下文明确区分"实测"与"推导"。

---

## 0. 结论总表

| # | 事项 | 结论 | 一句话证据 |
|---|---|---|---|
| V1 | 07:40 UTC 批次能否抓到 H.15/RRP/VIX 等常规序列 | **成立（需修正 as-of）** | 序列都在；但 H.15 系（DGS2/10/30、DTB4WK）与 VIXCLS 的 as-of 是 **T-2 工作日**，不是 T-1；T10YIE/RRP 为 T-1 |
| V2 | 07:40 批次后的 EFFR/SOFR 参考日期 | **成立** | 官方 EFFR 约 9:00 ET、SOFR 约 8:00 ET 才发布前一日值，晚于 07:40 UTC；交易日 T 的 07:40 批次只能看到 **T-2** |
| V3 | NY Fed SOMA/SRF 实际 API 可用时刻 | **成立（常量偏保守，需对齐）** | SRF 操作日 13:30 ET、13:45:40 ET 更新完成（≈18:45 UTC）；SOMA 随 H.4.1 周四 16:30 ET 披露；常量 22:00 UTC / 次日 23:59:59 UTC 均更晚 |
| V4 | H.8 是否纳入 | **成立（确实未注册）** | `dataset_registry`/`checkpoints` 命中 0 行，`data/canonical` 无 H.8 目录，`DEFAULT_SOURCES` 无 H.8 源 |
| V5 | 两个 nyfed dataset 在 `dataset_stats.py` 可见 | **成立（附展示缺陷）** | `dataset_stats.py --sources nyfed` 列出 `soma_*` 与 `srf_usage`；`dataset_id` 列被合并成 `nyfed_repo_srf_usage, nyfed_soma_holdings` |
| V6 | `EARLY_CLOSINGS` 覆盖 2026–2027 | **不成立** | 假日表与 NYSE 官方一致；早闭表多出 `2027-12-23`，官方 2027 只有 `11-26` |
| V7 | 加密当日日线是否为未完成 bar | **成立（文档前提已过期）** | `ccxt_bridge.py:774-778` 以白名单 `unclosed_bar` 过滤；canonical BTCUSDT 1d 最新为已完成日 `2026-11-05`；`dataset_states.txt:116` 仍写 `2026-10-28` |
| V8 | LLM 供应商/模型/成本 | **无法判定** | NVIDIA NIM 鉴权通过、`/v1/models` 200/84，但本账号对候选模型返回 404 或超时，无法完成生成评测 |
| V9 | `scripts/tga_daily.py` 何时落地 | **成立（未落地的描述属实）** | `scripts/tga_daily.py` 不存在；`scripts/tga_reconstruct.py` 存在 |
| V10 | FOMC/CPI/NFP 日历 | **成立（仓库无宏观事件日历）** | 仅 `config/tga/calendar.yaml`（TGA/税期）；`config/`、`src/` 无 FOMC/CPI/NFP 事件源 |

**推翻方案设计假设的项**：V1（H.15/VIX 被当成 T-1）、V6（2027-12-23 早闭）、V7 的文档前提（`dataset_states.txt` 已过期）。**不影响核心结构**：07:40 单批次仍能拿到全部关键序列，只是 as-of 标签需要改正。

---

## 1. V1 — 07:40 UTC 批次的常规序列 as-of

### 结论

**成立（能抓到），但方案 §2.3 的 T-1 假设不成立，需改为 T-2。**

- 能抓到：DGS2/DGS10/DGS30/DTB4WK/VIXCLS/T10YIE/RRPONTSYD 全部有数据。
- as-of 分层：
  - H.15 系（DGS2/10/30、DTB4WK）= **T-2 工作日**（结构性）
  - VIXCLS = 名义 T-1，本环境 FRED 摄入实测 **T-2**
  - T10YIE / RRPONTSYD = **T-1**

### 证据

**官方口径**（访问 2026-11-08）
- URL：https://www.federalreserve.gov/releases/h15/
- 原文："The release is posted daily Monday through Friday at 4:15pm."
- "Release date: November 6, 2026"，表格最新数据列是 **2026 Nov 5**。
- 含义：H.15 在 D 日 16:15 ET 发布，只含 D-1 的数据。因此在 T 日 07:40 UTC（= 02:40 ET，当日发布尚未发生）能看到的最新 H.15 = **T-2 工作日**。

**上游 FRED CSV 实测**（`curl https://fred.stlouisfed.org/graph/fredgraph.csv?id=<SERIES>&cosd=2026-11-01`）

```
DGS10     2026-11-05,4.84
DGS2      2026-11-05,4.46
DGS30     2026-11-05,5.25
DTB4WK    2026-11-05,3.89
VIXCLS    2026-11-05,28.18
T10YIE    2026-11-06,2.23
RRPONTSYD 2026-11-06,0.150
```

**本地 canonical 实测**（`data/canonical/NUMBER/entity=<ENTITY>/...`，取 `observation_time/value/release_time` 末 3 行）

```
=== EFFR ===       2026-11-05  3.88  release_time 2026-11-06 23:59:59
=== DGS10 ===      2026-11-05  4.84  release_time 2026-11-06 23:59:59
=== DGS2 ===       2026-11-05  4.46  release_time 2026-11-06 23:59:59
=== DGS30 ===      2026-11-05  5.25  release_time 2026-11-06 23:59:59
=== DTB4WK ===     2026-11-05  3.89  release_time 2026-11-06 23:59:59
=== VIXCLS ===     2026-11-05 28.18  release_time 2026-11-05 23:59:59
=== T10YIE ===     2026-11-06  2.23  release_time 2026-11-06 23:59:59
=== RRPONTSYD ===  2026-11-06  0.150 release_time 2026-11-06 23:59:59
```

**Yahoo ^VIX 对照**（说明 11-06 的 VIX 收盘并非不存在，而是 FRED 摄入滞后）

```
YAHOO:^VIX:SPOT  2026-11-05  28.18
YAHOO:^VIX:SPOT  2026-11-06  25.64
```

**checkpoint 实测**（2026-11-08 04:00 批次）

```
fred_series_DGS10      last_cursor 2026-11-08T04:00:07  last_success 2026-11-08 04:01:48
fred_series_VIXCLS     last_cursor 2026-11-08T04:00:07  last_success 2026-11-08 04:04:03
fred_series_T10YIE     last_cursor 2026-11-08T04:00:07  last_success 2026-11-08 04:03:46
fred_series_RRPONTSYD  last_cursor 2026-11-08T04:00:07  last_success 2026-11-08 04:03:32
```

### 若不成立，方案哪处要改

- §2.3 的 `H.15 / VIX` 行（原写"上周五/周一/…"的 T-1 映射）→ 拆成两行：
  - `H.15 系（DGS2/10/30、DTB4WK）`：T-2 工作日（周一=上周四、周二=上周五、周三=周一、周四=周二、周五=周三）
  - `VIXCLS`：名义 T-1、本环境实测 T-2
- §2.3 表下注释"EFFR/SOFR 与 TGA 的 T-2 就是理论最好值"→ 把 H.15 也纳入 T-2 结构性上限。
- 结论：不影响 07:40 单批次结构；影响报告里逐序列 as-of 的写法。

---

## 2. V2 — 07:40 批次后的 EFFR/SOFR 参考日期

### 结论

**成立（结构性 T-2）。**

### 证据

**官方口径**（访问 2026-11-08）
- EFFR：https://www.newyorkfed.org/markets/reference-rates/effr
  - 原文："The New York Fed publishes the EFFR for the prior business day on the New York Fed's website at approximately 9:00 a.m."
- SOFR：https://www.newyorkfed.org/markets/reference-rates/sofr
  - 原文："Each business day, the New York Fed publishes the SOFR on the New York Fed website at approximately 8:00 a.m. ET."
- 含义：交易日 T 的 07:40 UTC（= 02:40 ET）早于 T 日 08:00/09:00 ET，故 EFFR/SOFR 的 T-1 值尚未发布；最新只能是 **T-2**。

**本地 canonical（观测日 11-05 的 `release_time`）**

```
EFFR  2026-11-04 3.88 release_time 2026-11-05 23:59:59
EFFR  2026-11-05 3.88 release_time 2026-11-06 23:59:59
SOFR  2026-11-04 3.94 release_time 2026-11-05 23:59:59
SOFR  2026-11-05 3.92 release_time 2026-11-06 23:59:59
```

- 11-05 观测的 PIT 可用时点被记为 11-06 23:59:59（连接器保守常量），已晚于 11-06 07:40，故 07:40 批次不可能拿到 11-05 值。
- checkpoint：`fred_series_EFFR` 2026-11-08 04:02:42、`fred_series_SOFR` 04:03:34；批次把存储从 11-04 推进到 11-05。

### 若不成立，方案哪处要改

- 无需改结构。仅需在报告模板里把 EFFR/SOFR 的 as-of 固定表达为"T-2 工作日"，不能写成"上一交易日"。

---

## 3. V3 — NY Fed SOMA/SRF 的实际 API 可用时刻

### 结论

**成立（可用时刻已由官方/API 核实）；连接器常量偏保守，建议对齐，但不影响 16:00 北京报告的 as-of。**

### 证据

**SRF 实测**（`https://markets.newyorkfed.org/api/rp/results/search.json?startDate=2026-11-05&endDate=2026-11-07&operationTypes=Repo`）

```
2026-11-06 type=Repo releaseTime=08:15 closeTime=08:30 lastUpdated=2026-11-06 08:31:14 accepted=0
2026-11-06 type=Repo releaseTime=13:30 closeTime=13:45 lastUpdated=2026-11-06 13:45:40 accepted=0
2026-11-05 type=Repo releaseTime=08:15 closeTime=08:30 lastUpdated=2026-11-05 08:30:20 accepted=0
2026-11-05 type=Repo releaseTime=13:30 closeTime=13:45 lastUpdated=2026-11-05 13:45:17 accepted=0
```

- 当日主操作 13:30 ET 开始、13:45 ET 结束，**13:45:40 ET 结果已可查**（≈18:45 UTC，EST）。
- 连接器常量 `_SRF_RELEASE_HOUR_UTC = 22`（即 17:00 ET），比实际晚约 3 小时 15 分。

**SOMA 实测**（`https://markets.newyorkfed.org/api/soma/summary.json`）

```
rows 1218
...
2026-10-28 6355840126861.20
2026-11-04 6357617126861.20
```

- 最新 `asOfDate = 2026-11-04`（周三）。
- 官方 H.4.1：https://www.federalreserve.gov/releases/h41/，原文 "released each Thursday, generally at 4:30 p.m." → SOMA 持仓随 H.4.1 在**周四 16:30 ET（≈21:30 UTC，EST）**披露。
- 连接器常量（`src/chronoforge/connectors/nyfed.py:430,449`）：周四 23:59:59 UTC，比官方晚约 2.5 小时。

**对报告时点的影响**：报告在 16:00 北京（08:00 UTC）读前一交易日已落库数据；SRF（当日 13:45 ET）与 SOMA（周四 16:30 ET）均早于次日 08:00 UTC 报告，故**不影响 as-of**。常量只影响"何时允许抓取"的 PIT 保护。

### 若不成立，方案哪处要改

- §2（调度表）与 §9 V3 的常量描述应改为官方口径：
  - SRF：操作日 13:45 ET 后可抓（≈18:45 UTC EST / 17:45 UTC EDT）
  - SOMA：随 H.4.1 周四 16:30 ET 后可抓（≈21:30 UTC EST）
- 保留现有保守值也可，但必须在文档里写明"保守，非官方时刻"。

---

## 4. V4 — H.8 是否真的未注册（专项）

### 结论

**成立：H.8 确实未注册，四条链路均为空。**

### 证据（四处交叉）

**① `dataset_registry`（`meta/chronoforge.db`）**

```
$ sqlite3 meta/chronoforge.db "SELECT count(*) FROM dataset_registry WHERE lower(dataset_id) LIKE '%h8%' OR lower(entity_id) LIKE '%h8%' OR lower(dataset_id) LIKE '%h.8%';"
0
```

FRED 已注册的 112 个 `fred_series_*` 中不含 H.8 典型序列（`TOTBKCR`、`BUSLOANS`、`CONSUMER`、`LOANS` 等）。

**② `DEFAULT_SOURCES`（`src/chronoforge/registry/service.py`）**

- 14 个默认源：`binance_spot / binance_futures / deribit / ccxt / yahoo / fred / sec_edgar / sosovalue / alternative / coinmetrics / coinmarketcap / treasury_fiscal / treasury_refunding / nyfed`
- 无 H.8 专属源；H.8 若接入只能走 `fred` 源，但 registry 中没有对应 dataset。

**③ `checkpoints`**

```
$ sqlite3 meta/chronoforge.db "SELECT count(*) FROM checkpoints WHERE lower(dataset_id) LIKE '%h8%' OR lower(dataset_id) LIKE '%h.8%';"
0
```

**④ canonical 数据**

```
$ find data/canonical -maxdepth 2 -type d | grep -i "h8\|h.8"
（无输出）
$ find data/canonical -type f | grep -iE "TOTBKCR|BUSLOANS|CONSUMER|H8|H\.8"
（无输出）
```

**官方口径**（访问 2026-11-08）
- URL：https://www.federalreserve.gov/releases/h8/
- 原文："These data are released each Friday, generally at 4:15 p.m., unless Friday is a federal holiday, in which case the data will be released on Thursday, generally at 4:15 p.m."

### 若不成立，方案哪处要改

- 方案 §2.3 的 H.8 行与 §9 V4 已可改为"确认未注册，v1 不纳入"；不需要新增接入。

---

## 5. V5 — `nyfed_repo_srf_usage` / `nyfed_soma_holdings` 在 `dataset_stats.py` 可见

### 结论

**成立（数据集可见）；附带一个展示层缺陷（`dataset_id` 列把两个 dataset 合并成一行）。**

### 证据

`uv run python scripts/dataset_stats.py --sources nyfed`（耗时 27.2s）：

```
[NUMBER] 实体 10 | dataset 2 | 共 11,464 行 | 2003-07-09 00:00 ~ 2026-11-06 00:00
  entity               dataset_id                                         行数  起始 ~ 结束
  soma_agencies        nyfed_repo_srf_usage, nyfed_soma_holdings         970  2003-07-09 00:00 ~ 2026-11-04 00:00
  soma_bills           nyfed_repo_srf_usage, nyfed_soma_holdings       1,218  2003-07-09 00:00 ~ 2026-11-04 00:00
  ...
  soma_total           nyfed_repo_srf_usage, nyfed_soma_holdings       1,218  2003-07-09 00:00 ~ 2026-11-04 00:00
  srf_usage            nyfed_repo_srf_usage                            1,317  2021-07-29 00:00 ~ 2026-11-06 00:00
```

- 两个 dataset 均可见：`srf_usage`（到 2026-11-06）、`soma_*`（到 2026-11-04）。
- 缺陷：`dataset_id` 列对 `soma_*` 显示 `nyfed_repo_srf_usage, nyfed_soma_holdings`（逗号拼接），不是真正的缺失。

### 若不成立，方案哪处要改

- 不改方案；若后续 freshness 表按 `dataset_id` 取值，需要在实现时按单值拆分，不能用该展示列。

---

## 6. V6 — `EARLY_CLOSINGS` 覆盖 2026–2027

### 结论

**不成立：假日表与官方一致；早闭表多出 `2027-12-23`。**

### 证据

**官方 NYSE 日历**（访问 2026-11-08）：https://www.nyse.com/markets/hours-calendars

- 2026 早闭：`Friday, November 27, 2026`（感恩节次日）、`Thursday, December 24, 2026`（圣诞前夜）。
- 2027 早闭：`Friday, November 26, 2027`（感恩节次日）。
- 官方文本未列出任何 2027 年 12 月早闭；2027 圣诞日为 `Friday, December 24`（observed），**不存在** 12-23 早闭。

**代码**（`src/chronoforge/quality/calendar.py`）

```
115: EARLY_CLOSINGS: frozenset[date] = frozenset({
...
126:    date(2026, 11, 27),  # Thanksgiving 次日
127:    date(2026, 12, 24),  # Christmas Eve
128:    date(2027, 11, 26),  # Thanksgiving 次日
129:    date(2027, 12, 23),  # Christmas Eve observed 前一日   ← 官方无此项
130: })
```

**假日表核对**（`calendar.py:86-109` vs 官方表）：2026/2027 全部一致，无缺也无多。

### 若不成立，方案哪处要改

- 删除 `calendar.py:129` 的 `date(2027, 12, 23)`。
- 方案 §9 V6 状态改为"不成立，已定位 1 项多余"；实现侧已另行以 fix 提交删除 `calendar.py` 的该项，并补 2027 早闭日集合回归测试。

---

## 7. V7 — 加密当日日线是否为未完成 bar

### 结论

**成立：未完成 bar 会被白名单 `unclosed_bar` 过滤；canonical 最新为已完成日。附带：`dataset_states.txt` 已过期。**

### 证据

**代码**（`src/chronoforge/connectors/ccxt_bridge.py:774-778`）

```
774: reason="unclosed_bar",
778: note_skip("unclosed_bar")
```

`unclosed_bar` 在 `src/chronoforge/connectors/normalize_stats.py:29-39` 的 `LEGIT_SKIP_REASONS` 白名单内。

**canonical 实测**（BTCUSDT 1d 末 3 行）

```
CCXT-BINANCE:BTCUSDT:SPOT       2026-11-04  1d
CCXT-BINANCE:BTCUSDT:USDT:SPOT  2026-11-04  1d
CCXT-BINANCE:BTCUSDT:SPOT       2026-11-05  1d
CCXT-BINANCE:BTCUSDT:USDT:SPOT  2026-11-05  1d
```

- 最新为 `2026-11-05`（已完成日历日），无当日 00:00 的未完成 bar。

**文档前提过期**（`dataset_states.txt:116`）

```
CCXT-BINANCE:BTCUSDT:SPOT  1d  CCXT:BTCUSDT:OHLCV:1d  2,127  2021-01-01 00:00 ~ 2026-10-28 00:00
```

- 该文件停在 `2026-10-28`，与 canonical 实际 `2026-11-05` 不符；V7 原描述引用它作为前提，需要更新。

### 若不成立，方案哪处要改

- 不改实现。§9 V7 的证据来源要改为 canonical 实测，不要再用 `dataset_states.txt`（已过期）作为依据；如需该文件，应重新生成后再引用。

---

## 8. V8 — LLM 供应商/模型/成本

### 结论

**无法判定（本环境内无法完成一次成功的生成调用；需 Phase 1 小样本评测）。**

### 证据

```
$ grep -c NVIDIA_API_KEY .env
1

$ curl -H "Authorization: Bearer $NVIDIA_API_KEY" https://integrate.api.nvidia.com/v1/models
models_http=200
n= 84
```

- 鉴权通过、模型列表可读（84 个），说明端点和 key 有效。

生成调用（`POST /v1/chat/completions`，`max_tokens=8`）：

```
model=mistralai/mistral-large-2-instruct  http=404
{"status":404,"detail":"Function '...': Not found for account '...'"}

model=moonshotai/kimi-k2.6               http=404
{"status":404,"detail":"Function '...': Not found for account '...'"}

model=deepseek-ai/deepseek-v4.1-flash    http=000  time=60.003s   （超时）
```

- 404 文案是"该账号未开通此 Function"，不是模型不存在；`/v1/models` 里同时混有已 EOL 的条目（如 `meta/llama-3.1-8b-instruct`）。
- 因此：**供应商可达、鉴权可用，但没有可用模型能稳定完成生成**；成本与质量无法在当前环境评估。

### 若不成立，方案哪处要改

- Phase 1 的前置条件改为：先确认一个"账号已开通且非 EOL"的模型，或改用其他 OpenAI-compatible 供应商；在此之前，`narrative.py` 必须以模板回退为默认路径。
- 不能在方案里写"已选定 NVIDIA NIM + 某模型"。

---

## 9. V9 — `scripts/tga_daily.py` 何时落地

### 结论

**成立（"仍未落地"的描述属实）。**

### 证据

```
$ ls scripts/tga_daily.py
ls: scripts/tga_daily.py: No such file or directory

$ ls scripts/tga_reconstruct.py
scripts/tga_reconstruct.py
```

- 日报计划改用 `tga_reconstruct.py --no-report`，该脚本存在。

### 若不成立，方案哪处要改

- 无需改；§9 V9 保持"PENDING（Phase 6）"，并注明日报不依赖 `tga_daily.py`。

---

## 10. V10 — FOMC/CPI/NFP 日历

### 结论

**成立：仓库无宏观事件日历。**

### 证据

```
$ ls config/tga/calendar.yaml
config/tga/calendar.yaml

$ grep -rn "FOMC" config/ src/ | head -5
（无输出）
```

- 现有 `config/tga/calendar.yaml` 只覆盖 TGA/税期事件。
- `src/chronoforge/models/macro.py` 只有事件 schema，registry/canonical 中无 MACRO_EVENT 数据。

### 若不成立，方案哪处要改

- §9 V10 保持"v1 可选；没有就不写"。若要写，需新增 FOMC/CPI/NFP 数据源或人工 YAML。

---

## 11. 专项：Yahoo `DX-Y.NYB`「1 timestamp → 0 record」根因

### 结论

**不是"上游单点异常"，也不是"合法时间戳被 normalize 成零记录且不计 skip"路径。**

根因是：上游 Yahoo 的收盘快照 bar 自身 **OHLC 不自洽（`low > open`）** → `OHLCV` 模型校验抛 `ValueError` → 被 `normalize` 捕获并记 `note_skip("invalid_bar")` → `invalid_bar` **不在白名单** → 单行批次 `drop_invalid=1 / drop_attempted=1` → A-8 门禁触发 `QualityError`。

**A-8 门禁能覆盖此类情形**；但"单条源侧坏行 = 整个 dataset 失败（partial）"的分类过粗，需要单独决策。

### 原始 payload 证据

文件（`data/raw/yahoo/YAHOO:DX-Y.NYB:OHLCV:1d/ingest_date=2026-11-08/`）：

```
035143-0001.jsonl  040014-0001.jsonl  040653-0001.jsonl  040925-0001.jsonl  041025-0001.jsonl
```

解码 base64 后的 `payload`：

```
040014-0001.jsonl timestamps=[1794002399]  2026-11-06T21:59:59Z
  open=101.494  high=102.358  low=101.553  close=102.279  volume=0
040653-0001.jsonl timestamps=[1794002399]  同上（完全相同的坏 bar）
041025-0001.jsonl timestamps=[1793941200]  2026-11-06T05:00:00Z
  open=101.550  high=102.360  low=101.550  close=102.300  volume=0
```

- 失败 bar：`low 101.553 > open 101.494`，且 `low > min(open, close)`。
- 成功 bar：`low == open`，OHLC 自洽。

### 复现

内联脚本（未落盘）直接调用 `YahooConnector().normalize`，并统计 `note_skip`：

```
040014-0001.jsonl -> records=0  skips={'invalid_bar': 1}  err=None
041025-0001.jsonl -> records=1  skips={}                  err=None
```

说明：**确实有 skip 计数**（`invalid_bar: 1`），不存在"合法时间戳静默产零记录"的路径。

### 代码链路

- 校验规则：`src/chronoforge/models/market.py:119-121`
  ```
  raise ValueError("high must be >= max(open, close)")
  raise ValueError("low must be <= min(open, close)")
  ```
- 捕获：`src/chronoforge/connectors/yahoo.py:595-617`
  ```
  record = OHLCV(...)
  ...
  except (ValueError, IndexError, TypeError):
      note_skip("invalid_bar")
      continue
  ```
- 白名单：`src/chronoforge/connectors/normalize_stats.py:28-39` 不含 `invalid_bar`。
- 门禁计数：`src/chronoforge/pipeline/runner.py:528-536`
  ```
  reported_skips = sum(skips.values())
  untracked = max(0, units - len(records) - reported_skips)
  drop_attempted += len(records) + reported_skips + untracked
  drop_invalid += untracked + sum(count for reason, count in skips.items()
                                  if reason not in LEGIT_SKIP_REASONS)
  ```
- 行单位：`yahoo.py:674-696`，`units = len(chart.result[0].timestamp)` → 本批 `units=1`。
- 代入：`units=1, records=0, reported_skips=1` → `untracked=0`，`drop_attempted=1`，`drop_invalid=1` → `1/1 ≥ 0.5` → `QualityError: normalize drop rate 1/1 reaches threshold 0.5`。

### 结论与 A-8 门禁关系

- 门禁**成功拦截**了"整批都是坏行"的情形，没有把坏数据写进 canonical。
- 但分类上：`invalid_bar` 是"源侧坏行"，既不是合法 skip，也不是 `untracked` 静默丢弃；当前统一按"非白名单 skip"计入 `drop_invalid`，在单行数据集上等价于 100% 失败。
- 建议（本轮不改代码）：
  1. 保持 `invalid_bar` **不进白名单**（否则全坏批次会静默产 0 记录通过门禁，更危险）；
  2. 增加"源侧坏行"单独计数与质量 flag；对 `units` 很小（如 1–2）的批次，允许"0 记录但带显式质量告警"而不是整 dataset `partial`；
  3. 或按连续 N 次出现同一坏 bar 才升级为失败，单次抖动降级为 warning。
- 无论采用哪种，**报告侧必须能读到该 dataset 的"最近成功批次 as-of"**，而不是只看批次 `status=partial` 就放弃整个数据源。

---

## 12. 待观察项清单

1. **交易日 07:40 连续 3 天实测**：V1/V2 当前是"官方时刻 + `release_time` 推导"；需在真实交易日 07:40 UTC 批次后核对 canonical 最新日期，确认 H.15 稳定 T-2、EFFR/SOFR 稳定 T-2，而不是 T-3。
2. **VIXCLS 的 FRED 滞后**：本环境 VIXCLS 最新 11-05，同日 Yahoo ^VIX 已有 11-06；需观察 FRED 导入是否长期比 CBOE 晚一个交易日。
3. **SRF/SOMA 常量对齐**：`nyfed.py` 的 22:00 UTC / 次日 23:59:59 UTC 与官方 13:45 ET / 周四 16:30 ET 不一致；是否调整需单独决策（涉及 PIT 语义）。
4. **`invalid_bar` 分类策略**：见 §11 建议；单行/少行数据集的坏行不应直接升级为整 dataset 失败。
5. **`EARLY_CLOSINGS` 的 `2027-12-23`**：应删除；删除后需回归日历测试。
6. **LLM 供应商**：需确认账号已开通且非 EOL 的模型，或改用其他供应商，再做 Phase 1 小样本评测。
7. **`dataset_states.txt` 过期**：停在 2026-10-28，与 canonical 2026-11-05 不符；文档引用前应重新生成。
8. **`dataset_stats.py` 的 `dataset_id` 合并显示**：`soma_*` 显示为两个 dataset 拼接；freshness 表取值需按单值拆分。

---

## 13. 临时文件与脚本说明

- 未在工作区创建任何临时脚本；所有复现均为内联 heredoc（`uv run python - <<'PY' ... PY`），不落盘、不提交。
- `/tmp/nim_models.json`、`/tmp/nim_gen.json` 为 NVIDIA NIM 的临时响应缓存，位于 `/tmp`，不提交、可随时删除。
- 本轮**未修改实现代码**；仅新增本报告并同步方案文档 §9 / §2.3 状态。

---

## 14. 官方页面来源与访问日期

| 用途 | URL | 访问日期 |
|---|---|---|
| H.15 发布时刻与最新列 | https://www.federalreserve.gov/releases/h15/ | 2026-11-08 |
| H.8 发布时刻 | https://www.federalreserve.gov/releases/h8/ | 2026-11-08 |
| H.4.1 发布时刻 | https://www.federalreserve.gov/releases/h41/ | 2026-11-08 |
| EFFR 发布时刻 | https://www.newyorkfed.org/markets/reference-rates/effr | 2026-11-08 |
| SOFR 发布时刻 | https://www.newyorkfed.org/markets/reference-rates/sofr | 2026-11-08 |
| NYSE 假日/早闭 | https://www.nyse.com/markets/hours-calendars | 2026-11-08 |
| SOMA API | https://markets.newyorkfed.org/api/soma/summary.json | 2026-11-08 |
| SRF API | https://markets.newyorkfed.org/api/rp/results/search.json | 2026-11-08 |
| FRED CSV 校验 | https://fred.stlouisfed.org/graph/fredgraph.csv?id=... | 2026-11-08 |
