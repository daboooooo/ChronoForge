# TGA 前瞻模型 — ChronoForge 落地实现方案

> **依据**：`docs/strategy/TGA前瞻模型.md`（下称"原规范"）
> **落地仓库**：`/Users/horsenli/Works/ChronoForge`（下称"本仓库"）
> **方案性质**：子系统实施设计，不是 PRD，不替代 D01–D10 冻结契约
> **版本**：v1.6（2026-11-04）
> **状态**：实施中；Phase 0/1/2/3 已完成（1/2：8 dataset 注册 + 全历史回填；2：Model 0 历史重建 + taxonomy + Derived + tga.duckdb 视图 + 异常工作流；3：基准预测 Model 1/2 + ForecastStore + 日报 MVP）。Phase 4（中期与情景）与 Phase 5（历史回测）已落地；Phase 6 部分完成。**v1.6 起模型定位由"点位预测"调整为"情景与事件解释"（见 §0.1）。**
> **v1.1 变更**：不新增 `src/chronoforge/tga/` 包；TGA 预测、回测、报告代码全部放 `scripts/tga_*.py`，`src/` 只新增 Connector、Canonical Type、质量规则、迁移等基础设施。
> **v1.2 变更**：修正 DTS 历史账户分期映射与余额自然键；统一 dataset_id 为下划线约定；修正 as-of 视图与 Derived catalog 机制；修正未分类现金流公式；补齐 `is_subtotal`、发布时刻、schema 漂移熔断与 Q-TGA-009 语义；同步当前实施状态与文件清单。
> **v1.3 变更**：Phase 1 回填完成（DTS 2005-10-03 起、拍卖 1979-10-31 起、upcoming 2024-03-08 起，canonical 合计约 72.8 万行）；修正 TIPS/FRN 判定（实测 `auctions_query` 的 TIPS/FRN 行 `security_type` 仍为 "Note"，需以 `inflation_index_security` / `floating_rate` 字段判定）；补充分页无重叠/缺口单测。
> **v1.4 变更**：Phase 2 完成。落地 `config/tga/taxonomy.yaml`（v1.0）、`scripts/tga_common.py`、`scripts/tga_store.py`（Derived 原子写 + `meta/tga.duckdb` 四视图 + `tga_anomaly`）、`scripts/tga_reconstruct.py`（三类残差 + Q-TGA-003/004/005/006/011/015 + Model 0 报告）。实测修正：①QueryService 视图按 canonical type 组织，同类型多 dataset 返回同一视图，禁止按 dataset 重复拼接；②Table II 的 Sub-Total 行（365 天）必须经 taxonomy `is_subtotal` 排除；③IIIB 的 `Public Debt (Issues|Redemptions) (Table III-A/IIIA)` 行是面值锚定行（= IIIA 合计），分量行合计才是现金口径；④DTS 整数百万舍入致 Table I/II 官方数字间存在 ±9M 累积差，残差验收采用 ±15M 舍入带 + 带外日全部落 anomaly（ledger_residual 带内 98.66%，带外 71 日集中于 2008-2011 危机时代）。验收 7/8 PASS（#3 见报告解释），新增依赖 pyyaml。
> **v1.5 变更**：Phase 3 完成。落地 `config/tga/calendar.yaml` + `model.yaml`（horizons 7/14/30、bands 简化带宽、组件开关）、`scripts/tga_forecast.py`（五组件：debt/interest/tax/outlay/other + 聚合器守恒 + Q-TGA-012/013/014 + run 编排幂等复用）、`scripts/tga_store.py` 扩展 ForecastStore（forecast_runs 元数据 + run_date/scenario 分区写 + `tga_forecast_components`/`tga_forecast` 视图）、`scripts/tga_report.py`（日报 MVP，八章节）、`tga_common.config_hash` 叠加 calendar/model。实测修正：⑤拍卖查询再次确认 v1.4 教训①——`auctions_results` 与 `upcoming` 返回同一视图，concat 即翻倍（赎回虚增至 5.0T），只查一次；⑥TGA CUSIP 在再发行（`is_reopening=true`）间复用，未偿本金 = 同 CUSIP 全部 RESULT 拍卖 `total_accepted` 求和（17W 原始 + 8W/4W 再发行），到期赎回按该口径计；⑦到期/付息/节奏外推日必须顺延到联邦工作日并落在预测网格（origin 后工作日），聚合器加网格外断言；⑧DTS 发布时间采用保守规则 `next_business_day(record_date) < origin`（21:00 UTC 发布跨入次日）。验收：1753 tests passed（新增 TGA forecast/report 单测 15 项）；ruff/mypy/lint-imports 全绿；实跑 run `c1498dc3fdf05c76-r2`（origin 2026-11-02，T+30 预测 770.5B）。实施后修正⑨：拍卖行 `source_id` 曾落成 CUSIP（entity 分区散成数万个），与 §4.3 注册声明 `entity_id=US_TREASURY` 不一致致 `dataset_stats.py` 判"无 canonical 数据"——已改回固定 `US_TREASURY` 并从 Raw 重放（results 68,612 / upcoming 637 规范化记录，自然键去重后 canonical 11,250 行），`match_datasets` 增加"无标识符 params 的类型级 dataset"第三级回退。
> **v1.6 变更**：模型定位正式由"点位预测"改为"情景与事件解释"。T+7～T+90 数值的角色是**情景锚点 / 条件路径**，不再作为点位预测或置信区间；对外表述解释余额变动的事件驱动与情景切换条件，并显式引用 DM/MAE 证据声明当前不优于 Naive、不可用于交易决策（0.4.0 重跑后 T+7 亦显著劣于 Naive）。`scenarios.yaml` 增加 `triggers` / `key_drivers` / `interpretation` / `usage` / `confidence`；`tga_forecast.py` 预测输出新增 `output_type=scenario` / `usage` / `confidence` / `drivers` 标签（只增列，历史 run 兼容）；`tga_report.py` 章节重构为"情景路径 + 驱动归因 + 切换条件"，结论不再出现点位预测口径。Phase 4/5 的既有内容不推翻。

---

## 0. 结论摘要

原规范要求的是一个"官方数据采集 → 历史现金流重建 → 未来现金流预测 → TGA 余额预测 → 历史回测 → 每日报告"的持续运行系统。本方案不新建独立 `tga-forecast/` 工程，也**不在 `src/` 下新增 `tga/` 包**：数据接入、Canonical Schema、质量规则、存储迁移继续放在现有 `src/chronoforge/` 分层；TGA 历史重建、预测、情景、回测、报告、导出全部作为 `scripts/tga_*.py` 脚本落地，复用现有 Connector / Raw / Canonical / Quality / Pipeline / Query / Registry 基础设施。

核心决策：

| # | 决策 | 理由 |
|---|---|---|
| D-TGA-01 | 不新建独立项目，也不新增 `src/chronoforge/tga/` 包；数据接入放现有 `connectors/models/quality/storage`，预测代码放 `scripts/tga_*.py` | 复用已有的采集、存储、质量、查询、调度与测试体系，同时避免在 `src` 内形成第二套业务包 |
| D-TGA-02 | 存储沿用 **Parquet + DuckDB + SQLite**，不引入 PostgreSQL | 本仓库冻结技术决策（架构 06）；SQLite 存运行元数据，Parquet 存事实与预测，DuckDB 做分析视图 |
| D-TGA-03 | 新增 5 个 Canonical Type，走受控契约变更 | DTS 现金流、账户余额、债务交易、拍卖、融资公告的语义无法用现有 `FLOW/NUMBER` 无损表达 |
| D-TGA-04 | **Table II 为现金台账，Table I 为余额锚与总量校验，Table IIIA/IIIB 只做交叉校验与归因，不相加** | 避免债务面值与现金口径重复计算；Table I 自身提供 "Total TGA Deposits/Withdrawals (Table II)" 行，可做逐日总量对账 |
| D-TGA-05 | 数据源拆为 `treasury_fiscal`（Fiscal Data API）与 `treasury_refunding`（财政部 HTML/PDF 公告） | JSON API 与网页文档的采集、解析、重试语义不同 |
| D-TGA-06 | 预测输出放 Derived 层；`forecast_runs` / `backtest_runs` 放 SQLite 元数据 | 模型输出不是源事实；运行版本与输入快照需要事务化元数据 |
| D-TGA-07 | 全链路 **release_time 点时账本**；历史回填的发布时间标记 `ESTIMATED` | DTS 官方 API 不提供历史 vintage；回测必须显式区分"实际抓取时间"与"估算发布时间" |
| D-TGA-08 | P0 默认模型为 **事件驱动 + 季节性基准**；统计模型只在样本外优于基准后启用 | 原规范禁止用复杂模型替代确定性现金流 |
| D-TGA-09 | 不新增 launchd/cron 系统服务，接入现有前台调度循环 | 与 `scripts/hourly_update.sh` 的本地优先运维方式一致 |
| D-TGA-10 | 税收日历采用"年度人工复核 YAML + 历史税峰校验"，不在运行时爬 IRS 网页 | IRS 无稳定 API；避免不可复现的网页解析 |
| D-TGA-11 | 情景只作用于预测类现金流，**绝不调整已知现金流** | 防止把情景假设伪装成已公布事实 |
| D-TGA-12 | 不确定性来自 Walk-forward 残差分位数，不主观给置信度 | 原规范要求置信度可计算、可回测 |
| D-TGA-13 | `src` 只承载"数据基础设施"，TGA 业务逻辑（重建/预测/情景/回测/报告）只放 `scripts` | 与现有 `scripts/fdam_attention.py` 等策略模型的组织方式一致；避免预测代码渗透进 `src` 分层 |
| D-TGA-14 | DTS Table I/II 历史账户命名按三段映射：`Federal Reserve Account`（≤2021-09-30）与单行 `Treasury General Account (TGA)`（2021-10-01–2022-04-15）均视为历史 TGA；单行拆 `OPENING`/`CLOSING` 两条记录 | 否则 2005–2021 历史被整体跳过，Phase 1「≥10 年」与 Phase 5 训练窗口无法成立 |
| D-TGA-15 | `dataset_id` 统一采用 `{source_id}_{dataset_type}` 下划线格式（与现网 registry、`pipeline/windows.py` 前缀一致）；D03 冻结点号格式同步修订 | 避免方案、D03、注册脚本、窗口配置四套命名 |
| D-TGA-16 | TGA Derived 视图放独立 `meta/tga.duckdb`，由 `scripts/tga_store.py` 独占读写；不写 `meta/query.duckdb` | `open_query_service()` 稳态为只读连接，无法执行 `CREATE VIEW`；READY-005 只检查 canonical 视图 |
| D-TGA-17 | 日报与回测报告写 `reports/daily/`、`reports/tga/`；`output/` 继续只放运行产物 | `output/` 已被 `.gitignore` 忽略，日报需可入库审阅 |
| D-TGA-18 | `is_subtotal` 作为 `TREASURY_CASH_FLOW` 一等字段；Phase 1 connector 内置 taxonomy `builtin-v1` 仅过渡，Phase 2 迁移至 `config/tga/taxonomy.yaml` | 小计行排除需可审计；避免业务常量长期沉淀在 `src` |

### 0.1 模型定位：情景与事件解释（v1.6，2026-11-04）

**定位**：TGA 前瞻模型输出的是**情景锚点 / 条件路径**，不是 T+7～T+90 的点位预测，也不是可用于交易的置信区间。三档情景回答的是"余额变动由哪些事件驱动、在什么条件下会换挡"，而不是"某天余额会是多少"。

**目标**

- 解释余额变动由哪些事件驱动：发行、赎回、税收、福利、利息、其他。
- 说明在每个情景假设下，事件节奏把余额带向哪条条件路径。
- 给出情景切换的触发条件，让读者判断"现在按哪个情景读数、什么信号会换挡"。

**非目标**

- 不给出到期日 TGA 点位；不输出情景概率；不自动判定当前必然处于哪个情景。
- 不把 T+7～T+90 数值当作置信区间或交易信号。
- 不替代财政部官方预测，不用于交易决策与流动性择时。

**使用边界（引用 §7 样本外证据）**

- 主模型 vs Naive（0.4.0 重跑，backtest run `fe7a9c621dc3dc4b`，2023-11-01→2026-10-29 / stride 5）：T+7 平均损失差 +16.0B（DM 3.39，Bonferroni p=0.0138，**显著更差**）；T+14 +54.2B（p_Bonf=0.0000）、T+30 +153.7B（0.0000）、T+60 +265.0B（0.0000）、T+90 +372.9B（0.0000）——**全档显著更差**。T+7 对 seasonal_naive 为 +17.3B（p_Bonf=0.178，不显著）。
- 默认 MAE（USD B，0.4.0）：T+7 81.5 / T+14 138.0 / T+30 260.3 / T+60 383.4 / T+90 499.0；Naive 为 65.5 / 83.8 / 106.6 / 118.4 / 126.1，Seasonal Naive 为 64.2 / 83.9 / 127.9 / 179.7 / 214.2。各档全面高于 Naive。
- 结论：模型当前只用于情景与事件解释；0.4.0 起 T+7 也在 Bonferroni 校正后显著劣于 Naive，所有档位都不构成可用预测。
- 机器可读产物带 `output_type=scenario` / `usage=scenario_anchor` / `confidence=exploratory` / `drivers`；下游必须检查 `output_type`，不得按点位预测消费（§8.2）。

交付节奏：Phase 0 研究/契约变更 → Phase 1 数据接入 → Phase 2 历史重建（Model 0）→ Phase 3 基准预测（Model 1/2）→ Phase 4 中期与情景 → Phase 5 回测 → Phase 6 自动化与报告。每个 Phase 有独立退出条件，未通过不得进入下一阶段。

---

## 1. 现状与差距

### 1.1 本仓库已有能力（可直接复用）

| 能力 | 现状 | 对 TGA 的价值 |
|---|---|---|
| Connector 协议 | `connectors/base.py`：`fetch/normalize/validate/checkpoint`，分页内聚 | 新数据源只需实现协议，不重写采集框架 |
| Raw 层 | JSONL append-only，按 `ingest_date` 分区，保留 `fetch_time`/URL | 天然形成"抓取时刻"证据，可用于点时回测 |
| Canonical 层 | Parquet + merge-rewrite upsert，自然键去重，drift 检测 | DTS/拍卖数据的增量更新与重复合并 |
| Quality | `Q-xx-nnn` 规则注册、`quality_flags` 留痕、禁止静默修正 | 直接扩展 TGA 规则 |
| Pipeline | 七阶段 + checkpoint + 熔断 + replay | 新数据集注册后即可跑增量/回填 |
| Query | DuckDB 视图 + as-of 视图 + QueryService | 预测引擎只读查询，不直接读 Parquet |
| Feature/Research | FeatureEngine、ResearchSnapshot | 预测运行可锁定依赖版本，保证可复现 |
| Registry | `source_registry` / `dataset_registry` / `run_log` | 新源与数据集登记、状态推导、运行审计 |
| 脚本模式 | `scripts/*.py` 直接运行策略/分析任务（如 `fdam_attention.py`） | TGA 预测/回测/报告放 `scripts/tga_*.py`，不新增 CLI |
| 测试 | unit/integration/architecture/quality/fixtures | 新类型与连接器沿用同一测试体系 |

### 1.2 当前剩余缺口

1. `treasury_fiscal` 已落地；`treasury_refunding` connector 与 source 注册尚未落地（Phase 4）。
2. 5 个 Treasury Canonical Type 已落地（见 §4.1）；`treasury_refunding` 所需公告解析尚未实现。
3. TGA 历史重建、基准预测（Model 1/2）、日报已实现；**60/90 天、多情景、回测、流动性导出、每日自动化尚未实现**（Phase 4/5/6）。
4. `release_time` 已存在；DTS 侧发布时刻估算已在重建/预测脚本闭环（保守规则）；拍卖/MTS/Refunding 的 PIT 过滤在 Phase 5 回测中全面接入。
5. `forecast_runs` / `backtest_runs` / `tga_anomaly` 表已由迁移 0005 创建；ForecastStore 已实现（forecast_runs + 预测 Derived）；`backtest_runs` 读写待 Phase 5。
6. Q-TGA-001/002/003/004/005/006/007/008/009/011/012/013/014/015 已落地；Q-TGA-010 待 `treasury_refunding`（Phase 4）。
7. `treasury_fiscal` 的 8 个 dataset 已注册；历史回填已完成（2026-11-01），增量采集待接入每日调度（Phase 6）。

### 1.3 原规范 → 本仓库映射

| 原规范 | 本方案落地 |
|---|---|
| 独立目录 `tga-forecast/` | 数据接入/模型放现有 `src/chronoforge/` 分层；预测/回测/报告放 `scripts/tga_*.py` |
| PostgreSQL 或 SQLite | Parquet（Raw/Canonical/Derived）+ SQLite（元数据）+ DuckDB（视图） |
| `raw_dts` / `raw_auction` / `raw_refunding` 表 | 现有 Raw JSONL 分区 + 数据集注册；不再建三张原始表 |
| `daily_tga_balance` | Derived 数据集 `tga_daily_balance` |
| `daily_cash_flows` | Canonical `TREASURY_CASH_FLOW` + Derived `tga_daily_cashflow` |
| `treasury_security_cashflows` | Derived `tga_event_calendar`（由拍卖/到期/付息日历生成） |
| `forecast_runs` / `forecast_cash_flows` / `forecast_tga` / `forecast_evaluation` | SQLite `forecast_runs` + Derived `tga_forecast_components` / `tga_forecast` / `tga_forecast_eval` |
| `systemd timer` / `cron` | 现有 `scripts/hourly_update.sh` 前台循环 + `scripts/tga_daily.py --due-only` 到期闸门 |
| Plotly 可视化 | P0 Markdown 表格；P1 可选 Plotly（不阻塞核心交付） |

### 1.4 当前实施状态（2026-11-02，Phase 3 后）

| 范围 | 状态 | 证据/落点 |
|---|---|---|
| Phase 0 探测 | 完成 | `scripts/tga_probe.py`、`docs/tga/DATA_DICTIONARY.md`、`docs/tga/data_coverage.json`、`tests/fixtures/treasury_fiscal/` |
| 5 个 Canonical Type | 完成 | `models/treasury.py`、`identity.py`、`storage/canonical.py::_TYPE_FIELDS` |
| Q-TGA-001/002/007/008/009 | 完成 | `quality/rules.py`（已注册） |
| 迁移 0005 | 完成 | `0005_forecast_metadata.py` + `migrations/__init__.py` |
| `treasury_fiscal` connector | 完成（含历史分期映射、5 天回看、schema 漂移熔断、TIPS/FRN 字段判定） | `connectors/treasury_fiscal.py`、`pipeline/windows.py`、`cli/_wiring.py` |
| 数据集注册 | 完成（8 个 dataset） | 本地 `meta/chronoforge.db` 已注册 DTS 6 表 + 拍卖 2 表 |
| 历史回填 | 完成（2026-11-01） | DTS Table I/II/IIIA/IIIB/V 2005-10-03 起、Table IV 2023-02-14 起、拍卖结果 1979-10-31 起、upcoming 2024-03-08 起；canonical 合计 727,740 行；8 个 dataset 状态推导为 COMPLETE |
| Phase 2 历史重建 | 完成（2026-11-02） | `config/tga/taxonomy.yaml` + `scripts/tga_common.py` / `tga_store.py` / `tga_reconstruct.py`；Derived 四数据集（balance 5,294 日 / cashflow 498,942 / recon 5,294 / debt 118,224）+ `meta/tga.duckdb` 四视图；`tga_anomaly` 工作流；Model 0 报告 `reports/tga/`，验收 7/8 PASS |
| `treasury_refunding` | 未开始 | Phase 4 |
| `config/tga/calendar|scenarios|model`、`scripts/tga_forecast|backtest|report|daily|export_liquidity` | 部分完成（Phase 3） | `calendar.yaml`/`model.yaml`、`tga_forecast.py`、`tga_report.py` 已落地；`scenarios.yaml`、`tga_backtest/report 完整版/daily/export_liquidity` 未开始（Phase 4/5/6） |
| Phase 3 基准预测 | 完成（2026-11-02） | 五组件预测引擎 + 聚合守恒 + ForecastStore + 日报 MVP；实跑 run `c1498dc3fdf05c76-r2`（0.3.0 旧逻辑，受 TGA-1 去重坍缩影响，数值不可比）；0.4.0 已修复 TGA-1 并重跑（当日 origin 2026-11-05：T+7 846.80B / T+14 667.06B / T+30 828.77B / T+60 1,000.80B / T+90 633.24B，run `3349f9d395b9dd56-r2`）；详见 §12 Phase 3 |
| 测试 | 通过 | 1782 passed（含 TGA taxonomy/重建/预测/报告单测 39 项）；ruff（src/tests）/mypy/lint-imports 全绿，见 §11.6 |

---

## 2. 总体架构

### 2.1 分层与数据流

```text
┌──────────────────────────────────────────────────────────────────┐
│ Applications: CLI / Daily Report / Liquidity JSON Export          │
├──────────────────────────────────────────────────────────────────┤
│ TGA Analytics (scripts/tga_*.py): reconstruction / forecast      │
│                / scenarios / backtest / attribution / reporting   │
├──────────────────────────────────────────────────────────────────┤
│ Query: DuckDB views + QueryService (as-of / release_time filter)  │
├──────────────────────────────────────────────────────────────────┤
│ Canonical: TREASURY_* 5 types (+ existing NUMBER/FLOW for MTS)    │
├──────────────────────────────────────────────────────────────────┤
│ Quality: Q-TGA-xxx rules + quality_flags + tga_anomaly workflow   │
├──────────────────────────────────────────────────────────────────┤
│ Ingestion: treasury_fiscal / treasury_refunding connectors         │
├──────────────────────────────────────────────────────────────────┤
│ Storage: Raw JSONL / Canonical Parquet / Derived Parquet / SQLite │
├──────────────────────────────────────────────────────────────────┤
│ Sources: Fiscal Data API, Treasury.gov, FRED (H.4.1 校验)          │
└──────────────────────────────────────────────────────────────────┘

数据流（单向、可重建）：
SOURCE → RAW → CANONICAL → DERIVED(重建/事件日历) → FORECAST → REPORT
                                      ↑
                              quality_flags / anomalies
```

### 2.2 新增模块

```text
# ── src/：只放数据基础设施，不放 TGA 业务逻辑 ─────────────────────────
src/chronoforge/
├── models/
│   └── treasury.py                    # 5 个新 Canonical Type
├── connectors/
│   ├── treasury_fiscal.py             # DTS / MTS / auctions / upcoming
│   └── treasury_refunding.py          # Quarterly Refunding 文档
├── quality/
│   └── rules.py                       # 在现有规则表中追加 Q-TGA-xxx
├── storage/
│   ├── migrations/0005_forecast_metadata.py  # forecast_runs/backtest_runs/tga_anomaly
│   ├── migrations/__init__.py                # 同步注册 MIGRATION_0005_SQL
│   └── views.py                       # canonical 基础视图 + 通用 as-of 视图
├── pipeline/
│   └── windows.py                     # treasury_fiscal_dts_/auction_ 前缀的 5 天 overlap/30 天 chunk
├── registry/
│   └── service.py                     # DEFAULT_SOURCES 增加 2 个源
├── cli/
│   └── _wiring.py                     # build_connector 增加 treasury_fiscal 分支
└── config/
    └── update_schedule.py             # 增加 2 个源的发布时刻

# ── scripts/：TGA 重建、预测、回测、报告全部在此 ─────────────────────
scripts/
├── tga_common.py                      # 单位、taxonomy、日历、配置、Query/Store 公共函数
├── tga_probe.py                       # Phase 0 数据可行性探测
├── register_treasury_datasets.py      # 数据集登记
├── tga_store.py                       # ForecastStore + Derived Parquet + DuckDB 视图
├── tga_reconstruct.py                 # Model 0：余额/现金台账/债务口径/残差
├── tga_forecast.py                    # Model 1–4：事件/税收/发行/到期/付息/支出/情景/区间
├── tga_backtest.py                    # Walk-forward + 指标 + 误差归因
├── tga_report.py                      # Daily Markdown + JSON
├── tga_export_liquidity.py            # 对外流动性接口 JSON
└── tga_daily.py                       # 每日编排（采集→重建→预测→报告）

config/tga/
├── taxonomy.yaml
├── calendar.yaml
├── scenarios.yaml
└── model.yaml

docs/tga/
├── DATA_DICTIONARY.md
├── data_coverage.json                 # Phase 0 probe 输出
├── MODEL_METHODOLOGY.md
├── OPERATIONS.md
├── BACKTEST.md
└── DECISIONS.md

tests/
├── unit/test_tga_*.py
├── integration/test_tga_*.py
└── fixtures/treasury_fiscal/...       # 脱敏 API 响应
```

> 约束：`src/` 内不出现 `tga/` 目录；TGA 业务函数不写入 `src`。`scripts/tga_common.py` 只做公共函数，不替代 `src` 的采集/存储/模型职责。
> 说明：`models/__init__.py` 无需变更（全仓库直接从 `chronoforge.models.treasury` 导入）；`views.py` 的 `REVISION_TYPES` 已改为引用 `models/identity.py`，避免副本漂移。

### 2.3 依赖边界

- `src/chronoforge/` 内部继续遵守现有 `.importlinter` 分层；**不新增 `chronoforge.tga` 层**，也不新增 `src/chronoforge/tga/`。
- `scripts/` 是应用层，可直接 import `chronoforge.models/storage/quality/registry/research/pipeline`，但不把业务函数回写进 `src`；脚本不进入 wheel 打包，按现有 `uv run python scripts/...` 方式运行。
- 脚本通过 `chronoforge.cli._wiring.open_meta()` / `open_query_service()` 与 `Settings.load()` 获取元数据/查询连接，与 `scripts/fdam_attention.py` 的现有写法一致；不自行拼接 `data_dir/meta_dir` 或新建 SQLite 连接。
- 例外：`scripts/tga_store.py` 使用 `Settings.load()` 的 `data_dir`/`meta_dir` 打开专用 `meta/tga.duckdb`（Derived 视图），仍不新建 SQLite 连接。
- 预测/回测脚本只读 `QueryService`/DuckDB 视图或 `scripts/tga_store.py` 的只读封装，**不直接读 Raw/Canonical Parquet**；采集通过现有 `chronoforge pipeline run`，不在预测脚本里调用 Connector。
- `QueryService` 不新增 TGA 分支：canonical 数据集用 `service.query(dataset_id=...)`；TGA Derived 查询走 `scripts/tga_store.py` 打开的独立只读连接（`meta/tga.duckdb`），不通过 `open_query_service()` 注册/改写视图。两者都不写 `src` 业务逻辑。
- `tga_probe.py` 是唯一允许直接访问官方 API 的脚本（Phase 0 只读探测），其输出只写 `docs/tga/` 与 fixtures，不写生产数据目录。
- 新增 Architecture Test（`tests/architecture/test_tga_boundaries.py`，已落地）：断言 `src/chronoforge` 下不存在 `tga/` 目录；断言 `scripts/tga_*.py` 不 import `chronoforge.connectors`；断言 `src` 不 import `scripts`。`.importlinter` 不覆盖 `scripts`，因此用 AST 扫描实现。

---

## 3. 数据源接入

### 3.1 源清单与优先级

| 优先级 | source_id | 官方入口 | 用途 | 频率 | 本方案阶段 |
|---|---|---|---|---|---|
| P0 | `treasury_fiscal` | Fiscal Data API | DTS、拍卖、MTS | 日/按公告/月 | Phase 1 |
| P0 | `treasury_refunding` | treasury.gov Quarterly Refunding | 融资计划、现金余额假设 | 季度 | Phase 4 |
| P1 | `fred`（已有） | FRED `WTREGEN` | H.4.1 TGA 周度交叉校验 | 周 | Phase 2/4 |
| P2 | `usaspending` | USAspending API v2 | 支出结构辅助分析 | 按数据集 | Phase 4+ |
| P2 | IRS 税期 | IRS Pub 509 / 官方日历 | 税期事件 | 年度 | Phase 3（人工复核 YAML） |

> 原规范把 IRS 列为 P2，但税收预测是 P0 核心，因此本方案把"税期日历"前移到 Phase 3，以**版本化 YAML** 形式维护，而不是新建连接器。

### 3.2 Fiscal Data API 通用接入规范

**Base URL**：

```text
https://api.fiscaldata.treasury.gov/services/api/fiscal_service/
```

**必须遵守**：

1. 分页：`page[size]` + `page[number]`，循环到 `meta.total-pages`；不得只取前 100 条。
2. 过滤：`filter=record_date:gte:YYYY-MM-DD,record_date:lte:YYYY-MM-DD`；排序 `sort=record_date`（升序，保证 cursor 可重复）。
3. 元数据/字段校验：Phase 0 读取 `meta.labels` / `meta.dataTypes` / `meta.dataFormats` 写入数据字典；运行时由 `treasury_fiscal.py::_REQUIRED_FIELDS` 对每页首行做最小必需字段校验，字段缺失或更名 → `SchemaError` 熔断，禁止猜测补字段。
4. 所有金额保留原始值与原始单位；normalize 后统一为 **USD Million**。
5. 响应原样写入 Raw JSONL（含 URL、`fetched_at`、`http_status`、分页 meta），不做清洗。
6. 限流：`RateLimiter(rate=60/60 per second, burst=10)`；429 尊重 `Retry-After`，5xx 指数退避；4xx（非 429）不重试。
7. 采集日志：`run_log` 记录 `request_count/retry_count/duplicate_count/missing_count`，沿用现有七阶段。
8. 数据状态：`dataset_registry.status` 按现有 D1 规则推导；DTS 缺失工作日记 `STALE`，不静默补零。

**字段映射组织**：`connectors/treasury_fiscal.py` 内维护 `_ENDPOINTS`（表名 → 端点）、`_TABLE_CANONICAL_TYPE`（表名 → Type）、`_REQUIRED_FIELDS`（漂移熔断字段集），每张表由独立 normalizer 处理；字段名以 Phase 0 探测结果为准。

### 3.3 DTS 各表（已实测字段，实施前仍需复核）

实测时间：2026-10-31；以下字段名来自官方 API 响应与 `meta.labels`，Phase 0 必须写入 `docs/tga/DATA_DICTIONARY.md` 并附样例。

#### Table I — `operating_cash_balance`

**实测账户命名分三段（2026-10-31 复核）**：

| 期间 | 余额行 | 金额字段 | 映射 |
|---|---|---|---|
| ≤2021-09-30 | `Federal Reserve Account` | `open_today_bal` + `close_today_bal` 均有值 | 历史 TGA 单行：拆 `OPENING`/`CLOSING` 两条 |
| 2021-10-01–2022-04-15 | `Treasury General Account (TGA)` | 同上 | 历史 TGA 单行：拆 `OPENING`/`CLOSING` 两条 |
| ≥2022-04-18 | 四行 `TGA Opening / Total TGA Deposits / Total TGA Withdrawals / TGA Closing` | `close_today_bal=null`，金额在 `open_today_bal` | 按行映射为 `OPENING/TOTAL_DEPOSITS/TOTAL_WITHDRAWALS/CLOSING` |

`Tax and Loan Note Accounts (Table V)`、`Supplementary Financing Program Account`、`Short-Term Cash Investments` 不计入 TGA。

实测关键字段：

| 字段 | 含义 | 备注 |
|---|---|---|
| `record_date` | 记录日期 | 与 DTS 报表日一致 |
| `account_type` | 账户/行类型 | 三段命名见上表；**不得只匹配字符串 "TGA"**，否则丢失 2005–2021 历史 |
| `open_today_bal` | 当日开盘/行金额 | ≥2022-04-18 的 Closing/Total 行把余额放在此字段 |
| `close_today_bal` | 当日收盘金额 | 对 ≥2022-04-18 的 TGA 行可能为空；历史单行期间非空 |
| `open_month_bal` | 月初余额 | 用于月度校验 |
| `open_fiscal_year_bal` | 财年初余额 | 用于财年校验 |
| `src_line_nbr` | 源行号 | 排序与自然键辅助 |
| `table_nbr` | 表号 | `I` |

**处理规则**：

1. 不使用"所有 Operating Cash = TGA"的假设；按上表三段映射，`Federal Reserve Account` 是 TGA 的历史行名。
2. 余额序列取 TGA Opening / Closing；历史单行期间同一源行拆成两条，`balance_type` 必须参与自然键；Deposits/Withdrawals 总量行只用于对账，不参与余额累加。
3. ≥2022-04-18 的 Closing/Total 行优先取 `close_today_bal`，为空时取 `open_today_bal`；历史单行期间分别取 `open_today_bal`/`close_today_bal`。
4. 单位：DTS 官方口径为 **USD millions**；Phase 0 用 Fed H.4.1 周度 TGA 量级交叉确认。
5. 连续性校验：`Opening_t ≈ Closing_{t-1}`；周末/假日允许"无交易日"延续，记录 `EXPECTED_GAP`。
6. 回归样例固定三段：2005-10-03（Federal Reserve Account）、2021-12-31（单行 TGA）、2026-10-29（四行 TGA）。

#### Table II — `deposits_withdrawals_operating_cash`

实测关键字段：

| 字段 | 含义 |
|---|---|
| `record_date` | 记录日期 |
| `account_type` | 账户类型：2021-10 前为 `Federal Reserve Account`，之后为 `Treasury General Account (TGA)`；两者都视为 TGA |
| `transaction_type` | `Deposits` / `Withdrawals` |
| `transaction_catg` | 原始交易分类（如 `Dept of Agriculture (USDA) - misc`、`DoD - Health`） |
| `transaction_catg_desc` | 分类描述，可能为 `null` |
| `transaction_today_amt` | 当日金额（USD millions） |
| `transaction_mtd_amt` | 月累计 |
| `transaction_fytd_amt` | 财年累计 |
| `src_line_nbr` | 源行号（用于排序/层级判断） |
| `table_nbr` | `II` |

**处理规则**：

1. **Table II 是 TGA 现金台账主表**；每日净现金流来自其叶级分类之和。`account_type` 同时接受 `Federal Reserve Account`（历史）与 `Treasury General Account (TGA)`。
2. 原始分类名全部保留；通过 `taxonomy.yaml` 映射为归一分类；未命中 → `UNCLASSIFIED`，不得丢弃。
3. 识别小计/合计行：DTS API 不提供父子层级字段，需按 `src_line_nbr` 排序 + 金额等于 Table I 总量行 + 人工复核建立 `is_subtotal` 列表。小计行不进入叶级求和，避免重复计算。
4. `transaction_mtd_amt` / `fytd_amt` 只用于对账，不作为日频现金流。
5. 退款（Table V）与内部转移（Table IV）在 Table II 可能已反映；凡跨表出现的分类，标 `is_duplicate_candidate=true`，`include_in_net=false`，只做分析。
6. 单位：USD millions。

#### Table IIIA — `public_debt_transactions`

实测关键字段：

| 字段 | 含义 |
|---|---|
| `record_date` | 记录日期 |
| `transaction_type` | `Issues` / `Redemptions` |
| `security_market` | `Marketable` / `Nonmarketable` |
| `security_type` | `Bills` / `Notes` / `Bonds` / `Inflation-Protected Securities Increment` / `Government Account Series` 等 |
| `security_type_desc` | `Regular Series` / `Cash Management Series` 等 |
| `transaction_today_amt` | 当日金额（USD millions） |
| `transaction_mtd_amt` / `transaction_fytd_amt` | 月/财年累计 |
| `src_line_nbr` | 源行号 |
| `table_nbr` | `IIIA` |

**处理规则**：

1. IIIA 是**债务面值/交易口径**，不是现金结算口径；**禁止直接加到 TGA 净现金流**。
2. 只保留可映射到现金口径的类别；`Government Account Series` 等内部债务默认 `is_internal=true`，不计入 TGA。
3. 用 Table IIIB 做现金口径调整，再与 Table II 的债务类现金流交叉校验；差异写入质量报告。
4. 单位：USD millions。

#### Table IIIB — `adjustment_public_debt_transactions_cash_basis`

实测关键字段：

| 字段 | 含义 |
|---|---|
| `record_date` | 记录日期 |
| `transaction_type` | `Issues` / `Redemptions` |
| `adj_type` | 调整类型，实测含 `Public Debt Issues (Table IIIA)`、`Premium on New Issues`、`Bills (-) Discount on New Issues`、`Bonds and Notes (-) Discount on New Issues` |
| `adj_type_desc` | 调整说明，可能为 `null` |
| `adj_today_amt` | 当日调整金额 |
| `adj_mtd_amt` / `adj_fytd_amt` | 月/财年累计 |
| `src_line_nbr` | 源行号 |
| `table_nbr` | `IIIB` |

**处理规则**：

1. 建立 `cash_basis = IIIA + IIIB` 的逐日验证视图，符号以实测数据为准，不凭经验假设。
2. IIIB 的 `adj_type` 分类必须完整枚举；未知 `adj_type` → 质量告警，不丢弃。
3. 现金口径结果只用于**校验/归因**，不重复计入 Table II 已记录的净现金流。

#### Table IV — `inter_agency_tax_transfers`

实测字段：`record_date`、`classification`、`today_amt`、`mtd_amt`、`fytd_amt`、`src_line_nbr`、`table_nbr`（`IV`）。

处理：标记 `is_internal_transfer=true`、`include_in_net=false`；用于解释 Table II 中机构间转移的构成。实测覆盖仅 2023-02-14 起（3724 行），2023 年前不能用于内部转移解释。

#### Table V — `income_tax_refunds_issued`

实测字段：`record_date`、`tax_refund_type`（如 `Taxes - Individual Tax Refunds (Checks)` / `(EFT)`）、`tax_refund_today_amt`、`tax_refund_mtd_amt`、`tax_refund_fytd_amt`、`src_line_nbr`、`table_nbr`（`V`）。

处理：映射为 `TREASURY_CASH_FLOW` 的 `REFUND` 类，`is_duplicate_candidate=true`（Table II 已含退款支出），只用于退款季节性分析与交叉校验。

### 3.4 Treasury Securities Auctions（同一 Fiscal Data API）

**历史拍卖结果**：`/v1/accounting/od/auctions_query`

实测关键字段：

| 字段 | 含义 | 单位/备注 |
|---|---|---|
| `cusip` | CUSIP | 字符串 |
| `security_type` | `Bill` / `Note` / `Bond` / `TIPS Note` / `FRN Note` 等 | 字符串 |
| `security_term` | 期限描述 | 如 `4-Week`、`5-Year` |
| `announcemt_date` | 公告日期 | 官方字段拼写就是 `announcemt_date`，不要"修正" |
| `auction_date` | 拍卖日期 | |
| `issue_date` | 发行结算日期 | 现金流按此日计入 |
| `maturity_date` | 到期日期 | |
| `offering_amt` | 公告发行规模 | **USD（原始）** |
| `total_accepted` | 实际接受金额 | **USD**；结果发布前可能为 `null` |
| `total_tendered` | 投标金额 | **USD** |
| `int_rate` | 票面利率 | 百分比 |
| `high_yield` | 最高收益率 | 百分比 |
| `int_payment_frequency` | 付息频率 | `Semi-Annual` / `None` 等 |
| `reopening` | 是否再发行 | `Yes` / `No` |
| `cash_management_bill_cmb` | 是否 CMB | `Yes` / `No` |
| `price_per100` | 每 $100 价格 | 字符串；结果发布前可能为 `null` |
| `currently_outstanding` | 当前未偿 | USD；可能为 `null` |
| `record_date` | 记录日期 | |

**未来已公告拍卖**：`/v1/accounting/od/upcoming_auctions`

实测字段：`security_type`、`security_term`、`reopening`、`cusip`、`offering_amt`（USD）、`announcemt_date`、`auction_date`、`issue_date`、`record_date`。

**关键限制（必须写入文档）**：

1. `upcoming_auctions` 实测仅覆盖 **2024-03-08 起**（约 101 条），不能用于 2024 年以前的历史回测。
2. 历史回测的"当时已公告计划"用 `auctions_query` 的 `announcemt_date` + `offering_amt` 重建；结果发布前只能用 `offering_amt`，不能用 `total_accepted`。
3. 近期拍卖记录实测 `total_accepted`/`high_yield`/`price_per100` 可能为 `null`；必须以 `data_status` 区分 ANNOUNCED / RESULT。
4. 单位是 **USD**，不是百万美元；normalize 时除以 `1e6`。
5. 拍卖结果可能在拍卖日收盘后更新；增量采集需按 `record_date` 回看至少 5 个工作日。实现落点：`pipeline/windows.py` 对 `treasury_fiscal_dts_` / `treasury_fiscal_auction_` 前缀配置 `overlap_seconds=5*86400`、`chunk_size_seconds=30*86400`。

### 3.5 Quarterly Refunding

入口：`https://home.treasury.gov/policy-issues/financing-the-government/quarterly-refunding`

采集对象：

- Quarterly Refunding Statement
- Treasury Borrowing Estimates（本季度/下一季度）
- Financing Requirements
- Treasury Marketable Borrowing Estimates
- Treasury Bill / Coupon Issuance Plans
- 现金余额假设（cash balance assumption）

实现方式：

1. `treasury_refunding` connector 抓取索引页与文档链接；HTML/PDF 原样落盘。
2. HTML 可解析为文本与表格；PDF 提取文本 + `sha256`，原始 PDF 存 `data/raw/treasury_refunding/documents/{sha256}.pdf`，JSONL 记 URL/抓取时间/哈希。
3. 结构化字段写入 `TREASURY_ANNOUNCEMENT`，带 `extraction_method`（HTML_TABLE / PDF_TEXT / MANUAL）与 `extraction_confidence`。
4. **禁止覆盖历史版本**：同一 forecast period 的多次公告按 `revision_time` 追加；回测按 `publication_time <= cutoff` 过滤。
5. 解析失败 → `Q-TGA-010` 告警 + 保留原文；不得用 LLM 猜测数字写入核心表；如需人工修正，走 `config/tga/refunding_overrides.yaml` 并记录理由。

### 3.6 MTS、IRS、USAspending、Fed H.4.1

| 源 | 用法 | 落地方式 |
|---|---|---|
| MTS | 月度收入/支出校验、月度趋势约束、季节性基准 | `treasury_fiscal` 采集 `mts_table_*`，映射到现有 `FLOW`（不新增类型） |
| IRS 税期 | 税收事件日历 | `config/tga/calendar.yaml` 年度复核；用 DTS 税峰回验，不运行时爬取 |
| USAspending | 支出结构辅助 | P2；不得直接替代 DTS 现金支出 |
| Fed H.4.1 | TGA 周度交叉校验 | 复用现有 `fred` connector，注册 `WTREGEN`；仅校验，不进入日频预测输入 |

MTS → `FLOW` 的字段映射（Phase 0 以官方字段复核）：

| FLOW 字段 | MTS 映射 |
|---|---|
| `source_id` | `classification_id`（同一表内唯一） |
| `observation_time` | `record_date`（月末） |
| `period_start` / `period_end` | 月初 / `record_date` |
| `release_time` | 官方 MTS 发布日（配置化发布日历） |
| `revision_time` | 该次发布的日期；后续修订追加新 vintage |
| `vintage_date` | 发布日字符串 |
| `value` | `current_month_rcpt_outly_amt / 1e6`（USD → USD Million） |
| `units` | `USD_MILLIONS` |
| `seasonal_adjustment` | `NSA`（MTS 为未季调；以字段说明为准） |

> MTS 只用于月度校验/趋势约束/季节性基准；不得把预算口径直接当 DTS 现金支出。

### 3.7 单位登记表（必须先落地）

| 数据 | 原始单位 | 归一单位 | 转换 |
|---|---|---|---|
| DTS Table I/II/IIIA/IIIB/IV/V | USD millions | USD Million | 原样 |
| auctions `offering_amt` / `total_accepted` / `total_tendered` | USD | USD Million | `/ 1e6` |
| auctions `currently_outstanding` | USD | USD Million | `/ 1e6` |
| MTS `current_month_rcpt_outly_amt` | USD | USD Million | `/ 1e6` |
| Quarterly Refunding borrowing estimates | 常见 USD billions | USD Million | `* 1000`（以文档单位为准，必须显式识别） |
| FRED `WTREGEN` | USD billions | USD Million | `* 1000`（FRED units 字段为准） |

规则：每条记录保留 `original_value` + `original_unit`；`amount_usd_millions` 是唯一计算口径；展示层再转 USD Billion。任何单位不确定 → 质量告警，不猜测。

### 3.8 Phase 0 数据可行性探测（硬门槛）

`scripts/tga_probe.py` 对每个表输出：

- `meta.total-count`、`total-pages`、首末 `record_date`
- 去重后的 `account_type` / `transaction_catg` / `security_type` / `adj_type` / `tax_refund_type`；**`account_type` 必须按时间段枚举**（首尾页采样会漏掉 2021-10/2022-04 的命名切换）
- 字段全集与 `dataTypes/dataFormats`
- 单位量级抽样（与 Fed H.4.1 / 官方报表对比）
- 1 页脱敏 JSON fixture

探测结论写入 `docs/tga/DATA_DICTIONARY.md`。若 DTS 历史不足 10 年，记录实际覆盖范围，不得伪造。没有通过探测的表，不进入 Phase 1。

---

## 4. 数据模型与存储

### 4.1 新增 5 个 Canonical Type

所有类型继承 `BaseRecord`，补齐 `release_time` 与 `release_time_source`（点时可用性）。金额字段统一 `amount_usd_millions`，同时保留原始值/单位。

#### 4.1.1 `TREASURY_ACCOUNT_BALANCE`

| 字段 | 类型 | 说明 |
|---|---|---|
| `observation_time` | datetime | `record_date` 00:00Z；分区时间字段 |
| `account_type_raw` | str | 原始账户/行类型 |
| `balance_type` | enum | `OPENING` / `CLOSING` / `TOTAL_DEPOSITS` / `TOTAL_WITHDRAWALS` |
| `amount_usd_millions` | float | 归一金额 |
| `original_value` | float | 原始数值 |
| `original_unit` | str | 如 `USD_MILLIONS` |
| `is_tga` | bool | 是否 TGA 行 |
| `source_table` | str | `I` |
| `src_line_nbr` | int | 源行号 |
| `release_time` | datetime | 估算/实际发布时刻 |
| `release_time_source` | enum | `OBSERVED` / `ESTIMATED` |

自然键：`(source_id, observation_time, source_table, account_type_raw, src_line_nbr, balance_type)`。

> 历史单行期间（≤2022-04-15）同一源行同时携带 opening/closing，会拆成两条记录；`balance_type` 参与自然键，避免 merge 去重静默丢失一条。

> `open_month_bal` / `open_fiscal_year_bal` 保留在 Raw/对账层，用于月度/财年校验，不单独建模为余额序列。

#### 4.1.2 `TREASURY_CASH_FLOW`

| 字段 | 类型 | 说明 |
|---|---|---|
| `observation_time` | datetime | `record_date` 00:00Z |
| `account_type_raw` | str | 原始账户 |
| `transaction_type_raw` | str | `Deposits` / `Withdrawals` |
| `source_table` | str | `II` / `IV` / `V` |
| `raw_category` | str | 原始分类名 |
| `raw_category_desc` | str \| None | 原始描述 |
| `category` | str | 归一分类（如 `TAX_INDIVIDUAL`） |
| `subcategory` | str \| None | 二级分类 |
| `flow_type` | enum | `TAX` / `DEBT_ISSUANCE` / `DEBT_REDEMPTION` / `INTEREST` / `BENEFIT` / `OPERATING` / `REFUND` / `INTERNAL` / `OTHER` / `UNCLASSIFIED` |
| `direction` | enum | `INFLOW` / `OUTFLOW` / `INTERNAL` / `UNKNOWN` |
| `amount_usd_millions` | float | 归一金额 |
| `original_value` | float | 原始数值 |
| `original_unit` | str | `USD_MILLIONS` |
| `is_internal_transfer` | bool | 是否内部转移 |
| `is_duplicate_candidate` | bool | 是否可能与其他表重复 |
| `is_subtotal` | bool | 是否小计/合计行（不进入叶级求和） |
| `include_in_net` | bool | 是否计入 TGA 净现金流 |
| `classification_confidence` | float | 0–1 |
| `taxonomy_version` | str | 映射版本 |
| `src_line_nbr` | int | 源行号 |
| `release_time` | datetime | |
| `release_time_source` | enum | |

自然键：`(source_id, observation_time, source_table, transaction_type_raw, raw_category, src_line_nbr)`。

#### 4.1.3 `TREASURY_DEBT_TRANSACTION`

| 字段 | 类型 | 说明 |
|---|---|---|
| `observation_time` | datetime | `record_date` 00:00Z |
| `source_table` | str | `IIIA` / `IIIB` |
| `transaction_type_raw` | str | `Issues` / `Redemptions` |
| `security_market` | str \| None | `Marketable` / `Nonmarketable` |
| `security_type` | str \| None | Bills / Notes / Bonds / GAS 等 |
| `security_type_desc` | str \| None | Regular / CMB 等 |
| `adjustment_type` | str \| None | IIIB 的 `adj_type` |
| `amount_basis` | enum | `FACE` / `CASH_ADJUSTMENT` |
| `amount_usd_millions` | float | |
| `original_value` | float | |
| `original_unit` | str | |
| `is_marketable` | bool | |
| `is_internal` | bool | GAS 等内部债务 |
| `src_line_nbr` | int | |
| `release_time` | datetime | |
| `release_time_source` | enum | |

自然键：`(source_id, observation_time, source_table, transaction_type_raw, security_market, security_type, security_type_desc, adjustment_type, src_line_nbr)`。

#### 4.1.4 `TREASURY_AUCTION`

| 字段 | 类型 | 说明 |
|---|---|---|
| `auction_key` | str | 稳定键：`cusip:auction_date:data_status`；无 cusip 时 `type:term:auction_date:data_status` |
| `cusip` | str \| None | |
| `security_type` | str | |
| `security_term` | str | |
| `announcement_date` | date | |
| `auction_date` | date | 分区时间字段 |
| `issue_date` | date | 现金流入日 |
| `maturity_date` | date \| None | 到期日 |
| `offering_amount_usd_millions` | float | |
| `total_accepted_usd_millions` | float \| None | 结果前为空 |
| `total_tendered_usd_millions` | float \| None | |
| `interest_rate` | float \| None | 百分比 |
| `high_yield` | float \| None | 百分比 |
| `price_per_100` | float \| None | |
| `interest_payment_frequency` | str \| None | `Semi-Annual` / `None` / 未公告时为空 |
| `is_reopening` | bool | |
| `is_cmb` | bool | |
| `is_tips` | bool | 由 `security_type` 判定 |
| `is_frn` | bool | 由 `security_type` 判定（FRN Note） |
| `currently_outstanding_usd_millions` | float \| None | |
| `data_status` | enum | `ANNOUNCED` / `RESULT` |
| `release_time` | datetime | 公告或结果可用时刻 |
| `release_time_source` | enum | |

自然键：`(source_id, auction_key)`。

#### 4.1.5 `TREASURY_ANNOUNCEMENT`

| 字段 | 类型 | 说明 |
|---|---|---|
| `publication_time` | datetime | 分区时间字段 |
| `document_type` | enum | `REFUNDING_STATEMENT` / `BORROWING_ESTIMATE` / `FINANCING_REQUIREMENT` / `CASH_BALANCE_ASSUMPTION` / `TBAC` / `OTHER` |
| `forecast_period_start` | date \| None | |
| `forecast_period_end` | date \| None | |
| `security_type` | str \| None | |
| `maturity_bucket` | str \| None | |
| `borrowing_estimate_usd_millions` | float \| None | |
| `cash_balance_assumption_usd_millions` | float \| None | |
| `financing_requirement_usd_millions` | float \| None | |
| `source_url` | str | |
| `extraction_method` | enum | `HTML_TABLE` / `PDF_TEXT` / `MANUAL` |
| `extraction_confidence` | float | 0–1 |
| `revision_time` | datetime | 版本追加 |
| `release_time` | datetime | |
| `release_time_source` | enum | `OBSERVED` / `ESTIMATED` |

自然键：`(source_id, publication_time, document_type, forecast_period_start, forecast_period_end, security_type, maturity_bucket, revision_time)`；属于 revision 类，多版本追加。

### 4.2 契约变更清单（必须走受控流程）

新增 Canonical Type 属于冻结契约变更（D02/D03/D10）。Phase 0 必须提交变更申请，评审通过后一次性修改：

| 文件 | 变更 |
|---|---|
| `docs/design/02-data-models.md` | 新增 5 个类型字段表、身份键、时间字段适用矩阵 |
| `docs/design/03-storage.md` | 新增自然键映射、Arrow schema、视图名 |
| `docs/design/10-task-manifest.md` | 新增 TGA 任务组与依赖 |
| `src/chronoforge/models/enums.py` | `CanonicalType` 增加 5 个成员 |
| `src/chronoforge/models/treasury.py` | 新增模型类 + 校验器 |
| `src/chronoforge/models/identity.py` | `_NATURAL_KEY_MAP` / `_TIME_FIELD_MAP` / `REVISION_TYPES`（余额键含 `balance_type`） |
| `src/chronoforge/storage/canonical.py` | `_TYPE_FIELDS` 增加 Arrow schema（含 `is_subtotal`） |
| `src/chronoforge/storage/views.py` | 引用 `identity.REVISION_TYPES`；新增 `TREASURY_ANNOUNCEMENT` as-of 分区键；不承载 TGA Derived 视图 |
| `src/chronoforge/storage/migrations/0005_forecast_metadata.py` | `forecast_runs` / `backtest_runs` / `tga_anomaly` 元数据表 |
| `src/chronoforge/storage/migrations/__init__.py` | 注册 `MIGRATION_0005_SQL` |
| `src/chronoforge/quality/rules.py` | 追加记录级 Q-TGA-001/002/007/008/009（不新建 `tga` 模块） |
| `src/chronoforge/registry/service.py` | `DEFAULT_SOURCES` 增加 2 个源（当前已加 `treasury_fiscal`） |
| `src/chronoforge/config/update_schedule.py` | 增加 `treasury_fiscal` / `treasury_refunding` 发布时刻 |
| `src/chronoforge/pipeline/windows.py` | `treasury_fiscal_dts_` / `treasury_fiscal_auction_` 的 5 天 overlap、30 天 chunk |
| `src/chronoforge/cli/_wiring.py` | `build_connector` 增加 `treasury_fiscal` 分支 |
| `tests/architecture/test_nk_mapping_consistency.py` | `SAMPLES` 覆盖新类型与新增字段 |
| `tests/architecture/test_tga_boundaries.py` | 断言无 `tga/` 包、scripts 不 import connectors、src 不 import scripts |
| `tests/integration/test_views.py` | 类型数 27→32；revision 类型均有 as-of 分区键 |
| `docs/design/04-connectors.md` | 新增源规格 |

**PIT 视图约定**：`TREASURY_ANNOUNCEMENT` 加入 `models/identity.py::REVISION_TYPES`；`storage/views.py` 已改为直接引用该集合，并在 `_PARTITION_TIME_FIELD_MAP` 增加 `source_id, publication_time, document_type, forecast_period_start, forecast_period_end`，注册 `treasury_announcement_asof`。其余 4 个类型不走 as-of 视图，查询时显式 `release_time <= data_cutoff`，避免把"估算发布时间"当成真实 vintage。

**禁止**在实现层单方面改契约；若 Phase 0 评审不通过，退回使用现有 `FLOW/NUMBER` 的降级方案（会损失可查询的分类语义，不推荐）。

### 4.3 原始层与标准化层

- Raw：沿用 `data/raw/{source}/{dataset}/ingest_date=YYYY-MM-DD/*.jsonl`；每行含 `ingest_batch_id`、`fetched_at`、`url`、`payload`。
- PDF：`data/raw/treasury_refunding/documents/{sha256}.pdf` + 同名 `.meta.json`（URL、抓取时间、哈希、页数）；JSONL 记录引用哈希。
- Canonical：`data/canonical/{TYPE}/entity={id}/year=YYYY/month=MM/part-*.parquet`。
- Derived：`data/derived/{name}/...`；预测类用 run 分区（见 §4.5）。

数据集登记清单（`registry/service.py` / `scripts/register_treasury_datasets.py`）：

| dataset_id | Canonical Type | entity_id | params | frequency | continuity_model | available_from |
|---|---|---|---|---|---|---|
| `treasury_fiscal_dts_operating_cash_balance` | `TREASURY_ACCOUNT_BALANCE` | `TGA` | `{"table":"operating_cash_balance"}` | `1d` | `TRADING_CALENDAR` | `2005-10-03` |
| `treasury_fiscal_dts_deposits_withdrawals` | `TREASURY_CASH_FLOW` | `TGA` | `{"table":"deposits_withdrawals"}` | `1d` | `TRADING_CALENDAR` | `2005-10-03` |
| `treasury_fiscal_dts_public_debt_transactions` | `TREASURY_DEBT_TRANSACTION` | `TGA` | `{"table":"public_debt_transactions"}` | `1d` | `TRADING_CALENDAR` | `2005-10-03` |
| `treasury_fiscal_dts_public_debt_cash_basis` | `TREASURY_DEBT_TRANSACTION` | `TGA` | `{"table":"public_debt_cash_basis"}` | `1d` | `TRADING_CALENDAR` | `2005-10-03` |
| `treasury_fiscal_dts_inter_agency_tax_transfers` | `TREASURY_CASH_FLOW` | `TGA` | `{"table":"inter_agency_tax_transfers"}` | `1d` | `TRADING_CALENDAR` | `2023-02-14` |
| `treasury_fiscal_dts_income_tax_refunds` | `TREASURY_CASH_FLOW` | `TGA` | `{"table":"income_tax_refunds"}` | `1d` | `TRADING_CALENDAR` | `2005-10-03` |
| `treasury_fiscal_auction_results` | `TREASURY_AUCTION` | `US_TREASURY` | `{"table":"auctions_query"}` | `1d` | `EVENT_BASED` | `1979-11-15` |
| `treasury_fiscal_auction_upcoming` | `TREASURY_AUCTION` | `US_TREASURY` | `{"table":"upcoming_auctions"}` | `1d` | `EVENT_BASED` | `2024-03-08` |
| `treasury_fiscal_mts_summary` | `FLOW` | `US_TREASURY` | `{"table":"mts_table_5"}`（表号 Phase 0 定） | `1M` | `RELEASE_SCHEDULE` | 待探测 |
| `treasury_refunding_announcements` | `TREASURY_ANNOUNCEMENT` | `quarterly_refunding` | `{"document":"quarterly_refunding"}` | `1Q` | `EVENT_BASED` | 待探测 |

> dataset_id 统一采用下划线格式 `{source_id}_{dataset_type}`（与现网 registry、`pipeline/windows.py` 前缀一致）；`params_json` 的 `table` 必须是 Connector `_ENDPOINTS` 的键（`deposits_withdrawals` / `public_debt_cash_basis` 等）。实际登记的权威清单为 `scripts/register_treasury_datasets.py::_SPECS`。

### 4.4 SQLite 元数据（迁移 `0005_forecast_metadata.py`）

```sql
CREATE TABLE forecast_runs (
  forecast_run_id TEXT PRIMARY KEY,
  created_at TEXT NOT NULL,
  origin_date TEXT NOT NULL,
  data_cutoff_date TEXT NOT NULL,
  model_version TEXT NOT NULL,
  config_hash TEXT NOT NULL,
  scenarios_json TEXT NOT NULL,
  horizons_json TEXT NOT NULL,
  snapshot_id TEXT,
  status TEXT NOT NULL CHECK(status IN ('RUNNING','SUCCESS','FAILED')),
  is_repeat INTEGER NOT NULL DEFAULT 0,
  output_hash TEXT,
  error_summary TEXT
);
CREATE INDEX idx_forecast_runs_cutoff ON forecast_runs(data_cutoff_date DESC);

CREATE TABLE backtest_runs (
  backtest_run_id TEXT PRIMARY KEY,
  created_at TEXT NOT NULL,
  start_date TEXT NOT NULL,
  end_date TEXT NOT NULL,
  stride INTEGER NOT NULL,
  model_versions_json TEXT NOT NULL,
  config_hash TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('RUNNING','SUCCESS','FAILED')),
  output_hash TEXT,
  error_summary TEXT
);

CREATE TABLE tga_anomaly (
  anomaly_id TEXT PRIMARY KEY,
  detected_at TEXT NOT NULL,
  data_source TEXT NOT NULL,
  record_date TEXT,
  anomaly_type TEXT NOT NULL,
  original_value TEXT,
  expected_value TEXT,
  severity TEXT NOT NULL CHECK(severity IN ('ERROR','WARNING','INFO')),
  resolution_status TEXT NOT NULL DEFAULT 'OPEN'
    CHECK(resolution_status IN ('OPEN','ACKNOWLEDGED','EXPLAINED','RESOLVED')),
  quality_flag_ref TEXT,
  note TEXT
);
```

- 表结构由 `src/chronoforge/storage/migrations/0005_forecast_metadata.py` 创建；读写逻辑由 `scripts/tga_store.py` 实现，遵循现有 `MetaStore` 约定，不在脚本里另建 schema。
- `snapshot_id` 关联现有 `research_snapshot`，锁定依赖数据集版本。
- `forecast_runs` 的唯一性：同 `(origin_date, data_cutoff_date, model_version, config_hash, scenarios_json, horizons_json)` 允许重复运行，但报告取最新 `SUCCESS`；重复运行标 `is_repeat=1`。
- `tga_anomaly` 与 `quality_flags` 一对一/一对多关联，保留原值，不删除。

### 4.5 Derived 数据集与 DuckDB 视图

| 数据集 | 路径 | 分区 | 主要字段 |
|---|---|---|---|
| `tga_daily_balance` | `data/derived/tga_daily_balance/` | `year/month` | date, opening, closing, daily_change, quality_status |
| `tga_daily_cashflow` | `data/derived/tga_daily_cashflow/` | `year/month` | date, category, subcategory, direction, amount, source_type, include_in_net, confidence |
| `tga_reconciliation` | `data/derived/tga_reconciliation/` | `year/month` | date, delta_tga, total_deposits, total_withdrawals, classified_net, unclassified_net, residuals |
| `tga_debt_cash_basis` | `data/derived/tga_debt_cash_basis/` | `year/month` | date, transaction_type, security_type, face_amount, adjustment, cash_amount |
| `tga_event_calendar` | `data/derived/tga_event_calendar/` | `year/month` | event_date, event_type, event_key, amount, confidence, source |
| `tga_forecast_components` | `data/derived/tga_forecast_components/` | `run_date/scenario` | run_id, target_date, category, predicted, lower, upper, method, confidence, source_type, event_key |
| `tga_forecast` | `data/derived/tga_forecast/` | `run_date/scenario` | run_id, target_date, horizon, predicted_tga, lower, upper, daily_net, source_type |
| `tga_forecast_eval` | `data/derived/tga_forecast_eval/` | `year/month` | run_id, target_date, horizon, model_version, predicted, actual, errors, direction_correct |
| `tga_attribution` | `data/derived/tga_attribution/` | `year/month` | run_id, target_date, horizon, model_version, component, error_amount, error_type |

视图（**独立 catalog `meta/tga.duckdb`**，路径由 `Settings.load().data_dir` 注入绝对值）：

```sql
-- data_dir 为绝对路径（如 /Users/…/data），不得写相对路径，避免 cwd 漂移
CREATE OR REPLACE VIEW tga_daily_balance AS
SELECT * FROM read_parquet('{data_dir}/derived/tga_daily_balance/**/*.parquet', hive_partitioning=1);

CREATE OR REPLACE VIEW tga_forecast AS
SELECT * FROM read_parquet('{data_dir}/derived/tga_forecast/**/*.parquet', hive_partitioning=1);

-- 其余数据集同构；这些 TGA Derived 视图只由 scripts/tga_store.py 在 meta/tga.duckdb 注册
-- src/chronoforge/storage/views.py 只负责 canonical/query.duckdb，不加入 TGA 业务视图
```

`scripts/tga_store.py` 的写入规则：

1. Parquet 先写 `part.tmp-{uuid}` 再 `rename`，沿用现有原子写协议；崩溃遗留 temp 目录在启动时清理。
2. SQLite 连接来自 `open_meta()`；写 `forecast_runs` / `backtest_runs` / `tga_anomaly` 时使用事务，终态一次提交。
3. 不写 Raw/Canonical 层；只写 `data/derived/tga_*` 与上述元数据表。
4. Derived 视图写 `meta/tga.duckdb`（`duckdb.connect(..., read_only=False)`），由本脚本独占；**不得写 `meta/query.duckdb`**。查询方（报告/回测脚本）用 `read_only=True` 打开同一文件。
5. 若 `tga.duckdb` 缺失或视图缺失，`tga_store.py` 负责幂等重建，不依赖 `open_query_service()` 的 readiness 探测。

> 现状说明：本仓库目前没有 `storage/derived.py` 的 DerivedStore 实现（D03 规划中）。为避免新增 `src/chronoforge/tga/`，P0 的 TGA Derived 写入由 `scripts/tga_store.py` 按相同的分区与原子写协议实现；未来若落地通用 DerivedStore，再迁移到 `src/storage/`，脚本只保留编排调用。

### 4.6 为什么不用 PostgreSQL

- 本仓库已冻结 Parquet + DuckDB + SQLite 技术决策（架构 06），单机本地优先。
- TGA 事实表是时间序列 + 预测批次，Parquet 分区天然适配；DuckDB 做 as-of/窗口查询足够。
- 运行元数据量小，SQLite WAL 足够；引入 PostgreSQL 会增加部署、备份、迁移成本，与项目定位不符。
- 若未来多机/并发写需求出现，再按架构演进流程评估，不在本期。

---

## 5. 历史现金流重建（Model 0）

> 目标：先证明能解释历史 TGA 每日变化，再允许预测。未通过 Model 0，不得进入 Phase 3。
> 代码落点：`scripts/tga_reconstruct.py` + `scripts/tga_common.py`，不新增 `src/chronoforge/tga/`。

### 5.1 余额序列

从 `TREASURY_ACCOUNT_BALANCE` 构建：

```text
opening_t   = balance_type=OPENING 记录金额（若当日缺失，用 closing_{t-1} 推导）
closing_t   = balance_type=CLOSING 记录金额
daily_change_t = closing_t - opening_t
```

> 2022-04-18 前 DTS 单行同时携带 opening/closing，Connector 已在 normalize 阶段拆成两条（自然键含 `balance_type`）；重建脚本不得再按 `account_type` 去重。
> ≥2022-04-18 的四行结构里，Opening/Closing 行金额都在 `open_today_bal`；Connector 已按行映射，不需要重建层再猜字段。

连续性：

```text
opening_t ≈ closing_{t-1}
```

- 周末/联邦假日：允许 `EXPECTED_GAP`，记录在 `tga_reconciliation`；不得把假日当作缺失数据删除。
- 若 `opening_t` 与 `closing_{t-1}` 不一致：`Q-TGA-002` 告警，保留两者原值，不强制平滑。
- 若 TGA Closing 行缺失：`Q-TGA-001` ERROR，停止该日重建并标记 `INCOMPLETE`。

### 5.2 现金台账（Table II）

1. 读取 Table II 叶级分类；`transaction_type` 决定方向。
2. `taxonomy.yaml` 映射到 `category` / `flow_type` / `include_in_net`。
3. 小计/合计行按 `is_subtotal` 排除，避免重复求和。
4. 求和：

```text
classified_deposits   = Σ leaf where direction=INFLOW  and include_in_net and not is_subtotal
classified_withdrawals= Σ leaf where direction=OUTFLOW and include_in_net and not is_subtotal
unclassified_deposits = Σ leaf where category=UNCLASSIFIED and direction=INFLOW  and include_in_net
unclassified_withdrawals = Σ leaf where category=UNCLASSIFIED and direction=OUTFLOW and include_in_net
classification_residual_deposits = total_deposits - Σ all deposit leaf rows
classification_residual_withdrawals = total_withdrawals - Σ all withdrawal leaf rows
```

- `classification_residual_*` 只用于完整性校验，**不是未分类金额桶**；把残差直接当 unclassified 会因「all leaf 已包含未分类叶行」而恒为 0。
- 未命中 taxonomy 的叶行必须以 `category=UNCLASSIFIED`、`include_in_net=true` 落库，才能进入 `unclassified_*`。

5. 归一净现金流：

```text
net_t = (classified_deposits + unclassified_deposits)
      - (classified_withdrawals + unclassified_withdrawals)
```

6. 内部转移行计入 `Σ all deposit leaf rows`，但 `include_in_net=false`，因此既不进入 classified、也不进入 unclassified；只在分析视图保留。小计行（`is_subtotal=true`）同样不进入叶级求和。
7. 所有未命中映射的分类进入 `UNCLASSIFIED`，保留原始名称与金额，且 `include_in_net=true`；**不丢弃、不静默合并**。

### 5.3 债务现金口径（IIIA + IIIB）

```text
cash_basis_t = Σ IIIA_t + Σ IIIB_t
```

- 按 `transaction_type`、`security_market`、`security_type` 分组；
- 符号与调整方向以实测为准（IIIB 的 `adj_type` 已包含 "(-)" 语义，需在 Phase 0 用真实数据验证）；
- 结果写入 `tga_debt_cash_basis`；
- 只与 Table II 的债务类现金流、Table I 的余额变化交叉校验；**不加入 TGA 净现金流**；
- 若 Table II 没有细分的债务类现金流，用 IIIA/IIIB 作为归因来源，并在报告中标注"归因口径，不是现金台账"。

### 5.4 内部转移与重复项（IV / V）

- Table IV：`is_internal_transfer=true`、`include_in_net=false`；用于解释机构间税收转移。实测仅 2023-02-14 起，2023 年前不参与解释。
- Table V：`is_duplicate_candidate=true`、`include_in_net=false`；用于退款季节性分析，验证 Table II 的退款支出。
- 任何跨表同金额/同日/同分类的组合，记 `Q-TGA-006` 告警；不自动删除。

### 5.5 残差与对账指标

定义三类残差，分别报告：

| 残差 | 公式 | 含义 | 预期 |
|---|---|---|---|
| `balance_residual` | `closing_t - (opening_t + total_deposits_t - total_withdrawals_t)` | Table I 内部一致性 | 0；非 0 说明字段映射/数据异常 |
| `classification_residual` | `total_deposits_t - Σ deposit leaf`；`total_withdrawals_t - Σ withdrawal leaf` | 分类完整性 | 0 或接近 0；非 0 说明叶级捕获不完整，需查源，不得把残差当 UNCLASSIFIED 桶 |
| `ledger_residual` | `daily_change_t - net_t` | 台账与余额的一致性 | 0；非 0 必须解释 |

指标：

- `Daily Residual`、`Mean Absolute Residual`、`Residual Percentage`
- `Monthly Reconciliation`：DTS 日频汇总 vs MTS 月度（P1；口径差异单列）
- `Unclassified Cash Flow`：金额与占比
- `Duplicate Cash Flow`：金额与笔数

### 5.6 验收门槛

1. TGA 日余额序列连续，缺失日有 `EXPECTED_GAP`/`INCOMPLETE` 标识。
2. `balance_residual = 0` 的交易日占比 ≥ 99%；非 0 日全部有 anomaly 记录。
3. `ledger_residual = 0` 的交易日占比 ≥ 99%；非 0 日全部解释。
4. 未分类现金流占毛现金流比例 < 1%（首年可放宽到 3%，但必须逐项列明）。
5. Table IV/V 未造成净现金流重复计入（守恒测试通过）。
6. 月度交叉校验报告存在；差异有口径解释，不强行调平。
7. 所有异常保留原值；不存在静默修改。

未达标 → 不进入预测阶段，先修数据/映射/口径。

### 5.7 点时（PIT）账本

- 每个新类型带 `release_time` + `release_time_source`。
- 系统上线后：`release_time = 实际 fetch_time`（`OBSERVED`）。
- 历史回填：`release_time = 按官方发布规则估算`（`ESTIMATED`）：
  - DTS：`record_date` 的下一个联邦工作日 + `update_schedule.publish_time_utc("treasury_fiscal")`（21:00 UTC）；已由 Connector `_release_time` 落实；
  - 拍卖：公告日/拍卖结果可用日；
  - MTS：官方发布日；
  - Refunding：文档 `publication_time`。
- 回测查询必须 `WHERE release_time <= data_cutoff`；`release_time_source` 随结果输出，报告中标注"历史发布时间为估算"。
- Raw 层的 `fetched_at` 是第二道证据：上线后每次抓取都留痕；历史回填无法重建真实 vintage，必须在文档中声明限制。

---

## 6. 预测引擎（Model 1–4）

> 代码落点：`scripts/tga_forecast.py`（组件、聚合、情景、区间）；公共函数在 `scripts/tga_common.py`；输出存储在 `scripts/tga_store.py`。

### 6.1 组件协议与聚合

```python
@dataclass(frozen=True)
class ForecastContext:
    origin_date: date
    data_cutoff: datetime
    horizons: tuple[int, ...]
    scenario: str
    query: QueryServiceLike
    calendars: CalendarBundle
    config: TgaModelConfig

class ForecastComponent(Protocol):
    name: str
    categories: frozenset[str]
    def forecast(self, ctx: ForecastContext) -> pl.DataFrame: ...
    # DataFrame: target_date, category, predicted, lower, upper,
    #            method, confidence, source_type, event_key
```

聚合规则：

1. 每个组件输出按 `(target_date, category, event_key)` 去重；同一事件只应用一次。
2. 未知/未识别现金流单独成组件，不得摊入其他类别。
3. 已知事件不参与情景调整。
4. 总净现金流 = 所有组件 `predicted` 之和；`TGA_forecast = TGA_cutoff + cumulative(net)`（区别于财政部"目标余额"）。
5. 守恒断言：组件之和 == 聚合结果；否则 `Q-TGA-012` ERROR。

### 6.2 已知事件层（确定性）

来源：

- 拍卖发行：`issue_date` 落入 horizon → 流入；
- 到期偿还：`maturity_date` 落入 horizon → 流出；
- 付息：notes/bonds/TIPS/FRN 的付息日 → 流出；
- 税收事件：IRS 日历 + 历史税峰 → 流入/退款流出；
- 固定支出周期：社保、医保、联邦薪酬等 → 流出；
- 季末/月末/财年末：仅作为事件标记，不直接产生金额。

事件键：

```text
ISSUE:{cusip}:{issue_date}
REDEEM:{cusip}:{maturity_date}
COUPON:{cusip}:{payment_date}
TAX:{event_id}:{date}
OUTLAY:{category}:{date}
```

### 6.3 国债发行预测（三层）

**第一层：已公布且已有结果**

- `data_status=RESULT` 且 `release_time <= cutoff`：
  - 现金流入 ≈ `total_accepted × price_per_100 / 100`（附息债另加应计利息，若字段可得）；
  - bills 用 `price_per_100`；无价格则用 `high_yield` 折现公式估算；仍无 → `offering_amt` 平价近似，置信度降低。

**第二层：已公告、未出结果**

- `data_status=ANNOUNCED` 且 `announcement_date <= cutoff < auction_date`：
  - 用 `offering_amt` 作为暂定规模；
  - 价格用平价或近期同类均价，置信度 0.6–0.8。

**第三层：未公告**

- 按期限的发行节奏（4/8/13/17/26/52 周 bills、notes/bonds 月度/季度周期）预测下一次 `issue_date`；
- 规模 = 近 N 次同类中位数 × 趋势因子（来自 refunding / 近期净融资）；
- 只输出 `PREDICTED`，不伪装成公告；
- CMB 默认不预测；仅当用户启用 `cmb_scenario` 时作为情景假设。

**必须区分**：Gross Issuance、Redemption、Net Issuance、Rollover、New Money、CMB。报告中分别展示，不能只给净融资。

### 6.4 到期与付息预测

**到期本金**：

```text
principal_outstanding(cusip) = Σ total_accepted over all auctions of that CUSIP
```

- 若结果缺失：用 `offering_amt` 近似；
- 若 `currently_outstanding` 可用：作为交叉校验；
- 到期日 = `maturity_date`；到期本金为现金流出；
- 同一天有新发行与到期：分别记录，不只看净额。

**付息**：

- Notes/Bonds：按 `int_rate`、`int_payment_frequency`、到期日生成付息日（通常 15 日或月末），金额 = 未偿本金 × 票息 / 频率；
- Bills：无独立付息；发行折价与到期面值之差即利息成本，**不得再单列 coupon**；
- TIPS：P0 用原始本金近似或 DTS 历史利息支付基线；P1 用 CPI index ratio；
- FRN：P0 用最近票息/DTS 基线；P1 用指数利率；
- Savings/非市场化：用 DTS 历史基线，P2 细化。

### 6.5 税收预测

**数据**：DTS Table II → `TAX_*` 分类；月度总量可用 MTS 校验。

**P0 算法（可解释）**：

1. 建立税收事件日历（`calendar.yaml`）：1/4/6/9 月季度预缴、4 月年度个税、企业预缴、延期截止、退税季。
2. 对每个税收分类，计算历史"事件相对日"（-10…+10 个工作日）的中位数曲线。
3. 用近 12 个月同比/趋势因子缩放。
4. 非事件日：用近 4 周运行率 + 同月同日历史中位数混合。
5. 短周期（1–14 天）优先事件与运行率；30–90 天用月度季节性 + 事件曲线。
6. 每月总量可由 MTS 趋势约束，再按历史日分布拆回日频（不直接用 MTS 预算数）。

**P1 算法**：SARIMAX/ETS + 日历外生变量；或梯度提升；仅在 Walk-forward 中优于 P0 才启用。

### 6.6 政府支出预测

| 类别 | P0 方法 | 数据 |
|---|---|---|
| Social Security | 识别每月第 2/3/4 周三支付；月度总量用近 12 个月中位数；按历史日份额分配 | DTS |
| Medicare / Medicaid | 月内日分布 + 近 3 个月中位数 | DTS |
| National Defense | 月内日分布 + 13 周移动平均 | DTS |
| Federal Employee Compensation | 识别 14 天双周周期 | DTS |
| Interest Payments | 国债付息日历（§6.4） | Auction/FRED |
| Other Outlays | 4 周/13 周移动平均 + 同月中位数；低置信度 | DTS |

禁止：

- 把预算拨款金额直接当现金支出；
- 用 USAspending 义务金额替代 DTS 实际支付；
- 对不规则支出强行拟合到固定日期。

### 6.7 其他/未识别现金流

- `OTHER_DEPOSIT` / `OTHER_WITHDRAWAL`：4 周/13 周移动平均、同月中位数；
- `REFUND`：退税季曲线 + Table V 校验；
- `INTERNAL_TRANSFER`：不计入净现金流；
- `UNCLASSIFIED`：近 4 周中位数外推，标记最低置信度，单独展示，不摊平。

### 6.8 情景

`config/tga/scenarios.yaml`：

```yaml
scenarios:
  base:
    tax_adjustment: 0.00
    outlay_adjustment: 0.00
    issuance_adjustment: 0.00
    redemption_adjustment: 0.00
  liquidity_easing:
    tax_adjustment: -0.05
    outlay_adjustment: 0.05
    issuance_adjustment: -0.05
    redemption_adjustment: 0.00
  liquidity_tightening:
    tax_adjustment: 0.05
    outlay_adjustment: -0.05
    issuance_adjustment: 0.05
    redemption_adjustment: 0.00
limits:
  max_abs_adjustment: 0.20       # 防止极端情景
  apply_to_known: false          # 必须为 false
```

规则：

1. 只作用于 `PREDICTED` / `UNCLASSIFIED` 组件；已知事件（`KNOWN`）不调整。
2. 调整在组件级生效，记录 `scenario_adjustment` 审计字段。
3. 输出每个情景独立预测；报告并列展示；不把情景当官方预测。
4. 情景参数变更 → `config_hash` 变化 → 新 forecast run，不覆盖历史。
5. 每个情景带 `triggers` / `key_drivers` / `interpretation` / `usage` / `confidence`，只用于说明"现在怎么读、什么信号换挡"，**不参与数值计算**（v1.6）。
6. 情景输出是**条件路径 / 情景锚点**，不是点位预测；下游消费须检查 `output_type=scenario`（§8.2）。

### 6.9 融资压力与财政部行为（进阶，P1）

- `TGA_target_reference`：优先使用 Quarterly Refunding 公布的期末现金余额假设；没有则用历史余额分布区间，标注为"参考区间"，不得称为财政部正式目标。
- `FinancingGap = TGA_target_reference - TGA_forecast`；只输出"融资压力"与方向，不把它直接当作未来发行金额。
- 历史行为研究：`NetIssuance_t = f(TGA_{t-1}, 预测现金流, 赤字, 季节性)`，用回归/相关性描述财政部在低余额时是否增加短债发行；结论只作解释，不默认进入预测。
- 政策变化（债务上限、非常规措施、停摆、发行计划调整）作为独立情景/事件标记，不内生化。

### 6.10 区间与置信度

- 默认 80% 区间；可配置 50/80/95。
- 已知且最终的事件：区间 = [amount, amount]；已公告未结果：区间来自同类发行历史偏差。
- 预测类：用 Walk-forward 残差分位数（按 horizon × category），P0 用经验分位数；P1 用 bootstrap/协方差聚合。
- 置信度评分（用于报告构成，不用于替代区间）：

| 来源 | 分值 |
|---|---|
| KNOWN_FINAL | 1.0 |
| KNOWN_PROVISIONAL | 0.8 |
| SEASONAL / STATISTICAL | 0.5 |
| UNCLASSIFIED | 0.2 |
| SCENARIO | 不计入置信度，单独标注 |

- 报告展示"金额占比"：已知 / 预测 / 情景 / 未识别；按绝对金额加权，不主观打分。

### 6.11 模型版本与选择

| 模型 | 内容 | 默认 |
|---|---|---|
| M0 | 历史重建 | Phase 2 必须通过 |
| M1 | 历史季节性基准 | 对照 |
| M2 | 事件驱动 | P0 默认 |
| M3 | 统计模型（SARIMAX/ETS/LightGBM） | 仅样本外优于 M2 时启用 |
| M4 | 集成 | 仅回测证明优于 M2 时启用 |

默认模型选择原则：**简单、可解释、可回测**。复杂模型必须提供样本外证据，且不能替代确定性事件。

---

## 7. 回测与误差归因

> 代码落点：`scripts/tga_backtest.py`；评估结果由 `scripts/tga_store.py` 写入，报告由 `scripts/tga_report.py` 生成。

### 7.1 Walk-forward

- 起点：数据可用起点 + 最小训练窗口（默认 5 年，可配置）。
- 每个预测起点 `t`：
  - `data_cutoff = t` 当日可用数据边界（DTS 上一工作日、拍卖公告/结果按 §7.2）；
  - 预测 7/14/30/60/90 天；
  - 推进 1 个工作日（可用 `--stride` 降采样用于开发）。
- 每个起点生成独立 `forecast_run`，不覆盖。
- 计算实际 TGA 后写入 `tga_forecast_eval`。

### 7.2 防未来数据泄漏（硬规则）

1. 所有查询带 `release_time <= data_cutoff`；估算发布时间标记 `ESTIMATED`。
2. 拍卖：
   - `cutoff < auction_date`：只用 `ANNOUNCED` + `offering_amt`；
   - `cutoff >= auction_date`：可用 `RESULT` + `total_accepted`；若结果发布晚于 cutoff，仍不可用。
3. Refunding：只用 `publication_time <= cutoff` 的版本；禁止用最新公告回测过去。
4. MTS：只用 `release_time <= cutoff` 的 vintage；修订数据另存。
5. 季节性/统计模型的特征计算只用 cutoff 前数据；禁止"用全历史计算分位数再回填"。
6. Leak Sentinel 测试：注入一条 `release_time > cutoff` 的记录，断言预测结果不变。

### 7.3 指标

| 指标 | 公式/说明 |
|---|---|
| MAE | `mean(abs(forecast - actual))` |
| RMSE | `sqrt(mean((forecast-actual)^2))` |
| MAPE | `mean(abs(forecast-actual)/abs(actual))`，辅助指标 |
| Direction Accuracy | `correct_direction / total`；按日/按 horizon |
| Bias | `mean(forecast - actual)` |
| Interval Coverage | 实际值落入区间的比例（80%/95%） |
| Pinball Loss | 分位数损失，评估区间质量 |
| Event-day MAE | 税期/发行/到期/付息日的单独误差 |

按 7/14/30/60/90 天分别报告；同时报告事件日 vs 非事件日。

### 7.4 基准模型

| 模型 | 说明 |
|---|---|
| Naive | TGA 保持不变 |
| Historical Mean | 历史日均净现金流外推 |
| Seasonal Naive | 去年同期/同事件相对日 |
| Event-driven | 只计已公布事件（不做统计外推） |
| Statistical | M3 统计模型 |
| Ensemble | M4 |

只有复杂模型在**样本外**优于简单基准，才允许设为默认；报告中必须列出对比表。

### 7.5 误差归因

```text
total_error = forecast_total - actual_total
component_error = Σ (forecast_component - actual_component)
timing_error = 实际发生日与预测日的错位部分
unclassified_error = 未识别现金流误差
```

| 误差来源 | 说明 |
|---|---|
| Tax Error | 税收预测偏差 |
| Issuance Error | 发行规模/价格偏差 |
| Redemption Error | 到期本金偏差 |
| Interest Error | 付息偏差 |
| Outlay Error | 政府支出偏差 |
| Timing Error | 日期错位（金额对但日期错） |
| Unclassified | 未识别/未映射 |

P0 先做类别级误差 + 残差桶；P1 做事件级匹配（同一 event_key 的预测 vs 实际），分离 timing。

### 7.6 回测验收

1. 所有预测可追溯到 `forecast_run` 的 `data_cutoff` 与 `snapshot_id`。
2. Leak Sentinel 全部通过。
3. 至少 3 年样本外（或数据允许的最大范围），每个 horizon 有足够样本。
4. 基准对比表完整。
5. 误差归因报告能解释主要误差来源。
6. 若 M2 不优于 Naive/Seasonal，必须在报告中如实说明，不得宣称有效。

---

## 8. 报告与对外接口

> 代码落点：`scripts/tga_report.py`（Markdown/JSON/图表）；`scripts/tga_export_liquidity.py`（对外流动性接口）。

### 8.1 每日 Markdown

路径：`reports/daily/TGA_Forecast_YYYY-MM-DD.md`

必须包含元信息：

```text
数据截止日 / 预测生成时间 / 模型版本 / config_hash
预测起点 TGA / 单位 / 情景定义 / 数据来源 / 免责声明
```

章节：

1. **当前 TGA 状况**：最新余额、日期、前一交易日、日变化、过去 7/30 天变化。
2. **模型定位与能力边界**：目标 / 非目标；引用 MAE 与 DM 结论，声明不优于 Naive、不可用于交易决策。
3. **情景路径**：T+7/14/30/60/90 × base/easing/tightening，明确为**条件锚点而非点位预测**。
4. **情景定义与切换条件**：每个情景的触发条件、关键驱动事件、适用解读（来自 `scenarios.yaml`）。
5. **逐日现金流路径**：预测窗口逐日收入/支出/净现金流/主要事件，用于解释路径推动因素。
6. **驱动因素归因**：现金流构成（税收、发行、偿还、福利、利息、其他）+ 关键事件 Top N。
7. **来源构成与证据强度**：已知 / 季节性 / 统计 / 未识别金额占比。
8. **融资压力**：财政部现金余额假设 vs 情景锚点，只作压力估计。
9. **回测与 DM 检验（能力边界证据）**：7/14/30/60/90 的 MAE/RMSE/方向准确率/区间覆盖 + DM 显著性。
10. **使用限制与结论**：只做事件归因与情景判断，**不得**输出"预计增加/减少约 X B"式点位结论。

格式规则：

- 展示层用 **Billion USD**，保留 2 位小数；原始值仍为 USD Million。
- 每个数字标注来源类型（Known / Predicted / Scenario / Unclassified）。
- 预测段落必须带"非官方数据"声明；并明示数值为情景锚点、非点位预测。
- 不展示没有数据支撑的图表或结论。
- 报告目录：`reports/daily` 与 `reports/tga` 入库；`output/` 已被 `.gitignore` 忽略，仅用于运行产物。

### 8.2 机器可读接口（美元流动性模型）

路径：`reports/daily/TGA_Forecast_YYYY-MM-DD.json` + `latest.json`

```json
{
  "schema_version": "tga-forecast-1.0",
  "forecast_date": "YYYY-MM-DD",
  "data_cutoff_date": "YYYY-MM-DD",
  "model_version": "tga-0.3.0",
  "scenario": "base",
  "output_type": "scenario",
  "usage": "scenario_anchor",
  "confidence": "exploratory",
  "units": "USD_MILLIONS",
  "last_observed_tga": 0,
  "last_observed_date": "YYYY-MM-DD",
  "forecast": [
    {
      "date": "YYYY-MM-DD",
      "tga_scenario_anchor": 0,
      "tga_lower": 0,
      "tga_upper": 0,
      "daily_net_cashflow": 0,
      "drivers": ["DEBT_ISSUANCE", "DEBT_REDEMPTION"],
      "components": {
        "tax": 0,
        "issuance": 0,
        "redemption": 0,
        "interest": 0,
        "outlays": 0,
        "other": 0
      },
      "confidence": {
        "known_ratio": 0,
        "predicted_ratio": 0,
        "scenario_ratio": 0,
        "unclassified_ratio": 0
      }
    }
  ],
  "metadata": {
    "source": "ChronoForge TGA Forecast",
    "data_sources": ["DTS", "Treasury Auctions", "Quarterly Refunding"],
    "disclaimer": "Model output, not official Treasury forecast. Scenario anchors, not point forecasts. Not for trading decisions."
  }
}
```

约束：

- `schema_version` 语义化；字段只增不删，新增字段向前兼容；历史 run 缺新字段时下游按空值处理。
- `output_type=scenario` / `usage=scenario_anchor` / `confidence=exploratory` 标识输出为条件路径，**不得按点位预测消费**；`drivers` 为当日驱动类别清单。
- 单位显式；禁止混用 USD / USD Million / USD Billion。
- 观测值、预测值、情景值分字段，不混为一个 `tga`；情景锚点字段名不得使用 `tga_predicted` 之类的点位语义。
- 该接口只输出 TGA；银行准备金/ON RRP 由后续流动性模型负责，禁止在本项目里冒充。
- 后续流动性模型可按 `L = Reserves + ON RRP - TGA` 组合，但必须分别标注 observed / forecast / scenario，且不得把预测 TGA 直接等同于银行准备金变化。

### 8.3 图表（P1）

- 历史 TGA + 预测扇形图（base/easing/tightening）
- 现金流瀑布图（税收/发行/偿还/利息/支出/其他）
- 预测 vs 实际回测曲线
- 残差与误差归因柱状图

实现：Plotly（可选依赖 `tga-viz`）；不可用时报告仍完整，图表缺失只告警。

---

## 9. 数据质量与异常

### 9.1 Q-TGA 规则

| ID | 检查 | 级别 |
|---|---|---|
| Q-TGA-001 | TGA Closing/Opening 行缺失 | ERROR |
| Q-TGA-002 | `opening_t != closing_{t-1}`（非假日） | WARNING |
| Q-TGA-003 | `Σ Table II deposits != Table I Total TGA Deposits` | ERROR |
| Q-TGA-004 | `Σ Table II withdrawals != Table I Total TGA Withdrawals` | ERROR |
| Q-TGA-005 | 未分类现金流占比超阈值 | WARNING |
| Q-TGA-006 | 跨表重复候选（IV/V/II/IIIA） | WARNING |
| Q-TGA-007 | 单位/量级异常（如 USD 与 USD Million 混用） | ERROR |
| Q-TGA-008 | 拍卖日期非法（maturity < issue、issue < announcement） | ERROR |
| Q-TGA-009 | 拍卖已过 `issue_date` 但 `total_accepted` 仍缺失（或状态 RESULT 但金额为空） | WARNING |
| Q-TGA-010 | Refunding 解析置信度低/字段缺失 | WARNING |
| Q-TGA-011 | TGA 日变化超出历史分布（如 > 6σ） | WARNING |
| Q-TGA-012 | 组件之和 != 预测总额；情景调整了已知事件 | ERROR |
| Q-TGA-013 | 数据截止日落后于最新 DTS | WARNING |
| Q-TGA-014 | 预测 TGA < 0（未考虑非常规措施） | WARNING |
| Q-TGA-015 | 残差未报告 / UNCLASSIFIED 被丢弃 | ERROR |

规则分层（保持"预测逻辑在 scripts"的边界）：

| 规则 | 实现位置 | 原因 |
|---|---|---|
| Q-TGA-001/002/007/008/009 | `src/chronoforge/quality/rules.py` | 单类型记录级/相邻记录级，可复用现有 QualityRule 协议 |
| Q-TGA-003/004/005/006/011/015 | `scripts/tga_reconstruct.py` | 跨表/时序/重建对账，不属于单类型规则 |
| Q-TGA-010 | `src/chronoforge/connectors/treasury_refunding.py` 产生 finding，`scripts/tga_reconstruct.py` 汇总 | 解析置信度在采集侧产生 |
| Q-TGA-012/013/014 | `scripts/tga_forecast.py` / `scripts/tga_store.py` | 预测运行校验，不进入 `src` |

finding 统一写入 `quality_flags`，TGA 专属结果同步 `tga_anomaly`；脚本侧规则复用 `quality/rules.py` 的 `QualityFinding` 结构，不另造一套异常格式。

### 9.2 异常处置流程

```text
检测 → quality_flags 留痕 → tga_anomaly（OPEN）
     → 人工/规则确认（ACKNOWLEDGED）
     → 解释（EXPLAINED）或修复后关闭（RESOLVED）
```

- 任何异常都保留 `original_value` / `expected_value` / `raw_ref`。
- 禁止自动删除、自动填零、自动平滑。
- 修复只能通过重放 Raw → Canonical → Derived；不得直接改 Parquet。

### 9.3 交叉验证

| 验证 | P0/P1 | 说明 |
|---|---|---|
| Table I 总量 vs Table II 求和 | P0 | 最强校验 |
| IIIA/IIIB vs Table II 债务类 | P0 | 口径差异单独报告 |
| DTS vs MTS 月度 | P1 | 口径/时间差异，不强行调平 |
| DTS vs Fed H.4.1（WTREGEN） | P1 | 周度量级校验 |
| Auctions vs DTS 发行 | P1 | 结算日/面值 vs 现金差异 |

---

## 10. 入口、配置与自动化

### 10.1 入口与命令（scripts）

不新增 `chronoforge tga` CLI 子命令；数据采集复用现有 CLI，TGA 业务全部通过 `scripts/tga_*.py` 入口。

```bash
# 数据采集：复用现有 pipeline CLI（不新增 TGA CLI）
uv run chronoforge pipeline run --dataset treasury_fiscal_dts_operating_cash_balance --mode backfill --start 2015-01-01
uv run chronoforge pipeline run --all-due

# TGA 业务脚本
uv run python scripts/tga_probe.py --output docs/tga/DATA_DICTIONARY.md
uv run python scripts/tga_reconstruct.py --start YYYY-MM-DD --end YYYY-MM-DD
uv run python scripts/tga_forecast.py --origin YYYY-MM-DD --data-cutoff YYYY-MM-DD \
                                      --horizons 7,14,30,60,90 \
                                      --scenarios base,liquidity_easing,liquidity_tightening
uv run python scripts/tga_backtest.py --start YYYY-MM-DD --end YYYY-MM-DD --stride 1
uv run python scripts/tga_report.py --date YYYY-MM-DD --json
uv run python scripts/tga_export_liquidity.py --date YYYY-MM-DD
uv run python scripts/tga_store.py status
uv run python scripts/tga_store.py anomalies --status OPEN
uv run python scripts/tga_daily.py --due-only
```

约束：

- `src/chronoforge/cli/` 不新增 TGA 命令；脚本用 `argparse` 解析参数，业务逻辑留在脚本内。
- `collect` 只调用现有 `chronoforge pipeline run`，不重复实现七阶段。
- `forecast` 默认 `data_cutoff = 最新可用 DTS`；若显式指定，必须通过 release_time 校验。
- `--dry-run` 输出将采集的 dataset、将使用的 cutoff、缺失数据、预计写入路径。
- 脚本退出码：0 成功、1 数据/模型错误、2 参数错误、3 数据未就绪（可重试）。

### 10.2 配置文件

| 文件 | 内容 | 变更影响 |
|---|---|---|
| `config/tga/taxonomy.yaml` | DTS 原始分类 → 归一分类、`include_in_net`、`is_internal`、`is_subtotal` | taxonomy_version 变化 → 重建 + 新 forecast run |
| `config/tga/calendar.yaml` | 联邦假日、IRS 税期、支付周期、季末/财年末 | config_hash 变化 |
| `config/tga/scenarios.yaml` | 三情景参数、上限、是否作用于已知 | config_hash 变化 |
| `config/tga/model.yaml` | horizon、组件开关、分位数、最小训练窗口、CMB 情景开关 | config_hash 变化 |

配置原则：

- 所有文件提交到 git，带版本号；运行时路径通过脚本参数或现有 `CHRONOFORGE_DATA_DIR` / `CHRONOFORGE_META_DIR` 覆盖，不新增 Settings 字段。
- `config_hash = sha256(规范化 JSON of taxonomy+calendar+scenarios+model+code_version)`。
- 禁止在代码里散落税率、支付日、情景系数等业务常量。
- Phase 1 Connector 内置 `builtin-v1` 分类规则仅作过渡；Phase 2 起 `taxonomy.yaml` 为唯一事实源，Connector 只保留字段映射与单位归一。

### 10.3 自动化

每日编排 `scripts/tga_daily.py`：

```text
1. chronoforge pipeline run --all-due（DTS/拍卖数据集）
2. python scripts/tga_reconstruct.py（增量）
3. python scripts/tga_forecast.py（若 cutoff 新于上次成功运行）
4. python scripts/tga_report.py
5. python scripts/tga_export_liquidity.py
6. chronoforge quality report
```

接入方式：

- P0：手动或 `scripts/hourly_update.sh run` 后追加调用；不安装新系统服务。
- P1：在 `hourly_update.sh` 每轮 `run_plots` 后调用 `uv run python scripts/tga_daily.py --due-only`；用 `var/run/tga-daily.lock` 防重入。
- 到期闸门：最新 DTS record_date > 上次 forecast 的 `data_cutoff_date`，且当前时间已过配置的 DTS 发布时刻；否则跳过并记录 INFO。
- 失败处理：不阻塞其他 dataset 更新；退出码非 0 + `tga_anomaly`/日志告警；P1 可选 macOS 通知或 webhook。
- 幂等：同一 `(origin, cutoff, model_version, config_hash)` 重复运行复用已有 SUCCESS；若需重跑，显式 `--force` 生成新 run。

### 10.4 日志与状态

- structlog 事件：`tga.collect` / `tga.rebuild` / `tga.forecast` / `tga.backtest` / `tga.report` / `tga.anomaly`。
- `python scripts/tga_store.py status` 输出：最新 DTS 日期、各 dataset 状态、最近 forecast、最近 backtest、未关闭 anomalies。
- 报告与日志不输出密钥；沿用现有 redaction。

---

## 11. 测试方案

### 11.1 单元测试

| 对象 | 用例 |
|---|---|
| 字段解析 | 每个表 1 页 fixture → normalize 字段逐项一致 |
| 分页 | 2 页 fixture → cursor 推进、无重叠/缺口 |
| 单位 | DTS millions 原样；auction USD→millions；MTS USD→millions；refunding billions→millions |
| 分类 | 原始分类命中/未命中/小计/内部转移 |
| 余额 | 三段账户分期（Federal Reserve Account / 单行 TGA / 四行 TGA）；`close_today_bal` 为空时的取值；单行拆两条后自然键不同 |
| 残差 | 构造 Table I/II 总量一致/不一致样例 |
| 债务 | IIIA+IIIB 符号与调整方向 |
| 拍卖 | announced vs result；cusip 再发行；CMB；TIPS/FRN 判定 |
| 日期 | 联邦假日、周末、季末、财年末、4 月税期 |
| 预测 | 事件只计一次；情景不调整已知；组件求和守恒 |
| 区间 | lower ≤ predicted ≤ upper；覆盖率计算 |
| 回测 | MAE/RMSE/MAPE/direction 手工样例 |
| 报告 | 章节齐全、单位格式、免责声明 |

### 11.2 契约/集成测试

```text
fixture API → connector.fetch → raw.append → normalize → canonical.upsert
           → quality → rebuild → forecast → report
```

- 使用 `tmp_path` 的 `data_dir/meta_dir`；不访问真实网络（smoke 除外）。
- 断言 Raw 文件、Canonical Parquet、Derived Parquet、`forecast_runs` 行、报告文件均生成。
- 重放测试：删除 Derived → replay → 输出哈希一致（在固定输入/config 下）。

### 11.3 防泄漏测试（必须）

1. **DTS**：注入一条 `release_time > cutoff` 的记录，预测不变。
2. **拍卖**：`cutoff < auction_date` 时，`total_accepted` 即使已存在于库中也必须不被使用；只允许 `offering_amt`。
3. **Refunding**：注入 `publication_time > cutoff` 的新版本，预测不变。
4. **MTS**：注入晚于 cutoff 的修订，预测不变。
5. **季节性**：把未来数据加入历史表，断言特征/分位数不变。

### 11.4 边界测试

- 周末、美国联邦假日、季末、财年末；
- 4 月税收集中期、季度预缴日；
- 国债发行/到期集中日（月中、月末、周二/周四结算）；
- DTS 延迟、API 缺页、字段缺失、数据修订；
- 拍卖计划临时调整/取消、CMB 突发；
- 债务上限/非常规措施、政府停摆（只标记，不预测政策）；
- TGA 余额接近零/为负（告警，不钳制）。

### 11.5 数据质量测试

```bash
uv run pytest -m quality
```

对真实入库数据跑记录级 Q-TGA-001/002/007/008/009；跨表/时序规则（Q-TGA-003..006/011/015）由 `scripts/tga_reconstruct.py` 运行，预测规则（Q-TGA-012..014）由 `scripts/tga_forecast.py` 运行；两者都写入 `quality_flags` + `tga_anomaly`，并输出 `meta/tga-quality-report-{date}.json`。

### 11.6 命令与门槛

```bash
uv run pytest tests -q
uv run pytest tests -m "not smoke" -q
uv run pytest tests -m quality -q
uv run ruff check src tests
uv run mypy src
uv run lint-imports
```

Phase 退出前必须全绿；新增架构测试（`test_tga_boundaries.py`）必须被 CI 执行。

---

## 12. 分阶段实施计划

> 规则：每个 Phase 完成 → 跑测试 → 检查数据库/数据质量 → 更新文档 → git 提交 → 输出 Phase Report。未通过退出条件不得进入下一阶段。

### Phase 0 — 研究与契约变更

**目标**：把"猜测"清零，把数据可行性、字段、单位、账户映射、契约变更定下来。

任务：

1. 运行 `scripts/tga_probe.py`，记录每个表的覆盖范围、字段全集、单位、枚举值。
2. 实测 DTS Table I 的 `account_type` **按时间段**映射（Federal Reserve Account / 单行 TGA / 四行 TGA）与 `close_today_bal` 空值行为。
3. 实测 Table II 小计/叶级结构，形成 `taxonomy` 初版。
4. 实测 IIIA/IIIB 符号与现金口径关系。
5. 实测拍卖 `auctions_query` 的 announced/result 字段差异与 `upcoming_auctions` 覆盖限制。
6. 实测 MTS 表号、发布日、字段与单位。
7. 确定 Refunding 文档抓取/解析方案与 PDF 存储扩展。
8. 提交 Canonical Type 契约变更申请；更新 D02/D03/D04/D10。
9. 产出 `docs/tga/DATA_DICTIONARY.md`、`docs/tga/DECISIONS.md`。
10. 准备脱敏 fixtures。
11. 在 D10 登记 `TGA-RESEARCH-001` / `TGA-CONTRACT-001` 等任务，并更新 `docs/implement/01-dispatch-log.md`。

**交付物**：探测脚本、数据字典、契约变更清单、fixtures、风险清单。

**退出条件**：所有 P0 表字段/单位/覆盖有官方证据（含三段账户分期）；5 个新类型契约评审通过；没有未验证字段进入设计。

### Phase 1 — 数据接入（DTS/拍卖）

**目标**：DTS/拍卖可自动采集、可回溯、可增量；Refunding 留 Phase 4。

任务：

1. `src/chronoforge/connectors/treasury_fiscal.py`：表分发、分页、过滤、重试、限流、schema 校验、历史分期映射。
2. `src/chronoforge/connectors/treasury_refunding.py`：索引页、HTML/PDF、哈希、结构化解析（Phase 4 落地，可提前建骨架）。
3. 新 Canonical Types + `models/identity.py` + `storage/canonical.py::_SCHEMAS` + architecture tests。
4. `src/chronoforge/registry/service.py` 增加 2 个 source；`scripts/register_treasury_datasets.py` 注册数据集；`bootstrap_defaults` 幂等。
5. `src/chronoforge/quality/rules.py` 追加记录级 Q-TGA-001/002/007/008/009；`src/chronoforge/config/update_schedule.py` 增加两个源发布时刻；`src/chronoforge/pipeline/windows.py` 增加 5 天 overlap/30 天 chunk；`src/chronoforge/cli/_wiring.py` 增加 connector 分支。
6. 回填至少 10 年 DTS（历史分期映射后实际可得 2005-10-03 起）+ 拍卖历史；增量 cursor 验证。
7. `docs/tga/DATA_DICTIONARY.md` 与 `OPERATIONS.md` 初版。

**验收**：

- 能获取最新 DTS；能获取 ≥10 年历史（或记录实际范围，不伪造）。
- 能获取拍卖历史与未来已公告拍卖。
- 每条记录有 `source/source_id/fetch_time/release_time/raw_record_id`。
- 分页无重叠/缺口；字段漂移会熔断。
- `run_log`/`quality_flags` 可查。

### Phase 2 — 历史现金流重建（Model 0）

**目标**：能解释每日 TGA 变化，残差可审计。

任务：

1. `scripts/tga_common.py`：单位换算、taxonomy 匹配、联邦工作日/税期/支付周期、配置加载与 hash。
2. `scripts/tga_reconstruct.py`：日余额序列（按 `balance_type` 读取，单行期间由 Connector 已拆好）、连续性、Table II/IV/V 归一、UNCLASSIFIED。
3. `scripts/tga_reconstruct.py`：IIIA/IIIB 现金口径、三类残差、月度对账。
4. `config/tga/taxonomy.yaml`：原始分类 → 归一分类、`is_subtotal`/`is_internal` 初版。
5. `scripts/tga_store.py`：写 `tga_daily_balance` / `tga_daily_cashflow` / `tga_debt_cash_basis` / `tga_reconciliation`，在 `meta/tga.duckdb` 注册视图。
6. `scripts/tga_store.py`：写 `tga_anomaly`（表结构来自迁移 0005）与异常流程。
7. `scripts/tga_reconstruct.py`：Model 0 报告（残差分布、未分类、重复项）输出到 `reports/tga/`。

**验收**：§5.6 全部达标；不达标不得进入 Phase 3。

### Phase 3 — 基准预测（Model 1/2，7/14/30 天）

**目标**：可解释的短期预测 + 报告 MVP。

任务：

1. `scripts/tga_forecast.py`：拍卖事件日历（发行/到期/付息）。
2. `scripts/tga_forecast.py`：税收预测（事件日历 + 同事件中位数 + 趋势）。
3. `scripts/tga_forecast.py`：支出预测（周期识别 + 日分布）。
4. `scripts/tga_forecast.py`：发行/到期/付息三层预测。
5. `scripts/tga_forecast.py`：其他/未识别现金流。
6. `scripts/tga_forecast.py`：聚合器（事件去重、守恒、累计 TGA）。
7. `scripts/tga_store.py`：ForecastStore 读写 + 迁移 0005 生成的表 + `tga_forecast` 视图。
8. `scripts/tga_report.py`：日报 MVP（Markdown 表格，不含图表）。
9. 不新增 CLI；`scripts/tga_forecast.py` / `scripts/tga_report.py` 即可执行入口。

**验收**：7/14/30 天预测可复现；事件只计一次；报告区分 known/predicted/unclassified；无未来数据。

### Phase 4 — 中期与情景（60/90 天）

**目标**：接入 MTS/Refunding，输出 5 个 horizon 与 3 情景。

任务：

1. `treasury_fiscal` 增加 MTS 表；映射到现有 `FLOW`。
2. `treasury_refunding` 结构化公告 → `TREASURY_ANNOUNCEMENT`；历史版本保留。
3. 融资缺口/目标余额参考（只作压力估计，不冒充官方目标）。
4. `scripts/tga_forecast.py` + `config/tga/scenarios.yaml`：三情景配置与组件级调整；情景只作用于预测类。
5. `scripts/tga_forecast.py`：不确定性区间（Walk-forward 残差分位数；初始可用简化分位数，Phase 5 校准）。
6. `scripts/tga_forecast.py`：置信度构成（金额占比）。
7. `scripts/tga_report.py`：日报增加 60/90 与情景列。

**验收**：5 个 horizon 输出；3 情景可配置；Refunding 版本可按 cutoff 过滤；融资缺口不直接当作发行计划。

### Phase 5 — 历史回测

**目标**：证明样本外有效性，识别误差来源。

任务：

1. `scripts/tga_backtest.py`：逐日/步长回测、PIT 过滤、run 记录。
2. `scripts/tga_backtest.py`：MAE/RMSE/MAPE/direction/bias/coverage/pinball。
3. `scripts/tga_backtest.py`：类别误差 + timing/unclassified 归因。
4. `scripts/tga_backtest.py`：基准模型（Naive / Historical Mean / Seasonal Naive / Event-driven / Statistical / Ensemble）。
5. `tests/unit/test_tga_leakage.py`：Leak Sentinel 测试。
6. `scripts/tga_store.py`：写 `tga_forecast_eval` / `tga_attribution`；`scripts/tga_report.py` 输出回测报告。
7. `docs/tga/BACKTEST.md`：结果、限制、模型选择结论。

**验收**：§7.6 全部达标；所有预测可追溯；无泄漏；基准对比完整；误差归因可解释。

### Phase 6 — 自动化、报告与接口

**目标**：持续运行，不需要人工每天下载/跑模型。

任务：

1. `scripts/tga_daily.py` + `--due-only` 闸门 + 锁。
2. 接入 `hourly_update.sh` 或独立前台循环（不新增系统服务）。
3. `scripts/tga_report.py` 日报完整版（含回测表现、结论、免责声明）。
4. `scripts/tga_export_liquidity.py` + `latest.json` + schema 版本。
5. `scripts/tga_report.py`：P1 图表（Plotly，可选依赖）。
6. 异常通知（日志/可选 macOS/webhook）。
7. `docs/tga/OPERATIONS.md` 完整化：安装、回填、重放、故障处理、源变更。
8. 最终验收清单逐项核对。

**验收**：连续多日自动运行；数据延迟/失败可见；报告可复现；接口稳定；文档完整。

### Phase Report 模板（原规范 §17.4）

每个 Phase 完成后，按本仓库 `docs/implement/00-task-management.md` 的记录规则提交：

```text
Phase Report
├── Completed Tasks
├── Data Sources
├── Data Coverage
├── Database Changes
├── Model Changes
├── Tests Executed
├── Test Results
├── Known Issues
├── Remaining Work
└── Next Phase Plan
```

同时更新 `docs/implement/01-dispatch-log.md` 与对应 `tasks/{TASK-ID}.md`；验收未通过不得进入下一 Phase。

### 任务编号建议

| Phase | 任务组 |
|---|---|
| 0 | `TGA-RESEARCH-001`（探测）、`TGA-CONTRACT-001`（契约） |
| 1 | `TGA-ACQ-001`（fiscal connector）、`TGA-ACQ-002`（refunding）、`TGA-MODEL-001`（新类型）、`TGA-QUAL-001` |
| 2 | `TGA-RECON-001..004`、`TGA-STORE-001` |
| 3 | `TGA-FCST-001..006`、`TGA-SCRIPT-001` |
| 4 | `TGA-FCST-007..009`、`TGA-SCEN-001` |
| 5 | `TGA-BT-001..003` |
| 6 | `TGA-OPS-001`、`TGA-REPORT-001`、`TGA-EXPORT-001` |

---

## 13. 验收清单（映射原规范 §16.4）

| 项目 | 验收要求 | 本方案落点 | Phase |
|---|---|---|---|
| 数据采集 | 自动化、可追溯 | Connector + Raw + registry + run_log | 1/6 |
| 历史数据 | 尽可能覆盖 10 年 | Phase 0 探测 + Phase 1 回填；不足记录实际范围 | 0/1 |
| 数据完整性 | 有完整性检查 | Q-TGA-001..015 | 1/2 |
| 现金流重建 | 能解释每日 TGA 变化 | Model 0 三类残差 + 守恒 | 2 |
| 预测周期 | 7/14/30/60/90 天 | 5 horizon | 3/4 |
| 预测方法 | 可解释 | 事件驱动 + 季节性；统计模型需样本外胜出 | 3/4/5 |
| 历史回测 | Walk-forward | 逐日/步长回测 | 5 |
| 数据泄漏 | 不允许 | release_time 过滤 + Leak Sentinel | 1/5 |
| 预测误差 | 有完整统计 | MAE/RMSE/MAPE/direction/coverage | 5 |
| 情景分析 | 至少 3 种 | base/liquidity_easing/liquidity_tightening | 4 |
| 自动化 | 定时运行 | `scripts/tga_daily.py --due-only` | 6 |
| 文档 | 完整 | DATA_DICTIONARY / MODEL_METHODOLOGY / OPERATIONS / BACKTEST | 0–6 |
| 可追溯 | Raw→Canonical→Derived→Forecast | provenance + snapshot_id + config_hash | 全阶段 |
| 不伪装 | 预测/情景/事实分离 | `source_type` + 报告标注 | 3/4 |

---

## 14. 风险与开放问题

### 14.1 风险

| 风险 | 影响 | 缓解 |
|---|---|---|
| DTS 字段/账户口径变化 | 余额取错、重建失败 | Phase 0 探测 + 字段映射版本化 + schema 熔断 |
| `close_today_bal` 空值 | 误取 0 | 按 `account_type`/行号映射 + 单测 |
| 单位混用 | 预测量级错误 | 单位登记表 + 跨源量级校验 + Q-TGA-007 |
| IIIA/IIIB 重复计入 | 净现金流虚高 | Table II 为台账；IIIA/IIIB 只校验；守恒测试 |
| IV/V 重复计入 | 退款/内部转移双计 | `include_in_net=false` + 跨表告警 |
| DTS 无历史 vintage | 回测泄漏 | release_time 估算 + `ESTIMATED` 标注 + 上线后 raw 快照 |
| 拍卖结果延迟 | 回测误用结果 | announced/result 状态机 + cutoff 规则 |
| Upcoming 覆盖仅 2024+ | 历史回测无公告计划 | 用 auctions_query 的 announcement_date 重建 |
| Refunding 解析失败 | 60/90 天输入缺失 | 原文保留 + 置信度 + 人工 override；不猜数字 |
| IRS 日历变化 | 税期预测偏差 | 年度复核 YAML + DTS 税峰校验 |
| TIPS/FRN 复杂性 | 付息误差 | P0 近似 + DTS 交叉校验；P1 CPI/FRN 指数 |
| 债务上限/停摆 | 结构性断点 | 只标记异常/情景，不预测政策 |
| 过拟合 | 回测虚好 | 简单基准 + 样本外 + 模型选择门槛 |
| 调度失败 | 报告缺失 | 锁 + 到期闸门 + 状态 + 告警 |
| 数据源限流/中断 | 采集中断 | 重试/退避/熔断 + 增量续传 |

### 14.2 开放问题（需在 Phase 0 决策）

1. DTS 历史实际起点与完整度（以探测为准；若不足 10 年，回测起点相应后移）。
2. Table II 小计/叶级的确切结构（API 无父子字段，需要人工复核一次）。
3. IIIB 符号约定（用真实数据验证，不凭文档假设）。
4. Refunding 文档中 PDF 表格的解析覆盖率；是否需要对部分季度人工录入。
5. 是否将 `WTREGEN` 纳入 P0 校验（建议 P1，避免日频/周度混用）。
6. 90 天预测在数据不足时是否降级为"参考区间"而非点预测。
7. 是否允许 CMB 情景进入 base case（建议默认不进入）。

---

## 15. 附录

### 15.1 新类型自然键与 Arrow 字段清单

| 类型 | 自然键 | 分区时间字段 | revision 类 |
|---|---|---|---|
| `TREASURY_ACCOUNT_BALANCE` | `source_id, observation_time, source_table, account_type_raw, src_line_nbr, balance_type` | `observation_time` | 否 |
| `TREASURY_CASH_FLOW` | `source_id, observation_time, source_table, transaction_type_raw, raw_category, src_line_nbr` | `observation_time` | 否 |
| `TREASURY_DEBT_TRANSACTION` | `source_id, observation_time, source_table, transaction_type_raw, security_market, security_type, security_type_desc, adjustment_type, src_line_nbr` | `observation_time` | 否 |
| `TREASURY_AUCTION` | `source_id, auction_key` | `auction_date` | 否 |
| `TREASURY_ANNOUNCEMENT` | `source_id, publication_time, document_type, forecast_period_start, forecast_period_end, security_type, maturity_bucket, revision_time` | `publication_time` | 是 |

> `_SCHEMAS` 必须为每个类型声明显式 Arrow schema；日期用 `date32`，时间用 `timestamp[us]`，金额/置信度 `float64`，枚举/文本 `string`，布尔 `bool`。

### 15.2 配置样例

`config/tga/taxonomy.yaml`：

```yaml
version: "1"
rules:
  - match: "Taxes - Individual Income"
    category: TAX_INDIVIDUAL
    flow_type: TAX
    direction: INFLOW
    include_in_net: true
    is_subtotal: false
    confidence: 1.0
  - match: "Taxes - Corporation Income"
    category: TAX_CORPORATE
    flow_type: TAX
    direction: INFLOW
    include_in_net: true
    is_subtotal: false
    confidence: 1.0
  - match: "Public Debt - Issues"
    category: DEBT_ISSUANCE
    flow_type: DEBT_ISSUANCE
    direction: INFLOW
    include_in_net: true
    is_subtotal: false
    confidence: 1.0
  - match: "Public Debt - Redemptions"
    category: DEBT_REDEMPTION
    flow_type: DEBT_REDEMPTION
    direction: OUTFLOW
    include_in_net: true
    is_subtotal: false
    confidence: 1.0
  - match: "Inter-agency"
    category: INTERNAL_TRANSFER
    flow_type: INTERNAL
    direction: INTERNAL
    include_in_net: false
    is_subtotal: false
    confidence: 0.9
  - match: "Total*"
    category: SUBTOTAL
    flow_type: OTHER
    direction: UNKNOWN
    include_in_net: false
    is_subtotal: true
    confidence: 1.0
unmatched:
  category: UNCLASSIFIED
  flow_type: UNCLASSIFIED
  direction: UNKNOWN
  include_in_net: true
  is_subtotal: false
  confidence: 0.1
```

`config/tga/calendar.yaml`：

```yaml
federal_holidays: "USFederalHolidayCalendar"   # pandas 内置，代码实现
tax_events:
  - id: individual_estimated_q1
    due_date: "04-15"
    categories: [TAX_INDIVIDUAL]
    source: "IRS Pub 509"
  - id: corporate_estimated_q1
    due_date: "04-15"
    categories: [TAX_CORPORATE]
    source: "IRS Pub 509"
outlay_cycles:
  social_security:
    period: monthly
    day_rule: "2nd/3rd/4th Wednesday"
  federal_pay:
    period: biweekly
```

`config/tga/model.yaml`：

```yaml
horizons: [7, 14, 30, 60, 90]
min_train_years: 5
interval_levels: [0.5, 0.8, 0.95]
default_model: event_driven
enable_statistical: false
enable_cmb_scenario: false
```

### 15.3 数据源核验记录（2026-10-31）

| 数据 | 端点/入口 | 实测要点 |
|---|---|---|
| DTS Table I | `/v1/accounting/dts/operating_cash_balance` | 2005-10-03 起；三段命名：≤2021-09-30 `Federal Reserve Account`、2021-10-01–2022-04-15 单行 `Treasury General Account (TGA)`、≥2022-04-18 四行 TGA；后段 Closing 行金额在 `open_today_bal`，`close_today_bal` 为 null；单位 USD millions |
| DTS Table II | `/v1/accounting/dts/deposits_withdrawals_operating_cash` | `transaction_type`、`transaction_catg`、`transaction_today_amt`；单位 USD millions |
| DTS IIIA | `/v1/accounting/dts/public_debt_transactions` | `security_market`、`security_type`、`security_type_desc`；单位 USD millions |
| DTS IIIB | `/v1/accounting/dts/adjustment_public_debt_transactions_cash_basis` | `adj_type` 含 "Public Debt Issues (Table IIIA)" / "Bills (-) Discount..."；符号需实测 |
| DTS IV | `/v1/accounting/dts/inter_agency_tax_transfers` | `classification`、`today_amt`；内部转移；实测仅 2023-02-14 起 |
| DTS V | `/v1/accounting/dts/income_tax_refunds_issued` | `tax_refund_type`、`tax_refund_today_amt`；退款 |
| 拍卖结果 | `/v1/accounting/od/auctions_query` | 1979-11-15 起；`offering_amt` 为 USD；结果字段在结果前可能 null；`announcemt_date` 拼写特殊 |
| 未来拍卖 | `/v1/accounting/od/upcoming_auctions` | 实测仅 2024-03-08 起（约 101 条）；不能用于更早回测 |
| MTS | `/v1/accounting/mts/mts_table_*` | 18 个数据表；金额 USD；按 official release date 设 release_time |
| Quarterly Refunding | `home.treasury.gov/.../quarterly-refunding` | HTML/PDF 文档；无稳定 JSON；按 publication_time 版本化 |
| Fed H.4.1 | FRED `WTREGEN` | 周度；仅交叉校验 |

> 以上为 2026-10-31 实测样例；Phase 0 必须以官方 `meta.labels` 与数据字典再次核对，并把样例写入 fixtures。不得把本表当作永久字段契约。

### 15.4 术语

| 术语 | 含义 |
|---|---|
| TGA | Treasury General Account，财政部在美联储的主要现金账户 |
| DTS | Daily Treasury Statement，每日财政部报表 |
| MTS | Monthly Treasury Statement，月度预算报表 |
| IIIA/IIIB | 公共债务交易表 / 调整至现金口径表 |
| PIT | Point-in-time，按当时可用数据回测 |
| Known | 已公布、日期/金额确定的现金流 |
| Predicted | 模型预测的现金流 |
| Scenario | 用户假设的现金流调整 |
| Unclassified | 未识别/未映射现金流 |
| CMB | Cash Management Bill |
| Gross Issuance | 总发行 |
| Net Issuance | 净发行 = 总发行 − 到期偿还 |
| Rollover | 到期再融资 |
| New Money | 新增融资 |

---

## 16. 最小可行路径（如果只做一条线）

若资源有限，建议按以下顺序交付，任何一步都不能跳：

1. **Phase 0 + Phase 1**：DTS Table I/II + 拍卖历史/公告接入。
2. **Phase 2**：Model 0 重建，Table I/II 总量对账通过。
3. **Phase 3**：7/14/30 天事件驱动 + 季节性预测 + Markdown 报告。
4. **Phase 5**：先做 7/14/30 天 Walk-forward，再扩 60/90。
5. **Phase 4**：Refunding/MTS/情景。
6. **Phase 6**：自动化与接口。

理由：没有 Model 0 的重建，预测只是拟合；没有 Phase 5 的回测，无法证明预测有效；没有 Phase 1 的 provenance，无法复现。
