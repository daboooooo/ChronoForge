# USD 流动性可视化站点

把本地 ChronoForge 库的 canonical / derived 数据渲染成离线可用的展示页面：**18 个卡片（编号 1–18）+ 3 张 Dashboard**（`pane-overview` 总览 + `pane-d1/d2/d3`）。ECharts 容器共 18 个（`chart8b` 是 `chart8` 的副图，与卡 8 同卡）；卡 15 是无图表容器的 DOM 仪表盘。

> 数量与实现以 `index.html` / `app.js` 为准。本文件此前写的"16 张图"是图表扩张前的旧值，已按代码修正（审计 M-N5）。

## 使用

```bash
bash visualization/refresh.sh
```

该命令会：

1. 运行 `visualization/export_data.py`（只读本地库），重新生成 `visualization/data.js` + `data.json`；
2. 用系统默认浏览器打开 `visualization/index.html`。

也可以手动执行：

```bash
uv run python visualization/export_data.py
open visualization/index.html
```

页面完全离线：`index.html` 依次加载 `vendor/echarts.min.js` → `data.js` → `app.js`；`app.js` 只读取 `window.CF_DATA`，不发起网络请求，不依赖 CDN。

## 数据流

```
data/canonical/**  +  data/derived/**            （只读，不写回）
          │  QueryService / Parquet
          ▼
visualization/export_data.py::build_payload()
          │  json.dumps → "window.CF_DATA = {...};"
          ▼
visualization/data.js（生成物）+ data.json（同数据的可读版）
          │  <script src="data.js"> → <script src="app.js">
          ▼
index.html（18 卡片 + 3 Dashboard） ← app.js::renderers（ECharts 5.5.1）
```

- `export_data.py` 只读 `data/` 与元数据库，**不写** `data/`、`meta/`，也不经过 pipeline 阶段。
- `payload` 顶层键：`meta` / `cutoffs` / `series` / `weekly` / `btc_res` / `corr` / `macro` / `russell` / `yield_curve` / `yield_split` / `tga` / `reserve_forecast` / `framework` / `status`（`build_payload()` 为单一事实源）。
- 缺失序列不估算、不用占位值：`meta.note` 固定声明"缺失序列不做估算；partial 图表只展示真实可得的序列"。

## 文件边界

| 文件 | 类型 | 说明 |
|---|---|---|
| `index.html` | **源（入库）** | 站点入口：总览 + 三张 Dashboard |
| `styles.css` | **源（入库）** | 深色科技风样式 |
| `app.js` | **源（入库）** | 图表配置与交互；`D = window.CF_DATA` |
| `export_data.py` | **源（入库）** | 只读数据导出脚本 |
| `refresh.sh` | **源（入库）** | 一条命令刷新 + 打开 |
| `vendor/echarts.min.js` | **源（入库）** | 本地 ECharts 5.5.1 |
| `data.js` | **生成物（忽略）** | `window.CF_DATA`，由 `export_data.py` 再生；已在 `.gitignore` 排除 |
| `data.json` | **生成物（忽略）** | 同一份数据的可读版本，便于调试；已在 `.gitignore` 排除 |

## 数据口径

- **OHLCV**：直读 `data/canonical/OHLCV/entity=...` Parquet 分区；
- **NUMBER / FLOW**：经只读 `QueryService`，按 `revision_time` 去重保留最新版本；
- **TGA 日度 / 预测**：读取 `data/derived/tga_daily_balance` 与 `data/derived/tga_forecast`；
- **准备金预测**：读取 `data/derived/reserve_forecast`、`reserve_forecast_components`、`reserve_forecast_eval`（模型口径见 `docs/design/11-reserve-model.md`）；
- **SRF 用量**：NY Fed Markets API → canonical `NUMBER/entity=srf_usage`（USD，日频）。

### 方法学标注（与实现一致的表述）

- 准备金的 PIT 截断按 `max(observation_time, release_time, revision_time)` 判断；origin 周三收盘只看到上周三的观测（H.4.1 / SOMA 周四才发布）。
- 图 16 的 90% 带是**逐 horizon 边际**误差分位（近 5 年残差 `q05/q95`，horizons 1/2/4），**不能**读作"未来 4 周同时落在带内"的概率（审计 M-N2）。
- 站点此前把该带标为"联合 90%"、把 PIT 写成"只用该时点之前的数据"，均为错误表述；现按 `scripts/reserve_forecast.py` 的实现改正（审计 M-N6）。

## 首次接入 / 回填命令

```bash
uv run python scripts/register_symbols_in_json_files.py --only z_manual_yahoo_symbols.json
uv run python scripts/backfill_datasets.py --sources yahoo --entities TLT IWM '^RUT'
uv run python scripts/register_nyfed_datasets.py
uv run scripts/backfill_datasets.py --sources nyfed --start 2021-07-29
```

## 缺失项（如实声明）

- **DGS5**：源端 `fred_series_DGS5` 未接入；收益率曲线用 `DGS20` 补位，缺失事实由 `export_data.py` 写入 `payload.yield_curve.missing_tenor = "DGS5"`。**待办**：`app.js` / `index.html` 目前未读取该字段在页面上显式标注（旧 README 写的"图 10 标注"在 UI 中不存在），需补一处文案或图例——属实现改动，本轮按约束未动。
- 准备金预测此前列为缺失项（图 16 "数据待补充"），现已由 `scripts/reserve_forecast.py` 产出，图 16 有完整渲染路径（`app.js::renderers.chart16`）。

缺失序列不使用替代值、估算值或占位数值。
