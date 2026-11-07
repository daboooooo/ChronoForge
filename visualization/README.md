# USD 流动性可视化站点

把 `docs/USD_Liquidity/visualization_report.md` 的 16 张图做成离线可用的数据展示页面。

## 使用

```bash
bash visualization/refresh.sh
```

该命令会：

1. 运行 `visualization/export_data.py`（只读本地库），重新生成 `visualization/data.js`；
2. 用系统默认浏览器打开 `visualization/index.html`。

也可以手动执行：

```bash
uv run python visualization/export_data.py
open visualization/index.html
```

页面完全离线：ECharts 5.5.1 位于 `vendor/echarts.min.js`，数据通过 `data.js` 直接以 `window.CF_DATA` 注入，不依赖 CDN，也不需要 `fetch` 本地 JSON。

## 文件

| 文件 | 说明 |
|---|---|
| `index.html` | 站点入口：总览 + 三张 Dashboard |
| `styles.css` | 深色科技风样式 |
| `app.js` | 图表配置与交互 |
| `export_data.py` | 只读数据导出脚本 |
| `data.js` | 生成的页面数据（`window.CF_DATA`） |
| `data.json` | 同一份数据的可读版本，便于调试 |
| `vendor/echarts.min.js` | 本地 ECharts |
| `refresh.sh` | 一条命令刷新 + 打开 |

## 数据口径

- OHLCV：直读 `data/canonical/OHLCV/entity=...` Parquet 分区；
- NUMBER / FLOW：经只读 `QueryService`，按 `revision_time` 去重保留最新版本；
- TGA 日度 / 预测：读取既有 `data/derived/tga_daily_balance` 与 `data/derived/tga_forecast`；
- SRF 用量：NY Fed Markets API，canonical `NUMBER/entity=srf_usage`（USD，日频）；
- 导出脚本对本地库只读，不写 `data/`、`meta/`；采集侧连接器与注册脚本单独维护。

新数据源首次接入（已在本仓库执行过，可重复运行）：

```bash
uv run python scripts/register_symbols_in_json_files.py --only z_manual_yahoo_symbols.json
uv run python scripts/backfill_datasets.py --sources yahoo --entities TLT IWM '^RUT'
uv run python scripts/register_nyfed_datasets.py
uv run python scripts/backfill_datasets.py --sources nyfed --start 2021-07-29
```

## 缺失项（页面内已标注“数据待补充”）

- DGS5（图 10，用 20Y 补位并标注）
- 准备金预测运行输出（图 16；`forecast_runs` 仅有 TGA 模型，方案待确认后建模）

缺失序列不使用替代值、估算值或占位数值。

