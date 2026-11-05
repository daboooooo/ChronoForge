## 使用方式（按执行顺序）

```bash
# 1. 数据采集（增量；8 个数据集，通常只需在 DTS 发布后跑）
uv run chronoforge pipeline run --dataset treasury_fiscal_dts_operating_cash_balance
# … 其余 7 个同构；或逐个跑完 8 个（不建议 --all-due，会带上全仓库数据源）

# 2. 历史现金流重建（Model 0；数据更新后重跑，全量覆盖 Derived）
uv run python scripts/tga_reconstruct.py          # 可选 --start/--end/--dry-run

# 3. 基准预测（Model 1/2；同配置已有 SUCCESS 会复用，--force 强制重跑）
uv run python scripts/tga_forecast.py             # 可选 --origin/--data-cutoff/--horizons

# 4. 生成日报
uv run python scripts/tga_report.py               # 可选 --run <run_id> 指定某次预测
```

辅助工具（任何时刻可查，不产生数据）：

```bash
uv run python scripts/tga_store.py status            # 数据集/视图/异常总览
uv run python scripts/tga_store.py rebuild-views     # 重建 DuckDB 视图
uv run python scripts/tga_store.py anomalies --status OPEN
uv run python scripts/tga_probe.py                   # 仅 Phase 0 探测用，重跑会覆盖 docs/tga/，勿随意执行
```

日常运维只需记住 **3 步：`tga_reconstruct` → `tga_forecast` → `tga_report`**（采集由 pipeline 负责；Phase 6 的 `tga_daily.py` 会把这三步串成一键）。

## 输出位置（不存 output/）

按方案 D-TGA-17，`output/` 已被 `.gitignore` 忽略且只放运行产物，TGA 产出全部在可入库审阅的位置：

| 产出 | 位置 |
|---|---|
| 重建结果（余额/现金流/对账/债务口径） | `data/derived/tga_daily_balance` 等四个 Parquet 数据集 |
| 预测结果（组件/逐日 TGA） | `data/derived/tga_forecast_components`、`data/derived/tga_forecast`（run 分区） |
| 查询视图 | `meta/tga.duckdb`（`tga_daily_balance` 等 6 个视图） |
| Model 0 重建报告 | [reports/tga/TGA_MODEL0_RECONSTRUCTION_2026-11-02.md](file:///Users/horsenli/Works/ChronoForge/reports/tga/TGA_MODEL0_RECONSTRUCTION_2026-11-02.md) |
| 每日预测报告 | [reports/daily/TGA_Forecast_2026-11-02.md](file:///Users/horsenli/Works/ChronoForge/reports/daily/TGA_Forecast_2026-11-02.md) |
| 运行元数据 / 异常 | `meta/chronoforge.db` 的 `forecast_runs`、`tga_anomaly` 表（`tga_store.py status` 可查） |

当前最新一次预测结果：T+7 **982.1B** / T+14 **888.1B** / T+30 **770.5B**（数据截止 2026-10-29），已写入上面的日报。