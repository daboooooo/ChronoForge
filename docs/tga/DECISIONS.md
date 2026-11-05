# TGA 子系统决策记录

依据：`docs/strategy/TGA前瞻模型-实现方案.md` v1.6（2026-11-04）。
本文件只记录已裁决的实现决策；未决事项回到方案 §14.2。

| 决策 | 内容 | 状态 |
|---|---|---|
| D-TGA-01 | 不新建独立工程 / 不新增 `src/chronoforge/tga/`；预测/回测/报告放 `scripts/tga_*.py` | 已采纳 |
| D-TGA-02 | Parquet + DuckDB + SQLite，不引入 PostgreSQL | 已采纳 |
| D-TGA-03 | 新增 5 个 Treasury Canonical Type，走受控契约变更 | 已落地（D02/D03/代码/测试） |
| D-TGA-04 | Table II 为现金台账；Table I 为余额锚与总量校验；IIIA/IIIB 只做交叉校验与归因 | 已采纳 |
| D-TGA-05 | 数据源拆 `treasury_fiscal` 与 `treasury_refunding` | 两者均已落地（`connectors/treasury_fiscal.py`、`treasury_refunding.py` + `_wiring`/窗口/注册脚本） |
| D-TGA-06 | 预测输出放 Derived；`forecast_runs` / `backtest_runs` 放 SQLite 元数据 | 已落地（迁移 0005 + `scripts/tga_store.py` 的 ForecastStore/BacktestStore） |
| D-TGA-07 | 全链路 `release_time` 点时账本；历史回填标 `ESTIMATED` | 已落地（Connector） |
| D-TGA-08 | P0 默认事件驱动 + 季节性基准；统计模型需样本外胜出 | 已采纳 |
| D-TGA-09 | 不新增系统服务，接入现有前台调度循环 | 已采纳（调度方式确定；`scripts/tga_daily.py` 仍待 Phase 6） |
| D-TGA-10 | 税收日历用年度人工复核 YAML + 历史税峰校验 | 已落地（`config/tga/calendar.yaml` 的 `tax_events` / `refund_season_months`） |
| D-TGA-11 | 情景只作用于预测类现金流，不调整已知现金流 | 已采纳 |
| D-TGA-12 | 不确定性来自 Walk-forward 残差分位数 | 已采纳 |
| D-TGA-13 | `src` 只承载数据基础设施，TGA 业务逻辑只放 `scripts` | 已采纳（`test_tga_boundaries.py` 守卫） |
| D-TGA-14 | DTS 三段账户映射：`Federal Reserve Account`（≤2021-09-30）、单行 TGA（2021-10-01–2022-04-15）、四行 TGA（≥2022-04-18）；单行拆 OPENING/CLOSING | 已落地（Connector + 测试） |
| D-TGA-15 | `dataset_id` 统一下划线格式 `{source_id}_{dataset_type}`；D03 同步 | 已落地（D03/注册脚本/窗口配置） |
| D-TGA-16 | TGA Derived 视图放独立 `meta/tga.duckdb`，不写 `meta/query.duckdb` | 已采纳（Phase 2 落地） |
| D-TGA-17 | 日报/回测报告写 `reports/`；`output/` 只放运行产物 | 已采纳（Phase 6 落地） |
| D-TGA-18 | `is_subtotal` 作为 `TREASURY_CASH_FLOW` 一等字段；`builtin-v1` taxonomy 仅过渡 | 已落地（`config/tga/taxonomy.yaml` + 模型/schema/Connector） |

## 数据源实测结论（2026-10-31）

- DTS Table I/II 可用起点：2005-10-03；Table IV 仅 2023-02-14 起；拍卖结果 1979-11-15 起；`upcoming_auctions` 仅 2024-03-08 起。
- 历史回填的释放时刻为估算值（DTS：下一联邦工作日 21:00 UTC），不得当作真实 vintage；回测必须按 `release_time <= data_cutoff` 过滤并标注 `ESTIMATED`。
- `Federal Reserve Account` 在 2021-09-30 及以前承载 TGA 余额；该判断已写入 Connector 与 D02 契约。
