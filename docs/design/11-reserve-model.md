# D11. 准备金结构模型（reserve_forecast）

| 项 | 内容 |
|---|---|
| 实现 | `scripts/reserve_forecast.py` |
| 模型版本 | `reserve-structural-0.1.2`（`MODEL_VERSION`） |
| 频率 / 时点 | 周频；origin = 周三收盘（`origin + 1d − 1s`） |
| horizons | 1 / 2 / 4 周（`HORIZONS`） |
| scenario | `base`（唯一） |
| 输出 | `data/derived/reserve_forecast`、`reserve_forecast_components`、`reserve_forecast_eval`；`forecast_runs` 记 run |

> 本文所有事实均从源码与配置读出，行号会随重构漂移，以函数名为锚点。

## 1. 输入

### 1.1 面板序列（FRED H.4.1，周三水平值，USD 百万）

`_PANEL_SERIES` 共 33 条（21 条全国序列 + 12 个地区联储）：

- 资产侧：`WALCL`（总资产）、`WSHOSHO`（SOMA 证券）、`WUPSHO` / `WUDSHO` / `WLCFLL` / `WORAL` / `SWPT` / `WFCDA` / `WGCAL` / `WABPL`（非 SOMA 已列示资产）
- 负债与资本：`WLTLECL`、`WLFN`（流通中货币）、`WCICL`（硬币）、`WLRRAL`（逆回购）、`WLDLCL`（存款总额）、`WRBWFRBL`（准备金余额）、`WDTGAL`（TGA）、`TERMT`（定期存款）、`WLAD`、`WLDACLC`、`H41RESH4ENWW`（TCCF）
- 12 家地区联储：`D1WLFOL`…`D12WLFOL`（`_DISTRICTS`）

加载口径：`load_panel()` 只保留 `WALCL` 非空且不早于 `2002-12-18` 的周三；同一观测日多条版本按 `available` 保留最后可得的一条。

### 1.2 SOMA 9 分项（NY Fed）

`load_soma()` 读 `nyfed_soma_holdings` 的 9 个 entity（`_SOMA_ENTITIES`），USD → USD 百万（÷1e6）。仅用于结构展示与交叉校验：`soma_cross_check()` 断言 `WSHOSHO ≈ soma_total + soma_tips_inflation`（容差 1.0 USD 百万）。

## 2. PIT 语义（`AsOfFrame`）

- 每个单元格的可得时刻 `available = max(observation_time, release_time, revision_time)`（`_availability`）。
- `AsOfFrame.history(as_of)`：先丢弃 `index > as_of` 的行，再把 `available > as_of` 的单元格置空，最后前向填充——得到"该时点每个科目的最近已知值"。origin 之后发布的数据不会以任何形式进入预测。
- origin 收盘时点：`_asof_cutoff(origin) = origin + 1d − 1s`。H.4.1 与 SOMA 均在次日（周四）23:59:59 可得，因此**当周观测被排除**，锚点通常是上周三。
- 派生科目的可得时刻取构成项的最大值（`add_derived_accounts`）；`TCCF` 在 2002–2019 取值为 0 时按观测日已知处理，避免早期 origin 整行失效。
- 回测 origin 下界：`max(面板首行 + 2 年, _VINTAGE_ARCHIVE_FLOOR)`，`_VINTAGE_ARCHIVE_FLOOR = 2013-04-18`（ALFRED 对 H.4.1 的 vintage 归档起点；更早的观测是批量回填，无法重建首次发布值）。

## 3. 结构恒等式与独立校验

H.4.1 Table 1 负债侧唯一一条独立恒等式（`identity_checks()`）：

```
0 = WLTLECL − (WLFN + WLRRAL + WLDLCL + WLDACLC + WLAD + TCCF)      # L1
```

其余两条旧校验是代数派生，已删除（审计 M-N1 证明）：

- `other_dep` 定义为 WLO 残差 ⇒ 旧存款恒等式 L2 ≡ 0；
- `capital = WALCL − WLTLECL` 且 `other_dep` 为残差 ⇒ 旧准备金重构式 L3 ≡ −L1。

资产侧 `WALCL = WSHOSHO + non_soma_assets + other_assets` 中 `other_assets` 是残差桶，也是定义式，不构成独立校验。`scripts/reserve_forecast.py check` 只 gate L1（容差 3 USD 百万），输出"独立校验数 1"。

## 4. 预测方法（`forecast_from_history`）

| 科目 | 方法 | 说明 |
|---|---|---|
| `soma_securities` | `HOLD_STRUCTURAL` | 锚点值持平 |
| `non_soma_assets` | `HOLD_STRUCTURAL` | 8 项非 SOMA 资产求和后持平 |
| `other_assets_residual` | `HOLD_RESIDUAL` | 残差桶；含 PIT 跨 vintage 对齐额 `anchor_gap` |
| `currency` | `HOLD` | 0.1.0 的季节项经消融移除；`currency_gap = WLFN − WCICL` 单独计入 |
| `reverse_repo` | `HOLD`（含 0 下限） | `max(WLRRAL, 0)` |
| `tga` | `SEASONAL_MEDIAN` | 见下 |
| `other_liabilities` | `HOLD` | 资本 + 定期存款 + 外国官方存款 + 其他存款 + 递延现金项 + 其他负债 + TCCF |
| `reserve_balances` | `IDENTITY` | 由上式倒推 |

TGA 季节项（`_seasonal_cum_change` / `_weekly_medians`）：

- 在 origin 之前的历史周变化中，按目标周的 ISO 周取中位数；
- 目标周样本数 ≥ `_MIN_SEASONAL_SAMPLES = 5` 才采用，否则回退到全历史周变化中位数并标 `SEASONAL_MEDIAN_FALLBACK_ALL`；
- 若 origin 当周尚未发布，锚点是上周三，累计步数需补齐 `anchor → origin` 的周数（`steps = gap_weeks + horizon`）。

锚定残差：PIT 下各科目可能来自不同 vintage，`anchor_gap = WRBWFRBL − implied_anchor` 记入 `other_assets`，保证预测锚定在"当时可知的准备金水平"，而不是被未修订科目带偏。

## 5. 误差带与回测

- `backtest()`：扩张窗口，逐 origin 只用该时点可得的 `AsOfFrame`；同一批 origin 产出 `predicted`、`predicted_v010`、`naive`（最近可知值持平）、`seasonal_naive`（去年同 ISO 周，缺失回退 naive 并标记）。
- 误差带：`band.type = empirical_error_quantile_per_horizon`，取**最近 5 年**（`_BAND_WINDOW_YEARS`）回测残差的 `q05/q95`（`_QUANTILES`），逐 horizon 独立。
- **不是联合区间**：各 horizon 的分位分别计算，不能读作"未来 4 周同时落在带内的概率为 90%"。
- DM 检验：损失 = 绝对误差，对 `v010` / `naive` / `seasonal_naive` 各做一次，Bonferroni `m = 3`；负统计量 = 模型更优。

## 6. 运行与产物

```bash
uv run python scripts/reserve_forecast.py check            # 只校验 L1
uv run python scripts/reserve_forecast.py run --dry-run    # 打印回测与预测，不落库
uv run python scripts/reserve_forecast.py run              # 落库
```

- 落库路径：`data/derived/reserve_forecast`（按 `target_date` 分区）、`reserve_forecast_components`、`reserve_forecast_eval`；写入为原子替换（`_atomic_write`）。
- `forecast_runs`：`ForecastStore.create_run()` 记录 `origin_date`、`data_cutoff_date`、`model_version`、`config_hash`、`scenarios`、`horizons`，成功后 `finalize_run(..., output_hash=...)`。
- `config_hash` 由 `_config()` 的规范化 JSON 取 sha256 前 16 位；`_config()` 同时是"模型到底用什么参数"的单一事实源。

## 7. 已知限制（如实声明，不做修饰）

1. **单版本 canonical**：历史 vintage 不可重建，回测的 `actual` 是最终修订值；PIT 只保证"不看到未发布数据"，不能保证"看到当时的初值"。
2. **误差带是边际的**：逐 horizon 分位，不是联合覆盖；站点图 16 已按此标注（审计 M-N2）。
3. **结构分解不是行为模型**：资产侧与多数负债科目为持有假设，预测力主要来自 TGA 季节项与准备金恒等式；不应作为政策反应函数解读。
4. **SOMA 交叉校验有容差**：`|WSHOSHO − (soma_total + TIPS 通胀补偿)| ≤ 1.0` USD 百万，超出即告警但不阻断 run。
