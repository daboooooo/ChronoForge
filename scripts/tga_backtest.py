#!/usr/bin/env python3
"""TGA Walk-forward 回测与误差归因（Phase 5，依据实现方案 §7/§12 Phase 5）。

- 逐起点/步长回测：每个起点 t 用 data_cutoff 时点数据预测 7/14/30/60/90 天；
  预测组件复用 `tga_forecast.py` 的纯计算入口（与实盘预测完全同源）。
- PIT：所有输入按 release_time/发布规则过滤到 cutoff（§7.2）；估值发布时间
  相关约束由重建层 `ESTIMATED` 标注承接。
- 指标：MAE / RMSE / MAPE / Direction / Bias / Interval Coverage / Pinball。
- 基准：Naive / Historical Mean / Seasonal Naive / Event-driven（§7.4；
  Statistical/Ensemble 属 M3/M4，未实现前不参与对比）。
- 归因：类别级误差 + 未识别残差桶（§7.5 P0）。
- 落盘：`backtest_runs` 元数据 + `tga_forecast_eval` / `tga_attribution`
  （year/month 分区，经 `scripts/tga_store.py`）。

用法：
    uv run python scripts/tga_backtest.py [--start YYYY-MM-DD] [--end YYYY-MM-DD]
        [--stride N] [--horizons 7,14,30,60,90] [--max-origins N]
"""

from __future__ import annotations

# QueryService 行数上限按拍卖全历史读取放宽；必须在 Settings.load() 前设置。
import os

os.environ.setdefault("CHRONOFORGE_QUERY_MAX_ROWS", "500000")

import argparse
import copy
import hashlib
import math
import sys
from datetime import date, datetime, timedelta

import polars as pl
from tga_common import config_hash, load_taxonomy
from tga_forecast import (
    AUCTION_LOOKBACK_YEARS,
    MODEL_VERSION,
    ForecastContext,
    aggregate_forecast,
    apply_scenario,
    business_days,
    compute_base_components,
    load_config,
)

from chronoforge.cli._wiring import open_meta, open_query_service
from chronoforge.config.settings import Settings

# 基准模型（§7.4）；主模型为 tga_forecast 的 MODEL_VERSION
BASELINES: tuple[str, ...] = (
    "naive",
    "historical_mean",
    "seasonal_naive",
    "event_driven",
)
MODEL_VERSIONS: tuple[str, ...] = (MODEL_VERSION, *BASELINES)

# 类别 → 归因误差类型（§7.5 P0：类别级误差 + 残差桶）
ATTRIBUTION_TYPES: dict[str, str] = {
    "TAX": "tax_error",
    "REFUND": "tax_error",
    "DEBT_ISSUANCE": "issuance_error",
    "DEBT_REDEMPTION": "redemption_error",
    "INTEREST": "interest_error",
    "BENEFIT": "outlay_error",
    "OTHER": "outlay_error",
    "UNCLASSIFIED": "unclassified_error",
}

EVAL_SCHEMA: dict[str, pl.DataType] = {
    "origin_date": pl.Date, "data_cutoff": pl.Date, "target_date": pl.Date,
    "horizon": pl.Int64, "model_version": pl.String, "predicted": pl.Float64,
    "actual": pl.Float64, "error": pl.Float64, "abs_error": pl.Float64,
    "pct_error": pl.Float64, "direction_correct": pl.Boolean,
    "lower": pl.Float64, "upper": pl.Float64, "config_hash": pl.String,
}
ATTRIBUTION_SCHEMA: dict[str, pl.DataType] = {
    "origin_date": pl.Date, "data_cutoff": pl.Date, "target_date": pl.Date,
    "horizon": pl.Int64, "model_version": pl.String, "component": pl.String,
    "error_amount": pl.Float64, "error_type": pl.String, "config_hash": pl.String,
}
_AUCTION_DATASET = "treasury_fiscal_auction_results"


# ── 实际值与基准输入 ──────────────────────────────────────────────────


def actual_closings(balance: pl.DataFrame) -> dict[date, float]:
    """日收盘 TGA 实际值（date → USD Million）。"""
    return {
        row["date"]: float(row["closing"])
        for row in balance.iter_rows(named=True)
        if row["closing"] is not None
    }


def _signed_amount() -> pl.Expr:
    return (
        pl.when(pl.col("direction") == "INFLOW")
        .then(pl.col("amount_usd_millions"))
        .when(pl.col("direction") == "OUTFLOW")
        .then(-pl.col("amount_usd_millions"))
        .otherwise(0.0)
    )


def daily_net(cashflow: pl.DataFrame, start: date, end: date) -> pl.DataFrame:
    """叶级日频净现金流（include_in_net，流入 + / 流出 −）。"""
    df = cashflow.filter(
        (~pl.col("is_subtotal"))
        & pl.col("include_in_net")
        & (pl.col("date") >= start)
        & (pl.col("date") <= end)
        & (pl.col("direction").is_in(["INFLOW", "OUTFLOW"]))
    )
    return df.group_by("date").agg(_signed_amount().sum().alias("net")).sort("date")


def daily_net_by_category(
    cashflow: pl.DataFrame, start: date, end: date
) -> pl.DataFrame:
    """叶级日频净现金流按 (date, category) 汇总（归因用）。"""
    df = cashflow.filter(
        (~pl.col("is_subtotal"))
        & pl.col("include_in_net")
        & (pl.col("date") >= start)
        & (pl.col("date") <= end)
        & (pl.col("direction").is_in(["INFLOW", "OUTFLOW"]))
    )
    return df.group_by("date", "category").agg(
        _signed_amount().sum().alias("net")
    )


def boundary_dates(ctx: ForecastContext) -> dict[int, date]:
    """各 horizon 的预测边界日（与 aggregate_forecast 分桶规则一致）。"""
    return {
        int(h): max(d for d in ctx.grid if (d - ctx.origin).days <= int(h))
        for h in ctx.horizons
    }


# ── 基准模型（§7.4）───────────────────────────────────────────────────


def _boundary_predictions(
    ctx: ForecastContext, predicted_by_date: dict[date, float]
) -> dict[int, float]:
    return {
        h: predicted_by_date[d]
        for h, d in boundary_dates(ctx).items()
        if d in predicted_by_date
    }


def baseline_naive(ctx: ForecastContext) -> dict[int, float]:
    """TGA 保持不变（起点余额外推）。"""
    return {int(h): ctx.last_tga for h in ctx.horizons}


def baseline_historical_mean(
    ctx: ForecastContext, total_net: pl.DataFrame
) -> dict[int, float]:
    """历史（近 365 天）工作日日均净现金流外推。"""
    start = ctx.through - timedelta(days=365)
    window = total_net.filter(
        (pl.col("date") > start) & (pl.col("date") <= ctx.through)
    )
    n_days = max(len(business_days(start, ctx.through)), 1)
    mean_net = float(window["net"].sum()) / n_days
    out: dict[int, float] = {}
    for h, d in boundary_dates(ctx).items():
        steps = len(business_days(ctx.origin + timedelta(days=1), d))
        out[h] = ctx.last_tga + mean_net * steps
    return out


def baseline_seasonal_naive(
    ctx: ForecastContext, total_net: pl.DataFrame
) -> dict[int, float]:
    """去年同期（同一日期 −364 天）逐日净现金流累计。"""
    mapping = {
        row["date"]: float(row["net"])
        for row in total_net.iter_rows(named=True)
    }
    boundaries = {d: h for h, d in boundary_dates(ctx).items()}
    cumulative = 0.0
    out: dict[int, float] = {}
    for d in ctx.grid:
        cumulative += mapping.get(d - timedelta(days=364), 0.0)
        if d in boundaries:
            out[boundaries[d]] = ctx.last_tga + cumulative
    return out


def baseline_event_driven(
    ctx: ForecastContext, components: pl.DataFrame
) -> dict[int, float]:
    """只计已公布事件（KNOWN_*），不做统计外推。"""
    known = components.filter(pl.col("source_type").str.starts_with("KNOWN_"))
    daily = known.group_by("target_date").agg(
        pl.col("predicted").sum().alias("net")
    )
    mapping = {
        row["target_date"]: float(row["net"])
        for row in daily.iter_rows(named=True)
    }
    boundaries = {d: h for h, d in boundary_dates(ctx).items()}
    cumulative = 0.0
    out: dict[int, float] = {}
    for d in ctx.grid:
        cumulative += mapping.get(d, 0.0)
        if d in boundaries:
            out[boundaries[d]] = ctx.last_tga + cumulative
    return out


# ── 评估行与归因（§7.3/§7.5）────────────────────────────────────────


def eval_row(
    ctx: ForecastContext,
    horizon: int,
    target: date,
    predicted: float,
    actual: float,
    model_version: str,
    chash: str,
    *,
    lower: float | None = None,
    upper: float | None = None,
) -> dict:
    """单条评估记录（误差、方向正确性、区间）。"""
    error = predicted - actual
    actual_change = actual - ctx.last_tga
    predicted_change = predicted - ctx.last_tga
    return {
        "origin_date": ctx.origin, "data_cutoff": ctx.through,
        "target_date": target, "horizon": int(horizon),
        "model_version": model_version, "predicted": predicted,
        "actual": actual, "error": error, "abs_error": abs(error),
        "pct_error": abs(error) / abs(actual) if abs(actual) >= 1.0 else None,
        "direction_correct": (predicted_change >= 0) == (actual_change >= 0),
        "lower": lower, "upper": upper, "config_hash": chash,
    }


def attribution_rows(
    ctx: ForecastContext,
    eval_targets: dict[int, date],
    components: pl.DataFrame,
    actual_by_category: dict[date, dict[str, float]],
    chash: str,
) -> list[dict]:
    """类别级误差：forecast_component − actual_component（§7.5 P0）。"""
    comps = (
        components.group_by("target_date", "category")
        .agg(pl.col("predicted").sum().alias("predicted"))
    )
    predicted_map = {
        (row["target_date"], row["category"]): float(row["predicted"])
        for row in comps.iter_rows(named=True)
    }
    categories = sorted({cat for cats in actual_by_category.values() for cat in cats}
                        | {cat for _, cat in predicted_map})
    rows: list[dict] = []
    for horizon, target in eval_targets.items():
        actual_cats = actual_by_category.get(target, {})
        for category in categories:
            predicted = predicted_map.get((target, category), 0.0)
            actual = actual_cats.get(category, 0.0)
            if predicted == 0.0 and actual == 0.0:
                continue
            rows.append({
                "origin_date": ctx.origin, "data_cutoff": ctx.through,
                "target_date": target, "horizon": int(horizon),
                "model_version": MODEL_VERSION, "component": category,
                "error_amount": predicted - actual,
                "error_type": ATTRIBUTION_TYPES.get(category, "unclassified_error"),
                "config_hash": chash,
            })
    return rows


# ── Diebold-Mariano 显著性检验（§7.6）────────────────────────────────


def _newey_west_variance(d: list[float], lag: int) -> float:
    """Newey-West HAC 方差（Bartlett 核，lag 阶截断）。"""
    n = len(d)
    mean = sum(d) / n
    dev = [v - mean for v in d]
    var = sum(v * v for v in dev) / n
    for k in range(1, min(lag, n - 1) + 1):
        weight = 1.0 - k / (lag + 1)
        cov = sum(dev[i] * dev[i - k] for i in range(k, n)) / n
        var += 2.0 * weight * cov
    return max(var, 0.0)


def _nw_lag(n: int) -> int:
    """Newey-West 经验滞后阶：floor(4*(n/100)^(2/9))，至少 1。"""
    return max(1, int(4.0 * (n / 100.0) ** (2.0 / 9.0)))


def dm_test(
    main_losses: list[float],
    bench_losses: list[float],
    *,
    hac_lag: int | None = None,
) -> dict[str, object]:
    """主模型 vs 基准的 DM 检验（损失=绝对误差；HAC/Newey-West）。

    损失差分 d_t = |e_main| − |e_bench|；统计量负值表示主模型更优。
    p 值为双侧正态近似；返回 n/均值差/统计量/滞后阶。
    """
    pairs = [
        (a, b) for a, b in zip(main_losses, bench_losses, strict=False)
        if a is not None and b is not None
    ]
    n = len(pairs)
    if n < 8:
        return {"n": n, "mean_loss_diff": None, "dm_stat": None, "p_value": None,
                "hac_lag": None}
    d = [a - b for a, b in pairs]
    mean_diff = sum(d) / n
    lag = hac_lag if hac_lag is not None else _nw_lag(n)
    var = _newey_west_variance(d, lag)
    se = math.sqrt(var / n) if var > 0 else 0.0
    if se == 0:
        stat = 0.0 if mean_diff == 0 else math.copysign(math.inf, mean_diff)
    else:
        stat = mean_diff / se
    p_value = 1.0 if se == 0 and mean_diff == 0 else math.erfc(
        abs(stat) / math.sqrt(2.0)
    )
    return {"n": n, "mean_loss_diff": mean_diff, "dm_stat": stat,
            "p_value": p_value, "hac_lag": lag}


def _adjust_pvalues(pvalues: list[float]) -> tuple[list[float], list[float]]:
    """返回 (Bonferroni, Benjamini-Hochberg FDR) 调整后 p 值。"""
    m = len(pvalues)
    bonf = [min(1.0, p * m) for p in pvalues]
    order = sorted(range(m), key=lambda i: pvalues[i])
    fdr = [0.0] * m
    prev = 1.0
    for rank in range(m, 0, -1):
        idx = order[rank - 1]
        prev = min(prev, min(1.0, pvalues[idx] * m / rank))
        fdr[idx] = prev
    return bonf, fdr


DM_SCHEMA: dict[str, pl.DataType] = {
    "run_date": pl.Date, "horizon": pl.Int64, "benchmark": pl.String,
    "n": pl.Int64, "mean_loss_diff": pl.Float64, "dm_stat": pl.Float64,
    "p_value": pl.Float64, "p_bonferroni": pl.Float64, "p_fdr": pl.Float64,
    "hac_lag": pl.Int64, "significant_bonf": pl.Boolean,
}


def dm_results(
    eval_df: pl.DataFrame,
    main_label: str,
    *,
    run_date: date,
    extra_series: dict[str, pl.DataFrame] | None = None,
) -> pl.DataFrame:
    """对每个 horizon 比较主模型 vs 基准/其他 run 的 MAE 损失。

    extra_series：{标签: 评估行}，用于跨 run 对比校准方案；每条须含
    origin_date/horizon/abs_error，与主模型按 (origin_date, horizon) 配对。
    """
    main = eval_df.filter(pl.col("model_version") == main_label).select(
        "origin_date", "horizon", "abs_error"
    )
    benches = {
        str(v): eval_df.filter(pl.col("model_version") == v).select(
            "origin_date", "horizon", "abs_error"
        )
        for v in sorted(eval_df["model_version"].unique().to_list())
        if v != main_label
    }
    for label, frame in (extra_series or {}).items():
        benches[label] = frame.select("origin_date", "horizon", "abs_error")

    rows: list[dict] = []
    main_map = {
        (r["origin_date"], int(r["horizon"])): float(r["abs_error"])
        for r in main.iter_rows(named=True)
    }
    for label, frame in benches.items():
        bench_map = {
            (r["origin_date"], int(r["horizon"])): float(r["abs_error"])
            for r in frame.iter_rows(named=True)
        }
        for horizon in sorted(main["horizon"].unique().to_list()):
            keys = sorted(
                k for k in main_map
                if k[1] == horizon and k in bench_map
            )
            res = dm_test([main_map[k] for k in keys],
                          [bench_map[k] for k in keys])
            rows.append({
                "run_date": run_date, "horizon": int(horizon),
                "benchmark": label, **res,
            })
    if not rows:
        return pl.DataFrame(schema=DM_SCHEMA)
    df = pl.DataFrame(rows)
    valid_idx = [
        i for i, r in enumerate(df.iter_rows(named=True))
        if r["p_value"] is not None
    ]
    pvals = [float(df["p_value"][i]) for i in valid_idx]
    bonf, fdr = _adjust_pvalues(pvals) if pvals else ([], [])
    bonf_col: list[float | None] = [None] * df.height
    fdr_col: list[float | None] = [None] * df.height
    for pos, i in enumerate(valid_idx):
        bonf_col[i] = bonf[pos]
        fdr_col[i] = fdr[pos]
    df = df.with_columns(
        pl.Series("p_bonferroni", bonf_col, dtype=pl.Float64),
        pl.Series("p_fdr", fdr_col, dtype=pl.Float64),
    ).with_columns(
        (pl.col("p_bonferroni") < 0.05).fill_null(False).alias("significant_bonf")
    )
    return df.select(list(DM_SCHEMA.keys())).cast(DM_SCHEMA)


def print_dm(dm: pl.DataFrame) -> None:
    """打印 DM 表（统计量负值=主模型更优；显著性取 Bonferroni 调整后）。"""
    if dm.is_empty():
        print("  无 DM 检验样本")
        return
    print("| 周期 | 基准 | n | 平均损失差(B) | DM 统计量 | p | p(Bonf) | p(FDR) "
          "| 显著(Bonf 5%) |")
    print("|---|---|---:|---:|---:|---:|---:|---:|---|")
    for row in dm.sort("horizon", "benchmark").iter_rows(named=True):
        def fmt(v: float | None, d: int = 2) -> str:
            return "—" if v is None else f"{v:,.{d}f}"
        mean = (None if row["mean_loss_diff"] is None
                else row["mean_loss_diff"] / 1000)
        print(
            f"| T+{row['horizon']} | {row['benchmark']} | {row['n']} "
            f"| {fmt(mean, 3)} | {fmt(row['dm_stat'], 2)} | {fmt(row['p_value'], 4)} "
            f"| {fmt(row['p_bonferroni'], 4)} | {fmt(row['p_fdr'], 4)} "
            f"| {'YES' if row['significant_bonf'] else 'NO'} |"
        )


# ── 指标（§7.3）─────────────────────────────────────────────────────


def _pinball(q: float, prediction: pl.Expr) -> pl.Expr:
    return (
        pl.when(pl.col("actual") >= prediction)
        .then(q * (pl.col("actual") - prediction))
        .otherwise((1 - q) * (prediction - pl.col("actual")))
    )


def eval_metrics(rows: pl.DataFrame) -> pl.DataFrame:
    """按 (model_version, horizon) 汇总指标（§7.3）。

    Coverage/Pinball 仅在提供区间（lower/upper 非空）时计算；P0 用组件带宽
    近似 10%/90% 分位，属区间质量近似，不是校准后的置信区间。
    """
    if rows.is_empty():
        return pl.DataFrame()
    in_interval = (
        (pl.col("actual") >= pl.col("lower")) & (pl.col("actual") <= pl.col("upper"))
    ).cast(pl.Float64)
    return (
        rows.group_by("model_version", "horizon")
        .agg(
            pl.len().alias("n"),
            pl.col("abs_error").mean().alias("mae"),
            (pl.col("error") ** 2).mean().sqrt().alias("rmse"),
            pl.col("pct_error").mean().alias("mape"),
            pl.col("direction_correct").mean().alias("direction_accuracy"),
            pl.col("error").mean().alias("bias"),
            pl.when(pl.col("lower").is_not_null())
            .then(in_interval).otherwise(None).mean().alias("coverage"),
            pl.when(pl.col("lower").is_not_null())
            .then((_pinball(0.1, pl.col("lower"))
                    + _pinball(0.9, pl.col("upper"))) / 2)
            .otherwise(None).mean().alias("pinball"),
        )
        .sort("model_version", "horizon")
    )


# ── 数据加载 ────────────────────────────────────────────────────────


def _add_years(day: date, years: int) -> date:
    try:
        return day.replace(year=day.year + years)
    except ValueError:  # 2/29
        return day.replace(year=day.year + years, day=28)


# ── Walk-forward 主流程（§7.1）──────────────────────────────────────


def run_backtest(
    settings: Settings,
    start: date | None = None,
    end: date | None = None,
    stride: int = 1,
    horizons: list[int] | None = None,
    scenario: str = "base",
    min_train_years: int = 5,
    max_origins: int = 0,
    *,
    dry_run: bool = False,
    calibration: str | None = None,
    calendar_events: bool | None = None,
    dm_runs: list[str] | None = None,
) -> tuple[int, dict]:
    """执行 Walk-forward 回测。返回 (退出码, 摘要 dict)。

    calibration：覆盖 issuance.calibration.mode（none/trailing_scale/
    seasonal_window），用于在同一 Walk-forward 样本上对比候选校准方案。
    calendar_events：覆盖 calendar.enabled（日历层总开关），用于同口径
    对照“有/无日历层”，不影响 KNOWN_* 与发行校准。
    dm_runs：额外参与 DM 检验的历史 run_id（跨 run 对比校准方案）。
    """
    model, calendar_cfg, scenarios_cfg = load_config()
    if calibration:
        model = copy.deepcopy(model)
        model.setdefault("issuance", {}).setdefault("calibration", {})[
            "mode"
        ] = calibration
    label_suffixes: list[str] = []
    if calibration is not None:
        label_suffixes.append(calibration)
    if calendar_events is not None:
        calendar_cfg = copy.deepcopy(calendar_cfg)
        calendar_cfg["enabled"] = calendar_events
        label_suffixes.append("calendar" if calendar_events else "no_calendar")
    model_label = MODEL_VERSION + (
        "@" + "+".join(label_suffixes) if label_suffixes else ""
    )
    horizons = horizons or [int(h) for h in model["horizons"]]
    if scenario not in scenarios_cfg.get("scenarios", {}):
        raise ValueError(f"未知情景：{scenario!r}")
    taxonomy = load_taxonomy()
    chash = config_hash(taxonomy, calendar_cfg, model, scenarios_cfg)

    meta = open_meta(settings)
    try:
        from tga_store import (
            BacktestStore,
            DerivedStore,
            rebuild_views,
            resolve_data_dir,
        )

        derived = DerivedStore(resolve_data_dir(settings))
        balance = derived.read("tga_daily_balance")
        cashflow = derived.read("tga_daily_cashflow")
        data_start = balance["date"].min()
        data_end = balance["date"].max()
        if data_start is None or data_end is None:
            raise RuntimeError("tga_daily_balance 为空；请先运行 tga_reconstruct")
        start = start or _add_years(data_start, min_train_years)
        end = min(end or data_end, data_end)
        if start > end:
            raise RuntimeError(f"回测区间为空：start={start} end={end}")

        # 拍卖一次性覆盖回测区间（PIT 由各起点 ctx.through 过滤，§7.2）
        # 回看窗口与实盘 load_inputs 共用 AUCTION_LOOKBACK_YEARS（长券到期本金）。
        service, _ = open_query_service(settings, meta)
        result = service.query(
            dataset_id=_AUCTION_DATASET,
            start=datetime(start.year - AUCTION_LOOKBACK_YEARS, 1, 1),
            end=datetime(end.year, end.month, end.day) + timedelta(days=1),
        )
        if result.truncated:
            raise RuntimeError(f"query truncated for {_AUCTION_DATASET}")
        auctions = result.frame

        origins = business_days(start, end)[::max(stride, 1)]
        if max_origins > 0:
            origins = origins[:max_origins]
        print(
            f"[1/4] 回测区间 {start} → {end}，起点 {len(origins)} 个"
            f"（stride={stride}），horizons={horizons}，config_hash={chash}"
        )
        if dry_run:
            return 0, {"origins": len(origins)}

        store = BacktestStore(resolve_data_dir(settings), meta)
        run_id, is_repeat = store.create_run(
            start.isoformat(), end.isoformat(), stride,
            list((model_label, *BASELINES)), chash,
        )
        print(f"[2/4] backtest run={run_id}（重复运行={is_repeat}）")

        actuals = actual_closings(balance)
        net_by_day = daily_net(cashflow, data_start, data_end)
        net_by_cat = daily_net_by_category(cashflow, data_start, data_end)
        actual_cat_map: dict[date, dict[str, float]] = {}
        for row in net_by_cat.iter_rows(named=True):
            actual_cat_map.setdefault(row["date"], {})[str(row["category"])] = float(
                row["net"]
            )

        eval_rows: list[dict] = []
        attr_rows: list[dict] = []
        try:
            for idx, origin in enumerate(origins, start=1):
                ctx = ForecastContext(
                    origin, horizons, model, calendar_cfg,
                    balance, cashflow, auctions,
                )
                # 与 load_forecast_context 同口径：cutoff 取实际可用余额日
                ctx.through = min(ctx.through, ctx.last_balance_date)
                components = compute_base_components(ctx)
                if components.is_empty():
                    continue
                adjusted = apply_scenario(
                    components, scenarios_cfg["scenarios"][scenario], scenarios_cfg
                )
                comps, forecast = aggregate_forecast(ctx, adjusted)
                boundaries = {
                    int(row["horizon"]): row
                    for row in forecast.filter(
                        pl.col("is_horizon_boundary")
                    ).iter_rows(named=True)
                }
                bdays = boundary_dates(ctx)
                available = {h for h, d in bdays.items() if d in actuals}

                # 主模型 + 基准（仅评估已有实际值的边界日）
                predictions: dict[str, dict[int, tuple[float, float | None, float | None]]] = {
                    model_label: {
                        h: (float(r["predicted_tga"]), float(r["lower"]),
                            float(r["upper"]))
                        for h, r in boundaries.items()
                        if h in available
                    },
                    "naive": {
                        h: (v, None, None)
                        for h, v in baseline_naive(ctx).items() if h in available
                    },
                    "historical_mean": {
                        h: (v, None, None)
                        for h, v in baseline_historical_mean(ctx, net_by_day).items()
                        if h in available
                    },
                    "seasonal_naive": {
                        h: (v, None, None)
                        for h, v in baseline_seasonal_naive(ctx, net_by_day).items()
                        if h in available
                    },
                    "event_driven": {
                        h: (v, None, None)
                        for h, v in baseline_event_driven(ctx, components).items()
                        if h in available
                    },
                }
                eval_targets: dict[int, date] = {}
                for version, preds in predictions.items():
                    for h, (predicted, lower, upper) in preds.items():
                        target = bdays[h]
                        eval_targets[h] = target
                        eval_rows.append(eval_row(
                            ctx, h, target, predicted, actuals[target],
                            version, chash, lower=lower, upper=upper,
                        ))
                if eval_targets:
                    attr_rows.extend(attribution_rows(
                        ctx, eval_targets, comps, actual_cat_map, chash
                    ))
                if idx % 50 == 0 or idx == len(origins):
                    print(f"    进度 {idx}/{len(origins)}，评估行 {len(eval_rows)}")
        except Exception as exc:
            store.finalize_run(
                run_id, "FAILED", error_summary=f"{type(exc).__name__}: {exc}"
            )
            raise

        print("[3/4] 写入评估与归因")
        eval_df = pl.DataFrame(eval_rows, schema=EVAL_SCHEMA)
        attr_df = pl.DataFrame(attr_rows, schema=ATTRIBUTION_SCHEMA)
        n_eval, n_attr = store.write_outputs(run_id, eval_df, attr_df)
        extra_series: dict[str, pl.DataFrame] = {}
        if dm_runs:
            all_eval = store.read("tga_forecast_eval")
            for ref in dm_runs:
                frame = all_eval.filter(
                    (pl.col("run_id") == ref)
                    & pl.col("model_version").str.starts_with("tga-forecast")
                )
                if frame.height:
                    extra_series[str(frame["model_version"][0])] = frame
        dm_df = dm_results(
            eval_df, model_label, run_date=date.today(), extra_series=extra_series
        )
        n_dm = store.write_dm(run_id, dm_df)
        output_hash = hashlib.sha256(
            eval_df.select("target_date", "horizon", "model_version", "predicted")
            .sort("target_date", "horizon", "model_version").write_csv().encode("utf-8")
        ).hexdigest()[:16]
        store.finalize_run(run_id, "SUCCESS", output_hash=output_hash)
        rebuild_views(settings)

        print("[4/4] 指标（§7.3）")
        metrics = eval_metrics(eval_df)
        print_metrics(metrics, model_label)
        print("\n[DM] 主模型 vs 基准（Newey-West HAC；Bonferroni/FDR 多重校正）")
        print_dm(dm_df)
        summary = {
            "run_id": run_id, "origins": len(origins), "eval_rows": n_eval,
            "attribution_rows": n_attr, "dm_rows": n_dm, "metrics": metrics,
            "dm": dm_df,
        }
        return 0, summary
    finally:
        meta.close()


# ── 输出 ────────────────────────────────────────────────────────────


def _fmt(value: object, digits: int = 3) -> str:
    if value is None:
        return "—"
    return f"{float(value) / 1000:,.{digits}f}"


def print_metrics(metrics: pl.DataFrame, main_label: str = MODEL_VERSION) -> None:
    """打印指标表（MAE/RMSE/Bias 单位 Billion USD；MAPE/Coverage 为比例）。"""
    if metrics.is_empty():
        print("  无评估样本")
        return
    print(
        "| 模型 | 周期 | 样本 | MAE(B) | RMSE(B) | MAPE | 方向准确率 | 偏差(B) "
        "| 区间覆盖 | Pinball(B) |"
    )
    print("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    for row in metrics.iter_rows(named=True):
        mape = "—" if row["mape"] is None else f"{row['mape'] * 100:.1f}%"
        cov = "—" if row["coverage"] is None else f"{row['coverage'] * 100:.1f}%"
        print(
            f"| {row['model_version']} | T+{row['horizon']} | {row['n']} "
            f"| {_fmt(row['mae'])} | {_fmt(row['rmse'])} | {mape} "
            f"| {row['direction_accuracy'] * 100:.1f}% | {_fmt(row['bias'])} "
            f"| {cov} | {_fmt(row['pinball'])} |"
        )
    print_metric_warnings(metrics, main_label)


def print_metric_warnings(metrics: pl.DataFrame, main_label: str = MODEL_VERSION) -> None:
    """§7.6 规则 6：主模型未优于简单基准时如实说明。"""
    for horizon in sorted(metrics["horizon"].unique().to_list()):
        sub = metrics.filter(pl.col("horizon") == horizon)
        main = sub.filter(pl.col("model_version") == main_label)
        base = sub.filter(pl.col("model_version").is_in(list(BASELINES)))
        if main.is_empty() or base.is_empty():
            continue
        main_mae = float(main["mae"][0])
        best = base.sort("mae").row(0, named=True)
        if main_mae > float(best["mae"]):
            print(
                f"  ! T+{horizon}：主模型 MAE {_fmt(main_mae)}B 未优于基准 "
                f"{best['model_version']}（{_fmt(best['mae'])}B）——不得宣称有效"
            )


def main() -> int:
    parser = argparse.ArgumentParser(description="TGA Walk-forward 回测")
    parser.add_argument("--start", default=None,
                        help="回测起点（缺省=数据起点+最小训练窗口）")
    parser.add_argument("--end", default=None, help="回测终点（缺省=最新余额日）")
    parser.add_argument("--stride", type=int, default=1, help="起点步长（工作日）")
    parser.add_argument("--horizons", default=None, help="逗号分隔，如 7,14,30")
    parser.add_argument("--scenario", default="base", help="情景（默认 base）")
    parser.add_argument("--min-train-years", type=int, default=5,
                        help="最小训练窗口（年，§7.1）")
    parser.add_argument("--max-origins", type=int, default=0,
                        help="只跑前 N 个起点（0 = 全量，便于冒烟）")
    parser.add_argument("--dry-run", action="store_true", help="只打印计划，不落盘")
    parser.add_argument("--calibration", default=None,
                        choices=["none", "trailing_scale", "seasonal_window"],
                        help="覆盖 issuance.calibration.mode，用于候选方案对比")
    parser.add_argument("--calendar-events", default=None,
                        choices=["on", "off"],
                        help="覆盖 calendar.enabled，用于日历层同口径对照")
    parser.add_argument("--dm-run", action="append", default=None,
                        help="额外参与 DM 检验的历史 backtest_run_id（可重复）")
    args = parser.parse_args()

    settings = Settings.load()
    code, _ = run_backtest(
        settings,
        start=date.fromisoformat(args.start) if args.start else None,
        end=date.fromisoformat(args.end) if args.end else None,
        stride=args.stride,
        horizons=[int(h) for h in args.horizons.split(",")] if args.horizons else None,
        scenario=args.scenario,
        min_train_years=args.min_train_years,
        max_origins=args.max_origins,
        dry_run=args.dry_run,
        calibration=args.calibration,
        calendar_events=(
            None if args.calendar_events is None else args.calendar_events == "on"
        ),
        dm_runs=args.dm_run,
    )
    return code


if __name__ == "__main__":
    sys.exit(main())
