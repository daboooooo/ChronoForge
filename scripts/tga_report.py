#!/usr/bin/env python3
"""TGA 每日预测报告（Phase 4，依据实现方案 §8.1/§8.3/§12）。

读取最新 SUCCESS forecast run（或 --run 指定），生成 Markdown 日报：
    reports/daily/TGA_Forecast_YYYY-MM-DD.md

Phase 4 范围：5 档 horizon（7/14/30/60/90）× 3 情景并列；融资压力
（§6.9，Refunding 现金余额假设 vs 预测 TGA，仅压力估计）；回测表现
章节由 Phase 5 接入。展示单位 Billion USD（原始 USD Million，§2.3）。

用法：
    uv run python scripts/tga_report.py [--run RUN_ID] [--output-dir PATH]
"""

from __future__ import annotations

# QueryService 行数上限放宽（融资压力读取 canonical）；Settings.load() 前设置
import os

os.environ.setdefault("CHRONOFORGE_QUERY_MAX_ROWS", "500000")

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

import polars as pl
from tga_common import SCENARIOS_PATH, load_yaml

_REPO_ROOT = Path(__file__).resolve().parents[1]
_DEFAULT_REPORT_DIR = _REPO_ROOT / "reports" / "daily"
_DISCLAIMER = (
    "> 本报告为模型输出，**不是**美国财政部官方预测；表中的 T+7～T+90 数值是"
    "**情景锚点 / 条件路径**，不是点位预测，也不构成交易建议。"
)
_DATA_SOURCES = (
    "Daily Treasury Statement、Treasury Auctions、Quarterly Refunding "
    "（Fiscal Data API / treasury.gov）+ config/tga 配置"
)

_CATEGORY_LABELS = {
    "TAX": "税收收入",
    "DEBT_ISSUANCE": "国债发行",
    "DEBT_REDEMPTION": "国债偿还",
    "BENEFIT": "福利支出",
    "INTEREST": "国债利息",
    "REFUND": "退税",
    "OTHER": "其他",
    "UNCLASSIFIED": "未识别",
}
_SCENARIO_LABELS = {
    "base": "基准",
    "liquidity_easing": "宽松",
    "liquidity_tightening": "收紧",
}


def _b(value: float) -> str:
    """USD Million → Billion 字符串（2 位小数）。"""
    return f"{value / 1000:,.2f}"


def _fmt_date(d) -> str:
    return d.isoformat() if hasattr(d, "isoformat") else str(d)


def load_run(run_id: str | None, settings) -> tuple[dict, pl.DataFrame, pl.DataFrame]:
    """取 run 元数据 + 组件 + 预测表（经 tga.duckdb 视图，union_by_name）。

    run_id 缺省取最新 SUCCESS；无 → RuntimeError。
    """
    from tga_store import ForecastStore, open_tga_view_connection, resolve_data_dir

    from chronoforge.cli._wiring import open_meta

    meta = open_meta(settings)
    try:
        fs = ForecastStore(resolve_data_dir(settings), meta)
        run = fs.get_run(run_id) if run_id else fs.latest_success_run()
        if run is None:
            raise RuntimeError("没有 SUCCESS 的 forecast run；请先运行 tga_forecast.py")
    finally:
        meta.close()

    con = open_tga_view_connection(settings)
    try:
        comps = con.execute(
            "SELECT * FROM tga_forecast_components WHERE run_id = ?",
            [run["forecast_run_id"]],
        ).pl()
        fcst = con.execute(
            "SELECT * FROM tga_forecast WHERE run_id = ?",
            [run["forecast_run_id"]],
        ).pl()
    finally:
        con.close()
    return run, comps, fcst


def _current_status(balance: pl.DataFrame, through) -> list[str]:
    bal = balance.filter(pl.col("date") <= through).sort("date")
    last = bal.row(-1, named=True)
    prev = bal.row(-2, named=True) if bal.height >= 2 else None

    def change(days: int) -> str:
        idx = max(bal.height - 1 - days, 0)
        old = bal.row(idx, named=True)
        return _b(float(last["closing"]) - float(old["closing"]))

    lines = [
        "| 指标 | 数值 |",
        "|---|---:|",
        f"| 最新 TGA | {_b(float(last['closing']))} B |",
        f"| 最新日期 | {_fmt_date(last['date'])} |",
    ]
    if prev is not None:
        lines.append(
            f"| 日变化 | {_b(float(last['closing']) - float(prev['closing']))} B |"
        )
    lines.append(f"| 过去7天变化 | {change(7)} B |")
    lines.append(f"| 过去30天变化 | {change(30)} B |")
    return lines


def _horizon_table(fcst: pl.DataFrame) -> list[str]:
    """5 档 horizon × 情景并列表（§8.1 章节二；Phase 4 扩展）。"""
    df = fcst if "scenario" in fcst.columns else fcst.with_columns(
        pl.lit("base").alias("scenario")
    )
    scenarios = sorted(df["scenario"].unique().to_list())
    # 以 base 的边界日期为行锚（各情景同 origin/horizon 边界一致）
    base = df.filter(pl.col("scenario") == scenarios[0])
    boundary = base.filter(pl.col("is_horizon_boundary")).sort("horizon")
    lines = [
        "| 周期 | 日期 | " + " | ".join(
            _SCENARIO_LABELS.get(s, s) for s in scenarios
        ) + " |",
        "|---|---|" + "---:|" * len(scenarios),
    ]
    for row in boundary.iter_rows(named=True):
        cells = []
        for s in scenarios:
            r = df.filter(
                (pl.col("scenario") == s)
                & (pl.col("is_horizon_boundary"))
                & (pl.col("horizon") == row["horizon"])
            )
            cells.append(_b(r["predicted_tga"][0]) if r.height else "—")
        lines.append(
            f"| T+{row['horizon']} | {_fmt_date(row['target_date'])} | "
            + " | ".join(cells) + " |"
        )
    return lines


def _daily_flows_table(comps: pl.DataFrame, fcst: pl.DataFrame) -> list[str]:
    base_comps = comps.filter(
        pl.col("scenario") == "base"
    ) if "scenario" in comps.columns else comps
    base_fcst = fcst.filter(
        pl.col("scenario") == "base"
    ) if "scenario" in fcst.columns else fcst
    flows = (
        base_comps.group_by("target_date")
        .agg(
            pl.col("predicted").filter(pl.col("predicted") > 0).sum().alias("income"),
            pl.col("predicted").filter(pl.col("predicted") < 0).sum().alias("outlay"),
            pl.col("event_key").drop_nulls().alias("events"),
        )
        .sort("target_date")
    )
    events_by_day = {
        row["target_date"]: sorted({str(e).split(":")[0] for e in row["events"]})
        for row in flows.iter_rows(named=True)
    }
    net_by_day = {row["target_date"]: row["daily_net"]
                  for row in base_fcst.iter_rows(named=True)}
    lines = [
        "| 日期 | 流入 | 流出 | 净现金流 | 主要事件 |",
        "|---|---:|---:|---:|---|",
    ]
    for row in flows.iter_rows(named=True):
        d = row["target_date"]
        lines.append(
            f"| {_fmt_date(d)} | {_b(row['income'])} | {_b(row['outlay'])} "
            f"| {_b(net_by_day.get(d, 0.0))} | {', '.join(events_by_day[d]) or '—'} |"
        )
    return lines


def _composition_table(comps: pl.DataFrame) -> list[str]:
    agg = comps.group_by("category").agg(
        pl.col("predicted").sum().alias("net"),
        pl.col("predicted").filter(pl.col("predicted") > 0).sum().alias("inflow"),
        pl.col("predicted").filter(pl.col("predicted") < 0).sum().alias("outflow"),
    ).sort("net")
    lines = [
        "| 构成 | 流入 | 流出 | 净额 |",
        "|---|---:|---:|---:|",
    ]
    for row in agg.iter_rows(named=True):
        label = _CATEGORY_LABELS.get(row["category"], row["category"])
        lines.append(
            f"| {label} | {_b(row['inflow'])} | {_b(row['outflow'])} "
            f"| {_b(row['net'])} |"
        )
    return lines


def _events_table(comps: pl.DataFrame, top: int = 15) -> list[str]:
    events = comps.filter(pl.col("event_key").is_not_null()).sort(
        pl.col("predicted").abs(), descending=True
    ).head(top)
    lines = [
        "| 日期 | 事件 | 类别 | 金额 | 来源 |",
        "|---|---|---|---:|---|",
    ]
    for row in events.iter_rows(named=True):
        lines.append(
            f"| {_fmt_date(row['target_date'])} | `{row['event_key']}` "
            f"| {_CATEGORY_LABELS.get(row['category'], row['category'])} "
            f"| {_b(row['predicted'])} | {row['source_type']} |"
        )
    return lines


def _confidence_table(comps: pl.DataFrame) -> list[str]:
    total = comps["predicted"].abs().sum()
    source_labels = {
        "KNOWN_FINAL": "已确定（已知结果）",
        "KNOWN_PROVISIONAL": "已公告（暂定）",
        "SEASONAL": "季节性预测",
        "STATISTICAL": "统计预测",
        "UNCLASSIFIED": "未识别",
    }
    agg = comps.group_by("source_type").agg(
        pl.col("predicted").abs().sum().alias("abs_amount")
    ).sort("abs_amount", descending=True)
    lines = [
        "| 来源类型 | 金额占比 |",
        "|---|---:|",
    ]
    for row in agg.iter_rows(named=True):
        share = float(row["abs_amount"]) / float(total) * 100 if total else 0.0
        lines.append(
            f"| {source_labels.get(row['source_type'], row['source_type'])} "
            f"| {share:.1f}% |"
        )
    return lines


def _financing_section(pressure: dict | None) -> list[str]:
    """融资压力小节（§6.9；只作压力估计，不冒充发行计划）。"""
    if not pressure:
        return [
            "暂缺：无 release_time ≤ 数据截止日的 Refunding 现金余额假设"
            "（公告随季度增量采集累积）。"
        ]
    return [
        f"- 现金余额假设（财政部公告，期终点 {_fmt_date(pressure['period_end'])}，"
        f"发布 {_fmt_date(pressure['publication_time'])}）："
        f"**{_b(pressure['cash_target'])} B**。",
        f"- 情景锚点（同终点，基准情景）：**{_b(pressure['predicted_tga'])} B**。",
        f"- 融资压力 = 假设 − 情景锚点 = **{_b(pressure['gap'])} B**"
        f"（正 = 情景路径低于财政部假设，存在融资压力；"
        f"负 = 情景路径偏高）。",
        "- 注意：融资压力**不等于**财政部将按同等金额增发国债"
        "（§6.9；财政部可调整 bill/coupon 结构与现金管理节奏）。",
    ]


def _load_scenarios() -> dict:
    """读取 scenarios.yaml 的情景说明（供第四节；失败时退化为空）。"""
    try:
        return (load_yaml(str(SCENARIOS_PATH)) or {}).get("scenarios", {}) or {}
    except Exception:  # 配置文件缺失不应阻断日报
        return {}


def _naive_dm_text(dm_rows: pl.DataFrame) -> str:
    """把 DM 检验结论压缩成能力边界表述（相对 Naive）。"""
    if dm_rows.is_empty() or "benchmark" not in dm_rows.columns:
        return "DM 检验产物暂缺；不得据此推断模型优于 Naive。"
    rows = dm_rows.filter(pl.col("benchmark") == "naive").sort("horizon")
    if rows.is_empty():
        return "DM 检验产物暂缺（无 Naive 对照）；不得据此推断模型优于 Naive。"
    insig, worse, better = [], [], []
    for row in rows.iter_rows(named=True):
        tag = f"T+{row['horizon']}"
        if not row["significant_bonf"]:
            insig.append(tag)
        elif (row["mean_loss_diff"] or 0.0) > 0:
            worse.append(tag)
        else:
            better.append(tag)
    parts = []
    if insig:
        parts.append("、".join(insig) + " 差距不显著")
    if worse:
        parts.append("、".join(worse) + " 显著更差（Bonferroni 5%）")
    if better:
        parts.append("、".join(better) + " 显著更优（Bonferroni 5%）")
    return "相对 Naive：" + "；".join(parts) + "；不可用于交易决策。"


def _positioning_section(eval_rows: pl.DataFrame, dm_rows: pl.DataFrame) -> list[str]:
    """定位与能力边界（目标 / 非目标 / 样本外证据）。"""
    lines = [
        "**定位**：T+7～T+90 是**情景锚点 / 条件路径**，不是点位预测，"
        "也不是可用于交易的置信区间。",
        "",
        "**目标**",
        "- 解释余额变动由哪些事件驱动：发行、赎回、税收、福利、利息、其他。",
        "- 说明在每个情景假设下，事件节奏把余额带向哪条条件路径。",
        "- 给出情景切换的触发条件（第四节），让读者知道什么信号会改变读数。",
        "",
        "**非目标**",
        "- 不给出到期日的 TGA 点位；表中数值只是情景锚点。",
        "- 不输出情景概率，也不自动判定当前所处情景（默认按基准情景呈现）。",
        "- 不用于交易决策、流动性择时，不可替代财政部官方预测。",
        "",
        "**能力边界（样本外证据，详见第九节）**",
    ]
    if eval_rows.is_empty():
        lines.append("- 尚未完成样本外回测；在回测通过前只能作解释性参考。")
        return lines
    from tga_backtest import MODEL_VERSION, eval_metrics

    metrics = eval_metrics(eval_rows)
    for horizon in sorted(metrics["horizon"].unique().to_list()):
        sub = metrics.filter(pl.col("horizon") == horizon)
        main = sub.filter(pl.col("model_version") == MODEL_VERSION)
        naive = sub.filter(pl.col("model_version") == "naive")
        if main.height and naive.height:
            lines.append(
                f"- T+{horizon} MAE：主模型 {_b(float(main['mae'][0]))} B vs "
                f"Naive {_b(float(naive['mae'][0]))} B。"
            )
    lines.append(f"- {_naive_dm_text(dm_rows)}")
    return lines


def _scenario_rules_section() -> list[str]:
    """情景定义与切换条件（第四节的表格）。"""
    scenarios = _load_scenarios()
    lines = [
        "当前读数：默认按**基准**情景呈现；模型不输出情景概率，"
        "换挡需按下列信号人工核对。情景调整只作用于预测类现金流，"
        "已知事件（KNOWN_*）绝不调整（D-TGA-11）。",
        "",
        "| 情景 | 触发条件（任一满足即换挡） | 关键驱动事件 | 适用解读 |",
        "|---|---|---|---|",
    ]
    for name in ("base", "liquidity_easing", "liquidity_tightening"):
        cfg = scenarios.get(name, {})
        label = cfg.get("label") or _SCENARIO_LABELS.get(name, name)
        triggers = "；".join(cfg.get("triggers", []) or []) or "—"
        drivers = "、".join(
            _CATEGORY_LABELS.get(c, c) for c in (cfg.get("key_drivers", []) or [])
        ) or "—"
        interp = cfg.get("interpretation") or cfg.get("description") or "—"
        lines.append(f"| {label} | {triggers} | {drivers} | {interp} |")
    lines.extend([
        "",
        "- 换挡不等于改数字：三档情景始终并列展示，切换只是改变你优先看哪一列。",
        "- 触发信号来自税收现金流、拍卖公告净融资与 Refunding 现金余额假设，"
        "须按数据截止日之前的信息核对（PIT）。",
    ])
    return lines


def load_backtest(
    settings,
) -> tuple[dict | None, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """取最新 SUCCESS 回测 run 的评估/归因/DM（§7；无 → (None, 空, 空, 空)）。"""
    from tga_store import BacktestStore, resolve_data_dir

    from chronoforge.cli._wiring import open_meta

    meta = open_meta(settings)
    try:
        store = BacktestStore(resolve_data_dir(settings), meta)
        run = store.latest_success_run()
        if run is None:
            return None, pl.DataFrame(), pl.DataFrame(), pl.DataFrame()
    finally:
        meta.close()
    run_id = str(run["backtest_run_id"])
    eval_rows = store.read("tga_forecast_eval").filter(pl.col("run_id") == run_id)
    attribution = store.read("tga_attribution").filter(pl.col("run_id") == run_id)
    try:
        dm_rows = store.read("tga_dm_tests").filter(pl.col("run_id") == run_id)
    except Exception:  # 旧数据无 DM 产物
        dm_rows = pl.DataFrame()
    return run, eval_rows, attribution, dm_rows


def _backtest_section(
    run: dict | None,
    eval_rows: pl.DataFrame,
    attribution: pl.DataFrame,
    dm_rows: pl.DataFrame,
) -> list[str]:
    """§8.1 模型回测表现：MAE/RMSE/方向准确率/区间覆盖 + 基准对比（§7.3/§7.4）。"""
    if run is None or eval_rows.is_empty():
        return [
            "尚未运行 Walk-forward 回测（`uv run python scripts/tga_backtest.py`）。",
            "当前区间为配置的简化带宽，未经样本外校准，"
            "**不应**作为置信区间使用。",
        ]
    from tga_backtest import MODEL_VERSION, eval_metrics

    metrics = eval_metrics(eval_rows)
    lines = [
        f"- 回测区间：{run['start_date']} → {run['end_date']}"
        f"（stride={run['stride']}，config_hash=`{run['config_hash']}`）。",
        "",
        "| 周期 | 主模型 MAE(B) | Naive | Historical Mean | Seasonal Naive "
        "| Event-driven | 主模型 方向/覆盖 |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for horizon in sorted(metrics["horizon"].unique().to_list()):
        sub = metrics.filter(pl.col("horizon") == horizon)
        main = sub.filter(pl.col("model_version") == MODEL_VERSION)
        cells = []
        for version in ("naive", "historical_mean", "seasonal_naive", "event_driven"):
            row = sub.filter(pl.col("model_version") == version)
            cells.append(_b(float(row["mae"][0])) if row.height else "—")
        if main.height:
            m = main.row(0, named=True)
            cov = "—" if m["coverage"] is None else f"{m['coverage'] * 100:.0f}%"
            main_cell = _b(float(m["mae"]))
            tail = f"{m['direction_accuracy'] * 100:.0f}% / {cov}"
        else:
            main_cell, tail = "—", "—"
        lines.append(
            f"| T+{horizon} | {main_cell} | " + " | ".join(cells) + f" | {tail} |"
        )
    lines.extend(["", "误差归因（主模型，按类别平均绝对误差）：", ""])
    if attribution.is_empty():
        lines.append("暂缺归因数据。")
    else:
        agg = (
            attribution.group_by("error_type")
            .agg(pl.col("error_amount").abs().mean().alias("mean_abs"))
            .sort("mean_abs", descending=True)
        )
        lines.append("| 误差类型 | 平均绝对误差 |")
        lines.append("|---|---:|")
        for row in agg.head(8).iter_rows(named=True):
            lines.append(f"| {row['error_type']} | {_b(float(row['mean_abs']))} B |")
    # §7.6 规则 6：主模型未优于简单基准时如实说明
    lines.extend(["", "样本外对比结论："])
    for horizon in sorted(metrics["horizon"].unique().to_list()):
        sub = metrics.filter(pl.col("horizon") == horizon)
        main = sub.filter(pl.col("model_version") == MODEL_VERSION)
        base = sub.filter(pl.col("model_version").is_in(
            ["naive", "historical_mean", "seasonal_naive", "event_driven"]
        ))
        if main.is_empty() or base.is_empty():
            continue
        best = base.sort("mae").row(0, named=True)
        if float(main["mae"][0]) > float(best["mae"]):
            lines.append(
                f"- T+{horizon}：主模型未优于基准 {best['model_version']}"
                f"（MAE {_b(float(best['mae']))} B vs "
                f"{_b(float(main['mae'][0]))} B），不得宣称有效。"
            )
        else:
            lines.append(
                f"- T+{horizon}：主模型优于全部简单基准"
                f"（MAE {_b(float(main['mae'][0]))} B）。"
            )
    lines.append(
        "- 区间覆盖与 Pinball 为组件带宽近似（P0），尚非校准后的置信区间。"
    )
    lines.extend(["", "Diebold-Mariano 检验（HAC/Newey-West，损失=绝对误差）：", ""])
    if dm_rows.is_empty():
        lines.append("暂缺 DM 产物（重跑 `tga_backtest.py` 生成）。")
        return lines
    lines.extend([
        "| 周期 | 基准 | n | 平均损失差(B) | DM 统计量 | p | p(Bonf) "
        "| 显著(Bonf 5%) |",
        "|---|---|---:|---:|---:|---:|---:|---|",
    ])
    for row in dm_rows.sort("horizon", "benchmark").iter_rows(named=True):
        mean = "—" if row["mean_loss_diff"] is None else f"{row['mean_loss_diff'] / 1000:,.3f}"
        stat = "—" if row["dm_stat"] is None else f"{row['dm_stat']:,.2f}"
        p = "—" if row["p_value"] is None else f"{row['p_value']:.4f}"
        pb = "—" if row["p_bonferroni"] is None else f"{row['p_bonferroni']:.4f}"
        lines.append(
            f"| T+{row['horizon']} | {row['benchmark']} | {row['n']} | {mean} "
            f"| {stat} | {p} | {pb} | "
            f"{'YES' if row['significant_bonf'] else 'NO'} |"
        )
    lines.extend([
        "",
        "- 平均损失差 < 0 表示主模型 MAE 更低；p 值为双侧正态近似，"
        "多重比较同时给出 Bonferroni 与 BH-FDR 校正。",
        "- 仅当 Bonferroni 校正后仍 < 0.05 才视作**统计显著改善**；"
        "否则只是点估计改善，不得据此宣称模型优于基准。",
    ])
    return lines


def _conclusions(
    comps: pl.DataFrame, fcst: pl.DataFrame, dm_rows: pl.DataFrame
) -> list[str]:
    """使用限制与结论：只做事件归因与情景判断，不输出点位预测结论。"""
    base_fcst = fcst.filter(
        pl.col("scenario") == "base"
    ) if "scenario" in fcst.columns else fcst
    boundary = base_fcst.filter(pl.col("is_horizon_boundary")).sort("horizon")
    if boundary.is_empty():
        return ["（无预测行）"]
    comp = comps.group_by("category").agg(
        pl.col("predicted").sum().alias("net")
    ).sort(pl.col("net").abs(), descending=True)
    drivers = "、".join(
        f"{_CATEGORY_LABELS.get(r['category'], r['category'])}"
        f"（净额 {_b(r['net'])} B）"
        for r in comp.head(3).iter_rows(named=True)
    )
    total_abs = comps["predicted"].abs().sum()
    known_abs = comps.filter(
        pl.col("source_type").str.starts_with("KNOWN_")
    )["predicted"].abs().sum()
    unclassified = comps.filter(
        pl.col("category") == "UNCLASSIFIED"
    )["predicted"].abs().sum()
    share = float(known_abs) / float(total_abs) if total_abs else 0.0
    return [
        "1. 本报告的 T+7～T+90 数值是**情景锚点 / 条件路径**，不是点位预测；"
        "不构成交易建议，也不能替代财政部官方预测。",
        f"2. 驱动结构（基准情景、窗口合计的组件归因）：{drivers}。"
        "这是对余额变动来源的解释，不是到期点位结论。",
        "3. 当前默认按基准情景读数；换挡信号见第四节（税收偏离季节基线、"
        "净融资偏离公告节奏、Refunding 现金余额假设偏离）。",
        f"4. 能力边界：{_naive_dm_text(dm_rows)}",
        f"5. 证据强度：已知事件占毛现金流 {share:.1%}，"
        f"其余为季节性/统计推演；未识别现金流 {_b(unclassified)} B。",
    ]


def render_report(
    run: dict, comps: pl.DataFrame, fcst: pl.DataFrame,
    balance: pl.DataFrame, pressure: dict | None = None,
    backtest_run: dict | None = None,
    backtest_eval: pl.DataFrame | None = None,
    backtest_attribution: pl.DataFrame | None = None,
    backtest_dm: pl.DataFrame | None = None,
) -> str:
    """渲染 Markdown 日报（§8.1 章节一～十；数值按情景锚点口径）。"""
    scenario = json.loads(run["scenarios_json"])
    through = date.fromisoformat(str(run["data_cutoff_date"]))
    generated = datetime.now().strftime("%Y-%m-%d %H:%M")
    base_fcst = fcst.filter(
        pl.col("scenario") == "base"
    ) if "scenario" in fcst.columns else fcst
    base_fcst = base_fcst.sort("target_date")
    last_tga = float(
        base_fcst["predicted_tga"][0] - base_fcst["daily_net"][0]
    )
    base_comps = comps.filter(
        pl.col("scenario") == "base"
    ) if "scenario" in comps.columns else comps

    lines = [
        f"# TGA Forecast Report — {run['origin_date']}",
        "",
        _DISCLAIMER,
        "",
        "| 元信息 | 值 |",
        "|---|---|",
        f"| 数据截止日 | {run['data_cutoff_date']} |",
        f"| 预测生成时间 | {generated} |",
        f"| 模型版本 | {run['model_version']} |",
        f"| config_hash | `{run['config_hash']}` |",
        f"| 预测起点 TGA | {_b(last_tga)} B "
        f"（{run['data_cutoff_date']} 收盘） |",
        f"| 情景 | {'、'.join(_SCENARIO_LABELS.get(s, s) for s in scenario)} |",
        "| 输出类型 | 情景锚点 / 条件路径（**非点位预测**） |",
        "| 使用限制 | 不用于交易决策；当前不优于 Naive（见第九节） |",
        "| 单位 | 展示 Billion USD；原始 USD Million |",
        f"| 数据来源 | {_DATA_SOURCES} |",
        "",
        "## 一、当前 TGA 状况",
        "",
        *_current_status(balance, through),
        "",
        "## 二、模型定位与能力边界",
        "",
        *_positioning_section(
            backtest_eval if backtest_eval is not None else pl.DataFrame(),
            backtest_dm if backtest_dm is not None else pl.DataFrame(),
        ),
        "",
        "## 三、情景路径（T+7～T+90 条件锚点，非点位预测）",
        "",
        "每一列是一个**条件路径**：在对应情景假设下，事件节奏会把余额带向该位置。"
        "它不是点位预测，也不是置信区间；请结合第四节的切换条件阅读。",
        "",
        *_horizon_table(fcst),
        "",
        "## 四、情景定义与切换条件",
        "",
        *_scenario_rules_section(),
        "",
        "## 五、逐日现金流路径（基准情景，全预测窗口）",
        "",
        "流入/流出/净额为模型组件按日求和（Known + Predicted + Unclassified）；"
        "「主要事件」为事件键前缀（ISSUE/REDEEM/COUPON/TAX/OUTLAY/PRED）。"
        "该表用于解释路径由哪些事件推动，**不是**每日点位预测。",
        "",
        *_daily_flows_table(comps, fcst),
        "",
        "## 六、驱动因素归因（基准情景，窗口合计）",
        "",
        "### 6.1 现金流构成",
        "",
        *_composition_table(base_comps),
        "",
        "### 6.2 关键事件（按金额 Top 15）",
        "",
        *_events_table(base_comps),
        "",
        "## 七、来源构成与证据强度",
        "",
        "已知事件占比越高，路径越接近已公布事实；季节性/统计部分代表模型推演。",
        "",
        *_confidence_table(base_comps),
        "",
        "## 八、融资压力（财政部现金余额假设 vs 情景锚点）",
        "",
        *_financing_section(pressure),
        "",
        "## 九、回测与 DM 检验（能力边界证据）",
        "",
        *_backtest_section(
            backtest_run,
            backtest_eval if backtest_eval is not None else pl.DataFrame(),
            backtest_attribution if backtest_attribution is not None else pl.DataFrame(),
            backtest_dm if backtest_dm is not None else pl.DataFrame(),
        ),
        "",
        "## 十、使用限制与结论",
        "",
        *_conclusions(
            base_comps, fcst,
            backtest_dm if backtest_dm is not None else pl.DataFrame(),
        ),
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="TGA 每日预测报告")
    parser.add_argument("--run", default=None, help="forecast_run_id（缺省最新 SUCCESS）")
    parser.add_argument("--output-dir", default=None, help="输出目录（缺省 reports/daily/）")
    args = parser.parse_args()

    from tga_forecast import financing_pressure
    from tga_store import DerivedStore, resolve_data_dir

    from chronoforge.cli._wiring import open_meta, open_query_service
    from chronoforge.config.settings import Settings

    settings = Settings.load()
    run, comps, fcst = load_run(args.run, settings)
    backtest_run, backtest_eval, backtest_attr, backtest_dm = load_backtest(settings)

    meta = open_meta(settings)
    try:
        balance = DerivedStore(resolve_data_dir(settings)).read("tga_daily_balance")
        pressure = None
        through = date.fromisoformat(str(run["data_cutoff_date"]))
        base_fcst = (
            fcst.filter(pl.col("scenario") == "base")
            if "scenario" in fcst.columns else fcst
        )
        boundary = base_fcst.filter(pl.col("is_horizon_boundary")).sort("horizon")
        if not boundary.is_empty() and through:
            service, _ = open_query_service(settings, meta)
            pressure = financing_pressure(
                service, through,
                boundary.row(-1, named=True)["target_date"], base_fcst,
            )
    finally:
        meta.close()

    report = render_report(
        run, comps, fcst, balance, pressure,
        backtest_run, backtest_eval, backtest_attr, backtest_dm,
    )
    out_dir = Path(args.output_dir) if args.output_dir else _DEFAULT_REPORT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"TGA_Forecast_{run['origin_date']}.md"
    out_path.write_text(report, encoding="utf-8")
    print(f"报告已生成：{out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
