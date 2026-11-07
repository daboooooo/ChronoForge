#!/usr/bin/env python3
"""导出 USD 流动性可视化站点所需数据（只读）。

数据口径参考 scripts/plot_btc_etf_flow.py：
- OHLCV 直读 canonical parquet 分区；
- NUMBER / FLOW 经只读 QueryService，按 revision_time 去重保留最新版本；
- TGA 日度、TGA 预测读取既有 derived 分区（只读）。

输出：
    visualization/data.js    window.CF_DATA = {...}（file:// 下可直接加载）
    visualization/data.json  同一份数据，便于调试

用法：
    uv run python visualization/export_data.py
"""

from __future__ import annotations

import contextlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

from chronoforge.cli._wiring import open_meta, open_query_service
from chronoforge.config.settings import Settings

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SITE_DIR = Path(__file__).resolve().parent
DATA_JS = SITE_DIR / "data.js"
DATA_JSON = SITE_DIR / "data.json"

WEEKLY_START = datetime(2018, 1, 1)
DAILY_START = datetime(2019, 1, 1)
MACRO_START = datetime(2020, 12, 1)
BTC_START = datetime(2021, 1, 1)
STABLE_START = datetime(2022, 1, 1)

# dataset_id / entity（canonical）
FRED = {
    "WRESBAL": "fred_series_WRESBAL",
    "WRBWFRBL": "fred_series_WRBWFRBL",
    "WTREGEN": "fred_series_WTREGEN",
    "RRPONTSYD": "fred_series_RRPONTSYD",
    "RRP_AWARD": "fred_series_RRPONTSYAWARD",
    "EFFR": "fred_series_EFFR",
    "IORB": "fred_series_IORB",
    "SOFR": "fred_series_SOFR",
    "DGS10": "fred_series_DGS10",
    "DGS2": "fred_series_DGS2",
    "DGS20": "fred_series_DGS20",
    "DGS30": "fred_series_DGS30",
    "DTB4WK": "fred_series_DTB4WK",
    "DTB6": "fred_series_DTB6",
    "DTB1YR": "fred_series_DTB1YR",
    "DFII10": "fred_series_DFII10",
    "T10YIE": "fred_series_T10YIE",
    "VIXCLS": "fred_series_VIXCLS",
    "WALCL": "fred_series_WALCL",
    "WCURCIR": "fred_series_WCURCIR",
}

# canonical OHLCV 目录名（与 registry entity_id 不同）
OHLCV = {
    "BTC": "CCXT-BINANCE:BTCUSDT:USDT:SPOT",
    "DXY": "YAHOO:DX-Y.NYB:SPOT",
    "SPX": "YAHOO:^GSPC:SPOT",
    "NDX": "YAHOO:^NDX:SPOT",
    "GOLD": "YAHOO:GC=F:SPOT",
    "TLT": "YAHOO:TLT:SPOT",
    "RUT": "YAHOO:^RUT:SPOT",
    "IWM": "YAHOO:IWM:SPOT",
}

# NY Fed SRF 用量（canonical NUMBER，单位为 USD 原值）
SRF_DATASET = "nyfed_repo_srf_usage"
SRF_ENTITY = "srf_usage"
SRF_START = datetime(2021, 7, 29)

ETF_DATASET = "sosovalue_etf_us_btc_summary_total_net_inflow"
ETF_ENTITY = "etf_us_btc_summary_total_net_inflow"


# ── 读取 ─────────────────────────────────────────────────────────────
def load_number(
    service: object, dataset_id: str, entity: str, start: datetime | None
) -> pd.Series:
    """NUMBER 经 QueryService；同一观测时间保留 revision_time 最新。"""
    result = service.query(  # type: ignore[attr-defined]
        dataset_id,
        columns=["observation_time", "value", "revision_time"],
        start=start,
        filters={"entity": entity},
    )
    df = result.frame.to_pandas()
    if df.empty:
        return pd.Series(dtype=float)
    df = df.sort_values(["revision_time", "observation_time"], na_position="first")
    df = df.drop_duplicates("observation_time", keep="last")
    s = df.set_index(pd.to_datetime(df["observation_time"]))["value"]
    s.index = pd.DatetimeIndex(s.index).normalize()
    return s.astype(float).dropna().sort_index()


def load_flow(service: object, dataset_id: str, entity: str, start: datetime | None) -> pd.Series:
    """FLOW 经 QueryService；按 revision_time 去重。"""
    result = service.query(  # type: ignore[attr-defined]
        dataset_id,
        columns=["observation_time", "value", "revision_time"],
        start=start,
        filters={"entity": entity},
    )
    df = result.frame.to_pandas()
    if df.empty:
        return pd.Series(dtype=float)
    df = df.sort_values(["revision_time", "observation_time"], na_position="first")
    df = df.drop_duplicates("observation_time", keep="last")
    s = df.set_index(pd.to_datetime(df["observation_time"]))["value"]
    s.index = pd.DatetimeIndex(s.index).normalize()
    return s.astype(float).dropna().sort_index()


def load_ohlcv(data_dir: Path, entity: str, start: datetime | None) -> pd.Series:
    """直读 OHLCV canonical 分区（只读）。"""
    pattern = str(data_dir / "canonical" / "OHLCV" / f"entity={entity}" / "**" / "*.parquet")
    con = duckdb.connect()
    try:
        df = con.execute(
            "SELECT event_time, close, source_timestamp "
            f"FROM read_parquet('{pattern}', hive_partitioning=1) "
            "WHERE close IS NOT NULL AND event_time >= ? "
            "ORDER BY event_time, source_timestamp",
            [start],
        ).pl().to_pandas()
    finally:
        con.close()
    if df.empty:
        return pd.Series(dtype=float)
    df = df.drop_duplicates("event_time", keep="last")
    s = df.set_index(pd.to_datetime(df["event_time"]))["close"]
    s.index = pd.DatetimeIndex(s.index).normalize()
    return s.astype(float).dropna().sort_index()


def load_tga_daily(data_dir: Path, start: datetime) -> pd.Series:
    """读取既有 derived TGA 日度余额（closing，百万美元）。"""
    pattern = str(data_dir / "derived" / "tga_daily_balance" / "**" / "*.parquet")
    con = duckdb.connect()
    try:
        df = con.execute(
            "SELECT date, closing "
            f"FROM read_parquet('{pattern}', hive_partitioning=1) "
            "WHERE closing IS NOT NULL AND date >= ? ORDER BY date",
            [start.date()],
        ).pl().to_pandas()
    finally:
        con.close()
    s = df.set_index(pd.to_datetime(df["date"]))["closing"].astype(float)
    return s.dropna().sort_index()


def load_tga_forecast(data_dir: Path) -> dict[str, Any]:
    """读取最新一次 TGA 预测运行的三种情景。"""
    pattern = str(data_dir / "derived" / "tga_forecast" / "**" / "*.parquet")
    con = duckdb.connect()
    try:
        df = con.execute(
            "SELECT run_date, scenario, target_date, predicted_tga, lower, upper "
            f"FROM read_parquet('{pattern}', hive_partitioning=1) "
            "ORDER BY run_date, scenario, target_date"
        ).pl().to_pandas()
    finally:
        con.close()
    if df.empty:
        return {"run_date": None, "scenarios": {}}
    latest = df["run_date"].max()
    df = df[df["run_date"] == latest]
    out: dict[str, Any] = {"run_date": str(latest), "scenarios": {}}
    for scenario, group in df.groupby("scenario"):
        out["scenarios"][scenario] = {
            "t": [str(pd.to_datetime(d).date()) for d in group["target_date"]],
            "v": [round(float(x), 2) for x in group["predicted_tga"]],
            "lo": [round(float(x), 2) for x in group["lower"]],
            "hi": [round(float(x), 2) for x in group["upper"]],
        }
    return out


def load_reserve_forecast(data_dir: Path) -> dict[str, Any]:
    """读取最新一次准备金结构模型运行（reserve-structural-0.1.0）。

    输出：预测点（h=1/2/4）、结构分解（含残差桶标注）、回测误差统计与
    h=1 的逐期对照（用于“实际值事后对照”折线）。全部来自 derived 分区。
    """
    fc_pattern = str(data_dir / "derived" / "reserve_forecast" / "**" / "*.parquet")
    comp_pattern = str(
        data_dir / "derived" / "reserve_forecast_components" / "**" / "*.parquet"
    )
    eval_pattern = str(
        data_dir / "derived" / "reserve_forecast_eval" / "**" / "*.parquet"
    )
    con = duckdb.connect()
    try:
        fc = con.execute(
            "SELECT run_date, scenario, origin_date, target_date, horizon, "
            "predicted_reserve, lower, upper, origin_reserve, model_version "
            f"FROM read_parquet('{fc_pattern}', hive_partitioning=1) "
            "ORDER BY run_date, horizon"
        ).pl().to_pandas()
        comp = con.execute(
            "SELECT run_date, horizon, component, level, delta_from_origin, method, "
            "is_residual_bucket "
            f"FROM read_parquet('{comp_pattern}', hive_partitioning=1) "
            "ORDER BY run_date, horizon, component"
        ).pl().to_pandas()
        ev = con.execute(
            "SELECT origin_date, target_date, horizon, predicted, predicted_v010, "
            "actual, error, v010_error, naive, seasonal_naive, snaive_fallback, "
            "naive_error, snaive_error, dm_v010_dm_stat, dm_v010_p_value, "
            "dm_v010_p_bonferroni, dm_naive_dm_stat, dm_naive_p_value, "
            "dm_naive_p_bonferroni "
            f"FROM read_parquet('{eval_pattern}', hive_partitioning=1) "
            "ORDER BY target_date, horizon"
        ).pl().to_pandas()
    finally:
        con.close()

    if fc.empty:
        return {"run_date": None, "forecast": [], "components": [], "stats": {}, "backtest": {}}

    latest = fc["run_date"].max()
    latest = pd.Timestamp(latest).normalize()
    fc = fc[pd.to_datetime(fc["run_date"]).dt.normalize() == latest]
    comp = comp[pd.to_datetime(comp["run_date"]).dt.normalize() == latest]
    origin = str(pd.to_datetime(fc["origin_date"].iloc[0]).date())

    stats: dict[str, Any] = {}
    backtest: dict[str, Any] = {}
    if not ev.empty:
        ev["target_date"] = pd.to_datetime(ev["target_date"])
        cutoff = ev["target_date"].max() - pd.DateOffset(years=5)
        recent = ev[ev["target_date"] >= cutoff]
        for h in sorted(ev["horizon"].unique()):
            e_all = ev[ev["horizon"] == h]["error"].dropna()
            e = recent[recent["horizon"] == h]["error"].dropna()
            stats[str(int(h))] = {
                "n": int(e.size),
                "n_full": int(e_all.size),
                "mae": round(float(e.abs().mean()), 1),
                "rmse": round(float(np.sqrt((e ** 2).mean())), 1),
                "mae_full": round(float(e_all.abs().mean()), 1),
                "rmse_full": round(float(np.sqrt((e_all ** 2).mean())), 1),
                "q05": round(float(e.quantile(0.05)), 1),
                "q95": round(float(e.quantile(0.95)), 1),
            }
    benchmarks: dict[str, Any] = {}
    benchmark_note = ""
    if not ev.empty:

        def _metric(frame: pd.DataFrame, col: str) -> dict[str, float]:
            e = (frame["actual"] - frame[col]).dropna()
            if e.empty:
                return {"mae": None, "rmse": None}
            return {
                "mae": round(float(e.abs().mean()), 1),
                "rmse": round(float(np.sqrt((e ** 2).mean())), 1),
            }

        dm_names = {
            "v010": ("dm_v010_dm_stat", "dm_v010_p_value", "dm_v010_p_bonferroni"),
            "naive": ("dm_naive_dm_stat", "dm_naive_p_value", "dm_naive_p_bonferroni"),
        }
        for h in sorted(ev["horizon"].unique()):
            h = int(h)
            sub = ev[ev["horizon"] == h]
            rec = sub[sub["target_date"] >= cutoff]
            full = {
                "model": _metric(sub, "predicted"),
                "v010": _metric(sub, "predicted_v010"),
                "naive": _metric(sub, "naive"),
                "seasonal_naive": _metric(sub, "seasonal_naive"),
            }
            recent = {
                "model": _metric(rec, "predicted"),
                "v010": _metric(rec, "predicted_v010"),
                "naive": _metric(rec, "naive"),
                "seasonal_naive": _metric(rec, "seasonal_naive"),
            }
            dm = {
                name: {
                    "dm_stat": None if pd.isna(sub[cols[0]].iloc[0]) else round(float(sub[cols[0]].iloc[0]), 3),
                    "p_value": None if pd.isna(sub[cols[1]].iloc[0]) else round(float(sub[cols[1]].iloc[0]), 4),
                    "p_bonferroni": None if pd.isna(sub[cols[2]].iloc[0]) else round(float(sub[cols[2]].iloc[0]), 4),
                    "significant_bonferroni": False
                    if pd.isna(sub[cols[2]].iloc[0])
                    else bool(float(sub[cols[2]].iloc[0]) < 0.05),
                }
                for name, cols in dm_names.items()
            }
            benchmarks[str(h)] = {
                "recent": recent,
                "full": full,
                "tga_increment": {
                    "recent": round(recent["naive"]["mae"] - recent["model"]["mae"], 1),
                    "full": round(full["naive"]["mae"] - full["model"]["mae"], 1),
                },
                "fallback_share": round(float(sub["snaive_fallback"].astype(float).mean()), 4),
                "fallback_n": int(sub["snaive_fallback"].astype(float).sum()),
                "n": int(sub.shape[0]),
                "dm": dm,
            }

        # 一行结论（图注只补这一行，不新增图）
        def verdict(m: float, b: float) -> str:
            return "优于" if m < b else ("不如" if m > b else "持平")

        def sig(entry: dict[str, Any]) -> str:
            return "Bonferroni 显著" if entry["significant_bonferroni"] else "不显著"

        parts = []
        mv = str(fc["model_version"].iloc[0])
        for h in sorted(benchmarks):
            b = benchmarks[h]["full"]
            parts.append(
                f"h={h} {mv} {b['model']['mae'] / 1000:.1f}B vs 0.1.0 "
                f"{b['v010']['mae'] / 1000:.1f}B（{verdict(b['model']['mae'], b['v010']['mae'])}，"
                f"{sig(benchmarks[h]['dm']['v010'])}）"
                f"、vs naive {b['naive']['mae'] / 1000:.1f}B（"
                f"{verdict(b['model']['mae'], b['naive']['mae'])}，"
                f"{sig(benchmarks[h]['dm']['naive'])}）"
            )
        h1 = benchmarks.get("1", {})
        snaive = h1.get("full", {}).get("seasonal_naive", {}).get("mae")
        inc = h1.get("tga_increment", {})
        fallback = h1.get("fallback_share")
        benchmark_note = (
            "基准对比（全样本 MAE）：" + "；".join(parts)
            + f"；seasonal naive {'—' if snaive is None else f'{snaive / 1000:.1f}B'}"
            + "（跨年水平漂移，远差于两者）"
            + f"；增量来源仅 TGA（h=1 naive−模型 {inc.get('full', 0) / 1000:.1f}B）"
            + f"；seasonal naive 回退 {h1.get('fallback_n', 0)}/{h1.get('n', 0)}"
            + f"（{0 if fallback is None else fallback:.1%}）。"
        )

    # h=1 回测对照（近 3 年，控制 data.js 体积）
    recent = ev[(ev["horizon"] == 1) & (pd.to_datetime(ev["target_date"]) >= pd.Timestamp("2023-01-01"))]
    backtest["1"] = {
        "t": [str(pd.to_datetime(d).date()) for d in recent["target_date"]],
        "pred": [round(float(x), 0) for x in recent["predicted"]],
        "actual": [round(float(x), 0) for x in recent["actual"]],
    }

    comp_out: list[dict[str, Any]] = []
    for row in comp.itertuples():
        comp_out.append(
            {
                "h": int(row.horizon),
                "name": str(row.component),
                "level": round(float(row.level), 0),
                "delta": round(float(row.delta_from_origin), 0),
                "method": str(row.method),
                "residual": bool(row.is_residual_bucket),
            }
        )

    return {
        "run_date": str(pd.Timestamp(latest).date()),
        "origin_date": origin,
        "model_version": str(fc["model_version"].iloc[0]),
        "horizons": [int(x) for x in sorted(fc["horizon"].unique())],
        "forecast": [
            {
                "t": str(pd.to_datetime(r.target_date).date()),
                "h": int(r.horizon),
                "v": round(float(r.predicted_reserve), 0),
                "lo": round(float(r.lower), 0),
                "hi": round(float(r.upper), 0),
            }
            for r in fc.itertuples()
        ],
        "components": comp_out,
        "stats": stats,
        "benchmarks": benchmarks,
        "benchmark_note": benchmark_note,
        "backtest": backtest,
    }


def load_qra_targets(data_dir: Path) -> list[dict[str, Any]]:
    """读取季度再融资公告中的现金余额假设（单点目标）。"""
    pattern = str(
        data_dir
        / "canonical"
        / "TREASURY_ANNOUNCEMENT"
        / "entity=quarterly_refunding"
        / "**"
        / "*.parquet"
    )
    con = duckdb.connect()
    try:
        df = con.execute(
            "SELECT publication_time, document_type, cash_balance_assumption_usd_millions "
            f"FROM read_parquet('{pattern}', hive_partitioning=1) "
            "WHERE cash_balance_assumption_usd_millions IS NOT NULL "
            "ORDER BY publication_time"
        ).pl().to_pandas()
    finally:
        con.close()
    return [
        {
            "date": str(pd.to_datetime(row.publication_time).date()),
            "type": str(row.document_type),
            "value": round(float(row.cash_balance_assumption_usd_millions), 2),
        }
        for row in df.itertuples()
    ]


# ── 派生计算 ─────────────────────────────────────────────────────────
def weekly(s: pd.Series) -> pd.Series:
    return s.resample("W-WED").last().dropna()


def to_pairs(s: pd.Series, scale: float = 1.0, digits: int = 3) -> dict[str, list]:
    return {
        "t": [d.strftime("%Y-%m-%d") for d in s.index],
        "v": [round(float(x) * scale, digits) for x in s.values],
    }


def compute_weekly_decomp(wres: pd.Series, walcl: pd.Series, tga: pd.Series,
                          rrp: pd.Series, cash: pd.Series) -> list[dict[str, Any]]:
    """周度准备金变动分解；小数项取残差，单位为十亿美元。"""
    df = pd.concat(
        {
            "R": weekly(wres),
            "A": weekly(walcl),
            "TGA": weekly(tga),
            "RRP": weekly(rrp) * 1000.0,  # 十亿 → 百万
            "C": weekly(cash),
        },
        axis=1,
        join="inner",
    ).dropna()
    d = df.diff().dropna()
    rows: list[dict[str, Any]] = []
    for idx, row in d.iterrows():
        rows.append(
            {
                "t": idx.strftime("%Y-%m-%d"),
                "dR": round(row["R"] / 1000.0, 2),
                "dA": round(row["A"] / 1000.0, 2),
                "tga": round(-row["TGA"] / 1000.0, 2),
                "onrrp": round(-row["RRP"] / 1000.0, 2),
                "cash": round(-row["C"] / 1000.0, 2),
                "other": round(
                    (row["R"] - (row["A"] - row["TGA"] - row["RRP"] - row["C"])) / 1000.0,
                    2,
                ),
            }
        )
    return rows[-26:]


def compute_three_pool(wres: pd.Series, tga: pd.Series, rrp: pd.Series) -> dict[str, list]:
    """TGA / ON RRP / 准备金三池周度对齐（万亿美元）。"""
    df = pd.concat(
        {
            "reserve": weekly(wres) / 1e6,
            "tga": weekly(tga) / 1e6,
            "onrrp": weekly(rrp) * 1000.0 / 1e6,
        },
        axis=1,
        join="inner",
    ).dropna().loc["2019-01":]
    return {
        "t": [d.strftime("%Y-%m-%d") for d in df.index],
        "reserve": [round(float(x), 3) for x in df["reserve"]],
        "tga": [round(float(x), 3) for x in df["tga"]],
        "onrrp": [round(float(x), 3) for x in df["onrrp"]],
    }


def compute_btc_res(btc: pd.Series, wres: pd.Series) -> dict[str, list]:
    """BTC 日线日期上按 as-of 对齐准备金周值（万亿美元）。"""
    w = weekly(wres) / 1e6
    aligned = w.reindex(btc.index, method="ffill")
    df = pd.DataFrame({"btc": btc, "reserve": aligned}).dropna()
    return {
        "t": [d.strftime("%Y-%m-%d") for d in df.index],
        "btc": [round(float(x), 2) for x in df["btc"]],
        "reserve": [round(float(x), 4) for x in df["reserve"]],
    }


def compute_reserve_diff(wres: pd.Series) -> dict[str, list]:
    w = weekly(wres)
    d = w.diff().dropna()
    count = 0
    consec: list[int] = []
    for x in d.values:
        count = count + 1 if x < 0 else 0
        consec.append(count)
    return {
        "t": [x.strftime("%Y-%m-%d") for x in d.index],
        "v": [round(float(x) / 1000.0, 2) for x in d.values],  # 十亿
        "consec": consec,
    }


def compute_btc_reserve_corr(btc: pd.Series, wres: pd.Series) -> dict[str, Any]:
    btc_w = btc.resample("W-WED").last().pct_change() * 100.0
    res_w = weekly(wres).diff() / 1000.0
    df = pd.concat({"btc": btc_w, "res": res_w}, axis=1, join="inner").dropna()
    out: dict[str, Any] = {"t": [], "c13": [], "c26": [], "n": len(df)}
    if len(df) < 14:
        return out
    c13 = df["btc"].rolling(13).corr(df["res"])
    c26 = df["btc"].rolling(26).corr(df["res"])
    out["t"] = [d.strftime("%Y-%m-%d") for d in df.index]
    out["c13"] = [None if pd.isna(x) else round(float(x), 3) for x in c13.values]
    out["c26"] = [None if pd.isna(x) else round(float(x), 3) for x in c26.values]
    return out


def compute_macro(
    dxy: pd.Series, ust: pd.Series, vix: pd.Series, tlt: pd.Series
) -> dict[str, Any]:
    """四变量 Z-Score 与状态匹配（DXY / UST10Y / TLT / VIX）。

    TLT 为 20+ 年美债 ETF 价格：价格上涨等价于长端利率下行。
    A–D 状态定义与知识库一致：A/B 中 TLT 期望下跌，C/D 中期望上涨。
    """
    df = pd.DataFrame({"dxy": dxy})
    df["ust"] = ust.reindex(df.index, method="ffill")
    df["vix"] = vix.reindex(df.index, method="ffill")
    df["tlt"] = tlt.reindex(df.index, method="ffill")
    df = df.dropna()
    z = (df - df.mean()) / df.std(ddof=0)

    def direction(s: pd.Series, window: int = 20) -> bool:
        return bool(s.iloc[-1] >= s.iloc[-window])

    dxy_up = direction(df["dxy"])
    ust_up = direction(df["ust"])
    tlt_up = direction(df["tlt"])
    vix_up = direction(df["vix"])

    # A/B/C/D 四个变量上的方向匹配数（满分 4）
    scores = {
        "A": int(dxy_up) + int(ust_up) + int(not tlt_up) + int(vix_up),
        "B": int(dxy_up) + int(not ust_up) + int(tlt_up) + int(vix_up),
        "C": int(not dxy_up) + int(ust_up) + int(not tlt_up) + int(not vix_up),
        "D": int(not dxy_up) + int(not ust_up) + int(tlt_up) + int(not vix_up),
    }
    best_score = max(scores.values())
    ties = sorted(k for k, v in scores.items() if v == best_score)
    return {
        "t": [d.strftime("%Y-%m-%d") for d in z.index],
        "dxy_z": [round(float(x), 3) for x in z["dxy"].values],
        "ust_z": [round(float(x), 3) for x in z["ust"].values],
        "vix_z": [round(float(x), 3) for x in z["vix"].values],
        "tlt_z": [round(float(x), 3) for x in z["tlt"].values],
        "directions": {
            "dxy_up": dxy_up,
            "ust_up": ust_up,
            "tlt_up": tlt_up,
            "vix_up": vix_up,
        },
        "match": {
            k: {"hits": v, "pct": round(v / 4 * 100)}
            for k, v in scores.items()
        },
        "best": "/".join(ties),
        "best_pct": round(best_score / 4 * 100),
        "ties": ties,
        "window": 20,
        "variables": 4,
        "last": {
            "dxy": round(float(df["dxy"].iloc[-1]), 3),
            "ust10": round(float(df["ust"].iloc[-1]), 3),
            "tlt": round(float(df["tlt"].iloc[-1]), 3),
            "vix": round(float(df["vix"].iloc[-1]), 3),
            "chg20": {
                "dxy": round(float(df["dxy"].iloc[-1] - df["dxy"].iloc[-20]), 3),
                "ust10": round(float(df["ust"].iloc[-1] - df["ust"].iloc[-20]), 3),
                "tlt": round(float(df["tlt"].iloc[-1] - df["tlt"].iloc[-20]), 3),
                "vix": round(float(df["vix"].iloc[-1] - df["vix"].iloc[-20]), 3),
            },
        },
    }


def compute_russell(rut: pd.Series, spx: pd.Series, vix: pd.Series) -> dict[str, Any]:
    """罗素 2000 / 标普 500 相对强弱（比值首日归一为 100）+ VIX 叠加。"""
    df = pd.concat({"rut": rut, "spx": spx, "vix": vix}, axis=1, join="inner").dropna()
    if df.empty:
        return {"t": [], "ratio": [], "vix": [], "rut": [], "spx": []}
    ratio = (df["rut"] / df["spx"])
    ratio = ratio / ratio.iloc[0] * 100.0
    return {
        "t": [d.strftime("%Y-%m-%d") for d in df.index],
        "ratio": [round(float(x), 2) for x in ratio.values],
        "vix": [round(float(x), 2) for x in df["vix"].values],
        "rut": [round(float(x), 2) for x in df["rut"].values],
        "spx": [round(float(x), 2) for x in df["spx"].values],
        "base_date": df.index[0].strftime("%Y-%m-%d"),
    }


def compute_yield_curve(curve: pd.DataFrame, tips: pd.Series) -> dict[str, Any]:
    monthly = curve.resample("ME").last().loc["2019-01":]
    last_actual = curve.index.max()
    if len(monthly) and monthly.index[-1] > last_actual:
        monthly = monthly.rename(index={monthly.index[-1]: last_actual})
    tips_m = tips.resample("ME").last().reindex(monthly.index)
    values: list[list[float | None]] = []
    for _, row in monthly.iterrows():
        values.append([None if pd.isna(x) else round(float(x), 3) for x in row.values])
    return {
        "tenor_years": [0.08, 0.5, 1, 2, 10, 20, 30],
        "tenor_labels": ["4W", "6M", "1Y", "2Y", "10Y", "20Y", "30Y"],
        "dates": [d.strftime("%Y-%m-%d") for d in monthly.index],
        "values": values,
        "tips10": [None if pd.isna(x) else round(float(x), 3) for x in tips_m.values],
    }


def latest(s: pd.Series) -> float | None:
    return None if s.empty else float(s.iloc[-1])


def chg(s: pd.Series, periods: int) -> float | None:
    return None if len(s) <= periods else float(s.iloc[-1] - s.iloc[-periods])


def build_payload() -> dict[str, Any]:
    settings = Settings.load()
    data_dir = Path(settings.data_dir)
    if not data_dir.is_absolute():
        data_dir = PROJECT_ROOT / data_dir
    data_dir = data_dir.resolve()

    meta = open_meta(settings)
    with contextlib.ExitStack() as stack:
        stack.callback(meta.close)
        service, con = open_query_service(settings, meta)
        stack.callback(con.close)

        num: dict[str, pd.Series] = {}
        for name, dataset_id in FRED.items():
            start = WEEKLY_START if name in {"WRESBAL", "WRBWFRBL", "WTREGEN", "WALCL", "WCURCIR"} else DAILY_START
            entity = "RRPONTSYAWARD" if name == "RRP_AWARD" else name
            num[name] = load_number(service, dataset_id, entity, start)

        rrp = load_number(service, FRED["RRPONTSYD"], "RRPONTSYD", WEEKLY_START)
        srf = load_number(service, SRF_DATASET, SRF_ENTITY, SRF_START)

        ohlcv: dict[str, pd.Series] = {}
        for name, entity in OHLCV.items():
            start = BTC_START if name == "BTC" else MACRO_START
            ohlcv[name] = load_ohlcv(data_dir, entity, start)

        etf = load_flow(service, ETF_DATASET, ETF_ENTITY, None)
        usdt = load_number(service, "coinmetrics_usdt_supply", "usdt_supply", STABLE_START)
        usdc = load_number(service, "coinmetrics_usdc_supply", "usdc_supply", STABLE_START)

    tga_daily = load_tga_daily(data_dir, datetime(2018, 1, 1))
    forecast = load_tga_forecast(data_dir)
    reserve_forecast = load_reserve_forecast(data_dir)
    qra = load_qra_targets(data_dir)

    # 单位换算：准备金类为百万美元；RRP 为十亿美元
    cutoffs = {k: (s.index.max().strftime("%Y-%m-%d") if len(s) else None)
               for k, s in {**num, **ohlcv}.items()}
    cutoffs["TGA_DAILY"] = tga_daily.index.max().strftime("%Y-%m-%d") if len(tga_daily) else None
    cutoffs["ETF_FLOW"] = etf.index.max().strftime("%Y-%m-%d") if len(etf) else None
    cutoffs["USDT"] = usdt.index.max().strftime("%Y-%m-%d") if len(usdt) else None
    cutoffs["USDC"] = usdc.index.max().strftime("%Y-%m-%d") if len(usdc) else None
    cutoffs["SRF"] = srf.index.max().strftime("%Y-%m-%d") if len(srf) else None

    decomp = compute_weekly_decomp(
        num["WRESBAL"], num["WALCL"], num["WTREGEN"], rrp, num["WCURCIR"]
    )
    reserve_diff = compute_reserve_diff(num["WRESBAL"])
    corr = compute_btc_reserve_corr(ohlcv["BTC"], num["WRESBAL"])
    macro = compute_macro(ohlcv["DXY"], num["DGS10"], num["VIXCLS"], ohlcv["TLT"])
    russell = compute_russell(ohlcv["RUT"], ohlcv["SPX"], num["VIXCLS"])

    curve = pd.DataFrame(
        {
            "4W": num["DTB4WK"],
            "6M": num["DTB6"],
            "1Y": num["DTB1YR"],
            "2Y": num["DGS2"],
            "10Y": num["DGS10"],
            "20Y": num["DGS20"],
            "30Y": num["DGS30"],
        }
    ).sort_index()
    yield_curve = compute_yield_curve(curve, num["DFII10"])

    # 收益率拆分
    yield_split = pd.concat(
        {
            "nominal": num["DGS10"],
            "real": num["DFII10"],
            "be": num["DGS10"] - num["DFII10"],
            "be_market": num["T10YIE"],
        },
        axis=1,
        join="inner",
    ).dropna()

    # 核心状态读数
    wres_last = latest(num["WRESBAL"])
    reserve_t = round(wres_last / 1e6, 3) if wres_last is not None else None
    if reserve_t is None:
        reserve_zone = ("未知", "muted")
    elif reserve_t < 3.0:
        reserve_zone = ("紧张区", "danger")
    elif reserve_t < 3.2:
        reserve_zone = ("预警区", "warn")
    else:
        reserve_zone = ("充裕区", "ok")

    spread_bp = None
    if latest(num["SOFR"]) is not None and latest(num["EFFR"]) is not None:
        spread_bp = round((latest(num["SOFR"]) - latest(num["EFFR"])) * 100, 1)
    corridor_status = (
        ("正常", "ok") if spread_bp is not None and spread_bp < 12
        else ("预警", "warn") if spread_bp is not None and spread_bp <= 17
        else ("极端", "danger") if spread_bp is not None else ("未知", "muted")
    )

    c13 = next((x for x in reversed(corr["c13"]) if x is not None), None)
    if c13 is None:
        corr_label, corr_tone = ("数据不足", "muted")
    elif abs(c13) >= 0.6:
        corr_label, corr_tone = ("强相关", "ok" if c13 > 0 else "danger")
    elif abs(c13) >= 0.3:
        corr_label, corr_tone = ("中等相关", "warn" if c13 < 0 else "muted")
    else:
        corr_label, corr_tone = ("弱相关", "muted")

    # 稳定币与 ETF 聚合
    stable = pd.concat({"usdt": usdt, "usdc": usdc}, axis=1, join="outer").sort_index()
    stable = stable.ffill().dropna()
    stable_total = (stable["usdt"] + stable["usdc"]) if len(stable) else pd.Series(dtype=float)

    etf_cum = float(etf.sum()) if len(etf) else None
    etf_7d = float(etf.iloc[-7:].sum()) if len(etf) >= 7 else None

    framework = {
        "liquidity": {
            "reserve_t": reserve_t,
            "reserve_4w_b": (round(chg(num["WRESBAL"], 4) / 1000, 1)
                             if chg(num["WRESBAL"], 4) is not None else None),
            "spread_bp": spread_bp,
            "effr": latest(num["EFFR"]),
            "sofr": latest(num["SOFR"]),
            "iorb": latest(num["IORB"]),
            "onrrp_rate": latest(num["RRP_AWARD"]),
            "srf_usage_b": round(latest(srf) / 1e9, 2) if latest(srf) is not None else None,
            "srf_max_30d_b": (round(float(srf.tail(30).max()) / 1e9, 2)
                              if len(srf) >= 30 else None),
        },
        "transmission": {
            "dxy": latest(ohlcv["DXY"]),
            "dxy_chg20": chg(ohlcv["DXY"], 20),
            "real_yield": latest(num["DFII10"]),
            "real_chg20": chg(num["DFII10"], 20),
            "vix": latest(num["VIXCLS"]),
            "vix_chg20": chg(num["VIXCLS"], 20),
        },
        "crypto": {
            "stable_total_b": round(latest(stable_total) / 1e9, 2) if latest(stable_total) else None,
            "stable_chg30_b": (round(chg(stable_total, 30) / 1e9, 2)
                               if chg(stable_total, 30) is not None else None),
            "etf_last_b": round(float(etf.iloc[-1]) / 1e9, 2) if len(etf) else None,
            "etf_7d_b": round(etf_7d / 1e9, 2) if etf_7d is not None else None,
            "etf_cum_b": round(etf_cum / 1e9, 2) if etf_cum is not None else None,
            "btc": latest(ohlcv["BTC"]),
            "btc_chg30": chg(ohlcv["BTC"], 30),
        },
    }

    series = {
        "WRESBAL": to_pairs(num["WRESBAL"]),
        "WRBWFRBL": to_pairs(num["WRBWFRBL"]),
        "WTREGEN": to_pairs(num["WTREGEN"]),
        "RRPONTSYD": to_pairs(rrp),
        "WALCL": to_pairs(num["WALCL"]),
        "WCURCIR": to_pairs(num["WCURCIR"]),
        "EFFR": to_pairs(num["EFFR"]),
        "IORB": to_pairs(num["IORB"]),
        "SOFR": to_pairs(num["SOFR"]),
        "RRP_AWARD": to_pairs(num["RRP_AWARD"]),
        "DGS10": to_pairs(num["DGS10"]),
        "DGS2": to_pairs(num["DGS2"]),
        "DGS20": to_pairs(num["DGS20"]),
        "DGS30": to_pairs(num["DGS30"]),
        "DTB4WK": to_pairs(num["DTB4WK"]),
        "DTB6": to_pairs(num["DTB6"]),
        "DTB1YR": to_pairs(num["DTB1YR"]),
        "DFII10": to_pairs(num["DFII10"]),
        "T10YIE": to_pairs(num["T10YIE"]),
        "VIXCLS": to_pairs(num["VIXCLS"]),
        "BTC": to_pairs(ohlcv["BTC"], digits=2),
        "DXY": to_pairs(ohlcv["DXY"], digits=3),
        "SPX": to_pairs(ohlcv["SPX"], digits=2),
        "NDX": to_pairs(ohlcv["NDX"], digits=2),
        "GOLD": to_pairs(ohlcv["GOLD"], digits=2),
        "TLT": to_pairs(ohlcv["TLT"], digits=3),
        "RUT": to_pairs(ohlcv["RUT"], digits=2),
        "IWM": to_pairs(ohlcv["IWM"], digits=2),
        "SRF_USAGE": to_pairs(srf, scale=1e-9, digits=3),
        "USDT": to_pairs(usdt, digits=0),
        "USDC": to_pairs(usdc, digits=0),
        "ETF_FLOW": to_pairs(etf, digits=2),
    }

    payload: dict[str, Any] = {
        "meta": {
            "generated_at": datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC"),
            "data_cutoff": max(x for x in cutoffs.values() if x),
            "source": "本地 ChronoForge canonical / derived（只读）",
            "note": "缺失序列不做估算；partial 图表只展示真实可得的序列。",
        },
        "cutoffs": cutoffs,
        "series": series,
        "weekly": {
            "decomp": decomp,
            "reserve_diff": reserve_diff,
            "three_pool": compute_three_pool(num["WRESBAL"], num["WTREGEN"], rrp),
        },
        "btc_res": compute_btc_res(ohlcv["BTC"], num["WRESBAL"]),
        "corr": corr,
        "macro": macro,
        "russell": russell,
        "yield_curve": yield_curve,
        "yield_split": {
            "t": [d.strftime("%Y-%m-%d") for d in yield_split.index],
            "nominal": [round(float(x), 3) for x in yield_split["nominal"]],
            "real": [round(float(x), 3) for x in yield_split["real"]],
            "be": [round(float(x), 3) for x in yield_split["be"]],
            "be_market": [round(float(x), 3) for x in yield_split["be_market"]],
        },
        "tga": {
            "daily": to_pairs(tga_daily),
            "forecast": forecast,
            "qra": qra,
        },
        "reserve_forecast": reserve_forecast,
        "framework": framework,
        "status": {
            "reserve_t": reserve_t,
            "reserve_zone": reserve_zone[0],
            "reserve_tone": reserve_zone[1],
            "corridor_spread_bp": spread_bp,
            "corridor_status": corridor_status[0],
            "corridor_tone": corridor_status[1],
            "macro_state": macro["best"],
            "macro_pct": macro["best_pct"],
            "corr13": c13,
            "corr_label": corr_label,
            "corr_tone": corr_tone,
        },
    }
    return payload


def main() -> None:
    payload = build_payload()
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    js = "window.CF_DATA = " + text + ";\n"
    DATA_JS.write_text(js, encoding="utf-8")
    DATA_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"已导出 {DATA_JS}（{DATA_JS.stat().st_size / 1024:.0f} KB）")
    print(f"数据截止：{payload['meta']['data_cutoff']}")
    print(f"准备金：{payload['status']['reserve_t']} 万亿 → {payload['status']['reserve_zone']}")
    print(
        f"宏观状态：{payload['status']['macro_state']} "
        f"({payload['status']['macro_pct']}%，4/4 变量)"
    )
    print(
        f"SRF 用量：{payload['framework']['liquidity']['srf_usage_b']}B "
        f"（30日峰值 {payload['framework']['liquidity']['srf_max_30d_b']}B，截止 {payload['cutoffs']['SRF']}）"
    )


if __name__ == "__main__":
    main()
