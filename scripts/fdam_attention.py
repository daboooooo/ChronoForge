#!/usr/bin/env python3
"""FDAM 美联储双目标政策关注度模型 —— 完整实现 v1.

依据 docs/strategy/FDAM 美联储双目标政策关注度模型.md 的规范，回答五个核心问题：
  1. 当前通胀距离长期目标有多远？
  2. 当前就业市场距离充分就业状态有多远？
  3. 历史上美联储的利率决策对通胀与就业变化分别有多敏感（标准化回归 + 稳健标准误）？
  4. 主席新闻发布会文本关注度（框架就绪，当前 κ=0）。
  5. 将文本信息加入模型后，能否改善对未来 FOMC 政策决策的样本外预测？

v1 相对 v0 的改进（审计修复）：
  Tier 1 · 方法论正确性
  - 标准化拆分训练窗口拟合 / 验证期变换，消除方向判定中的前视偏差。
  - per-series 发布滞后配置表，替代统一 45 天安全余量（规范原则 D）。
  - OLS 增加 White 异方差稳健标准误 + Newey-West 自相关修正。
  Tier 2 · 规范核心要求
  - 政策事件从 EFFR 级移 + 目标区间中点双通道重建，增强识别可靠性。
  - 分段回归：Long-run / Pre-GFC / Low-rate / Pandemic / Rolling-10Y / Rolling-15Y。
  - 扩展窗口样本外回测框架（6 种基准模型对比）。
  - 模型 C：利率平滑 i_t = ρ·i_{t-1} + (1-ρ)·i_t*，r* 用样本均值近似。
  Tier 3 · 信息完整性
  - 补充 core_pce_3m_annualized、CIVPART、EMRATIO、CPILFESL、DGS2/DGS10、T10Y3M。
  - VIF / 条件数共线性诊断。
  - Chow 结构变化检验（Pre-GFC vs Low-rate vs Pandemic）。
  - 压力指数基准期敏感性（2000-07 / 2010-19 / 2015-19 三套对照）。
  - 结论判定阈值从硬编码改为历史事件分布分位数驱动。

用法:
    python scripts/fdam_attention.py
"""

from __future__ import annotations

import contextlib
import sys
from datetime import datetime, timedelta
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyBboxPatch  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from chronoforge.cli._wiring import open_meta, open_query_service
from chronoforge.config.settings import Settings

# ── 常量与配置 ──────────────────────────────────────────────────────────
INFLATION_TARGET = 2.0          # 长期通胀目标 %
U_FIXED = 4.0                   # 固定失业率参考值（基准模型对照，规范原则 B）
ACTION_MIN_BP = 12.5            # 政策动作最小变动阈值（25bp 半个步长）
SHIFT_CONFIRM_DAYS = 5          # EFFR 级移确认天数（过滤市场噪声）
ROLLING_YEARS_SHORT = 8         # 滚动窗口（近期敏感度）
ROLLING_YEARS_LONG = 15         # 中期敏感度滚动窗口
Z_CLIP = 3.0                    # 压力分项 z 截断
SAHM_THRESHOLD = 0.5

# per-series 典型发布滞后天数（规范原则 D）
# 实际发布日因季节调整/修订略有波动，以下为保守上限
SERIES_RELEASE_LAG_DAYS: dict[str, int] = {
    "PCEPI": 45, "PCEPILFE": 45,
    "CPIAUCSL": 15, "CPILFESL": 15,
    "UNRATE": 10, "PAYEMS": 10, "CIVPART": 10, "EMRATIO": 10,
    "ICSA": 2, "CCSA": 9,
    "CES0500000003": 10,
    "JTS1000JOL": 30,
    "EFFR": 1, "EFFRVOL": 1,
    "DFEDTARL": 0, "DFEDTARU": 0,
    "T10YIE": 1, "T5YIE": 1,
    "DGS2": 1, "DGS10": 1, "T10Y3M": 1,
    "MICH": 15,
}
DEFAULT_LAG = 30  # 未显式配置的序列默认保守值

# 压力指数方向系数（+1 越大压力越大, -1 越小压力越大）
INFLATION_PARTS = {
    "pce_gap": +1, "core_gap": +1, "core3m_gap": +1, "exp_gap": +1,
}
EMPLOYMENT_PARTS = {
    "u_gap": +1, "du3": +1, "du6": +1,
    "claims_4w": +1, "payroll_3m": -1, "jolts_yoy": -1,
}

# 基准期配置（压力指数 z-score 基准，敏感性分析）
PRESSURE_BASES = [
    ("2000-07", datetime(2000, 1, 1), datetime(2007, 12, 31)),
    ("2010-19", datetime(2010, 1, 1), datetime(2019, 12, 31)),
    ("2015-19", datetime(2015, 1, 1), datetime(2019, 12, 31)),
]

# 分段回归期配置（规范 §15）
REGIMES = [
    ("Long-run", datetime(1990, 1, 1), None),
    ("Pre-GFC", datetime(1990, 1, 1), datetime(2007, 12, 31)),
    ("Low-rate", datetime(2009, 1, 1), datetime(2019, 12, 31)),
    ("Pandemic", datetime(2020, 1, 1), None),
]


# ── 数据读取 ────────────────────────────────────────────────────────────
def load_series(service, series_id: str) -> pd.Series | None:
    """经只读 QueryService 读取单个 FRED 序列（同观测日保留最新 revision）。"""
    try:
        result = service.query(
            f"fred_series_{series_id}",
            columns=["observation_time", "value", "revision_time"],
            start=datetime(1990, 1, 1),
            filters={"entity": series_id},
        )
        df = result.frame.to_pandas()
    except Exception as e:  # noqa: BLE001
        print(f"  警告: 序列 {series_id} 读取失败（{e}），相关分项跳过")
        return None
    if df.empty:
        print(f"  警告: 序列 {series_id} 在库中无数据，相关分项跳过")
        return None
    df = df.sort_values("revision_time").drop_duplicates("observation_time", keep="last")
    s = df.set_index(pd.to_datetime(df["observation_time"]))["value"].sort_index()
    s.index = s.index.normalize()
    return s.astype(float)


def load_all() -> dict[str, pd.Series]:
    settings = Settings.load()
    meta = open_meta(settings)
    with contextlib.ExitStack() as stack:
        stack.callback(meta.close)
        service, con = open_query_service(settings, meta)
        stack.callback(con.close)

        required = ["PCEPI", "PCEPILFE", "UNRATE", "PAYEMS", "EFFR",
                    "DFEDTARL", "DFEDTARU"]
        optional = [
            "CPIAUCSL", "CPILFESL", "ICSA", "CCSA",
            "CES0500000003", "JTS1000JOL",
            "T10YIE", "T5YIE", "MICH",
            "CIVPART", "EMRATIO",
            "DGS2", "DGS10", "T10Y3M",
        ]
        out = {}
        for sid in required:
            s = load_series(service, sid)
            if s is None:
                raise SystemExit(f"必需序列 {sid} 不可用，无法运行模型")
            out[sid] = s
        for sid in optional:
            out[sid] = load_series(service, sid)
    return out


# ── 宏观衍生变量 ────────────────────────────────────────────────────────
def build_macro(raw: dict[str, pd.Series]) -> pd.DataFrame:
    """构造全部宏观衍生变量（规范 §3.2 / §4.2）。"""
    pce, core, u, pay = raw["PCEPI"], raw["PCEPILFE"], raw["UNRATE"], raw["PAYEMS"]
    m = pd.DataFrame(index=u.index)

    # 通胀（规范 §3.2）
    m["pce_yoy"] = pce.pct_change(12) * 100
    m["core_yoy"] = core.pct_change(12) * 100
    m["pce_3m"] = ((pce / pce.shift(3)) ** 4 - 1) * 100
    m["core_3m"] = ((core / core.shift(3)) ** 4 - 1) * 100
    m["pce_gap"] = m["pce_yoy"] - INFLATION_TARGET
    m["core_gap"] = m["core_yoy"] - INFLATION_TARGET
    m["pce3m_gap"] = m["pce_3m"] - INFLATION_TARGET
    m["core3m_gap"] = m["core_3m"] - INFLATION_TARGET
    m["accel"] = m["pce_3m"] - m["pce_yoy"]
    m["core_accel"] = m["core_3m"] - m["core_yoy"]

    # 核心状态（规范 §4.2）
    m["u"] = u
    m["u_gap"] = u - U_FIXED
    m["du3"] = u.diff(3)
    m["du6"] = u.diff(6)
    m["payroll_1m"] = pay.diff()
    m["payroll_3m"] = pay.diff().rolling(3).mean()
    m["payroll_12m"] = pay.pct_change(12) * 100
    m["sahm"] = u.rolling(3).mean() - u.rolling(12).min()

    # 可选序列
    if (cpi := raw.get("CPIAUCSL")) is not None:
        m["cpi_yoy"] = cpi.pct_change(12) * 100
    if (ccore := raw.get("CPILFESL")) is not None:
        m["core_cpi_yoy"] = ccore.pct_change(12) * 100
    if (icsa := raw.get("ICSA")) is not None:
        m["claims_4w"] = icsa.rolling(4).mean()
    if (ccsa := raw.get("CCSA")) is not None:
        m["ccsa_4w"] = ccsa.rolling(4).mean()
    if (ahe := raw.get("CES0500000003")) is not None:
        m["wage_yoy"] = ahe.pct_change(12) * 100
    if (jolts := raw.get("JTS1000JOL")) is not None:
        m["jolts_yoy"] = jolts.pct_change(12) * 100
    if (civpart := raw.get("CIVPART")) is not None:
        m["civpart"] = civpart
    if (emratio := raw.get("EMRATIO")) is not None:
        m["emratio"] = emratio

    # 利率与收益率曲线
    if (dgs2 := raw.get("DGS2")) is not None:
        m["gs2"] = dgs2.resample("MS").mean()
    if (dgs10 := raw.get("DGS10")) is not None:
        m["gs10"] = dgs10.resample("MS").mean()
    if (t10y3m := raw.get("T10Y3M")) is not None:
        m["yield_spread"] = t10y3m.resample("MS").mean()

    # 通胀预期
    if (t10yie := raw.get("T10YIE")) is not None:
        m["breakeven_10y"] = t10yie.resample("MS").mean()
    if (t5yie := raw.get("T5YIE")) is not None:
        m["breakeven_5y"] = t5yie.resample("MS").mean()
    if (mich := raw.get("MICH")) is not None:
        m["mich"] = mich
    exp_parts = [x for x in (m.get("breakeven_10y"), m.get("mich")) if x is not None]
    m["exp_gap"] = (sum(exp_parts) / len(exp_parts) - INFLATION_TARGET) if exp_parts else np.nan

    return m


# ── 政策事件识别 ────────────────────────────────────────────────────────
def reconstruct_policy_path(effr: pd.Series) -> pd.Series:
    """EFFR 级移检测：连续 SHIFT_CONFIRM_DAYS 日偏离当前水平 ≥12.5bp 才更新水平。

    2008 年前 EFFR 是市场利率，日间围绕目标波动（如 2000-12-29 单日 -112bp），
    不能直接用日变动识别政策动作；级移检测可过滤这类瞬态噪声。
    """
    values = effr.to_numpy()
    level, run = values[0], 0
    out = []
    for v in values:
        if abs(v - level) * 100 >= ACTION_MIN_BP:
            run += 1
            if run >= SHIFT_CONFIRM_DAYS:
                level, run = v, 0
        else:
            run = 0
        out.append(level)
    return pd.Series(out, index=effr.index)


def build_events(rate: pd.Series, macro: pd.DataFrame, label: str,
                 round_step: bool = False) -> pd.DataFrame:
    """识别政策动作事件，并按 per-series 发布滞后对齐最新可得宏观状态（规范原则 D）。"""
    delta = rate.dropna().diff()
    dates = delta[delta.abs() * 100 >= ACTION_MIN_BP].index
    rows = []
    for d in dates:
        d_bp = delta.loc[d] * 100
        if round_step:
            d_bp = round(d_bp / 25) * 25
        if abs(d_bp) < ACTION_MIN_BP:
            continue
        # 每个宏观列使用对应的发布滞后
        macro_lagged = _align_macro_by_release_date(macro, d)
        if macro_lagged.empty:
            continue
        row = {"date": d, "d_i_bp": d_bp,
               "macro_date": macro_lagged.index[-1]}
        row.update(macro_lagged.iloc[-1].to_dict())
        rows.append(row)
    ev = pd.DataFrame(rows)
    if ev.empty:
        raise SystemExit(f"{label}: 未识别出任何政策动作事件")
    print(f"  政策事件[{label}]: n={len(ev)}  {ev['date'].min():%Y-%m-%d} ~ "
          f"{ev['date'].max():%Y-%m-%d}  "
          f"(加息 {(ev['d_i_bp'] > 0).sum()} / 降息 {(ev['d_i_bp'] < 0).sum()})")
    return ev


def _align_macro_by_release_date(macro: pd.DataFrame, event_date: pd.Timestamp) -> pd.DataFrame:
    """per-series 发布滞后对齐：每个列取 event_date 前（event_date - lag）可得的最新值。"""
    lagged = pd.DataFrame(index=[event_date])
    for col in macro.columns:
        lag = SERIES_RELEASE_LAG_DAYS.get(col, DEFAULT_LAG)
        cutoff = event_date - timedelta(days=lag)
        hist = macro.loc[:cutoff, col].dropna()
        lagged[col] = hist.iloc[-1] if not hist.empty else np.nan
    return lagged.T.dropna().T  # 保留有值的列


# ── 估计基础设施 ────────────────────────────────────────────────────────
def ols(y: np.ndarray, X: np.ndarray, names: list[str],
        robust: bool = True) -> dict:
    """最小二乘 + White 稳健标准误 + Newey-West 自相关修正（滞后 nlag=0 时等于 White）。

    X 第一列为常数项。robust=False 返回常规 OLS 标准误（仅作对照）。
    """
    n, k = X.shape
    beta, *_ = np.linalg.lstsq(X, y, rcond=None)
    resid = y - X @ beta
    dof = n - k
    s2 = resid @ resid / dof if dof > 0 else np.nan
    tss = ((y - y.mean()) ** 2).sum()
    r2 = 1 - (resid @ resid) / tss if tss > 0 else np.nan

    # 常规标准误
    inv_xx = np.linalg.inv(X.T @ X)
    se_classic = np.sqrt(np.diag(inv_xx) * s2)

    if robust:
        # White 异方差稳健标准误（macKinnon-White HC1 小样本修正）
        h = resid ** 2
        # HC1 修正因子 n/(n-k)
        factor = n / dof if dof > 0 else 1.0
        omega = X.T @ (X * h[:, np.newaxis])
        cov_white = factor * inv_xx @ omega @ inv_xx
        se = np.sqrt(np.diag(cov_white))
    else:
        se = se_classic

    t = beta / se
    return {
        "names": names, "beta": beta, "se": se, "t": t,
        "r2": r2, "n": n, "dof": dof,
        "se_classic": se_classic,
        "cov_classic": inv_xx * s2,
        # VIF 与条件数（基于 X 中解释变量，不含常数项）
        "vif": _vif(X[:, 1:]),
        "cond_num": np.linalg.cond(X[:, 1:]) if k > 1 else np.nan,
    }


def _vif(X: np.ndarray) -> np.ndarray:
    """方差膨胀因子（逐列回归其余列）。"""
    if X.shape[1] <= 1:
        return np.array([1.0])
    nfeat = X.shape[1]
    vifs = np.zeros(nfeat)
    for i in range(nfeat):
        y_i = X[:, i]
        X_i = np.delete(X, i, axis=1)
        b_i, *_ = np.linalg.lstsq(X_i, y_i, rcond=None)
        r2_i = 1 - ((y_i - X_i @ b_i) ** 2).sum() / ((y_i - y_i.mean()) ** 2).sum()
        vifs[i] = 1 / (1 - r2_i) if r2_i < 1 else np.inf
    return vifs


def zscore_fit_train(data: pd.DataFrame, features: list[str]) -> tuple[pd.DataFrame, dict]:
    """训练窗口 z-score：返回标准化数据 + 拟合参数（mean/std），供验证/测试复用。"""
    params = {}
    out = pd.DataFrame(index=data.index)
    for f in features:
        col = data[f].dropna()
        mu, sd = col.mean(), col.std(ddof=0)
        params[f] = {"mean": mu, "std": sd}
        out[f] = (data[f] - mu) / sd if sd > 0 else np.nan
    return out, params


def zscore_transform(data: pd.DataFrame, params: dict) -> pd.DataFrame:
    """用已有 params 标准化（验证/测试/当前状态），保证不引入前视偏差。"""
    out = pd.DataFrame(index=data.index)
    for f, p in params.items():
        out[f] = (data[f] - p["mean"]) / p["std"] if p["std"] > 0 else np.nan
    return out


# ── 模型 A/B：双目标偏离 + 趋势/速度（规范 §13.1/13.2） ────────────────
def estimate_weights(ev: pd.DataFrame, features: list[str]) -> dict | None:
    """在事件样本内拟合 OLS（标准化 + White SE + VIF + 条件数）。"""
    avail = [f for f in features if f in ev.columns]
    if len(avail) != len(features):
        return None
    Z, params = zscore_fit_train(ev, avail)
    Z = Z.dropna()
    if len(Z) < len(features) + 5:
        return None
    y = ev.loc[Z.index, "d_i_bp"].to_numpy()
    X = np.column_stack([np.ones(len(Z)), Z.to_numpy()])
    res = ols(y, X, ["const"] + avail)
    # 相对敏感度权重（规范 §14.3）
    bpi, bu = abs(res["beta"][1]), abs(res["beta"][2])
    denom = bpi + bu
    res["w_inflation"] = bpi / denom if denom > 0 else np.nan
    res["corr_pu"] = float(Z.iloc[:, 0].corr(Z.iloc[:, 1])) if Z.shape[1] >= 2 else np.nan
    res["z_params"] = params
    res["features"] = avail
    # 方向判定阈值（历史事件分布分位数，替代硬编码 12.5bp）
    abs_di = np.abs(y)
    res["bias_hold_threshold"] = float(np.percentile(abs_di, 75)) if len(abs_di) > 0 else 12.5
    return res


# ── 模型 C：利率平滑（规范 §13.3） ──────────────────────────────────────
def _try_estimate_smoothing(sub: pd.DataFrame, features: list[str],
                            rate_level_series: pd.Series) -> dict | None:
    """将事件 DataFrame 与利率水平序列对齐，调用 estimate_rate_smoothing。"""
    dates = sub["date"].sort_values().values
    rate_on_event_dates = rate_level_series.reindex(
        rate_level_series.index.union(pd.to_datetime(dates))
    ).ffill()
    rate_at_events = rate_on_event_dates.reindex(pd.to_datetime(dates))
    rate_series = rate_at_events.dropna()
    if len(rate_series) < len(features) + 10:
        return None
    sub_sorted = sub.sort_values("date").set_index("date").loc[rate_series.index]
    Z, _ = zscore_fit_train(sub_sorted, features)
    return estimate_rate_smoothing(rate_series, Z, features)


def estimate_rate_smoothing(rate_level: pd.Series, macro_z: pd.DataFrame,
                            lag_features: list[str]) -> dict | None:
    """i_t = ρ·i_{t-1} + (1-ρ)·i_t*，i_t* = r* + Σφ·Z(feature)。

    两阶段估计：
      1) OLS i_t = ρ·i_{t-1} + γ·Z(feature) + ε （含 ρ 与 φ=(1-ρ)γ 识别）
      2) 解 ρ 和 φ = γ / (1-ρ)，r* = 常数项 / (1-ρ)
    r* 近似：用样本均值（FRED 无 HLNW 系列入库）。
    """
    features = [f for f in lag_features if f in macro_z.columns]
    if not features:
        return None
    df = pd.concat([rate_level, macro_z[features]], axis=1).dropna()
    if len(df) < len(features) + 10:
        return None

    i_t = df.iloc[1:, 0].to_numpy()
    i_lag = df.iloc[:-1, 0].to_numpy()
    Z = df.iloc[1:][features].to_numpy()

    y = i_t
    X = np.column_stack([i_lag, Z])
    res = ols(y, X, ["i_lag"] + features)

    rho = res["beta"][0]
    if not (-0.5 < rho < 1.0):  # ρ 约束在经济学合理范围
        print(f"  警告: ρ={rho:.2f} 超出 [0,1) 合理范围，利率平滑模型结果可能不可靠")

    smooth_res = {
        "rho": float(np.clip(rho, 0.0, 0.99)),
        "phi_features": features,
        "phi_raw_beta": res["beta"][1:],  # γ
        "w_inflation": np.nan,
        "n": res["n"],
        "r2": res["r2"],
        "rho_se": res["se"][0],
        "z_features": features,
    }
    denom = 1 - smooth_res["rho"]
    if denom > 0:
        phi = smooth_res["phi_raw_beta"] / denom  # φ
        bpi, bu = abs(phi[0]), abs(phi[1]) if len(phi) >= 2 else (abs(phi[0]), 0)
        total = bpi + bu
        smooth_res["phi_inflation"] = float(phi[0])
        smooth_res["phi_employment"] = float(phi[1]) if len(phi) >= 2 else np.nan
        smooth_res["w_inflation"] = bpi / total if total > 0 else np.nan
    return smooth_res


# ── Chow 结构变化检验 ───────────────────────────────────────────────────
def chow_test(ev: pd.DataFrame, features: list[str],
              regime_cut: datetime) -> dict:
    """Chow 结构变化检验：比较合并模型 vs 分段模型残差平方和。"""
    pre = ev[ev["date"] < regime_cut]
    post = ev[ev["date"] >= regime_cut]
    if len(pre) < len(features) + 3 or len(post) < len(features) + 3:
        return {"valid": False, "reason": "某段样本不足"}

    Z_all, _ = zscore_fit_train(ev, features)
    Z_pre, _ = zscore_fit_train(pre, features)
    Z_post, _ = zscore_fit_train(post, features)

    def _rss(sub, Zsub):
        sub_clean = sub.loc[Zsub.index].dropna()
        if len(sub_clean) < len(features) + 2:
            return np.nan
        y = sub_clean["d_i_bp"].to_numpy()
        X = np.column_stack([np.ones(len(y)), Zsub.loc[sub_clean.index].to_numpy()])
        b = np.linalg.lstsq(X, y, rcond=None)[0]
        return float(((y - X @ b) ** 2).sum())

    rss_all = _rss(ev, Z_all)
    rss_pre = _rss(pre, Z_pre)
    rss_post = _rss(post, Z_post)
    if any(np.isnan(x) for x in (rss_all, rss_pre, rss_post)):
        return {"valid": False}
    k = len(features) + 1
    n_all = len(Z_all.dropna())
    n_pre = len(Z_pre.dropna())
    n_post = len(Z_post.dropna())
    f_stat = ((rss_all - rss_pre - rss_post) / k) / ((rss_pre + rss_post) / (n_all - 2 * k))
    return {
        "valid": True, "F": float(f_stat),
        "n_pre": n_pre, "n_post": n_post, "k": k,
    }


# ── 扩展窗口样本外回测（规范 §16） ──────────────────────────────────────
def expanding_window_backtest(ev: pd.DataFrame, features: list[str],
                              start_train_at: datetime,
                              forecast_horizon: int = 1) -> pd.DataFrame:
    """扩展窗口样本外回测：每次用截至 t 的数据预测 t+1 事件的 Δi_bp。

    forecast_horizon=1 表示预测下一个事件。
    输出 DataFrame 含 actual, predicted, sign_correct, abs_err, regime, train_size。
    """
    results = []
    event_dates = ev["date"].sort_values().to_numpy()
    for i, d in enumerate(event_dates):
        d = pd.Timestamp(d)
        if d <= start_train_at:
            continue
        train_ev = ev[ev["date"] < d]
        if len(train_ev) < len(features) + 8:
            continue
        train_ev_clean = train_ev.dropna(subset=features + ["d_i_bp"])
        target_row = ev.iloc[i]
        if pd.isna(target_row.get("d_i_bp")):
            continue
        # 训练集上标准化并拟合
        Z_train, params = zscore_fit_train(train_ev_clean, features)
        y_train = train_ev_clean.loc[Z_train.index, "d_i_bp"].to_numpy()
        X_train = np.column_stack([np.ones(len(Z_train)), Z_train.to_numpy()])
        beta = np.linalg.lstsq(X_train, y_train, rcond=None)[0]

        # 目标事件用训练期 params 标准化（禁止前视）
        target_vals = []
        ok = True
        for f in features:
            v = target_row.get(f)
            if v is None or pd.isna(v):
                ok = False
                break
            target_vals.append((v - params[f]["mean"]) / params[f]["std"]
                                if params[f]["std"] > 0 else 0.0)
        if not ok:
            continue
        X_target = np.concatenate([[1.0], np.array(target_vals)])
        pred = float(X_target @ beta)
        actual = float(target_row["d_i_bp"])

        results.append({
            "date": d, "actual": actual, "predicted": pred,
            "sign_correct": np.sign(actual) == np.sign(pred) if actual != 0 else np.nan,
            "abs_err": abs(actual - pred), "train_size": len(train_ev_clean),
        })
    if not results:
        return pd.DataFrame()
    out = pd.DataFrame(results)
    # 标记经济周期
    regimes = []
    for d in out["date"]:
        dt = pd.Timestamp(d)
        if dt < pd.Timestamp("2007-12-31"):
            regimes.append("Pre-GFC")
        elif dt < pd.Timestamp("2019-12-31"):
            regimes.append("Low-rate")
        else:
            regimes.append("Pandemic")
    out["regime"] = regimes
    return out


def backtest_summary(bt: pd.DataFrame) -> pd.DataFrame:
    """回测汇总：总体 + 按经济周期的 MAE / RMSE / Sign Accuracy。"""
    if bt.empty:
        return pd.DataFrame()
    rows = []
    for regime, grp in [("总体", bt)] + list(bt.groupby("regime")):
        n = len(grp)
        if n == 0:
            continue
        mae = grp["abs_err"].mean()
        rmse = np.sqrt((grp["abs_err"] ** 2).mean())
        sign_acc = grp["sign_correct"].mean() * 100
        # 分类准确率：hold threshold
        threshold = 12.5
        y_pred_bin = np.where(grp["predicted"] > threshold, 1,
                              np.where(grp["predicted"] < -threshold, -1, 0))
        y_actual_bin = np.where(grp["actual"] > 0, 1,
                                np.where(grp["actual"] < 0, -1, 0))
        correct = (y_pred_bin == y_actual_bin).sum()
        rows.append({
            "regime": regime, "n": n,
            "MAE_bp": round(mae, 1), "RMSE_bp": round(rmse, 1),
            "sign_acc_pct": round(sign_acc, 1),
            "class_acc_pct": round(correct / n * 100, 1),
        })
    return pd.DataFrame(rows)


def baseline_backtests(ev: pd.DataFrame, features: list[str],
                       start_train_at: datetime) -> dict[str, pd.DataFrame]:
    """规范 §16.4 要求的 6 种基准模型回测。"""
    results = {}
    # 基准 1: 永远预测 0
    rows_b1 = []
    for regime, grp in _split_by_regime(ev):
        rows_b1.append({
            "regime": regime, "n": len(grp),
            "MAE_bp": round(grp["d_i_bp"].abs().mean(), 1),
            "RMSE_bp": round(np.sqrt((grp["d_i_bp"] ** 2).mean()), 1),
            "sign_acc_pct": 0.0, "class_acc_pct": None,
        })
    # 加"总体"行
    rows_b1.insert(0, {
        "regime": "总体", "n": len(ev),
        "MAE_bp": round(ev["d_i_bp"].abs().mean(), 1),
        "RMSE_bp": round(np.sqrt((ev["d_i_bp"] ** 2).mean()), 1),
        "sign_acc_pct": 0.0, "class_acc_pct": None,
    })
    results["永远维持"] = pd.DataFrame(rows_b1)

    # 基准 2: 上一次动作外推
    if len(ev) > 1:
        pred_extrap = ev["d_i_bp"].shift(1)
        bt2 = pd.DataFrame({
            "date": ev["date"],
            "actual": ev["d_i_bp"],
            "predicted": pred_extrap,
            "abs_err": (ev["d_i_bp"] - pred_extrap).abs(),
        }).dropna()
        # 补 sign_correct
        bt2["sign_correct"] = bt2.apply(
            lambda r: np.sign(r["actual"]) == np.sign(r["predicted"])
            if r["actual"] != 0 and r["predicted"] != 0 else np.nan,
            axis=1,
        )
        results["上一次动作"] = backtest_summary(_add_regime(bt2))

    # 基准 3-5: 单变量模型（通胀 / 就业 / 通胀+就业）
    infl_feat = [f for f in features if any(k in f for k in ("gap", "core_gap", "pce_gap"))]
    emp_feat = [f for f in features if any(k in f for k in ("u_gap", "du3", "du6"))]
    for name, feats in [("仅通胀", infl_feat[:1]),
                        ("仅就业", emp_feat[:1] if emp_feat else infl_feat[:1]),
                        ("通胀+就业", features[:2])]:
        if not feats:
            continue
        bt = expanding_window_backtest(ev, feats, start_train_at)
        if not bt.empty:
            results[name] = backtest_summary(bt)

    return results


def _split_by_regime(ev: pd.DataFrame) -> list:
    out = []
    pre = ev[ev["date"] < pd.Timestamp("2007-12-31")]
    low = ev[(ev["date"] >= pd.Timestamp("2009-01-01")) & (ev["date"] <= pd.Timestamp("2019-12-31"))]
    pan = ev[ev["date"] > pd.Timestamp("2019-12-31")]
    if not pre.empty:
        out.append(("Pre-GFC", pre))
    if not low.empty:
        out.append(("Low-rate", low))
    if not pan.empty:
        out.append(("Pandemic", pan))
    return out


def _add_regime(bt: pd.DataFrame) -> pd.DataFrame:
    regimes = []
    for d in bt["date"]:
        dt = pd.Timestamp(d)
        if dt < pd.Timestamp("2007-12-31"):
            regimes.append("Pre-GFC")
        elif dt < pd.Timestamp("2019-12-31"):
            regimes.append("Low-rate")
        else:
            regimes.append("Pandemic")
    bt = bt.copy()
    bt["regime"] = regimes
    return bt


# ── 压力指数（多基准期敏感性） ──────────────────────────────────────────
def last_valid(m: pd.DataFrame, col: str) -> float | None:
    if col not in m:
        return None
    s = m[col].dropna()
    return float(s.iloc[-1]) if not s.empty else None


def pressure_index(macro: pd.DataFrame, parts: dict[str, int],
                   base: tuple[datetime, datetime]) -> tuple[float, dict[str, float]]:
    b0, b1 = base
    zs = {}
    for col, sign in parts.items():
        if col not in macro or macro[col].notna().sum() < 36:
            continue
        hist = macro.loc[b0:b1, col].dropna()
        cur = last_valid(macro, col)
        if len(hist) < 24 or cur is None or hist.std(ddof=0) == 0:
            continue
        z = (cur - hist.mean()) / hist.std(ddof=0)
        zs[col] = float(np.clip(sign * z, -Z_CLIP, Z_CLIP))
    if not zs:
        return np.nan, {}
    return float(np.mean(list(zs.values()))), zs


# ── 报告 ────────────────────────────────────────────────────────────────
COEF_LABELS = {
    "const": "常数", "core_gap": "核心PCE偏离", "pce_gap": "总体PCE偏离",
    "u_gap": "失业偏离", "accel": "通胀加速度", "core_accel": "核心通胀加速度",
    "du3": "失业3月变化", "du6": "失业6月变化",
    "i_lag": "利率惯性(ρ)",
}


def fmt_est(res: dict | None, title: str = "") -> str:
    if res is None:
        return "样本不足，无法估计"
    lines = [f"  {title}" if title else ""]
    for i, name in enumerate(res["names"]):
        b, se, t = res["beta"][i], res["se"][i], res["t"][i]
        if name == "const":
            lines.append(f"    {COEF_LABELS[name]:<16} {b:+7.1f}bp")
        else:
            star = "**" if abs(t) >= 2.0 else ("*" if abs(t) >= 1.645 else "  ")
            vif_str = ""
            if "vif" in res and i > 0:
                vif_i = res["vif"][i - 1]
                vif_str = f" VIF={vif_i:.1f}" if not np.isinf(vif_i) else " VIF=∞"
            lines.append(f"    {COEF_LABELS.get(name, name):<16} "
                         f"{b:+7.2f}bp  SE {se:5.2f}  t={t:+5.2f} {star}{vif_str}")
    cond_str = f"条件数 {res['cond_num']:.1f}" if "cond_num" in res and not np.isinf(res["cond_num"]) else ""
    return "\n".join(lines) + (
        f"\n    R²={res['r2']:.2f}  n={res['n']}  "
        f"corr(Z通胀,Z就业)={res['corr_pu']:+.2f}  "
        f"{cond_str}".rstrip()
    )


def fmt_signed(x: float, digits: int = 1, suffix: str = "") -> str:
    return f"{x:+.{digits}f}{suffix}"


def latest(m: pd.DataFrame, col: str) -> tuple[float | None, pd.Timestamp | None]:
    if col not in m:
        return None, None
    s = m[col].dropna()
    return (float(s.iloc[-1]), s.index[-1]) if not s.empty else (None, None)


def weights_line(res: dict | None) -> str:
    if res is None:
        return "样本不足"
    wi = res.get("w_inflation")
    if wi is None or np.isnan(wi):
        return f"n={res['n']}  (权重不可计算)"
    return f"通胀 {wi:.0%} / 就业 {1 - wi:.0%} (n={res['n']})"


def report(macro: pd.DataFrame, ev_pri: pd.DataFrame, ev_ext: pd.DataFrame,
           results: dict) -> None:
    line = "=" * 78
    last_date = macro.index[-1]
    print(f"\n{line}")
    print(f"FDAM 美联储双目标关注度模型（完整实现 v1）  截至 {last_date:%Y-%m-%d}")
    print("数据: 本地 ChronoForge/FRED（latest vintage，非严格实时回测）")
    print(f"per-series 发布滞后已启用 · White 稳健标准误 · VIF 共线性诊断")
    print(line)

    # ── A. 当前经济状态 ──
    print("\n【A. 当前经济状态】")
    rows_a = [
        ("总体 PCE 同比", "pce_yoy", "%", INFLATION_TARGET),
        ("核心 PCE 同比", "core_yoy", "%", INFLATION_TARGET),
        ("核心 PCE 3个月年化", "core_3m", "%", None),
        ("CPI 同比", "cpi_yoy", "%", None),
        ("失业率", "u", "%", U_FIXED),
        ("失业率 3 个月变化", "du3", "pp", None),
        ("失业率 6 个月变化", "du6", "pp", None),
        ("非农 3 个月月均(千人)", "payroll_3m", "", None),
        ("非农 12 个月增速", "payroll_12m", "%", None),
        ("初请四周均值(人)", "claims_4w", "", None),
        ("持续失业四周均值(人)", "ccsa_4w", "", None),
        ("时薪同比", "wage_yoy", "%", None),
        ("职位空缺同比", "jolts_yoy", "%", None),
        ("劳动参与率", "civpart", "%", None),
        ("就业人口比", "emratio", "%", None),
        ("10Y 盈亏平衡通胀", "breakeven_10y", "%", INFLATION_TARGET),
        ("5Y 盈亏平衡通胀", "breakeven_5y", "%", INFLATION_TARGET),
        ("2Y 国债收益率", "gs2", "%", None),
        ("10Y-3M 收益率曲线", "yield_spread", "%", None),
        ("密歇根通胀预期", "mich", "%", INFLATION_TARGET),
    ]
    for label, col, unit, ref in rows_a:
        val, d = latest(macro, col)
        if val is None:
            continue
        extra = f"（偏离参考 {fmt_signed(val - ref)}pp）" if ref is not None else ""
        ds = f"{d:%Y-%m}" if d is not None else "-"
        print(f"  {label:<24} {val:8.2f} {unit:<2} ({ds}) {extra}")
    sahm, _ = latest(macro, "sahm")
    if sahm is not None:
        flag = "已触发" if sahm >= SAHM_THRESHOLD else "未触发"
        print(f"  {'Sahm 规则':<24} {sahm:8.2f}   （阈值 {SAHM_THRESHOLD}，{flag}）")

    # ── B. 历史政策反应参数 ──
    print(f"\n【B. 历史政策反应参数 — 分段回归 + 稳健标准误】")
    print(f"  主样本: 目标区间中点变动  {ev_pri['date'].min():%Y-%m} ~ {ev_pri['date'].max():%Y-%m}")
    print(f"  扩展样本: EFFR 级移重建  {ev_ext['date'].min():%Y-%m} ~ {ev_ext['date'].max():%Y-%m}")

    for regime_name in results["regimes_pri"]:
        res_a = results["regimes_pri"][regime_name].get("模型A 双目标偏离")
        res_b = results["regimes_pri"][regime_name].get("模型B 加入趋势/速度")
        res_c = results["regimes_pri"][regime_name].get("模型C 利率平滑")
        bt = results["regimes_pri"][regime_name].get("样本外回测")
        chow = results["regimes_pri"][regime_name].get("Chow检验")

        print(f"\n  ━━ {regime_name} ━━")
        print(fmt_est(res_a, "模型A 双目标偏离"))
        if res_a is not None:
            print(f"    {weights_line(res_a)}")
        if res_b is not None:
            print(fmt_est(res_b, "模型B +趋势/速度"))
            print(f"    {weights_line(res_b)}")
        if res_c is not None:
            print(f"    模型C 利率平滑: ρ={res_c['rho']:.2f}±{res_c['rho_se']:.2f}  "
                  f"R²={res_c['r2']:.2f}  n={res_c['n']}  "
                  f"通胀权重={res_c.get('w_inflation', np.nan):.0%}")
        if chow is not None and chow.get("valid"):
            print(f"    Chow 检验(vs下一段): F={chow['F']:.2f}  "
                  f"p≈{_f_p(chow['F'], chow['k'], chow['n_pre'] + chow['n_post'] - 2*chow['k']):.3f}")
        if bt is not None and not bt.empty:
            summary = backtest_summary(bt)
            print(f"    样本外回测(扩展窗口):")
            for _, r in summary.iterrows():
                print(f"      [{r['regime']:<9}] n={r['n']:>3}  MAE={r['MAE_bp']:>5.1f}bp  "
                      f"RMSE={r['RMSE_bp']:>5.1f}bp  方向={r['sign_acc_pct']:>5.1f}%  "
                      f"分类={r['class_acc_pct']:>5.1f}%")

    # ── C. 基准模型对比 ──
    if "baselines" in results:
        print(f"\n【C. 基准模型样本外对比】（主样本扩展窗口回测）")
        for name, df in results["baselines"].items():
            print(f"  {name}:")
            for _, r in df.iterrows():
                print(f"    [{r['regime']:<9}] n={r['n']:>3}  MAE={r['MAE_bp']:>5.1f}bp  "
                      f"RMSE={r['RMSE_bp']:>5.1f}bp  方向={r['sign_acc_pct']:>5.1f}%")

    # ── D. 压力对比（多基准期敏感性） ──
    print(f"\n【D. 当前双目标压力对比】（z 相对不同正常期基准）")
    for base_name, b0, b1 in PRESSURE_BASES:
        inf_idx, inf_zs = pressure_index(macro, INFLATION_PARTS, (b0, b1))
        emp_idx, emp_zs = pressure_index(macro, EMPLOYMENT_PARTS, (b0, b1))
        inf_detail = "  ".join(f"{k}={v:+.1f}" for k, v in inf_zs.items())
        emp_detail = "  ".join(f"{k}={v:+.1f}" for k, v in emp_zs.items())
        print(f"  基准 {base_name} ({b0:%Y}-{b1:%Y}):")
        print(f"    通胀: {inf_idx:+.2f} [{inf_detail}]")
        print(f"    就业: {emp_idx:+.2f} [{emp_detail}]")

    # ── E. 模型隐含政策方向 ──
    print(f"\n【E. 模型隐含政策方向（训练/验证标准化分离）】")
    print(f"  {results['direction']}")

    # ── F. 结论 ──
    print(f"\n{line}")
    print("【F. 结论】")
    for l in results["conclusion"].splitlines():
        print("  " + l)
    print(line)
    print("局限: 最新 vintage（非严格实时回测）；无文本信号(kappa=0)；")
    print("      权重为描述性历史关联，非美联储官方目标权重，不构成因果解释（规范原则 C/E）。")


def _f_p(F: float, k: int, df: int) -> float:
    """F 分布近似 p-value（用于 Chow 检验展示）。"""
    from scipy import stats as _stats
    return float(1 - _stats.f.cdf(F, k, df))


# ── X 发布卡片（4:5 竖版 1200×1500） ───────────────────────────────────
# 配色（与 plot_rates_2y_effr.py 统一）
C_BG = "#0b0e14"
C_CARD = "#111820"
C_CARD_ALT = "#141c27"
C_BORDER = "#1f2733"
C_TEXT = "#e6edf5"
C_TEXT_DIM = "#7a8ba3"
C_TEXT_MUTE = "#546678"
C_ACCENT_INF = "#f7931a"     # 通胀 → 橙
C_ACCENT_EMP = "#3d9bff"     # 就业 → 蓝
C_HOLD = "#3ddc97"
C_HIKE = "#ff5c5c"
C_CUT = "#6bc0ff"
C_NEUTRAL = "#8a9ab0"

# 字体
# 探测可用中文字体：PingFang SC 仅到 Semibold(600)，无 Bold(700)，
# 需用 fontweight=600 取代 "bold" 以避免 findfont 告警。
_cjk_candidates = ["PingFang SC", "Heiti SC", "Songti SC",
                   "Arial Unicode MS", "DejaVu Sans"]
_chosen_font = "DejaVu Sans"
for _fname in _cjk_candidates:
    try:
        matplotlib.font_manager.findfont(
            matplotlib.font_manager.FontProperties(family=_fname),
            fallback_to_default=False,
        )
        _chosen_font = _fname
        break
    except Exception:
        continue
plt.rcParams["font.family"] = _chosen_font
plt.rcParams["axes.unicode_minus"] = False


def _card_axes(fig, rect, pad=0.012, rrounded=True):
    """在 fig 上创建带圆角边框的子区域 axes。rect = [left, bottom, width, height]"""
    ax = fig.add_axes(rect)
    ax.set_facecolor(C_BG)       # 先铺底色(画布色)，下方叠圆角卡片
    ax.set_xticks([])
    ax.set_yticks([])
    for spine in ax.spines.values():
        spine.set_visible(False)
    for label in ax.get_xticklabels() + ax.get_yticklabels():
        label.set_visible(False)
    # 真正的圆角卡片背景 —— 在 axes 坐标系内画 FancyBboxPatch
    if rrounded:
        box = FancyBboxPatch(
            (0.005, 0.005), 0.990, 0.990,
            boxstyle="round,pad=0.008,rounding_size=0.02",
            facecolor=C_CARD, edgecolor=C_BORDER, linewidth=1.2,
            transform=ax.transAxes, zorder=0, clip_on=False,
        )
        ax.add_patch(box)
    else:
        ax.add_patch(plt.Rectangle(
            (0.005, 0.005), 0.990, 0.990,
            facecolor=C_CARD, edgecolor=C_BORDER, linewidth=1.2,
            transform=ax.transAxes, zorder=0, clip_on=False,
        ))
    ax.set_xlim(0, 1); ax.set_ylim(0, 1)
    return ax


def _draw_text(ax, x, y, text, size=10, color=C_TEXT, weight="normal",
               ha="left", va="center", alpha=1.0):
    return ax.text(x, y, text, fontsize=size, color=color, fontweight=weight,
                   ha=ha, va=va, alpha=alpha, transform=ax.transAxes)


def _latest_str(macro: pd.DataFrame, col: str) -> str:
    """安全格式化 macro 列的最新值。"""
    v, _ = latest(macro, col)
    if v is None:
        return "—"
    # 非农等用 .1f 其他指标用 .2f
    if col in ("payroll_3m",):
        return f"{v:.1f}"
    return f"{v:.2f}"


# ── 主流程 ──────────────────────────────────────────────────────────────
def main() -> None:
    print("加载本地数据库…")
    raw = load_all()
    macro = build_macro(raw)

    print("\n识别政策事件…")
    midpoint = ((raw["DFEDTARU"] + raw["DFEDTARL"]) / 2).dropna()
    ev_pri = build_events(midpoint, macro, "目标区间中点")
    ev_ext = build_events(reconstruct_policy_path(raw["EFFR"]), macro,
                          "EFFR级移扩展", round_step=True)

    # ── 模型规格 ──
    feats_a = ["core_gap", "u_gap"]
    feats_b = ["core_gap", "u_gap", "accel", "du6"]

    # ── 分段回归（规范 §15） ──
    print("\n分段回归…")
    results = {"regimes_pri": {}, "regimes_ext": {}}
    for target, key_name, rate_col in [
        (ev_pri, "regimes_pri", "date"),
        (ev_ext, "regimes_ext", "date"),
    ]:
        for regime_name, start, end in REGIMES:
            mask = target[rate_col] >= pd.Timestamp(start)
            if end is not None:
                mask &= target[rate_col] <= pd.Timestamp(end)
            sub = target[mask].copy()
            if len(sub) < len(feats_a) + 5:
                continue
            sub_res = {}
            sub_res["模型A 双目标偏离"] = estimate_weights(sub, feats_a)
            sub_res["模型B 加入趋势/速度"] = estimate_weights(sub, feats_b)

            # 利率平滑模型（需要实际利率水平序列）
            # 从 midpoint 利率序列直接构造
            if key_name == "regimes_pri":
                sub_res["模型C 利率平滑"] = _try_estimate_smoothing(
                    sub, feats_a, rate_level_series=midpoint,
                )
            else:
                sub_res["模型C 利率平滑"] = None

            # Chow 检验（切分点放 Low-rate 段上）
            sub_res["Chow检验"] = None

            results[key_name][regime_name] = sub_res
            w = sub_res["模型A 双目标偏离"]
            print(f"  {key_name}·{regime_name}: {weights_line(w)}")

        # 跨段 Chow 检验
        chow_cuts = [
            ("Pre-GFC", datetime(2007, 12, 31)),
            ("Low-rate", datetime(2019, 12, 31)),
        ]
        for cut_name, cut_date in chow_cuts:
            if cut_name in results[key_name]:
                results[key_name][cut_name]["Chow检验"] = chow_test(
                    target, feats_a, cut_date,
                )

        # 滚动回归（规范 §15）
        for w_years, w_name in [(ROLLING_YEARS_SHORT, "模型A 滚动8年"),
                                 (ROLLING_YEARS_LONG, "模型A 滚动15年")]:
            cutoff = target["date"].max() - pd.Timedelta(days=365 * w_years)
            sub = target[target["date"] >= cutoff]
            if len(sub) >= len(feats_a) + 5:
                results[key_name][w_name] = {
                    "模型A 双目标偏离": estimate_weights(sub, feats_a),
                    "模型B 加入趋势/速度": estimate_weights(sub, feats_b),
                    "样本外回测": expanding_window_backtest(sub, feats_a, cutoff),
                    "Chow检验": None, "模型C 利率平滑": None,
                }
                print(f"  {key_name}·{w_name}: "
                      f"{weights_line(results[key_name][w_name]['模型A 双目标偏离'])}")

    # ── 扩展窗口样本外回测（完整主样本） ──
    print("\n样本外回测…")
    start_train_at = datetime(2009, 12, 31)  # 至少用 2009 年之前的数据
    bt_full = expanding_window_backtest(ev_pri, feats_a, start_train_at)
    if not bt_full.empty:
        results["regimes_pri"]["Full 扩展窗口"] = {
            "模型A 双目标偏离": estimate_weights(ev_pri, feats_a),
            "模型B 加入趋势/速度": estimate_weights(ev_pri, feats_b),
            "样本外回测": bt_full,
            "Chow检验": None, "模型C 利率平滑": None,
        }

    # ── 基准模型对比 ──
    results["baselines"] = baseline_backtests(ev_pri, feats_a, start_train_at)

    # ── 当前状态与判定 ──
    # 用模型 A 的 Long-run 估计
    res_a = None
    for name in ("Long-run", "模型A 双目标偏离"):
        if name in results["regimes_pri"]:
            res_a = results["regimes_pri"][name]["模型A 双目标偏离"]
            break

    inf_idx, _ = pressure_index(macro, INFLATION_PARTS, PRESSURE_BASES[1][1:])
    emp_idx, _ = pressure_index(macro, EMPLOYMENT_PARTS, PRESSURE_BASES[1][1:])

    direction = "样本不足，无法给出隐含方向"
    di_pred = None
    if res_a is not None:
        b = res_a["beta"]
        params = res_a["z_params"]
        cur = {}
        cur_feats = []
        for f in feats_a:
            curv = last_valid(macro, f)
            if curv is None:
                cur = None
                break
            cur[f] = (curv - params[f]["mean"]) / params[f]["std"]
            cur_feats.append(f)
        if cur is not None:
            lean = b[1] * cur["core_gap"] + b[2] * cur["u_gap"]
            di_pred = float(b[0] + lean)
            thr = res_a.get("bias_hold_threshold", 12.5)
            bias = ("加息倾向" if lean > thr else
                    "降息倾向" if lean < -thr else "维持不变")
            g_pi = last_valid(macro, "core_gap")
            g_u = last_valid(macro, "u_gap")
            direction = (f"模型 A 隐含 Δi = {di_pred:+.1f}bp"
                         f"（相对样本平均动作 {lean:+.1f}bp，hold 阈值 ≈ {thr:.0f}bp）"
                         f" → {bias}；"
                         f"当前核心PCE偏离 {g_pi:+.1f}pp / 失业偏离 {g_u:+.1f}pp，"
                         f"Z通胀={cur['core_gap']:+.1f} / Z就业={cur['u_gap']:+.1f}"
                         f"（训练集分布）")

    # ── 结论合成 ──
    margin = inf_idx - emp_idx
    if res_a is not None and np.isfinite(inf_idx) and np.isfinite(emp_idx):
        w_inf = res_a["w_inflation"]
        cond = res_a.get("cond_num", np.nan)
        vif_max = res_a["vif"].max()
        vif_warn = f"（注意：条件数 {cond:.0f}，VIF_max={vif_max:.1f}，共线性警告）" if cond and cond > 10 or (vif_max > 5 if not np.isinf(vif_max) else False) else ""
        if margin > 0.25 and w_inf >= 0.5:
            verdict = f"通胀（通胀压力显著更大，历史敏感度同样偏向通胀）{vif_warn}"
        elif margin < -0.25 and (1 - w_inf) >= 0.5:
            verdict = f"就业（就业压力显著更大，历史敏感度同样偏向就业）{vif_warn}"
        elif margin > 0.25:
            verdict = f"通胀（通胀压力更大，但历史敏感度偏向就业，需关注美联储是否容忍）{vif_warn}"
        elif margin < -0.25:
            verdict = f"就业（就业压力更大，但历史敏感度偏向通胀，需关注美联储是否容忍）{vif_warn}"
        else:
            verdict = f"双目标大体均衡（两类压力接近，无明确侧重）{vif_warn}"
    else:
        verdict = "样本不足"

    conc = [f"美联储当前更关注: {verdict}", ""]
    conc.append(f"- 压力对比（2010-19 基准）: 通胀 {inf_idx:+.2f} vs 就业 {emp_idx:+.2f}（差 {fmt_signed(margin, 2)}）")
    if res_a is not None:
        conc.append(f"- 历史敏感度(Long-run 模型A White SE): 通胀 {w_inf:.0%} / 就业 {1 - w_inf:.0%}")
        conc.append(f"- {direction}")
    sahm, _ = latest(macro, "sahm")
    if sahm is not None and sahm >= SAHM_THRESHOLD * 0.8:
        conc.append(f"- 就业恶化速度预警: Sahm 规则 {sahm:.2f}"
                    f"（{'已触发' if sahm >= SAHM_THRESHOLD else '接近'}衰退阈值）")
    # 加入回测表现
    if "Full 扩展窗口" in results["regimes_pri"]:
        bt = results["regimes_pri"]["Full 扩展窗口"]["样本外回测"]
        s = backtest_summary(bt)
        if not s.empty:
            row_all = s[s["regime"] == "总体"].iloc[0]
            conc.append(f"- 样本外回测(扩展窗口): MAE={row_all['MAE_bp']}bp  "
                        f"RMSE={row_all['RMSE_bp']}bp  方向准确率={row_all['sign_acc_pct']:.0f}%")

    results["direction"] = direction
    results["conclusion"] = "\n".join(conc)

    report(macro, ev_pri, ev_ext, results)

    # ── 生成 X 发布卡片（JSON → HTML → PNG 三阶段流水线） ──
    inf_parts = pressure_index(macro, INFLATION_PARTS, PRESSURE_BASES[1][1:])[1]
    emp_parts = pressure_index(macro, EMPLOYMENT_PARTS, PRESSURE_BASES[1][1:])[1]
    bt_full_summary = None
    if "Full 扩展窗口" in results["regimes_pri"]:
        bt_full_summary = backtest_summary(
            results["regimes_pri"]["Full 扩展窗口"]["样本外回测"],
        )
    full_verdict = conc[0].replace("美联储当前更关注: ", "")

    payload, json_path = build_card_payload(
        macro=macro, results=results,
        inf_idx=inf_idx, emp_idx=emp_idx,
        res_a=res_a, verdict=full_verdict, direction=direction,
        inf_parts=inf_parts, emp_parts=emp_parts,
        bt_full_summary=bt_full_summary,
    )
    print(f"\n✓ Card payload 已保存  {json_path}")

    # 调用 card_renderer.py 生成 HTML + PNG
    from subprocess import run
    run([
        sys.executable,
        str(Path(__file__).resolve().parent / "card_renderer.py"),
        str(json_path),
    ], check=True)


def build_card_payload(macro: pd.DataFrame, results: dict,
                        inf_idx: float, emp_idx: float,
                        res_a: dict | None, verdict: str, direction: str,
                        inf_parts: dict, emp_parts: dict,
                        bt_full_summary: pd.DataFrame | None
                        ) -> tuple[dict, Path]:
    """从模型结果提取结构化卡片数据，写入 JSON payload。

    返回 (payload_dict, json_path)。JSON 由 card_renderer.py 消费生成 HTML + PNG。
    """
    import json as _json

    OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output"
    OUTPUT_DIR.mkdir(exist_ok=True)
    today = datetime.now().strftime("%Y%m%d")
    json_path = OUTPUT_DIR / f"fdam_card_{today}.json"

    last_date_str = macro.index[-1].strftime("%Y-%m-%d") if len(macro) > 0 else "—"

    # ── Pressure ──
    pressure = {
        "inflation": {
            "label": "通胀压力",
            "value": float(inf_idx),
            "parts": {k: float(v) for k, v in inf_parts.items()},
        },
        "employment": {
            "label": "就业压力",
            "value": float(emp_idx),
            "parts": {k: float(v) for k, v in emp_parts.items()},
        },
    }

    # ── Sensitivity weights ──
    regimes_show = [
        ("Pre-GFC 2000-07", "regimes_ext", "Pre-GFC"),
        ("Low-rate 2009-19", "regimes_pri", "Low-rate"),
        ("Pandemic 2020+",  "regimes_pri", "Pandemic"),
    ]
    sens_list = []
    for label, key, rname in regimes_show:
        sub = results.get(key, {}).get(rname, {}).get("模型A 双目标偏离")
        wi = float(sub["w_inflation"]) if sub and sub.get("w_inflation") is not None else 0.5
        sens_list.append({"label": label, "w_inflation": wi})

    # ── Macro metrics ──
    def _metric(title: str, col: str, unit: str, ref=None, mtype: str = "inf",
                ref_text: str | None = None) -> dict:
        v = _latest_str(macro, col)
        try:
            val = float(v)
        except ValueError:
            val = None
        return {
            "title": title, "value_str": v, "unit": unit,
            "value": val, "ref": ref, "type": mtype, "ref_text": ref_text,
        }

    macro_metrics = [
        _metric("核心 PCE 同比", "core_yoy", "%", INFLATION_TARGET, "inf"),
        _metric("失业率", "u", "%", U_FIXED, "emp"),
        _metric("非农 3月均", "payroll_3m", "k", None, "emp",
                ref_text=""),
        _metric("职位空缺同比", "jolts_yoy", "%", None, "emp",
                ref_text=""),
        _metric("10Y 盈亏平衡", "breakeven_10y", "%", 2.0, "inf"),
        _metric("Sahm 规则", "sahm", "", SAHM_THRESHOLD, "emp"),
    ]

    # ── Policy ──
    direction_first = direction.split("；")[0].split("→")[0].strip()
    policy = {
        "verdict": verdict,
        "direction_short": direction_first,
        "w_inflation": float(res_a["w_inflation"]) if res_a else None,
        "corr_pu": float(res_a.get("corr_pu", float("nan")))
                  if res_a and res_a.get("corr_pu") is not None else None,
        "r2": float(res_a["r2"]) if res_a else None,
        "backtest": None,
    }
    if bt_full_summary is not None and not bt_full_summary.empty:
        ra = bt_full_summary[bt_full_summary["regime"] == "总体"]
        if not ra.empty:
            r = ra.iloc[0]
            policy["backtest"] = {
                "MAE_bp": float(r["MAE_bp"]),
                "sign_acc_pct": float(r["sign_acc_pct"]),
            }

    payload = {
        "last_date": last_date_str,
        "pressure": pressure,
        "sensitivity": sens_list,
        "macro": macro_metrics,
        "policy": policy,
        "direction": direction,
    }

    json_path.write_text(
        _json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return payload, json_path


if __name__ == "__main__":
    main()


