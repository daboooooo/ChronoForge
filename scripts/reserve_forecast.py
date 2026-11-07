#!/usr/bin/env python3
"""准备金结构预测模型 reserve-structural-0.1.2（周频，h ∈ {1,2,4} 周）。

口径（统一周三水平，USD 百万美元）：
- 观测量：WRBWFRBL（准备金余额，周三）、WDTGAL（TGA，周三）、
  WCICL（流通中货币，周三）、WLRRAL（逆回购，周三）。
- 结构恒等式（H.4.1 Table 1）：
    WLTLECL = WLFN + WLRRAL + WLDLCL + WLDACLC + WLAD + TCCF   ← 唯一独立可校验项
    WLDLCL  = WRBWFRBL + WDTGAL + TERMT + 外国官方存款(ΣD*WLFOL) + 其他存款(WLO)
    WALCL   = WSHOSHO + 非SOMA资产 + 其他资产(残差桶)
    capital = WALCL − WLTLECL
  后三条不是独立校验：`other_dep`/`other_assets` 是残差桶、`capital` 是差分，
  代回后对任意数据恒成立（M-N1 已删掉这两条伪校验，代数证明见
  `identity_checks` docstring）。`check` 只对第一条做逐周容差判定。

预测方法（逐项，全部锚定在 origin 的观测水平上，不重新估水平）：
- SOMA 证券（WSHOSHO）：持有不变。无前瞻购债/缩表计划输入，短horizon 内
  持仓变动主要发生在结算日；按最近观察值持平（结构性科目）。
- 非 SOMA 资产（未摊销溢价/折价、贷款、回购、互换、外币、黄金、房产）：
  持有不变（结构性）。
- 其他资产（残差桶）：持有不变。该桶为“其他资产”不可拆项，单独标注，
  不再拆分子科目（数据源无对应明细序列）。
- 流通中货币（WCICL）：持有不变。0.1.0 曾用同 ISO 周季节中位数，
  消融回测证明该调整为负增量（去掉后 MAE 反而下降），0.1.1 移除。
  WLFN = WCICL + 货币口径差（持有不变）。
- TGA（WDTGAL）：季节中位数（同上）。
- 逆回购（WLRRAL）：持有不变（0 下限）。
- 其他负债与资本（capital + TERMT + 外国官方存款 + 其他存款 + WLDACLC
  + WLAD + TCCF）：持有不变。

误差带：对每个 horizon 做扩张窗口回测（每个 origin 仅用该时点之前的数据），
取 h 步外推残差的经验分位数（5%/95%）作为 90% 带。口径是**逐 horizon 边际**
（每个 h 单独取分位，未对多 horizon 做同时覆盖校正），但残差来自完整模型的
外推误差，不是只继承 TGA 模型误差。回测同时报告 MAE/RMSE。

模型定位：只回答短期（1/2/4 周）准备金路径。回测确认的增量来源是 TGA
季节项，其余科目按结构性持平处理；这不是完整的逐项结构预测，也不外推
资产负债表规模。

前视偏差控制（PIT，按发布时刻截断）：
- 每个观测的“可得时刻”取 max(observation_time, release_time, revision_time)：
  FRED 的 release_time 来自 ALFRED vintage 日（H.4.1 为观测日次日周四
  23:59:59）；NY Fed SOMA 的 revision_time 即落库发布日（同周四 23:59:59）。
- origin 定义为周三收盘（当日 23:59:59 截止）。该时点当周 H.4.1/SOMA 尚未
  发布，因此模型只能看到上周三的观测；周三当期数据不得进入预测。
- 回测与生产使用同一套截断：每个 origin 只用可得时刻 <= origin 的数据；
  实际值只在事后评估时读取。
- 本仓库 canonical 每个观测仅保留一个版本（最新 vintage）。若某值在 origin
  之后才被修订，按 revision_time 保守地视为该时点不可用，因此回测为
  “当时可得 + 最终修订值”口径，不使用任何未来 vintage。

用法：
    uv run python scripts/reserve_forecast.py check
    uv run python scripts/reserve_forecast.py run [--dry-run]
"""

from __future__ import annotations

# QueryService 行数上限：全历史面板读取放宽（必须在 Settings.load() 前设置）。
import os

os.environ.setdefault("CHRONOFORGE_QUERY_MAX_ROWS", "500000")

import argparse
import hashlib
import json
import shutil
import uuid
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from chronoforge.cli._wiring import open_meta, open_query_service
from chronoforge.config.settings import Settings

MODEL_VERSION = "reserve-structural-0.1.2"
_REPO_ROOT = Path(__file__).resolve().parents[1]
_START = date(2002, 12, 18)
HORIZONS: tuple[int, ...] = (1, 2, 4)
_SCENARIO = "base"
_MIN_SEASONAL_SAMPLES = 5
_BAND_WINDOW_YEARS = 5
_QUANTILES = (0.05, 0.95)

# 面板序列（全部为周三水平值，FRED H.4.1 / USD 百万美元）
_PANEL_SERIES: tuple[str, ...] = (
    "WALCL", "WSHOSHO", "WUPSHO", "WUDSHO", "WLCFLL", "WORAL", "SWPT",
    "WFCDA", "WGCAL", "WABPL",
    "WLTLECL", "WLFN", "WCICL", "WLRRAL", "WLDLCL", "WRBWFRBL", "WDTGAL",
    "TERMT", "WLAD", "WLDACLC", "H41RESH4ENWW",
    *[f"D{i}WLFOL" for i in range(1, 13)],
)
_NON_SOMA_ASSETS: tuple[str, ...] = (
    "WUPSHO", "WUDSHO", "WLCFLL", "WORAL", "SWPT", "WFCDA", "WGCAL", "WABPL",
)
_DISTRICTS: tuple[str, ...] = tuple(f"D{i}WLFOL" for i in range(1, 13))
# 其他负债桶：资本 + 定期存款 + 外国官方存款 + 其他存款 + 递延现金项 + 其他负债 + TCCF
_OTHER_LIAB_COLUMNS: tuple[str, ...] = (
    "capital", "TERMT", "foreign_dep", "other_dep", "WLDACLC", "WLAD", "TCCF",
)
# SOMA 9 分项（NY Fed soma_summary；USD 原值）→ 仅用于结构展示与交叉校验
_SOMA_ENTITIES: tuple[str, ...] = (
    "soma_total", "soma_bills", "soma_notesbonds", "soma_frn", "soma_tips",
    "soma_tips_inflation", "soma_mbs", "soma_cmbs", "soma_agencies",
)

_COMPONENT_METHODS: dict[str, str] = {
    "soma_securities": "HOLD_STRUCTURAL",
    "non_soma_assets": "HOLD_STRUCTURAL",
    "other_assets_residual": "HOLD_RESIDUAL",
    "currency": "HOLD",
    "reverse_repo": "HOLD",
    "tga": "SEASONAL_MEDIAN",
    "other_liabilities": "HOLD",
    "reserve_balances": "IDENTITY",
}


# ── 数据加载 ──────────────────────────────────────────────────────────

_PIT_COLUMNS: tuple[str, ...] = (
    "observation_time", "value", "release_time", "revision_time",
)


@dataclass(frozen=True)
class AsOfFrame:
    """PIT 面板：values 为观测值，available 为每个单元格的可得时刻。

    ``available`` 取 max(observation_time, release_time, revision_time)：
    FRED 侧 release_time、SOMA 侧 revision_time 都参与截断判断。
    """

    values: pd.DataFrame
    available: pd.DataFrame

    def history(self, as_of: pd.Timestamp) -> pd.DataFrame:
        """只保留 as_of 时刻已发布的值。

        未发布单元格置空后前向填充，得到“每个科目在该时点的最近已知值”；
        as_of 之后的数据不会以任何形式进入结果。
        """
        window = self.values.loc[self.values.index <= as_of]
        known = window.where(self.available.loc[window.index] <= as_of).ffill()
        return known.dropna(how="all")


def _asof_cutoff(origin: pd.Timestamp) -> pd.Timestamp:
    """origin（周三）收盘时刻：当日 23:59:59。

    H.4.1 / SOMA 均在次日（周四）23:59:59 才可得，因此当周观测被排除。
    """
    return origin + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)


def _latest_known_observation(
    frame: AsOfFrame, column: str, as_of: pd.Timestamp
) -> pd.Timestamp:
    """as_of 时点该科目最近一个已发布的观测日。"""
    avail = frame.available[column]
    known = avail[avail <= as_of]
    if known.empty:
        raise RuntimeError(f"{column} 在 {as_of} 时点无已发布观测")
    return pd.Timestamp(known.index[-1])


def _latest_times(frame: pd.DataFrame) -> pd.Series:
    """逐行取最晚的可得时刻（NaT 视为最小；全 NaT 才返回 NaT）。"""
    stacked = np.stack([
        frame[col].to_numpy(dtype="datetime64[ns]") for col in frame.columns
    ])
    return pd.Series(np.max(stacked, axis=0), index=frame.index)


# ALFRED 对这些 H.4.1 序列的 vintage 归档起点（WALCL 等为 2013-04-18）。
# 早于该日期的观测是批量回填 vintage，无法重建当时的首次发布值；
# 因此回测 origin 只从该日期之后开始（见 backtest）。
_VINTAGE_ARCHIVE_FLOOR = pd.Timestamp("2013-04-18")


def _availability(
    obs: pd.Series, released: pd.Series, revised: pd.Series
) -> pd.Series:
    """每个观测的可得时刻 = max(observation_time, release_time, revision_time)。

    FRED 的 release_time 与 SOMA 的 revision_time 都参与截断；真实的晚修订
    （如 WCICL 的批量重述）也按 revision_time 延后可得，不做任何回退。
    """
    return _latest_times(pd.concat([obs, released, revised], axis=1))


def _load_number_frame(service: Any, dataset_id: str, sid: str) -> pd.DataFrame:
    """单序列 → index=观测日，columns=[value, available]。"""
    result = service.query(dataset_id=dataset_id, filters={"source_id": sid})
    df = result.frame.select(list(_PIT_COLUMNS))
    if df.height == 0:
        raise RuntimeError(f"canonical 缺少序列 {sid}，请先回填")
    pdf = df.to_pandas()
    obs = pd.to_datetime(pdf["observation_time"]).dt.normalize()
    value = pd.to_numeric(pdf["value"], errors="coerce").astype(float)
    released = pd.to_datetime(pdf["release_time"], errors="coerce")
    revised = pd.to_datetime(pdf["revision_time"], errors="coerce")
    available = _availability(obs, released, revised)
    out = pd.DataFrame({"value": value, "available": available})
    out.index = pd.DatetimeIndex(obs, name="observation_time")
    out = out.sort_values("available")
    out = out[~out.index.duplicated(keep="last")].sort_index()
    return out


def load_panel(service: Any) -> AsOfFrame:
    """构建周频面板（索引为周三观测日，仅保留 WALCL 非空的周）。"""
    frames = {
        sid: _load_number_frame(service, f"fred_series_{sid}", sid)
        for sid in _PANEL_SERIES
    }
    values = pd.DataFrame({sid: f["value"] for sid, f in frames.items()})
    available = pd.DataFrame({sid: f["available"] for sid, f in frames.items()})
    keep = values["WALCL"].notna() & (values.index >= pd.Timestamp(_START))
    values = values.loc[keep].sort_index()
    available = available.loc[keep].sort_index()
    values.index.name = "date"
    available.index.name = "date"
    return AsOfFrame(values, available)


def load_soma(service: Any) -> AsOfFrame:
    """NY Fed SOMA 9 分项（USD → USD 百万），按周三对齐；带 revision_time 可得时刻。"""
    frames = {
        entity: _load_number_frame(service, "nyfed_soma_holdings", entity)
        for entity in _SOMA_ENTITIES
    }
    values = pd.DataFrame({e: f["value"] for e, f in frames.items()}) / 1e6
    available = pd.DataFrame({e: f["available"] for e, f in frames.items()})
    return AsOfFrame(values.sort_index(), available.loc[values.index])


def add_derived_accounts(frame: AsOfFrame) -> AsOfFrame:
    """派生恒等式科目（残差桶与倒推项），同时派生可得时刻。"""
    df = frame.values.copy()
    df["TCCF"] = df["H41RESH4ENWW"].fillna(0.0)
    df["foreign_dep"] = df[list(_DISTRICTS)].sum(axis=1, min_count=1)
    df["non_soma_assets"] = df[list(_NON_SOMA_ASSETS)].sum(axis=1, min_count=1)
    # 其他资产 = WALCL −（SOMA 证券 + 非 SOMA 已列示资产）；残差桶，不可拆
    df["other_assets"] = df["WALCL"] - df["WSHOSHO"] - df["non_soma_assets"]
    # 其他存款（WLO 行）= 存款总额 −（准备金 + TGA + 定期存款 + 外国官方存款）
    df["other_dep"] = df["WLDLCL"] - (
        df["WRBWFRBL"] + df["WDTGAL"] + df["TERMT"] + df["foreign_dep"]
    )
    df["capital"] = df["WALCL"] - df["WLTLECL"]
    df["currency_gap"] = df["WLFN"] - df["WCICL"]
    df["other_liabilities"] = df[list(_OTHER_LIAB_COLUMNS)].sum(axis=1)

    # 派生科目的可得时刻 = 构成项可得时刻的最大值（最晚可得者决定整行可用）
    avail = frame.available.copy()
    # TCCF（H41RESH4ENWW）的 FRED vintage 归档始于 2020-05-14，此前是批量回填；
    # 该科目 2002–2019 实测恒为 0。对取值为 0 的行按观测日即已知处理（0 是
    # 常数假设，不引入未来 vintage），否则早期 origin 会因单项不可得而整行失效。
    zero_tccf = df["H41RESH4ENWW"].fillna(0.0) == 0.0
    avail["TCCF"] = avail["H41RESH4ENWW"].where(
        ~zero_tccf, pd.Series(df.index, index=df.index)
    )
    avail["foreign_dep"] = _latest_times(avail[list(_DISTRICTS)])
    avail["non_soma_assets"] = _latest_times(avail[list(_NON_SOMA_ASSETS)])
    avail["other_assets"] = _latest_times(avail[["WALCL", "WSHOSHO", "non_soma_assets"]])
    avail["other_dep"] = _latest_times(
        avail[["WLDLCL", "WRBWFRBL", "WDTGAL", "TERMT", "foreign_dep"]]
    )
    avail["capital"] = _latest_times(avail[["WALCL", "WLTLECL"]])
    avail["currency_gap"] = _latest_times(avail[["WLFN", "WCICL"]])
    avail["other_liabilities"] = _latest_times(avail[list(_OTHER_LIAB_COLUMNS)])
    return AsOfFrame(df, avail)


# ── 恒等式校验 ────────────────────────────────────────────────────────


def _err_stats(values: pd.Series) -> dict[str, Any]:
    v = values.dropna().abs()
    return {
        "n": int(v.size),
        "max_abs": round(float(v.max()), 3) if v.size else None,
        "mean_abs": round(float(v.mean()), 3) if v.size else None,
        "le_0.5": int((v <= 0.5).sum()),
        "0.5_1": int(((v > 0.5) & (v <= 1)).sum()),
        "1_2": int(((v > 1) & (v <= 2)).sum()),
        "2_10": int(((v > 2) & (v <= 10)).sum()),
        "gt_10": int((v > 10).sum()),
    }


def identity_checks(df: pd.DataFrame) -> dict[str, Any]:
    """唯一一条独立恒等式（H.4.1 Table 1 负债侧）+ 派生科目诊断。

    独立性证明（M-N1；两条派生式已删除，不再计入覆盖度）：

    1. 存款恒等式恒为 0。`other_dep` 定义为 WLO 残差
           other_dep = WLDLCL − (WRBWFRBL + WDTGAL + TERMT + foreign_dep)
       代回旧 L2
           L2 = WLDLCL − (WRBWFRBL + WDTGAL + TERMT + foreign_dep + other_dep)
              = WLDLCL − WLDLCL = 0
       对任意数据恒成立，不含任何观测信息。
    2. 准备金重构式是 L1 的镜像。`capital = WALCL − WLTLECL`，代回
           reserve_implied = WALCL − capital − WLFN − WLRRAL − WDTGAL − TERMT
                             − foreign_dep − other_dep − WLDACLC − WLAD − TCCF
                            = WLTLECL − WLFN − WLRRAL − WDTGAL − TERMT
                              − foreign_dep − other_dep − WLDACLC − WLAD − TCCF
       而 L1 = WLTLECL − (WLFN + WLRRAL + WLDLCL + WLDACLC + WLAD + TCCF)，
       两者相减并用 other_dep 定义消去 WDTGAL/TERMT/foreign_dep：
           L1 + (WRBWFRBL − reserve_implied)
             = WRBWFRBL + WDTGAL + TERMT + foreign_dep + other_dep − WLDLCL = 0
       即旧 L3 ≡ −L1。同样的数据同一个校验做两遍，必须删。

    覆盖度边界（如实说明）：本函数只校验负债侧总额；资产侧的
    `WALCL = WSHOSHO + 非SOMA资产 + other_assets` 也是定义式（`other_assets`
    为残差桶），不可能失败，因此不构成第二条独立校验。
    """
    liabilities = (
        df["WLTLECL"]
        - (df["WLFN"] + df["WLRRAL"] + df["WLDLCL"] + df["WLDACLC"] + df["WLAD"] + df["TCCF"])
    )
    return {
        "liabilities_total": _err_stats(liabilities),
        "other_assets": {
            "last": round(float(df["other_assets"].iloc[-1]), 1),
            "min": round(float(df["other_assets"].min()), 1),
            "max": round(float(df["other_assets"].max()), 1),
        },
        "other_dep": {
            "last": round(float(df["other_dep"].iloc[-1]), 1),
            "min": round(float(df["other_dep"].min()), 1),
            "max": round(float(df["other_dep"].max()), 1),
            "negative_weeks": int((df["other_dep"] < 0).sum()),
        },
        "foreign_dep": {
            "last": round(float(df["foreign_dep"].iloc[-1]), 1),
            "max": round(float(df["foreign_dep"].max()), 1),
        },
    }


def soma_cross_check(panel: pd.DataFrame, soma: pd.DataFrame) -> dict[str, Any]:
    """WSHOSHO vs NY Fed SOMA（total + TIPS 通胀补偿），按周三对齐。"""
    joined = pd.DataFrame(
        {"WSHOSHO": panel["WSHOSHO"], "soma_total": soma["soma_total"],
         "tips_ic": soma["soma_tips_inflation"]}
    ).dropna()
    diff = joined["WSHOSHO"] - (joined["soma_total"] + joined["tips_ic"])
    bad = joined[diff.abs() > 1.0]
    return {
        "n": int(len(joined)),
        "le_1": int((diff.abs() <= 1.0).sum()),
        "gt_1": int((diff.abs() > 1.0).sum()),
        "max_abs": round(float(diff.abs().max()), 3),
        "bad_weeks": [
            {
                "date": str(idx.date()),
                "diff": round(float(diff.loc[idx]), 1),
                "ws_hosho": round(float(joined.loc[idx, "WSHOSHO"]), 1),
                "soma_total": round(float(joined.loc[idx, "soma_total"]), 1),
                "tips_ic": round(float(joined.loc[idx, "tips_ic"]), 1),
            }
            for idx in bad.index[:10]
        ],
    }


# ── 季节性预测 ────────────────────────────────────────────────────────


def _iso_week_index(idx: pd.DatetimeIndex) -> pd.Series:
    return pd.Series(idx.isocalendar().week.to_numpy(), index=idx, dtype="int64")


def _weekly_medians(delta: pd.Series, origin: pd.Timestamp) -> tuple[pd.Series, pd.Series]:
    """origin 之前、同一 ISO 周的历史周变化中位数与样本数（无前视）。"""
    hist = delta[delta.index < origin].dropna()
    if hist.empty:
        return pd.Series(dtype=float), pd.Series(dtype="int64")
    week = _iso_week_index(hist.index)
    grouped = hist.groupby(week)
    return grouped.median(), grouped.count()


def _seasonal_cum_change(
    delta: pd.Series,
    origin: pd.Timestamp,
    steps: int,
    anchor: pd.Timestamp | None = None,
) -> tuple[float, str, int]:
    """从 anchor 连续 steps 周的累计季节中位变化。

    anchor 为最近可知观测日；当 origin 当周尚未发布时 anchor=origin−7，
    此时 steps 需包含“补齐到 origin”的那一周，否则预测水平会漏掉一周的季节项。
    """
    med, counts = _weekly_medians(delta, origin)
    hist = delta[delta.index < origin].dropna()
    start = anchor if anchor is not None else origin
    total = 0.0
    used_fallback = False
    for k in range(1, steps + 1):
        target = start + pd.Timedelta(days=7 * k)
        wk = int(target.isocalendar()[1])
        if wk in med.index and int(counts.loc[wk]) >= _MIN_SEASONAL_SAMPLES:
            total += float(med.loc[wk])
        else:
            total += float(hist.median()) if len(hist) else 0.0
            used_fallback = True
    samples = int(counts.min()) if len(counts) else 0
    method = "SEASONAL_MEDIAN_FALLBACK_ALL" if used_fallback else "SEASONAL_MEDIAN"
    return total, method, samples


def forecast_from_history(
    hist: pd.DataFrame,
    origin: pd.Timestamp,
    horizon: int,
    adjust_currency: bool = False,
    anchor_date: pd.Timestamp | None = None,
) -> dict[str, Any]:
    """基于已按 PIT 截断的历史做结构分解预测；origin 为预测时点（周三收盘）。

    ``hist`` 必须已只包含 origin 时点可得的数据（见 ``AsOfFrame.history``）。
    当周三当期观测尚未发布时，水平锚定的是上周三；此时季节项需多走一周
    （补齐 anchor→origin），target_date 仍从 origin 起算。
    """
    if hist.empty:
        raise ValueError(f"origin {origin.date()} 无可用观测")
    row = hist.iloc[-1]
    anchor_date = anchor_date if anchor_date is not None else hist.index[-1]
    gap_weeks = max(int(round((origin - anchor_date).days / 7)), 0)
    steps = gap_weeks + horizon

    delta_tga = hist["WDTGAL"].diff()
    d_tga, m_tga, n_tga = _seasonal_cum_change(delta_tga, origin, steps, anchor_date)
    if adjust_currency:
        d_cur, m_cur, n_cur = _seasonal_cum_change(
            hist["WCICL"].diff(), origin, steps, anchor_date
        )
        cur_note = f"ISO周季节中位数 n={n_cur} 周（0.1.0 口径）"
    else:
        d_cur, m_cur, n_cur = 0.0, "HOLD", 0
        cur_note = "持有不变（0.1.0 季节项经消融移除）"

    soma = float(row["WSHOSHO"])
    non_soma = float(row["non_soma_assets"])
    other_assets = float(row["other_assets"])
    currency = float(row["WCICL"]) + d_cur + float(row["currency_gap"])
    repo = max(float(row["WLRRAL"]), 0.0)
    tga = float(row["WDTGAL"]) + d_tga
    other_liabs = float(row["other_liabilities"])
    # PIT 下各科目可能来自不同 vintage（如 WCICL 批量晚修订）：把锚点行
    # 的跨 vintage 不一致额记入残差桶，保证预测锚定在可知的准备金水平上，
    # 而不是被某个尚未修订的科目带偏。
    implied_anchor = (
        soma + non_soma + other_assets
        - (float(row["WCICL"]) + float(row["currency_gap"]))
        - repo - float(row["WDTGAL"]) - other_liabs
    )
    anchor_gap = float(row["WRBWFRBL"]) - implied_anchor
    other_assets += anchor_gap
    reserve = soma + non_soma + other_assets - currency - repo - tga - other_liabs

    components = {
        "soma_securities": (soma, 0.0, "HOLD_STRUCTURAL", ""),
        "non_soma_assets": (non_soma, 0.0, "HOLD_STRUCTURAL", ""),
        "other_assets_residual": (
            other_assets, 0.0, "HOLD_RESIDUAL", "其他资产残差桶；含 PIT 跨 vintage 对齐额"
        ),
        "currency": (currency, d_cur, m_cur, cur_note),
        "reverse_repo": (repo, 0.0, "HOLD", "含 0 下限"),
        "tga": (tga, d_tga, m_tga, f"ISO周季节中位数 n={n_tga} 周"),
        "other_liabilities": (other_liabs, 0.0, "HOLD", "资本+外国/其他存款+递延+其他负债"),
        "reserve_balances": (reserve, reserve - float(row["WRBWFRBL"]), "IDENTITY", ""),
    }
    return {
        "origin_date": origin,
        "anchor_date": anchor_date,
        "target_date": origin + pd.Timedelta(days=7 * horizon),
        "horizon": horizon,
        "origin_reserve": float(row["WRBWFRBL"]),
        "predicted_reserve": reserve,
        "components": components,
    }


def forecast_from_origin(
    frame: AsOfFrame,
    origin: pd.Timestamp,
    horizon: int,
    adjust_currency: bool = False,
) -> dict[str, Any]:
    """生产入口：按 origin（周三收盘）的发布时刻截断后再预测。"""
    asof = _asof_cutoff(origin)
    hist = frame.history(asof)
    anchor = _latest_known_observation(frame, "WRBWFRBL", asof)
    return forecast_from_history(hist, origin, horizon, adjust_currency, anchor)


def _iso_lookup(df: pd.DataFrame) -> dict[tuple[int, int], float]:
    """(ISO 年, ISO 周) → WRBWFRBL 观测值，用于 seasonal naive。"""
    iso = df.index.isocalendar()
    values = df["WRBWFRBL"].to_numpy()
    out: dict[tuple[int, int], float] = {}
    for year, week, value in zip(
        iso["year"].to_numpy(), iso["week"].to_numpy(), values, strict=True
    ):
        if pd.notna(value):
            out[(int(year), int(week))] = float(value)
    return out


def backtest(
    frame: AsOfFrame, min_origin: pd.Timestamp | None = None
) -> pd.DataFrame:
    """扩张窗口回测：每个 origin 只用该时点（周三收盘）已发布的数据。

    同一批 origin 上同时产出：
    - predicted（当前模型）；
    - predicted_v010（0.1.0 口径，用于 currency 消融）；
    - naive（origin 时点最近可知值持平）；
    - seasonal_naive（去年同 ISO 周值；缺失回退 naive，并标记回退）。
    """
    df = frame.values
    if min_origin is None:
        # 早于 ALFRED 归档起点的 origin 无法用单版本 canonical 做 PIT 回测：
        # 当时的最终修订值尚未存在，只能看到批量回填的整块历史。
        min_origin = max(
            df.index[0] + pd.DateOffset(years=2), _VINTAGE_ARCHIVE_FLOOR
        )
    origins = [d for d in df.index if d >= min_origin]
    rows: list[dict[str, Any]] = []
    max_h = max(HORIZONS)
    origins = origins[:-max_h] if len(origins) > max_h else []
    for origin in origins:
        asof = _asof_cutoff(origin)
        hist = frame.history(asof)
        # origin 时点可知的准备金水平（当周观测周四才发布，故通常为上周值）
        anchor = _latest_known_observation(frame, "WRBWFRBL", asof)
        origin_reserve = float(hist.iloc[-1]["WRBWFRBL"])
        seasonal_lookup = _iso_lookup(hist)
        for h in HORIZONS:
            fc = forecast_from_history(hist, origin, h, anchor_date=anchor)
            target = fc["target_date"]
            if target not in df.index:
                continue
            iso = target.isocalendar()
            prev_key = (int(iso[0]) - 1, int(iso[1]))
            if prev_key in seasonal_lookup:
                seasonal = seasonal_lookup[prev_key]
                fallback = False
            else:
                seasonal = origin_reserve
                fallback = True
            legacy = forecast_from_history(
                hist, origin, h, adjust_currency=True, anchor_date=anchor
            )
            rows.append(
                {
                    "origin_date": origin.date(),
                    "anchor_date": fc["anchor_date"].date(),
                    "target_date": target.date(),
                    "horizon": h,
                    "predicted": fc["predicted_reserve"],
                    "predicted_v010": legacy["predicted_reserve"],
                    "naive": origin_reserve,
                    "seasonal_naive": seasonal,
                    "snaive_fallback": fallback,
                    "actual": float(df.loc[target, "WRBWFRBL"]),
                }
            )
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out["error"] = out["actual"] - out["predicted"]
    out["v010_error"] = out["actual"] - out["predicted_v010"]
    out["naive_error"] = out["actual"] - out["naive"]
    out["snaive_error"] = out["actual"] - out["seasonal_naive"]
    return out


def error_summary(bt: pd.DataFrame) -> dict[str, Any]:
    """全样本与近 5 年误差统计；置信带取近 5 年（覆盖当前波动 regime）。"""
    stats: dict[str, Any] = {}
    if bt.empty:
        return stats
    cutoff = pd.Timestamp(bt["target_date"].max()) - pd.DateOffset(years=_BAND_WINDOW_YEARS)
    recent = bt[pd.to_datetime(bt["target_date"]) >= cutoff]
    for h in HORIZONS:
        e_all = bt.loc[bt["horizon"] == h, "error"].dropna()
        e = recent.loc[recent["horizon"] == h, "error"].dropna()
        if e_all.empty or e.empty:
            stats[str(h)] = {"n": 0}
            continue
        stats[str(h)] = {
            "n": int(e.size),
            "n_full": int(e_all.size),
            "mae": round(float(e.abs().mean()), 1),
            "rmse": round(float(np.sqrt((e ** 2).mean())), 1),
            "mae_full": round(float(e_all.abs().mean()), 1),
            "rmse_full": round(float(np.sqrt((e_all ** 2).mean())), 1),
            "bias": round(float(e.mean()), 1),
            "q05": round(float(e.quantile(_QUANTILES[0])), 1),
            "q95": round(float(e.quantile(_QUANTILES[1])), 1),
        }
    return stats


def _metric_block(actual: pd.Series, pred: pd.Series) -> dict[str, float]:
    e = (actual - pred).dropna()
    if e.empty:
        return {"mae": None, "rmse": None}
    return {
        "mae": round(float(e.abs().mean()), 1),
        "rmse": round(float(np.sqrt((e ** 2).mean())), 1),
    }


def benchmark_summary(bt: pd.DataFrame) -> dict[str, Any]:
    """基准对比 + DM 检验 + 科目级消融（口径与 tga_backtest.dm_test 一致）。

    DM 损失 = 绝对误差；统计量负值 = 模型更优；HAC/Newey-West。
    Bonferroni：每个 benchmark 在 h=1/2/4 三个 horizon 上做 m=3 校正。
    """
    from tga_backtest import dm_test

    out: dict[str, Any] = {}
    if bt.empty:
        return out
    target = pd.to_datetime(bt["target_date"])
    cutoff = pd.Timestamp(target.max()) - pd.DateOffset(years=_BAND_WINDOW_YEARS)
    rows: dict[str, dict[str, Any]] = {}
    dm_p: dict[str, list[float]] = {"v010": [], "naive": [], "seasonal_naive": []}
    for h in HORIZONS:
        # 所有对照与消融必须在同一批有效预测行上比较，避免样本漂移
        sub = bt[bt["horizon"] == h].dropna(subset=["actual", "predicted"])
        recent = sub[pd.to_datetime(sub["target_date"]) >= cutoff]
        metrics = {
            "model": _metric_block(recent["actual"], recent["predicted"]),
            "v010": _metric_block(recent["actual"], recent["predicted_v010"]),
            "naive": _metric_block(recent["actual"], recent["naive"]),
            "seasonal_naive": _metric_block(recent["actual"], recent["seasonal_naive"]),
        }
        full_metrics = {
            name: _metric_block(sub["actual"], sub[col])
            for name, col in (
                ("model", "predicted"),
                ("v010", "predicted_v010"),
                ("naive", "naive"),
                ("seasonal_naive", "seasonal_naive"),
            )
        }
        dm: dict[str, Any] = {}
        for bench, col in (
            ("v010", "predicted_v010"),
            ("naive", "naive"),
            ("seasonal_naive", "seasonal_naive"),
        ):
            valid = sub.dropna(subset=["actual", "predicted", col])
            res = dm_test(
                list((valid["actual"] - valid["predicted"]).abs()),
                list((valid["actual"] - valid[col]).abs()),
            )
            dm[bench] = {
                "n": res["n"],
                "dm_stat": (None if res["dm_stat"] is None
                            else round(float(res["dm_stat"]), 3)),
                "p_value": (None if res["p_value"] is None
                            else round(float(res["p_value"]), 4)),
                "hac_lag": res["hac_lag"],
            }
            if res["p_value"] is not None:
                dm_p[bench].append(float(res["p_value"]))
        rows[str(h)] = {
            "n": int(sub.shape[0]),
            "recent": metrics,
            "full": full_metrics,
            "fallback_share": round(float(sub["snaive_fallback"].mean()), 4),
            "fallback_n": int(sub["snaive_fallback"].sum()),
            # 0.1.1 只剩 TGA 一个调整项：去掉 TGA 即 naive
            "tga_increment_full": round(
                full_metrics["naive"]["mae"] - full_metrics["model"]["mae"], 1
            ),
            "tga_increment_recent": round(
                metrics["naive"]["mae"] - metrics["model"]["mae"], 1
            ),
            # currency 消融：0.1.0 保留 currency 季节项 → 0.1.1 改为持有不变
            "currency_increment_full": round(
                full_metrics["v010"]["mae"] - full_metrics["model"]["mae"], 1
            ),
            "currency_increment_recent": round(
                metrics["v010"]["mae"] - metrics["model"]["mae"], 1
            ),
            "dm": dm,
        }
    # Bonferroni：每个对照在各 horizon 上 m=3
    for bench in ("v010", "naive", "seasonal_naive"):
        pvals = dm_p[bench]
        m = len(pvals)
        for h in HORIZONS:
            entry = rows[str(h)]["dm"][bench]
            p = entry["p_value"]
            entry["p_bonferroni"] = (
                None if p is None else round(min(1.0, p * m), 4)
            )
            entry["significant_bonferroni"] = (
                False if entry["p_bonferroni"] is None
                else bool(entry["p_bonferroni"] < 0.05)
            )
    return rows


def _attach_dm_columns(bt: pd.DataFrame, bench: dict[str, Any]) -> pd.DataFrame:
    """把 horizon 级 DM 结果并入 eval 行（同分区落库，便于导出）。"""
    out = bt.copy()
    for label, name in (
        ("v010", "v010"),
        ("naive", "naive"),
        ("seasonal_naive", "seasonal_naive"),
    ):
        for field in ("dm_stat", "p_value", "p_bonferroni"):
            col = f"dm_{label}_{field}"
            out[col] = out["horizon"].map(
                lambda h, n=name, f=field: bench.get(str(int(h)), {})
                .get("dm", {}).get(n, {})
                .get(f)
            )
    return out


# ── Derived 写入 ──────────────────────────────────────────────────────


def _atomic_write(root: Path, df: pd.DataFrame, date_col: str) -> int:
    """按 year/month 分区原子覆盖写（tmp → rename-swap，与既有 Derived 一致）。"""
    if df.empty:
        return 0
    part = df.copy()
    part["_year"] = pd.to_datetime(part[date_col]).dt.year.astype(int)
    part["_month"] = pd.to_datetime(part[date_col]).dt.month.astype(int)
    written = 0
    for (year, month), group in part.groupby(["_year", "_month"]):
        part_dir = root / f"year={year}" / f"month={int(month):02d}"
        tmp_dir = part_dir.parent / f".tmp-{uuid.uuid4().hex[:8]}"
        old_dir = part_dir.parent / f".old-{uuid.uuid4().hex[:8]}"
        try:
            tmp_dir.mkdir(parents=True, exist_ok=True)
            group.drop(columns=["_year", "_month"]).to_parquet(
                tmp_dir / "part-0000.parquet", index=False
            )
            if part_dir.exists():
                part_dir.rename(old_dir)
            tmp_dir.rename(part_dir)
            written += len(group)
        finally:
            if tmp_dir.exists():
                shutil.rmtree(tmp_dir, ignore_errors=True)
            if old_dir.exists():
                shutil.rmtree(old_dir, ignore_errors=True)
    return written


def _config_hash(config: dict[str, Any]) -> str:
    raw = json.dumps(config, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _config() -> dict[str, Any]:
    return {
        "model_version": MODEL_VERSION,
        "frequency": "1W",
        "horizons": list(HORIZONS),
        "scenario": _SCENARIO,
        "observations": ["WRBWFRBL", "WDTGAL", "WCICL", "WLRRAL"],
        "methods": _COMPONENT_METHODS,
        "seasonal_min_samples": _MIN_SEASONAL_SAMPLES,
        "pit": {
            "asof": "origin_wednesday_close",
            "truncate_by": ["release_time", "revision_time"],
        },
        "band": {
            "type": "empirical_error_quantile_per_horizon",
            "window_years": _BAND_WINDOW_YEARS,
            "quantiles": list(_QUANTILES),
        },
        "residual_bucket": "other_assets",
    }


# ── 主流程 ────────────────────────────────────────────────────────────


def _open() -> tuple[Settings, Any, Any]:
    settings = Settings.load()
    meta = open_meta(settings)
    service, con = open_query_service(settings, meta)
    return settings, meta, service


def cmd_check() -> int:
    settings, meta, service = _open()
    try:
        frame = add_derived_accounts(load_panel(service))
        df = frame.values
        soma = load_soma(service)
        latest_cutoff = _asof_cutoff(df.index[-1])
        result = {
            "panel": {
                "rows": int(df.shape[0]),
                "start": str(df.index[0].date()),
                "end": str(df.index[-1].date()),
                "wednesdays": int((df.index.weekday == 2).sum()),
            },
            "pit": {
                "origin_weekday": df.index[-1].day_name(),
                "asof": str(latest_cutoff),
                "h41_latest_known_observation": str(
                    _latest_known_observation(frame, "WRBWFRBL", latest_cutoff).date()
                ),
                "soma_latest_known_observation": str(
                    _latest_known_observation(
                        soma, "soma_total", latest_cutoff
                    ).date()
                ),
            },
            "identities": identity_checks(df),
            "soma": soma_cross_check(df, soma.values),
        }
    finally:
        meta.close()
    print(json.dumps(result, indent=2, ensure_ascii=False))
    # 只有负债侧总额是独立校验（存款/准备金两条为代数派生，已删除，见 §M-N1）
    ok = result["identities"]["liabilities_total"]["max_abs"] <= 3.0
    print(f"\n恒等式校验：{'PASS' if ok else 'FAIL'}（负债侧总额，容差 3 USD 百万；独立校验数 1）")
    return 0 if ok else 2


def cmd_run(dry_run: bool = False) -> int:
    from tga_store import ForecastStore

    settings, meta, service = _open()
    try:
        frame = add_derived_accounts(load_panel(service))
        df = frame.values
        origin = df.index[-1]
        asof = _asof_cutoff(origin)
        known = frame.history(asof)
        anchor_date = _latest_known_observation(frame, "WRBWFRBL", asof)
        origin_reserve = float(known.iloc[-1]["WRBWFRBL"])
        published_reserve = float(df.loc[origin, "WRBWFRBL"])

        bt = backtest(frame)
        stats = error_summary(bt)
        bench = benchmark_summary(bt)
        bt_eval = _attach_dm_columns(bt, bench)

        fc_rows: list[dict[str, Any]] = []
        comp_rows: list[dict[str, Any]] = []
        for h in HORIZONS:
            fc = forecast_from_origin(frame, origin, h)
            st = stats.get(str(h), {})
            lo = fc["predicted_reserve"] + st.get("q05", 0.0)
            hi = fc["predicted_reserve"] + st.get("q95", 0.0)
            fc_rows.append(
                {
                    "run_date": origin.date(),
                    "scenario": _SCENARIO,
                    "origin_date": origin.date(),
                    "target_date": fc["target_date"].date(),
                    "horizon": h,
                    "predicted_reserve": round(fc["predicted_reserve"], 2),
                    "lower": round(lo, 2),
                    "upper": round(hi, 2),
                    "origin_reserve": round(origin_reserve, 2),
                    "model_version": MODEL_VERSION,
                }
            )
            for name, (level, change, method, note) in fc["components"].items():
                comp_rows.append(
                    {
                        "run_date": origin.date(),
                        "scenario": _SCENARIO,
                        "origin_date": origin.date(),
                        "target_date": fc["target_date"].date(),
                        "horizon": h,
                        "component": name,
                        "level": round(float(level), 2),
                        "delta_from_origin": round(float(change), 2),
                        "method": method,
                        "is_residual_bucket": name == "other_assets_residual",
                        "note": note,
                    }
                )

        fc_df = pd.DataFrame(fc_rows)
        comp_df = pd.DataFrame(comp_rows)

        config = _config()
        cfg_hash = _config_hash(config)
        # PIT 数据截止：origin 周三收盘（当周 H.4.1/SOMA 周四才发布）
        cutoff = str(origin.date())
        print(f"origin={origin.date()}（周三收盘，asof={asof}）")
        print(
            f"  可知最新观测={anchor_date.date()} 准备金={origin_reserve:,.1f}M；"
            f"当周已发布值={published_reserve:,.1f}M（仅事后对照，不参与预测）"
        )
        print("回测误差（USD 百万）：")
        for h in HORIZONS:
            print(f"  h={h}周 {stats.get(str(h))}")
        print("\n基准对比（MAE / RMSE，USD 百万；近5年 / 全样本）：")
        print("| h | 口径 | 0.1.2 | 0.1.0 | naive | seasonal naive |")
        print("|---|---|---:|---:|---:|---:|")
        for h in HORIZONS:
            b = bench.get(str(h), {})
            for label, key in (("近5年", "recent"), ("全样本", "full")):
                block = b.get(key, {})
                cells = []
                for name in ("model", "v010", "naive", "seasonal_naive"):
                    m = block.get(name, {})
                    cells.append(f"{m.get('mae')} / {m.get('rmse')}")
                print(f"| {h} | {label} | " + " | ".join(cells) + " |")
        print("\nDM（损失=绝对误差；负统计量=模型更优；Bonferroni m=3）：")
        for h in HORIZONS:
            b = bench.get(str(h), {})
            for name in ("v010", "naive", "seasonal_naive"):
                d = b.get("dm", {}).get(name, {})
                print(
                    f"  h={h} vs {name}: n={d.get('n')} stat={d.get('dm_stat')} "
                    f"p={d.get('p_value')} p_bonf={d.get('p_bonferroni')} "
                    f"显著={d.get('significant_bonferroni')}"
                )
        print("\n消融（USD 百万 MAE；近5年 / 全样本）：")
        for h in HORIZONS:
            b = bench.get(str(h), {})
            print(
                f"  h={h} TGA 增量（naive − 模型）: "
                f"{b.get('tga_increment_recent')} / {b.get('tga_increment_full')}；"
                f"currency 增量（0.1.0 − 0.1.2）: "
                f"{b.get('currency_increment_recent')} / {b.get('currency_increment_full')}"
            )
        print("\nseasonal naive 回退占比：")
        for h in HORIZONS:
            b = bench.get(str(h), {})
            print(f"  h={h}: {b.get('fallback_n')}/{b.get('n')} = {b.get('fallback_share')}")

        print("\n预测：")
        print(fc_df.to_string(index=False))

        if dry_run:
            return 0

        root = Path(settings.data_dir)
        if not root.is_absolute():
            root = _REPO_ROOT / root
        n_fc = _atomic_write(root / "derived" / "reserve_forecast", fc_df, "target_date")
        n_comp = _atomic_write(
            root / "derived" / "reserve_forecast_components", comp_df, "target_date"
        )
        n_eval = _atomic_write(
            root / "derived" / "reserve_forecast_eval", bt_eval, "target_date"
        )

        store = ForecastStore(settings.data_dir, meta)
        run_id, is_repeat = store.create_run(
            origin_date=str(origin.date()),
            data_cutoff_date=cutoff,
            model_version=MODEL_VERSION,
            config_hash=cfg_hash,
            scenarios=[_SCENARIO],
            horizons=list(HORIZONS),
            config_hash_v2=cfg_hash,
        )
        payload_hash = hashlib.sha256(
            (fc_df.to_json(orient="records") + comp_df.to_json(orient="records")).encode()
        ).hexdigest()
        store.finalize_run(run_id, "SUCCESS", output_hash=payload_hash)
        print(
            f"\n落库：fc={n_fc} comp={n_comp} eval={n_eval} "
            f"run_id={run_id} repeat={is_repeat} cutoff={cutoff}"
        )
    finally:
        meta.close()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="准备金结构预测 reserve-structural-0.1.2")
    parser.add_argument("command", choices=("check", "run"))
    parser.add_argument("--dry-run", action="store_true", help="只计算不落库")
    args = parser.parse_args()
    if args.command == "check":
        return cmd_check()
    return cmd_run(args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
