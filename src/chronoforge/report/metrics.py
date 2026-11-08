"""日报指标层：只消费 build_payload 的序列，不重建指标计算。

定义（写入 JSON ``definitions`` 供审计）：
- 变化：取最新观测日与「该日往前 N 个自然日（或可得的最近观测）」的差值
- 分位：最新值在过去 N 年（按观测日窗口）样本中的经验分位，
  rank = #{v <= latest} / N
- 三池：WRESBAL / WTREGEN / RRPONTSYD 同步到周频后取最新一周
- 周度分解：直接使用 build_payload 的 weekly.decomp（单位：十亿美元）
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

Pair = tuple[date, float]


def to_pairs(block: dict[str, list[Any]] | None) -> list[Pair]:
    if not block:
        return []
    out: list[Pair] = []
    for t, v in zip(block.get("t", []), block.get("v", []), strict=False):
        if v is None:
            continue
        try:
            out.append((date.fromisoformat(str(t)[:10]), float(v)))
        except (ValueError, TypeError):
            continue
    out.sort(key=lambda item: item[0])
    return out


def latest(pairs: list[Pair]) -> Pair | None:
    return pairs[-1] if pairs else None


def value_at_or_before(pairs: list[Pair], target: date) -> Pair | None:
    found = None
    for item in pairs:
        if item[0] <= target:
            found = item
        else:
            break
    return found


def change_over(pairs: list[Pair], periods: int, *, unit_div: float) -> float | None:
    last = latest(pairs)
    if last is None:
        return None
    base = value_at_or_before(pairs, last[0] - timedelta(days=periods))
    if base is None or base[0] == last[0]:
        return None
    return round((last[1] - base[1]) / unit_div, 2)


def percentile_rank(pairs: list[Pair], years: int) -> float | None:
    last = latest(pairs)
    if last is None:
        return None
    start = last[0] - timedelta(days=365 * years)
    window = [v for t, v in pairs if start <= t <= last[0]]
    if len(window) < 8:
        return None
    below_or_equal = sum(1 for v in window if v <= last[1])
    return round(below_or_equal / len(window) * 100, 1)


def reserve_zone(level_t: float | None) -> str:
    if level_t is None:
        return "未知"
    if level_t < 3.0:
        return "紧张区"
    if level_t < 3.2:
        return "预警区"
    return "充裕区"


def _three_pool(payload: dict[str, Any]) -> dict[str, Any]:
    """三池由原始周序列换算（WRESBAL/WTREGEN 百万→T，RRPONTSYD 十亿→T）。"""
    series = payload.get("series", {})

    def _scaled(code: str, scale: float) -> list[Pair]:
        return [(d, v / scale) for d, v in to_pairs(series.get(code))]

    rows = {
        "reserve": _scaled("WRESBAL", 1e6),
        "tga": _scaled("WTREGEN", 1e6),
        "onrrp": _scaled("RRPONTSYD", 1e3),
    }
    reserve = rows["reserve"]
    if not reserve:
        return {"latest_date": None, "levels": {}, "chg_1w": {}, "chg_4w": {}}

    last_day = reserve[-1][0]

    def _value_at(key: str, target: date) -> float | None:
        item = value_at_or_before(rows[key], target)
        return float(item[1]) if item else None

    def _diff(key: str, days: int) -> float | None:
        last_value = _value_at(key, last_day)
        base = _value_at(key, last_day - timedelta(days=days))
        if last_value is None or base is None:
            return None
        return round((last_value - base) * 1000.0, 1)  # T → B

    levels: dict[str, float | None] = {}
    for key in rows:
        value = _value_at(key, last_day)
        levels[key] = round(value, 3) if value is not None else None
    chg_1w = {key: _diff(key, 7) for key in rows}
    chg_4w = {key: _diff(key, 28) for key in rows}
    return {
        "latest_date": last_day.isoformat(),
        "levels": levels,
        "chg_1w": chg_1w,
        "chg_4w": chg_4w,
    }


def _decomp(payload: dict[str, Any]) -> dict[str, Any]:
    rows = payload.get("weekly", {}).get("decomp") or []
    if not rows:
        return {"latest_date": None, "latest": {}, "sum_4w": {}}
    latest_row = rows[-1]
    last4 = rows[-4:]
    keys = ("dR", "dA", "tga", "onrrp", "cash", "other")
    latest = {k: round(float(latest_row.get(k, 0.0)), 2) for k in keys}
    sum4 = {k: round(sum(float(r.get(k, 0.0)) for r in last4), 2) for k in keys}
    return {
        "latest_date": str(latest_row.get("t")),
        "latest": latest,
        "sum_4w": sum4,
    }


def _funding(payload: dict[str, Any], cutoffs: dict[str, Any]) -> dict[str, Any]:
    liq = payload.get("framework", {}).get("liquidity", {})
    spread = liq.get("spread_bp")
    if spread is None:
        corridor = "未知"
    elif spread < 12:
        corridor = "正常"
    elif spread <= 17:
        corridor = "预警"
    else:
        corridor = "极端"
    return {
        "effr_asof": cutoffs.get("EFFR"),
        "sofr_asof": cutoffs.get("SOFR"),
        "iorb_asof": cutoffs.get("IORB"),
        "srf_asof": cutoffs.get("SRF"),
        "effr": liq.get("effr"),
        "sofr": liq.get("sofr"),
        "iorb": liq.get("iorb"),
        "onrrp_rate": liq.get("onrrp_rate"),
        "spread_bp": spread,
        "corridor_status": corridor,
        "srf_usage_b": liq.get("srf_usage_b"),
        "srf_peak_30d_b": liq.get("srf_max_30d_b"),
    }


def _tga(payload: dict[str, Any], cutoffs: dict[str, Any]) -> dict[str, Any]:
    daily = to_pairs(payload.get("tga", {}).get("daily"))
    last = latest(daily)
    chg7 = None
    if last is not None:
        base = value_at_or_before(daily, last[0] - timedelta(days=7))
        if base is not None and base[0] != last[0]:
            chg7 = round((last[1] - base[1]) / 1000.0, 2)
    forecast = payload.get("tga", {}).get("forecast", {})
    return {
        "daily_asof": cutoffs.get("TGA_DAILY"),
        "daily_b": round(last[1] / 1000.0, 3) if last else None,
        "chg_7d_b": chg7,
        "forecast_run_date": forecast.get("run_date"),
        "scenarios": sorted(forecast.get("scenarios", {}).keys()),
    }


def _markets(payload: dict[str, Any], cutoffs: dict[str, Any]) -> dict[str, Any]:
    trans = payload.get("framework", {}).get("transmission", {})
    return {
        "dxy_asof": cutoffs.get("DXY"),
        "vix_asof": cutoffs.get("VIXCLS"),
        "spx_asof": cutoffs.get("SPX"),
        "tlt_asof": cutoffs.get("TLT"),
        "dxy": trans.get("dxy"),
        "dxy_chg20": trans.get("dxy_chg20"),
        "vix": trans.get("vix"),
        "vix_chg20": trans.get("vix_chg20"),
        "real_yield": trans.get("real_yield"),
        "real_chg20": trans.get("real_chg20"),
    }


def _reserve_forecast(payload: dict[str, Any]) -> dict[str, Any]:
    rf = payload.get("reserve_forecast", {})
    points = rf.get("forecast") or []
    first = points[0] if points else None
    if not first:
        return {"run_date": rf.get("run_date"), "origin_date": rf.get("origin_date"),
                "h1": None}
    return {
        "run_date": rf.get("run_date"),
        "origin_date": rf.get("origin_date"),
        "model_version": rf.get("model_version"),
        "h1": {
            "target_date": first.get("t"),
            "predicted_t": round(float(first.get("v")) / 1e6, 3),
            "lower_t": round(float(first.get("lo")) / 1e6, 3)
            if first.get("lo") is not None else None,
            "upper_t": round(float(first.get("hi")) / 1e6, 3)
            if first.get("hi") is not None else None,
        },
    }


def compute_metrics(payload: dict[str, Any]) -> dict[str, Any]:
    cutoffs = payload.get("cutoffs", {})
    wres = to_pairs(payload.get("series", {}).get("WRESBAL"))
    last = latest(wres)
    level_t = round(last[1] / 1e6, 3) if last else None
    reserve = {
        "asof": last[0].isoformat() if last else None,
        "level_t": level_t,
        "chg_1w_b": change_over(wres, 7, unit_div=1000.0),
        "chg_4w_b": change_over(wres, 28, unit_div=1000.0),
        "chg_13w_b": change_over(wres, 91, unit_div=1000.0),
        "pct_5y": percentile_rank(wres, 5),
        "pct_10y": percentile_rank(wres, 10),
        "zone": reserve_zone(level_t),
        "thresholds": {"tight_t": 3.0, "warn_t": 3.2},
    }

    return {
        "definitions": {
            "change": "最新观测值 − 往前 N 个自然日（或该日之前最近观测）的值；"
                      "1 周/4 周/13 周分别为 7/28/91 天",
            "percentile": "最新值在最近 N 年观测窗口中的经验分位 #{v<=latest}/N",
            "units": "准备金/TGA 为十亿美元（T）；利率为百分比；spread 为 bp",
            "weekly_h41": "H.4.1 周频，报告中可得最近一个周三观测",
        },
        "reserve": reserve,
        "three_pool": _three_pool(payload),
        "weekly_decomp": _decomp(payload),
        "funding": _funding(payload, cutoffs),
        "tga": _tga(payload, cutoffs),
        "markets": _markets(payload, cutoffs),
        "reserve_forecast": _reserve_forecast(payload),
        "data_cutoff": payload.get("meta", {}).get("data_cutoff"),
    }
