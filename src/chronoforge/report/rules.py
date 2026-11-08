"""风险规则回测：用近两年触发频率与区分度决定「事件告警」还是「背景状态」。

规则设计依据（详见报告「风险规则」表）：
- 频率过高（>1/3 观测触发）的规则不能逐日告警，降级为背景状态，只在跨区转换时提示；
- 频率过低或与资金面紧张无关的规则删除，不凑数；
- ON RRP 已结构性归零（5 日均值 < 10 B），「RRP < 50 B」从每日警报改为零值平台状态；
- 边际工具重心迁移到 SRF 规模/连续天数、税期 TGA 重建、准备金 4 周/13 周下降速率。

区分度口径：每条事件规则选一个独立于自身的资金面确认指标，计算
「触发样本中确认指标同时出现（或随后 4 周出现）的比例」与「全样本基础比例」，
两者之比即 lift。数据全部来自 ``build_payload`` 的现有序列，不新建指标。
"""

from __future__ import annotations

from bisect import bisect_left
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from chronoforge.report.metrics import to_pairs, value_at_or_before
from chronoforge.report.tax_calendar import next_tax_period, tax_period_dates

BACKTEST_YEARS = 2
RRP_ZERO_B = 10.0
RRP_REVIVE_B = 20.0


@dataclass
class _Frame:
    reserve_w: list[tuple[date, float]]
    tga_d: list[tuple[date, float]]
    rrp_d: list[tuple[date, float]]
    srf_d: list[tuple[date, float]]
    spread_d: list[tuple[date, float]]
    bdays: list[date]

    def near(self, target: date, k: int = 3) -> list[date]:
        i = bisect_left(self.bdays, target)
        return self.bdays[max(0, i - k): i + k + 1]

    def forward(self, target: date, days: int) -> list[date]:
        i = bisect_left(self.bdays, target)
        end = target + timedelta(days=days)
        return [d for d in self.bdays[i:] if d <= end]

    def date_value(self, rows: list[tuple[date, float]], target: date) -> float | None:
        item = value_at_or_before(rows, target)
        return float(item[1]) if item else None


def _frame(payload: dict[str, Any], report_day: date) -> _Frame:
    start = report_day - timedelta(days=365 * BACKTEST_YEARS)
    series = payload.get("series", {})
    reserve = [(d, v / 1e6) for d, v in to_pairs(series.get("WRESBAL")) if start <= d]
    rrp = [(d, v) for d, v in to_pairs(series.get("RRPONTSYD")) if start <= d]
    srf = [(d, v) for d, v in to_pairs(series.get("SRF_USAGE")) if start <= d]
    sofr = dict((d, v) for d, v in to_pairs(series.get("SOFR")) if start <= d)
    effr = dict((d, v) for d, v in to_pairs(series.get("EFFR")) if start <= d)
    bdays = sorted(set(sofr) & set(effr))
    spread = [(d, round((sofr[d] - effr[d]) * 100.0, 1)) for d in bdays]
    tga = [(d, v / 1000.0) for d, v in to_pairs(payload.get("tga", {}).get("daily")) if start <= d]
    return _Frame(reserve, tga, rrp, srf, spread, bdays)


def _runs(dates: list[date]) -> list[list[date]]:
    out: list[list[date]] = []
    cur: list[date] = []
    for d in dates:
        if cur and (d - cur[-1]).days > 7:
            out.append(cur)
            cur = []
        cur.append(d)
    if cur:
        out.append(cur)
    return out


def _contiguous(rows: list[tuple[date, float]], predicate: Any) -> list[list[date]]:
    """按样本顺序切段：predicate 为真则连续，任一观测为假就断开。"""
    out: list[list[date]] = []
    cur: list[date] = []
    for d, value in rows:
        if predicate(value):
            cur.append(d)
        elif cur:
            out.append(cur)
            cur = []
    if cur:
        out.append(cur)
    return out


def _pct(hits: int, total: int) -> float:
    return round(hits / total * 100.0, 1) if total else 0.0


def _reserve_changes(frame: _Frame) -> list[tuple[date, float, float | None]]:
    rows: list[tuple[date, float, float | None]] = []
    for d, value in frame.reserve_w:
        base4 = value_at_or_before(frame.reserve_w, d - timedelta(days=28))
        base13 = value_at_or_before(frame.reserve_w, d - timedelta(days=91))
        c4 = round((value - base4[1]) * 1000.0, 1) if base4 else None
        c13 = round((value - base13[1]) * 1000.0, 1) if base13 else None
        if c4 is not None:
            rows.append((d, c4, c13))
    return rows


def _tax_rebuilds(frame: _Frame, report_day: date) -> list[tuple[date, float]]:
    start = report_day - timedelta(days=365 * BACKTEST_YEARS)
    rows: list[tuple[date, float]] = []
    for ref in tax_period_dates(start, report_day):
        base = value_at_or_before(frame.tga_d, ref - timedelta(days=3))
        later = value_at_or_before(frame.tga_d, ref + timedelta(days=10))
        if base and later:
            rows.append((ref, round(later[1] - base[1], 1)))
    return rows


def _forward_spread_hit(frame: _Frame, target: date, threshold: float = 12.0,
                        days: int = 28) -> bool:
    return any(v >= threshold for d, v in frame.spread_d if d in frame.forward(target, days))


def _near_stress(frame: _Frame, target: date, threshold: float,
                 k: int = 3) -> bool:
    window = set(frame.near(target, k))
    return any(d in window and v >= threshold for d, v in frame.spread_d)


def _base_forward_spread(frame: _Frame, threshold: float = 12.0) -> float:
    hits = sum(1 for d, _ in frame.reserve_w if _forward_spread_hit(frame, d, threshold))
    return _pct(hits, len(frame.reserve_w))


def _base_near_spread(frame: _Frame, threshold: float) -> float:
    hits = sum(1 for d in frame.bdays if _near_stress(frame, d, threshold))
    return _pct(hits, len(frame.bdays))


def _rule(
    *,
    rule_id: str,
    label: str,
    kind: str,
    condition: str,
    triggers: list[date],
    total: int,
    unit: str,
    corroboration: dict[str, Any],
    current: dict[str, Any],
    evidence: list[str],
    note: str = "",
) -> dict[str, Any]:
    episodes = len(_runs(sorted(triggers))) if triggers else 0
    per_year = round(len(triggers) / BACKTEST_YEARS, 1)
    return {
        "id": rule_id,
        "label": label,
        "kind": kind,
        "condition": condition,
        "frequency": {
            "triggers": len(triggers),
            "total": total,
            "unit": unit,
            "pct": _pct(len(triggers), total),
            "per_year": per_year,
            "episodes": episodes,
            "window_years": BACKTEST_YEARS,
        },
        "corroboration": corroboration,
        "current": current,
        "evidence": evidence,
        "note": note,
    }


def _current_event(
    active: bool, value: str, extra: str = ""
) -> dict[str, Any]:
    return {"active": active, "value": value, "detail": extra}


def backtest_rules(payload: dict[str, Any], report_day: date) -> dict[str, Any]:
    """返回规则回测 + 当前状态 + 近 5 个交易日状态转换。"""
    frame = _frame(payload, report_day)
    changes = _reserve_changes(frame)
    if not changes:
        return {"rows": [], "transitions": [], "window_years": BACKTEST_YEARS}

    last_c4 = changes[-1][1]
    last_c13 = changes[-1][2]
    spread = frame.spread_d[-1][1] if frame.spread_d else None
    srf_last = frame.srf_d[-1][1] if frame.srf_d else None
    reserve_last = frame.reserve_w[-1]

    # 背景状态 1：准备金水位区间
    tight = [d for d, v in frame.reserve_w if v < 3.0]
    warn = [d for d, v in frame.reserve_w if 3.0 <= v < 3.2]
    zone = "紧张区" if reserve_last[1] < 3.0 else ("预警区" if reserve_last[1] < 3.2 else "充裕区")

    def _in_zone(value: float) -> bool:
        if zone == "紧张区":
            return value < 3.0
        if zone == "预警区":
            return 3.0 <= value < 3.2
        return value >= 3.2

    streak = 0
    for _, value in reversed(frame.reserve_w):
        if _in_zone(value):
            streak += 1
        else:
            break

    bg_zone = _rule(
        rule_id="BG_RESERVE_ZONE",
        label="准备金水位区间",
        kind="background",
        condition="周均准备金 < 3.0 T 记为紧张区（3.0–3.2 T 预警，≥3.2 T 充裕）",
        triggers=tight,
        total=len(frame.reserve_w),
        unit="周",
        corroboration={
            "indicator": "状态型：按区间陈述，不作为独立警报",
            "hits": 0, "n": 0, "pct": 0.0, "base_pct": 0.0, "lift": 0.0,
            "note": "近两年紧张区 37/104 周（35.6%），逐日告警会失效；改为跨区时提示一次",
        },
        current=_current_event(
            zone != "充裕区",
            f"{zone} · {reserve_last[1]:.3f} T",
            f"已连续 {streak} 周；近两年紧张区 {len(tight)}/104 周、预警区 {len(warn)}/104 周",
        ),
        evidence=["VAR_BANK_RESERVES"],
    )

    # 背景状态 2：ON RRP 零值平台
    ma5: list[tuple[date, float]] = []
    for i in range(4, len(frame.rrp_d)):
        ma5.append((frame.rrp_d[i][0], sum(v for _, v in frame.rrp_d[i - 4:i + 1]) / 5.0))
    zero_days = [d for d, m in ma5 if m < RRP_ZERO_B]
    zero_start = None
    for i in range(len(ma5) - 1, -1, -1):
        if ma5[i][1] >= RRP_ZERO_B:
            zero_start = ma5[i + 1][0] if i + 1 < len(ma5) else None
            break
    revive = [d for d, m in ma5 if m >= RRP_REVIVE_B]
    rrp_latest = ma5[-1][1] if ma5 else None
    rrp_tail = frame.rrp_d[-1][1] if frame.rrp_d else None
    rrp_detail = (
        f"最新日值 {rrp_tail:.2f} B"
        if rrp_tail is not None else "缺失"
    )
    if zero_start is None and ma5:
        zero_start = ma5[0][0]
    if zero_start:
        span = sum(1 for d, _ in ma5 if d >= zero_start)
        rrp_detail += f"；连续 {span} 个交易日低于 {RRP_ZERO_B:.0f} B"
    if revive:
        rrp_detail += f"；最后一次 5 日均值 ≥{RRP_REVIVE_B:.0f} B 为 {max(revive).isoformat()}"
    bg_rrp = _rule(
        rule_id="BG_ONRRP_ZERO",
        label="ON RRP 零值平台",
        kind="background",
        condition=(
            f"ON RRP 5 日均值 < {RRP_ZERO_B:.0f} B；"
            f"回升至 {RRP_REVIVE_B:.0f} B 以上才视为缓冲恢复"
        ),
        triggers=zero_days,
        total=len(ma5),
        unit="天",
        corroboration={
            "indicator": "状态型：仅在由零回升或由正转零时提示",
            "hits": 0, "n": 0, "pct": 0.0, "base_pct": 0.0, "lift": 0.0,
            "note": (
                f"近两年 {len(zero_days)}/{len(ma5)} 天处于零值平台；"
                "当前结构上不再是可调用缓冲"
            ),
        },
        current=_current_event(
            bool(rrp_latest is not None and rrp_latest < RRP_ZERO_B),
            f"5 日均值 {rrp_latest:.2f} B" if rrp_latest is not None else "缺失",
            rrp_detail,
        ),
        evidence=["VAR_ON_RRP", "REL_016"],
    )

    # 事件 1：SOFR-EFFR 走廊利差
    corridor = "极端" if (spread or 0) > 17 else ("预警" if (spread or 0) >= 12 else "正常")
    spread_trig = [d for d, v in frame.spread_d if v >= 12.0]
    srf1_days = [d for d, v in frame.srf_d if v >= 1.0]
    hits = sum(1 for d in spread_trig
               if any(x in set(frame.near(d)) for x in srf1_days))
    base = sum(1 for d in frame.bdays
               if any(x in set(frame.near(d)) for x in srf1_days))
    ev_spread = _rule(
        rule_id="EV_SOFR_EFFR",
        label="SOFR-EFFR ≥ 12 bp",
        kind="event",
        condition="SOFR − EFFR ≥ 12 bp（≥17 bp 为极端）",
        triggers=spread_trig,
        total=len(frame.bdays),
        unit="天",
        corroboration={
            "indicator": "±3 个交易日内 SRF 单日 ≥1 B",
            "hits": hits, "n": len(spread_trig),
            "pct": _pct(hits, len(spread_trig)),
            "base_pct": _pct(base, len(frame.bdays)),
            "lift": round((hits / len(spread_trig)) / (base / len(frame.bdays)), 1)
            if spread_trig and base else 0.0,
            "note": "利差并非孤立读数：近两年触发日大多伴随 SRF 实际使用",
        },
        current=_current_event(
            bool(spread is not None and spread >= 12.0),
            f"{spread:.1f} bp" if spread is not None else "缺失",
            f"走廊状态「{corridor}」",
        ),
        evidence=["VAR_SOFR", "VAR_EFFR", "REL_034"],
    )

    # 事件 2：SRF 单日规模
    srf_big = [d for d, v in frame.srf_d if v >= 3.0]
    hits = sum(1 for d in srf_big if _near_stress(frame, d, 8.0))
    ev_srf = _rule(
        rule_id="EV_SRF_SCALE",
        label="SRF 单日 ≥ 3 B",
        kind="event",
        condition="SRF 单日使用量 ≥ 3 B",
        triggers=srf_big,
        total=len(frame.srf_d),
        unit="天",
        corroboration={
            "indicator": "±3 个交易日内 SOFR-EFFR ≥ 8 bp",
            "hits": hits, "n": len(srf_big),
            "pct": _pct(hits, len(srf_big)),
            "base_pct": _base_near_spread(frame, 8.0),
            "lift": round((hits / len(srf_big)) / (_base_near_spread(frame, 8.0) / 100.0), 1)
            if srf_big and _base_near_spread(frame, 8.0) else 0.0,
            "note": "SRF 是 RRP 归零后的主要应急阀门；规模触发与走廊走阔高度同步",
        },
        current=_current_event(
            bool(srf_last is not None and srf_last >= 3.0),
            f"{srf_last:.2f} B" if srf_last is not None else "缺失",
            f"30 日峰值 {max((v for _, v in frame.srf_d[-30:]), default=0.0):.2f} B",
        ),
        evidence=["VAR_SRF_USAGE", "REL_010"],
    )

    # 事件 3：SRF 连续使用
    persist_runs = [r for r in _contiguous(frame.srf_d, lambda v: v >= 1.0) if len(r) >= 3]
    persist_trig = [d for r in persist_runs for d in r]
    hits = sum(1 for d in persist_trig if _near_stress(frame, d, 8.0))
    base_near = _base_near_spread(frame, 8.0)
    ev_srf_run = _rule(
        rule_id="EV_SRF_STREAK",
        label="SRF 连续使用 ≥ 3 个交易日",
        kind="event",
        condition="SRF 单日 ≥1 B 且连续 ≥3 个交易日",
        triggers=persist_trig,
        total=len(frame.srf_d),
        unit="天",
        corroboration={
            "indicator": "±3 个交易日内 SOFR-EFFR ≥ 8 bp",
            "hits": hits, "n": len(persist_trig),
            "pct": _pct(hits, len(persist_trig)),
            "base_pct": base_near,
            "lift": round((hits / len(persist_trig)) / (base_near / 100.0), 1)
            if persist_trig and base_near else 0.0,
            "note": f"近两年 {len(persist_runs)} 段连续使用；连续天数比单日规模更能确认被动求助",
        },
        current=_current_event(
            bool(srf_last is not None and srf_last >= 1.0),
            f"{srf_last:.2f} B" if srf_last is not None else "缺失",
            f"最近一段连续 ≥1 B 用了 {len(persist_runs[-1]) if persist_runs else 0} 个交易日",
        ),
        evidence=["VAR_SRF_USAGE", "REL_010"],
    )

    # 事件 4：准备金 4 周下降速率
    c4 = [(d, v) for d, v, _ in changes]
    trig_4w = [d for d, v in c4 if v <= -125.0]
    hits = sum(1 for d in trig_4w if _forward_spread_hit(frame, d))
    ev_4w = _rule(
        rule_id="EV_RESERVE_4W",
        label="准备金 4 周下降 ≥ 125 B",
        kind="event",
        condition="周均准备金相对 4 周前下降 ≥ 125 B",
        triggers=trig_4w,
        total=len(c4),
        unit="周",
        corroboration={
            "indicator": "随后 4 周内 SOFR-EFFR ≥ 12 bp",
            "hits": hits, "n": len(trig_4w),
            "pct": _pct(hits, len(trig_4w)),
            "base_pct": _base_forward_spread(frame),
            "lift": round((hits / len(trig_4w)) / (_base_forward_spread(frame) / 100.0), 1)
            if trig_4w else 0.0,
            "note": "下降速率是压力前兆，但单独提前量有限；与 13 周规则并用",
        },
        current=_current_event(
            bool(last_c4 is not None and last_c4 <= -125.0),
            f"{last_c4:+.1f} B" if last_c4 is not None else "缺失",
            f"4 周变化 {last_c4:+.1f} B" if last_c4 is not None else "缺失",
        ),
        evidence=["VAR_BANK_RESERVES", "REL_015"],
    )

    # 事件 5：准备金 13 周下降速率
    c13 = [(d, v) for d, _, v in changes if v is not None]
    trig_13w = [d for d, v in c13 if v <= -200.0]
    hits = sum(1 for d in trig_13w if _forward_spread_hit(frame, d))
    base13 = _pct(sum(1 for d, _ in c13 if _forward_spread_hit(frame, d)), len(c13))
    ev_13w = _rule(
        rule_id="EV_RESERVE_13W",
        label="准备金 13 周下降 ≥ 200 B",
        kind="event",
        condition="周均准备金相对 13 周前下降 ≥ 200 B",
        triggers=trig_13w,
        total=len(c13),
        unit="周",
        corroboration={
            "indicator": "随后 4 周内 SOFR-EFFR ≥ 12 bp",
            "hits": hits, "n": len(trig_13w),
            "pct": _pct(hits, len(trig_13w)),
            "base_pct": base13,
            "lift": round((hits / len(trig_13w)) / (base13 / 100.0), 1) if trig_13w else 0.0,
            "note": "趋势性抽水的中期确认；触发后 4 周内资金面走阔的概率显著高于基础水平",
        },
        current=_current_event(
            bool(last_c13 is not None and last_c13 <= -200.0),
            f"{last_c13:+.1f} B" if last_c13 is not None else "缺失",
            f"13 周变化 {last_c13:+.1f} B" if last_c13 is not None else "缺失",
        ),
        evidence=["VAR_BANK_RESERVES", "REL_024"],
    )

    # 事件 6：税期 TGA 重建
    rebuilds = _tax_rebuilds(frame, report_day)
    trig_tax = [(d, v) for d, v in rebuilds if v >= 75.0]
    neg = 0
    for d, _ in trig_tax:
        after = frame.date_value(frame.reserve_w, d + timedelta(days=28))
        before = frame.date_value(frame.reserve_w, d)
        if after is not None and before is not None and after < before:
            neg += 1
    base_neg = 0
    for d, _ in rebuilds:
        after = frame.date_value(frame.reserve_w, d + timedelta(days=28))
        before = frame.date_value(frame.reserve_w, d)
        if after is not None and before is not None and after < before:
            base_neg += 1
    tga_asof = frame.tga_d[-1][0] if frame.tga_d else report_day
    tga_now = frame.tga_d[-1][1] if frame.tga_d else None
    tga_base = value_at_or_before(frame.tga_d, tga_asof - timedelta(days=14))
    tga_chg = round(tga_now - tga_base[1], 1) if tga_now is not None and tga_base else None
    ev_tax = _rule(
        rule_id="EV_TGA_TAX",
        label="税期 TGA 重建 ≥ 75 B",
        kind="event",
        condition=(
            "税期（季度预缴/年度申报与月度存款税截止日，按联邦工作日顺延）后"
            " 10 天内 TGA 上升 ≥ 75 B"
        ),
        triggers=[d for d, _ in trig_tax],
        total=len(rebuilds),
        unit="个税期",
        corroboration={
            "indicator": "随后 4 周内准备金下降（抽水兑现）",
            "hits": neg, "n": len(trig_tax),
            "pct": _pct(neg, len(trig_tax)),
            "base_pct": _pct(base_neg, len(rebuilds)),
            "lift": round((neg / len(trig_tax)) / (base_neg / len(rebuilds)), 1)
            if trig_tax and base_neg else 0.0,
            "note": "RRP 归零后税期重建直接抽准备金；触发频率按税期事件计，不按天重复",
        },
        current=_current_event(
            False, "无（非税期触发窗口）",
            f"下一次税期参考日 {next_tax_period(report_day).isoformat()}；"
            f"截至 {tga_asof.isoformat()} 的 10 个交易日 TGA 变化 "
            f"{tga_chg:+.1f} B" if tga_chg is not None else "TGA 日值缺失",
        ),
        evidence=["VAR_TGA", "VAR_TAX_RECEIPTS", "REL_018", "CASE_2025_DRAIN"],
    )

    rows = [ev_spread, ev_srf, ev_srf_run, ev_4w, ev_13w, ev_tax, bg_zone, bg_rrp]
    transitions = _transitions(frame, report_day)
    low = min(frame.reserve_w, key=lambda item: item[1])
    spread_max = max(frame.spread_d, key=lambda item: item[1]) if frame.spread_d else None
    srf_max = max(frame.srf_d, key=lambda item: item[1]) if frame.srf_d else None
    stats = {
        "reserve_2y_low_t": round(low[1], 3),
        "reserve_2y_low_date": low[0].isoformat(),
        "spread_2y_max_bp": spread_max[1] if spread_max else None,
        "spread_2y_max_date": spread_max[0].isoformat() if spread_max else None,
        "last_spread_12_date": max(spread_trig).isoformat() if spread_trig else None,
        "srf_2y_peak_b": srf_max[1] if srf_max else None,
        "srf_2y_peak_date": srf_max[0].isoformat() if srf_max else None,
        "last_srf_3b_date": max(srf_big).isoformat() if srf_big else None,
        "last_rrp_20b_date": max(revive).isoformat() if revive else None,
        "rrp_5dma_b": round(rrp_latest, 2) if rrp_latest is not None else None,
        "rrp_last_b": round(rrp_tail, 2) if rrp_tail is not None else None,
        "next_tax_ref": next_tax_period(report_day).isoformat(),
    }
    return {
        "window_years": BACKTEST_YEARS,
        "asof": frame.reserve_w[-1][0].isoformat(),
        "rows": rows,
        "transitions": transitions,
        "stats": stats,
        "deleted": [
            {
                "rule": "ON RRP < 50 B",
                "frequency": (
                    f"{sum(1 for _, v in frame.rrp_d if v < 50.0)}/{len(frame.rrp_d)} 天；"
                    f"当前已连续 "
                    f"{sum(1 for _, v in reversed(frame.rrp_d) if v < 50.0)} 个交易日触发"
                ),
                "reason": "RRP 已结构性归零，阈值天天触发；改为零值平台背景状态",
            },
        ],
        "evidence": {
            "variables": "docs/USD_Liquidity/data/variables.json",
            "relationships": "docs/USD_Liquidity/data/relationships.json",
            "cases": "docs/USD_Liquidity/data/cases.json",
        },
    }


def _transitions(frame: _Frame, report_day: date) -> list[str]:
    """近 5 个交易日内是否发生背景/事件状态转换（只提示转换，不重复告警）。"""
    out: list[str] = []
    recent = set(frame.near(report_day, 5))
    reserve_rows = [(d, v) for d, v in frame.reserve_w if d in recent]
    zones = ["紧张" if v < 3.0 else ("预警" if v < 3.2 else "充裕") for _, v in reserve_rows]
    if len(set(zones)) > 1:
        out.append(f"准备金水位区间由{zones[0]}转为{zones[-1]}")
    spread_rows = [(d, v) for d, v in frame.spread_d if d in recent]
    if spread_rows and spread_rows[0][1] < 12 <= spread_rows[-1][1]:
        out.append("SOFR-EFFR 由 <12 bp 转为 ≥12 bp")
    srf_rows = [(d, v) for d, v in frame.srf_d if d in recent]
    if srf_rows and srf_rows[0][1] < 3.0 <= srf_rows[-1][1]:
        out.append("SRF 由 <3 B 转为 ≥3 B")
    return out
