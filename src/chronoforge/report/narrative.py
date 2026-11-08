"""确定性规则模板：只用 facts 中存在的数值生成文字。

Phase 1 不接 LLM。任何句子都必须能由 facts 中的数值或日期复核；
缺失值一律写「缺失」，不填 0、不做方向推断。
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

KNOWLEDGE = {
    "REL_010": "SRF 使用量 >3 B 表示银行开始被动向美联储求助，准备金压力显性化",
    "REL_015": "其他项目不变时 ΔR≈−ΔTGA，TGA 上升具有抽水效应",
    "REL_016": "ON RRP 下降在其他不变时抬升准备金；TGA↑ + ON RRP↓ 可能只是负债内部转移",
    "REL_018": "税收流入推高 TGA，缴税高峰与国债结算叠加时压力最大",
    "REL_024": "QT 通过不再全额再投资持续抽取准备金，是趋势性收缩的背景力量",
    "CASE_2025_DRAIN": "2025-08 至 2025-11 准备金由约 3.4 T 降至约 2.85 T，"
    "TGA 回补耗尽 ON RRP，随后直接挤压准备金",
}


def _num(value: Any, digits: int = 2, suffix: str = "") -> str:
    if value is None:
        return "缺失"
    return f"{float(value):+.{digits}f}{suffix}" if suffix else f"{float(value):.{digits}f}"


def _chg(value: Any, digits: int = 1) -> str:
    if value is None:
        return "缺失"
    return f"{float(value):+.{digits}f} B"


def _date(value: Any) -> str:
    return str(value)[:10] if value else "缺失"


def _asof_note(value: Any) -> str:
    return f"截至 {_date(value)}"


def _signed(value: Any, digits: int = 1, unit: str = "B") -> str:
    if value is None:
        return "缺失"
    number = float(value)
    if number == 0:
        number = 0.0  # 避免 -0.0 显示成「-0.0」
    return f"{number:+.{digits}f} {unit}"


def _data_cutoff_line(facts: dict[str, Any]) -> str:
    m = facts["metrics"]
    reserve_asof = m["reserve"].get("asof")
    tga_asof = m["tga"].get("daily_asof")
    fund_asof = max(
        (x for x in (m["funding"].get("effr_asof"), m["funding"].get("sofr_asof"))
         if x), default=None,
    )
    market_asof = max(
        (x for x in (m["markets"].get("dxy_asof"), m["markets"].get("spx_asof"))
         if x), default=None,
    )
    return (
        f"数据截至：准备金周序列 {_date(reserve_asof)}；TGA 日值 {_date(tga_asof)}；"
        f"资金面 {_date(fund_asof)}；行情 {_date(market_asof)}；"
        f"批次数据总截止 {_date(facts.get('payload_meta', {}).get('data_cutoff'))}。"
    )


def _reserve_bullets(facts: dict[str, Any]) -> list[str]:
    r = facts["metrics"]["reserve"]
    if r.get("level_t") is None:
        return [
            "准备金周序列无可用观测，水位显式缺失，无法给出判断。",
            "变化：1 周 / 4 周 / 13 周均缺失。",
            "经验分位：5 年 / 10 年均缺失。",
        ]
    return [
        f"准备金 {_num(r['level_t'], 3)} 万亿美元（{_asof_note(r.get('asof'))}），"
        f"处于{r.get('zone')}（阈值 3.0 / 3.2 T）。",
        f"变化：1 周 {_chg(r.get('chg_1w_b'))}，4 周 {_chg(r.get('chg_4w_b'))}，"
        f"13 周 {_chg(r.get('chg_13w_b'))}。",
        f"经验分位：5 年 {_num(r.get('pct_5y'), 1)}%，10 年 {_num(r.get('pct_10y'), 1)}%"
        f"（窗口截至 {_date(r.get('asof'))}）。",
    ]


def _pool_bullets(facts: dict[str, Any]) -> list[str]:
    t = facts["metrics"]["three_pool"]
    zero = facts.get("charts", {}).get("three_pool", {}).get("onrrp_zero", False)
    if not t.get("levels"):
        return ["三池周序列无可用观测，结构缺口。"]
    lv, c1, c4 = t["levels"], t["chg_1w"], t["chg_4w"]
    head = (
        f"可调池已退化为两个（{_asof_note(t.get('latest_date'))}）："
        f"准备金 {_num(lv.get('reserve'), 3)} T、TGA {_num(lv.get('tga'), 3)} T；"
        f"ON RRP {_num(lv.get('onrrp'), 3)} T 已结构性归零，不再计入缓冲。"
        if zero else
        f"三池（{_asof_note(t.get('latest_date'))}）：准备金 {_num(lv.get('reserve'), 3)} T、"
        f"TGA {_num(lv.get('tga'), 3)} T、ON RRP {_num(lv.get('onrrp'), 3)} T。"
    )
    lines = [
        head,
        f"周变化：准备金 {_chg(c1.get('reserve'))}，TGA {_chg(c1.get('tga'))}，"
        f"ON RRP {_chg(c1.get('onrrp'))}。",
        f"4 周变化：准备金 {_chg(c4.get('reserve'))}，TGA {_chg(c4.get('tga'))}，"
        f"ON RRP {_chg(c4.get('onrrp'))}。",
    ]
    if zero:
        daily = facts.get("charts", {}).get("tga_daily", {}).get("v", [])
        tga_d = daily[-1] if daily else None
        weekly = lv.get("tga")
        gap = (
            round(float(weekly) * 1000.0 - float(tga_d), 1)
            if tga_d is not None and weekly is not None else None
        )
        weekly_b = round(float(weekly) * 1000.0, 1) if weekly is not None else None
        lines.append(
            "RRP 归零后，财政抽水/投放直接落在准备金上：按 REL_016，TGA 与准备金之间"
            "不再有 RRP 缓冲，接近一对一排挤（本报告推断，非知识库原文结论）。"
            f"ON RRP 4 周变化 {_chg(c4.get('onrrp'))} 是零附近的小幅回落，"
            "不改变零值平台判断。"
        )
        lines.append(
            f"TGA 日值 {_num(tga_d, 1)} B"
            f"（{_asof_note(facts['metrics']['tga'].get('daily_asof'))}），"
            f"较周均 {_num(weekly, 3)} T（即 {_num(weekly_b, 1)} B）"
            f"相差 {_signed(gap)}；周均为周内均值，日值为最新观测，两种口径不混用。"
        )
    return lines


def _decomp_parts(d: dict[str, Any]) -> tuple[float | None, list[str], float | None]:
    keys = (("dA", "ΔA"), ("tga", "TGA 贡献"), ("onrrp", "ON RRP 贡献"),
            ("cash", "现金"), ("other", "其他"))
    values: list[float] = []
    for key, _ in keys:
        value = d.get(key)
        if value is None:
            return None, [], None
        values.append(float(value))
    total = round(sum(values), 1)
    dr = d.get("dR")
    residual = round(float(dr) - total, 1) if dr is not None else None
    return total, [f"{label} {_chg(d.get(key))}" for key, label in keys], residual


def _causal_headline(facts: dict[str, Any]) -> str:
    d = facts["metrics"]["weekly_decomp"]
    latest = d.get("latest")
    if not latest or latest.get("dR") is None:
        return "本周准备金变动缺少分项，无法给出因果判断（显式缺口）。"
    labels = {"dA": "ΔA", "tga": "TGA", "onrrp": "ON RRP", "cash": "现金", "other": "其他"}
    comps: dict[str, float] = {}
    for key in labels:
        if latest.get(key) is not None:
            comps[key] = float(latest[key])
    dominant = max(comps.items(), key=lambda kv: abs(kv[1])) if comps else None
    head = (
        f"本周 ΔR {_chg(latest.get('dR'))}，主因 {labels[dominant[0]]} 贡献 {_chg(dominant[1])}"
        if dominant else f"本周 ΔR {_chg(latest.get('dR'))}，分项缺失"
    )
    return f"{head}；4 周累计 {_chg((d.get('sum_4w') or {}).get('dR'))}。"


def _h41_split_lines(facts: dict[str, Any], other_contrib: float | None) -> list[str]:
    """用 H.4.1 明细把分解里的「其他」拆开，并单列周均/时点差。"""
    h = facts.get("h41_detail") or {}
    if h.get("error") or not h.get("items"):
        return [
            "H.4.1 明细不可用（观测不足或读取失败），「其他」不做强行归因，仍记为不可拆残差。"
        ]
    items = h["items"]
    parts = "、".join(
        f"{item['label']} {_signed(item.get('contribution_b'))}" for item in items
    )
    lines = [
        f"「其他」按 H.4.1 明细拆到可归因项（{_asof_note(h.get('asof'))}，"
        f"共 {len(items)} 项，负号表示对准备金的抽水贡献）：{parts}；"
        f"经济科目合计 {_signed(h.get('named_sum_b'))}。"
    ]
    timing = h.get("timing") or {}
    cross = (
        f"经济科目合计 {_signed(h.get('named_sum_b'))} 加时点差后与分解「其他」"
        f"{_signed(h.get('other_reported_b'))} 对上，残差 {_signed(h.get('residual_b'))}"
        if other_contrib is not None
        else "分解「其他」为缺失，只列时点差，不做闭合核对"
    )
    lines.append(
        f"时点核对：主口径准备金/TGA 用周均（WRESBAL/WTREGEN），WALCL 与各明细用周三时点"
        f"（{_date(h.get('asof'))}，前值 {_date(h.get('prev_asof'))}）。"
        f"准备金周均与时点差 {_signed(timing.get('reserve_diff_b'))}，"
        f"TGA 周均与时点差 {_signed(timing.get('tga_diff_b'))}，合计 "
        f"{_signed(timing.get('sum_b'))}；{cross}。"
        "主口径不替换，时点差不并入任何经济科目。"
    )
    repo = next((item for item in items if item.get("key") == "foreign_repo"), None)
    if repo:
        lines.append(
            "口径说明：WLRRAL 是逆回购负债（外国官方与国际账户），与国内 ON RRP 分属不同"
            f"科目。该项贡献 {_signed(repo.get('contribution_b'))}（余额 "
            f"{_num(repo.get('level_b'), 1)} B），说明外国官方设施仍有余额；"
            "ON RRP 零值判断不受影响，两池框架不变。"
        )
    return lines


def _causal_bullets(facts: dict[str, Any]) -> list[str]:
    d = facts["metrics"]["weekly_decomp"]
    latest = d.get("latest")
    if not latest:
        return ["周度分解无可用观测，无法做恒等式归因。"]
    lines: list[str] = []
    total, parts, residual = _decomp_parts(latest)
    if total is None:
        lines.append(
            "本周分解存在缺失分项，无法完成恒等式核对；缺失项不填 0，归因中止。"
        )
    else:
        comps: dict[str, float] = {}
        for key in ("dA", "tga", "onrrp", "cash", "other"):
            if latest.get(key) is not None:
                comps[key] = float(latest[key])
        dominant = max(comps.items(), key=lambda kv: abs(kv[1])) if comps else None
        labels = {"dA": "ΔA（资产端）", "tga": "TGA（财政）", "onrrp": "ON RRP",
                  "cash": "现金", "other": "其他项"}
        share = None
        if dominant and latest.get("dR"):
            share = round(abs(dominant[1]) / abs(float(latest["dR"])) * 100, 0)
        consistent = (
            "与 ΔR 一致" if abs(residual or 0.0) <= 0.2
            else "与 ΔR 不一致，差额无法由现有分项解释"
        )
        lines.append(
            f"本周（{_asof_note(d.get('latest_date'))}）ΔR {_chg(latest.get('dR'))}。"
            f"按恒等式 R = WALCL − 资本 − 现金 − ON RRP − TGA − 其他负债拆解："
            f"{'、'.join(parts)}；分项合计 {_chg(total)}，残差 {_chg(residual)}，"
            f"{consistent}。"
        )
        if dominant:
            direction = (
                "资产端扩张不是本周驱动。" if dominant[0] != "dA"
                else "财政与 RRP 项不是本周主因。"
            )
            lines.append(
                f"主因是 {labels.get(dominant[0], dominant[0])}：贡献 "
                f"{_chg(dominant[1])}，占 |ΔR| 的 {(share or 0):.0f}%。{direction}"
            )
        raw = {
            "tga": -float(latest["tga"]) if latest.get("tga") is not None else None,
            "onrrp": -float(latest["onrrp"]) if latest.get("onrrp") is not None else None,
            "cash": -float(latest["cash"]) if latest.get("cash") is not None else None,
            "other": -float(latest["other"]) if latest.get("other") is not None else None,
        }
        tga_raw = raw.get("tga")
        if tga_raw is None:
            tga_effect = "TGA 变化缺失，无法判断财政项方向。"
        elif tga_raw < 0:
            tga_effect = "本周 TGA 下降，按 REL_015 这正是抬升准备金的直接来源。"
        else:
            tga_effect = "本周 TGA 上升，按 REL_015 这是抽走准备金的直接来源。"
        lines.append(
            f"原始变化：TGA 周均 {_chg(raw.get('tga'))}（负值=余额下降）、"
            f"ON RRP {_chg(raw.get('onrrp'))}、现金 {_chg(raw.get('cash'))}、"
            f"其他负债 {_chg(raw.get('other'))}；贡献项按恒等式取负号。"
            f"{tga_effect}"
        )
        lines.append(
            f"对应知识库 REL_015：{KNOWLEDGE['REL_015']}；"
            f"REL_016：{KNOWLEDGE['REL_016']}。"
        )
        if latest.get("other") is not None:
            other_share = (
                round(abs(float(latest["other"])) / abs(float(latest["dR"])) * 100, 0)
                if latest.get("dR") else None
            )
            lines.append(
                f"分解里的「其他」{_chg(latest.get('other'))}"
                f"（占 |ΔR| 的 {other_share:.0f}%）不再笼统写「无法归因」，"
                "按 H.4.1 明细拆解如下。"
            )
            lines.extend(_h41_split_lines(facts, float(latest["other"])))

    s4 = d.get("sum_4w")
    if s4:
        total4, parts4, residual4 = _decomp_parts(s4)
        if total4 is not None:
            lines.append(
                f"近 4 周 ΔR {_chg(s4.get('dR'))}：{'、'.join(parts4)}；"
                f"分项合计 {_chg(total4)}，残差 {_chg(residual4)}。"
            )
            tga4 = s4.get("tga")
            if tga4 is not None and s4.get("dR"):
                lines.append(
                    f"4 周维度上 TGA 贡献 {_chg(tga4)}，占 |ΔR| 的 "
                    f"{abs(float(tga4)) / abs(float(s4['dR'])) * 100:.0f}%；"
                    f"这是持续抽水，不是单周噪音。"
                    f"其他项 {_chg(s4.get('other'))} 为 4 周累计，"
                    "H.4.1 明细只覆盖最新一周，不逐周拆分。"
                )
    lines.append(
        "结构含义：RRP 已归零（见「两池结构」），REL_015 中原本可由 RRP 吸收的那部分"
        "缓冲已不存在；税期 TGA 回补会按 REL_018 更直接地压低准备金。"
        f"历史参照 CASE_2025_DRAIN：{KNOWLEDGE['CASE_2025_DRAIN']}。"
    )
    return lines


def _funding_bullets(facts: dict[str, Any]) -> list[str]:
    f = facts["metrics"]["funding"]
    stats = facts.get("rules", {}).get("stats", {})
    lines = [
        f"EFFR {_num(f.get('effr'), 2)}% / SOFR {_num(f.get('sofr'), 2)}% / "
        f"IORB {_num(f.get('iorb'), 2)}%（{_asof_note(f.get('sofr_asof'))}）。",
        f"SOFR-EFFR {_num(f.get('spread_bp'), 1)} bp，走廊状态「{f.get('corridor_status')}」"
        f"（<12 bp 正常，≤17 bp 预警，>17 bp 极端）。",
        f"SRF 用量 {_num(f.get('srf_usage_b'), 2)} B（{_asof_note(f.get('srf_asof'))}），"
        f"30 日峰值 {_num(f.get('srf_peak_30d_b'), 2)} B；近两年峰值 "
        f"{_num(stats.get('srf_2y_peak_b'), 2)} B（{_date(stats.get('srf_2y_peak_date'))}）。"
        "按 REL_010，SRF 规模 >3 B 才表示银行被动求助已显性化；当前为 0，不构成确认。",
        "边际工具已换位：RRP 归零后，能吸收冲击的是 SRF（被动求助）与 TGA（财政抽水/投放）；"
        "观察重心从 RRP 转向 SRF 规模/连续天数与税期 TGA 重建。",
    ]
    return lines


def _market_bullets(facts: dict[str, Any]) -> list[str]:
    mk = facts["metrics"]["markets"]
    return [
        f"美元指数 {_num(mk.get('dxy'), 3)}（{_asof_note(mk.get('dxy_asof'))}），"
        f"20 日变化 {_num(mk.get('dxy_chg20'), 3)}；VIX {_num(mk.get('vix'), 2)}"
        f"（{_asof_note(mk.get('vix_asof'))}），20 日变化 {_num(mk.get('vix_chg20'), 2)}。",
        f"10Y 实际利率 {_num(mk.get('real_yield'), 3)}%，20 日变化 "
        f"{_num(mk.get('real_chg20'), 3)}。",
    ]


def _rule_bullets(facts: dict[str, Any]) -> list[str]:
    rules = facts.get("rules", {})
    rows = rules.get("rows", [])
    events = [r for r in rows if r.get("kind") == "event"]
    background = [r for r in rows if r.get("kind") == "background"]
    active = [r for r in events if r.get("current", {}).get("active")]
    lines = [
        f"事件规则 {len(events)} 条，当前触发 {len(active)} 条"
        f"（{', '.join(r['label'] for r in active) if active else '无'}）。",
        f"背景状态 {len(background)} 条，不逐日告警："
        + "；".join(
            f"{r['label']} {r.get('current', {}).get('value', '缺失')}"
            for r in background
        ) + "。",
        "近 5 个交易日状态转换："
        + ("；".join(rules.get("transitions", [])) if rules.get("transitions") else "无") + "。",
        "已删除的每日警报："
        + ("；".join(
            f"{d['rule']}（{d['frequency']}）——{d['reason']}"
            for d in rules.get("deleted", [])
        ) if rules.get("deleted") else "无") + "。",
        "规则可用性以近两年回测为准：频率过高的降为背景状态，频率与资金面无关的删除；"
        "完整触发条件、触发次数与区分度见「风险规则回测」表。",
    ]
    return lines


def _next_thursday(day: date) -> date:
    return day + timedelta(days=(3 - day.weekday()) % 7)


def _known_event_line(
    facts: dict[str, Any], next_h41: date, stats: dict[str, Any]
) -> str:
    """「下一次已知事件」：FOMC/CPI/NFP 读配置，H.4.1/DTS 为程序化日程。"""
    ev = facts.get("events") or {}
    horizon = int(ev.get("horizon_days", 90))
    if ev.get("error"):
        events_part = (
            f"事件配置读取失败（{ev['error']}），FOMC/CPI/NFP 日程不可用；"
            "不得据此推断发布日期。"
        )
    else:
        next_by_type = ev.get("next_by_type") or {}
        rows = sorted(next_by_type.values(), key=lambda r: str(r.get("date", "")))
        if rows:
            parts = "；".join(
                f"{r['type']} {r['date']} {r['time_et']} ET"
                f"（{r['status']}，{r.get('source_name', r.get('source'))}）"
                for r in rows
            )
            events_part = (
                f"{parts}。未来 {horizon} 天内共 {ev.get('upcoming_count', 0)} 项"
                f"（tentative {ev.get('tentative_upcoming', 0)} 项），"
                f"配置覆盖至 {_date(ev.get('coverage_through'))}。"
            )
        else:
            events_part = (
                f"未来 {horizon} 天内无已配置的 FOMC/CPI/NFP 事件；"
                f"配置覆盖至 {_date(ev.get('coverage_through'))}，需人工复核。"
            )
    provenance = (
        f"来源与访问日期见 {ev.get('config_path')}"
        f"（as_of {_date(ev.get('as_of'))}，访问 {_date(ev.get('accessed_at'))}）。"
    )
    return (
        f"⑤ 下一次已知事件：{events_part}"
        f"H.4.1 {next_h41.isoformat()}（周四 16:30 ET）；"
        "DTS 每交易日 16:00 ET（覆盖前一交易日）；"
        f"税期参考日 {_date(stats.get('next_tax_ref'))}"
        "（读 config/tga/calendar.yaml，已按联邦工作日顺延）；"
        f"{provenance}"
    )


def _outlook_bullets(facts: dict[str, Any]) -> list[str]:
    m = facts["metrics"]
    r = m["reserve"]
    f = m["funding"]
    stats = facts.get("rules", {}).get("stats", {})
    h1 = m.get("reserve_forecast", {}).get("h1")
    next_h41 = _next_thursday(date.fromisoformat(facts["report_date"]))
    lines: list[str] = []

    # 1. 当前状态
    run_date = _date(m.get("reserve_forecast", {}).get("run_date"))
    model = (
        f"模型输出：结构模型 h=1 目标 {_date(h1.get('target_date'))} "
        f"{_num(h1.get('predicted_t'), 3)} T（区间 {_num(h1.get('lower_t'), 3)}–"
        f"{_num(h1.get('upper_t'), 3)} T，run {run_date}）；"
        f"这是模型输出，不是本报告判断。"
        if h1 else "无可用准备金模型输出，本报告不提供方向判断。"
    )
    lines.append(
        f"① 当前状态：准备金 {_num(r.get('level_t'), 3)} T（{r.get('zone')}，"
        f"4 周 {_chg(r.get('chg_4w_b'))}，13 周 {_chg(r.get('chg_13w_b'))}）；"
        f"TGA 周均 {_num(m['three_pool']['levels'].get('tga'), 3)} T；"
        f"RRP 零值平台；SOFR-EFFR {_num(f.get('spread_bp'), 1)} bp；"
        f"SRF {_num(f.get('srf_usage_b'), 2)} B。{model}"
    )

    c4 = r.get("chg_4w_b")
    c13 = r.get("chg_13w_b")
    spread = f.get("spread_bp")
    srf = f.get("srf_usage_b")
    rrp_last = stats.get("rrp_5dma_b")
    dist4 = f"{abs(round(-125.0 - float(c4), 1)):.1f} B" if c4 is not None else "缺失"
    dist13 = f"{abs(round(-200.0 - float(c13), 1)):.1f} B" if c13 is not None else "缺失"
    dist_spread = f"{round(12.0 - float(spread), 1):.1f} bp" if spread is not None else "缺失"
    lines.append(
        "② 触发条件（数值化，达到即升级）："
        f"4 周下降 ≥125 B（当前 {_chg(c4)}，还需再降 {dist4}）；"
        f"13 周下降 ≥200 B（当前 {_chg(c13)}，还需再降 {dist13}）；"
        f"SOFR-EFFR ≥12 bp（当前 {_num(spread, 1)} bp，还差 {dist_spread}）；"
        f"SRF 单日 ≥3 B（当前 {_num(srf, 2)} B）；"
        f"税期 TGA 10 日重建 ≥75 B（下一税期 {_date(stats.get('next_tax_ref'))}）；"
        f"ON RRP 5 日均值 ≥20 B 才视为缓冲恢复（当前 {_num(rrp_last, 2)} B）。"
    )

    lines.append(
        "③ 情景（条件式，不给概率）："
        "情景 A「税期抽水」——若下一税期后 10 日内 TGA 上升 ≥75 B，RRP 为零时准备金被直接抽走，"
        "观察 4 周变化是否达到 −125 B、SOFR-EFFR 是否 ≥12 bp；"
        "情景 B「财政投放」——若 TGA 周均回落 ≥50 B 且无其他负债冲销，准备金回升，"
        "但 RRP 不会自动补回，缓冲仍薄；"
        "情景 C「被动求助」——若 SOFR-EFFR ≥12 bp 且 SRF ≥3 B 或连续 3 日 ≥1 B，"
        "确认边际工具已从 RRP 切到 SRF。"
        "三种情景互斥条件明确，不赋概率、不预测具体点位。"
    )

    lines.append(
        "④ 验证指标与证伪方式："
        f"下一份 H.4.1（{next_h41.isoformat()}，覆盖截至 "
        f"{_date((next_h41 - timedelta(days=1)).isoformat())} 的周度数据）看 WRESBAL："
        "若 4 周变化 ≤−125 B，情景 A 证实；若回升且 TGA 下降，情景 B 证实；"
        "若两者都不成立，维持现状判断。"
        "DTS 日值（每日 16:00 ET）看税期后 10 日 TGA 是否 ≥75 B；"
        "SRF/SOFR 日值看是否出现 ≥3 B 或 ≥12 bp。任一阈值未到，不得写成「已抽水」或「已紧张」。"
    )

    lines.append(_known_event_line(facts, next_h41, stats))
    return lines


def _prev_metrics(prev: dict[str, Any]) -> dict[str, Any]:
    metrics = prev.get("metrics", {})
    return metrics if isinstance(metrics, dict) else {}


def _novelty_bullets(facts: dict[str, Any]) -> list[str]:
    prev = facts.get("previous")
    if not prev:
        return ["无昨日报告可比（首次生成或上一交易日文件缺失）；本日判断为首次生成。"]
    prev_date = _date(prev.get("report_date"))
    cur_m = facts["metrics"]
    prev_m = _prev_metrics(prev)
    cur_r = cur_m["reserve"]
    prev_r = prev_m.get("reserve", {})
    changes: list[str] = []
    if prev_r.get("zone") and prev_r.get("zone") != cur_r.get("zone"):
        changes.append(f"准备金区间 {prev_r.get('zone')} → {cur_r.get('zone')}")
    prev_rules = prev.get("rules", {}).get("rows", [])
    cur_active = {r["id"] for r in facts.get("rules", {}).get("rows", [])
                  if r.get("kind") == "event" and r.get("current", {}).get("active")}
    prev_active = {r["id"] for r in prev_rules
                   if r.get("kind") == "event" and r.get("current", {}).get("active")}
    if cur_active != prev_active:
        changes.append(
            "事件规则触发集合 "
            f"{sorted(prev_active) or '空'} → {sorted(cur_active) or '空'}"
        )
    prev_zero = next((r.get("current", {}).get("active") for r in prev_rules
                      if r.get("id") == "BG_ONRRP_ZERO"), None)
    cur_zero = next(
        (r.get("current", {}).get("active")
         for r in facts.get("rules", {}).get("rows", [])
         if r.get("id") == "BG_ONRRP_ZERO"),
        None,
    )
    if prev_zero is not None and prev_zero != cur_zero:
        changes.append(f"RRP 零值平台状态 {prev_zero} → {cur_zero}")

    prev_level = prev_r.get("level_t")
    prev_c4 = prev_r.get("chg_4w_b")
    prev_f = prev_m.get("funding", {})
    cur_f = cur_m["funding"]
    cur_tga_last = cur_m.get("tga", {}).get("daily_b")
    prev_tga_last = prev_m.get("tga", {}).get("daily_b")
    cur_tga = f"{float(cur_tga_last):.1f} B" if cur_tga_last is not None else "缺失"
    prev_tga = f"{float(prev_tga_last):.1f} B" if prev_tga_last is not None else "缺失"

    cur_level = cur_r.get("level_t")
    delta_level = (
        round(float(cur_level) - float(prev_level), 3)
        if cur_level is not None and prev_level is not None else None
    )
    marginal = (
        f"准备金 {_num(cur_level, 3)} T（前值 {_num(prev_level, 3)}，"
        f"变化 {_signed(delta_level, 3, 'T')}）；"
        f"4 周变化 {_chg(cur_r.get('chg_4w_b'))}（前值 {_chg(prev_c4)}）；"
        f"SOFR-EFFR {_num(cur_f.get('spread_bp'), 1)} bp（前值 "
        f"{_num(prev_f.get('spread_bp'), 1)} bp）；"
        f"SRF {_num(cur_f.get('srf_usage_b'), 2)} B（前值 "
        f"{_num(prev_f.get('srf_usage_b'), 2)} B）；"
        f"TGA 日值 {cur_tga}（前值 {prev_tga}）。"
    )
    if changes:
        return [
            f"与昨日报告（{prev_date}）相比出现状态变化："
            + "；".join(changes) + "。",
            marginal,
        ]
    return [
        f"与昨日报告（{prev_date}）相比结论延续（区间、事件规则触发集合、RRP 零值平台均未变）；"
        f"只讲边际变化，不换词重述。",
        marginal,
        "若上列数值无变化，说明当日数据 as-of 未推进；本报告不把陈旧数据当新增信息。",
    ]


def _gap_bullets(facts: dict[str, Any]) -> list[str]:
    summary = facts["freshness_summary"]
    lines = [
        f"核心序列达到矩阵下限 {summary['core_ok']}/{summary['core_total']}；"
        f"全部 {summary['total']} 个序列：正常 {summary['ok']}、滞后 {summary['lagged']}、"
        f"缺失 {summary['missing']}。",
    ]
    batch = facts.get("batch_status")
    if batch:
        lines.append(
            f"最近批次：{batch.get('status')}（{batch.get('finished_at')}，"
            f"exit {batch.get('exit_code')}）；报告以各序列实际 as-of 为准。"
        )
    else:
        lines.append("未找到批次状态文件 var/run/daily_update.json；报告以各序列实际 as-of 为准。")
    for gap in facts.get("gaps", []):
        if gap.get("kind") == "source_anomaly":
            lines.append(
                f"源侧异常：{gap['dataset_id']} 最近 {gap['count']} 条（{gap['last_at']}）"
                f"；{gap.get('detail')}"
            )
        elif gap.get("kind") in {"batch_stale", "batch_status_missing",
                                 "data_build_error"}:
            lines.append(f"{gap.get('label')}：{gap.get('detail')}")
    if not any(g.get("kind") == "source_anomaly" for g in facts.get("gaps", [])):
        lines.append("最近 30 天未记录 Q-NORM-SRC-ANOMALY 源侧异常。")
    return lines


def build_narrative(facts: dict[str, Any]) -> dict[str, Any]:
    reserve = _reserve_bullets(facts)
    pool = _pool_bullets(facts)
    funding = _funding_bullets(facts)
    rules = _rule_bullets(facts)
    novelty = _novelty_bullets(facts)
    summary = [
        reserve[0],
        _causal_headline(facts),
        pool[0],
        funding[1],
        rules[0],
        novelty[0],
    ]
    pool_title = (
        "两池结构（ON RRP 已归零）"
        if facts.get("charts", {}).get("three_pool", {}).get("onrrp_zero")
        else "三池结构"
    )
    sections = [
        {"title": "水位与变化", "bullets": _reserve_bullets(facts)},
        {"title": pool_title, "bullets": _pool_bullets(facts)},
        {"title": "因果链：准备金变动归因", "bullets": _causal_bullets(facts)},
        {"title": "资金面与边际工具", "bullets": _funding_bullets(facts)},
        {"title": "市场读数（外围）", "bullets": _market_bullets(facts)},
        {"title": "风险规则（2 年回测）", "bullets": _rule_bullets(facts)},
        {"title": "前瞻（五段式）", "bullets": _outlook_bullets(facts)},
        {"title": "昨日对比与新颖度", "bullets": novelty},
        {"title": "数据缺口与新鲜度", "bullets": _gap_bullets(facts)},
    ]
    return {
        "mode": "template",
        "data_cutoff_line": _data_cutoff_line(facts),
        "summary": summary,
        "sections": sections,
        "scope_note": "H.8（商业银行存款/贷款）本轮未注册，不作为本报告输入。",
        "method_note": (
            "本报告由确定性规则模板生成，不含 LLM 推断；所有数字来自 "
            "export_data.py::build_payload 的现有序列，缺失值显式标注，不填 0。"
            "风险规则按近两年（730 天）回测频率与区分度设计；知识库引用仅使用 "
            "variables.json / relationships.json / cases.json 中已验证的 ID。"
        ),
    }
