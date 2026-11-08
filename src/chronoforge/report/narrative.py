"""确定性规则模板：只用 facts 中存在的数值生成文字。

Phase 1 不接 LLM。任何句子都必须能由 facts 中的数值或日期复核；
缺失值一律写「缺失」，不填 0、不做方向推断。
"""

from __future__ import annotations

from typing import Any


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
    if not t.get("levels"):
        return ["三池周序列无可用观测，结构缺口。"]
    lv, c1, c4 = t["levels"], t["chg_1w"], t["chg_4w"]
    return [
        f"三池（{_asof_note(t.get('latest_date'))}）：准备金 {_num(lv.get('reserve'), 3)} T、"
        f"TGA {_num(lv.get('tga'), 3)} T、ON RRP {_num(lv.get('onrrp'), 3)} T。",
        f"周变化：准备金 {_chg(c1.get('reserve'))}，TGA {_chg(c1.get('tga'))}，"
        f"ON RRP {_chg(c1.get('onrrp'))}。",
        f"4 周变化：准备金 {_chg(c4.get('reserve'))}，TGA {_chg(c4.get('tga'))}，"
        f"ON RRP {_chg(c4.get('onrrp'))}。",
    ]


def _decomp_bullets(facts: dict[str, Any]) -> list[str]:
    d = facts["metrics"]["weekly_decomp"]
    if not d.get("latest"):
        return ["周度分解无可用观测，缺口。"]
    x = d["latest"]
    keys = (("dR", "ΔR"), ("dA", "ΔA"), ("tga", "TGA 贡献"),
            ("onrrp", "ON RRP 贡献"), ("cash", "现金"), ("other", "其他"))
    parts = "、".join(f"{label} {_chg(x[key])}" for key, label in keys)
    return [
        f"最新一周（{_asof_note(d.get('latest_date'))}）：{parts}。",
        f"近 4 周累计：ΔR {_chg(d['sum_4w'].get('dR'))}，TGA 贡献 "
        f"{_chg(d['sum_4w'].get('tga'))}，ON RRP 贡献 {_chg(d['sum_4w'].get('onrrp'))}。",
    ]


def _funding_bullets(facts: dict[str, Any]) -> list[str]:
    f = facts["metrics"]["funding"]
    return [
        f"EFFR {_num(f.get('effr'), 2)}% / SOFR {_num(f.get('sofr'), 2)}% / "
        f"IORB {_num(f.get('iorb'), 2)}%（{_asof_note(f.get('sofr_asof'))}）。",
        f"SOFR-EFFR {_num(f.get('spread_bp'), 1)} bp，走廊状态「{f.get('corridor_status')}」"
        f"（<12 bp 正常，≤17 bp 预警，>17 bp 极端）。",
        f"SRF 用量 {_num(f.get('srf_usage_b'), 2)} B（{_asof_note(f.get('srf_asof'))}），"
        f"30 日峰值 {_num(f.get('srf_peak_30d_b'), 2)} B。",
    ]


def _market_bullets(facts: dict[str, Any]) -> list[str]:
    mk = facts["metrics"]["markets"]
    return [
        f"美元指数 {_num(mk.get('dxy'), 3)}（{_asof_note(mk.get('dxy_asof'))}），"
        f"20 日变化 {_num(mk.get('dxy_chg20'), 3)}；VIX {_num(mk.get('vix'), 2)}"
        f"（{_asof_note(mk.get('vix_asof'))}），20 日变化 {_num(mk.get('vix_chg20'), 2)}。",
        f"10Y 实际利率 {_num(mk.get('real_yield'), 3)}%，20 日变化 "
        f"{_num(mk.get('real_chg20'), 3)}。",
    ]


def _risk_bullets(facts: dict[str, Any]) -> list[str]:
    r = facts["metrics"]["reserve"]
    f = facts["metrics"]["funding"]
    pool = facts["metrics"]["three_pool"]
    level = r.get("level_t")
    spread = f.get("spread_bp")
    srf = f.get("srf_usage_b")
    onrrp_t = pool.get("levels", {}).get("onrrp")
    onrrp = round(onrrp_t * 1000.0, 1) if onrrp_t is not None else None
    rows = []

    if level is None:
        rows.append("准备金水位规则：数值缺失，无法判定。")
    else:
        rows.append(f"准备金水位 <3.0 T：{'已触发' if level < 3.0 else '未触发'}"
                    f"（实际 {_num(level, 3)} T）。")
    if spread is None:
        rows.append("SOFR-EFFR ≥12 bp：数值缺失，无法判定。")
    else:
        rows.append(f"SOFR-EFFR ≥12 bp：{'已触发' if spread >= 12 else '未触发'}"
                    f"（实际 {_num(spread, 1)} bp）。")
    if srf is None:
        rows.append("SRF 用量 ≥3 B：数值缺失，无法判定。")
    else:
        rows.append(f"SRF 用量 ≥3 B：{'已触发' if srf >= 3 else '未触发'}"
                    f"（实际 {_num(srf, 2)} B）。")
    if onrrp is None:
        rows.append("ON RRP <50 B：数值缺失，无法判定。")
    else:
        rows.append(f"ON RRP <50 B（缓冲不足）：{'已触发' if onrrp < 50 else '未触发'}"
                    f"（实际 {_num(onrrp, 1)} B）。")
    return rows


def _outlook_bullets(facts: dict[str, Any]) -> list[str]:
    rf = facts["metrics"].get("reserve_forecast", {})
    lines: list[str] = []
    h1 = rf.get("h1")
    if h1:
        lines.append(
            f"结构模型（run {_date(rf.get('run_date'))}，origin "
            f"{_date(rf.get('origin_date'))}）h=1 目标 {_date(h1.get('target_date'))}："
            f"{_num(h1.get('predicted_t'), 3)} T（区间 {_num(h1.get('lower_t'), 3)}–"
            f"{_num(h1.get('upper_t'), 3)} T）。该数值为模型输出，不是本报告判断。"
        )
    else:
        lines.append("无可用准备金预测，本报告不提供方向判断。")
    tg = facts["metrics"].get("tga", {})
    if tg.get("scenarios"):
        lines.append(
            f"TGA 预测（run {_date(tg.get('forecast_run_date'))}）提供 "
            f"{'/'.join(tg['scenarios'])} 三种情景，见图表；报告不对情景作选择。"
        )
    else:
        lines.append("无可用 TGA 预测，不提供情景判断。")
    return lines


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
    gaps = _gap_bullets(facts)
    summary = [
        reserve[0],
        reserve[1],
        pool[0],
        funding[1],
        gaps[0],
    ]
    sections = [
        {"title": "水位与变化", "bullets": _reserve_bullets(facts)},
        {"title": "三池结构", "bullets": _pool_bullets(facts)},
        {"title": "周度分解", "bullets": _decomp_bullets(facts)},
        {"title": "资金面", "bullets": _funding_bullets(facts)},
        {"title": "市场读数（外围）", "bullets": _market_bullets(facts)},
        {"title": "风险规则触发状态", "bullets": _risk_bullets(facts)},
        {"title": "前瞻（模型输出）", "bullets": _outlook_bullets(facts)},
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
        ),
    }
