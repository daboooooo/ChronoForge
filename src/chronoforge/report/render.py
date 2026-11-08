# ruff: noqa: E501  （内嵌 HTML/CSS/JS 资源，行宽不受 Python 代码规范约束）
"""日报渲染：自包含 HTML + Markdown 摘要 + 可审计 JSON。

HTML 的可视化沿用 visualization/app.js 的 ECharts 基础配置
（``base`` / ``xTime`` / ``yVal`` 与同一套配色），只承载当日报表所需的 5 张图；
ECharts 运行库从 ``reports/daily_liquidity/assets/echarts.min.js`` 加载。
"""

from __future__ import annotations

import html
import json
from datetime import date
from typing import Any, cast

from chronoforge.report.session import weekday_cn

# ── 格式化 ────────────────────────────────────────────────────────


def _fmt(value: Any, digits: int = 2) -> str:
    if value is None:
        return "缺失"
    return f"{float(value):.{digits}f}"


def _chg(value: Any, digits: int = 1) -> str:
    if value is None:
        return "缺失"
    return f"{float(value):+.{digits}f}"


def _pct(value: Any, digits: int = 1) -> str:
    if value is None:
        return "缺失"
    return f"{float(value):.{digits}f}%"


def _esc(value: Any) -> str:
    return html.escape(str(value))


def _zone_tone(zone: str | None) -> str:
    if zone == "紧张区":
        return "danger"
    if zone == "预警区":
        return "warn"
    if zone == "充裕区":
        return "ok"
    return "muted"


def _status_tone(status: str) -> str:
    return {"ok": "ok", "lagged": "warn", "missing": "danger"}.get(status, "muted")


def _json_default(obj: Any) -> Any:
    item = getattr(obj, "item", None)
    if callable(item):
        return item()
    if isinstance(obj, date):
        return obj.isoformat()
    raise TypeError(f"JSON 不支持类型: {type(obj)!r}")


# ── 摘要 / KPI ────────────────────────────────────────────────────


def _kpi_values(facts: dict[str, Any]) -> list[dict[str, str]]:
    m = facts["metrics"]
    r = m["reserve"]
    f = m["funding"]
    s = facts["freshness_summary"]
    return [
        {
            "label": "准备金",
            "value": _fmt(r.get("level_t"), 3),
            "unit": "T",
            "sub": f"{r.get('asof') or '缺失'} · {r.get('zone') or '未知'}",
            "tone": _zone_tone(r.get("zone")),
        },
        {
            "label": "准备金 4 周变化",
            "value": _chg(r.get("chg_4w_b")),
            "unit": "B",
            "sub": f"1 周 {_chg(r.get('chg_1w_b'))} B / 13 周 {_chg(r.get('chg_13w_b'))} B",
            "tone": "danger" if (r.get("chg_4w_b") or 0) <= -50 else "muted",
        },
        {
            "label": "准备金 5 年分位",
            "value": _pct(r.get("pct_5y")),
            "unit": "",
            "sub": f"10 年分位 {_pct(r.get('pct_10y'))}（截至 {r.get('asof') or '缺失'}）",
            "tone": "muted",
        },
        {
            "label": "SOFR-EFFR",
            "value": _fmt(f.get("spread_bp"), 1),
            "unit": "bp",
            "sub": f"{f.get('sofr_asof') or '缺失'} · {f.get('corridor_status')}",
            "tone": "danger" if (f.get("spread_bp") or 0) > 17 else (
                "warn" if (f.get("spread_bp") or 0) >= 12 else "ok"
            ),
        },
        {
            "label": "SRF 用量",
            "value": _fmt(f.get("srf_usage_b"), 2),
            "unit": "B",
            "sub": f"30 日峰值 {_fmt(f.get('srf_peak_30d_b'), 2)} B（{f.get('srf_asof') or '缺失'}）",
            "tone": "muted",
        },
        {
            "label": "核心序列完整度",
            "value": f"{s['core_ok']}/{s['core_total']}",
            "unit": "",
            "sub": f"滞后 {s['lagged']} / 缺失 {s['missing']}（全部 {s['total']} 序列）",
            "tone": "ok" if s["core_ok"] == s["core_total"] else "warn",
        },
    ]


def _summary_lines(facts: dict[str, Any], narrative: dict[str, Any]) -> list[str]:
    return cast(list[str], narrative["summary"][:6])


# ── Markdown ──────────────────────────────────────────────────────


def render_markdown(
    facts: dict[str, Any], narrative: dict[str, Any], report_day: date
) -> str:
    lines: list[str] = [
        f"# 每日流动性简报 · {report_day.isoformat()}（{weekday_cn(report_day)}）",
        "",
        f"> {narrative['data_cutoff_line']}",
    ]
    batch = facts.get("batch_status") or {}
    lines.append(
        f"> 最近批次：{batch.get('status', '缺失')}"
        f"（{batch.get('finished_at') or '无状态文件'}）"
    )
    lines.append(f"> 生成方式：{narrative['mode']}（确定性规则模板，无 LLM）")
    lines.append("")
    lines.append("## 摘要")
    lines.append("")
    lines.extend(f"- {line}" for line in _summary_lines(facts, narrative))
    lines.append("")
    lines.append("## 关键指标")
    lines.append("")
    lines.append("| 指标 | 数值 | 备注 |")
    lines.append("| --- | --- | --- |")
    for item in _kpi_values(facts):
        unit = f" {item['unit']}" if item["unit"] else ""
        lines.append(f"| {item['label']} | {item['value']}{unit} | {item['sub']} |")
    lines.append("")
    for section in narrative["sections"]:
        lines.append(f"## {section['title']}")
        lines.append("")
        lines.extend(f"- {line}" for line in section["bullets"])
        lines.append("")
    lines.extend(_rules_md(facts))
    lines.append("## 新鲜度矩阵")
    lines.append("")
    lines.append("| 序列 | 分组 | 矩阵下限 | 实际 as-of | 状态 | 说明 |")
    lines.append("| --- | --- | --- | --- | --- | --- |")
    for row in facts["freshness"]:
        lines.append(
            f"| {row['label']}（{row['series']}） | {row['group']} | "
            f"{row['expected_asof'] or '—'} | {row['actual_asof'] or '—'} | "
            f"{row['status_label']} | {row['note']} |"
        )
    lines.append("")
    lines.append("## 数据缺口")
    lines.append("")
    gaps = facts.get("gaps", [])
    if gaps:
        for gap in gaps:
            if gap["kind"] == "source_anomaly":
                text = (f"[源侧异常] {gap['dataset_id']}，{gap['count']} 条，"
                        f"最近 {gap['last_at']}：{gap['detail']}")
            elif gap["kind"] == "batch_status":
                text = (f"[批次状态] {gap['status']}（{gap['finished_at']}，"
                        f"exit {gap['exit_code']}）")
            else:
                label = gap.get("label") or gap.get("series") or gap["kind"]
                text = (f"[{gap['kind']}] {label}：预期 "
                        f"{gap.get('expected_asof') or '缺失'}，实际 "
                        f"{gap.get('actual_asof') or '缺失'}；{gap.get('detail', '')}")
            lines.append(f"- {text}")
    else:
        lines.append("- 未发现显式缺口。")
    lines.append("")
    lines.append(f"> {narrative['scope_note']}")
    lines.append("")
    lines.append(f"> {narrative['method_note']}")
    lines.append("")
    return "\n".join(lines)


# ── HTML ──────────────────────────────────────────────────────────


_CSS = """
:root{--bg:#0b0e14;--panel:#0f141d;--panel2:#111824;--line:#1f2733;--line2:#273345;
--text:#c9d4e3;--bright:#eaf2ff;--muted:#7b8ca4;--blue:#3d9bff;--orange:#f7931a;
--green:#3ddc97;--red:#ff5c5c;--purple:#a78bfa;--cyan:#22d3ee;--yellow:#f5c542;
--mono:ui-monospace,SFMono-Regular,"SF Mono",Menlo,Consolas,monospace}
*{box-sizing:border-box}
html,body{margin:0;padding:0;background:var(--bg);color:var(--text);
font-family:-apple-system,BlinkMacSystemFont,"PingFang SC","Microsoft YaHei","Segoe UI",sans-serif;
font-size:13px;-webkit-font-smoothing:antialiased}
body::before{content:"";position:fixed;inset:0;pointer-events:none;z-index:-1;
background:radial-gradient(900px 420px at 12% -10%,rgba(61,155,255,.10),transparent 60%),
radial-gradient(760px 380px at 88% -8%,rgba(34,211,238,.06),transparent 60%)}
.wrap{max-width:1280px;margin:0 auto;padding:22px 22px 56px}
.brand{font-family:var(--mono);font-size:10px;letter-spacing:.18em;text-transform:uppercase;
color:var(--muted);display:flex;align-items:center;gap:9px}
.dot{width:6px;height:6px;border-radius:50%;background:var(--green);box-shadow:0 0 10px var(--green)}
h1{font-size:23px;margin:8px 0 4px;color:var(--bright);font-weight:600;letter-spacing:.01em}
.sub{color:var(--muted);font-size:12px}
.cutoff{margin-top:9px;padding:9px 12px;border:1px solid var(--line2);border-left:3px solid var(--blue);
background:var(--panel);border-radius:4px;color:#a9bad1;font-size:12px}
.badge{display:inline-block;margin-left:8px;padding:2px 7px;border-radius:3px;font-family:var(--mono);
font-size:10px;border:1px solid var(--line2);color:var(--muted)}
.badge.warn{color:var(--yellow);border-color:rgba(245,197,66,.35);background:rgba(245,197,66,.08)}
.badge.ok{color:var(--green);border-color:rgba(61,220,151,.35);background:rgba(61,220,151,.08)}
.badge.danger{color:var(--red);border-color:rgba(255,92,92,.35);background:rgba(255,92,92,.08)}
.alert{margin:16px 0;padding:13px 16px;border:1px solid rgba(245,197,66,.32);
background:rgba(245,197,66,.06);border-radius:6px}
.alert.ok{border-color:rgba(61,220,151,.28);background:rgba(61,220,151,.05)}
.alert-title{color:var(--yellow);font-weight:600;margin-bottom:6px}
.alert.ok .alert-title{color:var(--green)}
.alert ul{margin:0;padding-left:18px;color:#b8c7db;font-size:12px;line-height:1.7}
.kpis{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px;margin:16px 0}
.kpi{background:var(--panel);border:1px solid var(--line);border-radius:7px;padding:13px 15px}
.kpi .k{color:var(--muted);font-size:11px;letter-spacing:.04em}
.kpi .v{font-size:25px;color:var(--bright);font-family:var(--mono);margin-top:4px}
.kpi .v .u{font-size:12px;color:var(--muted);margin-left:4px}
.kpi .s{color:var(--muted);font-size:11px;margin-top:5px}
.kpi.danger{border-color:rgba(255,92,92,.3)}.kpi.danger .v{color:var(--red)}
.kpi.warn{border-color:rgba(245,197,66,.26)}.kpi.warn .v{color:var(--yellow)}
.kpi.ok{border-color:rgba(61,220,151,.24)}.kpi.ok .v{color:var(--green)}
h2{font-size:14px;color:var(--bright);margin:26px 0 10px;padding-bottom:7px;
border-bottom:1px solid var(--line);font-weight:600}
.grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:13px}
.card{background:var(--panel);border:1px solid var(--line);border-radius:7px;padding:12px 14px}
.card.wide{grid-column:1 / -1}
.card h3{font-size:12px;margin:0 0 8px;color:#b9cbe2;font-weight:500}
.chart{height:264px;width:100%}
.chart.empty{display:flex;align-items:center;justify-content:center;color:var(--muted);font-size:12px;
border:1px dashed var(--line2);border-radius:5px}
.sect{background:var(--panel);border:1px solid var(--line);border-radius:7px;padding:13px 16px;margin-bottom:11px}
.sect h3{font-size:12px;margin:0 0 7px;color:var(--bright);font-weight:600}
.sect ul{margin:0;padding-left:17px;line-height:1.75;color:#b8c7db}
table{width:100%;border-collapse:collapse;font-size:11.5px}
th,td{text-align:left;padding:7px 9px;border-bottom:1px solid var(--line)}
th{color:var(--muted);font-weight:500;font-size:10.5px;letter-spacing:.04em}
tr.lagged td{background:rgba(245,197,66,.05)}
tr.missing td{background:rgba(255,92,92,.055)}
.st{font-family:var(--mono);font-size:10px}
.st.ok{color:var(--green)}.st.warn{color:var(--yellow)}.st.danger{color:var(--red)}
.muted{color:var(--muted)}
footer{margin-top:34px;padding-top:14px;border-top:1px solid var(--line);color:var(--muted);font-size:11px;line-height:1.8}
@media(max-width:900px){.kpis{grid-template-columns:repeat(2,minmax(0,1fr))}
.grid{grid-template-columns:1fr}.card.wide{grid-column:auto}table{display:block;overflow-x:auto}}
"""

_JS_CHARTS = r"""
(function(){
  const D = window.CF_REPORT_DATA;
  if (!D || !window.echarts) return;
  const C = {blue:"#3d9bff",orange:"#f7931a",green:"#3ddc97",red:"#ff5c5c",
             purple:"#a78bfa",cyan:"#22d3ee",yellow:"#f5c542",grey:"#6b7a90",
             text:"#c9d4e3",line:"#273345",grid:"#182130"};
  const MONO='ui-monospace,SFMono-Regular,Menlo,Consolas,monospace';
  function xTime(extra){return Object.assign({type:"time",axisLine:{lineStyle:{color:C.line}},
    axisTick:{show:false},axisLabel:{color:"#6f8098",fontSize:10,hideOverlap:true},
    splitLine:{show:false},axisPointer:{label:{backgroundColor:"#1a2431",color:C.text}},},extra||{});}
  function xCat(data){return {type:"category",data:data,boundaryGap:true,
    axisLine:{lineStyle:{color:C.line}},axisTick:{show:false},
    axisLabel:{color:"#6f8098",fontSize:9,interval:1,hideOverlap:true},splitLine:{show:false}};}
  function yVal(extra){return Object.assign({type:"value",axisLine:{show:false},
    axisTick:{show:false},axisLabel:{color:"#6f8098",fontSize:10},
    splitLine:{lineStyle:{color:C.grid,type:"dashed"}},
    nameTextStyle:{color:"#6f8098",fontSize:10}},extra||{});}
  function base(extra){return Object.assign({backgroundColor:"transparent",
    animationDuration:500,textStyle:{color:C.text,fontSize:11},
    grid:{left:56,right:20,top:34,bottom:34},legend:{top:0,right:4,
      textStyle:{color:"#8fa3bd",fontSize:10},itemWidth:12,itemHeight:8,icon:"roundRect"},
    tooltip:{trigger:"axis",backgroundColor:"rgba(13,18,26,.96)",borderColor:C.line,
      borderWidth:1,textStyle:{color:"#dbe6f5",fontSize:11},
      axisPointer:{lineStyle:{color:"#31435c"}}},
    xAxis:xTime(),yAxis:yVal()},extra||{});}
  function pairs(t,v){return t.map((x,i)=>[x,v[i]]);}
  function init(id,opt){
    const dom=document.getElementById(id);if(!dom||!opt)return;
    const inst=echarts.init(dom,null,{renderer:"canvas"});inst.setOption(opt);
    window.addEventListener("resize",()=>inst.resize());
  }
  const c=D.charts;
  // 1) 准备金水位
  if(c.reserve&&c.reserve.t.length){
    init("chart-reserve",base({
      yAxis:yVal({name:"T"}),
      series:[{name:"准备金",type:"line",showSymbol:false,lineStyle:{color:C.blue,width:1.4},
        areaStyle:{color:"rgba(61,155,255,.13)"},data:pairs(c.reserve.t,c.reserve.v),
        markLine:{silent:true,symbol:"none",label:{color:"#8fa3bd",fontSize:10,formatter:p=>p.value+" T"},
          data:[{yAxis:c.reserve.thresholds[0],lineStyle:{color:C.red,type:"dashed"}},
                {yAxis:c.reserve.thresholds[1],lineStyle:{color:C.yellow,type:"dashed"}}]}}]
    }));
  }
  // 2) 两池/三池结构
  if(c.three_pool&&c.three_pool.t.length){
    const p=c.three_pool;
    const series=[
      {name:"准备金",type:"line",showSymbol:false,lineStyle:{color:C.green,width:1.4},
       areaStyle:{color:"rgba(61,220,151,.10)"},data:pairs(p.t,p.reserve)},
      {name:"TGA",type:"line",showSymbol:false,lineStyle:{color:C.orange,width:1.4},data:pairs(p.t,p.tga)}
    ];
    if(!p.onrrp_zero) series.push({name:"ON RRP",type:"line",showSymbol:false,
      lineStyle:{color:C.cyan,width:1.2},data:pairs(p.t,p.onrrp)});
    init("chart-pool",base({yAxis:yVal({name:"T"}),series:series}));
  }
  // 3) 周度分解（RRP 归零时不再占一根恒零柱子）
  if(c.decomp&&c.decomp.t.length){
    const p=c.decomp;
    const keys=[["dR","ΔR",C.blue],["dA","ΔA",C.grey],["tga","TGA",C.orange],
                ["cash","现金",C.purple],["other","其他",C.yellow]];
    if(!p.onrrp_zero) keys.splice(3,0,["onrrp","ON RRP",C.cyan]);
    init("chart-decomp",base({
      xAxis:xCat(p.t),
      yAxis:yVal({name:"B"}),
      series:keys.map(([k,label,color])=>({name:label,type:"bar",barMaxWidth:12,
        itemStyle:{color:color},data:p.series[k]}))
    }));
  }
  // 4) 资金面
  if(c.funding&&c.funding.t.length){
    init("chart-funding",base({
      yAxis:[yVal({name:"bp"}),yVal({name:"B",position:"right",splitLine:{show:false}} )],
      series:[
        {name:"SOFR-EFFR (bp)",type:"line",showSymbol:false,lineStyle:{color:C.blue,width:1.5},
         areaStyle:{color:"rgba(61,155,255,.12)"},data:pairs(c.funding.t,c.funding.spread)},
        {name:"SRF 用量 (B)",type:"line",yAxisIndex:1,showSymbol:false,
         lineStyle:{color:C.orange,width:1.4},data:pairs(c.funding.srf_t,c.funding.srf)}
      ]
    }));
  }
  // 5) TGA 日值与预测情景
  if(c.tga_daily&&c.tga_daily.t.length){
    const f=c.tga_forecast||{scenarios:{}}, colors={base:C.purple,liquidity_easing:C.green,liquidity_tightening:C.red};
    const series=[{name:"TGA 实际",type:"line",showSymbol:false,lineStyle:{color:C.cyan,width:1.4},
      areaStyle:{color:"rgba(34,211,238,.10)"},data:pairs(c.tga_daily.t,c.tga_daily.v)}];
    Object.keys(f.scenarios||{}).forEach((name)=>{
      const s=f.scenarios[name];
      series.push({name:"预测:"+name,type:"line",showSymbol:false,lineStyle:{color:colors[name]||C.grey,width:1.2,type:"dashed"},
        data:pairs(s.t,s.v)});
    });
    init("chart-tga",base({yAxis:yVal({name:"T"}),series:series}));
  }
})();
"""


def _kpi_html(facts: dict[str, Any]) -> str:
    blocks: list[str] = []
    for item in _kpi_values(facts):
        unit = f'<span class="u">{_esc(item["unit"])}</span>' if item["unit"] else ""
        blocks.append(
            f'<div class="kpi {item["tone"]}"><div class="k">{_esc(item["label"])}</div>'
            f'<div class="v">{_esc(item["value"])}{unit}</div>'
            f'<div class="s">{_esc(item["sub"])}</div></div>'
        )
    return "".join(blocks)


def _gap_html(facts: dict[str, Any]) -> str:
    gaps = facts.get("gaps", [])
    if not gaps:
        return ('<div class="alert ok"><div class="alert-title">未发现显式缺口</div>'
                '<ul><li>所有展示序列均达到矩阵下限，且最近批次为成功态。</li></ul></div>')
    items: list[str] = []
    for gap in gaps[:8]:
        if gap["kind"] == "source_anomaly":
            text = (f"源侧异常：{gap['dataset_id']}，最近 {gap['count']} 条"
                    f"（{gap['last_at']}）")
        elif gap["kind"] == "batch_status":
            text = f"批次状态：{gap['status']}（{gap['finished_at']}，exit {gap['exit_code']}）"
        else:
            label = gap.get("label") or gap.get("series") or gap["kind"]
            text = (f"{label}：预期 {gap.get('expected_asof') or '缺失'}，"
                    f"实际 {gap.get('actual_asof') or '缺失'}；{gap.get('detail', '')}")
        items.append(f"<li>{_esc(text)}</li>")
    return (
        f'<div class="alert"><div class="alert-title">数据缺口 {len(gaps)} 项'
        f'（报告照常产出，结论以 as-of 为准）</div><ul>{"".join(items)}</ul></div>'
    )


def _freshness_html(facts: dict[str, Any]) -> str:
    rows: list[str] = []
    for row in facts["freshness"]:
        tone = _status_tone(row["status"])
        rows.append(
            f'<tr class="{row["status"] if row["status"] != "ok" else ""}">'
            f"<td>{_esc(row['label'])}<span class=\"muted\"> · {_esc(row['series'])}</span></td>"
            f"<td>{_esc(row['group'])}</td>"
            f"<td class=\"st\">{_esc(row['expected_asof'] or '—')}</td>"
            f"<td class=\"st\">{_esc(row['actual_asof'] or '—')}</td>"
            f'<td><span class="st {tone}">{_esc(row["status_label"])}</span></td>'
            f"<td class=\"muted\">{_esc(row['note'])}</td></tr>"
        )
    return (
        '<table><thead><tr><th>序列</th><th>分组</th><th>矩阵下限</th>'
        '<th>实际 as-of</th><th>状态</th><th>说明</th></tr></thead>'
        f'<tbody>{"".join(rows)}</tbody></table>'
    )


def _rule_frequency(row: dict[str, Any]) -> str:
    f = row.get("frequency", {})
    return (f"{f.get('triggers', 0)}/{f.get('total', 0)} {f.get('unit', '')}"
            f"（{f.get('pct', 0)}%，{f.get('per_year', 0)}/年）")


def _rule_corroboration(row: dict[str, Any]) -> str:
    c = row.get("corroboration", {})
    if row.get("kind") == "background":
        return str(c.get("note") or "状态型，不作独立警报")
    lift = f"；lift {c.get('lift')}×" if c.get("lift") else ""
    return (f"{c.get('indicator')}：{c.get('hits')}/{c.get('n')}"
            f"（{c.get('pct')}%），基础 {c.get('base_pct')}%{lift}")


def _rule_current(row: dict[str, Any]) -> str:
    cur = row.get("current", {})
    value = cur.get("value") or "缺失"
    detail = cur.get("detail") or ""
    if row.get("kind") == "background":
        head = "状态中" if cur.get("active") else "未处于该状态"
    else:
        head = "触发" if cur.get("active") else "未触发"
    return f"{head} · {value}" + (f"；{detail}" if detail else "")


def _rules_md(facts: dict[str, Any]) -> list[str]:
    rules = facts.get("rules", {})
    rows = rules.get("rows", [])
    lines = [
        "## 风险规则回测",
        "",
        f"回测窗口：近 {rules.get('window_years', 2)} 年；"
        f"数据截至 {rules.get('asof') or '缺失'}。"
        "频率过高的规则只在状态转换时提示，不逐日告警。",
        "",
        "| 规则 | 类型 | 触发条件 | 近两年触发 | 区分度（触发时对应 / 基础） | 当前 |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        kind = "事件" if row.get("kind") == "event" else "背景"
        lines.append(
            f"| {row.get('label')} | {kind} | {row.get('condition')} | "
            f"{_rule_frequency(row)} | {_rule_corroboration(row)} | {_rule_current(row)} |"
        )
    lines.append("")
    return lines


def _rules_html(facts: dict[str, Any]) -> str:
    rules = facts.get("rules", {})
    rows = rules.get("rows", [])
    if not rows:
        return '<div class="card muted">规则回测不可用（数据链路缺口）。</div>'
    body: list[str] = []
    for row in rows:
        kind = "事件" if row.get("kind") == "event" else "背景"
        tone = "danger" if row.get("current", {}).get("active") and row.get("kind") == "event" else ""
        body.append(
            f"<tr><td>{_esc(row.get('label'))}</td><td>{kind}</td>"
            f"<td class=\"muted\">{_esc(row.get('condition'))}</td>"
            f"<td class=\"st\">{_esc(_rule_frequency(row))}</td>"
            f"<td class=\"muted\">{_esc(_rule_corroboration(row))}</td>"
            f"<td><span class=\"st {tone}\">{_esc(_rule_current(row))}</span></td></tr>"
        )
    return (
        f'<div class="card"><div class="sect" style="border:0;padding:0;background:transparent">'
        f"<span class=\"muted\">回测窗口近 {rules.get('window_years', 2)} 年；"
        f"数据截至 {_esc(rules.get('asof') or '缺失')}。频率过高的规则只在状态转换时提示。</span>"
        f'</div><table><thead><tr><th>规则</th><th>类型</th><th>触发条件</th>'
        f"<th>近两年触发</th><th>区分度（触发时对应 / 基础）</th><th>当前</th>"
        f'</tr></thead><tbody>{"".join(body)}</tbody></table></div>'
    )


def _sections_html(narrative: dict[str, Any]) -> str:
    blocks: list[str] = []
    for section in narrative["sections"]:
        items = "".join(f"<li>{_esc(line)}</li>" for line in section["bullets"])
        blocks.append(
            f'<div class="sect"><h3>{_esc(section["title"])}</h3><ul>{items}</ul></div>'
        )
    return "".join(blocks)


def _chart_card(title: str, element_id: str, data_present: bool, wide: bool = False) -> str:
    cls = "card wide" if wide else "card"
    if data_present:
        body = f'<div class="chart" id="{element_id}"></div>'
    else:
        body = '<div class="chart empty">无可用序列（显式缺口）</div>'
    return f'<div class="{cls}"><h3>{_esc(title)}</h3>{body}</div>'


def render_html(
    facts: dict[str, Any], narrative: dict[str, Any], report_day: date
) -> str:
    charts = facts["charts"]
    js_data = json.dumps(facts, ensure_ascii=False, allow_nan=False, default=_json_default)
    js_data = js_data.replace("<", "\\u003c")

    kpis = _kpi_html(facts)
    summary = "".join(f"<li>{_esc(line)}</li>" for line in _summary_lines(facts, narrative))
    batch = facts.get("batch_status") or {}
    batch_status = str(batch.get("status", "缺失"))
    batch_tone = "ok" if batch_status in {"success", "ok"} else "warn"
    decomp_zero = bool(charts["decomp"].get("onrrp_zero"))
    decomp_title = ("周度流动性分解（近 13 周，B；ON RRP 已归零，不占柱）"
                    if decomp_zero else "周度流动性分解（近 13 周，B）")
    chart_cards = "".join([
        _chart_card("准备金水位（5 年，T）", "chart-reserve",
                    bool(charts["reserve"]["t"])),
        _chart_card("两池结构（ON RRP 已归零，2 年，T）"
                    if charts["three_pool"].get("onrrp_zero") else "三池结构（2 年，T）",
                    "chart-pool", bool(charts["three_pool"]["t"])),
        _chart_card(decomp_title, "chart-decomp",
                    bool(charts["decomp"]["t"]), wide=decomp_zero),
        _chart_card("SOFR-EFFR 与 SRF 用量（1 年）", "chart-funding",
                    bool(charts["funding"]["t"])),
        _chart_card("TGA 日值与预测情景（1 年，T）", "chart-tga",
                    bool(charts["tga_daily"]["t"]), wide=True),
    ])

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>每日流动性简报 · {report_day.isoformat()}</title>
<style>{_CSS}</style>
</head>
<body>
<div class="wrap">
  <div class="brand"><span class="dot"></span>CHRONOFORGE · DAILY LIQUIDITY</div>
  <h1>每日流动性简报</h1>
  <div class="sub">{report_day.isoformat()} · {weekday_cn(report_day)} · 美股盘前（手动运行）
    <span class="badge {batch_tone}">批次 {_esc(batch_status)}</span>
    <span class="badge">模板模式</span>
  </div>
  <div class="cutoff">{_esc(narrative['data_cutoff_line'])}</div>
  {_gap_html(facts)}
  <div class="kpis">{kpis}</div>
  <h2>摘要</h2>
  <div class="sect"><ul>{summary}</ul></div>
  <h2>图表</h2>
  <div class="grid">{chart_cards}</div>
  <h2>新鲜度矩阵</h2>
  <div class="card">{_freshness_html(facts)}</div>
  <h2>风险规则回测</h2>
  {_rules_html(facts)}
  <h2>分项说明</h2>
  {_sections_html(narrative)}
  <footer>
    {_esc(narrative['scope_note'])}<br>
    {_esc(narrative['method_note'])}<br>
    数据源：visualization/export_data.py::build_payload；质量 flag：meta/chronoforge.db；
    批次状态：var/run/daily_update.json。
  </footer>
</div>
<script>window.CF_REPORT_DATA = {js_data};</script>
<script src="assets/echarts.min.js"></script>
<script>{_JS_CHARTS}</script>
</body>
</html>
"""


def render_json(
    facts: dict[str, Any], narrative: dict[str, Any], report_day: date
) -> dict[str, Any]:
    audit = {k: v for k, v in facts.items() if k != "charts"}
    audit["report_date"] = report_day.isoformat()
    audit["narrative"] = narrative
    audit["chart_data_note"] = (
        "图表序列由同一 build_payload 结果裁剪生成，未写入本审计 JSON；"
        "可按 metrics.definitions 复算。"
    )
    return audit


def dump_json(data: dict[str, Any]) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2, allow_nan=False, default=_json_default)
