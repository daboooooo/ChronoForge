#!/usr/bin/env python3
"""FDAM 卡片渲染器 —— JSON → HTML → PNG 三阶段流水线。

用法:
    python3 scripts/card_renderer.py output/fdam_card_YYYYMMDD.json

输入:  JSON payload (由 fdam_attention.py 的 build_card_payload 生成)
输出:  output/fdam_card_YYYYMMDD.html  +  output/fdam_card_YYYYMMDD.png (1200×1500 4:5)
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

CARD_W, CARD_H = 1200, 1500

# ── HTML Template (用 __VAR__ 占位，避免与 CSS {} 冲突) ────────────────

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width:__CARD_W__, height:__CARD_H__">
<style>
*{box-sizing:border-box;margin:0;padding:0}
html,body{
  width:__CARD_W__px;height:__CARD_H__px;overflow:hidden;
  background:#0b0e14;color:#e6edf5;
  font-family:"PingFang SC","Heiti SC","Hiragino Sans GB",
    "Arial Unicode MS","STHeiti",-apple-system,"Segoe UI",sans-serif;
  -webkit-font-smoothing:antialiased;
}
.card{
  width:__CARD_W__px;height:__CARD_H__px;padding:48px 48px 36px;
  display:flex;flex-direction:column;gap:18px;
  background:#0b0e14;
}
.section{
  background:#111820;border:1px solid #1f2733;border-radius:14px;
  padding:16px 20px;
}

/* HEADER */
.header{
  flex:0 0 auto;border:none;background:transparent;padding:0 0 12px;
  border-bottom:1.5px solid #1f2733;
  display:flex;justify-content:space-between;align-items:flex-end;
}
.header .title-block{display:flex;flex-direction:column;gap:6px}
.header h1{font-size:26px;font-weight:700;color:#e6edf5;letter-spacing:.5px}
.header .sub{font-size:11px;color:#546678;letter-spacing:.2px}
.header .date{font-size:13px;color:#7a8ba3}

/* PRESSURE */
.pressure{flex:0 0 auto}
.pressure .sec-title{font-size:11px;color:#7a8ba3;margin-bottom:14px}
.gauge-row{display:flex;align-items:center;gap:12px;margin-bottom:12px}
.gauge-row:last-of-type{margin-bottom:14px}
.gauge-label{width:110px;font-size:14px;font-weight:700}
.gauge-label.inf{color:#f7931a}
.gauge-label.emp{color:#3d9bff}
.gauge-bar{flex:1;height:12px;background:#1a2230;border-radius:6px;position:relative;overflow:hidden}
.gauge-fill{height:100%;border-radius:6px;transition:width .3s ease}
.gauge-fill.inf{background:#f7931a}
.gauge-fill.emp{background:#3d9bff}
.gauge-ticks{position:absolute;top:0;left:18px;right:18px;height:100%;pointer-events:none}
.gauge-tick{position:absolute;top:-4px;width:1px;height:20px;background:#546678}
.gauge-tick span{position:absolute;top:12px;left:-10px;font-size:9px;color:#546678}
.gauge-value{width:80px;font-size:16px;font-weight:700;text-align:right}
.gauge-value.inf{color:#f7931a}
.gauge-value.emp{color:#3d9bff}
.parts-line{font-size:8px;color:#546678;opacity:.6;margin-top:4px;text-align:right;max-width:620px;margin-left:auto}
.badge-wrap{display:flex;justify-content:center;margin-top:6px}
.badge{
  display:inline-flex;align-items:center;gap:4px;
  background:#1a2230;border:1.2px solid;border-radius:20px;
  padding:6px 18px;font-size:11px;font-weight:700;
}
.badge.inf{color:#f7931a;border-color:#f7931a}
.badge.emp{color:#3d9bff;border-color:#3d9bff}

/* SENSITIVITY */
.sens{flex:0 0 auto}
.sens .sec-title{font-size:11px;color:#7a8ba3;margin-bottom:14px}
.sens-row{display:flex;align-items:center;gap:14px;margin-bottom:12px}
.sens-label{width:160px;font-size:12px;color:#e6edf5}
.sens-bar{flex:1;height:16px;background:#1a2230;border-radius:8px;display:flex;overflow:hidden}
.sens-seg{height:100%;display:flex;align-items:center;justify-content:center;font-size:10px;font-weight:700}
.sens-seg.inf{background:#f7931a;color:#0b0e14}
.sens-seg.emp{background:#3d9bff;color:#e6edf5}
.sens-legend{display:flex;gap:18px;justify-content:flex-end;margin-top:10px}
.legend-item{display:flex;align-items:center;gap:6px;font-size:9px;color:#7a8ba3}
.legend-dot{width:12px;height:8px;border-radius:2px}
.legend-dot.inf{background:#f7931a}
.legend-dot.emp{background:#3d9bff}

/* MACRO */
.macro{flex:1 1 auto;display:flex;flex-direction:column;min-height:0}
.macro .sec-title{font-size:11px;color:#7a8ba3;margin-bottom:10px;flex-shrink:0}
.macro-grid{
  flex:1 1 auto;display:grid;grid-template-columns:repeat(3,1fr);
  grid-template-rows:repeat(2,1fr);gap:10px;min-height:0;
}
.metric{
  background:#141c27;border:0.8px solid #1f2733;border-radius:10px;
  padding:12px 14px;display:flex;flex-direction:column;justify-content:space-between;
  overflow:hidden;
}
.metric .m-title{font-size:10px;color:#7a8ba3;margin-bottom:2px}
.metric .m-row{display:flex;align-items:baseline;gap:4px}
.metric .m-value{font-size:22px;font-weight:700;color:#e6edf5;line-height:1}
.metric .m-unit{font-size:10px;color:#546678;padding-bottom:2px}
.metric .m-ref{font-size:8px;margin-top:4px}
.metric .m-ref.highlight{color:#f7931a}
.metric .m-ref.highlight.emp{color:#3d9bff}

/* POLICY */
.policy{flex:0 0 auto;display:flex;gap:16px;align-items:stretch;padding:0}
.policy-left{
  flex:0 0 32%;border:0.8px solid #3ddc97;border-radius:12px;
  padding:14px 16px;display:flex;flex-direction:column;justify-content:center;
  position:relative;overflow:hidden;
}
.policy-left.hold{background:rgba(61,220,151,.18);border-color:#3ddc97}
.policy-left.hike{background:rgba(255,92,92,.18);border-color:#ff5c5c}
.policy-left.cut{background:rgba(107,192,255,.18);border-color:#6bc0ff}
.policy-left .pl-label{font-size:9px;color:#7a8ba3;margin-bottom:8px}
.policy-left .pl-main{display:flex;align-items:center;gap:12px}
.policy-left .pl-icon{font-size:36px;font-weight:700;line-height:1}
.policy-left .pl-text{font-size:18px;font-weight:700}
.policy-left.hold .pl-icon,.policy-left.hold .pl-text{color:#3ddc97}
.policy-left.hike .pl-icon,.policy-left.hike .pl-text{color:#ff5c5c}
.policy-left.cut .pl-icon,.policy-left.cut .pl-text{color:#6bc0ff}
.policy-left .pl-sub{font-size:8px;color:#546678;margin-top:6px;opacity:.7}
.policy-right{
  flex:1;background:#111820;border:1px solid #1f2733;border-radius:12px;
  padding:14px 18px;display:flex;flex-direction:column;gap:6px;
}
.policy-right .pr-verdict{font-size:11px;color:#e6edf5;font-weight:700;line-height:1.4}
.policy-right .pr-detail{font-size:9px;color:#7a8ba3;line-height:1.35}
.policy-right .pr-stats{display:flex;gap:14px;margin-top:2px;font-size:9px;color:#546678}
.policy-right .pr-stats span{white-space:nowrap}
.policy-right .pr-backtest{font-size:8px;color:#546678;margin-top:4px;opacity:.8}

/* FOOTER */
.footer{
  flex:0 0 auto;background:transparent;border:none;padding:10px 0 0;
  display:flex;justify-content:space-between;align-items:flex-start;
  border-top:1px solid #1f2733;
}
.footer .fl-left{flex:1;display:flex;flex-direction:column;gap:4px}
.footer .fl-warn{font-size:9px;color:#e6edf5;font-weight:700}
.footer .fl-text{font-size:8px;color:#7a8ba3;line-height:1.4}
.footer .fl-right{text-align:right;display:flex;flex-direction:column;gap:3px}
.footer .fl-brand{font-size:9px;color:#546678}
.footer .fl-src{font-size:7px;color:#546678}
.footer .fl-time{font-size:7px;color:#546678}
</style>
</head>
<body>
<div class="card">

  <!-- HEADER -->
  <div class="header">
    <div class="title-block">
      <h1>FDAM  美联储双目标关注度模型</h1>
      <div class="sub">ChronoForge Strategy Research  ·  FRED latest vintage</div>
    </div>
    <div class="date">截至 __LAST_DATE__</div>
  </div>

  <!-- PRESSURE -->
  <div class="section pressure">
    <div class="sec-title">当前双目标压力指数  (z · 2010-19 正常期基准)</div>
    __PRESSURE_HTML__
    <div class="badge-wrap">
      <div class="badge __MARGIN_CLASS__">压力差  __MARGIN__</div>
    </div>
  </div>

  <!-- SENSITIVITY -->
  <div class="section sens">
    <div class="sec-title">历史政策敏感度权重  (模型 A · 按经济周期分组)</div>
    __SENS_HTML__
    <div class="sens-legend">
      <div class="legend-item"><div class="legend-dot inf"></div>通胀权重</div>
      <div class="legend-item"><div class="legend-dot emp"></div>就业权重</div>
    </div>
  </div>

  <!-- MACRO -->
  <div class="section macro">
    <div class="sec-title">当前宏观快照</div>
    <div class="macro-grid">__MACRO_HTML__</div>
  </div>

  <!-- POLICY -->
  <div class="policy">
    <div class="policy-left __POLICY_CLASS__">
      <div class="pl-label">模型隐含政策方向</div>
      <div class="pl-main">
        <span class="pl-icon">__POLICY_ICON__</span>
        <span class="pl-text">__POLICY_WORD__</span>
      </div>
      <div class="pl-sub">policy stance</div>
    </div>
    <div class="policy-right">
      <div class="pr-verdict">__VERDICT__</div>
      <div class="pr-detail">__DIRECTION_SHORT__</div>
      __STATS_HTML__
      __BT_HTML__
    </div>
  </div>

  <!-- FOOTER -->
  <div class="footer">
    <div class="fl-left">
      <div class="fl-warn">⚠  局限性</div>
      <div class="fl-text">历史敏感度为描述性关联，非美联储官方目标权重。</div>
      <div class="fl-text">数据为最新 vintage，非严格实时回测。压力指数 2010-19 基准 ±3σ 截断。</div>
    </div>
    <div class="fl-right">
      <div class="fl-brand">ChronoForge · FDAM v1</div>
      <div class="fl-src">scripts/fdam_attention.py</div>
      <div class="fl-time">生成 __GEN_TIME__</div>
    </div>
  </div>

</div>
</body>
</html>
"""


def _fmt_signed(v: float, digits: int = 2) -> str:
    return f"{v:+.{digits}f}"


def render_pressure(p: dict) -> str:
    bars = []
    for key, css_cls in [("inflation", "inf"), ("employment", "emp")]:
        v = p[key]["value"]
        frac = max(0.0, min(1.0, v / 3.5))
        value_str = _fmt_signed(v, 2)
        parts = " ".join(
            f"{k.replace('_gap','').replace('_3m','').replace('_6','')}={_fmt_signed(vv,1)}"
            for k, vv in sorted(p[key].get("parts", {}).items())
        )
        bars.append(f"""
    <div>
      <div class="gauge-row">
        <div class="gauge-label {css_cls}">{p[key]["label"]}</div>
        <div class="gauge-bar">
          <div class="gauge-fill {css_cls}" style="width:{frac*100:.1f}%"></div>
          <div class="gauge-ticks">
            <div class="gauge-tick" style="left:28.6%"><span>+1</span></div>
            <div class="gauge-tick" style="left:57.1%"><span>+2</span></div>
            <div class="gauge-tick" style="left:85.7%"><span>+3</span></div>
          </div>
        </div>
        <div class="gauge-value {css_cls}">{value_str} z</div>
      </div>
      <div class="parts-line">{parts}</div>
    </div>""")
    return "".join(bars)


def render_sens(sens_list: list[dict]) -> str:
    rows = []
    for item in sens_list:
        wi = item["w_inflation"]
        we = 1 - wi
        wi_pct = f"{wi:.0%}" if wi >= 0.05 else ""
        we_pct = f"{we:.0%}" if we >= 0.05 else ""
        rows.append(f"""
    <div class="sens-row">
      <div class="sens-label">{item["label"]}</div>
      <div class="sens-bar">
        <div class="sens-seg inf" style="width:{wi*100:.1f}%">{wi_pct}</div>
        <div class="sens-seg emp" style="width:{we*100:.1f}%">{we_pct}</div>
      </div>
    </div>""")
    return "".join(rows)


def render_macro(metrics: list[dict]) -> str:
    cards = []
    for m in metrics:
        ref_html = ""
        if m.get("ref") is not None:
            diff = m["value"] - m["ref"] if m["value"] is not None else 0
            sign = "+" if diff >= 0 else ""
            hl_class = "highlight" if abs(diff) > 0.5 else ""
            if hl_class and m.get("type") == "emp":
                hl_class += " emp"
            ref_html = (
                f'<div class="m-ref {hl_class}">参考 {m["ref"]}  {sign}{diff:.1f}</div>'
                if m["value"] is not None
                else ""
            )
        elif m.get("ref_text"):
            ref_html = f'<div class="m-ref">{m["ref_text"]}</div>'
        cards.append(f"""
    <div class="metric">
      <div>
        <div class="m-title">{m["title"]}</div>
        <div class="m-row">
          <span class="m-value">{m["value_str"]}</span>
          <span class="m-unit">{m.get("unit","")}</span>
        </div>
      </div>
      {ref_html}
    </div>""")
    return "".join(cards)


def render_policy(p: dict) -> tuple[str, str, str]:
    verdict = p.get("verdict", "")
    direction = p.get("direction_short", "")
    wi = p.get("w_inflation")
    corr = p.get("corr_pu")
    r2 = p.get("r2")

    stats = ""
    if wi is not None:
        emp_w = 1 - wi
        corr_s = f"{corr:+.2f}" if corr is not None and corr == corr else "—"
        r2_s = f"{r2:.2f}" if r2 is not None else "—"
        stats = (
            f'<div class="pr-stats">'
            f'<span>历史敏感度  通胀 {wi:.0%} / 就业 {emp_w:.0%}</span>'
            f'<span>corr = {corr_s}</span>'
            f'<span>R² = {r2_s}</span>'
            f"</div>"
        )

    bt = ""
    bt_data = p.get("backtest")
    if bt_data:
        bt = (
            f'<div class="pr-backtest">样本外: MAE {bt_data.get("MAE_bp","—"):.0f}bp'
            f' · 方向准 {bt_data.get("sign_acc_pct","—"):.0f}%</div>'
        )

    return verdict, direction, stats, bt


def policy_decision(direction: str) -> tuple[str, str, str]:
    if "维持不变" in direction:
        return "●", "维持不变", "hold"
    elif "加息" in direction:
        return "▲", "加息倾向", "hike"
    else:
        return "▼", "降息倾向", "cut"


def build_html(payload: dict, gen_time: str) -> str:
    p = payload
    last_date = p.get("last_date", "—")

    inf_val = p["pressure"]["inflation"]["value"]
    emp_val = p["pressure"]["employment"]["value"]
    margin = inf_val - emp_val
    margin_str = _fmt_signed(margin, 2)
    margin_class = "inf" if margin > 0 else "emp"

    pressure_html = render_pressure(p["pressure"])
    sens_html = render_sens(p.get("sensitivity", []))
    macro_html = render_macro(p.get("macro", []))

    icon, word, cls = policy_decision(p.get("direction", ""))
    verdict, direction_short, stats_html, bt_html = render_policy(p.get("policy", {}))
    verdict = verdict[:32] + "…" if len(verdict) > 32 else verdict
    direction_short = (
        direction_short[:42] + "…" if len(direction_short) > 42 else direction_short
    )

    html = HTML_TEMPLATE
    replacements = {
        "__CARD_W__": str(CARD_W),
        "__CARD_H__": str(CARD_H),
        "__LAST_DATE__": last_date,
        "__PRESSURE_HTML__": pressure_html,
        "__MARGIN__": margin_str,
        "__MARGIN_CLASS__": margin_class,
        "__SENS_HTML__": sens_html,
        "__MACRO_HTML__": macro_html,
        "__POLICY_CLASS__": cls,
        "__POLICY_ICON__": icon,
        "__POLICY_WORD__": word,
        "__VERDICT__": verdict,
        "__DIRECTION_SHORT__": direction_short,
        "__STATS_HTML__": stats_html,
        "__BT_HTML__": bt_html,
        "__GEN_TIME__": gen_time,
    }
    for k, v in replacements.items():
        html = html.replace(k, str(v))
    return html


def render_png(html_path: Path, png_path: Path) -> None:
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_page(viewport={"width": CARD_W, "height": CARD_H})
        page.goto(html_path.resolve().as_uri())
        page.wait_for_timeout(300)
        page.screenshot(path=str(png_path), full_page=False)
        browser.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="FDAM 卡片 JSON → HTML → PNG")
    ap.add_argument("json_file", help="JSON payload 文件路径")
    ap.add_argument("--no-png", action="store_true", help="只生成 HTML")
    args = ap.parse_args()

    json_path = Path(args.json_file)
    if not json_path.exists():
        print(f"❌ JSON 不存在: {json_path}", file=sys.stderr)
        sys.exit(1)

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    from datetime import datetime
    gen_time_full = datetime.now().strftime("%Y-%m-%d %H:%M")

    html = build_html(payload, gen_time_full)

    html_path = OUTPUT_DIR / f"{json_path.stem}.html"
    html_path.write_text(html, encoding="utf-8")
    print(f"✓ HTML 已生成  {html_path}  ({len(html):,} bytes)")

    if not args.no_png:
        png_path = OUTPUT_DIR / f"{json_path.stem}.png"
        render_png(html_path, png_path)
        print(f"✓ PNG 已生成  {png_path}  ({CARD_W}×{CARD_H})")


if __name__ == "__main__":
    main()
