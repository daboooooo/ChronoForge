#!/usr/bin/env python3
"""Generate a self-contained styled HTML page from the FDAM strategy markdown.

Source : docs/strategy/FDAM 美联储双目标政策关注度模型.md
Output : docs/strategy/FDAM 美联储双目标政策关注度模型.html

Usage:
    python3 scripts/generate_fdam_html.py

No third-party dependencies: the markdown is parsed with a small purpose-built
converter (headings / tables / lists / fenced code / LaTeX math blocks), and
math is rendered client-side by KaTeX from CDN.
"""

import html
import re
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MD_PATH = ROOT / "docs" / "strategy" / "FDAM 美联储双目标政策关注度模型.md"
OUT_PATH = ROOT / "docs" / "strategy" / "FDAM 美联储双目标政策关注度模型.html"

HEADING_RE = re.compile(r"^(#{1,6})\s+(.*)$")
UL_ITEM_RE = re.compile(r"^\s*[-*]\s+(.*)$")
OL_ITEM_RE = re.compile(r"^\s*\d+\.\s+(.*)$")
TABLE_SEP_RE = re.compile(r"^\|?[\s:\-|]+\|?$")
META_LINE_RE = re.compile(r"^\*\*(.+?)：\*\*\s*(.*)$")


# ---------------------------------------------------------------------------
# inline rendering
# ---------------------------------------------------------------------------

def render_inline(text: str) -> str:
    """Escape and apply inline markdown (code spans, bold)."""
    s = html.escape(text, quote=False)
    s = re.sub(r"`([^`]+)`", r"<code>\1</code>", s)
    s = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", s)
    return s


def render_cell(text: str) -> str:
    """Render a table cell; give P0/P1/P2 priority markers a colored badge."""
    inner = render_inline(text.strip())
    if re.fullmatch(r"P[0-2]", inner):
        return f'<span class="badge {inner.lower()}">{inner}</span>'
    return inner


# ---------------------------------------------------------------------------
# block rendering
# ---------------------------------------------------------------------------

def split_row(row: str) -> list:
    return row.strip().strip("|").split("|")


def render_table(rows: list) -> str:
    header = split_row(rows[0])
    body_rows = rows[2:] if len(rows) > 1 and TABLE_SEP_RE.match(rows[1]) else rows[1:]
    thead = "<thead><tr>" + "".join(f"<th>{render_cell(c)}</th>" for c in header) + "</tr></thead>"
    trs = []
    for r in body_rows:
        cells = split_row(r)
        cells += [""] * (len(header) - len(cells))
        trs.append("<tr>" + "".join(f"<td>{render_cell(c)}</td>" for c in cells) + "</tr>")
    tbody = f"<tbody>{''.join(trs)}</tbody>"
    return f'<div class="table-wrap"><table>{thead}{tbody}</table></div>'


def render_content(lines: list):
    """Convert the document body (after the hero header) to HTML + TOC tree."""
    body = []
    toc = []  # (level, text, anchor_id)
    counters = {"h2": 0, "h3": 0}
    i, n = 0, len(lines)

    def is_block_start(s: str) -> bool:
        return (not s or s == "---" or s.startswith("```") or s == "\\["
                or s.startswith("#") or s.startswith("|")
                or UL_ITEM_RE.match(s) or OL_ITEM_RE.match(s))

    while i < n:
        raw = lines[i]
        stripped = raw.strip()

        if not stripped:
            i += 1
            continue

        if stripped == "---":
            body.append("<hr>")
            i += 1
            continue

        # fenced code block
        if stripped.startswith("```"):
            i += 1
            buf = []
            while i < n and not lines[i].strip().startswith("```"):
                buf.append(lines[i])
                i += 1
            i += 1  # closing fence
            code = html.escape("\n".join(buf), quote=False)
            body.append(f'<pre class="code-block"><code>{code}</code></pre>')
            continue

        # display math block \[ ... \]
        if stripped == "\\[":
            i += 1
            buf = []
            while i < n and lines[i].strip() != "\\]":
                buf.append(lines[i])
                i += 1
            i += 1  # closing \]
            src = html.escape("\n".join(buf).strip(), quote=False)
            body.append(f'<div class="math-block">\\[{src}\\]</div>')
            continue

        # headings: '#' -> section (h2), '##' -> subsection (h3), deeper -> h4
        m = HEADING_RE.match(stripped)
        if m:
            level, text = len(m.group(1)), m.group(2).strip()
            if level == 1:
                counters["h2"] += 1
                counters["h3"] = 0
                hid = f"sec-{counters['h2']}"
                toc.append((1, text, hid))
                body.append(f'<h2 id="{hid}" class="section">{render_inline(text)}</h2>')
            elif level == 2:
                counters["h3"] += 1
                hid = f"sec-{counters['h2']}-{counters['h3']}"
                toc.append((2, text, hid))
                body.append(f'<h3 id="{hid}">{render_inline(text)}</h3>')
            else:
                body.append(f"<h4>{render_inline(text)}</h4>")
            i += 1
            continue

        # table
        if stripped.startswith("|"):
            tbl = []
            while i < n and lines[i].strip().startswith("|"):
                tbl.append(lines[i].strip())
                i += 1
            body.append(render_table(tbl))
            continue

        # unordered list
        m = UL_ITEM_RE.match(raw)
        if m:
            items = []
            while i < n and UL_ITEM_RE.match(lines[i]):
                items.append(UL_ITEM_RE.match(lines[i]).group(1))
                i += 1
            lis = "".join(f"<li>{render_inline(t)}</li>" for t in items)
            body.append(f"<ul>{lis}</ul>")
            continue

        # ordered list
        m = OL_ITEM_RE.match(raw)
        if m:
            items = []
            while i < n and OL_ITEM_RE.match(lines[i]):
                items.append(OL_ITEM_RE.match(lines[i]).group(1))
                i += 1
            lis = "".join(f"<li>{render_inline(t)}</li>" for t in items)
            body.append(f"<ol>{lis}</ol>")
            continue

        # paragraph: gather consecutive plain lines
        para = [stripped]
        i += 1
        while i < n and not is_block_start(lines[i].strip()):
            para.append(lines[i].strip())
            i += 1
        body.append(f"<p>{render_inline(' '.join(para))}</p>")

    return "\n".join(body), toc


# ---------------------------------------------------------------------------
# hero header + TOC
# ---------------------------------------------------------------------------

def extract_hero(lines: list):
    """Pull the leading title / subtitle / meta lines out of the document."""
    title, subtitle, meta = None, None, []
    i, n = 0, len(lines)
    while i < n and not lines[i].startswith("# "):
        i += 1
    if i == n:  # fallback: no recognizable hero
        return None, None, [], lines
    title = lines[i][2:].strip()
    i += 1
    if i < n and lines[i].startswith("## "):
        subtitle = lines[i][3:].strip()
        i += 1
    while i < n and lines[i].strip() != "---":
        s = lines[i].strip()
        m = META_LINE_RE.match(s)
        if m:
            meta.append((m.group(1), m.group(2)))
        i += 1
    if i < n:
        i += 1  # skip the '---' separator
    return title, subtitle, meta, lines[i:]


def render_hero(title: str, subtitle, meta) -> str:
    items = []
    for label, value in meta:
        wide = " wide" if len(value) > 40 else ""
        items.append(
            f'<div class="meta-item{wide}">'
            f'<span class="meta-label">{html.escape(label, quote=False)}</span>'
            f'<span class="meta-value">{render_inline(value)}</span></div>'
        )
    meta_html = f'<div class="hero-meta">{"".join(items)}</div>' if items else ""
    sub_html = f'<p class="hero-sub">{html.escape(subtitle, quote=False)}</p>' if subtitle else ""
    return (
        '<header class="hero"><div class="hero-inner">'
        '<p class="hero-kicker">ChronoForge · 策略研究文档</p>'
        f'<h1 class="doc-title">{html.escape(title, quote=False)}</h1>'
        f'{sub_html}{meta_html}</div></header>'
    )


def render_toc(toc) -> str:
    out, i = [], 0
    while i < len(toc):
        level, text, hid = toc[i]
        label = html.escape(text, quote=False)
        if level == 1:
            j = i + 1
            subs = []
            while j < len(toc) and toc[j][0] == 2:
                subs.append(f'<li><a href="#{toc[j][2]}">{html.escape(toc[j][1], quote=False)}</a></li>')
                j += 1
            if subs:
                out.append(f'<li class="toc-sec"><a href="#{hid}">{label}</a>'
                           f'<ul class="toc-sub">{"".join(subs)}</ul></li>')
            else:
                out.append(f'<li class="toc-sec"><a href="#{hid}">{label}</a></li>')
            i = j
        else:
            out.append(f'<li><a href="#{hid}">{label}</a></li>')
            i += 1
    return '<ul class="toc-list">' + "".join(out) + "</ul>" if out else ""


# ---------------------------------------------------------------------------
# page template
# ---------------------------------------------------------------------------

# raw string: backslashes below are kept verbatim so the JS string literals
# '\\[' and '\\(' reach KaTeX auto-render intact.
PAGE_TEMPLATE = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="generator" content="scripts/generate_fdam_html.py">
<title>__TITLE__</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/katex@0.16.11/dist/katex.min.css">
<script defer src="https://cdn.jsdelivr.net/npm/katex@0.16.11/dist/katex.min.js"></script>
<script defer src="https://cdn.jsdelivr.net/npm/katex@0.16.11/dist/contrib/auto-render.min.js"
  onload="renderMathInElement(document.body,{delimiters:[{left:'\\[',right:'\\]',display:true},{left:'\\(',right:'\\)',display:false}],throwOnError:false});"></script>
<style>
:root{
  --navy:#0b2239; --navy-2:#123a5c; --ink:#1d2a36; --muted:#5c6b7a;
  --accent:#c9973c; --line:#e4e9ef; --bg:#f2f4f7; --card:#ffffff;
}
*{box-sizing:border-box}
html{scroll-behavior:smooth}
body{
  margin:0; background:var(--bg); color:var(--ink);
  font:16px/1.75 "PingFang SC","Hiragino Sans GB","Microsoft YaHei",-apple-system,"Segoe UI",sans-serif;
}
.hero{
  background:linear-gradient(135deg,#0b2239 0%,#123a5c 62%,#1b4a70 100%);
  color:#fff; padding:56px 32px 48px;
}
.hero-inner{max-width:1280px; margin:0 auto}
.hero-kicker{
  margin:0; font-size:.75rem; letter-spacing:.32em; text-transform:uppercase;
  color:var(--accent); font-weight:600;
}
h1.doc-title{
  font-family:"Songti SC","Noto Serif SC","STSong",serif;
  font-size:2.4rem; font-weight:700; margin:.35em 0 .25em;
}
.hero-sub{margin:0 0 28px; font-size:1.08rem; color:#c9d6e2}
.hero-meta{display:grid; grid-template-columns:repeat(auto-fit,minmax(260px,1fr)); gap:12px}
.meta-item{
  background:rgba(255,255,255,.07); border:1px solid rgba(255,255,255,.16);
  border-radius:10px; padding:12px 16px;
}
.meta-item.wide{grid-column:1/-1}
.meta-label{display:block; font-size:.72rem; letter-spacing:.22em; color:#9fb3c8; margin-bottom:4px}
.meta-value{font-size:.92rem; color:#f2f6fa}
.page{
  max-width:1280px; margin:0 auto; padding:32px;
  display:grid; grid-template-columns:270px minmax(0,1fr); gap:32px; align-items:start;
}
.sidebar{
  position:sticky; top:24px; background:var(--card);
  border:1px solid var(--line); border-radius:14px; padding:18px;
  max-height:calc(100vh - 48px); overflow:auto;
}
.sidebar-title{
  margin:0 0 10px; font-size:.78rem; letter-spacing:.25em; color:var(--muted);
  text-transform:uppercase; font-weight:600;
}
.toc-list{list-style:none; margin:0; padding:0; font-size:.85rem}
.toc-list a{display:block; padding:4px 8px; border-radius:6px; color:var(--muted); text-decoration:none}
.toc-list a:hover{background:#eef2f6; color:var(--navy)}
.toc-sec > a{font-weight:600; color:var(--navy)}
.toc-sub{list-style:none; margin:2px 0 8px; padding-left:12px; border-left:2px solid var(--line)}
.content{
  background:var(--card); border:1px solid var(--line);
  border-radius:14px; padding:48px 56px; min-width:0;
}
.content h2.section{
  font-family:"Songti SC","Noto Serif SC","STSong",serif;
  font-size:1.55rem; color:var(--navy); border-bottom:2px solid var(--navy);
  padding-bottom:.4em; margin:2.2em 0 1em; scroll-margin-top:24px;
}
.content > h2.section:first-of-type{margin-top:0}
.content h3{font-size:1.2rem; color:var(--navy-2); margin:1.8em 0 .8em; scroll-margin-top:24px}
.content h4{font-size:1.02rem; margin:1.5em 0 .6em}
.table-wrap{overflow-x:auto; margin:1.2em 0; border:1px solid var(--line); border-radius:10px}
table{border-collapse:collapse; width:100%; font-size:.86rem; line-height:1.55}
th{background:var(--navy); color:#fff; text-align:left; padding:9px 12px; font-weight:600; white-space:nowrap}
td{padding:8px 12px; border-top:1px solid var(--line); vertical-align:top}
tbody tr:nth-child(even){background:#f7f9fb}
code{
  font-family:"SF Mono",Menlo,Consolas,monospace; font-size:.88em;
  background:#eef2f6; color:#0b3a5c; padding:.15em .4em; border-radius:5px;
}
pre.code-block{
  background:#0f1b2a; color:#d9e4ee; padding:20px 24px; border-radius:12px;
  overflow-x:auto; font-size:.82rem; line-height:1.6;
}
pre.code-block code{background:none; color:inherit; padding:0}
.math-block{overflow-x:auto; padding:14px 8px; margin:1em 0; text-align:center; font-size:1.02rem}
hr{border:0; border-top:1px solid var(--line); margin:2.5em 0}
ul,ol{padding-left:1.6em}
li{margin:.3em 0}
strong{color:var(--navy)}
.badge{display:inline-block; font-size:.72rem; font-weight:700; padding:.15em .65em; border-radius:999px; letter-spacing:.05em}
.badge.p0{background:#fde8e4; color:#a4362a}
.badge.p1{background:#e3edfb; color:#1d4ed8}
.badge.p2{background:#e9edf1; color:#55606c}
.doc-footer{margin-top:3em; padding-top:1.2em; border-top:1px solid var(--line); color:var(--muted); font-size:.8rem}
.toc-mobile{display:none}
@media (max-width:1080px){
  .page{grid-template-columns:1fr; padding:16px}
  .sidebar{display:none}
  .toc-mobile{
    display:block; background:#f7f9fb; border:1px solid var(--line);
    border-radius:10px; padding:12px 16px; margin-bottom:1.5em;
  }
  .toc-mobile summary{cursor:pointer; font-weight:600; color:var(--navy)}
  .content{padding:28px 22px}
  h1.doc-title{font-size:1.7rem}
}
@media print{
  body{background:#fff}
  .sidebar,.toc-mobile{display:none}
  .page{display:block; padding:0}
  .content{border:0; padding:0}
}
</style>
</head>
<body>
__HERO__
<div class="page">
  <aside class="sidebar">
    <p class="sidebar-title">目录</p>
    <nav class="toc">__TOC__</nav>
  </aside>
  <main class="content">
    <details class="toc-mobile"><summary>目录</summary><nav class="toc">__TOC__</nav></details>
    __BODY__
    <footer class="doc-footer">
      本页面由 <code>scripts/generate_fdam_html.py</code> 从
      <code>docs/strategy/__SOURCE__</code> 自动生成 · 生成时间 __GENERATED__
    </footer>
  </main>
</div>
</body>
</html>
"""


def main() -> None:
    md_text = MD_PATH.read_text(encoding="utf-8")
    title, subtitle, meta, rest_lines = extract_hero(md_text.splitlines())
    if title is None:
        title = MD_PATH.stem

    body, toc = render_content(rest_lines)
    toc_html = render_toc(toc)
    generated = datetime.now().strftime("%Y-%m-%d %H:%M")

    page = (
        PAGE_TEMPLATE
        .replace("__TITLE__", title)
        .replace("__HERO__", render_hero(title, subtitle, meta))
        .replace("__TOC__", toc_html)
        .replace("__BODY__", body)
        .replace("__SOURCE__", MD_PATH.name)
        .replace("__GENERATED__", generated)
    )

    OUT_PATH.write_text(page, encoding="utf-8")
    print(f"OK  {MD_PATH.name} -> {OUT_PATH.name}  ({len(page):,} bytes, {len(toc)} TOC entries)")


if __name__ == "__main__":
    main()
