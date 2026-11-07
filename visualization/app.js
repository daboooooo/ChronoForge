/* global echarts */
(function () {
  "use strict";

  const D = window.CF_DATA;
  if (!D) {
    document.body.insertAdjacentHTML(
      "afterbegin",
      '<div style="padding:20px;color:#ff5c5c">data.js 未加载，请先执行 bash visualization/refresh.sh</div>'
    );
    return;
  }

  const C = {
    blue: "#3d9bff",
    orange: "#f7931a",
    green: "#3ddc97",
    red: "#ff5c5c",
    purple: "#a78bfa",
    cyan: "#22d3ee",
    yellow: "#f5c542",
    grey: "#6b7a90",
    text: "#c9d4e3",
    muted: "#7b8ca4",
    line: "#273345",
    grid: "#182130",
  };

  const $ = (id) => document.getElementById(id);
  const MONO = 'ui-monospace, SFMono-Regular, Menlo, Consolas, monospace';

  // ── 基础格式 ─────────────────────────────────────────────
  const fmt = (v, d = 2) => (v === null || v === undefined || Number.isNaN(v) ? "—" : Number(v).toFixed(d));
  const sign = (v, d = 2) => (v === null || v === undefined ? "—" : (v >= 0 ? "+" : "") + Number(v).toFixed(d));
  const trillion = (vMillion) => vMillion / 1e6;
  const billion = (vMillion) => vMillion / 1e3;
  const comma = (v, d = 0) =>
    v === null || v === undefined ? "—" : Number(v).toLocaleString("en-US", { minimumFractionDigits: d, maximumFractionDigits: d });

  function pairs(s, scale = 1, digits = 3) {
    return s.t.map((t, i) => [t, Number((s.v[i] * scale).toFixed(digits))]);
  }
  function slice(s, start) {
    const out = { t: [], v: [] };
    for (let i = 0; i < s.t.length; i++) {
      if (s.t[i] >= start) {
        out.t.push(s.t[i]);
        out.v.push(s.v[i]);
      }
    }
    return out;
  }

  // ── ECharts 基础样式 ─────────────────────────────────────
  function xTime(extra) {
    return Object.assign(
      {
        type: "time",
        axisLine: { lineStyle: { color: C.line } },
        axisTick: { show: false },
        axisLabel: { color: "#6f8098", fontSize: 10, hideOverlap: true },
        splitLine: { show: false },
        axisPointer: { label: { backgroundColor: "#1a2431", color: C.text } },
      },
      extra || {}
    );
  }
  function yVal(extra) {
    return Object.assign(
      {
        type: "value",
        axisLine: { show: false },
        axisTick: { show: false },
        axisLabel: { color: "#6f8098", fontSize: 10 },
        splitLine: { lineStyle: { color: C.grid, type: "dashed" } },
        nameTextStyle: { color: "#6f8098", fontSize: 10, padding: [0, 0, 0, 0] },
      },
      extra || {}
    );
  }
  function base(extra) {
    return Object.assign(
      {
        backgroundColor: "transparent",
        animationDuration: 500,
        textStyle: { color: C.text, fontSize: 11, fontFamily: '-apple-system, "PingFang SC", "Microsoft YaHei", sans-serif' },
        grid: { left: 58, right: 24, top: 42, bottom: 38 },
        tooltip: {
          trigger: "axis",
          backgroundColor: "rgba(13,18,26,.96)",
          borderColor: C.line,
          borderWidth: 1,
          textStyle: { color: "#dbe6f5", fontSize: 11 },
          axisPointer: { lineStyle: { color: "#31435c" } },
        },
        legend: { top: 2, right: 6, textStyle: { color: "#8fa3bd", fontSize: 10 }, itemWidth: 12, itemHeight: 8, icon: "roundRect" },
        xAxis: xTime(),
        yAxis: yVal(),
      },
      extra || {}
    );
  }
  // time 轴下 series 数据是 [日期, 数值] 二元组：ECharts 原样把该数组放进 params.value，
  // 而 params.axisValue 是数值时间戳。tooltip 必须在维度层面拆解，不能按标量取值。
  function tipDate(t) {
    if (typeof t === "number" && Number.isFinite(t)) {
      const d = new Date(t);
      const p2 = (n) => String(n).padStart(2, "0");
      return `${d.getFullYear()}-${p2(d.getMonth() + 1)}-${p2(d.getDate())}`;
    }
    return String(t).slice(0, 10);
  }
  // 取数值维度：二元组取第 2 维，标量/对象数据原样返回
  function tipValue(p) {
    const v = p && p.value;
    if (Array.isArray(v)) return v[1];
    if (v && typeof v === "object") return v.value;
    return v;
  }
  // 取日期维度：优先用数据元组里的日期字符串，避免时间戳换算
  function tipDay(p) {
    const v = p && p.value;
    if (Array.isArray(v) && typeof v[0] === "string") return tipDate(v[0]);
    return tipDate(p ? p.axisValue : "");
  }
  function tipRow(color, name, value) {
    return `<div style="display:flex;gap:8px;align-items:center;margin-top:3px">
      <span style="width:7px;height:7px;border-radius:50%;background:${color}"></span>
      <span style="color:#8fa3bd">${name}</span>
      <span style="margin-left:auto;font-family:${MONO};color:#eaf2ff">${value}</span></div>`;
  }

  // ── 顶部状态读数 ─────────────────────────────────────────
  function initStatus() {
    $("dataCutoff").textContent = D.meta.data_cutoff;
    $("generatedAt").textContent = D.meta.generated_at;
    const st = D.status;

    $("statReserveCard").className = "stat tone-" + st.reserve_tone;
    $("statReserve").innerHTML = `${fmt(st.reserve_t, 2)}<small>万亿美元</small>`;
    $("statReserveSub").textContent =
      `${st.reserve_zone} · 4周 ${sign(D.framework.liquidity.reserve_4w_b, 1)}B · 截止 ${D.cutoffs.WRESBAL}`;

    const macroTone = st.macro_state.includes("/") ? "warn" : "blue";
    $("statMacroCard").className = "stat tone-" + macroTone;
    $("statMacro").innerHTML = `${st.macro_state}${st.macro_state.includes("/") ? " 并列" : " 偏向"}<small>${st.macro_pct}%</small>`;
    $("statMacroSub").textContent =
      `4/4 变量 · DXY ${sign(D.macro.last.chg20.dxy)} / 10Y ${sign(D.macro.last.chg20.ust10)} / TLT ${sign(D.macro.last.chg20.tlt)} / VIX ${sign(D.macro.last.chg20.vix)}`;

    $("statCorridorCard").className = "stat tone-" + st.corridor_tone;
    $("statCorridor").innerHTML = `${sign(st.corridor_spread_bp, 1)}<small>bp</small>`;
    $("statCorridorSub").textContent =
      `${st.corridor_status} · EFFR ${fmt(D.framework.liquidity.effr)}% / IORB ${fmt(D.framework.liquidity.iorb)}% · 截止 ${D.cutoffs.SOFR}`;

    $("statCorrCard").className = "stat tone-blue";
    $("statCorr").innerHTML = `${fmt(st.corr13, 2)}<small>13周</small>`;
    $("statCorrSub").textContent = `${st.corr_label} · 26周 ${fmt(D.corr.c26.filter((x) => x !== null).slice(-1)[0], 2)}`;
  }

  // ── Tab / 渲染调度 ───────────────────────────────────────
  const PANE_CHARTS = {
    d1: ["chart1", "chart2", "chart3", "chart4", "chart5", "chart6", "chart7"],
    d2: ["chart8", "chart8b", "chart9", "chart10", "chart12"],
    d3: ["chart11", "chart13", "chart14", "chart16", "chart17", "chart18"],
  };
  const renderers = {};
  const instances = {};

  function renderChart(id) {
    if (instances[id]) return;
    const dom = $(id);
    if (!dom || !renderers[id]) return;
    // 上次断言失败留下的占位提示，重试前清掉
    if (dom.firstElementChild && dom.firstElementChild.classList.contains("chart-fail")) dom.innerHTML = "";
    // 容器所在 pane 未激活时尺寸为 0×0：此时 init 会得到一个永久 0 尺寸画布（图 16 空白即此因）
    const pane = dom.closest(".pane");
    if (pane && !pane.classList.contains("active")) {
      console.warn(`[chart ${id}] 所在 pane 未激活，跳过渲染`);
      return;
    }
    const inst = echarts.init(dom, null, { renderer: "canvas" });
    instances[id] = inst;
    renderers[id](inst);
    // 渲染后断言：尺寸与系列数据都不能为空，否则丢弃实例并留下可见提示（不再静默空白）
    const opt = inst.getOption();
    const series = (opt && opt.series) || [];
    const hasData = series.some((s) => Array.isArray(s.data) && s.data.length > 0);
    if (!dom.clientWidth || !dom.clientHeight || !hasData) {
      console.warn(`[chart ${id}] 渲染断言失败：${dom.clientWidth}×${dom.clientHeight}，系列 ${series.length} 个，数据非空=${hasData}`);
      inst.dispose();
      delete instances[id];
      dom.innerHTML = `<div class="chart-fail" style="padding:20px;color:#ff5c5c;font-size:12px">图表渲染断言失败：容器 ${dom.clientWidth}×${dom.clientHeight} px，系列 ${series.length} 个，数据非空=${hasData}。已丢弃实例，切换 Tab 会重试。</div>`;
    }
  }
  function resizeAll() {
    Object.values(instances).forEach((i) => i.resize());
  }
  function activate(name) {
    document.querySelectorAll(".tab").forEach((b) => b.classList.toggle("active", b.dataset.pane === name));
    document.querySelectorAll(".pane").forEach((p) => p.classList.toggle("active", p.id === "pane-" + name));
    (PANE_CHARTS[name] || []).forEach(renderChart);
    window.setTimeout(resizeAll, 60);
  }
  function go(name, anchor) {
    activate(name);
    if (anchor) window.setTimeout(() => $(anchor).scrollIntoView({ behavior: "smooth", block: "start" }), 100);
  }
  document.querySelectorAll(".tab").forEach((btn) => (btn.onclick = () => activate(btn.dataset.pane)));
  document.querySelectorAll("[data-go]").forEach((card) => (card.onclick = () => go(card.dataset.go, card.dataset.anchor)));
  window.addEventListener("resize", resizeAll);

  // ══════════════════ 01 准备金 ══════════════════
  renderers.chart1 = (inst) => {
    const s = slice(D.series.WRESBAL, "2018-01-01");
    const data = s.t.map((t, i) => [t, Number((s.v[i] / 1e6).toFixed(3))]);
    inst.setOption(
      base({
        grid: { left: 62, right: 30, top: 34, bottom: 34 },
        legend: { show: false },
        yAxis: yVal({ name: "万亿美元", min: 1.2, max: 4.5 }),
        xAxis: xTime(),
        tooltip: {
          trigger: "axis",
          backgroundColor: "rgba(13,18,26,.96)",
          borderColor: C.line,
          formatter: (p) => `${tipDay(p[0])}<br>` +
            `<div style="font-family:${MONO};font-size:15px;color:#eaf2ff">${fmt(tipValue(p[0]), 2)}T</div>` +
            `<div style="color:${C.muted};font-size:10px">银行准备金 · WRESBAL</div>`,
        },
        series: [
          {
            name: "银行准备金",
            type: "line",
            data,
            showSymbol: false,
            lineStyle: { color: C.blue, width: 2 },
            areaStyle: {
              color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                { offset: 0, color: "rgba(61,155,255,.30)" },
                { offset: 1, color: "rgba(61,155,255,.02)" },
              ]),
            },
            markArea: {
              silent: true,
              data: [
                [{ yAxis: 1.2, itemStyle: { color: "rgba(255,92,92,.065)" } }, { yAxis: 3.0 }],
                [{ yAxis: 3.0, itemStyle: { color: "rgba(245,197,66,.065)" } }, { yAxis: 3.2 }],
                [{ yAxis: 3.2, itemStyle: { color: "rgba(61,220,151,.045)" } }, { yAxis: 4.5 }],
              ],
            },
            markLine: {
              silent: true,
              symbol: "none",
              lineStyle: { color: "#5b6b82", type: "dashed", width: 1 },
              label: { color: "#8fa3bd", fontSize: 9, position: "insideStartTop" },
              data: [
                { yAxis: 3.0, name: "紧张线 3.0T", label: { formatter: "紧张线 3.0T" } },
                { yAxis: 3.2, name: "充裕线 3.2T", label: { formatter: "充裕线 3.2T" } },
                { xAxis: "2019-09-17", name: "2019 钱荒", lineStyle: { color: "rgba(255,92,92,.35)" }, label: { formatter: "2019 钱荒", rotate: 90, position: "insideEndTop", color: "#b07a6a" } },
                { xAxis: "2021-11-10", name: "2021-11 BTC 顶", lineStyle: { color: "rgba(247,147,26,.35)" }, label: { formatter: "2021-11 BTC 顶", rotate: 90, position: "insideEndTop", color: "#b08a5e" } },
                { xAxis: "2023-03-15", name: "2023-03 准备金底", lineStyle: { color: "rgba(61,220,151,.30)" }, label: { formatter: "2023-03 准备金底", rotate: 90, position: "insideEndTop", color: "#5f9e86" } },
                { xAxis: "2024-03-20", name: "2024-03 阶段顶", lineStyle: { color: "rgba(61,155,255,.30)" }, label: { formatter: "2024-03 阶段顶", rotate: 90, position: "insideEndTop", color: "#5b86ad" } },
                { xAxis: "2025-08-01", name: "2025 TGA 抽水", lineStyle: { color: "rgba(255,92,92,.35)" }, label: { formatter: "2025 TGA 抽水", rotate: 90, position: "insideEndTop", color: "#b07a6a" } },
              ],
            },
          },
        ],
      }),
      true
    );
  };

  // ══════════════════ 02 周变化 ══════════════════
  renderers.chart2 = (inst) => {
    const d = D.weekly.reserve_diff;
    const data = d.t.map((t, i) => [t, d.v[i]]);
    const consec = d.t.map((t, i) => [t, d.consec[i]]);
    inst.setOption(
      base({
        grid: { left: 58, right: 52, top: 40, bottom: 36 },
        legend: { data: ["周变化", "连续下跌周数"], top: 2, right: 6 },
        xAxis: xTime(),
        yAxis: [
          yVal({ name: "$B", axisLabel: { color: "#6f8098", fontSize: 10, formatter: (v) => v } }),
          yVal({ name: "周", min: 0, max: 12, minInterval: 1, splitLine: { show: false }, axisLabel: { color: "#6f8098", fontSize: 10 } }),
        ],
        tooltip: {
          trigger: "axis",
          backgroundColor: "rgba(13,18,26,.96)",
          borderColor: C.line,
          formatter: (p) => `${tipDay(p[0])}<br>` +
            tipRow(tipValue(p[0]) >= 0 ? C.green : C.red, "准备金周变化", sign(tipValue(p[0]), 1) + "B") +
            tipRow(C.orange, "连续下跌", (p[1] ? tipValue(p[1]) : 0) + " 周"),
        },
        series: [
          {
            name: "周变化",
            type: "bar",
            data,
            barWidth: "60%",
            itemStyle: { color: (p) => (tipValue(p) >= 0 ? "rgba(61,220,151,.75)" : "rgba(255,92,92,.75)") },
          },
          {
            name: "连续下跌周数",
            type: "line",
            yAxisIndex: 1,
            data: consec,
            showSymbol: false,
            step: "end",
            lineStyle: { color: C.orange, width: 1.6 },
            areaStyle: { color: "rgba(247,147,26,.08)" },
            markLine: {
              silent: true,
              symbol: "none",
              lineStyle: { color: "rgba(245,197,66,.42)", type: "dashed" },
              label: { color: "#a99055", fontSize: 9, formatter: (p) => p.name },
              data: [
                { yAxis: 2, name: "Pattern 1" },
                { yAxis: 4, name: "Pattern 2" },
              ],
            },
          },
        ],
      }),
      true
    );
  };

  // ══════════════════ 03 三池 ══════════════════
  renderers.chart3 = (inst) => {
    const s = D.weekly.three_pool;
    inst.setOption(
      base({
        grid: { left: 58, right: 26, top: 40, bottom: 34 },
        legend: { data: ["准备金", "TGA", "ON RRP"], top: 2, right: 6 },
        xAxis: xTime(),
        // 三池堆叠总额峰值 6.23T（2022-04-20），不能写死上限；交给自动范围并留出顶部余量
        yAxis: yVal({ name: "万亿美元", min: 0 }),
        tooltip: {
          trigger: "axis",
          backgroundColor: "rgba(13,18,26,.96)",
          borderColor: C.line,
          formatter: (p) => {
            const total = p.reduce((acc, x) => acc + (tipValue(x) || 0), 0);
            return `${tipDay(p[0])}<br>` +
              p.map((x) => tipRow(x.color, x.seriesName, fmt(tipValue(x), 2) + "T")).join("") +
              tipRow("#dbe6f5", "合计（堆叠顶）", fmt(total, 2) + "T") +
              `<div style="margin-top:4px;color:${C.muted};font-size:10px">三者之和为概念展示，不是严格恒等式</div>`;
          },
        },
        series: [
          {
            name: "准备金",
            type: "line",
            stack: "pool",
            data: s.t.map((t, i) => [t, s.reserve[i]]),
            symbol: "none",
            lineStyle: { color: C.blue, width: 1.6 },
            areaStyle: { color: "rgba(61,155,255,.42)" },
            markArea: {
              silent: true,
              data: [
                [{ xAxis: "2022-01-01", itemStyle: { color: "rgba(167,139,250,.055)" } }, { xAxis: "2023-12-31" }],
                [{ xAxis: "2025-01-01", itemStyle: { color: "rgba(255,92,92,.045)" } }, { xAxis: "2025-12-31" }],
              ],
            },
          },
          {
            name: "TGA",
            type: "line",
            stack: "pool",
            data: s.t.map((t, i) => [t, s.tga[i]]),
            symbol: "none",
            lineStyle: { color: C.orange, width: 1.4 },
            areaStyle: { color: "rgba(247,147,26,.42)" },
          },
          {
            name: "ON RRP",
            type: "line",
            stack: "pool",
            data: s.t.map((t, i) => [t, s.onrrp[i]]),
            symbol: "none",
            lineStyle: { color: C.purple, width: 1.4 },
            areaStyle: { color: "rgba(167,139,250,.42)" },
          },
        ],
      }),
      true
    );
  };

  // ══════════════════ 04 瀑布 ══════════════════
  function fillWaterfallSelect() {
    const sel = $("wfSelect");
    if (!sel || sel.options.length) return;
    const rows = D.weekly.decomp;
    const dates = rows.map((r) => r.t).slice().reverse();
    sel.innerHTML = dates.map((t) => `<option value="${t}">${t}</option>`).join("");
  }
  renderers.chart4 = (inst) => {
    const rows = D.weekly.decomp;
    const sel = $("wfSelect");
    const date = (sel && sel.value) || rows[rows.length - 1].t;
    const row = rows.find((r) => r.t === date) || rows[rows.length - 1];
    const cats = ["ΔA 资产", "TGA", "ON RRP", "现金", "其他", "ΔR 合计"];
    const vals = [row.dA, row.tga, row.onrrp, row.cash, row.other];
    const colors = [C.blue, C.orange, C.purple, C.cyan, C.grey];

    let cum = 0;
    const baseV = [];
    const delta = [];
    vals.forEach((v) => {
      if (v >= 0) {
        baseV.push(cum);
        delta.push(v);
      } else {
        baseV.push(cum + v);
        delta.push(-v);
      }
      cum += v;
    });
    baseV.push(Math.min(0, cum));
    delta.push(Math.abs(cum));
    const totalColor = cum >= 0 ? C.green : C.red;

    inst.setOption(
      base({
        grid: { left: 52, right: 18, top: 26, bottom: 32 },
        legend: { show: false },
        xAxis: {
          type: "category",
          data: cats,
          axisLine: { lineStyle: { color: C.line } },
          axisTick: { show: false },
          axisLabel: { color: "#8fa3bd", fontSize: 10 },
        },
        yAxis: yVal({ name: "$B", axisLabel: { color: "#6f8098", fontSize: 10 } }),
        tooltip: {
          trigger: "axis",
          backgroundColor: "rgba(13,18,26,.96)",
          borderColor: C.line,
          formatter: (p) => `${date}<br>` +
            `<div style="font-family:${MONO};font-size:15px;color:${totalColor}">${sign(cum, 1)}B</div>` +
            p.filter((x) => x.seriesName !== "起点").map((x) => tipRow(x.color, x.seriesName, x.value)).join(""),
        },
        series: [
          { name: "起点", type: "bar", stack: "w", data: baseV, itemStyle: { color: "transparent" }, silent: true, barWidth: "52%" },
          {
            name: row.t,
            type: "bar",
            stack: "w",
            data: delta.map((v, i) => ({
              value: v,
              itemStyle: { color: i === 5 ? totalColor : colors[i] },
            })),
            barWidth: "52%",
            label: {
              show: true,
              position: "top",
              fontSize: 9,
              fontFamily: MONO,
              formatter: (p) => {
                const i = p.dataIndex;
                const raw = i === 5 ? cum : vals[i];
                return (raw >= 0 ? "+" : "") + Number(raw).toFixed(1);
              },
              color: "#9fb1c9",
            },
          },
        ],
      }),
      true
    );
  };

  // ══════════════════ 05 TGA ══════════════════
  renderers.chart5 = (inst) => {
    const daily = slice(D.tga.daily, "2023-01-01");
    const fc = D.tga.forecast;
    const sc = fc.scenarios;
    const baseSc = sc.base || sc.liquidity_easing || Object.values(sc)[0];

    const bandLo = baseSc ? baseSc.t.map((t, i) => [t, baseSc.lo[i] / 1e6]) : [];
    const band = baseSc ? baseSc.t.map((t, i) => [t, (baseSc.hi[i] - baseSc.lo[i]) / 1e6]) : [];
    const scSeries = Object.entries(sc).map(([key, v], idx) => {
      const names = { base: "基线", liquidity_easing: "宽松", liquidity_tightening: "收紧" };
      const cols = { base: C.blue, liquidity_easing: C.green, liquidity_tightening: C.red };
      return {
        name: names[key] || key,
        type: "line",
        data: v.t.map((t, i) => [t, v.v[i] / 1e6]),
        symbol: "none",
        lineStyle: { color: cols[key] || C.grey, width: 1.5, type: "dashed" },
        z: 3,
      };
    });

    inst.setOption(
      base({
        grid: { left: 58, right: 28, top: 40, bottom: 34 },
        legend: { data: ["TGA 日度", "基线", "宽松", "收紧"], top: 2, right: 6 },
        xAxis: xTime(),
        yAxis: yVal({ name: "万亿美元", min: 0, max: 1.45 }),
        tooltip: {
          trigger: "axis",
          backgroundColor: "rgba(13,18,26,.96)",
          borderColor: C.line,
          formatter: (p) => `${tipDay(p[0])}<br>` +
            p.map((x) => tipRow(x.color, x.seriesName, fmt(tipValue(x), 3) + "T")).join(""),
        },
        series: [
          {
            name: "基线区间下沿",
            type: "line",
            data: bandLo,
            stack: "band",
            symbol: "none",
            lineStyle: { opacity: 0 },
            areaStyle: { opacity: 0 },
            silent: true,
            z: 1,
          },
          {
            name: "基线区间",
            type: "line",
            data: band,
            stack: "band",
            symbol: "none",
            lineStyle: { opacity: 0 },
            areaStyle: { color: "rgba(61,155,255,.15)" },
            silent: true,
            z: 1,
          },
          {
            name: "TGA 日度",
            type: "line",
            data: pairs(daily, 1 / 1e6, 4),
            showSymbol: false,
            lineStyle: { color: "#9bb6d6", width: 1.4 },
            areaStyle: {
              color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                { offset: 0, color: "rgba(155,182,214,.10)" },
                { offset: 1, color: "rgba(155,182,214,.0)" },
              ]),
            },
            markLine: {
              silent: true,
              symbol: "none",
              lineStyle: { color: "rgba(245,197,66,.45)", type: "dashed", width: 1 },
              label: { color: "#b9a25c", fontSize: 9, position: "insideStartTop" },
              data: (D.tga.qra || []).map((q) => ({
                yAxis: q.value / 1e6,
                name: `QRA ${q.value === 1000000 ? "1 万亿" : "8500 亿"}`,
                label: { formatter: `QRA ${q.value === 1000000 ? "1 万亿" : "8500 亿"}` },
              })),
            },
            z: 4,
          },
          ...scSeries,
        ],
      }),
      true
    );
  };

  // ══════════════════ 06 走廊 ══════════════════
  renderers.chart6 = (inst) => {
    const start = "2021-07-01";
    const S = D.series;
    const mk = (name, s, color, width, type) => ({
      name,
      type: "line",
      data: pairs(slice(s, start)),
      showSymbol: false,
      lineStyle: { color, width: width || 1.5, type: type || "solid" },
    });
    inst.setOption(
      base({
        grid: { left: 54, right: 22, top: 42, bottom: 34 },
        legend: { data: ["IORB 上界", "EFFR", "SOFR", "ON RRP 下限"], top: 2, right: 6 },
        xAxis: xTime(),
        yAxis: yVal({ name: "%", min: 0, max: 5.6, axisLabel: { color: "#6f8098", fontSize: 10, formatter: (v) => v.toFixed(1) } }),
        tooltip: {
          trigger: "axis",
          backgroundColor: "rgba(13,18,26,.96)",
          borderColor: C.line,
          formatter: (p) => `${tipDay(p[0])}<br>` +
            p.map((x) => tipRow(x.color, x.seriesName, fmt(tipValue(x), 2) + "%")).join("") +
            `<div style="margin-top:4px;color:${C.muted};font-size:10px">正常关系：ON RRP &lt; EFFR &lt; IORB</div>`,
        },
        series: [
          mk("IORB 上界", S.IORB, C.blue, 1.8),
          mk("EFFR", S.EFFR, C.orange, 1.8),
          mk("SOFR", S.SOFR, C.cyan, 1.2, "dashed"),
          mk("ON RRP 下限", S.RRP_AWARD, C.purple, 1.6),
        ],
      }),
      true
    );
  };

  // ══════════════════ 07 SOFR 利差 ══════════════════
  function spreadPairs() {
    const a = D.series.SOFR;
    const b = D.series.EFFR;
    const map = new Map(b.t.map((t, i) => [t, b.v[i]]));
    const out = [];
    for (let i = 0; i < a.t.length; i++) {
      const tv = map.get(a.t[i]);
      if (tv !== undefined) out.push([a.t[i], Number(((a.v[i] - tv) * 100).toFixed(1))]);
    }
    return out;
  }
  renderers.chart7 = (inst) => {
    const data = spreadPairs().filter((x) => x[0] >= "2019-01-01");
    const last = data[data.length - 1];
    const srf = D.series.SRF_USAGE || { t: [], v: [] };
    const srfData = srf.t.map((t, i) => [t, srf.v[i]]);
    const srfMax = srf.v.length
      ? { value: Math.max(...srf.v), t: srf.t[srf.v.indexOf(Math.max(...srf.v))] }
      : null;
    inst.setOption(
      base({
        grid: { left: 52, right: 58, top: 30, bottom: 34 },
        legend: { data: ["SOFR − EFFR", "SRF 用量"], top: 2, right: 6 },
        xAxis: xTime(),
        yAxis: [
          // SOFR−EFFR 在 2019-09-17 回购危机单日冲到 295bp，上限不能写死 35
          yVal({ name: "bp", min: -20 }),
          yVal({
            name: "SRF $B",
            min: 0,
            position: "right",
            splitLine: { show: false },
            axisLabel: { color: "#6f8098", fontSize: 10 },
          }),
        ],
        tooltip: {
          trigger: "axis",
          backgroundColor: "rgba(13,18,26,.96)",
          borderColor: C.line,
          formatter: (p) => {
            const bp = p.find((x) => x.seriesName === "SOFR − EFFR");
            const s = p.find((x) => x.seriesName === "SRF 用量");
            return `${tipDay(p[0])}` +
              `<div style="font-family:${MONO};font-size:15px;color:#eaf2ff">${sign(tipValue(bp), 1)} bp</div>` +
              `<div style="color:${C.muted};font-size:10px">SOFR − EFFR · 预警 12bp / 极端 17bp</div>` +
              (tipValue(s) !== undefined && tipValue(s) !== null
                ? tipRow(C.orange, "SRF 用量", `${fmt(tipValue(s), 3)} $B`)
                : "");
          },
        },
        series: [
          {
            name: "SOFR − EFFR",
            type: "bar",
            data,
            barWidth: "70%",
            itemStyle: {
              color: (p) => (tipValue(p) > 17 ? "rgba(255,92,92,.85)" : tipValue(p) > 12 ? "rgba(245,197,66,.8)" : "rgba(61,155,255,.55)"),
            },
            markLine: {
              silent: true,
              symbol: "none",
              lineStyle: { type: "dashed", width: 1 },
              label: { fontSize: 9, color: "#a99055", position: "insideEndTop" },
              data: [
                { yAxis: 12, name: "12bp 预警", lineStyle: { color: "rgba(245,197,66,.5)" }, label: { formatter: "12bp 预警" } },
                { yAxis: 17, name: "17bp 极端", lineStyle: { color: "rgba(255,92,92,.5)" }, label: { formatter: "17bp 极端" } },
              ],
            },
            markPoint: last
              ? {
                  symbol: "circle",
                  symbolSize: 7,
                  itemStyle: { color: last[1] >= 12 ? C.red : C.blue },
                  label: { show: true, formatter: `${sign(last[1], 1)}bp`, position: "top", color: "#eaf2ff", fontSize: 10, fontFamily: MONO },
                  data: [{ coord: last }],
                }
              : undefined,
          },
          {
            name: "SRF 用量",
            type: "line",
            yAxisIndex: 1,
            data: srfData,
            showSymbol: false,
            lineStyle: { color: "rgba(247,147,26,.75)", width: 1.4 },
            areaStyle: { color: "rgba(247,147,26,.12)" },
            markPoint: srfMax
              ? {
                  symbol: "circle",
                  symbolSize: 6,
                  itemStyle: { color: C.orange },
                  label: { show: true, formatter: `${fmt(srfMax.value, 2)}B`, position: "top", color: "#eaf2ff", fontSize: 9, fontFamily: MONO },
                  data: [{ coord: [srfMax.t, srfMax.value] }],
                }
              : undefined,
          },
        ],
      }),
      true
    );
  };

  // ══════════════════ 16 准备金预测 fan chart ══════════════════
  renderers.chart16 = (inst) => {
    const rf = D.reserve_forecast || {};
    const dom = $("chart16");
    // payload 一致性校验：forecast 每条必须具备 t/v/lo/hi 且为有限数，否则不画半成品
    const badFc = !rf.forecast || !rf.forecast.length ||
      rf.forecast.some((x) => !x.t || ![x.v, x.lo, x.hi].every((n) => Number.isFinite(n)));
    if (badFc) {
      if (dom) dom.innerHTML = `<div style="padding:20px;color:#ff5c5c">无有效准备金预测运行（${rf.model_version || "reserve-structural"}）：forecast 缺少 t/v/lo/hi</div>`;
      console.warn("[chart16] reserve_forecast payload 校验失败", rf.forecast);
      return;
    }
    const start = "2023-01-01";
    const actual = slice(D.series.WRBWFRBL, start);
    const actualData = actual.t.map((t, i) => [t, actual.v[i] / 1e6]);
    const bt = (rf.backtest && rf.backtest["1"]) || { t: [], pred: [] };
    const btData = bt.t.map((t, i) => [t, bt.pred[i] / 1e6]);

    const f = rf.forecast;
    const originT = rf.origin_date;
    // PIT：origin 周三收盘时当周观测尚未发布，扇图必须锚定在 origin 之前最近可知的观测，
    // 否则会画出“origin 当周已发布值 → 上周可知值”的伪跳变。
    const originIdx = actual.t.findIndex((t) => t >= originT);
    const anchorIdx = originIdx > 0 ? originIdx - 1 : Math.max(actual.t.length - 1, 0);
    const anchorT = actual.t[anchorIdx] || originT;
    const anchor = actual.v.length ? actual.v[anchorIdx] / 1e6 : f[0].v / 1e6;
    const bandLo = [[anchorT, anchor]].concat(f.map((x) => [x.t, x.lo / 1e6]));
    const bandW = [[anchorT, 0]].concat(f.map((x) => [x.t, (x.hi - x.lo) / 1e6]));
    const fcLine = [[anchorT, anchor]].concat(f.map((x) => [x.t, x.v / 1e6]));

    const stats = rf.stats || {};
    const statLine = Object.entries(stats)
      .map(([h, s]) => `h=${h}周 近5年 MAE ${fmt(s.mae / 1000, 1)}B / RMSE ${fmt(s.rmse / 1000, 1)}B（全样本 ${fmt(s.mae_full / 1000, 1)}B / ${fmt(s.rmse_full / 1000, 1)}B）`)
      .join(" · ");
    if ($("rfStats")) {
      $("rfStats").textContent =
        `${rf.model_version} · origin ${originT} · 近5年 n=${(stats["1"] || {}).n || 0} / 全样本 n=${(stats["1"] || {}).n_full || 0} · ${statLine}`;
    }
    if ($("rfBench")) {
      $("rfBench").textContent = rf.benchmark_note || "";
    }

    inst.setOption(
      base({
        grid: { left: 62, right: 28, top: 34, bottom: 34 },
        legend: { data: ["实际 WRBWFRBL", "回测 h=1 预测", "预测中值", "90% 联合误差带"], top: 2, right: 6 },
        xAxis: xTime(),
        yAxis: yVal({ name: "万亿美元", min: 2.4, max: 3.7 }),
        tooltip: {
          trigger: "axis",
          backgroundColor: "rgba(13,18,26,.96)",
          borderColor: C.line,
          formatter: (p) => {
            const rows = p.filter((x) => !["90% 区间下沿", "90% 联合误差带"].includes(x.seriesName));
            return `${tipDay(p[0])}<br>` + rows.map((x) =>
              tipRow(x.color, x.seriesName, fmt(tipValue(x), 2) + "T")).join("");
          },
        },
        series: [
          {
            name: "90% 区间下沿",
            type: "line",
            data: bandLo,
            stack: "rband",
            symbol: "none",
            lineStyle: { opacity: 0 },
            areaStyle: { opacity: 0 },
            silent: true,
            z: 1,
          },
          {
            name: "90% 联合误差带",
            type: "line",
            data: bandW,
            stack: "rband",
            symbol: "none",
            lineStyle: { opacity: 0 },
            areaStyle: { color: "rgba(61,155,255,.18)" },
            silent: true,
            z: 1,
          },
          {
            name: "实际 WRBWFRBL",
            type: "line",
            data: actualData,
            showSymbol: false,
            lineStyle: { color: C.blue, width: 2 },
            areaStyle: {
              color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                { offset: 0, color: "rgba(61,155,255,.16)" },
                { offset: 1, color: "rgba(61,155,255,.0)" },
              ]),
            },
            markLine: {
              silent: true,
              symbol: "none",
              lineStyle: { color: "rgba(245,197,66,.40)", type: "dashed", width: 1 },
              label: { color: "#b9a25c", fontSize: 9, position: "insideStartTop" },
              data: [
                { yAxis: 3.0, name: "紧张 3.0T", label: { formatter: "3.0T" } },
                { yAxis: 3.2, name: "预警 3.2T", label: { formatter: "3.2T" } },
              ],
            },
            z: 4,
          },
          {
            name: "回测 h=1 预测",
            type: "line",
            data: btData,
            showSymbol: false,
            lineStyle: { color: "rgba(155,182,214,.65)", width: 1.2, type: "dashed" },
            z: 3,
          },
          {
            name: "预测中值",
            type: "line",
            data: fcLine,
            symbol: "circle",
            symbolSize: 6,
            lineStyle: { color: C.orange, width: 1.6, type: "dashed" },
            itemStyle: { color: C.orange },
            label: {
              show: true,
              position: "top",
              fontSize: 9,
              color: "#f7b35c",
              formatter: (p) => `${fmt(p.value[1], 2)}T`,
            },
            z: 5,
          },
        ],
      }),
      true
    );
  };

  // ══════════════════ 08 四变量 ══════════════════
  renderers.chart8 = (inst) => {
    const m = D.macro;
    const mk = (name, arr, color, width) => ({
      name,
      type: "line",
      data: arr,
      showSymbol: false,
      lineStyle: { color, width: width || 1.5 },
    });
    inst.setOption(
      base({
        grid: { left: 52, right: 18, top: 40, bottom: 30 },
        legend: { data: ["DXY (Z)", "UST10Y (Z)", "TLT (Z)", "VIX (Z)"], top: 2, right: 6 },
        xAxis: xTime(),
        // VIX Z 在 2025-04-08 达 6.55，±3.2 会裁掉极端值，交给自动范围
        yAxis: yVal({ name: "Z-Score" }),
        tooltip: {
          trigger: "axis",
          backgroundColor: "rgba(13,18,26,.96)",
          borderColor: C.line,
          formatter: (p) => `${tipDay(p[0])}<br>` +
            p.map((x) => tipRow(x.color, x.seriesName, fmt(tipValue(x), 2))).join("") +
            `<div style="margin-top:4px;color:${C.muted};font-size:10px">4/4 变量（TLT 价格，↑ = 长端利率下行）</div>`,
        },
        series: [
          mk("DXY (Z)", m.t.map((t, i) => [t, m.dxy_z[i]]), C.blue, 1.6),
          mk("UST10Y (Z)", m.t.map((t, i) => [t, m.ust_z[i]]), C.orange, 1.6),
          mk("TLT (Z)", m.t.map((t, i) => [t, m.tlt_z[i]]), C.purple, 1.4),
          mk("VIX (Z)", m.t.map((t, i) => [t, m.vix_z[i]]), C.red, 1.4),
        ],
      }),
      true
    );
  };

  renderers.chart8b = (inst) => {
    const labels = ["A", "B", "C", "D"];
    const pct = labels.map((k) => D.macro.match[k].pct);
    inst.setOption(
      base({
        grid: { left: 30, right: 42, top: 18, bottom: 20 },
        legend: { show: false },
        xAxis: { type: "value", max: 100, axisLine: { show: false }, axisTick: { show: false }, axisLabel: { color: "#6f8098", fontSize: 9, formatter: "{value}%" }, splitLine: { show: false } },
        yAxis: { type: "category", data: labels, axisLine: { show: false }, axisTick: { show: false }, axisLabel: { color: "#8fa3bd", fontSize: 10, fontFamily: MONO } },
        tooltip: {
          trigger: "axis",
          backgroundColor: "rgba(13,18,26,.96)",
          borderColor: C.line,
          formatter: (p) => `状态 ${p[0].name}：${p[0].value}%（4/4 变量）`,
        },
        series: [
          {
            name: "20日方向匹配",
            type: "bar",
            data: pct.map((v, i) => ({
              value: v,
              itemStyle: { color: D.macro.ties.includes(labels[i]) ? C.blue : "#2a3441" },
            })),
            barWidth: "50%",
            label: { show: true, position: "right", color: "#9fb1c9", fontSize: 10, fontFamily: MONO, formatter: "{c}%" },
          },
        ],
      }),
      true
    );
  };

  function buildStateTable() {
    const t = $("stateTable");
    if (!t) return;
    const states = [
      { k: "A", dxy: true, ust: true, tlt: false, vix: true },
      { k: "B", dxy: true, ust: false, tlt: true, vix: true },
      { k: "C", dxy: false, ust: true, tlt: false, vix: false },
      { k: "D", dxy: false, ust: false, tlt: true, vix: false },
    ];
    const dir = D.macro.directions;
    const cell = (expected, actual) => {
      const arrow = expected ? "↑" : "↓";
      const hit = expected === actual;
      return `<td class="dir ${hit ? "hit" : "miss"}">${arrow}</td>`;
    };
    const rows = states
      .map((s) => {
        const best = D.macro.ties.includes(s.k) ? ' class="best"' : "";
        return `<tr${best}><td class="mono">${s.k}</td>
          ${cell(s.dxy, dir.dxy_up)}
          ${cell(s.ust, dir.ust_up)}
          ${cell(s.tlt, dir.tlt_up)}
          ${cell(s.vix, dir.vix_up)}
          <td class="mono">${D.macro.match[s.k].hits}/4 · ${D.macro.match[s.k].pct}%</td></tr>`;
      })
      .join("");
    t.innerHTML = `<thead><tr><th>状态</th><th>DXY</th><th>UST10Y</th><th>TLT</th><th>VIX</th><th>4/4 命中</th></tr></thead><tbody>${rows}</tbody>`;
  }

  // ══════════════════ 09 收益率拆分 ══════════════════
  renderers.chart9 = (inst) => {
    const y = D.yield_split;
    inst.setOption(
      base({
        grid: { left: 54, right: 22, top: 40, bottom: 34 },
        legend: { data: ["实际利率", "盈亏平衡通胀", "名义 10Y"], top: 2, right: 6 },
        xAxis: xTime(),
        // 实际利率（DFII10）存在负值（最低 -1.19%），坐标轴不能锁 min=0，交给自动范围覆盖正负全区间
        yAxis: yVal({ name: "%" }),
        tooltip: {
          trigger: "axis",
          backgroundColor: "rgba(13,18,26,.96)",
          borderColor: C.line,
          formatter: (p) => `${tipDay(p[0])}<br>` +
            p.map((x) => tipRow(x.color, x.seriesName, sign(tipValue(x), 2) + "%")).join(""),
        },
        series: [
          {
            name: "实际利率",
            type: "line",
            data: y.t.map((t, i) => [t, y.real[i]]),
            symbol: "none",
            lineStyle: { color: C.blue, width: 1.2 },
            // 实际利率自身可从正值转为负值（2020-2022），以 0 为基线填充，负值向下可见
            areaStyle: { color: "rgba(61,155,255,.38)" },
            markLine: {
              silent: true,
              symbol: "none",
              lineStyle: { color: "#5b6b82", type: "dashed", width: 1 },
              label: { show: false },
              data: [{ yAxis: 0 }],
            },
          },
          {
            name: "盈亏平衡通胀",
            type: "line",
            data: y.t.map((t, i) => [t, y.be[i]]),
            symbol: "none",
            lineStyle: { color: C.orange, width: 1.4 },
          },
          {
            name: "名义 10Y",
            type: "line",
            data: y.t.map((t, i) => [t, y.nominal[i]]),
            symbol: "none",
            lineStyle: { color: "#dbe6f5", width: 1.6 },
            z: 3,
          },
        ],
      }),
      true
    );
  };

  // ══════════════════ 10 曲线 ══════════════════
  let curveIndex = 0;
  let curveTimer = null;
  const TENOR_LABEL = { 0.08: "4W", 0.5: "6M", 1: "1Y", 2: "2Y", 10: "10Y", 20: "20Y", 30: "30Y" };
  function tenorLabel(v) {
    const hit = Object.keys(TENOR_LABEL).map(Number).find((x) => Math.abs(x - v) < 0.051);
    return hit === undefined ? "" : TENOR_LABEL[hit];
  }

  function curveOption(i) {
    const yc = D.yield_curve;
    const vals = yc.values[i];
    const tips = yc.tips10[i];
    const date = yc.dates[i];
    const inv = vals[4] !== null && vals[3] !== null ? (vals[4] - vals[3]) * 100 : null;
    return base({
      grid: { left: 54, right: 26, top: 30, bottom: 36 },
      legend: { show: false },
      xAxis: {
        type: "category",
        data: ["4W", "6M", "1Y", "2Y", "10Y", "10Y TIPS", "20Y", "30Y"],
        boundaryGap: false,
        axisLine: { lineStyle: { color: C.line } },
        axisTick: { show: false },
        splitLine: { show: false },
        axisLabel: { color: "#6f8098", fontSize: 10 },
      },
      // 10Y TIPS（DFII10）有 28 个负值月份（最低 -1.16%），min 必须自动
      yAxis: yVal({ name: "%", max: 6 }),
      tooltip: {
        trigger: "axis",
        backgroundColor: "rgba(13,18,26,.96)",
        borderColor: C.line,
        formatter: (p) => {
          const rows = p
            .filter((x) => x.seriesType === "line" && x.value !== null && x.value !== undefined)
            .map((x) => tipRow(C.blue, x.name, fmt(x.value, 2) + "%"))
            .join("");
          const tipRowHtml = p.some((x) => x.seriesType === "scatter")
            ? tipRow(C.orange, "10Y TIPS", fmt(tips, 2) + "%")
            : "";
          return `${date}<br>${rows}${tipRowHtml}` +
            `<div style="margin-top:4px;color:${C.muted};font-size:10px">10Y − 2Y = ${inv === null ? "—" : sign(inv, 1) + " bp"}</div>`;
        },
      },
      series: [
        {
          name: "收益率曲线",
          type: "line",
          data: [vals[0], vals[1], vals[2], vals[3], vals[4], null, vals[5], vals[6]],
          connectNulls: true,
          smooth: true,
          symbol: "circle",
          symbolSize: 7,
          lineStyle: { color: C.blue, width: 2 },
          itemStyle: { color: C.blue },
          areaStyle: {
            color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
              { offset: 0, color: "rgba(61,155,255,.22)" },
              { offset: 1, color: "rgba(61,155,255,.01)" },
            ]),
          },
        },
        {
          name: "10Y TIPS",
          type: "scatter",
          data: [[5, tips]],
          symbol: "diamond",
          symbolSize: 10,
          itemStyle: { color: C.orange },
          label: { show: true, formatter: "TIPS", position: "top", color: C.orange, fontSize: 9 },
        },
      ],
    });
  }

  function renderCurve(i) {
    curveIndex = Math.max(0, Math.min(i, D.yield_curve.dates.length - 1));
    const sel = $("curveDate");
    if (sel) sel.value = D.yield_curve.dates[curveIndex];
    const vals = D.yield_curve.values[curveIndex];
    const inv = vals[4] !== null && vals[3] !== null ? (vals[4] - vals[3]) * 100 : null;
    const meta = $("curveMeta");
    if (meta) {
      meta.textContent = `${D.yield_curve.dates[curveIndex]} · 10Y−2Y ${inv === null ? "—" : sign(inv, 1) + "bp"}`;
    }
    if (instances.chart10) instances.chart10.setOption(curveOption(curveIndex), true);
  }

  renderers.chart10 = (inst) => {
    const sel = $("curveDate");
    if (sel && !sel.options.length) {
      sel.innerHTML = D.yield_curve.dates
        .map((d, i) => `<option value="${d}">${d}</option>`)
        .reverse()
        .join("");
    }
    curveIndex = D.yield_curve.dates.length - 1;
    renderCurve(curveIndex);
    if (sel) {
      sel.onchange = () => {
        stopCurve();
        renderCurve(D.yield_curve.dates.indexOf(sel.value));
      };
    }
    const btn = $("curvePlay");
    if (btn) {
      btn.onclick = () => {
        if (curveTimer) {
          stopCurve();
          return;
        }
        btn.textContent = "停止";
        curveIndex = Math.max(0, D.yield_curve.dates.length - 36);
        renderCurve(curveIndex);
        curveTimer = window.setInterval(() => {
          if (curveIndex >= D.yield_curve.dates.length - 1) {
            stopCurve();
            return;
          }
          renderCurve(curveIndex + 1);
        }, 850);
      };
    }
  };
  function stopCurve() {
    if (curveTimer) window.clearInterval(curveTimer);
    curveTimer = null;
    const btn = $("curvePlay");
    if (btn) btn.textContent = "播放";
  }

  // ══════════════════ 12 罗素 2000 相对强弱 ══════════════════
  renderers.chart12 = (inst) => {
    const r = D.russell;
    const ratioData = r.t.map((t, i) => [t, r.ratio[i]]);
    const vixData = r.t.map((t, i) => [t, r.vix[i]]);
    inst.setOption(
      base({
        grid: { left: 52, right: 52, top: 40, bottom: 34 },
        legend: { data: ["RUT / SPX（首日=100）", "VIX"], top: 2, right: 6 },
        xAxis: xTime(),
        yAxis: [
          yVal({ name: "相对强弱", min: null }),
          yVal({
            name: "VIX",
            position: "right",
            min: 0,
            splitLine: { show: false },
            axisLabel: { color: "#6f8098", fontSize: 10 },
          }),
        ],
        tooltip: {
          trigger: "axis",
          backgroundColor: "rgba(13,18,26,.96)",
          borderColor: C.line,
          formatter: (p) => {
            const rt = p.find((x) => x.seriesName.indexOf("RUT") === 0);
            const vx = p.find((x) => x.seriesName === "VIX");
            const i = p[0].dataIndex;
            return `${tipDay(p[0])}` +
              `<div style="font-family:${MONO};font-size:15px;color:#eaf2ff">${fmt(tipValue(rt), 2)}</div>` +
              `<div style="color:${C.muted};font-size:10px">罗素 2000 / 标普 500 · 基准 ${r.base_date}=100</div>` +
              tipRow(C.red, "VIX", fmt(tipValue(vx), 2)) +
              tipRow(C.grey, "RUT", fmt(r.rut[i], 0)) +
              tipRow(C.grey, "SPX", fmt(r.spx[i], 0));
          },
        },
        series: [
          {
            name: "RUT / SPX（首日=100）",
            type: "line",
            data: ratioData,
            showSymbol: false,
            lineStyle: { color: C.green, width: 1.8 },
            areaStyle: {
              color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                { offset: 0, color: "rgba(61,220,151,.18)" },
                { offset: 1, color: "rgba(61,220,151,.01)" },
              ]),
            },
            markLine: {
              silent: true,
              symbol: "none",
              lineStyle: { color: "#5b6b82", type: "dashed", width: 1 },
              label: { color: "#8fa3bd", fontSize: 9, formatter: "基准 100", position: "insideStartTop" },
              data: [{ yAxis: 100 }],
            },
          },
          {
            name: "VIX",
            type: "line",
            yAxisIndex: 1,
            data: vixData,
            showSymbol: false,
            lineStyle: { color: "rgba(255,92,92,.65)", width: 1.2 },
          },
        ],
      }),
      true
    );
  };

  // ══════════════════ 11 热力图 ══════════════════
  renderers.chart11 = (inst) => {
    const cols = ["状态 A", "状态 B", "状态 C", "状态 D"];
    const rows = [
      { name: "纳指 / 成长", vals: ["重挫", "下跌", "上涨", "领涨"] },
      { name: "标普500", vals: ["下跌", "下跌", "上涨", "上涨"] },
      { name: "罗素2000", vals: ["重挫", "下跌", "领涨", "分化"] },
      { name: "黄金", vals: ["抗跌", "受益", "分化", "受益"] },
      { name: "BTC", vals: ["重挫", "先跌", "上涨", "强势"] },
      { name: "美债 (TLT)", vals: ["承压", "受益", "承压", "受益"] },
      { name: "美元 (DXY)", vals: ["走强", "走强", "走弱", "走弱"] },
      { name: "高收益债", vals: ["承压", "冻结", "受益", "分化"] },
    ];
    const score = { 领涨: 2, 强势: 2, 受益: 1.5, 上涨: 1.2, 走强: 1.2, 抗跌: 0.8, 分化: 0.2, 下跌: -0.6, 承压: -1, 先跌: -1, 冻结: -1.2, 走弱: -1, 重挫: -2 };
    const data = [];
    rows.forEach((r, ri) => r.vals.forEach((v, ci) => data.push({ value: [ci, ri, score[v]], label: v })));
    inst.setOption(
      base({
        grid: { left: 88, right: 18, top: 26, bottom: 26 },
        legend: { show: false },
        tooltip: {
          position: "top",
          backgroundColor: "rgba(13,18,26,.96)",
          borderColor: C.line,
          formatter: (p) => `${rows[p.value[1]].name} × ${cols[p.value[0]]}<br><span style="color:#eaf2ff">${p.data.label}</span>`,
        },
        xAxis: {
          type: "category",
          data: cols,
          splitArea: { show: false },
          axisLine: { lineStyle: { color: C.line } },
          axisTick: { show: false },
          axisLabel: { color: "#8fa3bd", fontSize: 10 },
        },
        yAxis: {
          type: "category",
          data: rows.map((r) => r.name),
          inverse: true,
          splitArea: { show: false },
          axisLine: { show: false },
          axisTick: { show: false },
          axisLabel: { color: "#8fa3bd", fontSize: 10 },
        },
        visualMap: {
          show: false,
          min: -2.2,
          max: 2.2,
          inRange: { color: ["#3a1d24", "#5d3a27", "#3f422b", "#23523f", "#1b6b4b"] },
        },
        series: [
          {
            name: "作者口径",
            type: "heatmap",
            data,
            label: { show: true, color: "#e8f1ff", fontSize: 10 },
            itemStyle: { borderColor: "#0b0e14", borderWidth: 2 },
            emphasis: { itemStyle: { borderColor: C.blue, borderWidth: 1 } },
          },
        ],
      }),
      true
    );
  };

  // ══════════════════ 13 BTC × 准备金 ══════════════════
  renderers.chart13 = (inst) => {
    const s = D.btc_res;
    inst.setOption(
      base({
        grid: { left: 62, right: 58, top: 40, bottom: 34 },
        legend: { data: ["BTC 价格", "银行准备金"], top: 2, right: 6 },
        xAxis: xTime(),
        yAxis: [
          yVal({
            name: "BTC (log)",
            type: "log",
            logBase: 10,
            min: 10000,
            max: 300000,
            axisLabel: { color: "#6f8098", fontSize: 10, formatter: (v) => (v >= 1000 ? v / 1000 + "k" : v) },
          }),
          yVal({ name: "万亿美元", min: 2.3, max: 4.4, splitLine: { show: false }, axisLabel: { color: "#6f8098", fontSize: 10 } }),
        ],
        tooltip: {
          trigger: "axis",
          backgroundColor: "rgba(13,18,26,.96)",
          borderColor: C.line,
          formatter: (p) => `${tipDay(p[0])}<br>` +
            p.map((x) => tipRow(x.color, x.seriesName, x.seriesName === "BTC 价格" ? "$" + comma(tipValue(x)) : fmt(tipValue(x), 2) + "T")).join(""),
        },
        series: [
          {
            name: "BTC 价格",
            type: "line",
            data: s.t.map((t, i) => [t, s.btc[i]]),
            showSymbol: false,
            lineStyle: { color: C.orange, width: 1.8 },
            areaStyle: { color: "rgba(247,147,26,.05)" },
            markLine: {
              silent: true,
              symbol: "none",
              lineStyle: { color: "rgba(247,147,26,.25)", type: "dashed" },
              label: { color: "#a9835a", fontSize: 9, rotate: 90, position: "insideEndTop" },
              data: [
                { xAxis: "2021-11-10", name: "2021-11 BTC 顶", label: { formatter: "2021-11 BTC 顶" } },
                { xAxis: "2023-03-15", name: "2023-03 准备金底", label: { formatter: "2023-03 准备金底" } },
                { xAxis: "2024-03-20", name: "2024-03 阶段顶", label: { formatter: "2024-03 阶段顶" } },
                { xAxis: "2025-08-01", name: "2025 TGA 抽水", label: { formatter: "2025 TGA 抽水" } },
              ],
            },
          },
          {
            name: "银行准备金",
            type: "line",
            yAxisIndex: 1,
            data: s.t.map((t, i) => [t, s.reserve[i]]),
            showSymbol: false,
            lineStyle: { color: C.blue, width: 1.8 },
          },
        ],
      }),
      true
    );
  };

  // ══════════════════ 14 滚动相关 ══════════════════
  renderers.chart14 = (inst) => {
    const c = D.corr;
    inst.setOption(
      base({
        grid: { left: 52, right: 22, top: 40, bottom: 34 },
        legend: { data: ["13 周", "26 周"], top: 2, right: 6 },
        xAxis: xTime(),
        yAxis: yVal({ name: "相关系数", min: -1, max: 1 }),
        tooltip: {
          trigger: "axis",
          backgroundColor: "rgba(13,18,26,.96)",
          borderColor: C.line,
          formatter: (p) => `${tipDay(p[0])}<br>` +
            p.filter((x) => tipValue(x) !== null).map((x) => tipRow(x.color, x.seriesName, fmt(tipValue(x), 3))).join(""),
        },
        series: [
          {
            name: "13 周",
            type: "line",
            data: c.t.map((t, i) => [t, c.c13[i]]),
            showSymbol: false,
            connectNulls: false,
            lineStyle: { color: C.blue, width: 1.8 },
          },
          {
            name: "26 周",
            type: "line",
            data: c.t.map((t, i) => [t, c.c26[i]]),
            showSymbol: false,
            connectNulls: false,
            lineStyle: { color: C.orange, width: 1.6 },
            markLine: {
              silent: true,
              symbol: "none",
              lineStyle: { color: "rgba(107,122,144,.35)", type: "dashed" },
              label: { show: false },
              data: [{ yAxis: 0 }, { yAxis: 0.6 }, { yAxis: -0.6 }],
            },
          },
        ],
      }),
      true
    );
  };

  // ══════════════════ 17 2Y / EFFR / BTC ══════════════════
  function seriesMap(s) {
    const m = new Map();
    for (let i = 0; i < s.t.length; i++) m.set(s.t[i], s.v[i]);
    return m;
  }
  function pairCorr(xs, ys) {
    const n = Math.min(xs.length, ys.length);
    if (n < 3) return null;
    let mx = 0, my = 0;
    for (let i = 0; i < n; i++) { mx += xs[i]; my += ys[i]; }
    mx /= n; my /= n;
    let num = 0, dx = 0, dy = 0;
    for (let i = 0; i < n; i++) {
      const a = xs[i] - mx, b = ys[i] - my;
      num += a * b; dx += a * a; dy += b * b;
    }
    return dx && dy ? num / Math.sqrt(dx * dy) : null;
  }
  function nextDay(day) {
    const d = new Date(day + "T00:00:00");
    d.setDate(d.getDate() + 1);
    return tipDate(d.getTime());
  }

  renderers.chart17 = (inst) => {
    const dgs2 = D.series.DGS2, effr = D.series.EFFR, btc = D.series.BTC;
    const eMap = seriesMap(effr);
    const rows = [];
    for (let i = 0; i < dgs2.t.length; i++) {
      const t = dgs2.t[i], a = dgs2.v[i], b = eMap.get(t);
      if (a == null || b == null) continue;
      rows.push({ t: t, a: a, b: b });
    }
    // 政策动作：EFFR 单日变动 ≥ 20bp（加息红上三角 / 降息绿下三角）
    const moves = [];
    for (let i = 1; i < rows.length; i++) {
      const db = (rows[i].b - rows[i - 1].b) * 100;
      if (Math.abs(db) >= 20) moves.push({ t: rows[i].t, bp: Math.round(db), rate: rows[i].b });
    }
    // 领先-滞后：5 日滚动累计变动，DGS2 领先 EFFR 的交易日数与最大相关
    const dx = [], dy = [];
    for (let i = 1; i < rows.length; i++) { dx.push(rows[i].a - rows[i - 1].a); dy.push(rows[i].b - rows[i - 1].b); }
    const sum5 = (v) => v.map((_, i) => (i + 5 <= v.length ? v[i] + v[i + 1] + v[i + 2] + v[i + 3] + v[i + 4] : null)).filter((x) => x !== null);
    const rx = sum5(dx), ry = sum5(dy);
    let bestLag = 0, bestCorr = 0;
    for (let lag = 0; lag <= 60; lag++) {
      if (rx.length - lag < 30) break;
      const c = pairCorr(rx.slice(0, rx.length - lag), ry.slice(lag));
      if (c !== null && c > bestCorr) { bestCorr = c; bestLag = lag; }
    }
    // 下次议息概率（启发式：30 日动量 + 距上次动作的缺口，与脚本口径一致）
    const sMom = (rows[rows.length - 1].a - rows[rows.length - 31].a) * 100;
    let sGap = sMom;
    if (moves.length) {
      const last = moves[moves.length - 1];
      const idx = rows.findIndex((r) => r.t === last.t);
      if (idx >= 0 && idx < rows.length - 2) sGap = (rows[rows.length - 1].a - rows[idx].a) * 100 - last.bp;
    }
    const signal = 0.5 * sMom + 0.5 * sGap;
    const up = Math.max(0, signal) / 25, down = Math.max(0, -signal) / 25;
    const lg = [1, up - 0.8 * down, down - 0.8 * up];
    const m = Math.max.apply(null, lg);
    const ex = lg.map((x) => Math.exp(x - m));
    const sm = ex.reduce((a, b) => a + b, 0);
    const pNone = ex[0] / sm, pHike = ex[1] / sm, pCut = ex[2] / sm;
    // BTC 日收益 vs DGS2 日变动
    const bMap = seriesMap(btc);
    const bret = [], dchg = [];
    for (let i = 1; i < rows.length; i++) {
      if (bMap.has(rows[i].t) && bMap.has(rows[i - 1].t)) {
        bret.push(bMap.get(rows[i].t) / bMap.get(rows[i - 1].t) - 1);
        dchg.push(rows[i].a - rows[i - 1].a);
      }
    }
    const corrAll = pairCorr(bret, dchg), corr30 = pairCorr(bret.slice(-30), dchg.slice(-30));

    const st = $("rate2yStats");
    if (st) {
      st.innerHTML =
        `领先-滞后：<b style="color:#eaf2ff">DGS2 领先 EFFR 约 ${bestLag} 个交易日（≈${Math.round(bestLag * 365 / 252)} 自然日）</b>` +
        `，最大相关 ${fmt(bestCorr, 2)} · BTC 日收益 vs DGS2 日变动：近30日 ${sign(corr30)} / 全样本 ${sign(corrAll)}` +
        `<br>下次议息（启发式）：不变 <b>${Math.round(pNone * 100)}%</b> / 加息 ${Math.round(pHike * 100)}% / 降息 ${Math.round(pCut * 100)}%` +
        ` · 覆盖：DGS2／EFFR 自 ${rows[0].t}；BTC 自 ${btc.t[0]}（2021 年前无 BTC，不插值补齐）`;
    }

    inst.setOption(
      base({
        grid: [
          { left: 62, right: 66, top: 40, height: 210 },
          { left: 62, right: 66, top: 292, height: 56 },
        ],
        legend: { data: ["2Y DGS2", "EFFR", "BTC", "DGS2−EFFR"], top: 2, right: 6 },
        axisPointer: { link: [{ xAxisIndex: "all" }] },
        xAxis: [xTime({ gridIndex: 0 }), xTime({ gridIndex: 1 })],
        yAxis: [
          yVal({ gridIndex: 0, name: "%" }),
          yVal({
            gridIndex: 0,
            type: "log",
            logBase: 10,
            position: "right",
            name: "BTC",
            splitLine: { show: false },
            axisLabel: { color: "#6f8098", fontSize: 10, formatter: (v) => (v >= 1000 ? (v / 1000).toFixed(0) + "k" : v) },
          }),
          yVal({ gridIndex: 1, name: "利差 %", splitNumber: 3 }),
        ],
        tooltip: {
          trigger: "axis",
          backgroundColor: "rgba(13,18,26,.96)",
          borderColor: C.line,
          formatter: (p) => {
            const rowsHtml = p.filter((x) => tipValue(x) !== null).map((x) => {
              let v;
              if (x.seriesName === "BTC") v = "$" + comma(tipValue(x));
              else if (x.seriesName === "DGS2−EFFR") v = sign(tipValue(x), 2) + "%（" + sign(tipValue(x) * 100, 0) + "bp）";
              else v = fmt(tipValue(x), 2) + "%";
              return tipRow(x.color, x.seriesName, v);
            }).join("");
            return tipDay(p[0]) + "<br>" + rowsHtml;
          },
        },
        series: [
          {
            name: "2Y DGS2",
            type: "line",
            data: rows.map((r) => [r.t, r.a]),
            showSymbol: false,
            lineStyle: { color: "#dbe6f5", width: 1.8 },
            z: 4,
            markLine: {
              silent: true,
              symbol: "none",
              label: { show: false },
              data: moves.map((x) => ({
                xAxis: x.t,
                lineStyle: { color: x.bp > 0 ? "rgba(255,92,92,.30)" : "rgba(61,220,151,.30)", width: 1, type: "dashed" },
              })),
            },
          },
          {
            name: "EFFR",
            type: "line",
            data: rows.map((r) => [r.t, r.b]),
            showSymbol: false,
            lineStyle: { color: C.blue, width: 1.8 },
            z: 5,
            markPoint: {
              symbol: "triangle",
              symbolSize: 7,
              label: { show: false },
              data: moves.map((x) => ({
                coord: [x.t, x.rate],
                symbolRotate: x.bp > 0 ? 0 : 180,
                itemStyle: { color: x.bp > 0 ? C.red : C.green },
              })),
            },
          },
          {
            name: "BTC",
            type: "line",
            yAxisIndex: 1,
            data: btc.t.map((t, i) => [t, btc.v[i]]),
            showSymbol: false,
            lineStyle: { color: C.orange, width: 1.3 },
            z: 3,
          },
          {
            name: "DGS2−EFFR",
            type: "line",
            xAxisIndex: 1,
            yAxisIndex: 2,
            data: rows.map((r) => [r.t, Number((r.a - r.b).toFixed(3))]),
            showSymbol: false,
            lineStyle: { color: C.purple, width: 1.2 },
            areaStyle: { color: "rgba(167,139,250,.20)" },
            markLine: { silent: true, symbol: "none", label: { show: false }, lineStyle: { color: "#31435c", width: 1 }, data: [{ yAxis: 0 }] },
          },
        ],
      }),
      true
    );
  };

  // ══════════════════ 18 BTC × ETF 净流入 ══════════════════
  renderers.chart18 = (inst) => {
    const etf = D.series.ETF_FLOW, btc = D.series.BTC;
    // 导出 payload 与前端消费字段一致性校验：缺 BTC / ETF_FLOW 或数组长度不匹配时直接暴露，不画空图
    const bad18 = !btc || !etf || !Array.isArray(btc.t) || !Array.isArray(etf.t) ||
      btc.t.length !== btc.v.length || etf.t.length !== etf.v.length ||
      !btc.t.length || !etf.v.some((x) => x != null);
    if (bad18) {
      console.warn("[chart18] payload 校验失败", { btc: btc && btc.t && btc.t.length, etf: etf && etf.t && etf.t.length });
      inst.setOption(base({ series: [] }));
      return;
    }
    const bMap = seriesMap(btc);
    // ETF 仅在其自身覆盖区间内取点；BTC 用完整历史，二者不互相截断、不补零
    const etfRows = [];
    for (let i = 0; i < etf.t.length; i++) {
      const t = etf.t[i], f = etf.v[i];
      if (f == null) continue;
      etfRows.push({ t: t, f: f });
    }
    let acc = 0;
    const cum = etfRows.map((r) => (acc += r.f));
    const flowMap = new Map(etfRows.map((r) => [r.t, r.f]));
    const cumMap = new Map(etfRows.map((r, i) => [r.t, cum[i]]));
    const etfStart = etfRows[0].t, btcStart = btc.t[0];
    // 极端日：净流入 > +$0.9B（黄） / 净流出 < -$0.6B（红）。用 custom 系列在三个面板
    // 画出单交易日宽的竖向背景色带（z=1，低于线/柱的 z=2），只覆盖当日、不侵入相邻交易日
    const EXTREME_MS = 43200000;
    const extremeData = etfRows
      .filter((r) => r.f > 900e6 || r.f < -600e6)
      .map((r) => ({ value: [new Date(r.t + "T00:00:00").getTime(), r.f > 900e6 ? 1 : -1] }));
    const bandSeries = (gi, yRef) => ({
      name: "极端日高亮",
      type: "custom",
      xAxisIndex: gi,
      yAxisIndex: gi,
      z: 1,
      silent: true,
      tooltip: { show: false },
      data: extremeData,
      renderItem: (params, api) => {
        const ts = api.value(0), dir = api.value(1);
        // 只用 x 像素定位；grid 0 是对数轴，y 参考值必须为正（0 在对数轴无定义）
        const a = api.coord([ts - EXTREME_MS, yRef]);
        const b = api.coord([ts + EXTREME_MS, yRef]);
        const cs = params.coordSys;
        // 单交易日宽（±0.5 天，合计 1 天）；像素下限 1px，优先细而不遮挡相邻柱子
        const w = Math.max(1, b[0] - a[0]);
        const x = (a[0] + b[0]) / 2 - w / 2;
        return {
          type: "rect",
          shape: { x: x, y: cs.y, width: w, height: cs.height },
          style: {
            fill: dir > 0 ? "rgba(245,197,66,.30)" : "rgba(255,92,92,.26)",
            stroke: dir > 0 ? "rgba(245,197,66,.90)" : "rgba(255,92,92,.90)",
            lineWidth: 1,
          },
          silent: true,
        };
      },
    });
    // ETF 未覆盖区间明确标注，不画 0 值柱、不插值
    const noDataFill = { color: "rgba(123,140,164,.08)", borderColor: "rgba(123,140,164,.22)", borderWidth: 1 };
    const noDataLabel = { show: true, position: "insideTop", color: "#8fa3bd", fontSize: 10, formatter: "无 ETF 净流入数据" };
    const noDataArea = [[{ xAxis: btcStart, itemStyle: noDataFill, label: noDataLabel }, { xAxis: etfStart }]];
    const noDataAreaPlain = [[{ xAxis: btcStart, itemStyle: noDataFill }, { xAxis: etfStart }]];
    const flow = etfRows.map((r) => r.f);
    const inD = flow.filter((v) => v > 0), outD = flow.filter((v) => v < 0);
    const avgIn = inD.reduce((a, b) => a + b, 0) / Math.max(1, inD.length);
    const avgOut = outD.reduce((a, b) => a + b, 0) / Math.max(1, outD.length);
    const maxRun = (sign) => {
      let best = 0, cur = 0;
      for (const v of flow) { cur = (sign > 0 ? v > 0 : v < 0) ? cur + 1 : 0; if (cur > best) best = cur; }
      return best;
    };
    const bret = [], fl = [];
    for (let i = 1; i < etfRows.length; i++) {
      if (!bMap.has(etfRows[i].t) || !bMap.has(etfRows[i - 1].t)) continue;
      bret.push(bMap.get(etfRows[i].t) / bMap.get(etfRows[i - 1].t) - 1);
      fl.push(etfRows[i].f);
    }
    const corr30 = pairCorr(bret.slice(-30), fl.slice(-30));

    const st = $("etfFlowStats");
    if (st) {
      st.innerHTML =
        `累计净流入 <b style="color:#eaf2ff">${cum[cum.length - 1] >= 0 ? "+" : ""}${fmt(cum[cum.length - 1] / 1e9, 1)}B</b>` +
        ` · 流入日 ${inD.length} 天（日均 +${fmt(avgIn / 1e9, 2)}B）／流出日 ${outD.length} 天（日均 ${fmt(avgOut / 1e9, 2)}B）` +
        ` · 最长连续流入 ${maxRun(1)} 日／连续流出 ${maxRun(-1)} 日` +
        ` · BTC 日收益 vs 当日净流入相关：近30日 ${sign(corr30)}` +
        `<br>BTC 覆盖 ${btcStart} → ${btc.t[btc.t.length - 1]}；ETF 净流入覆盖 ${etfStart} → ${etfRows[etfRows.length - 1].t}，此前区间标为「无 ETF 净流入数据」，不补零、不插值`;
    }

    inst.setOption(
      base({
        grid: [
          { left: 64, right: 64, top: 40, height: 200 },
          { left: 64, right: 64, top: 288, height: 78 },
          { left: 64, right: 64, top: 400, height: 56 },
        ],
        legend: { data: ["BTC 价格（对数）", "ETF 净流入", "累计净流入"], top: 2, right: 6 },
        axisPointer: { link: [{ xAxisIndex: "all" }] },
        xAxis: [
          xTime({ gridIndex: 0, min: btcStart, axisLabel: { show: false } }),
          xTime({ gridIndex: 1, min: btcStart, axisLabel: { show: false } }),
          xTime({ gridIndex: 2, min: btcStart }),
        ],
        yAxis: [
          yVal({
            gridIndex: 0,
            type: "log",
            logBase: 10,
            name: "BTC",
            axisLabel: { color: "#6f8098", fontSize: 10, formatter: (v) => (v >= 1000 ? (v / 1000).toFixed(0) + "k" : v) },
          }),
          yVal({ gridIndex: 1, name: "$B", splitNumber: 3, axisLabel: { color: "#6f8098", fontSize: 10, formatter: (v) => (v / 1e9).toFixed(0) } }),
          yVal({ gridIndex: 2, name: "累计 $B", splitNumber: 3, axisLabel: { color: "#6f8098", fontSize: 10, formatter: (v) => (v / 1e9).toFixed(0) } }),
        ],
        tooltip: {
          trigger: "axis",
          backgroundColor: "rgba(13,18,26,.96)",
          borderColor: C.line,
          formatter: (p) => {
            const day = tipDay(p[0]);
            const b = bMap.get(day), f = flowMap.get(day), c = cumMap.get(day);
            return day + "<br>" +
              tipRow(C.orange, "BTC", b == null ? "无数据" : "$" + comma(b)) +
              tipRow(f == null ? C.grey : f >= 0 ? C.green : C.red, "ETF 净流入", f == null ? "无数据" : (f >= 0 ? "+" : "") + fmt(f / 1e9, 3) + "B") +
              tipRow(C.purple, "累计净流入", c == null ? "无数据" : (c >= 0 ? "+" : "") + fmt(c / 1e9, 1) + "B");
          },
        },
        series: [
          {
            name: "BTC 价格（对数）",
            type: "line",
            data: btc.t.map((t, i) => [t, btc.v[i]]),
            showSymbol: false,
            lineStyle: { color: C.orange, width: 1.8 },
            markArea: { silent: true, data: noDataAreaPlain },
          },
          {
            name: "ETF 净流入",
            type: "bar",
            xAxisIndex: 1,
            yAxisIndex: 1,
            data: etfRows.map((r) => [r.t, r.f]),
            barMaxWidth: 3,
            itemStyle: { color: (p) => (tipValue(p) >= 0 ? "rgb(34,197,94)" : "rgb(239,68,68)") },
            markArea: { silent: true, data: noDataArea },
            markLine: { silent: true, symbol: "none", label: { show: false }, lineStyle: { color: "#31435c", width: 1 }, data: [{ yAxis: 0 }] },
          },
          {
            name: "累计净流入",
            type: "line",
            xAxisIndex: 2,
            yAxisIndex: 2,
            data: etfRows.map((r, i) => [r.t, cum[i]]),
            showSymbol: false,
            lineStyle: { color: C.purple, width: 1.4 },
            areaStyle: { color: "rgba(167,139,250,.22)" },
            markArea: { silent: true, data: noDataArea },
            markLine: { silent: true, symbol: "none", label: { show: false }, lineStyle: { color: "#31435c", width: 1 }, data: [{ yAxis: 0 }] },
          },
          // 极端日背景色带（custom，z=1 位于数据图形 z=2 之下）：三个面板对齐
          bandSeries(0, 1),
          bandSeries(1, 0),
          bandSeries(2, 0),
        ],
      }),
      true
    );
  };


  function meterHtml(label, value, unit, pct, tone, sub) {
    return `<div class="meter">
      <div class="meter-top"><span>${label}</span><b>${value}${unit ? `<span style="color:#7b8ca4;font-size:9px"> ${unit}</span>` : ""}</b></div>
      <div class="bar"><i class="${tone}" style="width:${Math.max(0, Math.min(100, pct))}%"></i></div>
      <small class="${tone === "missing" ? "miss" : ""}">${sub}</small>
    </div>`;
  }
  function buildFramework() {
    const box = $("framework");
    if (!box) return;
    const f = D.framework;
    const liq = f.liquidity;
    const tr = f.transmission;
    const cr = f.crypto;
    const zoneTone = D.status.reserve_tone;
    const resPct = ((liq.reserve_t - 2.4) / (4.4 - 2.4)) * 100;

    box.innerHTML = `
      <div class="layer l1">
        <h4><i></i>流动性层</h4>
        ${meterHtml("准备金水位", fmt(liq.reserve_t, 2), "T", resPct, zoneTone, `${D.status.reserve_zone} · 4周 ${sign(liq.reserve_4w_b, 1)}B · 阈值 3.0 / 3.2T`)}
        ${meterHtml("SOFR − EFFR", sign(liq.spread_bp, 1), "bp", Math.abs(liq.spread_bp) / 30 * 100, D.status.corridor_tone, `正常 &lt;12bp · 极端 &gt;17bp`)}
        ${meterHtml("ON RRP 利率 / EFFR", fmt(liq.onrrp_rate, 2), "%", (liq.onrrp_rate / 6) * 100, "muted", `EFFR ${fmt(liq.effr)}% · IORB ${fmt(liq.iorb)}%`)}
        ${meterHtml("SRF 用量", fmt(liq.srf_usage_b, 2), "B", Math.min(100, (liq.srf_usage_b / 75) * 100), liq.srf_usage_b > 10 ? "warn" : "muted", `30日峰值 ${fmt(liq.srf_max_30d_b, 2)}B · 截止 ${D.cutoffs.SRF}`)}
      </div>
      <div class="layer l2">
        <h4><i></i>传导层</h4>
        ${meterHtml("美元指数 DXY", fmt(tr.dxy, 2), "", ((tr.dxy - 90) / 20) * 100, "muted", `20日 ${sign(tr.dxy_chg20)}`)}
        ${meterHtml("实际利率 DFII10", fmt(tr.real_yield, 2), "%", (tr.real_yield / 4) * 100, "muted", `20日 ${sign(tr.real_chg20)} 个百分点`)}
        ${meterHtml("VIX", fmt(tr.vix, 2), "", (tr.vix / 40) * 100, tr.vix > 20 ? "warn" : "muted", `20日 ${sign(tr.vix_chg20)}`)}
      </div>
      <div class="layer l3">
        <h4><i></i>加密层</h4>
        ${meterHtml("稳定币总供给", fmt(cr.stable_total_b, 1), "B", (cr.stable_total_b / 300) * 100, "muted", `USDT + USDC · 30日 ${sign(cr.stable_chg30_b, 1)}B`)}
        ${meterHtml("BTC ETF 7日净流入", sign(cr.etf_7d_b, 2), "B", Math.abs(cr.etf_7d_b) / 5 * 100, cr.etf_7d_b >= 0 ? "ok" : "danger", `最新单日 ${sign(cr.etf_last_b, 2)}B · 累计 ${sign(cr.etf_cum_b, 1)}B`)}
        ${meterHtml("BTC 价格", "$" + comma(cr.btc), "", (cr.btc / 150000) * 100, cr.btc_chg30 >= 0 ? "ok" : "danger", `30日 ${sign(cr.btc_chg30, 0)} USD`)}
      </div>`;
  }

  // ── 初始化 ───────────────────────────────────────────────
  initStatus();
  buildStateTable();
  fillWaterfallSelect();
  buildFramework();
  if ($("wfSelect")) {
    $("wfSelect").onchange = () => {
      if (instances.chart4) renderers.chart4(instances.chart4);
    };
  }

  const initial = location.hash.replace("#", "");
  if (["d1", "d2", "d3"].includes(initial)) activate(initial);
})();
