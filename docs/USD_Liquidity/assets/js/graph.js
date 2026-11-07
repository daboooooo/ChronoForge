/* graph.js — 交互式知识图谱（离线 SVG，分层布局；点击节点/连线查看详情） */
(function () {
  'use strict';
  var D = window.KB_DATA;

  var CAT_COL = {
    '利率': 0, '利率与工具': 0, '工具': 0,
    '美联储负债': 1, '会计框架': 1, '美联储资产': 1,
    '财政机制': 2, '财政': 2, '财政指标': 2, '数据源': 2,
    '市场指标': 3, '利率结构': 3,
    '加密资产': 4,
    '机构与制度': 5, '机构': 5, '政策': 5, '制度机制': 5, '货币市场': 5, '证券': 5, '事件类型': 5, '风险管理': 5, '国债': 5, '国债（特殊）': 5, '宏观指标': 5, '研究规范': 5
  };
  var COL_X = [110, 320, 530, 740, 950, 1160];

  function nodeInfo(id) {
    var c = D.concepts.find(function (x) { return x.id === id; });
    if (c) return { kind: '概念', name: c.name_cn, en: c.name_en, def: c.definition, extra: c.notes ? '备注：' + c.notes : '', cat: c.category, src: c.source_document_ids };
    var v = D.variables.find(function (x) { return x.id === id; });
    if (v) return { kind: '变量', name: v.name_cn, en: v.name_en + (v.abbreviation ? ' (' + v.abbreviation + ')' : ''), def: v.definition, extra: '单位：' + (v.unit || '—') + ' · 序列：' + (v.series_id || '—') + ' · 频率：' + (v.frequency || '—') + (v.is_supplement_series ? ' · 补充序列' : ''), cat: v.category || '', src: v.source_document_ids };
    var f = D.formulas.find(function (x) { return x.id === id; });
    if (f) return { kind: '公式', name: f.name_cn, en: '', def: f.formula, extra: f.scope_and_conditions || '', cat: f.type, src: f.source_document_ids };
    return null;
  }

  function buildNodes() {
    var ids = {};
    D.relationships.forEach(function (r) { ids[r.source] = 1; ids[r.target] = 1; });
    var nodes = [];
    var colCount = {};
    Object.keys(ids).forEach(function (id) {
      var info = nodeInfo(id);
      if (!info) return;
      var col = CAT_COL[info.cat] !== undefined ? CAT_COL[info.cat] : 5;
      if (info.kind === '变量' && /RESERVES|TGA|ON_RRP|FED_ASSETS/.test(id)) col = 1;
      if (info.kind === '变量' && /DXY|UST|TLT|VIX|BREAKEVEN|REAL_YIELD|CURVE|TBILL/.test(id)) col = 3;
      if (info.kind === '变量' && /BTC|STABLECOIN|ETF|GLOBAL_M2/.test(id)) col = 4;
      if (info.kind === '变量' && /TAX|SPENDING|NET_BORROWING|DTS|QT|CURRENCY|TREASURY_HOLD|MBS_HOLD|TGA_TARGET|SOFR|EFFR|SRF|FFR|IORB/.test(id)) col = (id === 'VAR_SOFR' || id === 'VAR_EFFR' || id === 'VAR_SRF_USAGE' || id === 'VAR_FFR_TARGET' || id === 'VAR_IORB') ? 0 : 2;
      colCount[col] = (colCount[col] || 0) + 1;
      nodes.push({ id: id, col: col, info: info, idx: colCount[col] });
    });
    nodes.forEach(function (n) {
      n.x = COL_X[n.col];
      n.y = 60 + n.idx * 88;
    });
    return nodes;
  }

  function render(full) {
    var nodes = buildNodes();
    var pos = {};
    nodes.forEach(function (n) { pos[n.id] = n; });
    var maxRows = Math.max.apply(null, nodes.map(function (n) { return n.idx; }));
    var H = Math.max(560, 60 + maxRows * 88 + 60);
    var W = 1290;
    var colNames = ['利率走廊', '美联储负债', '财政与数据', '市场指标', '加密资产', '机构与国债'];
    var s = '<div class="graph-wrap"><div class="graph-svg"><svg viewBox="0 0 ' + W + ' ' + H + '" xmlns="http://www.w3.org/2000/svg" style="width:100%">';
    s += '<defs><marker id="garrow" markerWidth="8" markerHeight="8" refX="7" refY="2.6" orient="auto"><path d="M0,0 L7,2.6 L0,5.2 Z" fill="#a8b4c4"/></marker></defs>';
    colNames.forEach(function (cn, i) {
      s += '<text x="' + COL_X[i] + '" y="30" text-anchor="middle" font-size="13" fill="#155eab" font-weight="bold">' + cn + '</text>';
      s += '<line x1="' + (COL_X[i]) + '" y1="44" x2="' + COL_X[i] + '" y2="' + (H - 20) + '" stroke="#eef1f5" stroke-width="26"/>';
    });
    var edges = '';
    D.relationships.forEach(function (r, i) {
      var a = pos[r.source], b = pos[r.target];
      if (!a || !b) return;
      var mx = (a.x + b.x) / 2;
      var acct = r.relation_type === 'ACCOUNTING_LINK';
      var hyp = r.verification_status === 'hypothesis';
      var color = acct ? '#7a3aa0' : hyp ? '#c98a2b' : '#9fb2c8';
      var d = 'M' + (a.x + 12) + ',' + a.y + ' C' + mx + ',' + a.y + ' ' + mx + ',' + b.y + ' ' + (b.x - 12) + ',' + b.y;
      edges += '<path class="graph-edge" data-rel="' + i + '" d="' + d + '" fill="none" stroke="' + color + '" stroke-width="' + (acct ? 2.2 : 1.4) + '"' + (hyp ? ' stroke-dasharray="5,4"' : '') + ' marker-end="url(#garrow)" opacity="0.85"/>';
    });
    s += edges;
    nodes.forEach(function (n) {
      var r = n.info.kind === '概念' ? 13 : 11;
      var fill = n.info.kind === '概念' ? '#155eab' : '#0c7a5b';
      var acct = n.id === 'CONCEPT_BANK_RESERVES' || n.id === 'CONCEPT_TGA' || n.id === 'CONCEPT_ONRRP';
      if (acct) fill = '#7a3aa0';
      s += '<g class="graph-node" data-node="' + n.id + '">' +
        '<circle cx="' + n.x + '" cy="' + n.y + '" r="' + r + '" fill="' + fill + '" stroke="#fff" stroke-width="1.6"/>' +
        '<text x="' + n.x + '" y="' + (n.y + r + 13) + '" text-anchor="middle" font-size="10">' +
        n.info.name.replace(/^美国|^联邦/, '').slice(0, 9) + '</text></g>';
    });
    s += '<g font-size="10.5" fill="#5a6572">' +
      '<circle cx="900" cy="' + (H - 46) + '" r="6" fill="#155eab"/><text x="912" y="' + (H - 42) + '">概念</text>' +
      '<circle cx="980" cy="' + (H - 46) + '" r="6" fill="#0c7a5b"/><text x="992" y="' + (H - 42) + '">变量</text>' +
      '<circle cx="1060" cy="' + (H - 46) + '" r="6" fill="#7a3aa0"/><text x="1072" y="' + (H - 42) + '">核心负债三项</text>' +
      '<line x1="1150" y1="' + (H - 46) + '" x2="1186" y2="' + (H - 46) + '" stroke="#7a3aa0" stroke-width="2.2"/><text x="1192" y="' + (H - 42) + '">会计关系</text>' +
      '<line x1="900" y1="' + (H - 24) + '" x2="936" y2="' + (H - 24) + '" stroke="#c98a2b" stroke-width="1.6" stroke-dasharray="5,4"/><text x="942" y="' + (H - 20) + '">假设（虚线）</text>' +
      '<line x1="1060" y1="' + (H - 24) + '" x2="1096" y2="' + (H - 24) + '" stroke="#9fb2c8" stroke-width="1.4"/><text x="1102" y="' + (H - 20) + '">机制/经验</text>' +
      '</g>';
    s += '</svg></div><div class="graph-panel" id="graphPanel"><b>图谱使用说明</b><p style="font-size:12.5px;color:#5a6572">点击节点：查看定义与数据代码。<br>点击连线：查看机制解释、成立条件、时间滞后与证据等级。<br><br>紫色粗实线 = 会计关系（ACCOUNTING_LINK）；<br>橙色虚线 = 待验证假设；灰色 = 机制/经验。<br><br>五条核心链：<br>A 政策利率 · B 财政资金流 · C 流动性×BTC<br>D 四变量状态 · E 期限结构</p></div></div>';
    return s;
  }

  function bind(root) {
    if (!root) return;
    var panel = root.querySelector('#graphPanel');
    root.querySelectorAll('.graph-node').forEach(function (g) {
      g.addEventListener('click', function () {
        var info = nodeInfo(g.getAttribute('data-node'));
        if (info && panel) panel.innerHTML = '<span class="badge type">' + info.kind + '</span> <b>' + info.name + '</b><br><span class="pager-hint">' + info.en + '</span><p>' + info.def + '</p><p class="pager-hint">' + info.extra + '</p><p class="pager-hint">来源：' + (info.src || []).join(', ') + '</p>';
      });
    });
    root.querySelectorAll('.graph-edge').forEach(function (p) {
      p.addEventListener('click', function () {
        var r = D.relationships[+p.getAttribute('data-rel')];
        if (r && panel) panel.innerHTML = '<span class="badge ' + (r.relation_type === 'ACCOUNTING_LINK' ? 'acct' : r.verification_status === 'hypothesis' ? 'hyp' : 'type') + '">' + r.relation_type + '</span> <b>' + r.source + ' → ' + r.target + '</b><p>' + r.mechanism + '</p><p class="pager-hint">条件：' + (r.conditions || '—') + '<br>时滞：' + (r.lag || '未给出（不得编造）') + '<br>例外：' + (r.exceptions || '—') + '<br>证据：' + r.evidence_type + ' · 置信度 ' + r.confidence + '<br>来源：' + r.source_document_ids.join(',') + ' p' + r.source_pages.join(',') + '</p>';
      });
    });
  }

  window.KBGraph = { render: render, bind: bind };
})();
