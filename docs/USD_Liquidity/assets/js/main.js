/* main.js — 路由、渲染、搜索、阅读模式（全部本地数据，无外部请求） */
(function () {
  'use strict';
  var D = window.KB_DATA;
  var app = document.getElementById('app');

  /* ---------- 索引：id -> 出现位置（用于交叉跳转） ---------- */
  var idIndex = {}; // id -> {chapter, sectionId}
  D.chapters.forEach(function (ch) {
    ch.sections.forEach(function (sec) {
      (sec.kp_ids || []).forEach(function (kid) {
        if (!idIndex[kid]) idIndex[kid] = { chapter: ch.id, sectionId: sec.id, sectionTitle: sec.title, chapterTitle: ch.title };
      });
    });
  });
  var docById = {};
  D.documents.forEach(function (d) { docById[d.document_id] = d; });

  /* ---------- 徽标 ---------- */
  function badge(text, cls) { return '<span class="badge ' + (cls || '') + '">' + text + '</span>'; }
  function typeBadge(t) {
    var cls = { ACCOUNTING: 'acct', HYPOTHESIS: 'hyp', CAVEAT: 'cav', EMPIRICAL: '', STRATEGY: 'hyp', DATA: 'type', CASE: '', CONCEPT: 'type', MECHANISM: 'type', AGENT_TASK: 'type' }[t] || '';
    return badge(t, cls);
  }
  function verBadge(v) {
    var map = { accounting_identity: 'badge ok', author_analysis: 'badge', author_claimed: 'badge', source_claimed: 'badge hyp', hypothesis: 'badge hyp', cross_document_synthesis: 'badge', official: 'badge ok' };
    return badge('验证:' + v, map[v] || 'badge');
  }
  function srcBadge(docId, page) {
    var d = docById[docId];
    return '<button class="badge src" data-go="src:' + docId + '" title="查看来源">' + (d ? d.short_title : docId) + ' p' + page + '</button>';
  }

  /* ---------- 知识卡片 ---------- */
  function kpCard(k) {
    var links = (k.related_knowledge_ids || []).filter(function (id) { return idIndex[id]; }).slice(0, 8).map(function (id) {
      return '<a href="#/ch/' + idIndex[id].chapter + '?card=' + id + '" title="' + idIndex[id].sectionTitle + '">' + id + '</a>';
    }).join('');
    var varLinks = (k.related_variables || []).slice(0, 6).map(function (v) {
      return '<a href="#/dict?v=' + encodeURIComponent(v) + '">' + v.replace('VAR_', '') + '</a>';
    }).join('');
    return '<div class="kp-card t-' + k.knowledge_type + '" id="card-' + k.id + '">' +
      '<div class="kp-head"><span class="kp-id">' + k.id + '</span><span class="kp-title">' + k.title + '</span>' +
      typeBadge(k.knowledge_type) + verBadge(k.verification_status) + srcBadge(k.source_document_id, k.source_page) + '</div>' +
      '<div class="kp-desc">' + k.description + '</div>' +
      '<div class="kp-normal"><b>规范化表述：</b>' + k.normalized_statement + '</div>' +
      '<div class="kp-orig"><b>原文摘录（' + docById[k.source_document_id].short_title + ' 第 ' + k.source_page + ' 页 · ' + k.source_section + '）：</b><br>' + k.original_statement + '</div>' +
      '<div class="kp-links">置信度 ' + Math.round(k.confidence * 100) + '% · 证据 ' + (k.evidence_tags || []).join(' ') +
      (links ? '<br>相关：' + links : '') + (varLinks ? '<br>变量：' + varLinks : '') + '</div>' +
      '<button class="kp-toggle" data-toggle-orig>展开原文摘录 ▾</button>' +
      '</div>';
  }

  function renderCards(kpIds) {
    return kpIds.map(function (id) {
      if (id.indexOf('KP_') === 0) {
        var k = D.knowledge_points.find(function (x) { return x.id === id; });
        return k ? kpCard(k) : '';
      }
      return entityCard(id);
    }).join('');
  }

  function entityCard(id) {
    var html = '';
    var where = idIndex[id];
    var jump = where ? '<a href="#/ch/' + where.chapter + '?card=' + id + '" class="pager-hint">→ 正文位置：' + where.chapterTitle + ' / ' + where.sectionTitle + '</a>' : '';
    function head(idc, title, badges) {
      return '<div class="kp-card" id="card-' + idc + '"><div class="kp-head"><span class="kp-id">' + idc + '</span><span class="kp-title">' + title + '</span>' + badges + '</div>';
    }
    var c = D.concepts.find(function (x) { return x.id === id; });
    if (c) return head(id, c.name_cn, badge('CONCEPT', 'type') + verBadge(c.verification_status)) +
      '<div class="kp-normal">' + c.definition + '</div><div class="kp-links">英文：' + c.name_en + ' · ' + (c.notes || '') + ' · 来源：' + c.source_document_ids.join(',') + ' p' + (c.source_pages || []).join(',') + '</div>' + jump + '</div>';
    var f = D.formulas.find(function (x) { return x.id === id; });
    if (f) return head(id, f.name_cn, badge(f.type, 'acct') + verBadge(f.verification_status)) +
      '<div class="kp-normal" style="font-family:var(--mono);background:#f4f6fa;padding:8px 12px;border-radius:8px;border:1px solid var(--line)">' + f.formula + '</div>' +
      '<div class="kp-desc">变量说明：' + f.variables_explained + '</div>' +
      '<div class="kp-desc"><b>成立范围与条件：</b>' + f.scope_and_conditions + (f.omitted_items && f.omitted_items !== '—' ? '<br><b>省略项：</b>' + f.omitted_items : '') + '</div>' +
      '<div class="kp-links">来源：' + f.source_document_ids.join(',') + ' p' + f.source_pages.join(',') + ' · ' + (f.interpretation || '') + '</div>' + jump + '</div>';
    var cs = D.cases.find(function (x) { return x.id === id; });
    if (cs) return head(id, cs.title, badge('CASE') + badge('验证:' + cs.verification_status, 'hyp')) +
      '<div class="kp-desc"><b>期间：</b>' + cs.start_date + ' ~ ' + cs.end_date + ' · <b>宏观状态：</b>' + cs.macro_regime + '</div>' +
      '<div class="kp-normal">' + cs.background + '</div>' +
      '<div class="kp-desc"><b>关键事件：</b>' + (cs.key_events || []).join('；') + '</div>' +
      '<div class="kp-desc"><b>流动性变化：</b>' + (cs.liquidity_changes || '—') + ' · <b>BTC：</b>' + (cs.btc_changes || '—') + ' · <b>其他资产：</b>' + (cs.other_asset_changes || '—') + '</div>' +
      '<div class="kp-desc"><b>机制假设：</b>' + (cs.proposed_mechanism || '—') + '</div>' +
      '<div class="kp-desc"><b>支持证据：</b>' + (cs.supporting_evidence || '—') + '<br><b>反证/缺口：</b>' + (cs.counter_evidence || '—') + '<br><b>替代解释：</b>' + (cs.alternative_explanations || '—') + '</div>' +
      '<div class="kp-links">来源：' + cs.source_document_ids.join(',') + ' p' + cs.source_pages.join(',') + ' · 状态：' + cs.verification_status + (cs.external_sources && cs.external_sources.length ? '<br>外部链接：' + cs.external_sources.map(function (u) { return '<a href="' + u + '" target="_blank" rel="noopener">' + u.replace('https://x.com/qinbafrank/status/', 'tweet/') + '</a>'; }).join(' ') : '') + '</div>' + jump + '</div>';
    var r = D.regimes.find(function (x) { return x.id === id; });
    if (r) return head(id, r.name_cn, badge('REGIME', 'type') + badge(r.combo, 'type')) +
      '<div class="kp-normal">' + r.definition + '</div>' +
      '<div class="kp-desc"><b>指标与方向：</b>' + r.expected_directions + '</div>' +
      '<div class="kp-desc"><b>驱动：</b>' + r.drivers.join('；') + '</div>' +
      '<div class="kp-desc"><b>资产影响：</b>' + r.asset_impacts + '</div>' +
      '<div class="kp-desc"><b>与其他状态的区别：</b>' + r.distinguish_from + '<br><b>易误判：</b>' + r.misjudgment_risks.join('；') + '</div>' +
      '<div class="kp-desc"><b>形成/持续/结束：</b>' + r.formation_persistence_exit + '</div>' +
      '<div class="kp-links">来源：' + r.source_document_ids.join(',') + ' p' + r.source_pages.join(',') + ' · 案例：' + (r.historical_cases || []).join(', ') + '</div>' + jump + '</div>';
    var st = D.strategies.find(function (x) { return x.strategy_id === id; });
    if (st) return head(id, st.name, badge('STRATEGY', 'hyp') + badge(st.validation_status, 'cav')) +
      '<div class="kp-desc"><b>目标：</b>' + st.objective + ' · <b>资产：</b>' + st.applicable_assets.join(', ') + ' · <b>期限：</b>' + st.expected_horizon + '</div>' +
      '<div class="kp-desc"><b>所需指标：</b>' + st.required_indicators.join(', ') + '</div>' +
      '<div class="kp-desc"><b>信号定义：</b>' + st.signal_definition + '</div>' +
      '<div class="kp-desc"><b>入场：</b>' + st.entry_conditions + ' · <b>退出：</b>' + st.exit_conditions + '</div>' +
      '<div class="kp-desc"><b>风控：</b>' + st.risk_controls + '<br><b>失效条件：</b>' + (st.failure_conditions || []).join('；') + '<br><b>证伪标准：</b>' + st.falsification_criteria + '</div>' +
      '<div class="kp-desc"><b>历史例证：</b>' + (st.historical_examples || []).join('；') + '<br><b>反例：</b>' + (st.counter_examples || '—') + '</div>' +
      '<div class="kp-desc"><b>回测要求：</b>' + st.backtest_requirements + ' · <b>已知局限：</b>' + st.known_limitations + '</div>' +
      '<div class="kp-links">仓位管理：' + st.position_sizing + ' · 来源：' + st.source_document_ids.join(',') + ' p' + st.source_pages.join(',') + '</div>' + jump + '</div>';
    var ev = D.evidence.evidence_records.find(function (x) { return x.id === id; });
    if (ev) return head(id, ev.claim.slice(0, 40) + '…', badge('EVIDENCE', 'type')) +
      '<div class="kp-normal">' + ev.claim + '</div><div class="kp-links">' + ev.tags.join(' ') + ' · 依据：' + ev.basis + ' · 强度：' + ev.strength + '</div>' + jump + '</div>';
    var rl = D.relationships.find(function (x) { return x.id === id; });
    if (rl) return head(id, rl.relation_type + '：' + rl.source + ' → ' + rl.target, badge(rl.relation_type, rl.relation_type === 'ACCOUNTING_LINK' ? 'acct' : rl.verification_status === 'hypothesis' ? 'hyp' : 'type') + verBadge(rl.verification_status)) +
      '<div class="kp-normal">' + rl.mechanism + '</div>' +
      '<div class="kp-desc"><b>成立条件：</b>' + (rl.conditions || '—') + ' · <b>时间滞后：</b>' + (rl.lag || '未给出（原文无依据，不编造）') + ' · <b>例外：</b>' + (rl.exceptions || '—') + '</div>' +
      '<div class="kp-desc"><b>证据类型：</b>' + rl.evidence_type + ' · 置信度 ' + Math.round(rl.confidence * 100) + '%' + ((rl.related_cases || []).length ? ' · 相关案例：' + rl.related_cases.join(', ') : '') + ((rl.related_strategies || []).length ? ' · 相关策略：' + rl.related_strategies.join(', ') : '') + '</div>' +
      '<div class="kp-links">来源：' + rl.source_document_ids.join(',') + ' p' + rl.source_pages.join(',') + '</div>' + jump + '</div>';
    return '';
  }

  /* ---------- 章节渲染 ---------- */
  function renderChapter(chId, highlightCard) {
    var ch = D.chapters.find(function (c) { return c.id === chId; });
    if (!ch) return '<p>章节不存在</p>';
    var h = '<span class="layer-tag">' + ch.layer + '</span><h1>' + ch.title + '</h1><p class="goal">' + ch.goal + '</p>';
    if (chId === 'L1') h += '<div class="callout"><b>阅读模式</b>：' + D.chapters[0] ? '' : '';
    h += '<p class="pager-hint">' + (D.chapters[0] && chId === 'L1' ? '入门模式只读每节首段；进阶模式通读全文与知识卡片；研究模式展开原文摘录、证据标签与验证状态。' : '') + '</p>';
    ch.sections.forEach(function (sec) {
      h += '<h2 id="sec-' + sec.id.toLowerCase() + '">' + sec.title + '</h2>';
      h += sec.html || '';
      if (sec.diagram) {
        var fn = window.KBDiagrams[sec.diagram];
        if (fn) {
          h += sec.diagram === 'dia_regime_matrix' ? fn(D.regimes) : sec.diagram === 'dia_case_timeline' ? fn(D.cases) : sec.diagram === 'dia_knowledge_graph' ? fn(true) : fn();
        }
        if (sec.diagram_explain) h += '<div class="diagram" style="border-style:dashed"><div class="dia-explain">' + sec.diagram_explain + '</div></div>';
      }
      if (sec.kp_ids && sec.kp_ids.length) h += '<h3>本节知识点（' + sec.kp_ids.length + '）</h3>' + renderCards(sec.kp_ids);
    });
    h += '<div class="footer-note">本章所有论断的证据等级见各卡片徽标；作者观点 ≠ 已验证事实。数据文件：data/ 目录对应 JSON。</div>';
    return h;
  }

  /* ---------- 首页 ---------- */
  function renderHome() {
    var h = '<div class="home-hero"><h1>美元流动性研究知识库</h1>' +
      '<p>Dollar Liquidity Research Knowledge Base · 由五份 PDF 全文整理构建（121 个知识点 · 36 条关系 · 11 条公式 · 8 个案例 · 8 个策略）</p>' +
      '<p>数据底座：data/*.json ｜ 界面：本页（离线）｜ 构建：' + D.generated_at + '</p>' +
      '<div class="hero-stats">' +
      '<div class="hero-stat"><b>5</b>来源 PDF</div><div class="hero-stat"><b>121</b>知识点</div>' +
      '<div class="hero-stat"><b>6</b>递进层级</div><div class="hero-stat"><b>15</b>图表</div>' +
      '<div class="hero-stat"><b>90</b>数据序列条目</div><div class="hero-stat"><b>10</b>待核查项</div></div></div>';
    h += '<h2>推荐学习路径</h2><div class="card-grid">' +
      ['L1|第一层 · 宏观金融基础|概念先行：走廊、国债四家族、四变量', 'L2|第二层 · 资产负债表与资金流|TGA 抽水/放水与 ON RRP 缓冲', 'L3|第三层 · 流动性分析体系|可操作监测与预测框架', 'L4|第四层 · 宏观状态识别|四变量组合→状态分类', 'L5|第五层 · 历史案例|2021–2026 对应关系与反例', 'L6|第六层 · 策略与工作流|策略 A–E 与回测规范'].map(function (x) {
        var p = x.split('|');
        return '<a class="mini-card" href="#/ch/' + p[0] + '"><b>' + p[1] + '</b><span>' + p[2] + '</span></a>';
      }).join('') + '</div>';
    h += '<h2>核心变量导航</h2><p>' + ['VAR_BANK_RESERVES|银行准备金', 'VAR_TGA|TGA', 'VAR_ON_RRP|ON RRP', 'VAR_FED_ASSETS|美联储总资产', 'VAR_EFFR|EFFR', 'VAR_SOFR|SOFR', 'VAR_DXY|美元指数', 'VAR_BTC_PRICE|比特币'].map(function (x) {
      var p = x.split('|'); return '<a href="#/dict?v=' + p[0] + '"><code>' + p[1] + '</code></a>';
    }).join(' · ') + '</p>';
    h += '<h2>知识体系总览</h2><p>六层结构 + 五条关系链（政策利率 / 财政资金流 / 流动性×BTC / 四变量状态 / 期限结构），详见 <a href="#/graph">知识图谱</a>。理论、事实、经验与假设严格分层：会计关系（紫）＞机制（蓝）＞经验（绿）＞假设（橙），见 <a href="#/ch/L3">第三层 3.1</a>。</p>';
    h += '<h2>来源 PDF</h2><div class="card-grid">' + D.documents.map(function (d) {
      return '<a class="mini-card" href="#/sources?doc=' + d.document_id + '"><b>' + d.short_title + '</b><span>' + d.pages + ' 页 · ' + d.role + '</span></a>';
    }).join('') + '</div>';
    h += '<h2>AI Agent 使用说明</h2><p>结构化数据位于 data/*.json（concepts / variables / relationships / formulas / cases / regimes / strategies / patterns / knowledge_points / coverage_matrix / open_questions / agent_templates）。可直接复用的研究任务模板见 <a href="#/templates">Agent 任务模板</a>。权限边界：知识库仅提供研究依据，不自动授予交易执行权限。</p>';
    return h;
  }

  /* ---------- 数据字典 ---------- */
  function renderDict(param) {
    var h = '<span class="layer-tag">工具</span><h1>指标数据字典</h1><p class="goal">' + D.variables.length + ' 个变量：每个指标解释它究竟测量什么，而非只给代码。标注『补充序列』者为原始 PDF 未提供、知识库为研究补充（使用时须声明）。</p>';
    h += '<div class="tablewrap"><table class="kb"><tr><th>变量</th><th>英文/缩写</th><th>定义</th><th>单位/频率</th><th>来源与序列</th><th>预期关系</th><th>局限</th></tr>';
    D.variables.forEach(function (v) {
      h += '<tr id="dict-' + v.id + '"><td><b>' + v.name_cn + '</b><br><span class="kp-id">' + v.id + '</span></td>' +
        '<td>' + v.name_en + (v.abbreviation ? '<br>(' + v.abbreviation + ')' : '') + '</td>' +
        '<td>' + v.definition + '</td><td>' + (v.unit || '—') + ' / ' + (v.frequency || '—') + '</td>' +
        '<td>' + v.source + '<br><code>' + (v.series_id || '—') + '</code>' + (v.is_supplement_series ? ' ' + badge('补充序列', 'hyp') : '') + '<br><span class="pager-hint">发布:' + (v.release_schedule || '—') + ' · 滞后:' + (v.publication_lag || '—') + ' · 修订:' + (v.revision_behavior || '—') + '</span></td>' +
        '<td>' + (v.expected_relationships || []).join('<br>') + '</td><td>' + (v.limitations || '—') + '<br><span class="pager-hint">出处 ' + v.source_document_ids.join(',') + ' p' + (v.source_pages || []).join(',') + '</span></td></tr>';
    });
    h += '</table></div>';
    h += '<h2>概念字典（' + D.concepts.length + '）</h2><div class="tablewrap"><table class="kb"><tr><th>概念</th><th>英文</th><th>定义</th><th>类别</th><th>备注</th></tr>';
    D.concepts.forEach(function (c) {
      h += '<tr id="concept-' + c.id + '"><td><b>' + c.name_cn + '</b><br><span class="kp-id">' + c.id + '</span></td><td>' + c.name_en + '</td><td>' + c.definition + '</td><td>' + c.category + '</td><td>' + (c.notes || '—') + '<br><span class="pager-hint">' + c.source_document_ids.join(',') + ' p' + (c.source_pages || []).join(',') + '</span></td></tr>';
    });
    h += '</table></div>';
    h += '<h2>公式清单（' + D.formulas.length + '）</h2>' + D.formulas.map(function (f) { return entityCard(f.id); }).join('');
    return h;
  }

  /* ---------- 案例库 ---------- */
  function renderCases() {
    var h = '<span class="layer-tag">工具</span><h1>历史案例库（' + D.cases.length + '）</h1><p class="goal">每个案例都是结构化研究记录。所有案例均来自原始 PDF 且未经独立验证（状态：待验证）；禁止只挑选与观点一致的案例，反例见每张卡片的『反证/缺口』。</p>';
    h += window.KBDiagrams.dia_case_timeline(D.cases);
    h += D.cases.map(function (c) { return entityCard(c.id); }).join('');
    h += '<h2>模式匹配引擎设计</h2><div class="kp-card"><div class="kp-normal">输入特征：当前宏观指标状态、过去若干周变化趋势、TGA 预测路径、准备金变化、DXY/收益率/TLT/VIX 组合、BTC 趋势与资金流。</div>' +
      '<div class="kp-desc"><b>流程：</b>' + D.patterns.pattern_matching_engine.steps.map(function (s, i) { return (i + 1) + '. ' + s; }).join('<br>') + '</div>' +
      '<div class="kp-desc"><b>硬约束：</b>' + D.patterns.pattern_matching_engine.hard_constraints.join('；') + '</div></div>';
    h += '<h2>速查组合（' + D.patterns.quick_combos.length + '）</h2><div class="tablewrap"><table class="kb"><tr><th>组合</th><th>解读</th><th>注意</th><th>来源</th></tr>';
    D.patterns.quick_combos.forEach(function (p) { h += '<tr><td><code>' + p.combo + '</code></td><td>' + p.reading + '</td><td>' + p.notes + '</td><td>' + p.source_document_ids.join(',') + ' p' + p.source_pages.join(',') + '</td></tr>'; });
    h += '</table></div>';
    return h;
  }

  /* ---------- 策略库 ---------- */
  function renderStrategies() {
    var h = '<span class="layer-tag">工具</span><h1>交易策略库（' + D.strategies.length + '）</h1><p class="goal">所有策略均为待验证的研究假设。没有实际执行回测时一律标注『尚未回测』；禁止虚构收益率、胜率、最大回撤。</p>';
    h += window.KBDiagrams.dia_strategy_flow();
    h += '<div class="filterbar"><select id="stratFilter"><option value="">全部验证状态</option><option>尚未回测</option></select></div>';
    h += '<div id="stratList">' + D.strategies.map(function (s) { return entityCard(s.strategy_id); }).join('') + '</div>';
    h += '<h2>宏观状态分类（' + D.regimes.length + '）</h2>' + D.regimes.map(function (r) { return entityCard(r.id); }).join('');
    return h;
  }

  /* ---------- Agent 模板 ---------- */
  function renderTemplates() {
    var h = '<span class="layer-tag">附录</span><h1>AI Agent 研究任务模板</h1><p class="goal">可直接复用的模板。权限边界：知识库提供研究依据，不自动授予执行权限——下单、转账、修改账户设置等高影响操作必须遵循独立授权与确认机制。</p>';
    D.agent_templates.forEach(function (t) {
      h += '<div class="kp-card"><div class="kp-head"><span class="kp-id">' + t.template_id + '</span><span class="kp-title">' + t.name + '</span></div>' +
        '<div class="kp-desc"><b>输入：</b>' + t.inputs.join('；') + '</div>' +
        '<div class="kp-normal"><b>步骤：</b><ol>' + t.steps.map(function (s) { return '<li>' + s + '</li>'; }).join('') + '</ol></div>' +
        '<div class="kp-desc"><b>输出：</b>' + t.outputs.join('；') + '</div>' +
        '<div class="kp-desc" style="color:#8b2020"><b>防护栏：</b>' + t.guards.join('；') + '</div>' +
        '<div class="kp-links">来源：' + t.source_document_ids.join(',') + ' p' + t.source_pages.join(',') + '</div></div>';
    });
    return h;
  }

  /* ---------- 来源与覆盖 ---------- */
  function renderSources(param) {
    var h = '<span class="layer-tag">附录</span><h1>来源与覆盖矩阵</h1><p class="goal">双向可追溯：知识点→来源页码；来源→知识点锚点。逐页文本：sources/extraction_records/{uuid}/page_NNN.txt。</p>';
    h += '<div class="card-grid">' + D.documents.map(function (d) {
      var n = D.knowledge_points.filter(function (k) { return k.source_document_id === d.document_id; }).length;
      return '<a class="mini-card" href="#/ch/L' + ({ DOC_FED: 1, DOC_LIQ: 3, DOC_TGA: 2, DOC_TSY: 1, DOC_DXY: 4 }[d.document_id] || 1) + '"><b>' + d.short_title + '</b><span>' + d.document_id + ' · ' + d.pages + ' 页 · ' + n + ' 个知识点<br>' + d.role + '</span></a>';
    }).join('') + '</div>';
    h += '<div class="filterbar">文档筛选：<select id="covFilter"><option value="">全部</option>' + D.documents.map(function (d) { return '<option>' + d.document_id + '</option>'; }).join('') + '</select></div>';
    h += '<div class="tablewrap"><table class="kb" id="covTable"><tr><th>知识点</th><th>主题</th><th>来源</th><th>类型</th><th>归属章节</th><th>锚点</th><th>覆盖状态</th><th>验证状态</th></tr>';
    D.coverage_matrix.forEach(function (m) {
      h += '<tr data-doc="' + m.source_document + '"><td>' + m.knowledge_id + '</td><td>' + m.original_topic + '</td><td>' + m.source_document.replace('DOC_', '') + ' p' + m.source_page + '</td><td>' + m.knowledge_type + '</td><td>' + (m.chapter_title || '—') + '</td><td>' + (m.html_anchor ? '<a href="#/ch/' + m.final_chapter + '#' + m.html_anchor + '">#' + m.section_id + '</a>' : '—') + '</td><td>' + m.coverage_status + '</td><td>' + m.verification_status + '</td></tr>';
    });
    h += '</table></div>';
    return h;
  }

  /* ---------- 冲突与待核查 ---------- */
  function renderQuestions() {
    var h = '<span class="layer-tag">附录</span><h1>冲突与待核查</h1><h2>文档间冲突（' + D.evidence.conflicts.length + '）</h2>';
    D.evidence.conflicts.forEach(function (c) {
      h += '<div class="kp-card"><div class="kp-head"><span class="kp-id">' + c.id + '</span><span class="kp-title">' + c.topic + '</span>' + badge('CONFLICT', 'cav') + '</div>' +
        '<div class="kp-desc"><b>表述A：</b>' + c.statement_a + '</div><div class="kp-desc"><b>表述B：</b>' + c.statement_b + '</div>' +
        '<div class="kp-normal"><b>分析：</b>' + c.analysis + '</div><div class="kp-links"><b>处理：</b>' + c.resolution + '</div></div>';
    });
    h += '<h2>待核查清单（' + D.open_questions.length + '）</h2>';
    D.open_questions.forEach(function (q) {
      h += '<div class="kp-card"><div class="kp-head"><span class="kp-id">' + q.id + '</span><span class="kp-title">待核查</span>' + badge(q.status, q.status === '已标注' ? 'ok' : 'cav') + '</div>' +
        '<div class="kp-normal">' + q.question + '</div>' +
        '<div class="kp-links">影响：' + q.impact + ' · 建议核查：' + q.proposed_check + ' · 来源：' + q.source_document_ids.join(',') + (q.source_pages.length ? ' p' + q.source_pages.join(',') : '') + '</div></div>';
    });
    h += '<h2>证据标签体系</h2><div class="tablewrap"><table class="kb"><tr><th>标签</th><th>含义</th></tr>' +
      D.evidence.tag_definitions.map(function (t) { return '<tr><td><code>' + t.tag + '</code></td><td>' + t.meaning + ' — ' + t.usage + '</td></tr>'; }).join('') + '</table></div>';
    h += '<h2>重要结论的证据记录（' + D.evidence.evidence_records.length + '）</h2>' + D.evidence.evidence_records.map(function (e) { return entityCard(e.id); }).join('');
    return h;
  }

  /* ---------- 图谱页 ---------- */
  function renderGraphPage() {
    return '<span class="layer-tag">工具</span><h1>知识图谱</h1><p class="goal">' + D.relationships.length + ' 条显式关系 · ' + (D.concepts.length + D.variables.length) + ' 个实体。关系类型定义：ACCOUNTING_LINK（会计）≠ CORRELATES_WITH（经验相关）≠ MAY_AFFECT（可能影响）≠ TRANSMITS_TO（传导）≠ CONFIRMS（辅助确认）≠ DERIVED_FROM（派生）。</p>' + window.KBGraph.render(true);
  }

  /* ---------- 路由 ---------- */
  function parseHash() {
    var h = location.hash.replace(/^#\/?/, '');
    if (!h) return { route: 'home', param: '', hash2: '' };
    var parts = h.split('?');
    var seg = parts[0].split('#');
    return { route: seg[0], param: (seg[1] || '') + (parts[1] ? '?' + parts[1] : ''), raw: h };
  }

  function render() {
    var r = parseHash();
    var html = '';
    if (r.route === 'home') html = renderHome();
    else if (r.route.indexOf('ch/') === 0) { var chId = r.route.slice(3); html = renderChapter(chId, null); }
    else if (r.route === 'graph') html = renderGraphPage();
    else if (r.route === 'dict') html = renderDict();
    else if (r.route === 'cases') html = renderCases();
    else if (r.route === 'strategies') html = renderStrategies();
    else if (r.route === 'templates') html = renderTemplates();
    else if (r.route === 'sources') html = renderSources();
    else if (r.route === 'questions') html = renderQuestions();
    else html = '<p>未知路由：<code>' + r.route + '</code></p>';
    app.innerHTML = html;
    window.KBGraph.bind(app);
    // 高亮锚点卡片
    var m = r.param.match(/card=([A-Z0-9_]+)/);
    var anchor = r.param.split('#')[0].match(/#(.+)/);
    var targetId = null;
    if (m && document.getElementById('card-' + m[1])) targetId = 'card-' + m[1];
    else if (anchor && document.getElementById('sec-' + anchor[1].toLowerCase())) targetId = 'sec-' + anchor[1].toLowerCase();
    if (targetId) {
      var el = document.getElementById(targetId);
      if (el) {
        el.scrollIntoView({ behavior: 'smooth', block: 'start' });
        if (el.classList.contains('kp-card')) {
          el.classList.add('orig-open');
          el.style.boxShadow = '0 0 0 3px rgba(21,94,171,.35)';
          setTimeout(function () { el.style.boxShadow = ''; }, 2200);
        }
      }
    }
    // 侧栏高亮 + 关闭移动端菜单
    document.querySelectorAll('.sidebar a').forEach(function (a) {
      a.classList.toggle('active', a.getAttribute('href') === '#/' + (r.route === 'home' ? 'home' : r.route + (r.route.indexOf('ch/') === 0 ? '' : '')));
    });
    document.getElementById('sidebar').classList.remove('open');
    document.getElementById('navToggle').setAttribute('aria-expanded', 'false');
    app.focus({ preventScroll: true });
  }

  /* ---------- 搜索 ---------- */
  var corpus = [];
  function buildCorpus() {
    corpus = [];
    D.knowledge_points.forEach(function (k) { corpus.push({ id: k.id, type: '知识点', title: k.title, text: k.title + k.description + k.normalized_statement + k.original_statement, go: '#/ch/' + (idIndex[k.id] ? idIndex[k.id].chapter : 'L1') + '?card=' + k.id }); });
    D.concepts.forEach(function (c) { corpus.push({ id: c.id, type: '概念', title: c.name_cn, text: c.name_cn + c.name_en + c.definition, go: '#/dict?v=' + c.id }); });
    D.variables.forEach(function (v) { corpus.push({ id: v.id, type: '变量', title: v.name_cn, text: v.name_cn + v.name_en + v.definition + v.series_id, go: '#/dict?v=' + v.id }); });
    D.formulas.forEach(function (f) { corpus.push({ id: f.id, type: '公式', title: f.name_cn, text: f.name_cn + f.formula, go: '#/dict?v=' + f.id }); });
    D.cases.forEach(function (c) { corpus.push({ id: c.id, type: '案例', title: c.title, text: c.title + c.background + c.btc_changes, go: '#/cases?card=' + c.id }); });
    D.strategies.forEach(function (s) { corpus.push({ id: s.strategy_id, type: '策略', title: s.name, text: s.name + s.objective + s.signal_definition, go: '#/strategies?card=' + s.strategy_id }); });
    D.open_questions.forEach(function (q) { corpus.push({ id: q.id, type: '待核查', title: q.question.slice(0, 40) + '…', text: q.question, go: '#/questions' }); });
  }
  function doSearch(q) {
    q = q.trim().toLowerCase();
    var box = document.getElementById('searchResults');
    if (q.length < 1) { box.hidden = true; return; }
    var hits = corpus.filter(function (c) { return c.text.toLowerCase().indexOf(q) >= 0 || c.id.toLowerCase().indexOf(q) >= 0; }).slice(0, 24);
    box.innerHTML = hits.length ? hits.map(function (c) {
      return '<a href="' + c.go + '"><span class="sr-type">' + c.type + '</span>' + c.title + '<span class="sr-page">' + c.id + '</span></a>';
    }).join('') : '<a>无结果</a>';
    box.hidden = false;
  }

  /* ---------- 事件绑定 ---------- */
  document.addEventListener('input', function (e) {
    if (e.target.id === 'searchInput') doSearch(e.target.value);
  });
  document.addEventListener('click', function (e) {
    if (!e.target.closest('.searchbox')) document.getElementById('searchResults').hidden = true;
    var t = e.target.closest('[data-toggle-orig]');
    if (t) {
      var card = t.closest('.kp-card');
      card.classList.toggle('orig-open');
      t.textContent = card.classList.contains('orig-open') ? '收起原文摘录 ▴' : '展开原文摘录 ▾';
    }
    var go = e.target.closest('[data-go]');
    if (go) { location.hash = '#/' + go.getAttribute('data-go').replace('src:', 'sources?doc='); }
  });
  document.addEventListener('change', function (e) {
    if (e.target.id === 'covFilter') {
      var v = e.target.value;
      document.querySelectorAll('#covTable tr[data-doc]').forEach(function (tr) { tr.style.display = (!v || tr.getAttribute('data-doc') === v) ? '' : 'none'; });
    }
  });
  document.querySelectorAll('.mode-btn').forEach(function (b) {
    b.addEventListener('click', function () {
      document.querySelectorAll('.mode-btn').forEach(function (x) { x.setAttribute('aria-pressed', 'false'); });
      b.setAttribute('aria-pressed', 'true');
      var mode = b.getAttribute('data-mode');
      document.body.classList.remove('mode-beginner', 'mode-research');
      if (mode === 'beginner') document.body.classList.add('mode-beginner');
      if (mode === 'research') document.body.classList.add('mode-research');
      // research：展开全部原文；beginner：折叠卡片只留首段
      document.querySelectorAll('.kp-card').forEach(function (c) {
        var tog = c.querySelector('[data-toggle-orig]');
        if (!tog) return;
        if (mode === 'research') { c.classList.add('orig-open'); tog.textContent = '收起原文摘录 ▴'; }
        else { c.classList.remove('orig-open'); tog.textContent = '展开原文摘录 ▾'; }
      });
      document.body.classList.toggle('mode-beginner-cards', mode === 'beginner');
    });
  });
  var navToggle = document.getElementById('navToggle');
  navToggle.addEventListener('click', function () {
    var sb = document.getElementById('sidebar');
    var open = sb.classList.toggle('open');
    navToggle.setAttribute('aria-expanded', String(open));
  });
  window.addEventListener('hashchange', render);

  /* ---------- 启动 ---------- */
  buildCorpus();
  document.getElementById('metaVersion').textContent = '构建：' + D.generated_at + ' · 知识点 ' + D.knowledge_points.length;
  render();
  // 入门模式样式注入
  var st = document.createElement('style');
  st.textContent = '.mode-beginner-cards .kp-card .kp-normal,.mode-beginner-cards .kp-card .kp-links,.mode-beginner-cards .kp-card .kp-desc{display:none}';
  document.head.appendChild(st);
})();
