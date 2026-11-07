/* diagrams.js — 15 张离线 SVG/HTML 图表（无外部依赖）
 * 每个函数返回 HTML 字符串；解释性正文由 chapters.json 的 diagram_explain 渲染在图表下方。
 */
(function () {
  'use strict';
  function esc(s) { return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }

  /* 通用小工具：矩形节点 */
  function box(x, y, w, h, lines, cls, fs) {
    fs = fs || 12;
    var t = lines.map(function (ln, i) {
      return '<text x="' + (x + w / 2) + '" y="' + (y + h / 2 + 4 - (lines.length - 1) * (fs + 2) / 2 + i * (fs + 2)) + '" text-anchor="middle" font-size="' + fs + '">' + esc(ln) + '</text>';
    }).join('');
    return '<rect class="dbox ' + (cls || '') + '" x="' + x + '" y="' + y + '" width="' + w + '" height="' + h + '" rx="8"/>' + t;
  }
  function arrow(x1, y1, x2, y2, dash) {
    return '<path class="flow-arrow" d="M' + x1 + ',' + y1 + ' L' + x2 + ',' + y2 + '"' + (dash ? ' stroke-dasharray="5,4"' : '') + '/>';
  }
  function label(x, y, s, anchor, fs, color) {
    return '<text x="' + x + '" y="' + y + '" text-anchor="' + (anchor || 'middle') + '" font-size="' + (fs || 11) + '" fill="' + (color || '#5a6572') + '">' + esc(s) + '</text>';
  }
  function wrap(svg, caption) {
    return '<div class="diagram"><svg viewBox="0 0 ' + svg.split('viewBox="0 0 ')[1] + '</div>';
  }

  const DIA = {};

  /* 1. 美联储资产负债表简化结构图 */
  DIA.dia_fed_bs = function () {
    var s = '<svg viewBox="0 0 760 340" xmlns="http://www.w3.org/2000/svg">';
    s += '<defs><marker id="arrow" markerWidth="9" markerHeight="9" refX="8" refY="3" orient="auto"><path d="M0,0 L8,3 L0,6 Z" fill="#7c8ba0"/></marker></defs>';
    s += box(40, 40, 200, 46, ['资产端 Assets'], 'dbox-gov', 13);
    s += box(40, 100, 200, 38, ['美国国债（TREAST）']);
    s += box(40, 150, 200, 38, ['机构债与 MBS（WSHOSHO）']);
    s += box(40, 200, 200, 38, ['其他资产']);
    s += box(40, 250, 200, 42, ['总资产 = WALCL'], 'dbox-gov', 13);
    s += box(470, 40, 250, 46, ['负债端 Liabilities'], 'dbox-gov', 13);
    s += box(470, 100, 250, 38, ['银行准备金（WRESBAL）★核心'], 'dbox-cold');
    s += box(470, 150, 250, 38, ['TGA（WTREGEN）'], 'dbox-hot');
    s += box(470, 200, 250, 38, ['ON RRP（RRPONTSYD）'], 'dbox-hot');
    s += box(470, 250, 250, 38, ['流通现金（CURRCIR）+ 其他负债']);
    s += arrow(300, 271, 340, 271); s += arrow(420, 271, 460, 271);
    s += label(380, 260, '总额相等', 'middle', 11);
    // 跷跷板箭头：负债项之间互移
    s += '<path class="flow-arrow" d="M700,119 C740,130 740,159 700,169" stroke-dasharray="4,3"/>';
    s += '<path class="flow-arrow" d="M700,169 C740,180 740,209 700,219" stroke-dasharray="4,3"/>';
    s += label(752, 190, '负债项', 'middle', 10) + label(752, 204, '间搬移', 'middle', 10);
    s += label(380, 322, '恒等式：准备金 = 总资产 − TGA − ON RRP − 流通现金 − 其他负债', 'middle', 12, '#1c2330');
    s += '</svg>';
    return '<div class="diagram">' + s + '<div class="dia-caption">图：美联储资产负债表简化结构（★ 为流动性框架核心变量；括号内为 FRED 序列 ID）</div></div>';
  };

  /* 2. 利率走廊结构图 */
  DIA.dia_fed_corridor = function () {
    var s = '<svg viewBox="0 0 700 380" xmlns="http://www.w3.org/2000/svg">';
    s += '<defs><marker id="arrow" markerWidth="9" markerHeight="9" refX="8" refY="3" orient="auto"><path d="M0,0 L8,3 L0,6 Z" fill="#7c8ba0"/></marker></defs>';
    s += box(60, 30, 340, 40, ['贴现窗口（惩罚银行）· SRF 利率（惩罚一级交易商）'], 'dbox-hot', 11);
    s += label(230, 88, '惩罚性后备层 —— 正常情况无人使用', 'middle', 11, '#b3372f');
    s += box(60, 100, 340, 44, ['IORB 天花板 ≈ FFR 目标区间上限'], 'dbox-cold', 13);
    s += box(60, 176, 340, 44, ['EFFR（市场形成的无担保隔夜成交利率）'], '', 13);
    s += box(60, 252, 340, 44, ['ON RRP 利率 —— 地板'], 'dbox-cold', 13);
    s += label(230, 318, '走廊常态：ON RRP ＜ EFFR ＜ IORB', 'middle', 13, '#0c7a5b');
    // 套利机制箭头
    s += '<path class="flow-arrow" d="M430,110 C520,110 520,200 410,198" />';
    s += label(520, 150, 'EFFR＞IORB：银行取准备金', 'start', 10.5, '#155eab') + label(520, 165, '到市场出借 → 压低 EFFR', 'start', 10.5, '#155eab');
    s += '<path class="flow-arrow" d="M410,242 C520,242 520,160 410,162" />';
    s += label(520, 210, 'EFFR＜ON RRP：资金流向', 'start', 10.5, '#a05a00') + label(520, 225, '美联储逆回购 → 推高 EFFR', 'start', 10.5, '#a05a00');
    s += box(60, 336, 340, 34, ['更下层：SOFR（国债抵押回购）、T-bill'], '', 11.5);
    s += '</svg>';
    return '<div class="diagram">' + s + '<div class="dia-caption">图：利率走廊结构（IORB 天花板 / EFFR 中间 / ON RRP 地板）与自动稳定机制</div></div>';
  };

  /* 3. 国债类型对照表（HTML 表格） */
  DIA.dia_treasury_types = function () {
    var rows = [
      ['T-Bills 国库券', '≤1 年（4/8/13/17/26/52 周）', '无息 · 贴现发行', '否', '<code>DTB4WK DTB3/TB3MS DTB6 DTB1YR</code>', '✅', '短期流动性、资金面'],
      ['T-Notes 国库票据', '2–10 年', '半年付息', '否', '<code>DGS2 DGS3 DGS5 DGS7 DGS10</code>', '✅', '中期利率预期（曲线中段）'],
      ['T-Bonds 长期国债', '20–30 年', '半年付息', '否', '<code>DGS20 DGS30</code>', '✅', '长期资金成本、期限溢价'],
      ['TIPS 通胀保护', '5–30 年', '半年付息（本金随 CPI 调整）', '是', '<code>DFII5 DFII10 DFII30</code>', '✅', '实际收益率、通胀预期'],
      ['FRNs 浮动利率', '2 年', '每周按 13 周 T-Bill+利差重置', '否', '—', '⚠️', '降低利率风险'],
      ['CMBs 现金管理券', '几天–数月', '无息 · 贴现', '否', '—（TreasuryDirect）', '⚠️', '税收时差临时融资'],
      ['STRIPS 剥离债券', '2–30 年', '零息（本息剥离）', '否', '<code>GS1ZC GS5ZC GS10ZC GS30ZC</code>', '⚠️', '久期匹配（养老金）'],
      ['SLGS / 储蓄债券', '1–30 年', '固定/复利', '视品种', '—', '❌', '地方政府 / 个人，不可交易']
    ];
    var h = '<div class="diagram"><div class="tablewrap"><table class="kb"><tr><th>类别</th><th>期限</th><th>付息方式</th><th>通胀调整</th><th>FRED 序列</th><th>可得性</th><th>适用分析</th></tr>';
    rows.forEach(function (r) { h += '<tr>' + r.map(function (c) { return '<td>' + c + '</td>'; }).join('') + '</tr>'; });
    h += '</table></div><div class="dia-caption">表：美国国债类型与期限对照（✅ FRED 可取数 / ⚠️ 部分 / ❌ 不可）</div></div>';
    return h;
  };

  /* 4. 收益率曲线与来源拆分 */
  DIA.dia_yield_structure = function () {
    var s = '<svg viewBox="0 0 760 330" xmlns="http://www.w3.org/2000/svg">';
    s += '<defs><marker id="arrow" markerWidth="9" markerHeight="9" refX="8" refY="3" orient="auto"><path d="M0,0 L8,3 L0,6 Z" fill="#7c8ba0"/></marker></defs>';
    // 左：收益率曲线
    s += box(30, 20, 300, 30, ['收益率曲线（示意）'], 'dbox-gov', 12);
    s += '<line x1="60" y1="280" x2="330" y2="280" stroke="#7c8ba0" stroke-width="1.4"/>';
    s += '<line x1="60" y1="280" x2="60" y2="70" stroke="#7c8ba0" stroke-width="1.4"/>';
    s += '<path d="M60,240 C140,235 190,180 250,150 C285,133 310,126 330,122" fill="none" stroke="#155eab" stroke-width="2.5"/>';
    s += label(60, 300, '4周', 'middle', 10) + label(140, 300, '2年', 'middle', 10) + label(225, 300, '10年', 'middle', 10) + label(320, 300, '30年', 'middle', 10);
    s += label(46, 70, '收益率', 'end', 10);
    s += label(80, 90, '短端：受 FFR / IORB 支配', 'start', 10.5, '#155eab');
    s += label(120, 130, '中端：政策与增长预期（DGS2–DGS10）', 'start', 10.5, '#155eab');
    s += label(140, 170, '长端：+ 通胀预期 + 期限溢价', 'start', 10.5, '#a05a00');
    // 右：三块拆分
    s += box(420, 40, 300, 34, ['名义收益率 = DGS10', ''], 'dbox-gov', 12);
    s += box(420, 86, 300, 30, ['= 实际利率（DFII10）'], 'dbox-cold', 12);
    s += box(420, 124, 300, 30, ['+ 通胀预期（DGS10−DFII10 = breakeven）'], 'dbox-hot', 11);
    s += box(420, 162, 300, 30, ['+ 期限溢价（用 TLT 背离观察）'], '', 11.5);
    s += arrow(570, 74, 570, 84); s += arrow(570, 116, 570, 122); s += arrow(570, 154, 570, 160);
    s += label(570, 220, '收益率上行 ≠ 单一含义：', 'middle', 12, '#1c2330');
    s += label(570, 240, '三块中谁在动，决定宏观解读', 'middle', 12, '#1c2330');
    s += box(420, 256, 300, 44, ['TLT 价格 ≈ 长端收益率的反向', '背离 → 期限溢价变化（债务风险期）'], 'dbox-cold', 11.5);
    s += '</svg>';
    return '<div class="diagram">' + s + '<div class="dia-caption">图：国债价格、收益率与期限关系（左：曲线；右：收益率来源拆分）</div></div>';
  };

  /* 5. 国债发行资金流（泳道流程） */
  DIA.dia_treasury_flow = function () {
    var s = '<svg viewBox="0 0 860 360" xmlns="http://www.w3.org/2000/svg">';
    s += '<defs><marker id="arrow" markerWidth="9" markerHeight="9" refX="8" refY="3" orient="auto"><path d="M0,0 L8,3 L0,6 Z" fill="#7c8ba0"/></marker></defs>';
    s += label(110, 22, '私人部门', 'middle', 12, '#155eab') + label(370, 22, '财政部 TGA', 'middle', 12, '#155eab') + label(600, 22, '美联储负债端', 'middle', 12, '#155eab') + label(790, 22, '市场影响', 'middle', 12, '#155eab');
    s += box(40, 40, 150, 60, ['投资者付款', '（银行体系出金）'], 'dbox-hot', 12);
    s += box(290, 40, 160, 60, ['结算日：资金', '进入 TGA ↑'], 'dbox-hot', 12);
    s += box(520, 40, 170, 60, ['银行准备金 ↓', '（会计结果）'], 'dbox-hot', 12);
    s += box(720, 40, 120, 60, ['融资条件 /', '风险资产承压?'], 'dbox-hot', 11.5);
    s += arrow(190, 70, 288, 70); s += arrow(450, 70, 518, 70); s += arrow(690, 70, 718, 70);
    s += label(215, 60, '付款', 'middle', 10) + label(486, 60, '结算', 'middle', 10) + label(704, 60, '挤压', 'middle', 10);
    // 替代路径 ON RRP
    s += box(40, 150, 150, 54, ['货币基金资金', '（停放在 ON RRP）'], '', 11.5);
    s += box(520, 150, 170, 54, ['ON RRP ↓ 先行', '准备金受冲击小'], 'dbox-cold', 11.5);
    s += arrow(190, 177, 518, 177, true);
    s += label(354, 168, '资金来自 ON RRP 时（虚线）：缓冲路径', 'middle', 11, '#0c7a5b');
    // 放水
    s += box(290, 250, 160, 60, ['财政支出', 'TGA ↓'], 'dbox-cold', 12);
    s += box(520, 250, 170, 60, ['收款人存款 ↑', '银行准备金 ↑'], 'dbox-cold', 12);
    s += arrow(370, 248, 370, 120, true);
    s += label(395, 190, '反向：放水', 'start', 11, '#0c7a5b');
    s += arrow(450, 280, 518, 280);
    s += label(380, 335, '结论：TGA↑ 通常抽水但非一比一；资金来源决定冲击路径；净影响须逐项核算', 'middle', 12, '#1c2330');
    s += '</svg>';
    return '<div class="diagram">' + s + '<div class="dia-caption">图：国债发行与资金流动流程（红=抽水 / 绿=放水 / 虚线=条件依赖）</div></div>';
  };

  /* 6. 财政收支对 TGA 影响（放射） */
  DIA.dia_fiscal_tga = function () {
    var s = '<svg viewBox="0 0 760 340" xmlns="http://www.w3.org/2000/svg">';
    s += '<defs><marker id="arrow" markerWidth="9" markerHeight="9" refX="8" refY="3" orient="auto"><path d="M0,0 L8,3 L0,6 Z" fill="#7c8ba0"/></marker></defs>';
    s += box(300, 130, 170, 66, ['TGA 余额', 'WTREGEN / DTS-OCB'], 'dbox-gov', 13);
    s += box(40, 30, 210, 54, ['税收收入（税期日历）', '→ 概率预测'], 'dbox-hot', 11.5);
    s += box(510, 30, 210, 54, ['国债结算净流入', '（结算日·净额）→ 确定性日历'], 'dbox-hot', 11.5);
    s += box(40, 250, 210, 54, ['财政支出（月末/大额支付）', '→ 概率预测'], 'dbox-cold', 11.5);
    s += box(510, 250, 210, 54, ['其他现金调整', '→ 历史基线+异常标记'], '', 11.5);
    s += arrow(150, 84, 310, 138); s += arrow(610, 84, 462, 138);
    s += arrow(310, 196, 150, 252); s += arrow(462, 196, 610, 252);
    s += label(380, 318, 'ΔTGA = 现金收入 + 国债融资净流入 − 财政支出 + 其他调整（分类互斥，禁止重复计入）', 'middle', 12, '#1c2330');
    s += label(380, 110, '流入（抽水）', 'middle', 11, '#b3372f');
    s += label(380, 236, '流出（放水）', 'middle', 11, '#0c7a5b');
    s += '</svg>';
    return '<div class="diagram">' + s + '<div class="dia-caption">图：四类财政现金流对 TGA 的影响与预测方法</div></div>';
  };

  /* 7. TGA / ON RRP / 准备金 三池图 */
  DIA.dia_tga_onrrp = function () {
    var s = '<svg viewBox="0 0 780 360" xmlns="http://www.w3.org/2000/svg">';
    s += '<defs><marker id="arrow" markerWidth="9" markerHeight="9" refX="8" refY="3" orient="auto"><path d="M0,0 L8,3 L0,6 Z" fill="#7c8ba0"/></marker></defs>';
    s += box(40, 40, 190, 70, ['ON RRP', '非银资金池（货币基金）', 'RRPONTSYD'], '', 12);
    s += box(300, 40, 190, 70, ['银行准备金', 'R（WRESBAL）★', '约 4000 家银行·分布不均'], 'dbox-cold', 11.5);
    s += box(560, 40, 180, 70, ['TGA', '政府钱包（WTREGEN）', '目标为约束非插值'], 'dbox-hot', 11.5);
    // 情形①
    s += arrow(232, 66, 298, 66); s += label(265, 56, '①', 'middle', 11, '#155eab');
    s += label(122, 132, '① 发债资金来自货币基金 → ON RRP↓ 先行，准备金受损小', 'start', 11.5, '#0c7a5b');
    // 情形②
    s += arrow(298, 90, 232, 90); s += label(265, 82, '', 'middle');
    s += arrow(558, 66, 492, 66); s += label(525, 56, '②', 'middle', 11, '#b3372f');
    s += label(300, 158, '② 发债资金来自银行存款 → 准备金直接承压（红）', 'start', 11.5, '#b3372f');
    // 情形③
    s += arrow(560, 96, 494, 96); s += label(525, 108, '③', 'middle', 11, '#0c7a5b');
    s += label(300, 184, '③ 财政支出 → TGA↓ → 收款人存款↑ → 准备金↑（绿）', 'start', 11.5, '#0c7a5b');
    s += box(40, 210, 700, 40, ['完整恒等式：ΔR = ΔA − ΔTGA − ΔONRRP − ΔC − ΔD − ΔO'], 'dbox-gov', 12.5);
    s += box(40, 264, 340, 56, ['教学示例（假设）：', 'TGA +1000亿，ON RRP −600亿 ⇒ ΔR = −400亿', '只看 TGA 会误判为 −1000亿'], '', 11.5);
    s += box(400, 264, 340, 56, ['警告：ON RRP 与 TGA 的变化', '不一定一一对应 —— 禁止机械抵消，', '必须逐项核算（KP_TGA_024）'], 'dbox-hot', 11.5);
    s += '</svg>';
    return '<div class="diagram">' + s + '<div class="dia-caption">图：TGA、ON RRP 与银行准备金的三池关系（①②③ 为三种典型资金路径）</div></div>';
  };

  /* 8. 政策传导机制（纵向链） */
  DIA.dia_policy_transmission = function () {
    var nodes = [
      ['FOMC 调整 FFR 目标', 'dbox-gov'],
      ['同步调整 IORB / ON RRP 利率', ''],
      ['银行与非银机构资金成本变化', ''],
      ['短端市场利率：SOFR、T-bill ↑', ''],
      ['银行融资成本 ↑', ''],
      ['贷款利率 ↑（房贷/车贷/企业信贷）', ''],
      ['消费与投资减少', ''],
      ['经济降温 → 通胀下降', 'dbox-cold']
    ];
    var s = '<svg viewBox="0 0 560 560" xmlns="http://www.w3.org/2000/svg">';
    s += '<defs><marker id="arrow" markerWidth="9" markerHeight="9" refX="8" refY="3" orient="auto"><path d="M0,0 L8,3 L0,6 Z" fill="#7c8ba0"/></marker></defs>';
    var notes = ['每七周一次议息', '走廊框架', '两类机构分别感受', '回购与国债市场', '负债端定价', '信贷供给收缩', '需求端降温', '政策目标'];
    nodes.forEach(function (n, i) {
      var y = 18 + i * 64;
      s += box(80, y, 300, 40, [n[0]], n[1], 12.5);
      s += label(400, y + 26, notes[i], 'start', 11, '#5a6572');
      if (i < nodes.length - 1) s += arrow(230, y + 40, 230, y + 64);
    });
    s += '<path class="flow-arrow" d="M80,66 C30,66 30,490 76,490" stroke-dasharray="4,3"/>';
    s += label(24, 280, '数据反馈', 'middle', 10.5, '#5a6572') + label(24, 296, '下一次议息', 'middle', 10.5, '#5a6572');
    s += label(280, 546, '降息时整条链反向（资金变便宜 → 刺激经济）', 'middle', 11.5, '#0c7a5b');
    s += '</svg>';
    return '<div class="diagram">' + s + '<div class="dia-caption">图：FFR 上调的完整传导链（原文八节点，右侧为环节说明；虚线为数据反馈闭环）</div></div>';
  };

  /* 9. 美元流动性指标关系图 */
  DIA.dia_liquidity_metrics = function () {
    var s = '<svg viewBox="0 0 780 400" xmlns="http://www.w3.org/2000/svg">';
    s += '<defs><marker id="arrow" markerWidth="9" markerHeight="9" refX="8" refY="3" orient="auto"><path d="M0,0 L8,3 L0,6 Z" fill="#7c8ba0"/></marker></defs>';
    s += box(60, 20, 660, 44, ['Frank 口径：美元流动性 = 美联储总资产 − ON RRP − TGA = 银行准备金 + 流通中现金'], 'dbox-gov', 12.5);
    s += label(390, 86, '（近似恒等：负债端仅四项时成立；严格需扣除外国逆回购等）', 'middle', 10.5, '#8b2020');
    var boxes = [
      ['① 银行准备金', 'WRESBAL · 本框架核心', 40],
      ['② Fed BS 总规模', 'WALCL ≠ 准备金', 165],
      ['③ 公式化净流动性', 'Frank 定义', 290],
      ['④ 银行融资状况', 'SOFR / EFFR / 回购', 415],
      ['⑤ 整体金融条件', '美元·实际利率·风险偏好', 540],
      ['⑥ 加密边际资金', '稳定币·ETF 净流入', 660]
    ];
    boxes.forEach(function (b) {
      s += box(b[2], 120, 110, 62, [b[0], b[1]], 'dbox', 10.5);
    });
    s += label(390, 210, '六种"流动性"含义 —— 不可互相替代；引用任何讨论前先确认它在说哪一个', 'middle', 12, '#1c2330');
    s += box(60, 240, 300, 44, ['适用边界（作者 2026-10-02 划定）：', '准备金框架 → 大饼/加密；非美股主框架'], 'dbox-hot', 11.5);
    s += box(420, 240, 300, 44, ['水位标尺（Frank）：', '<3万亿紧张 · 3万亿充裕 · >3.2万亿充裕且过剩'], 'dbox-cold', 11.5);
    s += box(60, 308, 660, 44, ['监测四表：EFFR 看当下 · 利差看摩擦（>12bp 警惕 / >17bp 极端） · SRF 看求救（>3B） · 准备金看系统性风险（3 万亿红线）'], 'dbox-gov', 11.5);
    s += label(390, 386, '来源：DOC_LIQ p1 / DOC_FED p10-11 / DOC_TGA p4', 'middle', 10);
    s += '</svg>';
    return '<div class="diagram">' + s + '<div class="dia-caption">图：美元流动性指标关系（公式 + 六概念辨析 + 水位标尺 + 监测四表）</div></div>';
  };

  /* 10. 数据获取与验证工作流 */
  DIA.dia_data_workflow = function () {
    var s = '<svg viewBox="0 0 780 430" xmlns="http://www.w3.org/2000/svg">';
    s += '<defs><marker id="arrow" markerWidth="9" markerHeight="9" refX="8" refY="3" orient="auto"><path d="M0,0 L8,3 L0,6 Z" fill="#7c8ba0"/></marker></defs>';
    s += box(60, 20, 460, 76, ['① 数据采集层', '财政部：DTS（日频 OCB）· QRA · 拍卖日历 · CMB', 'FRED API：WRESBAL WTREGEN RRPONTSYD WALCL TREAST WSHOSHO CURRCIR SOFR EFFR'], 'dbox-gov', 11);
    s += box(60, 116, 460, 66, ['② 标准化与校验层', '统一日期/单位/频率；缺失与重复现金流检查', '周三口径对齐（周三值 ≠ 周内日均值）'], '', 11.5);
    s += box(60, 202, 460, 76, ['③ 预测引擎层', 'TGA 日度递推（三情景）→ 资产负债表会计分解', '→ 历史误差统计校准（五步实施，防过拟合）'], 'dbox-cold', 11.5);
    s += box(60, 298, 460, 66, ['④ 输出层', '1/2/4 周准备金预测 + 情景区间', '+ 历史误差（MAE/RMSE/方向/覆盖）+ 风险提示'], 'dbox-cold', 11.5);
    [96, 192, 288].forEach(function (y) { s += arrow(290, y, 290, y + 18); });
    s += box(560, 20, 190, 150, ['防前视偏差五元组', 'observation_date', 'release_date', 'retrieved_at', 'vintage_date', 'forecast_created_at', '——用『发布日』而非', '『观测日』判断信息可得'], 'dbox-hot', 10.5);
    s += box(560, 200, 190, 110, ['验证节点', '国债结算日 · 税期', 'H.4.1 每周四发布', 'QRA 季度更新', '债务上限事件'], '', 11);
    s += box(560, 340, 190, 56, ['节奏', '周报：每周四 H.4.1 后', '重大拍卖结算日加更'], 'dbox-gov', 11);
    s += arrow(522, 330, 558, 358);
    s += '</svg>';
    return '<div class="diagram">' + s + '<div class="dia-caption">图：数据获取与验证工作流（四层管线 + 防前视偏差元数据 + 验证节点）</div></div>';
  };

  /* 11. 四变量状态矩阵（HTML 表格，由 regimes 数据生成） */
  DIA.dia_regime_matrix = function (regimes) {
    var map = { 'REGIME_A_TIGHTENING': '🟥', 'REGIME_B_CRISIS': '🟨', 'REGIME_C_RISK_ON': '🟩', 'REGIME_D_EASING': '🟦' };
    var arrow = function (d) { return d === '↑' ? '<span class="up">↑</span>' : d === '↓' ? '<span class="down">↓</span>' : d; };
    var h = '<div class="diagram"><div class="tablewrap"><table class="kb"><tr><th>状态</th><th>DXY</th><th>收益率</th><th>TLT</th><th>VIX</th><th>典型驱动</th><th>代表资产反应（作者口径）</th></tr>';
    (regimes || []).filter(function (r) { return map[r.id]; }).forEach(function (r) {
      var parts = r.combo.split('｜').map(function (x) { return x.trim(); });
      h += '<tr><td><b>' + (map[r.id] || '') + ' ' + esc(r.name_cn.replace(/^状态[A-D]：/, '')) + '</b><br><span class="pager-hint">' + r.id + '</span></td>' +
        '<td>' + arrow(parts[0].split(' ')[1]) + '</td><td>' + arrow(parts[1].split(' ')[1]) + '</td><td>' + arrow(parts[2].split(' ')[1]) + '</td><td>' + arrow(parts[3].split(' ')[1]) + '</td>' +
        '<td>' + r.drivers.map(esc).join('；') + '</td><td>' + esc(r.asset_impacts).slice(0, 120) + '…</td></tr>';
    });
    h += '</table></div><div class="dia-caption">表：DXY × 收益率 × TLT × VIX 四变量状态矩阵（完整字段见 data/regimes.json）</div></div>';
    return h;
  };

  /* 12. BTC 分析框架图 */
  DIA.dia_btc_framework = function () {
    var s = '<svg viewBox="0 0 820 380" xmlns="http://www.w3.org/2000/svg">';
    s += '<defs><marker id="arrow" markerWidth="9" markerHeight="9" refX="8" refY="3" orient="auto"><path d="M0,0 L8,3 L0,6 Z" fill="#7c8ba0"/></marker></defs>';
    s += label(130, 22, '① 流动性层（会计）', 'middle', 12, '#155eab');
    s += label(420, 22, '② 传导层（机制）', 'middle', 12, '#155eab');
    s += label(700, 22, '③ 加密特有层（假设）', 'middle', 12, '#b3372f');
    s += box(30, 40, 200, 100, ['美联储总资产 − ON RRP − TGA', '＝ 准备金水位', '＋ 利差（SOFR−EFFR）', '＋ SRF 用量'], 'dbox-cold', 11);
    s += box(320, 40, 200, 100, ['银行可贷规模', '→ 杠杆空间', '→ 融资条件', '→ 风险偏好'], '', 11.5);
    s += box(610, 40, 180, 130, ['稳定币供给', 'ETF 净流入', '杠杆/衍生品仓位', '价格趋势/市场结构'], 'dbox-hot', 11.5);
    s += arrow(230, 90, 318, 90);
    s += arrow(520, 90, 608, 90);
    s += arrow(700, 172, 700, 210);
    s += box(300, 212, 220, 56, ['BTC 价格 / 趋势', '作者定位：全球流动性 β', '× 高杠杆风险资产'], 'dbox-gov', 12);
    s += arrow(700, 240, 522, 240, true);
    s += arrow(420, 142, 420, 210);
    s += label(560, 262, '虚线 = 待统计验证的确认变量', 'start', 10.5, '#a05a00');
    s += box(30, 300, 760, 56, ['四级信息组合竞赛（策略C）：① 仅准备金 → ② +TGA+ON RRP → ③ +稳定币+ETF → ④ +趋势/市场结构', '目标：控制美元、实际利率、波动、杠杆、资金流后，检验增量预测能力（BTC 未来 1/2/4 周收益率）'], 'dbox-gov', 11);
    s += '</svg>';
    return '<div class="diagram">' + s + '<div class="dia-caption">图：美元流动性与比特币分析框架（三层结构 + 信息组合竞赛）</div></div>';
  };

  /* 13. 案例时间线（HTML） */
  DIA.dia_case_timeline = function (cases) {
    var h = '<div class="diagram"><div class="timeline">';
    (cases || []).forEach(function (c) {
      h += '<div class="tl-item"><div class="tl-date">' + esc(c.start_date || '') + ' ～ ' + esc(c.end_date || '') + ' · ' + esc(c.title) + '</div>' +
        '<div style="font-size:13px">' + esc((c.btc_changes || c.other_asset_changes || c.liquidity_changes || '').slice(0, 90)) + '</div>' +
        '<div class="pager-hint">机制假设：' + esc((c.proposed_mechanism || '').slice(0, 80)) + ' · 证据：' + esc((c.supporting_evidence || '').slice(0, 60)) + ' · 状态：' + esc(c.verification_status) + '</div></div>';
    });
    h += '</div><div class="dia-caption">图：历史案例时间线（所有对应关系均为作者口径，虚线级证据；须回到 H.4.1/价格数据独立复核）</div></div>';
    return h;
  };

  /* 14. 策略决策流程（漏斗） */
  DIA.dia_strategy_flow = function () {
    var s = '<svg viewBox="0 0 780 470" xmlns="http://www.w3.org/2000/svg">';
    s += '<defs><marker id="arrow" markerWidth="9" markerHeight="9" refX="8" refY="3" orient="auto"><path d="M0,0 L8,3 L0,6 Z" fill="#7c8ba0"/></marker></defs>';
    s += box(190, 16, 400, 52, ['① 数据层：H.4.1 / DTS / FRED / 拍卖日历', '日更（DTS·ON RRP·SOFR·EFFR）+ 周更（准备金·TGA）'], 'dbox-gov', 11.5);
    s += box(140, 96, 500, 52, ['② 监测层（四表）：EFFR 基准判读 · SOFR−EFFR 利差阈值 · SRF 用量 · 准备金红线/连续周数'], '', 11.5);
    s += box(140, 176, 500, 52, ['③ 状态层：四变量矩阵（A–D）＋ 流动性状态（充裕/紧张）互证'], 'dbox-cold', 11.5);
    s += box(140, 256, 500, 66, ['④ 研究层（策略 A–E）：流动性拐点观察 · TGA 资金流预测 · BTC 增量信息检验 ·', 'DXY/收益率/TLT/VIX 模式匹配 · 宏观+技术结合（参数待研究）'], 'dbox-cold', 11.5);
    s += box(190, 350, 400, 60, ['⑤ 验证层：回测四指标（MAE/RMSE/方向/覆盖）', '+ 样本外 + 反例对照 —— 当前全部『尚未回测』'], 'dbox-hot', 11.5);
    [68, 148, 228, 322].forEach(function (y) { s += arrow(390, y, 390, y + 26); });
    s += '<path class="flow-arrow" d="M188,112 C90,112 90,288 138,288" stroke-dasharray="4,3"/>';
    s += label(78, 205, '不满足互证→退回', 'middle', 10, '#8b2020');
    s += label(390, 442, '约束：禁止从 ① 直接跳到交易；宏观解释 ≠ 交易指令；无回测不得声称有效', 'middle', 12, '#1c2330');
    s += '</svg>';
    return '<div class="diagram">' + s + '<div class="dia-caption">图：交易策略决策流程（五级漏斗；任何层级不满足则退回）</div></div>';
  };

  /* 15. 知识图谱（委托 graph.js） */
  DIA.dia_knowledge_graph = function (full) {
    return window.KBGraph ? window.KBGraph.render(full) : '<div class="diagram">图谱模块未加载</div>';
  };

  window.KBDiagrams = DIA;
})();
