#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""build_knowledge_base.py — 美元流动性研究知识库 构建脚本
职责：
1. 合并 kp_*.json -> data/knowledge_points.json（含统计）
2. 由 chapters.json 反查生成 data/coverage_matrix.json（知识点 -> 章节/锚点/状态）
3. 将全部 data/*.json 打包为 assets/js/data.js（file:// 离线可用）
4. 生成 reports/coverage_report.md 骨架数据
单一数据源原则：HTML 只从 data.js 渲染，不单独手工维护内容。
"""
import json, os, re, datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, 'data')
ASSETS = os.path.join(ROOT, 'assets')
REPORTS = os.path.join(ROOT, 'reports')

def load(name):
    with open(os.path.join(DATA, name), encoding='utf-8') as f:
        return json.load(f)

def dump(obj, name):
    with open(os.path.join(DATA, name), 'w', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def jdump(obj):
    return json.dumps(obj, ensure_ascii=False)

# ---------- 1. 合并知识点 ----------
kp_files = ['kp_fed.json', 'kp_liq.json', 'kp_tga.json', 'kp_tsy.json', 'kp_dxy.json']
doc_order = ['kp_fed.json', 'kp_liq.json', 'kp_tga.json', 'kp_tsy.json', 'kp_dxy.json']
kps = []
for fn in doc_order:
    kps.extend(load(fn))
# 证据标签归一化：统一为 [TAG] 带括号格式
for k in kps:
    k['evidence_tags'] = ['[' + t.strip('[]') + ']' for t in k.get('evidence_tags', [])]
ids = [k['id'] for k in kps]
assert len(ids) == len(set(ids)), 'duplicate KP id!'
kps.sort(key=lambda k: (k['source_document_id'], k['source_page'], k['id']))
dump(kps, 'knowledge_points.json')

# ---------- 2. 覆盖矩阵 ----------
chapters = load('chapters.json')['chapters']
kp_to_sections = {}
for ch in chapters:
    for sec in ch['sections']:
        for kid in sec.get('kp_ids', []):
            kp_to_sections.setdefault(kid, []).append((ch['id'], ch['title'], sec['id'], sec['title']))

def anchor(section_id):
    return 'sec-' + section_id.lower()

matrix = []
for k in kps:
    hits = kp_to_sections.get(k['id'], [])
    if hits:
        ch_id, ch_title, sec_id, sec_title = hits[0]
        status = '已写入知识库'
        # 已验证/存在疑问细分
        if k['verification_status'] in ('accounting_identity',):
            review = '已验证'
        elif 'OPEN_QUESTION' in jdump(k.get('related_knowledge_ids', [])) or k['verification_status'] == 'source_claimed':
            review = '存在疑问(待核查)'
        else:
            review = '已审核'
    else:
        ch_id, sec_id, ch_title, sec_title = '', '', '', ''
        status, review = '已提取-未入正文', '待处理'
    matrix.append({
        'knowledge_id': k['id'],
        'source_document': k['source_document_id'],
        'source_page': k['source_page'],
        'original_topic': k['title'],
        'normalized_topic': k['title'],
        'knowledge_type': k['knowledge_type'],
        'final_chapter': ch_id,
        'chapter_title': ch_title,
        'section_id': sec_id,
        'section_title': sec_title,
        'html_anchor': anchor(sec_id) if sec_id else '',
        'json_record_id': k['id'],
        'coverage_status': k.get('coverage_status', status),
        'review_status': k.get('review_status', review),
        'verification_status': k['verification_status'],
        'evidence_tags': k.get('evidence_tags', [])
    })
dump(matrix, 'coverage_matrix.json')

# ---------- 3. data.js ----------
doc_meta = load('sources.json')['documents']
payload = {
    'generated_at': datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),
    'documents': doc_meta,
    'knowledge_points': kps,
    'concepts': load('concepts.json'),
    'variables': load('variables.json'),
    'formulas': load('formulas.json'),
    'relationships': load('relationships.json'),
    'cases': load('cases.json'),
    'regimes': load('regimes.json'),
    'strategies': load('strategies.json'),
    'patterns': load('patterns.json'),
    'evidence': load('evidence.json'),
    'open_questions': load('open_questions.json'),
    'agent_templates': load('agent_templates.json'),
    'chapters': chapters,
    'coverage_matrix': matrix,
}
os.makedirs(os.path.join(ASSETS, 'js'), exist_ok=True)
with open(os.path.join(ASSETS, 'js', 'data.js'), 'w', encoding='utf-8') as f:
    f.write('/* 自动生成 by scripts/build_knowledge_base.py — 请勿手工编辑 */\n')
    f.write('window.KB_DATA = ' + jdump(payload) + ';\n')

# ---------- 4. 覆盖报告 ----------
by_doc, by_status, by_type = {}, {}, {}
for m in matrix:
    by_doc[m['source_document']] = by_doc.get(m['source_document'], 0) + 1
    by_status[m['coverage_status']] = by_status.get(m['coverage_status'], 0) + 1
    by_type[m['knowledge_type']] = by_type.get(m['knowledge_type'], 0) + 1
report = ['# 覆盖率报告', '', f"生成时间：{payload['generated_at']}", '',
          f"知识点总数：{len(kps)}", '', '## 按来源文档', '', '| 文档 | 知识点数 |', '|---|---|']
for d, c in sorted(by_doc.items()):
    report.append(f"| {d} | {c} |")
report += ['', '## 按覆盖状态', '', '| 状态 | 数量 |', '|---|---|']
for s, c in sorted(by_status.items()):
    report.append(f"| {s} | {c} |")
report += ['', '## 按知识类型', '', '| 类型 | 数量 |', '|---|---|']
for t, c in sorted(by_type.items()):
    report.append(f"| {t} | {c} |")
report += ['', '## 未入正文知识点', '']
orphans = [m for m in matrix if not m['final_chapter']]
if orphans:
    for m in orphans:
        report.append(f"- {m['knowledge_id']} {m['original_topic']}")
else:
    report.append('无 — 全部知识点均已归属章节。')
with open(os.path.join(REPORTS, 'coverage_report.md'), 'w', encoding='utf-8') as f:
    f.write('\n'.join(report) + '\n')

print('BUILD OK')
print(f"  knowledge_points: {len(kps)}")
print(f"  coverage_matrix : {len(matrix)} rows, orphans={len(orphans)}")
print(f"  concepts/variables/formulas/relationships/cases/regimes/strategies/patterns: "
      f"{len(payload['concepts'])}/{len(payload['variables'])}/{len(payload['formulas'])}/{len(payload['relationships'])}/"
      f"{len(payload['cases'])}/{len(payload['regimes'])}/{len(payload['strategies'])}/{len(payload['patterns']['quick_combos'])}+engine")
print(f"  data.js bytes   : {os.path.getsize(os.path.join(ASSETS,'js','data.js'))}")
