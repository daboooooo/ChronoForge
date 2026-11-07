#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""validate_json.py — JSON 结构验证：必填字段、ID 唯一性、枚举值。"""
import json, os, sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, 'data')
ERRORS = []

def load(name):
    with open(os.path.join(DATA, name), encoding='utf-8') as f:
        return json.load(f)

def check(cond, msg):
    if not cond:
        ERRORS.append(msg)

REQUIRED_KP = ['id', 'title', 'description', 'knowledge_type', 'source_document_id', 'source_page',
               'source_section', 'original_statement', 'normalized_statement', 'related_concepts',
               'related_variables', 'related_knowledge_ids', 'confidence', 'verification_status']
KP_TYPES = {'CONCEPT', 'ACCOUNTING', 'MECHANISM', 'EMPIRICAL', 'HYPOTHESIS', 'DATA',
            'CASE', 'STRATEGY', 'CAVEAT', 'AGENT_TASK', 'OPEN_QUESTION'}
DOC_IDS = {'DOC_FED', 'DOC_LIQ', 'DOC_TGA', 'DOC_TSY', 'DOC_DXY'}

def main():
    kps = load('knowledge_points.json')
    ids = [k['id'] for k in kps]
    check(len(ids) == len(set(ids)), '知识点 ID 重复')
    for k in kps:
        for f in REQUIRED_KP:
            check(f in k and k[f] not in (None, ''), f"{k.get('id')} 缺字段 {f}")
        check(k['knowledge_type'] in KP_TYPES, f"{k['id']} 非法类型 {k['knowledge_type']}")
        check(k['source_document_id'] in DOC_IDS, f"{k['id']} 非法来源 {k['source_document_id']}")
        check(isinstance(k['source_page'], int) and k['source_page'] >= 1, f"{k['id']} 页码非法")
        check(0 < k['confidence'] <= 1, f"{k['id']} 置信度越界")
    pages = {d['document_id']: d['pages'] for d in load('sources.json')['documents']}
    for k in kps:
        check(k['source_page'] <= pages[k['source_document_id']], f"{k['id']} 页码超出文档页数")
    # 实体表
    for fn, key in [('concepts.json', 'id'), ('variables.json', 'id'), ('formulas.json', 'id'),
                    ('relationships.json', 'id'), ('cases.json', 'id'), ('regimes.json', 'id'),
                    ('agent_templates.json', 'template_id')]:
        items = load(fn)
        lst = items if isinstance(items, list) else items.get('documents', [])
        if fn == 'agent_templates.json':
            lst = items
        idl = [x[key] for x in lst]
        check(len(idl) == len(set(idl)), f"{fn} ID 重复")
    rels = load('relationships.json')
    entity_ids = set(ids)
    for fn in ['concepts.json', 'variables.json', 'formulas.json', 'cases.json', 'regimes.json']:
        items = load(fn)
        lst = items if isinstance(items, list) else []
        entity_ids |= {x['id'] for x in lst}
    for r in rels:
        for f in ['source', 'target', 'relation_type', 'mechanism', 'confidence', 'verification_status']:
            check(f in r, f"关系 {r.get('id')} 缺 {f}")
        check(r['source'] in entity_ids, f"关系 {r['id']} 起点 {r['source']} 不存在")
        check(r['target'] in entity_ids, f"关系 {r['id']} 终点 {r['target']} 不存在")
        check(r['source'] != r['target'], f"关系 {r['id']} 自指")
    # chapters
    chapters = load('chapters.json')['chapters']
    for ch in chapters:
        check(ch.get('id') and ch.get('title') and ch.get('sections'), f"章节 {ch.get('id')} 结构不全")
        for sec in ch['sections']:
            check(sec.get('id') and sec.get('html') is not None, f"小节 {sec.get('id')} 结构不全")
            if sec.get('diagram'):
                check(bool(sec.get('diagram_explain')), f"小节 {sec['id']} 图表缺解释文字")
    if ERRORS:
        print('JSON VALIDATION FAILED:')
        for e in ERRORS:
            print(' -', e)
        sys.exit(1)
    print(f"JSON VALIDATION OK: {len(kps)} KPs, {len(rels)} relations, {len(chapters)} chapters")

if __name__ == '__main__':
    main()
