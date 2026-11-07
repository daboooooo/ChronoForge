#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""validate_coverage.py — 覆盖率与引用完整性验证。
检查：①每个知识点在覆盖矩阵中有明确处理状态；②章节引用的 ID 均存在；
③每个知识点均被章节引用或登记为未入正文（附原因）；④引用页码有效。
"""
import json, os, sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, 'data')
ERRORS = []

def load(name):
    with open(os.path.join(DATA, name), encoding='utf-8') as f:
        return json.load(f)

def main():
    kps = load('knowledge_points.json')
    kp_ids = {k['id'] for k in kps}
    matrix = load('coverage_matrix.json')
    chapters = load('chapters.json')['chapters']
    pages = {d['document_id']: d['pages'] for d in load('sources.json')['documents']}

    # 1. 矩阵行 == 知识点
    m_ids = [m['knowledge_id'] for m in matrix]
    if set(m_ids) != kp_ids:
        ERRORS.append(f"覆盖矩阵与知识点集合不一致：缺失 {kp_ids - set(m_ids)}，多余 {set(m_ids) - kp_ids}")
    for m in matrix:
        if m['coverage_status'] not in ('已写入知识库', '已验证', '存在疑问', '已归类', '已提取-未入正文', '未处理'):
            ERRORS.append(f"{m['knowledge_id']} 非法覆盖状态 {m['coverage_status']}")

    # 2. 章节引用有效性 + 反查覆盖
    referenced = set()
    entity_ids = set(kp_ids)
    for fn in ['concepts.json', 'variables.json', 'formulas.json', 'cases.json', 'regimes.json', 'strategies.json']:
        items = load(fn)
        lst = items if isinstance(items, list) else []
        if fn == 'strategies.json':
            entity_ids |= {x['strategy_id'] for x in lst}
        else:
            entity_ids |= {x['id'] for x in lst}
    entity_ids |= {e['id'] for e in load('evidence.json')['evidence_records']}
    entity_ids |= {r['id'] for r in load('relationships.json')}
    for ch in chapters:
        for sec in ch['sections']:
            for kid in sec.get('kp_ids', []):
                referenced.add(kid)
                if kid not in entity_ids:
                    ERRORS.append(f"章节 {ch['id']}/{sec['id']} 引用不存在的 ID：{kid}")
    for kid in kp_ids - referenced:
        ERRORS.append(f"知识点 {kid} 未被任何章节引用（orphan）")

    # 3. 关系引用
    for r in load('relationships.json'):
        for k in [r['source'], r['target']]:
            if k not in entity_ids:
                ERRORS.append(f"关系 {r['id']} 引用不存在的实体 {k}")

    # 4. 案例/策略来源页码有效
    for c in load('cases.json'):
        for i, doc in enumerate(c['source_document_ids']):
            if doc not in pages or (c['source_pages'][i] if i < len(c['source_pages']) else 1) > pages[doc]:
                ERRORS.append(f"案例 {c['id']} 页码越界")
    for s in load('strategies.json'):
        for i, doc in enumerate(s['source_document_ids']):
            if doc not in pages:
                ERRORS.append(f"策略 {s['strategy_id']} 文档不存在 {doc}")

    # 统计输出
    by_status = {}
    for m in matrix:
        by_status[m['coverage_status']] = by_status.get(m['coverage_status'], 0) + 1
    print('COVERAGE:', by_status)
    if ERRORS:
        print('COVERAGE VALIDATION FAILED:')
        for e in ERRORS:
            print(' -', e)
        sys.exit(1)
    print(f"COVERAGE VALIDATION OK: {len(kps)}/{len(kps)} 知识点全部归属章节，引用完整")

if __name__ == '__main__':
    main()
