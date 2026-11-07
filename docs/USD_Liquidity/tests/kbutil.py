# -*- coding: utf-8 -*-
"""共享测试工具：加载知识库数据。"""
import json, os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, 'data')

def load(name):
    with open(os.path.join(DATA, name), encoding='utf-8') as f:
        return json.load(f)

KPS = load('knowledge_points.json')
CONCEPTS = load('concepts.json')
VARIABLES = load('variables.json')
FORMULAS = load('formulas.json')
RELATIONSHIPS = load('relationships.json')
CASES = load('cases.json')
REGIMES = load('regimes.json')
STRATEGIES = load('strategies.json')
EVIDENCE = load('evidence.json')
OPEN_QUESTIONS = load('open_questions.json')
CHAPTERS = load('chapters.json')['chapters']
COVERAGE = load('coverage_matrix.json')
SOURCES = load('sources.json')['documents']
KP_IDS = {k['id'] for k in KPS}

ENTITY_IDS = set(KP_IDS)
ENTITY_IDS |= {c['id'] for c in CONCEPTS}
ENTITY_IDS |= {v['id'] for v in VARIABLES}
ENTITY_IDS |= {f['id'] for f in FORMULAS}
ENTITY_IDS |= {c['id'] for c in CASES}
ENTITY_IDS |= {r['id'] for r in REGIMES}
ENTITY_IDS |= {s['strategy_id'] for s in STRATEGIES}
ENTITY_IDS |= {e['id'] for e in EVIDENCE['evidence_records']}
ENTITY_IDS |= {r['id'] for r in RELATIONSHIPS}
ENTITY_IDS |= {q['id'] for q in load('open_questions.json')}

PAGES = {d['document_id']: d['pages'] for d in SOURCES}
