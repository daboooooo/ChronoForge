# -*- coding: utf-8 -*-
import unittest
from kbutil import KPS, CONCEPTS, VARIABLES, FORMULAS, RELATIONSHIPS, CASES, STRATEGIES, ENTITY_IDS, PAGES

REQUIRED_KP = ['id', 'title', 'description', 'knowledge_type', 'source_document_id', 'source_page',
               'source_section', 'original_statement', 'normalized_statement', 'related_concepts',
               'related_variables', 'related_knowledge_ids', 'confidence', 'verification_status']

class TestJsonSchema(unittest.TestCase):
    def test_kp_required_fields(self):
        for k in KPS:
            for f in REQUIRED_KP:
                self.assertIn(f, k, f"{k['id']} 缺 {f}")
                self.assertIsNotNone(k[f], f"{k['id']} 字段 {f} 为 None")
            # 核心叙述字段不得为空；related_* 允许空列表（字段必须存在）
            for f in ['title', 'description', 'original_statement', 'normalized_statement', 'source_section']:
                self.assertTrue(k[f] not in ('',), f"{k['id']} 字段 {f} 为空")

    def test_unique_ids(self):
        for lst, name in [(KPS, 'KP'), (CONCEPTS, 'CONCEPT'), (VARIABLES, 'VAR'), (FORMULAS, 'FORMULA'), (CASES, 'CASE'), (STRATEGIES, 'STRATEGY')]:
            key = 'strategy_id' if lst is STRATEGIES else 'id'
            ids = [x[key] for x in lst]
            self.assertEqual(len(ids), len(set(ids)), f'{name} 存在重复 ID')

    def test_kp_types_valid(self):
        valid = {'CONCEPT', 'ACCOUNTING', 'MECHANISM', 'EMPIRICAL', 'HYPOTHESIS', 'DATA', 'CASE', 'STRATEGY', 'CAVEAT', 'AGENT_TASK'}
        for k in KPS:
            self.assertIn(k['knowledge_type'], valid, k['id'])

    def test_confidence_range(self):
        for k in KPS:
            self.assertTrue(0 < k['confidence'] <= 1, k['id'])
        for r in RELATIONSHIPS:
            self.assertTrue(0 < r['confidence'] <= 1, r['id'])

    def test_evidence_tags_valid(self):
        valid = {'[SOURCE]', '[OFFICIAL]', '[ACCOUNTING]', '[MECHANISM]', '[EMPIRICAL]', '[HYPOTHESIS]', '[STRATEGY]', '[CAVEAT]', '[UNKNOWN]', '[DATA]', '[CASE]'}
        for k in KPS:
            for t in k.get('evidence_tags', []):
                self.assertIn(t, valid, f"{k['id']} 非法证据标签 {t}")

class TestEntityIntegrity(unittest.TestCase):
    def test_relationship_endpoints(self):
        for r in RELATIONSHIPS:
            self.assertIn(r['source'], ENTITY_IDS, r['id'])
            self.assertIn(r['target'], ENTITY_IDS, r['id'])
            self.assertNotEqual(r['source'], r['target'], r['id'])

    def test_kp_related_ids_exist(self):
        for k in KPS:
            for rid in k['related_knowledge_ids']:
                self.assertIn(rid, ENTITY_IDS, f"{k['id']} -> {rid}")

    def test_kp_variables_exist(self):
        var_ids = {v['id'] for v in VARIABLES}
        for k in KPS:
            for v in k['related_variables']:
                self.assertIn(v, var_ids, f"{k['id']} 引用未知变量 {v}")

    def test_no_fake_precision(self):
        """禁止编造精确时间滞后：因果性 lag 不得含具体天数；标注『节奏』（观察/发布频率，有出处）或『原文未』的除外。"""
        import re
        for r in RELATIONSHIPS:
            lag = r.get('lag') or ''
            if '节奏' in lag or '原文未' in lag or '同步' in lag or '季节性' in lag or not lag:
                continue
            self.assertFalse(re.search(r'\d+\s*(天|日|周)', lag), f"{r['id']} 疑似编造时滞：{lag}")

if __name__ == '__main__':
    unittest.main()
