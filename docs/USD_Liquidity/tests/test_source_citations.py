# -*- coding: utf-8 -*-
import unittest
from kbutil import KPS, CASES, STRATEGIES, FORMULAS, PAGES, SOURCES

class TestSourceCitations(unittest.TestCase):
    def test_documents_exist(self):
        ids = {d['document_id'] for d in SOURCES}
        for k in KPS:
            self.assertIn(k['source_document_id'], ids, k['id'])

    def test_page_in_range(self):
        for k in KPS:
            self.assertTrue(1 <= k['source_page'] <= PAGES[k['source_document_id']], k['id'])

    def test_original_statement_nonempty(self):
        for k in KPS:
            self.assertGreater(len(k['original_statement']), 10, f"{k['id']} 原文摘录过短，不足以复核")

    def test_normalized_not_identical_duplication(self):
        """规范化表述必须存在且非空（不要求与原文不同，但防止复制粘贴占位）。"""
        for k in KPS:
            self.assertGreater(len(k['normalized_statement']), 10, k['id'])

    def test_case_pages_valid(self):
        for c in CASES:
            for i, doc in enumerate(c['source_document_ids']):
                self.assertIn(doc, PAGES, c['id'])
                if i < len(c['source_pages']):
                    self.assertTrue(1 <= c['source_pages'][i] <= PAGES[doc], c['id'])

    def test_strategy_docs_valid(self):
        for s in STRATEGIES:
            for doc in s['source_document_ids']:
                self.assertIn(doc, PAGES, s['strategy_id'])

    def test_formulas_have_scope(self):
        for f in FORMULAS:
            self.assertTrue(f.get('scope_and_conditions'), f"{f['id']} 公式缺成立范围说明")

    def test_unverified_claims_flagged(self):
        """作者声称『已验证』的关键数字必须有 CAVEAT 标签（防冒充事实）。"""
        k = next(x for x in KPS if x['id'] == 'KP_FED_024')
        self.assertIn('[CAVEAT]', k['evidence_tags'])
        self.assertIn('尚未回测', next(s for s in STRATEGIES if s['strategy_id'] == 'STRATEGY_FED_PATTERN1')['validation_status'])

if __name__ == '__main__':
    unittest.main()
