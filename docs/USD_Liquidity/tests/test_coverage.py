# -*- coding: utf-8 -*-
import unittest
from kbutil import KPS, COVERAGE, CHAPTERS, KP_IDS, ENTITY_IDS

class TestCoverage(unittest.TestCase):
    def test_matrix_matches_kps(self):
        self.assertEqual({m['knowledge_id'] for m in COVERAGE}, KP_IDS)

    def test_every_kp_has_chapter(self):
        for m in COVERAGE:
            self.assertTrue(m['final_chapter'], f"{m['knowledge_id']} 未归属章节")
            self.assertEqual(m['coverage_status'], '已写入知识库', m['knowledge_id'])

    def test_every_kp_referenced_by_chapter(self):
        ref = set()
        for ch in CHAPTERS:
            for sec in ch['sections']:
                ref |= set(sec.get('kp_ids', []))
        orphans = KP_IDS - ref
        self.assertFalse(orphans, f'未入正文知识点：{sorted(orphans)}')

    def test_chapter_refs_exist(self):
        for ch in CHAPTERS:
            for sec in ch['sections']:
                for kid in sec.get('kp_ids', []):
                    self.assertIn(kid, ENTITY_IDS, f"{ch['id']}/{sec['id']} -> {kid}")

    def test_anchors_unique(self):
        seen = set()
        for ch in CHAPTERS:
            for sec in ch['sections']:
                a = 'sec-' + sec['id'].lower()
                self.assertNotIn(a, seen, f"锚点重复 {a}")
                seen.add(a)

    def test_matrix_rows_complete_fields(self):
        for m in COVERAGE:
            for f in ['knowledge_id', 'source_document', 'source_page', 'original_topic', 'final_chapter', 'html_anchor', 'json_record_id', 'coverage_status', 'review_status']:
                self.assertIn(f, m, m['knowledge_id'])

if __name__ == '__main__':
    unittest.main()
