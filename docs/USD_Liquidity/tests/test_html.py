# -*- coding: utf-8 -*-
import unittest, os, re
from kbutil import ROOT, KP_IDS, ENTITY_IDS, KPS, CHAPTERS

def read(p):
    with open(p, encoding='utf-8') as f:
        return f.read()

class TestLinks(unittest.TestCase):
    def setUp(self):
        self.main_js = read(os.path.join(ROOT, 'assets', 'js', 'main.js'))
        self.diagrams_js = read(os.path.join(ROOT, 'assets', 'js', 'diagrams.js'))
        self.graph_js = read(os.path.join(ROOT, 'assets', 'js', 'graph.js'))
        self.index_html = read(os.path.join(ROOT, 'index.html'))

    def test_index_references_assets(self):
        for f in ['assets/css/main.css', 'assets/js/data.js', 'assets/js/main.js', 'assets/js/diagrams.js', 'assets/js/graph.js']:
            self.assertIn(f, self.index_html)

    def test_offline_no_external_resources(self):
        """离线性：index/css/js 中不得引用外部 http(s) 资源。"""
        for name in ['index.html', os.path.join('assets', 'css', 'main.css'), os.path.join('assets', 'js', 'main.js'), os.path.join('assets', 'js', 'diagrams.js'), os.path.join('assets', 'js', 'graph.js')]:
            content = read(os.path.join(ROOT, name))
            externals = re.findall(r'(?:src|href)\s*=\s*["\']https?://', content)
            self.assertFalse(externals, f'{name} 存在外部资源引用 {externals}')

    def test_diagram_ids_covered(self):
        """章节引用的图表必须在 diagrams.js 中实现。"""
        used = set()
        for ch in CHAPTERS:
            for sec in ch['sections']:
                if sec.get('diagram'):
                    used.add(sec['diagram'])
        for d in used:
            self.assertIn(f'DIA.{d}', self.diagrams_js, f'图表 {d} 未实现')

    def test_routes_supported(self):
        for route in ['home', 'ch/', 'graph', 'dict', 'cases', 'strategies', 'templates', 'sources', 'questions']:
            self.assertIn(f"'{route}'", self.main_js if route != 'ch/' else "'ch/'", f'路由 {route} 未实现')

    def test_data_js_embeds_all_kps(self):
        data_js = read(os.path.join(ROOT, 'assets', 'js', 'data.js'))
        sample = ['KP_FED_001', 'KP_LIQ_001', 'KP_TGA_037', 'KP_TSY_019', 'KP_DXY_021', 'STRATEGY_A_LIQUIDITY_TURNING_POINT', 'CASE_2025_DRAIN', 'OPEN_QUESTION_FED_001']
        for sid in sample:
            self.assertIn(sid, data_js, f'data.js 缺少 {sid}')

    def test_tweet_links_preserved(self):
        """原始推文链接必须完整保留（可追溯性）。"""
        joined = str(KPS)
        for tid in ['1831275869869892035', '2106044439873433845', '2089000142427091042']:
            self.assertIn(tid, joined, f'推文链接缺失 {tid}')

if __name__ == '__main__':
    unittest.main()
