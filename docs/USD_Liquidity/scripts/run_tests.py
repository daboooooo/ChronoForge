#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""run_tests.py — 运行全部单元测试并生成质量报告。"""
import unittest, sys, os, io, json, datetime

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(ROOT, 'tests'))

def main():
    loader = unittest.TestLoader()
    suite = loader.discover(os.path.join(ROOT, 'tests'), pattern='test_*.py')
    buf = io.StringIO()
    runner = unittest.TextTestRunner(stream=buf, verbosity=2)
    result = runner.run(suite)
    report = ['# 质量检查报告', '', f"生成时间：{datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}", '',
              f"测试总数：{result.testsRun} ｜ 失败：{len(result.failures)} ｜ 错误：{len(result.errors)}", '',
              '```', buf.getvalue(), '```', '',
              '## 检查范围', '',
              '- test_json_schema：JSON 必填字段 / ID 唯一 / 枚举值',
              '- test_source_citations：来源文档存在、页码有效、原文摘录非空',
              '- test_coverage：121/121 知识点覆盖矩阵归属、章节引用闭环',
              '- test_links：关系端点、related_knowledge_ids、锚点唯一',
              '- test_html：index.html 资源引用、离线性（无外部 http 资源）、data.js 完整性', '',
              '## 结论', '',
              ('全部通过' if result.wasSuccessful() else '存在失败 —— 见上方明细')]
    with open(os.path.join(ROOT, 'reports', 'quality_report.md'), 'w', encoding='utf-8') as f:
        f.write('\n'.join(report) + '\n')
    print(buf.getvalue())
    print('Quality report -> reports/quality_report.md')
    sys.exit(0 if result.wasSuccessful() else 1)

if __name__ == '__main__':
    main()
