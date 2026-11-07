# 质量检查报告

生成时间：2026-11-07 21:35

测试总数：29 ｜ 失败：0 ｜ 错误：0

```
test_anchors_unique (test_coverage.TestCoverage.test_anchors_unique) ... ok
test_chapter_refs_exist (test_coverage.TestCoverage.test_chapter_refs_exist) ... ok
test_every_kp_has_chapter (test_coverage.TestCoverage.test_every_kp_has_chapter) ... ok
test_every_kp_referenced_by_chapter (test_coverage.TestCoverage.test_every_kp_referenced_by_chapter) ... ok
test_matrix_matches_kps (test_coverage.TestCoverage.test_matrix_matches_kps) ... ok
test_matrix_rows_complete_fields (test_coverage.TestCoverage.test_matrix_rows_complete_fields) ... ok
test_data_js_embeds_all_kps (test_html.TestLinks.test_data_js_embeds_all_kps) ... ok
test_diagram_ids_covered (test_html.TestLinks.test_diagram_ids_covered)
章节引用的图表必须在 diagrams.js 中实现。 ... ok
test_index_references_assets (test_html.TestLinks.test_index_references_assets) ... ok
test_offline_no_external_resources (test_html.TestLinks.test_offline_no_external_resources)
离线性：index/css/js 中不得引用外部 http(s) 资源。 ... ok
test_routes_supported (test_html.TestLinks.test_routes_supported) ... ok
test_tweet_links_preserved (test_html.TestLinks.test_tweet_links_preserved)
原始推文链接必须完整保留（可追溯性）。 ... ok
test_kp_related_ids_exist (test_json_schema.TestEntityIntegrity.test_kp_related_ids_exist) ... ok
test_kp_variables_exist (test_json_schema.TestEntityIntegrity.test_kp_variables_exist) ... ok
test_no_fake_precision (test_json_schema.TestEntityIntegrity.test_no_fake_precision)
禁止编造精确时间滞后：因果性 lag 不得含具体天数；标注『节奏』（观察/发布频率，有出处）或『原文未』的除外。 ... ok
test_relationship_endpoints (test_json_schema.TestEntityIntegrity.test_relationship_endpoints) ... ok
test_confidence_range (test_json_schema.TestJsonSchema.test_confidence_range) ... ok
test_evidence_tags_valid (test_json_schema.TestJsonSchema.test_evidence_tags_valid) ... ok
test_kp_required_fields (test_json_schema.TestJsonSchema.test_kp_required_fields) ... ok
test_kp_types_valid (test_json_schema.TestJsonSchema.test_kp_types_valid) ... ok
test_unique_ids (test_json_schema.TestJsonSchema.test_unique_ids) ... ok
test_case_pages_valid (test_source_citations.TestSourceCitations.test_case_pages_valid) ... ok
test_documents_exist (test_source_citations.TestSourceCitations.test_documents_exist) ... ok
test_formulas_have_scope (test_source_citations.TestSourceCitations.test_formulas_have_scope) ... ok
test_normalized_not_identical_duplication (test_source_citations.TestSourceCitations.test_normalized_not_identical_duplication)
规范化表述必须存在且非空（不要求与原文不同，但防止复制粘贴占位）。 ... ok
test_original_statement_nonempty (test_source_citations.TestSourceCitations.test_original_statement_nonempty) ... ok
test_page_in_range (test_source_citations.TestSourceCitations.test_page_in_range) ... ok
test_strategy_docs_valid (test_source_citations.TestSourceCitations.test_strategy_docs_valid) ... ok
test_unverified_claims_flagged (test_source_citations.TestSourceCitations.test_unverified_claims_flagged)
作者声称『已验证』的关键数字必须有 CAVEAT 标签（防冒充事实）。 ... ok

----------------------------------------------------------------------
Ran 29 tests in 0.007s

OK

```

## 检查范围

- test_json_schema：JSON 必填字段 / ID 唯一 / 枚举值
- test_source_citations：来源文档存在、页码有效、原文摘录非空
- test_coverage：121/121 知识点覆盖矩阵归属、章节引用闭环
- test_links：关系端点、related_knowledge_ids、锚点唯一
- test_html：index.html 资源引用、离线性（无外部 http 资源）、data.js 完整性

## 结论

全部通过
