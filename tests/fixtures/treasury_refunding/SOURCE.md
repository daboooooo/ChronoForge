# treasury_refunding fixtures

- 来源：U.S. Department of the Treasury 官网真实抓包（curl）
- 抓取日期：2026-11-05
- `most_recent_documents.html`
  - URL：`https://home.treasury.gov/policy-issues/financing-the-government/quarterly-refunding/most-recent-quarterly-refunding-documents`
  - HTTP 200，73,554 B，未裁剪全文
- `press_release_sb0675.html`
  - URL：`https://home.treasury.gov/news/press-releases/sb0675`
  - HTTP 200，72,572 B，未裁剪全文
  - 内容：2026 Q4 refunding —— 2026 年 10–12 月净借款 618B / 季末现金 850B；
    2027 年 1–3 月净借款 800B / 季末现金 1.000T
- 已知限制：历史新闻稿归档页为 JS 渲染，本 fixture 仅覆盖 most-recent 索引页与
  最新一期新闻稿（实现方案 §3.5 / §14.2 的 P0 限制）。
