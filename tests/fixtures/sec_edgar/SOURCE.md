# Fixture 来源与形状说明

- 目录：`tests/fixtures/sec_edgar/`
- 形状核对日期：2026-11-05
- 依据：SEC EDGAR `companyfacts` / `companyconcept` API 实测响应
  （`https://data.sec.gov/api/xbrl/companyfacts/CIK##########.json`）。
- 关键字段路径：
  - `facts[taxonomy][concept]["units"][unit] = [observation, ...]`
  - `units` 是以单位（`USD` / `shares`）为键的 object，不是 list。
  - observation 字段：`end`（期间结束日）、`val`（数值）、`accn`、
    `fy`、`fp`、`form`、`filed`，`frame` 可选。
  - 此前 fixture 误用 `units` list + `i`/`f`/`$` + `taxonomy.entry.url`，
    这些字段在真实响应中不存在。
- 说明：样本（CIK 320193 / Apple Inc.）为最小必要字段的真实形状裁剪，
  数值取自公开披露口径，非逐条抓包。
- 另：连接器 companyfacts endpoint 已由实测 404 的
  `/submissions/CIK{cik}/companyfacts.json` 修正为
  `/api/xbrl/companyfacts/CIK{cik}.json`。
- 2026-11-06 审计 M10 实抓（User-Agent 含联系邮箱，全部 HTTP 200）：
  - `GET https://data.sec.gov/submissions/CIK0000320193.json`
    → 164,219 bytes；`filings.recent` 1004 条；
    `filings.files = [{"name": "CIK0000320193-submissions-001.json",
    "filingCount": 1271, "filingFrom": "1994-01-26",
    "filingTo": "2015-10-03"}]`。
  - `GET https://data.sec.gov/submissions/CIK0000320193-submissions-001.json`
    → 200；形状为扁平并行数组（无 `filings` 包装），字段与
    `recent` 相同（`accessionNumber`/`filingDate`/`reportDate`/
    `acceptanceDateTime`/`form`/`primaryDocument` 等）。
    归档 fixture `CIK0000320193-submissions-001.json` 保留前 3 行真实值。
  - `primaryDocument` 真实路径实测：`GET
    https://www.sec.gov/Archives/edgar/data/320193/000032019326000026/aapl-20260926.htm`
    → 200；`…/000114036126042149/xslF345X06/form4.xml` → 200；
    旧实现的 `…/data/000032019326000026`（缺 CIK 目录与文件名）→ 404。
  - 归档 fixture `CIK0000320193.json` 为主文件裁剪（保留最近 3 行 +
    真实 `files` 条目），用于分页抓取与 URL 构造用例。
