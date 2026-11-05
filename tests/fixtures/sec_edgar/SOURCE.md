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
