# treasury_fiscal fixtures

- 来源：U.S. Treasury Fiscal Data API
  - Base：`https://api.fiscaldata.treasury.gov/services/api/fiscal_service`
  - 端点：`v1/accounting/dts/*`、`v1/accounting/od/auction_results`、`auction_upcoming`
- 来源类型：真实 HTTP 响应裁剪（每个端点保留 `first_page` / `last_page` 各 3 行）
- 裁剪说明：仅裁剪行数，保留完整 `meta.labels` / `meta.dataTypes` / `meta.dataFormats`
  以及全部列；字段名与类型均为真实响应，未人工构造。
- 核对日期：2026-11-05。抽样复核 `operating_cash_balance` 实时响应可访问，
  当日最新 `record_date=2026-11-03`，字段集合与 fixture 一致。
- 备注：fixture 的 `meta` 不含分页 `total-pages`（真实响应中存在，测试内按需注入）。
