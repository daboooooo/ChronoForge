# TGA 数据字典（Phase 0 探测）

> 探测时间：2026-10-31T03:13:27.663677Z
> 数据来源：美国财政部 Fiscal Data API
> 说明：字段名/类型来自 API `meta`；任何字段缺失或更名必须在 Phase 0 评审中确认。

## 1. 覆盖范围总表

| 数据集 | 端点 | total-count | total-pages | 最早 record_date | 最新 record_date |
|---|---|---:|---:|---|---|
| `dts_operating_cash_balance` | `/v1/accounting/dts/operating_cash_balance` | 16738 | 3348 | 2005-10-03 | 2026-10-29 |
| `dts_deposits_withdrawals` | `/v1/accounting/dts/deposits_withdrawals_operating_cash` | 488409 | 97682 | 2005-10-03 | 2026-10-29 |
| `dts_public_debt_transactions` | `/v1/accounting/dts/public_debt_transactions` | 128814 | 25763 | 2005-10-03 | 2026-10-29 |
| `dts_public_debt_cash_basis` | `/v1/accounting/dts/adjustment_public_debt_transactions_cash_basis` | 75872 | 15175 | 2005-10-03 | 2026-10-29 |
| `dts_inter_agency_tax_transfers` | `/v1/accounting/dts/inter_agency_tax_transfers` | 3724 | 745 | 2023-02-14 | 2026-10-29 |
| `dts_income_tax_refunds` | `/v1/accounting/dts/income_tax_refunds_issued` | 19164 | 3833 | 2005-10-03 | 2026-10-29 |
| `auction_results` | `/v1/accounting/od/auctions_query` | 11154 | 2231 | 1979-11-15 | 2026-10-30 |
| `auction_upcoming` | `/v1/accounting/od/upcoming_auctions` | 101 | 21 | 2024-03-08 | 2026-10-16 |

## 2. dts_operating_cash_balance

- 端点：`/v1/accounting/dts/operating_cash_balance`
- total-count：16738
- total-pages：3348
- 最早/最新 record_date：2005-10-03 / 2026-10-29

### 字段

| 字段 | 标签 | 类型 | 格式 | 关键字段 |
|---|---|---|---|---|
| `record_date` | Record Date | DATE | YYYY-MM-DD | yes |
| `account_type` | Type of Account | STRING | String | yes |
| `close_today_bal` | Closing Balance Today | CURRENCY0 | $1,000,000 | yes |
| `open_today_bal` | Opening Balance Today | CURRENCY0 | $1,000,000 | yes |
| `open_month_bal` | Opening Balance This Month | CURRENCY0 | $1,000,000 | yes |
| `open_fiscal_year_bal` | Opening Balance This Fiscal Year | CURRENCY0 | $1,000,000 | yes |
| `table_nbr` | Table Number | STRING | String | yes |
| `table_nm` | Table Name | STRING | String | no |
| `sub_table_name` | Sub Table Name | STRING | String | no |
| `src_line_nbr` | Source Line Number | INTEGER | 10 | yes |
| `record_fiscal_year` | Fiscal Year | YEAR | YYYY | no |
| `record_fiscal_quarter` | Fiscal Quarter Number | QUARTER | Q | no |
| `record_calendar_year` | Calendar Year | YEAR | YYYY | no |
| `record_calendar_quarter` | Calendar Quarter Number | QUARTER | Q | no |
| `record_calendar_month` | Calendar Month Number | MONTH | MM | no |
| `record_calendar_day` | Calendar Day Number | DAY | DD | no |

### 枚举值（首尾页样本）

- `account_type`：`Federal Reserve Account`, `Treasury General Account (TGA)`, `Tax and Loan Note Accounts (Table V)`, `Total TGA Deposits (Table II)`, `Total TGA Withdrawals (Table II) (-)`, `Treasury General Account (TGA) Closing Balance`, `Treasury General Account (TGA) Opening Balance`

### 历史账户分期（2026-10-31 复核）

| 期间 | 余额行 | 金额字段 | 映射 |
|---|---|---|---|
| ≤2021-09-30 | `Federal Reserve Account` | `open_today_bal` + `close_today_bal` 均有值 | 历史 TGA 单行，拆 OPENING/CLOSING |
| 2021-10-01–2022-04-15 | `Treasury General Account (TGA)` | 同上 | 历史 TGA 单行，拆 OPENING/CLOSING |
| ≥2022-04-18 | 四行 TGA Opening/Total Deposits/Total Withdrawals/Closing | `close_today_bal=null`，金额在 `open_today_bal` | 按行映射 |

`Tax and Loan Note Accounts (Table V)`、`Supplementary Financing Program Account`、`Short-Term Cash Investments` 不计入 TGA。

### 首行样例（截断）

```json
[
  {
    "record_date": "2005-10-03",
    "account_type": "Federal Reserve Account",
    "close_today_bal": "5448",
    "open_today_bal": "4381",
    "open_month_bal": "4381",
    "open_fiscal_year_bal": "4381",
    "table_nbr": "I",
    "table_nm": "Operating Cash Balance",
    "sub_table_name": "Type of account",
    "src_line_nbr": "1",
    "record_fiscal_year": "2006",
    "record_fiscal_quarter": "1",
    "record_calendar_year": "2005",
    "record_calendar_quarter": "4",
    "record_calendar_month": "10",
    "record_calendar_day": "03"
  }
]
```

## 3. dts_deposits_withdrawals

- 端点：`/v1/accounting/dts/deposits_withdrawals_operating_cash`
- total-count：488409
- total-pages：97682
- 最早/最新 record_date：2005-10-03 / 2026-10-29

### 字段

| 字段 | 标签 | 类型 | 格式 | 关键字段 |
|---|---|---|---|---|
| `record_date` | Record Date | DATE | YYYY-MM-DD | yes |
| `account_type` | Type of Account | STRING | String | yes |
| `transaction_type` | Transaction Type | STRING | String | yes |
| `transaction_catg` | Transaction Category | STRING | String | yes |
| `transaction_catg_desc` | Transaction Category Description | STRING | String | yes |
| `transaction_today_amt` | Transactions Today | CURRENCY0 | $1,000,000 | yes |
| `transaction_mtd_amt` | Transactions Month to Date | CURRENCY0 | $1,000,000 | yes |
| `transaction_fytd_amt` | Transactions Fiscal Year to Date | CURRENCY0 | $1,000,000 | yes |
| `table_nbr` | Table Number | STRING | String | yes |
| `table_nm` | Table Name | STRING | String | no |
| `src_line_nbr` | Source Line Number | INTEGER | 10 | yes |
| `record_fiscal_year` | Fiscal Year | YEAR | YYYY | no |
| `record_fiscal_quarter` | Fiscal Quarter Number | QUARTER | Q | no |
| `record_calendar_year` | Calendar Year | YEAR | YYYY | no |
| `record_calendar_quarter` | Calendar Quarter Number | QUARTER | Q | no |
| `record_calendar_month` | Calendar Month Number | MONTH | MM | no |
| `record_calendar_day` | Calendar Day Number | DAY | DD | no |

### 枚举值（首尾页样本）

- `account_type`：`Federal Reserve Account`, `Treasury General Account (TGA)`
- `transaction_type`：`Deposits`, `Withdrawals`
- `transaction_catg`：`Agriculture Loan Repayments (misc)`, `Commodity Credit Corporation programs`, `Customs and Certain Excise Taxes`, `Deposits by States`, `Dept of Agriculture (USDA) - misc`, `DoD - Health`, `USDA - Commodity Credit Corporation`, `USDA - Federal Crop Insurance Corp Fund`, `USDA - Rural Services`

### 首行样例（截断）

```json
[
  {
    "record_date": "2005-10-03",
    "account_type": "Federal Reserve Account",
    "transaction_type": "Deposits",
    "transaction_catg": "Agriculture Loan Repayments (misc)",
    "transaction_catg_desc": "null",
    "transaction_today_amt": "62",
    "transaction_mtd_amt": "62",
    "transaction_fytd_amt": "62",
    "table_nbr": "II",
    "table_nm": "Deposits and Withdrawals of Operating Cash",
    "src_line_nbr": "1",
    "record_fiscal_year": "2006",
    "record_fiscal_quarter": "1",
    "record_calendar_year": "2005",
    "record_calendar_quarter": "4",
    "record_calendar_month": "10",
    "record_calendar_day": "03"
  }
]
```

## 4. dts_public_debt_transactions

- 端点：`/v1/accounting/dts/public_debt_transactions`
- total-count：128814
- total-pages：25763
- 最早/最新 record_date：2005-10-03 / 2026-10-29

### 字段

| 字段 | 标签 | 类型 | 格式 | 关键字段 |
|---|---|---|---|---|
| `record_date` | Record Date | DATE | YYYY-MM-DD | yes |
| `transaction_type` | Transaction Type | STRING | String | yes |
| `security_market` | Security Marketability | STRING | String | yes |
| `security_type` | Security Type | STRING | String | yes |
| `security_type_desc` | Security Type Description | STRING | String | yes |
| `transaction_today_amt` | Transactions Today | CURRENCY0 | $1,000,000 | yes |
| `transaction_mtd_amt` | Transactions Month to Date | CURRENCY0 | $1,000,000 | no |
| `transaction_fytd_amt` | Transactions Fiscal Year to Date | CURRENCY0 | $1,000,000 | no |
| `table_nbr` | Table Number | STRING | String | yes |
| `table_nm` | Table Name | STRING | String | no |
| `src_line_nbr` | Source Line Number | INTEGER | 10 | yes |
| `record_fiscal_year` | Fiscal Year | YEAR | YYYY | no |
| `record_fiscal_quarter` | Fiscal Quarter Number | QUARTER | Q | no |
| `record_calendar_year` | Calendar Year | YEAR | YYYY | no |
| `record_calendar_quarter` | Calendar Quarter Number | QUARTER | Q | no |
| `record_calendar_month` | Calendar Month Number | MONTH | MM | no |
| `record_calendar_day` | Calendar Day Number | DAY | DD | no |

### 枚举值（首尾页样本）

- `transaction_type`：`Issues`
- `security_market`：`Marketable`
- `security_type`：`Bills`, `Bonds`, `Federal Financing Bank`, `Inflation-Protected Securities Increment`, `Notes`
- `security_type_desc`：`Cash Management Series`, `Regular Series`

### 首行样例（截断）

```json
[
  {
    "record_date": "2005-10-03",
    "transaction_type": "Issues",
    "security_market": "Marketable",
    "security_type": "Federal Financing Bank",
    "security_type_desc": "null",
    "transaction_today_amt": "0",
    "transaction_mtd_amt": "0",
    "transaction_fytd_amt": "0",
    "table_nbr": "III-A",
    "table_nm": "Public Debt Transactions",
    "src_line_nbr": "6",
    "record_fiscal_year": "2006",
    "record_fiscal_quarter": "1",
    "record_calendar_year": "2005",
    "record_calendar_quarter": "4",
    "record_calendar_month": "10",
    "record_calendar_day": "03"
  }
]
```

## 5. dts_public_debt_cash_basis

- 端点：`/v1/accounting/dts/adjustment_public_debt_transactions_cash_basis`
- total-count：75872
- total-pages：15175
- 最早/最新 record_date：2005-10-03 / 2026-10-29

### 字段

| 字段 | 标签 | 类型 | 格式 | 关键字段 |
|---|---|---|---|---|
| `record_date` | Record Date | DATE | YYYY-MM-DD | yes |
| `transaction_type` | Transaction Type | STRING | String | yes |
| `adj_type` | Adjustment Type | STRING | String | yes |
| `adj_type_desc` | Adjustment Type Description | STRING | String | yes |
| `adj_today_amt` | Adjustments Today | CURRENCY0 | $1,000,000 | yes |
| `adj_mtd_amt` | Adjustments Month to Date | CURRENCY0 | $1,000,000 | yes |
| `adj_fytd_amt` | Adjustments Fiscal Year to Date | CURRENCY0 | $1,000,000 | yes |
| `table_nbr` | Table Number | STRING | String | yes |
| `table_nm` | Table Name | STRING | String | no |
| `sub_table_name` | Sub Table Name | STRING | String | no |
| `src_line_nbr` | Source Line Number | INTEGER | 10 | yes |
| `record_fiscal_year` | Fiscal Year | YEAR | YYYY | no |
| `record_fiscal_quarter` | Fiscal Quarter Number | QUARTER | Q | no |
| `record_calendar_year` | Calendar Year | YEAR | YYYY | no |
| `record_calendar_quarter` | Calendar Quarter Number | QUARTER | Q | no |
| `record_calendar_month` | Calendar Month Number | MONTH | MM | no |
| `record_calendar_day` | Calendar Day Number | DAY | DD | no |

### 枚举值（首尾页样本）

- `transaction_type`：`Issues`, `Public Debt Cash Issues`
- `adj_type`：`Bills (-)`, `Bonds and Notes (-)`, `Discount on New Issues`, `Federal Financing Bank (-)`, `Premium on New Issues`, `Public Debt Issues (Table III-A)`, `Public Debt Issues (Table IIIA)`

### 首行样例（截断）

```json
[
  {
    "record_date": "2005-10-03",
    "transaction_type": "Public Debt Cash Issues",
    "adj_type": "Public Debt Issues (Table III-A)",
    "adj_type_desc": "null",
    "adj_today_amt": "192315",
    "adj_mtd_amt": "192315",
    "adj_fytd_amt": "192315",
    "table_nbr": "III-B",
    "table_nm": "Adjustment of Public Debt Transactions to Cash Basis",
    "sub_table_name": "Transactions",
    "src_line_nbr": "1",
    "record_fiscal_year": "2006",
    "record_fiscal_quarter": "1",
    "record_calendar_year": "2005",
    "record_calendar_quarter": "4",
    "record_calendar_month": "10",
    "record_calendar_day": "03"
  }
]
```

## 6. dts_inter_agency_tax_transfers

- 端点：`/v1/accounting/dts/inter_agency_tax_transfers`
- total-count：3724
- total-pages：745
- 最早/最新 record_date：2023-02-14 / 2026-10-29

### 字段

| 字段 | 标签 | 类型 | 格式 | 关键字段 |
|---|---|---|---|---|
| `record_date` | Record Date | DATE | YYYY-MM-DD | yes |
| `classification` | Classification | STRING | String | yes |
| `today_amt` | Today Amount | CURRENCY0 | $1,000,000 | yes |
| `mtd_amt` | Month to Date Amount | CURRENCY0 | $1,000,000 | yes |
| `fytd_amt` | Fiscal Year to Date Amount | CURRENCY0 | $1,000,000 | yes |
| `table_nbr` | Table Number | STRING | String | yes |
| `table_nm` | Table Name | STRING | String | no |
| `sub_table_name` | Sub Table Name | STRING | String | no |
| `src_line_nbr` | Source Line Number | INTEGER | 10 | yes |
| `record_fiscal_year` | Fiscal Year | YEAR | YYYY | no |
| `record_fiscal_quarter` | Fiscal Quarter Number | QUARTER | Q | no |
| `record_calendar_year` | Calendar Year | YEAR | YYYY | no |
| `record_calendar_quarter` | Calendar Quarter Number | QUARTER | Q | no |
| `record_calendar_month` | Calendar Month Number | MONTH | MM | no |
| `record_calendar_day` | Calendar Day Number | DAY | DD | no |

### 枚举值（首尾页样本）

- `classification`：`Taxes - Corporate Income`, `Taxes - Miscellaneous Excise`, `Taxes - Railroad Retirement`, `Taxes - Withheld Individual/FICA`

### 首行样例（截断）

```json
[
  {
    "record_date": "2023-02-14",
    "classification": "Taxes - Corporate Income",
    "today_amt": "0",
    "mtd_amt": "0",
    "fytd_amt": "0",
    "table_nbr": "IV",
    "table_nm": "Inter-agency Tax Transfers",
    "sub_table_name": "Classification",
    "src_line_nbr": "1",
    "record_fiscal_year": "2023",
    "record_fiscal_quarter": "2",
    "record_calendar_year": "2023",
    "record_calendar_quarter": "1",
    "record_calendar_month": "02",
    "record_calendar_day": "14"
  }
]
```

## 7. dts_income_tax_refunds

- 端点：`/v1/accounting/dts/income_tax_refunds_issued`
- total-count：19164
- total-pages：3833
- 最早/最新 record_date：2005-10-03 / 2026-10-29

### 字段

| 字段 | 标签 | 类型 | 格式 | 关键字段 |
|---|---|---|---|---|
| `record_date` | Record Date | DATE | YYYY-MM-DD | yes |
| `tax_refund_type` | Federal Tax Refund Type | STRING | String | yes |
| `tax_refund_type_desc` | Federal Tax Refund Type Description | STRING | String | yes |
| `tax_refund_today_amt` | Federal Tax Refunds Today | CURRENCY0 | $1,000,000 | yes |
| `tax_refund_mtd_amt` | Federal Tax Refunds Month to Date | CURRENCY0 | $1,000,000 | yes |
| `tax_refund_fytd_amt` | Federal Tax Refunds Fiscal Year to Date | CURRENCY0 | $1,000,000 | yes |
| `table_nbr` | Table Number | STRING | String | yes |
| `table_nm` | Table Name | STRING | String | no |
| `sub_table_name` | Sub Table Name | STRING | String | no |
| `src_line_nbr` | Source Line Number | INTEGER | 10 | yes |
| `record_fiscal_year` | Fiscal Year | YEAR | YYYY | no |
| `record_fiscal_quarter` | Fiscal Quarter Number | QUARTER | Q | no |
| `record_calendar_year` | Calendar Year | YEAR | YYYY | no |
| `record_calendar_quarter` | Calendar Quarter Number | QUARTER | Q | no |
| `record_calendar_month` | Calendar Month Number | MONTH | MM | no |
| `record_calendar_day` | Calendar Day Number | DAY | DD | no |

### 枚举值（首尾页样本）

- `tax_refund_type`：`Business`, `Individual`, `Taxes - Business Tax Refunds (Checks)`, `Taxes - Business Tax Refunds (EFT)`, `Taxes - Individual Tax Refunds (Checks)`, `Taxes - Individual Tax Refunds (EFT)`

### 首行样例（截断）

```json
[
  {
    "record_date": "2005-10-03",
    "tax_refund_type": "Individual",
    "tax_refund_type_desc": "null",
    "tax_refund_today_amt": "5",
    "tax_refund_mtd_amt": "5",
    "tax_refund_fytd_amt": "5",
    "table_nbr": "VI",
    "table_nm": "Income Tax Refunds Issued",
    "sub_table_name": "Classification",
    "src_line_nbr": "1",
    "record_fiscal_year": "2006",
    "record_fiscal_quarter": "1",
    "record_calendar_year": "2005",
    "record_calendar_quarter": "4",
    "record_calendar_month": "10",
    "record_calendar_day": "03"
  }
]
```

## 8. auction_results

- 端点：`/v1/accounting/od/auctions_query`
- total-count：11154
- total-pages：2231
- 最早/最新 record_date：1979-11-15 / 2026-10-30

### 字段

| 字段 | 标签 | 类型 | 格式 | 关键字段 |
|---|---|---|---|---|
| `record_date` | Record Date | DATE | YYYY-MM-DD | yes |
| `cusip` | CUSIP | STRING | String | yes |
| `security_type` | Security Type | STRING | String | yes |
| `security_term` | Security Term | STRING | String | yes |
| `auction_date` | Auction Date | DATE | YYYY-MM-DD | yes |
| `issue_date` | Issue Date | DATE | YYYY-MM-DD | yes |
| `maturity_date` | Maturity Date | DATE | YYYY-MM-DD | yes |
| `price_per100` | Price per $100 | STRING | String | yes |
| `accrued_int_per100` | Accrued Interest per $100 | NUMBER | 10.2 | no |
| `accrued_int_per1000` | Accrued Interest per $1,000 | NUMBER | 10.2 | no |
| `adj_accrued_int_per1000` | Adjusted Accrued Interest per $1,000 | NUMBER | 10.2 | no |
| `adj_price` | Adjusted Price | NUMBER | 10.2 | no |
| `allocation_pctage` | Allocation Percentage | NUMBER | 10.2 | no |
| `allocation_pctage_decimals` | Allocation Percentage Decimals | NUMBER | 10.2 | no |
| `announcemtd_cusip` | Announced As CUSIP | STRING | String | no |
| `announcemt_date` | Announcement Date | DATE | YYYY-MM-DD | yes |
| `auction_format` | Auction Format | STRING | String | no |
| `avg_med_discnt_rate` | Average/Median Discount Rate | NUMBER | 10.2 | no |
| `avg_med_investment_rate` | Average/Median Investment Rate | NUMBER | 10.2 | no |
| `avg_med_price` | Average/Median Price | STRING | String | no |
| `avg_med_discnt_margin` | Average/Median Discount Margin | NUMBER | 10.2 | no |
| `avg_med_yield` | Average/Median Yield | NUMBER | 10.2 | no |
| `back_dated` | Back Dated | STRING | String | no |
| `back_dated_date` | Back Dated Date | DATE | YYYY-MM-DD | no |
| `bid_to_cover_ratio` | Bid To Cover Ratio | NUMBER | 10.2 | no |
| `callable` | Callable | STRING | String | no |
| `call_date` | Call Date | DATE | YYYY-MM-DD | no |
| `called_date` | Called Date | DATE | YYYY-MM-DD | no |
| `cash_management_bill_cmb` | Cash Management Bill (CMB) | STRING | String | yes |
| `closing_time_comp` | Closing Time (ET) - Competitive | STRING | String | no |
| `closing_time_noncomp` | Closing Time (ET) - Noncompetitive | STRING | String | no |
| `comp_accepted` | Competitive Accepted | NUMBER | 10.2 | no |
| `comp_bid_decimals` | Competitive Bid Decimals | INTEGER | 10 | no |
| `comp_tendered` | Competitive Tendered | NUMBER | 10.2 | no |
| `comp_tenders_accepted` | Competitive Tenders Accepted | STRING | String | no |
| `corpus_cusip` | Corpus CUSIP | STRING | String | no |
| `cpi_base_reference_period` | CPI Base Reference Period | STRING | String | no |
| `currently_outstanding` | Currently Outstanding | INTEGER | 10 | yes |
| `dated_date` | Dated Date | DATE | YYYY-MM-DD | no |
| `direct_bidder_accepted` | Direct Bidder Accepted | NUMBER | 10.2 | no |
| `direct_bidder_tendered` | Direct Bidder Tendered | NUMBER | 10.2 | no |
| `est_pub_held_mat_by_type_amt` | Estimated Amount of Publicly Held Maturing Securities by Type | NUMBER | 10.2 | no |
| `fima_included` | FIMA Competitive Inside Of Offering Amount | STRING | String | no |
| `fima_noncomp_accepted` | FIMA Noncompetitive Accepted | NUMBER | 10.2 | no |
| `fima_noncomp_tendered` | FIMA Noncompetitive Tendered | NUMBER | 10.2 | no |
| `first_int_period` | First Interest Period | STRING | String | no |
| `first_int_payment_date` | First Interest Payment Date | DATE | YYYY-MM-DD | no |
| `floating_rate` | Floating Rate | STRING | String | no |
| `frn_index_determination_date` | Index Determination Date | DATE | YYYY-MM-DD | no |
| `frn_index_determination_rate` | Index Determination Rate | NUMBER | 10.2 | no |
| `high_discnt_rate` | High Discount Rate | NUMBER | 10.2 | no |
| `high_investment_rate` | High Investment Rate | NUMBER | 10.2 | no |
| `high_price` | Price | NUMBER | 10.2 | no |
| `high_discnt_margin` | High Discount Margin | NUMBER | 10.2 | no |
| `high_yield` | High Yield | NUMBER | 10.2 | yes |
| `index_ratio_on_issue_date` | Index Ratio On Issue Date | NUMBER | 10.2 | no |
| `indirect_bidder_accepted` | Indirect Bidder Accepted | NUMBER | 10.2 | no |
| `indirect_bidder_tendered` | Indirect Bidder Tendered | NUMBER | 10.2 | no |
| `int_payment_frequency` | Frequency Of Interest Payments | STRING | String | yes |
| `int_rate` | Interest Rate | NUMBER | 10.2 | yes |
| `low_discnt_rate` | Low Discount Rate | NUMBER | 10.2 | no |
| `low_investment_rate` | Low Investment Rate | NUMBER | 10.2 | no |
| `low_price` | Low Price | NUMBER | 10.2 | no |
| `low_discnt_margin` | Low Discount Margin | NUMBER | 10.2 | no |
| `low_yield` | Low Yield | NUMBER | 10.2 | no |
| `mat_date` | Maturing Date | DATE | YYYY-MM-DD | no |
| `max_comp_award` | Maximum Competitive Award | INTEGER | 10 | no |
| `max_noncomp_award` | Maximum Noncompetitive Award | INTEGER | 10 | no |
| `max_single_bid` | Maximum Recognized Bid At A Single Discount Margin | INTEGER | 10 | no |
| `min_bid_amt` | Minimum To Bid Amount | INTEGER | 10 | no |
| `min_strip_amt` | Minimum Amount Required For STRIPS | INTEGER | 10 | no |
| `min_to_issue` | Minimum To Issue | NUMBER | 10.2 | no |
| `multiples_to_bid` | Multiples To Bid | NUMBER | 10.2 | no |
| `multiples_to_issue` | Multiples To Issue | NUMBER | 10.2 | no |
| `nlp_exclusion_amt` | NLP Exclusion Amount | INTEGER | 10 | no |
| `nlp_reporting_threshold` | NLP Reporting Threshold | INTEGER | 10 | no |
| `noncomp_accepted` | Noncompetitive Accepted | NUMBER | 10.2 | no |
| `noncomp_tenders_accepted` | Noncompetitive Tenders Accepted | STRING | String | no |
| `offering_amt` | Offering Amount | CURRENCY | $10.20 | yes |
| `original_cusip` | Original CUSIP | STRING | String | no |
| `original_dated_date` | Original Dated Date | DATE | YYYY-MM-DD | no |
| `original_issue_date` | Original Issue Date | DATE | YYYY-MM-DD | no |
| `original_security_term` | Original Security Term | STRING | String | no |
| `pdf_filenm_announcemt` | Treasury Offering Announcement Press Release File Name | STRING | String | no |
| `pdf_filenm_comp_results` | Treasury Auction Results Press Release File Name | STRING | String | no |
| `pdf_filenm_noncomp_results` | Treasury Noncompetitive Auction Results Press Release File Name | STRING | String | no |
| `primary_dealer_accepted` | Primary Dealer Accepted | NUMBER | 10.2 | no |
| `primary_dealer_tendered` | Primary Dealer Tendered | NUMBER | 10.2 | no |
| `ref_cpi_on_dated_date` | Reference CPI On Dated Date | NUMBER | 10.2 | no |
| `ref_cpi_on_issue_date` | Reference CPI On Issue Date | NUMBER | 10.2 | no |
| `reopening` | Reopening | STRING | String | yes |
| `security_term_day_month` | Security Term Day Month | STRING | String | no |
| `security_term_week_year` | Security Term Week Year | STRING | String | no |
| `series` | Series | STRING | String | no |
| `soma_accepted` | SOMA Accepted | NUMBER | 10.2 | no |
| `soma_holdings` | SOMA Holdings | INTEGER | 10 | no |
| `soma_included` | SOMA Included | STRING | String | no |
| `soma_tendered` | SOMA Tendered | NUMBER | 10.2 | no |
| `spread` | Spread | NUMBER | 10.2 | no |
| `std_int_payment_per1000` | Standard Interest Payment per $1,000 | NUMBER | 10.2 | no |
| `strippable` | Strippable | STRING | String | no |
| `tiin_conversion_factor_per1000` | TIIN Conversion Factor per $1,000 | NUMBER | 10.2 | no |
| `total_accepted` | Total Accepted | NUMBER | 10.2 | yes |
| `total_tendered` | Total Tendered | NUMBER | 10.2 | yes |
| `treas_retail_accepted` | Treasury Retail Accepted | NUMBER | 10.2 | no |
| `treas_retail_tenders_accepted` | Treasury Retail Tenders Accepted | STRING | String | no |
| `unadj_accrued_int_per1000` | Unadjusted Accrued Interest per $1,000 | NUMBER | 10.2 | no |
| `unadj_price` | Unadjusted Price | NUMBER | 10.2 | no |
| `xml_filenm_announcemt` | Treasury Offering Announcement XML File Name | STRING | String | no |
| `xml_filenm_comp_results` | Treasury Auction Results XML File Name | STRING | String | no |
| `inflation_index_security` | Inflation Index Security | STRING | String | no |
| `tint_cusip_1` | Tint CUSIP 1 | STRING | String | no |
| `tint_cusip_2` | Tint CUSIP 2 | STRING | String | no |
| `pdf_filenm_spec_announcemt` | PDF Filename - Special Announcement | STRING | String | no |

### 枚举值（首尾页样本）

- `security_type`：`Bill`, `Bond`, `Note`

### 首行样例（截断）

```json
[
  {
    "record_date": "1979-11-15",
    "cusip": "912827KC5",
    "security_type": "Note",
    "security_term": "10-Year",
    "auction_date": "1979-10-31",
    "issue_date": "1979-11-15",
    "maturity_date": "1989-11-15",
    "price_per100": "null",
    "accrued_int_per100": "null",
    "accrued_int_per1000": "0.0000000000",
    "adj_accrued_int_per1000": "null",
    "adj_price": "null",
    "allocation_pctage": "99.000000",
    "allocation_pctage_decimals": "0",
    "announcemtd_cusip": "null",
    "announcemt_date": "1979-10-24",
    "auction_format": "Multi-Price",
    "avg_med_discnt_rate": "null",
    "avg_med_investment_rate": "null",
    "avg_med_price": "100.0",
    "avg_med_discnt_margin": "null",
    "avg_med_yield": "10.750000",
    "back_dated": "No",
    "back_dated_date": "null",
    "bid_to_cover_ratio": "null",
    "callable": "No",
    "call_date": "null",
    "called_date": "null",
    "cash_management_bill_cmb": "No",
    "closing_time_comp": "null",
    "closing_time_noncomp": "null",
    "comp_accepted": "null",
    "comp_bid_decimals": "2",
    "comp_tendered": "null",
    "comp_tenders_accepted": "null",
    "corpus_cusip": "null",
    "cpi_base_reference_period": "null",
    "currently_outstanding": "null",
    "dated_date": "1979-11-15",
    "direct_bidder_accepted": "null",
    "direct_bidder_tendered": "null",
    "est_pub_held_mat_by_type_amt": "null",
    "fima_included": "null",
    "fima_noncomp_accepted": "null",
    "fima_noncomp_tendered": "null",
    "first_int_period": "Normal",
    "first_int_payment_date": "1980-05-15",
    "floating_rate": "No",
    "frn_index_determination_date": "null",
    "frn_index_determination_rate": "null",
    "high_discnt_rate": "null",
    "high_investment_rate": "null",
    "high_price": "99.759",
    "high_discnt_margin": "null",
    "high_yield": "10.7900",
    "index_ratio_on_issue_date": "null",
    "indirect_bidder_accepted": "null",
    "indirect_bidder_tendered": "null",
    "int_payment_frequency": "Semi-Annual",
    "int_rate": "10.750000",
    "low_discnt_rate": "null",
    "low_investment_rate": "null",
    "low_price": "100.303",
    "low_discnt_margin": "null",
    "low_yield": "10.700000",
    "mat_date": "null",
    "max_comp_award": "null",
    "max_noncomp_award": "null",
    "max_single_bid": "null",
    "min_bid_amt": "null",
    "min_strip_amt": "null",
    "min_to_issue": "null",
    "multiples_to_bid": "null",
    "multiples_to_issue": "null",
    "nlp_exclusion_amt": "null",
    "nlp_reporting_threshold": "null",
    "noncomp_accepted": "null",
    "noncomp_tenders_accepted": "null",
    "offering_amt": "2000000000",
    "original_cusip": "null",
    "original_dated_date": "null",
    "original_issue_date": "null",
    "original_security_term": "10-Year",
    "pdf_filenm_announcemt": "null",
    "pdf_filenm_comp_results": "null",
    "pdf_filenm_noncomp_results": "null",
    "primary_dealer_accepted": "null",
    "primary_dealer_tendered": "null",
    "ref_cpi_on_dated_date": "null",
    "ref_cpi_on_issue_date": "null",
    "reopening": "No",
    "security_term_day_month": "0-Month",
    "security_term_week_year": "10-Year",
    "series": "B-1989",
    "soma_accepted": "null",
    "soma_holdings": "null",
    "soma_included": "null",
    "soma_tendered": "null",
    "spread": "null",
    "std_int_payment_per1000": "53.7500",
    "strippable": "null",
    "tiin_conversion_factor_per1000": "null",
    "total_accepted": "2401000000",
    "total_tendered": "3818000000",
    "treas_retail_accepted": "null",
    "treas_retail_tenders_accepted": "null",
    "unadj_accrued_int_per1000": "null",
    "unadj_price": "null",
    "xml_filenm_announcemt": "null",
    "xml_filenm_comp_results": "null",
    "inflation_index_security": "No",
    "tint_cusip_1": "null",
    "tint_cusip_2": "null",
    "pdf_filenm_spec_announcemt": "null"
  }
]
```

## 9. auction_upcoming

- 端点：`/v1/accounting/od/upcoming_auctions`
- total-count：101
- total-pages：21
- 最早/最新 record_date：2024-03-08 / 2026-10-16

### 字段

| 字段 | 标签 | 类型 | 格式 | 关键字段 |
|---|---|---|---|---|
| `record_date` | Record Date | DATE | YYYY-MM-DD | yes |
| `security_type` | Security Type | STRING | String | yes |
| `security_term` | Security Term | STRING | String | yes |
| `reopening` | Reopening | STRING | String | yes |
| `cusip` | CUSIP | STRING | String | yes |
| `offering_amt` | Offering Amount | CURRENCY | $10.20 | yes |
| `announcemt_date` | Announcement Date | DATE | YYYY-MM-DD | yes |
| `auction_date` | Auction Date | DATE | YYYY-MM-DD | yes |
| `issue_date` | Issue Date | DATE | YYYY-MM-DD | yes |

### 枚举值（首尾页样本）

- `security_type`：`Bill`, `Bond`, `Note`, `TIPS Note`

### 首行样例（截断）

```json
[
  {
    "record_date": "2024-03-08",
    "security_type": "Bill",
    "security_term": "8-Week",
    "reopening": "Yes",
    "cusip": "912797JW8",
    "offering_amt": "90000000000",
    "announcemt_date": "2024-03-12",
    "auction_date": "2024-03-14",
    "issue_date": "2024-03-19"
  }
]
```

