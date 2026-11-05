# 基于美国财政部现金流预测的 TGA 前瞻模型
## AI Agent 开发与测试实施规范

**项目名称：** TGA Cash Flow Forecasting System  
**项目类型：** 宏观经济数据采集、现金流预测、历史回测与分析系统  
**主要数据来源：** 美国财政部 FiscalData、TreasuryDirect、美国财政部官方公告  
**主要预测对象：** Treasury General Account（TGA）  
**预测周期：** 7天、14天、30天、60天、90天  
**开发原则：** 官方数据优先、现金流可解释、预测结果可回测、数据来源可追溯。

---

# 第一章 项目目标

## 1.1 项目背景

TGA（Treasury General Account）是美国财政部在美联储维护的主要现金账户。

TGA 余额的变化主要受到以下因素影响：

1. 联邦政府税收收入。
2. 国债发行募集资金。
3. 国债到期偿还。
4. 联邦政府日常支出。
5. 政府福利支付。
6. 国债利息支付。
7. 其他财政收入和支出。
8. 财政部短期现金管理操作。

TGA 余额变化会影响银行准备金和货币市场流动性。

本项目希望建立一个能够提前预测 TGA 余额变化的系统，而不是仅仅展示 TGA 历史数据。

## 1.2 核心目标

建立一个基于财政部现金流的预测模型：

\[
TGA_{t+n}=TGA_t+\sum_{i=1}^{n}NetCashFlow_{t+i}
\]

其中：

\[
NetCashFlow_t=Inflows_t-Outflows_t
\]

模型需要回答：

- 未来7天 TGA 余额如何变化？
- 未来14天 TGA 是增加还是减少？
- 未来30天财政部需要多少净融资？
- 未来60～90天是否存在明显的 TGA 抽水或放水压力？
- 哪些现金流项目是 TGA 变化的主要驱动因素？
- 预测结果的不确定性有多大？

## 1.3 最终交付物

Agent 必须交付：

| 交付物 | 内容 |
|---|---|
| 数据采集模块 | 自动下载美国财政部官方数据 |
| 数据库 | 存储历史现金流和预测数据 |
| 数据清洗模块 | 标准化、去重、单位转换、异常检查 |
| 现金流分析模块 | 重建历史 TGA 现金流 |
| 预测模型 | 预测未来7～90天 TGA |
| 回测模块 | 历史滚动预测和误差评估 |
| 可视化模块 | 历史余额、预测余额、现金流分解 |
| 分析报告 | 每日生成 TGA 预测报告 |
| 自动化任务 | 定时采集、预测和报告生成 |
| 技术文档 | 安装、运行、维护和测试说明 |

---

# 第二章 系统设计原则

## 2.1 必须区分三个概念

Agent 必须严格区分：

### A. 已知现金流

已经公布、能够确定发生日期和金额的现金流。

例如：

- 已公布的国债发行结算计划。
- 已公布的国债到期偿还日期。
- 已确定的国债利息支付日期。
- 已知的税收截止日期。
- 已经发生的实际税收和政府支出。

### B. 预测现金流

尚未发生，但可以根据历史规律、财政计划和统计模型预测的现金流。

例如：

- 未来税收收入。
- 未来社会保障支付。
- 未来政府采购支出。
- 尚未公布具体规模的国债发行。
- 其他政府收入和支出。

### C. 情景假设

无法通过现有信息准确预测，需要用户设定的参数。

例如：

- 财政部未来希望维持的 TGA 目标余额。
- 未来国债发行规模变化。
- 政府支出突然增加。
- 特殊财政政策或临时融资安排。

这三类数据必须分别存储和展示。

不能把预测值伪装成已经公布的事实。

## 2.2 模型必须遵循现金结算日期

所有预测都应以实际现金流发生日期为准。

例如：

国债拍卖日不等于资金进入 TGA 的日期。

必须区分：

- Announcement Date：公告日期。
- Auction Date：拍卖日期。
- Issue Date：发行结算日期。
- Maturity Date：到期日期。

对于国债发行和偿还，原则上按照结算日期记录现金流。

对于政府支出和税收，使用财政部 DTS 实际记录日期进行历史建模。

## 2.3 统一金额单位

数据库内部统一使用：

**USD Million（百万美元）**

所有原始数据必须保留原始金额和原始单位。

禁止在不同模块中混用：

- 美元
- 百万美元
- 十亿美元

展示时可以转换为 Billion USD。

例如：

1000000 Million USD = 1000 Billion USD。

---

# 第三章 数据采集体系

## 3.1 数据源优先级

必须优先使用美国政府官方数据。

| 优先级 | 数据源 | 主要用途 | 更新频率 |
|---|---|---|---|
| P0 | Daily Treasury Statement | TGA余额、实际现金流 | 每个工作日 |
| P0 | Treasury Auction Data | 国债发行和偿还 | 按公告更新 |
| P0 | Quarterly Refunding | 财政部融资计划 | 每季度 |
| P1 | Monthly Treasury Statement | 月度收入、支出和赤字 | 每月 |
| P1 | Monthly Statement of Public Debt | 国债余额及结构 | 每月 |
| P1 | Treasury Cash Forecasting | 财政部现金管理信息 | 不定期 |
| P2 | IRS 官方税收日历 | 未来税收日期 | 按年度 |
| P2 | USAspending.gov | 政府支出结构及历史信息 | 按数据集更新 |

必须保存每条数据的：

- source_name
- source_url
- fetch_time
- record_date
- publication_date
- original_value
- original_unit
- normalized_value
- data_status

---

## 3.2 Daily Treasury Statement（核心数据源）

官方数据集：

Daily Treasury Statement（DTS）

API 基础地址：

https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/dts/

需要重点采集以下数据表。

### Table I：Operating Cash Balance

Endpoint：

```text
operating_cash_balance
```

主要用途：

获得每日财政部运营现金余额。

重点字段：

- record_date
- account_type
- open_today_bal
- close_today_bal
- open_month_bal

要求：

1. 获取完整历史数据。
2. 正确识别 TGA 对应的账户类型。
3. 不能直接假设所有 Operating Cash 都等于 TGA。
4. 对账户类型进行分类映射。
5. 使用每日收盘余额作为 TGA 历史序列的主要观测值。
6. 使用次日开盘余额进行连续性校验。

### Table II：Deposits and Withdrawals of Operating Cash

Endpoint：

```text
deposits_withdrawals_operating_cash
```

主要用途：

获得每日现金收入和支出明细。

重点研究：

- Income Tax
- Corporation Income Taxes
- Employment Taxes
- Excise Taxes
- Customs Duties
- Other Taxes
- Individual Income Tax Refunds
- Social Security
- Medicare
- Other Government Payments
- Interest Payments
- Other Deposits
- Other Withdrawals

具体字段必须通过官方数据集元数据确认。

Agent 不得凭经验硬编码字段名称或交易分类。

要求：

1. 下载完整历史数据。
2. 建立交易类型映射表。
3. 将所有交易分类为收入、支出、内部转移和待识别项目。
4. 保留原始分类名称。
5. 不得把无法识别的交易直接丢弃。

### Table IIIA：Public Debt Transactions

Endpoint：

```text
public_debt_transactions
```

主要用途：

获得国债发行和偿还的历史记录。

重点研究：

- Issues
- Redemptions
- Marketable Securities
- Treasury Bills
- Treasury Notes
- Treasury Bonds
- Cash Management Bills

特别注意：

Table IIIA 的部分数据采用债务面值口径，而不是实际现金结算口径。

因此不能简单将 Table IIIA 的发行金额与 TGA 实际现金流相加。

必须同时研究 Table IIIB。

### Table IIIB：Adjustment of Public Debt Transactions to Cash Basis

Endpoint：

```text
adjustment_public_debt_transactions_cash_basis
```

用途：

将债务交易数据调整为现金口径。

要求：

1. 研究 Table IIIA 与 Table IIIB 的关系。
2. 建立债务发行和偿还的现金口径计算方法。
3. 与 DTS Table II 的实际现金变化进行交叉验证。
4. 避免国债发行现金收入被重复计算。

### Table IV：Inter-agency Tax Transfers

Endpoint：

```text
inter_agency_tax_transfers
```

用途：

研究政府机构之间的税收转移。

要求：

判断这些数据是否已经包含在其他现金流统计中。

如果属于 TGA 内部转移或已在其他项目反映，不得重复计入净现金流。

### Table V：Income Tax Refunds Issued

Endpoint：

```text
income_tax_refunds_issued
```

用途：

分析个人所得税退税。

要求：

1. 区分支票退税和 EFT 退税。
2. 分析退税的季节性。
3. 与 Table II 相关支出项目交叉验证。
4. 防止重复计算。

### 数据接口示例

```bash
curl -G \
'https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/dts/operating_cash_balance' \
--data-urlencode 'page[size]=100' \
--data-urlencode 'sort=record_date' \
--data-urlencode 'format=json'
```

完整数据必须实现分页获取。

不能只获取最近100条记录。

---

## 3.3 Treasury Auction Data

官方入口：

https://www.treasurydirect.gov/auctions/announcements-data-results/

主要采集：

| 字段 | 说明 |
|---|---|
| security_type | 国债类型 |
| security_term | 国债期限 |
| announcement_date | 公告日期 |
| auction_date | 拍卖日期 |
| issue_date | 发行结算日期 |
| maturity_date | 到期日期 |
| offering_amount | 发行规模 |
| total_accepted | 实际接受金额 |
| total_tendered | 投标金额 |
| cusip | 国债识别码 |

要求：

1. 获取历史国债拍卖数据。
2. 获取未来已经公布的拍卖计划。
3. 区分公告计划和最终拍卖结果。
4. 区分原始发行与再发行。
5. 记录发行结算日期。
6. 记录到期偿还日期。
7. 通过 CUSIP 关联发行与到期偿还。

特别要求：

必须区分 Gross Issuance 和 Net Issuance。

净融资：

\[
NetIssuance=GrossIssuance-Redemptions
\]

但在实际 TGA 预测中，还需要考虑发行价格、折价、溢价以及结算现金金额。

不能直接使用债券面值替代现金流。

---

## 3.4 Quarterly Refunding

官方入口：

https://home.treasury.gov/policy-issues/financing-the-government/quarterly-refunding

每季度采集：

- Quarterly Refunding Statement
- Treasury Borrowing Estimates
- Financing Requirements
- Treasury Marketable Borrowing Estimates
- Treasury Bill Issuance Plans
- Treasury Coupon Issuance Plans

建立独立数据表：

```text
treasury_borrowing_estimates
```

字段建议：

- publication_date
- forecast_period
- borrowing_estimate
- cash_balance_assumption
- security_type
- maturity_bucket
- source_url

要求：

1. 保存每次公告的历史版本。
2. 不得用最新公告覆盖历史预测。
3. 保留每个公告当时的预测值。
4. 评估财政部预测与实际融资之间的偏差。

这是后续研究财政部现金管理行为的重要数据。

---

## 3.5 Monthly Treasury Statement

官方入口：

https://fiscaldata.treasury.gov/datasets/monthly-treasury-statement/

主要采集：

- Total Receipts
- Individual Income Taxes
- Corporation Income Taxes
- Social Insurance and Retirement Receipts
- Customs Duties
- Total Outlays
- Net Interest
- Social Security
- Medicare
- National Defense
- Other Outlays
- Budget Deficit or Surplus

用途：

1. 对 DTS 日频数据进行月度校验。
2. 研究财政收入和支出的季节性。
3. 建立未来政府支出的预测基准。
4. 研究财政赤字的中长期变化。

注意：

MTS 是预算报告口径，DTS 是修改后的现金收付口径。

两者不能未经调整直接相减。

---

# 第四章 数据库设计

建议使用 PostgreSQL。

如果现有项目已经使用 SQLite，也可以先使用 SQLite 实现 MVP，但必须采用规范化的数据结构。

建议数据库包含以下表。

## 4.1 原始数据表

### raw_dts

存储 DTS 原始记录。

字段：

```text
id
table_name
record_date
raw_json
source_url
fetch_time
publication_time
```

### raw_auction

存储国债拍卖原始数据。

```text
id
cusip
security_type
announcement_date
auction_date
issue_date
maturity_date
raw_json
source_url
fetch_time
```

### raw_refunding

存储财政部融资计划公告。

```text
id
publication_date
forecast_period
document_type
raw_content
source_url
fetch_time
```

## 4.2 标准化数据表

### daily_tga_balance

```text
date
opening_balance
closing_balance
daily_change
source
quality_status
```

### daily_cash_flows

```text
id
date
category
subcategory
direction
amount_usd_millions
source_table
source_record_id
is_internal_transfer
is_duplicate_candidate
confidence
```

### treasury_security_cashflows

```text
id
cusip
security_type
event_type
event_date
face_value
cash_amount
amount_status
source
```

event_type：

- issuance
- maturity
- coupon
- reopening
- cash_management_bill

## 4.3 预测数据表

### forecast_runs

```text
id
forecast_created_at
data_cutoff_date
model_version
forecast_horizon
scenario
```

### forecast_cash_flows

```text
id
forecast_run_id
forecast_date
category
predicted_amount
lower_bound
upper_bound
forecast_method
confidence
```

### forecast_tga

```text
id
forecast_run_id
forecast_date
predicted_tga
lower_bound
upper_bound
scenario
```

### forecast_evaluation

```text
id
forecast_run_id
target_date
forecast_horizon
predicted_tga
actual_tga
absolute_error
percentage_error
model_version
```

重要要求：

所有预测必须保留预测时的数据版本。

不能在历史回测中使用未来公布的数据。

---

# 第五章 历史现金流重建

这是整个项目最重要的基础工作。

在开始预测之前，必须先证明能够正确解释历史 TGA 余额变化。

## 5.1 基础现金流恒等式

理论上：

\[
TGA_t=TGA_{t-1}+Inflows_t-Outflows_t
\]

因此：

\[
\Delta TGA_t=TGA_t-TGA_{t-1}
\]

建立每日现金流重建模型：

\[
\widehat{\Delta TGA_t}
=
\sum Inflows_t-\sum Outflows_t
\]

定义：

\[
Residual_t=
\Delta TGA_t-\widehat{\Delta TGA_t}
\]

Residual 是现金流重建误差。

## 5.2 必须解决的问题

DTS 是修改后的现金收付口径。

需要特别注意：

1. 周末和联邦假日的交易可能在下一个工作日记录。
2. 部分现金流项目属于内部转移。
3. Table IIIA 可能采用面值口径。
4. Table IIIB 提供现金口径调整。
5. 某些交易可能在多个表中出现。
6. 发行、到期、利息支付可能存在不同的记录方式。
7. TT&L 账户与 TGA 之间的资金转移可能影响现金账户的观察口径。

因此，Agent 必须先研究 DTS 数据字典和表间关系。

禁止简单将所有表格的金额求和。

## 5.3 重建质量评价

建立以下指标：

| 指标 | 作用 |
|---|---|
| Daily Residual | 每日重建误差 |
| Mean Absolute Residual | 平均绝对误差 |
| Residual Percentage | 相对误差 |
| Monthly Reconciliation | 月度累计误差 |
| Unclassified Cash Flow | 未分类现金流 |
| Duplicate Cash Flow | 重复现金流 |

验收要求：

1. 每日 TGA 余额能够正确获取。
2. 每日变化量能够计算。
3. 现金流分类完整。
4. 所有未解释的余额变化都有记录。
5. 不允许通过人为调整数据掩盖残差。

第一阶段的目标是实现现金流重建，而不是追求预测精度。

---

# 第六章 预测模型设计

采用分层预测架构。

不要一开始就使用复杂机器学习模型。

首先建立可解释的现金流预测模型，再逐步增加统计模型。

## 6.1 模型总体结构

未来 TGA 余额：

\[
TGA_{t+n}=TGA_t+
\sum_{i=1}^{n}
(R_i+I_i-D_i-O_i)
\]

其中：

- R：税收及其他财政收入。
- I：国债发行现金收入。
- D：国债到期本金偿还。
- O：政府其他现金支出。

如果现金流已经包含国债发行、到期和利息支付，就不能再次重复计入。

建议将预测分为以下模块：

```text
TGA Forecast Engine
│
├── Tax Revenue Forecast
│
├── Treasury Issuance Forecast
│
├── Treasury Redemption Forecast
│
├── Government Outlays Forecast
│
├── Interest Payment Forecast
│
├── Other Cash Flows Forecast
│
├── Cash Flow Reconciliation
│
└── TGA Balance Aggregation
```

---

## 6.2 税收收入预测模型

税收是 TGA 的重要现金流入来源。

主要研究：

- Individual Income Taxes
- Corporate Income Taxes
- Employment Taxes
- Excise Taxes
- Customs Duties
- Other Receipts

### A. 季节性分析

收集至少10年的历史日频数据。

计算：

- 每月税收收入。
- 每周税收收入。
- 每个星期几的平均税收收入。
- 每月特定日期的收入分布。
- 重要税收截止日期附近的收入。
- 财年末和季度末收入变化。

重点关注：

| 时间 | 主要事件 |
|---|---|
| 1月 | 季度预缴税款 |
| 4月 | 个人所得税集中缴纳 |
| 6月 | 季度预缴税款 |
| 9月 | 季度预缴税款 |
| 12月 | 季度预缴税款 |

具体日期应通过 IRS 每年度官方日历确认。

### B. 历史同期预测

最简单的基准模型：

\[
TaxForecast_{t}
=
Median(TaxFlow_{historical\ comparable\ dates})
\]

需要考虑：

- 工作日差异。
- 联邦假日。
- 税期变化。
- 重大税收政策变化。
- 经济规模变化。
- 退税因素。

不能机械地使用去年同一天的数据。

### C. 滚动预测

建立：

- 7天税收预测。
- 14天税收预测。
- 30天税收预测。
- 90天税收预测。

短期预测优先使用税收日历和历史同期数据。

中期预测可以使用 MTS 月度收入趋势和历史季节性模型。

---

## 6.3 国债发行预测模型

国债发行是财政部主动调节 TGA 的主要工具之一。

建立三个预测层次。

### 第一层：已经公布的发行计划

对于已经公布的国债发行计划：

使用最终拍卖结果和 Issue Date 预测实际现金流。

未完成拍卖的，使用公告发行规模作为暂定值。

### 第二层：已知但尚未公布最终规模的发行

根据：

- Quarterly Refunding。
- 历史发行规律。
- 近期国债发行节奏。
- 财政部融资需求。
- 财政部短期现金余额。

估计未来发行规模。

### 第三层：未公布发行计划

建立历史统计模型。

根据不同期限国债的发行周期估算。

例如：

| 国债类型 | 预测方法 |
|---|---|
| 4周 Bills | 历史发行频率 |
| 8周 Bills | 历史发行频率 |
| 13周 Bills | 历史发行频率 |
| 17周 Bills | 历史发行频率 |
| 26周 Bills | 历史发行频率 |
| 52周 Bills | 历史发行频率 |
| Notes | 历史再融资周期 |
| Bonds | 历史再融资周期 |
| CMB | 特殊现金需求情景 |

具体期限和发行周期应以实际公告为准。

### 特别要求

必须区分：

- New Money：新增融资。
- Rollover：到期再融资。
- Cash Management Bills：临时现金管理融资。

不能将国债总发行金额直接理解为 TGA 增加金额。

---

## 6.4 国债到期偿还预测

这是相对容易预测的现金流项目。

对于已经发行的国债：

可以通过 CUSIP 和 Maturity Date 确定到期时间。

建立未来到期偿还日历。

计算：

\[
Redemption_t=
\sum_{j\in Maturity_t}Principal_j
\]

需要区分：

- Treasury Bills。
- Treasury Notes。
- Treasury Bonds。
- TIPS。
- Floating Rate Notes。
- Savings Securities。

要求：

1. 记录到期日期。
2. 记录到期本金。
3. 识别是否存在再发行。
4. 预测偿还现金流。
5. 与 DTS 实际数据进行核对。

到期偿还属于现金流出。

但如果同日有新债发行，必须分别记录发行和偿还，不能只观察净发行金额。

---

## 6.5 国债利息支付预测

利息支付也是政府现金流出。

需要建立：

- CUSIP。
- Coupon Rate。
- Payment Frequency。
- Interest Payment Date。
- Principal Outstanding。
- Expected Coupon Payment。

计算：

\[
CouponPayment=
Principal\times CouponRate\times TimeFraction
\]

对于普通固定利率国债，可以根据付息日期和票面利率估算。

对于 TIPS、FRN 等特殊债券，需要分别实现对应计算逻辑。

预测时还需要区分：

- 国债利息支出。
- 国债本金偿还。

避免将利息和本金混淆。

---

## 6.6 政府支出预测模型

这是整个模型中不确定性相对较高的部分。

建议按照政府支出类型进行分类。

| 支出类别 | 预测方法 | 主要数据来源 |
|---|---|---|
| Social Security | 历史支付周期 | DTS |
| Medicare | 历史周期及金额 | DTS、MTS |
| Medicaid | 历史统计 | DTS、MTS |
| National Defense | 历史支付规律 | DTS、MTS |
| Federal Employee Compensation | 工作日和支付周期 | DTS |
| Interest Payments | 国债付息日历 | TreasuryDirect |
| Other Outlays | 历史统计 | DTS、MTS |

### A. 固定周期支出

对于有明显支付规律的项目：

使用历史支付日历进行预测。

例如：

- 每月固定日期支付。
- 每两周支付。
- 每月月初或月末支付。

建立支付周期识别算法。

### B. 不规则支出

对于无法确定具体付款日期的项目：

使用历史统计模型。

建议比较：

1. 过去4周移动平均。
2. 过去13周移动平均。
3. 去年同期平均。
4. 历史同类月份中位数。
5. 季节性模型。

### C. 预测限制

不得将预算拨款金额直接视为现金支出。

预算金额与实际现金支付不是同一概念。

USAspending.gov 数据也不能直接替代 DTS 的现金流数据。

它可以辅助分析支出结构，但不能未经验证直接加入 TGA 余额计算。

---

## 6.7 其他现金流预测

建立独立模块：

Other Cash Flows Forecast。

包括：

- Other Deposits。
- Other Withdrawals。
- Refunds。
- Inter-agency Transfers。
- Government Account Series 相关交易。
- 特殊财政操作。

对于无法解释的现金流，采用：

1. 历史均值。
2. 历史同期中位数。
3. 单独情景假设。

必须标记为低置信度。

---

# 第七章 财政部现金管理行为模型

这一部分是本项目的进阶研究内容。

财政部并不是被动接受收入和支出。

它会根据现金预测主动调整国债发行计划，以维持合理的现金储备。

因此，除了预测现金流，还需要研究财政部的现金管理目标。

## 7.1 TGA 目标余额

建立：

```text
treasury_cash_target
```

字段：

- publication_date
- target_period
- target_balance
- target_type
- source
- confidence

重点收集财政部明确公布的现金余额假设。

需要注意：

Quarterly Refunding 中的现金余额假设不一定等于实际每日 TGA 目标。

两者不能混为一谈。

如果财政部没有公布明确的目标余额：

使用历史余额分布建立参考区间。

不能把历史平均值伪装成财政部正式目标。

## 7.2 融资缺口计算

假设：

当前 TGA：

\[
TGA_0
\]

未来预计：

- 税收收入：R
- 政府支出：O
- 国债到期偿还：D
- 国债发行：I

则：

\[
TGA_{forecast}=TGA_0+R+I-D-O
\]

如果财政部希望达到目标余额：

\[
FinancingGap=
TGA_{target}-TGA_{forecast}
\]

如果 FinancingGap > 0：

意味着在其他条件不变时，需要额外融资或减少现金流出。

如果 FinancingGap < 0：

意味着预计现金余额高于目标。

但是：

**融资缺口不等于财政部一定会按照同等金额增加国债发行。**

财政部还可能调整：

- 短期国债发行规模。
- 长期国债发行规模。
- CMB 发行计划。
- 现金管理节奏。
- 其他现金操作。

因此，该模块应输出融资压力估计，而不是把融资缺口直接作为已确定发行金额。

## 7.3 财政部行为参数估计

通过历史数据研究：

1. TGA 余额低于什么水平时，财政部倾向于增加短期融资？
2. TGA 余额较高时，财政部是否倾向于减少短期融资？
3. 财政部在季度末是否存在特殊现金管理行为？
4. CMB 发行是否与短期现金缺口存在统计关系？
5. 财政部的季度融资预测与最终实际融资之间存在多大偏差？

建立历史回归模型：

\[
NetIssuance_t=
f(TGA_{t-1},CashFlowForecast_t,
DeficitForecast_t,Seasonality_t)
\]

注意：

该模型用于研究财政部历史行为，不代表可以完全预测财政部决策。

未来财政部的政策变化应作为独立情景处理。

---

# 第八章 模型架构与算法选择

## 8.1 模型开发顺序

必须按照以下顺序实施。

### Model 0：历史现金流重建

目标：

解释已经发生的 TGA 变化。

不允许跳过。

### Model 1：历史季节性基准模型

使用：

- 去年同期。
- 历史同期中位数。
- 星期效应。
- 月份效应。
- 税收季节性。
- 政府支付周期。

作为基准模型。

### Model 2：事件驱动现金流模型

使用：

- 国债发行日历。
- 国债到期日历。
- 利息支付日历。
- IRS 税收日历。
- 政府固定支付日历。

这是主要预测模型。

### Model 3：统计预测模型

可以尝试：

- SARIMA。
- ETS。
- Prophet。
- LightGBM。
- XGBoost。

主要用于预测尚未公布的现金流。

不要使用机器学习模型替代已知的确定性现金流。

### Model 4：集成预测模型

综合：

- 确定性事件现金流。
- 历史季节性预测。
- 统计模型预测。
- 财政部融资计划。
- 情景假设。

形成最终预测结果。

## 8.2 推荐的预测结构

```text
Observed TGA Balance
        |
        v
Historical Cash Flow Reconstruction
        |
        v
Known Future Cash Flows
        |
        +-------------------+
        |                   |
        v                   v
Tax Forecast          Treasury Forecast
        |                   |
        v                   v
Government Outlays    Debt Redemptions
        |                   |
        +---------+---------+
                  |
                  v
          Cash Flow Aggregator
                  |
                  v
          TGA Forecast Engine
                  |
          +-------+-------+
          |       |       |
          v       v       v
         7D      30D     90D
          |
          v
    Scenario Analysis
          |
          v
    Historical Backtesting
          |
          v
      Daily Report
```

---

# 第九章 预测情景设计

至少建立三个情景。

## 9.1 Base Case

基准情景。

使用：

- 已公布国债发行计划。
- 历史季节性税收预测。
- 历史政府支出规律。
- 已知到期偿还计划。
- 当前财政部融资估计。

## 9.2 Bullish Liquidity Scenario

美元流动性相对宽松情景。

假设：

- 税收收入低于基准预测。
- 政府支出高于基准预测。
- 国债净融资低于基准预测。
- 财政部现金余额出现下降。

观察 TGA 对银行准备金和流动性的潜在影响。

## 9.3 Bearish Liquidity Scenario

美元流动性相对收紧情景。

假设：

- 税收收入高于基准预测。
- 政府支出低于基准预测。
- 国债净融资高于基准预测。
- 财政部现金余额出现上升。

观察 TGA 对银行准备金和流动性的潜在影响。

注意：

Bullish 和 Bearish 是流动性情景标签，不是比特币价格预测。

## 9.4 情景参数

所有情景参数必须可配置。

例如：

```yaml
scenarios:
  base:
    tax_adjustment: 0.00
    outlay_adjustment: 0.00
    issuance_adjustment: 0.00

  liquidity_easing:
    tax_adjustment: -0.05
    outlay_adjustment: 0.05
    issuance_adjustment: -0.05

  liquidity_tightening:
    tax_adjustment: 0.05
    outlay_adjustment: -0.05
    issuance_adjustment: 0.05
```

上述数值仅用于初始测试，不代表真实财政预测。

---

# 第十章 历史回测体系

回测是判断模型是否真正有效的关键。

不能只展示模型拟合历史数据的效果。

必须模拟真实预测过程。

## 10.1 回测方式

采用 Walk-forward Backtesting。

例如：

在2022年1月1日：

只允许使用当时已经公布的数据，预测未来7天、14天和30天。

然后：

- 获取实际 TGA。
- 计算预测误差。
- 保存预测结果。

接着将预测起点推进一天，重复以上操作。

一直持续到最新历史日期。

## 10.2 防止未来数据泄漏

必须严格遵守：

假设预测生成时间为2024年5月1日。

只能使用截至2024年5月1日已经公布的数据。

不能使用：

- 2024年5月2日之后公布的拍卖结果。
- 之后修订的融资计划。
- 之后公布的实际税收数据。
- 未来的实际政府支出。

特别是财政部融资公告必须保存历史版本。

不能用当前最新的融资公告回测过去。

## 10.3 评价指标

至少计算：

### MAE

\[
MAE=\frac{1}{N}\sum |Forecast-Actual|
\]

### RMSE

\[
RMSE=
\sqrt{\frac{1}{N}
\sum(Forecast-Actual)^2}
\]

### MAPE

\[
MAPE=
\frac{100\%}{N}
\sum\left|
\frac{Forecast-Actual}{Actual}
\right|
\]

由于 TGA 余额通常不会接近零，MAPE 可以作为辅助指标，但不能作为唯一评价标准。

### Direction Accuracy

判断预测方向是否正确。

\[
DirectionAccuracy=
\frac{CorrectDirection}{TotalForecasts}
\]

还需要评估：

- 7天预测误差。
- 14天预测误差。
- 30天预测误差。
- 60天预测误差。
- 90天预测误差。

## 10.4 必须比较的基准模型

至少比较：

| 模型 | 说明 |
|---|---|
| Naive | 未来 TGA 保持不变 |
| Historical Mean | 历史平均现金流 |
| Seasonal Naive | 去年同期预测 |
| Event-driven | 确定性现金流预测 |
| Statistical | 统计模型预测 |
| Ensemble | 集成预测 |

只有当复杂模型在样本外回测中优于简单基准模型时，才允许将其设为默认模型。

## 10.5 预测误差归因

预测错误时，必须解释原因。

例如：

| 误差来源 | 说明 |
|---|---|
| Tax Error | 税收预测错误 |
| Issuance Error | 国债发行规模错误 |
| Redemption Error | 偿还金额错误 |
| Outlay Error | 政府支出错误 |
| Timing Error | 现金流发生日期错误 |
| Unclassified | 未识别现金流 |

建立误差归因报告。

目标不是简单提高模型精度，而是知道模型为什么预测错误。

---

# 第十一章 数据质量和异常监控

每日采集后必须执行质量检查。

## 11.1 数据完整性

检查：

- 是否成功获取最新 DTS。
- 是否存在日期缺失。
- 是否存在重复记录。
- 是否存在字段变化。
- 是否存在金额单位变化。
- 是否存在异常空值。

## 11.2 TGA 连续性

检查：

\[
DailyChange_t=
ClosingBalance_t-OpeningBalance_t
\]

同时检查：

\[
OpeningBalance_t
\approx ClosingBalance_{t-1}
\]

如果不一致：

记录异常，不允许静默修改。

## 11.3 数据来源交叉验证

建议：

- DTS 与 MTS 月度收入支出交叉验证。
- DTS 与 TreasuryDirect 国债发行数据交叉验证。
- DTS 与 Fed H.4.1 的 TGA 数据交叉验证。

注意不同数据源的统计日期、口径和发布时间可能不同。

## 11.4 异常处理原则

所有异常必须保留：

```text
anomaly_id
detected_at
data_source
record_date
anomaly_type
original_value
expected_value
severity
resolution_status
```

不允许自动删除异常数据。

---

# 第十二章 分析报告设计

系统每天生成一份 TGA Forecast Report。

建议采用 Markdown 格式。

文件命名：

```text
TGA_Forecast_YYYY-MM-DD.md
```

## 12.1 报告内容

### 一、当前 TGA 状况

| 指标 | 数值 |
|---|---:|
| 最新 TGA | xxx B |
| 最新日期 | YYYY-MM-DD |
| 前一交易日 | xxx B |
| 日变化 | xxx B |
| 过去7天变化 | xxx B |
| 过去30天变化 | xxx B |

### 二、未来 TGA 预测

| 日期 | 基准预测 | 宽松情景 | 收紧情景 |
|---|---:|---:|---:|
| T+7 | xxx B | xxx B | xxx B |
| T+14 | xxx B | xxx B | xxx B |
| T+30 | xxx B | xxx B | xxx B |
| T+60 | xxx B | xxx B | xxx B |
| T+90 | xxx B | xxx B | xxx B |

### 三、未来现金流预测

| 日期 | 收入 | 支出 | 净现金流 | 主要事件 |
|---|---:|---:|---:|---|
| Day 1 | | | | |
| Day 2 | | | | |
| Day 3 | | | | |

### 四、现金流构成

分别展示：

- 税收收入。
- 国债发行收入。
- 国债偿还。
- 政府福利支出。
- 国债利息。
- 其他政府支出。

### 五、重要事件

列出未来30天：

- 重要税收日期。
- 国债拍卖。
- 国债发行结算。
- 国债到期偿还。
- 重要利息支付。
- 季度末和月末。

### 六、预测置信度

分别给出：

- 已确定现金流比例。
- 统计预测现金流比例。
- 情景假设比例。
- 未识别现金流比例。

不能仅凭主观判断给出置信度。

应根据历史预测误差和现金流可识别程度计算。

### 七、模型回测表现

展示：

| 预测周期 | MAE | RMSE | 方向准确率 |
|---|---:|---:|---:|
| 7天 | | | |
| 14天 | | | |
| 30天 | | | |
| 60天 | | | |
| 90天 | | | |

### 八、主要结论

必须回答：

1. 未来30天 TGA 预计增加还是减少？
2. 主要驱动因素是什么？
3. 哪些日期可能出现大额现金流变化？
4. 预测结果中最大的风险是什么？
5. 当前预测与上一次预测相比发生了什么变化？

---

# 第十三章 与美元流动性模型的接口

本项目只负责预测 TGA。

但是必须为后续美元流动性模型提供标准化接口。

建议输出：

```json
{
  "forecast_date": "YYYY-MM-DD",
  "data_cutoff_date": "YYYY-MM-DD",
  "model_version": "1.0.0",
  "scenario": "base",
  "forecast": [
    {
      "date": "YYYY-MM-DD",
      "tga_predicted": 0,
      "tga_lower": 0,
      "tga_upper": 0,
      "daily_net_cashflow": 0
    }
  ]
}
```

后续将其与以下指标结合：

- Federal Reserve Total Assets。
- Reserve Balances with Federal Reserve Banks。
- ON RRP。
- SOFR。
- Treasury General Account。

建立简化流动性观察指标：

\[
L=Reserves+ONRRP-TGA
\]

注意：

该指标是分析工具，不是严格的全市场美元流动性定义。

TGA 预测值可以直接接入未来流动性预测模块。

但必须区分：

- 已经公布的实际数据。
- 模型预测的 TGA。
- 模型预测的银行准备金。
- 情景假设。

不得将预测的 TGA 直接等同于未来银行准备金变化。

---

# 第十四章 软件工程要求

## 14.1 推荐技术栈

| 模块 | 技术 |
|---|---|
| 开发语言 | Python 3.12+ |
| 数据采集 | requests / httpx |
| 数据处理 | pandas / polars |
| 数据库 | PostgreSQL 或 SQLite |
| 统计分析 | scipy / statsmodels |
| 机器学习 | scikit-learn / LightGBM |
| 可视化 | Plotly |
| 报告生成 | Markdown |
| 自动化 | systemd timer / cron |
| 测试 | pytest |

尽量使用成熟、稳定的 Python 库。

不要为了使用新技术而增加不必要的系统复杂度。

## 14.2 推荐项目结构

```text
tga-forecast/
│
├── config/
│   ├── settings.yaml
│   ├── data_sources.yaml
│   └── scenarios.yaml
│
├── data/
│   ├── raw/
│   ├── processed/
│   ├── database/
│   └── cache/
│
├── src/
│   ├── collectors/
│   │   ├── dts.py
│   │   ├── auction.py
│   │   ├── refunding.py
│   │   ├── mts.py
│   │   └── irs_calendar.py
│   │
│   ├── processors/
│   │   ├── normalize.py
│   │   ├── classify.py
│   │   ├── reconcile.py
│   │   └── validate.py
│   │
│   ├── models/
│   │   ├── tax_forecast.py
│   │   ├── issuance_forecast.py
│   │   ├── redemption_forecast.py
│   │   ├── outlay_forecast.py
│   │   ├── interest_forecast.py
│   │   ├── cashflow_forecast.py
│   │   └── tga_forecast.py
│   │
│   ├── backtesting/
│   │   ├── walk_forward.py
│   │   ├── metrics.py
│   │   └── attribution.py
│   │
│   ├── reporting/
│   │   ├── daily_report.py
│   │   └── charts.py
│   │
│   ├── database/
│   │   ├── schema.py
│   │   └── repository.py
│   │
│   └── main.py
│
├── tests/
│   ├── test_collectors.py
│   ├── test_normalization.py
│   ├── test_reconciliation.py
│   ├── test_forecast.py
│   ├── test_backtesting.py
│   └── test_data_quality.py
│
├── reports/
│   ├── daily/
│   └── backtesting/
│
├── scripts/
│   ├── download_history.py
│   ├── update_data.py
│   ├── run_forecast.py
│   └── run_backtest.py
│
├── docs/
│   ├── DATA_DICTIONARY.md
│   ├── MODEL_METHODOLOGY.md
│   └── OPERATIONS.md
│
├── requirements.txt
├── pyproject.toml
└── README.md
```

实际目录可以根据现有项目结构调整。

---

# 第十五章 开发实施阶段

Agent 必须分阶段开发。

禁止一次性生成所有代码后直接宣称完成。

## Phase 1：数据源研究与采集

任务：

1. 阅读 DTS 官方数据字典。
2. 确认 API Endpoint。
3. 确认字段名称和单位。
4. 下载历史数据。
5. 实现自动分页。
6. 建立原始数据存储。
7. 实现增量更新。
8. 建立数据采集日志。

验收：

- 能获取最新 DTS。
- 能获取至少10年历史数据。
- 能获取国债拍卖历史数据。
- 能获取未来已公布发行计划。
- 数据来源和发布时间可追溯。

如果某些数据源不支持10年历史数据，必须明确记录其实际可用范围，不得伪造历史记录。

## Phase 2：数据清洗与历史现金流重建

任务：

1. 标准化日期。
2. 标准化金额单位。
3. 识别 TGA 账户。
4. 分类收入和支出。
5. 建立债务交易现金口径。
6. 处理重复记录。
7. 建立每日现金流表。
8. 计算每日重建误差。
9. 建立月度交叉验证。

验收：

能够通过历史现金流解释 TGA 的每日余额变化。

对于不能解释的部分，必须保留残差。

## Phase 3：建立基准预测模型

任务：

1. 建立税收季节性模型。
2. 建立固定周期政府支出模型。
3. 建立国债发行日历。
4. 建立国债到期偿还日历。
5. 建立国债利息支付日历。
6. 建立未来30天现金流预测。
7. 生成未来30天 TGA 预测。

验收：

能够输出未来7、14、30天的预测余额。

## Phase 4：扩展中期预测

任务：

1. 加入 MTS 数据。
2. 加入季度融资计划。
3. 加入财政部现金余额假设。
4. 建立未来60天和90天预测。
5. 加入多情景预测。
6. 建立现金管理行为分析。

验收：

能够输出未来7、14、30、60、90天预测。

## Phase 5：历史回测

任务：

1. 实现 Walk-forward Backtesting。
2. 建立历史预测数据快照。
3. 防止未来数据泄漏。
4. 比较基准模型。
5. 计算预测误差。
6. 进行预测误差归因。
7. 生成回测报告。

验收：

所有预测结果都能追溯到当时可获得的数据。

能够证明模型是否优于简单基准模型。

## Phase 6：自动化和报告

任务：

1. 建立每日数据采集任务。
2. 建立数据质量检查。
3. 建立每日预测任务。
4. 生成 Markdown 报告。
5. 生成预测走势图。
6. 保存历史预测版本。
7. 记录异常并发送通知。

验收：

系统能够自动运行，不依赖人工每天手动下载数据。

---

# 第十六章 测试要求

## 16.1 单元测试

至少覆盖：

- DTS API 数据解析。
- 分页获取。
- 金额单位转换。
- 日期处理。
- 现金流分类。
- 国债发行现金流计算。
- 到期偿还计算。
- TGA 余额计算。
- 预测区间计算。
- 回测指标计算。

## 16.2 集成测试

测试完整流程：

```text
官方数据
   |
   v
数据采集
   |
   v
数据清洗
   |
   v
现金流重建
   |
   v
未来现金流预测
   |
   v
TGA 余额预测
   |
   v
历史回测
   |
   v
报告生成
```

## 16.3 边界测试

重点测试：

1. 周末。
2. 美国联邦假日。
3. 季度末。
4. 财年末。
5. 4月税收集中期。
6. 国债发行和到期集中日期。
7. DTS 数据延迟。
8. API 数据缺失。
9. 数据修订。
10. 国债发行计划临时调整。

## 16.4 最终验收标准

| 项目 | 验收要求 |
|---|---|
| 数据采集 | 自动化、可追溯 |
| 历史数据 | 尽可能覆盖10年 |
| 数据完整性 | 有完整性检查 |
| 现金流重建 | 能解释每日 TGA 变化 |
| 预测周期 | 7/14/30/60/90天 |
| 预测方法 | 可解释 |
| 历史回测 | Walk-forward |
| 数据泄漏 | 不允许 |
| 预测误差 | 有完整统计 |
| 情景分析 | 至少3种 |
| 自动化 | 定时运行 |
| 文档 | 完整 |

预测精度不预设人为目标值。

必须先建立基准模型，获得实际样本外误差，再根据研究结果判断是否达到可用水平。

---

# 第十七章 Agent 执行纪律

这是本项目特别重要的开发要求。

Agent 必须遵循以下规则。

## 17.1 先研究，后编码

在开始编码前：

1. 检查项目现有目录。
2. 检查现有数据库。
3. 阅读官方 API 文档。
4. 确认实际字段。
5. 确认历史数据范围。
6. 输出技术设计方案。

不得根据猜测编造 API 字段。

## 17.2 分阶段提交

每个 Phase 完成后：

1. 运行全部相关测试。
2. 检查数据库。
3. 检查数据质量。
4. 更新文档。
5. 提交 Git。
6. 输出阶段完成报告。

未通过验收不得进入下一阶段。

## 17.3 不允许的行为

- 不允许使用未经验证的第三方数据替代官方数据。
- 不允许把债务面值直接当作现金流。
- 不允许重复计算国债发行或偿还。
- 不允许重复计算跨表现金流。
- 不允许把 MTS 预算口径与 DTS 现金口径混用。
- 不允许使用未来数据进行历史回测。
- 不允许静默删除异常数据。
- 不允许将模型估计伪装成官方预测。
- 不允许只生成图表而没有可验证的数据。
- 不允许只展示预测结果而不解释现金流构成。

## 17.4 每个阶段必须提交的报告

```text
Phase Report
├── Completed Tasks
├── Data Sources
├── Data Coverage
├── Database Changes
├── Model Changes
├── Tests Executed
├── Test Results
├── Known Issues
├── Remaining Work
└── Next Phase Plan
```

---

# 第十八章 项目最终目标

本项目最终应形成一套持续运行的 TGA 前瞻预测系统。

系统每天自动回答：

1. 当前 TGA 余额是多少？
2. 过去一天 TGA 为什么发生变化？
3. 未来7天 TGA 预计如何变化？
4. 未来30天 TGA 预计如何变化？
5. 未来90天 TGA 预计如何变化？
6. 哪些税收、支出和国债事件是主要驱动因素？
7. 财政部是否可能需要增加短期融资？
8. 当前预测的不确定性主要来自哪里？
9. 模型最近一个月的预测准确度如何？

最终形成：

**官方数据采集 → 历史现金流重建 → 未来现金流预测 → TGA 余额预测 → 历史回测 → 每日分析报告。**

这套系统的核心价值不是预测一个 TGA 数字，而是建立对美国财政部现金流运行机制的量化理解。

只有能够解释 TGA 为什么变化，并且能够证明历史预测具有样本外有效性，才能将其作为美元流动性分析系统的可靠组成部分。
