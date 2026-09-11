# 架构审计记录 — 第 2 轮

- 时间：2026-09-10
- 参与者：PM、DM、AR
- 对象：第 1 轮修改后的 01–10 文档 + 上轮遗留项
- 重点：上轮遗留闭环（QueryService 契约、fixture 选型）+ 修改引入的新问题 + 深层细节

## 1. PM 审计意见

| # | 问题 | 严重度 | 涉及 |
|---|---|---|---|
| P1 | 遗留项未闭环：QueryService 仍无函数签名级契约（参数、返回、错误语义），AI Agent/研究者的调用方式仍是黑盒 | HIGH | 02/05 |
| P2 | FEATURE 依赖多源（需求 §27 的 BTC_MARKET_STRESS = 波动率+资金费+OI+爆仓+IV+预测概率），但 03 只定义了单记录 provenance，Feature 级"依赖哪些 dataset 哪个版本"未建模——研究结果无法解释"这个特征基于什么算出来的" | HIGH | 03 |
| P3 | 文本类源（GDELT/RSS）raw 全文"永久保留"（04 §6）可能不可行：全文量级未验证，无容量估算，retention 决策缺依据 | MEDIUM | 04 |

## 2. DM 审计意见

| # | 问题 | 严重度 | 涉及 |
|---|---|---|---|
| D1 | **07 import-linter 层级配置错误**（上轮文档自带，本轮复审发现）：`models` 排在 `connectors` 之上，意味着 models 可 import connectors——直接违反"models 零依赖"规则；connectors/storage 应位于 models 之上 | HIGH | 07 |
| D2 | schema_versions 机制混淆了两类迁移：SQLite 元数据 DDL 迁移（轻量、必须工具化）vs Parquet 数据 schema 迁移（= 新版本分区 + replay 重写），两者机制完全不同但未区分 | MEDIUM | 04 |
| D3 | run_log（SQLite）与数据 rename（文件系统）的跨存储一致性未定义：rename 成功但 run_log 写失败（或反之）时状态如何恢复 | MEDIUM | 08 |
| D4 | ingest_time 依赖本地时钟，NTP 回拨破坏单调性，影响延迟统计可信度 | LOW | 08 |
| D5 | 遗留项：fixture 录制工具未选型（上轮 D7） | LOW | 06/07 |

## 3. 三方讨论纪要

1. **P1（QueryService 契约）**：PM 出示 AI Agent 场景——Agent 需要程序化取数，接口含糊直接阻塞下游。AR 给出 `query(dataset_id, columns, start/end, asof, filters)` 签名，`asof` 为防 look-ahead 的点时视图参数（与 04 上轮新增的 as-of 查询机制对齐），返回携带 dataset_version/schema_version 元信息。DM 确认与存储方案无耦合。**三方一致通过，本轮闭环**。
2. **P2（Feature 依赖声明）**：AR 提议 DERIVED/FEATURE 记录增加 `dependencies: list[(dataset_id, dataset_version)]`；PM 追问"能否支持需求 §45 的可追溯闭环"，DM 确认 dependencies + provenance 双层即可覆盖"Feature→Canonical→Raw→Source"全链。**接受**。
3. **P3（文本容量）**：DM 指出第一阶段 P0 不含 GDELT/RSS（P2 级），量级评估属引入前置条件而非当期阻塞。决议：04 补充"文本类 retention = UNKNOWN"（约束 §79：Unknown 不是 False，不猜），引入前必须补 ADR。**部分接受（记录为约束而非估算数字）**。
4. **D1（import-linter 错误）**：AR 承认笔误，属"修改引入"类问题，教训记录：架构测试配置本身也要被 review。修正：`quality|registry|connectors|storage` 同层 → `config` → `models`（最底层零依赖）。**接受**。
5. **D2（迁移机制分离）**：SQLite=手写迁移函数按序号执行（表少，不值得引 alembic，约束 §44 boring tech）；Parquet=新 schema_version 分区 + replay 重写，不写通用工具。**接受**。
6. **D3/D4（一致性与时钟）**：AR 提出以数据 rename 为完成边界：rename 前失败→重放即恢复；rename 后 run_log 失败→按 ingest_batch_id 对账补记。D4：延迟分析以源 event_time 为准，ingest_time 不假设单调。**接受**。
7. **D5（fixture）**：手工保存的真实响应样例文件 + RawBatch 抽象解耦 fetch，不引入 vcr.py（少一个依赖，且录制回放与 connector 协议耦合）。**接受，决策入 06 决策表（D12）**。

## 4. 决议汇总

接受：P1、P2、D1–D5；部分接受：P3（记录为 UNKNOWN 约束 + ADR 前置条件）。

## 5. 修改结果（已执行）

| 项 | 文件 | 修改 |
|---|---|---|
| P1 | 02 | 新增 §5 QueryService 接口（函数签名 + asof 语义 + QueryResult 元信息） |
| P2 | 03 | 新增 §7 Derived/Feature 依赖声明（dependencies 清单） |
| P3 | 04 | 新增 §8 文本类 retention = UNKNOWN，引入前补 ADR |
| D1 | 07 | 修正 import-linter layers：connectors/storage 移至 models 之上，config 次底层，models 最底层 |
| D2 | 04 | 新增 §7 Schema 迁移机制分离（SQLite DDL 迁移 vs Parquet replay 重写）；schema_versions 表说明同步修正 |
| D3/D4 | 08 | §5 新增 3 行失败场景：跨存储对账、rename 前失败恢复、时钟回拨 |
| D5 | 06 | 决策表新增 D11（手写迁移函数）、D12（手工 fixture 样例）及依据 |
| D5 | 07 | fixture 策略改为手工样例，不引入 vcr.py（引用 06 D12） |

## 6. 遗留（转入第 3 轮）

- 研究可复现性落地机制（约束 §23/§52：研究结果如何绑定 dataset/code version）——PM 本轮提出，讨论认为属 research 层职责，与 P2 dependencies 相关但独立，第 3 轮处理
- 全文档交叉引用一致性终检（第 3 轮验证性审计内容）
