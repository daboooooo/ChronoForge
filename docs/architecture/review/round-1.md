# 架构审计记录 — 第 1 轮

- 时间：2026-09-10
- 参与者：产品经理（PM）、研发经理（DM）、架构师（AR）
- 对象：docs/architecture/ 01–10 全部文档
- 方式：PM/DM 独立审计 → 三方讨论 → 形成修改方案并执行

## 1. PM 审计意见

| # | 问题 | 严重度 | 涉及 |
|---|---|---|---|
| P1 | 需求 §45 四条核心原则（新增源不破坏模型等）只是陈述，无可验证的验收标准，交付时无法客观判定"做到没有" | HIGH | 01/07/10 |
| P2 | AI Agent 作为用户角色仅在 01 提及，查询能力只有一句 `QueryService`，无接口契约，产品可用性边界不清 | MEDIUM | 02/05 |
| P3 | 约束 §25 要求多环境配置（default/development/test/production），config 模块未体现 | MEDIUM | 02 |
| P4 | P0 交付清单有了，但每个 connector 缺"完成定义（DoD）"，无法管理交付节奏 | LOW | 10 |

## 2. DM 审计意见

| # | 问题 | 严重度 | 涉及 |
|---|---|---|---|
| D1 | "natural key upsert" 机制未定义：Parquet 不支持原地更新，读-改-写方案与写放大未说明，是当前最大实现风险 | HIGH | 04 |
| D2 | SQLite 单写者特性下的并发约束未声明：两个 pipeline run 并发执行同一 dataset 会锁冲突/数据竞争 | HIGH | 04 |
| D3 | 实时流（liquidation WebSocket）在批处理七阶段管道中无路径，"已预留"的说法不成立——raw 分区按 ingest_date 组织，流式数据如何落盘未定义 | HIGH | 05/08 |
| D4 | "谁写 raw"职责边界隐含（connector 返回 vs pipeline 落盘）但未显式化为规则 | MEDIUM | 02 |
| D5 | quality_flags.record_key 构成未定义；Parquet 记录无全局唯一 ID，与 03 natural key 的关系未打通 | MEDIUM | 03/04 |
| D6 | FRED vintage/revision 的 as-of 查询（防 look-ahead 的落地机制）在按 observation 分区的布局下如何实现未说明 | HIGH | 04 |
| D7 | 测试 fixture 录制工具未选型 | LOW | 07 |

## 3. 三方讨论纪要

1. **D1（upsert 机制）**：DM 提出 Parquet 分区级 merge-rewrite（读旧分区→合并→temp 写→原子 rename），写放大=整分区重写。AR 确认月度分区粒度下可控（个人研究量级），性能不达标时按约束 §42 补 ADR。**三方一致接受**。
2. **D6（as-of 查询）**：PM 强调这是防 look-ahead 的产品底线，必须第一轮就闭环，不能只靠 07 的测试条目。AR 提出修订记录 `(natural_key, revision_time)` 追加 + DuckDB 视图过滤方案。**接受**。
3. **P2（QueryService 契约）**：PM 要求本轮给出函数签名；DM 认为应先把存储正确性（D1/D2/D6）改完再定接口，避免接口跟着存储方案返工。**折中：接口签名化移入第 2 轮，作为 PM 遗留项跟踪**。
4. **D3（流式路径）**：DM 给出 buffer→周期 flush→复用批处理阶段的方案。AR 补充 collector 只落盘不转换（crash-safe）。**接受**。
5. **D2（并发）**：PM 质疑单写者约束是否限制产品（未来多任务并行）；AR/DM 确认约束 §45 单机架构下 pipeline 串行足够，多 run 并发属演进项（10 §6 已有触发条件）。**接受，记录为显式约束**。
6. P1/P3/P4/D4/D5/D7：无分歧，直接接受（D5 与 D1 一并解决：natural key 规范即 record_key）。

## 4. 决议汇总

接受：P1、P3、P4、D1–D6；推迟到第 2 轮：P2（QueryService 签名）、D7（fixture 工具）。

## 5. 修改结果（已执行）

| 项 | 文件 | 修改 |
|---|---|---|
| P1 | 01 | 新增 §7 验收标准：四原则 → 可验证测试映射 |
| P3 | 02 | config 模块补多环境（default/dev/test/prod） |
| P4 | 10 | §8 补 P0 DoD（文档核对/注册/契约测试/冒烟 + 四条验收测试） |
| D1/D5 | 04 | §2 Canonical 补 upsert 机制（分区级 merge-rewrite）与 natural key 规范（= record_key） |
| D2/D6 | 04 | §3 补 SQLite 单写者/禁并发约束；新增「Revision/Vintage as-of 查询」小节 |
| D3 | 05 | 新增 §7 实时流路径（buffer→flush→复用批处理阶段） |
| D4 | 02 | 边界规则新增第 6 条：写职责独占（connector 不写存储） |
| P1 | 07 | §3 新增 3 条验收测试（provider 可替换性、可重建性、revision as-of 正确性） |

## 6. 遗留（转入第 2 轮）

- QueryService 函数签名级契约（P2，责任人：AR，第 2 轮必须闭环）
- fixture 录制工具选型（D7）
