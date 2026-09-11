# 架构审计记录 — 第 3 轮（终审）

- 时间：2026-09-10
- 参与者：PM、DM、AR
- 对象：两轮修改后的 01–10 文档
- 性质：验证性审计——确认前两轮修改全部落地、遗留项闭环、交叉引用一致、约束 §86 终检

## 1. PM 审计意见

| # | 问题 | 严重度 | 涉及 |
|---|---|---|---|
| P1 | 遗留项闭环：研究可复现性（约束 §23/§52）仍缺落地机制——研究结果如何绑定 dataset version / code version / 参数，不解决则研究结果只能算 exploratory（约束 §52 原文） | HIGH | 02 |
| P2 | 交叉引用一致性终检：01→07、02→04/10、04→06/08/10、05→07 引用是否准确 | LOW | 全部 |

## 2. DM 审计意见

| # | 问题 | 结论 | 涉及 |
|---|---|---|---|
| D1 | 前两轮修改落地核验（逐项对照 round-1/round-2 修改清单） | ✅ 全部落地（见 §4 核验表） | — |
| D2 | 05 §5 调度与 06 D10 状态一致性 | ✅ 一致（均为 PROVISIONAL + 触发条件） | 05/06 |
| D3 | 03 时间字段 8 项与需求 §28 完全一致 | ✅ 一致 | 03 |
| D4 | 07 import-linter 配置与 02 依赖声明一致性（上轮 D1 修正后） | ✅ 一致：connectors 依赖 models/config，storage 依赖 models，research 不依赖 connectors | 02/07 |
| D5 | 约束 §86 Architect Final Checklist 终检 | ✅ 全项通过（见 §5） | — |

## 3. 三方讨论纪要

1. **P1（研究可复现性）**：AR 提议 research 模块增加 `ResearchSnapshot`——研究产出时记录 {datasets: [(dataset_id, version)], code_version, parameters, output_hash}。PM 确认这使约束 §52 的八个问题（What data/Which version/What period/…）可机读回答；DM 确认与 03 §7 dependencies 机制同构，实现成本低。**一致通过，本轮闭环**。
2. **P2/D2/D3/D4**：逐条核验通过，无分歧，无修改。
3. **收尾结论**：三方确认——三轮共提出 22 项发现，全部闭环（接受 20、部分接受 1、按约束 §79 记录 UNKNOWN 1）；无新增 HIGH 级问题；架构文档进入冻结状态，后续重大变更走约束 §85 Architecture Freeze Rule（ADR 流程）。

## 4. 前两轮修改落地核验表

| 轮次-项 | 位置 | 状态 |
|---|---|---|
| R1-P1 验收标准 | 01 §7 | ✅ |
| R1-D1/D5 upsert + natural key | 04 §2 | ✅ |
| R1-D2/D6 并发约束 + as-of 查询 | 04 §3 | ✅ |
| R1-D3 流式路径 | 05 §7 | ✅ |
| R1-P1 验收测试 | 07 §3 | ✅ |
| R1-P4 DoD | 10 §8 | ✅ |
| R2-P1 QueryService | 02 §5 | ✅ |
| R2-P2 dependencies | 03 §7 | ✅ |
| R2-D1 import-linter 修正 | 07 §2 | ✅ |
| R2-D2 迁移机制分离 | 04 §7 | ✅ |
| R2-P3 文本 retention UNKNOWN | 04 §8 | ✅ |
| R2-D3/D4 一致性与时钟 | 08 §5 | ✅ |
| R2-D5 D11/D12 | 06 | ✅ |

## 5. 约束 §86 Architect Final Checklist（终检）

- [x] 未参考现有代码决定架构　[x] Python 主要语言　[x] 模块/层次边界明确　[x] 依赖方向明确且经 import-linter 强制
- [x] 数据源抽象（DataConnector + capabilities）　[x] Canonical 数据模型　[x] 时间语义（8 字段词表）　[x] look-ahead 已防护（as-of 查询 + 验收测试）
- [x] Raw/Derived 分离　[x] lineage（provenance + dependencies）　[x] versioning　[x] 质量为独立模块
- [x] Instrument identity（三级结构）　[x] corporate actions/contract changes（adjustment 四元组）
- [x] 存储有依据（04 §5 决策）　[x] OLTP/OLAP 边界（SQLite/DuckDB）　[x] Research/Backtest 边界　[x] 可复现性（dependencies + ResearchSnapshot）
- [x] 幂等　[x] 失败模型　[x] 重试策略　[x] 可观测性（run_log）　[x] 安全（09）　[x] 测试架构　[x] 架构测试
- [x] 技术决策有证据/置信度　[x] alternatives 分析　[x] trade-offs 记录　[x] 无 speculative abstraction　[x] 无 premature microservices　[x] 无 vendor lock-in　[x] 无未证实假设（PROVISIONAL/UNKNOWN 已显式标注）

## 6. 修改结果（已执行）

| 项 | 文件 | 修改 |
|---|---|---|
| P1 | 02 | research 模块行新增 ResearchSnapshot 职责；§5 新增 ResearchSnapshot 定义（manifest 字段 + 可复现比对语义） |

## 7. 三轮审计总结

| 轮次 | 发现 | 决议 | 性质 |
|---|---|---|---|
| 第 1 轮 | PM 4 + DM 7 | 接受 10，推迟 2（留第 2 轮） | 结构性缺口（存储机制/并发/流式/验收标准） |
| 第 2 轮 | PM 3 + DM 5 | 接受 7，部分接受 1 | 接口契约与深层细节（QueryService/依赖声明/配置错误修正） |
| 第 3 轮 | PM 2 + DM 5 项核验 | 接受 1，核验通过 6 | 验证性终审 + 可复现性闭环 + §86 终检 |

架构文档定稿：`docs/architecture/01–10` + 审计记录 `review/round-1~3`。
