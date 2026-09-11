# IMP-01 — 任务派发台账

状态：READY（可派发）/ DISPATCHED（已派发）/ IN_PROGRESS（执行中）/ IN_REVIEW（验收中）/ DONE（完成）/ BLOCKED（受阻，见任务记录）/ FAILED（验收失败）。

## 1. 任务状态总表（25 单，依据 D10 §10 派发 DAG）

| Task ID | 层 | 标题 | 状态 | 派发时间 | 完成时间 | 记录 |
|---|---|---|---|---|---|---|
| MODEL-001 | L0 | BaseRecord 基座 | DONE | 2026-09-11 | 2026-09-11 | [tasks/MODEL-001.md](tasks/MODEL-001.md) |
| INFRA-001 | L0 | 测试脚手架 | DONE | 2026-09-11 | 2026-09-11 | [tasks/INFRA-001.md](tasks/INFRA-001.md) |
| MODEL-002 | L1 | 27 Canonical Type schema | READY | | | |
| MODEL-003 | L1 | InstrumentResolver | READY | | | |
| STORAGE-001 | L1 | MetaStore + 迁移 | READY | | | |
| STORAGE-002 | L1 | RawStore JSONL | READY | | | |
| STORAGE-003 | L2 | CanonicalStore merge-rewrite | READY | | | |
| ACQUISITION-001 | L2 | Connector 协议/限流/重试 | READY | | | |
| VALIDATION-001 | L2 | 质量规则引擎 | READY | | | |
| STORAGE-004 | L3 | DuckDB 视图 | READY | | | |
| STORAGE-005 | L3 | 原子性/对账 | READY | | | |
| ACQUISITION-002 | L3 | 窗口/overlap/cursor 引擎 | READY | | | |
| VALIDATION-002 | L3 | Continuity + 日历 | READY | | | |
| DATA-SOURCE-001 | L3 | binance_spot | READY | | | |
| DATA-SOURCE-002 | L3 | binance_futures | READY | | | |
| DATA-SOURCE-003 | L3 | deribit | READY | | | |
| DATA-SOURCE-004 | L3 | ccxt_bridge | READY | | | |
| DATA-SOURCE-005 | L3 | yahoo | READY | | | |
| DATA-SOURCE-006 | L3 | fred | READY | | | |
| DATA-SOURCE-007 | L3 | sec_edgar | READY | | | |
| PIPELINE-001 | L4 | Runner 七阶段 | READY | | | |
| QUERY-001 | L4 | QueryService | READY | | | |
| QUERY-002 | L5 | FeatureEngine | READY | | | |
| QUERY-003 | L5 | ResearchSnapshot | READY | | | |
| CLI-001 | L6 | 命令树/Settings/脱敏 | READY | | | |

## 2. 派发事件流水

| 时间 | 事件 | 备注 |
|---|---|---|
| 2026-09-11 | 派发 MODEL-001（Agent-A）、INFRA-001（Agent-B）并行 | L0 首批；file_ownership 无交集；Agent 不执行 git |
| 2026-09-11 | MODEL-001 验收通过 → DONE | 12 用例；UP042 契约优先决策入档；DEF-003 登记 |
| 2026-09-11 | INFRA-001 验收通过 → DONE | 24 用例全绿；DEF-001/002/004 登记；.gitignore 补 .hypothesis/ |

## 3. Design Issue 登记

| ID | 时间 | 任务 | 契约条目 | 状态 |
|---|---|---|---|---|

（空——期望保持为空；任何条目都意味着受控变更流程启动）

## 4. Deferred Acceptance 登记

| ID | 登记时间 | 任务 | 验收项 | 依赖 | 状态 |
|---|---|---|---|---|---|
| DEF-001 | 2026-09-11 | INFRA-001 | strategies.ohlcv() 通过 MODEL-002 schema 校验 | MODEL-002 | OPEN |
| DEF-002 | 2026-09-11 | INFRA-001 | conftest 的 settings fixture（Settings 类） | CLI-001 | OPEN |
| DEF-003 | 2026-09-11 | MODEL-001 | TC-M-001/002 的 OHLCV 实例断言 | MODEL-002 | OPEN |
| DEF-004 | 2026-09-11 | INFRA-001 | CI 覆盖率门槛启用 | 首个业务逻辑任务 | OPEN |
