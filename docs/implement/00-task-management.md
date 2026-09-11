# IMP-00 — 开发任务管理与记录机制

版本 1.0（2026-09-11）。本机制是 D10 任务清单（`docs/design/10-task-manifest.md`）的执行与记录层：任务**定义**以 D10 为唯一事实源（冻结契约），本目录只记录**派发、过程与结果**，二者不得混同。

## 1. 目录结构

```
docs/implement/
├── 00-task-management.md   # 本章程
├── 01-dispatch-log.md      # 台账：25 任务状态总表 + 派发事件流水
└── tasks/
    └── {TASK-ID}.md        # 每任务一个记录文件（派发时创建，随执行更新）
```

## 2. 角色与职责

| 角色 | 承担者 | 职责 |
|---|---|---|
| Orchestrator | 主会话 | 派发任务、维护台账与记录、验收 DoD、统一 git 提交、处置 Design Issue |
| Coding Agent | 子会话（每任务独立） | 按任务单实现 + 测试；汇报过程与结果；**不执行 git** |
| 验收 | Orchestrator（必要时另派 Review Agent） | 复跑测试、核对 acceptance、接管性抽查 |

## 3. 任务状态机

```
READY → DISPATCHED → IN_PROGRESS → IN_REVIEW → DONE
                     │                          ↘ BLOCKED（等待 Design Issue / 依赖）
                     └→ FAILED（验收不通过，重派或退回设计）
```

状态变更仅由 Orchestrator 写入台账；Agent 的汇报是变更依据。

## 4. 记录规则

**派发时**（Orchestrator）：
- 创建 `tasks/{TASK-ID}.md`：任务单引用、派发时间、Agent 标识、交付物清单、file_ownership、验收清单（空白待勾）、Deferred 项（见 §7）
- `01-dispatch-log.md` 事件流水追加一行

**执行中**（Agent 汇报 → Orchestrator 记录）：
- 关键实现决策（Agent 在 How 自由度内做的选择）
- 偏差：交付物与任务单的差异及理由
- 发现的 Design Issue：任务转 BLOCKED，引 §6 流程

**完成时**（验收后）：
- DoD 16 项逐项勾选（`docs/design/review/00-…spec.md` §B.5）
- 任务单 acceptance（GWT）逐条核对结果
- 测试命令输出摘要（pytest/ruff/mypy/lint-imports）
- 接管性抽查结论（任选一条：仅凭任务单 + 设计文档能否理解实现）
- git commit hash

## 5. 派发规则

1. 顺序：D10 §10 拓扑序（L0→L6）；同层任务可并行派发（file_ownership 无交集）
2. Agent 输入自足性：任务单 + design_refs 所引章节 + D01 公共约定 = 全部所需；Agent 不得依赖未派发任务的实现
3. Agent 边界：严守 D10 §0.2（可决定 How 5 类 / 不可决定 What 9 类）；发现 Contract 问题 → 停止实现并报告，禁止自行改契约
4. 并行约束：并行任务不得修改同一文件（台账登记 ownership，冲突即驳回）；一律不执行 git

## 6. Design Issue 流程（Contract 变更）

Agent 报告 Contract 问题 → Orchestrator 在任务记录登记 Issue（现象/证据/影响契约条目）→ 任务 BLOCKED → 按 ArchitectureConstraints §85 受控变更（评估 → 修改设计文档 → 版本化）→ 解除 BLOCKED 重派。**禁止**边写边改契约（final-design-audit §10 已冻结）。

## 7. Deferred Acceptance（延后验收）

验收项依赖未派发任务时（如 INFRA-001 的策略校验依赖 MODEL-002 schema），在任务记录登记 Deferred 清单：`验收项 → 依赖任务 → 关闭条件`；依赖任务 DONE 时由 Orchestrator 回归关闭。Deferred ≠ 缺口，未登记的缺失即验收失败。

## 8. Git 约定

- Agent 不执行任何 git 操作；Orchestrator 在验收通过后统一提交
- 每任务 ≥1 个 commit，格式 `feat(<domain>): <摘要>`（domain 取任务命名空间小写：model/storage/acquisition/…）
- BLOCKED 任务的已交付部分（若有）单独 commit 并在信息中标注 `WIP` 前缀

## 9. 台账更新义务

任何状态变更、Issue 登记/解除、Deferred 关闭，**必须同步更新 `01-dispatch-log.md`**；台账与本章程冲突时以章程为准，章程与 D10 冲突时以 D10（冻结契约）为准。
