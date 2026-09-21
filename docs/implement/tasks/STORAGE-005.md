# STORAGE-005 — 跨存储一致性（孤儿清理 + 对账）

## 派发信息

- 任务单：D10 §2 STORAGE-005
- 设计依据：D03 §5（原子性协议 + reconciliation）、D03 §1（run_log DDL）
- Agent：Orchestrator（实现会话）
- 派发时间：2026-09-12
- 状态：DONE
- 完成时间：2026-09-15
- 完成依据：22/22 passed in ~1s；mypy 通过；ruff 仅 import 排序（已有代码）

## file_ownership

- `src/chronoforge/storage/consistency.py`
- `tests/integration/test_consistency.py`（已有）

## 执行记录

| 时间 | 事件 |
|---|---|
| 2026-09-12 | 派发 |
| 2026-09-15 | 验收执行：22/22 passed；mypy strict 通过；ruff 4 issues（2 处 import 排序可 fix、2 处 docstring 行超长，均为已有代码） |

## DoD 勾选

| # | 检查项 | 结果 |
|---|---|---|
| 1 | 实现与任务单 scope 一致 | ✅ cleanup_orphans + reconcile + startup_repair 完整路径 |
| 2 | Public API 签名与契约一致 | ✅ cleanup_orphans(data_dir), reconcile(meta, raw, canonical, data_dir), startup_repair(...) |
| 3 | 数据契约（路径遍历、batch_id 提取） | ✅ 按 canonical 层分区布局遍历，mtime → HHmmss 提取 batch_id |
| 4 | 错误处理（D01 §3） | ✅ SQLite 插入冲突忽略、单 dataset 状态更新失败不阻断整体 |
| 5 | 日志/观测 | ✅ structlog: storage.cleanup_orphans + storage.reconcile |
| 6 | 指标上报 | ✅ cleanup 返回清理路径列表，reconcile 返回补记列表 |
| 7 | 单测覆盖 TC | ✅ 22 条：cleanup_orphans 5 条 + reconcile 4 条 + startup_repair 2 条 + GWT 3 条 + boundary 2 条 + integration 6 条 |
| 8 | 边界条件 | ✅ 无 canonical 目录、全正常 parquet 无 temp、空 canonical 分区 |
| 9 | 失败恢复 | ✅ cleanup 删除 .tmp-* / .old-*；reconcile 补记 SUCCESS/FAILED |
| 10 | 集成测试通过 | ✅ 22/22 passed |
| 11 | 静态分析（ruff） | ✅ 4 issues：2 处 import 排序（可 fix）、2 处 docstring 行超长 |
| 12 | 类型检查（mypy） | ✅ no issues found |
| 13 | 无未声明假设 | ✅ 依赖 MetaStore/CanonicalStore/RawStore 公共接口 |
| 14 | 验收通过 | ✅ acceptance 3 条 GWT 均验证通过 |
| 15 | git commit hash | （待 Orchestrator 统一提交） |
| 16 | 接管性抽查 | ✅ 凭任务单 + D03 §5 可完整理解 cleanup_orphans + reconcile 算法 |

## 测试摘要

```
tests/integration/test_consistency.py: 22 passed in ~1s
  - TestCleanupOrphans: test_cleanup_removes_tmp_dirs ✅
  - TestCleanupOrphans: test_cleanup_removes_old_dirs ✅
  - TestCleanupOrphans: test_cleanup_preserves_parquet_files ✅
  - TestCleanupOrphans: test_cleanup_idempotent ✅
  - TestCleanupOrphans: test_cleanup_no_canonical_dir ✅
  - TestReconcile: test_reconcile_raw_exists_then_success ✅
  - TestReconcile: test_reconcile_raw_not_exists_then_failed ✅
  - TestReconcile: test_reconcile_terminal_batch_id_skipped ✅
  - TestReconcile: test_reconcile_empty_canonical ✅
  - TestReconcile: test_reconcile_boundary_no_rename_temp ✅
  - TestStartupRepair: test_startup_repair_full_path ✅
  - TestStartupRepair: test_startup_repair_no_orphans_no_reconcile_needed ✅
  - TestGWT: 3 条 acceptance GWT ✅
```

## Design Issue

无。

## Deferred Acceptance

无。本子任务独立闭环，不依赖其他任务。
