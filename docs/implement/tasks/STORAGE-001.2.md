# STORAGE-001.2 — MetaStore 锁 + checkpoint + run_log + 状态推导

## 派发信息

- 任务单：D10 §2 STORAGE-001 拆分（子任务 2/2）
- 设计依据：D03 §6 接口、D05 §3 run 状态机、D03 §1 状态推导规则
- Agent：Orchestrator
- 派发时间：2026-09-11
- 完成时间：2026-09-14
- 状态：DONE
- 依赖：STORAGE-001.1（DDL + 迁移）

## file_ownership

- `src/chronoforge/storage/meta.py`（追加方法实现）
- `tests/integration/test_meta_operations.py`（新建）

## 交付物契约摘要

### try_lock_dataset(dataset_id: str) -> bool

- 使用 `BEGIN IMMEDIATE` 事务
- 查询 checkpoints 表是否存在 (source_id, dataset_id)
- 不存在 → INSERT + 返回 True
- 存在 → 返回 False（锁冲突）
- 锁冲突 → 抛 StorageError("dataset locked")

### finish_run(run_id: str, status: str) -> None

- 更新 run_log 表：status、ended_at、chunk_success/failed
- status ∈ {SUCCESS, PARTIAL_SUCCESS, FAILED, CANCELLED}
- 更新 checkpoints：last_success_time = now（仅 SUCCESS/PARTIAL_SUCCESS）

### save_checkpoint(source_id: str, dataset_id: str, cursor: str) -> None

- UPSERT checkpoints 表：(source_id, dataset_id) → last_cursor=cursor, last_success_time=now
- 原子写入（INSERT OR REPLACE）

### get_checkpoint(source_id: str, dataset_id: str) -> dict | None

- 查询 checkpoints 表
- 返回 None（无记录）或 {last_cursor, last_success_time}

### add_quality_flags(flags: list[dict]) -> None

- 批量 INSERT INTO quality_flags
- 字段：record_key、dataset_id、rule_id、severity、detail、raw_ref、payload_digest、run_id、created_at
- 使用 `INSERT OR REPLACE`（允许重处理覆盖）

### derive_dataset_status(dataset_id: str) -> str

按 D03 §1 状态推导规则（按序判定）：
1. 存在未处理 ERROR finding → "INCOMPLETE"
2. 存在 QUARANTINED 记录 → "QUARANTINED"
3. revision_supported 且修订扫描待处理 → "REVISION_PENDING"
4. now − last_success_time > frequency × 2 → "STALE"
5. 最新 run SUCCESS 且无 ERROR finding → "COMPLETE"
6. 其余 → "PARTIAL"

### rebuild_checkpoints(dataset_id: str) -> None

- D03 §5.5 路径：从 run_log 重建 checkpoints
- 扫描 run_log WHERE dataset_id = ? ORDER BY started_at DESC
- 取最新 SUCCESS/PARTIAL_SUCCESS 记录的 checkpoint_before/after

## 测试要求

- **try_lock 互斥**：两线程并发 try_lock 同 dataset → 后者抛 StorageError
- **checkpoint 原子性**：save_checkpoint → get_checkpoint 往返一致
- **finish_run**：status=FAILED 不更新 checkpoint、status=SUCCESS 更新 checkpoint
- **状态推导**：模拟 ERROR finding → derive_dataset_status = "INCOMPLETE"
- **rebuild_checkpoints**：删除 checkpoints 记录 → rebuild 恢复（从 run_log 回溯）
- **边界**：get_checkpoint 无记录返回 None、add_quality_flags 空列表不报错
- **property**：try_lock 成功后 get_checkpoint 返回非 None

## acceptance（GWT）

- [x] Given 两线程并发 try_lock 同 dataset Then 后者抛 StorageError → 已通过（TestGWT::test_given_two_concurrent_threads_try_lock_same_dataset_then_second_raises_storage_error）
- [x] Given finish_run status=FAILED Then checkpoint 不更新 → 已通过（TestGWT::test_given_finish_run_status_failed_then_checkpoint_not_updated）
- [x] Given ERROR finding 存在 When derive_dataset_status Then 返回 INCOMPLETE → 已通过（TestGWT::test_given_error_finding_exists_when_derive_dataset_status_then_returns_incomplete）
- [x] Given checkpoints 清空 When rebuild_checkpoints Then 从 run_log 恢复 → 已通过（TestGWT::test_given_checkpoints_cleared_when_rebuild_checkpoints_then_restored_from_run_log）

## 验收清单（验收时填写）

DoD 16 项逐项核对：

1. ✅ 实现：`src/chronoforge/storage/meta.py` 完成 try_lock_dataset/finish_run/save_checkpoint/get_checkpoint/add_quality_flags/derive_dataset_status/rebuild_checkpoints 七方法
2. ✅ 公共 API：D03 §6 接口签名逐条实现，`MetaStore` 类提供完整协议实现
3. ✅ 数据契约：D03 §1 DDL 全部对齐（checkpoints/run_log/quality_flags 表操作）
4. ✅ 错误处理：StorageError 异常类统一使用（锁冲突、无效状态、连接关闭、rebuild 失败）
5. ✅ 日志：现有 structlog 事件（`pipeline.stage`）已在 MetaStore 外部 pipeline stage 中消费
6. ✅ 指标：run_log 全部观测列写入（chunk_success/chunk_failed/checkpoint_before/after）
7. ✅ 单测：34 个集成测试用例全部通过（test_meta_operations.py）
8. ✅ 边界：get_checkpoint 无记录返回 None、add_quality_flags 空列表不报错、rebuild 无有效 run 抛 StorageError
9. ✅ 失败：FAILED 不更新 checkpoint、无效 status 抛 StorageError、锁互斥抛 StorageError
10. ✅ 恢复：rebuild_checkpoints 从 run_log 恢复 checkpoints（D03 §5.5）
11. ✅ 集成：完整流程 try_lock → save_checkpoint → finish_run → derive_dataset_status 通过
12. ✅ 静态分析：ruff check 通过、mypy strict 通过
13. ✅ 类型检查：dict[str, object] 类型标注完整、str | None 返回类型标注完整
14. ✅ 无未声明假设：所有 SQL 操作基于 D03 §1 DDL，无隐式依赖
15. ✅ 验收通过：GWT 4 条 + DoD 16 项全部通过
16. ✅ 测试：34 passed in ~1s（含 full flow 集成测试）

测试输出摘要：pytest 34 passed in ~1s（ruff check 通过，mypy no issues found）
接管性抽查：依据 D03 §1 DDL + D03 §6 接口 + test_meta_operations.py 34 用例可完整理解实现
git commit：（待 Orchestrator 提交，建议 `feat(storage): implement MetaStore lock/checkpoint/run_log/status`）

## Deferred Acceptance

无。本子任务独立闭环。
