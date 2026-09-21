# STORAGE-001.1 — MetaStore DDL + 迁移 0001

## 派发信息

- 任务单：D10 §2 STORAGE-001 拆分（子任务 1/2）
- 设计依据：D03 §1 DDL、D03 §6 接口、D04 §5
- Agent：待定
- 派发时间：2026-09-11
- 状态：DONE
- 依赖：MODEL-001（BaseRecord）、INFRA-001（conftest tmp_stores fixture）

## file_ownership

- `src/chronoforge/storage/meta.py`（新建）
- `src/chronoforge/storage/migrations/0001_init.py`（新建）
- `src/chronoforge/storage/migrations/__init__.py`（新建，迁移注册）
- `tests/integration/test_meta_migration.py`（新建）

## 交付物契约摘要

### migrations/0001_init.py

按 D03 §1 DDL 逐表实现迁移脚本：

**6 张表**（DDL 逐行实现，字段名/类型/约束/注释一致）：
1. `source_registry`：source_id PK、display_name、access_type(CHECK)、base_url、rate_limit_json、historical_limit_days、license、enabled
2. `dataset_registry`：dataset_id PK、source_id FK、canonical_type、entity_id、params_json、frequency、continuity_model(CHECK 4值)、status(CHECK 7值)、available_from/to、revision_supported、enabled、created_at
3. `checkpoints`：PK(source_id, dataset_id)、last_cursor、last_success_time
4. `run_log`：run_id PK、source_id、dataset_id、status(CHECK 6值)、started_at、ended_at、input/output/error/warning/request/retry/duplicate/missing/latency_ms、checkpoint_before/after、chunk_success/failed、error_summary(截断2000)、schema_version、code_version、ingest_batch_id
5. `quality_flags`：PK(record_key, rule_id, run_id)、severity(CHECK 3值)、detail、raw_ref、payload_digest、run_id FK、created_at
6. `schema_versions`：version PK、applied_at、description

**WAL 模式 + foreign_keys=ON**：连接后 `PRAGMA journal_mode=WAL`、`PRAGMA foreign_keys=ON`

**幂等性**：迁移可重入（IF NOT EXISTS / 检查 schema_versions 表）

### storage/meta.py — MetaStore 基础类（仅 migrate + schema 管理）

- `MetaStore.__init__(meta_dir: str)`：初始化 SQLite 连接
- `MetaStore.migrate()`：执行迁移 0001（调用 migrations/0001_init.py）
- `MetaStore._check_schema_version()`：校验 schema_versions 表存在
- `MetaStore.close()`：连接关闭

### 错误处理

- meta_dir 不存在 → 创建（os.makedirs）
- SQLite 连接失败 → StorageError
- 迁移失败 → StorageError（携带 SQL 错误信息）

## 测试要求

- **迁移幂等**：连续调用 migrate() 两次不报错
- **DDL 完整性**：6 张表全部存在、字段数/类型/约束与 D03 §1 一致（架构测试）
- **WAL 模式**：PRAGMA journal_mode 返回 WAL
- **foreign_keys=ON**：PRAGMA foreign_keys 返回 1
- **边界**：meta_dir 已存在（不报错）、meta_dir 含子目录（创建成功）
- **失败**：meta_dir 权限不足 → StorageError

## acceptance（GWT）

- [ ] Given 空 meta_dir When migrate() Then 6 张表全部创建成功
- [ ] Given 已迁移数据库 When migrate() Then 不报错（幂等）
- [ ] Given WAL 模式 When 查询 PRAGMA journal_mode Then 返回 WAL

## 验收清单（验收时填写）

DoD 16 项 + GWT 逐条勾选。

测试输出摘要：（待填）｜接管性抽查：（待填）｜git commit：（待填）

## Deferred Acceptance

无。本子任务独立闭环。
