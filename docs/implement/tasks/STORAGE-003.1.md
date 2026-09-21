# STORAGE-003.1 — CanonicalStore upsert 核心（分组/去重/merge-rewrite/分区布局）

## 派发信息

- 任务单：D10 §2 STORAGE-003 拆分（子任务 1/2）
- 设计依据：D03 §3（merge-rewrite 算法）、D02 §2（natural_key 身份键）
- Agent：Orchestrator
- 派发时间：2026-09-12
- 状态：DONE
- 依赖：MODEL-002（各 Type schema + natural_key()）、STORAGE-002（RawRef 构造）

## file_ownership

- `src/chronoforge/storage/canonical.py`（新建）
- `src/chronoforge/storage/base.py`（新建 CanonicalStore Protocol + UpsertStats 数据类）
- `tests/integration/test_canonical_upsert.py`（新建）

## 交付物契约摘要

### UpsertStats 数据类（storage/base.py）

```python
@dataclass(frozen=True)
class UpsertStats:
    inserted: int           # 新增记录数
    updated: int            # 更新记录数
    rewritten_partitions: int  # 被重写分区数
    total_records: int      # 本次处理总记录数
```

### CanonicalStore Protocol（storage/base.py）

```python
class CanonicalStore(Protocol):
    def upsert(
        self,
        records: Sequence[BaseRecord],
        canonical_type: CanonicalType,
        entity_id: str,
    ) -> UpsertStats: ...
```

### 路径布局（D03 §3）

- Parquet 路径：`{data_dir}/canonical/{type}/entity={id}/year={YYYY}/month={MM}/part-{seq}.parquet`
- 使用 pyarrow，`compression="zstd"`，时间列用 `timestamp[us]`
- 分区目录命名：`year=2026`、`month=09`（Hive 风格，与 DuckDB hive_partitioning=1 兼容）

### merge-rewrite upsert 算法（D03 §3 逐步实现）

**输入**：`records: list[BaseRecord]`、`canonical_type: CanonicalType`、`entity_id: str`

**步骤**：

1. **按 (year, month) 分组**：从记录的 event_time/observation_time 提取 year/month，得到受影响分区集合 P
2. **FOR each partition p in P**：
   a. 将 p 中记录转为 DataFrame（按 natural_key 去重，保留最后一条）
   b. **IF 分区无旧文件**：直接 temp 写 → rename（原子写协议）
   c. **ELSE（merge-rewrite）**：
      - 读取全部旧 parquet 文件 → old_df
      - `merged = concat(old_df, new_df).sort_values("_revision_seq").drop_duplicates(natural_key_cols, keep="last").drop("_revision_seq")`
      - 写 `p.tmp-{uuid}/` → fsync → rename 替换 p 的文件集
      - 旧文件先 `mv` 至 `p.old-{uuid}` 再整体删除
   d. 计数：inserted（原分区不存在且新记录 nk 不在旧数据中）/ updated（nk 存在且值列有变化）
3. **返回 UpsertStats{inserted, updated, rewritten_partitions=len(P), total_records=len(records)}**

### 关键实现细节

- **natural_key 解析**：通过 `canonical_type` 查 MODEL-002 注册的 `natural_key(CanonicalType) -> tuple[str,...]` 获取列名
- **_revision_seq**：写入时附加 `_revision_seq = datetime.now(UTC).replace(tzinfo=None)` 用于 merge 排序
- **原子写**：temp 目录命名 `{uuid}.tmp`，写入完成后 `os.rename(tmp_path, target_path)`（Unix 原子操作）
- **分区内文件**：单分区内可写多个 part-{seq}.parquet 文件（按输入批次编号），读时 `read_parquet(part-*.parquet)` 合并

###  revision 类类型特殊处理

- NUMBER/POSITION 的 natural_key 含 `revision_time`，天然多版本
- upsert 时不去重路径（`(nk, revision_time)` 组合唯一才合并）
- 不触发 drift（由 STORAGE-003.2 的 Q-DRIFT-001 处理非 revision 类）

## 测试要求（D09 TC-S 组）

- **TC-S-001**：upsert 幂等——同批次连写两次 → 第二次 stats.inserted=0
- **TC-S-002**：merge-rewrite 保留旧行——旧分区 3 行 + 新 1 行（同 nk）→ 终态 3 行（1 更新 2 原样）
- **边界**：空分区首写（无旧文件直接 temp→rename）、跨月分片（同批次跨 2 个月）
- **失败**：temp 目录写入后 rename 前抛异常 → 下次启动 temp 目录残留（孤儿）
- **property**：TC-PROP-004 upsert 交换律（任意两批次不同顺序 upsert → 终态一致）

## acceptance（GWT）

- [x] Given 同批次 records upsert 两次 Then 第二次 UpsertStats.inserted=0（幂等）
- [x] Given 旧分区有 3 条不同 nk 记录 When upsert 1 条同 nk Then 终态仍 3 行（1 被更新）
- [x] Given 空数据目录 When upsert 新数据 Then parquet 文件布局符合 {type}/entity=/year=/month=/ 规范

## 验收清单（验收时填写）

DoD 16 项逐条勾选：

| # | 项目 | 状态 |
|---|------|------|
| 1 | 实现符合公开 API 契约 | ✅ 通过 |
| 2 | 数据契约与 D02/D03 一致 | ✅ 通过 |
| 3 | 错误处理（StorageError） | ✅ 通过 |
| 4 | 日志事件符合 structlog 词表 | ✅ 通过 |
| 5 | 指标/可观测性 | ✅ N/A（无指标需求） |
| 6 | 单测覆盖 | ✅ 21 用例全部通过 |
| 7 | 边界条件测试 | ✅ 空分区、跨月、孤儿 |
| 8 | 失败路径测试 | ✅ merge-rewrite 异常清理 |
| 9 | 恢复路径测试 | ✅ temp 目录清理 |
| 10 | 集成测试通过 | ✅ 21 passed |
| 11 | 静态分析（ruff） | ✅ 无错误 |
| 12 | 类型检查（mypy） | ✅ 无错误 |
| 13 | 无未声明假设 | ✅ 仅依赖 MODEL-002 |
| 14 | 验收通过（GWT） | ✅ 3/3 勾选 |
| 15 | 测试命令输出 | ✅ 见下方 |
| 16 | 接管性抽查 | ✅ 见下方 |

测试输出摘要：
- pytest: 21 passed in 3.61s（含 GWT/幂等/merge-rewrite/边界/失败/property/revision/统计/布局/nk 全组）
- ruff: All checks passed
- mypy: Success: no issues found in 2 source files

接管性抽查：仅凭任务单 STORAGE-003.1.md + D03 §3 + D02 §2，可实现 canonical.py 的 merge-rewrite upsert 算法，接口签名、路径布局、分区字段、原子写协议均与文档一致。

## 执行记录

| 时间 | 事件 |
|---|---|
| 2026-09-13 | 任务验收通过，21 测试全部通过，ruff + mypy 无错误 |

## Deferred Acceptance

无。本子任务与 STORAGE-003.2 共同闭环，本任务覆盖 upsert 核心路径，drift 检测由子任务 2 负责。
