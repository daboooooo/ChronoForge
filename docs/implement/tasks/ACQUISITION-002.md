# ACQUISITION-002 — 窗口拆分 + cursor 推进引擎

## 派发信息

- 任务单：D10 §3 ACQUISITION-002
- 设计依据：D05 §1 FetchStage/§2/§3/§5（修复后）
- Agent：Orchestrator（主会话）
- 派发时间：2026-09-12
- 完成时间：2026-09-15
- 状态：DONE
- 依赖：ACQUISITION-001（RateLimiter、FetchRequest）、STORAGE-001（MetaStore.get_checkpoint）

## file_ownership

- `src/chronoforge/pipeline/windows.py`（新建，窗口计算）
- `src/chronoforge/pipeline/cursor.py`（新建，cursor 推进 + 回退防护）
- `tests/unit/test_windows.py`（新建）
- `tests/unit/test_cursor.py`（新建）

## 交付物契约摘要

### AcquisitionJob 数据类（pipeline/windows.py）

```python
from dataclasses import dataclass
from typing import Literal

@dataclass(frozen=True)
class AcquisitionJob:
    """获取任务（D05 §2，审计 F-01）"""
    dataset_id: str
    start: datetime | None = None        # None → incremental：从 checkpoint 续传
    end: datetime | None = None          # None → now()
    mode: Literal["incremental", "backfill"] = "incremental"
    priority: int = 0
```

### Chunk 数据类（pipeline/windows.py）

```python
@dataclass(frozen=True)
class Chunk:
    """单次窗口请求（D05 §5.2）"""
    chunk_id: str                        # uuid hex[:8]
    dataset_id: str
    start: datetime
    end: datetime
    request: FetchRequest                # 构造后由 FetchStage 调用
```

### plan_chunks 函数（pipeline/windows.py）

```python
def plan_chunks(
    job: AcquisitionJob,
    cursor: str | None,
    now: datetime,
) -> list[Chunk]:
    """
    计算窗口序列（D05 §5.2 语义表驱动）。
    返回按时间排序的 Chunk 列表，每个 Chunk 含对应的 FetchRequest。

    Args:
        job: 获取任务
        cursor: 上次 checkpoint（ISO datetime 字符串），None = 首次
        now: 当前时间
    Returns:
        Chunk 列表（空 = 无数据需获取）
    """
    ...
```

**窗口计算逻辑（表驱动，禁止硬编码）**：

窗口参数由 `dataset_registry` 中的 `continuity_model` 和 `frequency` 决定，按 D05 §5.2 语义表：

```python
# 语义表（D05 §5.2 六行数据即配置）
WINDOW_CONFIG = {
    # (dataset_type, mode) -> (overlap_window_seconds, boundary_inclusive)
    ("klines", "incremental"): {"overlap": 3600, "boundary": "both_ends"},  # 1m interval → overlap=1×interval
    ("klines", "backfill"):    {"overlap": 3600, "boundary": "both_ends"},
    ("funding", "incremental"): {"overlap": 28800, "boundary": "both_ends"},  # 8h cycle → overlap=1×cycle
    ("funding", "backfill"):   {"overlap": 28800, "boundary": "both_ends"},
    ("fred", "incremental"):   {"overlap": "full_window", "boundary": "both_ends"},  # 全窗口 diff
    ("fred", "backfill"):      {"overlap": "full_window", "boundary": "both_ends"},
    # ... 按 D05 §5.2 补全全部数据集
}
```

**窗口拆分算法**：

```python
def plan_chunks(job, cursor, now):
    # 1. 确定窗口范围
    if job.mode == "incremental":
        start = parse_iso(cursor) - overlap if cursor else job.start
        end = job.end or now
    else:  # backfill
        start = job.start
        end = job.end or now

    # 2. 按 chunk_size（如 24h）拆分窗口
    chunks = []
    chunk_start = start
    while chunk_start < end:
        chunk_end = min(chunk_start + chunk_size, end)
        chunks.append(Chunk(
            chunk_id=uuid4().hex[:8],
            dataset_id=job.dataset_id,
            start=chunk_start,
            end=chunk_end,
            request=FetchRequest(
                dataset_id=job.dataset_id,
                params={},  # 由 connector 实现填充
                start=chunk_start,
                end=chunk_end,
                cursor=format_iso(chunk_start) if cursor else None,
            ),
        ))
        chunk_start = chunk_end  # 下一窗口（不重叠，overlap 已在 start 计算中处理）

    return chunks
```

### Cursor 推进（pipeline/cursor.py）

```python
from enum import Enum

class CursorAction(Enum):
    ADVANCE = "advance"    # 推进 cursor
    SKIP = "skip"           # 跳过（新 cursor ≤ 旧 cursor）
    NOOP = "noop"           # 不变

@dataclass
class CursorUpdate:
    action: CursorAction
    new_cursor: str | None
    warning: str | None = None
```

```python
def advance_cursor(
    old_cursor: str | None,
    chunk_result: ChunkResult,  # 含 chunk 的 end 时间
) -> CursorUpdate:
    """
    推进 cursor（D05 §3 不变量 + 回退防护）。

    Args:
        old_cursor: 上次 checkpoint
        chunk_result: 本次 chunk 结果（含 end 时间 + 是否成功）
    Returns:
        CursorUpdate（含 new_cursor/warning）
    """
    new_cursor = format_iso(chunk_result.end)

    if old_cursor is not None and parse_iso(new_cursor) <= parse_iso(old_cursor):
        # 回退防护：新 cursor ≤ 旧 cursor → 跳过
        return CursorUpdate(
            action=CursorAction.SKIP,
            new_cursor=old_cursor,
            warning=f"cursor not advancing: {new_cursor} <= {old_cursor}",
        )

    return CursorUpdate(
        action=CursorAction.ADVANCE,
        new_cursor=new_cursor,
    )
```

### 关键不变量（D05 §3）

- **cursor 推进不变量**：cursor = 最后一个已落盘且通过校验的 chunk 右边界（exclusive = 下窗口 start）
- **SUCCESS 与 PARTIAL_SUCCESS 均按此推进**；失败 chunk 之后所有窗口不推进
- **恒有 checkpoint ≤ durable_valid_data_boundary**
- **cursor 回退防护**：新 cursor ≤ 旧 cursor → 跳过更新 + WARNING

### 错误处理

- 表外数据集 → ConfigError（fail fast）
- cursor 格式无效 → ConfigError

## 测试要求（D09 TC-P 组）

- **TC-P-010**：3-chunk 窗口第 2 chunk 失败 → chunk_failed=1、cursor=第 1 chunk 右界、终态 PARTIAL
- **六数据集×两 mode 窗口快照**：D05 §5.2 全部数据集的窗口计算结果（含 backfill）
- **边界**：闰日/年边界窗口切分、00:00 分区归属
- **失败**：cursor 回退 → 跳过+WARNING
- **property**：TC-PROP-002 区间拼接律（acquire(A,B) + acquire(B,C) ≡ acquire(A,C)）
- **窗口生成**：incremental mode 下 cursor=T1000 → 下窗口 start=T1000−1×interval（overlap 生效）

## acceptance（GWT）

- [x] Given checkpoint=T1000 And 3-chunk 窗口第 2 chunk 失败 Then cursor=第 1 chunk 右界、chunk_failed=1、终态 PARTIAL（test_tc_p_010）
- [x] Given klines cursor=T1000 Then 下窗口 start=T1000−1×interval（overlap 生效）（test_incremental_cursor_overlaps）
- [x] Given 新 cursor≤旧 cursor Then 跳过更新 + WARNING（回退防护）（test_equal_cursor_skips）

## 验收清单（验收时填写）

DoD 16 项：
- [x] DoD 1-4: 任务单契约完整（AcquisitionJob/Chunk/plan_chunks/advance_cursor 签名与 D05 §2/§3 一致）
- [x] DoD 5-8: 测试覆盖 TC-P 组全部要求（TC-P-010、六数据集×两 mode、边界、失败、property、overlap 验证）
- [x] DoD 9-10: mypy strict 通过（2 source files）、ruff 无错误
- [x] DoD 11-12: 代码无未派发依赖（零外部 imports，仅 FetchRequest）
- [x] DoD 13-14: 表驱动窗口配置覆盖 D05 §5.2 全部 10 个数据集
- [x] DoD 15-16: 全库 731 tests passed（新增 63 个用例），无回归

测试输出摘要：63 passed in 0.14s（新模块）｜全库 731 passed in 109s（含回归）｜ruff 无错误｜mypy strict 通过
接管性抽查：给定任务单 + D05 §2/§3/§5.2，可理解 plan_chunks 表驱动窗口计算和 advance_cursor 回退防护逻辑。
git commit：feat(acquisition): 窗口拆分 + cursor 推进引擎（ACQUISITION-002）

## 执行记录

| 时间 | 事件 |
|---|---|
| 2026-09-15 | 创建 `pipeline/windows.py`：AcquisitionJob、Chunk、plan_chunks（表驱动窗口配置覆盖 10 数据集）、ChunkResult、verify_interval_concatenation |
| 2026-09-15 | 创建 `pipeline/cursor.py`：CursorAction、CursorUpdate、advance_cursor（含 chunk_failed 传播）、advance_chunks |
| 2026-09-15 | 创建 `tests/unit/test_windows.py`：46 个用例，覆盖 AcquisitionJob/Chunk/frozen、基础功能、六数据集×两 mode、边界条件、property 拼接律、overlap 验证、FetchRequest 构造 |
| 2026-09-15 | 创建 `tests/unit/test_cursor.py`：17 个用例，覆盖基础推进/chunk 失败/回退防护/TC-P-010/batch 推进/CursorUpdate/边界精度 |
| 2026-09-15 | 全库 731 tests passed，ruff/mypy 通过，验收通过 |

## Deferred Acceptance

无。本子任务独立闭环。
