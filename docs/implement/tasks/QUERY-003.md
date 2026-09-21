# QUERY-003 — ResearchSnapshot 与复现

## 派发信息

- 任务单：D10 §7 QUERY-003
- 设计依据：D07 §3（snapshot DDL + contextmanager）、D03 §1（迁移 0002）
- Agent：待定
- 派发时间：2026-09-12
- 状态：READY
- 依赖：STORAGE-001（MetaStore）、QUERY-001（QueryService）

## file_ownership

- `src/chronoforge/research/snapshot.py`（新建，ResearchSnapshot + contextmanager）
- `src/chronoforge/storage/migrations/0002_snapshot.py`（新建，迁移脚本）
- `tests/integration/test_snapshot.py`（新建）

## 交付物契约摘要

### 迁移 0002（storage/migrations/0002_snapshot.py）

```sql
-- D07 §3 research_snapshot DDL
CREATE TABLE IF NOT EXISTS research_snapshot(
  snapshot_id TEXT PRIMARY KEY,           -- uuid hex 12
  created_at TEXT NOT NULL,
  datasets_json TEXT NOT NULL,            -- [{"dataset_id","dataset_version"}]
  code_version TEXT NOT NULL,             -- chronoforge.__version__
  params_json TEXT NOT NULL,              -- 研究参数
  output_hash TEXT NOT NULL,              -- sha256(结果序列化)
  notebook_ref TEXT,                      -- 可选溯源
  query_text TEXT                         -- 可选溯源
);
```

- 在 `migrations/__init__.py` 中注册迁移函数
- 幂等：IF NOT EXISTS

### ResearchSnapshot contextmanager（research/snapshot.py）

```python
import hashlib
import json
from contextlib import contextmanager
from datetime import datetime, UTC
import duckdb
import polars as pl

@contextmanager
def research_snapshot(
    qs: "DuckDBQueryService",
    datasets: list[str],
    params: dict[str, Any],
    meta,  # MetaStore instance
):
    """
    ResearchSnapshot contextmanager（D07 §3）。
    进入：锁定各 dataset 版本；退出：计算 output_hash 入库。
    """
    # 进入：锁定各 dataset 版本
    locked_datasets = []
    for ds_id in datasets:
        ds = meta.get_dataset(ds_id)
        if not ds:
            raise ValueError(f"Dataset not found: {ds_id}")
        locked_datasets.append({
            "dataset_id": ds_id,
            "dataset_version": ds.current_version,
        })

    snapshot_id = uuid4().hex[:12]
    created_at = datetime.now(UTC).replace(tzinfo=None).isoformat()

    # yield 给使用者执行研究
    try:
        yield ResearchSnapshotContext(
            snapshot_id=snapshot_id,
            datasets=locked_datasets,
            params=params,
            qs=qs,
        )
    finally:
        # 退出：计算 output_hash 入库
        pass  # hash 由使用者计算后传入


class ResearchSnapshotContext:
    """snapshot 执行上下文"""

    def __init__(self, snapshot_id, datasets, params, qs):
        self.snapshot_id = snapshot_id
        self.datasets = datasets
        self.params = params
        self.qs = qs
        self.notebook_ref: str | None = None
        self.query_text: str | None = None

    def compute(self, query_func: Callable[["DuckDBQueryService"], pl.DataFrame]) -> "ResearchSnapshot":
        """
        执行研究并创建 snapshot 记录。
        """
        # 执行查询
        result = query_func(self.qs)

        # 计算 output_hash
        # 序列化 frame 为有序行（按列排序确保确定性）
        serialized = result.sort(pl.all()).model_dump_json()
        output_hash = hashlib.sha256(serialized.encode()).hexdigest()

        # 入库
        import json
        datasets_json = json.dumps(self.datasets)
        params_json = json.dumps(self.params, sort_keys=True)

        # 通过 meta 写入
        self._insert_snapshot(
            snapshot_id=self.snapshot_id,
            created_at=datetime.now(UTC).replace(tzinfo=None).isoformat(),
            datasets_json=datasets_json,
            code_version=__version__,  # chronoforge.__version__
            params_json=params_json,
            output_hash=output_hash,
            notebook_ref=self.notebook_ref,
            query_text=self.query_text,
        )

        return ResearchSnapshot(
            snapshot_id=self.snapshot_id,
            output_hash=output_hash,
            datasets=self.datasets,
            params=self.params,
        )
```

### snapshot_reproduce 函数（research/snapshot.py）

```python
def snapshot_reproduce(
    snapshot_id: str,
    meta: MetaStore,
    qs: DuckDBQueryService,
    query_func: Callable[["DuckDBQueryService"], pl.DataFrame],
) -> ReproduceResult:
    """
    复现 snapshot（D07 §3 契约）。
    重跑并比对 output_hash。
    """
    # 1. 查询 snapshot 记录
    snapshot = meta.get_snapshot(snapshot_id)
    if not snapshot:
        raise ValueError(f"Snapshot not found: {snapshot_id}")

    # 2. 重新执行查询
    result = query_func(qs)
    serialized = result.sort(pl.all()).model_dump_json()
    new_hash = hashlib.sha256(serialized.encode()).hexdigest()

    # 3. 比对
    hash_match = new_hash == snapshot.output_hash

    # 4. 如果 hash 不一致，报告 dataset_version 变化
    version_changes = None
    if not hash_match:
        # 对比 snapshot.datasets 与当前 dataset_versions
        version_changes = {}
        for ds_info in json.loads(snapshot.datasets_json):
            ds_id = ds_info["dataset_id"]
            ds = meta.get_dataset(ds_id)
            if ds and ds.current_version != ds_info["dataset_version"]:
                version_changes[ds_id] = {
                    "snapshot_version": ds_info["dataset_version"],
                    "current_version": ds.current_version,
                }

    return ReproduceResult(
        snapshot_id=snapshot_id,
        hash_match=hash_match,
        original_hash=snapshot.output_hash,
        new_hash=new_hash,
        version_changes=version_changes,
    )
```

### 数据类

```python
@dataclass
class ResearchSnapshot:
    snapshot_id: str
    output_hash: str
    datasets: list[dict]
    params: dict


@dataclass
class ReproduceResult:
    snapshot_id: str
    hash_match: bool
    original_hash: str
    new_hash: str
    version_changes: dict[str, dict] | None
```

### 架构 02 §5 契约

- **快照不可变**：snapshot 创建后不可修改（仅追加）
- **复现确定性**：同 snapshot 两次 reproduce → hash 一致
- **版本变更报告**：数据更新后 reproduce → hash 不一致且报告 dataset_version 变化

## 测试要求（D09 TC-R 组）

- **TC-R-004**：同 snapshot 两次 reproduce → hash 一致
- **数据变更后 reproduce**：hash 不一致且报告 dataset_version 变化
- **边界**：空 datasets（raise ValueError）
- **失败**：hash 不一致报告版本变化

## acceptance（GWT）

- [ ] Given 同 snapshot 两次 reproduce Then hash 一致
- [ ] Given 数据更新后 reproduce Then hash 不一致且报告 dataset_version 变化

## 验收清单（验收时填写）

DoD 16 项 + GWT 逐条勾选。

测试输出摘要：（待填）｜接管性抽查：（待填）｜git commit：（待填）

## 执行记录

| 时间 | 事件 |
|---|---|
| （执行时填写） | |

## Deferred Acceptance

无。本子任务独立闭环。
