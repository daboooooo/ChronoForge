# QUERY-003 — ResearchSnapshot 与复现

## 派发信息

- 任务单：D10 §7 QUERY-003
- 设计依据：D07 §3（snapshot DDL + contextmanager）、D03 §1（迁移 0002）
- Agent：Orchestrator 主会话直执（Coding-Agent 模式）
- 派发时间：2026-09-12
- 状态：DONE（2026-09-21）
- 依赖：STORAGE-001（MetaStore）、QUERY-001（QueryService）

## file_ownership

- `src/chronoforge/research/snapshot.py`（新建，ResearchSnapshot + contextmanager）
- `src/chronoforge/storage/migrations/0003_snapshot.py`（新建，迁移脚本；原定 `0002_snapshot.py`，0002 版本号已被审计 H-6 占用顺延为 0003，见决策 D-1）
- `tests/integration/test_snapshot.py`（新建）
- 联动修改（非 ownership，必要最小改动）：`src/chronoforge/research/__init__.py`（导出公共 API）、`src/chronoforge/storage/migrations/__init__.py`（注册迁移 0003）、`tests/integration/test_meta_migration.py`（EXPECTED_TABLES 增加 research_snapshot，见 D-8）

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

## 决策记录（How 层自由度内的选择与偏差，2026-09-21 执行时登记）

| ID | 决策 | 依据 |
|---|---|---|
| D-1 | 迁移编号 0002→0003，文件 `0002_snapshot.py`→`0003_snapshot.py` | migrations/__init__.py 中版本号 "0002" 已被审计 H-6（quality_flags.resolved，2026-09-20 修复轮）注册占用（schema_versions 主键唯一、按版本排序执行）；DDL 契约与 D07 §3 逐列一致（What 不变，仅序号顺延），两处以同内容 SQL 同步维护（同 0001_init.py 既有模式） |
| D-2 | dataset_version 锁定经 DatasetRegistry.version_info()（run_log 最近 SUCCESS run 的 code+schema 复合；无 SUCCESS run 锁定 ""） | 任务单伪代码 `meta.get_dataset(ds_id).current_version` 接口不存在（STORAGE-001 MetaStore 无此方法）；与 QUERY-001 D-2 同源处置（run_log 为同一事实源），格式与 QueryService._get_dataset_version 一致 |
| D-3 | 落库走 meta.connection 直接 INSERT（模块内提供 get_snapshot 读侧辅助）；meta 参数类型为 MetaStoreLike 结构协议（四个公共函数签名保持 meta 入参不变） | MetaStore 无 get_snapshot 且 meta.py 不在本任务 file_ownership；实测 research→storage.meta 会被禁入契约传递闭包（→exceptions→connectors.errors）判违规；同层协议模式同 QUERY-002 D-4（QueryServiceLike），MetaStore 结构化满足协议，测试全程以真实 MetaStore 实例驱动 |
| D-4 | 错误模型：领域错误 ValueError（空 datasets/未知 dataset/未知 snapshot/重复 compute）、SQLite 失败 RuntimeError（rollback 后抛出）；不导入 chronoforge.exceptions | exceptions→connectors.errors 存量断裂链会经禁入契约传递闭包新增违规；同 features/engine.py 模式（ValueError/RuntimeError）——中途实测移除 exceptions 导入仍不够（storage.meta 传递链同罪），改 MetaStoreLike 后 "research must not import connectors" 恢复 KEPT |
| D-5 | output_hash = sha256（frame 按全部列升序排序 → Arrow IPC write_ipc 字节） | 任务单伪代码 `result.model_dump_json()` 为 pydantic API，polars DataFrame 无此方法（伪代码缺陷）；排序消除行序不确定性，IPC 二进制同 schema+数据字节确定（实测验证）且 null/空串可区分（CSV 文本序列化无法区分）；查询层默认稳定排序之外的二次防线 |
| D-6 | 入库时机 = ctx.compute()（使用者显式触发）；contextmanager 退出无强制动作；研究抛异常不产生记录（异常安全） | 任务单代码框架即此结构（finally 注释 "hash 由使用者计算后传入"）；D07 §3 注释"退出：入库"由 compute 承担，避免对无结果/失败研究落空记录 |
| D-7 | 快照不可变护栏：INSERT 直插（无 OR REPLACE/UPDATE 路径），主键冲突 → ValueError("Snapshot already exists") | 架构 02 §5"快照不可变（仅追加）"的执行点；重复 compute 是唯一同 id 写入路径（uuid4().hex[:12] 冲突概率可忽略）；test_compute_twice_rejected 锁定且断言记录数不增 |
| D-8 | 联动修改 `research/__init__.py`（__all__ 导出 6 个新符号）与 `tests/integration/test_meta_migration.py`（EXPECTED_TABLES 增加 research_snapshot，test_all_6_tables_exist→test_all_tables_exist） | 前者：D07 §3 公共 API 面交付必需（QUERY-001 已 DONE，无并行 ownership 冲突）；后者：精确集合断言因 schema 演进过期，属测试维护非契约变更；其余任何既有文件未动 |

## 测试要求（D09 TC-R 组）

- **TC-R-004**：同 snapshot 两次 reproduce → hash 一致
- **数据变更后 reproduce**：hash 不一致且报告 dataset_version 变化
- **边界**：空 datasets（raise ValueError）
- **失败**：hash 不一致报告版本变化

## acceptance（GWT）

- [x] Given 同 snapshot 两次 reproduce Then hash 一致（test_same_snapshot_reproduce_hash_consistent：两次 reproduce hash_match=True 且 new_hash==original==compute 时 output_hash；test_compute_on_empty_view 空帧复现同证；反例 test_reproduce_after_data_update_reports_version_change 证明数据变更后必不一致）
- [x] Given 数据更新后 reproduce Then hash 不一致且报告 dataset_version 变化（test_reproduce_after_data_update_reports_version_change：追加 parquet + 新 SUCCESS run 0.1.1 → hash_match=False、version_changes={"BINANCE:BTCUSDT:OHLCV": {"snapshot_version": "0.1.0+1.0", "current_version": "0.1.1+1.0"}}；test_version_bump_alone_keeps_hash_match 锁定"仅版本推进、数据未变→hash 一致"语义）

## 验收清单（验收时填写）

DoD 逐项勾选（howto §43）：

- [x] 实现完整（migrations/0003_snapshot.py + migrations/__init__.py 注册 + research/snapshot.py：research_snapshot contextmanager / ResearchSnapshotContext.compute / get_snapshot / snapshot_reproduce / 3 个 frozen dataclass）
- [x] Public API 完整（D07 §3 签名逐项对齐：research_snapshot(qs, datasets, params, meta) / snapshot_reproduce(snapshot_id, meta, qs, query_func)；research/__init__.py `__all__` 导出 6 个新符号）
- [x] 数据契约实现（research_snapshot DDL 与 D07 §3 逐列一致；datasets_json=[{"dataset_id","dataset_version"}]、params_json 按 key 排序、code_version=chronoforge.__version__、output_hash=sha256(排序 IPC 字节)、notebook_ref/query_text 可选溯源）
- [x] 错误处理实现（空 datasets/未知 dataset/未知 snapshot/重复 compute → ValueError；SQLite 写失败 → RuntimeError + rollback；D-3/D-4 入档）
- [x] 日志实现（snapshot_created 结构化日志 structlog：snapshot_id/output_hash/datasets；迁移描述写入 schema_versions）
- [x] 指标实现（ReproduceResult 携带 original_hash/new_hash/hash_match/version_changes，供 CLI reproduce 命令（D08 §4）与对账消费）
- [x] 单测完整（D10 tests 字段：integration TC-R-004 + 边界 + 失败全组）
- [x] 边界测试完整（空 datasets ValueError、无数据视图空帧 hash 可复现、无 SUCCESS run 锁定 ""、迁移幂等重入）
- [x] 失败测试完整（数据更新后 hash 不一致 + 版本变化报告；未知 snapshot_id → ValueError；重复 compute 拒绝且记录数不增）
- [x] 恢复测试完整（不适用——快照只追加无恢复路径；迁移幂等由 test_migration_idempotent_and_registered 覆盖）
- [x] 集成测试完整（12 用例：真实 MetaStore 全迁移链 + 手工 parquet + write 注册视图后 read_only 重开 + per-query glob 数据更新注入）
- [x] 静态分析通过（ruff All checks passed：snapshot.py/0003_snapshot.py/migrations/__init__.py/test_snapshot.py/test_meta_migration.py；存量 2 处 pipeline/replay.py I001 为台账在册问题，0 处涉及本任务）
- [x] 类型检查通过（mypy Success：全库 60 source files 0 issues）
- [x] 无未声明假设（D-1~D-8 全部入档本记录）
- [x] 验收标准满足（GWT-1/2 逐条通过）

测试输出摘要：test_snapshot.py **12 passed** in 5.41s（迁移 2 + 进入锁定 3 + compute 3 + 复现 4）；test_meta_migration.py 联动复跑 **30 passed**（与 test_snapshot 同批）；全量回归 **1272 passed / 0 failed** in 124.86s（基线 1260 + 新增 12，零破坏）；ruff All checks passed（新增/修改文件零问题；存量 2 处 pipeline I001 非本任务引入）；mypy Success（60 source files）；lint-imports "research must not import connectors" **KEPT**（中途曾因导入 exceptions/storage.meta 触发传递闭包违规，按 QUERY-002 同层协议模式改 MetaStoreLike + 内建异常后清零；存量 Layered 契约 broken 与本任务无关）。

接管性抽查：snapshot.py 模块 docstring 完整陈述 D07 §3 契约、output_hash 序列化语义、迁移编号顺延原因与错误模型；compute/reproduce 步骤注释对齐任务单伪代码；D-1~D-8 均给出设计依据；陌生 Agent 仅凭任务单 + D07 §3 + 本记录可接管维护。**通过**。

git commit：PENDING（feat(research) 交付物已提交，回填中）

## 执行记录

| 时间 | 事件 |
|---|---|
| 2026-09-21 | 主会话直执：读 D07 §3/D03 §1/D10 §7/D09 TC-R + QUERY-001/002 与 STORAGE-001 实况；发现迁移版本号 0002 已被审计 H-6 占用 → 顺延 0003（D-1）；发现伪代码 meta.get_dataset/get_snapshot 接口不存在 → run_log 事实源 + MetaStoreLike 同层协议（D-2/D-3） |
| 2026-09-21 | 发现伪代码 `result.model_dump_json()` 非 polars API → 全列排序 + Arrow IPC 序列化（D-5），实测 IPC 字节确定性成立；lint-imports 传递闭包违规两轮修正（先移除 exceptions 导入仍不够，storage.meta 同罪）→ research 禁入契约恢复 KEPT（D-3/D-4） |
| 2026-09-21 | 交付 migrations/0003_snapshot.py + migrations/__init__.py 注册 + research/snapshot.py + research/__init__.py 导出 + tests/integration/test_snapshot.py（12 用例）；联动 tests/integration/test_meta_migration.py EXPECTED_TABLES（D-8）；git commit 3e006cc |
| 2026-09-21 | 验收：GWT 逐条核对通过 + DoD 全勾 + 接管性抽查通过 → DONE；test_snapshot 12 passed，全量 1272 passed / 0 failed，ruff/mypy/lint-imports 清零（存量除外）；无新增 Design Issue，无 Deferred 项 |

## Deferred Acceptance

无。本子任务独立闭环。
