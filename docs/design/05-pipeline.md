# D05 — Pipeline 详细设计

依据：架构 05/08。本文档定义 PipelineRunner、七阶段签名、状态机、checkpoint/replay——实施者据此编写 `pipeline/`，测试者据此构造各阶段失败场景。

## 1. 阶段接口（pipeline/runner.py）

```python
class Stage(Protocol):
    name: str
    def execute(self, ctx: RunContext) -> StageResult: ...

@dataclass
class RunContext:
    run_id: str; ingest_batch_id: str
    source_id: str; dataset_id: str
    connector: DataConnector
    raw_store: RawStore; canonical_store: CanonicalStore
    meta: MetaStore; settings: Settings

@dataclass
class StageResult:
    input_count: int = 0; output_count: int = 0
    error_count: int = 0; warning_count: int = 0
    # 观测计数（审计 F-01/F-11：howto §34 全字段，随 stage 累加并入 run_log）
    request_count: int = 0; retry_count: int = 0
    duplicate_count: int = 0; missing_count: int = 0
    latency_ms: int = 0
    checkpoint_before: str | None = None; checkpoint_after: str | None = None
    chunk_success: int = 0; chunk_failed: int = 0
    errors: list[str] = field(default_factory=list)   # 截断入 error_summary
```

七阶段固定顺序（pipeline 装配，禁止跨阶段调用）：

| # | Stage | 职责与关键行为 | 失败语义 |
|---|---|---|---|
| 1 | FetchStage | 按 AcquisitionJob（§2）计算窗口：incremental=cursor−overlap，backfill=job.start−overlap（§5.2）；滚动窗口逐 chunk 调 `connector.fetch()` → 立即交 RawStage 落盘（流式，不积内存）；每 chunk 落盘且校验通过 → 推进内存 cursor（durable 边界）；丢弃未收盘 K 线（Q-TS-003）；空结果=正常（0 行） | AuthError→run FAILED；chunk 级重试耗尽→该 chunk 计失败、cursor 停留、后续 chunk 跳过→PARTIAL（审计 F-01/F-03） |
| 2 | RawAppendStage | raw.append()；构造 RawRef 回填 ctx → raw_record_id | StorageError→FAILED |
| 3 | ValidateStage | 对 normalize 前的源数据做 shape 校验（fixture schema 对比，SchemaError 熔断 dataset） | SchemaError→FAILED+熔断 |
| 4 | NormalizeStage | connector.normalize(raw)；注入 raw_record_id/ingest_timestamp/schema_version；InstrumentResolver 生成三级 ID | 记录级失败→计 error 不中断（阀值>10% 升级 FAILED，可配） |
| 5 | CanonicalStage | canonical.upsert()（D03 §3 算法）+ UpsertStats 记账 | StorageError→FAILED |
| 6 | QualityStage | quality.rules.run(records)（D06）→ meta.add_quality_flags()；含 gap detection（期望区间 vs actual） | 规则结果不阻断（除非 severity=ERROR 且 policy=BLOCK，D06 §3） |
| 7 | RunLogStage | meta.finish_run()（含全部观测计数与 chunk 计数）；save_checkpoint：SUCCESS/PARTIAL_SUCCESS 均写入 cursor=内存 durable 边界；按 D03 §1 推导规则更新 dataset_registry.status | 对账修复（D03 §5） |

## 2. PipelineRunner

```python
@dataclass(frozen=True)
class AcquisitionJob:                    # 审计 F-01：Backfill 与 Incremental 统一任务表达（howto §23/§24）
    dataset_id: str
    start: datetime | None = None        # None → incremental：从 checkpoint 续传
    end: datetime | None = None          # None → now()
    mode: Literal["incremental", "backfill"] = "incremental"
    priority: int = 0                    # run_many 排序

class PipelineRunner:
    def __init__(self, ctx_factory: Callable[[AcquisitionJob], RunContext]): ...
    def run(self, job: AcquisitionJob) -> RunRow:
        # 1. registry 查 dataset → source；2. meta.try_lock_dataset（锁冲突→StorageError 快速失败）
        # 3. 窗口拆分（FetchStage 内执行，chunk=单次滚动窗口请求）：
        #    incremental: start = cursor - overlap_window；backfill: start = job.start - overlap_window
        #    全窗口 diff 类（fred/sec）：两种 mode 等价，均全窗口拉取后 diff（§5.2）——同一引擎同一代码路径
        # 4. 逐 stage：exception→按失败语义定终态；stage 间传递计数
        # 5. 终态写入 run_log（chunk_success/chunk_failed 计账）；返回 RunRow
    def run_many(self, jobs: list[AcquisitionJob]) -> list[RunRow]:
        # 按 priority 排序逐个 try；单 job 失败不中断；汇总（全部成功=SUCCESS，否则 PARTIAL/FAILED 聚合，架构 05 §6）
```

- 进程内单线程执行（SQLite 单写者，架构 04 §3）；多 dataset 串行
- 每阶段开始/结束记 structlog event="pipeline.stage" {run_id, stage, counts}

## 3. 状态机（pipeline/state.py，转换矩阵）

| 当前\事件 | 阶段成功 | 阶段异常(可重试类耗尽) | Auth/Schema/Config | 用户取消 |
|---|---|---|---|---|
| PENDING | →RUNNING | — | →FAILED | →CANCELLED |
| RUNNING | 末阶段→SUCCESS；中途→RUNNING | →PARTIAL_SUCCESS（保留已落盘） | →FAILED | →CANCELLED |
| *SUCCESS/PARTIAL/FAILED/CANCELLED* | 终态；重跑=新 run_id | | | |

- **cursor 推进不变量（审计 F-03，Invariant 1）**：cursor = 最后一个已落盘且通过校验的 chunk 右边界（exclusive = 下窗口 start）。SUCCESS 与 PARTIAL_SUCCESS 均按此推进；失败 chunk 之后所有窗口不推进。恒有 checkpoint ≤ durable_valid_data_boundary
- cursor 回退防护：新 cursor ≤ 旧 cursor → 跳过更新 + WARNING（防源端回退引发重下/覆盖）
- 熔断：同 dataset 连续 3 次 FAILED → `checkpoints` 旁路表字段 `circuit_open=1`，run 直接 CANCELLED + error_summary="circuit open"（架构 08 §3 上限 N=3）

## 4. Replay（pipeline/replay.py）

```python
def replay(layer: Literal["canonical", "derived"], dataset_id: str) -> RunRow:
    # canonical: RawStore.iter_refs(dataset) → 从 RawStage 重放（跳过 fetch，不调 API）
    # derived:   读 dependencies（D02 §7）→ FeatureEngine.recompute(dataset_id)
    # 全程复用 run_log 记账（run_id 前缀 "R"）
```

验收（架构 07 §3）：删除 derived → replay → 输出与原快照逐字节一致（feature 内容 hash 比对）。

## 5. 增量语义与 cursor 契约（审计 F-02/F-03）

### 5.1 Cursor 语义属性（howto §6.1 八项逐源声明）

| 数据集 | cursor 类型 | 单调 | 连续 | 稳定 | 可重复 | 可跳跃 | 可回退 | 跨请求一致 |
|---|---|---|---|---|---|---|---|---|
| binance klines / funding | timestamp（窗口右界） | 严格 | 否 | 是 | 是 | 否 | 否 | 是 |
| binance aggTrades | trade_id（归集 id） | 严格 | 否（归集跳号正常） | 是 | 是 | 是 | 否 | 是 |
| OI / ticker / deribit 快照 | timestamp | 是 | 否（快照流） | 是 | 是 | 是 | 否 | 是 |
| fred observations | timestamp（观察日） | 是 | 否（发布节奏） | 是（vintage 内） | 是 | 是 | 否 | 是 |
| sec submissions | date | 是 | 否 | 是 | 是 | 是 | 否 | 是 |
| yahoo chart | timestamp（区间右界） | 严格 | 否 | 是 | 是 | 否 | 否 | 是 |

### 5.2 增量窗口、边界与 overlap

| 数据集 | cursor 格式 | 增量窗口 | boundary_semantics | overlap_window |
|---|---|---|---|---|
| binance_spot/futures klines | ISO datetime（下窗口 start） | start = cursor − overlap，end = now − 1×interval | startTime/endTime 均**含端点**（官方语义，实施时以文档核对记录为准） | 1×interval（依据：最后 1 根 K 线未闭合时源会重写；配合 Q-TS-003 丢弃未收盘 K 线） |
| funding | ISO datetime | 同上 | fundingTime 含端点 | 1×结算周期（8h） |
| OI / ticker / deribit 快照 | ISO datetime（上次快照） | 全量快照按 nk upsert | N/A（快照无窗口） | N/A |
| fred | ISO datetime（上次 release 扫描） | 全窗口拉取 diff 修订 | observation_start/end 含端点 | 全窗口（修订依赖全量 diff，upsert 幂等兜底） |
| sec submissions | ISO datetime（上次 seen date） | filing-recent 增量 | 日期含端点 | 1 天（filing date 归属日边界） |
| yahoo chart | ISO datetime（下区间 start） | 同 klines | period1/period2 含端点（秒精度） | 1×interval |

- overlap 引入的重复记录由 natural key upsert 消除（幂等，架构 05 §2）——overlap 是防御性设计，正确性不依赖其存在
- 全窗口 diff 类（fred/sec）：incremental 与 backfill 等价（同一代码路径，howto §23）

## 6. 调度（P0 不引入框架，架构 05 §5）

CLI `chronoforge pipeline run --dataset DS [--start --end --mode incremental|backfill] [--all-due] [--dry-run]`——构造 AcquisitionJob（§2）；`--all-due` = 遍历 dataset_registry 中 `enabled` 且 checkpoint.last_success_time + frequency 到期的 dataset（自动 incremental 模式）。APScheduler 评估推迟至 P1（架构 06 D10 PROVISIONAL）。

## 7. 流式路径接口预留（P1，liquidation WebSocket）

```python
class StreamCollector(Protocol):
    stream: str                                   # "!forceOrder@arr"
    def collect(self, flush_every_s: int = 30, flush_count: int = 500) -> None:
        # buffer → 阈值 flush 为 RawBatch(ingest_batch_id 分片) → 复用 Stage 3–7
```

## 8. 测试依据（D09 TC-P 组）

- 锁互斥；熔断 3 连败；PARTIAL_SUCCESS 保留成功部分；cursor 仅终态更新
- 各阶段注入失败（mock stage 抛各错误类）→ 终态与 run_log 计数断言
- replay canonical：改 normalize 代码后 replay → 记录数不变、内容按新规则
- run_many：1/3 失败 → 其余 SUCCESS，汇总 PARTIAL
