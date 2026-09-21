# PIPELINE-001 — PipelineRunner 七阶段编排 + 状态机 + replay

## 派发信息

- 任务单：D10 §6 PIPELINE-001
- 设计依据：D05 §1–4/§7（修复后）
- Agent：Coding-Agent-P（子会话）
- 派发时间：2026-09-12（2026-09-20 派发执行）
- 状态：DONE（2026-09-20 验收通过）
- 依赖：ACQUISITION-002（窗口/overlap 引擎）、STORAGE-001/002/003/005（Store 接口）、VALIDATION-001.1（规则引擎）、DATA-SOURCE-001~007（连接器实现/接口）

## file_ownership

- `src/chronoforge/pipeline/runner.py`（新建，PipelineRunner + Stage 实现）
- `src/chronoforge/pipeline/state.py`（新建，RunStatus 状态机）
- `src/chronoforge/pipeline/replay.py`（新建，replay 入口）
- `src/chronoforge/pipeline/__init__.py`（新建，__all__ 导出）
- `tests/integration/test_pipeline.py`（新建）

## 交付物契约摘要

### RunStatus 枚举（pipeline/state.py）

```python
from enum import Enum

class RunStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    PARTIAL_SUCCESS = "PARTIAL_SUCCESS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
```

### RunContext 数据类（pipeline/runner.py）

```python
@dataclass
class RunContext:
    run_id: str
    ingest_batch_id: str
    source_id: str
    dataset_id: str
    connector: DataConnector
    raw_store: RawStore
    canonical_store: CanonicalStore
    meta: MetaStore
    settings: Settings
    drift_findings: list[QualityFinding] = field(default_factory=list)
```

### StageResult 数据类（pipeline/runner.py）

```python
@dataclass
class StageResult:
    input_count: int = 0
    output_count: int = 0
    error_count: int = 0
    warning_count: int = 0
    request_count: int = 0
    retry_count: int = 0
    duplicate_count: int = 0
    missing_count: int = 0
    latency_ms: int = 0
    checkpoint_before: str | None = None
    checkpoint_after: str | None = None
    chunk_success: int = 0
    chunk_failed: int = 0
    errors: list[str] = field(default_factory=list)
```

### Stage Protocol（pipeline/runner.py）

```python
class Stage(Protocol):
    name: str
    def execute(self, ctx: RunContext) -> StageResult: ...
```

### 七阶段实现（pipeline/runner.py）

**阶段列表**（固定顺序，禁止跨阶段调用）：

| # | Stage | 职责 |
|---|---|---|
| 1 | FetchStage | 按 AcquisitionJob 计算窗口 → connector.fetch() → 流式交 RawStage 落盘 |
| 2 | RawAppendStage | raw.append() → RawRef 回填 ctx.raw_record_id |
| 3 | ValidateStage | normalize 前 shape 校验（fixture schema 对比） |
| 4 | NormalizeStage | connector.normalize(raw) → 注入 provenance |
| 5 | CanonicalStage | canonical.upsert() + UpsertStats 记账 |
| 6 | QualityStage | quality.rules.run(records) → meta.add_quality_flags() |
| 7 | RunLogStage | meta.finish_run() + save_checkpoint + dataset 状态推导 |

**FetchStage 实现**：

```python
class FetchStage:
    name = "fetch"

    def __init__(self, window_planner: Callable[[AcquisitionJob, str | None, datetime], list[Chunk]]):
        self.planner = window_planner

    def execute(self, ctx: RunContext) -> StageResult:
        result = StageResult()
        old_checkpoint = ctx.meta.get_checkpoint(ctx.source_id, ctx.dataset_id)
        result.checkpoint_before = old_checkpoint

        # 1. 窗口拆分
        cursor = old_checkpoint
        chunks = self.planner(ctx._job, cursor, datetime.now(UTC))
        if not chunks:
            return result  # 空结果=正常（0 行）

        # 2. 逐 chunk 执行
        chunk_idx = 0
        last_successful_end = None
        for chunk in chunks:
            chunk_idx += 1
            try:
                raw_batches = list(ctx.connector.fetch(chunk.request))
                result.request_count += len(raw_batches)

                # 立即落盘（流式，不积内存）
                raw_refs = ctx.raw_store.append(...)
                result.output_count += len(raw_refs)

                # cursor 推进
                last_successful_end = chunk.request.end
            except TransportError | RateLimitError as exc:
                result.retry_count += 1
                result.errors.append(str(exc))
                result.chunk_failed += 1
                # chunk 重试耗尽 → 该 chunk 计失败，后续跳过
                continue
            except AuthError:
                raise  # → run FAILED
            except SchemaError:
                raise  # → run FAILED + 熔断

        result.checkpoint_after = format_iso(last_successful_end) if last_successful_end else old_checkpoint
        result.chunk_success = chunk_idx - result.chunk_failed

        # 3. 判断终态
        if result.chunk_failed > 0:
            result.output_count = -1  # 标记 PARTIAL
        return result
```

**NormalizeStage 实现**：

```python
class NormalizeStage:
    name = "normalize"

    def __init__(self, error_threshold: float = 0.10):
        self.error_threshold = error_threshold  # Settings.normalize_error_threshold

    def execute(self, ctx: RunContext) -> StageResult:
        result = StageResult()
        total = 0
        errors = 0
        normalized_records = []

        # 遍历 raw 层所有 batch
        for raw_ref in ctx.raw_store.iter_refs(ctx.source_id, ctx.dataset_id):
            raw_batch = ctx.raw_store.read(raw_ref)  # 从 JSONL 读取
            try:
                records = ctx.connector.normalize(raw_batch)
                # 注入 provenance
                for rec in records:
                    rec.raw_record_id = f"{ctx.dataset_id}:{raw_ref.path}:{raw_ref.line_no}"
                    rec.ingest_timestamp = datetime.now(UTC).replace(tzinfo=None)
                    rec.schema_version = "1.0"
                normalized_records.extend(records)
                total += len(records)
            except Exception as exc:
                errors += 1
                result.errors.append(f"{raw_ref.path}: {str(exc)}")
                result.error_count += 1

        result.input_count = total
        result.output_count = total - errors
        result.error_count = errors

        # 失败率 > threshold → FAILED
        if total > 0 and errors / total > self.error_threshold:
            raise QualityError(f"Normalize error rate {errors/total:.2%} > {self.error_threshold:.0%}")

        # 将 normalized_records 存入 ctx 供后续阶段使用
        ctx._normalized_records = normalized_records
        return result
```

**RunLogStage 实现**：

```python
class RunLogStage:
    name = "runlog"

    def execute(self, ctx: RunContext, stage_results: dict[str, StageResult]) -> StageResult:
        result = StageResult()

        # 1. 聚合全部计数
        total_input = sum(r.input_count for r in stage_results.values())
        total_output = sum(r.output_count for r in stage_results.values())
        total_errors = sum(r.error_count for r in stage_results.values())

        # 2. finish_run
        ctx.meta.finish_run(
            run_id=ctx.run_id,
            status=self._determine_status(stage_results),
            counts=RunCounts(
                input_count=total_input,
                output_count=total_output,
                error_count=total_errors,
                request_count=sum(r.request_count for r in stage_results.values()),
                retry_count=sum(r.retry_count for r in stage_results.values()),
                duplicate_count=sum(r.duplicate_count for r in stage_results.values()),
                missing_count=sum(r.missing_count for r in stage_results.values()),
                latency_ms=sum(r.latency_ms for r in stage_results.values()),
                chunk_success=sum(r.chunk_success for r in stage_results.values()),
                chunk_failed=sum(r.chunk_failed for r in stage_results.values()),
                warning_count=sum(r.warning_count for r in stage_results.values()),
            ),
        )

        # 3. save_checkpoint
        ctx.meta.save_checkpoint(...)

        # 4. dataset 状态推导
        ctx.meta.update_dataset_status(ctx.dataset_id)

        result.output_count = total_output
        return result

    def _determine_status(self, stage_results: dict[str, StageResult]) -> RunStatus:
        # 判断终态：全部成功=SUCCESS，否则 PARTIAL_SUCCESS 或 FAILED
        has_errors = any(r.error_count > 0 for r in stage_results.values())
        if has_errors:
            return RunStatus.PARTIAL_SUCCESS
        return RunStatus.SUCCESS
```

### PipelineRunner 主类（pipeline/runner.py）

```python
class PipelineRunner:
    def __init__(self, ctx_factory: Callable[[AcquisitionJob], RunContext]):
        self.ctx_factory = ctx_factory
        self._circuit_breaker: dict[str, int] = {}  # dataset_id → consecutive failures

    def run(self, job: AcquisitionJob) -> RunRow:
        """执行单次 pipeline run（D05 §2）"""
        # 1. 构造 ctx
        ctx = self.ctx_factory(job)

        # 2. 锁 dataset
        run_row = ctx.meta.try_lock_dataset(ctx.dataset_id)
        ctx.run_id = run_row.run_id
        ctx.ingest_batch_id = run_row.ingest_batch_id

        # 3. 创建 stages（顺序固定）
        stages = [
            FetchStage(window_planner=plan_chunks),
            RawAppendStage(),
            ValidateStage(),
            NormalizeStage(error_threshold=ctx.settings.normalize_error_threshold),
            CanonicalStage(),
            QualityStage(rule_runner=run, block_on=ctx.settings.quality_block_on),
            RunLogStage(),
        ]

        # 4. 逐 stage 执行
        stage_results = {}
        try:
            for stage in stages:
                structlog.info("pipeline.stage", run_id=ctx.run_id, stage=stage.name)
                result = stage.execute(ctx)
                stage_results[stage.name] = result
        except AuthError | SchemaError | ConfigError as exc:
            # → FAILED
            ctx.meta.finish_run(ctx.run_id, "FAILED", ...)
            raise
        except QualityError as exc:
            # block_on 命中 → FAILED（不回滚已写分区）
            ctx.meta.finish_run(ctx.run_id, "FAILED", ...)
            raise
        finally:
            # 终态写入（失败时也写）
            if RunStatus.FAILED not in stage_results:
                pass  # 由 RunLogStage 处理

        # 5. 更新熔断计数
        final_status = self._determine_final_status(stage_results)
        if final_status == RunStatus.FAILED:
            self._circuit_breaker[ctx.dataset_id] = self._circuit_breaker.get(ctx.dataset_id, 0) + 1
        else:
            self._circuit_breaker[ctx.dataset_id] = 0

        # 6. 熔断检查
        if self._circuit_breaker.get(ctx.dataset_id, 0) >= 3:
            ctx.meta.finish_run(ctx.run_id, "CANCELLED", ..., error_summary="circuit open")
            return ctx.meta.get_run(ctx.run_id)

        return ctx.meta.get_run(ctx.run_id)

    def run_many(self, jobs: list[AcquisitionJob]) -> list[RunRow]:
        """批量执行（按 priority 排序，单 job 失败不中断）"""
        sorted_jobs = sorted(jobs, key=lambda j: j.priority, reverse=True)
        results = []
        for job in sorted_jobs:
            try:
                results.append(self.run(job))
            except Exception:
                # 单 job 失败不中断，继续
                results.append(self.run(job))  # 重新跑或标记 FAILED
        return results
```

### 状态机（D05 §3 转换矩阵）

| 当前\事件 | 阶段成功 | 阶段异常(可重试类耗尽) | Auth/Schema/Config | 用户取消 |
|---|---|---|---|---|
| PENDING | →RUNNING | — | →FAILED | →CANCELLED |
| RUNNING | 末阶段→SUCCESS；中途→RUNNING | →PARTIAL_SUCCESS | →FAILED | →CANCELLED |

- **cursor 推进不变量**：仅终态（SUCCESS/PARTIAL_SUCCESS）推进
- **熔断**：3 连败 → CANCELLED + error_summary="circuit open"
- **cursor 回退防护**：新 cursor ≤ 旧 cursor → 跳过更新 + WARNING

### replay 入口（pipeline/replay.py）

```python
def replay(
    layer: Literal["canonical", "derived"],
    dataset_id: str,
    meta: MetaStore,
    raw_store: RawStore,
    canonical_store: CanonicalStore,
    connector: DataConnector,
) -> RunRow:
    """
    Replay（D05 §4）。
    canonical: RawStore.iter_refs(dataset) → 从 RawStage 重放
    derived: FeatureEngine.recompute(dataset_id)
    """
    run_id = f"R{uuid4().hex[:11]}"  # replay run_id 前缀 "R"
    ...
```

## 测试要求（D09 TC-P 组）

- **TC-P-001**：七阶段顺序（spy 装配断言）
- **TC-P-002**：各错误类→终态映射表逐行断言
- **TC-P-003**：PARTIAL 保留成功部分
- **TC-P-004**：熔断 3 连败
- **TC-P-005**：cursor 仅终态更新
- **TC-P-006**：replay canonical 字节一致
- **TC-P-007**：run_many 部分失败聚合
- **TC-P-008**：空结果=SUCCESS(0 行)
- **TC-P-009**：cursor 三态恢复（回退/丢失/损坏）
- **TC-P-010**：chunk 语义（3-chunk 窗口第 2 chunk 失败）
- **TC-P-011**：幂等 8 场景（howto §7）
- **失败注入**：mock stage 抛各错误类 → 终态与 run_log 计数断言
- **replay**：改 normalize 代码后 replay → 记录数不变、内容按新规则
- **run_many**：1/3 失败 → 其余 SUCCESS，汇总 PARTIAL

## acceptance（GWT）

- [x] Given 各错误类注入 When run Then 终态与 D05 §1 映射表逐行一致（TC-P-002：Auth/Schema/Storage/Config/Quality→FAILED 共 5 行 + Transport/RateLimit/Provider chunk 级→PARTIAL_SUCCESS 共 3 行，参数化逐行断言）
- [x] Given 3 次 FAILED When 第 4 次 run Then CANCELLED（熔断）（TC-P-004：error_summary="circuit open"，stages 未执行）
- [x] Given 删除 derived When replay Then 输出与原快照逐字节一致（TC-P-006 canonical 层：业务值列逐字节一致 + 记录数不变 + 不调 API；derived 层依赖 QUERY-002 FeatureEngine，登记 DEF-005）

## 验收清单（验收时填写）

DoD 逐项勾选（howto §43）：

- [x] 实现完整（runner/state/replay/__init__ 四文件，七阶段 + 锁 + 熔断 + run_many + replay）
- [x] Public API 完整（run/run_many/replay + RunStatus/RunContext/StageResult/Stage，D10 public_api 逐项对齐）
- [x] 数据契约实现（StageResult 13 字段、run_log 全部观测列含 chunk/checkpoint 前后）
- [x] 错误处理实现（D05 §1 映射表逐行 + 异常携带 context；块级失败→finish_failed_run 统一记账）
- [x] 日志实现（pipeline.stage start/end、run.finish、pipeline.run_many、pipeline.circuit_open、pipeline.run_failed）
- [x] 指标实现（input/output/error/warning/request/retry/duplicate/missing/latency/chunk_success/chunk_failed 11 列入 run_log）
- [x] 单测完整（任务单 tests 字段仅定义 integration TC-P 组；窗口/cursor 单测由 ACQUISITION-002 承担）
- [x] 边界测试完整（TC-P-008 空结果、TC-P-010 单/chunk 边界、DEC-F0 首 chunk 失败）
- [x] 失败测试完整（TC-P-002 全错误类注入 + TC-P-011 断网重试）
- [x] 恢复测试完整（TC-P-009 三态恢复 + TC-P-011g 中途退出→release_stale_locks，均为真实 SQLite/fs 状态非纯 mock）
- [x] 集成测试完整（31 用例，真实 RawStore/CanonicalStoreImpl/MetaStore）
- [x] 静态分析通过（ruff All checks passed）
- [x] 类型检查通过（mypy strict：pipeline 6 文件 0 issues，零 ignore）
- [x] 无未声明假设（DEVIATION-1/DEC-F0/D-1~D-6 全部入档本记录）
- [x] 验收标准满足（GWT-1/2 全过；GWT-3 canonical 过、derived 登记 DEF-005）

测试输出摘要：test_pipeline.py **31 passed**（含参数化 38 用例）in 4.43s；全量回归 **1212 passed / 0 failed** in 121.04s（基线 1181 + 新增 31，零破坏）；ruff All checks passed；mypy strict Success（6 source files）；lint-imports 1 broken 为存量问题（违规边全在 connectors/quality/models/storage/config 只读文件，0 处涉及 chronoforge.pipeline，非本任务引入）。

接管性抽查：Orchestrator 抽阅 state.py（RunStatus + D05 §3 转换矩阵 docstring）、runner.py（RunContext/StageResult 契约字段逐字对齐任务单；run() 锁→熔断→七阶段→异常映射→RunRow 流程与 D05 §2 一致）、replay.py（五阶段复用 + DEVIATION-1 入档）。命名、结构、注释与设计文档一一对应，陌生 Agent 仅凭任务单 + D05 可接管。**通过**。

git commit：**PENDING**（git 因 Xcode 许可证未接受不可用；建议许可接受后执行 `feat(pipeline): PIPELINE-001 seven-stage runner + state machine + replay`）

## 执行记录

| 时间 | 事件 |
|---|---|
| 2026-09-20 | Orchestrator 派发 Coding-Agent-P 子会话执行；登记裁决：DEC-F0（首 chunk 失败→FAILED）、D-1（无 meta.get_run→Runner 聚合构造 RunRow）、D-2（熔断检查先于 try_lock）、replay 复用 try_lock（DEVIATION-1） |
| 2026-09-20 | Agent 交付五文件（state 45 行 / runner 930 行 / replay 162 行 / __init__ 52 行 / test 937 行）；31 用例 + 8 项偏差全部入档 |
| 2026-09-20 | Orchestrator 验收：复跑 31 passed + 全量 1212 passed + ruff/mypy 清零 + lint-imports 存量确认；GWT 逐条核对通过 → DONE |

## Deferred Acceptance

| ID | 验收项 | 依赖任务 | 关闭条件 |
|---|---|---|---|
| DEF-005 | GWT-3 derived 层：replay("derived") 输出与原快照逐字节一致（当前 raise NotImplementedError，测试已断言该边界） | QUERY-002（FeatureEngine） | QUERY-002 DONE 后在 replay.py 实现 derived 分支（FeatureEngine.recompute）并补快照 hash 比对测试，由 Orchestrator 回归关闭 |
