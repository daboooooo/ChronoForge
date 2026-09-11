# 08 — Failure Model

依据：约束 §27/§29/§30/§67/§68/§69/§82.8

## 1. 错误分类（Error Taxonomy，约束 §29）

| 错误类 | 示例 | 默认策略 |
|---|---|---|
| TransportError | 连接超时、DNS、5xx | 有上限退避重试 |
| RateLimitError | 429 / weight limit | 退避 + 尊重 Retry-After；计入 connector 限流器 |
| AuthError | 401/403、key 失效 | **不重试**；FAIL fast，run 置 FAILED 并告警 |
| ProviderError | 4xx 参数/资源错误 | **不重试**；记录并跳过该请求/分片 |
| SchemaError | 响应不符合录制契约 | **不重试**；标记 dataset 异常，人工核对 API 变更 |
| QualityError | 校验不过 | 不阻断整批；写 quality_flags（可配置升级为阻断） |
| StorageError | 磁盘满、写入中断 | 分区 temp+rename 原子写；重跑幂等恢复 |
| ConfigError | 缺 key、非法配置 | 启动时 fail fast |

禁止 `except Exception: pass / return None`（约束 §29）。

## 2. Retry Policy（约束 §27）

- 指数退避 + jitter，默认上限 5 次，超时上限可配置
- 仅对 TransportError / RateLimitError / 5xx 重试；4xx（除 429）不重试
- 每次重试计数进结构化日志（可观察），禁止 `while True: retry()`

## 3. Recovery（约束 §69）

- **checkpoint 续传**：(source, dataset) 粒度 last_cursor，重启从断点继续
- **幂等重放**：任意阶段失败 → 从上一稳定层重放（raw 在则不重打 API）
- **部分失败**：批量 run 中单 dataset 失败不影响其余，状态记 PARTIAL_SUCCESS，失败项可单独重跑
- 禁止以无限 retry 掩盖真实故障：连续失败 N 次 → 熔断该 dataset 并置 FAILED

## 4. 可观测性（约束 §30/§67）

每次 pipeline run 写入 `run_log`（SQLite）：

```
run_id, source, dataset, start_time, end_time,
input_count, output_count, error_count, warning_count,
status, schema_version, code_version
```

- 结构化日志（structlog 或 stdlib JSON formatter，实施时定）：按 run_id 关联
- 状态机：PENDING/RUNNING/SUCCESS/PARTIAL_SUCCESS/FAILED/CANCELLED（约束 §68）
- 质量报告：quality_flags 表 + run 级汇总（warning_count / error_count）

## 5. 失败场景推演

| 场景 | 系统行为 |
|---|---|
| fetch 中途 crash | raw 已落盘分片保留；重启后从 checkpoint 续传，幂等去重 |
| 源 API schema 变更 | SchemaError → dataset 熔断，不污染 canonical；人工更新 connector + fixture |
| 磁盘写满 | StorageError → run FAILED；释放空间后重放（temp 分片自动清理） |
| FRED 修订发布 | 追加新 version 记录，历史 vintage 保留 |
| 重复触发同一 run | natural key upsert 幂等，无重复记录 |
| 数据 rename 成功但 run_log 写失败 | 以 ingest_batch_id 对账（reconciliation，约束 §69），补记终态 |
| rename 前任一阶段失败 | 重放即恢复（temp 分片自动清理），无半写状态 |
| 本地时钟回拨（NTP 校正） | ingest_time 不假设单调；延迟分析一律以源 event_time 为准（需求 §29） |
