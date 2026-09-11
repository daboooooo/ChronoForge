# 05 — Data Pipeline

依据：需求 §31/§37/§38/§44；约束 §26/§27/§28/§31/§32/§67/§68/§69

## 1. Pipeline 阶段（单向）

```
fetch ──► raw append ──► validate ──► normalize ──► canonical upsert ──► quality check ──► run_log
 (限流/重试)  (不可变)    (schema)    (→canonical)   (幂等去重)        (gap/dup)        (状态)
```

阶段由 `pipeline/` 编排，每阶段职责单一（约束 §36）：

1. **fetch**：connector 按 capability 执行请求；限流/重试/分页在 connector 层（约束 §26/§28）
2. **raw append**：原始响应先落盘再处理（crash-safe：重放从 raw 开始，不重打 API）
3. **validate**：schema / timestamp / null / range 校验（quality 模块）
4. **normalize**：source schema → canonical schema，注入 provenance + 时间语义字段
5. **canonical upsert**：按 natural key 幂等写入 Parquet 分区
6. **quality check**：时间序列 expected/actual/missing interval 检测、duplicate 检测、OHLC 关系校验
7. **run_log**：记录本次 run 全部指标（约束 §67）

## 2. 幂等与增量（约束 §31/§32）

- **幂等**：同 input + 同参数重跑不产生重复记录——canonical 层 natural key upsert；raw 层按 (source, dataset, fetch_window, ingest_batch_id) 跳过已完成分片
- **增量**：`checkpoints` 表记录 (source, dataset) → last_cursor（如 Binance fromId、FRED realtime_end）；重启从 checkpoint 续传
- **重放**：`pipeline.replay(layer, dataset_id)` 可从 raw 重建 canonical、从 canonical 重建 derived（需求 §47 约束 5/6）

## 3. Connector 接入规范（需求 §37/§38/§44）

- 必须实现 `DataConnector` 协议 + `capabilities()` 声明实际能力
- **禁止**依据"交易所通常提供什么"假设 endpoint 存在；开发前核对官方 API 文档（需求 §44）
- 新增源流程：验证官方文档 → 验证访问方式/类型/历史/频率 → 注册 source_registry → 实现 connector
- capability detection 优先于静态假设（CCXT 场景，需求 §38）

## 4. 数据质量规则（需求 §31/§32；约束 §19）

每个 connector 必须覆盖：schema validation、timestamp validation、duplicate detection、gap detection、null validation、range validation、source availability、rate-limit handling、retry、checkpoint。

不合规数据的处理路径：

```
RAW ──► QUALITY FLAG ──► CANONICAL（携带 quality_status/quality_reason）
```

禁止静默修正或覆盖原始值（需求 §32；约束 §50）。

## 5. 调度

- 第一阶段：CLI 手动触发 + 简单循环（`pipeline run --dataset X`）
- P1 及以后：APScheduler 进程内调度（PROVISIONAL，依据不足时先不引入；约束 §77）
- 禁止默认引入 Airflow/Prefect 等编排系统（约束 §44/§72）

## 6. Pipeline 状态机（约束 §68）

```
PENDING → RUNNING → SUCCESS | PARTIAL_SUCCESS | FAILED | CANCELLED
```

PARTIAL_SUCCESS：多 dataset 批量 run 中部分源失败——失败的记 FAILED 并保留成功部分，不整体回滚（约束 §69）。

## 7. 实时流路径（P1：Binance liquidation stream 等）

```
WebSocket collector → 内存 buffer → 按周期/条数阈值 flush 为 raw JSONL 分片（携带 ingest_batch_id）→ 复用 validate → normalize → canonical 阶段
```

- collector 只负责接收与落盘，不做转换（crash-safe：重启后按 event_time 幂等去重续传）
- event_time / ingest_time 分离保证 `market_event_latency` 可测量（需求 §29）
