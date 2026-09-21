# ACQUISITION-001.3 — 重试装饰器 + HTTP 映射

## 派发信息

- 任务单：D10 §3 ACQUISITION-001 拆分（子任务 3/3）
- 设计依据：D04 §3（重试矩阵）、D01 §3（错误层级）、D08 §3（structlog 事件词表）
- Agent：Orchestrator
- 派发时间：2026-09-12
- 完成时间：2026-09-14
- 状态：DONE
- 依赖：ACQUISITION-001.1（错误类 + HTTP 映射）、ACQUISITION-001.2（RateLimiter）

## file_ownership

- `src/chronoforge/connectors/retry.py`（新建，重试装饰器）
- `src/chronoforge/connectors/errors.py`（已有，ACQUISITION-001.1 创建）
- `tests/unit/test_retry.py`（新建，21 用例）

## 交付物契约摘要

### 重试装饰器（connectors/retry.py）

已实现 `retry()` 装饰器，符合 D04 §3 重试矩阵：
- 默认 retryable_errors = (TransportError, RateLimitError)
- 指数退避：base_delay * 2^(attempt-1) + jitter[0, jitter_range]
- structlog event="connector.retry"（info 级别），字段含 attempt/max_retries/delay/error/error_type
- error 消息截断至 500 字符
- 保留原函数 __name__/__doc__（functools.wraps）

### 重试矩阵（D04 §3，逐行实现）

| 错误类 | 重试 | 退避 | 特殊处理 |
|---|---|---|---|
| TransportError | 是 | 指数退避 1→2→4→8→16s + jitter(0~0.5s) | 最大 5 次 |
| RateLimitError | 是 | 同上 + Retry-After（由 connector 实现处理） | 装饰器不硬编码 |
| AuthError | 否 | — | 立即抛出，不重试 |
| ProviderError(4xx≠429) | 否 | — | 立即抛出，不重试 |
| SchemaError | 否 | — | 立即抛出，不重试 |

### Retry-After 处理

装饰器只负责指数退避；Retry-After 由调用方（connector 实现）解析并注入 RateLimiter（ACQUISITION-001.2 已实现 `on_rate_limited()`）。

### structlog 事件（D08 §3 事件词表）

- 事件名：`connector.retry`（固定，architecture test 校验）
- 字段：`attempt`（int）、`max_retries`（int）、`delay`（float）、`error`（str 截断 500 字符）、`error_type`（str）
- 级别：`info`（非 error，重试是正常路径）

## 测试要求（D09 TC-C 组）

- **TC-C-008**：429 + Retry-After=5 → 冷却 5s 后重试（结合 RateLimiter）✅
- **退避序列**：mock sleep → 验证 delay = [1, 2, 4, 8, 16]s（不含 jitter）✅
- **jitter**：固定 random → jitter ∈ [0, 0.5] 均匀分布 ✅
- **不重试**：AuthError/ProviderError/SchemaError → 立即抛出，attempt=1 ✅
- **耗尽**：5 次重试全部失败 → 抛最后一次异常 ✅
- **属性**：retry 不改变成功路径返回值（同输入同输出）✅

## acceptance（GWT）

- [x] Given func 抛 429 When retry(5) Then 重试 5 次后抛最后一次异常，delay 序列符合指数退避
- [x] Given func 抛 AuthError When retry(5) Then 不重试，立即抛 AuthError
- [x] Given func 成功 When retry(5) Then 直接返回，无 sleep 调用

## 验收清单（验收时填写）

DoD 16 项 + GWT 逐条勾选。

测试输出摘要：21/21 passed in 44.22s（全库 669/669 passed）｜接管性抽查：通过任务单 + retry.py + 设计文档即可理解实现——装饰器签名、退避公式、structlog 事件格式均与 D04/D08 一一对应｜git commit：待 Orchestrator 统一提交

## 执行记录

| 时间 | 事件 |
|---|---|
| 2026-09-14 | 实现 retry.py 装饰器，覆盖 D04 §3 重试矩阵全部 5 行 |
| 2026-09-14 | 编写 21 个单元测试，覆盖基本行为/退避序列/jitter/不重试错误/RateLimitError/structlog/自定义 retryable_errors/边界情况 |
| 2026-09-14 | pytest 21/21 passed；ruff check 通过；mypy 通过；全库 669/669 passed |

## Deferred Acceptance

无。本子任务独立闭环，协议定义（ACQUISITION-001.1）和限流器（ACQUISITION-001.2）分别由子任务 1/2 负责。
