# ACQUISITION-001 — Connector 协议/限流/重试

## 派发信息

- 任务单：D10 §3 ACQUISITION-001（冻结契约）
- 设计依据：D04 §1（协议）、D04 §2（限流）、D04 §3（重试）
- Agent：待定
- 派发时间：2026-09-12
- 状态：READY
- 依赖：MODEL-002（CanonicalType 枚举用于 CapabilityMatrix）

## file_ownership

- `src/chronoforge/connectors/base.py`（DataConnector Protocol）
- `src/chronoforge/connectors/errors.py`（错误类层级）
- `src/chronoforge/connectors/ratelimit.py`（RateLimiter）
- `src/chronoforge/connectors/retry.py`（retry 装饰器）

## 执行记录

| 时间 | 事件 |
|---|---|
| 2026-09-12 | 拆分为 3 个子任务：协议+错误映射→限流器→重试装饰器 |
| 2026-09-14 | ACQUISITION-001.1 验收通过 → DONE（49 passed in 0.27s）|

**子任务**：
- [x] [ACQUISITION-001.1](ACQUISITION-001.1.md) — DataConnector 协议 + 错误映射（DONE）
- [ ] [ACQUISITION-001.2](ACQUISITION-001.2.md) — 令牌桶限流器（READY）
- [ ] [ACQUISITION-001.3](ACQUISITION-001.3.md) — 重试装饰器 + HTTP 映射（READY）

## Deferred Acceptance

无。本子任务为 DATA-SOURCE-001~007 的基础依赖。
