# ACQUISITION-001.2 — 令牌桶限流器

## 派发信息

- 任务单：D10 §3 ACQUISITION-001 拆分（子任务 2/3）
- 设计依据：D04 §2（令牌桶限流器）
- Agent：Coding Agent（子会话）
- 派发时间：2026-09-12
- 状态：DONE
- 依赖：ACQUISITION-001.1（错误类、RawBatch 类型）

## file_ownership

- `src/chronoforge/connectors/ratelimit.py`（新建）
- `tests/unit/test_ratelimit.py`（新建）

## 交付物契约摘要

### RateLimiter 类（connectors/ratelimit.py）

```python
import time as _time

class RateLimiter:
    """令牌桶限流器（D04 §2）"""

    def __init__(self, rate: float, burst: int, clock: Callable[[], float] | None = None):
        """
        Args:
            rate: 每秒产生令牌数（req/s）
            burst: 桶容量（最大累积令牌数）
            clock: 可注入时钟（测试用），默认 time.monotonic
        """
        self.rate = rate
        self.burst = burst
        self._clock = clock or _time.monotonic
        self._tokens = float(burst)  # 初始满桶
        self._last_refill = self._clock()

    def acquire(self, n: int = 1) -> float:
        """
        获取 n 个令牌，阻塞至可用。
        Returns:
            等待秒数（0 = 立即可用）
        """
        ...

    def _refill(self) -> None:
        """按时间增量补充令牌（不超过 burst）"""
        ...
```

### 实现要求

- **可注入时钟**：`clock` 参数用于测试（D04 §2 明确要求）
- **令牌补充公式**：`new_tokens = min(burst, old_tokens + elapsed * rate)`
- **acquire 阻塞**：`sleep(needed_seconds)`（使用注入的 clock 时，clock 需支持 sleep 或返回实际等待量）
- **线程安全**：使用 `threading.Lock` 保护 `_tokens` 和 `_last_refill`（pipeline 单线程但 connector 可被并发调用）

### Binance 权重适配

- `acquire(weight: int)`：一次获取 weight 个令牌（Binance 按 endpoint 权重计权）
- 权重表由各 connector 实现定义（不在此模块硬编码）

### 429 全局冷却

- `RateLimiter` 不直接处理 429，但提供 `on_rate_limited(retry_after: int)` 方法供上层调用：
  ```python
  def on_rate_limited(self, retry_after: int) -> None:
      """收到 429 + Retry-After 时的全局冷却"""
      self._tokens = 0
      self._last_refill = self._clock()  # 重置，强制等待
  ```

## 测试要求（D09 TC-C 组）

- **令牌桶时钟注入**：固定时钟 → acquire(n) 连续调用阻塞时长符合令牌桶公式
- **边界**：burst=1（最小桶）、rate=0.1（低频）、n > burst（需等待 refill）
- **权重**：acquire(3) with burst=5 → 消耗 3 令牌，剩余 2
- **429 冷却**：on_rate_limited(5) → 下次 acquire 需等待 ≥5s
- **线程安全**：双线程并发 acquire → 不超发（_tokens ≥ 0 不变量）
- **属性**：acquire(A) + acquire(B) 总等待 ≈ acquire(A+B) 总等待（近似加法性，因 refill 间隔）

## acceptance（GWT）

- [x] Given fixed clock When acquire(1) called 10 times Then 第 1 次不阻塞，后续按 rate 间隔阻塞
- [x] Given burst=5 rate=1 When acquire(3) Then 剩余令牌=2
- [x] Given 双线程并发 acquire Then _tokens ≥ 0 不变量成立

## 验收清单（验收时填写）

DoD 16 项 + GWT 逐条勾选。

测试输出摘要：24 passed in 60.68s（pytest -v）｜ruff：0 errors｜mypy：0 errors｜接管性抽查：通过——仅凭任务单 + D04 §2 即可理解 RateLimiter 实现（时钟注入、令牌补充公式、线程安全、权重适配、429 冷却均与设计要求一一对应）｜git commit：待 Orchestrator 统一提交

## 执行记录

| 时间 | 事件 |
|---|---|
| 2026-09-14 | 实现完成：创建 ratelimit.py（RateLimiter 类，含线程安全 acquire/429 冷却/clock 注入），创建 test_ratelimit.py（24 tests） |
| 2026-09-14 | 验收完成：24 passed；令牌桶算法/时钟注入/边界条件/权重/429 冷却/线程安全/加法性全部闭环；ruff + mypy 无错误 |

## Deferred Acceptance

无。本子任务独立闭环，协议定义由 ACQUISITION-001.1 负责，重试装饰器由 ACQUISITION-001.3 负责。
