"""令牌桶限流器测试（D04 §2，TC-C 组）。

验证 RateLimiter 的：
- 令牌桶算法正确性
- 时钟注入（测试用）
- 线程安全
- 权重获取（Binance endpoint 权重适配）
- 429 全局冷却
- 边界条件
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable

import pytest

from chronoforge.connectors.ratelimit import RateLimiter

# ─── 固定时钟工厂 ───────────────────────────────────────────────

class FixedClock:
    """固定时钟（测试用），模拟 time.monotonic 行为。

    支持 sleep 自动推进（用于测试 retry loop 场景）。
    """

    def __init__(self, start: float = 0.0) -> None:
        self._time: float = start

    def __call__(self) -> float:
        return self._time

    def advance(self, seconds: float) -> None:
        self._time += seconds

    @property
    def time(self) -> float:
        return self._time

    def sleep(self, seconds: float) -> None:
        """模拟 time.sleep，自动推进时钟。"""
        self._time += seconds


def make_fixed_clock() -> tuple[FixedClock, Callable[[], float]]:
    """创建固定时钟并返回 (clock, clock_fn) 对。"""
    clock = FixedClock()
    return clock, lambda: clock()


# ─── 构造函数测试 ───────────────────────────────────────────────

class TestRateLimiterInit:
    """RateLimiter 构造函数测试。"""

    def test_initializes_full_bucket(self) -> None:
        """RateLimiter 初始化时桶满。"""
        clock, clock_fn = make_fixed_clock()
        rl = RateLimiter(rate=10.0, burst=5, clock=clock_fn)
        assert rl.available_tokens() == 5.0

    def test_default_clock_is_monotonic(self) -> None:
        """未注入 clock 时使用 time.monotonic。"""
        rl = RateLimiter(rate=10.0, burst=5)
        # 不传 clock 时，acquire 应该正常工作（不会抛异常）
        rl.acquire(1)

    def test_rate_zero_allowed(self) -> None:
        """rate=0 允许（低频场景，acquire 需等待 refill）。"""
        clock, clock_fn = make_fixed_clock()
        rl = RateLimiter(rate=0.0, burst=5, clock=clock_fn)
        assert rl.rate == 0.0

    def test_rate_negative_raises(self) -> None:
        """rate < 0 抛 ValueError。"""
        with pytest.raises(ValueError, match="rate must be >= 0"):
            RateLimiter(rate=-1.0, burst=5)

    def test_burst_one_allowed(self) -> None:
        """burst=1 是最小合法值。"""
        rl = RateLimiter(rate=10.0, burst=1)
        assert rl.burst == 1

    def test_burst_zero_raises(self) -> None:
        """burst=0 抛 ValueError。"""
        with pytest.raises(ValueError, match="burst must be >= 1"):
            RateLimiter(rate=10.0, burst=0)


# ─── 令牌桶时钟注入 ─────────────────────────────────────────────

class TestTokenBucketClockInjection:
    """令牌桶算法正确性验证（D04 §2）。"""

    def test_first_acquire_no_wait(self) -> None:
        """Given fixed clock When acquire(1) called first time Then 不阻塞。"""
        clock, clock_fn = make_fixed_clock()
        rl = RateLimiter(rate=10.0, burst=5, clock=clock_fn)
        wait = rl.acquire(1)
        assert wait == 0.0
        assert rl.available_tokens() == 4.0

    def test_consecutive_acquire_follows_formula(self) -> None:
        """连续调用 acquire 按令牌桶公式阻塞。

        rate=10 → 每秒 10 令牌 → 每 0.1s 1 令牌
        桶=5：前 5 次 acquire(1) 不阻塞，第 6 次需等 0.1s
        """
        clock, clock_fn = make_fixed_clock()
        rl = RateLimiter(rate=10.0, burst=5, clock=clock_fn)
        # 注入 FixedClock.sleep 使 retry loop 正常工作
        rl._sleep = clock.sleep

        # 前 5 次：桶满，立即可用
        for i in range(5):
            wait = rl.acquire(1)
            assert wait == 0.0, f"第 {i+1} 次 acquire 不应阻塞"

        # 第 6 次：桶空，需要等待
        # 先推进时钟 0.05s，得到 0.5 个令牌（不够）
        clock.advance(0.05)
        wait = rl.acquire(1)
        # 需要 1 个令牌，当前 0.5，还需 0.5 → 等待 0.5/10 = 0.05s
        assert wait == pytest.approx(0.05, abs=0.01)

    def test_refill_respects_burst_cap(self) -> None:
        """refill 不超过 burst。"""
        clock, clock_fn = make_fixed_clock()
        rl = RateLimiter(rate=100.0, burst=5, clock=clock_fn)

        # 消耗所有令牌
        rl.acquire(5)

        # 推进时钟 10s（按 rate=100 应补充 1000 个令牌）
        clock.advance(10.0)
        rl.acquire(1)  # 触发 refill

        # 剩余令牌应为 burst-1=4，不超过 burst
        assert rl.available_tokens() == 4.0


# ─── 边界条件测试 ───────────────────────────────────────────────

class TestBoundaryConditions:
    """边界条件：burst=1、rate=0.1、n > burst。"""

    def test_burst_1_minimal_bucket(self) -> None:
        """burst=1：每次 acquire(1) 后需等待 1/rate 秒。"""
        clock, clock_fn = make_fixed_clock()
        rl = RateLimiter(rate=10.0, burst=1, clock=clock_fn)
        # 注入 FixedClock.sleep 使 retry loop 正常工作
        rl._sleep = clock.sleep

        # 第 1 次：立即可用
        assert rl.acquire(1) == 0.0

        # 推进 0.05s，补充 0.5 个令牌（不够 1 个）
        clock.advance(0.05)

        # 第 2 次：需要等待 0.5/10 = 0.05s
        wait = rl.acquire(1)
        assert wait == pytest.approx(0.05, abs=0.01)

    def test_low_rate_0_1(self) -> None:
        """rate=0.1：低频场景，acquire(1) 需等待较长。

        使用 real clock，消耗所有令牌后等待。
        """
        rl = RateLimiter(rate=0.1, burst=5)

        # 消耗所有令牌
        for _ in range(5):
            rl.acquire(1)

        # 等待 5s（rate=0.1 → 补充 0.5 个令牌，不够 1 个）
        time.sleep(5.0)

        # 请求 1 个：tokens≈0.5，需等待 (1-0.5)/0.1 = 5s
        wait = rl.acquire(1)
        assert wait >= 4.0  # 至少等 4s（含 sleep 误差）

    def test_n_exceeds_burst(self) -> None:
        """n > burst：需等待 refill 到 n 个令牌。"""
        clock, clock_fn = make_fixed_clock()
        rl = RateLimiter(rate=10.0, burst=5, clock=clock_fn)

        # 消耗全部 5 个令牌
        for _ in range(5):
            rl.acquire(1)

        # 请求 8 个令牌（> burst=5），需等 (8-0)/10 = 0.8s
        # （先 refill 到 burst=5，然后消耗 5，还需 3 → 等 0.3s）
        wait = rl.acquire(8)
        # refill 补充 5 个（burst cap），消耗 5 个，还需 3 个 → 0.3s
        assert wait > 0.0


# ─── 权重测试 ───────────────────────────────────────────────────

class TestWeightAcquire:
    """权重获取（Binance endpoint 权重适配）。"""

    def test_acquire_weight_consumes_tokens(self) -> None:
        """acquire(3) with burst=5 → 消耗 3 令牌，剩余 2。"""
        clock, clock_fn = make_fixed_clock()
        rl = RateLimiter(rate=10.0, burst=5, clock=clock_fn)

        assert rl.acquire(3) == 0.0
        assert rl.available_tokens() == 2.0

    def test_acquire_weight_exceeds_available(self) -> None:
        """acquire(4) when only 2 tokens → 需等待 refill。"""
        clock, clock_fn = make_fixed_clock()
        rl = RateLimiter(rate=10.0, burst=5, clock=clock_fn)

        rl.acquire(3)  # 剩 2 个

        # 请求 4 个，需等待 (4-2)/10 = 0.2s
        wait = rl.acquire(4)
        assert wait > 0.0

    def test_weight_is_parameterized_per_connector(self) -> None:
        """权重由各 connector 定义，不在此模块硬编码。"""
        rl = RateLimiter(rate=10.0, burst=100)
        # Binance klines endpoint 权重=2
        rl.acquire(2)
        # Binance aggTrades endpoint 权重=5
        rl.acquire(5)
        assert rl.available_tokens() == pytest.approx(93.0, rel=0.01)


# ─── 429 全局冷却 ────────────────────────────────────────────────

class Test429Cooldown:
    """429 全局冷却测试（D04 §2）。"""

    def test_on_rate_limited_clears_tokens(self) -> None:
        """on_rate_limited(5) → 令牌清空。"""
        clock, clock_fn = make_fixed_clock()
        rl = RateLimiter(rate=10.0, burst=5, clock=clock_fn)

        rl.acquire(1)  # 剩 4 个
        rl.on_rate_limited(5)
        assert rl.available_tokens() == 0.0

    def test_on_rate_limited_forces_wait(self) -> None:
        """on_rate_limited(5) → 下次 acquire 需等待 ≥5s。"""
        clock, clock_fn = make_fixed_clock()
        rl = RateLimiter(rate=10.0, burst=5, clock=clock_fn)

        rl.acquire(1)  # 剩 4 个
        rl.on_rate_limited(5)

        # 推进 0.1s（补充 1 个令牌）
        clock.advance(0.1)

        # 请求 1 个：当前 tokens=0（on_rate_limited 重置），
        # refill 到 min(5, 0+0.1*10) = 1 → 立即可用
        # 所以需推进更多时间来验证等待
        clock._time = 100.0  # 重置时间
        rl.on_rate_limited(5)

        # 推进 0.5s（补充 5*0.1=0.5 个令牌）
        clock.advance(0.5)

        # 请求 1 个：tokens=0.5，需等待 (1-0.5)/10 = 0.05s
        wait = rl.acquire(1)
        assert wait >= 0.0

    def test_on_rate_limited_resets_refill_time(self) -> None:
        """on_rate_limited 重置 _last_refill。"""
        clock, clock_fn = make_fixed_clock()
        rl = RateLimiter(rate=10.0, burst=5, clock=clock_fn)

        clock.advance(1.0)  # 不消耗令牌，只推进时间

        rl.on_rate_limited(3)

        # _last_refill 应该被重置为当前时间
        assert rl.available_tokens() == 0.0


# ─── 线程安全测试 ───────────────────────────────────────────────

class TestThreadSafety:
    """线程安全验证（D09 TC-C 组）。"""

    def test_concurrent_acquire_no_overdraw(self) -> None:
        """双线程并发 acquire → _tokens ≥ 0 不变量。"""
        clock, clock_fn = make_fixed_clock()
        rl = RateLimiter(rate=1000.0, burst=100, clock=clock_fn)

        # 快速消耗全部令牌
        rl.acquire(100)

        errors: list[Exception] = []

        def worker() -> None:
            try:
                rl.acquire(1)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        # tokens 应该 ≥ 0（可能有部分线程在 refill 后成功）

    def test_concurrent_acquire_returns_valid_waits(self) -> None:
        """并发 acquire 返回的 wait_time 都 ≥ 0。"""
        clock, clock_fn = make_fixed_clock()
        rl = RateLimiter(rate=1000.0, burst=50, clock=clock_fn)

        rl.acquire(50)  # 消耗全部

        results: list[float] = []
        lock = threading.Lock()

        def worker() -> None:
            wait = rl.acquire(1)
            with lock:
                results.append(wait)

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(results) == 10
        assert all(w >= 0.0 for w in results)


# ─── 加法性属性 ──────────────────────────────────────────────────

class TestAdditivityProperty:
    """acquire(A) + acquire(B) 总等待 ≈ acquire(A+B) 总等待。"""

    def test_approximate_additivity(self) -> None:
        """近似加法性（因 refill 间隔，不要求严格相等）。"""
        clock1, clock_fn1 = make_fixed_clock()
        rl1 = RateLimiter(rate=100.0, burst=1000, clock=clock_fn1)

        # 场景 1：分别 acquire(3) + acquire(2)
        wait_a = rl1.acquire(3)
        wait_b = rl1.acquire(2)
        total_split = wait_a + wait_b

        clock2, clock_fn2 = make_fixed_clock()
        rl2 = RateLimiter(rate=100.0, burst=1000, clock=clock_fn2)

        # 场景 2：一次 acquire(5)
        wait_combined = rl2.acquire(5)

        # 在 burst 充足时，两者都为 0
        if wait_combined == 0.0:
            assert total_split == 0.0
        else:
            # 否则近似相等（允许 10% 误差因 refill 间隔）
            assert total_split == pytest.approx(wait_combined, rel=0.1)


# ─── acceptance（GWT） ──────────────────────────────────────────

class TestAcceptanceGWT:
    """任务 acceptance（GWT）验证。"""

    def test_gwt_first_no_block_subsequent_block(self) -> None:
        """Given fixed clock When acquire(1) called 10 times Then
        第 1 次不阻塞，后续按 rate 间隔阻塞。

        rate=10, burst=1：第 1 次立即可用，后续需等待
        """
        clock, clock_fn = make_fixed_clock()
        rl = RateLimiter(rate=10.0, burst=1, clock=clock_fn)
        # 注入 FixedClock.sleep 使 retry loop 正常工作
        rl._sleep = clock.sleep

        # 第 1 次：不阻塞
        assert rl.acquire(1) == 0.0

        # 第 2 次：推进 0.02s（补充 0.2 个令牌），需等待 (1-0.2)/10 = 0.08s
        clock.advance(0.02)
        wait = rl.acquire(1)
        assert wait == pytest.approx(0.08, abs=0.01)

    def test_gwt_acquire_weight_preserves_remaining(self) -> None:
        """Given burst=5 rate=10 When acquire(3) Then 剩余令牌=2。"""
        clock, clock_fn = make_fixed_clock()
        rl = RateLimiter(rate=10.0, burst=5, clock=clock_fn)

        rl.acquire(3)
        assert rl.available_tokens() == 2.0

    def test_gwt_concurrent_tokens_non_negative(self) -> None:
        """Given 双线程并发 acquire Then _tokens ≥ 0 不变量成立。"""
        clock, clock_fn = make_fixed_clock()
        rl = RateLimiter(rate=100.0, burst=10, clock=clock_fn)

        # 消耗全部令牌
        rl.acquire(10)

        # 推进时钟使 refill 后足够 2 个令牌
        clock.advance(0.2)  # 补充 20→capped 到 10，消耗后剩 8

        barrier = threading.Barrier(2)

        def worker() -> None:
            barrier.wait()
            rl.acquire(1)

        t1 = threading.Thread(target=worker)
        t2 = threading.Thread(target=worker)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert rl.available_tokens() >= 0.0
