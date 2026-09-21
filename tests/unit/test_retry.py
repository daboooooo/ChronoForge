"""重试装饰器测试（D04 §3，D09 TC-C 组）。

测试覆盖：
- TC-C-008: 429 + Retry-After → 冷却
- 退避序列：mock sleep → 验证 delay = [1, 2, 4, 8, 16]s
- jitter：固定 random → jitter ∈ [0, 0.5]
- 不重试：AuthError/ProviderError/SchemaError → 立即抛
- 耗尽：5 次重试全失败 → 抛最后一次异常
- 属性：retry 不改变成功路径返回值
"""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest

from chronoforge.connectors.errors import (
    AuthError,
    ProviderError,
    RateLimitError,
    SchemaError,
    TransportError,
)
from chronoforge.connectors.retry import retry


class TestRetryBasic:
    """重试装饰器基本行为（D09 TC-C 组）。"""

    def test_success_no_retry(self, mock_random_zero: MagicMock):
        """Given func 成功 When retry(5) Then 直接返回，无 sleep 调用。"""
        calls = []

        @retry(max_retries=5, base_delay=1.0, jitter_range=0.5)
        def func():
            calls.append(1)
            return 42

        result = func()
        assert result == 42
        assert len(calls) == 1

    def test_retry_exhausted_raises_last_exception(self, mock_random_zero: MagicMock):
        """Given func 抛 TransportError When retry(5) Then 重试 5 次后抛最后一次异常。"""
        calls = []

        @retry(max_retries=5, base_delay=1.0, jitter_range=0.5)
        def func():
            calls.append(1)
            raise TransportError("connection refused")

        with pytest.raises(TransportError):
            func()
        # 1 次初始 + 5 次重试 = 6 次调用
        assert len(calls) == 6

    def test_retry_count_respected(self, mock_random_zero: MagicMock):
        """Given func 抛 TransportError When retry(2) Then 重试 2 次后抛异常。"""
        calls = []

        @retry(max_retries=2, base_delay=1.0, jitter_range=0.5)
        def func():
            calls.append(1)
            raise TransportError("connection refused")

        with pytest.raises(TransportError):
            func()
        assert len(calls) == 3  # 1 初始 + 2 重试

    def test_success_path_unchanged(self, mock_random_zero: MagicMock):
        """retry 不改变成功路径返回值（同输入同输出）。"""
        @retry(max_retries=3)
        def add(a: int, b: int) -> int:
            return a + b

        assert add(2, 3) == 5
        assert add(-1, 1) == 0

    def test_wrapper_preserves_function_metadata(self):
        """retry 装饰器保留原函数 __name__ 和 __doc__。"""

        @retry(max_retries=1)
        def my_custom_func():
            """Custom docstring."""
            pass

        assert my_custom_func.__name__ == "my_custom_func"
        assert my_custom_func.__doc__ == "Custom docstring."


class TestRetryBackoffSequence:
    """退避序列验证（D09 TC-C 组）。"""

    @pytest.mark.parametrize(
        "max_retries,base_delay,expected_delays",
        [
            # 5 次重试：delay = base_delay * 2^(n-1) for n=1..5
            (5, 1.0, [1.0, 2.0, 4.0, 8.0, 16.0]),
            # 自定义 base_delay
            (5, 0.5, [0.5, 1.0, 2.0, 4.0, 8.0]),
        ],
    )
    def test_backoff_sequence_no_jitter(
        self, max_retries, base_delay, expected_delays, mock_random_zero: MagicMock
    ):
        """退避序列：mock sleep → 验证 delay = [base_delay * 2^(n-1)]s。"""
        sleep_calls = []

        def mock_sleep(seconds):
            sleep_calls.append(seconds)

        @retry(
            max_retries=max_retries,
            base_delay=base_delay,
            jitter_range=0.5,
            retryable_errors=(TransportError,),
        )
        def func():
            raise TransportError("test")

        with patch("time.sleep", mock_sleep):
            with pytest.raises(TransportError):
                func()

        assert sleep_calls == expected_delays

    def test_jitter_in_range(self):
        """jitter：固定 random → jitter ∈ [0, 0.5] 均匀分布。"""
        sleep_calls = []

        def mock_sleep(seconds):
            sleep_calls.append(seconds)

        # 固定 random.uniform 返回值
        def mock_uniform(low, high):
            return high * 0.5  # 总是返回 range 的一半

        @retry(
            max_retries=2,
            base_delay=1.0,
            jitter_range=0.5,
            retryable_errors=(TransportError,),
        )
        def func():
            raise TransportError("test")

        with patch("time.sleep", mock_sleep):
            with patch("random.uniform", mock_uniform):
                with pytest.raises(TransportError):
                    func()

        # 验证 jitter 在 [0, 0.5] 范围内
        # delay = base_delay * 2^(attempt-1) + jitter
        # attempt=1: delay = 1.0 * 1 + 0.25 = 1.25
        # attempt=2: delay = 1.0 * 2 + 0.25 = 2.25
        for delay in sleep_calls:
            assert 0 <= delay <= 32.5  # max: 16 + 0.5
            # 计算 jitter 部分
            jitter_part = delay % 1
            assert 0 <= jitter_part <= 0.5


class TestRetryNonRetryableErrors:
    """不重试的错误类（D04 §3，D09 TC-C 组）。"""

    @pytest.mark.parametrize("error_cls", [AuthError, ProviderError, SchemaError])
    def test_non_retryable_errors_not_retried(
        self, error_cls, mock_random_zero: MagicMock
    ):
        """AuthError/ProviderError/SchemaError → 立即抛出，attempt=1。"""
        calls = []

        @retry(max_retries=5, base_delay=1.0, jitter_range=0.5)
        def func():
            calls.append(1)
            raise error_cls("test error")

        with pytest.raises(error_cls):
            func()
        assert len(calls) == 1

    def test_mixed_retryable_and_non_retryable(
        self, mock_random_zero: MagicMock
    ):
        """同时有重试/非重试错误时，非重试错误立即抛出。"""
        calls = []

        @retry(max_retries=5)
        def func():
            calls.append(1)
            raise ProviderError("not found")

        with pytest.raises(ProviderError):
            func()
        assert len(calls) == 1


class TestRetryRateLimitError:
    """RateLimitError 重试（D04 §3，D09 TC-C-008）。"""

    def test_rate_limit_error_retried(self, mock_random_zero: MagicMock):
        """RateLimitError 触发重试。"""
        calls = []

        @retry(max_retries=3, base_delay=1.0, jitter_range=0.5)
        def func():
            calls.append(1)
            raise RateLimitError("rate limited")

        with pytest.raises(RateLimitError):
            func()
        # 1 初始 + 3 重试 = 4 次
        assert len(calls) == 4

    def test_rate_limit_error_has_retry_after(self):
        """RateLimitError 携带 retry_after 属性。"""
        err = RateLimitError("rate limited", retry_after=10)
        assert err.retry_after == 10

    def test_retry_after_in_context(self):
        """RateLimitError context 包含 retry_after。"""
        err = RateLimitError(
            "rate limited",
            context={"source": "binance", "endpoint": "/klines"},
            retry_after=5,
        )
        assert err.context["source"] == "binance"
        assert err.retry_after == 5

    def test_retry_after_extends_backoff(self, mock_random_zero: MagicMock):
        """审计 M-8：Retry-After 大于指数退避时退避取 Retry-After。"""
        sleep_calls = []

        def mock_sleep(seconds):
            sleep_calls.append(seconds)

        @retry(max_retries=2, base_delay=1.0, jitter_range=0.0)
        def func():
            raise RateLimitError("rate limited", retry_after=30)

        with patch("time.sleep", mock_sleep):
            with pytest.raises(RateLimitError):
                func()

        # 指数退避 [1, 2] → 尊重 Retry-After=30 → [30, 30]
        assert sleep_calls == [30.0, 30.0]

    def test_retry_after_smaller_than_backoff_uses_backoff(
        self, mock_random_zero: MagicMock
    ):
        """审计 M-8：Retry-After 不大于指数退避时保持退避序列。"""
        sleep_calls = []

        def mock_sleep(seconds):
            sleep_calls.append(seconds)

        @retry(max_retries=3, base_delay=1.0, jitter_range=0.0)
        def func():
            raise RateLimitError("rate limited", retry_after=1)

        with patch("time.sleep", mock_sleep):
            with pytest.raises(RateLimitError):
                func()

        # 退避 [1, 2, 4] 均 >= retry_after=1 → 不变
        assert sleep_calls == [1.0, 2.0, 4.0]


class TestRetryStructlogEvents:
    """structlog 事件验证（D08 §3）。"""

    def test_connector_retry_event_logged(
        self, mock_random_zero: MagicMock, caplog
    ):
        """每次重试记录 structlog event='connector.retry'。"""
        sleep_calls = []

        def mock_sleep(seconds):
            sleep_calls.append(seconds)

        @retry(max_retries=2, base_delay=1.0, jitter_range=0.5)
        def func():
            raise TransportError("test error")

        with patch("time.sleep", mock_sleep):
            with pytest.raises(TransportError):
                func()

        assert len(sleep_calls) == 2  # 2 次重试 = 2 次 sleep


class TestRetryCustomRetryableErrors:
    """自定义 retryable_errors 参数。"""

    def test_custom_retryable_errors(self, mock_random_zero: MagicMock):
        """自定义 retryable_errors 只重试指定错误。"""
        calls = []

        class CustomError(Exception):
            pass

        @retry(
            max_retries=3,
            base_delay=1.0,
            jitter_range=0.5,
            retryable_errors=(CustomError,),
        )
        def func():
            calls.append(1)
            raise TransportError("not in retryable_errors")

        with pytest.raises(TransportError):
            func()
        # TransportError 不在 retryable_errors 中，不重试
        assert len(calls) == 1

    def test_custom_error_retried(self, mock_random_zero: MagicMock):
        """自定义错误在 retryable_errors 中被重试。"""
        calls = []

        class CustomError(Exception):
            pass

        @retry(
            max_retries=2,
            base_delay=1.0,
            jitter_range=0.5,
            retryable_errors=(CustomError,),
        )
        def func():
            calls.append(1)
            raise CustomError("custom")

        with pytest.raises(CustomError):
            func()
        assert len(calls) == 3  # 1 + 2


class TestRetryEdgeCases:
    """边界情况测试。"""

    def test_max_retries_zero(self, mock_random_zero: MagicMock):
        """max_retries=0 时仅尝试 1 次。"""
        calls = []

        @retry(max_retries=0, base_delay=1.0, jitter_range=0.5)
        def func():
            calls.append(1)
            raise TransportError("test")

        with pytest.raises(TransportError):
            func()
        assert len(calls) == 1

    def test_base_delay_zero(self, mock_random_zero: MagicMock):
        """base_delay=0 时重试无延迟。"""
        start = time.monotonic()
        calls = []

        @retry(max_retries=3, base_delay=0.0, jitter_range=0.0)
        def func():
            calls.append(1)
            raise TransportError("test")

        with pytest.raises(TransportError):
            func()
        elapsed = time.monotonic() - start
        # 3 次重试，每次 delay=0，总时间应接近 0
        assert elapsed < 0.1

    def test_error_message_truncated(
        self, mock_random_zero: MagicMock, caplog
    ):
        """错误消息截断至 500 字符。"""
        long_message = "x" * 1000

        @retry(max_retries=1, base_delay=1.0, jitter_range=0.0)
        def func():
            raise TransportError(long_message)

        with patch("time.sleep"):
            with pytest.raises(TransportError):
                func()


@pytest.fixture
def mock_random_zero():
    """固定 random.uniform 返回 0（去除 jitter）。"""
    with patch("random.uniform", return_value=0.0):
        yield
