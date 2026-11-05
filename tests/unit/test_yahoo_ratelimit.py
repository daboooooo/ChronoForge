"""H5：Yahoo 限流公式与 429/Retry-After 行为回归（修复前应失败）。

事实基线：
- 契约速率 = 30 req/min = 0.5 req/s（_DEFAULT_RATE）。
- 429 必须尊重 Retry-After（数字或 HTTP-date），并触发全局限流冷却。
- 重试沿用 connectors.retry（1→2→4→8→16s + jitter，Retry-After 下限）。
"""

from __future__ import annotations

import threading
import time
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from unittest.mock import MagicMock, patch

import httpx
import pytest

from chronoforge.connectors.errors import RateLimitError, TransportError
from chronoforge.connectors.retry import retry
from chronoforge.connectors.yahoo import YahooConnector, _YahooSettings

_RETRY_MOD = "chronoforge.connectors.retry"


def _conn() -> YahooConnector:
    conn = YahooConnector(_YahooSettings())
    conn._client = MagicMock()
    conn._crumb = "test-crumb"
    return conn


def _resp(status: int, headers: dict[str, str] | None = None) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status
    # 与 httpx 运行时一致：响应头大小写不敏感
    resp.headers = httpx.Headers(headers or {})
    resp.text = "crumb"
    return resp


# ── ① 速率公式 ────────────────────────────────────────────────────────


def test_rate_limit_formula_is_30_requests_per_minute() -> None:
    """契约 30 req/min；不得随 http_timeout_s 放大。"""
    conn = _conn()
    assert conn._rate_limiter.rate == pytest.approx(0.5)
    assert conn._rate_limiter.burst == 5


# ── ② Retry-After 尊重 ────────────────────────────────────────────────


def test_chart_429_uses_numeric_retry_after_and_cools_down() -> None:
    conn = _conn()
    conn._client.get.return_value = _resp(429, {"Retry-After": "120"})
    with pytest.raises(RateLimitError) as exc_info:
        conn._fetch_chart("AAPL", 0, 1, "1d")
    assert exc_info.value.retry_after == 120
    # 429 后必须清空令牌，避免并发线程继续透支（允许极小的补充误差）
    assert conn._rate_limiter.available_tokens() < 0.01


def test_crumb_429_uses_header_retry_after_instead_of_hardcoded_60() -> None:
    conn = _conn()
    conn._crumb = None
    conn._client.get.side_effect = [
        _resp(404),  # fc.yahoo.com
        _resp(429, {"Retry-After": "90"}),  # getcrumb
    ]
    with pytest.raises(RateLimitError) as exc_info:
        conn._ensure_crumb()
    assert exc_info.value.retry_after == 90
    assert conn._rate_limiter.available_tokens() < 0.01


def test_chart_429_accepts_http_date_retry_after() -> None:
    conn = _conn()
    target = datetime.now(UTC) + timedelta(seconds=120)
    conn._client.get.return_value = _resp(
        429, {"Retry-After": format_datetime(target, usegmt=True)}
    )
    with pytest.raises(RateLimitError) as exc_info:
        conn._fetch_chart("AAPL", 0, 1, "1d")
    assert exc_info.value.retry_after is not None
    assert 100 <= exc_info.value.retry_after <= 121


def test_retry_honours_rate_limit_retry_after() -> None:
    sleeps: list[float] = []
    with (
        patch(f"{_RETRY_MOD}._sleep", side_effect=sleeps.append),
        patch(f"{_RETRY_MOD}.random.uniform", return_value=0.0),
    ):
        @retry(max_retries=2)
        def always_429() -> None:
            raise RateLimitError("429", retry_after=7)

        with pytest.raises(RateLimitError) as exc_info:
            always_429()
    assert sleeps == [7.0, 7.0]
    assert exc_info.value.retry_after == 7


# ── ③ 退避序列 / 抖动 ─────────────────────────────────────────────────


def test_exponential_backoff_sequence_with_jitter() -> None:
    sleeps: list[float] = []
    calls = {"n": 0}

    with (
        patch(f"{_RETRY_MOD}._sleep", side_effect=sleeps.append),
        patch(f"{_RETRY_MOD}.random.uniform", return_value=0.25),
    ):
        @retry(max_retries=3)
        def flaky() -> str:
            calls["n"] += 1
            if calls["n"] < 4:
                raise TransportError("boom")
            return "ok"

        assert flaky() == "ok"
    assert sleeps == [1.25, 2.25, 4.25]


# ── ④ 并发实际速率不超契约 ────────────────────────────────────────────


def test_concurrent_acquire_stays_within_contract_rate() -> None:
    """4 线程并发 1.2s：总量不超过 burst + rate*窗口（+容差）。"""
    conn = _conn()
    count = 0
    lock = threading.Lock()
    deadline = time.monotonic() + 1.2

    def worker() -> None:
        nonlocal count
        while time.monotonic() < deadline:
            conn._rate_limiter.acquire()
            with lock:
                count += 1

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # burst=5 + 0.5 req/s * 1.2s ≈ 5.6；容差放宽到 9
    assert count <= 9, f"实际 {count} 次/1.2s，超过 30 req/min 契约"


# ── ⑤ 重试耗尽错误可诊断 ──────────────────────────────────────────────


def test_retry_exhaustion_keeps_diagnosable_rate_limit_error() -> None:
    sleeps: list[float] = []
    with (
        patch(f"{_RETRY_MOD}._sleep", side_effect=sleeps.append),
        patch(f"{_RETRY_MOD}.random.uniform", return_value=0.0),
    ):
        @retry(max_retries=1)
        def always_429() -> None:
            raise RateLimitError("yahoo: rate limited on chart", retry_after=60)

        with pytest.raises(RateLimitError) as exc_info:
            always_429()
    assert exc_info.value.retry_after == 60
    assert "rate limited" in str(exc_info.value)
    assert sleeps == [60.0]
