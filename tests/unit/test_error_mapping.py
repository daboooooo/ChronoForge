"""HTTP→错误映射测试（D04 §3，TC-C 组）。

验证 map_http_status 函数全覆盖 + RateLimitError.retry_after。
"""

from __future__ import annotations

import pytest

from chronoforge.connectors.errors import (
    AuthError,
    ChronoForgeError,
    ProviderError,
    RateLimitError,
    TransportError,
    map_http_status,
)


class TestMapHttpStatusFullCoverage:
    """map_http_status 全覆盖（D04 §3 映射表）。"""

    def test_401_returns_auth_error(self) -> None:
        """TC-C-009：401 → AuthError（不重试）。"""
        error_cls = map_http_status(401)
        assert error_cls is AuthError

    def test_403_returns_auth_error(self) -> None:
        """403 → AuthError（不重试）。"""
        error_cls = map_http_status(403)
        assert error_cls is AuthError

    def test_429_returns_ratelimit_error(self) -> None:
        """TC-C-008：429 → RateLimitError（可重试）。"""
        error_cls = map_http_status(429)
        assert error_cls is RateLimitError

    def test_400_returns_provider_error(self) -> None:
        """400 → ProviderError（不重试）。"""
        error_cls = map_http_status(400)
        assert error_cls is ProviderError

    def test_404_returns_provider_error(self) -> None:
        """404 → ProviderError（不重试）。"""
        error_cls = map_http_status(404)
        assert error_cls is ProviderError

    def test_422_returns_provider_error(self) -> None:
        """422 → ProviderError（不重试）。"""
        error_cls = map_http_status(422)
        assert error_cls is ProviderError

    def test_500_returns_transport_error(self) -> None:
        """500 → TransportError（可重试）。"""
        error_cls = map_http_status(500)
        assert error_cls is TransportError

    def test_502_returns_transport_error(self) -> None:
        """502 → TransportError（可重试）。"""
        error_cls = map_http_status(502)
        assert error_cls is TransportError

    def test_503_returns_transport_error(self) -> None:
        """503 → TransportError（可重试）。"""
        error_cls = map_http_status(503)
        assert error_cls is TransportError

    def test_custom_4xx_returns_provider_error(self) -> None:
        """自定义 4xx（如 418）→ ProviderError（不重试）。"""
        error_cls = map_http_status(418)
        assert error_cls is ProviderError

    def test_custom_4xx_410_returns_provider_error(self) -> None:
        """410 Gone → ProviderError。"""
        error_cls = map_http_status(410)
        assert error_cls is ProviderError

    def test_200_returns_transport_error(self) -> None:
        """2xx（非 4xx/5xx）→ TransportError。"""
        error_cls = map_http_status(200)
        assert error_cls is TransportError

    def test_301_returns_transport_error(self) -> None:
        """3xx → TransportError。"""
        error_cls = map_http_status(301)
        assert error_cls is TransportError


class TestRateLimitErrorRetryAfter:
    """TC-C-008：429 → RateLimitError（携带 retry_after）。"""

    def test_ratelimit_error_carries_retry_after(self) -> None:
        """429 → RateLimitError 携带 retry_after。"""
        error_cls = map_http_status(429)
        error = error_cls(
            "Too Many Requests",
            context={
                "source": "binance",
                "dataset": "BTCUSDT",
                "endpoint": "/klines",
                "status": 429,
            },
            retry_after=5,
        )
        assert isinstance(error, RateLimitError)
        assert error.retry_after == 5

    def test_ratelimit_error_without_retry_after(self) -> None:
        """RateLimitError 可以没有 retry_after。"""
        error_cls = map_http_status(429)
        error = error_cls(
            "Rate limited",
            context={"source": "deribit"},
        )
        assert isinstance(error, RateLimitError)
        assert error.retry_after is None

    def test_ratelimit_error_inherits_transport_error(self) -> None:
        """RateLimitError 同时是 TransportError 子类型。"""
        error = RateLimitError("test", retry_after=10)
        assert isinstance(error, TransportError)
        assert isinstance(error, ChronoForgeError)


class TestAuthErrorNoRetry:
    """TC-C-009：401 → AuthError（不重试）。"""

    def test_auth_error_context(self) -> None:
        """AuthError context 携带 source/dataset/endpoint。"""
        error_cls = map_http_status(401)
        error = error_cls(
            "Unauthorized",
            context={"source": "binance", "dataset": "BTCUSDT", "endpoint": "/account"},
        )
        assert error.context["source"] == "binance"
        assert error.context["dataset"] == "BTCUSDT"
        assert error.context["endpoint"] == "/account"
        assert isinstance(error, AuthError)
        assert isinstance(error, ChronoForgeError)

    def test_auth_error_not_retryable(self) -> None:
        """AuthError 不是 TransportError 子类型。"""
        error = AuthError("Forbidden", context={"source": "test"})
        assert not isinstance(error, TransportError)
        assert isinstance(error, ChronoForgeError)


class TestProviderErrorNoRetry:
    """TC-C-010：4xx≠429 → ProviderError（不重试）。"""

    def test_provider_error_context(self) -> None:
        """ProviderError context 携带 source/dataset/endpoint。"""
        error_cls = map_http_status(422)
        error = error_cls(
            "Invalid symbol",
            context={"source": "deribit", "dataset": "BTC", "endpoint": "/instruments"},
        )
        assert error.context["source"] == "deribit"
        assert error.context["dataset"] == "BTC"
        assert isinstance(error, ProviderError)
        assert isinstance(error, ChronoForgeError)

    def test_provider_error_not_retryable(self) -> None:
        """ProviderError 不是 TransportError 子类型。"""
        error = ProviderError("Not found", context={"source": "yahoo"})
        assert not isinstance(error, TransportError)
        assert isinstance(error, ChronoForgeError)


class TestTransportErrorRetryable:
    """TransportError 可重试。"""

    def test_transport_error_context(self) -> None:
        """TransportError context 携带 source/dataset/endpoint。"""
        error_cls = map_http_status(500)
        error = error_cls(
            "Internal Server Error",
            context={
                "source": "binance",
                "dataset": "BTCUSDT",
                "endpoint": "/klines",
                "status": 500,
            },
        )
        assert error.context["source"] == "binance"
        assert isinstance(error, TransportError)
        assert isinstance(error, ChronoForgeError)

    def test_transport_error_inherits_correctly(self) -> None:
        """TransportError 直接继承 ChronoForgeError。"""
        error = TransportError("Connection timeout", context={"source": "fred"})
        assert isinstance(error, TransportError)
        assert isinstance(error, ChronoForgeError)


class TestAllErrorsHaveContext:
    """所有错误类携带 context dict（D01 §3）。"""

    @pytest.mark.parametrize(
        "status_code,error_type",
        [
            (401, AuthError),
            (403, AuthError),
            (429, RateLimitError),
            (400, ProviderError),
            (404, ProviderError),
            (422, ProviderError),
            (418, ProviderError),
            (500, TransportError),
            (502, TransportError),
            (503, TransportError),
            (200, TransportError),
        ],
    )
    def test_error_class_returns_correct_type(self, status_code: int, error_type: type) -> None:
        """map_http_status 返回正确的错误类。"""
        assert map_http_status(status_code) is error_type

    def test_error_context_defaults_to_empty_dict(self) -> None:
        """不传 context 时默认为空 dict。"""
        error = AuthError("test")
        assert error.context == {}

    def test_error_message_preserved(self) -> None:
        """错误消息通过 Exception 基类正确保存。"""
        error = AuthError("Unauthorized: invalid API key")
        assert str(error) == "Unauthorized: invalid API key"
