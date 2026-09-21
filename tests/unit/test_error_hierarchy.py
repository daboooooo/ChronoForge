"""ChronoForge 错误层级测试（D01 §3，IMP-002）。

验证 9 个异常类的完整层级、继承关系、context dict。
"""

from chronoforge.connectors.errors import (
    AuthError,
    ChronoForgeError,
    ConfigError,
    ProviderError,
    QualityError,
    RateLimitError,
    SchemaError,
    TransportError,
)
from chronoforge.exceptions import StorageError


class TestChronoForgeErrorHierarchy:
    """全部异常继承 ChronoForgeError 根类（D01 §3）。"""

    def test_chronoforge_error_is_exception(self) -> None:
        """ChronoForgeError 继承 Exception。"""
        assert issubclass(ChronoForgeError, Exception)

    def test_all_errors_inherit_chronoforge_error(self) -> None:
        """所有异常类继承 ChronoForgeError。"""
        direct_subclasses = (
            AuthError, ConfigError, ProviderError,
            QualityError, TransportError, StorageError,
        )
        for cls in direct_subclasses:
            assert issubclass(
                cls, ChronoForgeError
            ), f"{cls.__name__} must inherit ChronoForgeError"

        # Indirect: RateLimitError inherits TransportError
        assert issubclass(RateLimitError, ChronoForgeError)
        assert issubclass(RateLimitError, TransportError)

        # SchemaError inherits ChronoForgeError
        assert issubclass(SchemaError, ChronoForgeError)

    def test_transport_error_inherits_chronoforge_error(self) -> None:
        """TransportError 继承 ChronoForgeError。"""
        assert issubclass(TransportError, ChronoForgeError)

    def test_ratelimit_error_inherits_transport_error(self) -> None:
        """RateLimitError 继承 TransportError。"""
        assert issubclass(RateLimitError, TransportError)

    def test_storage_error_inherits_chronoforge_error(self) -> None:
        """StorageError 继承 ChronoForgeError。"""
        assert issubclass(StorageError, ChronoForgeError)

    def test_auth_error_inherits_chronoforge_error(self) -> None:
        """AuthError 继承 ChronoForgeError。"""
        assert issubclass(AuthError, ChronoForgeError)

    def test_provider_error_inherits_chronoforge_error(self) -> None:
        """ProviderError 继承 ChronoForgeError。"""
        assert issubclass(ProviderError, ChronoForgeError)

    def test_schema_error_inherits_chronoforge_error(self) -> None:
        """SchemaError 继承 ChronoForgeError。"""
        assert issubclass(SchemaError, ChronoForgeError)

    def test_quality_error_inherits_chronoforge_error(self) -> None:
        """QualityError 继承 ChronoForgeError。"""
        assert issubclass(QualityError, ChronoForgeError)

    def test_config_error_inherits_chronoforge_error(self) -> None:
        """ConfigError 继承 ChronoForgeError。"""
        assert issubclass(ConfigError, ChronoForgeError)


class TestErrorContextDict:
    """所有异常携带 context: dict（D01 §3）。"""

    def test_chronoforge_error_has_context(self) -> None:
        """ChronoForgeError 携带 context dict。"""
        ctx = {"source": "test", "dataset": "ds1"}
        err = ChronoForgeError("test error", context=ctx)
        assert err.context == ctx

    def test_chronoforge_error_context_defaults_empty(self) -> None:
        """ChronoForgeError context 默认为空 dict。"""
        err = ChronoForgeError("test error")
        assert err.context == {}

    def test_storage_error_has_context(self) -> None:
        """StorageError 携带 context dict。"""
        ctx = {"source": "sqlite"}
        err = StorageError("db error", context=ctx)
        assert err.context == ctx

    def test_provider_error_has_context(self) -> None:
        """ProviderError 携带 context dict。"""
        ctx = {"source": "binance"}
        err = ProviderError("API error", context=ctx)
        assert err.context == ctx

    def test_error_messages_preserved(self) -> None:
        """异常消息通过 Exception.__init__ 保存。"""
        err = ChronoForgeError("specific error message")
        assert str(err) == "specific error message"


class TestIndividualExceptionClasses:
    """各异常类可独立实例化。"""

    def test_transport_error_instantiable(self) -> None:
        """TransportError 可独立实例化。"""
        err = TransportError("connection failed")
        assert isinstance(err, TransportError)
        assert isinstance(err, ChronoForgeError)

    def test_ratelimit_error_instantiable(self) -> None:
        """RateLimitError 可独立实例化。"""
        err = RateLimitError("rate limited")
        assert isinstance(err, RateLimitError)
        assert isinstance(err, TransportError)

    def test_auth_error_instantiable(self) -> None:
        """AuthError 可独立实例化。"""
        err = AuthError("invalid token")
        assert isinstance(err, AuthError)

    def test_provider_error_instantiable(self) -> None:
        """ProviderError 可独立实例化。"""
        err = ProviderError("404 not found")
        assert isinstance(err, ProviderError)

    def test_schema_error_instantiable(self) -> None:
        """SchemaError 可独立实例化。"""
        err = SchemaError("unexpected field")
        assert isinstance(err, SchemaError)

    def test_quality_error_instantiable(self) -> None:
        """QualityError 可独立实例化。"""
        err = QualityError("value out of range")
        assert isinstance(err, QualityError)

    def test_config_error_instantiable(self) -> None:
        """ConfigError 可独立实例化。"""
        err = ConfigError("missing env var")
        assert isinstance(err, ConfigError)
