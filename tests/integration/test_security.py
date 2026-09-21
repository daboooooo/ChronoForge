"""安全模块测试（D08 §4/§5，TC-SEC 组）。

测试:
- SecretStr: __repr__/__str__ 脱敏, get_secret_value, == 比较, hash
- redact(): 递归脱敏 dict/list/URL
- Settings.load(): 环境变量加载, SecretStr 字段, derived 字段
- FRED connector: Settings.load() 自动加载
- CCXT bridge: Settings.load() 自动加载
"""

from __future__ import annotations

from pathlib import Path

import pytest

from chronoforge.config.settings import Settings
from chronoforge.security import REDACT_KEYS, SecretStr, redact

# ── SecretStr 测试 ──


class TestSecretStr:
    """SecretStr 类单元测试（TC-SEC-001）。"""

    def test_repr_redacted(self) -> None:
        secret = SecretStr("abc123")
        assert "******" in repr(secret)

    def test_str_redacted(self) -> None:
        secret = SecretStr("abc123")
        assert str(secret) == "******"

    def test_get_secret_value(self) -> None:
        secret = SecretStr("abc123")
        assert secret.get_secret_value() == "abc123"

    def test_eq_with_string(self) -> None:
        secret = SecretStr("abc123")
        assert secret == "abc123"

    def test_eq_with_secretstr(self) -> None:
        s1 = SecretStr("abc123")
        s2 = SecretStr("abc123")
        assert s1 == s2

    def test_eq_with_different_values(self) -> None:
        s1 = SecretStr("abc123")
        s2 = SecretStr("def456")
        assert s1 != s2

    def test_hash(self) -> None:
        secret = SecretStr("abc123")
        assert hash(secret) == hash("abc123")

    def test_hash_in_set(self) -> None:
        secret1 = SecretStr("abc123")
        secret2 = SecretStr("abc123")
        s = {secret1}
        assert secret2 in s

    def test_len(self) -> None:
        secret = SecretStr("abc123")
        assert len(secret) == 6  # "abc123" is 6 chars

    def test_bool_empty(self) -> None:
        secret = SecretStr("")
        assert not secret

    def test_bool_nonempty(self) -> None:
        secret = SecretStr("abc")
        assert secret

    def test_startswith(self) -> None:
        secret = SecretStr("nvapi-abc123")
        assert secret.startswith("nvapi-")

    def test_endswith(self) -> None:
        secret = SecretStr("abc123")
        assert secret.endswith("123")

    def test_is_empty(self) -> None:
        assert SecretStr("").is_empty()
        assert not SecretStr("abc").is_empty()

    def test_get_hash(self) -> None:
        secret = SecretStr("abc123")
        h = secret.get_hash()
        assert len(h) == 64  # SHA256 hex digest
        assert h != secret.get_secret_value()

    def test_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_SECRET_KEY", "secret_value")
        secret = SecretStr.from_env("TEST_SECRET_KEY")
        assert secret.get_secret_value() == "secret_value"

    def test_from_env_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("TEST_NONEXISTENT", raising=False)
        secret = SecretStr.from_env("TEST_NONEXISTENT", default="default_val")
        assert secret.get_secret_value() == "default_val"

    def test_required(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_REQUIRED_KEY", "required_value")
        secret = SecretStr.required("TEST_REQUIRED_KEY")
        assert secret.get_secret_value() == "required_value"

    def test_required_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("TEST_MISSING_KEY", raising=False)
        with pytest.raises(ValueError, match="TEST_MISSING_KEY"):
            SecretStr.required("TEST_MISSING_KEY")

    def test_type_error(self) -> None:
        with pytest.raises(TypeError, match="must be a string"):
            SecretStr(123)  # type: ignore


# ── redact() 测试 ──


class TestRedact:
    """redact() 递归脱敏测试（TC-SEC-002）。"""

    def test_dict_redact_key(self) -> None:
        result = redact({"api_key": "abc123"})
        assert result == {"api_key": "***"}

    def test_dict_non_redact_key(self) -> None:
        result = redact({"name": "test"})
        assert result == {"name": "test"}

    def test_dict_multiple_keys(self) -> None:
        result = redact({
            "api_key": "abc123",
            "name": "test",
            "token": "xyz789",
        })
        assert result == {
            "api_key": "***",
            "name": "test",
            "token": "***",
        }

    def test_nested_dict(self) -> None:
        result = redact({
            "outer": {"api_key": "secret"},
        })
        assert result == {"outer": {"api_key": "***"}}

    def test_list(self) -> None:
        # List items indexed by "0", "1", etc. — not in REDACT_KEYS
        result = redact(["api_key", "value"])
        assert result == ["api_key", "value"]

    def test_url_query_params(self) -> None:
        result = redact({"url": "https://api.com?key=xyz&api_key=secret"})
        assert "api_key=***" in result["url"]
        assert "key=xyz" in result["url"]

    def test_case_insensitive(self) -> None:
        result = redact({"API_KEY": "abc123"})
        assert result == {"API_KEY": "***"}

    def test_empty_dict(self) -> None:
        result = redact({})
        assert result == {}

    def test_none_value(self) -> None:
        # None is not a string, so it's redacted to "***"
        result = redact({"api_key": None})
        assert result == {"api_key": "***"}

    def test_redact_keys_set(self) -> None:
        assert "api_key" in REDACT_KEYS
        assert "token" in REDACT_KEYS
        assert "authorization" in REDACT_KEYS
        assert "fred_api_key" in REDACT_KEYS
        assert "x-api-key" in REDACT_KEYS


# ── Settings 测试 ──


class TestSettings:
    """Settings 加载测试（TC-SEC-003）。"""

    def test_load_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CHRONOFORGE_ENV", "test")
        monkeypatch.setenv("CHRONOFORGE_DATA_DIR", "/tmp/test_data")
        monkeypatch.setenv("CHRONOFORGE_META_DIR", "/tmp/test_meta")
        monkeypatch.setenv("CHRONOFORGE_LOG_LEVEL", "DEBUG")
        monkeypatch.setenv("FRED_API_KEY", "test_fred_key")
        monkeypatch.setenv("CHRONOFORGE_SEC_CONTACT", "test@example.com")
        monkeypatch.setenv("CHRONOFORGE_HTTP_TIMEOUT", "60")
        monkeypatch.setenv("CHRONOFORGE_RETRY_MAX", "10")

        settings = Settings.load()
        assert settings.env == "test"
        assert settings.data_dir == Path("/tmp/test_data")
        assert settings.meta_dir == Path("/tmp/test_meta")
        assert settings.log_level == "DEBUG"
        assert settings.fred_api_key == "test_fred_key"
        assert settings.sec_contact_email == "test@example.com"
        assert settings.http_timeout_s == 60.0
        assert settings.retry_max == 10

    def test_secret_str_repr(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("FRED_API_KEY", "secret123")
        monkeypatch.setenv("CHRONOFORGE_SEC_CONTACT", "test@example.com")
        settings = Settings.load()
        assert "******" in repr(settings.fred_api_key)
        assert str(settings.fred_api_key) == "******"
        assert settings.fred_api_key.get_secret_value() == "secret123"

    def test_derived_sec_user_agent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CHRONOFORGE_SEC_CONTACT", "research@chronoforge.dev")
        monkeypatch.setenv("CHRONOFORGE_VERSION", "0.1.0")
        settings = Settings.load()
        assert "research@chronoforge.dev" in settings.sec_user_agent
        assert "ChronoForge/0.1.0" in settings.sec_user_agent

    def test_dict_for_logging_redacted(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("CHRONOFORGE_ENV", raising=False)
        monkeypatch.setenv("FRED_API_KEY", "secret123")
        monkeypatch.setenv("CHRONOFORGE_SEC_CONTACT", "test@example.com")
        monkeypatch.setenv("CCXT_BINANCE_API_KEY", "binance_key")
        settings = Settings.load()
        data = settings.dict_for_logging()
        assert data["fred_api_key"] == "***"
        assert data["ccxt_binance_api_key"] == "***"
        assert data["env"] == "default"

    def test_ccxt_secrets_env_mapping(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("CCXT_BINANCE_API_KEY", "binance_api")
        monkeypatch.setenv("CCXT_BINANCE_SECRET", "binance_secret")
        monkeypatch.setenv("CCXT_OKX_API_KEY", "okx_api")
        monkeypatch.setenv("CHRONOFORGE_SEC_CONTACT", "test@example.com")
        settings = Settings.load()
        assert settings.ccxt_binance_api_key == "binance_api"
        assert settings.ccxt_binance_secret == "binance_secret"
        assert settings.ccxt_okx_api_key == "okx_api"

    def test_quality_block_on_parse(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CHRONOFORGE_QUALITY_BLOCK", "Q-001, Q-002")
        monkeypatch.setenv("CHRONOFORGE_SEC_CONTACT", "test@example.com")
        settings = Settings.load()
        assert settings.quality_block_on == ["Q-001", "Q-002"]

    def test_defaults(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """仅设置 SEC_CONTACT 即可（其他用默认值）。"""
        monkeypatch.delenv("CHRONOFORGE_ENV", raising=False)
        monkeypatch.delenv("CHRONOFORGE_DATA_DIR", raising=False)
        monkeypatch.delenv("CHRONOFORGE_META_DIR", raising=False)
        monkeypatch.delenv("CHRONOFORGE_LOG_LEVEL", raising=False)
        monkeypatch.delenv("FRED_API_KEY", raising=False)
        monkeypatch.delenv("CHRONOFORGE_SEC_CONTACT", raising=False)
        monkeypatch.delenv("CHRONOFORGE_HTTP_TIMEOUT", raising=False)
        monkeypatch.delenv("CHRONOFORGE_RETRY_MAX", raising=False)
        monkeypatch.delenv("CHRONOFORGE_RATE_OVERRIDES", raising=False)
        monkeypatch.setenv("CHRONOFORGE_SEC_CONTACT", "test@example.com")
        settings = Settings.load()
        assert settings.env == "default"
        assert settings.retry_max == 5
        assert settings.http_timeout_s == 30.0
        assert settings.normalize_error_threshold == 0.10
