"""Settings 单元测试（D08 §1，TC-X-001 + 边界/失败组）。

注意：Settings 实现来自 969f60d 预备提交（非 pydantic-settings，见 CLI-001.md
偏差 D-1）；本文件按 D08 §1 字段表 + TC-X-001 优先级矩阵验收其行为。
"""

from __future__ import annotations

from pathlib import Path

import pytest

from chronoforge.config.settings import Settings
from chronoforge.connectors.errors import ConfigError

# Settings 全部已知 env 名（D08 §1 字段表）
_KNOWN_ENV_VARS = [
    "CHRONOFORGE_ENV",
    "CHRONOFORGE_DATA_DIR",
    "CHRONOFORGE_META_DIR",
    "CHRONOFORGE_LOG_LEVEL",
    "FRED_API_KEY",
    "CHRONOFORGE_SEC_CONTACT",
    "CHRONOFORGE_HTTP_TIMEOUT",
    "CHRONOFORGE_RETRY_MAX",
    "CHRONOFORGE_QUALITY_BLOCK",
    "CHRONOFORGE_NORMALIZE_ERROR_THRESHOLD",
    "CHRONOFORGE_RATE_OVERRIDES",
]


@pytest.fixture(autouse=True)
def _clean_env(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """隔离环境变量（Settings 从 os.environ/.env 读取）+ chdir 避免读仓库 .env。"""
    for var in _KNOWN_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("CHRONOFORGE_SEC_CONTACT", "research@example.com")
    monkeypatch.chdir(tmp_path)  # _load_dotenv 读 cwd/.env → 隔离


class TestLoadPriorityMatrix:
    """TC-X-001：配置优先级矩阵（.env < 环境变量）。"""

    def test_env_beats_dotenv(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """GWT-1: Given .env 与环境变量冲突 When Settings.load Then 环境变量胜出。"""
        (tmp_path / ".env").write_text(
            "CHRONOFORGE_DATA_DIR=/from/dotenv\n"
            "CHRONOFORGE_RETRY_MAX=2\n",
            encoding="utf-8",
        )
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("CHRONOFORGE_DATA_DIR", "/from/env")

        settings = Settings.load()
        assert settings.data_dir == Path("/from/env")  # 环境变量胜出
        assert settings.retry_max == 2  # .env 值在环境变量未设时生效

    def test_dotenv_only(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """仅 .env：值被加载。"""
        (tmp_path / ".env").write_text(
            "CHRONOFORGE_DATA_DIR=/only/dotenv\n"
            "CHRONOFORGE_LOG_LEVEL=DEBUG\n",
            encoding="utf-8",
        )
        monkeypatch.chdir(tmp_path)
        settings = Settings.load()
        assert settings.data_dir == Path("/only/dotenv")
        assert settings.log_level == "DEBUG"

    def test_dotenv_quoted_value(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """.env 引号值剥离。"""
        # _clean_env 预设的 SEC_CONTACT env 变量胜过 .env（TC-X-001 语义），
        # 此处删除它，让 .env 成为该字段的唯一来源。
        monkeypatch.delenv("CHRONOFORGE_SEC_CONTACT")
        (tmp_path / ".env").write_text(
            'CHRONOFORGE_SEC_CONTACT="quoted@example.com"\n',
            encoding="utf-8",
        )
        monkeypatch.chdir(tmp_path)
        settings = Settings.load()
        assert settings.sec_contact_email.get_secret_value() == "quoted@example.com"

    def test_kwargs_beat_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """显式 kwargs（测试注入）> 环境变量。"""
        monkeypatch.setenv("CHRONOFORGE_RETRY_MAX", "9")
        settings = Settings.load(retry_max=3)
        assert settings.retry_max == 3


class TestDefaults:
    """边界：空参数默认值（D08 §1 默认列）。"""

    def test_defaults(self) -> None:
        settings = Settings.load()
        assert settings.env == "default"
        assert settings.data_dir == Path("./data")
        assert settings.meta_dir == Path("./meta")
        assert settings.log_level == "INFO"
        assert settings.http_timeout_s == 30.0
        assert settings.retry_max == 5
        assert settings.quality_block_on == ["Q-SCHEMA-001", "Q-PROV-001"]
        assert settings.normalize_error_threshold == 0.10


class TestFieldParsing:
    """字段解析（D08 §1 字段表）。"""

    def test_env_literal_invalid_rejected(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """失败组：非法 env 值（Literal 域外）→ ValidationError。"""
        monkeypatch.setenv("CHRONOFORGE_ENV", "bogus")
        with pytest.raises(Exception, match="env"):
            Settings.load()

    def test_quality_block_csv_parse(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("CHRONOFORGE_QUALITY_BLOCK", "Q-001, Q-002")
        assert Settings.load().quality_block_on == ["Q-001", "Q-002"]

    def test_numeric_fields_parsed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("CHRONOFORGE_HTTP_TIMEOUT", "12.5")
        monkeypatch.setenv("CHRONOFORGE_RETRY_MAX", "3")
        monkeypatch.setenv("CHRONOFORGE_NORMALIZE_ERROR_THRESHOLD", "0.2")
        settings = Settings.load()
        assert settings.http_timeout_s == 12.5
        assert settings.retry_max == 3
        assert settings.normalize_error_threshold == 0.2

    def test_fred_key_secretstr_no_plaintext_repr(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Secret 字段：repr/str 不泄露明文（D08 §1）。"""
        monkeypatch.setenv("FRED_API_KEY", "super-secret-key")
        settings = Settings.load()
        assert "super-secret-key" not in repr(settings)
        assert "super-secret-key" not in str(settings.fred_api_key)
        assert settings.fred_api_key.get_secret_value() == "super-secret-key"


class TestDerived:
    """派生字段（D08 §1 sec_user_agent）。"""

    def test_sec_user_agent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CHRONOFORGE_SEC_CONTACT", "alice@lab.io")
        agent = Settings.load().sec_user_agent
        assert agent.startswith("ChronoForge/")
        assert "alice@lab.io" in agent
        assert agent.endswith("research (alice@lab.io)")

    def test_missing_sec_contact_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """失败组：SEC 联系邮箱缺失 → ConfigError（启动 fail fast）。"""
        monkeypatch.delenv("CHRONOFORGE_SEC_CONTACT")
        with pytest.raises(ConfigError):
            Settings.load()
