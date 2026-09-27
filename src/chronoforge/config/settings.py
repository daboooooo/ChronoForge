"""Settings 配置管理（D08 §1）。

使用 pydantic-settings 加载环境变量，SecretStr 包装敏感字段。
加载顺序: defaults ← .env ← 环境变量。
单入口: Settings.load()。

设计原则:
- Secret 字段使用 SecretStr（永不落盘/入库/入日志）
- 缺失 required secret → ConfigError（启动 fail fast）
- derived 字段通过 model_validator 自动计算
- test 环境通过环境变量注入（conftest fixture）
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Literal, cast

from pydantic import BaseModel, Field, field_validator, model_validator

from chronoforge.exceptions import ConfigError
from chronoforge.security.secret import SecretStr

# READY-003（SR-10）：查询默认行数上限——单一事实源，
# research/query.py DuckDBQueryService 构造缺省值引用此常量。
DEFAULT_QUERY_MAX_ROWS = 200_000


# D08 §1: 运行时配置（非 Secret 分类）
class Settings(BaseModel):
    """全局配置（D08 §1 + D04 §5.2）。

    加载顺序: defaults ← .env ← 环境变量
    Secret 字段: fred_api_key, sec_contact_email（SecretStr）
    """

    model_config = {"arbitrary_types_allowed": True}

    # ── 运行时配置（Runtime）──
    env: Literal["default", "development", "test", "production"] = Field(
        default="default",
        description="运行环境（约束 §25）",
    )
    data_dir: Path = Field(
        default=Path("./data"),
        description="数据存储目录（Raw/Canonical/Derived）",
    )
    meta_dir: Path = Field(
        default=Path("./meta"),
        description="元数据目录（SQLite）",
    )
    log_level: str = Field(
        default="INFO",
        description="日志级别",
    )
    http_timeout_s: float = Field(
        default=30.0,
        description="HTTP 超时（秒）",
    )
    retry_max: int = Field(
        default=5,
        description="重试上限（指数退避）",
    )
    circuit_cooldown_s: float = Field(
        default=1800.0,
        ge=0,
        description=(
            "熔断冷却期（秒，R2-01）：circuit_open 持续超过该时长后"
            "half-open 放行一次探测 run；探测失败重新计时"
        ),
    )
    quality_block_on: list[str] = Field(
        default=["Q-SCHEMA-001", "Q-PROV-001"],
        description="质量阻断规则（逗号分隔）",
    )
    normalize_error_threshold: float = Field(
        default=0.10,
        description="标准化错误阈值",
    )
    query_max_rows: int = Field(
        default=DEFAULT_QUERY_MAX_ROWS,
        ge=1,
        description="查询默认行数上限（READY-003/SR-10：query() 恒附加 LIMIT）",
    )
    ohlcv_chunk_bars: int = Field(
        default=200,
        ge=1,
        description=(
            "OHLCV 长周期 interval（1h/4h/1d）单 chunk 目标 K 线根数"
            "（chunk 跨度 = 根数 × 单根秒数；connector 内部分页适配"
            "各交易所单请求上限）"
        ),
    )
    version: str = Field(
        default="0.1.0",
        description="ChronoForge 版本",
    )

    # ── 秘密（Secret，仅 env/.env，永不落盘/入库/入日志）──
    fred_api_key: SecretStr = Field(
        default=SecretStr(""),
        description="FRED API Key（启用 FRED 时必填）",
    )
    sec_contact_email: SecretStr = Field(
        default=SecretStr(""),
        description="SEC fair-access 联系邮箱（User-Agent 必填项）",
    )
    # CCXT Bridge 凭证
    ccxt_binance_api_key: SecretStr = Field(
        default=SecretStr(""),
        description="CCXT Binance API Key",
    )
    ccxt_binance_secret: SecretStr = Field(
        default=SecretStr(""),
        description="CCXT Binance Secret",
    )
    ccxt_okx_api_key: SecretStr = Field(
        default=SecretStr(""),
        description="CCXT OKX API Key",
    )
    ccxt_okx_secret: SecretStr = Field(
        default=SecretStr(""),
        description="CCXT OKX Secret",
    )
    ccxt_okx_passphrase: SecretStr = Field(
        default=SecretStr(""),
        description="CCXT OKX Passphrase",
    )
    sosovalue_api_key: SecretStr = Field(
        default=SecretStr(""),
        description="SoSoValue API Key（启用 sosovalue 时必填）",
    )

    # ── 测试注入（不映射 env，pydantic 忽略 __ 前缀字段）──
    rate_overrides__: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _load_from_env(cls, values: Any) -> Any:
        """从环境变量加载非 Secret 字段。

        pydantic-settings 的 Field 不支持直接从 env 加载非 Secret 字段，
        所以手动读取。Secret 字段由 __init__ 处理。
        """
        if isinstance(values, dict):
            # 已有值（显式 kwargs）优先，env 仅填充未提供的键
            # （D08 §1 加载顺序：defaults ← .env ← 环境变量 ← kwargs）
            env_overrides = _read_env_fields(cls)
            for key, value in env_overrides.items():
                values.setdefault(key, value)
        else:
            values = _read_env_fields(cls)
        return values

    @field_validator("fred_api_key", mode="before")
    @classmethod
    def _parse_fred_api_key(cls, v: Any) -> SecretStr:
        """将 str → SecretStr（env 加载时自动转换）。"""
        if isinstance(v, SecretStr):
            return v
        if isinstance(v, str):
            return SecretStr(v)
        return SecretStr(str(v))

    @field_validator("sec_contact_email", mode="before")
    @classmethod
    def _parse_sec_contact_email(cls, v: Any) -> SecretStr:
        """将 str → SecretStr（env 加载时自动转换）。"""
        if isinstance(v, SecretStr):
            return v
        if isinstance(v, str):
            return SecretStr(v)
        return SecretStr(str(v))

    # CCXT 字段统一 str → SecretStr 转换
    @field_validator(
        "ccxt_binance_api_key",
        "ccxt_binance_secret",
        "ccxt_okx_api_key",
        "ccxt_okx_secret",
        "ccxt_okx_passphrase",
        mode="before",
    )
    @classmethod
    def _parse_ccxt_secret(cls, v: Any) -> SecretStr:
        """将 str → SecretStr（env 加载时自动转换）。"""
        if isinstance(v, SecretStr):
            return v
        if isinstance(v, str):
            return SecretStr(v)
        return SecretStr(str(v))

    @field_validator("sosovalue_api_key", mode="before")
    @classmethod
    def _parse_sosovalue_api_key(cls, v: Any) -> SecretStr:
        """将 str → SecretStr（env 加载时自动转换）。"""
        if isinstance(v, SecretStr):
            return v
        if isinstance(v, str):
            return SecretStr(v)
        return SecretStr(str(v))

    @field_validator("quality_block_on", mode="before")
    @classmethod
    def _parse_quality_block_on(cls, v: Any) -> list[str]:
        """将逗号分隔的字符串 → list[str]。"""
        if isinstance(v, list):
            return v
        if isinstance(v, str):
            return [s.strip() for s in v.split(",") if s.strip()]
        return []

    @model_validator(mode="after")
    def _validate_and_derive(self) -> Settings:
        """验证必填字段 + 计算 derived 字段。"""
        # 验证 SEC email 非空
        if self.sec_contact_email.is_empty():
            raise ConfigError(
                "sec_contact_email is required for SEC EDGAR User-Agent",
                context={"source": "sec_edgar"},
            )
        if isinstance(self.quality_block_on, str):
            self.quality_block_on = [
                s.strip() for s in self.quality_block_on.split(",") if s.strip()
            ]

        return self

    @property
    def sec_user_agent(self) -> str:
        """派生字段: SEC User-Agent（D04 §4.7）。"""
        email = self.sec_contact_email.get_secret_value()
        return f"ChronoForge/{self.version} research ({email})"

    @property
    def rate_overrides(self) -> dict[str, Any]:
        """测试注入的限流覆盖。"""
        return cast(dict[str, Any], self.__dict__.get("rate_overrides__", {}))

    def get_secret(self, name: str) -> SecretStr:
        """获取指定 secret 字段的 SecretStr 值。

        Args:
            name: 字段名

        Returns:
            SecretStr 实例
        """
        value = getattr(self, name, None)
        if value is None:
            return SecretStr("")
        if isinstance(value, SecretStr):
            return value
        return SecretStr(str(value))

    def dict_for_logging(self) -> dict[str, Any]:
        """返回用于日志的字典（Secret 字段脱敏）。"""
        from chronoforge.security import redact

        data = {
            "env": self.env,
            "data_dir": str(self.data_dir),
            "meta_dir": str(self.meta_dir),
            "log_level": self.log_level,
            "http_timeout_s": self.http_timeout_s,
            "retry_max": self.retry_max,
            "circuit_cooldown_s": self.circuit_cooldown_s,
            "query_max_rows": self.query_max_rows,
            "quality_block_on": self.quality_block_on,
            # Secret 字段（redact 会自动脱敏）
            "fred_api_key": self.fred_api_key,
            "sec_contact_email": self.sec_contact_email,
            "ccxt_binance_api_key": self.ccxt_binance_api_key,
            "ccxt_binance_secret": self.ccxt_binance_secret,
            "ccxt_okx_api_key": self.ccxt_okx_api_key,
            "ccxt_okx_secret": self.ccxt_okx_secret,
            "ccxt_okx_passphrase": self.ccxt_okx_passphrase,
            "sosovalue_api_key": self.sosovalue_api_key,
        }
        return cast(dict[str, Any], redact(data))

    def __repr__(self) -> str:
        data = self.dict_for_logging()
        pairs = ", ".join(f"{k}={v!r}" for k, v in data.items())
        return f"Settings({pairs})"

    # ── 单入口加载 ──

    @classmethod
    def load(cls, **kwargs: Any) -> Settings:
        """加载全局配置（单入口，D08 §1）。

        加载顺序: defaults ← .env ← 环境变量 ← kwargs

        Args:
            **kwargs: 测试注入（覆盖环境变量）

        Returns:
            Settings 实例

        Raises:
            ConfigError: 必填字段缺失
        """
        # 读取 .env 文件（如果存在）
        _load_dotenv()

        return cls(**kwargs)

    @classmethod
    def for_test(cls, **overrides: Any) -> Settings:
        """测试专用加载（data_dir/meta_dir 指向 tmp，强制 test 环境）。

        Args:
            **overrides: 额外覆盖字段

        Returns:
            Settings 实例
        """
        import tempfile

        tmp_path = tempfile.mkdtemp(prefix="chronoforge_test_")
        return cls.load(
            env="test",
            data_dir=Path(tmp_path) / "data",
            meta_dir=Path(tmp_path) / "meta",
            **overrides,
        )


def _read_env_fields(cls_model: type[BaseModel]) -> dict[str, Any]:
    """从环境变量读取字段值。

    仅处理已知字段名，忽略未知 env 变量。
    """
    result: dict[str, Any] = {}
    env_mapping: dict[str, str] = {
        "env": "CHRONOFORGE_ENV",
        "data_dir": "CHRONOFORGE_DATA_DIR",
        "meta_dir": "CHRONOFORGE_META_DIR",
        "log_level": "CHRONOFORGE_LOG_LEVEL",
        "fred_api_key": "FRED_API_KEY",
        "sec_contact_email": "CHRONOFORGE_SEC_CONTACT",
        "http_timeout_s": "CHRONOFORGE_HTTP_TIMEOUT",
        "retry_max": "CHRONOFORGE_RETRY_MAX",
        "circuit_cooldown_s": "CHRONOFORGE_CIRCUIT_COOLDOWN_S",
        "quality_block_on": "CHRONOFORGE_QUALITY_BLOCK",
        "normalize_error_threshold": "CHRONOFORGE_NORMALIZE_ERROR_THRESHOLD",
        "query_max_rows": "CHRONOFORGE_QUERY_MAX_ROWS",
        "ohlcv_chunk_bars": "CHRONOFORGE_OHLCV_CHUNK_BARS",
        "version": "CHRONOFORGE_VERSION",
        # CCXT Bridge 凭证
        "ccxt_binance_api_key": "CCXT_BINANCE_API_KEY",
        "ccxt_binance_secret": "CCXT_BINANCE_SECRET",
        "ccxt_okx_api_key": "CCXT_OKX_API_KEY",
        "ccxt_okx_secret": "CCXT_OKX_SECRET",
        "ccxt_okx_passphrase": "CCXT_OKX_PASSPHRASE",
        "sosovalue_api_key": "SOSOVALUE_API_KEY",
    }

    for field_name, env_name in env_mapping.items():
        value = os.environ.get(env_name)
        if value is not None:
            result[field_name] = value

    # rate_overrides_json 特殊处理（JSON 字符串 → dict）
    rate_json = os.environ.get("CHRONOFORGE_RATE_OVERRIDES")
    if rate_json:
        import json
        try:
            result["_rate_overrides"] = json.loads(rate_json)
        except json.JSONDecodeError:
            pass  # 忽略无效 JSON

    return result


def _load_dotenv() -> None:
    """加载 .env 文件（如果存在）。

    不依赖 python-dotenv，手动解析 .env 文件。
    仅加载未设置的环境变量（环境变量优先级更高）。
    """
    import re

    # 搜索 .env 文件路径
    env_paths = [
        Path.cwd() / ".env",
        Path.cwd() / ".env.local",
    ]

    for env_path in env_paths:
        if not env_path.exists():
            continue

        try:
            with open(env_path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    # 跳过注释和空行
                    if not line or line.startswith("#"):
                        continue
                    # 解析 KEY=VALUE
                    match = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)=(.*)$", line)
                    if match:
                        key = match.group(1)
                        value = match.group(2).strip()
                        # 去除引号
                        if (value.startswith('"') and value.endswith('"')) or \
                           (value.startswith("'") and value.endswith("'")):
                            value = value[1:-1]
                        # 仅当环境变量未设置时写入
                        if key not in os.environ:
                            os.environ[key] = value
        except OSError:
            # .env 文件读取失败不影响启动
            pass
