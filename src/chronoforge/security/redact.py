"""redact() — 递归脱敏工具（D08 §4）。

用于 structlog processor 链末位和异常格式化，确保 Secret 不泄露到日志。

REDACT_KEYS: 命中即脱敏的键名集合（case-insensitive）。
redact(obj): 递归遍历 dict/list，命中键名的值替换为 "***"。

使用示例:
    >>> redact({"api_key": "abc123", "url": "https://api.com?key=xyz"})
    {'api_key': '***', 'url': 'https://api.com?key=***'}
"""

from __future__ import annotations

import re
from typing import Any

# D08 §4: 命中即脱敏的键名（case-insensitive）
REDACT_KEYS = frozenset({
    "authorization",
    "api_key",
    "apikey",
    "token",
    "cookie",
    "set-cookie",
    "x-api-key",
    "fred_api_key",
    "secret",
    "secret_key",
    "password",
    "passphrase",
    "access_token",
    "refresh_token",
    "bearer",
    "signature",
    "private_key",
    "secretstr",
})

_REDACT_VALUE = "***"

# URL query parameter pattern: key=value or ?key=value
_URL_QUERY_PATTERN = re.compile(r"([?&])([^=&]+)=([^&]*)", re.IGNORECASE)


def _is_redact_key(key: str) -> bool:
    """判断键名是否命中脱敏集合（case-insensitive）。"""
    return key.lower().strip() in REDACT_KEYS


def _redact_url(url: str) -> str:
    """脱敏 URL 中的 query 参数值（只脱敏命中键名）。"""
    if "?" not in url:
        return url

    def _replace_match(match: re.Match[str]) -> str:
        prefix = match.group(1)  # ? or &
        param_key = match.group(2)
        if _is_redact_key(param_key):
            return f"{prefix}{param_key}={_REDACT_VALUE}"
        return match.group(0)

    return _URL_QUERY_PATTERN.sub(_replace_match, url)


def redact(obj: Any) -> Any:
    """递归脱敏 dict/list 中的敏感值。

    规则:
    - dict: 命中 REDACT_KEYS 的键 → 值替换为 "***"
    - URL 值（含 query 参数）: 命中 REDACT_KEYS 的 key → 值脱敏
    - list/tuple: 递归处理每个元素
    - 其他类型: 原样返回

    不修改原对象（返回新对象）。
    """
    if isinstance(obj, dict):
        return {k: _redact_value(k, v) for k, v in obj.items()}

    if isinstance(obj, (list, tuple)):
        result = [redact(item) for item in obj]
        return type(obj)(result)

    # 原始值: 原样返回
    return obj


def _redact_value(key: str, value: Any) -> Any:
    """根据键名脱敏单个值。"""
    if _is_redact_key(key):
        return _REDACT_VALUE

    # SecretStr 实例脱敏
    if _is_secret_value(value):
        return _REDACT_VALUE

    if isinstance(value, str):
        # URL 值脱敏 query parameters
        if "?" in value and "=" in value:
            return _redact_url(value)
        return value

    if isinstance(value, dict):
        return {k: _redact_value(k, v) for k, v in value.items()}

    if isinstance(value, (list, tuple)):
        result = [redact(item) for item in value]
        return type(value)(result)

    return value


def _is_secret_value(value: Any) -> bool:
    """判断值是否为 SecretStr（str() 返回纯星号字符串）。"""
    try:
        s = str(value)
        # SecretStr 的 str() 返回全星号（如"******"）
        return len(s) >= 3 and all(c == "*" for c in s)
    except Exception:
        return False
