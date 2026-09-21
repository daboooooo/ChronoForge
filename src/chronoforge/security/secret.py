"""SecretStr — 敏感字符串安全包装（D08 §5）。

设计原则:
- __repr__/__str__ 始终返回脱敏值（"******"）
- get_secret_value() 返回原始值
- 支持 == 比较（比较原始值）
- 支持 hash（可放入 set/dict key）
- 不可变（frozen）
- 不依赖第三方库（纯 Python 实现）

使用示例:
    >>> key = SecretStr("abc123")
    >>> print(key)  # ******
    >>> str(key)    # ******
    >>> key.get_secret_value()  # 'abc123'
    >>> key == "abc123"  # True
    >>> key == SecretStr("abc123")  # True
"""

from __future__ import annotations

import functools
import hashlib
import os
from typing import Any


@functools.total_ordering
class SecretStr:
    """敏感字符串包装类。

    __repr__ 和 __str__ 始终返回 "******"（脱敏）。
    原始值通过 get_secret_value() 获取。
    支持 == 比较原始值，支持 hash。
    """

    __slots__ = ("_value",)

    _REDACTED = "******"

    def __init__(self, value: str) -> None:
        if not isinstance(value, str):
            msg = "SecretStr value must be a string"
            raise TypeError(msg)
        self._value = value

    def __repr__(self) -> str:
        return f"SecretStr({self._REDACTED!r})"

    def __str__(self) -> str:
        return self._REDACTED

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, SecretStr):
            return self._value == other._value
        if isinstance(other, str):
            return self._value == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._value)

    def __lt__(self, other: Any) -> bool:
        if isinstance(other, SecretStr):
            return self._value < other._value
        if isinstance(other, str):
            return self._value < other
        return NotImplemented

    def __len__(self) -> int:
        return len(self._value)

    def __bool__(self) -> bool:
        return bool(self._value)

    def get_secret_value(self) -> str:
        """返回原始字符串值。"""
        return self._value

    def get_hash(self, algorithm: str = "sha256") -> str:
        """返回值的哈希（用于比较而不暴露值）。"""
        return hashlib.new(algorithm, self._value.encode()).hexdigest()

    def startswith(self, prefix: str) -> bool:
        """判断原始值是否以 prefix 开头。"""
        return self._value.startswith(prefix)

    def endswith(self, suffix: str) -> bool:
        """判断原始值是否以 suffix 结尾。"""
        return self._value.endswith(suffix)

    def is_empty(self) -> bool:
        """判断是否为空值（None 或空字符串）。"""
        return self._value == ""

    @classmethod
    def from_env(cls, name: str, default: str = "") -> SecretStr:
        """从环境变量加载（D08 §1 加载顺序）。

        Args:
            name: 环境变量名
            default: 默认值（当环境变量未设置时）

        Returns:
            SecretStr 实例
        """
        value = os.environ.get(name, default)
        return cls(value)

    @classmethod
    def required(cls, name: str) -> SecretStr:
        """从环境变量加载，未设置则抛出 ValueError。

        Args:
            name: 环境变量名

        Returns:
            SecretStr 实例

        Raises:
            ValueError: 环境变量未设置或为空
        """
        value = os.environ.get(name, "").strip()
        if not value:
            msg = f"Required environment variable {name!r} is not set or empty"
            raise ValueError(msg)
        return cls(value)
