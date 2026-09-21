"""安全模块（D08 §4/§5）。

包含:
- SecretStr: 敏感字符串包装类（__repr__/__str__ 脱敏，永不落盘/入日志）
- redact(): 递归脱敏 dict/list/URL（用于 structlog processor 和异常格式化）
- REDACT_KEYS: 命中即脱敏的键名集合
"""

from chronoforge.security.redact import REDACT_KEYS, redact
from chronoforge.security.secret import SecretStr

__all__ = ["SecretStr", "REDACT_KEYS", "redact"]
