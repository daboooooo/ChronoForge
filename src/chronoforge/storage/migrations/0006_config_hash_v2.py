"""迁移 0006 — TGA run 元数据新增 config_hash_v2（审计 C-4）。

旧 `config_hash` 列保留原值与语义；新列存带版本前缀的 v2 哈希。
版本化源文件与 migrations/__init__.py::MIGRATION_0006_SQL 同步维护。
"""

from __future__ import annotations

MIGRATION_VERSION = "0006"
MIGRATION_DESCRIPTION = "Add config_hash_v2 to forecast_runs/backtest_runs (C-4)"

SQL = """
ALTER TABLE forecast_runs ADD COLUMN config_hash_v2 TEXT;
ALTER TABLE backtest_runs ADD COLUMN config_hash_v2 TEXT;
"""
