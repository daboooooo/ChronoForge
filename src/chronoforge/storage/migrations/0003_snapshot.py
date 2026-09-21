"""迁移 0003 — research_snapshot 表（D07 §3，QUERY-003）。

编号说明：D07 §3/D10 原编号 0002 已被审计 H-6（quality_flags.resolved 列）
占用，本迁移顺延注册为 0003，DDL 与 D07 §3 契约一致（IF NOT EXISTS 幂等）。

本文件为迁移的版本化源文件；注册中心 migrations/__init__.py 以同内容 SQL
注册（与 0001_init.py 同模式，两处需同步维护）。
"""

from __future__ import annotations

MIGRATION_VERSION = "0003"
MIGRATION_DESCRIPTION = "Add research_snapshot table (D07 §3, QUERY-003)"

SQL = """
CREATE TABLE IF NOT EXISTS research_snapshot(
  snapshot_id TEXT PRIMARY KEY,           -- uuid hex 12
  created_at TEXT NOT NULL,
  datasets_json TEXT NOT NULL,            -- [{"dataset_id","dataset_version"}]
  code_version TEXT NOT NULL,             -- chronoforge.__version__
  params_json TEXT NOT NULL,              -- 研究参数
  output_hash TEXT NOT NULL,              -- sha256(结果序列化)
  notebook_ref TEXT,                      -- 可选溯源
  query_text TEXT                         -- 可选溯源
);
"""
