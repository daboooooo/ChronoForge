"""迁移 0001 — 初始 DDL（D03 §1）。

按 D03 §1 DDL 逐表创建 6 张表，支持幂等执行（IF NOT EXISTS）。
"""

from __future__ import annotations

MIGRATION_VERSION = "0001"
MIGRATION_DESCRIPTION = (
    "Initial DDL: source_registry, dataset_registry, checkpoints, "
    "run_log, quality_flags, schema_versions"
)

SQL = """
-- 1. source_registry
CREATE TABLE IF NOT EXISTS source_registry(
  source_id TEXT PRIMARY KEY,
  display_name TEXT NOT NULL,
  access_type TEXT NOT NULL CHECK(access_type IN ('PUBLIC','PUBLIC_WITH_KEY','AUTHENTICATED')),
  base_url TEXT NOT NULL,
  rate_limit_json TEXT NOT NULL,
  historical_limit_days INT,
  license TEXT NOT NULL,
  enabled INT NOT NULL DEFAULT 1
);

-- 2. dataset_registry
CREATE TABLE IF NOT EXISTS dataset_registry(
  dataset_id TEXT PRIMARY KEY,
  source_id TEXT NOT NULL REFERENCES source_registry(source_id),
  canonical_type TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  params_json TEXT NOT NULL,
  frequency TEXT,
  continuity_model TEXT NOT NULL CHECK(continuity_model IN
    ('ALWAYS_OPEN','TRADING_CALENDAR','EVENT_BASED','RELEASE_SCHEDULE')),
  status TEXT NOT NULL DEFAULT 'UNKNOWN' CHECK(status IN
    ('UNKNOWN','COMPLETE','PARTIAL','INCOMPLETE','QUARANTINED','STALE','REVISION_PENDING')),
  available_from TEXT, available_to TEXT,
  revision_supported INT NOT NULL DEFAULT 0,
  enabled INT NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL
);

-- 3. checkpoints
CREATE TABLE IF NOT EXISTS checkpoints(
  source_id TEXT NOT NULL,
  dataset_id TEXT NOT NULL,
  last_cursor TEXT NOT NULL,
  last_success_time TEXT NOT NULL,
  PRIMARY KEY(source_id, dataset_id)
);

-- 4. run_log
CREATE TABLE IF NOT EXISTS run_log(
  run_id TEXT PRIMARY KEY,
  source_id TEXT NOT NULL,
  dataset_id TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('PENDING','RUNNING','SUCCESS',
    'PARTIAL_SUCCESS','FAILED','CANCELLED')),
  started_at TEXT NOT NULL, ended_at TEXT,
  input_count INT DEFAULT 0, output_count INT DEFAULT 0,
  error_count INT DEFAULT 0, warning_count INT DEFAULT 0,
  request_count INT DEFAULT 0, retry_count INT DEFAULT 0,
  duplicate_count INT DEFAULT 0, missing_count INT DEFAULT 0,
  latency_ms INT DEFAULT 0,
  checkpoint_before TEXT, checkpoint_after TEXT,
  chunk_success INT DEFAULT 0, chunk_failed INT DEFAULT 0,
  error_summary TEXT,
  schema_version TEXT NOT NULL,
  code_version TEXT NOT NULL,
  ingest_batch_id TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_run_log_ds_time ON run_log(dataset_id, started_at DESC);

-- 5. quality_flags
CREATE TABLE IF NOT EXISTS quality_flags(
  record_key TEXT NOT NULL,
  dataset_id TEXT NOT NULL,
  rule_id TEXT NOT NULL,
  severity TEXT NOT NULL CHECK(severity IN ('ERROR','WARNING','INFO')),
  detail TEXT NOT NULL,
  raw_ref TEXT NOT NULL,
  payload_digest TEXT NOT NULL,
  run_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY(record_key, rule_id, run_id)
);

-- 6. schema_versions
CREATE TABLE IF NOT EXISTS schema_versions(
  version TEXT PRIMARY KEY,
  applied_at TEXT NOT NULL,
  description TEXT NOT NULL
);
"""
