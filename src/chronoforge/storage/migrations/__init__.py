"""SQLite 迁移注册中心（D03 §1）。

所有迁移在此集中注册，MetaStore 按顺序执行未应用的迁移。
"""

from __future__ import annotations

# 0001_init migration SQL
MIGRATION_0001_SQL = """
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

CREATE TABLE IF NOT EXISTS checkpoints(
  source_id TEXT NOT NULL,
  dataset_id TEXT NOT NULL,
  last_cursor TEXT NOT NULL,
  last_success_time TEXT NOT NULL,
  PRIMARY KEY(source_id, dataset_id)
);

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

CREATE TABLE IF NOT EXISTS schema_versions(
  version TEXT PRIMARY KEY,
  applied_at TEXT NOT NULL,
  description TEXT NOT NULL
);
"""

# 0002 migration SQL — 审计 H-6：quality_flags 增加处理状态，
# 支持 resolved 机制（derive_dataset_status 只统计未处理 ERROR flags）
MIGRATION_0002_SQL = """
ALTER TABLE quality_flags ADD COLUMN resolved INT NOT NULL DEFAULT 0;
ALTER TABLE quality_flags ADD COLUMN resolved_at TEXT;
CREATE INDEX IF NOT EXISTS idx_quality_flags_ds_unresolved
  ON quality_flags(dataset_id, severity, resolved);
"""

# 0003 migration SQL — QUERY-003：research_snapshot 表（D07 §3）。
# 版本化源文件见 0003_snapshot.py（与 0001_init.py 同模式，两处需同步维护）。
# 注：D07 §3 原编号 0002 已被上方审计 H-6 迁移占用，本迁移顺延为 0003。
MIGRATION_0003_SQL = """
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

# 0004 migration SQL — 审计 SR-03：熔断状态持久化（D05 §3）。
# 此前熔断计数为 runner 实例内存态，CLI 每 dataset 新建 runner → 计数跨
# job 归零，同 dataset 连续 3 次 FAILED 的熔断契约在真实运行中永不触发。
# checkpoints 增加旁路字段：consecutive_failed（连续 FAILED 计数）、
# circuit_open（熔断打开标志）、circuit_opened_at（打开时刻）。
MIGRATION_0004_SQL = """
ALTER TABLE checkpoints ADD COLUMN consecutive_failed INT NOT NULL DEFAULT 0;
ALTER TABLE checkpoints ADD COLUMN circuit_open INT NOT NULL DEFAULT 0;
ALTER TABLE checkpoints ADD COLUMN circuit_opened_at TEXT;
"""

# 0005 migration SQL — TGA 预测/回测运行元数据与异常处置。
# 版本化源文件见 0005_forecast_metadata.py（与 0001/0003 同模式，两处同步维护）。
# 注：0004 已被熔断状态字段占用，本迁移顺延为 0005。
MIGRATION_0005_SQL = """
CREATE TABLE IF NOT EXISTS forecast_runs(
  forecast_run_id TEXT PRIMARY KEY,
  created_at TEXT NOT NULL,
  origin_date TEXT NOT NULL,
  data_cutoff_date TEXT NOT NULL,
  model_version TEXT NOT NULL,
  config_hash TEXT NOT NULL,
  scenarios_json TEXT NOT NULL,
  horizons_json TEXT NOT NULL,
  snapshot_id TEXT,
  status TEXT NOT NULL CHECK(status IN ('RUNNING','SUCCESS','FAILED')),
  is_repeat INT NOT NULL DEFAULT 0,
  output_hash TEXT,
  error_summary TEXT
);
CREATE INDEX IF NOT EXISTS idx_forecast_runs_cutoff
  ON forecast_runs(data_cutoff_date DESC);

CREATE TABLE IF NOT EXISTS backtest_runs(
  backtest_run_id TEXT PRIMARY KEY,
  created_at TEXT NOT NULL,
  start_date TEXT NOT NULL,
  end_date TEXT NOT NULL,
  stride INT NOT NULL,
  model_versions_json TEXT NOT NULL,
  config_hash TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('RUNNING','SUCCESS','FAILED')),
  output_hash TEXT,
  error_summary TEXT
);

CREATE TABLE IF NOT EXISTS tga_anomaly(
  anomaly_id TEXT PRIMARY KEY,
  detected_at TEXT NOT NULL,
  data_source TEXT NOT NULL,
  record_date TEXT,
  anomaly_type TEXT NOT NULL,
  original_value TEXT,
  expected_value TEXT,
  severity TEXT NOT NULL CHECK(severity IN ('ERROR','WARNING','INFO')),
  resolution_status TEXT NOT NULL DEFAULT 'OPEN'
    CHECK(resolution_status IN ('OPEN','ACKNOWLEDGED','EXPLAINED','RESOLVED')),
  quality_flag_ref TEXT,
  note TEXT
);
"""

# 0006 migration SQL — 审计 C-4：TGA run 元数据新增 v2 配置哈希列。
# 版本化源文件见 0006_config_hash_v2.py（与 0001/0003/0005 同模式，两处同步维护）。
MIGRATION_0006_SQL = """
ALTER TABLE forecast_runs ADD COLUMN config_hash_v2 TEXT;
ALTER TABLE backtest_runs ADD COLUMN config_hash_v2 TEXT;
"""

# Migration registry — new migrations add entries here
_all_migrations: list[dict[str, str]] = [
    {
        "version": "0001",
        "sql": MIGRATION_0001_SQL,
        "description": (
            "Initial DDL: source_registry, dataset_registry, checkpoints, "
            "run_log, quality_flags, schema_versions"
        ),
    },
    {
        "version": "0002",
        "sql": MIGRATION_0002_SQL,
        "description": "Add resolved/resolved_at to quality_flags (audit H-6)",
    },
    {
        "version": "0003",
        "sql": MIGRATION_0003_SQL,
        "description": "Add research_snapshot table (D07 §3, QUERY-003)",
    },
    {
        "version": "0004",
        "sql": MIGRATION_0004_SQL,
        "description": "Add circuit breaker columns to checkpoints (audit SR-03)",
    },
    {
        "version": "0005",
        "sql": MIGRATION_0005_SQL,
        "description": "Add forecast_runs/backtest_runs/tga_anomaly (TGA)",
    },
    {
        "version": "0006",
        "sql": MIGRATION_0006_SQL,
        "description": "Add config_hash_v2 to forecast_runs/backtest_runs (C-4)",
    },
]
