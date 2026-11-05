"""迁移 0005 — TGA 预测/回测运行元数据与异常处置。

版本化源文件与 migrations/__init__.py::MIGRATION_0005_SQL 同步维护。
"""

from __future__ import annotations

MIGRATION_VERSION = "0005"
MIGRATION_DESCRIPTION = "Add forecast_runs/backtest_runs/tga_anomaly (TGA)"

SQL = """
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
