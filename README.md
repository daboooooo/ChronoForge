# ChronoForge

```
 __      __  _  ____  ____
/  \    /  ][    ](    \
\    \  /   / |  |  |   | _
 \    \/   /  |  |  |   |/ \
  \        /  |  |  |   |\  /
   \      /   |  |  |   | \
    \__/\__]  [____|____| \__
```

> **Financial Multi-Source Data Pipeline** — Canonical data platform for institutional-grade market data acquisition, normalization, and OLAP query.

[![Python](https://img.shields.io/badge/Python-3.12%2B-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-Apache--2.0-green)](LICENSE)
[![Status](https://img.shields.io/badge/Status-0.1.0%20alpha-orange)]()

---

## TL;DR

```bash
# Install
pip install -e '.[dev]'

# Bootstrap sources
chronoforge registry sync

# Register BTC-USDT spot OHLCV (1h bars) — note: Binance uses NO slash in symbol
chronoforge dataset add --dataset-id btcusdt-ohlcv-1h \
  --source binance_spot \
  --type OHLCV \
  --entity-id BTCUSDT \
  --params '{"symbol":"BTCUSDT","interval":"1h"}' \
  --frequency 1h \
  --continuity-model ALWAYS_OPEN

# Backfill from 2021 with 90-day windows
chronoforge pipeline run \
  --dataset btcusdt-ohlcv-1h \
  --mode backfill \
  --start 2021-01-01T00:00:00 \
  --end 2025-09-22T00:00:00 \
  --window-seconds 7776000

# Query the data
chronoforge query --dataset btcusdt-ohlcv-1h \
  --start 2024-01-01 --end 2024-02-01 \
  --columns event_time,open,high,low,close,volume \
  --csv > btcusdt-1h.csv
```

---

## Complete Walkthrough: Register → Pipeline → Verify

This section documents the full lifecycle from dataset registration to data verification.

### Step 0: Proxy Configuration (if needed)

Binance API requires a proxy for many networks. Set the `HTTPS_PROXY` environment variable:

```bash
# Option 1: Set in shell session
export HTTP_PROXY=http://127.0.0.1:7897
export HTTPS_PROXY=http://127.0.0.1:7897

# Option 2: Create a dotenv file
cp .env.example .env
echo 'HTTPS_PROXY=http://127.0.0.1:7897' >> .env

# Option 3: Pass inline for a single command
HTTPS_PROXY=http://127.0.0.1:7897 chronoforge pipeline run --dataset btcusdt-ohlcv-1h --mode backfill ...
```

### Step 1: Bootstrap Source Registry

```bash
chronoforge registry sync
# Output: source_registry bootstrapped: 7 sources
```

This populates `source_registry` in the SQLite metadata store with 7 pre-configured sources:

| source_id | Access Type | Description |
|-----------|-------------|-------------|
| `binance_spot` | PUBLIC | Binance Spot market data |
| `binance_futures` | PUBLIC | Binance USDT-M Futures |
| `deribit` | PUBLIC | Deribit derivatives |
| `ccxt` | PUBLIC | CCXT bridge (exchange-agnostic) |
| `yahoo` | PUBLIC | Yahoo Finance |
| `fred` | PUBLIC_WITH_KEY | FRED macroeconomic data |
| `sec_edgar` | PUBLIC | SEC EDGAR filings |

### Step 2: Register Datasets

A **dataset** binds a data source to a canonical type. Register before running the pipeline.

**Example 1: BTC-USDT Spot OHLCV (1h & 4h)**

```bash
# 1-hour candles
chronoforge dataset add \
  --dataset-id btcusdt-ohlcv-1h \
  --source binance_spot \
  --type OHLCV \
  --entity-id BTCUSDT \
  --params '{"symbol":"BTCUSDT","interval":"1h"}' \
  --frequency 1h \
  --continuity-model ALWAYS_OPEN

# 4-hour candles
chronoforge dataset add \
  --dataset-id btcusdt-ohlcv-4h \
  --source binance_spot \
  --type OHLCV \
  --entity-id BTCUSDT \
  --params '{"symbol":"BTCUSDT","interval":"4h"}' \
  --frequency 4h \
  --continuity-model ALWAYS_OPEN
```

**Example 2: ETH Funding Rate**

```bash
chronoforge dataset add \
  --dataset-id eth-funding-rate \
  --source binance_spot \
  --type FUNDING \
  --entity-id ETHUSDT \
  --params '{"symbol":"ETHUSDT"}' \
  --frequency 8h \
  --continuity-model TRADING_CALENDAR
```

**Example 3: S&P 500 via CCXT**

```bash
chronoforge dataset add \
  --dataset-id sp500-ohlcv-1h \
  --source ccxt \
  --type OHLCV \
  --entity-id SPX \
  --params '{"symbol":"SPX/USD","timeframe":"1h"}' \
  --frequency 1h \
  --continuity-model TRADING_CALENDAR
```

**Key fields:**

| Field | Description |
|-------|-------------|
| `--dataset-id` | Unique identifier (primary key) |
| `--source` | Source from `registry sync` |
| `--type` | Canonical type (OHLCV, FUNDING, TRADE, etc.) |
| `--entity-id` | Entity identifier (defaults to symbol or dataset_id) |
| `--params` | Source-specific query parameters as JSON object |
| `--frequency` | Data frequency (`1m`, `5m`, `15m`, `1h`, `4h`, `1d`, `1w`) |
| `--continuity-model` | Gap handling: `ALWAYS_OPEN` / `TRADING_CALENDAR` / `EVENT_BASED` / `RELEASE_SCHEDULE` |
| `--revision-supported` | Enable point-in-time queries (for NUMBER, POSITION, etc.) |

> **Binance symbol format**: Use the raw symbol without slashes — `BTCUSDT` not `BTC/USDT`. The API validates against `^[\\w\\-._&&[^a-z]]{1,50}$`.

### Step 3: Dry Run

Before executing, preview the job plan:

```bash
chronoforge pipeline run \
  --dataset btcusdt-ohlcv-1h \
  --mode backfill \
  --start 2021-01-01T00:00:00 \
  --end 2025-09-22T00:00:00 \
  --dry-run
# Output: job dataset=btcusdt-ohlcv-1h mode=backfill start=2021-01-01T00:00:00 ...
```

### Step 4: Run the Pipeline

The pipeline executes **7 stages** per dataset:

```
fetch → raw_append → validate → normalize → canonical → quality → runlog
```

**Incremental fetch (resume from last checkpoint):**

```bash
chronoforge pipeline run --dataset btcusdt-ohlcv-1h --mode incremental
```

**Full backfill (explicit date range):**

```bash
chronoforge pipeline run \
  --dataset btcusdt-ohlcv-1h \
  --mode backfill \
  --start 2021-01-01T00:00:00 \
  --end 2025-09-22T00:00:00
```

**Windowed backfill (memory-bounded, chunks time ranges):**

```bash
chronoforge pipeline run \
  --dataset btcusdt-ohlcv-1h \
  --mode backfill \
  --start 2021-01-01T00:00:00 \
  --end 2025-09-22T00:00:00 \
  --window-seconds 7776000
```

> **Window size**: `7776000` seconds = 90 days per chunk. This keeps memory usage bounded while processing large date ranges. For shorter ranges, use smaller windows.

**Verify pipeline status:**

```bash
# Check run history
chronoforge pipeline status --last 20 --dataset btcusdt-ohlcv-1h --json

# Check specific run details
chronoforge pipeline status --dataset btcusdt-ohlcv-1h --json
```

Expected output:
```json
[
  {
    "run_id": "...",
    "dataset_id": "btcusdt-ohlcv-1h",
    "status": "SUCCESS",
    "checkpoint_after": "2025-09-22T00:00:00",
    "output_count": 1162,
    "duplicate_count": 2,
    "latency_ms": 1992
  }
]
```

### Step 5: Query Data

ChronoForge exposes canonical data via **DuckDB views** (zero-copy, hive partitioning).

**Time-range query:**

```bash
chronoforge query \
  --dataset btcusdt-ohlcv-1h \
  --start 2024-01-01T00:00:00 \
  --end 2024-02-01T00:00:00
```

**CSV output for downstream tools:**

```bash
chronoforge query \
  --dataset btcusdt-ohlcv-1h \
  --start 2024-01-01 --end 2024-02-01 \
  --columns event_time,open,high,low,close,volume \
  --csv > btcusdt-2024-q1.csv
```

**JSON output (for programmatic consumption):**

```bash
chronoforge query \
  --dataset btcusdt-ohlcv-1h \
  --start 2024-01-01 --end 2024-01-02 \
  --json
```

**Filter + column projection:**

```bash
chronoforge query \
  --dataset btcusdt-ohlcv-1h \
  --start 2024-06-01 --end 2024-06-07 \
  --filters '{"high":50000}' \
  --columns event_time,open,high,low,close,volume
```

### Step 6: Data Verification

**Check data completeness:**

```bash
# Count total records in a time range
chronoforge query --dataset btcusdt-ohlcv-1h \
  --start 2024-01-01 --end 2024-01-02 \
  --json | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'Rows: {len(d[\"rows\"])}')"

# Expected: 24 rows for 1h data (one per hour)
```

**Verify raw files exist:**

```bash
# List canonical Parquet files
find data/canonical/OHLCV/ -name "*.parquet" | head -10

# Count total files
find data/canonical/OHLCV/ -name "*.parquet" | wc -l
```

**Check metadata store:**

```bash
# List all datasets
chronoforge registry list-datasets --source binance_spot

# Check run log entries
sqlite3 meta/chronoforge.db \
  "SELECT dataset_id, status, checkpoint_after, output_count FROM run_log WHERE dataset_id LIKE 'btcusdt%' ORDER BY started_at DESC LIMIT 10;"
```

**Validate data integrity via raw storage:**

```bash
# List raw JSONL files (immutable source payloads)
find data/raw/ -name "*.jsonl" | head -10

# Check a raw file size
ls -lh data/raw/binance_spot/btcusdt-ohlcv-1h/ingest_date=2024-01-01/*.jsonl
```

**Verify with DuckDB directly:**

```bash
# Launch DuckDB and query the registered views
duckdb meta/query.duckdb -c "
  SHOW TABLES;
  DESCRIBE SELECT * FROM ohlcv_btcusdt_1h;
  SELECT COUNT(*) FROM ohlcv_btcusdt_1h WHERE event_time BETWEEN '2024-01-01' AND '2024-01-02';
"
```

---

## Monitor & Maintain

```bash
# Check run history (last 50 runs)
chronoforge pipeline status --last 50

# Filter by dataset
chronoforge pipeline status --dataset btcusdt-ohlcv-1m

# View quality flags
chronoforge quality report
chronoforge quality report --dataset btcusdt-ohlcv-1m --json

# List all datasets (optionally filter by source)
chronoforge registry list-datasets --source binance_spot
chronoforge registry list-sources

# Reset circuit breaker for a dataset
chronoforge pipeline circuit-reset --dataset btcusdt-ohlcv-1m

# Incremental fetch (resume from last checkpoint)
chronoforge pipeline run --dataset btcusdt-ohlcv-1h --mode incremental
```

---

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                     CLI (typer)                          │
│  chronoforge pipeline run | query | dataset add ...      │
├──────────────────────────────────────────────────────────┤
│                    Service Layer                         │
│  PipelineRunner │ Registry │ Quality │ Research          │
├────────────┬─────────────────┬────────────┬──────────────┤
│ Connectors │  Raw Store      │ Canonical  │  Meta Store  │
│ (API)      │  (JSONL)        │  (Parquet) │  (SQLite)    │
├────────────┴─────────────────┴────────────┴──────────────┤
│                    Data Sources                          │
│  Binance │ Deribit │ CCXT │ Yahoo │ FRED │ SEC EDGAR     │
└──────────────────────────────────────────────────────────┘
```

**Three-layer storage model:**

| Layer | Format | Purpose |
|-------|--------|---------|
| **Raw** | JSONL (partitioned by `ingest_date`) | Immutable source payload, base64-encoded |
| **Canonical** | Parquet (partitioned by `year`/`month`, hive partitioning) | Normalized, typed, queryable data |
| **Meta** | SQLite (`meta_dir/`) | Registry, run logs, checkpoints, quality flags |

**Canonical type system** — 27 types across 7 asset classes:

| Class | Types |
|-------|-------|
| **Market Data** | OHLCV, TRADE, TICKER, ORDERBOOK, FUNDING, OPEN_INTEREST |
| **Derivatives** | OPTION, IMPLIED_VOLATILITY, GREEKS, LIQUIDATION_EVENT, LIQUIDATION_AGGREGATE |
| **Macro** | NUMBER, FLOW, MACRO_EVENT |
| **Fundamental** | FUNDAMENTAL, FILING, DOCUMENT |
| **Positioning** | POSITION, POSITION_AGGREGATE |
| **Prediction Markets** | PREDICTION_MARKET, PREDICTION_PRICE |
| **Text / Reference** | TEXT_MESSAGE, TEXT_EVENT, ENTITY, INSTRUMENT |
| **Derived** | DERIVED, FEATURE |

---

## Installation

```bash
# Clone the repo
git clone https://github.com/chronoforge/chronoforge.git
cd chronoforge

# Install in dev mode (Python 3.12+)
pip install -e '.[dev]'

# Verify
chronoforge --help
```

### Requirements

- **Python** >= 3.12
- **DuckDB** >= 1.0 (OLAP query engine)
- **PyArrow** >= 17 (Parquet I/O)
- **Pydantic** >= 2.7 (canonical schema validation)
- **CCXT** >= 4 (crypto exchange abstraction)

### Configuration

```bash
# Copy env template
cp .env.example .env

# Edit .env with your API keys and proxy
# Required at minimum:
#   FRED_API_KEY        (for FRED data source)
#   CHRONOFORGE_SEC_CONTACT  (for SEC EDGAR User-Agent)
#   HTTPS_PROXY         (for Binance access from China)
```

**Environment variables** (prefix `CHRONOFORGE_` unless noted):

| Variable | Default | Description |
|----------|---------|-------------|
| `CHRONOFORGE_ENV` | `default` | Runtime environment |
| `CHRONOFORGE_DATA_DIR` | `./data` | Raw/Canonical/Derived data root |
| `CHRONOFORGE_META_DIR` | `./meta` | SQLite metadata directory |
| `CHRONOFORGE_LOG_LEVEL` | `INFO` | Log level (DEBUG/INFO/WARNING/ERROR) |
| `CHRONOFORGE_HTTP_TIMEOUT` | `30` | HTTP timeout in seconds |
| `CHRONOFORGE_RETRY_MAX` | `5` | Max retry attempts (exponential backoff) |
| `CHRONOFORGE_QUALITY_BLOCK` | `Q-SCHEMA-001,Q-PROV-001` | Quality rules that block ingestion |
| `FRED_API_KEY` | *(empty)* | FRED API key (required for `fred` source) |
| `CHRONOFORGE_SEC_CONTACT` | *(empty)* | SEC EDGAR contact email |
| `HTTPS_PROXY` | *(empty)* | HTTP proxy for API access (e.g., ClashVerge on port 7897) |

---

## Data Layout

```
data/
├── raw/                          # Immutable JSONL
│   └── {source}/{dataset}/
│       └── ingest_date={YYYY-MM-DD}/
│           └── {HHmmss}-{seq}.jsonl
├── canonical/                    # Columnar Parquet
│   └── {type}/
│       └── entity={id}/
│           └── year={YYYY}/
│               └── month={MM}/
│                   └── part-{seq}.parquet
meta/
├── chronoforge.db                # SQLite: registry, run logs, checkpoints
└── query.duckdb                  # DuckDB catalog (view registry)
```

**Canonical storage guarantees:**

- **Merge-rewrite upsert** — old + new data merged by natural key, keep-last semantics
- **Atomic writes** — temp dir → fsync → rename-swap with `.old-*` backup
- **Drift detection** — value changes flagged as quality findings on merge
- **Frozen Arrow schema** — each CanonicalType has a declared schema; type coercion on legacy data

---

## Pipeline Internals

### 7-Stage Execution

| Stage | Name | Input | Output | Failure Semantics |
|-------|------|-------|--------|-------------------|
| 1 | **Fetch** | Window chunks | RawBatch iterator | TransportError: retry; ProviderError: skip chunk |
| 2 | **RawAppend** | RawBatches | JSONL refs with fsync | StorageError: run FAILED |
| 3 | **Validate** | RawBatches | Validated batches | SchemaError: run FAILED |
| 4 | **Normalize** | RawBatches + refs | Canonical dicts | Error rate > threshold: QualityError |
| 5 | **Canonical** | Canonical dicts | Parquet upserts | StorageError: run FAILED |
| 6 | **Quality** | Canonical groups | Quality findings | block_on hit: QualityError |
| 7 | **RunLog** | All counts | SQLite records | Terminal state |

### Fault Tolerance

- **Circuit breaker** — 3 consecutive FAILED runs opens circuit; after cooldown period, one probe run is allowed (half-open). Success resets; failure re-arms cooldown.
- **Checkpoint alignment** — cursor only advances on SUCCESS/PARTIAL_SUCCESS; FAILED leaves cursor at last good position.
- **Crash recovery** — `startup_repair` on write-command start: releases stale locks, cleans orphaned files, reconciles run states.

---

## Testing

```bash
# Run all tests (smoke tests skipped by default)
pytest

# Run smoke tests (requires real external sources)
pytest -m smoke

# Run quality tests only
pytest -m quality

# Run with coverage
pytest --cov=chronoforge --cov-report=term-missing
```

**Test categories:**

- **Unit tests** — model validation, storage operations, pipeline stages
- **Property-based tests** — Hypothesis tests for invariant preservation
- **Architecture tests** — import layer enforcement, module boundary rules
- **Quality tests** — data quality rule validation on ingested data

---

## Project Structure

```
chronoforge/
├── src/chronoforge/
│   ├── cli/                    # CLI commands (thin layer, zero business logic)
│   │   ├── main.py             # Typer app entry point
│   │   ├── _wiring.py          # Dependency injection & service construction
│   │   ├── pipeline_cmd.py     # pipeline run/replay/status/circuit-reset
│   │   ├── query_cmd.py        # query (top-level command)
│   │   ├── dataset_cmd.py      # dataset add
│   │   ├── registry_cmd.py     # registry sync/list-sources/list-datasets
│   │   ├── quality_cmd.py      # quality report
│   │   └── research_cmd.py     # research reproduce
│   ├── config/
│   │   └── settings.py         # Settings via pydantic-settings
│   ├── connectors/             # Data source adapters
│   │   ├── base.py             # DataConnector protocol
│   │   ├── binance_spot.py
│   │   ├── binance_futures.py
│   │   ├── deribit.py
│   │   ├── ccxt_bridge.py
│   │   ├── yahoo.py
│   │   ├── fred.py
│   │   └── sec_edgar.py
│   ├── models/                 # Canonical data models
│   │   ├── base.py             # BaseRecord (schema + provenance + quality)
│   │   ├── enums.py            # CanonicalType enum (27 types)
│   │   ├── market.py           # OHLCV, TRADE, TICKER, etc.
│   │   ├── derivatives.py
│   │   ├── macro.py
│   │   └── ...
│   ├── storage/                # Storage layer
│   │   ├── meta.py             # MetaStore (SQLite: registry, run_log, checkpoints)
│   │   ├── raw.py              # RawStore (JSONL append-only)
│   │   ├── canonical.py        # CanonicalStore (Parquet merge-rewrite upsert)
│   │   ├── views.py            # DuckDB view registration
│   │   └── consistency.py      # Startup repair (lock release, orphan cleanup)
│   ├── pipeline/               # Pipeline orchestration
│   │   ├── runner.py           # PipelineRunner (7-stage executor)
│   │   ├── replay.py           # Layer replay service
│   │   └── windows.py          # Windowed execution
│   ├── quality/                # Quality rules
│   │   └── rules.py            # Rule evaluation engine
│   ├── research/               # Research utilities
│   │   └── query.py            # DuckDBQueryService + snapshot reproduction
│   ├── registry/               # Registry service
│   │   └── service.py          # Source/dataset registry operations
│   └── security/               # Security primitives
│       └── secret.py           # SecretStr wrapper
├── tests/                      # Test suite
├── docs/                       # Architecture + design + implementation tasks
├── pyproject.toml
└── .env.example
```

---

## Architecture Principles

1. **Connector separation** — Connectors fetch + normalize; never write storage directly
2. **Immutable raw layer** — JSONL files are append-only, never modified
3. **Atomic canonical writes** — temp → fsync → rename-swap, crash-safe
4. **Thin CLI layer** — Zero business logic; parameters → service call → render
5. **Frozen canonical schema** — Arrow schemas declared per type; type coercion on legacy data
6. **Checkpoint-driven resume** — Pipeline state tracked in SQLite; safe restart at any point
7. **DuckDB-native query** — Hive-partitioned Parquet views, zero data copy

---

## License

Apache 2.0
