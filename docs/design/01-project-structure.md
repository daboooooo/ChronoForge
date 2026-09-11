# D01 — 项目结构与公共约定

依据：架构 01/02/06。本文档定义包结构、依赖清单、公共类型与错误层级，是所有模块实施的基线。

## 1. 目录结构

```
ChronoForge/
├── pyproject.toml
├── .env.example
├── .importlinter              # 与 09-testing-plan.md §2 一致
├── src/chronoforge/
│   ├── __init__.py            # __version__
│   ├── models/                # Canonical Schema（零业务依赖）
│   │   ├── base.py            # BaseRecord / provenance / 枚举
│   │   ├── enums.py           # CanonicalType / QualityStatus / ...
│   │   ├── market.py          # TICKER/TRADE/OHLCV/ORDERBOOK/FUNDING/OI
│   │   ├── derivatives.py     # OPTION/IV/GREEKS/LIQUIDATION_*
│   │   ├── macro.py           # NUMBER/FLOW/MACRO_EVENT
│   │   ├── fundamental.py     # FUNDAMENTAL/FILING/DOCUMENT
│   │   ├── positioning.py     # POSITION/POSITION_AGGREGATE
│   │   ├── prediction.py      # PREDICTION_MARKET/PREDICTION_PRICE
│   │   ├── text.py            # TEXT_MESSAGE/TEXT_EVENT
│   │   ├── reference.py       # ENTITY/INSTRUMENT 解析器
│   │   └── derived.py         # DERIVED/FEATURE
│   ├── config/
│   │   └── settings.py        # Settings（见 D08）
│   ├── connectors/
│   │   ├── base.py            # DataConnector 协议 / RawBatch / FetchRequest
│   │   ├── ratelimit.py       # 令牌桶限流器
│   │   ├── binance_spot.py    # 直连 Spot
│   │   ├── binance_futures.py # 直连 USDT-M Futures
│   │   ├── deribit.py
│   │   ├── ccxt_bridge.py     # ccxt 抽象层
│   │   ├── yahoo.py
│   │   ├── fred.py
│   │   ├── sec_edgar.py
│   │   └── errors.py          # 错误映射（HTTP → 错误类）
│   ├── storage/
│   │   ├── base.py            # RawStore/CanonicalStore/DerivedStore/MetaStore 协议
│   │   ├── raw.py             # JSONL 分区写
│   │   ├── canonical.py       # Parquet + merge-rewrite
│   │   ├── derived.py
│   │   ├── meta.py            # SQLite（含迁移函数注册表）
│   │   ├── migrations/        # 0001_init.py ...
│   │   └── views.py           # DuckDB 视图注册
│   ├── quality/
│   │   ├── rules.py           # 规则注册表（Q-xx-nnn，见 D06）
│   │   └── report.py          # QualityReport
│   ├── registry/
│   │   └── service.py         # SourceRegistry/DatasetRegistry
│   ├── pipeline/
│   │   ├── runner.py          # PipelineRunner / 阶段编排
│   │   ├── state.py           # RunStatus 状态机
│   │   └── replay.py
│   ├── features/
│   │   └── engine.py          # FeatureEngine 协议 + 内置指标
│   ├── research/
│   │   ├── query.py           # QueryService 实现
│   │   └── snapshot.py        # ResearchSnapshot
│   ├── cli/
│   │   ├── main.py            # typer app
│   │   ├── pipeline_cmd.py / dataset_cmd.py / query_cmd.py / quality_cmd.py
│   └── logging.py             # JSON formatter + redaction（见 D08 §4）
├── tests/
│   ├── unit/  ├── integration/  ├── quality/  ├── e2e/  ├── architecture/
│   └── fixtures/{source}/{endpoint}/*.json
├── data/   # 运行时生成（raw/canonical/derived/features），.gitignore
└── meta/chronoforge.db        # 运行时生成，.gitignore
```

## 2. 依赖清单（pyproject.toml，对应架构 06 决策表）

| 依赖 | 版本约束 | 用途 |
|---|---|---|
| python | >=3.12 | D1 |
| pydantic | ^2.x | Canonical Schema（D4） |
| pydantic-settings | ^2.x | 配置（D4/约束 §25） |
| duckdb | ^1.x | OLAP 查询（D2） |
| pyarrow | ^17+ | Parquet 读写 |
| httpx | ^0.27+ | HTTP client（D5） |
| ccxt | ^4.x | 交易所抽象层（D6） |
| typer | ^0.12+ | CLI（D7） |
| pandas / polars | 两者均锁版本 | 研究 / 批量（约束 §35） |
| structlog | ^24+ | 结构化日志（实施基线，如冲突改 stdlib JSON） |

dev 组：pytest、import-linter、ruff、mypy、pip-audit。锁定 lock file（uv）。

## 3. 公共类型与错误层级

```python
# connectors/errors.py —— 架构 08 §1 的错误分类，全部继承 ChronoForgeError
class ChronoForgeError(Exception): ...
class TransportError(ChronoForgeError): ...      # 重试
class RateLimitError(TransportError): ...        # 重试 + 退避尊重 Retry-After
class AuthError(ChronoForgeError): ...           # 不重试，run FAILED
class ProviderError(ChronoForgeError): ...       # 4xx，不重试
class SchemaError(ChronoForgeError): ...         # 不重试，dataset 熔断
class QualityError(ChronoForgeError): ...        # 由配置决定阻断/放行
class StorageError(ChronoForgeError): ...
class ConfigError(ChronoForgeError): ...         # 启动 fail fast
```

- 错误必须携带 `context: dict`（source/dataset/endpoint/status），禁止裸 raise 字符串
- 捕获规则：仅 pipeline 顶层捕获并归类入 run_log；模块内只抛出，不吞（禁 `except: pass`）

## 4. 运行时全局约定

- **时区**：所有 `datetime` 存储为 UTC naive（Parquet `timestamp[us]`）；API 交互层负责 tz 转换；`ingest_time = datetime.now(UTC).replace(tzinfo=None)`
- **ID 规则**：`run_id`/`ingest_batch_id` = `uuid4().hex[:12]`；replay run 的 `run_id` 前缀 `R`（如 `Ra1b2c3d4e5f6`，仍 12 位）；`schema_version` 语义化 `MAJOR.MINOR`（breaking.additive）
- **路径解析**：全部经 `Settings.data_dir/meta_dir`，禁止模块内硬编码路径
- **日志**：全部经 `chronoforge.logging.get_logger()`；结构化 JSON；redaction 规则见 D08 §4

## 5. 实施顺序（依赖序）

models → config/logging → storage(raw→meta→canonical→views) → connectors(base→各源) → quality → registry → pipeline → features → research → cli。每层完成即补 architecture test。
