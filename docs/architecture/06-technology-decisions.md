# 06 — Technology Decisions

依据：约束 §43/§44/§56/§77/§78/§82.6。格式：Decision / Alternatives / Evidence / Trade-offs / Confidence。

## 决策总表

| # | Decision | 主要 Alternatives | Confidence |
|---|---|---|---|
| D1 | Python 3.12+ 单语言 | 引入 Rust/Go 做采集 | HIGH（约束 §4 强制） |
| D2 | Parquet + DuckDB（OLAP） | Postgres/TimescaleDB、ClickHouse | HIGH（见 04 §5） |
| D3 | SQLite（元数据 OLTP） | Postgres | HIGH（见 04 §5） |
| D4 | Pydantic v2 定义 Canonical Schema | dataclass、marshmallow | HIGH |
| D5 | httpx 作 HTTP client | requests、aiohttp | MEDIUM |
| D6 | ccxt 抽象层 + 直连 connector 并存 | 仅 ccxt / 仅直连 | HIGH |
| D7 | typer 作 CLI | argparse、click | MEDIUM |
| D8 | pytest + import-linter | unittest | HIGH |
| D9 | ruff + mypy | 各单独工具 | HIGH |
| D10 | APScheduler（P1 后） | cron、Airflow | PROVISIONAL |
| D11 | SQLite 迁移：手写迁移函数 | alembic | HIGH |
| D12 | 测试 fixture：手工响应样例文件 | vcr.py、responses | HIGH |

## 关键决策说明

### D4 Pydantic v2 — HIGH
- Evidence：约束 §56 推荐（dataclasses/Pydantic where appropriate）；运行时校验是 schema-first 与 quality gate 的执行点（约束 §18）
- Alternatives：dataclass（无运行时校验，质量门槛失效）、marshmallow（社区活跃度低于 Pydantic）
- Trade-offs：序列化性能低于手写——本项目写入量级（个人研究）不构成瓶颈

### D5 httpx — MEDIUM
- Evidence：同步/异步统一 API，官方文档支持 timeout/连接池/HTTP/2；未来 WebSocket 需求由各 connector 自选库（如 `websockets`）
- Alternatives：requests（无异步）、aiohttp（仅异步）
- Trade-offs：两条 I/O 模式并存增加少量复杂度；第一阶段以同步批量拉取为主，WebSocket（实时 liquidation）属 P1 直连 binance connector 范围

### D6 ccxt 定位 — HIGH（需求 §6 直接规定）
- ccxt = EXCHANGE ABSTRACTION LAYER，非独立数据源：`source=ccxt, exchange=binance` 与 `source=binance` 并存
- 直连 connector 用于：交易所特有字段、高频 WebSocket、liquidation stream、ccxt 无法表达的数据
- Trade-offs：双路径有映射重复——通过共享 canonical normalize 层收敛

### D8/D9 测试与静态检查 — HIGH
- Evidence：约束 §39/§40/§56；import-linter 是 Python 生态成熟的分层依赖检查工具（官方文档），实现 architecture tests
- Trade-offs：无；工具链均低配置成本

### D10 APScheduler — PROVISIONAL
- 依据不足以定案（约束 §77）：第一阶段数据量与更新频率未量化
- 决策延后至 P1 调度需求明确时，按约束 §42 补 ADR

### D11 / D12 — HIGH（第 2 轮审计新增）
- D11：SQLite 元数据仅 6 表，手写迁移函数按序号执行即足够；alembic 为通用 ORM 迁移框架，收益不抵引入成本（约束 §44 prefer boring technology）
- D12：手工保存的真实响应样例 + `RawBatch` 抽象已使契约测试与 fetch 实现解耦；vcr.py 录制回放与 connector 协议耦合，额外依赖无净收益

## 依赖管理（约束 §58）

- `pyproject.toml` + lock file（uv 或 pip-tools，实施时定）；生产依赖全部显式声明并锁定版本
- 禁止 random pip install；新增依赖需评估维护状态与安全性
