# D08 — CLI、配置与安全实施设计

依据：架构 02/09、06。本文档定义 Settings 全字段、CLI 命令树、日志脱敏实现。

## 1. Settings（config/settings.py，pydantic-settings）

| 字段 | env | 类型 | 默认 | 说明 |
|---|---|---|---|---|
| env | CHRONOFORGE_ENV | Literal[default,development,test,production] | default | 多环境（约束 §25） |
| data_dir | CHRONOFORGE_DATA_DIR | Path | ./data | |
| meta_dir | CHRONOFORGE_META_DIR | Path | ./meta | SQLite 所在 |
| log_level | CHRONOFORGE_LOG_LEVEL | str | INFO | |
| fred_api_key | FRED_API_KEY | SecretStr | — | 缺失且启用 FRED → ConfigError 启动失败 |
| sec_contact_email | CHRONOFORGE_SEC_CONTACT | EmailStr | — | SEC User-Agent 必填（D04 §4.7） |
| sec_user_agent | —（派生） | str | ChronoForge/{ver} research ({email}) | |
| http_timeout_s | CHRONOFORGE_HTTP_TIMEOUT | float | 30 | |
| retry_max | CHRONOFORGE_RETRY_MAX | int | 5 | |
| quality_block_on | CHRONOFORGE_QUALITY_BLOCK | list[str] | [Q-SCHEMA-001,Q-PROV-001] | D06 §3 |
| normalize_error_threshold | — | float | 0.10 | D05 |
| rate_overrides_json | CHRONOFORGE_RATE_OVERRIDES | dict | {} | 测试注入 |

- `.env.example`：上表全部 env 名 + 注释（无值）；`.env` 在 `.gitignore`
- 加载顺序：defaults ← `.env` ← 环境变量；`Settings.load()` 单入口，模块间显式传递（架构 02）
- **配置四分类（最终审计 §30，不得混淆机制）**：
  - **Static**（随代码版本走）：.importlinter、pyproject、错误映射表、D05 §5.2 语义表、词表 D08 §3
  - **Runtime**（Settings/env）：上表除 Secret 外全部——可按环境覆盖，不入库
  - **Secret**：`fred_api_key`（SecretStr，仅 env/.env，永不落盘/入库/入日志）
  - **State**（运行时状态，禁入 Settings）：checkpoints/run_log/quality_flags/dataset_registry（SQLite）；raw/canonical/derived（文件系统）——用 MetaStore/Store API 管理，与配置机制物理隔离
- test 环境强制 `data_dir/meta_dir` 指向 tmp_path（conftest fixture `settings`）

## 2. CLI 命令树（cli/main.py，typer）

```
chronoforge
├── pipeline run --dataset DS [--start --end --mode incremental|backfill] [--all-due] [--dry-run]
├── pipeline replay --layer canonical|derived --dataset DS
├── pipeline status [--dataset DS] [--last N]         # 读 run_log
├── registry sync                                     # bootstrap_defaults（D04 §5）
├── registry list-sources / list-datasets
├── dataset add --source S --type T --params JSON     # 写 dataset_registry
├── query --dataset DS [--start --end --asof] [--filters JSON] [--json|--csv]
├── quality report [--dataset DS]                     # D06 §4
└── research reproduce --snapshot ID
```

约束执行：cli 层零业务逻辑（参数解析→service 调用→渲染，架构 02 规则 3）；`--dry-run` 输出将执行的 dataset 与 cursor 不落盘；`--json` 输出结构 = QueryResult 字段直序列化（D07 §4）。

## 3. 日志（logging.py）

- structlog：JSON lines → stderr；run_id/dataset 贯穿 contextvar
- 级别：INFO=阶段/重试/锁；WARNING=质量 finding、429；ERROR=终态失败
- 事件名受控词表：pipeline.stage / connector.retry / storage.upsert / quality.finding / run.finish（新事件名需入本表——architecture test 校验字符串集）

## 4. 脱敏（架构 09 §3 实现）

```python
REDACT_KEYS = {"authorization", "api_key", "apikey", "token", "cookie", "set-cookie",
               "x-api-key", "fred_api_key"}
def redact(obj: Any) -> Any:    # 递归 dict/list；命中键→"***"；URL query 参数同规则掩码
```

- structlog processor 链末位 `redact_processor`；异常链 format 前 redact
- 测试断言：构造含 key 的 context/异常 → 序列化输出无明文（D09 TC-SEC-001/002）

## 5. 凭证与网络边界（架构 09 §2 落地）

- SecretStr 全程不进日志/repr；httpx client 由 `connectors.base.make_client(settings)` 统一构造（timeout、User-Agent 注入、无代理旁路）
- 仅公开行情端点；任何写入类 API 调用代码禁止合入（code review checklist + grep CI：`POST|PUT|DELETE` 于 connector 目录 → 除 Deribit public JSON-RPC POST 外需白名单）

## 6. 测试依据（D09 TC-X 组）

- Settings 加载优先级矩阵（.env < 环境变量）；FRED 启用无 key → ConfigError
- 每个命令：参数→服务调用映射（mock service 断言调用参数）；--json 输出 schema 快照
- redact 单测 + 集成（触发 401 响应日志）
