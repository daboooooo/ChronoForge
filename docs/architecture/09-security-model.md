# 09 — Security Model

依据：约束 §25/§59/§60/§61/§82.9；需求 §39/§40

## 1. 数据源访问分级（需求 §39）

source_registry 中每源记录 `access_type`：

| 级别 | 源 |
|---|---|
| PUBLIC | Binance / Deribit / Polymarket 公开行情端点、SEC data.sec.gov（需合规 User-Agent）、CFTC、GDELT |
| PUBLIC_WITH_KEY | FRED（API key）、BLS |
| AUTHENTICATED | 第一阶段不使用 |

第一阶段仅使用 public market-data endpoints（需求 §39）；免费 ≠ 无限制——rate_limit / historical_limit / license / redistribution_policy 全部录入 source_registry（需求 §40）。

## 2. 凭证管理（约束 §25/§59/§60）

- 凭证仅经环境变量 / `.env`（gitignored）注入，由 `config.Settings`（pydantic-settings）显式加载
- 仓库提交 `.env.example`（仅键名，无值）；敏感 `.env` 永不入 Git
- 禁止凭证出现在：源代码、日志、异常信息、研究结果导出
- API key 采用最小权限：只读行情 key，无交易/提币权限

## 3. 日志脱敏（约束 §61）

- 结构化日志输出前经 redaction 过滤器：剥离 `authorization`、`api_key`、`token`、`cookie` 等字段
- 禁止 `print(headers)` / `print(api_response)` 类调试输出进入主干
- 异常消息可能携带 URL（含 key 参数）时，按参数黑名单掩码

## 4. Fair-Access 合规（需求 §39/§40）

- SEC：请求携带合规 User-Agent，遵守 fair-access 频率规则（官方要求）
- 各源 rate limit 在 connector 层集中实现（约束 §28），并写入 source_registry
- CFTC：避免过度访问；Binance/Deribit/Polymarket 优先 public endpoint

## 5. 供应链（约束 §58/§59）

- 依赖锁定版本 + 定期安全审计（`pip-audit` 或等价工具，CI 可选）
- 新增依赖前评估维护状态与已知 CVE
