# 07 — Testing Architecture

依据：约束 §39/§40/§82.7；需求 §31

## 1. 测试分层

| 层 | 对象 | 工具/方式 |
|---|---|---|
| Unit | models（schema 校验、时间语义、instrument 解析）、normalize 映射、quality 规则 | pytest，纯函数级，deterministic |
| Integration | connector × 录制 fixture：fetch→raw→normalize→canonical 全链 | pytest + fixtures（录制的真实 API 响应） |
| Data Quality | 已入库数据：gap/duplicate/OHLC 关系/时间序列连续性 | pytest 对 DuckDB/Parquet 运行断言 |
| End-to-End | 小规模真实源冒烟（1 个 symbol、数天数据） | pytest 标记 `@smoke`，手动/低频触发，CI 默认跳过 |
| Architecture | 模块依赖方向与边界规则 | import-linter（见下） |

## 2. Architecture Tests（约束 §40）

用 import-linter 强制 02-module-boundaries.md 的边界规则：

```ini
[importlinter]
root_package = chronoforge

[importlinter:contract:layers]
name = Layered dependency
type = layers
layers =
    chronoforge.cli
    chronoforge.pipeline
    chronoforge.features | chronoforge.research
    chronoforge.quality | chronoforge.registry | chronoforge.connectors | chronoforge.storage
    chronoforge.config
    chronoforge.models
```

关键禁止规则（独立 contract 声明）：
- `models` 不得 import 任何其他业务模块
- `research` / `features` 不得 import `connectors`（Provider Leakage 反模式）
- `quality` 不得 import `connectors`
- `cli` 不得 import `connectors`（只经由 pipeline）

## 3. 金融数据专项测试域（约束 §39）

必须覆盖 deterministic 测试：

- **timezone**：UTC 存储、DST 边界、源时区转换
- **duplicate timestamps / missing intervals**：expected vs actual interval（需求 §31）
- **look-ahead prevention**：研究查询层断言——`release_time > query_asof` 的记录不可见（约束 §11）
- **revision/vintage**：FRED 修订追加新版本，历史值不被覆盖
- **instrument 解析**：`BTC-26SEP26-100000-C` → 结构化字段（需求 §12）
- **corporate action / contract rollover**：adjusted/original 四元组保留（约束 §21）
- **幂等**：同一 raw 重放 canonical，结果逐条一致
- **provider 可替换性（验收，需求 §45 原则 1）**：同一 canonical 期望样例驱动两个不同 connector 的 normalize 输出一致
- **可重建性（验收，需求 §45 原则 3/4）**：删除 derived 分片 → replay → 与原输出逐字节一致
- **revision as-of 正确性（验收，约束 §11）**：构造 FRED 多 vintage 数据，断言 as-of T 查询不返回 T 之后 release/revision 的版本

## 4. Fixture 策略

- `tests/fixtures/{source}/{endpoint}/`：手工保存的真实响应样例（脱敏，不引入 vcr.py——见 06-technology-decisions.md D12）
- CI 禁止打真实外部 API（稳定性 + fair-access，约束 §40 之外的工程常识；实时能力用 `@smoke` 手动门控）
- normalize 映射的期望输出以手写 canonical 样例作断言，形成 source→canonical 契约测试

## 5. CI 检查

`ruff check` + `ruff format --check` + `mypy`（core 模块）+ `pytest`（unit/integration/quality/architecture）+ `lint-imports`。
