# INFRA-001 — 测试脚手架

## 派发信息

- 任务单：D10 §9 INFRA-001（冻结契约）
- 设计依据：D09 §1/§2/§5/§6、D08 §1（env 名）、D10 §0（公共约定）
- Agent：Agent-B
- 派发时间：2026-09-11
- 状态：DONE（2026-09-11 验收通过）

## file_ownership

- `tests/conftest.py`
- `tests/strategies.py`
- `tests/unit/test_strategies.py`（D10 未列，本记录扩展登记：策略自测用例）
- `.github/workflows/ci.yml`
- `tests/fixtures/README.md`

## 交付物契约摘要（详见 D09）

- **conftest.py**：autouse fixture（设置 `CHRONOFORGE_ENV=test` + `CHRONOFORGE_DATA_DIR/CHRONOFORGE_META_DIR` 指向 tmp）；`tmp_stores` fixture 返回 {data_dir, meta_dir} 路径；settings fixture 因 Settings 类未实现而 Deferred（DEF-002）
- **strategies.py**（hypothesis，dict 级生成——MODEL-002 未实现）：`ohlcv()`（D02 §2 OHLCV 字段 + BaseRecord 字段的 canonical dict，合法/显式非法标记）、`trade_seq()`（aggTrades 归集序列：first_id/last_id/count 连续对齐样例 + 跳号样例）、`records()`（通用 BaseRecord 形 dict）
- **ci.yml**：D09 §6 五步（ruff check + format --check / mypy / pytest -m "not smoke" / lint-imports / pip-audit 周调度），Python 3.12，`pip install -e ".[dev]"`
- **fixtures/README.md**：三件套规范（每 endpoint happy/edge/error）+ 十形态覆盖清单（normal/boundary/malformed/incomplete/duplicated/out-of-order/late-arriving/corrected/large-volume/empty）+ 命名约定 `{source}/{endpoint}/{形态}.json`

## 执行记录

| 时间 | 事件 |
|---|---|
| 2026-09-11 | 派发 Agent-B |
| 2026-09-11 | Agent-B 交付（conftest/strategies/test_strategies/ci.yml/fixtures README） |
| 2026-09-11 | Orchestrator 验收：复跑全绿（24 passed）+ CI 五步对照 D09 §6 + TC-ID 约定抽查通过 |

How 层决策（Agent-B，验收认可）：
- ohlcv() 增设可选 reason 参数保证"每类违规≥一例"的确定性；high==low 零区间 bar 允许（不变量仍成立）
- 覆盖率门槛（D09 §6 末行）暂未启用——src 为空壳会恒失败，已在 ci.yml 注释注明启用条件（业务代码落地后随对应任务开启，登记为 DEF-004）
- Orchestrator 补充：.gitignore 增 `.hypothesis/`（Agent-B 报告的缓存目录问题，属仓库级维护非任务单范围）

## Deferred Acceptance

| ID | 验收项 | 依赖 | 关闭条件 |
|---|---|---|---|
| DEF-001 | strategies.ohlcv() 生成记录通过 MODEL-002 schema 校验 | MODEL-002 | MODEL-002 DONE 后补类型实例级策略或校验性用例，本文件勾销 |
| DEF-002 | conftest 的 settings fixture | CLI-001 | Settings 落地后补 fixture，本文件勾销 |
| DEF-004 | CI 覆盖率门槛（--cov-fail-under，models/quality 100%、整体 ≥85%） | 首个含业务逻辑的 storage/quality 任务 | 门槛启用后 CI 绿，本文件勾销 |

## 验收清单（2026-09-11 验收通过）

DoD 16 项：

- [x] 实现 / [x] 公共 API（StorePaths/tmp_stores/strategies 签名）/ [x] 数据契约（env 名逐字对照 D08 §1）
- [x] 错误处理（N/A）/ [x] 日志（N/A）/ [x] 指标（N/A）
- [x] 单测（INFRA-SELF-01~08）/ [x] 边界（跳号注入/零区间 bar）/ [x] 失败（N/A）/ [x] 恢复（N/A）
- [x] 集成（pytest 全绿无网络）/ [x] 静态分析（ruff check + format）/ [x] 类型检查（mypy 14 files）/ [x] 无未声明假设 / [x] 验收通过

acceptance（D10 GWT，Deferred 项除外）：

- [x] Given strategies.ohlcv() 生成 ≥1000 例 Then 全部含必填键且数值 finite、合法/非法标记与内容一致（INFRA-SELF-01~04）
- [x] Given pytest -m "not smoke" Then 本地全绿（无网络依赖）
- [x] CI yml 语法有效（ruby YAML 解析通过；五步顺序与 D09 §6 一致，pip-audit 周调度独立 job）

测试输出摘要：pytest 24 passed；ruff check/format 通过；mypy no issues；lint-imports 2 kept 0 broken

接管性抽查：✅ 通过——conftest docstring 说明 DEF-002 延后原因与补充位置；fixtures README 含三件套/十形态/命名/脱敏完整规范

git commit：4bc6d39（feat(infra)）
