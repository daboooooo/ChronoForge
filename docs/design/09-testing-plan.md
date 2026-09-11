# D09 — 测试计划

依据：架构 07 + D01–D08 各"测试依据"条目。本文件汇总为可执行用例清单；TC-ID 即 pytest 用例 docstring 首行，与本文档一一对应。

## 1. 目录与映射

| tests/ | 对象 | marker |
|---|---|---|
| unit/ | models / quality.rules / ratelimit / redact / settings | 默认 |
| integration/ | connectors(fixture) / pipeline stages / storage | 默认 |
| quality/ | 入库数据质量 SQL 检查 | `quality` |
| e2e/ | 真实源冒烟 | `smoke`（CI 跳过） |
| architecture/ | import-linter + 视图/registry/事件名词表一致性 | 默认 |

**测试真实性要求（最终审计 §36）**：Recovery 用例（TC-P-009/TC-S-004/TC-S-007）禁止仅 mock 异常返回——必须构造真实文件系统/SQLite 中间状态（temp 目录残留、run_log 缺行、损坏 db），重启被测对象后断言恢复；crash 模拟点 = 原子协议的每个边界（rename 前/后、checkpoint 写前/后）。

**需求追踪链（最终审计 §54）**：ChronoForge.md 需求条目 → 架构 01 §7 四原则 + 06 决策表 → 本文件 §3 TC-ID + §4 原则映射 → D10 任务单 tests/acceptance 字段；每张任务单的 acceptance 即其覆盖需求的最终验收，任何 Critical Invariant（§G 七不变量）至少映射一个 TC（已满足，见 §4）。

## 2. Architecture Tests（.importlinter 完整文件 + 附加断言）

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
[importlinter:contract:forbidden_research_connectors]
name = research must not import connectors
type = forbidden
source_modules = chronoforge.research, chronoforge.features
forbidden_modules = chronoforge.connectors
```

附加（pytest 内省断言）：① views.py 注册视图集合 == CanonicalType 集合；② quality 规则注册表无重复 rule_id；③ logging 事件词表 == 代码中实际事件名集合。

## 3. 用例清单（核心断言）

### TC-M 模型（D02 §6）
- TC-M-001 OHLCV 合法样例通过；TC-M-002 high<low 拒绝；TC-M-003 裸 timestamp 拒绝（extra=forbid）
- TC-M-004 parse_deribit 全字段；TC-M-005 月份码非法拒绝；TC-M-006 过期合约→SUSPECT 标注不拒绝
- TC-M-007 NUMBER 双 vintage 两行共存；TC-M-008 iv>5 拒绝；TC-M-009 provenance 缺字段→Q-PROV-001 finding

### TC-S 存储（D03 §7）
- TC-S-001 upsert 幂等（二次 inserted=0）；TC-S-002 merge 保留旧行；TC-S-003 dataset 锁互斥
- TC-S-004 rename 后 run_log 缺失→reconciliation 补记；TC-S-005 as-of 视图取正确 vintage
- TC-S-006 revision 类 (nk,revision_time) 追加不覆盖；TC-S-007 temp/p.old 孤儿清理

### TC-C 连接器（D04 §6）
- TC-C-001~007 每源契约（fixture→canonical 逐字段比对）：binance_spot/futures、deribit、ccxt_bridge、yahoo、fred、sec
- TC-C-008 429+Retry-After→冷却；TC-C-009 401 不重试 run FAILED；TC-C-010 4xx 不重试
- TC-C-011 分页无重叠无缺口；TC-C-012 yahoo split→adjustment 四元组；TC-C-013 ccxt capability detection
- TC-C-014 SEC User-Agent 注入断言；TC-C-015 SchemaError 熔断
- TC-C-016 边界补齐（审计 F-13）：闰日 02-29、DST 切换日（3 月/11 月）、年边界 12-31→01-01、max/min page size（1000/1）、午夜 00:00 分区归属

### TC-P 管道（D05 §8）
- TC-P-001 七阶段顺序（spy 装配断言）；TC-P-002 各错误类→终态映射表逐行断言
- TC-P-003 PARTIAL 保留成功部分；TC-P-004 熔断 3 连败；TC-P-005 cursor 仅终态更新
- TC-P-006 replay canonical 字节一致；TC-P-007 run_many 部分失败聚合；TC-P-008 空结果=SUCCESS(0行)
- TC-P-009 cursor 三态恢复（审计 F-13/F-14）：回退（新≤旧→跳过+WARNING）、丢失（raw 重放重建）、损坏（meta.rebuild_checkpoints）
- TC-P-010 chunk 语义（审计 F-01）：3-chunk 窗口第 2 chunk 失败 → chunk_failed=1、cursor=第 1 chunk 右界、终态 PARTIAL
- TC-P-011 幂等 8 场景（howto §7）：全重复/部分重复/重叠窗口/cursor 回退/丢失/损坏/中途退出/断网重试

### TC-PROPERTY 性质测试（审计 F-13，hypothesis）

- TC-PROP-001 dedup 幂等律：dedup(dedup(X)) == dedup(X)（任意记录序列）
- TC-PROP-002 区间拼接律：acquire(A,B) + acquire(B,C) ≡ acquire(A,C)（canonical 逐键一致，含 overlap 窗口策略）
- TC-PROP-003 OHLC 不变量：low ≤ min(o,c) ∧ max(o,c) ≤ high ∧ volume ≥ 0（合法/非法混合生成器）
- TC-PROP-004 upsert 交换律：任意两批次以不同顺序 upsert → canonical 终态一致

### TC-Q 质量（D06 §5）
- TC-Q-001~003 Q-GAP-001 黄金样例（连续+分散缺失→区间合并）；TC-Q-004 block_on 阻断不回滚
- TC-Q-007 EXPECTED_GAP（审计 F-06）：构造周末缺失的 yahoo 网格 → INFO 标记不产生 WARNING；交易日缺失 → WARNING
- TC-Q-008 Q-SEQ-001（审计 F-05）：aggTrades 窗口跳号 → finding；对齐 → 无 finding
- TC-Q-009 Q-DRIFT-001（审计 F-04）：同 nk 值变化 → finding 含旧/新 digest（联动 D03 §7 值漂移用例）
- TC-Q-005 每规则触发/不触发成对（17 规则 ×2：Q-SCHEMA-001/Q-TS-001~003/Q-DUP-001/Q-SEQ-001/Q-DRIFT-001/Q-GAP-001~002/Q-RANGE-001~003/Q-NULL-001/Q-OHLC-001/Q-CROSS-001/Q-REV-001/Q-PROV-001）；TC-Q-006 quality --json 报告 schema

### TC-R 查询研究（D07 §5）
- TC-R-001 as-of 两 vintage；TC-R-002 SQL 注入防御；TC-R-003 read_only 写失败
- TC-R-004 snapshot 复现 hash 一致/数据变更后不一致；TC-R-005 特征链依赖注册+replay 一致

### TC-X CLI/配置/安全（D08 §6）
- TC-X-001 配置优先级矩阵；TC-X-002 FRED 无 key ConfigError
- TC-X-003 命令→服务调用映射（每命令）；TC-X-004 --json 输出快照
- TC-SEC-001 redact 递归掩码；TC-SEC-002 异常日志无明文 key；TC-SEC-003 connector 目录禁写方法扫描

## 4. 架构验收测试映射（架构 01 §7 四原则 ← 本表）

| 原则 | 用例 |
|---|---|
| provider 可替换 | TC-C-001 与 TC-C-005 同一 OHLCV 期望样例驱动 binance 直连与 yahoo/ccxt normalize |
| type 不迫使重写 | TC-M-001 全类型样例驱动 + connector 契约测试仅引用既有 type |
| Raw 可追溯 | TC-M-009 + 集成断言 raw_record_id 可回查 RawStore 行 |
| 可重建 | TC-P-006 / TC-R-005 |

## 5. Fixture 清单（tests/fixtures/，每 endpoint ≥：happy/edge/error 三个 JSON）

binance_spot: klines(1页/跨月边界/空数组) aggTrades ticker24hr；binance_futures: klines fundingRate openInterest；deribit: get_instruments get_book_summary tvchart_data；ccxt_binance: fetch_ohlcv(2页)；yahoo: chart(normal/split/dividend/429页)；fred: observations(vintage1/vintage2/dates)；sec: submissions_recent companyfacts(ETag)。
测试数据形态覆盖（最终审计 §38）：normal / boundary（页界/分区界/精度界）/ malformed / incomplete / duplicated / out-of-order / late-arriving（修订迟达）/ corrected（漂移）/ large-volume（≥10⁴ 行合成批次，contract 与 performance 冒烟）/ empty——各源 fixture 按此清单补齐，禁止只有"漂亮正常数据"。

## 6. CI（.github/workflows/ci.yml 步骤）

```
1. ruff check + ruff format --check
2. mypy src/chronoforge（models/storage/pipeline strict；其余 default）
3. pytest -m "not smoke"（unit+integration+architecture，tmp 数据目录）
4. lint-imports
5. pip-audit（每周调度）
```

覆盖率门槛：models/quality 100% 行覆盖（纯逻辑）；整体 ≥85%，`--cov-fail-under` 强制。
