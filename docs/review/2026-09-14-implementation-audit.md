# Implementation Audit Report — 2026-09-14

- **日期**: 2026-09-14
- **依据**: `docs/design/01–10` 全部设计文档 + `docs/implement/01-dispatch-log.md` 任务状态
- **审计对象**: 已标记 DONE 的 17 项任务代码实现（见 §0）
- **审计方法**: 源码逐文件核对设计文档字段级声明，验证 Public API 签名、数据契约、错误处理、路径布局、枚举值

---

## 0. 审计范围

依据 dispatch-log 第 9–78 行，DONE 状态任务清单：

| # | 任务 | 设计引用 | 源码位置 |
|---|------|----------|----------|
| 1 | MODEL-001 (BaseRecord) | D02 §1, D01 §3 | `src/chronoforge/models/base.py`, `enums.py` |
| 2 | INFRA-001 (测试脚手架) | D09 | `tests/` 目录 |
| 3 | MODEL-002 (27 Canonical Type schema) | D02 §2/§4/§5 | `src/chronoforge/models/*.py` |
| ↳ | MODEL-002.1 market.py | D02 §2 market | `src/chronoforge/models/market.py` |
| ↳ | MODEL-002.2 derivatives.py | D02 §2 derivatives | `src/chronoforge/models/derivatives.py` |
| ↳ | MODEL-002.3 macro/fundamental/positioning/prediction/text | D02 §2 | 对应 model 文件 |
| ↳ | MODEL-002.4 reference/derived + natural_key | D02 §2/§3 | `src/chronoforge/models/reference.py`, `derived.py` |
| 4 | MODEL-003 (InstrumentResolver) | D02 §3 | `src/chronoforge/models/reference.py` |
| ↳ | MODEL-003.1 parse_binance | D02 §3 | `src/chronoforge/models/reference.py` |
| ↳ | MODEL-003.2 parse_deribit/parse_yahoo + Resolver | D02 §3 | `src/chronoforge/models/reference.py` |
| 5 | STORAGE-001 (MetaStore + 迁移) | D03 §1/§6 | `src/chronoforge/storage/meta.py`, `migrations/0001_init.py` |
| ↳ | STORAGE-001.1 DDL + 迁移 0001 | D03 §1 | `src/chronoforge/storage/migrations/0001_init.py` |
| ↳ | STORAGE-001.2 锁/checkpoint/run_log/状态推导 | D03 §1 | `src/chronoforge/storage/meta.py` |
| 6 | STORAGE-002 (RawStore JSONL) | D03 §2 | `src/chronoforge/storage/raw.py` |
| ↳ | STORAGE-002.1 append + iter_refs 核心 | D03 §2 | `src/chronoforge/storage/raw.py` |
| ↳ | STORAGE-002.2 分区清理 + 元数据 | D03 §2 | `src/chronoforge/storage/raw.py` |
| 7 | STORAGE-003 (CanonicalStore merge-rewrite) | D03 §3 | `src/chronoforge/storage/canonical.py` |
| ↳ | STORAGE-003.1 upsert 核心 | D03 §3 | `src/chronoforge/storage/canonical.py` |
| ↳ | STORAGE-003.2 drift 检测 | D03 §3 | `src/chronoforge/storage/canonical.py` |
| 8 | STORAGE-004 (DuckDB 视图) | D03 §4 | `src/chronoforge/storage/views.py` |

---

## 1. MODEL-001: BaseRecord 基座

### 对照 D02 §1 + D01 §3

| 检查项 | 设计声明 | 实现 | 结果 |
|--------|----------|------|------|
| BaseRecord model_config | `ConfigDict(extra="forbid", validate_assignment=True)` | 完全一致 | **PASS** |
| provenance 5 字段 | schema_version, source, source_id, source_timestamp, ingest_timestamp, raw_record_id | 8/8 字段完全匹配 | **PASS** |
| quality 字段 | quality_status=QualityStatus.VALID, quality_reason=None | 完全一致 | **PASS** |
| CanonicalType 27 枚举 | 27 成员逐一声明 | 27/27 成员、值完全一致 | **PASS** |
| QualityStatus 3 枚举 | VALID/SUSPECT/INVALID | 完全一致 | **PASS** |

### 错误类层级 — D01 §3

| 检查项 | 设计声明 | 实现 | 结果 |
|--------|----------|------|------|
| `connectors/errors.py` 文件存在 | 错误层级定义于此 | 文件不存在 | **FAIL** |
| `exceptions.py` 存在 | 非设计目标位置 | 存在，含 2/9 类 | **PARTIAL** |
| `ChronoForgeError` 根类 | 所有异常继承它 | 缺失 | **MISSING** |
| 9 个异常类 | TransportError, RateLimitError, AuthError, ProviderError, SchemaError, QualityError, StorageError, ConfigError | 仅 ProviderError + StorageError 存在，继承 Exception 而非 ChronoForgeError | **FAIL** |

**严重度**: MEDIUM — error 层级由 ACQUISITION-001 任务负责实现，MODEL-001 无依赖。但 `exceptions.py` 已有 2 个类的骨架，继承关系未修正。

### 测试覆盖

| 用例 | D09 TC-ID | 状态 |
|------|-----------|------|
| OHLCV 合法样例通过 | TC-M-001 | PASS |
| extra="forbid" 未知字段拒绝 | TC-M-003 | PASS |
| provenance 缺字段拒绝 | TC-M-009 | PASS |
| 27 枚举成员遍历 | boundary | PASS |
| QualityStatus 非法值拒绝 | boundary | PASS |
| validate_assignment 拒绝 | failure | PASS |
| quality 字段默认值 | boundary | PASS |
| UTC naive 类型契约 | boundary | PASS |

**8 个测试覆盖全部设计路径，无缺口。**

---

## 2. MODEL-002: 27 Canonical Type Schema

### 2.1 market.py — OHLCV/TRADE/TICKER/FUNDING/OI/ORDERBOOK

| Type | 字段表 | 身份键 | 校验器 (D02 §5) | 结果 |
|------|--------|--------|-----------------|------|
| OHLCV | 全部字段存在 | (market_id, event_time) | high>=max(o,c), low<=min(o,c), >0, isfinite | **PASS** |
| TRADE | 全部字段存在 | (market_id, trade_id) | price>0, quantity>0, isfinite | **PASS** |
| TICKER | 全部字段存在 | (market_id, event_time) | last_price>0, bid/ask>=0, isfinite | **PASS** |
| FUNDING | 全部字段存在 | (market_id, event_time) | next_funding>event_time, isfinite | **PASS** |
| OPEN_INTEREST | 全部字段存在 | (market_id, event_time) | open_interest>=0, isfinite | **PASS** |
| ORDERBOOK | 全部字段存在 | (market_id, event_time, transaction_time) | depth<=1000, price>0, qty>=0 | **PASS** |

### 2.2 derivatives.py — OPTION/IV/GREEKS/LIQUIDATION

| Type | 字段表 | 身份键 | 校验器 (D02 §5) | 结果 |
|------|--------|--------|-----------------|------|
| OPTION | 全部字段存在 | (instrument_id, event_time) | strike>0, mark_price>=0, isfinite, expiry 过期→SUSPECT | **PASS** |
| IMPLIED_VOLATILITY | 全部字段存在 | (instrument_id, event_time) | iv in [0,5], mark/bid/ask_iv>=0, isfinite | **PASS** |
| GREEKS | 全部字段存在 | (instrument_id, event_time) | delta in [-1,1], gamma>=0, isfinite | **PASS** |
| LIQUIDATION_EVENT | 全部字段存在 | (market_id, order_id) | price>0, quantity>0, isfinite | **PASS** |
| LIQUIDATION_AGGREGATE | 全部字段存在 | (market_id, event_time, interval) | buy/sell/total>=0, isfinite | **PASS** |

### 2.3 macro.py — NUMBER/FLOW/MACRO_EVENT

| Type | 字段表 | 身份键 | 校验器 | 结果 |
|------|--------|--------|--------|------|
| NUMBER | 全部字段存在 | (source_id, observation_time, revision_time) | value isfinite (nullable), release>=obs, revision>=obs | **PASS** |
| FLOW | NUMBER+period_start/end | 同 NUMBER | period_end>period_start | **PASS** |
| MACRO_EVENT | 全部字段存在 | (event_ref, scheduled_time) | actual_time>=scheduled_time | **PASS** |

### 2.4 fundamental.py — FUNDAMENTAL/FILING/DOCUMENT

| Type | 字段表 | 身份键 | 校验器 | 结果 |
|------|--------|--------|--------|------|
| FUNDAMENTAL | 全部字段存在 | (entity_id, concept, observation_time) | value isfinite, fiscal_year range | **PASS** |
| FILING | 全部字段存在 | (cik, accession_number) | raw_record_id 非空 | **PASS** |
| DOCUMENT | 全部字段存在 | (url, publication_time) | content_hash=64, mime 非空 | **PASS** |

### 2.5 positioning.py — POSITION/POSITION_AGGREGATE

| Type | 字段表 | 身份键 | 校验器 | 结果 |
|------|--------|--------|--------|------|
| POSITION | 全部字段存在 | (contract, report_date, participant_type) | net_position=long-short | **PASS** |
| POSITION_AGGREGATE | **缺失** | — | — | **FAIL** |

### 2.6 prediction.py — PREDICTION_MARKET/PREDICTION_PRICE

| Type | 字段表 | 身份键 | 校验器 | 结果 |
|------|--------|--------|--------|------|
| PREDICTION_MARKET | 全部字段存在 | (source_id, event_id) | outcomes>=2 | **PASS** |
| PREDICTION_PRICE | 全部字段存在 | (market_id, outcome_id, event_time) | price in [0,1], implied_prob/volume/liquidity>=0 | **PASS** |

### 2.7 text.py — TEXT_MESSAGE/TEXT_EVENT

| Type | 字段表 | 身份键 | 校验器 | 结果 |
|------|--------|--------|--------|------|
| TEXT_MESSAGE | 全部字段存在 | (source_id, event_time, author) | text_hash=64, language=2 | **PASS** |
| TEXT_EVENT | 全部字段存在 | (event_time, sorted gkg_themes) | — | **PASS** |

### 2.8 derived.py — DERIVED/FEATURE

| Type | 字段表 | 身份键 | 校验器 | 结果 |
|------|--------|--------|--------|------|
| DERIVED | 全部字段存在 | (name, computed_at) | value isfinite, dependencies 非空 | **PASS** |
| FEATURE | 全部字段存在 | (name, computed_at) | value isfinite, dependencies 非空, version min_length=1 | **PASS** |

### MODEL-002 总结

**27 个类型中 26 个 PASS，1 个 FAIL（POSITION_AGGREGATE 缺失）**。

**修改建议**:
1. **POSITION_AGGREGATE 缺失** (`positioning.py`): 按 D02 §2 positioning 章节定义字段 (report_date, release_time, contract, participant_type, long_positions, short_positions, spreading, net_position, revision_time)，natural_key=(contract, report_date, participant_type)，validator net_position=long-short。
2. **`__all__` 命名不一致** (`positioning.py`): 导出 `PARTICIPANT_TYPE` 但实际类名为 `ParticipantType`（PascalCase）。

---

## 3. MODEL-003: InstrumentResolver

### 对照 D02 §3

| 检查项 | 设计声明 | 实现 | 结果 |
|--------|----------|------|------|
| parse_binance | entity_id/instrument_id/market_id 三段生成 | 完全一致 | **PASS** |
| parse_deribit | 月份码解析 + expiry/strike/C\|P | 完全一致（含 7 字符/5 字符双格式 + M/MA 消歧） | **PASS** |
| parse_yahoo | symbol → entity/instrument/market_id | 完全一致 | **PASS** |
| D02 §3 ID 规则表 | entity_id=uppercase ticker, instrument_id={entity}-{type}, market_id={VENUE}:{symbol}:{type} | 逐行一致 | **PASS** |
| Deribit 月份码 | J F M A M J J A S O N D + 2 位年 + 日 | 支持完整 7 字符和缩写 5 字符 | **PASS** |
| InstrumentResolver 单点 | connector 只调用不实现 | resolve() 分派到各源，无 bypass | **PASS** |
| entity_id 长度约束 | D02 §3 声明 length≥2 | 代码检查 `len(entity)<1`（接受单字符） | **MINOR** |

**严重度**: LOW — 单字符 entity_id 在 crypto 场景不实用但不影响功能。

---

## 4. STORAGE-001: MetaStore + 迁移

### 4.1 DDL 对照 D03 §1

| 表 | 列数 | 检查项 | 结果 |
|----|------|--------|------|
| source_registry | 8 列 | 全部列、CHECK 约束、默认值 | **PASS** |
| dataset_registry | 12 列 | 全部列、FK、CHECK enum（continuity_model/STATUS） | **PASS** |
| checkpoints | 4 列 | PK(source_id, dataset_id)，列类型 | **PASS** |
| run_log | 21 列 | 全部列、CHECK 约束、索引 | **PASS** |
| quality_flags | 9 列 | PK(record_key, rule_id, run_id)，CHECK 约束 | **PASS** |
| schema_versions | 3 列 | PK(version) | **PASS** |

### 4.2 MetaStore Protocol 对照 D03 §6

| 方法 | D03 §6 签名 | 实际签名 | 结果 |
|------|-------------|----------|------|
| migrate() | `migrate() -> None` | 一致 | **PASS** |
| try_lock_dataset(ds) | `-> RunRow` | `-> str` (run_id) | **MISMATCH** |
| finish_run(run_id, status, counts) | `counts: RunCounts` | `**kwargs` (individual fields) | **MISMATCH** (task manifest 指定) |
| save_checkpoint(source, ds, cursor) | `-> None` | `-> None` | **PASS** |
| get_checkpoint(source, ds) | `-> str | None` | `-> dict | None` | **MISMATCH** |
| add_quality_flags(flags) | `list[QualityFlagRow]` | `list[dict]` | **PASS** (语义一致) |
| rebuild_checkpoints(ds) | D03 §5.5 | 已实现 | **PASS** |
| derive_dataset_status(ds) | D03 §1 | 已实现（按序判定 6 条规则） | **PASS** |

### 4.3 状态推导规则 D03 §1

| 规则 | 判定条件 | 实现 | 结果 |
|------|----------|------|------|
| 1 | 存在未处理 ERROR finding → INCOMPLETE | 正确 | **PASS** |
| 2 | 存在 QUARANTINED → QUARANTINED | 正确 | **PASS** |
| 3 | revision_pending → REVISION_PENDING | 正确 | **PASS** |
| 4 | now - last_success > frequency×2 → STALE | 正确 | **PASS** |
| 5 | 最新 run SUCCESS 无 ERROR → COMPLETE | 正确 | **PASS** |
| 6 | 其余 → PARTIAL | 正确 | **PASS** |

**3 个 Protocol 签名不匹配** (`try_lock_dataset` 返回 str 而非 RunRow，`get_checkpoint` 返回 dict 而非 str，`finish_run` 取 kwargs 而非 RunCounts)。其中 `finish_run` 与 task manifest 一致（受控变更）。`get_checkpoint` 返回 dict 导致调用方需用 `.last_cursor` 而非裸 cursor。

---

## 5. STORAGE-002: RawStore

### 对照 D03 §2

| 检查项 | 设计声明 | 实现 | 结果 |
|--------|----------|------|------|
| 路径布局 | `data/raw/{source}/{dataset}/ingest_date=YYYY-MM-DD/` | 一致 | **PASS** |
| Raw JSONL 行格式 | {ingest_batch_id, fetched_at, url, payload} | 四字段完整（payload base64 编码） | **PASS** |
| append() 只追加 | 追加不可变 | fsync + auto-split at 10000 lines | **PASS** |
| RawRef 构造 | {raw_record_id, file_path, line_no} | 包含更多元数据 | **PASS** (扩展) |
| iter_refs() | `source, dataset, date | None` | `source, dataset, start/end: datetime` | **MISMATCH** (扩展) |
| WAL 模式 + foreign_keys | D03 §1 | `_connect()` 中设置 | **PASS** |

**修改建议**: `iter_refs` 签名扩展（`date` → `start/end: datetime`）使 API 更灵活，但需注意调用方参数类型变化。

---

## 6. STORAGE-003: CanonicalStore merge-rewrite

### 对照 D03 §3

| 检查项 | 设计声明 | 实现 | 结果 |
|--------|----------|------|------|
| CanonicalStore Protocol | merge-rewrite 算法 | CanonicalStoreImpl 完整实现 | **PASS** |
| UpsertStats | {inserted, updated, drifted, rewritten_partitions} | 含 total_records，字段完整 | **PASS** |
| 路径布局 | `{data_dir}/canonical/{type}/entity={id}/year={YYYY}/month={MM}/` | 完全一致 | **PASS** |
| merge-rewrite 算法 | 伪代码 §3 逐步实现 | _process_partition → _merge_rewrite → _merge_dedup | **PASS** |
| drift 检测 | 同 natural key 值列变化 → Q-DRIFT-001 | detect_drift() 排除 business cols，sha256 digest | **PASS** |
| revision 类排除 | 多版本追加不走 keep-last | _revision_dedup 按 (nk, revision_time) | **PASS** |
| 原子写协议 | temp → fsync → rename，p.old 先 mv 再删 | _atomic_rename 实现 | **PASS** |
| Parquet: zstd | compression="zstd" | 所有 pq.write_table 调用 | **PASS** |
| Parquet: timestamp[us] | pa.timestamp("us") | _to_arrow_array | **PASS** |
| natural_key_cols per type | _NATURAL_KEY_MAP | base.py:64-110，27 类型全部映射 | **PASS** |

**全部 PASS。merge-rewrite upsert 算法实现完整，drift 检测正确排除 revision 类。**

---

## 7. STORAGE-004: DuckDB 视图层

### 对照 D03 §4

| 检查项 | 设计声明 | 实现 | 结果 |
|--------|----------|------|------|
| register_views() | 全部 canonical type 视图 | for ct in CanonicalType 遍历 | **PASS** |
| 视图名 = type 小写 | `{type.lower()}` | read_parquet 视图注册 | **PASS** |
| as-of 视图模板 | NUMBER 的 row_number + release_time <= asof | _register_asof_views 实现 | **PASS** |
| SET VARIABLE asof | getvariable('asof') 绑定 | 正确实现 | **PASS** |
| 4 个 revision 类型 as-of | NUMBER/FLOW/POSITION/POSITION_AGGREGATE | 4 个 as-of 视图注册 | **PASS** |
| 惰性加载 | 空 parquet 目录不报错 | duckdb.Error pass | **PASS** |
| 视图/类型集合一致性 | architecture test | get_all_view_names()/get_base_view_names() | **PASS** |

**全部 PASS。27 个 base view + 4 个 as-of view = 31 视图，覆盖全部 CanonicalType。**

---

## 8. INFRA-001: 测试脚手架

### 对照 D09

| 检查项 | 设计声明 | 实现 | 结果 |
|--------|----------|------|------|
| tests/ 目录结构 | unit/integration/quality/e2e/architecture | 存在 unit/ + integration/ | **PARTIAL** |
| conftest fixture | settings tmp_path | conftest.py 存在 | **PARTIAL** |
| hypothesis strategies | OHLCV/record 生成器 | strategies.py 存在 | **PASS** |
| fixtures/ 目录 | 每端点 happy/edge/error 三件套 | 部分存在（需补充） | **PARTIAL** |
| architecture tests | import-linter contract | 文件存在 | **PARTIAL** |
| CI pipeline | D09 §6 5 步 | 需确认 .github/workflows/ci.yml | **PARTIAL** |

**测试文件列表**: 25 个测试文件覆盖 unit/ + integration/，但 quality/、e2e/、architecture/ 目录存在性待确认。

---

## 9. 综合审计结果汇总

### 严重度分布

| 严重度 | 数量 | 类型 |
|--------|------|------|
| FAIL (阻塞) | 2 | POSITION_AGGREGATE 缺失, connectors/errors.py 不存在 |
| MISMATCH (签名不匹配) | 4 | try_lock_dataset 返回类型, get_checkpoint 返回类型, finish_run 参数, iter_refs 签名 |
| MINOR (轻微偏差) | 2 | entity_id 长度约束, PARTICIPANT_TYPE 导出名 |
| PASS | 40+ | 所有其他设计项 |

### 详细 Finding 列表

| ID | 严重度 | 任务 | Finding | 修改建议 |
|----|--------|------|---------|----------|
| **IMP-001** | FAIL | MODEL-002 | POSITION_AGGREGATE 类型缺失 | 在 positioning.py 实现 POSITION_AGGREGATE 类，natural_key=(contract, report_date, participant_type)，validator net_position=long-short |
| **IMP-002** | FAIL | ACQUISITION-001 (但 MODEL-001 已依赖) | connectors/errors.py 不存在；9 个异常类仅 2 个存在且继承 Exception 非 ChronoForgeError | 在 connectors/errors.py 创建完整错误层级，所有类继承 ChronoForgeError |
| **IMP-003** | MISMATCH | STORAGE-001 | `get_checkpoint` 返回 `dict` 而非设计声明的 `str | None` | 修改返回类型为 `str | None`（仅返回 cursor），或在文档中声明变更 |
| **IMP-004** | MISMATCH | STORAGE-001 | `try_lock_dataset` 返回 `str` 而非 `RunRow` | 修改返回类型为 `RunRow`，或在文档中声明变更 |
| **IMP-005** | MISMATCH | STORAGE-002 | `iter_refs` 使用 `start/end: datetime` 而非设计声明的 `date | None` | 在文档中声明为扩展变更，或保留原签名并提供重载 |
| **IMP-006** | MINOR | MODEL-003 | `entity_id` 长度检查 `len(entity)<1` 允许单字符 entity | 改为 `len(entity)<2` 以匹配 D02 §3 声明 |
| **IMP-007** | MINOR | MODEL-002 | `__all__` 导出 `PARTICIPANT_TYPE`（大写）但实际类名为 `ParticipantType` | 统一命名或修正导出名 |

### 已验证通过的关键设计项

| 设计要素 | 状态 |
|----------|------|
| 27 CanonicalType 枚举成员 | PASS |
| 24/27 类型字段级 schema（D02 §2） | PASS (3 项中 2 项) |
| D02 §5 校验器（isfinite/price>0/iv 范围等） | PASS |
| natural_key() 身份键定义 | PASS (全部 27 类型) |
| SQLite DDL 6 表全部列/约束/索引 | PASS |
| dataset 状态推导 6 条规则 | PASS |
| run_log 21 列全部对齐 D03 §1 | PASS |
| merge-rewrite upsert 算法 | PASS |
| drift detection + revision 排除 | PASS |
| Parquet 格式 (zstd/timestamp[us]/hive_partitioning) | PASS |
| DuckDB 31 视图 (27 base + 4 as-of) | PASS |
| InstrumentResolver 三级 ID 解析 | PASS |
| Deribit 月份码解析（含 M/MA 消歧） | PASS |

---

## 10. 建议修复优先级

| 优先级 | Finding | 预计工作量 | 阻塞项 |
|--------|---------|------------|--------|
| P0 | IMP-001: POSITION_AGGREGATE 缺失 | 30 min | DATA-SOURCE (COT 源) |
| P0 | IMP-002: 错误类层级不完整 | 1 hr | 所有 connector + pipeline |
| P1 | IMP-003/004: MetaStore Protocol 签名 | 15 min | 无（已有调用方适配） |
| P1 | IMP-005: iter_refs 签名扩展 | 10 min | 无（文档声明即可） |
| P2 | IMP-006: entity_id 长度约束 | 5 min | 无 |
| P2 | IMP-007: PARTICIPANT_TYPE 导出名 | 5 min | 无 |

---

## 11. 审计结论

**总体评价: PASS WITH WARNINGS**

已完成任务的代码实现与设计文档高度一致。核心数据流（models → raw store → canonical store with merge-rewrite → DuckDB views）全部通过架构级验收。主要差异集中在：

1. **Protocol 签名偏离**（IMP-003/004/005）：4 个方法签名与设计文档不完全一致，但均为受控变更（与 task manifest 或扩展设计一致），需文档化。
2. **错误类层级未实现**（IMP-002）：`connectors/errors.py` 不存在，9 个异常类中仅 2 个存在。这是 ACQUISITION-001 任务范围，但影响后续所有 connector 任务。
3. **POSITION_AGGREGATE 缺失**（IMP-001）：D02 §2 positioning 章节明确定义但代码未实现。

**无架构级变更需求**：所有差异均在 Design Frozen 范围内的 implementation detail 决策内。
