# D06 — 质量模块详细设计

依据：架构 05 §4、08 §1。规则 ID 全局唯一，写入 quality_flags.rule_id；测试用例与规则一一对应（D09 TC-Q 组）。

## 1. 结构（quality/rules.py）

```python
class QualityRule(Protocol):
    rule_id: str            # Q-xx-nnn
    applies_to: frozenset[CanonicalType] | Literal["ALL"]
    severity: Literal["ERROR", "WARNING", "INFO"]
    def check(self, records: Sequence[BaseRecord]) -> list[QualityFinding]: ...

@dataclass
class QualityFinding:
    record_key: str; rule_id: str; severity: str; detail: str

def run(records, canonical_type, *, context: GapContext | None) -> QualityReport
# QualityReport: findings + summary{error,warning,info}; 序列化入 run_log 与 quality_flags
```

## 2. 规则清单

| ID | 对象 | 逻辑 | severity |
|---|---|---|---|
| Q-SCHEMA-001 | ALL | Pydantic 校验失败捕获转 finding（记录级，不中断） | ERROR |
| Q-TS-001 | ALL | event/observation_time > ingest_time+5min（时钟异常） | WARNING |
| Q-TS-002 | OHLCV/AGG | 相邻 event_time 间隔 ≠ interval（乱序） | WARNING |
| Q-TS-003 | OHLCV | 未收盘 K 线（event_time+interval > now）：normalize 丢弃并计数——防半根 K 线入库后被源重写触发漂移（审计 F-02） | INFO |
| Q-DUP-001 | ALL | natural key 在批次内重复（应被 upsert 合并，残留即异常） | ERROR |
| Q-SEQ-001 | TRADE | aggTrades 归集序列对账（审计 F-05）：窗口内相邻记录 prev.last_id+1 == curr.first_id；跳号=漏单或归集边界 → 标记不阻断（silent loss 防线） | WARNING |
| Q-DRIFT-001 | 非 revision 全类型 | 同 natural key 值列漂移（merge-rewrite 前检测，D03 §3）：旧/新值 digest 随 finding；禁止静默覆盖（审计 F-04） | WARNING |
| Q-GAP-001 | OHLCV | 网格连续性按 dataset 的 continuity_model 判定（审计 F-06）：ALWAYS_OPEN→全网格；TRADING_CALENDAR→剔除非交易日（P0 内置简化日历：周末+固定节假日，无第三方依赖；正式日历库 P1 评估）→ 非交易日缺失标 **EXPECTED_GAP**（INFO）；交易日缺失=Unexpected Gap（WARNING）→ 缺失区间列表 | INFO / WARNING（按 gap 类型） |
| Q-GAP-002 | LIQ_AGG/NUMBER | 与 frequency 声明对比缺行 | INFO |
| Q-RANGE-001 | OHLCV | high≥max(o,c) 或 low≤min(o,c) 违反；volume<0；NaN/Inf（isfinite 显式拒绝，审计 F-15） | ERROR |
| Q-RANGE-002 | IV | iv∉[0,5]；probability∉[0,1] | ERROR |
| Q-RANGE-003 | OPTION | strike≤0；expiry 过期（status 仍 VALID，此规则只标注） | INFO |
| Q-NULL-001 | ALL | 声明必填字段为 None（FRED value 允许 None→记 INFO 缺失观测） | WARNING |
| Q-OHLC-001 | OHLCV | closeTime-openTime ≈ interval（Binance 语义） | INFO |
| Q-CROSS-001 | TICKER | bid>ask、last 出界 [low24, high24] | WARNING |
| Q-REV-001 | NUMBER/POSITION | 同 nk 多 revision_time 并存 → 确认追加而非覆盖（计数） | INFO |
| Q-PROV-001 | ALL | provenance 五字段空缺（不应发生，防线） | ERROR |

**Gap 算法**（Q-GAP-001）：对单 market_id+interval，`expected = date_range(min_t, max_t, interval)`；`missing = expected - set(actual)`；输出区间合并列表 `[(s,e),...]`；结果缓存于 finding.detail JSON。

## 3. 处置策略（policy，Settings 可配，架构 05 §4"可配置"）

```yaml
quality:
  block_on: [Q-SCHEMA-001, Q-PROV-001]   # ERROR 级默认阻断 run（FAILED）
  flag_only: [...]                        # 其余仅写 quality_flags
  normalize_error_threshold: 0.10         # NormalizeStage 失败率上限（D05 §1）
```

- 阻断 = QualityStage 抛 `QualityError` → run FAILED；已落盘 canonical **不回滚**（provenance 可追溯，重放修复）
- SUSPECT/INVALID 写入记录的 quality_status/quality_reason（Q-RANGE-003 → SUSPECT；Q-SCHEMA-001 → INVALID 且该记录不写 canonical，仅 flag）
- **Quarantine 语义（审计 F-08）**：INVALID 记录不写 canonical；事后定位 = raw 层原文（append-only 保留）+ quality_flags 行的 raw_ref（jsonl 路径+行号）与 payload_digest（sha256）；重处理路径：raw_ref → 原始 payload → 修复 normalize → replay（howto §21）

## 4. 数据质量测试（tests/quality/，对真实入库数据，架构 07 §1）

`pytest --quality`（独立 marker）：对每个 dataset 跑 Q-GAP-001/002、Q-DUP-001、Q-REV-001、Q-SEQ-001、Q-DRIFT-001 的 SQL 版本（DuckDB 直接查询），产出报告 `meta/quality-report-{date}.json`；CLI `chronoforge quality report` 同入口。

## 5. 测试依据（D09 TC-Q 组）

每规则≥2 用例（触发/不触发）；gap 算法黄金样例：1m 网格缺 3 行连续+2 行分散 → 区间合并 [(10:05,10:08),(10:20,10:22)]；block_on 阻断 run 且不回滚已写分区。
