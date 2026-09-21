# MODEL-002.3 — macro/fundamental/positioning/prediction/text 类型 schema

## 派发信息

- 任务单：D10 §2 MODEL-002 拆分（子任务 3/4）
- 设计依据：D02 §2 macro.py/fundamental.py/positioning.py/prediction.py/text.py 字段表、D02 §5 校验器、D01 §4（UTC naive datetime）
- Agent：Agent-C
- 派发时间：2026-09-11
- 状态：DONE
- 依赖：MODEL-001（BaseRecord、CanonicalType 枚举）

## file_ownership

- `src/chronoforge/models/macro.py`（新建）
- `src/chronoforge/models/fundamental.py`（新建）
- `src/chronoforge/models/positioning.py`（新建）
- `src/chronoforge/models/prediction.py`（新建）
- `src/chronoforge/models/text.py`（新建）
- `src/chronoforge/models/__init__.py`（追加导出）
- `tests/unit/test_macro_types.py`（新建）
- `tests/unit/test_fundamental_types.py`（新建）
- `tests/unit/test_positioning_types.py`（新建）
- `tests/unit/test_prediction_types.py`（新建）
- `tests/unit/test_text_types.py`（新建）

## 交付物契约摘要

### macro.py

1. **NUMBER**（FRED 存量/序列指标）：source_id(series_id)、observation_time(datetime)、release_time(datetime)、revision_time(datetime, 默认=release_time)、value(float|None)、units(str)、seasonal_adjustment(str)、vintage_date(str)
   - 校验：value=None 合法（缺失）、release_time ≥ observation_time、revision_time ≥ observation_time
   - natural_key() → ("source_id", "observation_time", "revision_time")

2. **FLOW**：NUMBER + period_start(datetime)、period_end(datetime)
   - 校验：period_end > period_start
   - natural_key() → ("source_id", "observation_time", "revision_time")（同 NUMBER）

3. **MACRO_EVENT**：event_ref(str CPI/FOMC)、scheduled_time(datetime)、actual_time(datetime|None)、actual(str|None)、forecast(str|None)、previous(str|None)
   - 校验：actual_time 存在时 ≥ scheduled_time（实际不早于计划）
   - natural_key() → ("event_ref", "scheduled_time")

### fundamental.py

4. **FILING**：entity_id、cik(str)、accession_number(str)、form_type(str 10-K/10-Q/8-K…)、filing_date(date)、accepted_datetime(datetime)、report_period_end(date)、primary_doc_url(str)、raw_record_id(str)
   - 校验：raw_record_id 非空（D02 §2 要求必须指向 raw JSONL 行）
   - natural_key() → ("cik", "accession_number")

5. **FUNDAMENTAL**：entity_id、cik、concept(str Assets/Revenues…)、taxon(str us-gaap/ifrs-full)、unit(str)、observation_time(datetime=period end)、value(float)、fiscal_year(int)、fiscal_period(str Q1/Q2…)、frame(str)
   - 校验：value isfinite、fiscal_year > 2000 && fiscal_year < 2100
   - natural_key() → ("entity_id", "concept", "observation_time")

6. **DOCUMENT**：url(str)、publication_time(datetime)、content_hash(str sha256)、mime(str)
   - 校验：content_hash 长度 64（sha256 hex）、mime 非空
   - natural_key() → ("url", "publication_time")

### positioning.py

7. **POSITION**：report_date(date)、release_time(datetime 周五15:00ET→UTC)、contract(str MARKET代码)、participant_type(ParticipantType枚举)、long_positions(float)、short_positions(float)、spreading(float)、net_position(float计算=long-short)、revision_time(datetime)
   - 校验：net_position = long_positions - short_positions（computed field 或校验）
   - ParticipantType.COMMERCIAL、NON_COMMERCIAL、NON_REPORTABLE 等（D02 §2 枚举）
   - natural_key() → ("contract", "report_date", "participant_type")

### prediction.py

8. **PREDICTION_MARKET**：source_id(market slug)、event_id(str)、question(str)、outcomes(list[str])、close_time(datetime)、status(Status枚举 OPEN/CLOSED/RESOLVED)
   - 校验：len(outcomes) ≥ 2、close_time > now（容差）
   - natural_key() → ("source_id", "event_id")

9. **PREDICTION_PRICE**：market_id(str)、outcome_id(str)、event_time(datetime)、price(float 0~1)、implied_probability(float=price)、volume(float)、liquidity(float)
   - 校验：price ∈ [0, 1]、isfinite
   - natural_key() → ("market_id", "outcome_id", "event_time")

### text.py

10. **TEXT_MESSAGE**：source_id、event_time(datetime publication)、author(str)、text_hash(str sha256)、language(str ISO 639-1)
    - 校验：text_hash 长度 64、language 长度 2
    - natural_key() → ("source_id", "event_time", "author")

11. **TEXT_EVENT**：event_time(datetime)、gkg_themes(list[str] 仅 GDELT)、entities(list[entity_id])
    - natural_key() → ("event_time", "gkg_themes")（themes 排序后哈希）

### 时间字段（D02 §4 矩阵）

- NUMBER/FLOW：observation + release + revision + ingest
- MACRO_EVENT：event(scheduled) + actual_time + ingest
- FILING：observation(period end) + release(filing_date) + ingest
- FUNDAMENTAL：observation(period end) + ingest
- POSITION：observation(report_date) + release + revision + ingest
- PREDICTION：event + ingest
- TEXT：event(publication) + ingest

## 测试要求

- NUMBER：value=None 合法、release_time < observation_time 拒绝
- FLOW：period_end ≤ period_start 拒绝
- FILING：raw_record_id="" 拒绝、form_type 未知值允许（不枚举限制）
- POSITION：net_position ≠ long-short 拒绝
- PREDICTION_PRICE：price=1.1 拒绝、price=0.5 合法
- TEXT_MESSAGE：text_hash 长度≠64 拒绝
- 边界：FRED value=None（缺失值合法）、2/29 report_date（闰年合法）

## acceptance（GWT）

- [x] Given NUMBER value=None When 实例化 Then 成功（缺失值合法）
- [x] Given FLOW period_end < period_start When 实例化 Then ValidationError
- [x] Given POSITION net_position != long - short When 实例化 Then ValidationError
- [x] Given PREDICTION_PRICE price=0.5 When 实例化 Then implied_probability=0.5（默认值）

## 验收清单（验收时填写）

DoD 16 项 + GWT 逐条勾选。

测试输出摘要：81 passed in 0.56s ｜接管性抽查：全部通过 ｜git commit：待提交

## Deferred Acceptance

无。本子任务独立闭环。
