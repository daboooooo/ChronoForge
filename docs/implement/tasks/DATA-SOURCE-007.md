# DATA-SOURCE-007 — sec_edgar 连接器实现

## 派发信息

- 任务单：D10 §4 DATA-SOURCE-007
- 设计依据：D04 §4.7（规格表）
- Agent：待定
- 派发时间：2026-09-12
- 状态：DONE
- 依赖：ACQUISITION-001、MODEL-002、MODEL-003

## file_ownership

- `src/chronoforge/connectors/sec_edgar.py`（新建）
- `tests/fixtures/sec_edgar/`（新建，fixture 目录）
- `tests/integration/test_sec_edgar.py`（新建）

## 交付物契约摘要

### SECEdgarConnector 类

```python
class SECEdgarConnector(DataConnector):
    source_id = "sec_edgar"

    def __init__(self, settings: Settings):
        contact_email = settings.sec_contact_email
        if not contact_email:
            raise ConfigError("CHRONOFORGE_SEC_CONTACT not configured", context={"source": "sec"})
        self.contact_email = contact_email
        self.user_agent = f"ChronoForge/{settings.__version__} research ({contact_email})"
        self.rate_limiter = RateLimiter(rate=10, burst=10)  # ≤10 req/s
        # 串行 + 令牌桶双保险

    def capabilities(self) -> CapabilityMatrix:
        return CapabilityMatrix(
            canonical_types=frozenset({CanonicalType.FILING, CanonicalType.FUNDAMENTAL, CanonicalType.DOCUMENT}),
            intervals=frozenset(),  # SEC 无 interval 概念
            supports_revision=False,
            supports_websocket=False,
            max_history_days=None,  # 全量可用
        )
```

### client 构造（D04 §4.7）

```python
def _make_client(self) -> httpx.Client:
    """构造 SEC client（D04 §4.7 User-Agent 必填）"""
    return httpx.Client(
        base_url="https://data.sec.gov/submissions",
        headers={
            "User-Agent": self.user_agent,  # 必填！官方 fair-access 要求
        },
        timeout=self.settings.http_timeout_s,
    )
```

- **User-Agent 必填**：`ChronoForge/{version} research ({email})`（D04 §4.7）
- **无 UA 配置** → ConfigError（fail fast）
- **限流**：≤10 req/s；串行 + 令牌桶双保险

### fetch 实现（D04 §4.7）

**submissions**（`/CIK{cik}.json`）：

```python
def _fetch_submissions(self, cik: str) -> Iterator[RawBatch]:
    """
    FILING 索引（D04 §4.7）。
    cursor: filing-recent.json 增量。
    """
    # 全量拉取（SEC 无分页，一次性返回）
    response = self.client.get(f"/CIK{cik}.json")
    self.rate_limiter.acquire(1)

    yield RawBatch(
        endpoint=f"/CIK{cik}.json",
        payload=response.json(),
        raw_meta={...},
    )
```

- **cursor**：`filing-recent.json` 增量；按 accession 缓存
- **ETag/Last-Modified**：response 有则条件请求

**companyfacts**（`/CIK{cik}/companyfacts.json`）：

```python
def _fetch_companyfacts(self, cik: str) -> RawBatch:
    """
    FUNDAMENTAL 数据（D04 §4.7）。
    """
    response = self.client.get(f"/CIK{cik}/companyfacts.json")
    self.rate_limiter.acquire(1)

    # 条件请求：If-None-Match / If-Modified-Since
    # ...

    return RawBatch(...)
```

### normalize 实现

```python
def normalize(self, raw: RawBatch) -> list[BaseRecord]:
    match raw.endpoint:
        case endpoint if endpoint.endswith(".json") and "companyfacts" in endpoint:
            return self._normalize_fundamentals(raw.payload)
        case endpoint if endpoint.endswith(".json"):
            return self._normalize_filings(raw.payload)
```

**submissions → FILING**：

```python
def _normalize_filings(self, payload: dict) -> list[FILING]:
    filings = payload.get("filings", {}).get("recent", [])
    records = []

    for filing in filings:
        accession_number = filing["accessionNumber"]
        form_type = filing["form"]
        filing_date = filing["filingDate"]
        report_period = filing.get("reportDate")
        report_period_date = parse_date(report_period) if report_period else None

        # acceptanceDateTime 带 EDT/EST 后缀 → 先转 UTC 再去 tzinfo（审计 F-09）
        accepted_dt = filing.get("acceptedDate")
        ingest_time = datetime.now(UTC).replace(tzinfo=None)

        filing_rec = FILING(
            entity_id=self.resolver.resolve_cik(cik).entity_id,
            cik=cik,
            accession_number=accession_number,
            form_type=form_type,
            filing_date=parse_date(filing_date),
            accepted_datetime=parse_acceptance_datetime(accepted_dt),
            report_period_end=report_period_date,
            primary_doc_url=self._extract_doc_url(accession_number),
        )
        records.append(filing_rec)

    return records
```

**companyfacts → FUNDAMENTAL**：

```python
def _normalize_fundamentals(self, payload: dict) -> list[FUNDAMENTAL]:
    facts = payload.get("facts", {})
    records = []

    for concept, concept_data in facts.items():
        for item in concept_data.get("units", []):
            # 解析 us-gaap/ifrs-full taxon
            taxon = item.get("taxonomy", {}).get("entry", {}).get("url", "")
            observation_time = datetime(
                year=int(item["i"]), month=int(item["f"]),
                hour=23, minute=59, second=59
            )

            fundamental = FUNDAMENTAL(
                entity_id=self.resolver.resolve_cik(cik).entity_id,
                cik=cik,
                concept=concept,
                taxon=taxon,
                unit=item.get("u", "USD"),
                observation_time=observation_time,
                value=float(item["$"]),
                fiscal_year=int(item["i"]) if item.get("i") else None,
                fiscal_period=int(item["f"]) if item.get("f") else None,
                frame=item.get("frame"),
            )
            records.append(fundamental)

    return records
```

### 时间语义（审计 F-09）

- `acceptanceDateTime` 带 EDT/EST 后缀 → 先转 UTC 再去 tzinfo
- `filingDate` 日期型 → 保守 `T23:59:59Z`
- **continuity_model**：EVENT_BASED（无 gap 概念）

## 测试要求（D09 TC-C 组）

- **TC-C-007**：fixture→canonical 逐字段比对
- **TC-C-014**：UA 注入断言（User-Agent 正确格式）
- **ETag 304 处理**：条件请求响应正确
- **acceptanceDatetime EDT→UTC**：时区转换正确

## acceptance（GWT）

- [x] Given 无 UA 配置 When 构造 client Then ConfigError（fail fast）
- [x] Given submissions fixture When normalize Then FILING 记录数与 fixture 一致

## 验收清单（验收时填写）

- [x] DoD 16 项逐项通过（mypy strict 无错误、ruff 无错误、pytest 34 passed）
- [x] GWT 逐条核对通过（见 TC-C-007、TC-C-014、审计 F-09）
- [x] TC-C-007 fixture→canonical 逐字段比对（FILING + FUNDAMENTAL）
- [x] TC-C-014 UA 注入断言（User-Agent 正确格式）
- [x] acceptanceDatetime EDT/EST → UTC 时区转换正确
- [x] 空结果边界（0 filings、0 facts → 0 行）
- [x] 错误路径（429→RateLimitError、404/500→ProviderError）
- [x] checkpoint 提取（submissions→accessionNumber、companyfacts→year-month）
- [x] validate 基础校验（future filing→WARNING、finite value）

测试输出摘要：34 passed in 0.44s｜接管性抽查：通过（connector 实现可凭任务单+设计文档理解）｜git commit：（待 Orchestrator 统一提交）

## 执行记录

| 时间 | 事件 |
|---|---|
| 2026-09-18 | 实现 SECEdgarConnector 完整代码（connectors/sec_edgar.py） |
| 2026-09-18 | 创建 4 个 fixture 文件（submissions/happy+empty, companyfacts/happy+empty） |
| 2026-09-18 | 编写 34 个集成测试用例（test_sec_edgar.py） |
| 2026-09-18 | 修复：SEC submissions API 返回并行数组格式，改为按索引遍历 |
| 2026-09-18 | 修复：checkpoint_from 处理并行数组格式 |
| 2026-09-18 | 修复：companyfacts normalize 兼容 `$` 和 `value` 键 |
| 2026-09-18 | 验收通过：34 passed in 0.44s，mypy strict 无错误，ruff 无错误 |

## Deferred Acceptance

无。本子任务独立闭环。
