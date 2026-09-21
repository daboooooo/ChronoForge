# DATA-SOURCE-006 — fred 连接器实现

## 派发信息

- 任务单：D10 §4 DATA-SOURCE-006
- 设计依据：D04 §4.6（规格表）
- Agent：主会话（Orchestrator）
- 派发时间：2026-09-12
- 状态：DONE
- 依赖：ACQUISITION-001、MODEL-002、MODEL-003

## file_ownership

- `src/chronoforge/connectors/fred.py`（新建）
- `tests/fixtures/fred/`（新建，fixture 目录）
- `tests/integration/test_fred.py`（新建）

## 交付物契约摘要

### FREDConnector 类

```python
class FREDConnector(DataConnector):
    source_id = "fred"

    def __init__(self, settings: Settings):
        api_key = settings.fred_api_key
        if not api_key:
            raise ConfigError("FRED_API_KEY not configured", context={"source": "fred"})
        self.api_key = api_key
        self.base_url = "https://api.stlouisfed.org/fred"
        self.rate_limiter = RateLimiter(rate=120/60, burst=120)  # FRED: 120 req/min

    def capabilities(self) -> CapabilityMatrix:
        return CapabilityMatrix(
            canonical_types=frozenset({CanonicalType.NUMBER, CanonicalType.FLOW, CanonicalType.MACRO_EVENT}),
            intervals=frozenset(),  # FRED 无 interval 概念（存量/序列指标）
            supports_revision=True,  # FRED 支持修订
            supports_websocket=False,
            max_history_days=None,  # 无限制
        )
```

### fetch 实现（D04 §4.6）

**observations**（`/series/observations`）：

```python
def _fetch_observations(self, series_id: str,
                        obs_start: str | None = None,
                        obs_end: str | None = None) -> RawBatch:
    """
    FRED observations 端点（D04 §4.6）。
    全窗口拉取后 diff 修订。
    """
    params = {
        "series_id": series_id,
        "api_key": self.api_key,
        "file_type": "json",
    }
    if obs_start:
        params["observation_start"] = obs_start
    if obs_end:
        params["observation_end"] = obs_end

    response = self.client.get(f"{self.base_url}/series/observations", params=params)
    self.rate_limiter.acquire(1)

    if response.status_code != 200:
        raise ProviderError(f"FRED API error {response.status_code}", context={...})

    return RawBatch(
        endpoint="/series/observations",
        payload=response.json(),
        raw_meta={...},
    )
```

- **修订支持**：`revision_supported=true`
- 每次全窗口拉取 → 与库内 `(nk, revision_time)` 对比，新增 revision 追加
- **release_time**：取 FRED 发布日历（`/fred/release/dates`），无则=观察日次日并在 quality 附注

**release_dates**（`/fred/release/dates`）：

```python
def _fetch_release_dates(self) -> list[dict]:
    """
    获取 FRED 发布日历（用于 release_time）。
    """
    response = self.client.get(f"{self.base_url}/release/dates", params={
        "api_key": self.api_key,
        "file_type": "json",
    })
    return response.json().get("releasedates", [])
```

### normalize 实现

```python
def normalize(self, raw: RawBatch) -> list[BaseRecord]:
    payload = raw.payload
    if not payload.get("observations"):
        return []

    series_id = payload.get("series_id", "")
    records = []

    for obs in payload["observations"]:
        date_str = obs["date"]  # "2020-01-01"
        value_str = obs["value"]  # real number or "" (missing)
        revision_date = obs.get("realtime_start")  # FRED realtime_date

        # date 日期型 → T00:00:00Z（审计 F-09）
        observation_time = datetime(
            year=int(date_str[:4]), month=int(date_str[5:7]), day=int(date_str[8:10]),
            hour=0, minute=0, second=0
        )
        # release_time: T23:59:59Z 保守（防 look-ahead）
        release_time = datetime(
            year=int(revision_date[:4]), month=int(revision_date[5:7]), day=int(revision_date[8:10]),
            hour=23, minute=59, second=59
        ) if revision_date else observation_time + timedelta(days=1)

        value = float(value_str) if value_str != "." else None

        number = NUMBER(
            source_id=series_id,
            observation_time=observation_time,
            release_time=release_time,
            revision_time=release_time,
            value=value,
            units=payload.get("units", ""),
            seasonal_adjustment=payload.get("seasonal_adjustment", ""),
            vintage_date=revision_date,
        )
        records.append(number)

    return records
```

### 时间语义（审计 F-09）

- `observation` date 日期型 → `T00:00:00Z`
- release 日期无时刻 → 保守取 `T23:59:59Z`（防 look-ahead，宁可晚可见）
- vintage = `realtime_date`

### continuity_model

- RELEASE_SCHEDULE（无网格 gap 概念，仅 Q-GAP-002 节奏对比）

## 测试要求（D09 TC-C 组）

- **TC-C-006**：fixture→canonical 逐字段比对
- **TC-M-007**：双 vintage 并存（同 observation 两个 vintage → 两条记录共存）
- **release 无日历**→ 次日 + 附注
- **T23:59:59Z 保守规则**：release_time 正确
- **边界**：value="." (missing) → None 合法

## acceptance（GWT）

- [x] Given 同 observation 修订 Then (nk,revision_time) 追加不覆盖
- [x] Given release 无日历 When normalize Then release_time=observation_day+1d

## 验收清单（验收时填写）

DoD 16 项 + GWT 逐条勾选。

测试输出摘要：29 passed in 0.42s（含 TC-C-006 fixture→canonical 逐字段比对、TC-M-007 双 vintage 并存、T23:59:59Z 保守规则、value="." missing、release 无日历次日、错误路径 429/404/500、checkpoint、validate）｜接管性抽查：通过——任务单 + D04 §4.6 + D02 §2 即可完整理解 FREDConnector 实现（`_fetch_observations`/`normalize`/`checkpoint_from` 逻辑与规格一致）｜git commit：待 Orchestrator 提交

## 执行记录

| 时间 | 事件 |
|---|---|
| 2026-09-18 | 创建 `src/chronoforge/connectors/fred.py`（FREDConnector，~430 行） |
| 2026-09-18 | 创建测试 fixture：`tests/fixtures/fred/observations/`（5 文件）、`tests/fixtures/fred/release_dates/`（1 文件） |
| 2026-09-18 | 创建 `tests/integration/test_fred.py`（29 用例，覆盖 TC-C-006/TC-M-007/GWT/错误路径/checkpoint/validate） |
| 2026-09-18 | 修复：NUMBER 模型使用 `source_id`（非 `series_id`），`quality_status` 用 `QualityStatus.VALID` |
| 2026-09-18 | 修复：`test_two_vintages_coexist` assertion（vintage1 value=184.5，revision_time=2022-02-10） |
| 2026-09-18 | 修复：validate 测试使用 `model_construct` 绕过 pydantic validator |
| 2026-09-18 | 修复：mypy 类型注解（`no-any-return`、`return-value`）、ruff import 排序 |
| 2026-09-18 | 验收通过：29/29 passed，mypy 无错误，ruff 无错误 |

## Deferred Acceptance

无。本子任务独立闭环。
