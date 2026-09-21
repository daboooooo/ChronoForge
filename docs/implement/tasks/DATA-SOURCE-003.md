# DATA-SOURCE-003 — deribit 连接器实现

## 派发信息

- 任务单：D10 §4 DATA-SOURCE-003
- 设计依据：D04 §4.3（规格表）
- Agent：Orchestrator
- 派发时间：2026-09-12
- 状态：DONE
- 依赖：ACQUISITION-001、MODEL-002、MODEL-003

## file_ownership

- `src/chronoforge/connectors/deribit.py`（新建）
- `tests/fixtures/deribit/`（新建，fixture 目录）
- `tests/integration/test_deribit.py`（新建）

## 交付物契约摘要

### DeribitConnector 类

```python
class DeribitConnector(DataConnector):
    source_id = "deribit"

    def capabilities(self) -> CapabilityMatrix:
        return CapabilityMatrix(
            canonical_types=frozenset({
                CanonicalType.OHLCV, CanonicalType.OPTION,
                CanonicalType.IMPLIED_VOLATILITY,
            }),
            intervals=frozenset({"1m","5m","15m","30m","1h","4h","1d"}),
            supports_revision=False,
            supports_websocket=False,
            max_history_days=365,  # Deribit 历史限制
        )
```

### discover 实现（D04 §4.3，必须实现）

```python
def discover(self) -> list[InstrumentRef]:
    """
    全量 instrument 发现（D04 §4.3 必须实现）。
    返回 INSTRUMENT canonical 记录列表。
    """
    response = self.client.post("/api/v2", json={
        "method": "public/get_instruments",
        "params": {"currency": "BTC", "expired": False},
    })

    instruments = []
    for item in response.json()["result"]:
        instruments.append(InstrumentRef(
            entity_id=self.resolver.parse_deribit(item["instrument_name"]).entity_id,
            instrument_id=self.resolver.parse_deribit(item["instrument_name"]).instrument_id,
            market_id=self.resolver.parse_deribit(item["instrument_name"]).market_id,
        ))
    return instruments
```

- **必须实现**：全量 instrument → INSTRUMENT canonical 记录 + 本地 instrument 表缓存
- 调用 `models/reference.py` 的 `parse_deribit` 解析 `instrument_name`
- discover 返回的 InstrumentRef 供 pipeline 注册 dataset

### fetch 实现（D04 §4.3）

**get_book_summary_by_currency**（OPTION 快照）：

```python
def _fetch_option_summary(self, currency: str) -> Iterator[RawBatch]:
    """
    OPTION 快照（单次请求，返回该货币全部活跃期权）。
    """
    response = self.client.post("/api/v2", json={
        "method": "public/get_book_summary_by_currency",
        "params": {"currency": currency, "kind": "option"},
    })
    return [RawBatch(endpoint="/public/get_book_summary_by_currency", payload=..., raw_meta=...)]
```

**get_tradingview_chart_data**（OHLCV/IV 历史）：

```python
def _fetch_chart_data(self, instrument_name: str, start: int, end: int, resolution: str) -> Iterator[RawBatch]:
    """
    OHLCV/IV 历史（D04 §4.3）。
    resolution: 1, 5, 15, 30, 60, 240, 1440（分钟）
    """
    params = {
        "instrument_name": instrument_name,
        "start": start,
        "end": end,
        "resolution": resolution,
    }
    response = self.client.post("/api/v2", json={"method": "public/get_tradingview_chart_data", "params": params})
    # 构造 RawBatch
    ...
```

### normalize 实现

```python
def normalize(self, raw: RawBatch) -> list[BaseRecord]:
    match raw.endpoint:
        case "/public/get_instruments":
            return self._normalize_instruments(raw.payload)
        case "/public/get_book_summary_by_currency":
            return self._normalize_options(raw.payload)
        case "/public/get_tradingview_chart_data":
            return self._normalize_chart(raw.payload)
```

**get_instruments → INSTRUMENT**：

```python
def _normalize_instruments(self, payload: dict) -> list[INSTRUMENT]:
    instruments = []
    for item in payload.get("result", []):
        instrument = INSTRUMENT(
            instrument_id=self.resolver.parse_deribit(item["instrument_name"]).instrument_id,
            entity_id=self.resolver.parse_deribit(item["instrument_name"]).entity_id,
            instrument_type=InstrumentType.OPTION if item["kind"] == "option" else InstrumentType.PERP,
            expiry=date.fromisoformat(item["expiration_timestamp"]) if item.get("expiration_timestamp") else None,
            strike=float(item["strike"]) if item.get("strike") else None,
            option_type=OptionType.CALL if item.get("currency") else OptionType.PUT,
            settlement_asset=item.get("settlement_currency", "USD"),
        )
        instruments.append(instrument)
    return instruments
```

**get_book_summary → OPTION**：

- `mark_price`, `bid`, `ask` 来自 raw 响应
- IV 推导属 derived 层，connector 只存 OPTION 价格快照与源侧 IV 字段

### 解析（D02 §3 + D04 §4.3）

**parse_deribit 月份码解析**：

```python
# Deribit 月份码：J F M A M J J A S O N D + 2 位年 + 日
# 示例：BTC-26SEP26-100000-C
#   underlying=BTC, expiry=2026-09-26, strike=100000, option_type=CALL
MONTH_CODES = {"F":"01","G":"02","H":"03","J":"04","K":"05","M":"06",
               "N":"07","Q":"08","U":"09","V":"10","X":"11","Z":"12"}

def parse_deribit(instrument_name: str) -> DeribitParsed:
    """解析 Deribit instrument_name（D02 §3 + D04 §4.3）"""
    # 实现：正则匹配 BTC-26SEP26-100000-C
    ...
```

- **时间语义**（审计 F-09）：`timestamp` ms → us ×1000；`expiration`（"26SEP26"）解析为 UTC 日期；源即 UTC 无 DST 歧义
- **continuity_model**：ALWAYS_OPEN（7×24）；到期后 instrument 标 SUSPECT（Q-RANGE-003）

## 测试要求（D09 TC-C 组）

- **TC-C-003**：fixture→canonical 逐字段比对
- **discover 全量**→ INSTRUMENT 记录数=源返回数且三级 ID 正确
- **月份码联动**：TC-M-004/005（parse_deribit 全字段）
- **过期合约**：解析成功 + 调用方获知过期（不抛异常）
- **非法月份码**：`"BTC-32FOO26-100000-C"` → ProviderError

## acceptance（GWT）

- [x] Given discover Then INSTRUMENT 记录数=源返回数且三级 ID 正确
- [x] Given "BTC-26SEP26-100000-C" When parse_deribit Then (BTC, BTC-2026-09-26-100000-C, DERIBIT:...:OPTION) 全字段正确
- [x] Given 过期合约 When parse_deribit Then 解析成功 + 不抛异常

## 验收清单（验收时填写）

DoD 16 项 + GWT 逐条勾选。

测试输出摘要：35 passed in 0.41s（pytest tests/integration/test_deribit.py，全库 870 passed）｜接管性抽查：仅凭任务单 + D04 §4.3 设计文档可理解实现：DeribitConnector 严格遵循 DataConnector 协议，discover/fetch/normalize/checkpoint_from 方法签名与 Binance 连接器一致，parse_deribit 支持 PERP/OPTION 双路径｜git commit：待 Orchestrator 统一提交

## 执行记录

| 时间 | 事件 |
|---|---|
| 2026-09-17 | 派发执行（Orchestrator） |
| 2026-09-17 | DeribitConnector 完整实现（deribit.py）+ 35 集成测试（TC-C-003 + TC-M-004/005 + GWT + 错误路径 + 分页 + checkpoint/validate） |
| 2026-09-17 | 修复：parse_deribit 增加 PERP 永续合约支持（`BTC-PERP` → underlying=BTC, PERP type, expiry=None）；normalize_instruments 优先使用 instrument_name 解析结果（D02 §3 为唯一事实源），expiration_timestamp 仅作兜底 |
| 2026-09-17 | 验收通过：35 passed，ruff 修复导入排序，无 mypy 错误 |

## Deferred Acceptance

无。本子任务独立闭环。
