"""hypothesis 策略库（INFRA-001）：dict 级 Canonical 记录生成器。

背景：本模块与 MODEL-001 并行开发，Canonical Type schema（MODEL-002）尚未落地，
故全部策略生成 dict（字段形态依据 docs/design/02-data-models.md §1–§2），
由调用方在 MODEL-002 可用后负责 dict → pydantic 实例化与校验。

公开策略（docs/design/10-task-manifest.md §9 INFRA-001）：
- ``ohlcv()``：合法 canonical OHLCV dict（OHLC 不变量成立，数值全 >0 且 finite）；
- ``ohlcv_invalid()``：同形态但显式注入单类违规，附 ``invalid_reason`` 键；
- ``trade_seq()``：aggTrades 归集序列（相邻条 prev.l+1==curr.a，可注入跳号）；
- ``records()``：通用 BaseRecord 形 dict（仅基座字段）。

时间约定：所有 datetime 字段为 ISO 字符串（UTC naive，无 tz 后缀），
且 event_time ≤ ingest_timestamp（保守满足 D02 §5 的时间容差校验）。
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from hypothesis import strategies as st

# ---- 契约常量（D02 §1/§2、D04 §4）----

#: schema_version 固定值（D02 §1）
SCHEMA_VERSION = "1.0"
#: quality_status 枚举（D02 §1 QualityStatus）
QUALITY_STATUSES = ("VALID", "SUSPECT", "INVALID")
#: OHLCV interval 枚举（D02 §2 market.py）
INTERVALS = ("1m", "5m", "1h", "1d")
#: 非法 interval 取值池（ohlcv_invalid 用，与 INTERVALS 不相交）
ILLEGAL_INTERVALS = ("1s", "2m", "15m", "3h", "1w")
#: D04 §4 各源的 source_id（source_registry.source_id 形态）
SOURCES = (
    "binance_spot",
    "binance_futures",
    "deribit",
    "ccxt",
    "yahoo",
    "fred",
    "sec_edgar",
)
#: market_id = {VENUE}:{SYMBOL}:{MKT}（D02 §3）
VENUES = ("BINANCE", "DERIBIT", "YAHOO")
MARKET_TYPES = ("SPOT", "USDT-FUT", "OPTION")
#: ohlcv_invalid() 的违规类别（invalid_reason 取值，四选一）
INVALID_REASONS = ("high_lt_low", "negative_value", "nan_value", "invalid_interval")
#: BaseRecord 基座字段（D02 §1）
BASE_RECORD_KEYS = (
    "schema_version",
    "source",
    "source_id",
    "source_timestamp",
    "ingest_timestamp",
    "raw_record_id",
    "quality_status",
    "quality_reason",
)
#: OHLCV 专属字段（D02 §2）
OHLCV_KEYS = ("market_id", "event_time", "interval", "open", "high", "low", "close", "volume")
#: 价格/数量字段（OHLCV 数值域）
PRICE_FIELDS = ("open", "high", "low", "close", "volume")

# ---- 内部构件（How 层实现细节）----

# 生成时间窗：加密数据纪元，避免极端年份在下游（parquet us 精度等）溢出
_DATETIMES = st.datetimes(
    min_value=datetime(2015, 1, 1), max_value=datetime(2035, 12, 31, 23, 59, 59)
)
# event_time → ingest_timestamp 的非负延迟（D02 §5：event_time ≤ ingest_timestamp + 5min 容差）
_LAGS = st.timedeltas(min_value=timedelta(0), max_value=timedelta(days=30))
# 价格/数量：正且 finite（D02 §5 isfinite 校验）
_POSITIVE_FLOATS = st.floats(min_value=1e-8, max_value=1e8, allow_nan=False, allow_infinity=False)
# 源侧唯一标识（symbol / series_id / cik / instrument_name，D02 §1），兼作 market_id 的 SYMBOL 池
_SOURCE_IDS = st.sampled_from(
    (
        "BTCUSDT",
        "ETHUSDT",
        "SOLUSDT",
        "AAPL",
        "MSFT",
        "CPIAUCSL",
        "UNRATE",
        "DGS10",
        "BTC-26SEP26-100000-C",
    )
)
# raw 分片文件名形态 {HHmmss}-{seq}.jsonl（D03 §2）
_JSONL_SHARDS = st.tuples(
    st.integers(0, 23), st.integers(0, 59), st.integers(0, 59), st.integers(0, 999)
).map(lambda t: f"{t[0]:02d}{t[1]:02d}{t[2]:02d}-{t[3]:03d}.jsonl")
_DATASET_NAMES = st.sampled_from(
    ("klines_1m", "klines_1h", "aggtrades", "ticker24hr", "chart", "observations")
)
_LINE_NOS = st.integers(min_value=1, max_value=10_000_000)


@st.composite
def _base_fields(draw: st.DrawFn, event_dt: datetime) -> dict[str, Any]:
    """绘制 BaseRecord 基座字段（D02 §1）；ingest_timestamp = event_dt + 非负延迟。"""
    source = draw(st.sampled_from(SOURCES))
    ingest_dt = event_dt + draw(_LAGS)
    quality_status = draw(st.sampled_from(QUALITY_STATUSES))
    return {
        "schema_version": SCHEMA_VERSION,
        "source": source,
        "source_id": draw(_SOURCE_IDS),
        "source_timestamp": draw(st.none() | st.just(event_dt.isoformat())),
        "ingest_timestamp": ingest_dt.isoformat(),
        # raw_record_id = "{source}:{dataset}:{jsonl_file}:{line_no}"（D02 §1）
        "raw_record_id": f"{source}:{draw(_DATASET_NAMES)}:{draw(_JSONL_SHARDS)}:{draw(_LINE_NOS)}",
        "quality_status": quality_status,
        "quality_reason": (
            None if quality_status == "VALID" else f"autogen_{quality_status.lower()}"
        ),
    }


@st.composite
def ohlcv(draw: st.DrawFn) -> dict[str, Any]:
    """合法 canonical OHLCV dict（D02 §2）。

    不变量：low ≤ min(open, close) ∧ max(open, close) ≤ high；
    数值字段全 >0 且 finite；interval ∈ {1m,5m,1h,1d}；market_id 为
    ``VENUE:SYMBOL:MKT`` 三段式；event_time ≤ ingest_timestamp。
    """
    event_dt = draw(_DATETIMES)
    low = draw(_POSITIVE_FLOATS)
    high = low + draw(_POSITIVE_FLOATS)  # high ≥ low（零区间 bar 亦满足不变量）
    body = st.floats(min_value=low, max_value=high, allow_nan=False, allow_infinity=False)
    market_id = ":".join(
        (draw(st.sampled_from(VENUES)), draw(_SOURCE_IDS), draw(st.sampled_from(MARKET_TYPES)))
    )
    record = draw(_base_fields(event_dt))
    record.update(
        {
            "market_id": market_id,
            "event_time": event_dt.isoformat(),
            "interval": draw(st.sampled_from(INTERVALS)),
            "open": draw(body),
            "high": high,
            "low": low,
            "close": draw(body),
            "volume": draw(_POSITIVE_FLOATS),
        }
    )
    return record


@st.composite
def ohlcv_invalid(draw: st.DrawFn, reason: str | None = None) -> dict[str, Any]:
    """同 ohlcv() 形态但显式注入单类违规，附 ``invalid_reason`` 键说明。

    reason 缺省时随机选取 ∈ INVALID_REASONS；显式指定时必须合法
    （"每类违规至少一例" 的自测依赖该参数强制类别）。
    注意：返回 dict 带 invalid_reason 附加键（违反 extra=forbid 的自标记），
    用于 MODEL-002 校验前需先 pop 该键。
    """
    if reason is None:
        reason = draw(st.sampled_from(INVALID_REASONS))
    elif reason not in INVALID_REASONS:
        raise ValueError(f"未知违规类别 {reason!r}，合法值：{INVALID_REASONS}")
    record = draw(ohlcv())
    if reason == "high_lt_low":
        lo, hi = record["low"], record["high"]
        if not lo < hi:  # 零区间 bar：先撑开区间，保证交换后严格 high < low
            hi = lo + 1.0
        record["low"], record["high"] = hi, lo
    elif reason == "negative_value":
        field = draw(st.sampled_from(PRICE_FIELDS))
        record[field] = -draw(_POSITIVE_FLOATS)  # 仅该字段取负
    elif reason == "nan_value":
        field = draw(st.sampled_from(PRICE_FIELDS))
        record[field] = float("nan")  # 仅该字段为 NaN（isfinite 拒绝，D02 §5）
    else:  # invalid_interval
        record["interval"] = draw(st.sampled_from(ILLEGAL_INTERVALS))
    record["invalid_reason"] = reason
    return record


@st.composite
def trade_seq(
    draw: st.DrawFn,
    min_size: int = 1,
    max_size: int = 25,
    gap_after: int | None = None,
) -> list[dict[str, Any]]:
    """aggTrades 归集序列（D04 §4.1 id 语义）：相邻条 prev.l+1==curr.a。

    每条 dict 含 a（首笔 id）/l（末笔 id）/n（笔数，恒有 l = a+n-1）/
    price / quantity / event_time（ISO 字符串，随序严格递增）。

    gap_after=i 时在第 i 与 i+1 条之间注入跳号（curr.a = prev.l + 1 + jump），
    其余相邻关系保持连续对齐；序列长度自动保证 ≥ i+2（供 Q-SEQ-001 跳号测试）。
    """
    if gap_after is not None:
        if gap_after < 0:
            raise ValueError("gap_after 必须 ≥ 0")
        min_size = max(min_size, gap_after + 2)
    max_size = max(max_size, min_size)
    size = draw(st.integers(min_value=min_size, max_value=max_size))
    gap_jump = draw(st.integers(min_value=1, max_value=1000))
    moment = draw(_DATETIMES)
    prev_last_id = draw(st.integers(min_value=1, max_value=10**12)) - 1
    seq: list[dict[str, Any]] = []
    for i in range(size):
        n = draw(st.integers(min_value=1, max_value=10_000))
        skip = gap_jump if (gap_after is not None and i == gap_after + 1) else 0
        first_id = prev_last_id + 1 + skip
        last_id = first_id + n - 1
        moment = moment + timedelta(microseconds=draw(st.integers(1, 10**6)))
        seq.append(
            {
                "a": first_id,
                "l": last_id,
                "n": n,
                "price": draw(_POSITIVE_FLOATS),
                "quantity": draw(_POSITIVE_FLOATS),
                "event_time": moment.isoformat(),
            }
        )
        prev_last_id = last_id
    return seq


@st.composite
def records(draw: st.DrawFn) -> dict[str, Any]:
    """通用 BaseRecord 形 dict（仅 D02 §1 基座字段，不含 Type 专属字段）。"""
    return draw(_base_fields(draw(_DATETIMES)))
