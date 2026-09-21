"""架构测试：模型层 natural_key ↔ 存储层 _NATURAL_KEY_MAP 一致性。

背景（审计 2026-09-19 C-1）：storage/base.py 的身份键映射曾与冻结设计
（D02 §2）及模型层 natural_key() 系统性偏离（OHLCV 缺 interval、TRADE 用
event_time 而非 trade_id 等），导致非 revision 类 upsert keep-last 去重时
系统性静默合并不同身份的记录。

本测试对全部 CanonicalType 逐类型守卫三条不变量：
1. 存储层身份键列均存在于模型字段中（防列名漂移/删除）；
2. 存储层身份键从合法样例记录提取的值 == 模型层 natural_key()（防
   键集合增删/顺序漂移）；
3. REVISION_TYPES 的身份键均含 revision_time（D03 §3 天然多版本语义）。

任一模型字段/身份键变更未同步两侧时，本测试必须失败。
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

import pytest

from chronoforge.models.base import BaseRecord
from chronoforge.models.derivatives import (
    GREEKS,
    IMPLIED_VOLATILITY,
    LIQUIDATION_AGGREGATE,
    LIQUIDATION_EVENT,
    OPTION,
)
from chronoforge.models.derived import DERIVED, FEATURE
from chronoforge.models.enums import CanonicalType
from chronoforge.models.fundamental import DOCUMENT, FILING, FUNDAMENTAL
from chronoforge.models.macro import FLOW, MACRO_EVENT, NUMBER
from chronoforge.models.market import FUNDING, OHLCV, OPEN_INTEREST, ORDERBOOK, TICKER, TRADE
from chronoforge.models.positioning import POSITION, POSITION_AGGREGATE
from chronoforge.models.prediction import PREDICTION_MARKET, PREDICTION_PRICE
from chronoforge.models.reference import ENTITY, INSTRUMENT
from chronoforge.models.text import TEXT_EVENT, TEXT_MESSAGE
from chronoforge.storage.base import REVISION_TYPES, natural_key

# ── 合法样例（每类型一条，满足 D02 §2 全部校验器）──────────────────────

_DT = datetime(2026, 9, 19, 12, 0, 0)
_DATE = date(2026, 9, 19)
_SHA256 = "a" * 64


def _base(**overrides: Any) -> dict[str, Any]:
    """BaseRecord 基座字段（D02 §1）。"""
    fields: dict[str, Any] = {
        "schema_version": "1.0",
        "source": "binance_spot",
        "source_id": "BTCUSDT",
        "source_timestamp": None,
        "ingest_timestamp": _DT,
        "raw_record_id": "binance_spot:klines:shard.jsonl:1",
    }
    fields.update(overrides)
    return fields


#: CanonicalType → (模型类, 合法样例 dict)
SAMPLES: dict[CanonicalType, tuple[type[BaseRecord], dict[str, Any]]] = {
    CanonicalType.OHLCV: (
        OHLCV,
        _base(
            market_id="BINANCE:BTCUSDT:SPOT",
            event_time=_DT,
            interval="1m",
            open=100.0,
            high=110.0,
            low=90.0,
            close=105.0,
            volume=1.0,
        ),
    ),
    CanonicalType.TRADE: (
        TRADE,
        _base(
            market_id="BINANCE:BTCUSDT:SPOT",
            event_time=_DT,
            price=100.0,
            quantity=1.0,
            side="BUY",
            trade_id="T1",
        ),
    ),
    CanonicalType.TICKER: (
        TICKER,
        _base(
            market_id="BINANCE:BTCUSDT:SPOT",
            event_time=_DT,
            last_price=100.0,
            bid=99.0,
            ask=101.0,
            volume_24h=1.0,
            quote_volume_24h=1.0,
        ),
    ),
    CanonicalType.FUNDING: (
        FUNDING,
        _base(
            market_id="BINANCE:BTCUSDT:USDT-FUT",
            event_time=_DT,
            funding_rate=0.0001,
            next_funding_time=datetime(2026, 9, 19, 20, 0, 0),
        ),
    ),
    CanonicalType.OPEN_INTEREST: (
        OPEN_INTEREST,
        _base(
            market_id="BINANCE:BTCUSDT:USDT-FUT",
            event_time=_DT,
            open_interest=1.0,
            unit="USD",
        ),
    ),
    CanonicalType.ORDERBOOK: (
        ORDERBOOK,
        _base(
            market_id="BINANCE:BTCUSDT:SPOT",
            event_time=_DT,
            transaction_time=_DT,
            bids=[[99.0, 1.0]],
            asks=[[101.0, 1.0]],
        ),
    ),
    CanonicalType.OPTION: (
        OPTION,
        _base(
            market_id="DERIBIT:BTC-26SEP26-100000-C:OPTION",
            instrument_id="BTC-2026-09-26-100000-C",
            event_time=_DT,
            underlying="BTC",
            expiry=date(2026, 9, 26),
            strike=100000.0,
            option_type="CALL",
            settlement_asset="USD",
            mark_price=1.0,
            bid=1.0,
            ask=2.0,
        ),
    ),
    CanonicalType.IMPLIED_VOLATILITY: (
        IMPLIED_VOLATILITY,
        _base(
            instrument_id="BTC-2026-09-26-100000-C",
            event_time=_DT,
            iv=0.5,
            mark_iv=None,
            bid_iv=None,
            ask_iv=None,
            implied_forward=None,
            data_tier="ATM",
        ),
    ),
    CanonicalType.GREEKS: (
        GREEKS,
        _base(
            instrument_id="BTC-2026-09-26-100000-C",
            event_time=_DT,
            delta=0.5,
            gamma=0.1,
            vega=1.0,
            theta=-1.0,
            rho=0.1,
        ),
    ),
    CanonicalType.LIQUIDATION_EVENT: (
        LIQUIDATION_EVENT,
        _base(
            market_id="BINANCE:BTCUSDT:USDT-FUT",
            event_time=_DT,
            side="SELL",
            price=100.0,
            quantity=1.0,
            order_id="O1",
        ),
    ),
    CanonicalType.LIQUIDATION_AGGREGATE: (
        LIQUIDATION_AGGREGATE,
        _base(
            market_id="BINANCE:BTCUSDT:USDT-FUT",
            event_time=_DT,
            interval="1h",
            buy_vol=1.0,
            sell_vol=1.0,
            total_notional=2.0,
        ),
    ),
    CanonicalType.NUMBER: (
        NUMBER,
        _base(
            source_id="CPIAUCSL",
            observation_time=_DT,
            release_time=_DT,
            revision_time=_DT,
            value=1.0,
            units="index",
            seasonal_adjustment="SA",
            vintage_date="2026-09-19",
        ),
    ),
    CanonicalType.FLOW: (
        FLOW,
        _base(
            source_id="GDP",
            observation_time=_DT,
            release_time=_DT,
            revision_time=_DT,
            value=1.0,
            units="USD",
            seasonal_adjustment="SA",
            vintage_date="2026-09-19",
            period_start=datetime(2026, 1, 1),
            period_end=datetime(2026, 3, 31),
        ),
    ),
    CanonicalType.MACRO_EVENT: (
        MACRO_EVENT,
        _base(
            source_id="fred",
            event_ref="CPI",
            scheduled_time=_DT,
            actual_time=None,
            actual=None,
            forecast=None,
            previous=None,
        ),
    ),
    CanonicalType.FUNDAMENTAL: (
        FUNDAMENTAL,
        _base(
            source_id="sec_edgar",
            entity_id="AAPL",
            cik="0000320193",
            concept="Assets",
            taxon="us-gaap",
            unit="USD",
            observation_time=_DT,
            value=1.0,
            fiscal_year=2026,
            fiscal_period="Q1",
            frame="CY2026Q1",
        ),
    ),
    CanonicalType.FILING: (
        FILING,
        _base(
            source_id="sec_edgar",
            entity_id="AAPL",
            cik="0000320193",
            accession_number="0000320193-26-000012",
            form_type="10-K",
            filing_date=_DATE,
            accepted_datetime=_DT,
            report_period_end=_DATE,
            primary_doc_url="https://sec.gov/aapl-10k.pdf",
        ),
    ),
    CanonicalType.DOCUMENT: (
        DOCUMENT,
        _base(
            source_id="sec_edgar",
            url="https://sec.gov/aapl-10k.pdf",
            publication_time=_DT,
            content_hash=_SHA256,
            mime="application/pdf",
        ),
    ),
    CanonicalType.POSITION: (
        POSITION,
        _base(
            source_id="cftc",
            report_date=_DATE,
            release_time=_DT,
            contract="CL",
            participant_type="COMMERCIAL",
            long_positions=10.0,
            short_positions=4.0,
            spreading=0.0,
            net_position=6.0,
            revision_time=_DT,
        ),
    ),
    CanonicalType.POSITION_AGGREGATE: (
        POSITION_AGGREGATE,
        _base(
            source_id="cftc",
            report_date=_DATE,
            release_time=_DT,
            contract="CL",
            participant_type="COMMERCIAL",
            long_positions=10.0,
            short_positions=4.0,
            spreading=0.0,
            net_position=6.0,
            revision_time=_DT,
        ),
    ),
    CanonicalType.PREDICTION_MARKET: (
        PREDICTION_MARKET,
        _base(
            source_id="polymarket",
            event_id="E1",
            question="q?",
            outcomes=["A", "B"],
            close_time=_DT,
            status="OPEN",
        ),
    ),
    CanonicalType.PREDICTION_PRICE: (
        PREDICTION_PRICE,
        _base(
            source_id="polymarket",
            market_id="POLY:E1",
            outcome_id="A",
            event_time=_DT,
            price=0.5,
            implied_probability=0.5,
            volume=1.0,
            liquidity=1.0,
        ),
    ),
    CanonicalType.TEXT_MESSAGE: (
        TEXT_MESSAGE,
        _base(
            source_id="twitter",
            event_time=_DT,
            author="alice",
            text_hash=_SHA256,
            language="en",
        ),
    ),
    CanonicalType.TEXT_EVENT: (
        TEXT_EVENT,
        _base(
            source_id="gdelt",
            event_time=_DT,
            gkg_themes=["ECON", "MARKET"],
            entities=["AAPL"],
        ),
    ),
    CanonicalType.ENTITY: (
        ENTITY,
        _base(
            source_id="reference",
            entity_id="AAPL",
            canonical_name="Apple Inc",
            entity_type="EQUITY",
            aliases=[],
        ),
    ),
    CanonicalType.INSTRUMENT: (
        INSTRUMENT,
        _base(
            source_id="reference",
            instrument_id="AAPL-SPOT",
            entity_id="AAPL",
            instrument_type="SPOT",
            expiry=None,
            strike=None,
            option_type=None,
            settlement_asset=None,
        ),
    ),
    CanonicalType.DERIVED: (
        DERIVED,
        _base(
            source_id="derived",
            name="BTC_MARKET_STRESS",
            computed_at=_DT,
            dependencies=[("BINANCE:BTCUSDT:SPOT", "1")],
            value=1.0,
            params={},
            source_type="derived",
        ),
    ),
    CanonicalType.FEATURE: (
        FEATURE,
        _base(
            source_id="derived",
            name="btc_volatility_ratio",
            computed_at=_DT,
            dependencies=[("BINANCE:BTCUSDT:SPOT", "1")],
            value=1.0,
            params={},
            feature_engine_version="1",
        ),
    ),
}


def _normalize(value: Any) -> Any:
    """归一化模型/存储两侧的 NK 值以便比较。

    - list → 排序后 tuple（TEXT_EVENT.gkg_themes：模型层 sorted tuple 语义）；
    - 其余（str 枚举 == 字符串值、date/datetime 相等）原样返回。
    """
    if isinstance(value, list):
        return tuple(sorted(value))
    return value


# ── 不变量 1/2：逐类型映射一致性 ──────────────────────────────────────

assert set(SAMPLES) == set(CanonicalType), "样例表必须覆盖全部 CanonicalType"


@pytest.mark.parametrize("canonical_type", sorted(CanonicalType, key=lambda c: c.value))
def test_storage_nk_matches_model_natural_key(canonical_type: CanonicalType) -> None:
    """存储层身份键 ↔ 模型层 natural_key() 逐类型一致（审计 C-1 守卫）。"""
    model_cls, sample = SAMPLES[canonical_type]
    record = model_cls(**sample)

    nk_cols = natural_key(canonical_type)

    # 不变量 1：存储层身份键列必须是模型声明的字段
    model_fields = set(model_cls.model_fields)
    unknown_cols = [col for col in nk_cols if col not in model_fields]
    assert not unknown_cols, (
        f"{canonical_type.value}: 存储层身份键列 {unknown_cols} 不在模型字段中 "
        f"（存储层与模型层身份契约漂移）"
    )

    # 不变量 2：存储层身份键提取值 == 模型层 natural_key()
    storage_key = tuple(_normalize(sample[col]) for col in nk_cols)
    model_key = tuple(_normalize(v) for v in record.natural_key())
    assert storage_key == model_key, (
        f"{canonical_type.value}: 存储层身份键 {storage_key} != "
        f"模型层 natural_key() {model_key}"
    )


# ── 不变量 3：REVISION_TYPES 身份键含 revision_time ──────────────────


@pytest.mark.parametrize("canonical_type", sorted(REVISION_TYPES, key=lambda c: c.value))
def test_revision_types_nk_contains_revision_time(canonical_type: CanonicalType) -> None:
    """D03 §3：revision 类身份键必须含 revision_time（天然多版本）。"""
    nk_cols = natural_key(canonical_type)
    assert "revision_time" in nk_cols, (
        f"{canonical_type.value}: REVISION_TYPES 成员身份键缺少 revision_time"
    )


# ── 不变量 4（审计 H-4）：冻结 Arrow schema 覆盖全部类型与身份列 ─────


@pytest.mark.parametrize("canonical_type", sorted(CanonicalType, key=lambda c: c.value))
def test_declared_arrow_schema_covers_nk_and_unique(canonical_type: CanonicalType) -> None:
    """_SCHEMAS[ct] 必须存在、无重名列、覆盖身份键列（审计 H-4 冻结 schema）。"""
    from chronoforge.storage.canonical import _SCHEMAS

    schema = _SCHEMAS[canonical_type]
    names = schema.names
    assert len(names) == len(set(names)), (
        f"{canonical_type.value}: 声明 schema 存在重名列"
    )
    missing = [col for col in natural_key(canonical_type) if col not in names]
    assert not missing, f"{canonical_type.value}: 声明 schema 缺少身份键列 {missing}"


@pytest.mark.parametrize("canonical_type", sorted(CanonicalType, key=lambda c: c.value))
def test_records_to_df_roundtrip_declared_schema(canonical_type: CanonicalType) -> None:
    """合法样例经 _records_to_df 后 schema == 声明 schema，身份键值保真。"""
    from chronoforge.storage.canonical import _SCHEMAS, _records_to_df

    model_cls, sample = SAMPLES[canonical_type]
    record = model_cls(**sample)

    table = _records_to_df([dict(sample)], canonical_type)
    assert table.schema.equals(_SCHEMAS[canonical_type], check_metadata=False)

    for col, model_value in zip(
        natural_key(canonical_type), record.natural_key(), strict=True
    ):
        stored = _normalize(table.column(col)[0].as_py())
        assert stored == _normalize(model_value), (
            f"{canonical_type.value}: 身份键列 {col} 经声明 schema 转换后失真"
        )
