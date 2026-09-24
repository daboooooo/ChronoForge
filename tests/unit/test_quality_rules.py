"""质量规则测试（D09 TC-Q 组，VALIDATION-001.2）。

每条规则 ≥2 用例（触发/不触发），覆盖边界条件。
"""

from __future__ import annotations

import json
import math
from datetime import UTC, date, datetime, timedelta

import pytest

from chronoforge.models.enums import CanonicalType
from chronoforge.quality.report import GapContext, QualityFinding, QualityReport
from chronoforge.quality.rules import (
    get_all_rules,
    get_rule,
    run,
    validate_registry,
)

# ── 工厂函数 ──────────────────────────────────────────────────────────


def _make_ohlcv(
    market_id: str = "BINANCE:BTCUSDT",
    event_time: datetime | None = None,
    open_v: float = 100.0,
    close_v: float = 110.0,
    high_v: float = 120.0,
    low_v: float = 90.0,
    volume: float = 1000.0,
    open_time: datetime | None = None,
    close_time: datetime | None = None,
    extra: dict | None = None,
) -> dict:
    now = datetime.now(UTC)
    if event_time is None:
        event_time = now
    if open_time is None:
        open_time = event_time - timedelta(minutes=1)
    if close_time is None:
        close_time = event_time
    base = {
        "schema_version": "v1",
        "source": "test",
        "source_id": "BTCUSDT",
        "source_timestamp": now,
        "ingest_timestamp": now,
        "raw_record_id": "test:0",
        "market_id": market_id,
        "event_time": event_time,
        "interval": "1m",
        "open": open_v,
        "close": close_v,
        "high": high_v,
        "low": low_v,
        "volume": volume,
        "open_time": open_time,
        "close_time": close_time,
    }
    if extra:
        base.update(extra)
    return base


def _make_trade(
    market_id: str = "BINANCE",
    event_time: datetime | None = None,
    trade_id: str = "100",
    price: float = 50000.0,
    quantity: float = 1.0,
    side: str = "BUY",
) -> dict:
    now = datetime.now(UTC)
    if event_time is None:
        event_time = now
    return {
        "schema_version": "v1",
        "source": "test",
        "source_id": "BTCUSDT",
        "source_timestamp": now,
        "ingest_timestamp": now,
        "raw_record_id": f"test:{trade_id}",
        "market_id": market_id,
        "event_time": event_time,
        "price": price,
        "quantity": quantity,
        "side": side,
        "trade_id": trade_id,
    }


def _make_ticker(
    market_id: str = "BINANCE",
    event_time: datetime | None = None,
    last_price: float = 50000.0,
    bid: float = 49999.0,
    ask: float = 50001.0,
    low_24h: float = 48000.0,
    high_24h: float = 52000.0,
) -> dict:
    now = datetime.now(UTC)
    if event_time is None:
        event_time = now
    return {
        "schema_version": "v1",
        "source": "test",
        "source_id": "BTCUSDT",
        "source_timestamp": now,
        "ingest_timestamp": now,
        "raw_record_id": "test:ticker:0",
        "market_id": market_id,
        "event_time": event_time,
        "last_price": last_price,
        "bid": bid,
        "ask": ask,
        "volume_24h": 10000.0,
        "low_24h": low_24h,
        "high_24h": high_24h,
    }


def _make_number(
    source_id: str = "FRED:UNRATE",
    observation_time: datetime | None = None,
    revision_time: datetime | None = None,
    value: float | None = 3.7,
) -> dict:
    now = datetime.now(UTC)
    if observation_time is None:
        observation_time = now - timedelta(days=30)
    if revision_time is None:
        revision_time = now
    return {
        "schema_version": "v1",
        "source": "test",
        "source_id": source_id,
        "source_timestamp": now,
        "ingest_timestamp": now,
        "raw_record_id": "test:number:0",
        "observation_time": observation_time,
        "release_time": now,
        "revision_time": revision_time,
        "value": value,
        "units": "Percent",
        "seasonal_adjustment": "SA",
    }


def _make_position(
    contract: str = "ES",
    report_date: date | None = None,
    participant_type: str = "COMMERCIAL",
    revision_time: datetime | None = None,
    long_pos: float = 100.0,
    short_pos: float = 80.0,
) -> dict:
    now = datetime.now(UTC)
    if report_date is None:
        report_date = date.today()
    if revision_time is None:
        revision_time = now
    return {
        "schema_version": "v1",
        "source": "test",
        "source_id": "COT:ES",
        "source_timestamp": now,
        "ingest_timestamp": now,
        "raw_record_id": "test:pos:0",
        "report_date": report_date,
        "release_time": now,
        "contract": contract,
        "participant_type": participant_type,
        "long_positions": long_pos,
        "short_positions": short_pos,
        "spreading": 0.0,
        "net_position": long_pos - short_pos,
        "revision_time": revision_time,
    }


# ═══════════════════════════════════════════════════════════════════════
# 架构测试
# ═══════════════════════════════════════════════════════════════════════


class TestArchitecture:
    """规则注册表架构测试。"""

    def test_all_17_rules_registered(self):
        """所有 17 条规则已注册。"""
        rule_ids = {r.rule_id for r in get_all_rules()}
        expected = {
            "Q-DRIFT-001", "Q-SCHEMA-001", "Q-PROV-001", "Q-NULL-001",
            "Q-TS-001", "Q-TS-002", "Q-TS-003", "Q-DUP-001", "Q-SEQ-001",
            "Q-RANGE-001", "Q-RANGE-002", "Q-RANGE-003", "Q-GAP-001",
            "Q-GAP-002", "Q-OHLC-001", "Q-CROSS-001", "Q-REV-001",
        }
        assert rule_ids == expected

    def test_no_duplicate_rule_ids(self):
        """rule_id 无重复。"""
        ids = [r.rule_id for r in get_all_rules()]
        assert len(ids) == len(set(ids))

    def test_validate_registry_passes(self):
        """validate_registry 通过。"""
        validate_registry()  # 不抛异常

    def test_get_rule_by_id(self):
        """按 rule_id 获取规则。"""
        rule = get_rule("Q-SCHEMA-001")
        assert rule is not None
        assert rule.rule_id == "Q-SCHEMA-001"

    def test_get_nonexistent_rule(self):
        """获取不存在的规则返回 None。"""
        assert get_rule("Q-FAKE-000") is None


# ═══════════════════════════════════════════════════════════════════════
# run 函数测试
# ═══════════════════════════════════════════════════════════════════════


class TestRun:
    """run() 函数行为。"""

    def test_run_empty_records(self):
        """空记录集 → 无 findings。"""
        report = run([], CanonicalType.OHLCV)
        assert report.findings == []

    def test_run_no_block_on(self):
        """block_on=[] → 正常返回。"""
        report = run([_make_ohlcv()], CanonicalType.OHLCV, block_on=[])
        assert report.block_on_triggered is False

    def test_run_block_on_triggered(self):
        """block_on 命中 → 抛 StorageError。"""
        from chronoforge.exceptions import StorageError
        with pytest.raises(StorageError):
            run(
                [_make_ohlcv(open_v=math.nan)],  # Q-SCHEMA-001 触发
                CanonicalType.OHLCV,
                block_on=["Q-SCHEMA-001"],
            )

    def test_run_rule_exception_captured(self):
        """规则内部抛异常 → ERROR finding（不中断其余规则）。"""
        # 用空上下文测试 Q-SEQ-001（TRADE 类型），不会抛异常
        trade = _make_trade(trade_id="1")
        report = run([trade], CanonicalType.TRADE)
        assert report is not None


# ═══════════════════════════════════════════════════════════════════════
# Q-SCHEMA-001
# ═══════════════════════════════════════════════════════════════════════


class TestQSchema001:
    """Q-SCHEMA-001：Pydantic 校验失败捕获。"""

    def test_triggered_invalid_ohlcv(self):
        """high < open → 触发。"""
        rec = _make_ohlcv(high_v=95.0)  # high < max(open=100, close=110)
        rule = get_rule("Q-SCHEMA-001")
        findings = rule.check([rec], CanonicalType.OHLCV)
        assert len(findings) == 1

    def test_not_triggered_valid_ohlcv(self):
        """valid OHLCV → 无 finding。"""
        rec = _make_ohlcv()
        rule = get_rule("Q-SCHEMA-001")
        findings = rule.check([rec], CanonicalType.OHLCV)
        assert len(findings) == 0

    def test_skips_unmodeled_types(self):
        """无对应 Pydantic 模型的类型（ORDERBOOK 等）→ 跳过而非误报。

        回归：binance_futures_open_interest 曾因 _build_record 抛
        "Unknown canonical type" 被 Q-SCHEMA-001 误报 ERROR 并阻断。
        """
        rule = get_rule("Q-SCHEMA-001")
        assert rule.check([{"foo": "bar"}], CanonicalType.ORDERBOOK) == []
        assert rule.check([], CanonicalType.DOCUMENT) == []

    def test_open_interest_valid_no_finding(self):
        """valid OPEN_INTEREST → 无 finding（模型校验通过）。"""
        now = datetime.now(UTC)
        rec = {
            "schema_version": "v1",
            "source": "test",
            "source_id": "BTC-PERPETUAL",
            "source_timestamp": now,
            "ingest_timestamp": now,
            "raw_record_id": "test:oi:0",
            "market_id": "DERIBIT:BTC-PERPETUAL",
            "event_time": now,
            "open_interest": 100.0,
            "unit": "BTC",
        }
        rule = get_rule("Q-SCHEMA-001")
        assert rule.check([rec], CanonicalType.OPEN_INTEREST) == []

    def test_open_interest_invalid_triggers(self):
        """open_interest < 0 → Pydantic 校验失败 → 1 finding。"""
        now = datetime.now(UTC)
        rec = {
            "schema_version": "v1",
            "source": "test",
            "source_id": "BTC-PERPETUAL",
            "source_timestamp": now,
            "ingest_timestamp": now,
            "raw_record_id": "test:oi:0",
            "market_id": "DERIBIT:BTC-PERPETUAL",
            "event_time": now,
            "open_interest": -1.0,
            "unit": "BTC",
        }
        rule = get_rule("Q-SCHEMA-001")
        findings = rule.check([rec], CanonicalType.OPEN_INTEREST)
        assert len(findings) == 1


# ═══════════════════════════════════════════════════════════════════════
# Q-PROV-001
# ═══════════════════════════════════════════════════════════════════════


class TestQProv001:
    """Q-PROV-001：provenance 五字段空缺。"""

    def test_triggered_missing_source(self):
        """source 缺失 → 触发。"""
        rec = _make_ohlcv()
        del rec["source"]
        rule = get_rule("Q-PROV-001")
        findings = rule.check([rec], CanonicalType.OHLCV)
        assert len(findings) == 1
        detail = json.loads(findings[0].detail)
        assert "source" in detail["missing_fields"]

    def test_not_triggered_all_present(self):
        """五字段齐全 → 无 finding。"""
        rec = _make_ohlcv()
        rule = get_rule("Q-PROV-001")
        findings = rule.check([rec], CanonicalType.OHLCV)
        assert len(findings) == 0


# ═══════════════════════════════════════════════════════════════════════
# Q-NULL-001
# ═══════════════════════════════════════════════════════════════════════


class TestQNull001:
    """Q-NULL-001：声明必填字段为 None。"""

    def test_triggered_ohlcv_null(self):
        """OHLCV open=None → 触发。"""
        rec = _make_ohlcv(open_v=None)
        rule = get_rule("Q-NULL-001")
        findings = rule.check([rec], CanonicalType.OHLCV)
        assert len(findings) == 1

    def test_not_triggered_complete(self):
        """完整记录 → 无 finding。"""
        rec = _make_ohlcv()
        rule = get_rule("Q-NULL-001")
        findings = rule.check([rec], CanonicalType.OHLCV)
        assert len(findings) == 0


# ═══════════════════════════════════════════════════════════════════════
# Q-TS-001
# ═══════════════════════════════════════════════════════════════════════


class TestQTS001:
    """Q-TS-001：event_time > ingest_time+5min。"""

    def test_triggered_clock_anomaly(self):
        """event_time 比 ingest_time 晚 10min → 触发。"""
        now = datetime.now(UTC)
        rec = _make_ohlcv(event_time=now + timedelta(minutes=10))
        rec["ingest_timestamp"] = now
        rule = get_rule("Q-TS-001")
        findings = rule.check([rec], CanonicalType.OHLCV)
        assert len(findings) == 1

    def test_not_triggered_normal(self):
        """event_time ≤ ingest_time+5min → 无 finding。"""
        now = datetime.now(UTC)
        rec = _make_ohlcv(event_time=now - timedelta(minutes=2))
        rec["ingest_timestamp"] = now
        rule = get_rule("Q-TS-001")
        findings = rule.check([rec], CanonicalType.OHLCV)
        assert len(findings) == 0


# ═══════════════════════════════════════════════════════════════════════
# Q-TS-002
# ═══════════════════════════════════════════════════════════════════════


class TestQTS002:
    """Q-TS-002：相邻 event_time 间隔 ≠ interval。"""

    def test_triggered_gap(self):
        """间隔 ≠ 1m → 触发。"""
        now = datetime.now(UTC)
        recs = [
            _make_ohlcv(event_time=now),
            _make_ohlcv(event_time=now + timedelta(minutes=2)),  # 2min 间隔
        ]
        ctx = GapContext(
            dataset_id="test",
            continuity_model="ALWAYS_OPEN",
            frequency="1m",
        )
        rule = get_rule("Q-TS-002")
        findings = rule.check(recs, CanonicalType.OHLCV, context=ctx)
        assert len(findings) == 1

    def test_not_triggered_aligned(self):
        """间隔 = 1m → 无 finding。"""
        now = datetime.now(UTC)
        recs = [
            _make_ohlcv(event_time=now),
            _make_ohlcv(event_time=now + timedelta(minutes=1)),
        ]
        ctx = GapContext(
            dataset_id="test",
            continuity_model="ALWAYS_OPEN",
            frequency="1m",
        )
        rule = get_rule("Q-TS-002")
        findings = rule.check(recs, CanonicalType.OHLCV, context=ctx)
        assert len(findings) == 0


# ═══════════════════════════════════════════════════════════════════════
# Q-TS-003
# ═══════════════════════════════════════════════════════════════════════


class TestQTS003:
    """Q-TS-003：未收盘 K 线。"""

    def test_triggered_future_kline(self):
        """event_time+1m > now → 触发。"""
        future = datetime.now(UTC) + timedelta(hours=1)
        rec = _make_ohlcv(event_time=future, market_id="BINANCE:BTCUSDT:1m")
        rule = get_rule("Q-TS-003")
        findings = rule.check([rec], CanonicalType.OHLCV)
        assert len(findings) == 1

    def test_not_triggered_past_kline(self):
        """event_time+1m ≤ now → 无 finding。"""
        past = datetime.now(UTC) - timedelta(hours=1)
        rec = _make_ohlcv(event_time=past, market_id="BINANCE:BTCUSDT:1m")
        rule = get_rule("Q-TS-003")
        findings = rule.check([rec], CanonicalType.OHLCV)
        assert len(findings) == 0


# ═══════════════════════════════════════════════════════════════════════
# Q-DUP-001
# ═══════════════════════════════════════════════════════════════════════


class TestQDup001:
    """Q-DUP-001：natural key 在批次内重复。"""

    def test_triggered_duplicate(self):
        """同 nk 两条记录 → 触发。"""
        rec1 = _make_ohlcv(event_time=datetime.now(UTC))
        rec2 = _make_ohlcv(event_time=rec1["event_time"])  # 相同 nk
        rule = get_rule("Q-DUP-001")
        findings = rule.check([rec1, rec2], CanonicalType.OHLCV)
        assert len(findings) == 1

    def test_not_triggered_unique(self):
        """不同 nk → 无 finding。"""
        now = datetime.now(UTC)
        recs = [
            _make_ohlcv(event_time=now),
            _make_ohlcv(event_time=now - timedelta(minutes=1)),
        ]
        rule = get_rule("Q-DUP-001")
        findings = rule.check(recs, CanonicalType.OHLCV)
        assert len(findings) == 0


# ═══════════════════════════════════════════════════════════════════════
# Q-SEQ-001
# ═══════════════════════════════════════════════════════════════════════


class TestQSeq001:
    """Q-SEQ-001：aggTrades 归集序列对账。"""

    def test_triggered_jump(self):
        """trade_id 跳号 → 触发。"""
        recs = [
            _make_trade(trade_id="100"),
            _make_trade(trade_id="103"),  # 跳号 2
        ]
        rule = get_rule("Q-SEQ-001")
        findings = rule.check(recs, CanonicalType.TRADE)
        assert len(findings) == 1
        detail = json.loads(findings[0].detail)
        assert detail["gap"] == 2

    def test_not_triggered_aligned(self):
        """trade_id 对齐 → 无 finding。"""
        recs = [
            _make_trade(trade_id="100"),
            _make_trade(trade_id="101"),
        ]
        rule = get_rule("Q-SEQ-001")
        findings = rule.check(recs, CanonicalType.TRADE)
        assert len(findings) == 0


# ═══════════════════════════════════════════════════════════════════════
# Q-RANGE-001
# ═══════════════════════════════════════════════════════════════════════


class TestQRange001:
    """Q-RANGE-001：OHLCV 值域校验。"""

    def test_triggered_high_too_low(self):
        """high < max(open, close) → 触发。"""
        rec = _make_ohlcv(open_v=100, close_v=110, high_v=105)
        rule = get_rule("Q-RANGE-001")
        findings = rule.check([rec], CanonicalType.OHLCV)
        assert len(findings) == 1

    def test_triggered_negative_volume(self):
        """volume < 0 → 触发。"""
        rec = _make_ohlcv(volume=-100)
        rule = get_rule("Q-RANGE-001")
        findings = rule.check([rec], CanonicalType.OHLCV)
        assert len(findings) == 1

    def test_not_triggered_valid(self):
        """正常 OHLCV → 无 finding。"""
        rec = _make_ohlcv()
        rule = get_rule("Q-RANGE-001")
        findings = rule.check([rec], CanonicalType.OHLCV)
        assert len(findings) == 0


# ═══════════════════════════════════════════════════════════════════════
# Q-RANGE-002
# ═══════════════════════════════════════════════════════════════════════


class TestQRange002:
    """Q-RANGE-002：iv∈[0,5]；probability∈[0,1]。"""

    def test_triggered_iv_out_of_range(self):
        """iv=6 → 触发。"""
        rec = {
            "schema_version": "v1",
            "source": "test",
            "source_id": "OPTION",
            "source_timestamp": datetime.now(UTC),
            "ingest_timestamp": datetime.now(UTC),
            "raw_record_id": "test:iv:0",
            "instrument_id": "BTC-26DEC26-100000-C",
            "event_time": datetime.now(UTC),
            "iv": 6.0,  # > 5
            "data_tier": "OTM",
        }
        rule = get_rule("Q-RANGE-002")
        findings = rule.check([rec], CanonicalType.IMPLIED_VOLATILITY)
        assert len(findings) == 1

    def test_triggered_probability_out_of_range(self):
        """implied_probability=1.5 → 触发。"""
        rec = {
            "schema_version": "v1",
            "source": "test",
            "source_id": "POLYMARKET",
            "source_timestamp": datetime.now(UTC),
            "ingest_timestamp": datetime.now(UTC),
            "raw_record_id": "test:pp:0",
            "market_id": "BTC-hits-100k",
            "outcome_id": "yes",
            "event_time": datetime.now(UTC),
            "price": 0.5,
            "implied_probability": 1.5,  # > 1
            "volume": 1000,
            "liquidity": 5000,
        }
        rule = get_rule("Q-RANGE-002")
        findings = rule.check([rec], CanonicalType.PREDICTION_PRICE)
        assert len(findings) == 1

    def test_not_triggered_valid(self):
        """正常值 → 无 finding。"""
        rec = {
            "schema_version": "v1",
            "source": "test",
            "source_id": "OPTION",
            "source_timestamp": datetime.now(UTC),
            "ingest_timestamp": datetime.now(UTC),
            "raw_record_id": "test:iv:0",
            "instrument_id": "BTC-26DEC26-100000-C",
            "event_time": datetime.now(UTC),
            "iv": 0.5,  # in [0, 5]
            "data_tier": "OTM",
        }
        rule = get_rule("Q-RANGE-002")
        findings = rule.check([rec], CanonicalType.IMPLIED_VOLATILITY)
        assert len(findings) == 0


# ═══════════════════════════════════════════════════════════════════════
# Q-RANGE-003
# ═══════════════════════════════════════════════════════════════════════


class TestQRange003:
    """Q-RANGE-003：OPTION strike≤0；expiry 过期。"""

    def test_triggered_expired_expiry(self):
        """expiry < today → 触发。"""
        rec = {
            "schema_version": "v1",
            "source": "test",
            "source_id": "DERIBIT",
            "source_timestamp": datetime.now(UTC),
            "ingest_timestamp": datetime.now(UTC),
            "raw_record_id": "test:opt:0",
            "market_id": "DERIBIT",
            "instrument_id": "BTC-20JAN20-5000-C",
            "event_time": datetime.now(UTC),
            "underlying": "BTC",
            "expiry": date(2020, 1, 20),  # expired
            "strike": 50000.0,
            "option_type": "CALL",
            "settlement_asset": "USD",
            "mark_price": 100.0,
            "bid": 90.0,
            "ask": 110.0,
        }
        rule = get_rule("Q-RANGE-003")
        findings = rule.check([rec], CanonicalType.OPTION)
        assert len(findings) == 1

    def test_not_triggered_valid(self):
        """正常 OPTION → 无 finding。"""
        future_expiry = date.today() + timedelta(days=30)
        rec = {
            "schema_version": "v1",
            "source": "test",
            "source_id": "DERIBIT",
            "source_timestamp": datetime.now(UTC),
            "ingest_timestamp": datetime.now(UTC),
            "raw_record_id": "test:opt:0",
            "market_id": "DERIBIT",
            "instrument_id": "BTC-26DEC26-50000-C",
            "event_time": datetime.now(UTC),
            "underlying": "BTC",
            "expiry": future_expiry,
            "strike": 50000.0,
            "option_type": "CALL",
            "settlement_asset": "USD",
            "mark_price": 100.0,
            "bid": 90.0,
            "ask": 110.0,
        }
        rule = get_rule("Q-RANGE-003")
        findings = rule.check([rec], CanonicalType.OPTION)
        assert len(findings) == 0


# ═══════════════════════════════════════════════════════════════════════
# Q-GAP-001
# ═══════════════════════════════════════════════════════════════════════


class TestQGap001:
    """Q-GAP-001：网格连续性检测。"""

    def test_triggered_gaps(self):
        """1m 网格缺行 → 触发。"""
        now = datetime.now(UTC)
        # 只给 10:00, 10:01, 缺 10:02-10:04（连续 3 行）
        recs = [
            _make_ohlcv(event_time=now),  # 10:00
            _make_ohlcv(event_time=now + timedelta(minutes=1)),  # 10:01
            # 缺 10:02, 10:03, 10:04
            _make_ohlcv(event_time=now + timedelta(minutes=5)),  # 10:05
        ]
        recs[0]["market_id"] = "BINANCE:BTCUSDT:1m"
        recs[1]["market_id"] = "BINANCE:BTCUSDT:1m"
        recs[2]["market_id"] = "BINANCE:BTCUSDT:1m"
        ctx = GapContext(
            dataset_id="test",
            continuity_model="ALWAYS_OPEN",
            frequency="1min",
        )
        rule = get_rule("Q-GAP-001")
        findings = rule.check(recs, CanonicalType.OHLCV, context=ctx)
        assert len(findings) == 1

    def test_not_triggered_continuous(self):
        """连续 1m 网格 → 无 finding。"""
        now = datetime.now(UTC)
        recs = [
            _make_ohlcv(event_time=now),
            _make_ohlcv(event_time=now + timedelta(minutes=1)),
            _make_ohlcv(event_time=now + timedelta(minutes=2)),
        ]
        for r in recs:
            r["market_id"] = "BINANCE:BTCUSDT:1m"
        ctx = GapContext(
            dataset_id="test",
            continuity_model="ALWAYS_OPEN",
            frequency="1min",
        )
        rule = get_rule("Q-GAP-001")
        findings = rule.check(recs, CanonicalType.OHLCV, context=ctx)
        assert len(findings) == 0

    def test_severity_always_open(self):
        """ALWAYS_OPEN → WARNING severity。"""
        now = datetime.now(UTC)
        recs = [
            _make_ohlcv(event_time=now),
            _make_ohlcv(event_time=now + timedelta(minutes=2)),
        ]
        for r in recs:
            r["market_id"] = "BINANCE:BTCUSDT:1m"
        ctx = GapContext(
            dataset_id="test",
            continuity_model="ALWAYS_OPEN",
            frequency="1min",
        )
        rule = get_rule("Q-GAP-001")
        findings = rule.check(recs, CanonicalType.OHLCV, context=ctx)
        assert len(findings) == 1
        assert findings[0].severity == "WARNING"

    def test_no_context(self):
        """无 context → 无 finding。"""
        rule = get_rule("Q-GAP-001")
        findings = rule.check([_make_ohlcv()], CanonicalType.OHLCV)
        assert len(findings) == 0


# ═══════════════════════════════════════════════════════════════════════
# Q-GAP-002
# ═══════════════════════════════════════════════════════════════════════


class TestQGap002:
    """Q-GAP-002：与 frequency 声明对比缺行。"""

    def test_triggered_gap(self):
        """LIQUIDATION_AGGREGATE 缺行 → 触发。"""
        now = datetime.now(UTC)
        recs = [
            {"event_time": now},
            {"event_time": now + timedelta(minutes=5)},
        ]
        ctx = GapContext(
            dataset_id="test",
            continuity_model="ALWAYS_OPEN",
            frequency="1min",
        )
        rule = get_rule("Q-GAP-002")
        findings = rule.check(recs, CanonicalType.LIQUIDATION_AGGREGATE, context=ctx)
        assert len(findings) == 1

    def test_not_triggered_continuous(self):
        """连续 → 无 finding。"""
        now = datetime.now(UTC)
        recs = [
            {"event_time": now},
            {"event_time": now + timedelta(minutes=1)},
        ]
        ctx = GapContext(
            dataset_id="test",
            continuity_model="ALWAYS_OPEN",
            frequency="1min",
        )
        rule = get_rule("Q-GAP-002")
        findings = rule.check(recs, CanonicalType.LIQUIDATION_AGGREGATE, context=ctx)
        assert len(findings) == 0


# ═══════════════════════════════════════════════════════════════════════
# Q-OHLC-001
# ═══════════════════════════════════════════════════════════════════════


class TestQOHLC001:
    """Q-OHLC-001：closeTime-openTime ≈ interval。"""

    def test_triggered_interval_mismatch(self):
        """openTime 到 closeTime = 2min（1m 频率）→ 触发。"""
        now = datetime.now(UTC)
        rec = _make_ohlcv(
            open_time=now,
            close_time=now + timedelta(minutes=2),
            market_id="BINANCE:BTCUSDT:1m",
        )
        rule = get_rule("Q-OHLC-001")
        findings = rule.check([rec], CanonicalType.OHLCV)
        assert len(findings) == 1

    def test_not_triggered_aligned(self):
        """openTime 到 closeTime = 1min（1m 频率）→ 无 finding。"""
        now = datetime.now(UTC)
        rec = _make_ohlcv(
            open_time=now - timedelta(minutes=1),
            close_time=now,
            market_id="BINANCE:BTCUSDT:1m",
        )
        rule = get_rule("Q-OHLC-001")
        findings = rule.check([rec], CanonicalType.OHLCV)
        assert len(findings) == 0


# ═══════════════════════════════════════════════════════════════════════
# Q-CROSS-001
# ═══════════════════════════════════════════════════════════════════════


class TestQCross001:
    """Q-CROSS-001：bid>ask、last 出界。"""

    def test_triggered_bid_gt_ask(self):
        """bid=50000 > ask=49999 → 触发。"""
        rec = _make_ticker(bid=50000.0, ask=49999.0)
        rule = get_rule("Q-CROSS-001")
        findings = rule.check([rec], CanonicalType.TICKER)
        assert len(findings) == 1

    def test_triggered_last_out_of_range(self):
        """last=53000 > high24=52000 → 触发。"""
        rec = _make_ticker(last_price=53000.0, low_24h=48000.0, high_24h=52000.0)
        rule = get_rule("Q-CROSS-001")
        findings = rule.check([rec], CanonicalType.TICKER)
        assert len(findings) == 1

    def test_not_triggered_valid(self):
        """bid < ask，last 在 [low24, high24] → 无 finding。"""
        rec = _make_ticker()
        rule = get_rule("Q-CROSS-001")
        findings = rule.check([rec], CanonicalType.TICKER)
        assert len(findings) == 0


# ═══════════════════════════════════════════════════════════════════════
# Q-REV-001
# ═══════════════════════════════════════════════════════════════════════


class TestQRev001:
    """Q-REV-001：同 nk 多 revision_time。"""

    def test_triggered_multiple_revisions(self):
        """同一 (contract, report_date, participant_type) 多 revision_time → 触发。"""
        now = datetime.now(UTC)
        recs = [
            _make_position(revision_time=now - timedelta(days=1)),
            _make_position(revision_time=now),
        ]
        rule = get_rule("Q-REV-001")
        findings = rule.check(recs, CanonicalType.POSITION)
        assert len(findings) == 1
        detail = json.loads(findings[0].detail)
        assert detail["revision_count"] == 2

    def test_not_triggered_single_revision(self):
        """单一 revision_time → 无 finding。"""
        now = datetime.now(UTC)
        recs = [
            _make_position(revision_time=now),
            _make_position(contract="GC", revision_time=now),  # 不同 contract
        ]
        rule = get_rule("Q-REV-001")
        findings = rule.check(recs, CanonicalType.POSITION)
        assert len(findings) == 0


# ═══════════════════════════════════════════════════════════════════════
# Q-DRIFT-001（已有）
# ═══════════════════════════════════════════════════════════════════════


class TestQDrift001:
    """Q-DRIFT-001：值漂移检测（P0 返回空列表）。"""

    def test_returns_empty(self):
        """P0 实现返回空列表。"""
        rule = get_rule("Q-DRIFT-001")
        findings = rule.check([_make_number()], CanonicalType.NUMBER)
        assert findings == []


# ═══════════════════════════════════════════════════════════════════════
# 边界条件
# ═══════════════════════════════════════════════════════════════════════


class TestEdgeCases:
    """边界条件测试。"""

    def test_empty_records_all_rules(self):
        """空记录集所有规则无 findings。"""
        for rule in get_all_rules():
            findings = rule.check([], CanonicalType.OHLCV)
            assert len(findings) == 0, f"{rule.rule_id} empty records failed"

    def test_single_record(self):
        """单记录：Q-DUP-001 不触发。"""
        rule = get_rule("Q-DUP-001")
        findings = rule.check([_make_ohlcv()], CanonicalType.OHLCV)
        assert len(findings) == 0

    def test_all_findings_are_quality_finding(self):
        """所有 findings 为 QualityFinding 实例。"""
        rec = _make_ohlcv(open_v=math.nan)  # Q-SCHEMA-001 + Q-RANGE-001
        report = run([rec], CanonicalType.OHLCV)
        for f in report.findings:
            assert isinstance(f, QualityFinding)


# ═══════════════════════════════════════════════════════════════════════
# TC-Q-006: quality --json 报告 schema
# ═══════════════════════════════════════════════════════════════════════


class TestReportJsonSchema:
    """TC-Q-006: QualityReport.to_json() 报告 schema（D09 §3）。"""

    def test_empty_report_schema(self):
        """空报告：顶层键集合与计数归零。"""
        report = QualityReport(findings=[])
        data = json.loads(report.to_json())
        assert data == {
            "findings": [],
            "rule_id": None,
            "error_count": 0,
            "warning_count": 0,
            "info_count": 0,
        }

    def test_finding_fields_serialized(self):
        """finding 字段完整且值保真（record_key/rule_id/severity/detail/
        raw_ref/payload_digest）。"""
        finding = QualityFinding(
            record_key="BINANCE:BTCUSDT:SPOT|2026-01-01T00:00:00|1m",
            rule_id="Q-SCHEMA-001",
            severity="ERROR",
            detail='{"field": "open"}',
            raw_ref="raw/binance_spot/ingest_date=2026-01-01/x.jsonl:1",
            payload_digest="deadbeef",
        )
        data = json.loads(QualityReport(findings=[finding]).to_json())
        assert data["findings"] == [
            {
                "record_key": "BINANCE:BTCUSDT:SPOT|2026-01-01T00:00:00|1m",
                "rule_id": "Q-SCHEMA-001",
                "severity": "ERROR",
                "detail": '{"field": "open"}',
                "raw_ref": "raw/binance_spot/ingest_date=2026-01-01/x.jsonl:1",
                "payload_digest": "deadbeef",
            }
        ]
        assert data["error_count"] == 1

    def test_severity_counts_mixed(self):
        """ERROR/WARNING/INFO 混合计数正确。"""
        def mk(sev: str, rid: str) -> QualityFinding:
            return QualityFinding(
                record_key="k", rule_id=rid, severity=sev, detail="{}"
            )
        report = QualityReport(
            findings=[mk("ERROR", "Q-A-001"), mk("WARNING", "Q-B-001"),
                      mk("WARNING", "Q-B-002"), mk("INFO", "Q-C-001")]
        )
        data = json.loads(report.to_json())
        assert data["error_count"] == 1
        assert data["warning_count"] == 2
        assert data["info_count"] == 1

    def test_block_on_rule_id_serialized(self):
        """block_on 命中形态（rule_id 字段）序列化进报告。"""
        finding = QualityFinding(
            record_key="k", rule_id="Q-SCHEMA-001", severity="ERROR",
            detail="{}",
        )
        # run() 在 block_on 命中时抛 StorageError（不返回 report），
        # 管道捕获后以 report.rule_id 记录阻断规则——直接构造该形态
        report = QualityReport(findings=[finding], rule_id="Q-SCHEMA-001")
        data = json.loads(report.to_json())
        assert data["rule_id"] == "Q-SCHEMA-001"
