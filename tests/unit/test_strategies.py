"""INFRA-001 策略库自检（用例 ID 即 docstring 首行，D09 §1 约定）。

策略生成目标形态：docs/design/02-data-models.md §1–§2 的 dict 级 Canonical 记录；
MODEL-002 落地前仅做 dict 级合法性校验（任务单 INFRA-001 tests.self 项）。
"""

from __future__ import annotations

import math
import os
from datetime import datetime
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from strategies import (
    BASE_RECORD_KEYS,
    INTERVALS,
    INVALID_REASONS,
    OHLCV_KEYS,
    PRICE_FIELDS,
    QUALITY_STATUSES,
    ohlcv,
    ohlcv_invalid,
    records,
    trade_seq,
)

_OHLCV_REQUIRED_KEYS = set(BASE_RECORD_KEYS) | set(OHLCV_KEYS)
_TRADE_KEYS = {"a", "l", "n", "price", "quantity", "event_time"}


def _assert_violation_matches(record: dict[str, Any], reason: str) -> None:
    """按 invalid_reason 断言显式注入的违规确实存在（且仅注入该类缺陷）。"""
    if reason == "high_lt_low":
        assert record["high"] < record["low"]
    elif reason == "negative_value":
        assert sum(record[f] < 0 for f in PRICE_FIELDS) == 1
    elif reason == "nan_value":
        assert sum(math.isnan(record[f]) for f in PRICE_FIELDS) == 1
    elif reason == "invalid_interval":
        assert record["interval"] not in INTERVALS
        # interval 之外的数值域保持合法形态（单类违规，OHLC 不变量未被触碰）
        assert record["low"] <= min(record["open"], record["close"])
        assert max(record["open"], record["close"]) <= record["high"]
        assert all(record[f] > 0 and math.isfinite(record[f]) for f in PRICE_FIELDS)
    else:
        pytest.fail(f"未知 invalid_reason: {reason}")


@given(ohlcv())
@settings(max_examples=1000)
def test_ohlcv_keys_finite_and_invariants(record: dict[str, Any]) -> None:
    """TC-PROP-003/INFRA-SELF-01 ohlcv() 千例含全部必填键、数值 finite
    且 OHLC 不变量成立（low ≤ min(o,c) ∧ max(o,c) ≤ high ∧ volume ≥ 0）"""
    assert set(record) == _OHLCV_REQUIRED_KEYS
    for field in PRICE_FIELDS:
        value = record[field]
        assert math.isfinite(value), f"{field} 非 finite: {value}"
        assert value > 0, f"{field} 非正: {value}"
    assert record["low"] <= min(record["open"], record["close"])
    assert max(record["open"], record["close"]) <= record["high"]


@given(ohlcv())
def test_ohlcv_shape_details(record: dict[str, Any]) -> None:
    """INFRA-SELF-02 ohlcv() market_id 三段式、interval 合法、event_time≤ingest_timestamp"""
    parts = record["market_id"].split(":")
    assert len(parts) == 3 and all(parts)
    assert record["interval"] in INTERVALS
    assert record["schema_version"] == "1.0"
    event = datetime.fromisoformat(record["event_time"])
    ingest = datetime.fromisoformat(record["ingest_timestamp"])
    assert event <= ingest  # D02 §5 时间容差（保守满足）
    if record["source_timestamp"] is not None:
        datetime.fromisoformat(record["source_timestamp"])


@given(ohlcv_invalid())
def test_ohlcv_invalid_default_reason(record: dict[str, Any]) -> None:
    """INFRA-SELF-03 ohlcv_invalid() 默认生成携带 invalid_reason 且与实际违规一致"""
    assert set(record) == _OHLCV_REQUIRED_KEYS | {"invalid_reason"}
    reason = record["invalid_reason"]
    assert reason in INVALID_REASONS
    _assert_violation_matches(record, reason)


@pytest.mark.parametrize("reason", INVALID_REASONS)
@settings(max_examples=25)
@given(data=st.data())
def test_ohlcv_invalid_each_class(reason: str, data: st.DataObject) -> None:
    """INFRA-SELF-04 ohlcv_invalid() 四类违规每类至少一例且与 invalid_reason 一致"""
    record = data.draw(ohlcv_invalid(reason=reason), label="record")
    assert record["invalid_reason"] == reason
    _assert_violation_matches(record, reason)


@given(trade_seq())
@settings(max_examples=200)
def test_trade_seq_continuity(seq: list[dict[str, Any]]) -> None:
    """INFRA-SELF-05 trade_seq() 相邻条 prev.l+1==curr.a 且 a/l/n 语义自洽"""
    assert isinstance(seq, list) and len(seq) >= 1
    prev: dict[str, Any] | None = None
    for rec in seq:
        assert set(rec) == _TRADE_KEYS
        assert rec["a"] >= 1 and rec["n"] >= 1
        assert rec["l"] == rec["a"] + rec["n"] - 1  # n 恒等于 l-a+1（D04 §4.1）
        assert math.isfinite(rec["price"]) and rec["price"] > 0
        assert math.isfinite(rec["quantity"]) and rec["quantity"] > 0
        event = datetime.fromisoformat(rec["event_time"])
        if prev is not None:
            assert rec["a"] == prev["l"] + 1  # 连续对齐（Q-SEQ-001 对账依据）
            assert event > datetime.fromisoformat(prev["event_time"])
        prev = rec


@pytest.mark.parametrize("gap_after", [0, 2])
@settings(max_examples=50)
@given(data=st.data())
def test_trade_seq_gap_injection(gap_after: int, data: st.DataObject) -> None:
    """INFRA-SELF-06 trade_seq(gap_after=k) 在第 k/k+1 条间注入跳号，其余相邻仍连续"""
    seq = data.draw(trade_seq(gap_after=gap_after), label="trade_seq")
    assert len(seq) >= gap_after + 2
    for i in range(len(seq) - 1):
        if i == gap_after:
            assert seq[i + 1]["a"] >= seq[i]["l"] + 2  # 跳号处：curr.a > prev.l+1
        else:
            assert seq[i + 1]["a"] == seq[i]["l"] + 1  # 其余处保持连续对齐


@given(records())
def test_records_base_shape(record: dict[str, Any]) -> None:
    """INFRA-SELF-07 records() 键集合恰为 BaseRecord 基座字段（D02 §1）"""
    assert set(record) == set(BASE_RECORD_KEYS)
    assert record["schema_version"] == "1.0"
    assert record["quality_status"] in QUALITY_STATUSES
    ingest = datetime.fromisoformat(record["ingest_timestamp"])
    if record["source_timestamp"] is not None:
        assert datetime.fromisoformat(record["source_timestamp"]) <= ingest
    rid_parts = record["raw_record_id"].split(":")  # {source}:{dataset}:{file}:{line}
    assert len(rid_parts) == 4 and all(rid_parts)
    assert record["source"] and record["source_id"]


def test_conftest_env_isolation(tmp_stores: Any) -> None:
    """INFRA-SELF-08 conftest autouse 注入 test 环境 env 且与 tmp_stores 目录一致"""
    # env 变量名直书字面量：独立复核 D08 §1 契约，避免与 conftest 常量同源（套套逻辑）
    assert os.environ["CHRONOFORGE_ENV"] == "test"
    assert os.environ["CHRONOFORGE_DATA_DIR"] == str(tmp_stores.data_dir)
    assert os.environ["CHRONOFORGE_META_DIR"] == str(tmp_stores.meta_dir)
    assert tmp_stores.data_dir.is_dir()
    assert tmp_stores.meta_dir.is_dir()


# ── TC-PROP-001: dedup 幂等律（D09 §3）─────────────────────────────────


def _table_from_records(records: list[dict[str, Any]]):
    """将 dict 列表转为 PyArrow Table（列转置）。"""
    import pyarrow as pa

    cols: dict[str, list[Any]] = {}
    for rec in records:
        for k, v in rec.items():
            cols.setdefault(k, []).append(v)
    return pa.table(cols)


@settings(max_examples=50, deadline=None)
@given(st.lists(ohlcv(), min_size=1, max_size=30))
def test_dedup_idempotent_law(records: list[dict[str, Any]]) -> None:
    """TC-PROP-001 dedup 幂等律：dedup(dedup(X)) == dedup(X)（任意记录序列）"""
    from chronoforge.storage.canonical import CanonicalStoreImpl

    # _dedup_by_columns 为无状态纯表操作，__new__ 跳过 __post_init__
    # （无需 data_dir；hypothesis given 测试不接受 pytest fixture 参数）
    store = CanonicalStoreImpl.__new__(CanonicalStoreImpl)
    table = _table_from_records(records)
    for nk_cols in (
        ["market_id", "event_time", "interval"],
        ["market_id", "event_time"],
    ):
        once = store._dedup_by_columns(table, nk_cols)
        twice = store._dedup_by_columns(once, nk_cols)
        assert twice.equals(once, check_metadata=False)
        # 行数不超过输入（去重只减不增）
        assert once.num_rows <= table.num_rows
