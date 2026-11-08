"""quality 层：行级静默丢弃门禁（审计 A-8）。

对照 C1（批次异常走失败单元数）：本组用例验证连接器**逐行跳过**也会被
结构化计数并拦截——非空 payload 但产出为 0 或丢弃率过高时必须 QualityError，
checkpoint 不推进；合法跳过（未收盘 K 线等）不得误报。

统计口径：连接器经 ``note_skip(reason)`` 上报；NormalizeStage 以
``返回行 + 跳过行 + 未上报行`` 为分母。分子按来源拆分：
- 未上报丢失（静默）→ 始终按丢弃率判罚，不受小样本下限豁免；
- 已上报的非白名单跳过（源侧坏行）→ 样本量 ≥ min_units 时按丢弃率判罚，
  低于 min_units 时降级为「源侧异常」：不判失败，但计数 + 质量 flag 留痕。
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from chronoforge.connectors.normalize_stats import LEGIT_SKIP_REASONS
from chronoforge.exceptions import QualityError
from chronoforge.timeutil import to_epoch_milliseconds
from real_pipeline_harness import (
    DS_ID,
    FULL_CURSOR,
    JOB_START,
    build_harness,
    canonical_rows,
    decoded_raw_payloads,
    get_cursor,
    latest_run_log_row,
    load_fixture,
    make_handler,
    make_job,
    raw_lines,
)

pytestmark = pytest.mark.quality


def _bad_numeric_rows(payload: list[list[object]]) -> list[list[object]]:
    """把 fixture 行改成非数值 open（逐行 float 失败 → note_skip 跳过）。"""
    bad: list[list[object]] = []
    for row in payload:
        broken = list(row)
        broken[1] = "bad"
        bad.append(broken)
    return bad


def _unclosed_row(open_time: datetime) -> list[object]:
    """构造未收盘 K 线（closeTime 在未来 → normalize 合法跳过）。"""
    close_time = open_time + timedelta(minutes=1)
    return [
        to_epoch_milliseconds(open_time),
        "0.01500000",
        "0.01600000",
        "0.01400000",
        "0.01550000",
        "100.0",
        to_epoch_milliseconds(close_time),
        "0",
        1,
        "0",
        "0",
        "0",
    ]


def _bar_row(open_time: datetime, *, bad: bool = False) -> list[object]:
    """构造一根已收盘 1m K 线；bad=True 时 open 非数值。"""
    close_time = open_time + timedelta(minutes=1)
    return [
        to_epoch_milliseconds(open_time),
        "bad" if bad else "0.01500000",
        "0.01600000",
        "0.01400000",
        "0.01550000",
        "100.0",
        to_epoch_milliseconds(close_time),
        "0",
        1,
        "0",
        "0",
        "0",
    ]


def test_below_min_units_malformed_is_source_anomaly(
    tmp_stores, monkeypatch
) -> None:
    """小样本（2 行 < min_units=10）全畸形 → 不按丢弃率判罚，记为源侧异常。"""
    payload = _bad_numeric_rows(load_fixture())
    handler, _requests = make_handler([payload])
    h = build_harness(tmp_stores, monkeypatch, "a8_below_min_anomaly", handler)

    row = h.runner.run(make_job())
    # 不判失败（不抛 QualityError），但因存在源侧异常留痕 → PARTIAL_SUCCESS
    assert row.status == "PARTIAL_SUCCESS"
    assert row.checkpoint_after == FULL_CURSOR
    # 门禁不再拦截：canonical 无行（全部源侧异常），但 cursor 照常推进
    assert canonical_rows(h.data_dir) == []
    assert get_cursor(h.meta) == FULL_CURSOR

    logged = latest_run_log_row(h.meta)
    assert logged["status"] == "PARTIAL_SUCCESS"
    assert logged["warning_count"] == 2
    assert "normalize source anomaly: 2/2" in str(logged["error_summary"])
    assert "min_units=10" in str(logged["error_summary"])
    # Binance 连接器对非数值 open 的上报原因是 non_numeric_value
    assert "non_numeric_value" in str(logged["error_summary"])

    # 源侧异常必须落 quality_flags，供报告侧作为缺口读取
    flags = h.meta.connection.execute(
        "SELECT rule_id, severity, detail FROM quality_flags WHERE dataset_id = ?",
        (DS_ID,),
    ).fetchall()
    assert any(
        rule == "Q-NORM-SRC-ANOMALY" and sev == "WARNING"
        for rule, sev, _detail in flags
    )

    # raw 先落盘且完整保留，可重放定位
    _raw_file, lines = raw_lines(h.data_dir)
    assert len(decoded_raw_payloads(h.data_dir)[0]) == 2
    assert lines[0]["ingest_batch_id"] == logged["ingest_batch_id"]


def test_drop_rate_at_min_units_still_blocks(tmp_stores, monkeypatch) -> None:
    """样本量达到下限（10 行）且丢弃率 6/10（≥0.5）→ 仍 QualityError。"""
    payload = [
        _bar_row(JOB_START + timedelta(minutes=i), bad=(i < 6)) for i in range(10)
    ]
    handler, _requests = make_handler([payload])
    h = build_harness(tmp_stores, monkeypatch, "a8_at_min_drop", handler)

    with pytest.raises(QualityError, match=r"normalize drop rate 6/10"):
        h.runner.run(make_job())

    logged = latest_run_log_row(h.meta)
    assert logged["status"] == "FAILED"
    assert logged["checkpoint_after"] is None
    assert canonical_rows(h.data_dir) == []
    assert get_cursor(h.meta) is None


def test_small_sample_silent_loss_still_blocks(tmp_stores, monkeypatch) -> None:
    """小样本静默丢失不走下限豁免：A-8 守的正是"悄悄丢行且返回成功"。"""
    handler, _requests = make_handler([load_fixture()])
    h = build_harness(tmp_stores, monkeypatch, "a8_small_silent", handler)

    # 注入坏实现：有输入、normalize 返回 []、不上报 note_skip
    monkeypatch.setattr(h.connector, "normalize", lambda _raw: [])
    monkeypatch.setattr(h.connector, "row_units", lambda _raw: 1)

    with pytest.raises(QualityError, match=r"normalize drop rate 1/1"):
        h.runner.run(make_job())

    logged = latest_run_log_row(h.meta)
    assert logged["status"] == "FAILED"
    assert logged["checkpoint_after"] is None
    assert get_cursor(h.meta) is None


def test_single_unclosed_bar_is_legit_skip(tmp_stores, monkeypatch) -> None:
    """尾根未收盘 K 线（增量常态）→ 成功，不得误报。"""
    payload = load_fixture()
    now = datetime.now(UTC).replace(tzinfo=None)
    unclosed = _unclosed_row(now)
    handler, _requests = make_handler([[*payload, unclosed]])
    h = build_harness(tmp_stores, monkeypatch, "a8_unclosed", handler)

    row = h.runner.run(make_job())
    assert row.status == "SUCCESS"
    assert row.checkpoint_after == FULL_CURSOR

    rows = canonical_rows(h.data_dir)
    assert len(rows) == 2
    assert get_cursor(h.meta) == FULL_CURSOR


def test_high_ratio_legit_skips_do_not_block(tmp_stores, monkeypatch) -> None:
    """整批合法跳过（未收盘）→ 成功且 checkpoint 照常推进，不得误报。"""
    now = datetime.now(UTC).replace(tzinfo=None)
    payload = [
        _unclosed_row(now + timedelta(minutes=offset)) for offset in (1, 2, 3)
    ]
    handler, _requests = make_handler([payload])
    h = build_harness(tmp_stores, monkeypatch, "a8_legit_high_ratio", handler)

    row = h.runner.run(make_job())
    assert row.status == "SUCCESS"
    assert row.checkpoint_after == FULL_CURSOR

    # 合法跳过 → canonical 无行（既有语义），但 run 不被门禁拦截
    assert canonical_rows(h.data_dir) == []
    assert get_cursor(h.meta) == FULL_CURSOR


def test_empty_payload_is_not_reported(tmp_stores, monkeypatch) -> None:
    """空 payload（真无数据）→ 既有语义不变，不得误报。"""
    handler, _requests = make_handler([[]])
    h = build_harness(tmp_stores, monkeypatch, "a8_empty", handler)

    row = h.runner.run(make_job())
    assert row.status == "SUCCESS"
    assert row.checkpoint_after == FULL_CURSOR
    assert canonical_rows(h.data_dir) == []
    assert get_cursor(h.meta) == FULL_CURSOR


def test_legit_skip_reasons_whitelist_is_locked() -> None:
    """白名单必须锁定：新增合法原因需同时改测试与文档，不可静默扩大。"""
    assert LEGIT_SKIP_REASONS == frozenset({
        "unclosed_bar",
        "unfinished_kline",
        "unfrozen_today",
        "pending_settlement",
        "source_missing_value",
        "non_target_account",
        "duplicate_observation",
        "sve_test_operation",
        "non_overnight_operation",
        "not_yet_published",
    })
