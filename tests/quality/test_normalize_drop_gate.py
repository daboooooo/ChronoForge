"""quality 层：行级静默丢弃门禁（审计 A-8）。

对照 C1（批次异常走失败单元数）：本组用例验证连接器**逐行跳过**也会被
结构化计数并拦截——非空 payload 但产出为 0 或丢弃率过高时必须 QualityError，
checkpoint 不推进；合法跳过（未收盘 K 线等）不得误报。

统计口径：连接器经 ``note_skip(reason)`` 上报；NormalizeStage 以
``返回行 + 跳过行 + 未上报行`` 为分母、``非白名单跳过 + 未上报行`` 为分子。
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from chronoforge.connectors.normalize_stats import LEGIT_SKIP_REASONS
from chronoforge.exceptions import QualityError
from chronoforge.timeutil import to_epoch_milliseconds
from real_pipeline_harness import (
    FULL_CURSOR,
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


def test_all_rows_malformed_blocks_checkpoint(tmp_stores, monkeypatch) -> None:
    """非空 payload、全部行畸形 → 丢弃率 2/2 → QualityError。"""
    payload = _bad_numeric_rows(load_fixture())
    handler, _requests = make_handler([payload])
    h = build_harness(tmp_stores, monkeypatch, "a8_all_malformed", handler)

    with pytest.raises(QualityError, match=r"normalize drop rate 2/2"):
        h.runner.run(make_job())

    logged = latest_run_log_row(h.meta)
    assert logged["status"] == "FAILED"
    assert logged["checkpoint_after"] is None
    assert "normalize drop rate 2/2" in str(logged["error_summary"])

    # 门禁在 canonical 前 → 无 parquet、cursor 不推进
    assert canonical_rows(h.data_dir) == []
    assert get_cursor(h.meta) is None

    # raw 先落盘且完整保留，可重放定位
    _raw_file, lines = raw_lines(h.data_dir)
    assert len(decoded_raw_payloads(h.data_dir)[0]) == 2
    assert lines[0]["ingest_batch_id"] == logged["ingest_batch_id"]


def test_drop_rate_above_threshold_blocks_checkpoint(
    tmp_stores, monkeypatch
) -> None:
    """非空 payload、丢弃率 2/3（≥0.5）→ QualityError。"""
    fixture = load_fixture()
    payload = [fixture[0], *_bad_numeric_rows([fixture[1], fixture[1]])]
    handler, _requests = make_handler([payload])
    h = build_harness(tmp_stores, monkeypatch, "a8_high_drop", handler)

    with pytest.raises(QualityError, match=r"normalize drop rate 2/3"):
        h.runner.run(make_job())

    logged = latest_run_log_row(h.meta)
    assert logged["status"] == "FAILED"
    assert logged["checkpoint_after"] is None
    assert canonical_rows(h.data_dir) == []
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
    })
