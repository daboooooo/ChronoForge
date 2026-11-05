"""e2e 层：真实 Binance Spot 连接器 → 七阶段 → 三层落盘。

网络由 httpx.MockTransport 回放 tests/fixtures 录制响应，其余全部为生产
代码（fetch → normalize → canonical → quality → runlog）。断言具体到
raw 行数/内容、canonical 行数/字段值/分区路径、checkpoint 与 run_log 计数。

真实源冒烟（smoke）仍为手动触发，不在默认套件内。
"""

from __future__ import annotations

from datetime import datetime

import pytest

from chronoforge.timeutil import to_epoch_milliseconds
from real_pipeline_harness import (
    DS_ID,
    FULL_CURSOR,
    JOB_END,
    MARKET_ID,
    NEXT_CURSOR,
    NEXT_JOB_END,
    SOURCE_ID,
    build_harness,
    canonical_files,
    canonical_rows,
    decoded_raw_payloads,
    get_cursor,
    load_fixture,
    make_handler,
    make_job,
    raw_lines,
    run_log_row,
)


def test_fixture_source_fetches_normalizes_and_publishes(
    tmp_stores, monkeypatch
) -> None:
    payload = load_fixture()
    handler, requests = make_handler([payload])
    h = build_harness(tmp_stores, monkeypatch, "e2e_full", handler)

    row = h.runner.run(make_job())

    # fetch：回退一根 K 线 + 分页推进到 fixture 末根 closeTime+1ms
    expected_start = str(
        to_epoch_milliseconds(datetime(2017, 7, 2, 22, 59))
    )
    assert len(requests) == 2
    assert str(requests[0].url.params["startTime"]) == expected_start
    assert str(requests[0].url.params["endTime"]) == str(
        to_epoch_milliseconds(JOB_END)
    )
    assert str(requests[1].url.params["startTime"]) == str(payload[-1][6] + 1)

    # run 终态：单 chunk 成功，计数覆盖 fetch/raw/validate(1) + 行级 stage(2×3)
    assert row.status == "SUCCESS"
    assert row.checkpoint_before is None
    assert row.checkpoint_after == FULL_CURSOR
    assert (row.chunk_success, row.chunk_failed) == (1, 0)
    assert (row.input_count, row.output_count) == (9, 9)

    # raw 层：一个批次一行 JSONL，payload 原样保留
    raw_file, lines = raw_lines(h.data_dir)
    assert len(lines) == 1
    assert lines[0]["ingest_batch_id"] == row.ingest_batch_id
    assert lines[0]["url"] == "/api/v3/klines"
    raw_payload = decoded_raw_payloads(h.data_dir)[0]
    assert len(raw_payload) == 2
    assert raw_payload[0][:6] == payload[0][:6]

    # canonical 层：年/月分区落盘，2 行且关键字段精确匹配
    files = canonical_files(h.data_dir)
    assert len(files) == 1
    assert "year=2017" in str(files[0])
    assert "month=07" in str(files[0])
    rows = canonical_rows(h.data_dir)
    assert len(rows) == 2
    assert [r["event_time"] for r in rows] == [
        datetime(2017, 7, 3, 0, 0),
        datetime(2017, 7, 3, 0, 1),
    ]
    assert [r["market_id"] for r in rows] == [MARKET_ID, MARKET_ID]
    assert [r["interval"] for r in rows] == ["1m", "1m"]
    assert rows[0]["open"] == pytest.approx(0.01634790)
    assert rows[0]["high"] == pytest.approx(0.80000000)
    assert rows[0]["low"] == pytest.approx(0.01575800)
    assert rows[0]["close"] == pytest.approx(0.01577100)
    assert rows[0]["volume"] == pytest.approx(148976.11427815)
    assert rows[1]["close"] == pytest.approx(0.01544400)
    assert rows[1]["volume"] == pytest.approx(230898.31512848)
    expected_raw_id = f"{SOURCE_ID}:{DS_ID}:{raw_file.name}:1"
    assert rows[0]["raw_record_id"] == expected_raw_id
    assert rows[1]["raw_record_id"] == expected_raw_id

    # checkpoint + run_log：与 RunRow 一致
    assert get_cursor(h.meta) == FULL_CURSOR
    logged = run_log_row(h.meta, row.run_id)
    assert logged["status"] == "SUCCESS"
    assert (logged["input_count"], logged["output_count"]) == (9, 9)
    assert logged["checkpoint_after"] == FULL_CURSOR
    assert logged["ingest_batch_id"] == row.ingest_batch_id


def test_incremental_run_starts_from_checkpoint_and_upserts(
    tmp_stores, monkeypatch
) -> None:
    payload = load_fixture()
    # 01:00 的新 K 线（openTime=1499043600000，closeTime=1499043660000）
    new_bar = [
        1499043600000,
        "0.01577100",
        "0.01600000",
        "0.01570000",
        "0.01590000",
        "1000.0",
        1499043660000,
        "0",
        1,
        "0",
        "0",
        "0",
    ]
    handler, requests = make_handler([payload, [], [new_bar]])
    h = build_harness(tmp_stores, monkeypatch, "e2e_incremental", handler)

    first = h.runner.run(make_job())
    assert first.status == "SUCCESS"
    assert first.checkpoint_after == FULL_CURSOR

    second = h.runner.run(
        make_job(mode="incremental", start=None, end=NEXT_JOB_END)
    )

    # 增量窗口 = checkpoint - overlap(3600s)，connector 再回退一根 1m K 线
    expected_start = str(
        to_epoch_milliseconds(datetime(2017, 7, 2, 23, 59))
    )
    assert len(requests) == 4
    assert str(requests[2].url.params["startTime"]) == expected_start
    assert str(requests[3].url.params["startTime"]) == str(new_bar[6] + 1)

    assert second.status == "SUCCESS"
    assert second.checkpoint_before == FULL_CURSOR
    assert second.checkpoint_after == NEXT_CURSOR
    assert get_cursor(h.meta) == NEXT_CURSOR

    # 旧分区被 merge-rewrite 保留，新 K 线追加 → 终态 3 行
    rows = canonical_rows(h.data_dir)
    assert len(rows) == 3
    assert rows[0]["open"] == pytest.approx(0.01634790)
    assert rows[2]["event_time"] == datetime(2017, 7, 3, 1, 0)
    assert rows[2]["open"] == pytest.approx(0.01577100)
    assert rows[2]["close"] == pytest.approx(0.01590000)
    assert rows[2]["volume"] == pytest.approx(1000.0)

    # raw 追加第二行，两个批次各自保留 ingest_batch_id
    _raw_file, lines = raw_lines(h.data_dir)
    assert len(lines) == 2
    assert lines[0]["ingest_batch_id"] == first.ingest_batch_id
    assert lines[1]["ingest_batch_id"] == second.ingest_batch_id
    assert len(decoded_raw_payloads(h.data_dir)[1]) == 1
