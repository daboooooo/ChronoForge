"""quality 层：验证质量门禁真的能拦截，而非只跑通链路。

两个门禁路径：
1) normalize 全失败（非空 payload → 0 条产出）→ 错误率超阈值 → QualityError，
   run FAILED、checkpoint 不推进、canonical 不落盘；
2) 批次内 natural key 重复命中 block_on 规则 → QualityError，checkpoint 不推进。

均使用真实 BinanceSpotConnector + 真实三层存储，仅 HTTP 由 MockTransport 替换。
"""

from __future__ import annotations

import pytest

from chronoforge.exceptions import QualityError
from real_pipeline_harness import (
    DS_ID,
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


def _invalid_ohlcv_payload(payload: list[list[object]]) -> list[list[object]]:
    """把 fixture 行改成 high < open 的模型非法行（3 行全部触发 ValidationError）。

    connector 的逐行 try 只覆盖 float 转换/有限性，OHLCV 模型约束违规会
    冒泡为批次异常，从而走 NormalizeStage 的错误率门禁。
    """
    invalid: list[list[object]] = []
    for row, high in zip(payload, ("0.01000000", "0.01500000"), strict=True):
        bad = list(row)
        bad[2] = high  # high 低于 open/close
        invalid.append(bad)
    # 第三行复用第一行（同 NK），仍是非法的 → 三个 payload 单元全部失败
    invalid.append(list(invalid[0]))
    return invalid


def test_normalize_all_failures_loses_nothing_and_blocks_checkpoint(
    tmp_stores, monkeypatch
) -> None:
    payload = _invalid_ohlcv_payload(load_fixture())
    handler, _requests = make_handler([payload])
    h = build_harness(tmp_stores, monkeypatch, "quality_normalize_gate", handler)

    with pytest.raises(QualityError, match=r"normalize error rate 3/3"):
        h.runner.run(make_job())

    logged = latest_run_log_row(h.meta)
    assert logged["status"] == "FAILED"
    assert logged["checkpoint_after"] is None
    assert "normalize error rate 3/3" in str(logged["error_summary"])

    # 门禁在 canonical 前：不应有 parquet 落盘，checkpoint 不推进
    assert canonical_rows(h.data_dir) == []
    assert get_cursor(h.meta) is None

    # raw 层先于 normalize 落盘（可重放），原始 payload 完整保留 3 行
    _raw_file, lines = raw_lines(h.data_dir)
    assert len(lines) == 1
    assert len(decoded_raw_payloads(h.data_dir)[0]) == 3


def test_duplicate_natural_keys_block_quality_gate(
    tmp_stores, monkeypatch
) -> None:
    payload = load_fixture()
    duplicated = [payload[0], payload[1], payload[0]]  # 第 1/3 行同 natural key
    handler, _requests = make_handler([duplicated])
    h = build_harness(
        tmp_stores,
        monkeypatch,
        "quality_block_on_gate",
        handler,
        quality_block_on=["Q-DUP-001"],
    )

    with pytest.raises(QualityError, match="Block on rule Q-DUP-001"):
        h.runner.run(make_job())

    logged = latest_run_log_row(h.meta)
    assert logged["status"] == "FAILED"
    assert logged["checkpoint_after"] is None
    assert "Block on rule Q-DUP-001" in str(logged["error_summary"])

    # 质量门禁在 canonical 之后：去重后的 2 行已落盘，但 cursor 不推进
    assert len(canonical_rows(h.data_dir)) == 2
    assert get_cursor(h.meta) is None

    # 阻断路径不落 quality_flags（由 RunLogStage 统一落盘，未执行到）
    cur = h.meta.connection.execute(
        "SELECT COUNT(*) FROM quality_flags WHERE dataset_id = ?", (DS_ID,)
    )
    assert cur.fetchone()[0] == 0

    # raw 保留 3 行原始批次，便于失败后重放定位
    _raw_file, lines = raw_lines(h.data_dir)
    assert len(lines) == 1
    assert len(decoded_raw_payloads(h.data_dir)[0]) == 3
    assert lines[0]["ingest_batch_id"] == logged["ingest_batch_id"]
