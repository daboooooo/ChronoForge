"""DATA-SOURCE nyfed 集成测试 — NYFedConnector SRF 口径与 checkpoint 契约。

覆盖：
- 真实 SRF + SVE 同日：只汇总真实隔夜操作，SVE 不计入
- 纯 SVE 日：不产出记录（而不是产出 0 或非零值）
- Term 类 SVE：即使 note 标记为 SVE 也不计入 SRF
- SVE note 的 HTML 包裹与大小写变体：按不区分大小写的子串识别
- note_skip：合法跳过（SVE / Term）与非法跳过（畸形行）分别上报
- checkpoint_from：与 normalize 实际产出对齐，零产出返回 None
- fixture 全链路：fetch（httpx.MockTransport）→ normalize

SVE 识别依据：NY Fed Markets API `rp/results/search.json` 返回的
`repo.operations[].note` 字段，真实响应中该字段为
"Small Value Exercise (SVE)" / "early settling overnight repo small value
exercise (SVE)" / 带 `<p>` 包裹的变体。`operationId` 尾号不具辨识度
（实测尾号 1/2/4/25/26/28/29/99 均有 SVE），因此以 note 为准。

fixture 形状来源与核对日期见 `tests/fixtures/nyfed/SOURCE.md`。
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx

from chronoforge.connectors.base import FetchRequest, RawBatch
from chronoforge.connectors.normalize_stats import begin_normalize, end_normalize
from chronoforge.connectors.nyfed import _BASE_URL, _ENDPOINT, _SOMA_ENDPOINT, NYFedConnector
from chronoforge.models.macro import NUMBER

_FIXTURE_DIR = Path(__file__).parent.parent / "fixtures" / "nyfed"


def _load(relative: str) -> dict[str, Any]:
    with open(_FIXTURE_DIR / relative, encoding="utf-8") as f:
        payload: dict[str, Any] = json.load(f)
    return payload


def _batch(operations: list[dict[str, object]]) -> RawBatch:
    return RawBatch(
        endpoint=_ENDPOINT,
        payload={"repo": {"operations": operations}},
        raw_meta={"http_status": 200, "series_id": "srf_usage"},
    )


def _srf_operation(
    operation_id: str,
    *,
    operation_date: str = "2025-11-05",
    term: str = "Overnight",
    amount: float | str | None,
    note: str = "",
) -> dict[str, object]:
    row: dict[str, object] = {
        "operationId": operation_id,
        "operationDate": operation_date,
        "term": term,
        "note": note,
    }
    if amount is not None:
        row["totalAmtAccepted"] = amount
    return row


def _normalize(rows: list[dict[str, object]]) -> list[Any]:
    # 不构造 HTTP 客户端（normalize 不依赖网络）
    return NYFedConnector.__new__(NYFedConnector).normalize(_batch(rows))


def _mock_connector(payload: dict[str, Any]) -> NYFedConnector:
    connector = NYFedConnector()
    connector._client = httpx.Client(  # noqa: SLF001 — 测试替换 HTTP 层
        base_url=_BASE_URL,
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=payload)),
    )
    return connector


def _request(params: dict[str, Any], start: datetime, end: datetime) -> FetchRequest:
    return FetchRequest(
        dataset_id="nyfed_repo_srf_usage",
        params=params,
        start=start,
        end=end,
        cursor=None,
    )


# ── SRF 口径 ───────────────────────────────────────────────────────────


def test_sve_same_day_excluded_from_srf_total() -> None:
    """真实 3M + SVE 99M 同日 → 只计 3M。"""
    records = _normalize(
        [
            _srf_operation("RP 110525 3", amount=3_000_000),
            _srf_operation(
                "RP 110525 25",
                amount=99_000_000,
                note="<p>This operation is a Small Value Exercise (SVE); "
                "the daily SRF operation will still take place at its "
                "regularly scheduled time.</p>",
            ),
        ]
    )
    assert len(records) == 1
    number = records[0]
    assert isinstance(number, NUMBER)
    assert number.value == 3_000_000


def test_pure_sve_day_yields_no_record() -> None:
    """纯 SVE 日不产出记录（不是 0，也不是 SVE 金额）。"""
    records = _normalize(
        [
            _srf_operation(
                "RP 041122 1",
                operation_date="2022-04-11",
                amount=61_000_000,
                note="This operation is a Small Value Exercise.",
            )
        ]
    )
    assert records == []


def test_term_sve_not_counted() -> None:
    """Term 类 SVE 即使被 note 标记，也不属于 SRF 隔夜口径。"""
    records = _normalize(
        [
            _srf_operation(
                "RP 102026 29",
                operation_date="2026-10-20",
                term="Term",
                amount=67_000_000,
                note="This operation is a Small Value Exercise (SVE).",
            )
        ]
    )
    assert records == []


def test_sve_note_variants_case_insensitive() -> None:
    """SVE note 的 HTML 包裹、大小写与 "early settling" 变体都能识别。"""
    records = _normalize(
        [
            _srf_operation(
                "RP 030525 4",
                amount=84_000_000,
                note="This operation is an early settling overnight "
                "repo small value exercise (SVE); the daily SRF "
                "operation will still take place.",
            ),
            _srf_operation(
                "RP 082125 2",
                amount=61_000_000,
                note="This operation is a Small Value Exercise.",
            ),
        ]
    )
    assert records == []


# ── note_skip 上报 ────────────────────────────────────────────────────


def test_normalize_reports_note_skip_reasons() -> None:
    """合法（SVE / Term）与非法（畸形行）跳过分别结构化上报。"""
    token = begin_normalize()
    records = _normalize(
        [
            _srf_operation("RP 110525 3", amount=3_000_000),
            _srf_operation("RP 110525 25", amount=99_000_000,
                           note="This operation is a Small Value Exercise (SVE)."),
            _srf_operation("RP 110525 4", term="Term", amount=50_000_000),
            _srf_operation("SYNTH-BAD-AMOUNT", amount="not-a-number"),
            _srf_operation("SYNTH-BAD-DATE", operation_date="", amount=1_000_000),
            _srf_operation("SYNTH-MISSING-AMOUNT", amount=None),
        ]
    )
    skips = end_normalize(token)

    assert len(records) == 1
    assert skips == {
        "sve_test_operation": 1,
        "non_overnight_operation": 1,
        "non_numeric_amount": 1,
        "bad_operation_date": 1,
        "missing_amount": 1,
    }


def test_normalize_reports_malformed_row() -> None:
    """非对象行按 malformed_row 上报（不是静默跳过）。"""
    token = begin_normalize()
    rows: list[Any] = ["not-an-object"]
    raw = RawBatch(
        endpoint=_ENDPOINT,
        payload={"repo": {"operations": rows}},
        raw_meta={"http_status": 200},
    )
    assert NYFedConnector.__new__(NYFedConnector).normalize(raw) == []
    assert end_normalize(token) == {"malformed_row": 1}


# ── checkpoint_from 契约 ─────────────────────────────────────────────


def test_checkpoint_from_ignores_non_producing_rows() -> None:
    """纯 SVE / Term 行不产记录 → checkpoint_from 返回 None。"""
    connector = NYFedConnector.__new__(NYFedConnector)
    raw = _batch(
        [
            _srf_operation("RP 041122 1", operation_date="2022-04-11",
                           amount=61_000_000,
                           note="This operation is a Small Value Exercise."),
            _srf_operation("RP 102026 29", operation_date="2026-10-20",
                           term="Term", amount=67_000_000),
        ]
    )
    assert connector.checkpoint_from(raw) is None


def test_checkpoint_from_uses_last_produced_date() -> None:
    """SVE 行晚于真实行时，cursor 仍停在真实行的日期。"""
    connector = NYFedConnector.__new__(NYFedConnector)
    raw = _batch(
        [
            _srf_operation("RP 110525 3", operation_date="2025-11-05", amount=3_000_000),
            _srf_operation("RP 110625 25", operation_date="2025-11-06", amount=99_000_000,
                           note="This operation is a Small Value Exercise (SVE)."),
        ]
    )
    assert connector.checkpoint_from(raw) == "2025-11-05"


def test_checkpoint_from_records_matches_observation() -> None:
    """records 路径取 observation_time 最大日期。"""
    connector = NYFedConnector.__new__(NYFedConnector)
    records = _normalize(
        [_srf_operation("RP 110525 3", operation_date="2025-11-05", amount=3_000_000)]
    )
    assert connector.checkpoint_from(records) == "2025-11-05"


def test_soma_checkpoint_from_skips_rows_without_values() -> None:
    """SOMA 行有日期但 9 个分项全缺 → 无产出，checkpoint 不推进。"""
    connector = NYFedConnector.__new__(NYFedConnector)
    raw = RawBatch(
        endpoint=_SOMA_ENDPOINT,
        payload={"soma": {"summary": [{"asOfDate": "2025-11-05"}]}},
        raw_meta={"http_status": 200},
    )
    assert connector.normalize(raw) == []
    assert connector.checkpoint_from(raw) is None


# ── 发布时刻（PIT）────────────────────────────────────────────────────


def _freeze_now(monkeypatch: Any, now: datetime) -> None:
    """冻结连接器墙钟（22:00 UTC 发布判定依赖它）。"""
    monkeypatch.setattr("chronoforge.connectors.nyfed._utc_now", lambda: now)


def test_srf_before_release_skips_unpublished_day(monkeypatch: Any) -> None:
    """操作日 15:23 UTC（13:30 ET 已执行、尚未公布）→ 当日不产出记录。

    修复前：占位 0 与真实金额都被写成 release_time=22:00 的可发布值，
    22:00 一到即被视为已发布，污染 PIT（H-N2）。
    """
    _freeze_now(monkeypatch, datetime(2025, 11, 5, 15, 23))
    token = begin_normalize()
    records = _normalize(
        [
            # 当日：占位 0 + 真实 3M（均未公布）
            _srf_operation("RP 110525 3", operation_date="2025-11-05", amount=3_000_000),
            _srf_operation("RP 110525 0", operation_date="2025-11-05", amount=0),
            # 前一日：已过发布时刻
            _srf_operation("RP 110425 3", operation_date="2025-11-04", amount=5_000_000),
        ]
    )
    skips = end_normalize(token)

    assert [r.observation_time for r in records] == [datetime(2025, 11, 4)]
    assert records[0].release_time == datetime(2025, 11, 4, 22, 0)
    assert skips["not_yet_published"] == 2


def test_srf_before_release_checkpoint_stops_at_previous_day(monkeypatch: Any) -> None:
    """checkpoint_from 不得推进到尚未公布的日期（否则 cursor 越过定稿值）。"""
    _freeze_now(monkeypatch, datetime(2025, 11, 5, 15, 23))
    connector = NYFedConnector.__new__(NYFedConnector)
    raw = _batch(
        [
            _srf_operation("RP 110525 3", operation_date="2025-11-05", amount=3_000_000),
            _srf_operation("RP 110425 3", operation_date="2025-11-04", amount=5_000_000),
        ]
    )
    assert connector.checkpoint_from(raw) == "2025-11-04"


def test_srf_after_release_publishes_same_day(monkeypatch: Any) -> None:
    """22:00 之后：当日定稿值可见，release_time 固定为 22:00（重拉幂等）。"""
    _freeze_now(monkeypatch, datetime(2025, 11, 5, 22, 30))
    token = begin_normalize()
    records = _normalize(
        [_srf_operation("RP 110525 3", operation_date="2025-11-05", amount=3_000_000)]
    )
    skips = end_normalize(token)
    connector = NYFedConnector.__new__(NYFedConnector)
    raw = _batch(
        [_srf_operation("RP 110525 3", operation_date="2025-11-05", amount=3_000_000)]
    )

    assert len(records) == 1
    assert records[0].release_time == datetime(2025, 11, 5, 22, 0)
    assert records[0].revision_time == datetime(2025, 11, 5, 22, 0)
    assert skips == {}
    assert connector.checkpoint_from(raw) == "2025-11-05"


def test_srf_placeholder_zero_is_never_published(monkeypatch: Any) -> None:
    """纯占位 0 的当日（尚未公布）不得产出 0 记录（否则写死"零用量"）。"""
    _freeze_now(monkeypatch, datetime(2025, 11, 5, 13, 30))
    assert _normalize(
        [_srf_operation("RP 110525 0", operation_date="2025-11-05", amount=0)]
    ) == []
    assert NYFedConnector.__new__(NYFedConnector).checkpoint_from(
        _batch([_srf_operation("RP 110525 0", operation_date="2025-11-05", amount=0)])
    ) is None


# ── fetch → normalize 全链路（真实 fixture 形状） ────────────────────


def test_fetch_to_normalize_srf_real_fixture() -> None:
    """真实 2025-11-05 响应形状：3 笔 Overnight（0 / SVE 99M / 真实 3M）→ 3M。"""
    payload = _load("repo_results/happy.json")
    connector = _mock_connector(payload)
    try:
        batches = list(
            connector.fetch(
                _request(
                    {"endpoint": "rp_results", "series": "srf_usage"},
                    datetime(2021, 7, 29),
                    datetime(2025, 11, 6),
                )
            )
        )
        assert len(batches) == 1
        assert batches[0].payload == payload
        records = connector.normalize(batches[0])
    finally:
        connector.close()

    assert len(records) == 1
    assert isinstance(records[0], NUMBER)
    assert records[0].value == 3_000_000
    assert records[0].observation_time == datetime(2025, 11, 5)


def test_fetch_to_normalize_soma_real_fixture() -> None:
    """真实 SOMA 快照（字符串数值）→ 2 行 × 9 分项，窗口外被裁剪。"""
    payload = _load("soma_summary/happy.json")
    connector = _mock_connector(payload)
    try:
        batches = list(
            connector.fetch(
                _request(
                    {"endpoint": "soma_summary", "series": "soma_holdings"},
                    datetime(2025, 11, 1),
                    datetime(2026, 11, 5),
                )
            )
        )
        assert len(batches) == 1
        assert len(batches[0].payload["soma"]["summary"]) == 2
        records = connector.normalize(batches[0])
        assert [r.observation_time for r in records[:9]][0] == datetime(2025, 11, 5)
    finally:
        connector.close()

    assert len(records) == 18
    bills = next(
        r for r in records
        if r.source_id == "soma_bills" and r.observation_time.year == 2025
    )
    assert bills.value == 195_492_926_700
