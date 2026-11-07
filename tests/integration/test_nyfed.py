"""DATA-SOURCE nyfed 集成测试 — NYFedConnector SRF 口径。

覆盖：
- 真实 SRF + SVE 同日：只汇总真实隔夜操作，SVE 不计入
- 纯 SVE 日：不产出记录（而不是产出 0 或非零值）
- Term 类 SVE：即使 note 标记为 SVE 也不计入 SRF
- SVE note 的 HTML 包裹与大小写变体：按不区分大小写的子串识别

SVE 识别依据：NY Fed Markets API `rp/results/search.json` 返回的
`repo.operations[].note` 字段，真实响应中该字段为
"Small Value Exercise (SVE)" / "early settling overnight repo small value
exercise (SVE)" / 带 `<p>` 包裹的变体。`operationId` 尾号不具辨识度
（实测尾号 1/2/4/25/26/28/29/99 均有 SVE），因此以 note 为准。
"""

from __future__ import annotations

from chronoforge.connectors.base import RawBatch
from chronoforge.connectors.nyfed import _ENDPOINT, NYFedConnector
from chronoforge.models.macro import NUMBER


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
    amount: float,
    note: str = "",
) -> dict[str, object]:
    return {
        "operationId": operation_id,
        "operationDate": operation_date,
        "term": term,
        "totalAmtAccepted": amount,
        "note": note,
    }


def test_sve_same_day_excluded_from_srf_total() -> None:
    """真实 3M + SVE 99M 同日 → 只计 3M。"""
    connector = NYFedConnector.__new__(NYFedConnector)  # 不构造 HTTP 客户端
    records = connector.normalize(
        _batch(
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
    )
    assert len(records) == 1
    number = records[0]
    assert isinstance(number, NUMBER)
    assert number.value == 3_000_000


def test_pure_sve_day_yields_no_record() -> None:
    """纯 SVE 日不产出记录（不是 0，也不是 SVE 金额）。"""
    connector = NYFedConnector.__new__(NYFedConnector)
    records = connector.normalize(
        _batch(
            [
                _srf_operation(
                    "RP 041122 1",
                    operation_date="2022-04-11",
                    amount=61_000_000,
                    note="This operation is a Small Value Exercise.",
                )
            ]
        )
    )
    assert records == []


def test_term_sve_not_counted() -> None:
    """Term 类 SVE 即使被 note 标记，也不属于 SRF 隔夜口径。"""
    connector = NYFedConnector.__new__(NYFedConnector)
    records = connector.normalize(
        _batch(
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
    )
    assert records == []


def test_sve_note_variants_case_insensitive() -> None:
    """SVE note 的 HTML 包裹、大小写与 "early settling" 变体都能识别。"""
    connector = NYFedConnector.__new__(NYFedConnector)
    records = connector.normalize(
        _batch(
            [
                _srf_operation("RP 030525 4", amount=84_000_000,
                               note="This operation is an early settling overnight "
                               "repo small value exercise (SVE); the daily SRF "
                               "operation will still take place."),
                _srf_operation("RP 082125 2", amount=61_000_000,
                               note="This operation is a Small Value Exercise."),
            ]
        )
    )
    assert records == []
