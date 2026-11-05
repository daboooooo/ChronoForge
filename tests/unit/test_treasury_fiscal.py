"""Treasury Fiscal Data connector 单元测试（Phase 1）。"""

from __future__ import annotations

import json
from datetime import date, datetime
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from structlog.testing import capture_logs

from chronoforge.connectors.base import FetchRequest, RawBatch
from chronoforge.connectors.errors import SchemaError
from chronoforge.connectors.treasury_fiscal import TreasuryFiscalConnector
from chronoforge.models.treasury import (
    TREASURY_ACCOUNT_BALANCE,
    TREASURY_AUCTION,
    TREASURY_CASH_FLOW,
    TREASURY_DEBT_TRANSACTION,
)

_FIXTURES = "tests/fixtures/treasury_fiscal"
_DT = datetime(2026, 10, 31, 12, 0, 0)


def _connector() -> TreasuryFiscalConnector:
    return TreasuryFiscalConnector(settings=SimpleNamespace(http_timeout_s=30.0))  # type: ignore[arg-type]


def _fixture(name: str) -> dict[str, Any]:
    path = f"{_FIXTURES}/{name}/first_page.json"
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def _raw(table: str, payload: dict[str, Any], fetched_at: datetime = _DT) -> RawBatch:
    return RawBatch(
        endpoint="test",
        payload=payload,
        raw_meta={"table": table, "http_status": 200, "fetched_at": fetched_at},
    )


def test_balance_closing_uses_open_today_when_close_is_null() -> None:
    row = {
        "record_date": "2026-10-29",
        "account_type": "Treasury General Account (TGA) Closing Balance",
        "close_today_bal": "null",
        "open_today_bal": "1040236",
        "open_month_bal": "1040236",
        "src_line_nbr": "4",
        "table_nbr": "I",
    }
    records = _connector().normalize(_raw("operating_cash_balance", {"data": [row]}))
    assert len(records) == 1
    record = records[0]
    assert isinstance(record, TREASURY_ACCOUNT_BALANCE)
    assert record.balance_type == "CLOSING"
    assert record.amount_usd_millions == pytest.approx(1_040_236.0)
    assert record.release_time_source == "OBSERVED"


def test_balance_includes_historical_federal_reserve_account() -> None:
    payload = _fixture("dts_operating_cash_balance")
    records = _connector().normalize(_raw("operating_cash_balance", payload))
    assert all(isinstance(r, TREASURY_ACCOUNT_BALANCE) for r in records)
    assert all(r.is_tga for r in records)
    # 2005 年首个 fixture 行是 Federal Reserve Account，应作为历史 TGA 单行拆分
    historical = [r for r in records if r.observation_time.date().isoformat() == "2005-10-03"]
    assert historical
    assert any(r.account_type_raw == "Federal Reserve Account" for r in historical)
    # Tax and Loan Note Accounts 不得计入 TGA
    assert all("Tax and Loan" not in r.account_type_raw for r in records)


def test_balance_historical_single_row_splits_opening_and_closing() -> None:
    row = {
        "record_date": "2021-12-31",
        "account_type": "Treasury General Account (TGA)",
        "open_today_bal": "284174",
        "close_today_bal": "406108",
        "src_line_nbr": "1",
        "table_nbr": "I",
    }
    records = _connector().normalize(_raw("operating_cash_balance", {"data": [row]}))
    assert len(records) == 2
    by_type = {r.balance_type: r for r in records}
    assert set(by_type) == {"OPENING", "CLOSING"}
    assert by_type["OPENING"].amount_usd_millions == pytest.approx(284_174.0)
    assert by_type["CLOSING"].amount_usd_millions == pytest.approx(406_108.0)
    assert by_type["OPENING"].natural_key() != by_type["CLOSING"].natural_key()


def test_cash_flow_accepts_historical_federal_reserve_account() -> None:
    rows = [
        {
            "record_date": "2005-10-03",
            "account_type": "Federal Reserve Account",
            "transaction_type": "Deposits",
            "transaction_catg": "Taxes - Individual Income",
            "transaction_today_amt": "893",
            "src_line_nbr": "1",
            "table_nbr": "II",
        }
    ]
    records = _connector().normalize(_raw("deposits_withdrawals", {"data": rows}))
    assert len(records) == 1
    record = records[0]
    assert isinstance(record, TREASURY_CASH_FLOW)
    assert record.account_type_raw == "Federal Reserve Account"
    assert record.include_in_net is True
    assert record.is_subtotal is False
    assert record.amount_usd_millions == pytest.approx(893.0)


def test_cash_flow_only_tga_and_classifies() -> None:
    rows = [
        {
            "record_date": "2026-10-29",
            "account_type": "Treasury General Account (TGA)",
            "transaction_type": "Deposits",
            "transaction_catg": "Dept of Defense (DoD) - misc",
            "transaction_today_amt": "893",
            "src_line_nbr": "6",
            "table_nbr": "II",
        },
        {
            "record_date": "2026-10-29",
            "account_type": "Federal Reserve Account",
            "transaction_type": "Deposits",
            "transaction_catg": "Dept of Defense (DoD) - misc",
            "transaction_today_amt": "893",
            "src_line_nbr": "6",
            "table_nbr": "II",
        },
    ]
    records = _connector().normalize(_raw("deposits_withdrawals", {"data": rows}))
    assert len(records) == 2
    assert {r.account_type_raw for r in records} == {
        "Treasury General Account (TGA)", "Federal Reserve Account",
    }
    for record in records:
        assert isinstance(record, TREASURY_CASH_FLOW)
        assert record.direction == "INFLOW"
        assert record.include_in_net is True
        assert record.is_subtotal is False
        assert record.amount_usd_millions == pytest.approx(893.0)


def test_debt_table_code_variants_are_normalized() -> None:
    rows = [
        {
            "record_date": "2005-10-03",
            "transaction_type": "Issues",
            "security_market": "Marketable",
            "security_type": "Bills",
            "security_type_desc": "Regular Series",
            "transaction_today_amt": "354996",
            "src_line_nbr": "1",
            "table_nbr": "III-A",
        },
        {
            "record_date": "2026-10-29",
            "transaction_type": "Issues",
            "security_market": "Marketable",
            "security_type": "Bills",
            "security_type_desc": "Regular Series",
            "transaction_today_amt": "354996",
            "src_line_nbr": "1",
            "table_nbr": "IIIA",
        },
    ]
    records = _connector().normalize(_raw("public_debt_transactions", {"data": rows}))
    assert [r.source_table for r in records] == ["IIIA", "IIIA"]
    assert all(isinstance(r, TREASURY_DEBT_TRANSACTION) for r in records)
    assert all(r.amount_basis == "FACE" for r in records)


def test_debt_cash_basis_uses_adjustment_amount() -> None:
    row = {
        "record_date": "2005-10-03",
        "transaction_type": "Public Debt Cash Issues",
        "adj_type": "Public Debt Issues (Table III-A)",
        "adj_today_amt": "192315",
        "src_line_nbr": "1",
        "table_nbr": "III-B",
    }
    records = _connector().normalize(_raw("public_debt_cash_basis", {"data": [row]}))
    assert len(records) == 1
    record = records[0]
    assert isinstance(record, TREASURY_DEBT_TRANSACTION)
    assert record.source_table == "IIIB"
    assert record.amount_basis == "CASH_ADJUSTMENT"
    assert record.amount_usd_millions == pytest.approx(192_315.0)


def test_auction_result_converts_usd_to_millions() -> None:
    row = {
        "record_date": "2024-06-27",
        "cusip": "91282CKW0",
        "security_type": "Note",
        "security_term": "7-Year",
        "announcemt_date": "2024-06-20",
        "auction_date": "2024-06-27",
        "issue_date": "2024-07-01",
        "maturity_date": "2031-06-30",
        "offering_amt": "44000000000",
        "total_accepted": "45532190200",
        "total_tendered": "115108672600",
        "int_rate": "4.250000",
        "high_yield": "4.2760",
        "int_payment_frequency": "Semi-Annual",
        "reopening": "No",
        "cash_management_bill_cmb": "No",
        "price_per100": "99.843947",
        "currently_outstanding": "null",
    }
    records = _connector().normalize(_raw("auctions_query", {"data": [row]}))
    assert len(records) == 1
    record = records[0]
    assert isinstance(record, TREASURY_AUCTION)
    assert record.data_status == "RESULT"
    assert record.offering_amount_usd_millions == pytest.approx(44_000.0)
    assert record.total_accepted_usd_millions == pytest.approx(45_532.1902)
    assert record.total_tendered_usd_millions == pytest.approx(115_108.6726)
    assert record.auction_key == "91282CKW0:2024-06-27:RESULT"


def test_upcoming_auction_null_offering_is_announced() -> None:
    row = {
        "record_date": "2026-10-16",
        "security_type": "Bill",
        "security_term": "6-Week",
        "reopening": "Yes",
        "cusip": "912797VG9",
        "offering_amt": "null",
        "announcemt_date": "2026-10-22",
        "auction_date": "2026-10-27",
        "issue_date": "2026-10-29",
    }
    records = _connector().normalize(_raw("upcoming_auctions", {"data": [row]}))
    assert len(records) == 1
    record = records[0]
    assert isinstance(record, TREASURY_AUCTION)
    assert record.data_status == "ANNOUNCED"
    assert record.offering_amount_usd_millions is None
    assert record.maturity_date is None


def test_auction_tips_frn_identified_by_index_fields() -> None:
    """auctions_query 的 TIPS/FRN 行 security_type 为 "Note"，
    必须以 inflation_index_security / floating_rate 字段判定。"""
    base = {
        "record_date": "2026-10-30",
        "cusip": "91282CRR4",
        "security_type": "Note",
        "security_term": "5-Year",
        "announcemt_date": "2026-10-15",
        "auction_date": "2026-10-22",
        "issue_date": "2026-10-30",
        "offering_amt": "26000000000",
        "cash_management_bill_cmb": "No",
    }
    tips_row = {**base, "inflation_index_security": "Yes"}
    frn_row = {
        **base,
        "cusip": "91282CGJ9",
        "inflation_index_security": "No",
        "floating_rate": "Yes",
    }
    plain_row = {**base, "cusip": "91282CXX1", "inflation_index_security": "No"}
    records = _connector().normalize(_raw(
        "auctions_query", {"data": [tips_row, frn_row, plain_row]}
    ))
    by_cusip = {r.cusip: r for r in records}
    assert by_cusip["91282CRR4"].is_tips is True
    assert by_cusip["91282CGJ9"].is_frn is True
    assert by_cusip["91282CXX1"].is_tips is False
    assert by_cusip["91282CXX1"].is_frn is False


def test_old_records_marked_estimated() -> None:
    row = {
        "record_date": "2005-10-03",
        "account_type": "Treasury General Account (TGA) Closing Balance",
        "close_today_bal": "null",
        "open_today_bal": "100",
        "src_line_nbr": "4",
    }
    records = _connector().normalize(_raw("operating_cash_balance", {"data": [row]}))
    assert len(records) == 1
    assert records[0].release_time_source == "ESTIMATED"
    assert records[0].release_time.hour == 21
    assert records[0].release_time.minute == 0


def test_schema_drift_raises_schema_error() -> None:
    """缺必需字段 → 熔断，不静默补字段。"""
    connector = _connector()
    connector._client = httpx.Client(
        base_url="https://example.test",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, json={"data": [{"record_date": "2026-10-29"}], "meta": {}}
            )
        ),
    )
    with pytest.raises(SchemaError, match="schema drift"):
        connector._get_page(
            "/v1/accounting/dts/operating_cash_balance",
            table="operating_cash_balance",
            start_date=None,
            end_date=None,
            sort="record_date",
            size=10,
            page=1,
        )


def test_pagination_walks_all_pages_without_overlap_or_gap() -> None:
    """2 页 fixture：page[number] 推进到 total-pages，行不重叠、无缺口。"""
    dates = ["2026-10-26", "2026-10-27", "2026-10-28", "2026-10-29"]
    pages = {
        1: dates[:2],
        2: dates[2:],
    }

    def handler(request: httpx.Request) -> httpx.Response:
        page = int(request.url.params.get("page[number]", "1"))
        return httpx.Response(
            200,
            json={
                "data": [
                    {
                        "record_date": d,
                        "account_type": "Treasury General Account (TGA) Closing Balance",
                        "close_today_bal": "null",
                        "open_today_bal": "1000",
                        "src_line_nbr": "1",
                        "table_nbr": "I",
                    }
                    for d in pages[page]
                ],
                "meta": {"total-pages": 2},
            },
        )

    connector = _connector()
    connector._client = httpx.Client(
        base_url="https://example.test", transport=httpx.MockTransport(handler)
    )
    batches = list(connector.fetch(FetchRequest(
        dataset_id="treasury_fiscal_dts_operating_cash_balance",
        params={"table": "operating_cash_balance"},
        start=datetime(2026, 10, 1),
        end=datetime(2026, 11, 1),
        cursor=None,
    )))
    # 逐页产出且按序推进
    assert [b.raw_meta["page"] for b in batches] == [1, 2]
    assert [b.raw_meta["total_pages"] for b in batches] == [2, 2]
    # 页间无重叠、无缺口：合并 record_date 恰为完整有序日期序列
    merged = [
        row["record_date"]
        for batch in batches
        for row in batch.payload["data"]  # type: ignore[index]
    ]
    assert merged == dates
    assert len(set(merged)) == len(merged)


def test_fetch_rewinds_start_by_one_day_for_t_plus_1() -> None:
    """DTS 次日发布：起点须回退 1 天，否则边界日行永久漏采（H4）。"""
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"data": [], "meta": {"total-pages": 1}})

    connector = _connector()
    connector._client = httpx.Client(
        base_url="https://example.test", transport=httpx.MockTransport(handler)
    )
    list(connector.fetch(FetchRequest(
        dataset_id="treasury_fiscal_dts_operating_cash_balance",
        params={"table": "operating_cash_balance"},
        start=datetime(2026, 10, 1),
        end=datetime(2026, 11, 1),
        cursor=None,
    )))
    assert seen
    assert seen[0].url.params["filter"] == (
        "record_date:gte:2026-09-30,record_date:lte:2026-11-01"
    )


def test_fetch_to_normalize_operating_cash_balance_e2e() -> None:
    """真实 fixture → fetch（2 页）→ normalize：行数与关键字段对齐。"""
    root = f"{_FIXTURES}/dts_operating_cash_balance"
    pages: dict[int, dict[str, Any]] = {}
    for page, name in ((1, "first_page.json"), (2, "last_page.json")):
        with open(f"{root}/{name}", encoding="utf-8") as handle:
            body = json.load(handle)
        body["meta"] = {**body.get("meta", {}), "total-pages": 2}
        pages[page] = body

    def handler(request: httpx.Request) -> httpx.Response:
        page = int(request.url.params.get("page[number]", "1"))
        return httpx.Response(200, json=pages[page])

    connector = _connector()
    connector._client = httpx.Client(
        base_url="https://example.test", transport=httpx.MockTransport(handler)
    )
    batches = list(connector.fetch(FetchRequest(
        dataset_id="treasury_fiscal_dts_operating_cash_balance",
        params={"table": "operating_cash_balance"},
        start=datetime(2026, 10, 1),
        end=datetime(2026, 11, 1),
        cursor=None,
    )))
    assert [batch.raw_meta["page"] for batch in batches] == [1, 2]

    records = [
        record for batch in batches for record in connector.normalize(batch)
    ]
    # first_page 4 行（历史单行拆 OPENING/CLOSING）+ last_page 3 行
    assert len(records) == 7
    first = records[0]
    assert first.observation_time.date() == date(2005, 10, 3)
    assert first.balance_type == "OPENING"
    assert first.amount_usd_millions == pytest.approx(4381.0)
    second = records[1]
    assert second.balance_type == "CLOSING"
    assert second.amount_usd_millions == pytest.approx(5448.0)
    assert records[-1].observation_time.date() == date(2026, 10, 29)


def test_non_empty_payload_with_unparseable_rows_skips_with_warning() -> None:
    """非空但缺字段的行 → normalize 返回 0 行并记录 normalize_skipped。"""
    raw = _raw("operating_cash_balance", {"data": [
        {"account_type": "TGA", "open_today_bal": "1"},  # 缺 record_date
        {"record_date": "bad", "close_today_bal": "x"},  # 日期非法
    ]})
    with capture_logs() as logs:
        records = _connector().normalize(raw)
    assert records == []
    assert any(
        entry["event"] == "treasury_fiscal.normalize_skipped" for entry in logs
    )


def test_checkpoint_from_raw_uses_max_record_date() -> None:
    raw = _raw("operating_cash_balance", {"data": [
        {"record_date": "2026-10-28"},
        {"record_date": "2026-10-29"},
    ]})
    assert _connector().checkpoint_from(raw) == "2026-10-29"
