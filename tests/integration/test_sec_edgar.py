"""DATA-SOURCE-007 集成测试 — SECEdgarConnector。

覆盖：
- TC-C-007：fixture→canonical 逐字段比对（FILING + FUNDAMENTAL）
- TC-C-014：UA 注入断言（User-Agent 正确格式）
- acceptanceDatetime EDT/EST → UTC 时区转换（审计 F-09）
- 边界：空 filings/empty facts → 0 行
- 错误路径：429/404/500
- checkpoint 提取
- validate 基础校验
"""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest

from chronoforge.connectors.base import (
    CapabilityMatrix,
    FetchRequest,
    RawBatch,
)
from chronoforge.connectors.errors import (
    ConfigError,
    ProviderError,
    RateLimitError,
)
from chronoforge.connectors.sec_edgar import (
    SECEdgarConnector,
    _SECSettings,
)
from chronoforge.models.enums import CanonicalType, QualityStatus
from chronoforge.models.fundamental import FILING, FUNDAMENTAL

# Fixture paths
_SUBMISSIONS_FIXTURES = (
    Path(__file__).parent.parent / "fixtures" / "sec_edgar" / "submissions"
)
_COMPANYFACTS_FIXTURES = (
    Path(__file__).parent.parent / "fixtures" / "sec_edgar" / "companyfacts"
)


# ── Helpers ────────────────────────────────────────────────────────────


def _load_fixture(name: str) -> Any:
    """Load a JSON fixture file."""
    path = _SUBMISSIONS_FIXTURES / name
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _load_companyfacts_fixture(name: str) -> Any:
    """Load a companyfacts fixture file."""
    path = _COMPANYFACTS_FIXTURES / name
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _mock_response(
    status_code: int = 200,
    json_data: Any = None,
    headers: dict | None = None,
    url: str = "https://data.sec.gov/submissions/CIK0000325023.json",
) -> MagicMock:
    """Create a mock httpx Response."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.headers = headers or {}
    resp.url = url
    resp.text = json.dumps(json_data) if json_data else ""
    return resp


def _mock_client(
    resp: MagicMock | list[MagicMock] | dict[str, Any],
) -> MagicMock:
    """Create a mock httpx Client that returns the given response(s)."""
    client = MagicMock()
    if isinstance(resp, list):
        queue = list(resp)

        def _side_effect(*args: Any, **kwargs: Any) -> MagicMock:
            if not queue:
                empty = MagicMock(spec=httpx.Response)
                empty.status_code = 200
                empty.json.return_value = {
                    "filings": {
                        "recent": {
                            "accessionNumber": [],
                            "filingDate": [],
                            "reportDate": [],
                            "acceptanceDate": [],
                            "form": [],
                            "documents": [],
                        }
                    }
                }
                empty.headers = {}
                empty.url = ""
                return empty
            return queue.pop(0)

        client.get.side_effect = _side_effect
    elif isinstance(resp, dict):
        client.get.return_value = _mock_response(json_data=resp)
    else:
        client.get.return_value = resp
    return client


def _create_mock_connector(
    resp: MagicMock | list[MagicMock] | dict[str, Any],
) -> tuple[SECEdgarConnector, MagicMock]:
    """Create a mock connector instance for fetch/normalize tests."""
    mock_client = _mock_client(resp)
    mock_rate_limiter = MagicMock()

    conn = SECEdgarConnector.__new__(SECEdgarConnector)
    conn._client = mock_client
    conn._rate_limiter = mock_rate_limiter
    conn._settings = _SECSettings(sec_contact_email="test@example.com")
    conn.contact_email = "test@example.com"
    conn.user_agent = "ChronoForge/0.1.0 research (test@example.com)"

    return conn, mock_client


# ── capabilities ───────────────────────────────────────────────────────


class TestCapabilities:
    """SECEdgarConnector.capabilities() 测试（D04 §4.7）。"""

    def test_capabilities_returns_correct_matrix(self) -> None:
        """Given valid connector When capabilities() Then FILING/FUNDAMENTAL/DOCUMENT."""
        conn = SECEdgarConnector(_SECSettings(sec_contact_email="test@example.com"))
        cap = conn.capabilities()

        assert isinstance(cap, CapabilityMatrix)
        assert cap.canonical_types == frozenset({
            CanonicalType.FILING,
            CanonicalType.FUNDAMENTAL,
            CanonicalType.DOCUMENT,
        })
        assert cap.supports_revision is False
        assert cap.supports_websocket is False
        assert cap.max_history_days is None
        assert len(cap.intervals) == 0  # SEC 无 interval
        conn.close()


# ── TC-C-014: UA 注入断言 ────────────────────────────────────────────


class TestUserAgent:
    """User-Agent 注入断言（TC-C-014）。"""

    def test_user_agent_format(self) -> None:
        """Given valid settings When init Then User-Agent 格式正确。"""
        conn = SECEdgarConnector(
            _SECSettings(sec_contact_email="research@example.com")
        )

        assert "ChronoForge/" in conn.user_agent
        assert "research" in conn.user_agent
        assert "research@example.com" in conn.user_agent
        conn.close()

    def test_user_agent_in_client_headers(self) -> None:
        """Client 构造时 User-Agent 注入到 headers。"""
        conn = SECEdgarConnector(
            _SECSettings(sec_contact_email="test@example.com")
        )

        # 验证 client 的 headers 中包含 User-Agent
        assert "User-Agent" in conn._client.headers
        assert "ChronoForge/" in str(conn._client.headers["User-Agent"])
        conn.close()

    def test_missing_email_raises_config_error(self) -> None:
        """Given no contact_email When init Then ConfigError。"""
        with pytest.raises(ConfigError, match="CHRONOFORGE_SEC_CONTACT"):
            SECEdgarConnector(_SECSettings(sec_contact_email=""))


# ── TC-C-007: fixture→canonical 逐字段比对 — FILING ──────────────────


class TestNormalizeFilings:
    """fixture→canonical 逐字段比对（TC-C-007 FILING）。"""

    def test_normalize_submissions_happy_path(self) -> None:
        """Given submissions response When normalize Then FILING 逐字段正确。"""
        sub_data = _load_fixture("happy.json")

        conn, _ = _create_mock_connector(sub_data)
        raw = RawBatch(
            endpoint="/CIK0000325023.json",
            payload=sub_data,
            raw_meta={"cik": "0000325023"},
        )
        records = conn.normalize(raw)

        # 3 条 FILING 记录
        assert len(records) == 3
        assert all(isinstance(r, FILING) for r in records)

        # 逐条校验
        for i, record in enumerate(records):
            assert record.source == "sec_edgar"
            assert record.source_id == "0000325023"
            assert record.quality_status == QualityStatus.VALID
            assert record.entity_id == "CIK0000325023"
            assert record.cik == "0000325023"

            # accession_number 对应
            expected_acc = sub_data["filings"]["recent"]["accessionNumber"][i]
            assert record.accession_number == expected_acc

            # form_type 对应
            expected_form = sub_data["filings"]["recent"]["form"][i]
            assert record.form_type == expected_form

            # filing_date 对应
            expected_fd = sub_data["filings"]["recent"]["filingDate"][i]
            assert record.filing_date == datetime.strptime(expected_fd, "%Y-%m-%d").date()

            # report_period_end 对应
            expected_rp = sub_data["filings"]["recent"]["reportDate"][i]
            assert record.report_period_end == datetime.strptime(expected_rp, "%Y-%m-%d").date()

            # primary_doc_url 格式正确
            assert record.primary_doc_url.startswith("https://www.sec.gov/Archives/edgar/data/")

            conn.close()

    def test_normalize_filings_provenance_fields(self) -> None:
        """FILING 记录包含所有 provenance 字段。"""
        sub_data = _load_fixture("happy.json")
        conn, _ = _create_mock_connector(sub_data)

        raw = RawBatch(
            endpoint="/CIK0000325023.json",
            payload=sub_data,
            raw_meta={"cik": "0000325023"},
        )
        records = conn.normalize(raw)

        record = records[0]
        assert record.schema_version == "1.0"
        assert record.raw_record_id.startswith("sec_edgar:0000325023:")
        assert record.ingest_timestamp is not None
        assert record.source_timestamp is not None
        conn.close()


# ── TC-C-007: fixture→canonical 逐字段比对 — FUNDAMENTAL ─────────────


class TestNormalizeFundamentals:
    """fixture→canonical 逐字段比对（TC-C-007 FUNDAMENTAL）。"""

    def test_normalize_companyfacts_happy_path(self) -> None:
        """Given companyfacts response When normalize Then FUNDAMENTAL 逐字段正确。"""
        cf_data = _load_companyfacts_fixture("happy.json")

        conn, _ = _create_mock_connector(cf_data)
        raw = RawBatch(
            endpoint="/CIK0000325023/companyfacts.json",
            payload=cf_data,
            raw_meta={"cik": "0000325023"},
        )
        records = conn.normalize(raw)

        # 2 Assets + 2 Revenues + 1 NetIncomeLoss = 5 条记录
        assert len(records) == 5
        assert all(isinstance(r, FUNDAMENTAL) for r in records)

        # 逐条校验
        for record in records:
            assert record.source == "sec_edgar"
            assert record.source_id == "0000325023"
            assert record.quality_status == QualityStatus.VALID
            assert record.entity_id == "CIK0000325023"
            assert record.cik == "0000325023"

            # 确认字段存在
            assert record.concept in ("Assets", "Revenues", "NetIncomeLoss")
            assert record.unit == "USD"
            assert record.fiscal_year in (2023, 2024)
            assert record.observation_time.hour == 23
            assert record.observation_time.minute == 59
            assert record.observation_time.second == 59

        conn.close()

    def test_normalize_fundamentals_taxon_extracted(self) -> None:
        """FUNDAMENTAL 记录 taxon 字段正确提取。"""
        cf_data = _load_companyfacts_fixture("happy.json")
        conn, _ = _create_mock_connector(cf_data)

        raw = RawBatch(
            endpoint="/CIK0000325023/companyfacts.json",
            payload=cf_data,
            raw_meta={"cik": "0000325023"},
        )
        records = conn.normalize(raw)

        for record in records:
            # taxon 必须包含 us-gaap 或 ifrs-full
            assert "us-gaap" in record.taxon or "ifrs-full" in record.taxon

        conn.close()


# ── acceptanceDatetime EDT/EST → UTC 时区转换（审计 F-09） ───────────


class TestTimezoneConversion:
    """acceptanceDateTime EDT/EST → UTC 时区转换（审计 F-09）。"""

    def test_acceptance_datetime_edt_converted_to_utc(self) -> None:
        """Given EDT 后缀 When normalize Then accepted_datetime 转为 UTC。"""
        # 2025-01-31 16:30:00 EDT → 2025-01-31 20:30:00 UTC
        sub_data = _load_fixture("happy.json")
        conn, _ = _create_mock_connector(sub_data)

        raw = RawBatch(
            endpoint="/CIK0000325023.json",
            payload=sub_data,
            raw_meta={"cik": "0000325023"},
        )
        records = conn.normalize(raw)

        # 第一条记录：EDT → UTC（+4 小时）
        assert len(records) >= 1
        first = records[0]
        # EDT offset = -4, so 16:30 EDT = 20:30 UTC
        assert first.accepted_datetime.hour == 20
        assert first.accepted_datetime.minute == 30

        conn.close()

    def test_acceptance_datetime_est_converted_to_utc(self) -> None:
        """Given EST 后缀 When normalize Then accepted_datetime 转为 UTC。"""
        # 2025-01-15 17:45:00 EST → 2025-01-15 22:45:00 UTC
        conn, _ = _create_mock_connector(_load_fixture("happy.json"))

        raw = RawBatch(
            endpoint="/CIK0000325023.json",
            payload=_load_fixture("happy.json"),
            raw_meta={"cik": "0000325023"},
        )
        records = conn.normalize(raw)

        # 第二条记录：EST → UTC（+5 小时）
        assert len(records) >= 2
        second = records[1]
        assert second.accepted_datetime.hour == 22
        assert second.accepted_datetime.minute == 45

        conn.close()

    def test_acceptance_datetime_utc_no_conversion(self) -> None:
        """Given UTC 后缀 When normalize Then accepted_datetime 不变。"""
        conn, _ = _create_mock_connector(_load_fixture("happy.json"))

        raw = RawBatch(
            endpoint="/CIK0000325023.json",
            payload=_load_fixture("happy.json"),
            raw_meta={"cik": "0000325023"},
        )
        records = conn.normalize(raw)

        # 第三条记录：UTC → 不变（offset=0）
        assert len(records) >= 3
        third = records[2]
        # 20:00:00 UTC → 20:00:00 UTC
        assert third.accepted_datetime.hour == 20
        assert third.accepted_datetime.minute == 0

        conn.close()

    def test_parse_acceptance_datetime_pure_date(self) -> None:
        """纯日期格式解析。"""
        conn = SECEdgarConnector(
            _SECSettings(sec_contact_email="test@example.com")
        )

        result = conn._parse_acceptance_datetime("2025-01-31")
        assert result == datetime(2025, 1, 31)

        result_none = conn._parse_acceptance_datetime("")
        assert result_none is None

        conn.close()


# ── 边界：空结果 ────────────────────────────────────────────────────


class TestEmptyResults:
    """空结果测试。"""

    def test_empty_submissions_returns_zero_records(self) -> None:
        """Given empty submissions When normalize Then 0 行。"""
        sub_data = _load_fixture("empty.json")
        conn, _ = _create_mock_connector(sub_data)

        raw = RawBatch(
            endpoint="/CIK0000000000.json",
            payload=sub_data,
            raw_meta={"cik": "0000000000"},
        )
        records = conn.normalize(raw)

        assert len(records) == 0
        conn.close()

    def test_empty_companyfacts_returns_zero_records(self) -> None:
        """Given empty companyfacts When normalize Then 0 行。"""
        cf_data = _load_companyfacts_fixture("empty.json")
        conn, _ = _create_mock_connector(cf_data)

        raw = RawBatch(
            endpoint="/CIK0000000000/companyfacts.json",
            payload=cf_data,
            raw_meta={"cik": "0000000000"},
        )
        records = conn.normalize(raw)

        assert len(records) == 0
        conn.close()

    def test_missing_filings_key_returns_zero(self) -> None:
        """Payload missing 'filings' key → 0 行。"""
        conn, _ = _create_mock_connector({"cik": "0000325023"})

        raw = RawBatch(
            endpoint="/CIK0000325023.json",
            payload={"cik": "0000325023"},
            raw_meta={"cik": "0000325023"},
        )
        records = conn.normalize(raw)

        assert len(records) == 0
        conn.close()


# ── 错误路径 ────────────────────────────────────────────────────────


class TestErrorPaths:
    """错误路径测试。"""

    def test_429_raises_rate_limit_error(self) -> None:
        """Given 429 response When fetch Then RateLimitError。"""
        resp_429 = _mock_response(
            status_code=429,
            json_data={"error": "Rate limit exceeded"},
        )
        conn, _ = _create_mock_connector(resp_429)

        request = FetchRequest(
            dataset_id="sec:0000325023",
            params={"cik": "0000325023"},
            start=None,
            end=None,
            cursor=None,
        )

        with pytest.raises(RateLimitError):
            list(conn.fetch(request))

        conn.close()

    def test_404_raises_provider_error(self) -> None:
        """Given 404 response When fetch Then ProviderError。"""
        resp_404 = _mock_response(
            status_code=404,
            json_data={"error": "CIK not found"},
        )
        conn, _ = _create_mock_connector(resp_404)

        request = FetchRequest(
            dataset_id="sec:0000000000",
            params={"cik": "0000000000"},
            start=None,
            end=None,
            cursor=None,
        )

        with pytest.raises(ProviderError):
            list(conn.fetch(request))

        conn.close()

    def test_500_raises_provider_error(self) -> None:
        """Given 500 response When fetch Then ProviderError。"""
        resp_500 = _mock_response(
            status_code=500,
            json_data={"error": "Internal server error"},
        )
        conn, _ = _create_mock_connector(resp_500)

        request = FetchRequest(
            dataset_id="sec:0000325023",
            params={"cik": "0000325023"},
            start=None,
            end=None,
            cursor=None,
        )

        with pytest.raises(ProviderError):
            list(conn.fetch(request))

        conn.close()

    def test_missing_cik_raises_value_error(self) -> None:
        """No cik in params When fetch Then ValueError。"""
        conn, _ = _create_mock_connector({})

        request = FetchRequest(
            dataset_id="sec:test",
            params={},
            start=None,
            end=None,
            cursor=None,
        )

        with pytest.raises(ValueError, match="cik is required"):
            list(conn.fetch(request))

        conn.close()


# ── checkpoint ──────────────────────────────────────────────────────


class TestCheckpoint:
    """checkpoint_from 测试。"""

    def test_checkpoint_from_submissions(self) -> None:
        """Given submissions RawBatch When checkpoint_from Then last accession_number。"""
        sub_data = _load_fixture("happy.json")
        conn, _ = _create_mock_connector(sub_data)

        raw = RawBatch(
            endpoint="/CIK0000325023.json",
            payload=sub_data,
            raw_meta={"cik": "0000325023"},
        )

        cursor = conn.checkpoint_from(raw)
        # 最后一条 accession number
        assert cursor == "0001628280-25-001800"
        conn.close()

    def test_checkpoint_from_filings_records(self) -> None:
        """Given FILING records When checkpoint_from Then last accession_number。"""
        sub_data = _load_fixture("happy.json")
        conn, _ = _create_mock_connector(sub_data)

        raw = RawBatch(
            endpoint="/CIK0000325023.json",
            payload=sub_data,
            raw_meta={"cik": "0000325023"},
        )
        records = conn.normalize(raw)

        cursor = conn.checkpoint_from(records)
        assert cursor == "0001628280-25-001800"
        conn.close()

    def test_checkpoint_empty_returns_none(self) -> None:
        """Given empty data When checkpoint_from Then None。"""
        sub_data = _load_fixture("empty.json")
        conn, _ = _create_mock_connector(sub_data)

        raw = RawBatch(
            endpoint="/CIK0000000000.json",
            payload=sub_data,
            raw_meta={"cik": "0000000000"},
        )
        cursor = conn.checkpoint_from(raw)
        assert cursor is None
        conn.close()

    def test_checkpoint_from_companyfacts(self) -> None:
        """Given companyfacts RawBatch When checkpoint_from Then year-month cursor。"""
        cf_data = _load_companyfacts_fixture("happy.json")
        conn, _ = _create_mock_connector(cf_data)

        raw = RawBatch(
            endpoint="/CIK0000325023/companyfacts.json",
            payload=cf_data,
            raw_meta={"cik": "0000325023"},
        )

        cursor = conn.checkpoint_from(raw)
        # 最后一条数据的 year-month（NetIncomeLoss 2024-9 是字典迭代的最后一条）
        assert cursor is not None
        assert "2024" in cursor
        conn.close()

    def test_checkpoint_empty_list_returns_none(self) -> None:
        """Given empty list When checkpoint_from Then None。"""
        conn, _ = _create_mock_connector({})

        cursor = conn.checkpoint_from([])
        assert cursor is None
        conn.close()


# ── validate ────────────────────────────────────────────────────────


class TestValidate:
    """validate 测试（D04 §1，委托 quality.rules）。"""

    def test_validate_valid_filings(self) -> None:
        """Given valid FILING When validate Then no findings (past dates)。"""
        sub_data = _load_fixture("happy.json")
        conn, _ = _create_mock_connector(sub_data)

        raw = RawBatch(
            endpoint="/CIK0000325023.json",
            payload=sub_data,
            raw_meta={"cik": "0000325023"},
        )
        records = conn.normalize(raw)
        report = conn.validate(records)

        # 所有 filing_date 都在过去，无 findings
        assert len(report.findings) == 0
        conn.close()

    def test_validate_future_filing_warning(self) -> None:
        """Given future filing_date When validate Then WARNING。"""
        conn, _ = _create_mock_connector({})
        # Use model_construct to bypass pydantic validators
        future_filing = FILING.model_construct(
            schema_version="1.0",
            source="sec_edgar",
            source_id="0000325023",
            source_timestamp=datetime(2025, 1, 1),
            ingest_timestamp=datetime(2025, 1, 1),
            raw_record_id="sec_edgar:0000325023:TEST",
            quality_status="VALID",
            quality_reason=None,
            entity_id="CIK0000325023",
            cik="0000325023",
            accession_number="0001234567-25-000001",
            form_type="10-K",
            filing_date=date(2030, 1, 1),  # future
            accepted_datetime=datetime(2025, 1, 1),
            report_period_end=date(2029, 12, 31),
            primary_doc_url="https://www.sec.gov/Archives/edgar/data/123456725000001",
        )

        report = conn.validate([future_filing])
        assert len(report.findings) == 1
        assert report.findings[0].rule_id == "Q-TS-001"
        assert report.findings[0].severity == "WARNING"
        conn.close()

    def test_validate_valid_fundamentals(self) -> None:
        """Given valid FUNDAMENTAL When validate Then no findings。"""
        cf_data = _load_companyfacts_fixture("happy.json")
        conn, _ = _create_mock_connector(cf_data)

        raw = RawBatch(
            endpoint="/CIK0000325023/companyfacts.json",
            payload=cf_data,
            raw_meta={"cik": "0000325023"},
        )
        records = conn.normalize(raw)
        report = conn.validate(records)

        assert len(report.findings) == 0
        conn.close()


# ── fetch ──────────────────────────────────────────────────────────


class TestFetch:
    """fetch 测试。"""

    def test_fetch_submissions(self) -> None:
        """Given submissions request When fetch Then yields RawBatch。"""
        sub_data = _load_fixture("happy.json")
        conn, _ = _create_mock_connector(sub_data)

        request = FetchRequest(
            dataset_id="sec:0000325023",
            params={"cik": "0000325023", "data_type": "filing"},
            start=None,
            end=None,
            cursor=None,
        )

        batches = list(conn.fetch(request))
        assert len(batches) == 1
        assert batches[0].endpoint == "/CIK0000325023.json"
        assert batches[0].payload == sub_data
        conn.close()

    def test_fetch_companyfacts(self) -> None:
        """Given companyfacts request When fetch Then yields companyfacts RawBatch。"""
        cf_data = _load_companyfacts_fixture("happy.json")
        conn, _ = _create_mock_connector(cf_data)

        request = FetchRequest(
            dataset_id="sec:0000325023",
            params={"cik": "0000325023", "data_type": "fundamental"},
            start=None,
            end=None,
            cursor=None,
        )

        batches = list(conn.fetch(request))
        assert len(batches) == 1
        assert "companyfacts" in batches[0].endpoint
        conn.close()

    def test_fetch_default_all(self) -> None:
        """Default data_type=all → submissions only。"""
        sub_data = _load_fixture("happy.json")
        conn, _ = _create_mock_connector(sub_data)

        request = FetchRequest(
            dataset_id="sec:0000325023",
            params={"cik": "0000325023"},
            start=None,
            end=None,
            cursor=None,
        )

        batches = list(conn.fetch(request))
        assert len(batches) == 1
        assert batches[0].endpoint == "/CIK0000325023.json"
        conn.close()


# ── GWT: Given submissions fixture When normalize Then FILING 记录数 ─


class TestGWT:
    """Given-When-Then 验收测试。"""

    def test_gwt_filing_count_matches_fixture(self) -> None:
        """Given submissions fixture When normalize Then FILING 记录数与 fixture 一致。"""
        sub_data = _load_fixture("happy.json")
        conn, _ = _create_mock_connector(sub_data)

        raw = RawBatch(
            endpoint="/CIK0000325023.json",
            payload=sub_data,
            raw_meta={"cik": "0000325023"},
        )
        records = conn.normalize(raw)

        expected_count = len(sub_data["filings"]["recent"]["accessionNumber"])
        assert len(records) == expected_count
        assert all(isinstance(r, FILING) for r in records)
        conn.close()

    def test_gwt_fundamental_count_matches_fixture(self) -> None:
        """Given companyfacts fixture When normalize Then FUNDAMENTAL 记录数与 fixture 一致。"""
        cf_data = _load_companyfacts_fixture("happy.json")
        conn, _ = _create_mock_connector(cf_data)

        raw = RawBatch(
            endpoint="/CIK0000325023/companyfacts.json",
            payload=cf_data,
            raw_meta={"cik": "0000325023"},
        )
        records = conn.normalize(raw)

        # Count total units across all concepts
        expected_count = sum(
            len(concept_data.get("units", []))
            for concept_data in cf_data.get("facts", {}).values()
        )
        assert len(records) == expected_count
        assert all(isinstance(r, FUNDAMENTAL) for r in records)
        conn.close()

    def test_gwt_filing_natural_key_unique(self) -> None:
        """FILING natural_key (cik, accession_number) 唯一。"""
        sub_data = _load_fixture("happy.json")
        conn, _ = _create_mock_connector(sub_data)

        raw = RawBatch(
            endpoint="/CIK0000325023.json",
            payload=sub_data,
            raw_meta={"cik": "0000325023"},
        )
        records = conn.normalize(raw)

        keys = [r.natural_key() for r in records]
        assert len(keys) == len(set(keys)), "natural_keys 应唯一"
        conn.close()

    def test_gwt_fundamental_natural_key_unique(self) -> None:
        """FUNDAMENTAL natural_key (entity_id, concept, observation_time) 唯一。"""
        cf_data = _load_companyfacts_fixture("happy.json")
        conn, _ = _create_mock_connector(cf_data)

        raw = RawBatch(
            endpoint="/CIK0000325023/companyfacts.json",
            payload=cf_data,
            raw_meta={"cik": "0000325023"},
        )
        records = conn.normalize(raw)

        keys = [r.natural_key() for r in records]
        assert len(keys) == len(set(keys)), "natural_keys 应唯一"
        conn.close()
