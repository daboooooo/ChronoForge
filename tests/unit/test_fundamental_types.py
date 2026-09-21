"""Fundamental type tests（D09，MODEL-002.3）。

覆盖 FILING/FUNDAMENTAL/DOCUMENT 的合法实例化、
边界校验、失败拒绝及 natural_key() 契约。
"""

from datetime import date, datetime

import pytest
from pydantic import ValidationError

from chronoforge.models.fundamental import DOCUMENT, FILING, FUNDAMENTAL


class TestFILING:
    """FILING 合法/边界/失败用例（D02 §2）。"""

    _now = datetime(2026, 9, 11, 12, 0, 0)

    def _sample(self, **overrides) -> dict:
        base = {
            "source_id": "sec-edgar",
            "entity_id": "AAPL",
            "cik": "0000320193",
            "accession_number": "0000320193-26-000012",
            "form_type": "10-K",
            "filing_date": date(2026, 9, 10),
            "accepted_datetime": self._now,
            "report_period_end": date(2026, 6, 30),
            "primary_doc_url": "https://sec.gov/aapl-10k.pdf",
            "raw_record_id": "sec:test:file.jsonl:1",
            "schema_version": "1.0",
            "source": "sec_edgar",
            "source_timestamp": self._now,
            "ingest_timestamp": self._now,
        }
        base.update(overrides)
        return base

    def test_filing_valid(self) -> None:
        """FILING 合法实例化。"""
        record = FILING(**self._sample())
        assert record.cik == "0000320193"
        assert record.accession_number == "0000320193-26-000012"

    def test_filing_10q_form_type_ok(self) -> None:
        """FILING form_type=10-Q 合法。"""
        record = FILING(**self._sample(form_type="10-Q"))
        assert record.form_type == "10-Q"

    def test_filing_8k_form_type_ok(self) -> None:
        """FILING form_type=8-K 合法。"""
        record = FILING(**self._sample(form_type="8-K"))
        assert record.form_type == "8-K"

    def test_filing_unknown_form_type_ok(self) -> None:
        """FILING 未知 form_type 不拒绝（不枚举限制）。"""
        record = FILING(**self._sample(form_type="CUSTOM"))
        assert record.form_type == "CUSTOM"

    def test_filing_empty_raw_record_id_rejected(self) -> None:
        """GWT: FILING raw_record_id="" → ValidationError。"""
        with pytest.raises(ValidationError):
            FILING(**self._sample(raw_record_id=""))

    def test_filing_natural_key(self) -> None:
        """FILING natural_key = (cik, accession_number)。"""
        rec = FILING(**self._sample())
        assert rec.natural_key() == ("0000320193", "0000320193-26-000012")


class TestFUNDAMENTAL:
    """FUNDAMENTAL 合法/边界/失败用例（D02 §2）。"""

    _now = datetime(2026, 9, 11, 12, 0, 0)

    def _sample(self, **overrides) -> dict:
        base = {
            "source_id": "sec-edgar",
            "raw_record_id": "sec:test:file.jsonl:1",
            "entity_id": "AAPL",
            "cik": "0000320193",
            "concept": "NetIncomeLoss",
            "taxon": "us-gaap",
            "unit": "USD",
            "observation_time": self._now,
            "value": 97000000000.0,
            "fiscal_year": 2026,
            "fiscal_period": "Q1",
            "frame": "trailing",
            "schema_version": "1.0",
            "source": "sec_edgar",
            "source_timestamp": self._now,
            "ingest_timestamp": self._now,
        }
        base.update(overrides)
        return base

    def test_fundamental_valid(self) -> None:
        """FUNDAMENTAL 合法实例化。"""
        record = FUNDAMENTAL(**self._sample())
        assert record.concept == "NetIncomeLoss"
        assert record.value == 97000000000.0

    def test_fundamental_value_nan_rejected(self) -> None:
        """FUNDAMENTAL value=NaN → ValidationError。"""
        with pytest.raises(ValidationError):
            FUNDAMENTAL(**self._sample(value=float("nan")))

    def test_fundamental_value_inf_rejected(self) -> None:
        """FUNDAMENTAL value=Inf → ValidationError。"""
        with pytest.raises(ValidationError):
            FUNDAMENTAL(**self._sample(value=float("inf")))

    def test_fundamental_fiscal_year_2001_ok(self) -> None:
        """FUNDAMENTAL fiscal_year=2001 合法（> 2000）。"""
        record = FUNDAMENTAL(**self._sample(fiscal_year=2001))
        assert record.fiscal_year == 2001

    def test_fundamental_fiscal_year_2100_rejected(self) -> None:
        """FUNDAMENTAL fiscal_year=2100 → ValidationError（>= 2100）。"""
        with pytest.raises(ValidationError):
            FUNDAMENTAL(**self._sample(fiscal_year=2100))

    def test_fundamental_fiscal_year_2000_rejected(self) -> None:
        """FUNDAMENTAL fiscal_year=2000 → ValidationError（<= 2000）。"""
        with pytest.raises(ValidationError):
            FUNDAMENTAL(**self._sample(fiscal_year=2000))

    def test_fundamental_natural_key(self) -> None:
        """FUNDAMENTAL natural_key = (entity_id, concept, observation_time)。"""
        rec = FUNDAMENTAL(**self._sample())
        assert rec.natural_key() == ("AAPL", "NetIncomeLoss", self._now)


class TestDOCUMENT:
    """DOCUMENT 合法/边界/失败用例（D02 §2）。"""

    _now = datetime(2026, 9, 11, 12, 0, 0)

    def _sample(self, **overrides) -> dict:
        base = {
            "source_id": "sec-edgar",
            "raw_record_id": "sec:test:file.jsonl:1",
            "url": "https://sec.gov/aapl-10k.pdf",
            "publication_time": self._now,
            "content_hash": "a" * 64,
            "mime": "application/pdf",
            "schema_version": "1.0",
            "source": "sec_edgar",
            "source_timestamp": self._now,
            "ingest_timestamp": self._now,
        }
        base.update(overrides)
        return base

    def test_document_valid(self) -> None:
        """DOCUMENT 合法实例化。"""
        record = DOCUMENT(**self._sample())
        assert record.url == "https://sec.gov/aapl-10k.pdf"

    def test_document_content_hash_64_ok(self) -> None:
        """DOCUMENT content_hash=64 字符合法。"""
        record = DOCUMENT(**self._sample(content_hash="a" * 64))
        assert len(record.content_hash) == 64

    def test_document_content_hash_63_rejected(self) -> None:
        """DOCUMENT content_hash=63 字符 → ValidationError。"""
        with pytest.raises(ValidationError):
            DOCUMENT(**self._sample(content_hash="a" * 63))

    def test_document_content_hash_65_rejected(self) -> None:
        """DOCUMENT content_hash=65 字符 → ValidationError。"""
        with pytest.raises(ValidationError):
            DOCUMENT(**self._sample(content_hash="a" * 65))

    def test_document_empty_mime_rejected(self) -> None:
        """DOCUMENT mime="" → ValidationError。"""
        with pytest.raises(ValidationError):
            DOCUMENT(**self._sample(mime=""))

    def test_document_natural_key(self) -> None:
        """DOCUMENT natural_key = (url, publication_time)。"""
        rec = DOCUMENT(**self._sample())
        assert rec.natural_key() == ("https://sec.gov/aapl-10k.pdf", self._now)
