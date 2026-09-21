"""Fundamental canonical type schemas（D02 §2 fundamental.py，MODEL-002.3）。

包含 FILING/FUNDAMENTAL/DOCUMENT。全部继承 BaseRecord。
"""

from __future__ import annotations

import math
from datetime import date, datetime
from typing import Any

from pydantic import field_validator

from chronoforge.models.base import BaseRecord


class FILING(BaseRecord):
    """SEC 文件申报（D02 §2）。

    身份键 = (cik, accession_number)。
    """

    entity_id: str
    cik: str
    accession_number: str
    form_type: str
    filing_date: date
    accepted_datetime: datetime
    report_period_end: date
    primary_doc_url: str
    raw_record_id: str

    @field_validator("raw_record_id")
    @classmethod
    def _check_raw_record_id_non_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("raw_record_id must not be empty")
        return v

    def natural_key(self) -> tuple[str, str]:
        return (self.cik, self.accession_number)


class FUNDAMENTAL(BaseRecord):
    """GAAP/IFRS 财务数据（D02 §2）。

    身份键 = (entity_id, concept, observation_time)。
    """

    entity_id: str
    cik: str
    concept: str
    taxon: str
    unit: str
    observation_time: datetime
    value: float
    fiscal_year: int
    fiscal_period: str
    frame: str

    @field_validator("value", mode="before")
    @classmethod
    def _check_value_finite(cls, v: Any) -> Any:
        if v is not None and not math.isfinite(v):
            raise ValueError("must be finite (not NaN/Inf)")
        return v

    @field_validator("fiscal_year")
    @classmethod
    def _check_fiscal_year_range(cls, v: int) -> int:
        if v <= 2000 or v >= 2100:
            raise ValueError("fiscal_year must be > 2000 and < 2100")
        return v

    def natural_key(self) -> tuple[str, str, datetime]:
        return (self.entity_id, self.concept, self.observation_time)


class DOCUMENT(BaseRecord):
    """文档元数据（D02 §2）。

    身份键 = (url, publication_time)。
    """

    url: str
    publication_time: datetime
    content_hash: str
    mime: str

    @field_validator("content_hash")
    @classmethod
    def _check_content_hash_length(cls, v: str) -> str:
        if len(v) != 64:
            raise ValueError("content_hash must be 64 characters (sha256 hex)")
        return v

    @field_validator("mime")
    @classmethod
    def _check_mime_non_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("mime must not be empty")
        return v

    def natural_key(self) -> tuple[str, datetime]:
        return (self.url, self.publication_time)
