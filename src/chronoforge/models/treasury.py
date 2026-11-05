"""Treasury canonical types（TGA 子系统，D02/TGA 契约变更）。

包含 5 个类型：
- TREASURY_ACCOUNT_BALANCE：DTS Table I 账户余额行
- TREASURY_CASH_FLOW：DTS Table II/IV/V 现金流行
- TREASURY_DEBT_TRANSACTION：DTS Table IIIA/IIIB 债务交易行
- TREASURY_AUCTION：Treasury 拍卖公告/结果
- TREASURY_ANNOUNCEMENT：Quarterly Refunding 结构化公告（revision 类）

全部继承 BaseRecord，金额统一 `*_usd_millions`，同时保留 original_value/unit。
时间语义显式声明；不使用裸 timestamp。
"""

from __future__ import annotations

import math
from datetime import date, datetime
from typing import Literal

from pydantic import field_validator, model_validator

from chronoforge.models.base import BaseRecord


class TreasuryRecord(BaseRecord):
    """Treasury 记录公共校验：有限数值、置信度范围。"""

    @model_validator(mode="after")
    def _validate_numeric_fields(self) -> TreasuryRecord:
        for name, value in self.__dict__.items():
            if isinstance(value, float) and not math.isfinite(value):
                raise ValueError(f"{name} must be finite (not NaN/Inf)")
            if name.endswith("_confidence") and isinstance(value, float):
                if not 0.0 <= value <= 1.0:
                    raise ValueError(f"{name} must be in [0, 1]")
        return self


class TREASURY_ACCOUNT_BALANCE(TreasuryRecord):
    """DTS Table I 账户余额行。

    身份键 = (source_id, observation_time, source_table, account_type_raw,
              src_line_nbr, balance_type)。

    历史分期：2022-04-18 前 TGA 余额由单行携带 opening/closing，同一源行
    会拆成 OPENING/CLOSING 两条记录，故 balance_type 必须参与身份键。
    """

    observation_time: datetime
    account_type_raw: str
    balance_type: Literal[
        "OPENING", "CLOSING", "TOTAL_DEPOSITS", "TOTAL_WITHDRAWALS"
    ]
    amount_usd_millions: float
    original_value: float
    original_unit: str
    is_tga: bool
    source_table: str
    src_line_nbr: int
    release_time: datetime
    release_time_source: Literal["OBSERVED", "ESTIMATED"]

    @field_validator("original_unit")
    @classmethod
    def _unit_not_empty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("original_unit must not be empty")
        return value

    def natural_key(self) -> tuple[str, datetime, str, str, int, str]:
        return (
            self.source_id,
            self.observation_time,
            self.source_table,
            self.account_type_raw,
            self.src_line_nbr,
            self.balance_type,
        )


class TREASURY_CASH_FLOW(TreasuryRecord):
    """DTS Table II/IV/V 现金流行。

    身份键 = (source_id, observation_time, source_table, transaction_type_raw,
              raw_category, src_line_nbr)。
    """

    observation_time: datetime
    account_type_raw: str
    transaction_type_raw: str
    source_table: str
    raw_category: str
    raw_category_desc: str | None
    category: str
    subcategory: str | None
    flow_type: Literal[
        "TAX", "DEBT_ISSUANCE", "DEBT_REDEMPTION", "INTEREST", "BENEFIT",
        "OPERATING", "REFUND", "INTERNAL", "OTHER", "UNCLASSIFIED",
    ]
    direction: Literal["INFLOW", "OUTFLOW", "INTERNAL", "UNKNOWN"]
    amount_usd_millions: float
    original_value: float
    original_unit: str
    is_internal_transfer: bool
    is_duplicate_candidate: bool
    is_subtotal: bool
    include_in_net: bool
    classification_confidence: float
    taxonomy_version: str
    src_line_nbr: int
    release_time: datetime
    release_time_source: Literal["OBSERVED", "ESTIMATED"]

    def natural_key(self) -> tuple[str, datetime, str, str, str, int]:
        return (
            self.source_id,
            self.observation_time,
            self.source_table,
            self.transaction_type_raw,
            self.raw_category,
            self.src_line_nbr,
        )


class TREASURY_DEBT_TRANSACTION(TreasuryRecord):
    """DTS Table IIIA/IIIB 债务交易行。

    身份键 = (source_id, observation_time, source_table, transaction_type_raw,
              security_market, security_type, security_type_desc,
              adjustment_type, src_line_nbr)。
    """

    observation_time: datetime
    source_table: Literal["IIIA", "IIIB"]
    transaction_type_raw: str
    security_market: str | None
    security_type: str | None
    security_type_desc: str | None
    adjustment_type: str | None
    amount_basis: Literal["FACE", "CASH_ADJUSTMENT"]
    amount_usd_millions: float
    original_value: float
    original_unit: str
    is_marketable: bool
    is_internal: bool
    src_line_nbr: int
    release_time: datetime
    release_time_source: Literal["OBSERVED", "ESTIMATED"]

    def natural_key(
        self,
    ) -> tuple[
        str, datetime, str, str, str | None, str | None, str | None, str | None, int
    ]:
        return (
            self.source_id,
            self.observation_time,
            self.source_table,
            self.transaction_type_raw,
            self.security_market,
            self.security_type,
            self.security_type_desc,
            self.adjustment_type,
            self.src_line_nbr,
        )


class TREASURY_AUCTION(TreasuryRecord):
    """Treasury 拍卖公告/结果。

    身份键 = (source_id, auction_key)；auction_key 含 data_status，
    使 ANNOUNCED 与 RESULT 两种状态并存，供 PIT 回测选择。
    """

    auction_key: str
    cusip: str | None
    security_type: str
    security_term: str
    announcement_date: date
    auction_date: date
    issue_date: date
    maturity_date: date | None
    offering_amount_usd_millions: float | None
    total_accepted_usd_millions: float | None
    total_tendered_usd_millions: float | None
    interest_rate: float | None
    high_yield: float | None
    price_per_100: float | None
    interest_payment_frequency: str | None
    is_reopening: bool
    is_cmb: bool
    is_tips: bool
    is_frn: bool
    currently_outstanding_usd_millions: float | None
    data_status: Literal["ANNOUNCED", "RESULT"]
    release_time: datetime
    release_time_source: Literal["OBSERVED", "ESTIMATED"]

    @field_validator("auction_key")
    @classmethod
    def _auction_key_not_empty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("auction_key must not be empty")
        return value

    def natural_key(self) -> tuple[str, str]:
        return (self.source_id, self.auction_key)


class TREASURY_ANNOUNCEMENT(TreasuryRecord):
    """Quarterly Refunding 结构化公告（revision 类）。

    身份键 = (source_id, publication_time, document_type, forecast_period_start,
              forecast_period_end, security_type, maturity_bucket, revision_time)。
    """

    publication_time: datetime
    document_type: Literal[
        "REFUNDING_STATEMENT", "BORROWING_ESTIMATE", "FINANCING_REQUIREMENT",
        "CASH_BALANCE_ASSUMPTION", "TBAC", "OTHER",
    ]
    forecast_period_start: date | None
    forecast_period_end: date | None
    security_type: str | None
    maturity_bucket: str | None
    borrowing_estimate_usd_millions: float | None
    cash_balance_assumption_usd_millions: float | None
    financing_requirement_usd_millions: float | None
    source_url: str
    extraction_method: Literal["HTML_TABLE", "HTML_TEXT", "PDF_TEXT", "MANUAL"]
    extraction_confidence: float
    revision_time: datetime
    release_time: datetime
    release_time_source: Literal["OBSERVED", "ESTIMATED"]

    def natural_key(
        self,
    ) -> tuple[
        str, datetime, str, date | None, date | None, str | None, str | None, datetime
    ]:
        return (
            self.source_id,
            self.publication_time,
            self.document_type,
            self.forecast_period_start,
            self.forecast_period_end,
            self.security_type,
            self.maturity_bucket,
            self.revision_time,
        )
