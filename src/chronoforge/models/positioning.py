"""Positioning canonical type schemas（D02 §2 positioning.py，MODEL-002.3）。

包含 POSITION（COT 持仓报告）、POSITION_AGGREGATE 及 ParticipantType 枚举。全部继承 BaseRecord。
"""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Any

from pydantic import field_validator

from chronoforge.models.base import BaseRecord


class ParticipantType(str, Enum):  # noqa: UP042
    """COT 参与者类型（D02 §2）。"""

    COMMERCIAL = "COMMERCIAL"
    NON_COMMERCIAL = "NON_COMMERCIAL"
    NON_REPORTABLE = "NON_REPORTABLE"


class POSITION(BaseRecord):
    """COT 持仓报告（D02 §2）。

    身份键 = (contract, report_date, participant_type, revision_time)
    （D03 §3：含 revision_time，天然多版本）。
    net_position 计算字段 = long_positions - short_positions。
    """

    report_date: date
    release_time: datetime
    contract: str
    participant_type: ParticipantType
    long_positions: float
    short_positions: float
    spreading: float
    net_position: float
    revision_time: datetime

    @field_validator("net_position")
    @classmethod
    def _check_net_position_consistency(cls, v: float, info: Any) -> float:
        data = info.data if hasattr(info, "data") else getattr(info, "data", {})
        lp = data.get("long_positions") if isinstance(data, dict) else None
        sp = data.get("short_positions") if isinstance(data, dict) else None
        if lp is not None and sp is not None:
            expected = lp - sp
            if abs(v - expected) > 1e-9:
                raise ValueError(
                    f"net_position must be long_positions - short_positions = {expected}, got {v}"
                )
        return v

    def natural_key(self) -> tuple[str, date, ParticipantType, datetime]:
        return (self.contract, self.report_date, self.participant_type, self.revision_time)


class POSITION_AGGREGATE(BaseRecord):
    """COT 持仓聚合报告（D02 §2）。

    与 POSITION 同构，表示按 participant_type 聚合后的汇总数据。
    身份键 = (contract, report_date, participant_type, revision_time)
    （D03 §3：含 revision_time，天然多版本）。
    net_position 计算字段 = long_positions - short_positions。
    """

    report_date: date
    release_time: datetime
    contract: str
    participant_type: ParticipantType
    long_positions: float
    short_positions: float
    spreading: float
    net_position: float
    revision_time: datetime

    @field_validator("net_position")
    @classmethod
    def _check_net_position_consistency(cls, v: float, info: Any) -> float:
        data = info.data if hasattr(info, "data") else getattr(info, "data", {})
        lp = data.get("long_positions") if isinstance(data, dict) else None
        sp = data.get("short_positions") if isinstance(data, dict) else None
        if lp is not None and sp is not None:
            expected = lp - sp
            if abs(v - expected) > 1e-9:
                raise ValueError(
                    f"net_position must be long_positions - short_positions = {expected}, got {v}"
                )
        return v

    def natural_key(self) -> tuple[str, date, ParticipantType, datetime]:
        return (self.contract, self.report_date, self.participant_type, self.revision_time)
