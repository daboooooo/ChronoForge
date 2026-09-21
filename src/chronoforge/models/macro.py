"""Macro canonical type schemas（D02 §2 macro.py，MODEL-002.3）。

包含 NUMBER/FLOW/MACRO_EVENT。全部继承 BaseRecord。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import field_validator

from chronoforge.models.base import BaseRecord


class NUMBER(BaseRecord):
    """FRED 存量/序列指标（D02 §2）。

    身份键 = (source_id, observation_time, revision_time)。
    """

    source_id: str
    observation_time: datetime
    release_time: datetime
    revision_time: datetime
    value: float | None
    units: str
    seasonal_adjustment: str
    vintage_date: str

    @field_validator("value", mode="before")
    @classmethod
    def _check_value_finite(cls, v: Any) -> Any:
        if v is not None:
            import math
            if not math.isfinite(v):
                raise ValueError("must be finite (not NaN/Inf)")
        return v

    @field_validator("release_time")
    @classmethod
    def _check_release_after_observation(cls, v: datetime, info: Any) -> datetime:
        data = info.data if hasattr(info, "data") else getattr(info, "data", {})
        obs = data.get("observation_time") if isinstance(data, dict) else None
        if obs and v < obs:
            raise ValueError("release_time must be >= observation_time")
        return v

    @field_validator("revision_time")
    @classmethod
    def _check_revision_after_observation(cls, v: datetime, info: Any) -> datetime:
        data = info.data if hasattr(info, "data") else getattr(info, "data", {})
        obs = data.get("observation_time") if isinstance(data, dict) else None
        if obs and v < obs:
            raise ValueError("revision_time must be >= observation_time")
        return v

    def natural_key(self) -> tuple[str, datetime, datetime]:
        return (self.source_id, self.observation_time, self.revision_time)


class FLOW(BaseRecord):
    """FRED 流量指标（D02 §2，NUMBER + period_start/period_end）。

    身份键 = (source_id, observation_time, revision_time)。
    """

    source_id: str
    observation_time: datetime
    release_time: datetime
    revision_time: datetime
    value: float | None
    units: str
    seasonal_adjustment: str
    vintage_date: str
    period_start: datetime
    period_end: datetime

    @field_validator("value", mode="before")
    @classmethod
    def _check_value_finite(cls, v: Any) -> Any:
        if v is not None:
            import math
            if not math.isfinite(v):
                raise ValueError("must be finite (not NaN/Inf)")
        return v

    @field_validator("release_time")
    @classmethod
    def _check_release_after_observation(cls, v: datetime, info: Any) -> datetime:
        data = info.data if hasattr(info, "data") else getattr(info, "data", {})
        obs = data.get("observation_time") if isinstance(data, dict) else None
        if obs and v < obs:
            raise ValueError("release_time must be >= observation_time")
        return v

    @field_validator("revision_time")
    @classmethod
    def _check_revision_after_observation(cls, v: datetime, info: Any) -> datetime:
        data = info.data if hasattr(info, "data") else getattr(info, "data", {})
        obs = data.get("observation_time") if isinstance(data, dict) else None
        if obs and v < obs:
            raise ValueError("revision_time must be >= observation_time")
        return v

    @field_validator("period_end")
    @classmethod
    def _check_period_end_after_start(cls, v: datetime, info: Any) -> datetime:
        data = info.data if hasattr(info, "data") else getattr(info, "data", {})
        ps = data.get("period_start") if isinstance(data, dict) else None
        if ps and v <= ps:
            raise ValueError("period_end must be > period_start")
        return v

    def natural_key(self) -> tuple[str, datetime, datetime]:
        return (self.source_id, self.observation_time, self.revision_time)


class MACRO_EVENT(BaseRecord):
    """宏观经济事件（D02 §2）。

    身份键 = (event_ref, scheduled_time)。
    """

    event_ref: str
    scheduled_time: datetime
    actual_time: datetime | None
    actual: str | None
    forecast: str | None
    previous: str | None

    @field_validator("actual_time")
    @classmethod
    def _check_actual_after_scheduled(cls, v: datetime, info: Any) -> datetime:
        data = info.data if hasattr(info, "data") else getattr(info, "data", {})
        sched = data.get("scheduled_time") if isinstance(data, dict) else None
        if v is not None and sched and v < sched:
            raise ValueError("actual_time must be >= scheduled_time")
        return v

    def natural_key(self) -> tuple[str, datetime]:
        return (self.event_ref, self.scheduled_time)
