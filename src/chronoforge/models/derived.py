"""Derived canonical type schemas（D02 §2 derived.py，MODEL-002.4）。

包含 DERIVED/FEATURE。全部继承 BaseRecord。
"""

from __future__ import annotations

import math
from datetime import datetime
from typing import Any

from pydantic import Field, field_validator

from chronoforge.models.base import BaseRecord


class DERIVED(BaseRecord):
    """衍生数据（D02 §2）。

    身份键 = (name, computed_at)。
    dependencies 非空、value 有限。
    """

    name: str
    computed_at: datetime
    dependencies: list[tuple[str, str]]
    value: float
    params: dict[str, Any]
    source_type: str

    @field_validator("value", mode="before")
    @classmethod
    def _check_value_finite(cls, v: Any) -> Any:
        if v is not None and not math.isfinite(v):
            raise ValueError("value must be finite (not NaN/Inf)")
        return v

    @field_validator("dependencies")
    @classmethod
    def _check_dependencies_non_empty(cls, v: list[tuple[str, str]]) -> list[tuple[str, str]]:
        if len(v) <= 0:
            raise ValueError("dependencies must have at least one entry")
        return v

    def natural_key(self) -> tuple[str, datetime]:
        return (self.name, self.computed_at)


class FEATURE(BaseRecord):
    """特征数据（D02 §2，同 DERIVED 结构，增加 feature_engine_version）。

    身份键 = (name, computed_at)。
    """

    name: str
    computed_at: datetime
    dependencies: list[tuple[str, str]]
    value: float
    params: dict[str, Any]
    feature_engine_version: str = Field(min_length=1)

    @field_validator("value", mode="before")
    @classmethod
    def _check_value_finite(cls, v: Any) -> Any:
        if v is not None and not math.isfinite(v):
            raise ValueError("value must be finite (not NaN/Inf)")
        return v

    @field_validator("dependencies")
    @classmethod
    def _check_dependencies_non_empty(cls, v: list[tuple[str, str]]) -> list[tuple[str, str]]:
        if len(v) <= 0:
            raise ValueError("dependencies must have at least one entry")
        return v

    def natural_key(self) -> tuple[str, datetime]:
        return (self.name, self.computed_at)
