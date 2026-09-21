"""Text canonical type schemas（D02 §2 text.py，MODEL-002.3）。

包含 TEXT_MESSAGE/TEXT_EVENT。全部继承 BaseRecord。
"""

from __future__ import annotations

from datetime import datetime

from pydantic import field_validator

from chronoforge.models.base import BaseRecord


class TEXT_MESSAGE(BaseRecord):
    """文本消息（D02 §2）。

    身份键 = (source_id, event_time, author)。
    """

    source_id: str
    event_time: datetime
    author: str
    text_hash: str
    language: str

    @field_validator("text_hash")
    @classmethod
    def _check_text_hash_length(cls, v: str) -> str:
        if len(v) != 64:
            raise ValueError("text_hash must be 64 characters (sha256 hex)")
        return v

    @field_validator("language")
    @classmethod
    def _check_language_length(cls, v: str) -> str:
        if len(v) != 2:
            raise ValueError("language must be 2 characters (ISO 639-1)")
        return v

    def natural_key(self) -> tuple[str, datetime, str]:
        return (self.source_id, self.event_time, self.author)


class TEXT_EVENT(BaseRecord):
    """文本事件（D02 §2，GDELT）。

    身份键 = (event_time, gkg_themes)。
    """

    event_time: datetime
    gkg_themes: list[str]
    entities: list[str]

    def natural_key(self) -> tuple[datetime, tuple[str, ...]]:
        # themes 排序后哈希
        return (self.event_time, tuple(sorted(self.gkg_themes)))
