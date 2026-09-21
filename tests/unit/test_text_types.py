"""Text type tests（D09，MODEL-002.3）。

覆盖 TEXT_MESSAGE/TEXT_EVENT 的合法实例化、
边界校验、失败拒绝及 natural_key() 契约。
"""

from datetime import datetime

import pytest
from pydantic import ValidationError

from chronoforge.models.text import TEXT_EVENT, TEXT_MESSAGE


class TestTEXT_MESSAGE:
    """TEXT_MESSAGE 合法/边界/失败用例（D02 §2）。"""

    _now = datetime(2026, 9, 11, 12, 0, 0)

    def _sample(self, **overrides) -> dict:
        base = {
            "source_id": "gdelthttp-20260911",
            "raw_record_id": "gdt:test:file.jsonl:1",
            "event_time": self._now,
            "author": "gordon",
            "text_hash": "a" * 64,
            "language": "en",
            "schema_version": "1.0",
            "source": "gdelthttp",
            "source_timestamp": self._now,
            "ingest_timestamp": self._now,
        }
        base.update(overrides)
        return base

    def test_text_message_valid(self) -> None:
        """TEXT_MESSAGE 合法实例化。"""
        record = TEXT_MESSAGE(**self._sample())
        assert record.author == "gordon"
        assert record.language == "en"

    def test_text_message_text_hash_64_ok(self) -> None:
        """TEXT_MESSAGE text_hash=64 字符合法。"""
        record = TEXT_MESSAGE(**self._sample(text_hash="b" * 64))
        assert len(record.text_hash) == 64

    def test_text_message_text_hash_63_rejected(self) -> None:
        """GWT: TEXT_MESSAGE text_hash 长度≠64 → ValidationError。"""
        with pytest.raises(ValidationError):
            TEXT_MESSAGE(**self._sample(text_hash="a" * 63))

    def test_text_message_text_hash_65_rejected(self) -> None:
        """TEXT_MESSAGE text_hash=65 字符 → ValidationError。"""
        with pytest.raises(ValidationError):
            TEXT_MESSAGE(**self._sample(text_hash="a" * 65))

    def test_text_message_language_2_chars_ok(self) -> None:
        """TEXT_MESSAGE language="en"（2 字符）合法。"""
        record = TEXT_MESSAGE(**self._sample(language="en"))
        assert record.language == "en"

    def test_text_message_language_zh_ok(self) -> None:
        """TEXT_MESSAGE language="zh" 合法。"""
        record = TEXT_MESSAGE(**self._sample(language="zh"))
        assert record.language == "zh"

    def test_text_message_language_3_chars_rejected(self) -> None:
        """TEXT_MESSAGE language="eng"（3 字符）→ ValidationError。"""
        with pytest.raises(ValidationError):
            TEXT_MESSAGE(**self._sample(language="eng"))

    def test_text_message_language_empty_rejected(self) -> None:
        """TEXT_MESSAGE language="" → ValidationError。"""
        with pytest.raises(ValidationError):
            TEXT_MESSAGE(**self._sample(language=""))

    def test_text_message_natural_key(self) -> None:
        """TEXT_MESSAGE natural_key = (source_id, event_time, author)。"""
        rec = TEXT_MESSAGE(**self._sample())
        assert rec.natural_key() == ("gdelthttp-20260911", self._now, "gordon")


class TestTEXT_EVENT:
    """TEXT_EVENT 合法/边界/失败用例（D02 §2）。"""

    _now = datetime(2026, 9, 11, 12, 0, 0)

    def _sample(self, **overrides) -> dict:
        base = {
            "source_id": "gdt-event",
            "raw_record_id": "gdt:test:file.jsonl:1",
            "event_time": self._now,
            "gkg_themes": ["ECONOMY", "POLITICS"],
            "entities": ["GPE:USA", "PERSON:Biden"],
            "schema_version": "1.0",
            "source": "gdelthttp",
            "source_timestamp": self._now,
            "ingest_timestamp": self._now,
        }
        base.update(overrides)
        return base

    def test_text_event_valid(self) -> None:
        """TEXT_EVENT 合法实例化。"""
        record = TEXT_EVENT(**self._sample())
        assert record.gkg_themes == ["ECONOMY", "POLITICS"]
        assert record.entities == ["GPE:USA", "PERSON:Biden"]

    def test_text_event_empty_themes_ok(self) -> None:
        """TEXT_EVENT empty gkg_themes 合法。"""
        record = TEXT_EVENT(**self._sample(gkg_themes=[]))
        assert record.gkg_themes == []

    def test_text_event_empty_entities_ok(self) -> None:
        """TEXT_EVENT empty entities 合法。"""
        record = TEXT_EVENT(**self._sample(entities=[]))
        assert record.entities == []

    def test_text_event_natural_key(self) -> None:
        """TEXT_EVENT natural_key = (event_time, sorted(gkg_themes))。"""
        rec = TEXT_EVENT(**self._sample())
        expected_themes = tuple(sorted(["ECONOMY", "POLITICS"]))
        assert rec.natural_key() == (self._now, expected_themes)

    def test_text_event_natural_key_sorted(self) -> None:
        """TEXT_EVENT themes 排序后作为 key 的一部分。"""
        rec = TEXT_EVENT(**self._sample(gkg_themes=["ZEBRA", "ALPHA"]))
        # 排序后应该是 ("ALPHA", "ZEBRA")
        assert rec.natural_key()[1] == ("ALPHA", "ZEBRA")
