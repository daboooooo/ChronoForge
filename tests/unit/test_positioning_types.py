"""Positioning type tests（D09，MODEL-002.3）。

覆盖 POSITION/ParticipantType 的合法实例化、
边界校验、失败拒绝及 natural_key() 契约。
"""

from datetime import date, datetime

import pytest
from pydantic import ValidationError

from chronoforge.models.positioning import POSITION, ParticipantType


class TestParticipantType:
    """ParticipantType 枚举合法值遍历（D02 §2）。"""

    def test_all_values(self) -> None:
        """ParticipantType 全枚举值遍历。"""
        assert list(ParticipantType) == [
            ParticipantType.COMMERCIAL,
            ParticipantType.NON_COMMERCIAL,
            ParticipantType.NON_REPORTABLE,
        ]

    def test_invalid_value(self) -> None:
        """ParticipantType 非法值拒绝。"""
        with pytest.raises(ValueError):
            ParticipantType("BOGUS")


class TestPOSITION:
    """POSITION 合法/边界/失败用例（D02 §2）。"""

    _now = datetime(2026, 9, 11, 15, 0, 0)  # 15:00 ET

    def _sample(self, **overrides) -> dict:
        base = {
            "source_id": "cftc-cot",
            "raw_record_id": "cot:test:file.jsonl:1",
            "report_date": date(2026, 9, 5),
            "release_time": self._now,
            "contract": "GC",
            "participant_type": ParticipantType.COMMERCIAL,
            "long_positions": 50000.0,
            "short_positions": 30000.0,
            "spreading": 1000.0,
            "net_position": 20000.0,  # 50000 - 30000 = 20000
            "revision_time": self._now,
            "schema_version": "1.0",
            "source": "cftc_cot",
            "source_timestamp": self._now,
            "ingest_timestamp": self._now,
        }
        base.update(overrides)
        return base

    def test_position_valid(self) -> None:
        """POSITION 合法实例化。"""
        record = POSITION(**self._sample())
        assert record.contract == "GC"
        assert record.participant_type == ParticipantType.COMMERCIAL
        assert record.net_position == 20000.0

    def test_position_non_commercial(self) -> None:
        """POSITION NON_COMMERCIAL 合法。"""
        record = POSITION(**self._sample(participant_type=ParticipantType.NON_COMMERCIAL))
        assert record.participant_type == ParticipantType.NON_COMMERCIAL

    def test_position_non_reportable(self) -> None:
        """POSITION NON_REPORTABLE 合法。"""
        record = POSITION(**self._sample(participant_type=ParticipantType.NON_REPORTABLE))
        assert record.participant_type == ParticipantType.NON_REPORTABLE

    def test_position_net_position_correct(self) -> None:
        """POSITION net_position = long - short 一致。"""
        rec = POSITION(**self._sample(
            long_positions=100.0,
            short_positions=40.0,
            net_position=60.0,
        ))
        assert rec.net_position == 60.0

    def test_position_net_position_mismatch_rejected(self) -> None:
        """GWT: POSITION net_position != long - short → ValidationError。"""
        with pytest.raises(ValidationError):
            POSITION(**self._sample(
                long_positions=100.0,
                short_positions=40.0,
                net_position=999.0,  # should be 60
            ))

    def test_position_leap_year_feb29_ok(self) -> None:
        """POSITION 2/29 report_date（闰年）合法。"""
        record = POSITION(**self._sample(report_date=date(2024, 2, 29)))
        assert record.report_date == date(2024, 2, 29)

    def test_position_invalid_participant_type_rejected(self) -> None:
        """POSITION invalid ParticipantType → ValidationError。"""
        with pytest.raises(ValueError):
            POSITION(**self._sample(participant_type="BOGUS"))  # type: ignore[arg-type]

    def test_position_natural_key(self) -> None:
        """POSITION natural_key = (contract, report_date, participant_type, revision_time)。"""
        rec = POSITION(**self._sample())
        assert rec.natural_key() == (
            "GC", date(2026, 9, 5), ParticipantType.COMMERCIAL, self._now,
        )
