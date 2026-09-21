"""POSITION_AGGREGATE type tests（D09，MODEL-002.3）。

覆盖 POSITION_AGGREGATE 的合法实例化、
边界校验、失败拒绝及 natural_key() 契约。
"""

from datetime import date, datetime

import pytest
from pydantic import ValidationError

from chronoforge.models.enums import CanonicalType
from chronoforge.models.positioning import (
    POSITION_AGGREGATE,
    ParticipantType,
)


class TestPOSITION_AGGREGATE:
    """POSITION_AGGREGATE 合法/边界/失败用例（D02 §2）。"""

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

    def test_position_aggregate_valid(self) -> None:
        """POSITION_AGGREGATE 合法实例化。"""
        record = POSITION_AGGREGATE(**self._sample())
        assert record.contract == "GC"
        assert record.participant_type == ParticipantType.COMMERCIAL
        assert record.net_position == 20000.0

    def test_position_aggregate_non_commercial(self) -> None:
        """POSITION_AGGREGATE NON_COMMERCIAL 合法。"""
        record = POSITION_AGGREGATE(
            **self._sample(participant_type=ParticipantType.NON_COMMERCIAL)
        )
        assert record.participant_type == ParticipantType.NON_COMMERCIAL

    def test_position_aggregate_non_reportable(self) -> None:
        """POSITION_AGGREGATE NON_REPORTABLE 合法。"""
        record = POSITION_AGGREGATE(
            **self._sample(participant_type=ParticipantType.NON_REPORTABLE)
        )
        assert record.participant_type == ParticipantType.NON_REPORTABLE

    def test_position_aggregate_net_position_correct(self) -> None:
        """POSITION_AGGREGATE net_position = long - short 一致。"""
        rec = POSITION_AGGREGATE(**self._sample(
            long_positions=100.0,
            short_positions=40.0,
            net_position=60.0,
        ))
        assert rec.net_position == 60.0

    def test_position_aggregate_net_position_mismatch_rejected(self) -> None:
        """GWT: POSITION_AGGREGATE net_position != long - short → ValidationError。"""
        with pytest.raises(ValidationError):
            POSITION_AGGREGATE(**self._sample(
                long_positions=100.0,
                short_positions=40.0,
                net_position=999.0,  # should be 60
            ))

    def test_position_aggregate_leap_year_feb29_ok(self) -> None:
        """POSITION_AGGREGATE 2/29 report_date（闰年）合法。"""
        record = POSITION_AGGREGATE(
            **self._sample(report_date=date(2024, 2, 29))
        )
        assert record.report_date == date(2024, 2, 29)

    def test_position_aggregate_invalid_participant_type_rejected(self) -> None:
        """POSITION_AGGREGATE invalid ParticipantType → ValidationError。"""
        with pytest.raises(ValueError):
            POSITION_AGGREGATE(**self._sample(
                participant_type="BOGUS"  # type: ignore[arg-type]
            ))

    def test_position_aggregate_natural_key(self) -> None:
        """POSITION_AGGREGATE natural_key = (contract, report_date, participant_type, revision_time)。"""  # noqa: E501
        rec = POSITION_AGGREGATE(**self._sample())
        assert rec.natural_key() == (
            "GC",
            date(2026, 9, 5),
            ParticipantType.COMMERCIAL,
            self._now,
        )

    def test_canonical_type_registered(self) -> None:
        """POSITION_AGGREGATE 在 CanonicalType 枚举中存在。"""
        assert hasattr(CanonicalType, "POSITION_AGGREGATE")
        assert CanonicalType.POSITION_AGGREGATE.value == "POSITION_AGGREGATE"

    def test_position_aggregate_zero_positions(self) -> None:
        """POSITION_AGGREGATE zero positions → net_position=0。"""
        rec = POSITION_AGGREGATE(**self._sample(
            long_positions=0.0,
            short_positions=0.0,
            net_position=0.0,
            spreading=0.0,
        ))
        assert rec.net_position == 0.0

    def test_position_aggregate_large_values(self) -> None:
        """POSITION_AGGREGATE 大数值合法。"""
        rec = POSITION_AGGREGATE(**self._sample(
            long_positions=1e9,
            short_positions=5e8,
            net_position=5e8,
        ))
        assert rec.net_position == 5e8


class TestParticipantTypeExport:
    """ParticipantType 导出名称一致性（IMP-007）。"""

    def test_participant_type_class_exists(self) -> None:
        """ParticipantType 类可正确导入。"""
        assert ParticipantType.COMMERCIAL is not None

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
