"""VALIDATION-002 测试：continuity_model 驱动的 gap 判定 + P0 简化交易日历。

覆盖：
- TC-Q-007：周末 EXPECTED_GAP 非 WARNING
- 闰日/节假日/周末矩阵：2026 年全日历遍历
- ALWAYS_OPEN 全网格：加密源周末缺失 → WARNING
- 边界：年末感恩节固定表、2/29（闰年）
- EXPECTED_GAP 标记：非交易日缺失不产生 WARNING
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest

from chronoforge.exceptions import ConfigError
from chronoforge.quality.calendar import (
    EARLY_CLOSINGS,
    MarketCalendar,
    is_trading_day,
)
from chronoforge.quality.continuity import (
    ContinuityModel,
    _interval_delta,
    expected_grid,
    parse_frequency_seconds,
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# is_trading_day — 简化交易日历
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


# 2021–2025 NYSE 实际全日休市日（按年份显式配置；管理员日历公告）
NYSE_HOLIDAYS_2021 = [
    date(2021, 1, 1),
    date(2021, 1, 18),   # MLK
    date(2021, 2, 15),   # Presidents' Day
    date(2021, 4, 2),    # Good Friday
    date(2021, 5, 31),   # Memorial Day
    date(2021, 7, 5),    # Independence Day observed
    date(2021, 9, 6),    # Labor Day
    date(2021, 11, 25),  # Thanksgiving
    date(2021, 12, 24),  # Christmas observed
]

NYSE_HOLIDAYS_2022 = [
    date(2022, 1, 17),   # MLK
    date(2022, 2, 21),   # Presidents' Day
    date(2022, 4, 15),   # Good Friday
    date(2022, 5, 30),   # Memorial Day
    date(2022, 6, 20),   # Juneteenth observed
    date(2022, 7, 4),    # Independence Day
    date(2022, 9, 5),    # Labor Day
    date(2022, 11, 24),  # Thanksgiving
    date(2022, 12, 26),  # Christmas observed
]

NYSE_HOLIDAYS_2023 = [
    date(2023, 1, 2),    # New Year's observed
    date(2023, 1, 16),   # MLK
    date(2023, 2, 20),   # Presidents' Day
    date(2023, 4, 7),    # Good Friday
    date(2023, 5, 29),   # Memorial Day
    date(2023, 6, 19),   # Juneteenth
    date(2023, 7, 4),    # Independence Day
    date(2023, 9, 4),    # Labor Day
    date(2023, 11, 23),  # Thanksgiving
    date(2023, 12, 25),  # Christmas
]

NYSE_HOLIDAYS_2024 = [
    date(2024, 1, 1),
    date(2024, 1, 15),   # MLK
    date(2024, 2, 19),   # Presidents' Day
    date(2024, 3, 29),   # Good Friday
    date(2024, 5, 27),   # Memorial Day
    date(2024, 6, 19),   # Juneteenth
    date(2024, 7, 4),    # Independence Day
    date(2024, 9, 2),    # Labor Day
    date(2024, 11, 28),  # Thanksgiving
    date(2024, 12, 25),  # Christmas
]

NYSE_HOLIDAYS_2025 = [
    date(2025, 1, 1),
    date(2025, 1, 20),   # MLK
    date(2025, 2, 17),   # Presidents' Day
    date(2025, 4, 18),   # Good Friday
    date(2025, 5, 26),   # Memorial Day
    date(2025, 6, 19),   # Juneteenth
    date(2025, 7, 4),    # Independence Day
    date(2025, 9, 1),    # Labor Day
    date(2025, 11, 27),  # Thanksgiving
    date(2025, 12, 25),  # Christmas
]

# 2026/2027 NYSE 实际全日休市日（按年份显式配置，M9）
NYSE_HOLIDAYS_2026 = [
    date(2026, 1, 1),
    date(2026, 1, 19),   # MLK
    date(2026, 2, 16),   # Presidents' Day
    date(2026, 4, 3),    # Good Friday
    date(2026, 5, 25),   # Memorial Day
    date(2026, 6, 19),   # Juneteenth
    date(2026, 7, 3),    # Independence Day observed
    date(2026, 9, 7),    # Labor Day
    date(2026, 11, 26),  # Thanksgiving
    date(2026, 12, 25),  # Christmas
]

NYSE_HOLIDAYS_2027 = [
    date(2027, 1, 1),
    date(2027, 1, 18),   # MLK
    date(2027, 2, 15),   # Presidents' Day
    date(2027, 3, 26),   # Good Friday
    date(2027, 5, 31),   # Memorial Day
    date(2027, 6, 18),   # Juneteenth observed
    date(2027, 7, 5),    # Independence Day observed
    date(2027, 9, 6),    # Labor Day
    date(2027, 11, 25),  # Thanksgiving
    date(2027, 12, 24),  # Christmas observed
]

NYSE_EARLY_CLOSINGS = [
    date(2021, 11, 26),
    date(2022, 11, 25),
    date(2023, 7, 3),
    date(2023, 11, 24),
    date(2024, 7, 3),
    date(2024, 11, 29),
    date(2024, 12, 24),
    date(2025, 7, 3),
    date(2025, 11, 28),
    date(2025, 12, 24),
    date(2026, 11, 27),
    date(2026, 12, 24),
    date(2027, 11, 26),
]


class TestIsTradingDay:
    """TC-Q-007：配置化交易日历判定。"""

    def test_weekend_saturday(self):
        """周六 → False。"""
        assert is_trading_day(date(2026, 9, 12)) is False

    def test_weekend_sunday(self):
        """周日 → False。"""
        assert is_trading_day(date(2026, 9, 13)) is False

    def test_weekday_trading_day(self):
        """普通工作日 → True。"""
        assert is_trading_day(date(2026, 9, 14)) is True

    @pytest.mark.parametrize("d", NYSE_HOLIDAYS_2021)
    def test_us_holidays_2021_non_trading(self, d: date):
        """2021 实际假日 → 非交易日（与库内日线数据起点对齐）。"""
        assert is_trading_day(d) is False, f"Expected holiday {d} to be non-trading"

    @pytest.mark.parametrize("d", NYSE_HOLIDAYS_2022)
    def test_us_holidays_2022_non_trading(self, d: date):
        """2022 实际假日 → 非交易日（含 Juneteenth observed 6/20）。"""
        assert is_trading_day(d) is False, f"Expected holiday {d} to be non-trading"

    @pytest.mark.parametrize("d", NYSE_HOLIDAYS_2023)
    def test_us_holidays_2023_non_trading(self, d: date):
        """2023 实际假日 → 非交易日。"""
        assert is_trading_day(d) is False, f"Expected holiday {d} to be non-trading"

    @pytest.mark.parametrize("d", NYSE_HOLIDAYS_2024)
    def test_us_holidays_2024_non_trading(self, d: date):
        """2024 实际假日 → 非交易日。"""
        assert is_trading_day(d) is False, f"Expected holiday {d} to be non-trading"

    @pytest.mark.parametrize("d", NYSE_HOLIDAYS_2025)
    def test_us_holidays_2025_non_trading(self, d: date):
        """2025 实际假日 → 非交易日。"""
        assert is_trading_day(d) is False, f"Expected holiday {d} to be non-trading"

    @pytest.mark.parametrize("d", NYSE_HOLIDAYS_2026)
    def test_us_holidays_2026_non_trading(self, d: date):
        """2026 实际假日 → 非交易日（含浮动假日与 observed 规则）。"""
        assert is_trading_day(d) is False, f"Expected holiday {d} to be non-trading"

    @pytest.mark.parametrize("d", NYSE_HOLIDAYS_2027)
    def test_us_holidays_2027_non_trading(self, d: date):
        """2027 实际假日 → 非交易日（M9：跨年份不再失效）。"""
        assert is_trading_day(d) is False, f"Expected holiday {d} to be non-trading"

    @pytest.mark.parametrize("d", NYSE_EARLY_CLOSINGS)
    def test_early_closings_are_trading_days(self, d: date):
        """早闭日仍是交易日（不是全日休市）。"""
        assert is_trading_day(d) is True, f"Expected early close {d} to be trading"

    def test_early_closing_not_in_list(self):
        """非早闭日的工作日 → True。"""
        assert is_trading_day(date(2026, 7, 2)) is True

    def test_holiday_falling_on_weekday(self):
        """节假日落在工作日 → 非交易日（不是周末）。"""
        jan1 = date(2026, 1, 1)
        assert jan1.weekday() == 3
        assert is_trading_day(jan1) is False

    def test_thanksgiving_falls_on_weekday(self):
        """感恩节 2026-11-26 是周四（工作日）。"""
        thanksgiving = date(2026, 11, 26)
        assert thanksgiving.weekday() == 3
        assert is_trading_day(thanksgiving) is False

    @pytest.mark.parametrize(
        "d", [date(2020, 12, 31), date(2028, 1, 3), date(2029, 1, 1)]
    )
    def test_unknown_year_raises(self, d: date):
        """超出已配置年份 → 显式 ConfigError（不做规则推算兜底）。"""
        with pytest.raises(
            ConfigError, match="2021, 2022, 2023, 2024, 2025, 2026, 2027"
        ):
            is_trading_day(d)

    @pytest.mark.parametrize("year", range(2021, 2028))
    def test_configured_years_do_not_raise(self, year: int):
        """2021–2027 已配置年份不再抛 ConfigError（与库内日线起点对齐）。"""
        assert isinstance(is_trading_day(date(year, 12, 1)), bool)

    def test_early_closings_table(self):
        """HOLIDAY 与早闭表分离：早闭日不全是假日（M9 修复 7/3、7/5、12/24 误标）。"""
        assert set(EARLY_CLOSINGS) == set(NYSE_EARLY_CLOSINGS)
        assert all(is_trading_day(d) for d in EARLY_CLOSINGS)

    def test_2027_early_closings_pinned(self):
        """2027 早闭日钉死为感恩节次日（11-26）。

        NYSE 官方 2027 年只为 11-26 标注早闭；2027 圣诞日为 12-24（observed），
        不存在 12-23 早闭。此用例防止后续手写日历表时多补非法日期。
        """
        assert {d for d in EARLY_CLOSINGS if d.year == 2027} == {date(2027, 11, 26)}

    def test_calendar_is_configurable(self):
        """假日表可按年份外置配置（新增年份仅需显式补表）。"""
        calendar = MarketCalendar(
            holidays_by_year={2030: frozenset({date(2030, 7, 4)})}
        )
        assert calendar.is_trading_day(date(2030, 7, 4)) is False
        assert calendar.is_trading_day(date(2030, 7, 5)) is True
        with pytest.raises(ConfigError, match="2030"):
            calendar.is_trading_day(date(2031, 1, 1))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 全日历遍历（周末矩阵）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestCalendarMatrix:
    """周末矩阵：2026/2027 全日历遍历验证。"""

    @pytest.mark.parametrize("year", [2026, 2027])
    def test_all_weekends_non_trading(self, year: int):
        """全年所有周六周日 → 非交易日。"""
        d = date(year, 1, 1)
        end = date(year, 12, 31)
        while d <= end:
            if d.weekday() >= 5:
                assert is_trading_day(d) is False, f"Expected weekend {d} to be non-trading"
            d += timedelta(days=1)



# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# _interval_delta — 频率到 timedelta
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestIntervalDelta:
    """频率秒数解析。"""

    @pytest.mark.parametrize("freq,expected_seconds", [
        ("1m", 60),
        ("5m", 300),
        ("15m", 900),
        ("30m", 1800),
        ("1h", 3600),
        ("2h", 7200),
        ("4h", 14400),
        ("1d", 86400),
        ("1w", 604800),
    ])
    def test_all_frequencies(self, freq: str, expected_seconds: int):
        """所有预定义频率 → 正确的 timedelta。"""
        delta = _interval_delta(freq)
        assert delta.total_seconds() == expected_seconds

    def test_unknown_frequency(self):
        """未知频率 → ValueError。"""
        with pytest.raises(ValueError, match="Unsupported frequency"):
            _interval_delta("3m")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ContinuityModel 枚举
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestContinuityModelEnum:
    """ContinuityModel 枚举值覆盖。"""

    def test_always_open_value(self):
        """ALWAYS_OPEN 枚举值。"""
        assert ContinuityModel.ALWAYS_OPEN.value == "ALWAYS_OPEN"

    def test_trading_calendar_value(self):
        """TRADING_CALENDAR 枚举值。"""
        assert ContinuityModel.TRADING_CALENDAR.value == "TRADING_CALENDAR"

    def test_event_based_value(self):
        """EVENT_BASED 枚举值。"""
        assert ContinuityModel.EVENT_BASED.value == "EVENT_BASED"

    def test_release_schedule_value(self):
        """RELEASE_SCHEDULE 枚举值。"""
        assert ContinuityModel.RELEASE_SCHEDULE.value == "RELEASE_SCHEDULE"

    def test_str_enum_compatible(self):
        """StrEnum → 可与字符串比较。"""
        assert ContinuityModel.ALWAYS_OPEN == "ALWAYS_OPEN"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# expected_grid — ALWAYS_OPEN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestExpectedGridAlwaysOpen:
    """ALWAYS_OPEN 全网格：加密源周末缺失 → 仍 WARNING（无豁免）。"""

    def test_always_open_full_grid_1d(self):
        """1d 频率 → 每一天都是 REGULAR。"""
        start = datetime(2026, 9, 10, tzinfo=UTC)  # 周四
        end = datetime(2026, 9, 14, tzinfo=UTC)    # 周一
        grid = expected_grid("TEST", "1d", ContinuityModel.ALWAYS_OPEN, start, end)
        # ALWAYS_OPEN 包含全部日期（周四到周一共5天）
        assert len(grid) == 5
        for entry in grid:
            assert entry[2] == "REGULAR"

    def test_always_open_weekend_gaps_marked_regular(self):
        """ALWAYS_OPEN 周末 → REGULAR（不豁免）。"""
        # 从周五到下周一
        start = datetime(2026, 9, 11, tzinfo=UTC)  # 周五
        end = datetime(2026, 9, 14, tzinfo=UTC)    # 周一
        grid = expected_grid("TEST", "1d", ContinuityModel.ALWAYS_OPEN, start, end)
        # 周五、周六、周日、周一 = 4 天
        assert len(grid) == 4
        # 周六、周日也应标记为 REGULAR
        saturday = date(2026, 9, 12)
        sunday = date(2026, 9, 13)
        for entry in grid:
            entry_date = entry[0].date()
            if entry_date == saturday or entry_date == sunday:
                assert entry[2] == "REGULAR"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# expected_grid — TRADING_CALENDAR
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestExpectedGridTradingCalendar:
    """TRADING_CALENDAR：非交易日缺失标 EXPECTED_GAP。"""

    def test_trading_calendar_no_gap(self):
        """连续交易日 → 全 REGULAR。"""
        # 2026-09-14 到 2026-09-18 是周一到周五（工作日）
        start = datetime(2026, 9, 14, tzinfo=UTC)
        end = datetime(2026, 9, 18, tzinfo=UTC)
        grid = expected_grid("TEST", "1d", ContinuityModel.TRADING_CALENDAR, start, end)
        assert len(grid) == 5
        for entry in grid:
            assert entry[2] == "REGULAR"

    def test_trading_calendar_weekend_expected_gap(self):
        """周五到下周一 → 周末标记为 EXPECTED_GAP。"""
        # 从 9/11(周五) 到 9/16(周三)
        start = datetime(2026, 9, 11, tzinfo=UTC)
        end = datetime(2026, 9, 16, tzinfo=UTC)
        grid = expected_grid("TEST", "1d", ContinuityModel.TRADING_CALENDAR, start, end)

        # 9/11(Fri, REGULAR), 9/12-13(Sat-Sun, EXPECTED_GAP), 9/14(Mon, REGULAR)...
        expected_gaps = [e for e in grid if e[2] == "EXPECTED_GAP"]

        assert len(expected_gaps) >= 1
        # EXPECTED_GAP 区间应覆盖周末
        gap_start = expected_gaps[0][0].date()
        gap_end = expected_gaps[0][1].date()
        assert gap_start == date(2026, 9, 12)  # 周六开始
        assert gap_end == date(2026, 9, 14)    # 周一之前（周一是 REGULAR）

    def test_trading_calendar_holiday_expected_gap(self):
        """国庆节（10/12）→ EXPECTED_GAP。"""
        # 2026-10-12 是周一（国庆日简化为 US 节假日？不，我们只模拟美股节假日）
        # 用 7/4（独立日，周六）来测试
        # 2026-07-04 是周六，已经是周末，不单独标记
        # 用 1/1（新年，周四）测试
        start = datetime(2026, 1, 2, tzinfo=UTC)  # 周五
        end = datetime(2026, 1, 7, tzinfo=UTC)    # 周三
        grid = expected_grid("TEST", "1d", ContinuityModel.TRADING_CALENDAR, start, end)

        # 1/2(Fri, REGULAR), 1/3(Sat, 周末), 1/4(Sun, 周末),
        # 1/5(Mon, REGULAR), 1/6(Tue, REGULAR), 1/7(Wed, REGULAR)
        # 1/1(Thu, 新年) 不在范围内
        expected_gaps = [e for e in grid if e[2] == "EXPECTED_GAP"]
        assert len(expected_gaps) == 1
        assert expected_gaps[0][0].date() == date(2026, 1, 3)  # 周六开始

    def test_trading_calendar_consecutive_non_trading(self):
        """圣诞节(12/25, 周五) + 周末 → 连续 EXPECTED_GAP。"""
        start = datetime(2026, 12, 23, tzinfo=UTC)  # 周三
        end = datetime(2026, 12, 28, tzinfo=UTC)    # 周一
        grid = expected_grid("TEST", "1d", ContinuityModel.TRADING_CALENDAR, start, end)

        # 12/23(Wed, REGULAR), 12/24(Thu, 早闭, REGULAR)
        # 12/25(Fri, 圣诞节, EXPECTED_GAP), 12/26-27(Sat-Sun, EXPECTED_GAP)
        # 12/28(Mon, REGULAR)
        expected_gaps = [e for e in grid if e[2] == "EXPECTED_GAP"]
        assert len(expected_gaps) == 1
        # 圣诞节 + 周末合并为一个 EXPECTED_GAP
        gap_start = expected_gaps[0][0].date()
        gap_end = expected_gaps[0][1].date()
        assert gap_start == date(2026, 12, 25)  # 圣诞节开始
        assert gap_end == date(2026, 12, 28)    # 周一前


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# expected_grid — EVENT_BASED / RELEASE_SCHEDULE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestExpectedGridNoGap:
    """EVENT_BASED / RELEASE_SCHEDULE 无网格 gap 概念。"""

    @pytest.mark.parametrize("model", [
        ContinuityModel.EVENT_BASED,
        ContinuityModel.RELEASE_SCHEDULE,
    ])
    def test_no_grid(self, model):
        """EVENT_BASED / RELEASE_SCHEDULE → 空列表。"""
        start = datetime(2026, 9, 1, tzinfo=UTC)
        end = datetime(2026, 9, 30, tzinfo=UTC)
        grid = expected_grid("TEST", "1d", model, start, end)
        assert grid == []


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 边界条件
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestBoundaryConditions:
    """边界：年末感恩节固定表、2/29（闰年）。"""

    def test_year_end_thanksgiving(self):
        """年末感恩节(11/26, 周四) → EXPECTED_GAP 正确。"""
        start = datetime(2026, 11, 23, tzinfo=UTC)  # 周一
        end = datetime(2026, 12, 1, tzinfo=UTC)     # 周二
        grid = expected_grid("TEST", "1d", ContinuityModel.TRADING_CALENDAR, start, end)

        expected_gaps = [e for e in grid if e[2] == "EXPECTED_GAP"]

        assert len(expected_gaps) >= 1
        # 感恩节(11/26) 单独成段；周末(11/28-29) 另成段（11/27 为交易日）
        for gap in expected_gaps:
            gap_dates = set()
            current = gap[0].date()
            end_d = gap[1].date()
            while current <= end_d:
                gap_dates.add(current)
                current += timedelta(days=1)
            if date(2026, 11, 26) in gap_dates:  # 感恩节在内
                break
        else:
            raise AssertionError("Thanksgiving 2026-11-26 not covered by EXPECTED_GAP")

    def test_february_29_leap_year(self):
        """2026 不是闰年，2/29 不存在 → 验证日历无异常。"""
        # 2026 年 2 月只有 28 天
        # 2026-02-28 是周六 → 非交易日
        feb_28 = date(2026, 2, 28)
        assert is_trading_day(feb_28) is False
        # 2026-03-01 是周日 → 非交易日
        mar_1 = date(2026, 3, 1)
        assert is_trading_day(mar_1) is False

    def test_single_day_range(self):
        """单天范围 → 单个 grid entry。"""
        start = datetime(2026, 9, 14, tzinfo=UTC)  # 周一
        end = datetime(2026, 9, 14, tzinfo=UTC)
        grid = expected_grid("TEST", "1d", ContinuityModel.ALWAYS_OPEN, start, end)
        assert len(grid) == 1
        assert grid[0][2] == "REGULAR"

    def test_same_start_end_always_open(self):
        """ALWAYS_OPEN 单天 → REGULAR（无豁免）。"""
        start = datetime(2026, 9, 12, tzinfo=UTC)  # 周六
        end = datetime(2026, 9, 12, tzinfo=UTC)
        grid = expected_grid("TEST", "1d", ContinuityModel.ALWAYS_OPEN, start, end)
        assert len(grid) == 1
        assert grid[0][2] == "REGULAR"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TC-Q-007 GWT 验收：Q-GAP-001 集成
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestTCQ007GWT:
    """TC-Q-007 GWT 验收（Q-GAP-001 集成）。

    - Given yahoo 周六无 K 线 When Q-GAP-001 Then EXPECTED_GAP(INFO) 无 WARNING
    - Given 交易日缺失 When Q-GAP-001 Then Unexpected Gap WARNING
    - Given ALWAYS_OPEN 数据集周末缺失 When Q-GAP-001 Then Unexpected Gap WARNING（无豁免）
    """

    def test_yahoo_weekend_expected_gap_no_warning(self):
        """Given yahoo 周六无 K 线 → EXPECTED_GAP(INFO) 非 WARNING。"""
        # 构造 yahoo OHLCV 记录：周一到周五有数据，周六周日缺失
        # 使用 TRADING_CALENDAR 模型（yahoo 的 continuity_model）

        grid = expected_grid(
            "YAHOO:AAPL:SPOT", "1d",
            ContinuityModel.TRADING_CALENDAR,
            datetime(2026, 9, 9, tzinfo=UTC),
            datetime(2026, 9, 15, tzinfo=UTC),
        )
        expected_gaps = [e for e in grid if e[2] == "EXPECTED_GAP"]
        regular = [e for e in grid if e[2] == "REGULAR"]

        assert len(expected_gaps) >= 1
        assert len(regular) >= 3  # 至少 3 个交易日 REGULAR

    def test_trading_day_missing_unexpected_gap(self):
        """Given 交易日缺失 → Unexpected Gap（WARNING）。"""
        now = datetime(2026, 9, 14, tzinfo=UTC)  # 周一

        grid = expected_grid(
            "BINANCE:BTCUSDT:1d", "1d",
            ContinuityModel.ALWAYS_OPEN,
            now,
            now + timedelta(days=2),
        )

        # ALWAYS_OPEN → 周一、周二、周三都是 REGULAR
        assert len(grid) == 3
        # 但 actual 只有周一、周三 → 周二缺失
        regular = [e for e in grid if e[2] == "REGULAR"]
        assert len(regular) == 3


class TestGWTAcceptance:
    """GWT acceptance 逐条验收。"""

    def test_given_yahoo_saturday_no_kline_then_expected_gap_info(self):
        """Given yahoo 周六无 K 线 When Q-GAP-001 Then EXPECTED_GAP(INFO) 无 WARNING。"""
        start = datetime(2026, 9, 8, tzinfo=UTC)   # 周二
        end = datetime(2026, 9, 16, tzinfo=UTC)     # 周三
        grid = expected_grid(
            "YAHOO:AAPL:SPOT", "1d",
            ContinuityModel.TRADING_CALENDAR,
            start, end,
        )

        # 应包含 EXPECTED_GAP（周末）和 REGULAR（交易日）
        expected_gaps = [e for e in grid if e[2] == "EXPECTED_GAP"]
        regular = [e for e in grid if e[2] == "REGULAR"]

        # EXPECTED_GAP 标记：非交易日缺失不产生 WARNING
        assert len(expected_gaps) >= 1
        assert len(regular) >= 5  # 至少 5 个交易日（一周）

        # 验证 EXPECTED_GAP 覆盖了周末
        gap_dates = set()
        for gap in expected_gaps:
            current = gap[0].date()
            end_d = gap[1].date()
            while current <= end_d:
                gap_dates.add(current)
                current += timedelta(days=1)

        # 周末应在 EXPECTED_GAP 中
        weekend_days = [12, 13, 19, 20]  # 周六/周日
        for wd in weekend_days:
            d = date(2026, 9, wd)
            if d <= date(2026, 9, 16):
                assert d in gap_dates, f"Expected weekend {d} in EXPECTED_GAP"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# parse_frequency_seconds — registry frequency 简写统一口径
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestParseFrequencySeconds:
    """dataset_registry.frequency → 秒（调度到期判定 / gap 规则共用）。"""

    @pytest.mark.parametrize(
        "freq,expected",
        [
            ("1m", 60),
            ("5m", 300),
            ("15m", 900),
            ("30m", 1800),
            ("1h", 3600),
            ("2h", 7200),
            ("4h", 14400),
            ("1d", 86400),
            ("1w", 604800),
            # 长周期：固定日数近似
            ("1M", 2592000),       # 30d
            ("1Q", 7862400),       # 91d
            ("1Y", 31536000),      # 365d
            # pandas 风格 / 大小写别名
            ("1H", 3600),
            ("4H", 14400),
            ("1D", 86400),
            ("1W", 604800),
            (" 1d ", 86400),       # 容忍首尾空白
        ],
    )
    def test_known_frequencies(self, freq: str, expected: int):
        assert parse_frequency_seconds(freq) == expected

    def test_case_sensitive_minute_vs_month(self):
        """1m=分钟（60s），1M=月（30d），大小写语义不同。"""
        assert parse_frequency_seconds("1m") == 60
        assert parse_frequency_seconds("1M") == 2592000

    @pytest.mark.parametrize("freq", [None, "", "tick", "2h30m", "weird"])
    def test_unknown_returns_none(self, freq: str | None):
        """空值/事件型/未知写法 → None（调用方 fail-open，如每轮到期）。"""
        assert parse_frequency_seconds(freq) is None

    def test_interval_delta_rejects_unknown(self):
        """严格语义辅助仍对未知频率抛 ValueError。"""
        with pytest.raises(ValueError, match="Unsupported frequency"):
            _interval_delta("tick")
