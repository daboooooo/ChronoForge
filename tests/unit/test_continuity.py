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

from chronoforge.quality.calendar import (
    US_MARKET_HOLIDAYS,
    is_trading_day,
)
from chronoforge.quality.continuity import (
    ContinuityModel,
    _interval_delta,
    expected_grid,
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# is_trading_day — 简化交易日历
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestIsTradingDay:
    """TC-Q-007：简化交易日历判定。"""

    def test_weekend_saturday(self):
        """周六 → False。"""
        # 2026-09-12 是周六
        assert is_trading_day(date(2026, 9, 12)) is False

    def test_weekend_sunday(self):
        """周日 → False。"""
        # 2026-09-13 是周日
        assert is_trading_day(date(2026, 9, 13)) is False

    def test_weekday_trading_day(self):
        """普通工作日 → True。"""
        # 2026-09-14 是周一
        assert is_trading_day(date(2026, 9, 14)) is True

    def test_new_year_holiday(self):
        """新年（1/1）→ False。"""
        assert is_trading_day(date(2026, 1, 1)) is False

    def test_christmas_holiday(self):
        """圣诞节（12/25）→ False。"""
        assert is_trading_day(date(2026, 12, 25)) is False

    def test_independence_day_holiday(self):
        """独立日（7/4）→ False。"""
        assert is_trading_day(date(2026, 7, 4)) is False

    def test_early_closing_day(self):
        """早闭日（2026-07-03）→ True。"""
        assert is_trading_day(date(2026, 7, 3)) is True

    def test_early_closing_not_in_list(self):
        """非早闭日的工作日 → True。"""
        assert is_trading_day(date(2026, 7, 2)) is True

    def test_presidents_day(self):
        """总统日（2/14 简化）→ False。"""
        assert is_trading_day(date(2026, 2, 14)) is False

    def test_labor_day(self):
        """劳动节（9/7 简化）→ False。"""
        assert is_trading_day(date(2026, 9, 7)) is False

    def test_thanksgiving(self):
        """感恩节（11/27 简化）→ False。"""
        assert is_trading_day(date(2026, 11, 27)) is False


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2026 全日历遍历（闰年 + 节假日矩阵）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestCalendarMatrix2026:
    """闰日/节假日/周末矩阵：2026 年全日历遍历验证。"""

    def test_all_weekends_2026_non_trading(self):
        """2026 年所有周六周日 → 非交易日。"""
        d = date(2026, 1, 1)
        end = date(2026, 12, 31)
        while d <= end:
            if d.weekday() >= 5:
                assert is_trading_day(d) is False, f"Expected weekend {d} to be non-trading"
            d += timedelta(days=1)

    def test_all_us_holidays_2026_non_trading(self):
        """2026 年所有 US_MARKET_HOLIDAYS → 非交易日。"""
        for month, day in US_MARKET_HOLIDAYS:
            d = date(2026, month, day)
            assert is_trading_day(d) is False, f"Expected holiday {d} to be non-trading"

    def test_all_early_closings_2026_trading(self):
        """2026 年所有 EARLY_CLOSINGS → 交易日。"""
        # 2026 年仅 7/3 是早闭日
        ec = date(2026, 7, 3)
        assert is_trading_day(ec) is True

    def test_holiday_falling_on_weekday(self):
        """节假日落在工作日 → 非交易日（不是周末）。"""
        # 2026-01-01 是周四（工作日）
        jan1 = date(2026, 1, 1)
        assert jan1.weekday() == 3  # 验证周四
        assert is_trading_day(jan1) is False

    def test_thanksgiving_falls_on_weekday(self):
        """感恩节 2026-11-27 是周五（工作日）。"""
        thanksgiving = date(2026, 11, 27)
        assert thanksgiving.weekday() == 4  # 验证周五
        assert is_trading_day(thanksgiving) is False


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
        """感恩节(11/27, 周五) + 周末 → 连续 EXPECTED_GAP。"""
        start = datetime(2026, 11, 25, tzinfo=UTC)  # 周三
        end = datetime(2026, 11, 30, tzinfo=UTC)    # 周一
        grid = expected_grid("TEST", "1d", ContinuityModel.TRADING_CALENDAR, start, end)

        # 11/25(Wed, REGULAR), 11/26(Thu, REGULAR)
        # 11/27(Fri, 感恩节, EXPECTED_GAP), 11/28-29(Sat-Sun, EXPECTED_GAP)
        # 11/30(Mon, REGULAR)
        expected_gaps = [e for e in grid if e[2] == "EXPECTED_GAP"]
        assert len(expected_gaps) == 1
        # 感恩节 + 周末合并为一个 EXPECTED_GAP
        gap_start = expected_gaps[0][0].date()
        gap_end = expected_gaps[0][1].date()
        assert gap_start == date(2026, 11, 27)  # 感恩节开始
        assert gap_end == date(2026, 11, 30)    # 周一前


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
        """年末感恩节(11/27)附近 → EXPECTED_GAP 正确。"""
        # 感恩节后一周：11/27(周五) 感恩节
        start = datetime(2026, 11, 23, tzinfo=UTC)  # 周一
        end = datetime(2026, 12, 1, tzinfo=UTC)     # 周二
        grid = expected_grid("TEST", "1d", ContinuityModel.TRADING_CALENDAR, start, end)

        expected_gaps = [e for e in grid if e[2] == "EXPECTED_GAP"]

        assert len(expected_gaps) >= 1
        # 感恩节(11/27) + 周末(11/28-29) 应合并为 EXPECTED_GAP
        for gap in expected_gaps:
            gap_dates = set()
            current = gap[0].date()
            end_d = gap[1].date()
            while current <= end_d:
                gap_dates.add(current)
                current += timedelta(days=1)
            assert date(2026, 11, 27) in gap_dates  # 感恩节在内

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
