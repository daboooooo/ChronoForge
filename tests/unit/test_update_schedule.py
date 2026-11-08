"""config.update_schedule 每日更新批次配置测试。

口径：所有数据集统一在每日单一批次刷新，批次时刻为北京时间 15:40
（07:40 UTC），紧随其后的 16:00（08:00 UTC）触发日报。配置以 UTC 存储，
此处锁定数值与 PIT 发布时刻的独立语义。
"""

from __future__ import annotations

from chronoforge.config.update_schedule import (
    DAILY_UPDATE_TIME_UTC,
    TREASURY_DTS_RELEASE_TIME_UTC,
)


class TestDailyUpdateTime:
    """每日统一批次时刻。"""

    def test_batch_is_beijing_1540(self):
        """北京时间 15:40 → 07:40 UTC。"""
        assert DAILY_UPDATE_TIME_UTC == (7, 40)

    def test_batch_precedes_report_by_20_minutes(self):
        """批次 07:40 UTC → 报告 08:00 UTC（16:00 北京），间隔 20 分钟。"""
        hour, minute = DAILY_UPDATE_TIME_UTC
        assert (hour, minute) == (7, 40)
        assert 8 * 60 - (hour * 60 + minute) == 20

    def test_no_per_source_or_dataset_schedule_remains(self):
        """分散的 source/dataset 级发布时刻结构已移除。"""
        import chronoforge.config.update_schedule as mod

        assert not hasattr(mod, "SOURCE_PUBLISH_TIME_UTC")
        assert not hasattr(mod, "DATASET_PUBLISH_TIME_UTC")
        assert not hasattr(mod, "publish_time_utc")


class TestTreasuryDtsReleaseTime:
    """DTS 的 PIT 发布时间与抓取批次解耦。"""

    def test_release_is_next_business_day_2100_utc(self):
        """官方约次日 16:00 ET，保守取 21:00 UTC（北京次日 05:00）。"""
        assert TREASURY_DTS_RELEASE_TIME_UTC == (21, 0)
