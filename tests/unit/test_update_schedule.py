"""config.update_schedule 数据源发布时刻配置测试。

运营口径（北京时间 UTC+8）：
- FRED 每日 16:10
- Binance 系列（含 ccxt 桥接源）日线 08:00
- 其余数据源 00:00
配置以 UTC 存储（UTC = 北京时间 − 8 小时），此处逐条锁定换算结果。
"""

from __future__ import annotations

from datetime import time

import pytest

from chronoforge.config.update_schedule import (
    DEFAULT_PUBLISH_TIME_UTC,
    SOURCE_PUBLISH_TIME_UTC,
    publish_time_utc,
)

# 北京时间 → UTC 小时偏移
_CN_UTC_OFFSET_HOURS = 8


def _to_utc(hour: int, minute: int) -> time:
    return time((hour - _CN_UTC_OFFSET_HOURS) % 24, minute)


class TestPublishTimeUtc:
    """source_id → 发布时刻（UTC）。"""

    def test_fred_beijing_1610(self):
        """FRED 北京时间 16:10 → 08:10 UTC。"""
        assert publish_time_utc("fred") == time(8, 10)

    @pytest.mark.parametrize("source_id", ["binance_spot", "binance_futures", "ccxt"])
    def test_binance_beijing_0800(self, source_id: str):
        """Binance 日线北京时间 08:00 收线 → 00:00 UTC。"""
        assert publish_time_utc(source_id) == time(0, 0)

    @pytest.mark.parametrize(
        "source_id",
        ["deribit", "sosovalue", "yahoo", "", None, "unknown_source"],
    )
    def test_others_beijing_0000(self, source_id: str | None):
        """未配置数据源（含 None/空串/未知）→ 北京 00:00 = 16:00 UTC。"""
        assert publish_time_utc(source_id) == time(16, 0)

    def test_config_matches_beijing_offset(self):
        """全部显式配置项均等于其北京时间来源减 8 小时。"""
        beijing = {
            "fred": (16, 10),
            "binance_spot": (8, 0),
            "binance_futures": (8, 0),
            "ccxt": (8, 0),
        }
        assert set(SOURCE_PUBLISH_TIME_UTC) == set(beijing)
        for source_id, (hour, minute) in beijing.items():
            assert publish_time_utc(source_id) == _to_utc(hour, minute)
        assert DEFAULT_PUBLISH_TIME_UTC == (16, 0)
