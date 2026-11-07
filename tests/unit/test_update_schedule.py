"""config.update_schedule 数据源发布时刻配置测试。

运营口径（北京时间 UTC+8）：
- FRED 每日 16:10
- Binance 系列（含 ccxt 桥接源）日线 08:00
- Alternative.me / Coin Metrics / CoinMarketCap 日频值次日冻结，08:10 采集（T+1）
- NY Fed SOMA 周四发布 → 次日北京 07:00（23:00 UTC）
- NY Fed SRF 操作日当晚公布 → 次日北京 06:00（22:00 UTC）
- 其余数据源 00:00
配置以 UTC 存储（UTC = 北京时间 − 8 小时），此处逐条锁定换算结果。
"""

from __future__ import annotations

from datetime import time

import pytest

from chronoforge.config.update_schedule import (
    DATASET_PUBLISH_TIME_UTC,
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
        "source_id", ["alternative", "coinmetrics", "coinmarketcap"]
    )
    def test_daily_freezes_beijing_0810(self, source_id: str):
        """Alternative/CoinMetrics/CoinMarketCap 日频值次日冻结 → 北京 08:10 = 00:10 UTC。"""
        assert publish_time_utc(source_id) == time(0, 10)

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
            "alternative": (8, 10),
            "coinmetrics": (8, 10),
            "coinmarketcap": (8, 10),
            # DTS 每工作日约 16:00 ET（保守取 21:00 UTC = 北京次日 05:00）
            "treasury_fiscal": (5, 0),
            # 季度再融资新闻稿（UTC 15:15 = 北京 23:15）
            "treasury_refunding": (23, 15),
            # NY Fed 源级回退 = SRF 操作日当晚（22:00 UTC = 北京次日 06:00）
            "nyfed": (6, 0),
        }
        assert set(SOURCE_PUBLISH_TIME_UTC) == set(beijing)
        for source_id, (hour, minute) in beijing.items():
            assert publish_time_utc(source_id) == _to_utc(hour, minute)
        assert DEFAULT_PUBLISH_TIME_UTC == (16, 0)


class TestDatasetPublishTime:
    """dataset_id 级覆盖优先于 source_id 级。"""

    def test_soma_thursday_2300_utc(self):
        """SOMA 周四发布 → 23:00 UTC（北京次日 07:00）。"""
        assert publish_time_utc("nyfed", "nyfed_soma_holdings") == time(23, 0)

    def test_srf_same_day_2200_utc(self):
        """SRF 操作日当晚公布 → 22:00 UTC（北京次日 06:00）。"""
        assert publish_time_utc("nyfed", "nyfed_repo_srf_usage") == time(22, 0)

    def test_unknown_dataset_falls_back_to_source(self):
        """未配置的 nyfed 数据集回退到 source 级 22:00 UTC。"""
        assert publish_time_utc("nyfed", "nyfed_other") == time(22, 0)

    def test_dataset_keys_are_expected(self):
        """数据集级覆盖仅限这两个发布语义不同的 nyfed 数据集。"""
        assert set(DATASET_PUBLISH_TIME_UTC) == {
            "nyfed_soma_holdings",
            "nyfed_repo_srf_usage",
        }
