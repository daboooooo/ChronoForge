"""数据源日内发布时刻配置（数据更新策略依据）。

记录各数据源每日数据"可用"的时点（运营观测值），供 hourly_update.sh 的
到期增量（backfill_datasets.py --due-only）使用。到期判定分两档：

- 小时级（周期 < 1 天，1h/4h 等）：以 UTC 00:00 为原点对齐周期网格取
  下一个网格点——1h 每小时整点更新，4h 每 4 小时（00/04/08/12/16/20 时）
  更新。网格与整点调度同拍，避免"上次成功时刻 + 周期"被轮次耗时带偏后
  与调度错位、隔轮才触发；
- 日频及以上（1d/1W/1M/1Q/1Y）：对齐到下表的源发布时刻，避免在源尚未
  发布新数据时抓取——否则每轮都拿到上一周期的旧值并推进
  last_success_time，导致数据恒定滞后一天。

配置以 UTC 存储（判定链路统一用 UTC naive datetime），注释标注对应的
北京时间（UTC+8）来源，便于与运营口径核对。
"""

from __future__ import annotations

from datetime import time

# 各数据源日内发布时刻（UTC）：source_id → (hour, minute)
# 换算关系：UTC = 北京时间 − 8 小时。
#   FRED            北京时间 16:10 → 08:10 UTC
#   Binance 系列    北京时间 08:00 → 00:00 UTC（日线 00:00 UTC 收线）
#   ccxt            Binance 桥接源，同上
SOURCE_PUBLISH_TIME_UTC: dict[str, tuple[int, int]] = {
    "fred": (8, 10),
    "binance_spot": (0, 0),
    "binance_futures": (0, 0),
    "ccxt": (0, 0),
}

# 未显式配置的数据源默认发布时刻（UTC）：北京时间 00:00 → 16:00 UTC
# （deribit / sosovalue / yahoo 等）
DEFAULT_PUBLISH_TIME_UTC: tuple[int, int] = (16, 0)


def publish_time_utc(source_id: str | None) -> time:
    """返回 source_id 的日内发布时刻（UTC）；未配置的数据源取默认值。"""
    hour, minute = SOURCE_PUBLISH_TIME_UTC.get(str(source_id or ""), DEFAULT_PUBLISH_TIME_UTC)
    return time(hour, minute)
