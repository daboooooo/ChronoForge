"""backfill_datasets.py --due-only 到期选择纯函数测试。

覆盖 select_due_datasets 的到期谓词：
- 从未成功（无 last_success_time / 不可解析）→ 到期（补历史）
- frequency 未知（空值 / tick / 未注册写法）→ 每轮到期（fail-open）
- now − last_success ≥ cadence → 到期（含边界等值）
- 未到期 → skipped 且 next_due_at = last_success + cadence
- 时间字符串兼容 SQLite datetime('now') 空格格式与 ISO Z 后缀
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

# scripts/ 不是已安装包（pyproject 仅打包 src/chronoforge），引导项目根入 path
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts.backfill_datasets import (  # noqa: E402
    _next_due_at,
    _parse_utc,
    select_due_datasets,
)

_NOW = datetime(2026, 9, 29, 12, 0, 0)


def _ds(dataset_id: str, frequency: str | None, source_id: str = "") -> dict[str, object]:
    return {"dataset_id": dataset_id, "frequency": frequency, "source_id": source_id}


class TestParseUtc:
    """last_success_time 两种落盘格式 + 坏值。"""

    def test_sqlite_space_format(self):
        assert _parse_utc("2026-09-29 11:00:00") == datetime(2026, 9, 29, 11, 0, 0)

    def test_iso_z_suffix(self):
        assert _parse_utc("2026-09-29T11:00:00Z") == datetime(2026, 9, 29, 11, 0, 0)

    @pytest.mark.parametrize("value", [None, "", "not-a-time"])
    def test_bad_value_returns_none(self, value: str | None):
        assert _parse_utc(value) is None


class TestNextDueAt:
    """下次到期时刻：小时级对齐 UTC 整点网格，日频及以上对齐源发布时刻。"""

    @pytest.mark.parametrize(
        ("last", "expected"),
        [
            # 1h：下一个整点
            (datetime(2026, 9, 29, 10, 30, 5), datetime(2026, 9, 29, 11, 0)),
            (datetime(2026, 9, 29, 10, 0), datetime(2026, 9, 29, 11, 0)),
            (datetime(2026, 9, 29, 10, 0, 5), datetime(2026, 9, 29, 11, 0)),
            (datetime(2026, 9, 29, 23, 30), datetime(2026, 9, 30, 0, 0)),
        ],
    )
    def test_hourly_aligns_to_next_hour(self, last: datetime, expected: datetime):
        """1h 周期：每小时整点更新（跨日正确进位）。"""
        assert _next_due_at(last, 3600, "binance_spot") == expected

    @pytest.mark.parametrize(
        ("last", "expected"),
        [
            # 4h：00/04/08/12/16/20 时的下一个网格点
            (datetime(2026, 9, 29, 0, 35), datetime(2026, 9, 29, 4, 0)),
            (datetime(2026, 9, 29, 3, 50), datetime(2026, 9, 29, 4, 0)),
            (datetime(2026, 9, 29, 4, 0, 10), datetime(2026, 9, 29, 8, 0)),
            (datetime(2026, 9, 29, 23, 0), datetime(2026, 9, 30, 0, 0)),
        ],
    )
    def test_4h_aligns_to_utc_grid(self, last: datetime, expected: datetime):
        """4h 周期：00 时起每隔 4 小时更新。"""
        assert _next_due_at(last, 14400, "binance_spot") == expected

    def test_sub_daily_ignores_source_publish_time(self):
        """小时级不受源发布时刻影响（FRED 与 Binance 同规则）。"""
        last = datetime(2026, 9, 29, 10, 30)
        assert _next_due_at(last, 3600, "fred") == datetime(2026, 9, 29, 11, 0)
        assert _next_due_at(last, 3600, "yahoo") == datetime(2026, 9, 29, 11, 0)

    def test_fred_daily_after_publish_rolls_to_next_day(self):
        """FRED 08:10 UTC 发布：08:15 已成功 → 次日 08:10。"""
        last = datetime(2026, 9, 29, 8, 15)
        assert _next_due_at(last, 86400, "fred") == datetime(2026, 9, 30, 8, 10)

    def test_fred_daily_before_publish_stays_same_day(self):
        """FRED 08:10 UTC 发布：00:30 已成功（发布前）→ 当日 08:10 仍会再抓。"""
        last = datetime(2026, 9, 29, 0, 30)
        assert _next_due_at(last, 86400, "fred") == datetime(2026, 9, 29, 8, 10)

    def test_binance_daily_at_utc_midnight(self):
        """Binance 日线 00:00 UTC（北京 08:00）收线。"""
        last = datetime(2026, 9, 29, 0, 5)
        assert _next_due_at(last, 86400, "binance_spot") == datetime(2026, 9, 30, 0, 0)
        last_early = datetime(2026, 9, 28, 20, 0)
        assert _next_due_at(last_early, 86400, "ccxt") == datetime(2026, 9, 29, 0, 0)

    def test_default_source_aligned_to_beijing_midnight(self):
        """未配置数据源：北京 00:00 = 16:00 UTC。"""
        last = datetime(2026, 9, 29, 11, 0)
        assert _next_due_at(last, 86400, "yahoo") == datetime(2026, 9, 29, 16, 0)

    def test_weekly_uses_day_arithmetic(self):
        """1W：last 日期 + 7 天的发布时刻。"""
        last = datetime(2026, 9, 22, 10, 0)
        assert _next_due_at(last, 604800, "fred") == datetime(2026, 9, 29, 8, 10)

    def test_monthly_uses_30_day_approximation(self):
        """1M：固定 30 天近似 + 发布时刻。"""
        last = datetime(2026, 8, 29, 12, 0)
        assert _next_due_at(last, 2592000, "fred") == datetime(2026, 9, 28, 8, 10)


class TestSelectDueDatasetsPublishAlignment:
    """发布时刻对齐对到期判定的影响（now = 2026-09-29 12:00 UTC）。"""

    def test_fred_just_fetched_is_skipped_until_next_publish(self):
        """FRED 日频 08:15 刚成功 → 未到期，next_due = 次日 08:10。"""
        due, skipped = select_due_datasets(
            [_ds("FRED:X", "1d", "fred")],
            {"FRED:X": "2026-09-29 08:15:00"},
            now=_NOW,
        )
        assert due == []
        assert skipped[0]["next_due_at"] == datetime(2026, 9, 30, 8, 10)

    def test_fred_fetched_before_publish_is_due_after_publish(self):
        """FRED 日频 00:30 成功（发布前）→ 当日 08:10 后再次到期。"""
        due, _ = select_due_datasets(
            [_ds("FRED:X", "1d", "fred")],
            {"FRED:X": "2026-09-29 00:30:00"},
            now=_NOW,
        )
        assert [d["dataset_id"] for d in due] == ["FRED:X"]

    def test_binance_daily_not_due_before_utc_midnight(self):
        """Binance 日频 00:05 成功 → 当日 12:00 未到期（次日 00:00 才到期）。"""
        due, skipped = select_due_datasets(
            [_ds("CCXT:BTC:OHLCV:1d", "1d", "ccxt")],
            {"CCXT:BTC:OHLCV:1d": "2026-09-29 00:05:00"},
            now=_NOW,
        )
        assert due == []
        assert skipped[0]["next_due_at"] == datetime(2026, 9, 30, 0, 0)

    def test_equal_boundary_is_due_inclusive(self):
        """now == 下次到期时刻 → 到期（>= 语义）。"""
        due, _ = select_due_datasets(
            [_ds("YAHOO:AAPL:OHLCV:1d", "1d", "yahoo")],
            {"YAHOO:AAPL:OHLCV:1d": "2026-09-28 20:00:00"},
            now=datetime(2026, 9, 29, 16, 0, 0),
        )
        assert [d["dataset_id"] for d in due] == ["YAHOO:AAPL:OHLCV:1d"]


class TestSelectDueDatasets:
    """到期划分主逻辑。"""

    def test_never_succeeded_is_due(self):
        """无 last_success_time（含 map 缺键 / None / 坏值）→ 到期补历史。"""
        datasets = [_ds("a", "1d"), _ds("b", "1d"), _ds("c", "1d")]
        success = {"a": None, "b": "garbage"}  # c 缺键
        due, skipped = select_due_datasets(datasets, success, now=_NOW)
        assert [d["dataset_id"] for d in due] == ["a", "b", "c"]
        assert skipped == []

    @pytest.mark.parametrize("freq", [None, "", "tick", "weird"])
    def test_unknown_frequency_always_due(self, freq: str | None):
        """周期未知 fail-open：即使刚成功也每轮到期，避免 dataset 被饿死。"""
        datasets = [_ds("a", freq)]
        due, skipped = select_due_datasets(
            datasets, {"a": "2026-09-29 11:59:59"}, now=_NOW
        )
        assert [d["dataset_id"] for d in due] == ["a"]
        assert skipped == []

    def test_within_same_hour_is_skipped_until_next_hour(self):
        """1h 周期：本小时整点刚成功 → 跳过，next_due = 下一个整点。"""
        due, skipped = select_due_datasets(
            [_ds("hourly", "1h")],
            {"hourly": "2026-09-29 12:00:05"},
            now=datetime(2026, 9, 29, 12, 30),
        )
        assert due == []
        assert len(skipped) == 1
        assert skipped[0]["dataset_id"] == "hourly"
        assert skipped[0]["frequency"] == "1h"
        assert skipped[0]["next_due_at"] == datetime(2026, 9, 29, 13, 0)

    def test_cadence_boundary_is_due_inclusive(self):
        """now == 下一个整点 → 到期（>= 语义）。"""
        last = datetime(2026, 9, 29, 11, 0)
        datasets = [_ds("hourly", "1h")]
        due, _ = select_due_datasets(
            datasets, {"hourly": last.isoformat()}, now=_NOW
        )
        assert [d["dataset_id"] for d in due] == ["hourly"]

    def test_overdue_is_due(self):
        """日频 dataset 已 25 小时未成功 → 到期。"""
        datasets = [_ds("daily", "1d")]
        due, skipped = select_due_datasets(
            datasets, {"daily": "2026-09-28 11:00:00"}, now=_NOW
        )
        assert [d["dataset_id"] for d in due] == ["daily"]
        assert skipped == []

    def test_mixed_batch_partition(self):
        """混合批次：1h 到期、1d 未到期、1M 到期、tick 到期、无记录到期。"""
        datasets = [
            _ds("kline_1h", "1h"),       # 2h 前成功 → 到期
            _ds("fred_daily", "1d"),     # 1h 前成功 → 跳过
            _ds("fred_monthly", "1M"),   # 31 天前成功 → 到期
            _ds("trades", "tick"),       # 事件型 → 到期
            _ds("brand_new", "1d"),      # 从无成功 → 到期
        ]
        success = {
            "kline_1h": (_NOW - timedelta(hours=2)).isoformat(sep=" "),
            "fred_daily": (_NOW - timedelta(hours=1)).isoformat(sep=" "),
            "fred_monthly": (_NOW - timedelta(days=31)).isoformat(sep=" "),
            "trades": (_NOW - timedelta(minutes=1)).isoformat(sep=" "),
        }
        due, skipped = select_due_datasets(datasets, success, now=_NOW)
        assert [d["dataset_id"] for d in due] == [
            "kline_1h", "fred_monthly", "trades", "brand_new",
        ]
        assert [s["dataset_id"] for s in skipped] == ["fred_daily"]

    def test_does_not_mutate_inputs(self):
        """过滤结果为新列表，输入顺序/内容不变。"""
        datasets = [_ds("a", "1d"), _ds("b", "1h")]
        due, _ = select_due_datasets(
            datasets,
            {"a": "2026-09-20 00:00:00", "b": "2026-09-29 12:00:05"},
            now=datetime(2026, 9, 29, 12, 30),
        )
        assert len(datasets) == 2
        assert [d["dataset_id"] for d in due] == ["a"]
