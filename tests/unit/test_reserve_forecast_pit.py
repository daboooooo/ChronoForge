"""准备金预测 PIT（按发布时刻截断）回归测试。

覆盖 H-N3：
- origin 周三收盘不得使用周四才发布的当周 H.4.1/SOMA；
- FRED release_time 与 SOMA revision_time 都参与可得时刻判断；
- 回测同样按 origin 时点截断（naive 也只能用最近可知值）。
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

_SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from reserve_forecast import (  # noqa: E402
    _PANEL_SERIES,
    AsOfFrame,
    _asof_cutoff,
    _availability,
    _latest_known_observation,
    add_derived_accounts,
    backtest,
    forecast_from_origin,
)

_ONE_DAY = pd.Timedelta(days=1)


def _frame(periods: int = 20, start: str = "2025-01-01") -> AsOfFrame:
    dates = pd.date_range(start, periods=periods, freq="W-WED")
    data = {sid: [0.0] * periods for sid in _PANEL_SERIES}
    data["WALCL"] = [1200.0 + i for i in range(periods)]
    data["WRBWFRBL"] = [1000.0 + i * 10 for i in range(periods)]
    data["WDTGAL"] = [100.0 + i for i in range(periods)]
    data["WCICL"] = [2000.0] * periods
    data["WLFN"] = [2000.0] * periods
    values = pd.DataFrame(data, index=dates)
    # H.4.1 周三观测 → 周四发布（release/revision = 观测日次日 23:59:59）
    released = dates + _ONE_DAY + pd.Timedelta(hours=23, minutes=59, seconds=59)
    available = pd.DataFrame(
        {sid: released for sid in _PANEL_SERIES}, index=dates
    )
    return add_derived_accounts(AsOfFrame(values, available))


def test_history_excludes_same_week_release() -> None:
    frame = _frame()
    origin = frame.values.index[10]

    hist = frame.history(_asof_cutoff(origin))

    # origin 当周观测周四才发布，周三收盘时不可见：最后一行只能用上周值
    assert hist.loc[origin, "WRBWFRBL"] == frame.values.loc[
        frame.values.index[9], "WRBWFRBL"
    ]
    assert _latest_known_observation(frame, "WRBWFRBL", _asof_cutoff(origin)) == (
        frame.values.index[9]
    )


def test_forecast_ignores_origin_week_values() -> None:
    frame = _frame()
    origin = frame.values.index[10]
    base = forecast_from_origin(frame, origin, 1)

    polluted = frame.values.copy()
    polluted.loc[origin, ["WRBWFRBL", "WDTGAL", "WALCL", "WSHOSHO"]] += 500.0
    polluted_frame = add_derived_accounts(AsOfFrame(polluted, frame.available))

    # origin 当周数据被污染不应影响预测（它还没发布）
    assert forecast_from_origin(polluted_frame, origin, 1)["predicted_reserve"] == (
        base["predicted_reserve"]
    )
    assert base["origin_reserve"] == frame.values.loc[
        frame.values.index[9], "WRBWFRBL"
    ]


def test_revision_after_origin_is_excluded() -> None:
    frame = _frame()
    origin = frame.values.index[10]
    anchor_row = frame.values.index[9]

    # 上周观测被延迟修订（revision_time 晚于 origin）→ 按 PIT 视为当时不可用
    available = frame.available.copy()
    available.loc[anchor_row, "WDTGAL"] = origin + pd.Timedelta(days=2)
    revised_frame = add_derived_accounts(AsOfFrame(frame.values, available))

    hist = revised_frame.history(_asof_cutoff(origin))
    assert hist.loc[origin, "WDTGAL"] == frame.values.loc[
        frame.values.index[8], "WDTGAL"
    ]


def test_backtest_uses_pit_anchor_and_target() -> None:
    frame = _frame()
    origin = frame.values.index[10]

    bt = backtest(frame, min_origin=frame.values.index[2])
    row = bt[(bt["origin_date"] == origin.date()) & (bt["horizon"] == 1)].iloc[0]

    # naive 只能用 origin 时点可知值（上周），target 仍从 origin 起算一周
    assert row["naive"] == frame.values.loc[frame.values.index[9], "WRBWFRBL"]
    assert row["target_date"] == frame.values.index[11].date()
    assert row["actual"] == frame.values.loc[frame.values.index[11], "WRBWFRBL"]
    assert row["anchor_date"] == frame.values.index[9].date()


def test_availability_uses_release_and_revision_time() -> None:
    # 正常的周四发布记录按 release/revision 真实时刻截断
    obs = pd.Series(pd.to_datetime(["2026-11-04"]))
    released = pd.Series(pd.to_datetime(["2026-11-05 23:59:59"]))
    revised = pd.Series(pd.to_datetime(["2026-11-05 23:59:59"]))

    avail = _availability(obs, released, revised)

    assert avail.iloc[0] == pd.Timestamp("2026-11-05 23:59:59")


def test_late_revision_is_not_treated_as_archive_floor() -> None:
    # 真实的晚修订（如 WCICL 批量重述）必须按 revision_time 延后可得，
    # 不能套用归档回退规则，否则回测会偷看未来修订。
    obs = pd.Series(pd.to_datetime(["2025-08-13", "2025-08-20"]))
    released = pd.Series(pd.to_datetime(["2025-09-18 23:59:59"] * 2))
    revised = released.copy()
    # 序列最早 vintage 早于这些观测 → 不构成“归档下界回填”
    floor_row = pd.Series(pd.to_datetime(["2013-04-18 23:59:59"]))

    avail = _availability(
        pd.concat([floor_row, obs], ignore_index=True),
        pd.concat([floor_row, released], ignore_index=True),
        pd.concat([floor_row, revised], ignore_index=True),
    )

    assert avail.iloc[0] == pd.Timestamp("2013-04-18 23:59:59")
    assert avail.iloc[1] == pd.Timestamp("2025-09-18 23:59:59")
    assert avail.iloc[2] == pd.Timestamp("2025-09-18 23:59:59")
