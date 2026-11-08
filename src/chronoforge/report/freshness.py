"""逐序列新鲜度矩阵。

矩阵按实施方案 §2.3（Phase 0 修正版）：
- H.4.1 周频：报告中可得的最近一个周三观测（周五为 T-2，其余为上周三）
- H.15 日频：T-2 个交易日
- EFFR / SOFR / TGA 日值：结构性 T-2 个交易日
- VIXCLS：单独一行，T-2 个交易日（实测滞后）
- RRP / SRF / IORB / 行情：T-1 个交易日（IORB 当日适用值在 07:40 批次前不可得）
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

from chronoforge.report.session import shift_trading_days

STATUS_LABEL = {"ok": "正常", "lagged": "滞后", "missing": "缺失"}

# weekday: Mon=0 ... Fri=4 → H.4.1 可得的最近周三距报告日的自然日数
_WEEKLY_H41_BACK = {0: 5, 1: 6, 2: 7, 3: 8, 4: 2}


@dataclass(frozen=True)
class SeriesSpec:
    code: str
    label: str
    group: str
    rule: str
    core: bool = False


SERIES_SPECS: tuple[SeriesSpec, ...] = (
    SeriesSpec("WRESBAL", "准备金余额（周均）", "H.4.1（周）", "weekly_h41", True),
    SeriesSpec("WALCL", "美联储总资产", "H.4.1（周）", "weekly_h41"),
    SeriesSpec("WTREGEN", "TGA（周均）", "H.4.1（周）", "weekly_h41", True),
    SeriesSpec("TGA_DAILY", "TGA（日值）", "TGA", "trading_lag_2", True),
    SeriesSpec("RRPONTSYD", "ON RRP", "资金面", "trading_lag_1", True),
    SeriesSpec("EFFR", "EFFR", "资金面", "trading_lag_2", True),
    SeriesSpec("SOFR", "SOFR", "资金面", "trading_lag_2", True),
    SeriesSpec("IORB", "IORB", "资金面", "trading_lag_1"),
    SeriesSpec("SRF", "SRF 用量", "资金面", "trading_lag_1", True),
    SeriesSpec("DGS10", "10Y 名义利率", "H.15（日）", "trading_lag_2"),
    SeriesSpec("DGS2", "2Y 名义利率", "H.15（日）", "trading_lag_2"),
    SeriesSpec("DFII10", "10Y 实际利率", "H.15（日）", "trading_lag_2"),
    SeriesSpec("T10YIE", "10Y 盈亏平衡", "H.15（日）", "trading_lag_2"),
    SeriesSpec("VIXCLS", "VIX（FRED）", "行情", "trading_lag_2"),
    SeriesSpec("DXY", "美元指数", "行情", "trading_lag_1"),
    SeriesSpec("SPX", "S&P 500", "行情", "trading_lag_1"),
    SeriesSpec("TLT", "TLT（20Y+ 美债 ETF）", "行情", "trading_lag_1"),
)


@dataclass(frozen=True)
class FreshnessRow:
    code: str
    label: str
    group: str
    core: bool
    expected: date | None
    actual: date | None
    status: str
    lag_days: int | None
    ahead_days: int
    note: str

    def to_dict(self) -> dict[str, object]:
        return {
            "series": self.code,
            "label": self.label,
            "group": self.group,
            "core": self.core,
            "expected_asof": self.expected.isoformat() if self.expected else None,
            "actual_asof": self.actual.isoformat() if self.actual else None,
            "status": self.status,
            "status_label": STATUS_LABEL[self.status],
            "lag_days": self.lag_days,
            "ahead_days": self.ahead_days,
            "note": self.note,
        }


def expected_asof(rule: str, report_day: date) -> date:
    if rule == "weekly_h41":
        back = _WEEKLY_H41_BACK.get(report_day.weekday())
        if back is None:
            raise ValueError(f"H.4.1 矩阵未覆盖 {report_day.isoformat()}（周末？）")
        return report_day - timedelta(days=back)
    if rule == "trading_lag_1":
        return shift_trading_days(report_day, -1)
    if rule == "trading_lag_2":
        return shift_trading_days(report_day, -2)
    raise ValueError(f"未知新鲜度规则: {rule}")


def _parse_date(value: object) -> date | None:
    if not value:
        return None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def build_freshness(
    cutoffs: dict[str, str | None], report_day: date
) -> list[FreshnessRow]:
    rows: list[FreshnessRow] = []
    for spec in SERIES_SPECS:
        expected = expected_asof(spec.rule, report_day)
        actual = _parse_date(cutoffs.get(spec.code))
        if actual is None:
            rows.append(FreshnessRow(
                spec.code, spec.label, spec.group, spec.core, expected, None,
                "missing", None, 0, "序列无可用观测（显式缺口，不估算）",
            ))
            continue
        if actual >= expected:
            ahead = (actual - expected).days
            note = "符合矩阵下限"
            if ahead:
                note = f"实际观测比矩阵下限新 {ahead} 天"
            rows.append(FreshnessRow(
                spec.code, spec.label, spec.group, spec.core, expected, actual,
                "ok", 0, ahead, note,
            ))
            continue
        lag = (expected - actual).days
        rows.append(FreshnessRow(
            spec.code, spec.label, spec.group, spec.core, expected, actual,
            "lagged", lag, 0,
            f"比矩阵下限滞后 {lag} 天（预期 ≥ {expected.isoformat()}）",
        ))
    return rows


def build_gaps(rows: list[FreshnessRow]) -> list[dict[str, object]]:
    """把滞后/缺失序列转成报告侧显式缺口。"""
    gaps: list[dict[str, object]] = []
    for row in rows:
        if row.status == "ok":
            continue
        gaps.append({
            "kind": "series_" + row.status,
            "series": row.code,
            "label": row.label,
            "expected_asof": row.expected.isoformat() if row.expected else None,
            "actual_asof": row.actual.isoformat() if row.actual else None,
            "detail": row.note,
        })
    return gaps
