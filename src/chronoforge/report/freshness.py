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
    datasets: tuple[str, ...] = ()


SERIES_SPECS: tuple[SeriesSpec, ...] = (
    SeriesSpec("WRESBAL", "准备金余额（周均）", "H.4.1（周）", "weekly_h41", True,
               ("fred_series_WRESBAL",)),
    SeriesSpec("WALCL", "美联储总资产", "H.4.1（周）", "weekly_h41",
               datasets=("fred_series_WALCL",)),
    SeriesSpec("WTREGEN", "TGA（周均）", "H.4.1（周）", "weekly_h41", True,
               ("fred_series_WTREGEN",)),
    SeriesSpec("TGA_DAILY", "TGA（日值）", "TGA", "trading_lag_2", True,
               ("treasury_fiscal_dts_operating_cash_balance",)),
    SeriesSpec("RRPONTSYD", "ON RRP", "资金面", "trading_lag_1", True,
               ("fred_series_RRPONTSYD",)),
    SeriesSpec("EFFR", "EFFR", "资金面", "trading_lag_2", True,
               ("fred_series_EFFR",)),
    SeriesSpec("SOFR", "SOFR", "资金面", "trading_lag_2", True,
               ("fred_series_SOFR",)),
    SeriesSpec("IORB", "IORB", "资金面", "trading_lag_1",
               datasets=("fred_series_IORB",)),
    SeriesSpec("SRF", "SRF 用量", "资金面", "trading_lag_1", True,
               ("nyfed_repo_srf_usage",)),
    SeriesSpec("DGS10", "10Y 名义利率", "H.15（日）", "trading_lag_2",
               datasets=("fred_series_DGS10",)),
    SeriesSpec("DGS2", "2Y 名义利率", "H.15（日）", "trading_lag_2",
               datasets=("fred_series_DGS2",)),
    SeriesSpec("DFII10", "10Y 实际利率", "H.15（日）", "trading_lag_2",
               datasets=("fred_series_DFII10",)),
    SeriesSpec("T10YIE", "10Y 盈亏平衡", "H.15（日）", "trading_lag_2",
               datasets=("fred_series_T10YIE",)),
    SeriesSpec("VIXCLS", "VIX（FRED）", "行情", "trading_lag_2",
               datasets=("fred_series_VIXCLS",)),
    SeriesSpec("DXY", "美元指数", "行情", "trading_lag_1",
               datasets=("YAHOO:DX-Y.NYB:OHLCV:1d",)),
    SeriesSpec("SPX", "S&P 500", "行情", "trading_lag_1",
               datasets=("YAHOO:^GSPC:OHLCV:1d",)),
    SeriesSpec("TLT", "TLT（20Y+ 美债 ETF）", "行情", "trading_lag_1",
               datasets=("YAHOO:TLT:OHLCV:1d",)),
)

# 缺口分类：两类都显示，只影响读者该「等」还是该「查」。
GAP_CATEGORY_LABEL = {
    "await_release": "未到发布时点（正常）",
    "needs_check": "源侧数据异常或批次问题（需查）",
}


def series_datasets(code: object) -> tuple[str, ...]:
    for spec in SERIES_SPECS:
        if spec.code == code:
            return spec.datasets
    return ()


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


def _next_step(category: str, gap: dict[str, object]) -> str:
    if category == "await_release":
        expected = gap.get("expected_asof") or "矩阵下限"
        if gap.get("kind") == "series_missing":
            return f"确认该序列是否已注册采集；按矩阵下限 {expected} 应已可得"
        return f"等待下一次官方发布（矩阵下限 {expected}），无需人工干预"
    kind = str(gap.get("kind") or "")
    if kind == "source_anomaly":
        return (
            "检查上游返回是否已修正；本地已丢弃坏行并保留上一有效观测，"
            "必要时重跑该数据集并核对 canonical"
        )
    if kind == "events_config_invalid":
        return "修正 config/events/known_events.yaml 后重新生成报告"
    if kind.startswith("batch_"):
        return "检查 var/log/daily_batch.log 与 run_log 中的 PARTIAL/FAILED 数据集及退出码"
    return "检查上游与批次日志，确认是否为一次性异常"


def classify_gaps(
    gaps: list[dict[str, object]], anomaly_datasets: set[str]
) -> list[dict[str, object]]:
    """按事实把缺口分成「等发布」与「需核查」两类。

    判定依据只有两条：
    - 该缺口本身来自 Q-NORM-SRC-ANOMALY（source_anomaly），或同一序列的
      数据集在窗口内出现过该质量标记 → 需核查；
    - 其余按 as-of 矩阵滞后/缺失 → 未到发布时点。
    分类不删除任何条目。
    """
    for gap in gaps:
        kind = str(gap.get("kind") or "")
        if kind == "source_anomaly":
            category = "needs_check"
        elif kind.startswith("series_"):
            linked = series_datasets(gap.get("series"))
            category = "needs_check" if any(d in anomaly_datasets for d in linked) \
                else "await_release"
        else:
            category = "needs_check"
        gap["category"] = category
        gap["category_label"] = GAP_CATEGORY_LABEL[category]
        gap["next_step"] = _next_step(category, gap)
    return gaps
