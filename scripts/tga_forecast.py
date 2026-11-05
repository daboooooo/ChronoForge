#!/usr/bin/env python3
"""TGA 基准预测引擎（Model 1/2，Phase 3，依据实现方案 §6/§9.1）。

组件（§6.1 组件协议；输出统一 schema，金额带符号：流入 + / 流出 −）：
- debt_component    ：发行三层（§6.3）+ 到期本金（§6.4）
- interest_component：固定利率 coupon 事件 + 非付息日基线（§6.4）
- tax_component     ：TAX 事件曲线 + REFUND 季节基线（§6.5/§6.7）
- outlay_component  ：社保周三支付规则 + 通用日分布（§6.6）
- other_component   ：OTHER/UNCLASSIFIED 双向基线（§6.7）

聚合（§6.1）：仅对事件行按 (target_date, category, detail, event_key) 去重
（基线行不参与去重）、守恒断言（Q-TGA-012）、自 TGA cutoff 累计。情景调整
属 Phase 4（D-TGA-11：绝不调整已知现金流）；Phase 3 固定 base 情景。

数据入口（§2.3）：Canonical 拍卖走 QueryService；Derived 余额/现金流走
scripts/tga_store.py 只读封装。PIT：所有输入按 release_time/发布规则
过滤到 data_cutoff（§5.7/§7.2）。

用法：
    uv run python scripts/tga_forecast.py [--origin YYYY-MM-DD]
        [--data-cutoff YYYY-MM-DD] [--horizons 7,14,30] [--force] [--dry-run]
"""

from __future__ import annotations

# QueryService 行数上限按拍卖全历史读取放宽；必须在 Settings.load() 前设置。
import os

os.environ.setdefault("CHRONOFORGE_QUERY_MAX_ROWS", "500000")

import argparse
import calendar as pycal
import hashlib
import json
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl
from tga_common import (
    CALENDAR_PATH,
    MODEL_PATH,
    SCENARIOS_PATH,
    config_hash,
    is_business_day,
    load_taxonomy,
    load_yaml,
    next_business_day,
)

from chronoforge.cli._wiring import open_meta, open_query_service
from chronoforge.config.settings import Settings

MODEL_VERSION = "tga-forecast-0.4.0"  # TGA-1 去重键修正：版本须变更以阻断旧逻辑 run 复用
_REPO_ROOT = Path(__file__).resolve().parents[1]

# 组件输出统一 schema（§6.1；predicted 带符号，单位 USD Million）
COMPONENT_SCHEMA: dict[str, pl.DataType] = {
    "target_date": pl.Date, "category": pl.String, "predicted": pl.Float64,
    "lower": pl.Float64, "upper": pl.Float64, "method": pl.String,
    "confidence": pl.Float64, "source_type": pl.String,
    "event_key": pl.String, "detail": pl.String,
    "scenario_adjustment": pl.Float64,   # §6.8 审计字段：情景乘法因子
}
FORECAST_COLUMNS = (
    "target_date", "horizon", "is_horizon_boundary",
    "predicted_tga", "lower", "upper", "daily_net", "source_type",
)
# 质量旗标虚拟 dataset_id（与 tga_reconstruct.RECONSTRUCTION_DATASET_ID 同约定）
FORECAST_DATASET_ID = "tga_forecast_run"
# 守恒断言容差（USD Million；金额为整数百万，浮点求和误差远小于 1）
_CONSERVATION_TOL = 0.5
# TGA-1：仅事件行去重，且去重键必须保留子类身份（`detail` 携带 subcategory）。
# 事件行 label（如 `TAX:<id>`）跨子类共享，缺 detail 会跨子类合并；基线行
# （event_key=None）同子类同日还存在流入/流出两个独立组件，任何去重都会
# 丢失现金流，因此基线行不参与去重，改由月度守恒断言兜底。
_DEDUPE_KEY = ["target_date", "category", "detail", "event_key"]
# 子类停更判定：最后一笔历史早于 cutoff 超过该天数即不再外推（§6.5/§6.6）
STALE_SUBCATEGORY_DAYS = 90
# 融资缺口输入（§6.9）：Canonical 数据集与来源
_REFUNDING_DS = "treasury_refunding_announcements"
# 拍卖回看窗口（年）：到期偿还需要覆盖 30 年期债券等长券的原始发行记录，
# 只用 3 年窗口会漏掉到期本金（实测 90 天低估约 1.3T）。回测与实盘共用该值，
# 保证两者同构。
AUCTION_LOOKBACK_YEARS = 35


# ── 日历工具 ───────────────────────────────────────────────────────────


def prev_business_day(day: date) -> date:
    prev = day - timedelta(days=1)
    while not is_business_day(prev):
        prev -= timedelta(days=1)
    return prev


def business_days(start: date, end: date) -> list[date]:
    """[start, end] 内的联邦工作日（含端点）。"""
    days: list[date] = []
    cur = start
    while cur <= end:
        if is_business_day(cur):
            days.append(cur)
        cur += timedelta(days=1)
    return days


def rel_business_index(anchor: date, day: date) -> int:
    """day 相对 anchor 的工作日序号（anchor=0，向后为正）。"""
    sign = 1
    lo, hi = anchor, day
    if day < anchor:
        sign, lo, hi = -1, day, anchor
    n = 0
    cur = lo
    while cur < hi:
        cur += timedelta(days=1)
        if is_business_day(cur):
            n += 1
    return sign * n


def add_months(day: date, months: int) -> date:
    month_index = day.year * 12 + (day.month - 1) + months
    year, month = divmod(month_index, 12)
    month += 1
    last = pycal.monthrange(year, month)[1]
    return date(year, month, min(day.day, last))


def nth_weekday_of_month(year: int, month: int, weekday: int, ordinals: list[int]) -> list[date]:
    """每月第 N 个指定星期（ordinal 从 1 起；weekday 0=Mon）。"""
    first_wd = date(year, month, 1).weekday()
    out: list[date] = []
    for ordinal in ordinals:
        day = 1 + (weekday - first_wd) % 7 + (ordinal - 1) * 7
        if day <= pycal.monthrange(year, month)[1]:
            out.append(date(year, month, day))
    return out


def data_through(origin: date) -> date:
    """DTS 数据可用截止日（§5.7：record_date 的下一工作日 21:00 UTC 发布）。

    保守规则：next_business_day(record_date) < origin 才视为已发布
    （21:00 UTC 发布在多数运行时区已跨入次日，以日期粒度取保守侧）。
    """
    day = prev_business_day(origin)
    while next_business_day(day) >= origin:
        day = prev_business_day(day)
    return day


# ── 上下文与输入 ───────────────────────────────────────────────────────


class ForecastContext:
    """预测运行上下文（§6.1 ForecastContext 的 P0 落地）。"""

    def __init__(
        self,
        origin: date,
        horizons: list[int],
        model: dict,
        calendar: dict,
        balance: pl.DataFrame,
        cashflow: pl.DataFrame,
        auctions: pl.DataFrame,
        through: date | None = None,
    ) -> None:
        self.origin = origin
        self.horizons = sorted(horizons)
        self.model = model
        self.calendar = calendar
        self.max_horizon = max(horizons)
        self.through = through or data_through(origin)
        self.grid = business_days(
            origin + timedelta(days=1), origin + timedelta(days=self.max_horizon)
        )
        self.hist_start = prev_business_day(origin - timedelta(days=1)) - timedelta(
            days=365 * 4
        )
        self.balance = balance
        self.cashflow = cashflow
        self.auctions = auctions
        bal = balance.filter(pl.col("date") <= self.through).sort("date")
        if bal.is_empty():
            raise RuntimeError(
                f"TGA 余额无截至 {self.through} 的数据；请先运行 tga_reconstruct"
            )
        self.last_tga = float(bal["closing"][-1])
        self.last_balance_date = bal["date"][-1]

    def hist(
        self, categories: tuple[str, ...], direction: str | None = None
    ) -> pl.DataFrame:
        """叶级日频现金流（is_subtotal 排除、include_in_net 过滤），按日/子类聚合。"""
        df = self.cashflow.filter(
            pl.col("category").is_in(list(categories))
            & (~pl.col("is_subtotal"))
            & pl.col("include_in_net")
            & (pl.col("date") <= self.through)
            & (pl.col("date") >= self.hist_start)
        )
        if direction:
            df = df.filter(pl.col("direction") == direction)
        return (
            df.group_by("date", "category", "subcategory")
            .agg(pl.col("amount_usd_millions").sum())
            .rename({"amount_usd_millions": "amount"})
        )


def load_inputs(
    settings: Settings, meta, through: date, origin: date
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Derived 余额/现金流（tga_store 只读封装）+ Canonical 拍卖（QueryService）。"""
    from tga_store import DerivedStore, resolve_data_dir

    store = DerivedStore(resolve_data_dir(settings))
    balance = store.read("tga_daily_balance")
    cashflow = store.read("tga_daily_cashflow")

    service, _ = open_query_service(settings, meta)
    auction_start = datetime(through.year - AUCTION_LOOKBACK_YEARS, 1, 1)
    auction_end = datetime(origin.year, origin.month, origin.day) + timedelta(days=1)
    # 注意（方案 v1.4 实测修正①）：QueryService 视图按 canonical type 组织，
    # auctions_results 与 auctions_upcoming 返回同一 TREASURY_AUCTION 视图，
    # 禁止按 dataset 重复拼接（否则全部翻倍）——只查询一次。
    result = service.query(
        dataset_id="treasury_fiscal_auction_results",
        start=auction_start, end=auction_end,
    )
    if result.truncated:
        raise RuntimeError("query truncated for treasury_fiscal_auction_results")
    return balance, cashflow, result.frame


def pit_auctions(ctx: ForecastContext) -> pl.DataFrame:
    """PIT 过滤：release_time <= data_cutoff（§7.2 硬规则 1/2）。"""
    return ctx.auctions.filter(pl.col("release_time").dt.date() <= ctx.through)


def announced_auctions(ctx: ForecastContext) -> pl.DataFrame:
    """公告层 PIT 过滤：官方 announcement_date <= data_cutoff（日粒度保守）。

    announcement_date 来自 Treasury Auction Results API 的 announcemt_date 字段，
    是拍卖公告的官方发布日（覆盖 1979 起全部历史），不是采集日，也不是事后推断；
    缺失公告日的“未出结果”条目直接不进入公告层（不回填、不推断）。
    """
    return ctx.auctions.filter(
        (
            pl.col("announcement_date").is_not_null()
            & (pl.col("announcement_date") <= ctx.through)
        )
        # 结果已官方发布（release_time<=cutoff）→ 即使公告日缺失也可用；
        # 仅对“尚未发布结果”的条目要求 announcement_date（不推断、不回填）。
        | (
            (pl.col("data_status") == "RESULT")
            & (pl.col("release_time").dt.date() <= ctx.through)
        )
    )


def resulted_auctions(ctx: ForecastContext) -> pl.DataFrame:
    """结果层 PIT：仅 release_time <= cutoff 且已有 total_accepted 的 RESULT 行。"""
    return pit_auctions(ctx).filter(
        (pl.col("data_status") == "RESULT")
        & pl.col("total_accepted_usd_millions").is_not_null()
    )


# ── 组件公共工具 ───────────────────────────────────────────────────────


def _empty_components() -> pl.DataFrame:
    return pl.DataFrame(schema=COMPONENT_SCHEMA)


def _rows_to_components(rows: list[dict]) -> pl.DataFrame:
    if not rows:
        return _empty_components()
    return pl.DataFrame(rows, schema=COMPONENT_SCHEMA)


def _band(source_type: str, model: dict) -> float:
    return float(model["bands"].get(source_type, 0.5))


def _confidence(source_type: str, model: dict) -> float:
    return float(model["confidence"].get(source_type, 0.5))


def _component_row(
    target_date: date, category: str, amount: float, source_type: str,
    method: str, model: dict, event_key: str | None = None, detail: str = "",
) -> dict:
    band = _band(source_type, model) * abs(amount)
    return {
        "target_date": target_date, "category": category, "predicted": amount,
        "lower": amount - band, "upper": amount + band,
        "method": method, "confidence": _confidence(source_type, model),
        "source_type": source_type, "event_key": event_key, "detail": detail,
        "scenario_adjustment": 1.0,
    }


def trend_factor(
    sub: pl.DataFrame, through: date, months: int, cap: tuple[float, float]
) -> float:
    """近 N 月总量 / 前 N 月总量（截断到 cap；前段缺失/非正 → 1.0）。"""
    recent_start = through - timedelta(days=30 * months)
    prior_start = recent_start - timedelta(days=30 * months)
    recent = sub.filter((pl.col("date") > recent_start) & (pl.col("date") <= through))
    prior = sub.filter((pl.col("date") > prior_start) & (pl.col("date") <= recent_start))
    if prior.is_empty() or float(prior["amount"].sum()) <= 0:
        return 1.0
    ratio = float(recent["amount"].sum()) / float(prior["amount"].sum())
    return min(max(ratio, cap[0]), cap[1])


def run_rate_and_month_rate(
    sub: pl.DataFrame, through: date, model: dict
) -> tuple[float, float]:
    """(近 4 周日均运行率, 去年同月日均率)——§6.5 P0 步骤 4 的两路混合输入。"""
    weeks = int(model["windows"]["recent_run_rate_weeks"])
    recent = sub.filter(pl.col("date") > through - timedelta(weeks=weeks))
    run_rate = float(recent["amount"].sum()) / max(
        len(business_days(through - timedelta(weeks=weeks), through)), 1
    )
    same_month = sub.filter(
        (pl.col("date").dt.month() == through.month)
        & (pl.col("date") <= through - timedelta(days=365))
        & (pl.col("date") > through - timedelta(days=365 + 62))
    )
    if same_month.is_empty():
        return run_rate, run_rate
    last_year, last_month = through.year - 1, through.month
    month_days = business_days(
        date(last_year, last_month, 1),
        date(last_year, last_month, pycal.monthrange(last_year, last_month)[1]),
    )
    month_rate = float(same_month["amount"].sum()) / max(len(month_days), 1)
    return run_rate, month_rate


def tax_event_dates(calendar: dict, years: range) -> dict[str, list[date]]:
    """税收事件日历：MM-DD 顺延到联邦工作日（§6.5 P0 步骤 1）。

    Returns:
        {event_id: [顺延后的截止日]}（覆盖 years 内全部年份）。
    """
    out: dict[str, list[date]] = {}
    for event in calendar.get("tax_events", []):
        dates: list[date] = []
        for year in years:
            for md in event["due_dates"]:
                month, day = (int(x) for x in md.split("-"))
                due = date(year, month, day)
                dates.append(due if is_business_day(due) else next_business_day(due))
        out[str(event["id"])] = sorted(set(dates))
    return out


def subcategory_event_ids(calendar: dict) -> dict[str, set[str]]:
    """subcategory → 生效事件 id 集合（calendar.tax_events 关联）。"""
    mapping: dict[str, set[str]] = defaultdict(set)
    for event in calendar.get("tax_events", []):
        for sub in event.get("subcategories", []):
            mapping[sub].add(str(event["id"]))
    return mapping


def monthly_event_dates(
    calendar: dict, start: date, end: date
) -> dict[str, list[date]]:
    """月度/半月度事件：按 day_of_month 生成 [start, end] 内的锚点。

    顺延规则显式配置（税 next / 福利 previous），不做隐式推断。
    """
    out: dict[str, list[date]] = {}
    for event in calendar.get("monthly_events", []):
        raw_days = event.get("day_of_month", [])
        days = [int(raw_days)] if isinstance(raw_days, int) else [int(x) for x in raw_days]
        shift = str(event.get("shift", "next"))
        dates: list[date] = []
        index = start.year * 12 + start.month - 1
        end_index = end.year * 12 + end.month - 1
        while index <= end_index:
            year, month = divmod(index, 12)
            month += 1
            last = pycal.monthrange(year, month)[1]
            for day in days:
                if day > last:
                    continue
                d = date(year, month, day)
                if not is_business_day(d):
                    if shift == "previous":
                        d = prev_business_day(d)
                    elif shift == "next":
                        d = next_business_day(d)
                if start <= d <= end:
                    dates.append(d)
            index += 1
        out[str(event["id"])] = sorted(set(dates))
    return out


def first_of_month_dates(
    calendar: dict, start: date, end: date
) -> list[date]:
    """月初福利发放日（rule=day_of_month，遇非工作日按 shift 提前/顺延）。"""
    cfg = calendar.get("outlay_cycles", {}).get("first_of_month", {})
    if not cfg or cfg.get("rule") != "day_of_month":
        return []
    day = int(cfg.get("day_of_month", 1))
    shift = str(cfg.get("shift", "previous"))
    dates: list[date] = []
    index = start.year * 12 + start.month - 1
    end_index = end.year * 12 + end.month - 1
    while index <= end_index:
        year, month = divmod(index, 12)
        month += 1
        if day <= pycal.monthrange(year, month)[1]:
            d = date(year, month, day)
            if not is_business_day(d):
                d = prev_business_day(d) if shift == "previous" else next_business_day(d)
            if start <= d <= end:
                dates.append(d)
        index += 1
    return sorted(set(dates))


def calendar_events_by_subcategory(
    ctx: ForecastContext,
) -> dict[str, list[tuple[str, date]]]:
    """subcategory → [(事件标签, 锚点日期)]；年度税期 + 月度税/福利 + 社保。

    历史锚点覆盖 hist_start 起，未来锚点覆盖到 max_horizon（含跨年）。
    """
    start = ctx.hist_start
    end = ctx.origin + timedelta(days=ctx.max_horizon)
    years = range(start.year - 1, end.year + 1)
    annual = tax_event_dates(ctx.calendar, years)
    monthly = monthly_event_dates(ctx.calendar, start, end)
    mapping: dict[str, list[tuple[str, date]]] = defaultdict(list)

    for event in ctx.calendar.get("tax_events", []):
        label = f"TAX:{event['id']}"
        for sub in event.get("subcategories", []):
            mapping[str(sub)].extend((label, d) for d in annual.get(str(event["id"]), []))
    for event in ctx.calendar.get("monthly_events", []):
        label = f"{event.get('category', 'TAX')}:{event['id']}"
        for sub in event.get("subcategories", []):
            mapping[str(sub)].extend((label, d) for d in monthly.get(str(event["id"]), []))

    ss_cfg = ctx.calendar.get("outlay_cycles", {}).get("social_security", {})
    ss_sub = str(ss_cfg.get("subcategory", "social_security"))
    mapping[ss_sub].extend(("OUTLAY:social_security", d) for d in _social_security_dates(ctx))

    first_cfg = ctx.calendar.get("outlay_cycles", {}).get("first_of_month", {})
    if first_cfg:
        dates = first_of_month_dates(ctx.calendar, start, end)
        for sub in first_cfg.get("subcategories", []):
            mapping[str(sub)].extend(("OUTLAY:first_of_month", d) for d in dates)
    return mapping


def _social_security_dates(ctx: ForecastContext) -> list[date]:
    """历史 + 未来窗口内的社保支付日（每月第 2/3/4 个周三，顺延到工作日）。"""
    cfg = ctx.calendar.get("outlay_cycles", {}).get("social_security", {})
    if not cfg or cfg.get("rule") != "nth_weekday_of_month":
        return []
    weekday = int(cfg["weekday"])
    ordinals = [int(x) for x in cfg["ordinals"]]
    end_index = ctx.origin.year * 12 + ctx.origin.month - 1 + (ctx.max_horizon // 28) + 1
    out: list[date] = []
    index = ctx.hist_start.year * 12 + ctx.hist_start.month - 1
    while index <= end_index:
        year, month = divmod(index, 12)
        month += 1
        for raw in nth_weekday_of_month(year, month, weekday, ordinals):
            out.append(raw if is_business_day(raw) else next_business_day(raw))
        index += 1
    return out


# ── 国债组件（§6.3/§6.4）──────────────────────────────────────────────


def _coupon_dates(maturity: date, freq: int, lookback_years: int = 8) -> list[date]:
    """自到期日锚定、按频率回溯的付息日（未顺延；调用方统一顺延到工作日）。"""
    step = 12 // freq
    dates: list[date] = []
    k = 1
    floor = date(maturity.year - lookback_years, 1, 1)
    while True:
        d = add_months(maturity, -k * step)
        if d < floor:
            break
        dates.append(d)
        k += 1
    return dates


def _principal_by_cusip(auctions: pl.DataFrame) -> dict[str, float]:
    """CUSIP → 未偿本金（PIT 已发布的 RESULT 行 total_accepted 求和；§6.4）。"""
    res = auctions.filter(
        (pl.col("data_status") == "RESULT")
        & pl.col("total_accepted_usd_millions").is_not_null()
    )
    if res.is_empty():
        return {}
    return {
        cusip: float(amt)
        for cusip, amt in res.group_by("cusip")
        .agg(pl.col("total_accepted_usd_millions").sum())
        .iter_rows()
    }


def debt_component(ctx: ForecastContext) -> pl.DataFrame:
    """发行（三层）/到期现金流（§6.3/§6.4；付息在 interest_component）。"""
    model = ctx.model
    # 第一/二层取“公告可用”集合（announcement_date <= cutoff），结果是否可用
    # 由 release_time 单独判定；第三层只学习 release_time <= cutoff 的结果。
    announced = announced_auctions(ctx)
    resulted = resulted_auctions(ctx)
    rows: list[dict] = []
    horizon_end = ctx.origin + timedelta(days=ctx.max_horizon)

    # 第一/二层：已公告发行（issue_date 落窗；结果已发布用 RESULT，否则用公告规模）
    seen_issue: set[tuple[str, str, date]] = set()
    candidates = announced.filter(
        pl.col("issue_date").is_not_null()
        & (pl.col("issue_date") > ctx.origin)
        & (pl.col("issue_date") <= horizon_end)
    ).with_columns(
        pl.when(
            (pl.col("data_status") == "RESULT")
            & (pl.col("release_time").dt.date() <= ctx.through)
            & pl.col("total_accepted_usd_millions").is_not_null()
        )
        .then(0).otherwise(1).alias("_pit_rank")
    ).sort(["issue_date", "_pit_rank"])
    for row in candidates.iter_rows(named=True):
        key = (str(row["security_type"]), str(row["security_term"]), row["issue_date"])
        if key in seen_issue:  # results 与 upcoming 重叠：同一事件只计一次（§6.1 规则 1）
            continue
        accepted = row["total_accepted_usd_millions"]
        finalized = (
            row["data_status"] == "RESULT"
            and accepted is not None
            and row["release_time"].date() <= ctx.through
        )
        if finalized:
            price = row["price_per_100"]
            cash = float(accepted) * (float(price) / 100.0 if price is not None else 1.0)
            source, method = "KNOWN_FINAL", "auction_result"
        else:
            offering = row["offering_amount_usd_millions"]
            if offering is None:  # 公告缺规模：无法定价，跳过而非伪造
                continue
            cash = float(offering)
            source, method = "KNOWN_PROVISIONAL", "announced_offering"
        seen_issue.add(key)
        rows.append(_component_row(
            row["issue_date"], "DEBT_ISSUANCE", cash, source, method, model,
            event_key=f"ISSUE:{row['cusip']}:{row['issue_date']}",
            detail=f"{row['security_type']} {row['security_term']}",
        ))

    # 到期偿还（§6.4）：maturity 落窗且已发行（announcement<=cutoff）→ 本金流出；
    # 到期日落在周末/假日时顺延到下一工作日（财政部次一工作日支付）
    # 同一 CUSIP 的多次 reopening 共享同一到期日与同一笔记账日，故按 CUSIP 去重，
    # 只在该 CUSIP 的到期日计一次（本金已由 _principal_by_cusip 汇总全部 reopening）。
    principal = _principal_by_cusip(resulted)
    grid_set = set(ctx.grid)
    seen_maturity: set[str] = set()
    for row in announced.filter(
        pl.col("maturity_date").is_not_null()
        & (pl.col("maturity_date") > ctx.origin - timedelta(days=7))
        & (pl.col("maturity_date") <= horizon_end)
        & (pl.col("issue_date") <= ctx.through)
    ).sort("maturity_date").iter_rows(named=True):
        cusip = str(row["cusip"])
        if cusip in seen_maturity:
            continue
        target = row["maturity_date"]
        if not is_business_day(target):
            target = next_business_day(target)
        if target not in grid_set:
            continue
        seen_maturity.add(cusip)
        offering = row["offering_amount_usd_millions"]
        amount = principal.get(cusip)
        if amount is None:
            if offering is None:
                continue
            amount = float(offering)
        source = "KNOWN_FINAL" if cusip in principal else "KNOWN_PROVISIONAL"
        rows.append(_component_row(
            target, "DEBT_REDEMPTION", -amount, source,
            "maturity_principal", model,
            event_key=f"REDEEM:{cusip}:{target}",
            detail=f"{row['security_type']} {row['security_term']}",
        ))

    # 第三层：未公告发行节奏预测（§6.3；CMB 不预测）
    rows.extend(_predicted_issuance(ctx, announced, resulted))
    return _rows_to_components(rows)


def _issuance_scale(
    ctx: ForecastContext,
    group: pl.DataFrame,
    median_gap: int,
    median_offering: float,
    cal_cfg: dict,
) -> float:
    """Layer 3 规模回灌校正：近 trailing_weeks 实际发行 / 外推隐含发行。

    外推隐含 = 规模中位数 × (窗口天数 / 中位间隔)。仅用 cutoff 前历史（PIT），
    比例截断到 scale_cap，防止个别停发/新券种放大。
    """
    if median_gap <= 0 or median_offering <= 0:
        return 1.0
    trailing_weeks = int(cal_cfg.get("trailing_weeks", 26))
    cap = tuple(cal_cfg.get("scale_cap", [0.75, 1.35]))
    recent = group.filter(
        pl.col("issue_date") > ctx.through - timedelta(weeks=trailing_weeks)
    )
    actual = float(recent["offering_amount_usd_millions"].sum())
    implied = median_offering * (trailing_weeks * 7 / median_gap)
    if implied <= 0:
        return 1.0
    return min(max(actual / implied, float(cap[0])), float(cap[1]))


def _seasonal_issuance_dates(
    ctx: ForecastContext, group: pl.DataFrame
) -> list[date]:
    """去年同日历窗口（−364 天）的真实发行日期重放（季节性节奏）。

    规模仍用当前窗口的发行规模中位数（不做水平平移，避免与规模偏差叠加）。
    PIT：历史窗口终点 = origin + max_horizon − 364 天，恒早于 cutoff。
    """
    shift = timedelta(days=364)
    lo = ctx.origin - shift
    hi = ctx.origin + timedelta(days=ctx.max_horizon) - shift
    hist = group.filter(
        (pl.col("issue_date") > lo) & (pl.col("issue_date") <= hi)
    ).sort("issue_date")
    out: list[date] = []
    for issue_date in hist["issue_date"].to_list():
        target = issue_date + shift
        if not is_business_day(target):
            target = next_business_day(target)
        if target > ctx.origin:
            out.append(target)
    return out


def _predicted_issuance(
    ctx: ForecastContext, announced: pl.DataFrame, resulted: pl.DataFrame
) -> list[dict]:
    """Layer 3：按 (security_type, security_term) 发行节奏外推（§6.3 第三层）。

    announced：公告层 PIT 集合（用于与已公告事件去重）；
    resulted：结果层 PIT 集合（仅用于节奏/规模学习，不含未来结果）。
    """
    cfg = ctx.model["issuance"]
    cal_cfg: dict = cfg.get("calibration") or {}
    mode = str(cal_cfg.get("mode", "none"))
    weeks = int(cfg["cadence_history_weeks"])
    history = resulted.filter(
        (pl.col("issue_date") <= ctx.through)
        & (pl.col("issue_date") > ctx.through - timedelta(weeks=weeks))
        & (~pl.col("is_cmb"))
        & (pl.col("offering_amount_usd_millions").is_not_null())
    )
    # 去重锚点：已公告且可定价的 issue_date（与 Layer 3 外推同日/近日的跳过）
    known: set[tuple[str, str, date]] = {
        (str(t), str(s), d)
        for t, s, d, offering, accepted, release in announced.filter(
            pl.col("issue_date").is_not_null()
        ).select(
            "security_type", "security_term", "issue_date",
            "offering_amount_usd_millions", "total_accepted_usd_millions",
            "release_time",
        ).iter_rows()
        if offering is not None
        or (accepted is not None and release.date() <= ctx.through)
    }
    horizon_end = ctx.origin + timedelta(days=ctx.max_horizon)
    rows: list[dict] = []
    for (sec_type, term), group in history.group_by(["security_type", "security_term"]):
        issues = sorted(set(group["issue_date"].to_list()))
        if len(issues) < 3:
            continue
        intervals = sorted(
            i for i in (
                (b - a).days for a, b in zip(issues, issues[1:], strict=False)
            ) if 0 < i <= 120  # 剔除停发期
        )
        if not intervals:
            continue
        median_gap = intervals[len(intervals) // 2]
        offerings = sorted(
            group.sort("issue_date")["offering_amount_usd_millions"].to_list()[
                -int(cfg["offering_median_count"]):
            ]
        )
        median_offering = float(offerings[len(offerings) // 2])
        scale = (
            _issuance_scale(ctx, group, median_gap, median_offering, cal_cfg)
            if mode == "trailing_scale" else 1.0
        )
        term_days = _term_days(str(term))
        grid_set = set(ctx.grid)
        schedule: list[tuple[date, float]] = []
        if mode == "seasonal_window":
            schedule = [
                (d, median_offering)
                for d in _seasonal_issuance_dates(ctx, group)
            ]
        if not schedule:  # none / 同窗口无样本回退
            nxt = issues[-1] + timedelta(days=median_gap)
            while nxt <= horizon_end:
                if not is_business_day(nxt):
                    nxt = next_business_day(nxt)
                schedule.append((nxt, median_offering * scale))
                nxt += timedelta(days=median_gap)
        for nxt, amount in schedule:
            if nxt not in grid_set:
                continue
            dedup_ok = all(
                (str(t), str(s)) != (str(sec_type), str(term))
                or abs((d - nxt).days) > int(cfg["cadence_dedupe_days"])
                for t, s, d in known
            )
            if not dedup_ok:
                continue
            rows.append(_component_row(
                nxt, "DEBT_ISSUANCE", amount, "SEASONAL",
                "issuance_cadence", ctx.model,
                event_key=f"PRED-ISSUE:{sec_type}:{term}:{nxt}",
                detail=f"{sec_type} {term}",
            ))
            if term_days and nxt + timedelta(days=term_days) <= horizon_end:
                maturity = nxt + timedelta(days=term_days)
                # 到期日落在假日时顺延；网格外（如感恩节）不得进入组件
                if not is_business_day(maturity):
                    maturity = next_business_day(maturity)
                if maturity in grid_set:
                    rows.append(_component_row(
                        maturity, "DEBT_REDEMPTION",
                        -amount, "SEASONAL", "issuance_cadence",
                        ctx.model,
                        event_key=f"PRED-REDEEM:{sec_type}:{term}:{nxt}",
                        detail=f"{sec_type} {term}",
                    ))
    return rows


def _term_days(term: str) -> int | None:
    """期限描述 → 天数（仅 Bills 在 30 天窗内有意义；Notes/Bonds → None）。"""
    t = term.lower()
    if "week" in t:
        try:
            return int(t.split("-")[0]) * 7
        except ValueError:
            return None
    return None


def _historical_coupon_days(ctx: ForecastContext, freq_map: dict[str, int]) -> set[date]:
    """历史 coupon 日标记（用于从付息基线中剔除付息大额日，避免双计）。"""
    auctions = pit_auctions(ctx)
    res = auctions.filter(
        (pl.col("data_status") == "RESULT")
        & pl.col("interest_rate").is_not_null()
        & pl.col("maturity_date").is_not_null()
        & pl.col("interest_payment_frequency").is_in(list(freq_map))
        & (~pl.col("security_type").str.contains("FRN"))
    )
    lo = ctx.through - timedelta(weeks=int(ctx.model["windows"]["coupon_baseline_weeks"]))
    days: set[date] = set()
    for row in res.iter_rows(named=True):
        freq = freq_map[str(row["interest_payment_frequency"])]
        for raw in _coupon_dates(row["maturity_date"], freq):
            pay = raw if is_business_day(raw) else next_business_day(raw)
            if lo <= pay <= ctx.through:
                days.add(pay)
    return days


def interest_component(ctx: ForecastContext) -> pl.DataFrame:
    """付息：固定利率 coupon 事件 + 非付息日基线（§6.4；FRN/TIPS P0 近似）。"""
    model = ctx.model
    freq_map: dict[str, int] = dict(model["issuance"]["coupon_frequencies"])
    auctions = pit_auctions(ctx)
    horizon_end = ctx.origin + timedelta(days=ctx.max_horizon)
    rows: list[dict] = []
    coupon_days: set[date] = set()

    res = auctions.filter(
        (pl.col("data_status") == "RESULT")
        & pl.col("interest_rate").is_not_null()
        & pl.col("maturity_date").is_not_null()
        & (pl.col("maturity_date") >= ctx.origin)
        & pl.col("interest_payment_frequency").is_in(list(freq_map))
        & (~pl.col("security_type").str.contains("FRN"))
    )
    principal = _principal_by_cusip(res)
    seen: set[tuple[str, date]] = set()
    for row in res.iter_rows(named=True):
        cusip = str(row["cusip"])
        rate = float(row["interest_rate"])
        freq = freq_map[str(row["interest_payment_frequency"])]
        for raw in _coupon_dates(row["maturity_date"], freq):
            pay = raw if is_business_day(raw) else next_business_day(raw)
            if pay <= ctx.origin or pay > horizon_end:
                continue
            if (cusip, pay) in seen:
                continue
            seen.add((cusip, pay))
            amount = principal.get(cusip, 0.0) * rate / 100.0 / freq
            if amount <= 0:
                continue
            coupon_days.add(pay)
            rows.append(_component_row(
                pay, "INTEREST", -amount, "KNOWN_FINAL", "coupon_schedule", model,
                event_key=f"COUPON:{cusip}:{pay}",
                detail=f"{row['security_type']} {row['security_term']} coupon",
            ))

    # 非付息日基线：历史 INTEREST 剔除历史 coupon 日后的日均中位数与近期
    # 运行率的均值 × 趋势（剔除付息日避免 coupon 双计）
    weeks_window = int(model["windows"]["coupon_baseline_weeks"])
    hist_interest = ctx.cashflow.filter(
        (pl.col("category") == "INTEREST") & (~pl.col("is_subtotal"))
        & pl.col("include_in_net") & (pl.col("date") <= ctx.through)
        & (pl.col("date") > ctx.through - timedelta(weeks=weeks_window))
    ).group_by("date").agg(pl.col("amount_usd_millions").sum().alias("amount"))
    if not hist_interest.is_empty():
        hist_coupon_days = _historical_coupon_days(ctx, freq_map)
        baseline_df = hist_interest.filter(~pl.col("date").is_in(sorted(hist_coupon_days)))
        trend = trend_factor(
            hist_interest, ctx.through, int(model["windows"]["trend_months"]),
            tuple(model["trend_cap"]),
        )
        baseline_weeks = int(model["windows"]["interest_baseline_weeks"])
        recent = hist_interest.filter(
            pl.col("date") > ctx.through - timedelta(weeks=baseline_weeks)
        )
        run_rate = float(recent["amount"].sum()) / max(baseline_weeks * 5, 1)
        daily = sorted(baseline_df["amount"].to_list()) if not baseline_df.is_empty() else [0.0]
        median_daily = daily[len(daily) // 2]
        for d in ctx.grid:
            if d in coupon_days:
                continue
            amount = -(median_daily + run_rate) / 2 * trend
            rows.append(_component_row(
                d, "INTEREST", amount, "STATISTICAL", "interest_baseline", model,
                detail="non-coupon-day baseline",
            ))
    return _rows_to_components(rows)


# ── 税收 / 支出 / 其他组件（§6.5/§6.6/§6.7）──────────────────────────


def tax_component(ctx: ForecastContext) -> pl.DataFrame:
    """TAX（流入）+ REFUND（流出）：事件曲线 + 运行率/同月率混合。"""
    rows: list[dict] = []
    rows.extend(_seasonal_flows(
        ctx, categories=("TAX",), direction="INFLOW", sign=1.0,
        base_category="TAX", use_events=True,
    ))
    rows.extend(_seasonal_flows(
        ctx, categories=("REFUND",), direction="OUTFLOW", sign=-1.0,
        base_category="REFUND", use_events=False,
    ))
    return _rows_to_components(rows)


def outlay_component(ctx: ForecastContext) -> pl.DataFrame:
    """BENEFIT 支出：社保周三规则 + 其他子类通用日分布（§6.6）。"""
    return _rows_to_components(_seasonal_flows(
        ctx, categories=("BENEFIT",), direction="OUTFLOW", sign=-1.0,
        base_category="BENEFIT", use_events=True,
    ))


def other_component(ctx: ForecastContext) -> pl.DataFrame:
    """OTHER / UNCLASSIFIED 双向基线（§6.7；低置信度，单独展示）。"""
    rows: list[dict] = []
    for category, direction, sign in (
        ("OTHER", "INFLOW", 1.0), ("OTHER", "OUTFLOW", -1.0),
        ("UNCLASSIFIED", "INFLOW", 1.0), ("UNCLASSIFIED", "OUTFLOW", -1.0),
    ):
        source = "UNCLASSIFIED" if category == "UNCLASSIFIED" else "STATISTICAL"
        rows.extend(_seasonal_flows(
            ctx, categories=(category,), direction=direction, sign=sign,
            base_category=category, use_events=False, force_source=source,
        ))
    return _rows_to_components(rows)


def _seasonal_flows(
    ctx: ForecastContext,
    *,
    categories: tuple[str, ...],
    direction: str,
    sign: float,
    base_category: str,
    use_events: bool,
    force_source: str | None = None,
) -> list[dict]:
    """子类级季节性预测（§6.5 P0 / §6.6 P0 的统一实现）。

    每个子类独立：
    1. 事件日（tax_event 命中 / 社保周三）→ 同事件相对日中位数曲线 × 趋势；
    2. 非事件日 → (近 4 周运行率 + 去年同月日均率) / 2 × 趋势。
    全部统计输入截至 ctx.through（PIT：无未来数据参与）。
    """
    model = ctx.model
    hist = ctx.hist(categories, direction)
    if hist.is_empty():
        return []
    use_events = use_events and bool(ctx.calendar.get("enabled", True))
    cap = tuple(model["trend_cap"])
    window = int(ctx.calendar.get("event_window_business_days", 10))
    anchors_by_sub = calendar_events_by_subcategory(ctx) if use_events else {}
    # 工作日序号缓存：事件曲线需对 (日期, 锚点) 反复求相对工作日序号，
    # 逐日循环版在月度事件下是 O(百万) 级；改用一次建表 + O(1) 查表。
    span_start = date(ctx.hist_start.year - 1, 1, 1) - timedelta(days=45)
    span_end = ctx.origin + timedelta(days=ctx.max_horizon) + timedelta(days=45)
    bd_pos = {d: i for i, d in enumerate(business_days(span_start, span_end))}

    rows: list[dict] = []
    stale_after = ctx.through - timedelta(days=STALE_SUBCATEGORY_DAYS)
    for key, sub_df in hist.partition_by("subcategory", as_dict=True).items():
        sub_name = str(key[0]) if key[0] is not None else f"{base_category.lower()}_other"
        # 停更/停用子类保护：最后一笔历史早于 cutoff 逾 STALE_SUBCATEGORY_DAYS
        # （如 canonical 分类变更后消失的 federal_tax_deposits）时不再外推，
        # 否则事件曲线会把陈旧历史复活成幻影现金流。缺口只记录在归因里。
        if sub_df["date"].max() < stale_after:
            continue
        trend = trend_factor(
            sub_df, ctx.through, int(model["windows"]["trend_months"]), cap
        )
        run_rate, month_rate = run_rate_and_month_rate(sub_df, ctx.through, model)
        baseline = (run_rate + month_rate) / 2 * trend

        # 事件锚点（历史 + 未来）：rel_idx → 金额样本
        anchors: list[tuple[str, date]] = list(anchors_by_sub.get(sub_name, []))

        curve: dict[int, list[float]] = defaultdict(list)
        if anchors:
            for d, amount in zip(sub_df["date"], sub_df["amount"], strict=True):
                for _, anchor in anchors:
                    if anchor - timedelta(days=window + 6) <= d <= anchor + timedelta(
                        days=window + 6
                    ):
                        pos_d, pos_a = bd_pos.get(d), bd_pos.get(anchor)
                        if pos_d is None or pos_a is None:
                            continue
                        rel = pos_d - pos_a
                        if abs(rel) <= window:
                            curve[rel].append(float(amount))

        day_rows: list[tuple[date, dict, bool]] = []
        for d in ctx.grid:
            event_hit: tuple[int, float] | None = None
            event_key: str | None = None
            if anchors:
                best: tuple[int, float] | None = None
                best_key: str | None = None
                for label, anchor in anchors:
                    pos_d, pos_a = bd_pos.get(d), bd_pos.get(anchor)
                    if pos_d is None or pos_a is None:
                        continue
                    rel = pos_d - pos_a
                    if abs(rel) <= window and len(curve.get(rel, [])) >= 2:
                        if best is None or abs(rel) < abs(best[0]):
                            values = sorted(curve[rel])
                            best = (rel, values[len(values) // 2] * trend)
                            best_key = f"{label}:{anchor}:{rel:+d}"
                if best is not None and best[1] != 0:
                    event_hit, event_key = best, best_key
            if event_hit is not None:
                amount = sign * event_hit[1]
                source = force_source or "SEASONAL"
                day_rows.append((d, _component_row(
                    d, base_category, amount, source, "event_curve", model,
                    event_key=event_key, detail=sub_name,
                ), True))
            else:
                amount = sign * baseline
                if amount == 0:
                    continue
                source = force_source or (
                    "SEASONAL" if base_category in ("REFUND", "BENEFIT") else "STATISTICAL"
                )
                day_rows.append((d, _component_row(
                    d, base_category, amount, source, "run_rate+same_month",
                    model, detail=sub_name,
                ), False))
        # 月度水平守恒：事件曲线只改变“月内时点分布”，不改变该子类的月度总量。
        # 否则事件日样本中位数逐日累加会给每个预测月叠加水平偏移（实测 90 天
        # 约 +540B），违背“修峰谷不平移”的目标。基线本身仍是 PIT 统计量。
        if use_events and any(is_event for _, _, is_event in day_rows):
            by_month: dict[tuple[int, int], list[int]] = defaultdict(list)
            for i, (d, _, _) in enumerate(day_rows):
                by_month[(d.year, d.month)].append(i)
            for idxs in by_month.values():
                expected = sum(sign * baseline for _ in idxs)
                actual = sum(day_rows[i][1]["predicted"] for i in idxs)
                if expected == 0 or actual == 0:
                    continue
                ratio = min(max(expected / actual, 0.5), 2.0)
                for i in idxs:
                    for field in ("predicted", "lower", "upper"):
                        day_rows[i][1][field] *= ratio
        rows.extend(row for _, row, _ in day_rows)
    return rows


# ── 情景应用（§6.8，D-TGA-11）────────────────────────────────────────


def apply_scenario(
    components: pl.DataFrame, scenario_cfg: dict, scenarios_cfg: dict
) -> pl.DataFrame:
    """对预测类组件应用情景乘法因子；已知事件绝不调整。

    - 类别 → 参数映射：tax/issuance/redemption 按 category_params，
      其余（BENEFIT/INTEREST/OTHER/UNCLASSIFIED）按 outlay；
    - 因子截断到 limits.max_abs_adjustment；base 情景因子恒 1；
    - 调整记录于 scenario_adjustment 审计列（§6.8 规则 2）。
    """
    limits = scenarios_cfg.get("limits", {})
    if limits.get("apply_to_known", False):
        raise RuntimeError("D-TGA-11：scenarios.limits.apply_to_known 必须为 false")
    max_adj = float(limits.get("max_abs_adjustment", 0.20))
    cat_map = scenarios_cfg.get("category_params", {})
    tax_adj = float(scenario_cfg.get("tax_adjustment", 0.0))
    outlay_adj = float(scenario_cfg.get("outlay_adjustment", 0.0))
    iss_adj = float(scenario_cfg.get("issuance_adjustment", 0.0))
    red_adj = float(scenario_cfg.get("redemption_adjustment", 0.0))

    df = components.with_columns(
        pl.col("scenario_adjustment").fill_null(1.0)
    )
    known = pl.col("source_type").str.starts_with("KNOWN_")
    factor = (
        pl.when(known)
        .then(1.0)
        .when(pl.col("category").is_in(cat_map.get("tax", ["TAX", "REFUND"])))
        .then(1.0 + tax_adj)
        .when(pl.col("category").is_in(cat_map.get("issuance", ["DEBT_ISSUANCE"])))
        .then(1.0 + iss_adj)
        .when(pl.col("category").is_in(cat_map.get("redemption", ["DEBT_REDEMPTION"])))
        .then(1.0 + red_adj)
        .otherwise(1.0 + outlay_adj)
    )
    return df.with_columns(
        factor.clip(1.0 - max_adj, 1.0 + max_adj).alias("scenario_adjustment")
    ).with_columns(
        (pl.col("predicted") * pl.col("scenario_adjustment")).alias("predicted"),
        (pl.col("lower") * pl.col("scenario_adjustment")).alias("lower"),
        (pl.col("upper") * pl.col("scenario_adjustment")).alias("upper"),
    )


# ── 融资压力（§6.9，P0 展示用途）──────────────────────────────────────


def financing_pressure(
    service, through: date, horizon_end: date, forecast_df: pl.DataFrame
) -> dict | None:
    """TGA 目标参考（Refunding 现金余额假设）与融资缺口（只作压力估计）。

    - 取 release_time <= through 的最新公告（PIT）；
    - 优先选 forecast_period 覆盖预测终点的记录，否则取最新一条；
    - FinancingGap = cash_balance_assumption - predicted_tga(终点)；
      **不**把它当作未来发行金额（§6.9）。
    """
    result = service.query(
        dataset_id=_REFUNDING_DS,
        start=datetime(through.year - 2, 1, 1),
        end=datetime(horizon_end.year, horizon_end.month, horizon_end.day)
        + timedelta(days=1),
    )
    df = result.frame
    if df.is_empty():
        return None
    df = df.filter(pl.col("release_time").dt.date() <= through)
    if df.is_empty():
        return None
    # 覆盖预测终期的记录优先，取 publication_time 最新
    covering = df.filter(pl.col("forecast_period_end") >= pl.lit(horizon_end))
    pick = (
        covering.sort("publication_time", descending=True).head(1)
        if not covering.is_empty()
        else df.sort("publication_time", descending=True).head(1)
    )
    row = pick.row(0, named=True)
    cash_target = row["cash_balance_assumption_usd_millions"]
    if cash_target is None:
        return None
    boundary = forecast_df.filter(pl.col("is_horizon_boundary")).sort(
        "horizon"
    )
    if boundary.is_empty():
        return None
    last = boundary.row(-1, named=True)
    return {
        "cash_target": float(cash_target),
        "period_end": row["forecast_period_end"],
        "publication_time": row["publication_time"],
        "predicted_tga": float(last["predicted_tga"]),
        "gap": float(cash_target) - float(last["predicted_tga"]),
    }


# ── 聚合与预测表（§6.1/§6.8/§6.10）───────────────────────────────────


def _monthly_totals(
    df: pl.DataFrame, value_col: str
) -> dict[tuple[int, int], float]:
    """按自然月汇总指定值列（守恒校验用）。"""
    if df.is_empty():
        return {}
    grouped = df.group_by(
        pl.col("target_date").dt.year().alias("_year"),
        pl.col("target_date").dt.month().alias("_month"),
    ).agg(pl.col(value_col).sum().alias("_total"))
    return {
        (int(row["_year"]), int(row["_month"])): float(row["_total"])
        for row in grouped.iter_rows(named=True)
    }


def _group_month_keys(df: pl.DataFrame) -> set[tuple[int, int, str, str]]:
    """(年, 月, category, detail) 身份集合；检测去重造成的子类/月份丢失。"""
    if df.is_empty():
        return set()
    rows = df.select(
        pl.col("target_date").dt.year().alias("_year"),
        pl.col("target_date").dt.month().alias("_month"),
        pl.col("category"),
        pl.col("detail"),
    ).unique().rows()
    return {(int(y), int(m), str(c), str(d)) for y, m, c, d in rows}


def aggregate_forecast(
    ctx: ForecastContext, components: pl.DataFrame
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """组件聚合 → 逐日净现金流 + TGA 累计（去重、守恒、horizon 分桶）。

    Returns:
        (去重后的组件 DataFrame, tga_forecast DataFrame)。

    Raises:
        RuntimeError: 守恒断言失败（Q-TGA-012 ERROR）。
    """
    if components.is_empty():
        return components, pl.DataFrame(
            schema={
                "target_date": pl.Date, "horizon": pl.Int64,
                "is_horizon_boundary": pl.Boolean, "predicted_tga": pl.Float64,
                "lower": pl.Float64, "upper": pl.Float64, "daily_net": pl.Float64,
                "source_type": pl.String,
            }
        )
    # §6.1 规则 1：同一 (target_date, category, detail, event_key) 只应用一次
    if "scenario_adjustment" in components.columns:
        components = components.with_columns(
            pl.col("scenario_adjustment").fill_null(1.0)
        )
    # TGA-1：去重前先固定原始组件的月度守恒参照，供去重后核对。
    raw_monthly = _monthly_totals(components, "predicted")
    raw_total = float(components["predicted"].sum())
    if abs(sum(raw_monthly.values()) - raw_total) > _CONSERVATION_TOL:
        raise RuntimeError(
            "Q-TGA-012 pre-dedupe monthly conservation failed: "
            f"monthly_sum={sum(raw_monthly.values())} components={raw_total}"
        )
    raw_baseline = components.filter(pl.col("event_key").is_null())
    raw_events = components.filter(pl.col("event_key").is_not_null())
    raw_baseline_monthly = _monthly_totals(raw_baseline, "predicted")
    raw_baseline_count = raw_baseline.height
    raw_group_month_keys = _group_month_keys(components)
    # 同一去重键下若出现不同金额，说明身份键不足（缺 subcategory/方向等），
    # 继续 keep="first" 会静默丢弃不同现金流 → 显式失败。
    dup_conflicts = (
        raw_events.group_by(_DEDUPE_KEY)
        .agg(
            pl.len().alias("_rows"),
            pl.struct("predicted", "lower", "upper").n_unique().alias("_variants"),
        )
        .filter((pl.col("_rows") > 1) & (pl.col("_variants") > 1))
    )
    if dup_conflicts.height > 0:
        raise RuntimeError(
            "Q-TGA-012 dedupe key collides distinct event components: "
            f"{dup_conflicts.select(_DEDUPE_KEY).head(3).to_dicts()}"
        )
    # 仅事件行去重（同一事件只应用一次）；基线行逐子类/逐方向独立生成，
    # 不参与去重，其守恒由去重前后断言兜底。
    deduped_events = (
        raw_events.sort("confidence", descending=True)
        .unique(subset=_DEDUPE_KEY, keep="first")
    )
    comps = pl.concat([raw_baseline, deduped_events]).sort(
        "confidence", descending=True
    )
    # 防御：组件必须落在预测网格（origin 之后的联邦工作日）
    outside = comps.filter(~pl.col("target_date").is_in(ctx.grid))
    if outside.height > 0:
        raise RuntimeError(
            "components contain target_date outside forecast grid: "
            f"{outside['target_date'].unique().to_list()[:5]}"
        )
    # TGA-1：去重后核对——基线行不得减少、基线月度总量按子类守恒、
    # (月, category, detail) 身份不得消失。任何组件丢失都显式失败。
    baseline = comps.filter(pl.col("event_key").is_null())
    if baseline.height != raw_baseline_count:
        raise RuntimeError(
            "Q-TGA-012 component loss after dedupe: baseline rows "
            f"{raw_baseline_count} -> {baseline.height}"
        )
    dedup_baseline_monthly = _monthly_totals(baseline, "predicted")
    for month, total in raw_baseline_monthly.items():
        got = dedup_baseline_monthly.get(month, 0.0)
        if abs(got - total) > _CONSERVATION_TOL:
            raise RuntimeError(
                "Q-TGA-012 monthly conservation failed after dedupe: "
                f"{month} baseline={total} deduped={got}"
            )
    # 去重后月度总量核对：原始月度合计 − 完全重复事件的重复部分 == 去重后合计。
    # 任何无法解释为“完全重复事件”的组件丢失都会在此显式失败。
    duplicate_extra = (
        raw_events.group_by(_DEDUPE_KEY)
        .agg(pl.len().alias("_rows"), pl.col("predicted").first().alias("predicted"))
        .filter(pl.col("_rows") > 1)
        .with_columns(((pl.col("_rows") - 1) * pl.col("predicted")).alias("predicted"))
        .select("target_date", "predicted")
    )
    expected_monthly = dict(raw_monthly)
    for month, amount in _monthly_totals(duplicate_extra, "predicted").items():
        expected_monthly[month] = expected_monthly.get(month, 0.0) - amount
    deduped_monthly = _monthly_totals(comps, "predicted")
    for month, expected in expected_monthly.items():
        got = deduped_monthly.get(month, 0.0)
        if abs(got - expected) > _CONSERVATION_TOL:
            raise RuntimeError(
                "Q-TGA-012 monthly conservation failed after dedupe: "
                f"{month} expected={expected} deduped={got}"
            )
    lost_groups = raw_group_month_keys - _group_month_keys(comps)
    if lost_groups:
        raise RuntimeError(
            "Q-TGA-012 component loss after dedupe: "
            f"{sorted(lost_groups)[:5]}"
        )
    daily = comps.group_by("target_date").agg(
        pl.col("predicted").sum().alias("daily_net"),
        pl.col("lower").sum().alias("daily_lower"),
        pl.col("upper").sum().alias("daily_upper"),
    ).sort("target_date")
    # Q-TGA-012：组件之和 == 聚合结果（守恒，整体 + 逐月）
    total_components = float(comps["predicted"].sum())
    total_daily = float(daily["daily_net"].sum())
    if abs(total_components - total_daily) > _CONSERVATION_TOL:
        raise RuntimeError(
            f"Q-TGA-012 conservation failed: components={total_components} "
            f"aggregate={total_daily}"
        )
    dedup_monthly = _monthly_totals(comps, "predicted")
    daily_monthly = _monthly_totals(daily, "daily_net")
    for month, total in dedup_monthly.items():
        got = daily_monthly.get(month, 0.0)
        if abs(got - total) > _CONSERVATION_TOL:
            raise RuntimeError(
                "Q-TGA-012 monthly conservation failed: "
                f"{month} components={total} aggregate={got}"
            )
    dominant = (
        comps.with_columns(pl.col("predicted").abs().alias("_abs"))
        .sort("_abs", descending=True)
        .unique(subset="target_date", keep="first")
        .select("target_date", "source_type")
    )
    # 覆盖完整网格：无组件日补零，保证 TGA 累计逐日连续
    grid_df = pl.DataFrame({"target_date": ctx.grid}, schema={"target_date": pl.Date})
    daily = (
        grid_df.join(daily, on="target_date", how="left")
        .join(dominant, on="target_date", how="left")
        .sort("target_date")
        .with_columns(
            pl.col("daily_net").fill_null(0.0),
            pl.col("daily_lower").fill_null(0.0),
            pl.col("daily_upper").fill_null(0.0),
            pl.col("source_type").fill_null("STATISTICAL"),
        )
        .with_columns(
            (ctx.last_tga + pl.col("daily_net").cum_sum()).alias("predicted_tga"),
            (ctx.last_tga + pl.col("daily_lower").cum_sum()).alias("lower"),
            (ctx.last_tga + pl.col("daily_upper").cum_sum()).alias("upper"),
        )
    )
    boundary_dates = {
        max(d for d in ctx.grid if (d - ctx.origin).days <= h) for h in ctx.horizons
    }
    rows = []
    for row in daily.iter_rows(named=True):
        d = row["target_date"]
        horizon = min(h for h in ctx.horizons if (d - ctx.origin).days <= h)
        rows.append({
            "target_date": d, "horizon": horizon,
            "is_horizon_boundary": d in boundary_dates,
            "predicted_tga": row["predicted_tga"], "lower": row["lower"],
            "upper": row["upper"], "daily_net": row["daily_net"],
            "source_type": row["source_type"],
        })
    forecast = pl.DataFrame(rows, schema={
        "target_date": pl.Date, "horizon": pl.Int64,
        "is_horizon_boundary": pl.Boolean, "predicted_tga": pl.Float64,
        "lower": pl.Float64, "upper": pl.Float64, "daily_net": pl.Float64,
        "source_type": pl.String,
    })
    return comps, forecast


def annotate_scenario_output(
    forecast: pl.DataFrame, components: pl.DataFrame, scenario_cfg: dict
) -> pl.DataFrame:
    """给情景预测加机器可读标签，避免下游当作点位预测消费（§8.2）。

    - `output_type` 恒为 `scenario`：T+7～T+90 是条件路径 / 情景锚点；
    - `usage` / `confidence` 取自 scenarios.yaml，缺省 `scenario_anchor` /
      `exploratory`；
    - `drivers` 为当日 |predicted| 最大的至多 3 个类别（`|` 连接）。

    只增列、不改既有列，历史 run 不含这些列时下游按空值处理。
    """
    if forecast.is_empty():
        return forecast
    driver_map: dict[date, list[tuple[float, str]]] = {}
    if not components.is_empty():
        agg = components.group_by(["target_date", "category"]).agg(
            pl.col("predicted").abs().sum().alias("_abs")
        )
        for row in agg.iter_rows(named=True):
            driver_map.setdefault(row["target_date"], []).append(
                (float(row["_abs"]), str(row["category"]))
            )
    drivers = [
        "|".join(c for _, c in sorted(driver_map.get(d, []), reverse=True)[:3])
        for d in forecast["target_date"].to_list()
    ]
    return forecast.with_columns(
        pl.lit("scenario").alias("output_type"),
        pl.lit(str(scenario_cfg.get("usage", "scenario_anchor"))).alias("usage"),
        pl.lit(str(scenario_cfg.get("confidence", "exploratory"))).alias("confidence"),
        pl.Series("drivers", drivers, dtype=pl.String),
    )


# ── 运行编排（§10.1/§10.3/§9.1 Q-TGA-012/013/014）────────────────────


# ── 可复用计算入口（§7.1：run_forecast 与回测共用）──────────────────


def load_config() -> tuple[dict, dict, dict]:
    """加载 model/calendar/scenarios 配置（供 run_forecast 与回测共用）。"""
    return (
        load_yaml(str(MODEL_PATH)),
        load_yaml(str(CALENDAR_PATH)),
        load_yaml(str(SCENARIOS_PATH)),
    )


def load_forecast_context(
    settings: Settings,
    meta,
    origin: date,
    horizons: list[int],
    model: dict,
    calendar_cfg: dict,
    data_cutoff: date | None = None,
) -> ForecastContext:
    """加载预测上下文（PIT：所有输入按发布规则过滤到 cutoff；§7.2）。

    回测复用同一入口 → 与实盘预测完全一致的 PIT 语义。
    """
    through = data_cutoff or data_through(origin)
    balance, cashflow, auctions = load_inputs(settings, meta, through, origin)
    ctx = ForecastContext(
        origin, horizons, model, calendar_cfg, balance, cashflow, auctions,
        through=through,
    )
    # cutoff 以实际可用余额日为准（derived 落后于日历估算时收紧）
    ctx.through = min(ctx.through, ctx.last_balance_date)
    return ctx


def compute_base_components(ctx: ForecastContext) -> pl.DataFrame:
    """未调整基准组件（§6.2-§6.7 五组件；情景由 apply_scenario 在外层应用）。"""
    return pl.concat([
        debt_component(ctx),
        interest_component(ctx),
        tax_component(ctx),
        outlay_component(ctx),
        other_component(ctx),
    ], how="vertical")


def run_forecast(
    settings: Settings,
    origin: date | None = None,
    horizons: list[int] | None = None,
    scenarios: list[str] | None = None,
    data_cutoff: date | None = None,
    *,
    force: bool = False,
    dry_run: bool = False,
) -> tuple[int, dict]:
    """执行一次预测 run。返回 (退出码, 摘要 dict)。

    scenarios 缺省取 config/tga/scenarios.yaml 全部情景（§6.8）；
    每个情景独立聚合、独立分区落盘（run_date/scenario hive）。
    """
    model = load_yaml(str(MODEL_PATH))
    calendar_cfg = load_yaml(str(CALENDAR_PATH))
    scenarios_cfg = load_yaml(str(SCENARIOS_PATH))
    all_scenarios = list((scenarios_cfg.get("scenarios") or {"base": {}}).keys())
    scenarios = [s for s in (scenarios or all_scenarios) if s in all_scenarios]
    horizons = horizons or [int(h) for h in model["horizons"]]
    taxonomy = load_taxonomy()
    chash = config_hash(taxonomy, calendar_cfg, model, scenarios_cfg)
    origin = origin or date.today()

    meta = open_meta(settings)
    try:
        ctx = load_forecast_context(
            settings, meta, origin, horizons, model, calendar_cfg, data_cutoff
        )
        print(f"[1/5] 加载输入（origin={origin} through={ctx.through} "
              f"config_hash={chash} scenarios={scenarios}）")

        if dry_run:
            print(f"[dry-run] last_tga={ctx.last_tga:.0f}M "
                  f"({ctx.last_balance_date})；grid={len(ctx.grid)} 个工作日；"
                  f"horizons={ctx.horizons}；scenarios={scenarios}")
            return 0, {}

        from tga_store import (
            ForecastStore,
            rebuild_views,
            resolve_data_dir,
        )

        store = ForecastStore(resolve_data_dir(settings), meta)
        # 幂等（§10.3）：同一 identity 已有 SUCCESS → 复用，除非 --force
        existing = store.find_success_run(
            origin.isoformat(), ctx.through.isoformat(), MODEL_VERSION, chash,
            scenarios, horizons,
        )
        if existing and not force:
            print(f"已存在同配置 SUCCESS run：{existing}（--force 可重跑）")
            return 0, {"run_id": existing, "reused": True}

        print("[2/5] 运行组件预测（未调整基准组件）")
        base_components = compute_base_components(ctx)

        run_id, _ = store.create_run(
            origin.isoformat(), ctx.through.isoformat(), MODEL_VERSION, chash,
            scenarios, horizons,
        )
        try:
            print("[3/5] 情景聚合与累计 TGA")
            base_forecast: pl.DataFrame | None = None
            n_comp = n_fcst = 0
            for scenario in scenarios:
                adjusted = apply_scenario(
                    base_components, scenarios_cfg["scenarios"][scenario],
                    scenarios_cfg,
                )
                comps, forecast = aggregate_forecast(ctx, adjusted)
                forecast = annotate_scenario_output(
                    forecast, comps, scenarios_cfg["scenarios"][scenario]
                )
                n_comp_, n_fcst_ = store.write_outputs(
                    run_id, origin, scenario, comps, forecast
                )
                n_comp, n_fcst = n_comp + n_comp_, n_fcst + n_fcst_
                if scenario == "base" or base_forecast is None:
                    base_forecast = forecast
                boundary = forecast.filter(pl.col("is_horizon_boundary"))
                marks = "  ".join(
                    f"T+{r['horizon']}:{r['predicted_tga'] / 1000:,.1f}B"
                    for r in boundary.sort("horizon").iter_rows(named=True)
                )
                print(f"    [{scenario}] {marks}")
                if scenario == scenarios[-1]:
                    # Q-TGA 检查以最后一个情景的 forecast 为载体（负值/守恒
                    # 与情景无关，组件级守恒在各情景均已断言）
                    _qtga_checks(meta, ctx, model, forecast)

            print("[4/5] 融资压力（§6.9）")
            service, _ = open_query_service(settings, meta)
            pressure = financing_pressure(
                service, ctx.through,
                base_forecast.filter(pl.col("is_horizon_boundary"))
                .sort("horizon").row(-1, named=True)["target_date"],
                base_forecast,
            )
            if pressure:
                print(f"    现金余额假设（{pressure['period_end']}）："
                      f"{pressure['cash_target'] / 1000:,.1f}B；"
                      f"融资缺口 = {pressure['gap'] / 1000:+,.1f}B"
                      f"（仅压力估计，非发行计划）")

            print("[5/5] finalize run")
            output_hash = hashlib.sha256(
                base_forecast.select("target_date", "predicted_tga")
                .write_csv().encode("utf-8")
            ).hexdigest()[:16]
            store.finalize_run(run_id, "SUCCESS", output_hash=output_hash)
        except Exception as exc:
            store.finalize_run(
                run_id, "FAILED", error_summary=f"{type(exc).__name__}: {exc}"
            )
            raise
        rebuild_views(settings)

        summary = {
            "run_id": run_id, "origin": origin.isoformat(),
            "data_cutoff": ctx.through.isoformat(), "last_tga": ctx.last_tga,
            "scenarios": scenarios, "components": n_comp,
            "forecast_rows": n_fcst, "financing_pressure": pressure,
        }
        print(f"完成：run={run_id} 组件 {n_comp} 行 / 预测 {n_fcst} 行")
        return 0, summary
    finally:
        meta.close()


def _qtga_checks(meta, ctx: ForecastContext, model: dict, forecast: pl.DataFrame) -> None:
    """Q-TGA-013/014（Q-TGA-012 守恒在 aggregate_forecast 内断言）。"""
    from tga_store import record_anomalies

    def _flag(rule_id: str, key: str, detail: dict) -> dict:
        return {
            "record_key": key, "dataset_id": FORECAST_DATASET_ID,
            "rule_id": rule_id, "severity": "WARNING",
            "detail": json.dumps(detail), "raw_ref": "",
            "payload_digest": "", "run_id": MODEL_VERSION,
        }

    findings: list[dict] = []
    anomalies: list[dict] = []
    stale_days = (date.today() - ctx.through).days
    if stale_days > int(model["limits"]["cutoff_stale_days"]):
        findings.append(_flag(
            "Q-TGA-013", f"tga:forecast:cutoff-stale:{ctx.through}",
            {"through": ctx.through.isoformat(), "stale_days": stale_days},
        ))
        anomalies.append({
            "data_source": FORECAST_DATASET_ID,
            "record_date": ctx.through.isoformat(),
            "anomaly_type": "FORECAST_CUTOFF_STALE",
            "original_value": stale_days, "expected_value": 0,
            "severity": "WARNING", "key": f"cutoff-stale:{ctx.through}",
            "note": "数据截止日落后于今天（DTS 延迟或未采集）",
        })
    negative = forecast.filter(pl.col("predicted_tga") < 0)
    for row in negative.iter_rows(named=True):
        d = row["target_date"]
        findings.append(_flag(
            "Q-TGA-014", f"tga:forecast:negative:{d}",
            {"date": d.isoformat(), "predicted_tga": row["predicted_tga"]},
        ))
        anomalies.append({
            "data_source": FORECAST_DATASET_ID, "record_date": d.isoformat(),
            "anomaly_type": "FORECAST_TGA_NEGATIVE",
            "original_value": float(row["predicted_tga"]), "expected_value": 0.0,
            "severity": "WARNING", "key": f"negative:{d}",
            "note": "预测 TGA < 0（非常规措施/债务上限情形未建模）",
        })
    if findings:
        meta.add_quality_flags(findings)
    if anomalies:
        record_anomalies(meta, anomalies)


def main() -> int:
    parser = argparse.ArgumentParser(description="TGA 基准预测（Model 1/2）")
    parser.add_argument("--origin", default=None, help="预测起点（YYYY-MM-DD，缺省今天）")
    parser.add_argument("--data-cutoff", default=None,
                        help="数据截止日（YYYY-MM-DD，缺省按发布规则推算）")
    parser.add_argument("--horizons", default=None, help="逗号分隔，如 7,14,30")
    parser.add_argument("--scenarios", default=None,
                        help="逗号分隔（缺省 scenarios.yaml 全部情景）")
    parser.add_argument("--force", action="store_true",
                        help="同配置已有 SUCCESS 时强制新 run")
    parser.add_argument("--dry-run", action="store_true", help="只打印计划，不落盘")
    args = parser.parse_args()

    origin = date.fromisoformat(args.origin) if args.origin else None
    cutoff = date.fromisoformat(args.data_cutoff) if args.data_cutoff else None
    horizons = [int(h) for h in args.horizons.split(",")] if args.horizons else None
    scenarios = [s.strip() for s in args.scenarios.split(",")] if args.scenarios else None
    settings = Settings.load()
    try:
        code, _ = run_forecast(
            settings, origin=origin, horizons=horizons, scenarios=scenarios,
            data_cutoff=cutoff, force=args.force, dry_run=args.dry_run,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"预测失败：{type(exc).__name__}: {exc}", file=sys.stderr)
        raise
    return code


if __name__ == "__main__":
    sys.exit(main())
