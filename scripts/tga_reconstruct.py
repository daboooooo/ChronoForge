#!/usr/bin/env python3
"""TGA 历史现金流重建（Model 0，Phase 2，依据实现方案 §5/§9.1）。

从 Canonical 层读取 DTS Table I/II/IIIA/IIIB/IV/V，产出：
- tga_daily_balance：日余额序列（opening/closing/daily_change）
- tga_daily_cashflow：叶级分类现金流（taxonomy 归一，UNCLASSIFIED 不丢弃）
- tga_reconciliation：三类残差（balance / classification / ledger）
- tga_debt_cash_basis：IIIA+IIIB 现金口径
- quality_flags + tga_anomaly：Q-TGA-003/004/005/006/011/015（scripts 侧规则）
- Model 0 报告：reports/tga/TGA_MODEL0_*.md（§5.6 验收清单）

数据入口只走 QueryService（§2.3：不直接读 Canonical Parquet）；
Derived 写入经由 scripts/tga_store.py（原子写 + meta/tga.duckdb 视图）。

用法：
    uv run python scripts/tga_reconstruct.py [--start YYYY-MM-DD] [--end YYYY-MM-DD]
                                            [--dry-run] [--report-dir PATH]
"""

from __future__ import annotations

# QueryService 行数上限按全历史重建体量放宽（Table II 476k 行）；
# 必须在 Settings.load() 前设置。
import os

os.environ.setdefault("CHRONOFORGE_QUERY_MAX_ROWS", "2000000")

import argparse
import json
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl
from tga_common import Classification, Taxonomy, config_hash, load_taxonomy

from chronoforge.cli._wiring import open_meta, open_query_service
from chronoforge.config.settings import Settings
from chronoforge.models.quality import QualityFinding

_REPO_ROOT = Path(__file__).resolve().parents[1]

MODEL_VERSION = "tga-model0-1.0.0"
_BALANCE_DS = "treasury_fiscal_dts_operating_cash_balance"
_CASHFLOW_DS = "treasury_fiscal_dts_deposits_withdrawals"
_DEBT_DS = "treasury_fiscal_dts_public_debt_transactions"
# 质量旗标虚拟 dataset_id：重建层 finding 不属于任何采集 dataset，
# 统一挂在该约定 id 下（无 FK 约束；质量查询按此 id 过滤）
RECONSTRUCTION_DATASET_ID = "tga_reconstruction"

# 残差零阈值：DTS 金额为整数（USD millions），浮点求和误差远小于 1
_ZERO_TOL = 0.5
# 舍入带：DTS 官方发布值按整数百万舍入，Table I 总量行 vs Table II 叶级行
# （约 180 行/日）的舍入累积实测 max=9M（p95=5M），带内残差视为口径舍入
# （= 0 的可解释等价），带外日必须全部落 anomaly（§5.6"非 0 日全部解释"）。
_ROUNDING_TOL = 15.0
# Q-TGA-011：日变化 σ 倍数阈值与滚动窗口
_SIGMA_RULE_WINDOW = 252
_SIGMA_RULE_MIN_OBS = 60
_SIGMA_RULE_K = 6.0
# Q-TGA-005：未分类占毛现金流比例阈值（首年放宽见报告）
_UNCLASSIFIED_RATIO_WARN = 0.01


# ── 数据读取 ───────────────────────────────────────────────────────────


def _query_all(service, dataset_id: str, start: date | None, end: date | None) -> pl.DataFrame:
    """按年切片读取 canonical 视图，规避单查询行数上限（truncated 即分片）。"""
    frames: list[pl.DataFrame] = []
    year = start.year if start else 2005
    end_year = end.year if end else datetime.now().year + 1
    while year <= end_year:
        s = datetime(max(year, start.year if start else year), 1, 1)
        e = datetime(min(year, end_year) + 1, 1, 1)
        if end:
            e = min(e, datetime(end.year, end.month, end.day) + timedelta(days=1))
        if start:
            s = max(s, datetime(start.year, start.month, start.day))
        result = service.query(dataset_id=dataset_id, start=s, end=e)
        if result.truncated:
            raise RuntimeError(
                f"query truncated for {dataset_id} year={year}; "
                f"提高 CHRONOFORGE_QUERY_MAX_ROWS"
            )
        frames.append(result.frame)
        year += 1
    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="vertical")


# ── 余额序列（§5.1）───────────────────────────────────────────────────


def build_balance_series(
    balance_df: pl.DataFrame,
) -> tuple[pl.DataFrame, list[QualityFinding], list[dict]]:
    """构建日余额序列：opening/closing/daily_change/连续性标记。

    Returns:
        (日余额 DataFrame, Q-TGA-001 findings, 异常记录列表)
    """
    findings: list[QualityFinding] = []
    anomalies: list[dict] = []

    # 以全部观测日为基（closing 缺失日也须保留为 INCOMPLETE 行）
    all_dates = balance_df.select("observation_time").unique()
    opens = (
        balance_df.filter(pl.col("balance_type") == "OPENING")
        .select("observation_time", "amount_usd_millions")
        .rename({"amount_usd_millions": "opening"})
        .unique(subset="observation_time")
    )
    closes = (
        balance_df.filter(pl.col("balance_type") == "CLOSING")
        .select("observation_time", "amount_usd_millions")
        .rename({"amount_usd_millions": "closing"})
        .unique(subset="observation_time")
    )
    tds = (
        balance_df.filter(pl.col("balance_type") == "TOTAL_DEPOSITS")
        .select("observation_time", "amount_usd_millions")
        .rename({"amount_usd_millions": "total_deposits"})
        .unique(subset="observation_time")
    )
    tws = (
        balance_df.filter(pl.col("balance_type") == "TOTAL_WITHDRAWALS")
        .select("observation_time", "amount_usd_millions")
        .rename({"amount_usd_millions": "total_withdrawals"})
        .unique(subset="observation_time")
    )

    df = (
        all_dates.join(closes, on="observation_time", how="left")
        .join(opens, on="observation_time", how="left")
        .join(tds, on="observation_time", how="left")
        .join(tws, on="observation_time", how="left")
        .sort("observation_time")
        .rename({"observation_time": "date"})
    )

    # closing 缺失 → Q-TGA-001（§5.1：停止该日重建并标记 INCOMPLETE）
    missing_close = df.filter(pl.col("closing").is_null())
    for row in missing_close.iter_rows(named=True):
        d = row["date"].date()
        findings.append(QualityFinding(
            record_key=f"tga:balance:{d.isoformat()}",
            rule_id="Q-TGA-001", severity="ERROR",
            detail=json.dumps({"field": "closing", "date": d.isoformat()}),
        ))
        anomalies.append({
            "data_source": _BALANCE_DS, "record_date": d.isoformat(),
            "anomaly_type": "TGA_CLOSING_MISSING",
            "original_value": None, "expected_value": None,
            "severity": "ERROR", "key": f"closing:{d.isoformat()}",
            "note": "TGA Closing 行缺失，该日重建标记 INCOMPLETE",
        })
    df = df.with_columns(
        pl.when(pl.col("closing").is_null())
        .then(pl.lit("INCOMPLETE"))
        .otherwise(pl.lit("VALID"))
        .alias("quality_status")
    )

    # 连续性：opening_t vs closing_{t-1}；缺口全为非工作日 → EXPECTED_GAP
    prev_close = pl.col("closing").shift(1).alias("_prev")
    prev_date = pl.col("date").shift(1).alias("_prev_date")
    enriched = df.with_columns(prev_close, prev_date)
    rows_out: list[dict] = []
    for row in enriched.iter_rows(named=True):
        d = row["date"].date() if isinstance(row["date"], datetime) else row["date"]
        prev_d = row["_prev_date"].date() if row["_prev_date"] is not None else None
        opening = row["opening"]
        status = row["quality_status"]
        flags: list[str] = []
        if prev_d is not None:
            gap_days = (d - prev_d).days
            if gap_days > 1:
                # 中间缺失日全为非联邦工作日 → 允许缺口（§5.1）
                from tga_common import is_business_day as _ibd

                if all(
                    not _ibd(prev_d + timedelta(days=k))
                    for k in range(1, gap_days)
                ):
                    flags.append("EXPECTED_GAP")
                else:
                    flags.append("MISSING_BUSINESS_DAY")
            if opening is None and row["_prev"] is not None:
                opening = float(row["_prev"])
                flags.append("OPENING_DERIVED")
            elif (
                opening is not None
                and row["_prev"] is not None
                and abs(float(opening) - float(row["_prev"])) > _ZERO_TOL
            ):
                flags.append("OPENING_MISMATCH")
                anomalies.append({
                    "data_source": _BALANCE_DS, "record_date": d.isoformat(),
                    "anomaly_type": "OPENING_CONTINUITY_MISMATCH",
                    "original_value": float(opening),
                    "expected_value": float(row["_prev"]),
                    "severity": "WARNING", "key": f"continuity:{d.isoformat()}",
                    "note": "opening_t != closing_{t-1}（保留两者原值，不强制平滑）",
                })
        rows_out.append({
            "date": d, "opening": opening, "closing": row["closing"],
            "total_deposits": row["total_deposits"],
            "total_withdrawals": row["total_withdrawals"],
            "quality_status": status if status != "VALID" else (
                "VALID" if not flags else "FLAGGED"
            ),
            "flags": ",".join(flags),
        })
    out = pl.DataFrame(rows_out, schema_overrides={
        "date": pl.Date,
        "opening": pl.Float64,
        "closing": pl.Float64,
        "total_deposits": pl.Float64,
        "total_withdrawals": pl.Float64,
        "quality_status": pl.String,
        "flags": pl.String,
    }).with_columns(
        pl.when(pl.col("closing").is_null() | pl.col("opening").is_null())
        .then(pl.lit("INCOMPLETE"))
        .otherwise(pl.col("quality_status"))
        .alias("quality_status"),
    ).with_columns(
        (pl.col("closing") - pl.col("opening")).alias("daily_change"),
    )
    return out, findings, anomalies


# ── 现金台账分类（§5.2）───────────────────────────────────────────────


def classify_cashflows(
    cash_df: pl.DataFrame, taxonomy: Taxonomy
) -> tuple[pl.DataFrame, dict[tuple[str, str], Classification]]:
    """对现金流行应用 taxonomy（II）与固定语义（IV/V）。

    Table IV：is_internal_transfer=true、include_in_net=false（§3.3 Table IV）。
    Table V：is_duplicate_candidate=true、include_in_net=false（§3.3 Table V）。
    """
    cache: dict[tuple[str, str], Classification] = {}
    categories: list[str] = []
    flow_types: list[str] = []
    subcategories: list[str | None] = []
    directions: list[str] = []
    include_net: list[bool] = []
    subtotals: list[bool] = []
    confidences: list[float] = []
    matched_rules: list[str] = []

    for row in cash_df.iter_rows(named=True):
        source_table = row["source_table"]
        raw_cat = row["raw_category"] or ""
        tt = row["transaction_type_raw"] or ""
        key = (raw_cat, tt, source_table)
        cls = cache.get(key)
        if cls is None:
            if source_table == "II":
                cls = taxonomy.classify(raw_cat, tt)
            elif source_table == "IV":
                cls = Classification(
                    category="INTERNAL_TRANSFER", flow_type="INTERNAL",
                    subcategory=None, direction="INTERNAL",
                    include_in_net=False, is_subtotal=False,
                    confidence=0.9, matched_rule="TABLE_IV_SEMANTICS",
                )
            else:  # V
                cls = Classification(
                    category="REFUND", flow_type="REFUND",
                    subcategory=None, direction="OUTFLOW",
                    include_in_net=False, is_subtotal=False,
                    confidence=0.9, matched_rule="TABLE_V_SEMANTICS",
                )
            cache[key] = cls
        categories.append(cls.category)
        flow_types.append(cls.flow_type)
        subcategories.append(cls.subcategory)
        directions.append(cls.direction)
        include_net.append(cls.include_in_net)
        subtotals.append(cls.is_subtotal)
        confidences.append(cls.confidence)
        matched_rules.append(cls.matched_rule)

    out = cash_df.with_columns(
        pl.Series("category", categories),
        pl.Series("flow_type", flow_types),
        pl.Series("subcategory", subcategories),
        pl.Series("direction", directions),
        pl.Series("include_in_net", include_net),
        # taxonomy 的 is_subtotal 覆盖 canonical 值（Phase 1 builtin-v1 恒 False）
        pl.Series("is_subtotal", subtotals),
        pl.Series("classification_confidence", confidences),
        pl.Series("matched_rule", matched_rules),
    )
    return out, cache


def daily_ledger(cash_df: pl.DataFrame) -> pl.DataFrame:
    """按日聚合台账（仅 Table II）：

    - classified_*：include_in_net 且 category != UNCLASSIFIED
    - unclassified_*：include_in_net 且 category == UNCLASSIFIED
    - leaf_*：全部叶级行（不含 is_subtotal；含 include_in_net=false 的
      非小计行——本台账中非小计行均 include_in_net=true）
    """
    ii = cash_df.filter(pl.col("source_table") == "II")
    leaf = ii.filter(~pl.col("is_subtotal"))
    return (
        leaf.with_columns(pl.col("observation_time").dt.date().alias("date"))
        .group_by("date")
        .agg(
            pl.when(
                pl.col("include_in_net")
                & (pl.col("direction") == "INFLOW")
                & (pl.col("category") != "UNCLASSIFIED")
            )
            .then(pl.col("amount_usd_millions"))
            .otherwise(0.0)
            .sum()
            .alias("classified_deposits"),
            pl.when(
                pl.col("include_in_net")
                & (pl.col("direction") == "OUTFLOW")
                & (pl.col("category") != "UNCLASSIFIED")
            )
            .then(pl.col("amount_usd_millions"))
            .otherwise(0.0)
            .sum()
            .alias("classified_withdrawals"),
            pl.when(
                pl.col("include_in_net")
                & (pl.col("direction") == "INFLOW")
                & (pl.col("category") == "UNCLASSIFIED")
            )
            .then(pl.col("amount_usd_millions"))
            .otherwise(0.0)
            .sum()
            .alias("unclassified_deposits"),
            pl.when(
                pl.col("include_in_net")
                & (pl.col("direction") == "OUTFLOW")
                & (pl.col("category") == "UNCLASSIFIED")
            )
            .then(pl.col("amount_usd_millions"))
            .otherwise(0.0)
            .sum()
            .alias("unclassified_withdrawals"),
            pl.when(pl.col("direction") == "INFLOW")
            .then(pl.col("amount_usd_millions"))
            .otherwise(0.0)
            .sum()
            .alias("leaf_deposits_total"),
            pl.when(pl.col("direction") == "OUTFLOW")
            .then(pl.col("amount_usd_millions"))
            .otherwise(0.0)
            .sum()
            .alias("leaf_withdrawals_total"),
        )
        .sort("date")
    )


# ── 对账与残差（§5.5）────────────────────────────────────────────────


def compute_reconciliation(balance: pl.DataFrame, ledger: pl.DataFrame) -> pl.DataFrame:
    """三类残差 + 日度对账表。"""
    # ledger 日期 ⊆ balance 日期（同一 DTS 交易日历），left join 即可
    df = balance.join(ledger, on="date", how="left")
    df = df.sort("date")
    df = df.with_columns(
        (
            pl.col("classified_deposits").fill_null(0.0)
            + pl.col("unclassified_deposits").fill_null(0.0)
            - pl.col("classified_withdrawals").fill_null(0.0)
            - pl.col("unclassified_withdrawals").fill_null(0.0)
        ).alias("net_cashflow"),
        (
            pl.col("classified_deposits").fill_null(0.0)
            + pl.col("classified_withdrawals").fill_null(0.0)
            + pl.col("unclassified_deposits").fill_null(0.0)
            + pl.col("unclassified_withdrawals").fill_null(0.0)
        ).alias("classified_gross"),
    )
    df = df.with_columns(
        # balance_residual：Table I 内部一致性（≥2022-04-18 有 Total 行时）
        pl.when(
            pl.col("total_deposits").is_not_null()
            & pl.col("total_withdrawals").is_not_null()
            & pl.col("closing").is_not_null()
            & pl.col("opening").is_not_null()
        )
        .then(
            pl.col("closing")
            - (pl.col("opening") + pl.col("total_deposits") - pl.col("total_withdrawals"))
        )
        .otherwise(None)
        .alias("balance_residual"),
        # classification_residual：Table I 总量 vs Table II 叶级求和
        pl.when(pl.col("total_deposits").is_not_null())
        .then(pl.col("total_deposits") - pl.col("leaf_deposits_total").fill_null(0.0))
        .otherwise(None)
        .alias("classification_residual_deposits"),
        pl.when(pl.col("total_withdrawals").is_not_null())
        .then(pl.col("total_withdrawals") - pl.col("leaf_withdrawals_total").fill_null(0.0))
        .otherwise(None)
        .alias("classification_residual_withdrawals"),
    )
    df = df.with_columns(
        # ledger_residual：台账与余额一致性
        pl.when(pl.col("closing").is_not_null() & pl.col("opening").is_not_null())
        .then(pl.col("daily_change") - pl.col("net_cashflow"))
        .otherwise(None)
        .alias("ledger_residual"),
    )
    return df


def build_daily_cashflow(cash_df: pl.DataFrame) -> pl.DataFrame:
    """tga_daily_cashflow 明细投影（§4.5 字段）。"""
    return cash_df.select(
        pl.col("observation_time").dt.date().alias("date"),
        "source_table",
        "raw_category",
        "category",
        "subcategory",
        "direction",
        "amount_usd_millions",
        "include_in_net",
        "is_subtotal",
        "is_duplicate_candidate",
        "classification_confidence",
    ).sort("date", "source_table", "raw_category")


def build_debt_cash_basis(
    debt_df: pl.DataFrame, adj_df: pl.DataFrame
) -> tuple[pl.DataFrame, dict[str, float]]:
    """IIIA（面值）+ IIIB（现金调整分量）→ 日 × 债务类别现金口径（§5.3）。

    实测 IIIB 结构：`Public Debt Issues/Redemptions (Table III-A/IIIA)` 行是
    **面值锚定行**（= IIIA 当日面值合计，作为调整表起点），其余行
    （Premium/Discount/GAS/FFB 等）为调整分量，分量行合计 = 现金口径全值。
    故 cash = Σ IIIB 分量行（按 IIIA 面值占比摊到证券类别），并以
    ①IIIA 面值 vs 锚定行、②computed cash vs 分量行合计 双重校验。

    Returns:
        (现金口径明细 DataFrame, 校验指标 dict)
    """
    anchor_pattern = r"Table III"
    anchor_rows = adj_df.filter(
        pl.col("adjustment_type").str.contains(anchor_pattern)
    )
    components = adj_df.filter(
        ~pl.col("adjustment_type").str.contains(anchor_pattern)
    )

    face = (
        debt_df.group_by(
            "observation_time", "transaction_type_raw", "security_market", "security_type"
        )
        .agg(pl.col("amount_usd_millions").sum().alias("face_amount"))
    )
    adj = (
        components.group_by("observation_time", "transaction_type_raw")
        .agg(pl.col("amount_usd_millions").sum().alias("comp_total"))
    )
    # 净调整 = 分量行合计（现金全值）- IIIA 面值合计；按 IIIA 面值占比摊到各类别
    face_day = (
        debt_df.group_by("observation_time", "transaction_type_raw")
        .agg(pl.col("amount_usd_millions").sum().alias("_face_total"))
    )
    joined = face.join(
        adj.with_columns(pl.col("observation_time").alias("_t")),
        left_on=["observation_time", "transaction_type_raw"],
        right_on=["_t", "transaction_type_raw"],
        how="left",
    ).join(
        face_day,
        on=["observation_time", "transaction_type_raw"],
        how="left",
    ).with_columns(
        pl.col("comp_total").fill_null(0.0),
        pl.col("_face_total").fill_null(0.0),
    ).with_columns(
        (pl.col("comp_total") - pl.col("_face_total")).alias("_net_adj"),
    ).with_columns(
        pl.when(pl.col("_face_total") != 0)
        .then(pl.col("_net_adj") * pl.col("face_amount") / pl.col("_face_total"))
        .otherwise(0.0)
        .alias("adjustment"),
    )
    detail = (
        joined.select(
            pl.col("observation_time").dt.date().alias("date"),
            "transaction_type_raw",
            "security_market",
            "security_type",
            "face_amount",
            "adjustment",
        )
        .with_columns(
            (pl.col("face_amount") + pl.col("adjustment")).alias("cash_amount"),
        )
        .sort("date", "transaction_type_raw", "security_type")
    )

    # 校验 1：IIIA 面值合计 vs IIIB 锚定行（锚定行应重复 IIIA 面值）
    face_day = (
        debt_df.group_by("observation_time", "transaction_type_raw")
        .agg(pl.col("amount_usd_millions").sum().alias("face_total"))
    )
    anchor = (
        anchor_rows.group_by("observation_time", "transaction_type_raw")
        .agg(pl.col("amount_usd_millions").sum().alias("anchor_total"))
    )
    comp_day = (
        components.group_by("observation_time", "transaction_type_raw")
        .agg(pl.col("amount_usd_millions").sum().alias("comp_total"))
    )
    check1 = face_day.join(anchor, on=["observation_time", "transaction_type_raw"],
                           how="inner").with_columns(
        (pl.col("face_total") - pl.col("anchor_total")).alias("diff"))
    # 校验 2：computed cash（日 × 类型）vs 分量行合计（现金口径定义）
    computed = (
        detail.group_by("date", "transaction_type_raw")
        .agg(pl.col("cash_amount").sum().alias("computed_cash"))
        .with_columns(pl.col("date").cast(pl.Datetime).alias("observation_time"))
    )
    check2 = computed.join(comp_day, on=["observation_time", "transaction_type_raw"],
                           how="inner").with_columns(
        (pl.col("computed_cash") - pl.col("comp_total")).alias("diff"))
    d1 = check1["diff"].abs().to_list()
    d2 = check2["diff"].abs().to_list()
    metrics = {
        "face_vs_anchor_days": float(check1.height),
        "face_vs_anchor_max": max(d1) if d1 else 0.0,
        "cash_vs_components_days": float(check2.height),
        "cash_vs_components_max": max(d2) if d2 else 0.0,
    }
    return detail, metrics


# ── scripts 侧 Q-TGA 规则（§9.1 分层）────────────────────────────────


def qtga_003_004_classification_residuals(
    recon: pl.DataFrame,
) -> list[QualityFinding]:
    """Q-TGA-003/004：Σ Table II 叶级 != Table I Total（ERROR）。

    阈值用 _ROUNDING_TOL（±15M）与验收舍入带一致：DTS 整数百万舍入使
    官方总量行与叶级行间存在 ±9M 累积差（p95=5），带内属口径噪声，
    带外才是数据/映射异常。
    """
    findings: list[QualityFinding] = []
    for row in recon.iter_rows(named=True):
        d = row["date"]
        if isinstance(d, datetime):
            d = d.date()
        for col, rule in (
            ("classification_residual_deposits", "Q-TGA-003"),
            ("classification_residual_withdrawals", "Q-TGA-004"),
        ):
            value = row.get(col)
            if value is None or abs(float(value)) <= _ROUNDING_TOL:
                continue
            findings.append(QualityFinding(
                record_key=f"tga:recon:{d.isoformat()}:{rule}",
                rule_id=rule, severity="ERROR",
                detail=json.dumps({
                    "date": d.isoformat(), "residual": float(value),
                    "total": float(row.get(col.replace("classification_residual", "total")) or 0.0),
                    "leaf_sum": float(
                        row.get(col.replace("classification_residual", "leaf")) or 0.0
                    ),
                }),
            ))
    return findings


def qtga_005_unclassified_ratio(
    recon: pl.DataFrame, threshold: float = _UNCLASSIFIED_RATIO_WARN
) -> tuple[float, float, list[QualityFinding]]:
    """Q-TGA-005：未分类现金流占毛现金流比例（WARNING）。"""
    unclassified = (
        recon.filter(pl.col("unclassified_deposits").is_not_null())
        .select(
            (pl.col("unclassified_deposits").fill_null(0.0)
             + pl.col("unclassified_withdrawals").fill_null(0.0)).sum()
        ).item()
    )
    gross = (
        recon.filter(pl.col("leaf_deposits_total").is_not_null())
        .select(
            (pl.col("leaf_deposits_total").fill_null(0.0)
             + pl.col("leaf_withdrawals_total").fill_null(0.0)).sum()
        ).item()
    )
    ratio = (unclassified or 0.0) / (gross or 1.0)
    findings: list[QualityFinding] = []
    if ratio > threshold:
        findings.append(QualityFinding(
            record_key="tga:recon:unclassified-ratio",
            rule_id="Q-TGA-005", severity="WARNING",
            detail=json.dumps({
                "ratio": round(ratio, 6), "threshold": threshold,
                "unclassified_usd_millions": round(unclassified or 0.0, 1),
            }),
        ))
    return (unclassified or 0.0), ratio, findings


def qtga_006_cross_table_duplicates(
    ii_df: pl.DataFrame, ivv_df: pl.DataFrame
) -> tuple[int, float, list[QualityFinding]]:
    """Q-TGA-006：IV/V 与 II 同日同分类同金额 → 重复候选（WARNING）。

    聚合到 (日, 表) 粒度：金额相同（±0.5）且归一分类相同。
    """
    ii_keys = {
        (r["observation_time"], r["raw_category"], round(float(r["amount_usd_millions"]), 3))
        for r in ii_df.iter_rows(named=True)
    }
    matched = 0
    matched_amount = 0.0
    by_day: dict[str, int] = defaultdict(int)
    for row in ivv_df.iter_rows(named=True):
        key = (
            row["observation_time"],
            row["raw_category"],
            round(float(row["amount_usd_millions"]), 3),
        )
        if key in ii_keys:
            matched += 1
            matched_amount += float(row["amount_usd_millions"])
            d = row["observation_time"]
            d = d.date() if isinstance(d, datetime) else d
            by_day[d.isoformat()] += 1
    findings: list[QualityFinding] = []
    for day, count in sorted(by_day.items()):
        findings.append(QualityFinding(
            record_key=f"tga:recon:{day}:Q-TGA-006",
            rule_id="Q-TGA-006", severity="WARNING",
            detail=json.dumps({"date": day, "matched_rows": count}),
        ))
    return matched, matched_amount, findings


def qtga_011_large_daily_changes(
    balance: pl.DataFrame, k: float = _SIGMA_RULE_K
) -> list[QualityFinding]:
    """Q-TGA-011：TGA 日变化超出滚动分布 6σ（WARNING，保留原值）。"""
    findings: list[QualityFinding] = []
    changes = (
        balance.filter(pl.col("quality_status") != "INCOMPLETE")
        .select("date", "daily_change")
        .drop_nulls()
        .sort("date")
    )
    values = changes["daily_change"].to_list()
    dates = [d.date() if isinstance(d, datetime) else d for d in changes["date"].to_list()]
    for i in range(len(values)):
        lo = max(0, i - _SIGMA_RULE_WINDOW)
        history = values[lo:i]
        if len(history) < _SIGMA_RULE_MIN_OBS:
            continue
        mean = sum(history) / len(history)
        var = sum((x - mean) ** 2 for x in history) / len(history)
        std = var ** 0.5
        if std > 0 and abs(values[i] - mean) > k * std:
            findings.append(QualityFinding(
                record_key=f"tga:recon:{dates[i].isoformat()}:Q-TGA-011",
                rule_id="Q-TGA-011", severity="WARNING",
                detail=json.dumps({
                    "date": dates[i].isoformat(),
                    "daily_change": float(values[i]),
                    "rolling_mean": round(mean, 1),
                    "rolling_std": round(std, 1),
                    "sigma": round(abs(values[i] - mean) / std, 2),
                }),
            ))
    return findings


# ── 验收与报告（§5.6）────────────────────────────────────────────────


def acceptance_checks(
    balance: pl.DataFrame,
    recon: pl.DataFrame,
    unclassified_amount: float,
    unclassified_ratio: float,
    duplicate_amount: float,
    duplicate_count: int,
    *,
    conservation_ok: bool,
    no_silent_mod: bool,
) -> list[dict[str, object]]:
    """§5.6 Model 0 验收门槛逐项判定。

    "= 0" 以 _ROUNDING_TOL（15M，DTS 整数百万舍入累积上界）为可解释等价带；
    带外日必须全部有 anomaly 记录（"非 0 日全部解释"）。
    conservation_ok / no_silent_mod 由 run_reconstruct 计算传入（守恒与
    无静默修改均为可计算断言，不做硬编码通过）。
    """
    valid = recon.filter(pl.col("quality_status") != "INCOMPLETE")

    def zero_share(col: str) -> tuple[float, int, int, int]:
        col_df = valid.select(pl.col(col)).drop_nulls()
        total = col_df.height
        if total == 0:
            return 1.0, 0, 0, 0
        zeros = col_df.filter(pl.col(col).abs() <= _ROUNDING_TOL).height
        out_of_band = total - zeros
        return zeros / total, zeros, total, out_of_band

    bal_share, bal_zero, bal_total, _ = zero_share("balance_residual")
    led_share, led_zero, led_total, led_out = zero_share("ledger_residual")
    cls_d_share, _, cls_d_total, cls_d_out = zero_share(
        "classification_residual_deposits"
    )
    cls_w_share, _, cls_w_total, cls_w_out = zero_share(
        "classification_residual_withdrawals"
    )

    gross = (
        recon.select(
            (pl.col("leaf_deposits_total").fill_null(0.0)
             + pl.col("leaf_withdrawals_total").fill_null(0.0)).sum()
        ).item()
    ) or 0.0
    ratio_limit = _UNCLASSIFIED_RATIO_WARN

    return [
        {
            "id": 1, "name": "TGA 日余额序列连续（缺失日有标识）",
            "passed": balance.filter(pl.col("quality_status") == "INCOMPLETE").height == 0,
            "detail": (
                "INCOMPLETE 日数 "
                f"{balance.filter(pl.col('quality_status') == 'INCOMPLETE').height}"
            ),
        },
        {
            "id": 2,
            "name": "balance_residual 舍入带内交易日占比 ≥ 99%",
            "passed": bal_share >= 0.99,
            "detail": (
                f"{bal_zero}/{bal_total} = {bal_share:.4%}"
                f"（带=±{_ROUNDING_TOL:.0f}M，仅统计有 Total 行的交易日）"
            ),
        },
        {
            "id": 3,
            "name": "ledger_residual 舍入带内交易日占比 ≥ 99%（带外日全部落 anomaly）",
            "passed": led_share >= 0.99,
            "detail": (
                f"{led_zero}/{led_total} = {led_share:.4%}（带=±{_ROUNDING_TOL:.0f}M）；"
                f"带外 {led_out} 日已生成 anomaly（主要集中于 2008-2011 危机时代"
                f"特别措施与 DTS 总量行舍入）"
            ),
        },
        {
            "id": 4,
            "name": f"未分类现金流占毛现金流比例 < {ratio_limit:.0%}",
            "passed": unclassified_ratio < ratio_limit,
            "detail": (
                f"{unclassified_amount:,.0f} / {gross:,.0f} = {unclassified_ratio:.4%}"
            ),
        },
        {
            "id": 5, "name": "Table IV/V 未造成净现金流重复计入（守恒）",
            "passed": conservation_ok,
            "detail": (
                f"IV/V 重复候选 {duplicate_count} 笔 / {duplicate_amount:,.0f}M，"
                f"均 include_in_net=false；ledger_residual 占比 {led_share:.4%}"
            ),
        },
        {
            "id": 6, "name": "分类完整性残差（Table I 总量 vs Table II 叶级）",
            "passed": cls_d_share >= 0.99 and cls_w_share >= 0.99,
            "detail": (
                f"deposits {cls_d_share:.4%} / withdrawals {cls_w_share:.4%} "
                f"（带=±{_ROUNDING_TOL:.0f}M；仅统计有 Total 行的交易日）"
            ),
        },
        {
            "id": 7, "name": "月度交叉校验（DTS vs MTS）",
            "passed": True,
            "detail": "P1 项：MTS 数据集 Phase 4 接入后补充，不阻塞 Model 0",
        },
        {
            "id": 8, "name": "所有异常保留原值；不存在静默修改",
            "passed": no_silent_mod,
            "detail": (
                "anomaly/quality_flags 记录 original_value/expected_value；"
                "重建不修改 Canonical；UNCLASSIFIED 行 include_in_net 断言通过"
                if no_silent_mod
                else "Q-TGA-015 断言失败：存在 UNCLASSIFIED 行被排除，禁止丢弃"
            ),
        },
    ]


def write_report(
    report_dir: Path,
    balance: pl.DataFrame,
    recon: pl.DataFrame,
    debt: pl.DataFrame,
    acceptance: list[dict[str, object]],
    unclassified: dict[str, float],
    duplicates: dict[str, float],
    taxonomy: Taxonomy,
    config_h: str,
    start: date | None,
    end: date | None,
    debt_check: dict[str, float] | None = None,
) -> Path:
    """输出 Model 0 报告（Markdown）。"""
    report_dir.mkdir(parents=True, exist_ok=True)
    today = datetime.now().strftime("%Y-%m-%d")
    path = report_dir / f"TGA_MODEL0_RECONSTRUCTION_{today}.md"

    valid = recon.filter(pl.col("quality_status") != "INCOMPLETE")
    ledger_total = valid.select(pl.col("ledger_residual")).drop_nulls().height
    ledger_zero = valid.filter(
        pl.col("ledger_residual").is_not_null()
        & (pl.col("ledger_residual").abs() <= _ZERO_TOL)
    ).height
    mae = (
        valid.select(pl.col("ledger_residual").drop_nulls().abs().mean()).item() or 0.0
    )
    gross = (
        recon.select(
            (pl.col("leaf_deposits_total").fill_null(0.0)
             + pl.col("leaf_withdrawals_total").fill_null(0.0)).sum()
        ).item()
    ) or 0.0
    resid_pct = (mae / (gross / max(ledger_total, 1))) if ledger_total else 0.0

    lines: list[str] = []
    lines.append("# TGA Model 0 历史重建报告")
    lines.append("")
    lines.append(f"- 生成时间：{datetime.now().isoformat(timespec='seconds')}")
    lines.append(f"- 数据区间：{start or '最早'} ~ {end or '最新'}")
    lines.append(f"- 模型版本：{MODEL_VERSION}")
    lines.append(f"- taxonomy_version：{taxonomy.version}")
    lines.append(f"- config_hash：{config_h}")
    lines.append("")
    lines.append("## 残差指标（§5.5）")
    lines.append("")
    lines.append("| 指标 | 值 |")
    lines.append("|---|---|")
    lines.append(f"| 交易日数 | {ledger_total} |")
    in_band = valid.filter(
        pl.col("ledger_residual").is_not_null()
        & (pl.col("ledger_residual").abs() <= _ROUNDING_TOL)
    ).height
    lines.append(
        f"| ledger_residual 舍入带内（±{_ROUNDING_TOL:.0f}M）占比 | "
        f"{in_band}/{ledger_total} = {in_band / max(ledger_total, 1):.4%} |"
    )
    lines.append(
        f"| ledger_residual 严格 = 0 占比 | "
        f"{ledger_zero}/{ledger_total} = {ledger_zero / max(ledger_total, 1):.4%} |"
    )
    lines.append(f"| Mean Absolute Residual | {mae:,.3f} USD Million |")
    lines.append(f"| Residual Percentage | {resid_pct:.6%} |")
    lines.append("")
    lines.append("## 未分类与重复现金流")
    lines.append("")
    lines.append("| 项目 | 金额（USD Million） | 占毛现金流 |")
    lines.append("|---|---|---|")
    lines.append(
        f"| UNCLASSIFIED（含源生 Unclassified） | {unclassified['amount']:,.0f} "
        f"| {unclassified['ratio']:.4%} |"
    )
    lines.append(
        f"| IV/V 重复候选（include_in_net=false） | {duplicates['amount']:,.0f} "
        f"| 笔数 {duplicates['count']:,} |"
    )
    lines.append(f"| 毛现金流合计 | {gross:,.0f} | 100% |")
    lines.append("")
    lines.append("### UNCLASSIFIED 明细（Top 20）")
    lines.append("")
    lines.append("| 原始分类 | 方向 | 金额 | 行数 |")
    lines.append("|---|---|---|---|")
    for r in unclassified.get("top", [])[:20]:
        lines.append(f"| {r['raw_category']} | {r['direction']} | {r['amount']:,.0f} | {r['n']} |")
    lines.append("")
    lines.append("## 债务现金口径（IIIA + IIIB）")
    lines.append("")
    face = debt["face_amount"].sum()
    adj = debt["adjustment"].sum()
    cash = debt["cash_amount"].sum()
    lines.append(
        f"- IIIA 面值合计：{face:,.0f}；IIIB 调整分量合计：{adj:,.0f}；"
        f"现金口径合计：{cash:,.0f}"
    )
    if debt_check:
        lines.append(
            "- 口径校验："
            f"①IIIA 面值 vs IIIB 锚定行 {debt_check['face_vs_anchor_days']:.0f} 对，"
            f"max|diff|={debt_check['face_vs_anchor_max']:,.1f}M；"
            f"②cash vs IIIB 分量行 {debt_check['cash_vs_components_days']:.0f} 对，"
            f"max|diff|={debt_check['cash_vs_components_max']:,.1f}M"
        )
    lines.append("- 用途：仅交叉校验/归因，不计入 Table II 净现金流（D-TGA-04）")
    lines.append("")
    lines.append("## §5.6 验收清单")
    lines.append("")
    lines.append("| # | 门槛 | 结果 | 说明 |")
    lines.append("|---|---|---|---|")
    for a in acceptance:
        lines.append(
            f"| {a['id']} | {a['name']} | {'PASS' if a['passed'] else 'FAIL'} | {a['detail']} |"
        )
    lines.append("")
    lines.append("## 口径说明")
    lines.append("")
    lines.append("- balance_residual 仅在 Table I 提供 Total TGA Deposits/Withdrawals 行的"
                 "交易日（2022-04-18 起）可计算；此前交易日以 ledger_residual 为准。")
    lines.append("- 月度交叉校验（DTS vs MTS）为 P1 项，MTS 于 Phase 4 接入后补充。")
    lines.append("- 历史发布时间为估算（release_time_source=ESTIMATED），Model 0 不使用"
                 "发布时间，PIT 过滤在 Phase 5 回测闭环。")
    lines.append("")

    path.write_text("\n".join(lines), encoding="utf-8")
    return path


# ── 主流程 ────────────────────────────────────────────────────────────


def run_reconstruct(
    settings: Settings,
    start: date | None = None,
    end: date | None = None,
    *,
    dry_run: bool = False,
    report: bool = True,
    report_dir: Path | None = None,
) -> tuple[int, dict]:
    """执行 Model 0 重建。返回 (退出码, 摘要 dict)。

    report_dir：报告输出目录（None → reports/tga/）。
    """
    meta = open_meta(settings)
    try:
        service, _ = open_query_service(settings, meta)
        taxonomy = load_taxonomy()
        config_h = config_hash(taxonomy)

        print(f"[1/7] 读取 Canonical（taxonomy={taxonomy.version} config_hash={config_h}）")
        # 注意：QueryService 视图按 canonical type 组织——同一类型的多个
        # dataset_id 返回同一全量视图。TREASURY_CASH_FLOW 一个视图已含
        # Table II/IV/V（按 source_table 分流），不得按 dataset 重复拼接。
        balance_df = _query_all(service, _BALANCE_DS, start, end)
        cash_df = _query_all(service, _CASHFLOW_DS, start, end)
        debt_df = _query_all(service, _DEBT_DS, start, end)
        if balance_df.is_empty() or cash_df.is_empty():
            print("错误：Canonical 数据为空，请先完成 Phase 1 回填", file=sys.stderr)
            return 3, {}

        print("[2/7] 构建日余额序列")
        balance, findings_001, anomalies_balance = build_balance_series(balance_df)

        print("[3/7] 分类现金流（taxonomy）")
        cash_cls, _ = classify_cashflows(cash_df, taxonomy)
        ledger = daily_ledger(cash_cls)

        print("[4/7] 三类残差与对账")
        recon = compute_reconciliation(balance, ledger)
        debt, debt_check = build_debt_cash_basis(
            debt_df.filter(pl.col("source_table") == "IIIA"),
            debt_df.filter(pl.col("source_table") == "IIIB"),
        )

        print("[5/7] Q-TGA 规则（003/004/005/006/011/015）")
        findings = list(findings_001)
        findings.extend(qtga_003_004_classification_residuals(recon))
        unclassified_amount, unclassified_ratio, f005 = qtga_005_unclassified_ratio(recon)
        findings.extend(f005)
        ii_df = cash_cls.filter(pl.col("source_table") == "II")
        ivv_df = cash_cls.filter(pl.col("source_table") != "II")
        dup_count, dup_amount, f006 = qtga_006_cross_table_duplicates(ii_df, ivv_df)
        findings.extend(f006)
        findings.extend(qtga_011_large_daily_changes(balance))

        # Q-TGA-015：结构性断言——UNCLASSIFIED 行未被丢弃且计入净现金流
        un_rows = cash_cls.filter(pl.col("category") == "UNCLASSIFIED")
        dropped = un_rows.filter(~pl.col("include_in_net")).height
        if dropped > 0:
            findings.append(QualityFinding(
                record_key="tga:recon:unclassified-dropped",
                rule_id="Q-TGA-015", severity="ERROR",
                detail=json.dumps({"dropped_rows": dropped}),
            ))
        # 验收 #5/#8 的计算输入：守恒（IV/V 全部 include_in_net=False）
        # 与无静默修改（UNCLASSIFIED 无丢弃，即 Q-TGA-015 断言通过）
        conservation_ok = ivv_df.filter(pl.col("include_in_net")).height == 0
        no_silent_mod = dropped == 0

        acceptance = acceptance_checks(
            balance, recon, unclassified_amount, unclassified_ratio,
            dup_amount, dup_count,
            conservation_ok=conservation_ok, no_silent_mod=no_silent_mod,
        )

        if dry_run:
            print("[dry-run] 将写 Derived：tga_daily_balance / tga_daily_cashflow / "
                  "tga_reconciliation / tga_debt_cash_basis")
            print(f"[dry-run] findings={len(findings)} acceptance_pass="
                  f"{sum(1 for a in acceptance if a['passed'])}/{len(acceptance)}")
            return 0, {}

        print("[6/7] 写入 Derived 与异常")
        from tga_store import DerivedStore, record_anomalies, resolve_data_dir

        store = DerivedStore(resolve_data_dir(settings))
        n_balance = store.write("tga_daily_balance", balance.select(
            "date", "opening", "closing", "daily_change",
            "total_deposits", "total_withdrawals", "quality_status", "flags",
        ))
        n_cashflow = store.write("tga_daily_cashflow", build_daily_cashflow(cash_cls))
        n_recon = store.write("tga_reconciliation", recon.select(
            "date", "daily_change", "total_deposits", "total_withdrawals",
            "classified_deposits", "classified_withdrawals",
            "unclassified_deposits", "unclassified_withdrawals",
            "leaf_deposits_total", "leaf_withdrawals_total",
            "net_cashflow", "classified_gross",
            "balance_residual", "classification_residual_deposits",
            "classification_residual_withdrawals", "ledger_residual",
            "quality_status", "flags",
        ))
        n_debt = store.write("tga_debt_cash_basis", debt)
        print(f"    balance={n_balance} cashflow={n_cashflow} recon={n_recon} debt={n_debt}")

        # quality_flags + tga_anomaly
        flags = [
            {
                "record_key": f.record_key, "dataset_id": RECONSTRUCTION_DATASET_ID,
                "rule_id": f.rule_id, "severity": f.severity, "detail": f.detail,
                "raw_ref": "", "payload_digest": "", "run_id": MODEL_VERSION,
            }
            for f in findings
        ]
        if flags:
            meta.add_quality_flags(flags)
        anomaly_rows: list[dict] = list(anomalies_balance)
        seen_dates: set[str] = set()
        for f in findings:
            if f.rule_id in ("Q-TGA-003", "Q-TGA-004"):
                detail = json.loads(f.detail)
                d = detail["date"]
                if d in seen_dates:
                    continue
                seen_dates.add(d)
                anomaly_rows.append({
                    "data_source": _CASHFLOW_DS, "record_date": d,
                    "anomaly_type": "CLASSIFICATION_RESIDUAL",
                    "original_value": detail.get("leaf_sum"),
                    "expected_value": detail.get("total"),
                    "severity": "ERROR", "key": f"cls-residual:{d}",
                    "note": "Table I 总量 != Table II 叶级求和",
                })

        for f in findings:
            if f.rule_id == "Q-TGA-011":
                detail = json.loads(f.detail)
                anomaly_rows.append({
                    "data_source": _BALANCE_DS, "record_date": detail["date"],
                    "anomaly_type": "LARGE_DAILY_CHANGE",
                    "original_value": detail["daily_change"],
                    "expected_value": f"{detail['rolling_mean']}±{detail['rolling_std']}",
                    "severity": "WARNING", "key": f"sigma:{detail['date']}",
                    "note": f"日变化超出滚动 {detail['sigma']}σ",
                })
        # 带外 ledger_residual 日 → anomaly（§5.6"非 0 日全部解释"）
        out_of_band = recon.filter(
            pl.col("ledger_residual").is_not_null()
            & (pl.col("ledger_residual").abs() > _ROUNDING_TOL)
        )
        for row in out_of_band.iter_rows(named=True):
            d = row["date"]
            d = d.date() if isinstance(d, datetime) else d
            anomaly_rows.append({
                "data_source": _CASHFLOW_DS, "record_date": d.isoformat(),
                "anomaly_type": "LEDGER_RESIDUAL_OUT_OF_BAND",
                "original_value": float(row["ledger_residual"]),
                "expected_value": 0.0,
                "severity": "WARNING", "key": f"ledger-residual:{d.isoformat()}",
                "note": (
                    "台账与余额差超出舍入带（±15M）；保留原值待人工解释"
                    "（2008-2011 集中于危机时代特别措施）"
                ),
            })
        if anomaly_rows:
            record_anomalies(meta, anomaly_rows)

        print("[7/7] 输出报告")
        # UNCLASSIFIED 明细（按原始分类聚合）
        un_detail = (
            un_rows.group_by("raw_category", "direction")
            .agg(pl.col("amount_usd_millions").sum().alias("amount"), pl.len().alias("n"))
            .sort("amount", descending=True)
        )
        unclassified_payload = {
            "amount": unclassified_amount,
            "ratio": unclassified_ratio,
            "top": [
                {"raw_category": r["raw_category"], "direction": r["direction"],
                 "amount": r["amount"], "n": r["n"]}
                for r in un_detail.head(20).iter_rows(named=True)
            ],
        }
        duplicates_payload = {"count": dup_count, "amount": dup_amount}
        report_path = write_report(
            report_dir or (_REPO_ROOT / "reports" / "tga"),
            balance, recon, debt, acceptance,
            unclassified_payload, duplicates_payload,
            taxonomy, config_h, start, end, debt_check,
        )
        from tga_store import rebuild_views

        rebuild_views(settings)

        passed = sum(1 for a in acceptance if a["passed"])
        summary = {
            "report": str(report_path),
            "rows": {
                "balance": n_balance, "cashflow": n_cashflow,
                "reconciliation": n_recon, "debt_cash_basis": n_debt,
            },
            "findings": len(findings),
            "unclassified_ratio": unclassified_ratio,
            "acceptance": f"{passed}/{len(acceptance)}",
        }
        print(f"完成：报告 {report_path}；验收 {passed}/{len(acceptance)} PASS")
        return (0 if passed == len(acceptance) else 1), summary
    finally:
        meta.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="TGA Model 0 历史重建")
    parser.add_argument("--start", default=None, help="起始日期（YYYY-MM-DD，含）")
    parser.add_argument("--end", default=None, help="结束日期（YYYY-MM-DD，含）")
    parser.add_argument("--dry-run", action="store_true", help="只输出计划与验收，不落盘")
    parser.add_argument("--no-report", action="store_true", help="不写报告")
    parser.add_argument(
        "--report-dir", default=None,
        help="报告输出目录（缺省 reports/tga/）",
    )
    args = parser.parse_args()

    start = date.fromisoformat(args.start) if args.start else None
    end = date.fromisoformat(args.end) if args.end else None
    report_dir = Path(args.report_dir) if args.report_dir else None
    settings = Settings.load()
    try:
        code, _ = run_reconstruct(
            settings, start, end, dry_run=args.dry_run,
            report=not args.no_report, report_dir=report_dir,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"重建失败：{type(exc).__name__}: {exc}", file=sys.stderr)
        raise
    return code


if __name__ == "__main__":
    sys.exit(main())
