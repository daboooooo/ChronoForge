"""tga_backtest 单元测试（Phase 5）：指标计算与类别误差归因。"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import polars as pl

_SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from tga_backtest import (  # noqa: E402
    ATTRIBUTION_TYPES,
    MODEL_VERSION,
    _adjust_pvalues,
    attribution_rows,
    dm_results,
    dm_test,
    eval_metrics,
)


def _eval_rows() -> pl.DataFrame:
    return pl.DataFrame({
        "model_version": [MODEL_VERSION, MODEL_VERSION, "naive"],
        "horizon": [7, 7, 7],
        "target_date": [date(2026, 11, 6), date(2026, 11, 9), date(2026, 11, 6)],
        "predicted": [1_300_000.0, 1_200_000.0, 1_200_000.0],
        "actual": [1_290_000.0, 1_250_000.0, 1_290_000.0],
        "error": [10_000.0, -50_000.0, -90_000.0],
        "abs_error": [10_000.0, 50_000.0, 90_000.0],
        "pct_error": [10_000.0 / 1_290_000.0, 50_000.0 / 1_250_000.0, None],
        "direction_correct": [True, False, True],
        "lower": [1_250_000.0, 1_150_000.0, None],
        "upper": [1_350_000.0, 1_260_000.0, None],
    })


def test_eval_metrics_known_values():
    metrics = eval_metrics(_eval_rows())
    main = metrics.filter(pl.col("model_version") == MODEL_VERSION).row(0, named=True)
    naive = metrics.filter(pl.col("model_version") == "naive").row(0, named=True)

    assert main["n"] == 2
    assert main["mae"] == 30_000.0
    assert abs(main["rmse"] - ((10_000 ** 2 + 50_000 ** 2) / 2) ** 0.5) < 1e-6
    assert main["bias"] == -20_000.0
    assert main["direction_accuracy"] == 0.5
    # 两条样本均落入区间
    assert main["coverage"] == 1.0
    assert main["pinball"] is not None and main["pinball"] > 0
    # 基准无区间 → 覆盖/Pinball 不适用
    assert naive["coverage"] is None
    assert naive["pinball"] is None


class _StubCtx:
    origin = date(2026, 11, 2)
    through = date(2026, 10, 29)


def test_attribution_category_errors():
    components = pl.DataFrame({
        "target_date": [date(2026, 11, 6), date(2026, 11, 6)],
        "category": ["DEBT_ISSUANCE", "TAX"],
        "predicted": [100_000.0, 5_000.0],
    })
    actual = {
        date(2026, 11, 6): {
            "DEBT_ISSUANCE": 90_000.0, "TAX": 6_000.0, "OTHER": 1_000.0,
        }
    }
    rows = attribution_rows(
        _StubCtx(), {7: date(2026, 11, 6)}, components, actual, "hash"
    )
    by_component = {row["component"]: row for row in rows}
    assert set(by_component) == {"DEBT_ISSUANCE", "TAX", "OTHER"}
    assert by_component["DEBT_ISSUANCE"]["error_amount"] == 10_000.0
    assert by_component["TAX"]["error_amount"] == -1_000.0
    # 未预测但有实际值的类别 → 未映射残差桶
    assert by_component["OTHER"]["error_amount"] == -1_000.0
    assert by_component["OTHER"]["error_type"] == "outlay_error"
    assert ATTRIBUTION_TYPES["UNCLASSIFIED"] == "unclassified_error"


# ── Diebold-Mariano（HAC + 多重比较校正）──────────────────────────────


def test_dm_test_negative_stat_when_main_better():
    n = 60
    # 主模型损失恒为 0，基准损失在 500/1000 间波动 → 主模型显著更优
    main = [0.0] * n
    bench = [500.0 if i % 2 else 1000.0 for i in range(n)]
    res = dm_test(main, bench)
    assert res["n"] == n
    assert res["mean_loss_diff"] < 0
    assert res["dm_stat"] < -2
    assert res["p_value"] < 0.05
    assert res["hac_lag"] >= 1


def test_dm_test_insufficient_samples_is_null():
    res = dm_test([1.0, 2.0], [3.0, 4.0])
    assert res["n"] == 2
    assert res["p_value"] is None


def test_pvalue_adjustments_bonferroni_and_fdr():
    bonf, fdr = _adjust_pvalues([0.01, 0.04, 0.03])
    assert bonf == [0.03, 0.12, 0.09]
    # BH: 0.03 → 0.045 → 0.04，自右向左取单调包络
    assert [round(v, 6) for v in fdr] == [0.03, 0.04, 0.04]


def test_dm_results_pairs_by_origin_and_flags_significance():
    origins = [date(2026, 1, 2), date(2026, 1, 9), date(2026, 1, 16),
               date(2026, 1, 23), date(2026, 1, 30), date(2026, 2, 6),
               date(2026, 2, 13), date(2026, 2, 20)]
    rows = []
    for i, origin in enumerate(origins):
        rows.append({"origin_date": origin, "horizon": 7,
                     "model_version": MODEL_VERSION,
                     "abs_error": 10.0})
        rows.append({"origin_date": origin, "horizon": 7,
                     "model_version": "naive", "abs_error": 100.0 + i})
    eval_df = pl.DataFrame(rows)
    dm = dm_results(eval_df, MODEL_VERSION, run_date=date(2026, 3, 1))
    assert dm.height == 1
    row = dm.row(0, named=True)
    assert row["benchmark"] == "naive"
    assert row["n"] == 8
    assert row["dm_stat"] < 0
    assert row["significant_bonf"] is True
