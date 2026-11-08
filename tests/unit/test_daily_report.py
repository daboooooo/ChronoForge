"""日报 Phase 1 单元测试：交易日/新鲜度矩阵/非交易日跳过。"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from chronoforge.report import narrative, run_report
from chronoforge.report.freshness import build_freshness, expected_asof


def test_weekly_h41_matrix_by_weekday() -> None:
    # 周一→上周三；周五→本周三（T-2）
    assert expected_asof("weekly_h41", date(2026, 11, 9)) == date(2026, 11, 4)
    assert expected_asof("weekly_h41", date(2026, 11, 13)) == date(2026, 11, 11)
    assert expected_asof("weekly_h41", date(2026, 11, 12)) == date(2026, 11, 4)


def test_trading_lag_two_skips_weekend() -> None:
    assert expected_asof("trading_lag_2", date(2026, 11, 9)) == date(2026, 11, 5)
    assert expected_asof("trading_lag_1", date(2026, 11, 9)) == date(2026, 11, 6)


def test_freshness_marks_lagged_and_missing() -> None:
    rows = build_freshness(
        {"WRESBAL": "2026-10-28", "DXY": "2026-11-06"}, date(2026, 11, 9)
    )
    by_code = {row.code: row for row in rows}
    assert by_code["WRESBAL"].status == "lagged"
    assert by_code["WRESBAL"].lag_days == 7
    assert by_code["DXY"].status == "ok"
    assert by_code["VIXCLS"].status == "missing"
    assert by_code["VIXCLS"].actual is None


def test_non_trading_day_writes_nothing(tmp_path) -> None:
    result = run_report(date(2026, 11, 8), output_dir=tmp_path)
    assert result.status == "skipped"
    assert result.html_path is None
    assert list(tmp_path.iterdir()) == []


def test_data_build_failure_still_writes_report(tmp_path, monkeypatch) -> None:
    import chronoforge.report.pipeline as pipeline

    def _boom(day):
        raise RuntimeError("meta db unavailable")

    monkeypatch.setattr(pipeline, "build_facts", _boom)
    result = pipeline.run_report(date(2026, 11, 9), output_dir=tmp_path)
    assert result.status == "degraded"
    assert result.html_path and result.html_path.exists()
    assert result.markdown_path and result.markdown_path.exists()
    assert result.json_path and result.json_path.exists()
    md = result.markdown_path.read_text(encoding="utf-8")
    assert "数据装配失败" in md
    assert "缺失" in md


def test_source_anomaly_flag_is_consumed_as_gap(tmp_path, monkeypatch) -> None:
    """任务一的源侧异常必须是报告侧可读的数据缺口，而不是孤立计数器。"""
    import chronoforge.report.facts as facts
    from chronoforge.storage.meta import MetaStore

    meta = MetaStore(str(tmp_path))
    meta.migrate()
    meta.add_quality_flags([
        {
            "record_key": "normalize-source-anomaly:run-1:1",
            "dataset_id": "fred_series_DGS10",
            "rule_id": "Q-NORM-SRC-ANOMALY",
            "severity": "WARNING",
            "detail": "normalize source anomaly: 1/1 min_units=10",
            "run_id": "run-1",
        },
        {
            "record_key": "normalize-source-anomaly:run-2:1",
            "dataset_id": "fred_series_DGS10",
            "rule_id": "Q-NORM-SRC-ANOMALY",
            "severity": "WARNING",
            "detail": "normalize source anomaly: 2/2 min_units=10",
            "run_id": "run-2",
        },
    ])
    monkeypatch.setattr(facts, "open_meta", lambda settings: meta)
    monkeypatch.setattr(
        facts.Settings, "load", classmethod(lambda cls: object())
    )

    anomalies = facts._source_anomalies(date(2026, 11, 9))
    assert len(anomalies) == 1
    assert anomalies[0]["dataset_id"] == "fred_series_DGS10"
    assert anomalies[0]["count"] == 2
    assert anomalies[0]["kind"] == "source_anomaly"


# ── H.4.1 明细拆分（第 4 项）────────────────────────────────────────────
_WEEKS = pd.DatetimeIndex(["2026-10-21", "2026-10-28", "2026-11-04"])


def _series(values: list[float]) -> pd.Series:
    return pd.Series(values, index=_WEEKS, dtype=float)


def _h41_split(*, walcl: float = 6_748_000.0, wlad: float = 24_000.0,
               wltlecl: float = 6_742_000.0) -> dict:
    """构造自洽的 H.4.1 周度明细（百万美元；RRP 为十亿美元）。"""
    from chronoforge.report import facts

    mod = facts.load_export_module()
    detail = {
        "WLTLECL": _series([6_647_000.0, 6_647_000.0, wltlecl]),
        "WLDLCL": _series([3_950_000.0, 3_950_000.0, 4_022_000.0]),
        "WLAD": _series([22_000.0, 22_000.0, wlad]),
        "WLDACLC": _series([-10_000.0, -10_000.0, -10_000.0]),
        "TERMT": _series([0.0, 0.0, 0.0]),
        "WLRRAL": _series([325_000.0, 325_000.0, 330_000.0]),
        "WLFN": _series([2_360_000.0, 2_360_000.0, 2_376_000.0]),
    }
    districts = {f"D{i}WLFOL": _series([0.0, 0.0, 0.0]) for i in range(1, 13)}
    return mod.compute_h41_detail(
        _series([2_895_000.0, 2_928_000.0, 2_956_000.0]),  # WRESBAL 周均
        _series([815_000.0, 824_000.0, 865_000.0]),        # WTREGEN 周均
        _series([6_648_000.0, 6_650_000.0, walcl]),
        _series([2_295_000.0, 2_300_000.0, 2_305_000.0]),
        _series([2_925_000.0, 2_925_000.0, 2_955_000.0]),
        _series([825_000.0, 825_000.0, 865_000.0]),
        _series([0.2, 0.2, 0.2]),
        detail,
        districts,
    )


def test_h41_detail_splits_other_and_keeps_timing_separate() -> None:
    out = _h41_split()
    by_key = {item["key"]: item for item in out["items"]}
    assert by_key["capital"]["contribution_b"] == pytest.approx(-3.0)
    assert by_key["cash_gap"]["contribution_b"] == pytest.approx(-11.0)
    assert by_key["foreign_repo"]["contribution_b"] == pytest.approx(-5.0)
    assert by_key["other_deposits"]["contribution_b"] == pytest.approx(-2.0)
    assert by_key["other_liabilities"]["contribution_b"] == pytest.approx(-2.0)
    assert out["named_sum_b"] == pytest.approx(-23.0)
    assert out["timing"]["reserve_diff_b"] == pytest.approx(-2.0)
    assert out["timing"]["tga_diff_b"] == pytest.approx(1.0)
    assert out["timing"]["sum_b"] == pytest.approx(-1.0)
    assert out["other_reported_b"] == pytest.approx(-24.0)
    assert out["residual_b"] == pytest.approx(0.0)


def test_h41_detail_reports_nonzero_residual_as_is() -> None:
    # 破坏 WLTLECL 恒等式后，残差必须如实输出，不被抹平。
    out = _h41_split(wlad=26_000.0)
    assert out["residual_b"] == pytest.approx(2.0)
    lines = narrative._h41_split_lines({"h41_detail": out}, out["other_reported_b"])
    assert "残差 +2.0 B" in lines[1]


def test_h41_narrative_has_timing_row_and_wlrral_semantics() -> None:
    out = _h41_split()
    lines = narrative._h41_split_lines({"h41_detail": out}, out["other_reported_b"])
    joined = "\n".join(lines)
    assert "时点核对" in joined
    assert "准备金周均与时点差 -2.0 B" in joined
    assert "残差 +0.0 B" in joined
    assert "WLRRAL" in joined
    assert "两池框架不变" in joined


def test_h41_narrative_degrades_without_detail() -> None:
    lines = narrative._h41_split_lines(
        {"h41_detail": {"error": "H.4.1 明细观测不足两周"}}, -5.0
    )
    assert lines[0].startswith("H.4.1 明细不可用")
