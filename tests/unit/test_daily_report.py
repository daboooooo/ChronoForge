"""日报 Phase 1 单元测试：交易日/新鲜度矩阵/非交易日跳过。"""

from __future__ import annotations

from datetime import date

from chronoforge.report import run_report
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
