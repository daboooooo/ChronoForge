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


# ── 缺口分类：等发布 vs 需核查 ──────────────────────────────────────────
def _series_gap(code: str) -> dict[str, object]:
    return {
        "kind": "series_lagged",
        "series": code,
        "label": "美元指数",
        "expected_asof": "2026-11-06",
        "actual_asof": "2026-11-05",
        "detail": "比矩阵下限滞后 1 天",
    }


def test_gap_classification_uses_quality_flags_not_dataset_allowlist() -> None:
    from chronoforge.report.freshness import classify_gaps

    gaps = [_series_gap("DXY"), _series_gap("SPX")]
    classify_gaps(gaps, {"YAHOO:DX-Y.NYB:OHLCV:1d"})
    assert [g["category"] for g in gaps] == ["needs_check", "await_release"]
    assert len(gaps) == 2  # 分类不删除任何条目
    assert "上游" in str(gaps[0]["next_step"])
    assert "等待下一次官方发布" in str(gaps[1]["next_step"])


def test_gap_classification_without_flags_treats_lag_as_awaiting_release() -> None:
    from chronoforge.report.freshness import classify_gaps

    gaps = [_series_gap("DXY")]
    classify_gaps(gaps, set())
    assert gaps[0]["category"] == "await_release"


def test_gap_narrative_keeps_both_categories_and_next_steps() -> None:
    from chronoforge.report.freshness import classify_gaps

    gaps = [
        _series_gap("SPX"),
        {
            "kind": "source_anomaly",
            "dataset_id": "YAHOO:DX-Y.NYB:OHLCV:1d",
            "count": 1,
            "last_at": "2026-11-08T13:08:34Z",
            "detail": "reasons={'invalid_bar': 1}",
        },
        {
            "kind": "batch_status",
            "status": "partial",
            "finished_at": "2026-11-08T13:14:50Z",
            "exit_code": 1,
            "detail": "最近一次数据更新批次非成功态",
        },
    ]
    classify_gaps(gaps, {"YAHOO:DX-Y.NYB:OHLCV:1d"})
    facts = {
        "freshness_summary": {"core_ok": 7, "core_total": 7, "total": 17,
                              "ok": 16, "lagged": 1, "missing": 0},
        "batch_status": {"status": "partial", "finished_at": "2026-11-08T13:14:50Z",
                         "exit_code": 1},
        "gaps": gaps,
    }
    lines = narrative._gap_bullets(facts)
    joined = "\n".join(lines)
    assert "未到发布时点（正常）：1 项。" in joined
    assert "源侧数据异常或批次问题（需查）：2 项。" in joined
    assert joined.count("下一步：") == 3
    assert "YAHOO:DX-Y.NYB:OHLCV:1d" in joined
    assert "等待下一次官方发布" in joined
    assert "检查 var/log/daily_batch.log" in joined


# ── 阶段 A：调度与运维闭环 ────────────────────────────────────────────
def test_calendar_block_marks_early_close_and_next_events() -> None:
    from chronoforge.report import facts

    normal = facts._calendar_block(date(2026, 11, 9))
    assert normal["early_close"] is False
    assert normal["next_early_close"] == "2026-11-27"
    assert normal["next_holiday"] == "2026-11-26"

    early = facts._calendar_block(date(2026, 11, 27))
    assert early["early_close"] is True
    assert early["early_close_time_et"] == "13:00"
    assert early["next_early_close"] == "2026-12-24"


def test_run_report_skips_holiday_without_writing_files(tmp_path) -> None:
    r = run_report(report_day=date(2026, 11, 26), output_dir=tmp_path)
    assert r.status == "skipped"
    assert list(tmp_path.iterdir()) == []


def test_write_set_keeps_previous_products_on_failure(tmp_path, monkeypatch) -> None:
    from chronoforge.report import pipeline

    html = tmp_path / "a.html"
    html.write_text("old", encoding="utf-8")
    md = tmp_path / "b.md"

    real_write = pipeline.Path.write_text

    def flaky(self, content, encoding=None, **kwargs):
        if self.name == "b.md.tmp":
            raise OSError("disk full")
        return real_write(self, content, encoding=encoding, **kwargs)

    monkeypatch.setattr(pipeline.Path, "write_text", flaky)
    with pytest.raises(OSError):
        pipeline._write_set({html: "new", md: "new"})
    assert html.read_text(encoding="utf-8") == "old"
    assert not (tmp_path / "a.html.tmp").exists()
    assert not md.exists()


def test_prune_reports_removes_only_expired(tmp_path) -> None:
    from chronoforge.report.pipeline import _prune_reports

    for day in ("2026-11-09", "2026-01-01"):
        for suffix in (".html", ".md", ".json"):
            (tmp_path / f"{day}{suffix}").write_text("x", encoding="utf-8")
    removed = _prune_reports(tmp_path, date(2026, 11, 9))
    assert removed == ["2026-01-01"]
    assert not (tmp_path / "2026-01-01.html").exists()
    assert (tmp_path / "2026-11-09.json").exists()


def test_index_lists_reports_and_survives_bad_json(tmp_path) -> None:
    import json

    from chronoforge.report.pipeline import _index_entries
    from chronoforge.report.render import render_index

    good = {
        "report_date": "2026-11-09",
        "status": "generated",
        "metrics": {"reserve": {"level_t": 2.95, "chg_4w_b": -125.0}},
        "freshness_summary": {"core_ok": 7, "core_total": 7},
        "gaps": [],
    }
    (tmp_path / "2026-11-09.json").write_text(json.dumps(good), encoding="utf-8")
    (tmp_path / "2026-11-08.json").write_text("{not json", encoding="utf-8")

    entries = _index_entries(tmp_path)
    assert [item["date"] for item in entries] == ["2026-11-09"]
    html = render_index(entries, date(2026, 11, 9))
    assert "2026-11-09.html" in html
    assert "已生成" in html
    assert "保留最近 180 天" in html


