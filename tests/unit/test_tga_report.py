"""tga_report 单元测试（Phase 3）：报告章节与格式约定。"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import polars as pl

_SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from tga_forecast import MODEL_VERSION, _rows_to_components  # noqa: E402
from tga_report import render_report  # noqa: E402


def _comps() -> pl.DataFrame:
    rows = [
        {"target_date": date(2026, 11, 3), "category": "DEBT_ISSUANCE",
         "predicted": 100000.0, "lower": 100000.0, "upper": 100000.0,
         "method": "m", "confidence": 1.0, "source_type": "KNOWN_FINAL",
         "event_key": "ISSUE:X:2026-11-03", "detail": "Bill 4-Week"},
        {"target_date": date(2026, 11, 4), "category": "TAX",
         "predicted": 500.0, "lower": 250.0, "upper": 750.0,
         "method": "m", "confidence": 0.5, "source_type": "STATISTICAL",
         "event_key": None, "detail": "individual"},
    ]
    return _rows_to_components(rows)


def _fcst() -> pl.DataFrame:
    # last_tga = 1,200,000M；day1 net +100,000M；day2 net +500M
    return pl.DataFrame({
        "target_date": [date(2026, 11, 3), date(2026, 11, 4)],
        "horizon": [7, 7], "is_horizon_boundary": [False, True],
        "predicted_tga": [1300000.0, 1300500.0],
        "lower": [1300000.0, 1300250.0],
        "upper": [1300000.0, 1300750.0],
        "daily_net": [100000.0, 500.0],
        "source_type": ["KNOWN_FINAL", "STATISTICAL"],
    })


def _run() -> dict:
    return {
        "forecast_run_id": "r1", "origin_date": "2026-11-02",
        "data_cutoff_date": "2026-10-29", "model_version": MODEL_VERSION,
        "config_hash": "abc123", "scenarios_json": '["base"]',
        "horizons_json": "[7, 14, 30]", "snapshot_id": None,
        "status": "SUCCESS", "is_repeat": 0, "output_hash": "x",
    }


def _balance() -> pl.DataFrame:
    return pl.DataFrame({
        "date": [date(2026, 10, 29)],
        "opening": [1100.0], "closing": [1200.0], "daily_change": [100.0],
        "total_deposits": [None], "total_withdrawals": [None],
        "quality_status": ["VALID"], "flags": [""],
    })


def test_render_report_sections_and_disclaimer():
    text = render_report(_run(), _comps(), _fcst(), _balance())
    for section in (
        "## 一、当前 TGA 状况", "## 二、模型定位与能力边界",
        "## 三、情景路径", "## 四、情景定义与切换条件",
        "## 五、逐日现金流路径", "## 六、驱动因素归因",
        "## 七、来源构成与证据强度", "## 八、融资压力",
        "## 九、回测与 DM 检验", "## 十、使用限制与结论",
    ):
        assert section in text
    assert "不是**美国财政部官方预测" in text
    # 单位为 Billion USD（2 位小数）
    assert "1,200.00 B" in text  # 预测起点 TGA（1200000M）
    assert "1,300.50" in text  # T+7 边界（1300500M）
    assert "100.00" in text  # 发行流入 100000M → 100.00B
    # 预测区间为简化带宽，须明确提示
    assert "不应**作为置信区间使用" in text
    # 情景定义与切换条件来自 scenarios.yaml
    assert "触发条件" in text and "净融资" in text


def test_render_report_composition_labels():
    text = render_report(_run(), _comps(), _fcst(), _balance())
    assert "国债发行" in text and "税收收入" in text
    assert "KNOWN_FINAL" in text  # 事件表标注来源


def test_conclusions_use_scenario_wording_not_point_forecast():
    """锁定口径：结论章节不得出现点位预测式表述。"""
    text = render_report(_run(), _comps(), _fcst(), _balance())
    tail = text.split("## 十、使用限制与结论", 1)[1]
    for forbidden in ("预计较数据截止日", "增加约", "减少约", "TGA 预计", "点位预测为"):
        assert forbidden not in tail
    assert "不是点位预测" in tail
    assert "情景锚点" in tail
    assert "能力边界" in tail


def _backtest_run() -> dict:
    return {
        "backtest_run_id": "b1", "created_at": "2026-11-03 10:00:00",
        "start_date": "2021-11-01", "end_date": "2026-10-29", "stride": 5,
        "model_versions_json":
            f'["event_driven", "historical_mean", "naive", "seasonal_naive", '
            f'"{MODEL_VERSION}"]',
        "config_hash": "abc123", "output_hash": "h",
    }


def _backtest_eval() -> pl.DataFrame:
    def rows(version: str, predicted: float, lower, upper) -> dict:
        return {
            "run_id": "b1", "origin_date": date(2026, 10, 22),
            "data_cutoff": date(2026, 10, 21), "target_date": date(2026, 11, 2),
            "horizon": 7, "model_version": version, "predicted": predicted,
            "actual": 1_250_000.0, "error": predicted - 1_250_000.0,
            "abs_error": abs(predicted - 1_250_000.0), "pct_error": 0.01,
            "direction_correct": True, "lower": lower, "upper": upper,
            "config_hash": "abc123",
        }

    return pl.DataFrame([
        rows(MODEL_VERSION, 1_300_000.0, 1_280_000.0, 1_320_000.0),
        rows("naive", 1_200_000.0, None, None),
        rows("historical_mean", 1_240_000.0, None, None),
        rows("seasonal_naive", 1_260_000.0, None, None),
        rows("event_driven", 1_230_000.0, None, None),
    ])


def test_render_report_backtest_section():
    attribution = pl.DataFrame({
        "run_id": ["b1"], "error_type": ["tax_error"],
        "error_amount": [-5000.0],
    })
    text = render_report(
        _run(), _comps(), _fcst(), _balance(),
        backtest_run=_backtest_run(),
        backtest_eval=_backtest_eval(),
        backtest_attribution=attribution,
    )
    assert "回测区间：2021-11-01 → 2026-10-29" in text
    assert "| T+7 |" in text
    assert "tax_error" in text
    # §7.6 规则 6：主模型 MAE 最差时须如实说明
    assert "不得宣称有效" in text
