"""每日流动性报告（Phase 1 最薄闭环）。

入口 ``chronoforge.report.pipeline.run_report`` 读取现有 canonical/derived 数据
（复用 ``visualization/export_data.py::build_payload`` 的指标链路），生成
``reports/daily_liquidity/YYYY-MM-DD.{html,md,json}``。Phase 1 不使用 LLM，
文字由确定性规则模板生成。
"""

from chronoforge.report.pipeline import ReportResult, run_report

__all__ = ["ReportResult", "run_report"]
