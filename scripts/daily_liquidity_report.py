#!/usr/bin/env python
"""每日流动性简报入口。

用法：
  uv run python scripts/daily_liquidity_report.py                  # 按美东当前日期
  uv run python scripts/daily_liquidity_report.py --date 2026-11-09
  uv run python scripts/daily_liquidity_report.py --output-dir reports/daily_liquidity

退出码：0 已生成 / 非交易日跳过；2 数据降级但仍产出（显式缺口）；3 生成失败。
非交易日不写任何文件；失败时三件产物保持上一次的完整状态（原子写）。
"""

from __future__ import annotations

import argparse
import sys
import traceback
from datetime import date
from pathlib import Path

import structlog

from chronoforge.report import run_report

logger = structlog.get_logger()


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"日期格式应为 YYYY-MM-DD: {value}") from exc


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="生成每日流动性简报（模板模式）")
    parser.add_argument(
        "--date",
        type=_parse_date,
        default=None,
        help="报告日期（美东交易日），默认取 America/New_York 当天",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="产物目录，默认 reports/daily_liquidity",
    )
    args = parser.parse_args(argv)

    try:
        result = run_report(report_day=args.date, output_dir=args.output_dir)
    except Exception as exc:
        logger.error("report.failed", report_date=str(args.date), exc_info=True)
        print(f"报告生成失败：{type(exc).__name__}: {exc}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return 3
    if result.status == "skipped":
        print(result.message)
        return 0
    print(result.message)
    print(f"HTML: {result.html_path}")
    print(f"MD:   {result.markdown_path}")
    print(f"JSON: {result.json_path}")
    return 0 if result.status == "generated" else 2


if __name__ == "__main__":
    sys.exit(main())
