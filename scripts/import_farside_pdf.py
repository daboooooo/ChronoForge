#!/usr/bin/env python3
"""将 Farside Investors 的 ETF Flow PDF 导入为 sosovalue FLOW canonical 数据。

用途：
    SoSoValue OpenAPI 仅能回看最近 1 个月；Farside Investors 提供自美股
    现货 ETF 上市日（BTC: 2024-01-11）起的全量日频分 ticker 流向。
    本脚本把手工下载的 PDF 全量表转换为与 sosovalue.normalize 完全一致
    的 FLOW 记录格式，按 natural key upsert 补充到既有 entity 分区，
    使历史数据完整。

转换规则（严格对齐 connectors/sosovalue.py normalize）：
    - 单位：Farside 为 US$m（百万美元）→ value × 1_000_000，units="USD"
    - observation_time = D 00:00:00（UTC naive）
    - release_time = revision_time = (D+1) 00:00:00（固定值）
    - FLOW period_start=D，period_end=D+1（右开）
    - source="sosovalue"，source_id=entity_id，
      raw_record_id=f"sosovalue:{entity_id}:{date}"
    - quality_status="VALID"，seasonal_adjustment="NOT_SEASONALLY_ADJUSTED"
    - PDF 内全部 ticker 列为 "-" 的行（休市日，Total=0.0）→ 跳过
      （SoSoValue API 在非交易日不产出记录）
    - 已存在的 observation_time（重叠窗口）→ 跳过，不覆盖源方实测值

列映射（13 个 FLOW entity）：
    IBIT/FBTC/BITB/ARKB/BTCO/EZBC/BRRR/HODL/BTCW/MSBT/GBTC/BTC
      → etf_us_btc_{TICKER}_net_inflow
    Total → etf_us_btc_summary_total_net_inflow

注意：本 PDF 只含净流入，无法补充 total_value_traded / total_net_assets /
      cum_net_inflow 三个指标。

用法：
    python scripts/import_farside_pdf.py --dry-run           # 预览
    python scripts/import_farside_pdf.py                     # 正式导入
    python scripts/import_farside_pdf.py --verbose
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb

from chronoforge.models.enums import CanonicalType
from chronoforge.storage.canonical import CanonicalStoreImpl

_REPO_ROOT = Path(__file__).resolve().parents[1]
_DEFAULT_PDF = (
    _REPO_ROOT / "data"
    / "Bitcoin ETF Flow – All Data (US$m) – Farside Investors.pdf"
)

_MONTHS = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}

# PDF 列（顺序） → entity_id
_TICKER_COLUMNS = [
    "IBIT", "FBTC", "BITB", "ARKB", "BTCO", "EZBC",
    "BRRR", "HODL", "BTCW", "MSBT", "GBTC", "BTC",
]
_COLUMN_ENTITIES: dict[str, str] = {
    t: f"etf_us_btc_{t}_net_inflow" for t in _TICKER_COLUMNS
}
_COLUMN_ENTITIES["Total"] = "etf_us_btc_summary_total_net_inflow"
_COLUMNS = [*_TICKER_COLUMNS, "Total"]

_UNITS = "USD"
_SEASONAL_ADJUSTMENT = "NOT_SEASONALLY_ADJUSTED"


# ── PDF 文本提取 ─────────────────────────────────────────────────────


def pdf_to_text(pdf_path: Path) -> str:
    """用 poppler pdftotext -layout 把 PDF 转成布局文本。"""
    proc = subprocess.run(
        ["pdftotext", "-layout", str(pdf_path), "-"],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"pdftotext 失败（code={proc.returncode}）: {proc.stderr.strip()}"
        )
    return proc.stdout


# ── 表格解析 ──────────────────────────────────────────────────────────

# 形态：token 流中识别「日 → 月 → 13 单元格 → 年」，或跨页变体
# 「日 → 13 单元格 → 月 → 年」（日期与数值同行、月份在下一行）
_DAY = re.compile(r"^\d{1,2}$")
_YEAR = re.compile(r"^\d{4}$")
_CELL = re.compile(
    r"^(?:\(?-?\d{1,4}(?:,\d{3})*(?:\.\d+)?\)?|-)$"
)
_N_COLUMNS = len(_COLUMNS)


def _is_noise(line: str) -> bool:
    """表头 / 空行 / 页脚 Total 行等非数据行。"""
    if not line:
        return True
    if line.startswith("Date"):
        return True
    if "IBIT" in line:  # 跨页重复表头
        return True
    if line.startswith("Total ") or line == "Total":
        return True
    return False


def parse_table(text: str) -> list[dict[str, Any]]:
    """解析布局文本为 [{"date": date, "cells": {col: float|None}}, ...]。

    先把有效行拍平成 token 流（跨页表头行整体剔除），再按两种排列识别：
      常规:  day month cell×13 year
      跨页:  day cell×13 month year

    Raises:
        ValueError: 结构异常（缺列/坏单元格/日期非递增）。
    """
    tokens: list[str] = []
    for raw in text.splitlines():
        line = raw.strip()
        if _is_noise(line):
            continue
        tokens.extend(line.split())

    entries: list[dict[str, Any]] = []
    i = 0
    n = len(tokens)

    while i < n:
        t = tokens[i]
        advance = True

        if _DAY.match(t) and i + 1 < n:
            day = int(t)

            if tokens[i + 1] in _MONTHS:
                # 常规排列：day month cells ... year
                mon = tokens[i + 1]
                j = i + 2
                cell_tokens: list[str] = []
                while j < n and not _YEAR.match(tokens[j]):
                    cell_tokens.append(tokens[j])
                    j += 1
                if j >= n:
                    raise ValueError(f"{day} {mon}: 未找到年份")
                if len(cell_tokens) != _N_COLUMNS:
                    raise ValueError(
                        f"{day} {mon}: 期望 {_N_COLUMNS} 列，"
                        f"实际 {len(cell_tokens)}: {cell_tokens}"
                    )
                year = int(tokens[j])
                i = j + 1
                advance = False

            elif (
                i + _N_COLUMNS + 2 < n
                and _CELL.match(tokens[i + 1])
                and tokens[i + 1 + _N_COLUMNS] in _MONTHS
                and _YEAR.match(tokens[i + 2 + _N_COLUMNS])
            ):
                # 跨页排列：day cells×13 month year
                start = i + 1
                end = start + _N_COLUMNS
                cell_tokens = tokens[start:end]
                mon = tokens[i + 1 + _N_COLUMNS]
                year = int(tokens[i + 2 + _N_COLUMNS])
                i += _N_COLUMNS + 3
                advance = False

        if advance:
            i += 1
            continue

        if not 1 <= day <= 31:
            raise ValueError(f"非法日期: {day} {mon} {year}")

        cells: dict[str, float | None] = {}
        for col, tok in zip(_COLUMNS, cell_tokens):
            cells[col] = _parse_cell(tok)

        obs_date = datetime(year, _MONTHS[mon], day).date()
        entries.append({"date": obs_date, "cells": cells})

    # 全局校验：日期严格递增
    prev = None
    for e in entries:
        d = e["date"]
        if prev is not None and d <= prev:
            raise ValueError(f"日期非严格递增: {prev} → {d}")
        prev = d

    return entries


def _parse_cell(token: str) -> float | None:
    """单元格 → float（百万美元）；"-" → None。

    支持括号负数 (95.1)、千分位逗号 1,045.0。
    """
    if token == "-":
        return None
    negative = False
    t = token
    if t.startswith("(") and t.endswith(")"):
        negative = True
        t = t[1:-1]
    t = t.replace(",", "")
    try:
        v = float(t)
    except ValueError as e:
        raise ValueError(f"无法解析数值单元格: {token!r}") from e
    return -v if negative else v


# ── 已有日期查询 ─────────────────────────────────────────────────────


def existing_dates(data_dir: Path, entity: str) -> set[str]:
    """读取 entity 已落盘的 observation_time 集合（YYYY-MM-DD）。"""
    ent_dir = data_dir / "canonical/FLOW" / f"entity={entity}"
    if not ent_dir.exists():
        return set()
    glob_path = ent_dir / "**/*.parquet"
    con = duckdb.connect()
    try:
        rows = con.execute(
            f"SELECT DISTINCT strftime(observation_time, '%Y-%m-%d') "
            f"FROM read_parquet('{glob_path.as_posix()}')"
        ).fetchall()
    finally:
        con.close()
    return {r[0] for r in rows}


# ── FLOW 记录构造 ────────────────────────────────────────────────────


def build_record(
    entity: str, obs_date: datetime, value_m: float, now: datetime
) -> dict[str, Any]:
    """构造与 sosovalue.normalize 输出一致的 FLOW 记录 dict。

    value_m 单位为百万美元，落盘 ×1e6 为美元。
    """
    date_str = obs_date.strftime("%Y-%m-%d")
    release = obs_date + timedelta(days=1)
    return {
        "schema_version": "1.0",
        "source": "sosovalue",
        "source_id": entity,
        "source_timestamp": obs_date,
        "ingest_timestamp": now,
        "raw_record_id": f"sosovalue:{entity}:{date_str}",
        "quality_status": "VALID",
        "quality_reason": None,
        "observation_time": obs_date,
        "release_time": release,
        "revision_time": release,
        "value": value_m * 1_000_000.0,
        "units": _UNITS,
        "seasonal_adjustment": _SEASONAL_ADJUSTMENT,
        "vintage_date": "",
        "period_start": obs_date,
        "period_end": release,
    }


# ── 主流程 ───────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="将 Farside ETF Flow PDF 导入补充为 sosovalue FLOW 数据"
    )
    p.add_argument("--pdf", type=Path, default=_DEFAULT_PDF, help="PDF 路径")
    p.add_argument(
        "--dry-run", action="store_true", help="仅预览，不写入 canonical"
    )
    p.add_argument("--verbose", action="store_true", help="逐 entity 输出明细")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    if not args.pdf.exists():
        print(f"错误: PDF 不存在: {args.pdf}", file=sys.stderr)
        return 1

    text = pdf_to_text(args.pdf)
    entries = parse_table(text)

    session_rows = [
        e for e in entries
        if any(e["cells"][t] is not None for t in _TICKER_COLUMNS)
    ]
    holiday_rows = len(entries) - len(session_rows)

    print(f"PDF 解析: {len(entries)} 行 "
          f"({entries[0]['date']} .. {entries[-1]['date']})，"
          f"其中交易日 {len(session_rows)}、休市/空行 {holiday_rows}")

    data_dir = _REPO_ROOT / "data"
    now = datetime.now().replace(microsecond=0)
    store = CanonicalStoreImpl(str(data_dir))

    total_insert = 0
    total_skip_existing = 0
    total_skip_missing_cell = 0

    for col in _COLUMNS:
        entity = _COLUMN_ENTITIES[col]
        existed = existing_dates(data_dir, entity)

        records: list[dict[str, Any]] = []
        for row in session_rows:
            d = row["date"]
            date_str = d.strftime("%Y-%m-%d")
            if date_str in existed:
                total_skip_existing += 1
                continue
            v = row["cells"][col]
            if v is None:
                # 该 ticker 当日无数据（未上市/无成交），不产记录
                total_skip_missing_cell += 1
                continue
            records.append(
                build_record(entity, datetime(d.year, d.month, d.day), v, now)
            )

        if args.dry_run:
            overlap = sum(
                1 for r in session_rows
                if r["date"].strftime("%Y-%m-%d") in existed
            )
            print(f"  [DRY RUN] {entity:<42} 将写入 {len(records)} 条"
                  f"（已存在 {overlap}）")
            total_insert += len(records)
            continue

        stats = store.upsert(records, CanonicalType.FLOW, entity)
        total_insert += stats.inserted
        if args.verbose or stats.inserted > 0:
            print(f"  [OK] {entity:<42} inserted={stats.inserted} "
                  f"updated={stats.updated} partitions={stats.rewritten_partitions}"
                  + (f" drift={stats.drifted}" if stats.drifted else ""))

    mode = "DRY RUN" if args.dry_run else "导入完成"
    print(f"\n{mode}: 新增 {total_insert} 条；跳过重叠日期 {total_skip_existing}，"
          f"跳过无数据单元格 {total_skip_missing_cell}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
