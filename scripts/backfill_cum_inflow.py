#!/usr/bin/env python3
"""回填 etf_us_btc_summary_cum_net_inflow（NUMBER）历史数据。

背景：
    SoSoValue OpenAPI 仅能回看最近 1 个月，cum_net_inflow 仅有 22 个观测日；
    Farside PDF 只含日频净流入（FLOW），不含累计值。
    经验证：既有 SoSoValue 累计值的每日增量与日频 Total 净流入逐日完全
    相等，仅存在一个恒定基准差（口径/历史修订差异）。

口径 A（校准对齐）：
    自上市首日 2024-01-11 起对 FLOW entity
    etf_us_btc_summary_total_net_inflow 逐日累加，
    基准差 offset 由既有重叠窗口自校准：
        offset = mean(既有累计值 − 日累加值)
    要求重叠窗口内各日 offset 严格一致（否则脚本中止），
    回填值 = 日累加值 + offset。
    既有 observation_time 一律跳过，不覆盖源方实测值。

记录格式严格对齐 connectors/sosovalue.py 的 NUMBER normalize
（无 period_start/period_end；时间语义与 FLOW 相同）。

用法：
    python scripts/backfill_cum_inflow.py --dry-run
    python scripts/backfill_cum_inflow.py
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb

from chronoforge.models.enums import CanonicalType
from chronoforge.storage.canonical import CanonicalStoreImpl

_REPO_ROOT = Path(__file__).resolve().parents[1]

_FLOW_ENTITY = "etf_us_btc_summary_total_net_inflow"
_NUMBER_ENTITY = "etf_us_btc_summary_cum_net_inflow"

_UNITS = "USD"
_SEASONAL_ADJUSTMENT = "NOT_SEASONALLY_ADJUSTED"

# 重叠窗口内 offset 日间最大允许相对偏差（相对累计值量级）
_OFFSET_SPREAD_RTOL = 1e-10
_OFFSET_SPREAD_FLOOR = 1e-6  # 美元


# ── 数据读取 ─────────────────────────────────────────────────────────


def read_series(data_dir: Path, canonical_type: str, entity: str) -> dict:
    """读取 entity 全部 (observation_time → value)，按时间升序返回。"""
    ent_dir = data_dir / "canonical" / canonical_type / f"entity={entity}"
    result: dict[datetime, float] = {}
    if not ent_dir.exists():
        return result
    glob_path = ent_dir / "**/*.parquet"
    con = duckdb.connect()
    try:
        rows = con.execute(
            f"SELECT observation_time, value "
            f"FROM read_parquet('{glob_path.as_posix()}')"
        ).fetchall()
    finally:
        con.close()
    for obs, value in rows:
        result[obs] = float(value)
    return dict(sorted(result.items()))


# ── NUMBER 记录构造 ──────────────────────────────────────────────────


def build_record(obs: datetime, value: float, now: datetime) -> dict[str, Any]:
    """构造与 sosovalue.normalize 输出一致的 NUMBER 记录 dict。"""
    date_str = obs.strftime("%Y-%m-%d")
    release = obs + timedelta(days=1)
    return {
        "schema_version": "1.0",
        "source": "sosovalue",
        "source_id": _NUMBER_ENTITY,
        "source_timestamp": obs,
        "ingest_timestamp": now,
        "raw_record_id": f"sosovalue:{_NUMBER_ENTITY}:{date_str}",
        "quality_status": "VALID",
        "quality_reason": None,
        "observation_time": obs,
        "release_time": release,
        "revision_time": release,
        "value": value,
        "units": _UNITS,
        "seasonal_adjustment": _SEASONAL_ADJUSTMENT,
        "vintage_date": "",
    }


# ── 主流程 ───────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="按口径 A 回填 cum_net_inflow NUMBER 历史数据"
    )
    p.add_argument(
        "--dry-run", action="store_true", help="仅预览，不写入 canonical"
    )
    p.add_argument("--verbose", action="store_true", help="输出逐日明细")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    data_dir = _REPO_ROOT / "data"

    flows = read_series(data_dir, "FLOW", _FLOW_ENTITY)
    if not flows:
        print(f"错误: FLOW entity 无数据: {_FLOW_ENTITY}", file=sys.stderr)
        return 1

    existing = read_series(data_dir, "NUMBER", _NUMBER_ENTITY)

    # 日累加（FLOW 序列已按 observation_time 升序）
    running: dict[datetime, float] = {}
    total = 0.0
    for obs, value in flows.items():
        total += value
        running[obs] = total

    # 自校准 offset：既有累计值 − 日累加值
    offsets = [
        existing[obs] - running[obs]
        for obs in existing
        if obs in running
    ]
    if offsets:
        spread = max(offsets) - min(offsets)
        max_scale = max(abs(running[obs]) for obs in existing if obs in running)
        tol = max(_OFFSET_SPREAD_FLOOR, _OFFSET_SPREAD_RTOL * max_scale)
        if spread > tol:
            print(
                f"错误: 重叠窗口 offset 非常数（spread={spread:.6f} "
                f"> tol={tol:.6f}），口径 A 前提不成立，中止",
                file=sys.stderr,
            )
            return 1
        offset = sum(offsets) / len(offsets)
        print(
            f"自校准: 重叠 {len(offsets)} 日，offset={offset:,.4f} USD，"
            f"spread={spread:.2e}（tol={tol:.2e}）"
        )
    else:
        offset = 0.0
        print("无既有重叠数据，offset=0（纯累加）")

    now = datetime.now().replace(microsecond=0)
    records: list[dict[str, Any]] = []
    for obs, run_value in running.items():
        if obs in existing:
            continue
        records.append(build_record(obs, run_value + offset, now))

    first = next(iter(flows))
    last = list(flows)[-1]
    print(
        f"FLOW 序列: {len(flows)} 个交易日（{first:%Y-%m-%d} .. "
        f"{last:%Y-%m-%d}）；既有 NUMBER {len(existing)} 日，"
        f"待回填 {len(records)} 日"
    )

    if args.verbose:
        for r in records[:5]:
            print(f"  {r['observation_time']:%Y-%m-%d} {r['value']:>20,.2f}")
        if len(records) > 5:
            print(f"  ...（共 {len(records)} 条）")

    if args.dry_run:
        print(f"\nDRY RUN: 将新增 {len(records)} 条")
        return 0

    store = CanonicalStoreImpl(str(data_dir))
    stats = store.upsert(records, CanonicalType.NUMBER, _NUMBER_ENTITY)
    print(
        f"\n导入完成: inserted={stats.inserted} updated={stats.updated} "
        f"partitions={stats.rewritten_partitions}"
        + (f" drift={stats.drifted}" if stats.drifted else "")
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
