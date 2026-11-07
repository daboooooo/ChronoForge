#!/usr/bin/env python3
"""注册 NY Fed Markets API 数据集（SRF 用量 + SOMA 持仓）。

覆盖：
- SRF（Standing Repo Facility）每日用量（USD，隔夜回购操作接受金额汇总）
- SOMA 持仓 9 个分项（USD 原值，周频，asOfDate=周三）

命名与参数风格对齐既有 registry 约定（underscore dataset_id + 前缀
`nyfed_`，_get_config 按前缀分配全窗口窗口语义）。
available_from 取 API 实测最早日期，不估值。

用法：
    uv run python scripts/register_nyfed_datasets.py
    uv run python scripts/register_nyfed_datasets.py --dry-run
"""

from __future__ import annotations

import argparse
import sys
from typing import Any

from chronoforge.cli._wiring import open_meta
from chronoforge.config.settings import Settings
from chronoforge.registry.service import add_dataset, bootstrap_defaults, get_dataset

# (dataset_id, canonical_type, entity_id, params, frequency, available_from, continuity)
_SPECS: tuple[tuple[str, str, str, dict[str, Any], str, str | None, str], ...] = (
    (
        "nyfed_repo_srf_usage",
        "NUMBER",
        "srf_usage",
        {"endpoint": "rp_results", "series": "srf_usage"},
        "1d",
        "2021-07-29",
        "TRADING_CALENDAR",
    ),
    (
        "nyfed_soma_holdings",
        "NUMBER",
        "soma_holdings",
        {"endpoint": "soma_summary", "series": "soma_holdings"},
        "1W",
        "2003-07-09",
        "RELEASE_SCHEDULE",
    ),
)


def main() -> int:
    parser = argparse.ArgumentParser(description="注册 NY Fed 数据集")
    parser.add_argument("--dry-run", action="store_true", help="只打印计划")
    args = parser.parse_args()

    settings = Settings.load()
    meta = open_meta(settings)
    if not args.dry_run:
        bootstrap_defaults(meta)

    added = 0
    updated = 0
    failed = 0
    for dataset_id, ctype, entity, params, frequency, available_from, continuity in _SPECS:
        existing = get_dataset(meta, dataset_id)
        try:
            if args.dry_run:
                action = "exists" if existing else "add"
            else:
                add_dataset(
                    meta,
                    dataset_id=dataset_id,
                    source_id="nyfed",
                    canonical_type=ctype,
                    entity_id=entity,
                    params=params,
                    frequency=frequency,
                    continuity_model=continuity,
                    revision_supported=False,
                    available_from=available_from,
                )
                meta.connection.commit()
                action = "updated" if existing else "added"
        except Exception as exc:  # noqa: BLE001
            print(f"FAILED {dataset_id}: {exc}", file=sys.stderr)
            failed += 1
            continue
        if action == "added":
            added += 1
        else:
            updated += 1
        print(f"{action:7s} {dataset_id}")

    print(f"\n新增 {added} | 已存在/更新 {updated} | 失败 {failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
