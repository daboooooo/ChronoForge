#!/usr/bin/env python3
"""注册美国财政部 Fiscal Data 数据集（TGA 子系统 Phase 1）。

覆盖：
- DTS Table I/II/IIIA/IIIB/IV/V
- Treasury 拍卖结果/未来公告

所有 dataset 使用 underscore 命名（与现有 registry/_get_config 前缀约定一致）。
available_from 取 Phase 0 实测值（docs/tga/data_coverage.json），不估值。

用法：
    uv run python scripts/register_treasury_datasets.py
    uv run python scripts/register_treasury_datasets.py --dry-run
"""

from __future__ import annotations

import argparse
import sys
from typing import Any

from chronoforge.cli._wiring import open_meta
from chronoforge.config.settings import Settings
from chronoforge.registry.service import add_dataset, bootstrap_defaults, get_dataset

# (dataset_id, canonical_type, entity_id, params, available_from, continuity_model)
_SPECS: tuple[tuple[str, str, str, dict[str, Any], str | None, str], ...] = (
    (
        "treasury_fiscal_dts_operating_cash_balance",
        "TREASURY_ACCOUNT_BALANCE",
        "TGA",
        {"table": "operating_cash_balance"},
        "2005-10-03",
        "TRADING_CALENDAR",
    ),
    (
        "treasury_fiscal_dts_deposits_withdrawals",
        "TREASURY_CASH_FLOW",
        "TGA",
        {"table": "deposits_withdrawals"},
        "2005-10-03",
        "TRADING_CALENDAR",
    ),
    (
        "treasury_fiscal_dts_public_debt_transactions",
        "TREASURY_DEBT_TRANSACTION",
        "TGA",
        {"table": "public_debt_transactions"},
        "2005-10-03",
        "TRADING_CALENDAR",
    ),
    (
        "treasury_fiscal_dts_public_debt_cash_basis",
        "TREASURY_DEBT_TRANSACTION",
        "TGA",
        {"table": "public_debt_cash_basis"},
        "2005-10-03",
        "TRADING_CALENDAR",
    ),
    (
        "treasury_fiscal_dts_inter_agency_tax_transfers",
        "TREASURY_CASH_FLOW",
        "TGA",
        {"table": "inter_agency_tax_transfers"},
        # Phase 0 实测：Table IV 仅 2023-02-14 起，不能伪造更早历史。
        "2023-02-14",
        "TRADING_CALENDAR",
    ),
    (
        "treasury_fiscal_dts_income_tax_refunds",
        "TREASURY_CASH_FLOW",
        "TGA",
        {"table": "income_tax_refunds"},
        "2005-10-03",
        "TRADING_CALENDAR",
    ),
    (
        "treasury_fiscal_auction_results",
        "TREASURY_AUCTION",
        "US_TREASURY",
        {"table": "auctions_query"},
        "1979-11-15",
        "EVENT_BASED",
    ),
    (
        "treasury_fiscal_auction_upcoming",
        "TREASURY_AUCTION",
        "US_TREASURY",
        {"table": "upcoming_auctions"},
        "2024-03-08",
        "EVENT_BASED",
    ),
    # Phase 4：季度再融资公告（借款估计 + 现金余额假设；HTML 新闻稿）
    (
        "treasury_refunding_announcements",
        "TREASURY_ANNOUNCEMENT",
        "quarterly_refunding",
        {"document": "quarterly_refunding"},
        "2026-11-02",
        "EVENT_BASED",
    ),
)


def main() -> int:
    parser = argparse.ArgumentParser(description="注册 Treasury Fiscal Data 数据集")
    parser.add_argument("--dry-run", action="store_true", help="只打印计划")
    args = parser.parse_args()

    settings = Settings.load()
    meta = open_meta(settings)
    if not args.dry_run:
        bootstrap_defaults(meta)

    added = 0
    updated = 0
    failed = 0
    for dataset_id, ctype, entity, params, available_from, continuity in _SPECS:
        existing = get_dataset(meta, dataset_id)
        source_id = "treasury_refunding" if dataset_id.startswith("treasury_refunding_") else "treasury_fiscal"
        frequency = "1Q" if dataset_id.startswith("treasury_refunding_") else "1d"
        try:
            if args.dry_run:
                action = "exists" if existing else "add"
            else:
                add_dataset(
                    meta,
                    dataset_id=dataset_id,
                    source_id=source_id,
                    canonical_type=ctype,
                    entity_id=entity,
                    params=params,
                    frequency=frequency,
                    continuity_model=continuity,
                    # 拍卖以 auction_key（含 data_status）区分 ANNOUNCED/RESULT，
                    # 不属于 revision 类；dataset 状态不应因此进入 REVISION_PENDING。
                    # TREASURY_ANNOUNCEMENT 属 revision 类（自然键含 revision_time）。
                    revision_supported=ctype == "TREASURY_ANNOUNCEMENT",
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
        elif action == "updated" or action == "exists":
            updated += 1
        print(f"{action:7s} {dataset_id}")

    print(f"\n新增 {added} | 已存在/更新 {updated} | 失败 {failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
