#!/usr/bin/env python3
"""注册 Coin Metrics 社区层 asset-metrics 数据集到 ChronoForge registry。

范围（source=coinmetrics）:
    - coinmetrics_usdt_supply        USDT 流通量（SplyCur，2014-10-06 起）
    - coinmetrics_usdt_market_cap    USDT 市值（CapMrktCurUSD，2014-10-06 起）
    - coinmetrics_usdc_supply        USDC 流通量（SplyCur，2018-08-03 起）
    - coinmetrics_usdc_market_cap    USDC 市值（CapMrktCurUSD，2018-09-28 起）
    - coinmetrics_btc_addresses_with_balance
                                     BTC 有余额地址数（AdrBalCnt，2009-01-03 起）
    - coinmetrics_btc_active_addresses
                                     BTC 活跃地址数（AdrActCnt，2009-01-03 起）
    全部为 NUMBER 类型日频序列，社区层免费无 key。
    available_from 一律取源端实际最早可回溯日（实测核对，非估值）。

行为:
    - 注册前先 _get_config 预检窗口合法性 + get_dataset 检测是否已注册，
      已存在则跳过（幂等，可重复执行）
    - 已存在但 available_from 与 spec 不一致（元数据漂移）→ 同一 upsert 对齐，
      计入"已更新"
    - 执行后输出汇总统计（新增/已存在/已更新/失败），失败退出码 1
    - 窗口语义由 windows._get_config 的 coinmetrics_ 前缀回退解析
      （全窗口 diff，同 fred_series）

用法:
    python scripts/register_coinmetrics_metrics.py             # 注册
    python scripts/register_coinmetrics_metrics.py --dry-run   # 仅打印计划
    python scripts/register_coinmetrics_metrics.py --verbose   # 输出详情
"""

from __future__ import annotations

import argparse
import sys

from chronoforge.cli._wiring import open_meta
from chronoforge.config.settings import Settings
from chronoforge.pipeline.windows import _get_config
from chronoforge.registry.service import (
    RegistryStoreLike,
    add_dataset,
    bootstrap_defaults,
    get_dataset,
)

# (dataset_id, entity_id, params, available_from, desc)
_SPECS: tuple[tuple[str, str, dict[str, str], str, str], ...] = (
    (
        "coinmetrics_usdt_supply",
        "usdt_supply",
        {"asset": "usdt", "metric": "SplyCur", "series_id": "usdt_supply"},
        "2014-10-06",
        "Coin Metrics USDT 流通量（SplyCur）",
    ),
    (
        "coinmetrics_usdt_market_cap",
        "usdt_market_cap",
        {"asset": "usdt", "metric": "CapMrktCurUSD", "series_id": "usdt_market_cap"},
        "2014-10-06",
        "Coin Metrics USDT 市值（CapMrktCurUSD）",
    ),
    (
        "coinmetrics_usdc_supply",
        "usdc_supply",
        {"asset": "usdc", "metric": "SplyCur", "series_id": "usdc_supply"},
        "2018-08-03",
        "Coin Metrics USDC 流通量（SplyCur）",
    ),
    (
        "coinmetrics_usdc_market_cap",
        "usdc_market_cap",
        {"asset": "usdc", "metric": "CapMrktCurUSD", "series_id": "usdc_market_cap"},
        "2018-09-28",
        "Coin Metrics USDC 市值（CapMrktCurUSD）",
    ),
    (
        "coinmetrics_btc_addresses_with_balance",
        "btc_addresses_with_balance",
        {
            "asset": "btc",
            "metric": "AdrBalCnt",
            "series_id": "btc_addresses_with_balance",
        },
        "2009-01-03",
        "Coin Metrics BTC 有余额地址数（AdrBalCnt）",
    ),
    (
        "coinmetrics_btc_active_addresses",
        "btc_active_addresses",
        {"asset": "btc", "metric": "AdrActCnt", "series_id": "btc_active_addresses"},
        "2009-01-03",
        "Coin Metrics BTC 活跃地址数（AdrActCnt）",
    ),
)

_SOURCE_ID = "coinmetrics"
_CANONICAL_TYPE = "NUMBER"
_FREQUENCY = "1d"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="注册 Coin Metrics asset-metrics 数据集")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="仅打印计划，不写入注册表",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="输出注册详情",
    )
    return parser.parse_args()


def _upsert_dataset(
    meta: RegistryStoreLike,
    *,
    dataset_id: str,
    entity_id: str,
    params: dict[str, str],
    available_from: str,
) -> None:
    """登记/更新单个数据集（add_dataset 为 upsert，status 不受影响）。"""
    add_dataset(
        meta,
        dataset_id=dataset_id,
        source_id=_SOURCE_ID,
        canonical_type=_CANONICAL_TYPE,
        entity_id=entity_id,
        params=params,
        frequency=_FREQUENCY,
        continuity_model="ALWAYS_OPEN",
        revision_supported=False,
        available_from=available_from,
    )


def register(dry_run: bool, verbose: bool) -> int:
    """注册全部 spec，返回退出码（0 成功 / 1 失败）。"""
    settings = Settings.load()

    meta = None
    if not dry_run:
        meta = open_meta(settings)
        n = bootstrap_defaults(meta)
        if verbose:
            print(f"bootstrap_defaults: {n} 个数据源（幂等）")

    print(
        f"\n{'=' * 60}\nCoin Metrics 注册计划: {len(_SPECS)} 个数据集"
        f"\n{'=' * 60}"
    )

    added = existed = updated = failed = 0
    for ds_id, entity_id, params, available_from, desc in _SPECS:
        try:
            # 校验 dataset_id 可被窗口配置解析（否则 backfill 阶段才炸）
            _get_config(ds_id)

            existing = get_dataset(meta, ds_id) if meta is not None else None
            if existing is not None:
                cur_from = existing.get("available_from")
                if cur_from == available_from:
                    existed += 1
                    if verbose:
                        print(f"  [SKIP] {ds_id}（已注册）")
                    continue
                # 元数据漂移（如 available_from 修正）→ 同一 upsert 对齐
                updated += 1
                assert meta is not None
                _upsert_dataset(
                    meta,
                    dataset_id=ds_id,
                    entity_id=entity_id,
                    params=params,
                    available_from=available_from,
                )
                print(
                    f"  [UPDATE] {ds_id}: "
                    f"available_from {cur_from} → {available_from}"
                )
                continue

            if dry_run:
                added += 1
                print(f"  [DRY RUN] 将注册: {ds_id} ({desc})")
                continue

            assert meta is not None
            _upsert_dataset(
                meta,
                dataset_id=ds_id,
                entity_id=entity_id,
                params=params,
                available_from=available_from,
            )
            added += 1
            if verbose:
                print(f"  [OK] {ds_id} ({desc}, available_from={available_from})")
        except Exception as e:
            failed += 1
            print(f"  [FAIL] {ds_id}: {e}", file=sys.stderr)

    print(f"\n{'=' * 60}\n汇总统计\n{'=' * 60}")
    print(
        f"  共 {len(_SPECS):<3} 条 | "
        f"新增 {added:<3} 已存在 {existed:<3} 已更新 {updated:<3} 失败 {failed:<3}"
    )

    if failed == 0 and added > 0 and not dry_run:
        print(
            "\n首跑示例（全历史回填，incremental 无 cursor 无 start 时"
            "为空窗口，首跑必须显式 --start）:\n"
            + "\n".join(
                f"  chronoforge pipeline run --dataset {ds_id} "
                f"--mode incremental --start {available_from}"
                for ds_id, _, _, available_from, _ in _SPECS
            )
        )

    if meta is not None:
        meta.close()
    return 1 if failed > 0 else 0


def main() -> None:
    args = parse_args()
    raise SystemExit(register(args.dry_run, args.verbose))


if __name__ == "__main__":
    main()
