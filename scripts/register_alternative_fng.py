#!/usr/bin/env python3
"""注册 Alternative.me Crypto Fear & Greed Index 数据集到 ChronoForge registry。

范围（source=alternative）:
    - alternative_fng_btc：BTC 口径恐慌贪婪指数日频历史（2018-02-01 起），
      NUMBER 类型；GET /fng/?limit=0 单请求全历史，免费无 key

行为:
    - 注册前先 _get_config 预检窗口合法性 + get_dataset 检测是否已注册，
      已存在则跳过（幂等，可重复执行）
    - 执行后输出汇总统计（新增/已存在/失败），失败退出码 1
    - 窗口语义由 windows._get_config 的 alternative_ 前缀回退解析
      （全窗口 diff，同 fred_series）

用法:
    python scripts/register_alternative_fng.py             # 注册
    python scripts/register_alternative_fng.py --dry-run   # 仅打印计划
    python scripts/register_alternative_fng.py --verbose   # 输出详情
"""

from __future__ import annotations

import argparse
import sys

from chronoforge.cli._wiring import open_meta
from chronoforge.config.settings import Settings
from chronoforge.pipeline.windows import _get_config
from chronoforge.registry.service import (
    add_dataset,
    bootstrap_defaults,
    get_dataset,
)

_AVAILABLE_FROM = "2018-02-01"

_SPEC = {
    "dataset_id": "alternative_fng_btc",
    "source_id": "alternative",
    "canonical_type": "NUMBER",
    "entity_id": "fng_btc",
    "params": {"metric": "fear_greed_index", "symbol": "BTC"},
    "frequency": "1d",
    "desc": "Alternative.me Crypto Fear & Greed Index (BTC)",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="注册 Alternative.me F&G 指数数据集")
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


def register(dry_run: bool, verbose: bool) -> int:
    """注册 spec，返回退出码（0 成功 / 1 失败）。"""
    settings = Settings.load()
    ds_id = _SPEC["dataset_id"]

    meta = None
    if not dry_run:
        meta = open_meta(settings)
        n = bootstrap_defaults(meta)
        if verbose:
            print(f"bootstrap_defaults: {n} 个数据源（幂等）")

    print(
        f"\n{'=' * 60}\nAlternative F&G 注册计划: 1 个数据集"
        f"（available_from={_AVAILABLE_FROM}）\n{'=' * 60}"
    )

    added = existed = failed = 0
    try:
        # 校验 dataset_id 可被窗口配置解析（否则 backfill 阶段才炸）
        _get_config(ds_id)

        if meta is not None and get_dataset(meta, ds_id) is not None:
            existed = 1
            if verbose:
                print(f"  [SKIP] {ds_id}（已注册）")
        elif dry_run:
            added = 1
            print(f"  [DRY RUN] 将注册: {ds_id} ({_SPEC['desc']})")
        else:
            assert meta is not None
            add_dataset(
                meta,
                dataset_id=ds_id,
                source_id=_SPEC["source_id"],
                canonical_type=_SPEC["canonical_type"],
                entity_id=_SPEC["entity_id"],
                params=_SPEC["params"],
                frequency=_SPEC["frequency"],
                continuity_model="ALWAYS_OPEN",
                revision_supported=False,
                available_from=_AVAILABLE_FROM,
            )
            added = 1
            if verbose:
                print(f"  [OK] {ds_id} ({_SPEC['desc']})")
    except Exception as e:
        failed = 1
        print(f"  [FAIL] {ds_id}: {e}", file=sys.stderr)

    print(f"\n{'=' * 60}\n汇总统计\n{'=' * 60}")
    print(f"  共   1 条 | 新增 {added:<3} 已存在 {existed:<3} 失败 {failed:<3}")

    if failed == 0 and added > 0 and not dry_run:
        print(
            "\n首跑示例（全历史回填，incremental 无 cursor 无 start 时"
            "为空窗口，首跑必须显式 --start）:\n"
            f"  chronoforge pipeline run --dataset {ds_id} "
            f"--mode incremental --start {_AVAILABLE_FROM}"
        )

    if meta is not None:
        meta.close()
    return 1 if failed > 0 else 0


def main() -> None:
    args = parse_args()
    raise SystemExit(register(args.dry_run, args.verbose))


if __name__ == "__main__":
    main()
