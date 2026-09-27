#!/usr/bin/env python3
"""将 scripts/symbols 目录中的符号清单批量注册到 ChronoForge registry。

清单发现（扫描目录内 *.json，按文件名推断数据源与注册规则）:
    - yahoo_symbols.json                      → source=yahoo : OHLCV 1d（ticker 原样）
    - fred_symbols.json                       → source=fred  : NUMBER（FRED 序列）
    - {exchange}_xstocks.json                 → source=ccxt  : 代币化股票（如 okx_xstocks.json）
    - {exchange}_{spot|futures}_symbols.json  → source=ccxt  : 交易对清单
      （如 binance_spot_symbols.json，由 fetch_top_symbols_from_ccxt_exchange.py 生成）

行为:
    - 注册前先 get_dataset 检测是否已注册，已存在则跳过（幂等，可重复执行）
    - 执行后按文件输出汇总统计（新增/已存在/失败），任一失败退出码 1
    - FRED 的 fred_series_{ID} 窗口语义由 windows._get_config 的 fred_ 前缀
      回退解析（全窗口 diff，同 fred_series）

用法:
    python scripts/register_symbols.py                     # 注册全部清单
    python scripts/register_symbols.py --dry-run           # 仅打印计划，不写入
    python scripts/register_symbols.py --only yahoo fred   # 只处理指定清单/数据源
    python scripts/register_symbols.py --verbose           # 逐条输出详情
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Callable

from chronoforge.cli._wiring import open_meta
from chronoforge.config.settings import Settings
from chronoforge.pipeline.windows import _get_config
from chronoforge.registry.service import (
    add_dataset,
    bootstrap_defaults,
    get_dataset,
)

SYMBOLS_DIR_DEFAULT = Path(__file__).resolve().parent / "symbols"

# FRED 文件 freq 字段 → registry frequency 值
_FREQ_MAP = {
    "daily": "1d",
    "weekly": "1W",
    "monthly": "1M",
    "quarterly": "1Q",
    "annual": "1Y",
}


def build_yahoo_spec(key: str, entry: dict[str, Any]) -> dict[str, Any]:
    """yahoo_symbols.json 条目 → yahoo OHLCV 1d 数据集。"""
    ticker = entry["ticker"]
    return {
        "dataset_id": f"YAHOO:{ticker}:OHLCV:1d",
        "source_id": "yahoo",
        "canonical_type": "OHLCV",
        "entity_id": ticker,
        "params": {"symbol": ticker, "interval": "1d"},
        "frequency": "1d",
        "desc": entry.get("desc", key),
    }


def build_fred_spec(key: str, entry: dict[str, Any]) -> dict[str, Any]:
    """fred_symbols.json 条目 → fred NUMBER 数据集（fred_series_{ID}）。"""
    series_id = entry["id"]
    return {
        "dataset_id": f"fred_series_{series_id}",
        "source_id": "fred",
        "canonical_type": "NUMBER",
        "entity_id": series_id,
        "params": {"series_id": series_id},
        "frequency": _FREQ_MAP.get(entry.get("freq", "daily"), "1d"),
        "desc": entry.get("desc", key),
    }


def make_ccxt_ohlcv_spec(exchange_id: str) -> Callable[[str, dict[str, Any]], dict[str, Any]]:
    """生成 ccxt 交易对/代币化股票清单的 spec 构造函数。

    ticker 为 ccxt unified symbol（BTC/USDT、XAAPL/USDT）；dataset_id/entity_id
    用紧凑大写（BTCUSDT），params.symbol 保留原生格式（OKX 要求 BASE/QUOTE）。
    """
    def build(key: str, entry: dict[str, Any]) -> dict[str, Any]:
        ticker = entry["ticker"]
        compact = ticker.replace("/", "").upper()
        return {
            "dataset_id": f"CCXT:{compact}:OHLCV:1d",
            "source_id": "ccxt",
            "canonical_type": "OHLCV",
            "entity_id": compact,
            "params": {"symbol": ticker, "interval": "1d", "exchange": exchange_id},
            "frequency": "1d",
            "desc": entry.get("desc", key),
        }
    return build


# 无需解析文件名即可确定的静态清单
_STATIC_FILES: dict[str, tuple[str, Callable[[str, dict[str, Any]], dict[str, Any]]]] = {
    "yahoo_symbols.json": ("yahoo", build_yahoo_spec),
    "fred_symbols.json": ("fred", build_fred_spec),
}

# {exchange}_xstocks.json / {exchange}_{spot|futures}_symbols.json 模式
_EXCHANGE_PATTERN = re.compile(
    r"^(?P<exchange>[a-z0-9]+?)_(?:xstocks\.json|(?:spot|futures)_symbols\.json)$"
)


def discover_manifests(
    symbols_dir: Path,
) -> dict[str, tuple[str, Callable[[str, dict[str, Any]], dict[str, Any]]]]:
    """扫描目录自主发现清单文件，返回 {文件名: (source_id, spec 构造函数)}。

    未识别的 *.json 打印警告后跳过（避免静默遗漏）。
    """
    manifests: dict[str, tuple[str, Callable[[str, dict[str, Any]], dict[str, Any]]]] = {}
    for path in sorted(symbols_dir.glob("*.json")):
        name = path.name
        if name in _STATIC_FILES:
            manifests[name] = _STATIC_FILES[name]
            continue
        m = _EXCHANGE_PATTERN.match(name)
        if m:
            manifests[name] = ("ccxt", make_ccxt_ohlcv_spec(m.group("exchange")))
        else:
            print(f"警告: 未识别的清单文件，跳过 {name}", file=sys.stderr)
    return manifests


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="批量注册 scripts/symbols 清单到 ChronoForge registry"
    )
    parser.add_argument(
        "--symbols-dir", type=Path, default=SYMBOLS_DIR_DEFAULT,
        help=f"符号清单目录（默认 {SYMBOLS_DIR_DEFAULT}）",
    )
    parser.add_argument(
        "--only", type=str, nargs="+", default=None,
        metavar="FILE",
        help="只处理指定清单文件（如 --only yahoo_symbols.json fred_symbols.json）",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="仅打印计划，不写入注册表",
    )
    parser.add_argument(
        "--verbose", action="store_true", help="逐条输出注册详情",
    )
    return parser.parse_args()


def register_all(
    symbols_dir: Path,
    only: list[str] | None,
    dry_run: bool,
    verbose: bool,
) -> int:
    """注册全部清单，返回退出码（0 成功 / 1 有失败）。"""
    meta = None
    if not dry_run:
        settings = Settings.load()
        meta = open_meta(settings)
        n = bootstrap_defaults(meta)
        if verbose:
            print(f"bootstrap_defaults: {n} 个数据源（幂等）")

    # 汇总统计：file -> {total, added, existed, failed, failures: [(id, err)]}
    stats: dict[str, dict[str, Any]] = {}
    manifests = discover_manifests(symbols_dir)
    planned_files = [
        name for name in manifests
        if only is None or name in only or manifests[name][0] in only
    ]

    for file_name in planned_files:
        path = symbols_dir / file_name
        source_id, builder = manifests[file_name]
        if not path.exists():
            print(f"警告: 清单不存在，跳过 {path}", file=sys.stderr)
            continue
        entries: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))

        stat = stats[file_name] = {
            "source_id": source_id, "total": 0,
            "added": 0, "existed": 0, "failed": 0, "failures": [],
        }
        print(f"\n{'=' * 60}\n{file_name} (source={source_id}): {len(entries)} 个符号\n{'=' * 60}")

        for key, entry in entries.items():
            spec = builder(key, entry)
            ds_id = spec["dataset_id"]
            stat["total"] += 1

            try:
                # 校验 dataset_id 可被窗口配置解析（否则 backfill 阶段才炸）
                _get_config(ds_id)

                if meta is not None and get_dataset(meta, ds_id) is not None:
                    stat["existed"] += 1
                    if verbose:
                        print(f"  [SKIP] {ds_id}（已注册）")
                    continue

                if dry_run:
                    stat["added"] += 1
                    print(f"  [DRY RUN] 将注册: {ds_id} ({spec['desc']})")
                    continue

                add_dataset(
                    meta,
                    dataset_id=ds_id,
                    source_id=spec["source_id"],
                    canonical_type=spec["canonical_type"],
                    entity_id=spec["entity_id"],
                    params=spec["params"],
                    frequency=spec["frequency"],
                    continuity_model="ALWAYS_OPEN",
                    revision_supported=False,
                )
                stat["added"] += 1
                if verbose:
                    print(f"  [OK] {ds_id} ({spec['desc']})")
            except Exception as e:
                stat["failed"] += 1
                stat["failures"].append((ds_id, str(e)))
                print(f"  [FAIL] {ds_id}: {e}", file=sys.stderr)

    # 汇总统计
    print(f"\n{'=' * 60}\n汇总统计\n{'=' * 60}")
    grand = {"total": 0, "added": 0, "existed": 0, "failed": 0}
    for file_name, s in stats.items():
        print(
            f"  {file_name:<22} (source={s['source_id']:<6})"
            f" 共 {s['total']:>3} 条 | 新增 {s['added']:<3}"
            f" 已存在 {s['existed']:<3} 失败 {s['failed']:<3}"
        )
        for k in grand:
            grand[k] += s[k]
    print(f"  {'-' * 66}")
    print(
        f"  {'总计':<24}          共 {grand['total']:>3} 条"
        f" | 新增 {grand['added']:<3} 已存在 {grand['existed']:<3}"
        f" 失败 {grand['failed']:<3}"
    )

    for s in stats.values():
        for ds_id, err in s["failures"]:
            print(f"  失败明细: {ds_id}: {err}", file=sys.stderr)

    if meta is not None:
        meta.close()
    return 1 if grand["failed"] > 0 else 0


def main() -> None:
    args = parse_args()
    raise SystemExit(
        register_all(args.symbols_dir, args.only, args.dry_run, args.verbose)
    )


if __name__ == "__main__":
    main()
