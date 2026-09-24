#!/usr/bin/env python3
"""从 JSON 文件批量注入交易对到 ChronoForge 注册表。

用法:
    python scripts/bulk_add_from_json.py --file trading_pairs.json

JSON 文件格式:
{
    "datasets": [
        {
            "source": "binance_spot",
            "symbols": ["BTCUSDT", "ETHUSDT", "ADAUSDT"],
            "interval": "1m",
            "canonical_type": "OHLCV",
            "continuity_model": "ALWAYS_OPEN"
        },
        {
            "source": "binance_futures",
            "symbols": ["BTCUSDT", "ETHUSDT"],
            "interval": "1m",
            "canonical_type": "OHLCV",
            "continuity_model": "ALWAYS_OPEN"
        }
    ]
}

字段说明:
    - source: 数据源 ID（binance_spot, binance_futures, ccxt, deribit, yahoo）
    - symbols: 原始交易对符号列表
    - interval: K线周期（1m, 5m, 15m, 30m, 1h, 4h, 1d 等）
    - canonical_type: 规范类型（OHLCV, TRADE, TICKER, FUNDING, OPEN_INTEREST）
    - continuity_model: 连续性模型（可选，默认 ALWAYS_OPEN）
    - frequency: 数据频率（可选，与 interval 一致）
    - revision_supported: 是否支持修订（可选，默认 false）
    - available_from: 起始时间 ISO 字符串（可选）
    - available_to: 结束时间 ISO 字符串（可选）
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from chronoforge.cli._wiring import open_meta
from chronoforge.config.settings import Settings
from chronoforge.registry.service import add_dataset


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="从 JSON 文件批量注入交易对到 ChronoForge 注册表"
    )
    parser.add_argument(
        "--file",
        type=str,
        required=True,
        help="JSON 文件路径",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="仅打印将要注册的 dataset，不实际写入",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="输出详细日志",
    )
    return parser.parse_args()


def load_json(file_path: str) -> dict[str, Any]:
    """加载并验证 JSON 文件。"""
    path = Path(file_path)
    if not path.exists():
        print(f"错误: 文件不存在: {file_path}", file=sys.stderr)
        sys.exit(1)

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"错误: JSON 解析失败: {e}", file=sys.stderr)
        sys.exit(1)

    if "datasets" not in data:
        print("错误: JSON 文件缺少 'datasets' 键", file=sys.stderr)
        sys.exit(1)

    return data


def validate_dataset_entry(entry: dict[str, Any]) -> None:
    """验证单个 dataset 条目。"""
    required_keys = ["source", "symbols", "interval"]
    for key in required_keys:
        if key not in entry:
            raise ValueError(f"缺少必填字段: {key}")

    valid_sources = {"binance_spot", "binance_futures", "ccxt", "deribit", "yahoo"}
    if entry["source"] not in valid_sources:
        raise ValueError(
            f"无效的 source: {entry['source']}。"
            f"支持的 source: {', '.join(sorted(valid_sources))}"
        )

    valid_types = {"OHLCV", "TRADE", "TICKER", "FUNDING", "OPEN_INTEREST"}
    if entry.get("canonical_type", "OHLCV") not in valid_types:
        raise ValueError(
            f"无效的 canonical_type: {entry.get('canonical_type')}。"
            f"支持的类型: {', '.join(sorted(valid_types))}"
        )

    valid_continuity = {"ALWAYS_OPEN", "TRADING_CALENDAR", "EVENT_BASED", "RELEASE_SCHEDULE"}
    cm = entry.get("continuity_model", "ALWAYS_OPEN")
    if cm not in valid_continuity:
        raise ValueError(
            f"无效的 continuity_model: {cm}。"
            f"支持的值: {', '.join(sorted(valid_continuity))}"
        )


def generate_dataset_id(source: str, symbol: str, canonical_type: str, interval: str) -> str:
    """根据数据源、交易对、类型和周期生成 dataset_id。"""
    # 规范化 symbol: 移除可能的连字符，统一为大写
    symbol_upper = symbol.upper().replace("-", "")
    return f"{source.upper()}:{symbol_upper}:{canonical_type}:{interval}"


def bulk_add_from_json(file_path: str, dry_run: bool = False, verbose: bool = False) -> None:
    """从 JSON 文件批量注入交易对。"""
    data = load_json(file_path)
    settings = Settings.load()
    meta = open_meta(settings)

    entries = data["datasets"]
    total_added = 0
    total_skipped = 0
    total_failed = 0

    print(f"开始处理 {len(entries)} 个数据源配置...")
    print(f"{'[DRY RUN] ' if dry_run else ''}")

    for entry_idx, entry in enumerate(entries, 1):
        try:
            validate_dataset_entry(entry)
        except ValueError as e:
            print(f"配置 {entry_idx}: 验证失败: {e}", file=sys.stderr)
            total_skipped += len(entry.get("symbols", []))
            continue

        source = entry["source"]
        symbols = entry["symbols"]
        interval = entry["interval"]
        canonical_type = entry.get("canonical_type", "OHLCV")
        continuity_model = entry.get("continuity_model", "ALWAYS_OPEN")
        frequency = entry.get("frequency", interval)
        revision_supported = entry.get("revision_supported", False)
        available_from = entry.get("available_from")
        available_to = entry.get("available_to")

        if not isinstance(symbols, list) or len(symbols) == 0:
            print(f"配置 {entry_idx} ({source}): symbols 为空或不是列表，跳过", file=sys.stderr)
            continue

        for symbol in symbols:
            try:
                dataset_id = generate_dataset_id(
                    source, symbol, canonical_type, interval
                )

                if dry_run:
                    print(f"  [DRY RUN] 将注册: {dataset_id}")
                    total_added += 1
                    continue

                params: dict[str, Any] = {
                    "symbol": symbol.upper().replace("-", ""),
                    "interval": interval,
                }

                # 对于非 OHLCV 类型，添加额外参数
                if canonical_type == "FUNDING":
                    params["data_type"] = "funding"
                elif canonical_type == "OPEN_INTEREST":
                    params["data_type"] = "open_interest"
                elif canonical_type == "TRADE":
                    params["data_type"] = "aggregate"
                elif canonical_type == "TICKER":
                    params["data_type"] = "ticker"

                add_dataset(
                    meta,
                    dataset_id=dataset_id,
                    source_id=source,
                    canonical_type=canonical_type,
                    entity_id=symbol.upper().replace("-", ""),
                    params=params,
                    frequency=frequency,
                    continuity_model=continuity_model,
                    revision_supported=revision_supported,
                    available_from=available_from,
                    available_to=available_to,
                )
                total_added += 1

                if verbose:
                    print(f"  已注册: {dataset_id}")

            except Exception as e:
                total_failed += 1
                print(f"  注册失败 {symbol}: {e}", file=sys.stderr)

    print(f"\n完成!")
    print(f"  成功注册: {total_added}")
    if total_skipped > 0:
        print(f"  跳过: {total_skipped}")
    if total_failed > 0:
        print(f"  失败: {total_failed}")


def main() -> None:
    args = parse_args()
    bulk_add_from_json(
        file_path=args.file,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    main()
