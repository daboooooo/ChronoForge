#!/usr/bin/env python3
"""批量注册交易对 OHLCV 数据并回填历史数据。

调用 bulk_add_from_json 注册交易对的 1h/4h/1d OHLCV 数据（支持 binance_spot、
binance_futures、ccxt-okx 三个数据源），然后通过 pipeline backfill 从 2021-01-01
回填到 2026-09-22。

用法:
    # 使用默认交易对列表（BTCUSDT, ETHUSDT, BNBUSDT）
    python scripts/bulk_register_and_backfill.py

    # 指定交易对列表（JSON 文件，每行一个符号）
    python scripts/bulk_register_and_backfill.py --pairs my_pairs.txt

    # 仅注册，不回填
    python scripts/bulk_register_and_backfill.py --register-only

    # 仅回填已注册的 dataset，不注册新 dataset
    python scripts/bulk_register_and_backfill.py --backfill-only

    # 指定回填时间范围
    python scripts/bulk_register_and_backfill.py --start 2022-01-01 --end 2026-09-22

    # 限制只处理 binance_spot（不处理 futures 和 ccxt）
    python scripts/bulk_register_and_backfill.py --sources binance_spot

依赖:
    - Binance: 无需 API Key（公开接口）
    - OKX: 通过 ccxt 库，无需 API Key
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from typing import Any

from chronoforge.cli._wiring import build_connector, open_meta
from chronoforge.config.settings import Settings
from chronoforge.pipeline.runner import PipelineRunner, RunContext
from chronoforge.pipeline.windows import AcquisitionJob
from chronoforge.registry.service import add_dataset


# 默认交易对列表
DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]

# 需要回放的 K 线周期
INTERVALS = ["1h", "4h", "1d"]

# 数据源配置
SOURCES = [
    {"source_id": "binance_spot", "window_key": "binance_spot_klines"},
    {"source_id": "binance_futures", "window_key": "binance_futures_klines"},
    {"source_id": "ccxt", "window_key": "ccxt_ohlcv"},
]

# 回填时间范围
DEFAULT_START = datetime(2021, 1, 1)
DEFAULT_END = datetime(2026, 9, 22)

# ccxt 交易所原生符号的常见计价资产后缀（BTCUSDT → BTC/USDT）
_CCXT_QUOTE_SUFFIXES = ("USDT", "USDC", "TUSD", "BUSD", "BTC", "ETH", "BNB")


def to_ccxt_symbol(symbol: str) -> str:
    """将紧凑符号转为 ccxt 交易所原生格式（BTCUSDT → BTC/USDT）。

    ccxt（OKX 等）要求 BASE/QUOTE 形式，紧凑形式会 BadSymbol。
    """
    for quote in _CCXT_QUOTE_SUFFIXES:
        if symbol.endswith(quote) and len(symbol) > len(quote):
            return f"{symbol[: -len(quote)]}/{quote}"
    return symbol


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="批量注册交易对 OHLCV 数据并回填历史数据"
    )
    parser.add_argument(
        "--pairs",
        type=str,
        help="交易对列表文件路径（每行一个符号，JSON 数组格式）",
    )
    parser.add_argument(
        "--sources",
        type=str,
        nargs="+",
        default=["binance_spot", "binance_futures", "ccxt"],
        help="要处理的数据源（默认全部）",
    )
    parser.add_argument(
        "--start",
        type=str,
        help=f"回填起始时间（ISO 格式，默认 {DEFAULT_START.strftime('%Y-%m-%d')}）",
    )
    parser.add_argument(
        "--end",
        type=str,
        help=f"回填结束时间（ISO 格式，默认 {DEFAULT_END.strftime('%Y-%m-%d')}）",
    )
    parser.add_argument(
        "--register-only",
        action="store_true",
        help="仅注册 dataset，不回放",
    )
    parser.add_argument(
        "--backfill-only",
        action="store_true",
        help="仅回放已注册的 dataset，不注册",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="仅打印计划，不执行",
    )
    parser.add_argument(
        "--window-seconds",
        type=int,
        default=86400,
        help="分窗口大小（秒，默认 86400 = 24h）",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="输出详细日志",
    )
    return parser.parse_args()


def load_symbols(pairs_file: str | None) -> list[str]:
    """加载交易对符号列表。"""
    if pairs_file:
        with open(pairs_file, "r", encoding="utf-8") as f:
            content = f.read().strip()
            # 尝试 JSON 数组格式
            try:
                symbols = json.loads(content)
                if isinstance(symbols, list):
                    return [s.upper().replace("-", "") for s in symbols]
            except json.JSONDecodeError:
                pass
            # 逐行读取格式
            return [
                line.strip().upper().replace("-", "")
                for line in content.splitlines()
                if line.strip()
            ]
    return [s.upper() for s in DEFAULT_SYMBOLS]


def register_datasets(
    symbols: list[str],
    sources: list[dict[str, str]],
    dry_run: bool = False,
) -> list[tuple[str, str, dict[str, Any]]]:
    """注册交易对 OHLCV dataset。

    Returns:
        [(dataset_id, source_id, params), ...]
    """
    registered = []
    total = len(symbols) * len(sources) * len(INTERVALS)
    count = 0

    meta = None
    if not dry_run:
        settings = Settings.load()
        meta = open_meta(settings, repair=True)

    for src in sources:
        source_id = src["source_id"]
        for symbol in symbols:
            for interval in INTERVALS:
                count += 1
                symbol_clean = symbol.upper().replace("-", "")
                dataset_id = f"{source_id.upper()}:{symbol_clean}:OHLCV:{interval}"

                params: dict[str, Any] = {
                    "symbol": symbol_clean,
                    "interval": interval,
                }

                # ccxt 需要交易所原生符号格式（BTC/USDT）+ exchange 参数
                if source_id == "ccxt":
                    params["symbol"] = to_ccxt_symbol(symbol_clean)
                    params["exchange"] = "okx"

                if dry_run:
                    print(f"[{count}/{total}] [DRY RUN] 将注册: {dataset_id}")
                    registered.append((dataset_id, source_id, params))
                    continue

                print(f"[{count}/{total}] 注册: {dataset_id}")

                try:
                    add_dataset(
                        meta,
                        dataset_id=dataset_id,
                        source_id=source_id,
                        canonical_type="OHLCV",
                        entity_id=symbol_clean,
                        params=params,
                        frequency=interval,
                        continuity_model="ALWAYS_OPEN",
                        revision_supported=False,
                    )
                    registered.append((dataset_id, source_id, params))
                except Exception as e:
                    print(f"  注册失败 {dataset_id}: {e}", file=sys.stderr)

    if meta is not None:
        meta.close()
    print(f"\n注册完成: {len(registered)}/{total}")
    return registered


def load_registered_datasets(
    meta: Any, source_ids: list[str]
) -> list[tuple[str, str, dict[str, Any]]]:
    """从注册表加载已有的 OHLCV dataset（--backfill-only 模式用）。"""
    from chronoforge.registry.service import list_datasets

    results = []
    for row in list_datasets(meta, enabled_only=True):
        if row.get("source_id") not in source_ids:
            continue
        if row.get("canonical_type") != "OHLCV":
            continue
        params_raw = row.get("params_json") or "{}"
        try:
            params = json.loads(params_raw)
        except json.JSONDecodeError:
            continue
        if not isinstance(params, dict) or params.get("interval") not in INTERVALS:
            continue
        results.append(
            (str(row["dataset_id"]), str(row["source_id"]), params)
        )
    return results


def run_backfill(
    registered: list[tuple[str, str, dict[str, Any]]],
    start: datetime,
    end: datetime,
    window_seconds: int,
    dry_run: bool = False,
    verbose: bool = False,
) -> None:
    """对已注册的 dataset 执行 pipeline backfill。"""
    settings = Settings.load()
    meta = open_meta(settings, repair=True)

    total = len(registered)
    success_count = 0
    fail_count = 0

    for idx, (dataset_id, source_id, params) in enumerate(registered, 1):
        print(
            f"\n[{idx}/{total}] 回填: {dataset_id}"
            f" ({start.strftime('%Y-%m-%d')} ~ {end.strftime('%Y-%m-%d')})"
        )

        if dry_run:
            print(
                f"  [DRY RUN] 将执行: dataset={dataset_id} mode=backfill"
                f" start={start.isoformat()} end={end.isoformat()}"
            )
            success_count += 1
            continue

        try:
            # 构建 connector
            connector = build_connector(source_id, settings, params)

            # 构建 runner
            raw_store = None
            canonical_store = None

            def ctx_factory(job: AcquisitionJob) -> RunContext:
                nonlocal raw_store, canonical_store
                if raw_store is None:
                    from chronoforge.storage.raw import RawStore
                    from chronoforge.storage.canonical import CanonicalStoreImpl
                    raw_store = RawStore(str(settings.data_dir))
                    canonical_store = CanonicalStoreImpl(str(settings.data_dir))
                return RunContext(
                    run_id="",
                    ingest_batch_id="",
                    source_id=source_id,
                    dataset_id=job.dataset_id,
                    connector=connector,
                    raw_store=raw_store,
                    canonical_store=canonical_store,
                    meta=meta,
                    settings=settings,
                )

            runner = PipelineRunner(ctx_factory)

            # 构建 job
            job = AcquisitionJob(
                dataset_id=dataset_id,
                start=start,
                end=end,
                mode="backfill",
                priority=0,
                params=params,
            )

            # 执行回填（分窗口）
            if window_seconds is not None and window_seconds > 0:
                results = runner.run_windowed(job, window_seconds=window_seconds)
            else:
                results = [runner.run(job)]

            for result in results:
                print(f"  run={result.run_id} status={result.status} "
                      f"output={result.output_count} errors={result.error_count}")

            # 空结果 = checkpoint 已覆盖全部范围（无窗口可跑）→ 视为成功
            if not results or any(
                r.status in ("SUCCESS", "PARTIAL_SUCCESS") for r in results
            ):
                success_count += 1
            else:
                fail_count += 1

            connector.close()
            if raw_store:
                raw_store.close()

        except Exception as e:
            fail_count += 1
            print(f"  回填失败: {e}", file=sys.stderr)
            if verbose:
                import traceback
                traceback.print_exc()

    print(f"\n{'='*60}")
    print("回填完成:")
    print(f"  成功: {success_count}")
    print(f"  失败: {fail_count}")
    print(f"  总计: {total}")


def main() -> None:
    args = parse_args()

    if args.register_only and args.backfill_only:
        print("错误: --register-only 与 --backfill-only 互斥", file=sys.stderr)
        sys.exit(1)

    # 解析时间范围
    start_str = args.start or DEFAULT_START.strftime("%Y-%m-%d")
    end_str = args.end or DEFAULT_END.strftime("%Y-%m-%d")
    start = datetime.fromisoformat(start_str)
    end = datetime.fromisoformat(end_str)

    if start >= end:
        print("错误: --start 必须早于 --end", file=sys.stderr)
        sys.exit(1)

    # 过滤数据源
    source_configs = [
        s for s in SOURCES
        if s["source_id"] in args.sources
    ]
    if not source_configs:
        print(f"错误: 不支持的数据源: {args.sources}", file=sys.stderr)
        sys.exit(1)

    # 加载交易对
    symbols = load_symbols(args.pairs)
    if not symbols:
        print("错误: 未找到任何交易对", file=sys.stderr)
        sys.exit(1)

    print(f"交易对: {symbols}")
    print(f"数据源: {[s['source_id'] for s in source_configs]}")
    print(f"周期: {INTERVALS}")
    print(f"时间范围: {start.strftime('%Y-%m-%d')} ~ {end.strftime('%Y-%m-%d')}")

    # 注册 dataset
    registered = []
    if args.backfill_only:
        print("\n" + "=" * 60)
        print("步骤 1: 从注册表加载已注册 dataset")
        print("=" * 60)
        settings = Settings.load()
        meta = open_meta(settings)
        try:
            registered = load_registered_datasets(
                meta, [s["source_id"] for s in source_configs]
            )
        finally:
            meta.close()
        print(f"找到 {len(registered)} 个已注册的 OHLCV dataset")
        if not registered:
            print("错误: 注册表中没有匹配的 dataset，请先注册", file=sys.stderr)
            sys.exit(1)
    elif not args.register_only:
        print("\n" + "=" * 60)
        print("步骤 1: 注册 dataset")
        print("=" * 60)
        registered = register_datasets(
            symbols, source_configs, dry_run=args.dry_run
        )

    # 执行 backfill
    if not args.register_only and registered:
        print("\n" + "=" * 60)
        print("步骤 2: 回填历史数据")
        print("=" * 60)
        run_backfill(
            registered,
            start=start,
            end=end,
            window_seconds=args.window_seconds,
            dry_run=args.dry_run,
            verbose=args.verbose,
        )


if __name__ == "__main__":
    main()
