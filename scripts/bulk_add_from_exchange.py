#!/usr/bin/env python3
"""从 Binance 或 OKX 获取成交量占比前 N% 的交易对并批量注入。

用法:
    # 从 Binance 获取 24h 成交量前 80% 的交易对
    python scripts/bulk_add_from_exchange.py --exchange binance --top-volume 80

    # 从 OKX 获取 24h 成交量前 80% 的交易对
    python scripts/bulk_add_from_exchange.py --exchange okx --top-volume 80

    # 限制最多注册 100 个交易对
    python scripts/bulk_add_from_exchange.py --exchange binance --top-volume 80 --max-pairs 100

    # 同时获取 Spot 和 Futures
    python scripts/bulk_add_from_exchange.py \
        --exchange binance --top-volume 80 \
        --market-types spot futures

    # 预览不写入
    python scripts/bulk_add_from_exchange.py --exchange binance --top-volume 80 --dry-run

依赖:
    - Binance: 无需 API Key（公开接口）
    - OKX: 无需 API Key（公开接口获取行情）
    - ccxt 库（用于 OKX）
"""

from __future__ import annotations

import argparse
import sys
import time
from typing import Any

from chronoforge.cli._wiring import open_meta
from chronoforge.config.settings import Settings
from chronoforge.connectors.binance_spot import BinanceSpotConnector
from chronoforge.connectors.binance_futures import BinanceFuturesConnector
from chronoforge.connectors.ccxt_bridge import CcxtBridgeConnector
from chronoforge.registry.service import add_dataset


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="从 Binance/OKX 获取高成交量交易对并批量注入 ChronoForge"
    )
    parser.add_argument(
        "--exchange",
        type=str,
        required=True,
        choices=["binance", "okx"],
        help="交易所名称",
    )
    parser.add_argument(
        "--top-volume",
        type=float,
        default=80.0,
        help="保留成交量占比前 N 百分位的交易对（默认 80）",
    )
    parser.add_argument(
        "--max-pairs",
        type=int,
        default=500,
        help="最多注册的交易对数量（默认 500）",
    )
    parser.add_argument(
        "--market-types",
        type=str,
        nargs="+",
        default=["spot"],
        choices=["spot", "futures"],
        help="市场类型（默认只获取 spot）",
    )
    parser.add_argument(
        "--interval",
        type=str,
        default="1m",
        help="K线周期",
    )
    parser.add_argument(
        "--canonical-type",
        type=str,
        default="OHLCV",
        choices=["OHLCV", "TRADE", "TICKER", "FUNDING", "OPEN_INTEREST"],
        help="规范类型（默认 OHLCV）",
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


def filter_active_symbols(
    symbols: list[dict[str, Any]], source: str = "binance"
) -> list[str]:
    """过滤出可交易的交易对符号。"""
    if source == "binance":
        # Binance: 需要 status == "TRADING" 且 isSpot/isMargin=true
        return [
            s["symbol"]
            for s in symbols
            if s.get("status") == "TRADING"
            and s.get("isSpot", False)
            and s.get("isMargin", False) is False  # 排除杠杆
            and not s.get("isContract", False)  # 排除合约
        ]
    return symbols


def get_binance_spot_volume_ranking(
    connector: BinanceSpotConnector, top_percentile: float = 80.0
) -> list[tuple[str, float]]:
    """从 Binance Spot 获取按 24h 成交量排序的交易对。

    Returns:
        list of (symbol, quote_volume) sorted by volume descending.
    """
    print("正在获取 Binance Spot 交易对列表...")
    try:
        exchange_info = connector._client.get("/api/v3/exchangeInfo").json()
        all_symbols = exchange_info.get("symbols", [])
    except Exception as e:
        print(f"获取 exchangeInfo 失败: {e}", file=sys.stderr)
        return []

    # 过滤可交易的现货交易对
    active_symbols = [
        s["symbol"]
        for s in all_symbols
        if s.get("status") == "TRADING"
        and s.get("isSpotTradingAllowed", False) is True
    ]

    if not active_symbols:
        print("未找到可交易的现货交易对", file=sys.stderr)
        return []

    print(f"找到 {len(active_symbols)} 个可交易现货交易对，正在获取 24h 行情...")

    # 获取所有交易对的 24h 行情（批量请求）
    tickers_data = []
    try:
        tickers_data = connector._client.get("/api/v3/ticker/24hr").json()
    except Exception as e:
        print(f"获取 ticker/24hr 失败: {e}", file=sys.stderr)
        return []

    # 构建 symbol -> volume 映射（只保留 quoteVolume 有效的）
    volume_map = {}
    for ticker in tickers_data:
        symbol = ticker.get("symbol")
        quote_volume = ticker.get("quoteVolume")
        if symbol in active_symbols and quote_volume:
            try:
                volume_map[symbol] = float(quote_volume)
            except (ValueError, TypeError):
                continue

    if not volume_map:
        print("未获取到有效的成交量数据", file=sys.stderr)
        return []

    # 按成交量排序
    sorted_pairs = sorted(volume_map.items(), key=lambda x: x[1], reverse=True)

    # 计算前 N% 分位阈值
    n_keep = max(1, int(len(sorted_pairs) * (100 - top_percentile) / 100))
    threshold_index = min(n_keep, len(sorted_pairs))
    selected = sorted_pairs[:threshold_index]

    print(f"按 {top_percentile}% 筛选: 保留前 {len(selected)} 个交易对（阈值: {len(sorted_pairs)} 个）")
    return selected


def get_binance_futures_volume_ranking(
    connector: BinanceFuturesConnector, top_percentile: float = 80.0
) -> list[tuple[str, float]]:
    """从 Binance Futures USDT-M 获取按 24h 成交量排序的交易对。"""
    print("正在获取 Binance Futures 交易对列表...")
    try:
        exchange_info = connector._client.get("/fapi/v1/exchangeInfo").json()
        all_symbols = exchange_info.get("symbols", [])
    except Exception as e:
        print(f"获取 exchangeInfo 失败: {e}", file=sys.stderr)
        return []

    # 过滤 USDT-M 永续合约
    active_symbols = [
        s["symbol"]
        for s in all_symbols
        if s.get("status") == "TRADING"
        and s.get("contractType") == "PERPETUAL"
        and s.get("quoteAsset") == "USDT"
        and s.get("isSpotTradingAllowed", False) is False  # 只选合约
    ]

    if not active_symbols:
        print("未找到可交易的 USDT-M 永续合约", file=sys.stderr)
        return []

    print(f"找到 {len(active_symbols)} 个可交易永续合约，正在获取 24h 行情...")

    # 获取 24h 行情
    tickers_data = []
    try:
        tickers_data = connector._client.get("/fapi/v1/ticker/24hr").json()
    except Exception as e:
        print(f"获取 ticker/24hr 失败: {e}", file=sys.stderr)
        return []

    volume_map = {}
    for ticker in tickers_data:
        symbol = ticker.get("symbol")
        quote_volume = ticker.get("quoteVolume")
        if symbol in active_symbols and quote_volume:
            try:
                volume_map[symbol] = float(quote_volume)
            except (ValueError, TypeError):
                continue

    if not volume_map:
        print("未获取到有效的成交量数据", file=sys.stderr)
        return []

    sorted_pairs = sorted(volume_map.items(), key=lambda x: x[1], reverse=True)
    n_keep = max(1, int(len(sorted_pairs) * (100 - top_percentile) / 100))
    threshold_index = min(n_keep, len(sorted_pairs))
    selected = sorted_pairs[:threshold_index]

    print(f"按 {top_percentile}% 筛选: 保留前 {len(selected)} 个交易对")
    return selected


def get_okx_volume_ranking(
    connector: CcxtBridgeConnector, top_percentile: float = 80.0, max_pairs: int = 500
) -> list[tuple[str, float]]:
    """从 OKX 获取按 24h 成交量排序的交易对。"""
    print("正在加载 OKX 市场数据...")
    try:
        connector.exchange.load_markets()
    except Exception as e:
        print(f"加载市场数据失败: {e}", file=sys.stderr)
        return []

    # 筛选现货交易对
    spot_symbols = [
        symbol
        for symbol, market in (connector.exchange.markets or {}).items()
        if market.get("type") == "spot" and market.get("active", False)
    ]

    if not spot_symbols:
        print("未找到可交易的现货交易对", file=sys.stderr)
        return []

    print(f"找到 {len(spot_symbols)} 个可交易现货交易对，正在获取 24h 行情...")

    volume_map = {}
    errors = 0
    max_errors = 20  # 允许最多 20 个错误

    for symbol in spot_symbols:
        if errors > max_errors:
            print("达到最大错误数限制，停止获取...")
            break

        try:
            connector._rate_limiter.acquire(1)
            ticker = connector.exchange.fetchTicker(symbol)
            quote_volume = ticker.get("quoteVolume")
            if quote_volume:
                volume_map[symbol] = float(quote_volume)
        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"获取 {symbol} 行情失败: {e}", file=sys.stderr)

    if not volume_map:
        print("未获取到有效的成交量数据", file=sys.stderr)
        return []

    sorted_pairs = sorted(volume_map.items(), key=lambda x: x[1], reverse=True)

    # 限制最大数量
    if len(sorted_pairs) > max_pairs:
        print(f"交易对数量超过限制 ({len(sorted_pairs)} > {max_pairs})，按成交量取前 {max_pairs} 个")
        sorted_pairs = sorted_pairs[:max_pairs]

    n_keep = max(1, int(len(sorted_pairs) * (100 - top_percentile) / 100))
    threshold_index = min(n_keep, len(sorted_pairs))
    selected = sorted_pairs[:threshold_index]

    print(f"按 {top_percentile}% 筛选: 保留前 {len(selected)} 个交易对")
    return selected


def bulk_add_from_exchange(
    exchange: str,
    top_percentile: float = 80.0,
    max_pairs: int = 500,
    market_types: list[str] | None = None,
    interval: str = "1m",
    canonical_type: str = "OHLCV",
    dry_run: bool = False,
    verbose: bool = False,
) -> None:
    """从交易所获取高成交量交易对并批量注入。"""
    settings = Settings.load()
    meta = open_meta(settings)

    market_types = market_types or ["spot"]
    total_added = 0
    total_failed = 0

    for market_type in market_types:
        try:
            if exchange == "binance" and market_type == "spot":
                connector = BinanceSpotConnector(settings=settings)
                volume_ranking = get_binance_spot_volume_ranking(
                    connector, top_percentile
                )
                source_id = "binance_spot"

            elif exchange == "binance" and market_type == "futures":
                connector = BinanceFuturesConnector(settings=settings)
                volume_ranking = get_binance_futures_volume_ranking(
                    connector, top_percentile
                )
                source_id = "binance_futures"

            elif exchange == "okx":
                connector = CcxtBridgeConnector(
                    settings=settings, exchange="okx"
                )
                volume_ranking = get_okx_volume_ranking(
                    connector, top_percentile, max_pairs
                )
                source_id = "ccxt"
            else:
                print(f"不支持的组合: {exchange} + {market_type}", file=sys.stderr)
                continue

        except Exception as e:
            print(f"初始化 {exchange} 连接器失败: {e}", file=sys.stderr)
            continue

        print(f"\n开始注入 {market_type} 交易对 ({len(volume_ranking)} 个)...")

        for symbol, volume in volume_ranking:
            try:
                # 生成 dataset_id
                symbol_upper = symbol.upper().replace("-", "")
                dataset_id = f"{source_id.upper()}:{symbol_upper}:{canonical_type}:{interval}"

                if dry_run:
                    print(f"  [DRY RUN] {dataset_id} (volume: {volume:.2e})")
                    total_added += 1
                    continue

                # 构建 params
                params: dict[str, Any] = {
                    "symbol": symbol_upper,
                    "interval": interval,
                }

                if canonical_type == "FUNDING":
                    params["data_type"] = "funding"
                elif canonical_type == "OPEN_INTEREST":
                    params["data_type"] = "open_interest"
                elif canonical_type == "TRADE":
                    params["data_type"] = "aggregate"
                elif canonical_type == "TICKER":
                    params["data_type"] = "ticker"

                # OKX 使用 ccxt source，其他使用原始 source
                actual_source = source_id

                add_dataset(
                    meta,
                    dataset_id=dataset_id,
                    source_id=actual_source,
                    canonical_type=canonical_type,
                    entity_id=symbol_upper,
                    params=params,
                    frequency=interval,
                    continuity_model="ALWAYS_OPEN",
                    revision_supported=False,
                )
                total_added += 1

                if verbose:
                    print(f"  已注册: {dataset_id} (volume: {volume:.2e})")

            except Exception as e:
                total_failed += 1
                print(f"  注册失败 {symbol}: {e}", file=sys.stderr)

        connector.close()
        time.sleep(1)  # 请求间隔

    print(f"\n{'[DRY RUN] ' if dry_run else ''}完成!")
    print(f"  成功注册: {total_added}")
    if total_failed > 0:
        print(f"  失败: {total_failed}")


def main() -> None:
    args = parse_args()
    bulk_add_from_exchange(
        exchange=args.exchange,
        top_percentile=args.top_volume,
        max_pairs=args.max_pairs,
        market_types=args.market_types,
        interval=args.interval,
        canonical_type=args.canonical_type,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    main()
