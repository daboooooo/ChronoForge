#!/usr/bin/env python3
"""从 ccxt 支持的交易所（binance、okx 等）抓取成交量帕累托头部的交易对清单。

按 24h 计价成交量降序累加，选出累计成交量达到总量 top_volume% 的最小头部
集合（帕累托分布），并保存为 scripts/symbols/{exchange}_{market_type}_symbols.json。
清单结构与目录内其他 *_symbols.json 一致（ticker/desc/unit/freq/comment，
ticker 为 ccxt unified symbol），可衔接后续批量注册流程。

用法:
    # 抓取 binance + okx 现货累计成交量头部 80% 的交易对
    python scripts/fetch_top_symbols_from_ccxt_exchange.py --exchange binance okx

    # 同时抓取合约（futures）
    python scripts/fetch_top_symbols_from_ccxt_exchange.py \
        --exchange binance --market-types spot futures

    # 只保留 USDT 计价，最多 100 个
    python scripts/fetch_top_symbols_from_ccxt_exchange.py \
        --exchange okx --quote USDT --max-pairs 100

    # 网络受限环境
    python scripts/fetch_top_symbols_from_ccxt_exchange.py \
        --exchange okx --proxy http://127.0.0.1:7897

依赖:
    - 交易所公开接口，无需 API Key
    - ccxt 库（binance/okx 等统一走 ccxt，fetchTickers 批量获取行情）
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import ccxt

OUT_DIR_DEFAULT = Path(__file__).resolve().parent / "symbols"

# futures 的 ccxt defaultType 因交易所而异
_FUTURE_DEFAULT_TYPE = {
    "binance": "future",  # USDT-M
    "okx": "swap",
}


def _build_exchange(exchange_id: str, market_type: str, proxy: str | None) -> ccxt.Exchange:
    """构建 ccxt 交易所实例（代理处理与 connectors/ccxt_bridge.py 惯例一致）。"""
    exchange_class = getattr(ccxt, exchange_id, None)
    if exchange_class is None:
        raise ValueError(f"ccxt: unknown exchange '{exchange_id}'")

    config: dict[str, Any] = {"enableRateLimit": True}
    if market_type != "spot":
        config["options"] = {
            "defaultType": _FUTURE_DEFAULT_TYPE.get(exchange_id, "swap"),
        }
    resolved = (
        proxy
        or os.environ.get("HTTPS_PROXY")
        or os.environ.get("https_proxy")
        or os.environ.get("HTTP_PROXY")
        or os.environ.get("http_proxy")
    )
    if resolved:
        config["proxies"] = {"http": resolved, "https": resolved}
    return exchange_class(config)


def select_top_volume_pairs(
    volume_map: dict[str, float], top_percentile: float = 80.0
) -> list[tuple[str, float]]:
    """按成交量降序累加，选出累计成交量达到总量 top_percentile% 的头部交易对。

    与"按数量取前 N%"不同：先排序再逐个累加成交量，返回贡献了绝大部分
    成交量的最小头部集合（帕累托分布下该集合通常远小于全量交易对）。
    最后加入的币种可能使累计占比略超 top_percentile，属预期行为。

    Returns:
        list of (symbol, quote_volume) sorted by volume descending.
    """
    sorted_pairs = sorted(volume_map.items(), key=lambda x: x[1], reverse=True)
    total_volume = sum(v for _, v in sorted_pairs)
    if total_volume <= 0:
        return sorted_pairs
    threshold = total_volume * top_percentile / 100
    selected: list[tuple[str, float]] = []
    cumulative = 0.0
    for pair in sorted_pairs:
        selected.append(pair)
        cumulative += pair[1]
        if cumulative >= threshold:
            break
    return selected


def fetch_volume_map(
    exchange: ccxt.Exchange, market_type: str, quote: str | None
) -> dict[str, float]:
    """抓取指定市场类型的全量 24h 行情，返回 {symbol: quote_volume}。

    使用 fetchTickers 批量接口（单请求），替代逐个 fetchTicker。
    quoteVolume 缺失时（如 OKX swap 的 unified 字段无计价成交量），
    用 baseVolume × contractSize × last 估算：
      - baseVolume 语义跨所不一（okx swap=张数，binance=币数），
        统一乘 contractSize 归一到币数量（binance futures contractSize=1）
      - last 为时点价非 24h 均价，排序用途精度足够
    """
    exchange.load_markets()
    want = "spot" if market_type == "spot" else "swap"
    target_markets = {
        m["symbol"]: m
        for m in exchange.markets.values()
        if m.get("type") == want
        and m.get("active", False)
        and (quote is None or m.get("quote") == quote)
    }
    if not target_markets:
        return {}
    if not exchange.has.get("fetchTickers"):
        raise RuntimeError(f"{exchange.id} 不支持 fetchTickers，无法批量获取行情")

    tickers = exchange.fetch_tickers()
    volume_map: dict[str, float] = {}
    for sym, market in target_markets.items():
        t = tickers.get(sym) or {}
        qv = t.get("quoteVolume")
        if qv is None:
            base_vol, last = t.get("baseVolume"), t.get("last")
            if base_vol and last:
                try:
                    contract_size = float(market.get("contractSize") or 1)
                    qv = float(base_vol) * contract_size * float(last)
                except (ValueError, TypeError):
                    continue
        if qv:
            try:
                volume_map[sym] = float(qv)
            except (ValueError, TypeError):
                continue
    return volume_map


def build_entries(
    volume_map: dict[str, float],
    exchange_id: str,
    market_type: str,
    top_percentile: float,
    max_pairs: int,
) -> tuple[dict[str, dict[str, Any]], int]:
    """帕累托筛选并构建清单条目。

    Returns:
        (entries, filtered_total)：entries 为 {紧凑符号: 条目}，filtered_total
        为筛选前有效成交量交易对总数（用于统计输出）。
    """
    filtered_total = len(volume_map)
    selected = select_top_volume_pairs(volume_map, top_percentile)
    if len(selected) > max_pairs:
        selected = selected[:max_pairs]

    grand_total = sum(volume_map.values())
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    entries: dict[str, dict[str, Any]] = {}
    cumulative = 0.0
    for rank, (symbol, vol) in enumerate(selected, 1):
        cumulative += vol
        pct = cumulative / grand_total * 100 if grand_total > 0 else 0.0
        compact = symbol.replace("/", "").upper()
        quote_ccy = symbol.split("/", 1)[1] if "/" in symbol else ""
        entries[compact] = {
            "ticker": symbol,
            "desc": f"{exchange_id} {market_type} 交易对（24h 计价成交量排名 #{rank}）",
            "unit": quote_ccy,
            "freq": "daily",
            "comment": (
                f"24h 计价成交量 {vol:.3e}；累计占比 {pct:.1f}%"
                f"（帕累托头部 {top_percentile:g}% 筛选）。"
                f"ccxt fetchTickers 抓取于 {now}。"
            ),
            "quote_volume_24h": vol,
            "rank": rank,
            "cumulative_pct": round(pct, 2),
        }
    return entries, filtered_total


def fetch_top_symbols(
    exchanges: list[str],
    market_types: list[str],
    top_percentile: float,
    max_pairs: int,
    quote: str | None,
    out_dir: Path,
    proxy: str | None,
    dry_run: bool,
) -> int:
    """对交易所 × 市场类型组合逐一抓取并保存清单，返回退出码。"""
    exit_code = 0
    for exchange_id in exchanges:
        for market_type in market_types:
            label = f"{exchange_id} {market_type}"
            print(f"\n{'=' * 60}\n{label}\n{'=' * 60}")
            try:
                exchange = _build_exchange(exchange_id, market_type, proxy)
            except ValueError as e:
                print(f"  {e}", file=sys.stderr)
                exit_code = 1
                continue

            try:
                volume_map = fetch_volume_map(exchange, market_type, quote)
            except ccxt.BaseError as e:
                print(f"  行情获取失败: {e}", file=sys.stderr)
                exit_code = 1
                continue
            finally:
                exchange.close()

            if not volume_map:
                print("  未获取到有效成交量数据", file=sys.stderr)
                exit_code = 1
                continue

            entries, filtered_total = build_entries(
                volume_map, exchange_id, market_type, top_percentile, max_pairs
            )
            cum_pct = max(
                (e["cumulative_pct"] for e in entries.values()), default=0.0
            )
            print(
                f"  有效交易对 {filtered_total} 个 → 帕累托头部 {top_percentile:g}%:"
                f" 保留 {len(entries)} 个（累计成交量占比 {cum_pct:.1f}%）"
            )

            out_path = out_dir / f"{exchange_id}_{market_type}_symbols.json"
            if dry_run:
                print(f"  [DRY RUN] 将保存 {len(entries)} 条 → {out_path}")
                for compact, e in list(entries.items())[:5]:
                    print(f"    {compact}: {e['ticker']} (rank={e['rank']},"
                          f" vol={e['quote_volume_24h']:.3e})")
                continue

            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(
                json.dumps(entries, ensure_ascii=False, indent=4) + "\n",
                encoding="utf-8",
            )
            print(f"  已保存 {len(entries)} 条 → {out_path}")
    return exit_code


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="从 ccxt 交易所抓取成交量帕累托头部交易对并存为符号清单"
    )
    parser.add_argument(
        "--exchange", type=str, nargs="+", required=True,
        help="交易所 ID（ccxt 支持，如 binance okx bybit）",
    )
    parser.add_argument(
        "--market-types", type=str, nargs="+", default=["spot"],
        choices=["spot", "futures"],
        help="市场类型（默认 spot）",
    )
    parser.add_argument(
        "--top-volume", type=float, default=80.0,
        help="头部交易对累计成交量达到总成交量的 N%% 即停止（默认 80）",
    )
    parser.add_argument(
        "--max-pairs", type=int, default=500,
        help="最多保存的交易对数量（默认 500）",
    )
    parser.add_argument(
        "--quote", type=str, default="USDT",
        help="只保留指定计价货币（默认 USDT；跨法币 quote 的成交量量纲不可比，"
             "会污染帕累托筛选。传 all 不过滤）",
    )
    parser.add_argument(
        "--out-dir", type=Path, default=OUT_DIR_DEFAULT,
        help=f"输出目录（默认 {OUT_DIR_DEFAULT}）",
    )
    parser.add_argument(
        "--proxy", type=str, default=None,
        help="HTTP(S) 代理地址（也可用 HTTPS_PROXY 环境变量）",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="仅打印计划，不写文件",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    raise SystemExit(
        fetch_top_symbols(
            exchanges=args.exchange,
            market_types=args.market_types,
            top_percentile=args.top_volume,
            max_pairs=args.max_pairs,
            quote=None if args.quote == "all" else args.quote.upper(),
            out_dir=args.out_dir,
            proxy=args.proxy,
            dry_run=args.dry_run,
        )
    )


if __name__ == "__main__":
    main()
