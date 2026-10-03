#!/usr/bin/env python3
"""从 binance、okx 抓取代币化股票（tokenized equities）清单，生成
scripts/symbols/{exchange}_stocks.json。

统一筛选模型（ccxt unified market 公共条件 + 交易所官方品种标记）：
  1. 现货市场（spot）且处于 active 状态；
  2. 计价货币为 USDT（同一 base 同时存在 USDT/USDC 交易对时优先 USDT，
     流动性主市场；--quote all 可关闭过滤，此时仍优先保留 USDT 交易对）；
  3. 交易所官方资产类别标记为"代币化股票"（两家所标记位置不同，不可用
     统一的名字前缀推断，必须取官方标记）：
       - okx:      market.info.instCategory == "3"（OKX 官方品种分类，
                   3 = 美股代币化/xStocks），且 instId 以 "X" 开头。
                   instCategory 用于精确排除 XAUT（Tether Gold）、XCH（Chia）
                   等同样以 X 开头的非股票品种；"X 前缀"是 OKX xStocks 的
                   命名约定（XAAPL-USDT），双条件取交集。
       - binance: 币安 2026-06 推出的 bStocks（NVDAB/USDT 等，base 统一
                   带 "B" 后缀）。exchangeInfo 不含资产类别字段（TRD_GRP
                   权限组与普通币种无差异），官方产品目录 get-products 的
                   tags 含 "bStocks" 才是权威标记；目录中另有 "tCommodities"
                   标签的 PAXG/XAUT 贵金属代币，严格按标签排除。
                   注意 base 后缀 "B" 不能作为筛选条件（ARB/SHIB/BNB 等
                   普通币种同样以 B 结尾），仅用于还原底层股票代码。

输出格式与目录内其他 *_symbols.json 一致（ticker/desc/unit/freq/comment），
ticker 为 ccxt unified symbol（如 "XAAPL/USDT"、"NVDAB/USDT"），可直接用于
ccxt fetchOHLCV / register_symbols.py 批量注册。

用法：
    python3 scripts/get_cex_stock_symbols.py                  # binance + okx
    python3 scripts/get_cex_stock_symbols.py --exchange okx
    python3 scripts/get_cex_stock_symbols.py --proxy http://127.0.0.1:7897
    python3 scripts/get_cex_stock_symbols.py --quote all --dry-run
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Callable

import ccxt

OUT_DIR_DEFAULT = Path(__file__).resolve().parent / "symbols"

# Binance 官方产品目录（公开接口）：exchangeInfo 不携带资产类别标签，
# 该目录的 tags 字段是 bStocks / tCommodities 等品种分类的权威来源。
_BINANCE_PRODUCT_CATALOG = (
    "https://www.binance.com/bapi/asset/v1/public/asset-service/product/get-products"
)
_BINANCE_STOCK_TAG = "bStocks"

# OKX xStocks 人工维护名称表：OKX instruments 接口不返回底层标的名称。
# 未知代码走"官方名称待确认"兜底，新增标的无需改表即可入清单。
KNOWN_SYMBOLS: dict[str, str] = {
    "XAAOI": "Applied Optoelectronics（应用光电，光模块/数据中心）",
    "XAAPL": "Apple（苹果）",
    "XADBE": "Adobe（创意软件）",
    "XALAB": "Astera Labs（互连芯片）",
    "XAMAT": "Applied Materials（应用材料，半导体设备）",
    "XAMD": "AMD（超威半导体）",
    "XAMZN": "Amazon（亚马逊）",
    "XAPP": "AppLovin（移动广告平台）",
    "XARM": "Arm Holdings（安谋，CPU IP 授权）",
    "XASML": "ASML（阿斯麦，光刻机龙头）",
    "XASTS": "AST SpaceMobile（卫星直连手机通信）",
    "XAVGO": "Broadcom（博通，AI 网络芯片/定制 ASIC）",
    "XBE": "Bloom Energy（燃料电池发电）",
    "XBMNR": "Bitmine Immersion（比特币矿企，ETH 财库公司）",
    "XBX": "Blackstone（黑石，另类资产管理）",
    "XCIEN": "Ciena（光网络设备）",
    "XCOHR": "Coherent（高意，光器件/激光）",
    "XCOIN": "Coinbase（美国合规加密交易所）",
    "XCRCL": "Circle（USDC 发行方）",
    "XCRM": "Salesforce（赛富时，SaaS CRM）",
    "XCRWD": "CrowdStrike（云安全）",
    "XCRWV": "CoreWeave（AI 算力云）",
    "XCSCO": "Cisco（思科，网络设备）",
    "XDELL": "Dell（戴尔，AI 服务器）",
    "XDKNG": "DraftKings（在线体育博彩）",
    "XEWY": "iShares MSCI South Korea ETF（韩国市场 ETF）",
    "XGEV": "GE Vernova（电力设备/电网）",
    "XGLW": "Corning（康宁，光纤玻璃）",
    "XGME": "GameStop（游戏驿站，meme 股代表）",
    "XGOOGL": "Alphabet Class A（谷歌母公司）",
    "XHIMS": "Hims & Hers Health（在线医疗）",
    "XHOOD": "Robinhood（零售券商）",
    "XHPE": "Hewlett Packard Enterprise（慧与，企业 IT/AI 服务器）",
    "XIBM": "IBM（国际商业机器，混合云/量子计算）",
    "XINTC": "Intel（英特尔）",
    "XIREN": "IREN（比特币矿企转型 AI 算力）",
    "XISRG": "Intuitive Surgical（直觉外科，手术机器人）",
    "XIWM": "iShares Russell 2000 ETF（美国小盘股 ETF）",
    "XJNJ": "Johnson & Johnson（强生）",
    "XKLAC": "KLA（半导体过程控制设备）",
    "XKO": "Coca-Cola（可口可乐）",
    "XLITE": "Lumentum（光模块）",
    "XLLY": "Eli Lilly（礼来，GLP-1 减重药）",
    "XLRCX": "Lam Research（泛林，半导体刻蚀设备）",
    "XMETA": "Meta Platforms（Facebook/Instagram 母公司）",
    "XMRVL": "Marvell（迈威尔，AI 互联芯片）",
    "XMSFT": "Microsoft（微软）",
    "XMSTR": "MicroStrategy（比特币财库公司）",
    "XMU": "Micron（美光，DRAM/NAND 存储）",
    "XNBIS": "Nebius（AI 云基础设施）",
    "XNFLX": "Netflix（奈飞，流媒体）",
    "XNOW": "ServiceNow（企业工作流 SaaS）",
    "XNVDA": "NVIDIA（英伟达，AI 算力龙头）",
    "XOKTA": "Okta（身份认证 SaaS）",
    "XON": "ON Semiconductor（安森美，功率/模拟芯片）",
    "XONDS": "Ondas Holdings（无人机/专用无线网络）",
    "XORCL": "Oracle（甲骨文，数据库/AI 云）",
    "XPLTR": "Palantir（数据分析/AI 平台）",
    "XPOPMART": "泡泡玛特（港股 9992.HK 代币化）",
    "XQCOM": "Qualcomm（高通，手机 SoC）",
    "XQQQ": "Invesco QQQ（纳斯达克 100 ETF）",
    "XRDDT": "Reddit（社区平台）",
    "XRIVN": "Rivian（电动车）",
    "XRKLB": "Rocket Lab（商业航天）",
    "XROK": "Rockwell Automation（罗克韦尔，工业自动化）",
    "XSHEIN": "SHEIN（未上市公司私募份额代币化）",
    "XSMCI": "Super Micro Computer（超微电脑，AI 服务器）",
    "XSMH": "VanEck Semiconductor ETF（半导体行业 ETF）",
    "XSNDK": "SanDisk（NAND 存储独立后主体）",
    "XSNOW": "Snowflake（云数据仓库）",
    "XSOXL": "Direxion Semiconductor Bull 3X ETF（半导体 3 倍做多 ETF）",
    "XSOXS": "Direxion Semiconductor Bear 3X ETF（半导体 3 倍做空 ETF）",
    "XSPY": "SPDR S&P 500 ETF（标普 500 ETF，全球最大 ETF）",
    "XSTRC": "Strategy (STRC) 可变利率优先股",
    "XTER": "Teradyne（泰瑞达，半导体测试设备）",
    "XTQQQ": "ProShares UltraPro QQQ（纳指 100 三倍杠杆 ETF）",
    "XTSM": "TSMC（台积电，芯片代工龙头）",
    "XTTWO": "Take-Two Interactive（游戏发行商）",
    "XTWLO": "Twilio（通信 API 平台）",
    "XUNH": "UnitedHealth（联合健康）",
    "XVRT": "Vertiv（维谛，数据中心电源/散热）",
    "XWDC": "Western Digital（西部数据，HDD）",
    "XXIAOMI": "小米集团（港股 1810.HK 代币化）",
    "XXLE": "Energy Select Sector SPDR（标普能源行业 ETF）",
    "XZM": "Zoom（视频会议）",
}

# 与底层股票代码不完全一致的变体代码（如 XAPLD 对应 Applied Digital？）
_VARIANT_HINTS = frozenset({"XAPLD", "XINTW", "XMUU", "XMVLL", "XTESTA"})
_LEVERAGED_ETF = frozenset({"XSOXL", "XSOXS", "XTQQQ"})
_NON_US_LISTINGS = frozenset({"XPOPMART", "XXIAOMI", "XSHEIN", "XSTRC"})


def _build_exchange(exchange_id: str, proxy: str | None) -> ccxt.Exchange:
    """构建 ccxt 现货交易所实例（代理处理与 connectors/ccxt_bridge.py 惯例一致）。"""
    exchange_class = getattr(ccxt, exchange_id, None)
    if exchange_class is None:
        raise ValueError(f"ccxt: unknown exchange '{exchange_id}'")

    config: dict[str, Any] = {"enableRateLimit": True}
    # 与 src/chronoforge/connectors/ccxt_bridge.py 惯例一致：
    # ccxt 不读环境代理变量，需显式传 proxies
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


def _pick(picked: dict[str, dict], market: dict, quote: str | None) -> None:
    """按 base 收集市场；quote 过滤 + 同 base 多 quote 时优先 USDT。"""
    if quote is not None and market["quote"] != quote:
        return
    base = market["base"]
    current = picked.get(base)
    if current is None or (
        market["quote"] == "USDT" and current["market"]["quote"] != "USDT"
    ):
        picked[base] = {"market": market}


# --------------------------------------------------------------------------- #
# 各交易所检测器：返回 {base: {"market": ccxt market, "name": 官方名称|None}}
# --------------------------------------------------------------------------- #

def fetch_okx_stock_markets(
    exchange: ccxt.Exchange, quote: str | None
) -> dict[str, dict]:
    """OKX xStocks：spot + active + instId 以 X 开头 + instCategory == "3"。"""
    exchange.load_markets()
    picked: dict[str, dict] = {}
    for market in exchange.markets.values():
        info = market.get("info") or {}
        inst_id = info.get("instId", "")
        is_stock_token = (
            market.get("spot")
            and market.get("active", True)
            and inst_id.upper().startswith("X")
            and info.get("instCategory") == "3"
        )
        if is_stock_token:
            _pick(picked, market, quote)
    for item in picked.values():
        item["name"] = None
    return picked


def fetch_binance_stock_names(exchange: ccxt.Exchange) -> dict[str, str]:
    """拉取 Binance 官方产品目录，返回 {symbol(exchange id): asset_name}。

    仅保留 tags 含 "bStocks" 的产品；复用 ccxt 自身 HTTP 栈，自动走代理。
    """
    response = exchange.fetch(
        _BINANCE_PRODUCT_CATALOG, method="GET", headers={"Accept": "application/json"}
    )
    products = response.get("data") if isinstance(response, dict) else None
    if not isinstance(products, list):
        raise RuntimeError("Binance 产品目录响应格式异常（缺少 data 列表）")
    return {
        p["s"]: p.get("an", "")
        for p in products
        if _BINANCE_STOCK_TAG in (p.get("tags") or [])
    }


def fetch_binance_stock_markets(
    exchange: ccxt.Exchange, quote: str | None
) -> dict[str, dict]:
    """Binance bStocks：spot + active + market.id 命中目录 bStocks 标签集合。"""
    exchange.load_markets()
    tagged_names = fetch_binance_stock_names(exchange)
    picked: dict[str, dict] = {}
    for market in exchange.markets.values():
        if not (market.get("spot") and market.get("active", True)):
            continue
        name = tagged_names.get(market["id"])
        if name is None:
            continue
        if quote is not None and market["quote"] != quote:
            continue
        picked[market["base"]] = {"market": market, "name": name}
    return picked


STOCK_FETCHERS: dict[str, Callable[[ccxt.Exchange, str | None], dict[str, dict]]] = {
    "okx": fetch_okx_stock_markets,
    "binance": fetch_binance_stock_markets,
}


# --------------------------------------------------------------------------- #
# 条目文案
# --------------------------------------------------------------------------- #

def _okx_desc(base: str) -> str:
    if base in KNOWN_SYMBOLS:
        return f"代币化股票：{KNOWN_SYMBOLS[base]}"
    core = base[1:]
    if base in _VARIANT_HINTS:
        return f"OKX xStocks 代币化标的（{core} 关联变体代码，官方名称待确认）"
    return f"OKX xStocks 系列代币化标的（代码 {core}，官方名称待确认）"


def _okx_comment(base: str) -> str:
    head = (
        "OKX xStocks 代币化标的，7×24 交易。ticker 为 ccxt unified symbol，"
        "可直接用于 ccxt okx fetchOHLCV。"
    )
    core = base[1:]
    if base in _LEVERAGED_ETF:
        return head + " 杠杆 ETF 波动放大，仅适合短线趋势观察。"
    if base in _NON_US_LISTINGS:
        return head + " 非美股上市主体（港股/私募/优先股）的代币化版本。"
    if base in _VARIANT_HINTS:
        return head + f" {base} 为 {core} 关联变体代码，对应标的待确认。"
    extra = ""
    if base == "XMU":
        extra = " 与 scripts/symbols/yahoo_symbols.json 中 Micron (MU) 对应。"
    elif base == "XINTC":
        extra = " 与 scripts/symbols/yahoo_symbols.json 中 Intel (INTC) 对应。"
    return (
        f"OKX xStocks 代币化股票，7×24 交易，追踪 {core} 美股价格。"
        f"ticker 为 ccxt unified symbol，可直接用于 ccxt okx fetchOHLCV。{extra}"
    )


def _binance_desc(name: str, base: str) -> str:
    # 目录名称形如 "NVIDIA (bStocks)"，去掉品牌后缀
    clean = name.removesuffix(f" ({_BINANCE_STOCK_TAG})").strip() or base
    return f"代币化股票：{clean}"


def _binance_comment(base: str) -> str:
    # bStocks 代码统一以 B 结尾（NVDAB -> NVDA、MUB -> MU）
    core = base[:-1] if base.endswith("B") and len(base) > 1 else base
    return (
        f"Binance bStocks 代币化证券，7×24 交易，追踪 {core} 美股价格"
        f"（代币代码 {base}，bStocks 统一 B 后缀；筛选依据为币安产品目录 "
        f"tags={_BINANCE_STOCK_TAG}）。ticker 为 ccxt unified symbol，"
        f"可直接用于 ccxt binance fetchOHLCV。"
    )


def build_entries(exchange_id: str, picked: dict[str, dict]) -> dict[str, dict]:
    entries: dict[str, dict] = {}
    for base in sorted(picked):
        item = picked[base]
        market = item["market"]
        if exchange_id == "okx":
            desc, comment = _okx_desc(base), _okx_comment(base)
        elif exchange_id == "binance":
            desc, comment = _binance_desc(item["name"] or "", base), _binance_comment(base)
        else:  # 未来新增交易所的兜底
            desc, comment = (
                f"{exchange_id} 代币化股票（代码 {base}）",
                f"{exchange_id} 代币化股票，7×24 交易。ticker 为 ccxt unified symbol。",
            )
        entries[base] = {
            "ticker": market["symbol"],
            "desc": desc,
            "unit": "USD",
            "freq": "daily",
            "comment": comment,
        }
    return entries


def fetch_stock_symbols(
    exchanges: list[str],
    quote: str | None,
    out_dir: Path,
    proxy: str | None,
    dry_run: bool,
) -> int:
    """对交易所逐一抓取代币化股票清单并保存，返回退出码。"""
    exit_code = 0
    for exchange_id in exchanges:
        label = f"{exchange_id} xStocks"
        print(f"\n{'=' * 60}\n{label}\n{'=' * 60}")
        fetcher = STOCK_FETCHERS.get(exchange_id)
        if fetcher is None:
            print(f"  不支持的交易所 '{exchange_id}'（支持：{sorted(STOCK_FETCHERS)}）",
                  file=sys.stderr)
            exit_code = 1
            continue

        try:
            exchange = _build_exchange(exchange_id, proxy)
        except ValueError as e:
            print(f"  {e}", file=sys.stderr)
            exit_code = 1
            continue

        try:
            picked = fetcher(exchange, quote)
        except (ccxt.BaseError, RuntimeError) as e:
            print(f"  股票代币清单获取失败: {type(e).__name__}: {e}", file=sys.stderr)
            exit_code = 1
            continue
        finally:
            exchange.close()

        if not picked:
            print("  未筛选到任何代币化股票，检查筛选条件或网络", file=sys.stderr)
            exit_code = 1
            continue

        entries = build_entries(exchange_id, picked)
        quotes = sorted({v["ticker"].split("/", 1)[1] for v in entries.values()})
        print(f"  保留 {len(entries)} 个代币化股票（计价币种：{', '.join(quotes)}）")

        out_path = out_dir / f"{exchange_id}_stocks.json"
        if dry_run:
            print(f"  [DRY RUN] 将保存 {len(entries)} 条 → {out_path}")
            for key, e in list(entries.items())[:5]:
                print(f"    {key}: {e['ticker']} - {e['desc']}")
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
        description="从 binance/okx 抓取代币化股票清单并存为符号清单"
    )
    parser.add_argument(
        "--exchange", type=str, nargs="+", default=["binance", "okx"],
        help="交易所 ID（默认 binance, okx）",
    )
    parser.add_argument(
        "--quote", type=str, default="USDT",
        help="只保留指定计价货币（默认 USDT；传 all 不过滤，同 base 多 quote "
             "时优先 USDT）",
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
        fetch_stock_symbols(
            exchanges=args.exchange,
            quote=None if args.quote == "all" else args.quote.upper(),
            out_dir=args.out_dir,
            proxy=args.proxy,
            dry_run=args.dry_run,
        )
    )


if __name__ == "__main__":
    main()
