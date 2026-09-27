#!/usr/bin/env python3
"""从 OKX 抓取 xStocks 代币化股票列表，生成 scripts/symbols/okx_xstocks.json。

筛选规则：现货市场 + instId 以 "X" 开头 + instCategory == "3"。
instCategory 是 OKX 官方的品种分类标记（3 = 美股代币化/xStocks），
比单纯按 "X 开头" 更精确：可自动排除 XAUT（Tether Gold）、XCH（Chia）、
XAU/XAG（贵金属现货）等同样以 X 开头的非股票品种。

输出格式与 yahoo_symbols.json 一致（ticker/desc/unit/freq/comment），
ticker 为 ccxt unified symbol（如 "XAAPL/USDT"），可直接用于
ccxt okx 的 fetchOHLCV / 数据集注册。

用法：
    python3 fetch_okx_xstocks.py                        # 自动读 HTTPS_PROXY 环境变量
    python3 fetch_okx_xstocks.py --proxy http://127.0.0.1:7897
    python3 fetch_okx_xstocks.py --out /tmp/x.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import ccxt

OUTPUT_DEFAULT = Path(__file__).resolve().parent / "symbols" / "okx_xstocks.json"

# 已知标的映射：OKX base（去 X 前缀后的美股/ETF 代码）-> 中文描述
KNOWN_SYMBOLS: dict[str, str] = {
    # ---- 美股个股 ----
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
    "XQCOM": "Qualcomm（高通，手机 SoC）",
    "XRDDT": "Reddit（社区平台）",
    "XRIVN": "Rivian（电动车）",
    "XRKLB": "Rocket Lab（商业航天）",
    "XROK": "Rockwell Automation（罗克韦尔，工业自动化）",
    "XSMCI": "Super Micro Computer（超微电脑，AI 服务器）",
    "XSNDK": "SanDisk（NAND 存储独立后主体）",
    "XSNOW": "Snowflake（云数据仓库）",
    "XTER": "Teradyne（泰瑞达，半导体测试设备）",
    "XTTWO": "Take-Two Interactive（游戏发行商）",
    "XTWLO": "Twilio（通信 API 平台）",
    "XTSM": "TSMC（台积电，芯片代工龙头）",
    "XUNH": "UnitedHealth（联合健康）",
    "XVRT": "Vertiv（维谛，数据中心电源/散热）",
    "XWDC": "Western Digital（西部数据，HDD）",
    "XZM": "Zoom（视频会议）",
    # ---- ETF（美股上市基金）----
    "XEWY": "iShares MSCI South Korea ETF（韩国市场 ETF）",
    "XIWM": "iShares Russell 2000 ETF（美国小盘股 ETF）",
    "XQQQ": "Invesco QQQ（纳斯达克 100 ETF）",
    "XSMH": "VanEck Semiconductor ETF（半导体行业 ETF）",
    "XSOXL": "Direxion Semiconductor Bull 3X ETF（半导体 3 倍做多 ETF）",
    "XSOXS": "Direxion Semiconductor Bear 3X ETF（半导体 3 倍做空 ETF）",
    "XSPY": "SPDR S&P 500 ETF（标普 500 ETF，全球最大 ETF）",
    "XTQQQ": "ProShares UltraPro QQQ（纳指 100 三倍杠杆 ETF）",
    "XXLE": "Energy Select Sector SPDR（标普能源行业 ETF）",
    # ---- 非美股上市主体的代币化（港股/私募等）----
    "XPOPMART": "泡泡玛特（港股 9992.HK 代币化）",
    "XXIAOMI": "小米集团（港股 1810.HK 代币化）",
    "XSHEIN": "SHEIN（未上市公司私募份额代币化）",
    "XSTRC": "Strategy (STRC) 可变利率优先股",
}
# 疑似变体代码的描述模板（如 XAPLD/XINTW/XMUU 等带后缀的变体）
_VARIANT_HINTS = ("XAPLD", "XINTW", "XMUU", "XMVLL", "XTESTA")


def _desc_for(base: str) -> str:
    if base in KNOWN_SYMBOLS:
        return f"代币化股票：{KNOWN_SYMBOLS[base]}"
    core = base[1:]
    if base in _VARIANT_HINTS:
        return f"OKX xStocks 代币化标的（{core} 关联变体代码，官方名称待确认）"
    return f"OKX xStocks 系列代币化标的（代码 {core}，官方名称待确认）"


def _comment_for(base: str, symbol: str) -> str:
    head = "OKX xStocks 代币化标的，7×24 交易。ticker 为 ccxt unified symbol，可直接用于 ccxt okx fetchOHLCV。"
    core = base[1:]
    if base in ("XSOXL", "XSOXS", "XTQQQ"):
        return head + " 杠杆 ETF 波动放大，仅适合短线趋势观察。"
    if base in ("XPOPMART", "XXIAOMI", "XSHEIN", "XSTRC"):
        return head + " 非美股上市主体（港股/私募/优先股）的代币化版本。"
    if base in _VARIANT_HINTS:
        return head + f" {base} 为 {core} 关联变体代码，对应标的待确认。"
    extra = ""
    if base == "XMU":
        extra = " 与 scripts/symbols/yahoo_symbols.json 中 Micron (MU) 对应。"
    elif base == "XINTC":
        extra = " 与 scripts/symbols/yahoo_symbols.json 中 Intel (INTC) 对应。"
    return f"OKX xStocks 代币化股票，7×24 交易，追踪 {core} 美股价格。" \
           f"ticker 为 ccxt unified symbol，可直接用于 ccxt okx fetchOHLCV。{extra}"


def _build_exchange(proxy: str | None) -> ccxt.Exchange:
    config: dict = {"enableRateLimit": True}
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
    return ccxt.okx(config)


def fetch_xstocks(exchange: ccxt.Exchange) -> dict[str, dict]:
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
        if not is_stock_token:
            continue
        # 同一 base 存在 USDT/USDC 交易对时优先 USDT（流动性主市场）
        base = market["base"]
        current = picked.get(base)
        prefer = current is None or (
            market["quote"] == "USDT" and current["market"]["quote"] != "USDT"
        )
        if prefer:
            picked[base] = {"market": market, "inst_id": inst_id}
    return picked


def build_entries(picked: dict[str, dict]) -> dict[str, dict]:
    entries: dict[str, dict] = {}
    for base in sorted(picked):
        item = picked[base]
        symbol = item["market"]["symbol"]
        entries[base] = {
            "ticker": symbol,
            "desc": _desc_for(base),
            "unit": "USD",
            "freq": "daily",
            "comment": _comment_for(base, symbol),
        }
    return entries


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--proxy", default=None, help="HTTP(S) 代理地址（也可用 HTTPS_PROXY 环境变量）")
    parser.add_argument("--out", type=Path, default=OUTPUT_DEFAULT,
                        help=f"输出路径（默认 {OUTPUT_DEFAULT}）")
    args = parser.parse_args()

    exchange = _build_exchange(args.proxy)
    try:
        picked = fetch_xstocks(exchange)
    except ccxt.BaseError as exc:
        print(f"ccxt 错误：{exc}", file=sys.stderr)
        return 1

    if not picked:
        print("未筛选到任何 xStocks 代币，检查筛选条件或网络", file=sys.stderr)
        return 1

    entries = build_entries(picked)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(entries, ensure_ascii=False, indent=4) + "\n", encoding="utf-8")
    print(f"共 {len(entries)} 个 xStocks 代币 -> {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
