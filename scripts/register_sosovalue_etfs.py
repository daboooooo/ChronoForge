#!/usr/bin/env python3
"""注册 SoSoValue ETF 资金流数据集到 ChronoForge registry。

范围（source=sosovalue）:
    - US-{symbol} 汇总日频流向 4 指标（summary-history 端点）
      total_net_inflow→FLOW / total_value_traded→FLOW /
      total_net_assets→NUMBER / cum_net_inflow→NUMBER
    - 分 ETF 日频 net_inflow（etf-history 端点，ticker 经 GET /etfs
      动态发现，1 次 API 调用）

行为:
    - 注册前先 _get_config 预检窗口合法性 + get_dataset 检测是否已注册，
      已存在则跳过（幂等，可重复执行）
    - available_from = 注册日往前 30 天（demo 计划实测回看边界）
    - 执行后分组输出汇总统计（新增/已存在/失败），任一失败退出码 1
    - 窗口语义由 windows._get_config 的 sosovalue_ 前缀回退解析
      （全窗口 diff，同 fred_series）

用法:
    python scripts/register_sosovalue_etfs.py                 # BTC/US
    python scripts/register_sosovalue_etfs.py --dry-run       # 仅打印计划
    python scripts/register_sosovalue_etfs.py --symbol ETH    # 其他资产
    python scripts/register_sosovalue_etfs.py --verbose       # 逐条输出详情
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime, timedelta
from typing import Any

from chronoforge.cli._wiring import open_meta
from chronoforge.config.settings import Settings
from chronoforge.connectors.sosovalue import SoSoValueConnector
from chronoforge.pipeline.windows import _get_config
from chronoforge.registry.service import (
    add_dataset,
    bootstrap_defaults,
    get_dataset,
)

# 汇总指标 → canonical 类型（日流量 → FLOW，累计/存量 → NUMBER）
_SUMMARY_METRICS: dict[str, str] = {
    "total_net_inflow": "FLOW",
    "total_value_traded": "FLOW",
    "total_net_assets": "NUMBER",
    "cum_net_inflow": "NUMBER",
}

_TICKER_METRIC = "net_inflow"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="注册 SoSoValue ETF 资金流数据集（汇总 + 分 ETF 净流入）"
    )
    parser.add_argument(
        "--symbol", type=str, default="BTC",
        help="标的资产（默认 BTC，可选 ETH/SOL/XRP 等）",
    )
    parser.add_argument(
        "--country", type=str, default="US",
        help="上市国家（默认 US，可选 HK）",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="仅打印计划，不写入注册表",
    )
    parser.add_argument(
        "--verbose", action="store_true", help="逐条输出注册详情",
    )
    return parser.parse_args()


def build_summary_specs(
    symbol: str, country_code: str
) -> list[dict[str, Any]]:
    """构造 summary-history 4 指标 spec。"""
    prefix = f"etf_{country_code.lower()}_{symbol.lower()}_summary"
    specs: list[dict[str, Any]] = []
    for metric, ctype in _SUMMARY_METRICS.items():
        entity_id = f"{prefix}_{metric}"
        specs.append({
            "dataset_id": f"sosovalue_{entity_id}",
            "source_id": "sosovalue",
            "canonical_type": ctype,
            "entity_id": entity_id,
            "params": {
                "endpoint": "summary-history",
                "symbol": symbol,
                "country_code": country_code,
                "metric": metric,
            },
            "frequency": "1d",
            "desc": f"SoSoValue US {symbol} ETF summary {metric}",
        })
    return specs


def build_ticker_spec(
    ticker: str, symbol: str, country_code: str
) -> dict[str, Any]:
    """构造单个 ticker 的 etf-history spec（仅 net_inflow）。"""
    entity_id = (
        f"etf_{country_code.lower()}_{symbol.lower()}_{ticker}_{_TICKER_METRIC}"
    )
    return {
        "dataset_id": f"sosovalue_{entity_id}",
        "source_id": "sosovalue",
        "canonical_type": "FLOW",
        "entity_id": entity_id,
        "params": {
            "endpoint": "etf-history",
            "symbol": symbol,
            "country_code": country_code,
            "ticker": ticker,
            "metric": _TICKER_METRIC,
        },
        "frequency": "1d",
        "desc": f"SoSoValue {ticker} net_inflow",
    }


def register(
    symbol: str, country_code: str, dry_run: bool, verbose: bool
) -> int:
    """注册全部 spec，返回退出码（0 成功 / 1 有失败）。"""
    settings = Settings.load()
    if settings.sosovalue_api_key.is_empty():
        print(
            "错误: SOSOVALUE_API_KEY 未配置（.env 或环境变量）",
            file=sys.stderr,
        )
        return 1

    # ticker 发现（1 次 API 调用；dry-run 同样需要真实清单）
    connector = SoSoValueConnector(settings=settings)
    try:
        etfs = connector.list_etfs(symbol, country_code)
    finally:
        connector.close()
    tickers = [str(e["ticker"]) for e in etfs if e.get("ticker")]
    if not tickers:
        print(
            f"错误: {symbol}/{country_code} 未发现任何 ETF 清单",
            file=sys.stderr,
        )
        return 1
    print(f"发现 {len(tickers)} 个 {country_code} {symbol} 现货 ETF: "
          f"{', '.join(tickers)}")

    specs = build_summary_specs(symbol, country_code)
    specs.extend(build_ticker_spec(t, symbol, country_code) for t in tickers)

    # available_from = 注册日 - 30 天（demo 计划实测回看边界）
    available_from = (
        datetime.now(UTC).replace(tzinfo=None) - timedelta(days=30)
    ).date().isoformat()

    meta = None
    added = existed = failed = 0
    failures: list[tuple[str, str]] = []

    if not dry_run:
        meta = open_meta(settings)
        n = bootstrap_defaults(meta)
        if verbose:
            print(f"bootstrap_defaults: {n} 个数据源（幂等）")

    print(f"\n{'=' * 60}\nSoSoValue ETF 注册计划: {len(specs)} 个数据集"
          f"（available_from={available_from}）\n{'=' * 60}")

    for spec in specs:
        ds_id = spec["dataset_id"]
        try:
            # 校验 dataset_id 可被窗口配置解析（否则 backfill 阶段才炸）
            _get_config(ds_id)

            if meta is not None and get_dataset(meta, ds_id) is not None:
                existed += 1
                if verbose:
                    print(f"  [SKIP] {ds_id}（已注册）")
                continue

            if dry_run:
                added += 1
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
                available_from=available_from,
            )
            added += 1
            if verbose:
                print(f"  [OK] {ds_id} ({spec['desc']})")
        except Exception as e:
            failed += 1
            failures.append((ds_id, str(e)))
            print(f"  [FAIL] {ds_id}: {e}", file=sys.stderr)

    print(f"\n{'=' * 60}\n汇总统计\n{'=' * 60}")
    print(f"  共 {len(specs):>3} 条 | 新增 {added:<3} 已存在 {existed:<3}"
          f" 失败 {failed:<3}")
    for ds_id, err in failures:
        print(f"  失败明细: {ds_id}: {err}", file=sys.stderr)

    if failed == 0 and added > 0 and not dry_run:
        start = available_from
        print(
            f"\n首跑示例（incremental 无 cursor 无 start 时空窗口，"
            f"首跑必须显式 --start）:\n"
            f"  chronoforge pipeline run "
            f"--dataset sosovalue_etf_{country_code.lower()}_{symbol.lower()}"
            f"_summary_total_net_inflow --mode incremental --start {start}"
        )

    if meta is not None:
        meta.close()
    return 1 if failed > 0 else 0


def main() -> None:
    args = parse_args()
    raise SystemExit(
        register(args.symbol, args.country, args.dry_run, args.verbose)
    )


if __name__ == "__main__":
    main()
