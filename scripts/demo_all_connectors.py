#!/usr/bin/env python3
"""全 connector 演示脚本：覆盖 ChronoForge 所有数据源与主要功能模块。

覆盖 7 个 connector：
    binance_spot      klines + aggTrades（公开，无需 key）
    binance_futures   klines + funding + open_interest（公开）
    ccxt (okx)        ohlcv + trades（公开）
    deribit           chart + option_summary（公开）
    yahoo             chart（公开）
    fred              series（需 FRED_API_KEY，缺失时自动跳过）
    sec_edgar         submissions（需 sec_contact_email，缺失时自动跳过）

覆盖的功能模块：
    1. registry   —— add_dataset / get_dataset / list_sources / list_datasets
    2. pipeline   —— PipelineRunner.run_windowed（七阶段：fetch → raw →
                     validate → normalize → canonical → quality → runlog）
    3. research   —— open_query_service / DuckDBQueryService.query
    4. features   —— FeatureRegistry（returns / realized_vol）
    5. quality    —— quality.rules.run 显式质量检查
    6. storage    —— RawStore / CanonicalStoreImpl（经 pipeline 间接覆盖）

dataset_id 命名约束（pipeline/windows.py WINDOW_CONFIG）：
    - 精确键：fred_series / sec_submissions / binance_spot_aggtrades /
      binance_futures_funding / binance_futures_open_interest /
      deribit_open_interest / ccxt_trades
    - 或 ID 含 ohlcv/klines 词元（如 YAHOO:AAPL:OHLCV:1d）→ klines 窗口语义

用法:
    python scripts/demo_all_connectors.py                # 全量演示
    python scripts/demo_all_connectors.py --dry-run      # 仅打印计划
    python scripts/demo_all_connectors.py --days 7       # 调整回填天数
    python scripts/demo_all_connectors.py --skip-network # 只跑注册/查询/特征

依赖:
    - 公开源无需 API Key；FRED 需 .env 中 FRED_API_KEY
    - SEC 需 .env 中 SEC_CONTACT_EMAIL（fair-access 政策要求 UA 声明）
    - 网络受限环境请先设置 HTTPS_PROXY（connector 走环境代理）
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

import polars as pl

from chronoforge.cli._wiring import (
    build_connector,
    open_meta,
    open_query_service,
)
from chronoforge.config.settings import Settings
from chronoforge.features.builtin import RealizedVolFeature, ReturnsFeature
from chronoforge.features.engine import FeatureRegistry
from chronoforge.pipeline.runner import PipelineRunner, RunContext
from chronoforge.pipeline.windows import AcquisitionJob
from chronoforge.quality.rules import run as run_quality_rules
from chronoforge.registry.service import (
    bootstrap_defaults,
    get_dataset,
    list_sources,
    list_datasets,
    add_dataset,
)

# ---------------------------------------------------------------------------
# 演示数据集规划
# ---------------------------------------------------------------------------

# 今天（UTC），各数据集的回填起点按回溯天数计算
TODAY = datetime(2026, 9, 24)

# (source_id, dataset_id, canonical_type, entity_id, params, lookback_days, frequency)
DEMO_DATASETS: list[dict[str, Any]] = [
    # ── binance_spot ───────────────────────────────────────────────
    {
        "source_id": "binance_spot",
        "dataset_id": "BINANCE_SPOT:BTCUSDT:OHLCV:1h",
        "canonical_type": "OHLCV",
        "entity_id": "BTCUSDT",
        "params": {"symbol": "BTCUSDT", "interval": "1h"},
        "lookback_days": 30,
        "frequency": "1h",
        "desc": "Binance 现货 K 线",
    },
    {
        "source_id": "binance_spot",
        "dataset_id": "binance_spot_aggtrades",
        "canonical_type": "TRADE",
        "entity_id": "BTCUSDT",
        "params": {"symbol": "BTCUSDT", "data_type": "aggregate"},
        "lookback_days": 1,
        "frequency": "tick",
        "desc": "Binance 现货归集成交（aggTrades）",
    },
    # ── binance_futures ────────────────────────────────────────────
    {
        "source_id": "binance_futures",
        "dataset_id": "BINANCE_FUTURES:BTCUSDT:OHLCV:1h",
        "canonical_type": "OHLCV",
        "entity_id": "BTCUSDT",
        "params": {"symbol": "BTCUSDT", "interval": "1h"},
        "lookback_days": 30,
        "frequency": "1h",
        "desc": "Binance U 本位合约 K 线",
    },
    {
        "source_id": "binance_futures",
        "dataset_id": "binance_futures_funding",
        "canonical_type": "FUNDING",
        "entity_id": "BTCUSDT",
        "params": {"symbol": "BTCUSDT", "data_type": "funding"},
        "lookback_days": 7,
        "frequency": "8h",
        "desc": "Binance 合约资金费率",
    },
    {
        "source_id": "binance_futures",
        "dataset_id": "binance_futures_open_interest",
        "canonical_type": "OPEN_INTEREST",
        "entity_id": "BTCUSDT",
        "params": {"symbol": "BTCUSDT", "data_type": "open_interest"},
        "lookback_days": 7,
        "frequency": "point",
        "desc": "Binance 合约持仓量",
    },
    # ── ccxt (okx) ─────────────────────────────────────────────────
    {
        "source_id": "ccxt",
        "dataset_id": "CCXT:BTCUSDT:OHLCV:1h",
        "canonical_type": "OHLCV",
        "entity_id": "BTCUSDT",
        "params": {"symbol": "BTC/USDT", "interval": "1h", "exchange": "okx"},
        "lookback_days": 30,
        "frequency": "1h",
        "desc": "OKX（经 ccxt）K 线",
    },
    {
        "source_id": "ccxt",
        "dataset_id": "ccxt_trades",
        "canonical_type": "TRADE",
        "entity_id": "BTCUSDT",
        "params": {
            "symbol": "BTC/USDT",
            "exchange": "okx",
            "type": "trades",
        },
        "lookback_days": 1,
        "frequency": "tick",
        "desc": "OKX（经 ccxt）逐笔成交",
    },
    # ── deribit ────────────────────────────────────────────────────
    {
        "source_id": "deribit",
        "dataset_id": "DERIBIT:BTC-PERPETUAL:OHLCV:1h",
        "canonical_type": "OHLCV",
        "entity_id": "BTC-PERPETUAL",
        "params": {
            "instrument_name": "BTC-PERPETUAL",
            "data_type": "chart",
            "resolution": "60",  # deribit 分钟数 → 1h
        },
        "lookback_days": 7,
        "frequency": "1h",
        "desc": "Deribit BTC 永续合约 K 线",
    },
    {
        "source_id": "deribit",
        "dataset_id": "deribit_open_interest",
        "canonical_type": "OPTION",
        "entity_id": "BTC",
        "params": {"currency": "BTC", "data_type": "option_summary"},
        "lookback_days": 7,
        "frequency": "point",
        "desc": "Deribit BTC 期权摘要（含 OI/IV）",
    },
    # ── yahoo ──────────────────────────────────────────────────────
    {
        "source_id": "yahoo",
        "dataset_id": "YAHOO:AAPL:OHLCV:1d",
        "canonical_type": "OHLCV",
        "entity_id": "AAPL",
        "params": {"symbol": "AAPL", "interval": "1d"},
        "lookback_days": 270,
        "frequency": "1d",
        "desc": "Yahoo Finance 苹果日线",
    },
    # ── fred（需 API key）─────────────────────────────────────────
    {
        "source_id": "fred",
        "dataset_id": "fred_series",
        "canonical_type": "NUMBER",
        "entity_id": "DGS10",
        "params": {"series_id": "DGS10"},  # 10 年期国债收益率
        "lookback_days": 990,
        "frequency": "1d",
        "desc": "FRED DGS10 利率序列（需 FRED_API_KEY）",
    },
    # ── sec_edgar（需联系邮箱）────────────────────────────────────
    {
        "source_id": "sec_edgar",
        "dataset_id": "sec_submissions",
        "canonical_type": "FILING",
        "entity_id": "320193",  # Apple Inc.
        "params": {"cik": "320193", "data_type": "filing"},
        "lookback_days": 0,  # full window（最近 filings）
        "frequency": "event",
        "desc": "SEC EDGAR Apple 提交文件（需 SEC_CONTACT_EMAIL）",
    },
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="全 connector 功能演示")
    parser.add_argument("--dry-run", action="store_true", help="仅打印计划")
    parser.add_argument(
        "--skip-network",
        action="store_true",
        help="跳过网络回填，仅注册/查询/特征/质量",
    )
    parser.add_argument(
        "--days", type=int, default=None, help="覆盖所有 lookback 天数（调试用）"
    )
    return parser.parse_args()


def section(title: str) -> None:
    print(f"\n{'=' * 64}\n  {title}\n{'=' * 64}")


# ---------------------------------------------------------------------------
# 阶段 1：注册表
# ---------------------------------------------------------------------------


def step_registry(meta: Any, dry_run: bool) -> list[dict[str, Any]]:
    """注册演示数据集（幂等 upsert），返回实际可回填的列表。"""
    section("阶段 1: 注册表（registry）")

    n = bootstrap_defaults(meta)
    sources = list_sources(meta)
    print(f"数据源注册表（bootstrap_defaults 幂等写入 {n} 条）:")
    for s in sources:
        print(f"  - {s['source_id']:<16} {s['display_name']}")

    planned: list[dict[str, Any]] = []
    for spec in DEMO_DATASETS:
        ds_id = spec["dataset_id"]
        if dry_run:
            print(f"[DRY RUN] 将注册: {ds_id} ({spec['desc']})")
            planned.append(spec)
            continue
        try:
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
            row = get_dataset(meta, ds_id)
            print(f"  已注册: {ds_id}  -> {spec['desc']}")
            if row is not None:
                planned.append(spec)
        except Exception as exc:
            print(f"  注册失败: {ds_id}: {exc}", file=sys.stderr)

    total = len(list_datasets(meta, enabled_only=True))
    print(f"注册表现共 {total} 个 dataset（含历史注册）")
    return planned


# ---------------------------------------------------------------------------
# 阶段 2：Pipeline 回填（七阶段）
# ---------------------------------------------------------------------------


def step_pipeline(
    meta: Any,
    settings: Settings,
    planned: list[dict[str, Any]],
    days_override: int | None,
    dry_run: bool,
) -> dict[str, str]:
    """对每个演示数据集执行 run_windowed backfill，返回 dataset_id -> 状态。"""
    section("阶段 2: Pipeline 回填（fetch → raw → validate → normalize → canonical → quality → runlog）")

    status_map: dict[str, str] = {}
    from chronoforge.storage.canonical import CanonicalStoreImpl
    from chronoforge.storage.raw import RawStore

    raw_store: RawStore | None = None
    canonical_store: CanonicalStoreImpl | None = None

    try:
        for spec in planned:
            ds_id = spec["dataset_id"]
            source_id = spec["source_id"]
            lookback = days_override or int(spec["lookback_days"])
            end = TODAY
            start = end - timedelta(days=lookback) if lookback else end - timedelta(days=30)

            print(f"\n>>> {ds_id}  ({spec['desc']})")
            print(
                f"    范围: {start:%Y-%m-%d} ~ {end:%Y-%m-%d}"
                f"  lookback={lookback}d"
            )
            if dry_run:
                print("    [DRY RUN] 跳过执行")
                status_map[ds_id] = "DRY_RUN"
                continue

            try:
                connector = build_connector(source_id, settings, spec["params"])
            except Exception as exc:
                print(f"    [SKIP] connector 构建失败: {exc}")
                status_map[ds_id] = "SKIP(connector)"
                continue

            def ctx_factory(job: AcquisitionJob) -> RunContext:
                nonlocal raw_store, canonical_store
                if raw_store is None:
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
            job = AcquisitionJob(
                dataset_id=ds_id,
                start=start,
                end=end,
                mode="backfill",
                priority=0,
                params=spec["params"],
            )

            try:
                # 上次失败的 dataset 可能处于熔断 open 状态，重置后可重试
                meta.reset_circuit(source_id, ds_id)
                results = runner.run_windowed(job, window_seconds=86400)
                if not results:
                    print("    无窗口可跑（checkpoint 已覆盖）")
                    status_map[ds_id] = "UP-TO-DATE"
                else:
                    ok = 0
                    total_out = 0
                    for r in results:
                        total_out += r.output_count
                        if r.status in ("SUCCESS", "PARTIAL_SUCCESS"):
                            ok += 1
                    status = (
                        "OK" if ok == len(results)
                        else "PARTIAL" if ok else "FAIL"
                    )
                    print(
                        f"    {len(results)} 个窗口: {ok} 成功, "
                        f"输出 {total_out} 条 → {status}"
                    )
                    status_map[ds_id] = status
            except Exception as exc:
                print(f"    [FAIL] {type(exc).__name__}: {exc}", file=sys.stderr)
                status_map[ds_id] = "FAIL"
            finally:
                connector.close()
    finally:
        if raw_store:
            raw_store.close()

    return status_map


# ---------------------------------------------------------------------------
# 阶段 3：查询（research.query）
# ---------------------------------------------------------------------------


def step_query(meta: Any, settings: Settings) -> list[str]:
    """通过 DuckDBQueryService 查询各数据集，返回可查询的 dataset_id 列表。"""
    section("阶段 3: 数据查询（DuckDBQueryService）")

    service, _con = open_query_service(settings, meta)
    queryable: list[str] = []

    for spec in DEMO_DATASETS:
        ds_id = spec["dataset_id"]
        try:
            result = service.query(ds_id, limit=5)
            frame: pl.DataFrame = result.frame
            n = frame.height
            print(
                f"  {ds_id:<36} 类型={spec['canonical_type']:<14}"
                f" 样本={n} 行 truncated={result.truncated}"
            )
            if n:
                queryable.append(ds_id)
        except Exception as exc:
            print(f"  {ds_id:<36} 查询失败: {exc}")

    # 打印一个 OHLCV 样本
    try:
        r = service.query(
            "BINANCE_SPOT:BTCUSDT:OHLCV:1h",
            columns=["event_time", "open", "high", "low", "close", "volume"],
            limit=3,
        )
        print("\n  OHLCV 样本（最近 3 行）:")
        for row in r.frame.to_dicts():
            print(
                f"    {row['event_time']}  O={row['open']:.2f} "
                f"H={row['high']:.2f} L={row['low']:.2f} "
                f"C={row['close']:.2f} V={row['volume']:.2f}"
            )
    except Exception as exc:
        print(f"  样本查询失败: {exc}")

    return queryable


# ---------------------------------------------------------------------------
# 阶段 4：特征计算（features）
# ---------------------------------------------------------------------------


def step_features(meta: Any, settings: Settings) -> None:
    """用 FeatureRegistry 计算 returns / realized_vol（基于 ohlcv 视图）。"""
    section("阶段 4: 特征计算（features）")

    # 内置特征 dependencies=["ohlcv"]：registry 需存在 dataset_id="ohlcv"
    # （查询时按 canonical_type 解析到全局 ohlcv 视图，注册为幂等别名）
    add_dataset(
        meta,
        dataset_id="ohlcv",
        source_id="binance_spot",
        canonical_type="OHLCV",
        entity_id="BTCUSDT",
        params={"symbol": "BTCUSDT", "interval": "1h"},
        frequency="1h",
        continuity_model="ALWAYS_OPEN",
        revision_supported=False,
    )

    service, _con = open_query_service(settings, meta)
    registry = FeatureRegistry()
    registry.register(ReturnsFeature())
    registry.register(RealizedVolFeature())

    for name, params in (
        ("returns", {}),
        ("realized_vol", {"window": 24}),
    ):
        try:
            result = registry.compute(name, service, params)
            df = result.frame
            valid = df.drop_nulls()
            tail = valid.tail(3)
            print(f"  特征 {name:<14} 输出 {df.height} 行（有效 {valid.height}）")
            for row in tail.to_dicts():
                col = name
                val = row.get(col)
                ts = row.get("event_time")
                print(f"    {ts}  {col}={val:.6f}" if val is not None else f"    {ts}  {col}=null")
        except Exception as exc:
            print(f"  特征 {name} 计算失败: {exc}")


# ---------------------------------------------------------------------------
# 阶段 5：质量规则（quality）
# ---------------------------------------------------------------------------


def step_quality(meta: Any, settings: Settings) -> None:
    """对最近一批 OHLCV 记录显式执行质量规则。"""
    section("阶段 5: 质量规则（quality.rules.run）")

    service, _con = open_query_service(settings, meta)
    try:
        # 全列查询：质量规则需要 natural key、provenance 等完整字段，
        # 缺列会误报 Q-PROV-001/Q-SCHEMA-001
        result = service.query(
            "BINANCE_SPOT:BTCUSDT:OHLCV:1h",
            limit=200,
        )
        records = result.frame.to_dicts()
        # interval 枚举对象 -> 字符串，规则按字符串处理
        for rec in records:
            if hasattr(rec.get("interval"), "value"):
                rec["interval"] = rec["interval"].value
        from chronoforge.models.enums import CanonicalType

        report = run_quality_rules(records, CanonicalType.OHLCV)
        findings = report.findings
        print(f"  检查 {len(records)} 条 OHLCV 记录: {len(findings)} 个发现")
        for f in findings[:10]:
            print(f"    [{f.severity}] {f.rule_id}: {f.detail[:80]}")
        if not findings:
            print("    （全部通过）")
    except Exception as exc:
        print(f"  质量检查失败: {exc}")


# ---------------------------------------------------------------------------
# 阶段 6：run_log 审计
# ---------------------------------------------------------------------------


def step_runlog(meta: Any) -> None:
    section("阶段 6: 运行审计（run_log 最近 10 条）")
    try:
        rows = meta.connection.execute(
            "SELECT dataset_id, status, started_at, output_count "
            "FROM run_log ORDER BY started_at DESC LIMIT 10"
        ).fetchall()
        for r in rows:
            print(f"  {str(r[1]):<16} out={r[3] or 0:<8} {r[0]}  @ {r[2]}")
    except Exception as exc:
        print(f"  run_log 查询失败: {exc}")


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------


def main() -> None:
    args = parse_args()
    started = datetime.now(timezone.utc).replace(tzinfo=None)

    section("ChronoForge 全 connector 演示")
    settings = Settings.load()

    # 环境提示
    proxy = (
        settings.__dict__.get("https_proxy")
        or __import__("os").environ.get("HTTPS_PROXY")
        or __import__("os").environ.get("https_proxy")
    )
    print(f"数据目录: {settings.data_dir}")
    print(f"HTTPS_PROXY: {proxy or '（未设置——网络受限环境请先设置）'}")

    try:
        has_fred = bool(settings.get_secret("fred_api_key").get_secret_value())
    except Exception:
        has_fred = False
    try:
        has_sec = bool(
            settings.get_secret("sec_contact_email").get_secret_value()
        )
    except Exception:
        has_sec = False
    print(f"FRED_API_KEY: {'已配置' if has_fred else '缺失（fred 数据集将跳过）'}")
    print(f"SEC_CONTACT_EMAIL: {'已配置' if has_sec else '缺失（sec 数据集将跳过）'}")

    meta = open_meta(settings, repair=True)
    try:
        # 阶段 1
        planned = step_registry(meta, dry_run=args.dry_run)

        # 凭据缺失的数据源剔除（避免回填阶段报错）
        if not has_fred:
            planned = [s for s in planned if s["source_id"] != "fred"]
        if not has_sec:
            planned = [s for s in planned if s["source_id"] != "sec_edgar"]

        # 阶段 2
        status_map: dict[str, str] = {}
        if args.skip_network:
            section("阶段 2: Pipeline 回填（--skip-network，跳过）")
            status_map = {s["dataset_id"]: "SKIPPED" for s in planned}
        else:
            status_map = step_pipeline(
                meta, settings, planned, args.days, args.dry_run
            )

        if not args.dry_run:
            # 阶段 3
            step_query(meta, settings)
            # 阶段 4
            step_features(meta, settings)
            # 阶段 5
            step_quality(meta, settings)
            # 阶段 6
            step_runlog(meta)

        # 汇总
        section("演示汇总")
        ok = sum(1 for v in status_map.values() if v == "OK")
        print(f"  回填结果: {ok}/{len(status_map)} 全部成功")
        for ds_id, st in status_map.items():
            print(f"    [{st:<14}] {ds_id}")
        elapsed = (
            datetime.now(timezone.utc).replace(tzinfo=None) - started
        ).total_seconds()
        print(f"\n  总耗时: {elapsed:.0f}s")
    finally:
        meta.close()


if __name__ == "__main__":
    main()
