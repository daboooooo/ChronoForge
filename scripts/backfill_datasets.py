#!/usr/bin/env python3
"""批量回填注册表中已注册 dataset 的历史数据（backfill-only）。

从注册表自动发现符合过滤条件的已注册 dataset（全部 canonical 类型：
OHLCV/FLOW/NUMBER 等），逐个执行 pipeline backfill，并对执行结果进行
分类统计与报告。注册职责由 register_symbols.py / register_sosovalue_etfs.py
承担。

用法:
    # 回填注册表中全部已注册 dataset（自动发现所有类型，建议先 --dry-run 预览）
    python scripts/backfill_datasets.py

    # 指定回填时间范围
    python scripts/backfill_datasets.py --start 2022-01-01 --end 2026-09-22

    # 只回填指定数据源 / 实体（entity_id）
    python scripts/backfill_datasets.py --sources binance_spot ccxt
    python scripts/backfill_datasets.py --entities BTCUSDT ETHUSDT

    # 只回填指定 canonical 类型（默认全部）
    python scripts/backfill_datasets.py --canonical-types OHLCV
    python scripts/backfill_datasets.py --canonical-types FLOW NUMBER

    # 只回填指定 K 线周期（仅作用于 params 含 interval 的 K 线类 dataset）
    python scripts/backfill_datasets.py --intervals 1d

    # 回填前重置熔断器（连续失败触发 dataset 熔断 CANCELLED 后恢复用）
    python scripts/backfill_datasets.py --reset-circuit

    # 预览不执行
    python scripts/backfill_datasets.py --dry-run

输出:
    默认单行实时进度（\\r 原地刷新：dataset 进度 / 窗口进度 / 百分比 /
    已用 / 预估剩余）；pipeline JSON 日志默认 WARNING+ 走 stderr，
    --verbose 恢复 INFO 全量日志。Ctrl+C 中断安全（当前窗口记
    CANCELLED，已完成数据已落盘，重跑自动续传）。

依赖:
    - 多数数据源连接器为公开接口；SoSoValue 需 .env 中配置
      SOSOVALUE_API_KEY（仅可回看最近 30 天，更早窗口自动钳制为空产出）
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from chronoforge.cli._wiring import build_connector, open_meta
from chronoforge.config.settings import Settings
from chronoforge.logging import setup_logging
from chronoforge.pipeline.runner import PipelineRunner, RunContext
from chronoforge.pipeline.windows import AcquisitionJob
from chronoforge.registry.service import list_datasets

DEFAULT_START = datetime(2021, 1, 1)
DEFAULT_INTERVALS = ["1h", "4h", "1d"]

_OK_STATUSES = ("SUCCESS", "PARTIAL_SUCCESS")


def _default_end() -> datetime:
    """默认回填终点：当前 UTC 时间（裸 datetime，与 pipeline 时间戳语义一致）。"""
    return datetime.now(UTC).replace(tzinfo=None)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="批量回填注册表中已注册 dataset 的历史数据（自动发现全部类型）"
    )
    parser.add_argument(
        "--sources", type=str, nargs="+", default=None,
        help="只处理指定数据源（如 binance_spot ccxt sosovalue；默认全部）",
    )
    parser.add_argument(
        "--entities", type=str, nargs="+", default=None,
        help="只处理指定实体（entity_id，如 BTCUSDT / etf_us_btc_IBIT_net_inflow；默认全部）",
    )
    parser.add_argument(
        "--canonical-types", type=str, nargs="+", default=None,
        help="只处理指定 canonical 类型（如 OHLCV FLOW NUMBER；默认全部已注册类型）",
    )
    parser.add_argument(
        "--intervals", type=str, nargs="+", default=DEFAULT_INTERVALS,
        help=f"只处理指定 K 线周期（仅作用于 params 含 interval 的 dataset，默认 {'/'.join(DEFAULT_INTERVALS)}）",
    )
    parser.add_argument(
        "--start", type=str,
        help=f"回填起始时间（ISO 格式，默认 {DEFAULT_START.strftime('%Y-%m-%d')}）",
    )
    parser.add_argument(
        "--end", type=str,
        help="回填结束时间（ISO 格式，默认当前 UTC 时间）",
    )
    parser.add_argument(
        "--window-seconds", type=int, default=86400,
        help="分窗口大小（秒，默认 86400 = 24h）",
    )
    parser.add_argument(
        "--reset-circuit", action="store_true",
        help="回填前重置每个 dataset 的熔断器（恢复 CANCELLED 状态）",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="仅列出将回填的 dataset，不执行",
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="启用 INFO 级 pipeline 日志并输出每个窗口的 run 详情（默认 WARNING）",
    )
    return parser.parse_args()


def _fmt_duration(seconds: float) -> str:
    """秒 → 紧凑时长（如 1h20m05s / 3m12s / 45s）。"""
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h{m:02d}m{s:02d}s"
    if m:
        return f"{m}m{s:02d}s"
    return f"{s}s"


def _progress_printer(
    idx: int, total: int, dataset_id: str
) -> tuple[Callable[[int, int, Any], None], dict[str, Any]]:
    """构造单 dataset 的 \\r 单行进度回调（run_windowed 每窗口回调一次）。

    返回 (回调, state)；state["printed"] 标记是否实际打印过（全 COVERED
    的 dataset 不会触发回调，调用方据此决定是否补换行）。
    """
    state: dict[str, Any] = {"t0": time.perf_counter(), "out": 0, "err": 0, "printed": False}

    def _cb(done: int, est: int, row: Any) -> None:
        state["out"] += int(row.output_count)
        state["err"] += int(row.error_count)
        state["printed"] = True
        elapsed = time.perf_counter() - state["t0"]
        pct = 100 if est > 0 and done >= est else (done * 100 // est if est > 0 else 0)
        eta = (
            f" 剩余~{_fmt_duration(elapsed / done * (est - done))}"
            if est > done > 0
            else ""
        )
        print(
            f"\r[{idx}/{total}] {dataset_id}"
            f" 窗口 {done}/{est} {pct}%"
            f" 已用 {_fmt_duration(elapsed)}{eta}"
            f" 输出 {state['out']} 错 {state['err']}  ",
            end="", flush=True,
        )

    return _cb, state


def load_backfill_datasets(meta: Any, args: argparse.Namespace) -> list[dict[str, Any]]:
    """从注册表自动发现符合过滤条件的已注册 dataset（全部 canonical 类型）。"""
    results: list[dict[str, Any]] = []
    for row in list_datasets(meta, enabled_only=True):
        if args.canonical_types and row.get("canonical_type") not in args.canonical_types:
            continue
        if args.sources and row.get("source_id") not in args.sources:
            continue
        if args.entities and row.get("entity_id") not in args.entities:
            continue
        params_raw = row.get("params_json") or "{}"
        try:
            params = json.loads(params_raw)
        except json.JSONDecodeError:
            continue
        if not isinstance(params, dict):
            continue
        # interval 过滤仅作用于 K 线类 dataset（params 含 interval）；
        # FLOW/NUMBER 等非 K 线 dataset 不受 --intervals 影响
        if "interval" in params and params.get("interval") not in args.intervals:
            continue
        results.append(
            {
                "dataset_id": str(row["dataset_id"]),
                "source_id": str(row["source_id"]),
                "entity_id": str(row.get("entity_id") or ""),
                "canonical_type": str(row.get("canonical_type") or ""),
                "params": params,
            }
        )
    return results


def classify_results(results: list[Any]) -> str:
    """将一个 dataset 的窗口结果分类为总体状态。

    CANCELLED 优先级最高（dataset 熔断，需 --reset-circuit 恢复）；
    results 为空表示 checkpoint 已覆盖全部范围，属正常。
    """
    if not results:
        return "COVERED"
    statuses = {r.status for r in results}
    if "CANCELLED" in statuses:
        return "CANCELLED"
    if "FAILED" in statuses:
        return "FAILED"
    if "PARTIAL_SUCCESS" in statuses:
        return "PARTIAL"
    return "SUCCESS"


def backfill_one(
    meta: Any,
    settings: Any,
    ds: dict[str, Any],
    start: datetime,
    end: datetime,
    window_seconds: int,
    progress: Callable[[int, int, Any], None] | None = None,
) -> dict[str, Any]:
    """回填单个 dataset，返回结果记录。"""
    t0 = time.perf_counter()
    record: dict[str, Any] = {
        "dataset_id": ds["dataset_id"],
        "source_id": ds["source_id"],
        "status": "ERROR",
        "windows_total": 0,
        "windows_ok": 0,
        "output_count": 0,
        "error_count": 0,
        "elapsed": 0.0,
        "detail": "",
        "runs": [],
    }
    connector = build_connector(ds["source_id"], settings, ds["params"])
    raw_store = None
    canonical_store = None

    def ctx_factory(job: AcquisitionJob) -> RunContext:
        nonlocal raw_store, canonical_store
        if raw_store is None:
            from chronoforge.storage.canonical import CanonicalStoreImpl
            from chronoforge.storage.raw import RawStore

            raw_store = RawStore(str(settings.data_dir))
            canonical_store = CanonicalStoreImpl(str(settings.data_dir))
        return RunContext(
            run_id="",
            ingest_batch_id="",
            source_id=ds["source_id"],
            dataset_id=job.dataset_id,
            connector=connector,
            raw_store=raw_store,
            canonical_store=canonical_store,
            meta=meta,
            settings=settings,
        )

    try:
        runner = PipelineRunner(ctx_factory)
        job = AcquisitionJob(
            dataset_id=ds["dataset_id"],
            start=start,
            end=end,
            mode="backfill",
            priority=0,
            params=ds["params"],
        )
        results = runner.run_windowed(job, window_seconds=window_seconds, progress=progress)
        record["status"] = classify_results(results)
        record["windows_total"] = len(results)
        record["windows_ok"] = sum(
            1 for r in results if r.status in _OK_STATUSES
        )
        record["output_count"] = sum(r.output_count for r in results)
        record["error_count"] = sum(r.error_count for r in results)
        record["runs"] = [
            (str(r.run_id), str(r.status), int(r.output_count), int(r.error_count))
            for r in results
        ]
    except Exception as e:
        record["status"] = "ERROR"
        record["detail"] = str(e)
        raise
    finally:
        connector.close()
        if raw_store is not None:
            raw_store.close()
        record["elapsed"] = time.perf_counter() - t0
    return record


def print_report(records: list[dict[str, Any]], args: argparse.Namespace) -> int:
    """输出分组统计报告，返回退出码。"""
    print(f"\n{'=' * 70}\n回填报告\n{'=' * 70}")

    by_source: dict[str, list[dict[str, Any]]] = {}
    for r in records:
        by_source.setdefault(r["source_id"], []).append(r)

    status_counter: dict[str, int] = {}
    total_windows = windows_ok = total_output = total_errors = 0
    total_elapsed = 0.0
    failed_records: list[dict[str, Any]] = []

    for source_id in sorted(by_source):
        group = by_source[source_id]
        print(f"\n[{source_id}] {len(group)} 个 dataset")
        for r in sorted(group, key=lambda x: x["dataset_id"]):
            status_counter[r["status"]] = status_counter.get(r["status"], 0) + 1
            total_windows += r["windows_total"]
            windows_ok += r["windows_ok"]
            total_output += r["output_count"]
            total_errors += r["error_count"]
            total_elapsed += r["elapsed"]
            mark = "" if r["status"] in _OK_STATUSES or r["status"] == "COVERED" else "  <--"
            print(
                f"  {r['status']:<9} {r['dataset_id']:<34}"
                f" 窗口 {r['windows_ok']}/{r['windows_total']}"
                f" 输出 {r['output_count']:<4} 错误 {r['error_count']:<3}"
                f" {r['elapsed']:>7.1f}s{mark}"
            )
            if args.verbose:
                for run_id, status, out, err in r["runs"]:
                    print(f"      run={run_id} {status} output={out} errors={err}")
            if r["status"] not in _OK_STATUSES and r["status"] != "COVERED":
                failed_records.append(r)
                if r["detail"]:
                    print(f"      detail: {r['detail']}")

    print(f"\n{'-' * 70}")
    print("汇总:")
    print(f"  dataset: {len(records)} 个 | " + " | ".join(
        f"{k} {v}" for k, v in sorted(status_counter.items())
    ))
    print(
        f"  窗口: {windows_ok}/{total_windows} 成功"
        f" | 输出 chunk {total_output} | 错误 {total_errors}"
        f" | 总耗时 {total_elapsed:.1f}s"
    )
    if failed_records:
        print(f"\n失败明细 ({len(failed_records)}):")
        for r in failed_records:
            hint = (
                "（dataset 熔断，可 --reset-circuit 恢复后重试）"
                if r["status"] == "CANCELLED"
                else ""
            )
            print(f"  {r['dataset_id']}: {r['status']} {hint}{r['detail']}")
    return 1 if failed_records else 0


def run_backfill(args: argparse.Namespace) -> int:
    """执行回填主流程，返回退出码。"""
    setup_logging("INFO" if args.verbose else "WARNING")
    start_str = args.start or DEFAULT_START.strftime("%Y-%m-%d")
    end_str = args.end or _default_end().isoformat(timespec="seconds")
    start = datetime.fromisoformat(start_str)
    end = datetime.fromisoformat(end_str)
    if start >= end:
        print("错误: --start 必须早于 --end", file=sys.stderr)
        return 1

    settings = Settings.load()
    meta = open_meta(settings)
    datasets: list[dict[str, Any]] = []
    try:
        datasets = load_backfill_datasets(meta, args)
    finally:
        meta.close()

    print(f"匹配到 {len(datasets)} 个已注册 dataset")
    print(f"数据源: {sorted({d['source_id'] for d in datasets}) or '-'}")
    print(
        f"类型: {sorted({d['canonical_type'] for d in datasets}) or '-'}"
        f" | 时间范围: {start.strftime('%Y-%m-%d')} ~ {end.strftime('%Y-%m-%d')}"
    )
    print(f"窗口: {args.window_seconds}s | 熔断重置: {'是' if args.reset_circuit else '否'}")

    if args.dry_run:
        for ds in datasets:
            print(f"  [DRY RUN] 将回填: {ds['dataset_id']} ({ds['canonical_type']})")
        return 0
    if not datasets:
        print("错误: 没有匹配的 dataset，请先用注册脚本注册", file=sys.stderr)
        return 1

    meta = open_meta(settings, repair=True)
    records: list[dict[str, Any]] = []
    try:
        if args.reset_circuit:
            for ds in datasets:
                meta.reset_circuit(ds["source_id"], ds["dataset_id"])
            print(f"已重置 {len(datasets)} 个 dataset 的熔断器")

        total = len(datasets)
        for idx, ds in enumerate(datasets, 1):
            print(
                f"\n[{idx}/{total}] 回填: {ds['dataset_id']}"
                f" ({start.strftime('%Y-%m-%d')} ~ {end.strftime('%Y-%m-%d')})"
            )
            progress_cb, pstate = _progress_printer(idx, total, ds["dataset_id"])
            try:
                record = backfill_one(
                    meta, settings, ds, start, end, args.window_seconds,
                    progress=progress_cb,
                )
            except KeyboardInterrupt:
                # run_windowed 已把当前窗口记 CANCELLED 并释放锁；已完成
                # 窗口数据均落盘，checkpoint 支持重跑续传
                record = {
                    "dataset_id": ds["dataset_id"], "source_id": ds["source_id"],
                    "status": "CANCELLED", "windows_total": 0, "windows_ok": 0,
                    "output_count": 0, "error_count": 0, "elapsed": 0.0,
                    "detail": "用户中断（已完成窗口已落盘，重跑可续传）", "runs": [],
                }
                print("\n  已中断（Ctrl+C）", file=sys.stderr)
                records.append(record)
                break
            except Exception as e:
                record = {
                    "dataset_id": ds["dataset_id"], "source_id": ds["source_id"],
                    "status": "ERROR", "windows_total": 0, "windows_ok": 0,
                    "output_count": 0, "error_count": 0, "elapsed": 0.0,
                    "detail": str(e), "runs": [],
                }
                print(f"  回填失败: {e}", file=sys.stderr)
            finally:
                if pstate["printed"]:
                    print()  # 结束 \r 进度行
            records.append(record)
            print(
                f"  -> {record['status']}"
                f" 窗口 {record['windows_ok']}/{record['windows_total']}"
                f" 输出 {record['output_count']} 耗时 {record['elapsed']:.1f}s"
            )
    finally:
        meta.close()

    return print_report(records, args)


def main() -> None:
    raise SystemExit(run_backfill(parse_args()))


if __name__ == "__main__":
    main()
