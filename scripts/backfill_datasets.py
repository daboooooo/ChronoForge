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

    # 到期增量：仅更新到达更新周期的 dataset（hourly_update.sh 每轮轮询使用）
    python scripts/backfill_datasets.py --due-only

    # 预览不执行
    python scripts/backfill_datasets.py --dry-run

输出:
    默认单行实时进度（\\r 原地刷新：dataset 进度 / 窗口进度 / 百分比 /
    已用 / 预估剩余）；pipeline JSON 日志默认 WARNING+ 走 stderr，
    --verbose 恢复 INFO 全量日志。Ctrl+C 中断安全（当前窗口记
    CANCELLED，已完成数据已落盘，重跑自动续传）。

到期增量（--due-only）:
    - 更新周期取 dataset_registry.frequency（1h/4h/1d/1W/1M/1Q/1Y 简写，
      统一由 quality.continuity.parse_frequency_seconds 解析）；
    - 更新时间取 checkpoints.last_success_time（仅 SUCCESS/PARTIAL 更新）；
    - 数据源日内发布时刻取 config.update_schedule（FRED 北京 16:10、
      Binance 北京 08:00、其余北京 00:00）：日频及以上 dataset 的下次
      到期时刻对齐到该发布时刻，避免源未发布时抓取导致数据滞后一天；
    - 小时级（1h/4h 等）按以 UTC 00:00 为原点的周期网格对齐（1h 每小时
      整点、4h 每 4 小时：00/04/08/12/16/20 时），与整点调度同拍；
    - 到期 = 从未成功（补历史）或 frequency 未知/tick（每轮到期，fail-open）
      或 now ≥ 下次到期时刻；失败/熔断 dataset 因 last_success 不推进
      而保持到期，熔断冷却期内仍由 runner 快速 CANCELLED，不打源。
    - 报告同时列出【已更新】与【未更新（未到期，含下次到期时刻）】两段，
      而非只提示哪些更新了。

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
from datetime import UTC, datetime, timedelta
from typing import Any

from chronoforge.cli._wiring import build_connector, open_meta
from chronoforge.config.settings import Settings
from chronoforge.config.update_schedule import publish_time_utc
from chronoforge.logging import setup_logging
from chronoforge.pipeline.runner import PipelineRunner, RunContext
from chronoforge.pipeline.windows import AcquisitionJob
from chronoforge.quality.continuity import parse_frequency_seconds
from chronoforge.registry.service import list_datasets

DEFAULT_START = datetime(2021, 1, 1)
DEFAULT_INTERVALS = ["1h", "4h", "1d"]

_OK_STATUSES = ("SUCCESS", "PARTIAL_SUCCESS")
_DAY_SECONDS = 86400


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
        "--due-only", action="store_true",
        help=(
            "到期增量：仅更新到达更新周期的 dataset（周期取 frequency，"
            "更新时间取 checkpoints.last_success_time，日频及以上对齐源发布"
            "时刻 config.update_schedule；从未成功/周期未知的 dataset 仍到期）"
        ),
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
                "frequency": row.get("frequency"),
                "params": params,
            }
        )
    return results


def _parse_utc(value: str | None) -> datetime | None:
    """解析 SQLite datetime('now') / ISO（含 Z）时间为 UTC naive；坏值 None。"""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(UTC).replace(tzinfo=None)
    return dt


def load_last_success_times(meta: Any) -> dict[str, str | None]:
    """批量读取各 dataset 最近成功时间（checkpoints.last_success_time）。

    一个 dataset_id 理论上可能因 lineage 断裂存在多行 checkpoint，
    MAX 取最新值，与 STALE/到期判定语义一致。
    """
    rows = meta.connection.execute(
        "SELECT dataset_id, MAX(last_success_time) "
        "FROM checkpoints GROUP BY dataset_id"
    ).fetchall()
    return {str(r[0]): (r[1] if r[1] else None) for r in rows}


def _next_due_at(last: datetime, cadence: int, source_id: str | None) -> datetime:
    """计算下次到期时刻（UTC）。

    - cadence < 1 天（1m/1h/4h 等）：对齐到以 UTC 00:00 为原点的周期网格，
      取严格晚于 last 的下一个网格点——1h 每小时整点、4h 每 4 小时
      （00/04/08/12/16/20 时）。若按"last + cadence"滚动，成功时刻会被
      上一轮的秒级耗时带偏，与整点调度错位后变成隔轮才触发；
    - cadence ≥ 1 天（1d/1W/1M/1Q/1Y）：对齐到数据源发布时刻
      （config.update_schedule，日频/长周期一律按发布时刻的时:分）。
      日频取"严格晚于 last 的下一个发布时刻"，故在发布时刻前抓取的轮次
      当天仍会再抓一次，不会漏掉当日新数据；更长周期取 last 日期 + N 天
      的发布时刻（N = 周期天数，30/91/365 为近似值）。
    """
    if cadence < _DAY_SECONDS:
        # 网格原点 = epoch 0 = UTC 00:00（本函数内 last 为 UTC naive）
        secs_of_day = last.hour * 3600 + last.minute * 60 + last.second
        base = (secs_of_day // cadence + 1) * cadence
        day_start = last.replace(hour=0, minute=0, second=0, microsecond=0)
        return day_start + timedelta(seconds=base)
    pub = publish_time_utc(source_id)
    days = cadence // _DAY_SECONDS
    if days <= 1:
        candidate = datetime.combine(last.date(), pub)
        if candidate <= last:
            candidate = datetime.combine(last.date() + timedelta(days=1), pub)
        return candidate
    return datetime.combine(last.date() + timedelta(days=days), pub)


def select_due_datasets(
    datasets: list[dict[str, Any]],
    last_success: dict[str, str | None],
    *,
    now: datetime | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """按更新周期（+ 数据源发布时刻）与最近成功时间划分到期/未到期 dataset。

    到期条件（fail-open：宁多勿漏，避免 dataset 被长期饿死）：
      - 从未成功（无 last_success_time 或不可解析）→ 到期（补历史）；
      - frequency 无法解析（空值/未知/tick 事件型）→ 每轮到期；
      - now ≥ 下次到期时刻（见 _next_due_at，日频及以上对齐源发布时刻）。
    失败/熔断 dataset 的 last_success_time 不推进，故天然保持到期；
    熔断冷却期内由 runner 快速 CANCELLED，不打源。

    Returns:
        (due, skipped)；skipped 元素含 dataset_id / frequency / next_due_at。
    """
    now = now or datetime.now(UTC).replace(tzinfo=None)
    due: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for ds in datasets:
        cadence = parse_frequency_seconds(ds.get("frequency"))
        last = _parse_utc(last_success.get(ds["dataset_id"]))
        if cadence is None or last is None:
            due.append(ds)
            continue
        next_due = _next_due_at(last, cadence, ds.get("source_id"))
        if now >= next_due:
            due.append(ds)
        else:
            skipped.append(
                {
                    "dataset_id": ds["dataset_id"],
                    "frequency": ds.get("frequency"),
                    "next_due_at": next_due,
                }
            )
    return due, skipped


def print_skipped(skipped: list[dict[str, Any]]) -> None:
    """输出未更新（未到期跳过）明细，与已更新列表对照。"""
    if not skipped:
        return
    ordered = sorted(skipped, key=lambda s: (s["next_due_at"], s["dataset_id"]))
    print(f"\n未更新（未到期）{len(ordered)} 个:")
    for s in ordered:
        print(
            f"  [未更新] {s['dataset_id']} ({s['frequency']})"
            f" 下次到期 {s['next_due_at'].strftime('%Y-%m-%d %H:%M:%S')}"
        )


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


def print_report(
    records: list[dict[str, Any]],
    args: argparse.Namespace,
    skipped: list[dict[str, Any]] | None = None,
) -> int:
    """输出分组统计报告（已更新 + 未更新两段），返回退出码。

    skipped 为本轮未更新的 dataset（--due-only 下未到期者），在报告中
    同样逐条列出，避免只提示"哪些更新了"。
    """
    skipped = skipped or []
    print(f"\n{'=' * 70}\n回填报告\n{'=' * 70}")

    by_source: dict[str, list[dict[str, Any]]] = {}
    for r in records:
        by_source.setdefault(r["source_id"], []).append(r)

    status_counter: dict[str, int] = {}
    total_windows = windows_ok = total_output = total_errors = 0
    total_elapsed = 0.0
    failed_records: list[dict[str, Any]] = []

    if records:
        print("── 已更新 ──")
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
                f"  [已更新] {r['status']:<9} {r['dataset_id']:<34}"
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
    print(f"  dataset: 已更新 {len(records)} 个 | " + " | ".join(
        f"{k} {v}" for k, v in sorted(status_counter.items())
    ))
    print(f"          未更新（未到期）{len(skipped)} 个")
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
    print_skipped(skipped)
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
    skipped: list[dict[str, Any]] = []
    try:
        datasets = load_backfill_datasets(meta, args)
        if args.due_only:
            success_map = load_last_success_times(meta)
            datasets, skipped = select_due_datasets(datasets, success_map)
    finally:
        meta.close()

    matched = len(datasets) + len(skipped)
    due_summary = (
        f" | 到期更新 {len(datasets)} | 未到期跳过 {len(skipped)}"
        if args.due_only else ""
    )
    print(f"匹配到 {matched} 个已注册 dataset{due_summary}")
    print(f"数据源: {sorted({d['source_id'] for d in datasets}) or '-'}")
    print(
        f"类型: {sorted({d['canonical_type'] for d in datasets}) or '-'}"
        f" | 时间范围: {start.strftime('%Y-%m-%d')} ~ {end.strftime('%Y-%m-%d')}"
    )
    print(f"窗口: {args.window_seconds}s | 熔断重置: {'是' if args.reset_circuit else '否'}")

    if args.dry_run:
        for ds in datasets:
            print(f"  [DRY RUN] 将回填: {ds['dataset_id']} ({ds['canonical_type']})")
        print_skipped(skipped)
        return 0
    if matched == 0:
        print("错误: 没有匹配的 dataset，请先用注册脚本注册", file=sys.stderr)
        return 1
    if not datasets:
        print(f"本轮无到期 dataset（{len(skipped)} 个未到期跳过）")
        print_skipped(skipped)
        return 0

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

    return print_report(records, args, skipped)


def main() -> None:
    raise SystemExit(run_backfill(parse_args()))


if __name__ == "__main__":
    main()
