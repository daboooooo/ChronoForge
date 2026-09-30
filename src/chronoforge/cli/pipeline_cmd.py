"""pipeline 命令组（D08 §2）：run / replay / status。

薄层：参数解析 → service 调用 → 渲染；业务规则全部位于
pipeline.runner / pipeline.replay / pipeline.windows（架构 02 规则 3）。
"""

from __future__ import annotations

import contextlib
import json
import signal
from typing import Any

import typer

from chronoforge import ui
from chronoforge.cli import _wiring
from chronoforge.config.settings import Settings
from chronoforge.connectors.errors import ChronoForgeError
from chronoforge.logging import bind_context, clear_context
from chronoforge.pipeline.windows import AcquisitionJob
from chronoforge.registry import get_dataset, list_datasets
from chronoforge.storage.canonical import CanonicalStoreImpl
from chronoforge.storage.raw import RawStore

pipeline_app = typer.Typer(help="获取流水线（D05）")

_MODES = ("incremental", "backfill")
_LAYERS = ("canonical", "derived")


@pipeline_app.command("run")
def pipeline_run(
    dataset: str | None = typer.Option(
        None, "--dataset", help="Dataset ID（与 --all-due 二选一）"
    ),
    start: str | None = typer.Option(None, "--start", help="起始时间（ISO，含端点）"),
    end: str | None = typer.Option(None, "--end", help="结束时间（ISO，不含端点）"),
    mode: str = typer.Option("incremental", "--mode", help="incremental | backfill"),
    all_due: bool = typer.Option(
        False, "--all-due", help="运行全部启用 dataset（due 语义 = enabled=1，见决策 D-6）"
    ),
    window_seconds: int | None = typer.Option(
        None,
        "--window-seconds",
        help=(
            "分窗口回填（READY-001 内存治理）：单次 run 只处理一个时间窗"
            "（秒，向上对齐 chunk 边界），循环推进 checkpoint；不传 = 整段单 run"
        ),
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="仅打印 job 计划，不执行、不落盘"
    ),
) -> None:
    """执行获取流水线（七阶段，D05 §1）。"""
    if mode not in _MODES:
        raise typer.BadParameter(f"--mode must be one of {_MODES}")
    if all_due and dataset is not None:
        raise typer.BadParameter("--dataset and --all-due are mutually exclusive")
    if not all_due and dataset is None:
        raise typer.BadParameter("either --dataset or --all-due is required")
    if window_seconds is not None and window_seconds <= 0:
        raise typer.BadParameter("--window-seconds must be positive")

    try:
        # 审计 SR-11：ExitStack 统一管理 open_meta / build_connector 资源，
        # finally 语义覆盖异常路径与 typer.Exit（含 SIGTERM→KeyboardInterrupt）。
        with contextlib.ExitStack() as stack:
            settings = Settings.load()
            # R2-03③：startup_repair 收敛到写入口（run/replay），读命令免修复
            meta = _wiring.open_meta(settings, repair=True)
            stack.callback(meta.close)
            targets = (
                [r["dataset_id"] for r in list_datasets(meta, enabled_only=True)]
                if all_due
                else [dataset or ""]
            )

            jobs: list[AcquisitionJob] = []
            for ds in targets:
                params = _wiring.load_dataset_params(meta, ds)
                jobs.append(
                    AcquisitionJob(
                        dataset_id=ds,
                        start=_wiring.parse_dt(start),
                        end=_wiring.parse_dt(end),
                        mode=mode,  # type: ignore[arg-type]
                        params=params,
                    )
                )

            if dry_run:
                # GWT-3：仅打印 job 计划不落盘——不构造 runner、不 try_lock、不写存储
                for job in jobs:
                    row = get_dataset(meta, job.dataset_id)
                    cursor = meta.get_checkpoint(
                        str(row["source_id"]) if row else "", job.dataset_id
                    )
                    ui.kv_line(
                        "job",
                        [
                            ("dataset", job.dataset_id),
                            ("mode", job.mode),
                            ("start", job.start.isoformat() if job.start else None),
                            ("end", job.end.isoformat() if job.end else None),
                            ("params", json.dumps(dict(job.params), sort_keys=True)),
                            ("cursor", cursor),
                        ],
                    )
                return

            # 审计 SR-02：SIGTERM 默认直接终止进程（无异常），run_log 滞留
            # PENDING、dataset 锁不释放。转换为 KeyboardInterrupt，由
            # PipelineRunner 捕获写 CANCELLED 终态（D05 §3 用户取消）。
            # SIGINT（Ctrl+C）默认即抛 KeyboardInterrupt，无需注册。
            def _sigterm_to_interrupt(signum: int, frame: object) -> None:
                raise KeyboardInterrupt(f"SIGTERM ({signum}) received")

            signal.signal(signal.SIGTERM, _sigterm_to_interrupt)

            # R2-02（审计 2026-09-22）：逐 dataset 异常隔离——单 dataset
            # 失败（ConfigError/AuthError 等）不中断 --all-due 队列
            # （架构 08 §3「批量 run 中单 dataset 失败不影响其余」）；
            # 结束统一汇总并按是否有失败决定退出码。
            failures: list[str] = []
            for job in jobs:
                try:
                    row = get_dataset(meta, job.dataset_id)
                    if row is None:
                        raise ChronoForgeError(
                            f"Dataset not found in registry: {job.dataset_id}",
                            context={"dataset_id": job.dataset_id},
                        )
                    source_id = str(row["source_id"])
                    # SR-11：connector 仅在本迭代存活，迭代结束即 close，
                    # 避免 --all-due 时 N 个连接器同时存活。
                    with contextlib.ExitStack() as job_stack:
                        connector = _wiring.build_connector(
                            source_id, settings, dict(job.params)
                        )
                        _wiring.enter_closeable(job_stack, connector)
                        # R2-07：runner 内部 RawStore 常驻句柄随迭代关闭
                        runner = _wiring.build_runner(
                            settings, meta, connector, source_id, job_stack
                        )
                        bind_context(dataset=job.dataset_id)
                        try:
                            # READY-001：--window-seconds 走分窗口循环（每窗一次 run），
                            # 不传保持单 run 语义；CLI 薄层，窗口规则在 runner 层。
                            if window_seconds is not None:
                                results = runner.run_windowed(
                                    job, window_seconds=window_seconds
                                )
                            else:
                                results = [runner.run(job)]
                        finally:
                            clear_context()
                        for result in results:
                            ui.kv_line(
                                f"run {result.run_id}",
                                [
                                    ("dataset", result.dataset_id),
                                    ("status", result.status),
                                    ("output", result.output_count),
                                    ("errors", result.error_count),
                                ],
                                value_styles={"status": ui.status_text(result.status)},
                            )
                except ChronoForgeError as exc:
                    ui.print_error(str(exc))
                    failures.append(job.dataset_id)

            if failures:
                ui.print_error(
                    f"{len(failures)}/{len(jobs)} dataset(s) failed: "
                    f"{', '.join(failures)}"
                )
                raise typer.Exit(code=1)
    except (ChronoForgeError, ValueError) as exc:
        ui.print_error(str(exc))
        raise typer.Exit(code=1) from exc


@pipeline_app.command("replay")
def pipeline_replay(
    layer: str = typer.Option(..., "--layer", help="canonical | derived"),
    dataset: str = typer.Option(..., "--dataset", help="Dataset ID"),
) -> None:
    """重放指定层（D05 §4；derived 依赖 FeatureEngine → NotImplementedError）。"""
    if layer not in _LAYERS:
        raise typer.BadParameter(f"--layer must be one of {_LAYERS}")
    try:
        with contextlib.ExitStack() as stack:
            settings = Settings.load()
            # R2-03③：startup_repair 收敛到写入口（run/replay），读命令免修复
            meta = _wiring.open_meta(settings, repair=True)
            stack.callback(meta.close)
            row = get_dataset(meta, dataset)
            if row is None:
                raise ChronoForgeError(
                    f"Dataset not found in registry: {dataset}",
                    context={"dataset_id": dataset},
                )
            source_id = str(row["source_id"])
            params = _wiring.load_dataset_params(meta, dataset)
            connector = _wiring.build_connector(source_id, settings, params)
            _wiring.enter_closeable(stack, connector)

            from chronoforge.pipeline.replay import replay as replay_service

            # R2-07：replay 直建的 RawStore 同样注册进 ExitStack
            raw_store = RawStore(str(settings.data_dir))
            _wiring.enter_closeable(stack, raw_store)
            result = replay_service(
                layer,  # type: ignore[arg-type]
                dataset,
                meta=meta,
                raw_store=raw_store,
                canonical_store=CanonicalStoreImpl(str(settings.data_dir)),
                connector=connector,
                settings=settings,
                params=params,
            )
            ui.kv_line(
                f"replay {result.run_id}",
                [
                    ("dataset", result.dataset_id),
                    ("status", result.status),
                    ("output", result.output_count),
                ],
                value_styles={"status": ui.status_text(result.status)},
            )
    except (ChronoForgeError, ValueError) as exc:
        ui.print_error(str(exc))
        raise typer.Exit(code=1) from exc


@pipeline_app.command("circuit-reset")
def pipeline_circuit_reset(
    dataset: str = typer.Option(..., "--dataset", help="Dataset ID"),
) -> None:
    """复位指定 dataset 的熔断状态（R2-01 运维恢复入口，架构 08 §3）。

    熔断冷却期 half-open 之外的即时恢复通道：数据源维护结束等场景下
    免改库手工干预。幂等：未打开时执行无副作用。
    """
    try:
        with contextlib.ExitStack() as stack:
            settings = Settings.load()
            meta = _wiring.open_meta(settings)
            stack.callback(meta.close)
            row = get_dataset(meta, dataset)
            if row is None:
                raise ChronoForgeError(
                    f"Dataset not found in registry: {dataset}",
                    context={"dataset_id": dataset},
                )
            source_id = str(row["source_id"])
            meta.reset_circuit(source_id, dataset)
            ui.kv_line(
                "circuit-reset",
                [
                    ("dataset", dataset),
                    ("source", source_id),
                    ("status", "RESET"),
                ],
                value_styles={"status": ui.status_text("RESET")},
            )
    except (ChronoForgeError, ValueError) as exc:
        ui.print_error(str(exc))
        raise typer.Exit(code=1) from exc


@pipeline_app.command("status")
def pipeline_status(
    dataset: str | None = typer.Option(None, "--dataset", help="按 dataset 过滤"),
    last: int = typer.Option(10, "--last", help="最近 N 次 run"),
    as_json: bool = typer.Option(False, "--json", help="JSON 行输出"),
) -> None:
    """查看 run 状态（读 run_log，新→旧）。"""
    try:
        with contextlib.ExitStack() as stack:
            settings = Settings.load()
            meta = _wiring.open_meta(settings)
            stack.callback(meta.close)
            rows: list[dict[str, Any]] = _wiring.list_runs(meta, dataset, last)
            if as_json:
                typer.echo(json.dumps(_wiring.to_jsonable(rows), ensure_ascii=False))
                return
            if not rows:
                ui.print_hint("(no runs)")
                return
            table = ui.make_table(
                "run_id", "status", "started_at", "dataset", "output", "errors"
            )
            for r in rows:
                table.add_row(
                    str(r["run_id"]),
                    ui.status_text(r["status"]),
                    str(r["started_at"]),
                    str(r["dataset_id"]),
                    str(r["output_count"]),
                    str(r["error_count"]),
                )
            ui.print_table(table)
    except ChronoForgeError as exc:
        ui.print_error(str(exc))
        raise typer.Exit(code=1) from exc
