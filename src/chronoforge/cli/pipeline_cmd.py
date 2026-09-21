"""pipeline 命令组（D08 §2）：run / replay / status。

薄层：参数解析 → service 调用 → 渲染；业务规则全部位于
pipeline.runner / pipeline.replay / pipeline.windows（架构 02 规则 3）。
"""

from __future__ import annotations

import json
import signal
from typing import Any

import typer

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

    try:
        settings = Settings.load()
        meta = _wiring.open_meta(settings)
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
                typer.echo(
                    "job "
                    f"dataset={job.dataset_id} mode={job.mode} "
                    f"start={job.start.isoformat() if job.start else None} "
                    f"end={job.end.isoformat() if job.end else None} "
                    f"params={json.dumps(dict(job.params), sort_keys=True)} "
                    f"cursor={cursor}"
                )
            return

        # 审计 SR-02：SIGTERM 默认直接终止进程（无异常），run_log 滞留
        # PENDING、dataset 锁不释放。转换为 KeyboardInterrupt，由
        # PipelineRunner 捕获写 CANCELLED 终态（D05 §3 用户取消）。
        # SIGINT（Ctrl+C）默认即抛 KeyboardInterrupt，无需注册。
        def _sigterm_to_interrupt(signum: int, frame: object) -> None:
            raise KeyboardInterrupt(f"SIGTERM ({signum}) received")

        signal.signal(signal.SIGTERM, _sigterm_to_interrupt)

        for job in jobs:
            row = get_dataset(meta, job.dataset_id)
            if row is None:
                raise ChronoForgeError(
                    f"Dataset not found in registry: {job.dataset_id}",
                    context={"dataset_id": job.dataset_id},
                )
            source_id = str(row["source_id"])
            connector = _wiring.build_connector(source_id, settings, dict(job.params))
            runner = _wiring.build_runner(settings, meta, connector, source_id)
            bind_context(dataset=job.dataset_id)
            try:
                result = runner.run(job)
            finally:
                clear_context()
            typer.echo(
                f"run {result.run_id} dataset={result.dataset_id} "
                f"status={result.status} output={result.output_count} "
                f"errors={result.error_count}"
            )
    except (ChronoForgeError, ValueError) as exc:
        typer.secho(f"error: {exc}", fg=typer.colors.RED, err=True)
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
        settings = Settings.load()
        meta = _wiring.open_meta(settings)
        row = get_dataset(meta, dataset)
        if row is None:
            raise ChronoForgeError(
                f"Dataset not found in registry: {dataset}",
                context={"dataset_id": dataset},
            )
        source_id = str(row["source_id"])
        params = _wiring.load_dataset_params(meta, dataset)
        connector = _wiring.build_connector(source_id, settings, params)

        from chronoforge.pipeline.replay import replay as replay_service

        result = replay_service(
            layer,  # type: ignore[arg-type]
            dataset,
            meta=meta,
            raw_store=RawStore(str(settings.data_dir)),
            canonical_store=CanonicalStoreImpl(str(settings.data_dir)),
            connector=connector,
            settings=settings,
        )
        typer.echo(
            f"replay {result.run_id} dataset={result.dataset_id} "
            f"status={result.status} output={result.output_count}"
        )
    except (ChronoForgeError, ValueError) as exc:
        typer.secho(f"error: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1) from exc


@pipeline_app.command("status")
def pipeline_status(
    dataset: str | None = typer.Option(None, "--dataset", help="按 dataset 过滤"),
    last: int = typer.Option(10, "--last", help="最近 N 次 run"),
    as_json: bool = typer.Option(False, "--json", help="JSON 行输出"),
) -> None:
    """查看 run 状态（读 run_log，新→旧）。"""
    try:
        settings = Settings.load()
        meta = _wiring.open_meta(settings)
        rows: list[dict[str, Any]] = _wiring.list_runs(meta, dataset, last)
        if as_json:
            typer.echo(json.dumps(_wiring.to_jsonable(rows), ensure_ascii=False))
            return
        if not rows:
            typer.echo("(no runs)")
            return
        for r in rows:
            typer.echo(
                f"{r['run_id']} {r['status']:<16} {r['started_at']} "
                f"dataset={r['dataset_id']} output={r['output_count']} "
                f"errors={r['error_count']}"
            )
    except ChronoForgeError as exc:
        typer.secho(f"error: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1) from exc
