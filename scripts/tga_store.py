#!/usr/bin/env python3
"""TGA Derived 存储与运行元数据（Phase 2/3，依据实现方案 §4.4/§4.5）。

职责（D-TGA-06/16/17）：
- Derived Parquet 写入：data/derived/{name}/year=YYYY/month=MM/part-*.parquet，
  原子写协议与 src CanonicalStore 一致（temp 目录 → rename-swap）。
- 预测 Derived（Phase 3）：ForecastStore 按 run 分区
  run_date=YYYY-MM-DD/scenario={s}/part-*.parquet + forecast_runs 元数据。
- DuckDB 视图：独立 catalog meta/tga.duckdb，由本脚本独占读写；
  不写 meta/query.duckdb（open_query_service 稳态只读，不承载 TGA 视图）。
- SQLite 元数据：forecast_runs / backtest_runs / tga_anomaly（迁移 0005），
  连接来自 open_meta()，不另建 schema。

用法：
    uv run python scripts/tga_store.py rebuild-views
    uv run python scripts/tga_store.py status
    uv run python scripts/tga_store.py anomalies --status OPEN
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from chronoforge.cli._wiring import open_meta
from chronoforge.config.settings import Settings

# Derived 数据集名 → 视图名（§4.5；Phase 3 追加 forecast 系列）
DERIVED_DATASETS: tuple[str, ...] = (
    "tga_daily_balance",
    "tga_daily_cashflow",
    "tga_reconciliation",
    "tga_debt_cash_basis",
)
# 预测类 Derived（run 分区：run_date/scenario，§4.5）
FORECAST_DERIVED_DATASETS: tuple[str, ...] = (
    "tga_forecast_components",
    "tga_forecast",
)
# 回测评估类 Derived（year/month 分区，Phase 5 §7）
BACKTEST_DERIVED_DATASETS: tuple[str, ...] = (
    "tga_forecast_eval",
    "tga_attribution",
    "tga_dm_tests",
)


def _derive_year_month(df: pl.DataFrame, date_col: str) -> pl.DataFrame:
    """为日期列附加 year/month hive 分区列（int64，与 canonical 分区语义一致）。"""
    return df.with_columns(
        pl.col(date_col).dt.year().alias("year"),
        pl.col(date_col).dt.month().alias("month"),
    )


class DerivedStore:
    """Derived 层分区写入器（原子写：.tmp-{uuid} → rename-swap）。"""

    def __init__(self, data_dir: Path) -> None:
        self._root = Path(data_dir) / "derived"

    def write(self, name: str, df: pl.DataFrame, *, date_col: str = "date") -> int:
        """整表覆盖写（按分区粒度 replace），返回写入行数。

        每个分区目录独立原子替换；崩溃只遗留 .tmp-* 目录，下次启动清理。
        """
        if name not in DERIVED_DATASETS:
            raise ValueError(f"unknown derived dataset: {name!r}")
        target_root = self._root / name
        target_root.mkdir(parents=True, exist_ok=True)
        _cleanup_stale_tmp(target_root)

        if df.is_empty():
            return 0
        part = _derive_year_month(df, date_col)
        written = 0
        for (year, month), group in part.group_by(["year", "month"]):
            part_dir = target_root / f"year={int(year)}" / f"month={int(month):02d}"
            tmp_dir = part_dir.parent / f".tmp-{uuid.uuid4().hex[:8]}"
            old_dir = part_dir.parent / f".old-{uuid.uuid4().hex[:8]}"
            try:
                tmp_dir.mkdir(parents=True, exist_ok=True)
                group.drop(["year", "month"]).write_parquet(tmp_dir / "part-0000.parquet")
                if part_dir.exists():
                    part_dir.rename(old_dir)
                tmp_dir.rename(part_dir)
                written += group.height
            finally:
                if tmp_dir.exists():
                    shutil.rmtree(tmp_dir, ignore_errors=True)
                if old_dir.exists():
                    shutil.rmtree(old_dir, ignore_errors=True)
        return written

    def read(self, name: str) -> pl.DataFrame:
        path = self._root / name / "**" / "*.parquet"
        return pl.read_parquet(str(path))


def _cleanup_stale_tmp(root: Path) -> None:
    """清理崩溃遗留的 .tmp-* / .old-* 目录（§4.5 写入规则 1）。

    tmp 目录创建于 part_dir.parent（即 year=YYYY/ 子目录），故递归扫描。
    """
    if not root.exists():
        return
    for entry in root.rglob(".tmp-*"):
        if entry.is_dir():
            shutil.rmtree(entry, ignore_errors=True)
    for entry in root.rglob(".old-*"):
        if entry.is_dir():
            shutil.rmtree(entry, ignore_errors=True)


def _atomic_write_partition(target_dir: Path, df: pl.DataFrame) -> None:
    """单分区目录原子替换写（tmp → rename-swap，与 DerivedStore 同协议）。"""
    target_dir.parent.mkdir(parents=True, exist_ok=True)
    tmp_dir = target_dir.parent / f".tmp-{uuid.uuid4().hex[:8]}"
    old_dir = target_dir.parent / f".old-{uuid.uuid4().hex[:8]}"
    try:
        tmp_dir.mkdir(parents=True, exist_ok=True)
        df.write_parquet(tmp_dir / "part-0000.parquet")
        if target_dir.exists():
            target_dir.rename(old_dir)
        tmp_dir.rename(target_dir)
    finally:
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir, ignore_errors=True)
        if old_dir.exists():
            shutil.rmtree(old_dir, ignore_errors=True)


class ForecastStore:
    """forecast_runs 元数据 + 预测 Derived 写入（§4.4/§4.5，Phase 3）。

    - run 元数据走迁移 0005 的 forecast_runs 表（事务、终态一次提交）；
    - 预测 Parquet 按 run 分区：run_date=YYYY-MM-DD/scenario={s}/part-*.parquet；
    - 同一 run 重复输出覆盖同分区（同 run_id 幂等）。
    """

    def __init__(self, data_dir: Path, meta) -> None:
        self._root = Path(data_dir) / "derived"
        self._meta = meta

    # ── forecast_runs 元数据（§4.4）───────────────────────────────────

    @staticmethod
    def _identity_base(
        origin_date: str,
        data_cutoff_date: str,
        model_version: str,
        config_hash: str,
        scenarios: list[str],
        horizons: list[int],
    ) -> str:
        raw = "|".join((
            origin_date, data_cutoff_date, model_version, config_hash,
            ",".join(sorted(scenarios)), ",".join(str(h) for h in sorted(horizons)),
        ))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

    def find_success_run(
        self,
        origin_date: str,
        data_cutoff_date: str,
        model_version: str,
        config_hash: str,
        scenarios: list[str],
        horizons: list[int],
    ) -> str | None:
        """同 identity 的最新 SUCCESS run（§10.3 幂等复用；无 → None）。"""
        base = self._identity_base(
            origin_date, data_cutoff_date, model_version, config_hash,
            scenarios, horizons,
        )
        cur = self._meta.connection.execute(
            "SELECT forecast_run_id FROM forecast_runs "
            "WHERE (forecast_run_id = ? OR forecast_run_id LIKE ?) "
            "AND status = 'SUCCESS' ORDER BY created_at DESC LIMIT 1",
            (base, f"{base}-r%"),
        )
        row = cur.fetchone()
        return str(row[0]) if row else None

    def create_run(
        self,
        origin_date: str,
        data_cutoff_date: str,
        model_version: str,
        config_hash: str,
        scenarios: list[str],
        horizons: list[int],
        snapshot_id: str | None = None,
    ) -> tuple[str, bool]:
        """登记 RUNNING run，返回 (run_id, is_repeat)。

        同一 (origin, cutoff, model_version, config_hash, scenarios, horizons)
        重复运行生成新 run_id（-rN 后缀）并标 is_repeat=1；报告取最新 SUCCESS。
        """
        base = self._identity_base(
            origin_date, data_cutoff_date, model_version, config_hash,
            scenarios, horizons,
        )
        cur = self._meta.connection.execute(
            "SELECT COUNT(*) FROM forecast_runs WHERE forecast_run_id = ? "
            "OR forecast_run_id LIKE ?",
            (base, f"{base}-r%"),
        )
        prior = int(cur.fetchone()[0])
        run_id = base if prior == 0 else f"{base}-r{prior}"
        now = datetime.now(UTC).replace(tzinfo=None).isoformat(timespec="seconds")
        self._meta.connection.execute(
            "INSERT INTO forecast_runs (forecast_run_id, created_at, origin_date, "
            "data_cutoff_date, model_version, config_hash, scenarios_json, "
            "horizons_json, snapshot_id, status, is_repeat) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'RUNNING', ?)",
            (run_id, now, origin_date, data_cutoff_date, model_version, config_hash,
             json.dumps(sorted(scenarios)), json.dumps(sorted(horizons)),
             snapshot_id, 1 if prior > 0 else 0),
        )
        self._meta.connection.commit()
        return run_id, prior > 0

    def finalize_run(
        self,
        run_id: str,
        status: str,
        *,
        output_hash: str | None = None,
        error_summary: str | None = None,
    ) -> None:
        """终态一次提交（SUCCESS / FAILED）。"""
        if status not in ("SUCCESS", "FAILED"):
            raise ValueError(f"invalid run status: {status!r}")
        self._meta.connection.execute(
            "UPDATE forecast_runs SET status = ?, output_hash = ?, error_summary = ? "
            "WHERE forecast_run_id = ?",
            (status, output_hash, error_summary, run_id),
        )
        self._meta.connection.commit()

    def latest_success_run(self) -> dict[str, object] | None:
        """取最新 SUCCESS run（报告入口；无 → None）。"""
        cur = self._meta.connection.execute(
            "SELECT forecast_run_id, created_at, origin_date, data_cutoff_date, "
            "model_version, config_hash, scenarios_json, horizons_json, "
            "snapshot_id, is_repeat, output_hash "
            "FROM forecast_runs WHERE status = 'SUCCESS' "
            "ORDER BY created_at DESC LIMIT 1"
        )
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        return dict(zip(cols, rows[0], strict=True)) if rows else None

    def get_run(self, run_id: str) -> dict[str, object] | None:
        cur = self._meta.connection.execute(
            "SELECT forecast_run_id, created_at, origin_date, data_cutoff_date, "
            "model_version, config_hash, scenarios_json, horizons_json, "
            "snapshot_id, status, is_repeat, output_hash, error_summary "
            "FROM forecast_runs WHERE forecast_run_id = ?",
            (run_id,),
        )
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        return dict(zip(cols, rows[0], strict=True)) if rows else None

    # ── 预测 Derived（run 分区，§4.5）────────────────────────────────

    def write_outputs(
        self,
        run_id: str,
        run_date,
        scenario: str,
        components: pl.DataFrame,
        forecast: pl.DataFrame,
    ) -> tuple[int, int]:
        """写 tga_forecast_components / tga_forecast（同 run 幂等覆盖）。"""
        run_day = run_date.isoformat() if hasattr(run_date, "isoformat") else run_date
        run_part = f"run_date={run_day}"
        n_comp = 0
        n_fcst = 0
        if not components.is_empty():
            comp = components.with_columns(pl.lit(run_id).alias("run_id"))
            _atomic_write_partition(
                self._root / "tga_forecast_components" / run_part / f"scenario={scenario}",
                comp,
            )
            n_comp = comp.height
        if not forecast.is_empty():
            fc = forecast.with_columns(pl.lit(run_id).alias("run_id"))
            _atomic_write_partition(
                self._root / "tga_forecast" / run_part / f"scenario={scenario}",
                fc,
            )
            n_fcst = fc.height
        return n_comp, n_fcst

    def read(self, name: str) -> pl.DataFrame:
        """读取预测 Derived（含 run_date/scenario hive 列）。"""
        if name not in FORECAST_DERIVED_DATASETS:
            raise ValueError(f"unknown forecast derived dataset: {name!r}")
        return pl.read_parquet(
            str(self._root / name / "**" / "*.parquet"), hive_partitioning=True
        )


class BacktestStore:
    """backtest_runs 元数据 + 回测评估 Derived 写入（Phase 5，§4.4/§4.5/§7.1）。

    - run 元数据走迁移 0005 的 backtest_runs 表（事务、终态一次提交）；
    - `tga_forecast_eval` / `tga_attribution` 按 year/month 分区（§4.5）；
    - 同一 run 重跑覆盖自身行；不同 run 的行按 run_id 合并保留，避免分区级
      整表覆盖丢失历史回测结果。
    """

    _RUN_ID_COL = "run_id"

    def __init__(self, data_dir: Path, meta) -> None:
        self._root = Path(data_dir) / "derived"
        self._meta = meta

    # ── backtest_runs 元数据（§4.4）───────────────────────────────────

    @staticmethod
    def _identity_base(
        start_date: str,
        end_date: str,
        stride: int,
        model_versions: list[str],
        config_hash: str,
    ) -> str:
        raw = "|".join((
            start_date, end_date, str(stride),
            ",".join(sorted(model_versions)), config_hash,
        ))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

    def create_run(
        self,
        start_date: str,
        end_date: str,
        stride: int,
        model_versions: list[str],
        config_hash: str,
    ) -> tuple[str, bool]:
        """登记 RUNNING run，返回 (backtest_run_id, is_repeat)。"""
        base = self._identity_base(
            start_date, end_date, stride, model_versions, config_hash
        )
        cur = self._meta.connection.execute(
            "SELECT COUNT(*) FROM backtest_runs WHERE backtest_run_id = ? "
            "OR backtest_run_id LIKE ?",
            (base, f"{base}-r%"),
        )
        prior = int(cur.fetchone()[0])
        run_id = base if prior == 0 else f"{base}-r{prior}"
        now = datetime.now(UTC).replace(tzinfo=None).isoformat(timespec="seconds")
        self._meta.connection.execute(
            "INSERT INTO backtest_runs (backtest_run_id, created_at, start_date, "
            "end_date, stride, model_versions_json, config_hash, status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, 'RUNNING')",
            (run_id, now, start_date, end_date, stride,
             json.dumps(sorted(model_versions)), config_hash),
        )
        self._meta.connection.commit()
        return run_id, prior > 0

    def finalize_run(
        self,
        run_id: str,
        status: str,
        *,
        output_hash: str | None = None,
        error_summary: str | None = None,
    ) -> None:
        """终态一次提交（SUCCESS / FAILED）。"""
        if status not in ("SUCCESS", "FAILED"):
            raise ValueError(f"invalid run status: {status!r}")
        self._meta.connection.execute(
            "UPDATE backtest_runs SET status = ?, output_hash = ?, error_summary = ? "
            "WHERE backtest_run_id = ?",
            (status, output_hash, error_summary, run_id),
        )
        self._meta.connection.commit()

    def latest_success_run(self) -> dict[str, object] | None:
        """取最新 SUCCESS run（报告入口；无 → None）。"""
        cur = self._meta.connection.execute(
            "SELECT backtest_run_id, created_at, start_date, end_date, stride, "
            "model_versions_json, config_hash, output_hash "
            "FROM backtest_runs WHERE status = 'SUCCESS' "
            "ORDER BY created_at DESC LIMIT 1"
        )
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        return dict(zip(cols, rows[0], strict=True)) if rows else None

    def get_run(self, run_id: str) -> dict[str, object] | None:
        cur = self._meta.connection.execute(
            "SELECT backtest_run_id, created_at, start_date, end_date, stride, "
            "model_versions_json, config_hash, status, output_hash, error_summary "
            "FROM backtest_runs WHERE backtest_run_id = ?",
            (run_id,),
        )
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        return dict(zip(cols, rows[0], strict=True)) if rows else None

    # ── 回测评估 Derived（year/month 分区，§4.5）─────────────────────

    def write_outputs(
        self,
        run_id: str,
        eval_df: pl.DataFrame,
        attribution_df: pl.DataFrame,
    ) -> tuple[int, int]:
        """写 tga_forecast_eval / tga_attribution，返回 (评估行数, 归因行数)。"""
        n_eval = self._write_partitioned("tga_forecast_eval", run_id, eval_df)
        n_attr = self._write_partitioned("tga_attribution", run_id, attribution_df)
        return n_eval, n_attr

    def write_dm(self, run_id: str, dm_df: pl.DataFrame) -> int:
        """写 tga_dm_tests（按 run_date 分区；每 run 独立行）。"""
        return self._write_partitioned(
            "tga_dm_tests", run_id, dm_df, date_col="run_date"
        )

    def _write_partitioned(
        self, name: str, run_id: str, df: pl.DataFrame, *, date_col: str = "target_date"
    ) -> int:
        if df.is_empty():
            return 0
        payload = df.with_columns(pl.lit(run_id).alias(self._RUN_ID_COL))
        part = _derive_year_month(payload, date_col)
        written = 0
        for (year, month), group in part.group_by(["year", "month"]):
            part_dir = self._root / name / f"year={int(year)}" / f"month={int(month):02d}"
            new_rows = group.drop(["year", "month"])
            merged = new_rows
            if part_dir.exists() and list(part_dir.glob("*.parquet")):
                old = pl.read_parquet(str(part_dir / "*.parquet")).filter(
                    pl.col(self._RUN_ID_COL) != run_id
                )
                if not old.is_empty():
                    merged = pl.concat([old, new_rows], how="diagonal")
            _atomic_write_partition(part_dir, merged)
            written += new_rows.height
        return written

    def read(self, name: str) -> pl.DataFrame:
        """读取回测 Derived（含 year/month hive 列）。"""
        if name not in BACKTEST_DERIVED_DATASETS:
            raise ValueError(f"unknown backtest derived dataset: {name!r}")
        return pl.read_parquet(
            str(self._root / name / "**" / "*.parquet"), hive_partitioning=True
        )


_REPO_ROOT = Path(__file__).resolve().parents[1]


def resolve_data_dir(settings: Settings) -> Path:
    """将 Settings.data_dir 锚定为绝对路径（相对路径基于仓库根而非 CWD）。

    Settings.data_dir 可为相对值（如 "data"）；脚本从任意目录运行时
    resolve 相对 CWD 会得到错误路径并内嵌进 DuckDB 视图 SQL，故此处
    以仓库根（scripts/ 的父目录）为相对基准。绝对路径（如
    CHRONOFORGE_DATA_DIR 指定的外部目录）原样保留。
    """
    data_dir = Path(settings.data_dir)
    if not data_dir.is_absolute():
        data_dir = _REPO_ROOT / data_dir
    return data_dir.resolve()


def rebuild_views(settings: Settings | None = None) -> dict[str, bool]:
    """在 meta/tga.duckdb 幂等重建 TGA Derived 视图（§4.5 视图 DDL）。

    Returns:
        {dataset_name: 视图是否注册成功}——目录无 Parquet 时跳过（False）。
    """
    import duckdb

    settings = settings or Settings.load()
    data_dir = resolve_data_dir(settings)
    duck_path = Path(settings.meta_dir) / "tga.duckdb"
    duck_path.parent.mkdir(parents=True, exist_ok=True)

    results: dict[str, bool] = {}
    con = duckdb.connect(str(duck_path), read_only=False)
    try:
        for name in (*DERIVED_DATASETS, *FORECAST_DERIVED_DATASETS,
                     *BACKTEST_DERIVED_DATASETS):
            dataset_dir = data_dir / "derived" / name
            if not dataset_dir.exists() or not list(dataset_dir.rglob("*.parquet")):
                results[name] = False
                continue
            parquet_glob = str(dataset_dir / "**" / "*.parquet")
            con.execute(
                f"CREATE OR REPLACE VIEW {name} AS "
                f"SELECT * FROM read_parquet('{parquet_glob}', "
                f"hive_partitioning=1, union_by_name=true)"
            )
            results[name] = True
    finally:
        con.close()
    return results


def list_views(settings: Settings | None = None) -> dict[str, bool]:
    """只读探测 meta/tga.duckdb 中视图注册状态（无写副作用）。

    duckdb 文件缺失或视图缺失 → False；不创建、不重写任何文件
    （与项目"读命令跳过修复"惯例一致，重建仅由 rebuild_views 触发）。
    """
    import duckdb

    settings = settings or Settings.load()
    duck_path = Path(settings.meta_dir) / "tga.duckdb"
    names = (*DERIVED_DATASETS, *FORECAST_DERIVED_DATASETS, *BACKTEST_DERIVED_DATASETS)
    results = {name: False for name in names}
    if not duck_path.exists():
        return results
    con = duckdb.connect(str(duck_path), read_only=True)
    try:
        rows = con.execute(
            "SELECT view_name FROM duckdb_views() "
            "WHERE internal = false AND view_name NOT LIKE 'v%'"
        ).fetchall()
        existing = {str(r[0]) for r in rows}
        for name in names:
            results[name] = name in existing
    finally:
        con.close()
    return results


def open_tga_view_connection(settings: Settings | None = None):
    """只读打开 meta/tga.duckdb（查询方入口；视图缺失时先 rebuild_views）。"""
    import duckdb

    settings = settings or Settings.load()
    duck_path = Path(settings.meta_dir) / "tga.duckdb"
    if not duck_path.exists():
        rebuild_views(settings)
    return duckdb.connect(str(duck_path), read_only=True)


# ── tga_anomaly（迁移 0005）────────────────────────────────────────────


def _anomaly_id(data_source: str, record_date: str | None, anomaly_type: str, key: str) -> str:
    raw = f"{data_source}|{record_date or ''}|{anomaly_type}|{key}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def record_anomalies(
    meta,
    anomalies: list[dict[str, object]],
) -> int:
    """批量写入 tga_anomaly（INSERT OR IGNORE，已存在不重置处置状态）。

    每条：data_source / record_date / anomaly_type / original_value /
    expected_value / severity / quality_flag_ref / note。
    """
    if not anomalies:
        return 0
    now = datetime.now(UTC).replace(tzinfo=None).isoformat(timespec="seconds")
    rows = []
    for a in anomalies:
        anomaly_id = _anomaly_id(
            str(a["data_source"]),
            a.get("record_date") and str(a["record_date"]),
            str(a["anomaly_type"]),
            str(a.get("key", "")),
        )
        original = a.get("original_value")
        expected = a.get("expected_value")
        rows.append((
            anomaly_id,
            now,
            str(a["data_source"]),
            a.get("record_date"),
            str(a["anomaly_type"]),
            None if original is None else str(original),
            None if expected is None else str(expected),
            str(a["severity"]),
            str(a.get("note", "")),
        ))
    meta.connection.executemany(
        "INSERT OR IGNORE INTO tga_anomaly "
        "(anomaly_id, detected_at, data_source, record_date, anomaly_type, "
        " original_value, expected_value, severity, resolution_status, note) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', ?)",
        rows,
    )
    meta.connection.commit()
    return len(rows)


def list_anomalies(meta, status: str | None = None) -> list[dict[str, object]]:
    """查询异常清单（status 缺省全部）。"""
    sql = (
        "SELECT anomaly_id, detected_at, data_source, record_date, anomaly_type, "
        "original_value, expected_value, severity, resolution_status, note "
        "FROM tga_anomaly"
    )
    params: tuple = ()
    if status:
        sql += " WHERE resolution_status = ?"
        params = (status,)
    sql += " ORDER BY detected_at DESC, data_source, record_date"
    cursor = meta.connection.execute(sql, params)
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row, strict=True)) for row in cursor.fetchall()]


def print_status(settings: Settings) -> int:
    """输出 TGA 存储状态摘要（§10.4：各 dataset 最近 run + 视图 + 异常）。

    只读命令：视图状态经 list_views 只读探测，不产生写副作用
    （重建仅由 rebuild-views 子命令或重建脚本触发）。
    """
    meta = open_meta(settings)
    try:
        print("== Derived 视图（meta/tga.duckdb）==")
        for name, ok in sorted(list_views(settings).items()):
            print(f"  {name}: {'已注册' if ok else '未注册/无数据'}")
        print("\n== treasury_fiscal 数据集状态 ==")
        rows = meta.connection.execute(
            "SELECT dataset_id, status FROM dataset_registry "
            "WHERE source_id='treasury_fiscal' ORDER BY dataset_id"
        ).fetchall()
        for dataset_id, status in rows:
            print(f"  {dataset_id}: {status}")
        print("\n== 未关闭异常（tga_anomaly OPEN）==")
        open_anomalies = list_anomalies(meta, "OPEN")
        if not open_anomalies:
            print("  无")
        for a in open_anomalies[:50]:
            print(
                f"  [{a['severity']}] {a['data_source']} {a['record_date']} "
                f"{a['anomaly_type']}"
            )
        if len(open_anomalies) > 50:
            print(f"  ... 共 {len(open_anomalies)} 条")
        return 0
    finally:
        meta.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="TGA Derived 存储工具")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("rebuild-views", help="幂等重建 meta/tga.duckdb 视图")
    sub.add_parser("status", help="输出 TGA 存储状态摘要")
    p_anom = sub.add_parser("anomalies", help="列出 tga_anomaly")
    p_anom.add_argument("--status", default=None, help="OPEN/ACKNOWLEDGED/EXPLAINED/RESOLVED")
    args = parser.parse_args()

    settings = Settings.load()
    if args.command == "rebuild-views":
        for name, ok in rebuild_views(settings).items():
            print(f"{name}: {'已注册' if ok else '无数据，跳过'}")
        return 0
    if args.command == "status":
        return print_status(settings)
    if args.command == "anomalies":
        meta = open_meta(settings)
        try:
            for a in list_anomalies(meta, args.status):
                print(
                    f"[{a['severity']}] {a['record_date']} {a['anomaly_type']} "
                    f"orig={a['original_value']} expect={a['expected_value']} "
                    f"note={a['note']}"
                )
        finally:
            meta.close()
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
