"""CLI 单元测试（D08 §2/§6，TC-X-002/003/004 + GWT + 边界/失败组）。

映射测试经 monkeypatch _wiring 组合根（stub runner/service），真实渲染路径
（registry sync/dataset add/status/quality report）走真实 MetaStore。
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import typer
from typer.testing import CliRunner

from chronoforge.cli import _wiring
from chronoforge.cli.main import app
from chronoforge.config.settings import Settings
from chronoforge.connectors.errors import ChronoForgeError, ConfigError
from chronoforge.models.enums import CanonicalType
from chronoforge.registry import add_dataset, bootstrap_defaults
from chronoforge.research.query import QueryResult
from chronoforge.research.snapshot import ReproduceResult, SnapshotRecord
from chronoforge.storage.meta import MetaStore, RunRow
from chronoforge.storage.views import get_registered_view_names, missing_views

runner = CliRunner()

CONTACT = "research@example.com"


# ── fixtures ────────────────────────────────────────────────────────────


@pytest.fixture()
def cli_env(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> dict[str, Path]:
    """隔离 CLI 环境：tmp 数据/元目录 + 必填 sec 联系人 + 清空凭证。

    chdir(tmp_path)：隔离仓库本地 .env（_load_dotenv 读 cwd/.env）。
    """
    data_dir = tmp_path / "data"
    meta_dir = tmp_path / "meta"
    meta_dir.mkdir(parents=True)
    for var in (
        "FRED_API_KEY",
        "CCXT_BINANCE_API_KEY",
        "CCXT_BINANCE_SECRET",
        "CCXT_OKX_API_KEY",
        "CCXT_OKX_SECRET",
        "CCXT_OKX_PASSPHRASE",
        "CHRONOFORGE_QUALITY_BLOCK",
        "CHRONOFORGE_RATE_OVERRIDES",
        "CHRONOFORGE_NORMALIZE_ERROR_THRESHOLD",
    ):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("CHRONOFORGE_SEC_CONTACT", CONTACT)
    monkeypatch.setenv("CHRONOFORGE_DATA_DIR", str(data_dir))
    monkeypatch.setenv("CHRONOFORGE_META_DIR", str(meta_dir))
    monkeypatch.chdir(tmp_path)
    return {"data": data_dir, "meta": meta_dir}


@pytest.fixture()
def meta(cli_env: dict[str, Path]) -> MetaStore:
    """与 CLI 同目录的 MetaStore（命令内部另开实例，落同一 SQLite）。"""
    store = MetaStore(str(cli_env["meta"]))
    store.migrate()
    return store


@pytest.fixture()
def registered(meta: MetaStore) -> str:
    """引导 7 源 + 登记 ds_ok（binance_spot / OHLCV）。"""
    bootstrap_defaults(meta)
    add_dataset(
        meta,
        dataset_id="ds_ok",
        source_id="binance_spot",
        canonical_type="OHLCV",
        entity_id="BTCUSDT",
        params={"symbol": "BTCUSDT", "interval": "1m"},
        frequency="1m",
    )
    return "ds_ok"


class _StubRunner:
    """记录 run(job) 调用（TC-X-003 映射断言）。"""

    def __init__(self) -> None:
        self.jobs: list[Any] = []

    def run(self, job: Any) -> RunRow:
        self.jobs.append(job)
        return RunRow(
            run_id="runabc123",
            source_id="binance_spot",
            dataset_id=job.dataset_id,
            status="SUCCESS",
            started_at="2024-01-01T00:00:00",
            output_count=3,
            schema_version="1.0",
            code_version="0.1.0",
            ingest_batch_id="b1",
        )


# ── pipeline run（TC-X-003 + GWT-3）────────────────────────────────────


class TestPipelineRun:
    def test_run_maps_args_to_service(
        self, monkeypatch: pytest.MonkeyPatch, cli_env: dict[str, Path], registered: str
    ) -> None:
        """TC-X-003: 参数 → AcquisitionJob + connector/runner 装配。"""
        stub = _StubRunner()
        connector_seen: dict[str, Any] = {}
        monkeypatch.setattr(
            _wiring, "build_connector", lambda sid, s, params: connector_seen.update(
                source_id=sid, params=params
            )
            or object()
        )
        monkeypatch.setattr(
            _wiring, "build_runner", lambda s, m, c, sid, stack=None: stub
        )

        result = runner.invoke(
            app,
            ["pipeline", "run", "--dataset", "ds_ok", "--mode", "backfill"],
        )
        assert result.exit_code == 0, result.output
        (job,) = stub.jobs
        assert job.dataset_id == "ds_ok"
        assert job.mode == "backfill"
        assert job.params == {"symbol": "BTCUSDT", "interval": "1m"}
        assert job.start is None and job.end is None  # incremental 缺省窗口
        assert connector_seen["source_id"] == "binance_spot"
        assert connector_seen["params"] == {"symbol": "BTCUSDT", "interval": "1m"}
        assert "status=SUCCESS" in result.output

    def test_run_start_end_parsed_naive_utc(
        self, monkeypatch: pytest.MonkeyPatch, cli_env: dict[str, Path], registered: str
    ) -> None:
        stub = _StubRunner()
        monkeypatch.setattr(_wiring, "build_connector", lambda *a: object())
        monkeypatch.setattr(_wiring, "build_runner", lambda *a: stub)
        result = runner.invoke(
            app,
            [
                "pipeline", "run", "--dataset", "ds_ok",
                "--start", "2024-01-01T00:00:00",
                "--end", "2024-01-02T00:00:00+00:00",
            ],
        )
        assert result.exit_code == 0, result.output
        (job,) = stub.jobs
        assert job.start == datetime(2024, 1, 1)
        assert job.end == datetime(2024, 1, 2)  # tz → naive-UTC

    def test_run_all_due_runs_enabled_datasets(
        self,
        monkeypatch: pytest.MonkeyPatch,
        cli_env: dict[str, Path],
        meta: MetaStore,
        registered: str,
    ) -> None:
        add_dataset(
            meta, dataset_id="ds_ok2", source_id="binance_spot",
            canonical_type="OHLCV", entity_id="ETHUSDT", params={},
        )
        stub = _StubRunner()
        monkeypatch.setattr(_wiring, "build_connector", lambda *a: object())
        monkeypatch.setattr(_wiring, "build_runner", lambda *a: stub)
        result = runner.invoke(app, ["pipeline", "run", "--all-due"])
        assert result.exit_code == 0, result.output
        assert [j.dataset_id for j in stub.jobs] == ["ds_ok", "ds_ok2"]

    def test_dry_run_prints_plan_without_persistence(
        self,
        monkeypatch: pytest.MonkeyPatch,
        cli_env: dict[str, Path],
        registered: str,
        meta: MetaStore,
    ) -> None:
        """GWT-3: Given pipeline run --dry-run Then 仅打印 job 计划不落盘。"""
        ran = False

        def _boom(*a: Any, **k: Any) -> None:
            nonlocal ran
            ran = True

        monkeypatch.setattr(_wiring, "build_runner", _boom)
        result = runner.invoke(
            app,
            ["pipeline", "run", "--dataset", "ds_ok", "--dry-run"],
        )
        assert result.exit_code == 0, result.output
        assert "job dataset=ds_ok" in result.output
        assert "mode=incremental" in result.output
        assert not ran  # runner 未被构造
        assert _wiring.list_runs(meta) == []  # run_log 无写入

    def test_run_unknown_dataset_fails(
        self, cli_env: dict[str, Path], meta: MetaStore
    ) -> None:
        result = runner.invoke(app, ["pipeline", "run", "--dataset", "ghost"])
        assert result.exit_code == 1
        assert "error:" in result.stderr

    def test_run_requires_dataset_or_all_due(
        self, cli_env: dict[str, Path]
    ) -> None:
        result = runner.invoke(app, ["pipeline", "run"])
        assert result.exit_code == 2  # BadParameter

    def test_run_dataset_and_all_due_exclusive(
        self, cli_env: dict[str, Path]
    ) -> None:
        result = runner.invoke(
            app, ["pipeline", "run", "--dataset", "ds_ok", "--all-due"]
        )
        assert result.exit_code == 2

    def test_run_invalid_mode_rejected(self, cli_env: dict[str, Path]) -> None:
        result = runner.invoke(
            app, ["pipeline", "run", "--dataset", "ds_ok", "--mode", "bogus"]
        )
        assert result.exit_code == 2


# ── pipeline replay / status ────────────────────────────────────────────


class TestPipelineReplayStatus:
    def test_replay_maps_to_service(
        self, monkeypatch: pytest.MonkeyPatch, cli_env: dict[str, Path], registered: str
    ) -> None:
        """TC-X-003: replay → pipeline.replay.replay(layer, dataset, ...)。"""
        # 注意：pipeline/__init__ 的 `from .replay import replay` 把包属性覆盖为
        # 函数，`import a.b.c as x` 会绑定到该函数；须用 importlib 取真子模块。
        import importlib

        replay_module = importlib.import_module("chronoforge.pipeline.replay")

        seen: dict[str, Any] = {}

        def _fake_replay(layer: str, dataset_id: str, **kw: Any) -> RunRow:
            seen.update(layer=layer, dataset_id=dataset_id)
            seen["connector_type"] = type(kw["connector"]).__name__
            return RunRow(
                run_id="Rabc123",
                source_id="binance_spot",
                dataset_id=dataset_id,
                status="SUCCESS",
                started_at="2024-01-01T00:00:00",
            )

        monkeypatch.setattr(replay_module, "replay", _fake_replay)
        monkeypatch.setattr(
            _wiring, "build_connector", lambda *a, **kw: object()
        )
        result = runner.invoke(
            app,
            ["pipeline", "replay", "--layer", "canonical", "--dataset", "ds_ok"],
        )
        assert result.exit_code == 0, result.output
        assert seen["layer"] == "canonical"
        assert seen["dataset_id"] == "ds_ok"
        assert seen["connector_type"] == "object"

    def test_replay_invalid_layer(self, cli_env: dict[str, Path]) -> None:
        result = runner.invoke(
            app, ["pipeline", "replay", "--layer", "raw", "--dataset", "ds_ok"]
        )
        assert result.exit_code == 2

    def test_status_reads_run_log(
        self, cli_env: dict[str, Path], meta: MetaStore
    ) -> None:
        for i in range(12):
            meta.connection.execute(
                "INSERT INTO run_log(run_id, source_id, dataset_id, status, "
                "started_at, schema_version, code_version, ingest_batch_id) "
                "VALUES (?, 'binance_spot', 'ds_ok', 'SUCCESS', ?, '1.0', '0.1.0', 'b')",
                (f"run{i:012d}", f"2024-01-{i + 1:02d}T00:00:00"),
            )
        meta.connection.commit()
        result = runner.invoke(
            app, ["pipeline", "status", "--dataset", "ds_ok", "--json"]
        )
        assert result.exit_code == 0, result.output
        rows = json.loads(result.output)
        assert len(rows) == 10  # 默认 --last 10
        assert rows[0]["run_id"] == "run000000000011"  # started_at 倒序
        assert rows[0]["status"] == "SUCCESS"

    def test_status_empty(self, cli_env: dict[str, Path], meta: MetaStore) -> None:
        result = runner.invoke(app, ["pipeline", "status"])
        assert result.exit_code == 0
        assert "(no runs)" in result.output


# ── registry（D04 §5）───────────────────────────────────────────────────


class TestRegistryCommands:
    def test_sync_bootstraps_all_sources_idempotent(
        self, cli_env: dict[str, Path], meta: MetaStore
    ) -> None:
        """bootstrap_defaults 幂等（ON CONFLICT UPDATE，D04 §5）。"""
        for _ in range(2):
            result = runner.invoke(app, ["registry", "sync"])
            assert result.exit_code == 0, result.output
            assert "8 sources" in result.output
        assert meta.connection.execute(
            "SELECT COUNT(*) FROM source_registry"
        ).fetchone()[0] == 8

    def test_sync_restores_edited_row(
        self, cli_env: dict[str, Path], meta: MetaStore
    ) -> None:
        runner.invoke(app, ["registry", "sync"])
        meta.connection.execute(
            "UPDATE source_registry SET display_name = 'EDITED' WHERE source_id = 'fred'"
        )
        meta.connection.commit()
        runner.invoke(app, ["registry", "sync"])
        name = meta.connection.execute(
            "SELECT display_name FROM source_registry WHERE source_id = 'fred'"
        ).fetchone()[0]
        assert name == "FRED (St. Louis Fed)"

    def test_list_sources_json(self, cli_env: dict[str, Path]) -> None:
        runner.invoke(app, ["registry", "sync"])
        result = runner.invoke(app, ["registry", "list-sources", "--json"])
        assert result.exit_code == 0
        rows = json.loads(result.output)
        assert {r["source_id"] for r in rows} == {
            "binance_spot", "binance_futures", "deribit", "ccxt",
            "yahoo", "fred", "sec_edgar", "sosovalue",
        }
        fred = next(r for r in rows if r["source_id"] == "fred")
        assert fred["access_type"] == "PUBLIC_WITH_KEY"
        assert json.loads(fred["rate_limit_json"]) == {"req_per_min": 120}
        sosovalue = next(r for r in rows if r["source_id"] == "sosovalue")
        assert sosovalue["access_type"] == "PUBLIC_WITH_KEY"
        assert json.loads(sosovalue["rate_limit_json"]) == {"req_per_min": 20}
        assert sosovalue["historical_limit_days"] == 30

    def test_list_sources_empty_hint(self, cli_env: dict[str, Path]) -> None:
        result = runner.invoke(app, ["registry", "list-sources"])
        assert result.exit_code == 0
        assert "registry sync" in result.output

    def test_list_datasets_filtered(
        self, cli_env: dict[str, Path], registered: str
    ) -> None:
        result = runner.invoke(
            app, ["registry", "list-datasets", "--source", "binance_spot", "--json"]
        )
        rows = json.loads(result.output)
        assert [r["dataset_id"] for r in rows] == ["ds_ok"]


# ── dataset add（写侧映射）──────────────────────────────────────────────


class TestDatasetAdd:
    def test_add_writes_registry_row(
        self, cli_env: dict[str, Path], meta: MetaStore
    ) -> None:
        bootstrap_defaults(meta)
        result = runner.invoke(
            app,
            [
                "dataset", "add",
                "--dataset-id", "ds_new",
                "--source", "binance_spot",
                "--type", "TRADE",
                "--params", '{"symbol": "ETHUSDT"}',
                "--frequency", "raw",
                "--continuity-model", "ALWAYS_OPEN",
            ],
        )
        assert result.exit_code == 0, result.output
        rows = _wiring_list_datasets_json()
        row = next(r for r in rows if r["dataset_id"] == "ds_new")
        assert row["canonical_type"] == "TRADE"
        assert row["entity_id"] == "ETHUSDT"  # 缺省取 params.symbol
        assert json.loads(row["params_json"]) == {"symbol": "ETHUSDT"}

    def test_add_invalid_continuity_model_fails(
        self, cli_env: dict[str, Path], meta: MetaStore
    ) -> None:
        """失败组：CHECK 域外 continuity_model → 退出码 1。"""
        bootstrap_defaults(meta)
        result = runner.invoke(
            app,
            [
                "dataset", "add", "--dataset-id", "ds_bad",
                "--source", "binance_spot", "--type", "TRADE",
                "--continuity-model", "BOGUS",
            ],
        )
        assert result.exit_code == 1

    def test_add_bad_params_json(self, cli_env: dict[str, Path]) -> None:
        result = runner.invoke(
            app,
            ["dataset", "add", "--dataset-id", "x", "--source", "s",
             "--type", "TRADE", "--params", "{not json"],
        )
        assert result.exit_code == 2


def _wiring_list_datasets_json() -> list[dict[str, Any]]:
    """经组合根读取 dataset_registry（复用 registry.service）。"""
    from chronoforge.registry import list_datasets

    settings = Settings_load()
    store = MetaStore(str(settings.meta_dir))
    return list_datasets(store)


def Settings_load(**kwargs: Any) -> Any:  # noqa: N802  # 测试辅助，保持调用点简短
    from chronoforge.config.settings import Settings

    return Settings.load(**kwargs)


# ── query（TC-X-004 + 边界）────────────────────────────────────────────


class _StubQueryService:
    def __init__(self, frame: pl.DataFrame, row_count: int) -> None:
        self._frame = frame
        self._row_count = row_count
        self.calls: list[dict[str, Any]] = []

    def query(self, dataset_id: str, **kw: Any) -> QueryResult:
        self.calls.append({"dataset_id": dataset_id, **kw})
        return QueryResult(
            frame=self._frame,
            dataset_id=dataset_id,
            dataset_version="0.1.0+1.0",
            schema_version="1.0",
            row_count=self._row_count,
            elapsed_ms=5,
        )


class _StubCon:
    """DuckDB 连接 stub（open_query_service 返回的 con 需 close）。"""

    def close(self) -> None:
        """no-op"""


class TestQuery:
    def _stub(
        self, monkeypatch: pytest.MonkeyPatch, frame: pl.DataFrame, n: int
    ) -> _StubQueryService:
        stub = _StubQueryService(frame, n)
        monkeypatch.setattr(
            _wiring, "open_query_service", lambda s, m: (stub, _StubCon())
        )
        return stub

    def test_json_output_snapshot(
        self, monkeypatch: pytest.MonkeyPatch, cli_env: dict[str, Path]
    ) -> None:
        """TC-X-004: --json 输出 = QueryResult 字段直序列化快照。"""
        frame = pl.DataFrame({
            "event_time": [datetime(2024, 1, 1)],
            "close": [50500.5],
        })
        stub = self._stub(monkeypatch, frame, 1)
        result = runner.invoke(
            app,
            [
                "query", "--dataset", "ds_ok",
                "--start", "2024-01-01T00:00:00",
                "--asof", "2024-06-01T00:00:00",
                "--filters", '{"interval": "1m"}',
                "--columns", "close,event_time",
                "--json",
            ],
        )
        assert result.exit_code == 0, result.output
        assert json.loads(result.output) == {
            "dataset_id": "ds_ok",
            "dataset_version": "0.1.0+1.0",
            "schema_version": "1.0",
            "row_count": 1,
            "elapsed_ms": 5,
            "truncated": False,  # READY-003：截断元信息随 QueryResult 序列化
            "rows": [{"event_time": "2024-01-01T00:00:00", "close": 50500.5}],
        }
        (call,) = stub.calls
        assert call["dataset_id"] == "ds_ok"
        assert call["start"] == datetime(2024, 1, 1)
        assert call["asof"] == datetime(2024, 6, 1)
        assert call["filters"] == {"interval": "1m"}
        assert call["columns"] == ["close", "event_time"]

    def test_json_empty_result(
        self, monkeypatch: pytest.MonkeyPatch, cli_env: dict[str, Path]
    ) -> None:
        """边界：--json 空结果 → rows=[]。"""
        self._stub(monkeypatch, pl.DataFrame(schema={"x": pl.Float64}), 0)
        result = runner.invoke(app, ["query", "--dataset", "ds_ok", "--json"])
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["row_count"] == 0
        assert payload["rows"] == []

    def test_json_csv_exclusive(self, cli_env: dict[str, Path]) -> None:
        result = runner.invoke(app, ["query", "--dataset", "d", "--json", "--csv"])
        assert result.exit_code == 2

    def test_bad_filters_json(self, cli_env: dict[str, Path]) -> None:
        result = runner.invoke(
            app, ["query", "--dataset", "d", "--filters", "{bad"]
        )
        assert result.exit_code == 2
        assert "not valid JSON" in result.output

    def test_unknown_dataset_value_error_exit_1(
        self, monkeypatch: pytest.MonkeyPatch, cli_env: dict[str, Path]
    ) -> None:
        """失败组：未知 dataset（service ValueError）→ 退出码 1。"""

        class _Raising:
            def query(self, *_: Any, **__: Any) -> QueryResult:
                raise ValueError("Dataset not found: ghost")

        monkeypatch.setattr(
            _wiring, "open_query_service", lambda s, m: (_Raising(), _StubCon())
        )
        result = runner.invoke(app, ["query", "--dataset", "ghost", "--json"])
        assert result.exit_code == 1
        assert "Dataset not found" in result.stderr


# ── quality report ──────────────────────────────────────────────────────


class TestQualityReport:
    _FLAG = {
        "record_key": "k1",
        "dataset_id": "ds_ok",
        "rule_id": "Q-GAP-001",
        "severity": "WARNING",
        "detail": "{}",
        "raw_ref": "raw/1.jsonl:1",
        "payload_digest": "d" * 64,
        "run_id": "runabc123",
        "created_at": "2024-01-01T00:00:00",
    }

    def test_report_renders_flags(
        self, cli_env: dict[str, Path], meta: MetaStore, registered: str
    ) -> None:
        meta.add_quality_flags([dict(self._FLAG)])
        result = runner.invoke(app, ["quality", "report"])
        assert result.exit_code == 0, result.output
        assert "findings=1" in result.output
        assert "Q-GAP-001" in result.output

    def test_report_json(
        self, cli_env: dict[str, Path], meta: MetaStore, registered: str
    ) -> None:
        meta.add_quality_flags([dict(self._FLAG)])
        result = runner.invoke(app, ["quality", "report", "--json"])
        rows = json.loads(result.output)
        assert len(rows) == 1
        assert rows[0]["rule_id"] == "Q-GAP-001"

    def test_report_empty(self, cli_env: dict[str, Path], meta: MetaStore) -> None:
        result = runner.invoke(app, ["quality", "report"])
        assert result.exit_code == 0
        assert "(no findings)" in result.output


# ── research reproduce（映射）──────────────────────────────────────────


class TestResearchReproduce:
    def test_reproduce_maps_to_service(
        self, monkeypatch: pytest.MonkeyPatch, cli_env: dict[str, Path]
    ) -> None:
        """TC-X-003: reproduce → snapshot_reproduce(snapshot_id, meta, qs, query_func)。"""
        from chronoforge.cli import research_cmd

        record = SnapshotRecord(
            snapshot_id="snap123abc",
            created_at="2024-01-01T00:00:00",
            datasets=[{"dataset_id": "ds_ok", "dataset_version": "0.1.0+1.0"}],
            code_version="0.1.0",
            params={"queries": [{"dataset_id": "ds_ok"}]},
            output_hash="h0",
            notebook_ref=None,
            query_text=None,
        )
        seen: dict[str, Any] = {}

        def _fake_reproduce(snapshot_id: str, meta_: Any, qs: Any, qf: Any) -> ReproduceResult:
            seen["snapshot_id"] = snapshot_id
            seen["query_ran"] = qf(qs) is not None
            return ReproduceResult(
                snapshot_id=snapshot_id,
                hash_match=True,
                original_hash="h0",
                new_hash="h0",
                version_changes=None,
            )

        monkeypatch.setattr(research_cmd, "get_snapshot", lambda m, sid: record)
        monkeypatch.setattr(research_cmd, "snapshot_reproduce", _fake_reproduce)

        frame = pl.DataFrame({"x": [1]})
        stub = _StubQueryService(frame, 1)
        monkeypatch.setattr(
            _wiring, "open_query_service", lambda s, m: (stub, _StubCon())
        )

        result = runner.invoke(
            app, ["research", "reproduce", "--snapshot", "snap123abc"]
        )
        assert result.exit_code == 0, result.output
        assert seen["snapshot_id"] == "snap123abc"
        assert seen["query_ran"] is True
        payload = json.loads(result.output)
        assert payload == {
            "snapshot_id": "snap123abc",
            "hash_match": True,
            "original_hash": "h0",
            "new_hash": "h0",
            "version_changes": None,
        }

    def test_reproduce_missing_snapshot(
        self, monkeypatch: pytest.MonkeyPatch, cli_env: dict[str, Path]
    ) -> None:
        from chronoforge.cli import research_cmd

        monkeypatch.setattr(research_cmd, "get_snapshot", lambda m, sid: None)
        result = runner.invoke(
            app, ["research", "reproduce", "--snapshot", "ghost"]
        )
        assert result.exit_code == 1
        assert "Snapshot not found" in result.stderr


# ── build_connector（TC-X-002 + D08 §5）────────────────────────────────


class TestBuildConnector:
    @pytest.fixture(autouse=True)
    def _contact_env(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        """Settings.load 必需 sec 联系人 + chdir 隔离本地 .env。"""
        monkeypatch.setenv("CHRONOFORGE_SEC_CONTACT", CONTACT)
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("FRED_API_KEY", raising=False)

    def test_fred_without_key_raises_config_error(self) -> None:
        """TC-X-002: FRED 无 key → ConfigError。"""
        settings = Settings_load()
        with pytest.raises(ConfigError, match="FRED_API_KEY"):
            _wiring.build_connector("fred", settings, {})

    def test_fred_with_key_constructs(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("FRED_API_KEY", "k-test")
        connector = _wiring.build_connector("fred", Settings_load(), {})
        assert type(connector).__name__ == "FREDConnector"

    def test_sosovalue_without_key_raises_config_error(self) -> None:
        """SoSoValue 无 key → ConfigError（D08 §1）。

        kwargs 优先于 env/.env（load 顺序 defaults ← .env ← env ← kwargs），
        避免开发者本地 .env 真实 key 泄漏进测试。
        """
        settings = Settings_load(sosovalue_api_key="")
        with pytest.raises(ConfigError, match="SOSOVALUE_API_KEY"):
            _wiring.build_connector("sosovalue", settings, {})

    def test_sosovalue_with_key_constructs(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("SOSOVALUE_API_KEY", "k-test")
        connector = _wiring.build_connector("sosovalue", Settings_load(), {})
        assert type(connector).__name__ == "SoSoValueConnector"
        connector.close()

    def test_unknown_source_raises(self) -> None:
        with pytest.raises(ConfigError, match="Unknown source_id"):
            _wiring.build_connector("nope", Settings_load(), {})


# ── open_meta 启动修复（审计 SR-01，D03 §5.5）──────────────────────────


class TestOpenMetaStartupRepair:
    def test_open_meta_releases_stale_orphan_lock(
        self, cli_env: dict[str, Path], meta: MetaStore, registered: str
    ) -> None:
        """审计 SR-01 + R2-03③: Given 崩溃残留的 stale PENDING run When
        open_meta(repair=True)（写入口语义）Then release_stale_locks 置
        CANCELLED 且同 dataset 锁可重新获取。
        """
        locked = meta.try_lock_dataset("ds_ok", source_id="binance_spot")
        # 回拨 started_at 至固定过去时刻（> 默认 3600s 租约超时）模拟崩溃残留
        meta.connection.execute(
            "UPDATE run_log SET started_at = '2024-01-01T00:00:00Z' "
            "WHERE run_id = ?",
            (locked.run_id,),
        )
        meta.connection.commit()

        repaired = _wiring.open_meta(Settings_load(), repair=True)

        row = repaired.connection.execute(
            "SELECT status, error_summary FROM run_log WHERE run_id = ?",
            (locked.run_id,),
        ).fetchone()
        assert row is not None
        assert row[0] == "CANCELLED"
        assert "orphaned" in str(row[1])

        # 锁已释放：同一 dataset 可重新 try_lock（原本会抛 "dataset locked"）
        relocked = repaired.try_lock_dataset("ds_ok", source_id="binance_spot")
        assert relocked.run_id != locked.run_id

    def test_open_meta_default_skips_repair(
        self, cli_env: dict[str, Path], meta: MetaStore, registered: str
    ) -> None:
        """R2-03③: Given 读命令默认 open_meta When 存在崩溃残留行
        Then 不执行 startup_repair（防 cron 重叠期间误伤在途 run）。
        """
        locked = meta.try_lock_dataset("ds_ok", source_id="binance_spot")
        meta.connection.execute(
            "UPDATE run_log SET started_at = '2024-01-01T00:00:00Z' "
            "WHERE run_id = ?",
            (locked.run_id,),
        )
        meta.connection.commit()

        opened = _wiring.open_meta(Settings_load())

        row = opened.connection.execute(
            "SELECT status FROM run_log WHERE run_id = ?", (locked.run_id,)
        ).fetchone()
        assert row is not None
        assert row[0] == "PENDING"  # 残留行未被 startup_repair 触碰


# ── 资源生命周期（审计 SR-11 / READY-004）──────────────────────────────


def _spy_meta_close(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """监视 MetaStore.close（类级 patch，覆盖 CLI 内部 open_meta 的新实例）。"""
    calls: list[int] = []
    orig = MetaStore.close

    def _spy(self: MetaStore) -> None:
        calls.append(1)
        orig(self)

    monkeypatch.setattr(MetaStore, "close", _spy)
    return calls


class _CloseSpyConnector:
    """close 可观测的 connector stub（close 为 DataConnector 协议外可选能力）。"""

    def __init__(self, events: list[str]) -> None:
        self._events = events

    def close(self) -> None:
        self._events.append("close")


class TestResourceLifecycle:
    """SR-11：正常 / 异常 / typer.Exit 路径均显式关闭 connector 与 MetaStore。"""

    def test_run_releases_connector_and_meta_on_success(
        self,
        monkeypatch: pytest.MonkeyPatch,
        cli_env: dict[str, Path],
        registered: str,
    ) -> None:
        events: list[str] = []
        monkeypatch.setattr(
            _wiring, "build_connector", lambda *a, **k: _CloseSpyConnector(events)
        )
        monkeypatch.setattr(_wiring, "build_runner", lambda *a: _StubRunner())
        meta_close = _spy_meta_close(monkeypatch)

        result = runner.invoke(app, ["pipeline", "run", "--dataset", "ds_ok"])
        assert result.exit_code == 0, result.output
        assert events == ["close"]
        assert meta_close == [1]

    def test_run_releases_connector_and_meta_on_runner_exception(
        self,
        monkeypatch: pytest.MonkeyPatch,
        cli_env: dict[str, Path],
        registered: str,
    ) -> None:
        class _BoomRunner:
            def run(self, job: Any) -> RunRow:
                raise ChronoForgeError("boom")

        events: list[str] = []
        monkeypatch.setattr(
            _wiring, "build_connector", lambda *a, **k: _CloseSpyConnector(events)
        )
        monkeypatch.setattr(_wiring, "build_runner", lambda *a: _BoomRunner())
        meta_close = _spy_meta_close(monkeypatch)

        result = runner.invoke(app, ["pipeline", "run", "--dataset", "ds_ok"])
        assert result.exit_code == 1
        assert "error: boom" in result.stderr
        assert events == ["close"]
        assert meta_close == [1]

    def test_run_releases_connector_and_meta_on_typer_exit(
        self,
        monkeypatch: pytest.MonkeyPatch,
        cli_env: dict[str, Path],
        registered: str,
    ) -> None:
        """typer.Exit 穿透 with 块：ExitStack 仍释放全部资源后向上传播。"""

        def _raise_exit(*a: Any, **k: Any) -> Any:
            raise typer.Exit(code=7)

        events: list[str] = []
        monkeypatch.setattr(
            _wiring, "build_connector", lambda *a, **k: _CloseSpyConnector(events)
        )
        monkeypatch.setattr(_wiring, "build_runner", _raise_exit)
        meta_close = _spy_meta_close(monkeypatch)

        result = runner.invoke(app, ["pipeline", "run", "--dataset", "ds_ok"])
        assert result.exit_code == 7
        assert events == ["close"]
        assert meta_close == [1]

    def test_run_all_due_releases_connector_per_iteration(
        self,
        monkeypatch: pytest.MonkeyPatch,
        cli_env: dict[str, Path],
        meta: MetaStore,
        registered: str,
    ) -> None:
        """GWT-2：--all-due 多 dataset → connector 单次迭代结束即 close，
        不存在跨 dataset 存活的未关闭连接器（close 先于下一次 build）。"""
        add_dataset(
            meta, dataset_id="ds_ok2", source_id="binance_spot",
            canonical_type="OHLCV", entity_id="ETHUSDT", params={},
        )
        events: list[str] = []

        def _fake_build(*a: Any, **k: Any) -> _CloseSpyConnector:
            events.append("build")
            return _CloseSpyConnector(events)

        monkeypatch.setattr(_wiring, "build_connector", _fake_build)
        monkeypatch.setattr(_wiring, "build_runner", lambda *a: _StubRunner())
        meta_close = _spy_meta_close(monkeypatch)

        result = runner.invoke(app, ["pipeline", "run", "--all-due"])
        assert result.exit_code == 0, result.output
        assert events == ["build", "close", "build", "close"]
        assert meta_close == [1]

    def test_replay_releases_connector_and_meta(
        self,
        monkeypatch: pytest.MonkeyPatch,
        cli_env: dict[str, Path],
        registered: str,
    ) -> None:
        import importlib

        replay_module = importlib.import_module("chronoforge.pipeline.replay")
        monkeypatch.setattr(
            replay_module,
            "replay",
            lambda *a, **k: RunRow(
                run_id="Rabc123",
                source_id="binance_spot",
                dataset_id="ds_ok",
                status="SUCCESS",
                started_at="2024-01-01T00:00:00",
            ),
        )
        events: list[str] = []
        monkeypatch.setattr(
            _wiring, "build_connector", lambda *a, **k: _CloseSpyConnector(events)
        )
        meta_close = _spy_meta_close(monkeypatch)

        result = runner.invoke(
            app,
            ["pipeline", "replay", "--layer", "canonical", "--dataset", "ds_ok"],
        )
        assert result.exit_code == 0, result.output
        assert events == ["close"]
        assert meta_close == [1]

    def test_query_releases_con_and_meta_on_success(
        self,
        monkeypatch: pytest.MonkeyPatch,
        cli_env: dict[str, Path],
    ) -> None:
        con_close: list[int] = []

        class _SpyCon:
            def close(self) -> None:
                con_close.append(1)

        stub = _StubQueryService(pl.DataFrame({"x": [1]}), 1)
        monkeypatch.setattr(
            _wiring, "open_query_service", lambda s, m: (stub, _SpyCon())
        )
        meta_close = _spy_meta_close(monkeypatch)

        result = runner.invoke(app, ["query", "--dataset", "ds_ok", "--json"])
        assert result.exit_code == 0, result.output
        assert con_close == [1]
        assert meta_close == [1]

    def test_query_releases_con_and_meta_on_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
        cli_env: dict[str, Path],
    ) -> None:
        con_close: list[int] = []

        class _SpyCon:
            def close(self) -> None:
                con_close.append(1)

        class _Raising:
            def query(self, *_: Any, **__: Any) -> QueryResult:
                raise ValueError("boom")

        monkeypatch.setattr(
            _wiring, "open_query_service", lambda s, m: (_Raising(), _SpyCon())
        )
        meta_close = _spy_meta_close(monkeypatch)

        result = runner.invoke(app, ["query", "--dataset", "ds_ok", "--json"])
        assert result.exit_code == 1
        assert con_close == [1]
        assert meta_close == [1]

    @pytest.mark.parametrize(
        "argv",
        [
            ["registry", "list-sources"],
            ["registry", "list-datasets"],
            ["quality", "report"],
            ["pipeline", "status"],
        ],
    )
    def test_meta_only_commands_close_meta(
        self,
        monkeypatch: pytest.MonkeyPatch,
        cli_env: dict[str, Path],
        argv: list[str],
    ) -> None:
        meta_close = _spy_meta_close(monkeypatch)
        result = runner.invoke(app, argv)
        assert result.exit_code == 0, result.output
        assert meta_close == [1]


# ── READY-005：open_query_service 并发窗口消除（SR-16，方案 A）──────────

_READY005_BASE_TIME = datetime(2026, 9, 11, 10, 0, 0)


def _seed_canonical_parquet(
    data_dir: Path, canonical_type: CanonicalType, records: list[dict[str, Any]]
) -> None:
    """records 写为 canonical/{TYPE}/entity=…/year=…/month=…/part-0001.parquet。"""
    first = records[0]
    entity = first.get("entity_id", first.get("source_id", "test"))
    obs = first.get("observation_time", first.get("event_time", _READY005_BASE_TIME))
    partition_dir = (
        data_dir / "canonical" / canonical_type.value
        / f"entity={entity}" / f"year={obs.strftime('%Y')}"
        / f"month={obs.strftime('%m')}"
    )
    partition_dir.mkdir(parents=True, exist_ok=True)
    columns: dict[str, list[Any]] = {}
    for rec in records:
        for k, v in rec.items():
            columns.setdefault(k, []).append(v)
    arrays: dict[str, pa.Array] = {}
    for name, values in columns.items():
        non_none = [v for v in values if v is not None]
        if non_none and all(isinstance(v, datetime) for v in non_none):
            arrays[name] = pa.array(
                [v.replace(tzinfo=None) if v.tzinfo else v for v in values],
                type=pa.timestamp("us"),
            )
        elif non_none and all(isinstance(v, str) for v in non_none):
            arrays[name] = pa.array(values, type=pa.string())
        else:
            arrays[name] = pa.array(values, type=pa.float64())
    pq.write_table(pa.table(arrays), str(partition_dir / "part-0001.parquet"))


def _ready005_ohlcv_records() -> list[dict[str, Any]]:
    """OHLCV 5 行（event_time 递增 1 分钟；同 test_query.py 列约定）。"""
    return [
        {
            "schema_version": "1.0",
            "source": "BINANCE",
            "source_id": "btcusdt",
            "source_timestamp": _READY005_BASE_TIME,
            "ingest_timestamp": _READY005_BASE_TIME,
            "raw_record_id": f"BINANCE:ohlcv:f.jsonl:{i}",
            "market_id": "BINANCE:BTCUSDT:SPOT",
            "event_time": _READY005_BASE_TIME + timedelta(minutes=i),
            "interval": "1m",
            "open": 50000.0 + i,
            "high": 51000.0 + i,
            "low": 49000.0 - i,
            "close": 50500.0 + i,
            "volume": 100.0 + i,
        }
        for i in range(5)
    ]


def _ready005_number_records() -> list[dict[str, Any]]:
    """NUMBER 2 行（含 release_time/revision_time，as-of 视图可绑定）。"""
    return [
        {
            "schema_version": "1.0",
            "source": "FRED",
            "source_id": "FRED:GDP",
            "source_timestamp": _READY005_BASE_TIME,
            "ingest_timestamp": _READY005_BASE_TIME,
            "raw_record_id": f"FRED:macro:f.jsonl:{i}",
            "observation_time": _READY005_BASE_TIME,
            "release_time": _READY005_BASE_TIME,
            "revision_time": _READY005_BASE_TIME,
            "value": 100.0 + i,
            "units": "IDX",
            "seasonal_adjustment": "SA",
        }
        for i in range(2)
    ]


def _seed_success_run(meta: MetaStore, dataset_id: str, source_id: str) -> None:
    """经 try_lock/finish_run 真实路径写 SUCCESS run（版本元信息事实源）。"""
    row = meta.try_lock_dataset(dataset_id, source_id=source_id)
    meta.finish_run(row.run_id, "SUCCESS", code_version="0.1.0", schema_version="1.0")


class TestOpenQueryServiceConcurrency:
    """READY-005（SR-16）：方案 A 只读探测优先 + 写路径有界退避。"""

    def _seed_queryable(self, cli_env: dict[str, Path], meta: MetaStore) -> None:
        """OHLCV parquet 5 行 + SUCCESS run（query 可真实返回行）。"""
        _seed_canonical_parquet(
            cli_env["data"], CanonicalType.OHLCV, _ready005_ohlcv_records()
        )
        _seed_success_run(meta, "ds_ok", "binance_spot")

    def _spawn_query_subprocess(
        self, cli_env: dict[str, Path]
    ) -> subprocess.Popen[str]:
        script = (
            "from chronoforge.cli.main import app; "
            "app(['query', '--dataset', 'ds_ok', '--json'])"
        )
        env = {
            **os.environ,
            "CHRONOFORGE_DATA_DIR": str(cli_env["data"]),
            "CHRONOFORGE_META_DIR": str(cli_env["meta"]),
            "CHRONOFORGE_SEC_CONTACT": CONTACT,
        }
        return subprocess.Popen(
            [sys.executable, "-c", script],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

    def test_ready_views_skip_write_mode_and_idempotent(
        self,
        monkeypatch: pytest.MonkeyPatch,
        cli_env: dict[str, Path],
        registered: str,
        meta: MetaStore,
    ) -> None:
        """GWT-2 + 幂等：视图齐全时连续打开不进写模式，视图集合稳定。"""
        self._seed_queryable(cli_env, meta)
        settings = Settings.load()

        # 预热：首次调用创建 catalog 并注册视图（写路径，spy 尚未生效）
        service0, con0 = _wiring.open_query_service(settings, meta)
        assert service0.query("ds_ok").row_count == 5
        con0.close()

        def _no_write(path: Path, data_dir: str) -> duckdb.DuckDBPyConnection:
            raise AssertionError("write path entered despite complete views")

        monkeypatch.setattr(_wiring, "_register_and_reopen", _no_write)
        seen: list[set[str]] = []
        for _ in range(3):
            service, con = _wiring.open_query_service(settings, meta)
            seen.append(get_registered_view_names(con))
            assert service.query("ds_ok").row_count == 5
            con.close()
        assert all(s == seen[0] for s in seen)
        assert "ohlcv" in seen[0]

        # D07 §5：返回连接为 read_only（写 → 异常）
        _, con = _wiring.open_query_service(settings, meta)
        with pytest.raises(duckdb.Error):
            con.execute("CREATE TABLE t(x INT)")
        con.close()

    def test_write_path_lock_retry_converges(
        self,
        monkeypatch: pytest.MonkeyPatch,
        cli_env: dict[str, Path],
        registered: str,
        meta: MetaStore,
    ) -> None:
        """有界重试语义：写打开遇锁冲突 → 指数退避后收敛（进程内注入）。"""
        monkeypatch.setattr(_wiring, "_WRITE_OPEN_BACKOFF_S", 0.01)
        real_connect = duckdb.connect
        state = {"fails": 0}

        def flaky_connect(path: str, *args: Any, **kwargs: Any) -> Any:
            if "read_only" not in kwargs and state["fails"] < 2:
                state["fails"] += 1
                raise duckdb.IOException("Could not set lock on file")
            return real_connect(path, *args, **kwargs)

        monkeypatch.setattr(_wiring.duckdb, "connect", flaky_connect)
        _, con = _wiring.open_query_service(Settings.load(), meta)
        con.close()
        assert state["fails"] == 2
        assert (cli_env["meta"] / "query.duckdb").exists()

    def test_write_path_lock_retry_exhausted(
        self,
        monkeypatch: pytest.MonkeyPatch,
        cli_env: dict[str, Path],
        registered: str,
        meta: MetaStore,
    ) -> None:
        """重试耗尽 → IOException 有界上抛（不死等、不静默）。"""
        monkeypatch.setattr(_wiring, "_WRITE_OPEN_BACKOFF_S", 0.01)
        real_connect = duckdb.connect

        def always_locked(path: str, *args: Any, **kwargs: Any) -> Any:
            if "read_only" not in kwargs:
                raise duckdb.IOException("Could not set lock on file")
            return real_connect(path, *args, **kwargs)

        monkeypatch.setattr(_wiring.duckdb, "connect", always_locked)
        with pytest.raises(duckdb.IOException):
            _wiring.open_query_service(Settings.load(), meta)

    def test_missing_views_detects_late_type(
        self,
        cli_env: dict[str, Path],
        registered: str,
        meta: MetaStore,
    ) -> None:
        """探测与惰性注册同源：已注册 → 空集；新增 NUMBER 数据 → 检出缺失。"""
        settings = Settings.load()
        _seed_canonical_parquet(
            cli_env["data"], CanonicalType.OHLCV, _ready005_ohlcv_records()
        )
        _, con1 = _wiring.open_query_service(settings, meta)
        assert missing_views(con1, str(settings.data_dir)) == set()

        # 新增 NUMBER 数据（catalog 未重注册）→ 基础 + as-of 视图均检出
        _seed_canonical_parquet(
            cli_env["data"], CanonicalType.NUMBER, _ready005_number_records()
        )
        assert missing_views(con1, str(settings.data_dir)) == {
            "number",
            "number_asof",
        }
        con1.close()

        # 重开 → 检测到缺失走写路径补注册 → 再探测为空集
        _, con2 = _wiring.open_query_service(settings, meta)
        assert missing_views(con2, str(settings.data_dir)) == set()
        assert {"number", "number_asof"} <= get_registered_view_names(con2)
        con2.close()

    def test_concurrent_query_fresh_catalog_smoke(
        self, cli_env: dict[str, Path], registered: str, meta: MetaStore
    ) -> None:
        """GWT-1 态 1（全新 query.duckdb）：两进程并发 query → 收敛无锁异常。"""
        self._seed_queryable(cli_env, meta)
        procs = [self._spawn_query_subprocess(cli_env) for _ in range(2)]
        outs = [p.communicate(timeout=60) for p in procs]
        for proc, (out, err) in zip(procs, outs, strict=True):
            assert proc.returncode == 0, f"stdout={out}\nstderr={err}"
            assert "Could not set lock" not in err
            payload = json.loads(out)
            assert payload["row_count"] == 5
            assert payload["rows"][0]["close"] == 50500.0

    def test_concurrent_query_registered_views_smoke(
        self, cli_env: dict[str, Path], registered: str, meta: MetaStore
    ) -> None:
        """GWT-1 态 2（视图已注册）：预热后两进程并发 query → 双 fast path。"""
        self._seed_queryable(cli_env, meta)
        _, warm = _wiring.open_query_service(Settings.load(), meta)
        warm.close()
        procs = [self._spawn_query_subprocess(cli_env) for _ in range(2)]
        outs = [p.communicate(timeout=60) for p in procs]
        for proc, (out, err) in zip(procs, outs, strict=True):
            assert proc.returncode == 0, f"stdout={out}\nstderr={err}"
            assert "Could not set lock" not in err
            payload = json.loads(out)
            assert payload["row_count"] == 5
            assert payload["dataset_version"] == "0.1.0+1.0"


# ── 采集就绪审计回归（2026-09-22 R2-01/R2-02）────────────────────────


class TestPipelineRunIsolation:
    """R2-02：--all-due 逐 dataset 异常隔离（架构 08 §3「批量 run 中
    单 dataset 失败不影响其余」）。"""

    def test_run_all_due_isolates_per_dataset_failures(
        self,
        monkeypatch: pytest.MonkeyPatch,
        cli_env: dict[str, Path],
        meta: MetaStore,
        registered: str,
    ) -> None:
        """Given --all-due 且 ds_bad 装配抛 ConfigError When run
        Then ds_ok 照常执行、汇总失败清单、退出码 1。"""
        add_dataset(
            meta,
            dataset_id="ds_bad",
            source_id="binance_spot",
            canonical_type="OHLCV",
            entity_id="BADUSDT",
            params={},
        )

        def _build_connector(sid: str, s: Any, params: dict[str, Any]) -> object:
            if not params:  # ds_bad（params={}）→ 装配失败
                raise ConfigError("injected bad params", context={"source_id": sid})
            return object()

        stub = _StubRunner()
        monkeypatch.setattr(_wiring, "build_connector", _build_connector)
        monkeypatch.setattr(
            _wiring, "build_runner", lambda s, m, c, sid, stack=None: stub
        )

        result = runner.invoke(app, ["pipeline", "run", "--all-due"])
        assert result.exit_code == 1
        assert [j.dataset_id for j in stub.jobs] == ["ds_ok"]  # 失败未阻塞队列
        assert "ds_bad" in result.stderr
        assert "error: 1/2 dataset(s) failed" in result.stderr


class TestPipelineCircuitReset:
    """R2-01：pipeline circuit-reset 运维恢复入口（幂等）。"""

    def test_circuit_reset_reopens_dataset(
        self, cli_env: dict[str, Path], meta: MetaStore, registered: str
    ) -> None:
        """Given 熔断打开 When circuit-reset Then 复位且输出 status=RESET。"""
        for _ in range(3):
            meta.record_circuit_failure("binance_spot", "ds_ok", threshold=3)
        assert meta.is_circuit_open("binance_spot", "ds_ok")

        result = runner.invoke(app, ["pipeline", "circuit-reset", "--dataset", "ds_ok"])
        assert result.exit_code == 0, result.output
        assert "status=RESET" in result.output
        assert not meta.is_circuit_open("binance_spot", "ds_ok")

        # 幂等：未打开时执行无副作用
        result2 = runner.invoke(app, ["pipeline", "circuit-reset", "--dataset", "ds_ok"])
        assert result2.exit_code == 0, result2.output

    def test_circuit_reset_unknown_dataset_fails(
        self, cli_env: dict[str, Path], meta: MetaStore
    ) -> None:
        result = runner.invoke(app, ["pipeline", "circuit-reset", "--dataset", "ghost"])
        assert result.exit_code == 1
        assert "error:" in result.stderr
