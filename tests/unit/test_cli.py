"""CLI 单元测试（D08 §2/§6，TC-X-002/003/004 + GWT + 边界/失败组）。

映射测试经 monkeypatch _wiring 组合根（stub runner/service），真实渲染路径
（registry sync/dataset add/status/quality report）走真实 MetaStore。
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from typer.testing import CliRunner

from chronoforge.cli import _wiring
from chronoforge.cli.main import app
from chronoforge.connectors.errors import ConfigError
from chronoforge.registry import add_dataset, bootstrap_defaults
from chronoforge.research.query import QueryResult
from chronoforge.research.snapshot import ReproduceResult, SnapshotRecord
from chronoforge.storage.meta import MetaStore, RunRow

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
            _wiring, "build_runner", lambda s, m, c, sid: stub
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
    def test_sync_bootstraps_seven_sources_idempotent(
        self, cli_env: dict[str, Path], meta: MetaStore
    ) -> None:
        """bootstrap_defaults 幂等（ON CONFLICT UPDATE，D04 §5）。"""
        for _ in range(2):
            result = runner.invoke(app, ["registry", "sync"])
            assert result.exit_code == 0, result.output
            assert "7 sources" in result.output
        assert meta.connection.execute(
            "SELECT COUNT(*) FROM source_registry"
        ).fetchone()[0] == 7

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
            "yahoo", "fred", "sec_edgar",
        }
        fred = next(r for r in rows if r["source_id"] == "fred")
        assert fred["access_type"] == "PUBLIC_WITH_KEY"
        assert json.loads(fred["rate_limit_json"]) == {"req_per_min": 120}

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


def Settings_load() -> Any:  # noqa: N802  # 测试辅助，保持调用点简短
    from chronoforge.config.settings import Settings

    return Settings.load()


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

    def test_unknown_source_raises(self) -> None:
        with pytest.raises(ConfigError, match="Unknown source_id"):
            _wiring.build_connector("nope", Settings_load(), {})


# ── open_meta 启动修复（审计 SR-01，D03 §5.5）──────────────────────────


class TestOpenMetaStartupRepair:
    def test_open_meta_releases_stale_orphan_lock(
        self, cli_env: dict[str, Path], meta: MetaStore, registered: str
    ) -> None:
        """审计 SR-01: Given 崩溃残留的 stale PENDING run When open_meta
        Then release_stale_locks 置 CANCELLED 且同 dataset 锁可重新获取。
        """
        locked = meta.try_lock_dataset("ds_ok", source_id="binance_spot")
        # 回拨 started_at 至固定过去时刻（> 默认 3600s 租约超时）模拟崩溃残留
        meta.connection.execute(
            "UPDATE run_log SET started_at = '2024-01-01T00:00:00Z' "
            "WHERE run_id = ?",
            (locked.run_id,),
        )
        meta.connection.commit()

        repaired = _wiring.open_meta(Settings_load())

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
