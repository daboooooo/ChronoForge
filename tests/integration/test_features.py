"""QUERY-002 集成测试 — FeatureEngine 协议 + 5 个 P0 特征（D07 §2）。

依据：
- D07 §2 FeatureEngine 协议/dependencies 声明式/5 特征公式（公式列即契约）
- QUERY-002.md 测试要求：特征链依赖注册、黄金值（固定输入→固定输出）、
  property 确定性（同输入两次计算逐字节一致）、replay（dependencies 版本 +
  code_version → output hash 一致）
- D09 TC-R-005（特征链依赖注册 + replay 一致）

测试环境：SQLite meta 登记 dataset_registry（特征依赖数据集 ohlcv/funding/
open_interest/liquidation_aggregate/implied_volatility/prediction_price）；
canonical parquet 手工写入（test_query.py 同惯例）；DuckDB catalog 先 write
注册视图再 read_only 重开供 QueryService 使用；特征链经真实 FEATURE parquet
落盘 + 视图重注册 + QueryService 取数闭环。
"""

from __future__ import annotations

import math
import sqlite3
import statistics
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from chronoforge.features import (
    BTCMarketStressFeature,
    FeatureRegistry,
    FeatureResult,
    FundingOIDivergenceFeature,
    IVSurfaceFeature,
    RealizedVolFeature,
    ReturnsFeature,
    create_default_registry,
)
from chronoforge.models.enums import CanonicalType
from chronoforge.research import DatasetRegistry, DuckDBQueryService
from chronoforge.storage.meta import MetaStore
from chronoforge.storage.views import register_views

# ── 固定基准时刻与确定性数据网格 ────────────────────────────────────────

BASE_TIME = datetime(2026, 9, 11, 10, 0, 0)
# 6 点对齐网格 T0..T5（全部数据集共用；OHLCV 收盘序列派生 returns/vol）
GRID = [BASE_TIME + timedelta(hours=i) for i in range(6)]
CLOSES = [100.0, 110.0, 105.0, 115.0, 108.0, 120.0]
FUNDING_RATES = [0.0100, 0.0200, 0.0050, 0.0120, 0.0180, 0.0040]
OI_VALUES = [1000.0, 1100.0, 1200.0, 1050.0, 1300.0, 1250.0]
# IV：两 instrument（ATM strike=forward=100000 → moneyness 1.0；OTM 0.9）+ PERP 干扰行
IV_ATM_ID = "BTC-2026-09-26-100000-C"
IV_OTM_ID = "BTC-2026-09-26-90000-P"
IV_PERP_ID = "BTC-PERP"  # 不可解析（D-6：排除）
IV_ROWS = [
    # (event_time, instrument_id, mark_iv, implied_forward)
    (GRID[0], IV_ATM_ID, 0.50, 100000.0),
    (GRID[0], IV_OTM_ID, 0.60, 100000.0),
    (GRID[0], IV_PERP_ID, 0.40, None),
    (GRID[1], IV_ATM_ID, 0.52, 100000.0),
    (GRID[1], IV_OTM_ID, 0.64, 100000.0),
]
EXPIRY = datetime(2026, 9, 26).date()

DEPENDENCY_DATASETS: dict[str, str] = {
    # dataset_id → canonical_type（视图名 = type 小写，与 D07 §2 dependencies 一致）
    "ohlcv": "OHLCV",
    "funding": "FUNDING",
    "open_interest": "OPEN_INTEREST",
    "liquidation_aggregate": "LIQUIDATION_AGGREGATE",
    "implied_volatility": "IMPLIED_VOLATILITY",
    "prediction_price": "PREDICTION_PRICE",
}


# ── 测试环境装配 ────────────────────────────────────────────────────────


@dataclass(frozen=True)
class FeatureEnv:
    """测试环境：service/registry/依赖句柄（meta 保持存活以维持 SQLite 连接）。"""

    service: DuckDBQueryService
    con_ro: duckdb.DuckDBPyConnection
    meta: MetaStore
    data_dir: Path
    catalog_path: Path
    registry: FeatureRegistry


def _register_base_datasets(meta: MetaStore) -> None:
    """登记 6 个特征依赖数据集（幂等；liquidation_aggregate 无 parquet 亦登记）。"""
    for dataset_id, canonical_type in DEPENDENCY_DATASETS.items():
        meta.connection.execute(
            "INSERT OR IGNORE INTO source_registry"
            "(source_id, display_name, access_type, base_url, rate_limit_json, "
            "historical_limit_days, license, enabled) "
            "VALUES ('TEST', 'TEST', 'PUBLIC', 'https://example.com', '{}', "
            "NULL, 'public', 1)"
        )
        meta.connection.execute(
            "INSERT OR IGNORE INTO dataset_registry"
            "(dataset_id, source_id, canonical_type, entity_id, params_json, "
            "frequency, continuity_model, status, revision_supported, enabled, "
            "created_at) "
            "VALUES (?, 'TEST', ?, 'test', '{}', NULL, 'ALWAYS_OPEN', "
            "'UNKNOWN', 0, 1, datetime('now'))",
            (dataset_id, canonical_type),
        )
    meta.connection.commit()


def _base_records(
    canonical_type: str, values: list[float]
) -> list[dict[str, Any]]:
    """6 点网格上的基础记录（OHLCV 之外为单值列；OHLCV 由 CLOSES 派生四价）。"""
    records: list[dict[str, Any]] = []
    for i, t in enumerate(GRID):
        base: dict[str, Any] = {
            "schema_version": "1.0",
            "source": "TEST",
            "source_id": "test",
            "source_timestamp": t,
            "ingest_timestamp": BASE_TIME,
            "raw_record_id": f"TEST:{canonical_type.lower()}:f.jsonl:{i}",
            "event_time": t,
        }
        if canonical_type == "OHLCV":
            base.update(
                market_id="BINANCE:BTCUSDT:SPOT",
                interval="1h",
                open=CLOSES[i],
                high=CLOSES[i] + 5.0,
                low=CLOSES[i] - 5.0,
                close=CLOSES[i],
                volume=100.0 + i,
            )
        elif canonical_type == "FUNDING":
            base.update(market_id="BINANCE:BTCUSDT:USDT-FUT", funding_rate=values[i])
        elif canonical_type == "OPEN_INTEREST":
            base.update(market_id="BINANCE:BTCUSDT:USDT-FUT", open_interest=values[i])
        elif canonical_type == "PREDICTION_PRICE":
            base.update(market_id="POLY:BTC-UP:OUT", outcome_id="UP", price=values[i])
        records.append(base)
    return records


def _iv_records() -> list[dict[str, Any]]:
    """IMPLIED_VOLATILITY 记录（IV_ROWS 展开；data_tier 列模型必填，值无关 surface）。"""
    return [
        {
            "schema_version": "1.0",
            "source": "TEST",
            "source_id": "test",
            "source_timestamp": t,
            "ingest_timestamp": BASE_TIME,
            "raw_record_id": f"TEST:iv:f.jsonl:{i}",
            "instrument_id": instrument_id,
            "event_time": t,
            "iv": mark_iv,
            "mark_iv": mark_iv,
            "bid_iv": None,
            "ask_iv": None,
            "implied_forward": forward,
            "data_tier": "FULL",
        }
        for i, (t, instrument_id, mark_iv, forward) in enumerate(IV_ROWS)
    ]


def _write_parquet(
    data_dir: Path, canonical_type: CanonicalType, records: list[dict[str, Any]]
) -> None:
    """records 写为 canonical/{TYPE}/entity=…/year=…/month=…/part-0001.parquet。"""
    first = records[0]
    entity = first.get("entity_id", first.get("source_id", "test"))
    obs = first.get("observation_time", first.get("event_time", BASE_TIME))
    partition_dir = (
        data_dir / "canonical" / canonical_type.value
        / f"entity={entity}" / f"year={obs.strftime('%Y')}" / f"month={obs.strftime('%m')}"
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
    table = pa.table(arrays)
    pq.write_table(table, str(partition_dir / "part-0001.parquet"), compression="zstd")


def _persist_feature_parquet(
    data_dir: Path, feature_name: str, times: list[datetime], values: list[float]
) -> None:
    """FEATURE 视图兼容 parquet（name/computed_at/value；嵌套列略——视图按列读取）。"""
    records = [
        {
            "schema_version": "1.0",
            "source": "CHRONOFORGE",
            "source_id": feature_name,
            "source_timestamp": BASE_TIME,
            "ingest_timestamp": BASE_TIME,
            "raw_record_id": f"FEATURE:{feature_name}:part-0001:{i}",
            "name": feature_name,
            "computed_at": t,
            "value": v,
        }
        for i, (t, v) in enumerate(zip(times, values, strict=True))
    ]
    _write_parquet(data_dir, CanonicalType.FEATURE, records)


def _reopen_read_only(env: FeatureEnv) -> FeatureEnv:
    """关闭只读连接 → write 重注册视图（拾取新增 parquet）→ 只读重开。"""
    env.con_ro.close()
    con_write = duckdb.connect(str(env.catalog_path))
    register_views(con_write, str(env.data_dir))
    con_write.close()
    con_ro = duckdb.connect(str(env.catalog_path), read_only=True)
    service = DuckDBQueryService(con_ro, DatasetRegistry(env.meta.connection))
    return FeatureEnv(
        service=service,
        con_ro=con_ro,
        meta=env.meta,
        data_dir=env.data_dir,
        catalog_path=env.catalog_path,
        registry=env.registry,
    )


def _make_env(tmp_stores: Any, *, with_data: bool = True) -> FeatureEnv:
    """装配测试环境：meta 登记 → parquet（可选）→ write 注册视图 → read_only 重开。"""
    meta = MetaStore(str(tmp_stores.meta_dir))
    meta.migrate()
    _register_base_datasets(meta)

    data_dir = tmp_stores.data_dir
    if with_data:
        _write_parquet(
            data_dir, CanonicalType.OHLCV, _base_records("OHLCV", CLOSES)
        )
        _write_parquet(
            data_dir, CanonicalType.FUNDING, _base_records("FUNDING", FUNDING_RATES)
        )
        _write_parquet(
            data_dir, CanonicalType.OPEN_INTEREST, _base_records("OPEN_INTEREST", OI_VALUES)
        )
        _write_parquet(
            data_dir,
            CanonicalType.PREDICTION_PRICE,
            _base_records("PREDICTION_PRICE", [0.4, 0.6, 0.5, 0.55, 0.45, 0.5]),
        )
        _write_parquet(data_dir, CanonicalType.IMPLIED_VOLATILITY, _iv_records())
        # liquidation_aggregate 有登记无 parquet（D07 公式空分量剔除路径）

    catalog_path = data_dir / "query.duckdb"
    con_write = duckdb.connect(str(catalog_path))
    register_views(con_write, str(data_dir))
    con_write.close()

    con_ro = duckdb.connect(str(catalog_path), read_only=True)
    service = DuckDBQueryService(con_ro, DatasetRegistry(meta.connection))
    registry = create_default_registry(meta.connection)
    return FeatureEnv(
        service=service,
        con_ro=con_ro,
        meta=meta,
        data_dir=data_dir,
        catalog_path=catalog_path,
        registry=registry,
    )


def _expected_returns() -> list[float | None]:
    """独立公式黄金值：log(c_t/c_{t-1})。"""
    return [None] + [math.log(CLOSES[i] / CLOSES[i - 1]) for i in range(1, 6)]


# ── GWT-1 / 依赖声明（D07 §2 表）────────────────────────────────────────


class TestDependencyDeclaration:

    def test_btc_market_stress_dependencies_six(self) -> None:
        """GWT-1: btc_market_stress.dependencies 含 6 项 D07 §2 表所列 dataset。"""
        assert BTCMarketStressFeature.dependencies == [
            "realized_vol",
            "funding",
            "open_interest",
            "liquidation_aggregate",
            "implied_volatility",
            "prediction_price",
        ]

    def test_all_builtin_dependencies_match_d07_table(self) -> None:
        """其余 4 特征 dependencies 与 D07 §2 表逐项一致。"""
        assert ReturnsFeature.dependencies == ["ohlcv"]
        assert RealizedVolFeature.dependencies == ["ohlcv"]
        assert IVSurfaceFeature.dependencies == ["implied_volatility"]
        assert FundingOIDivergenceFeature.dependencies == ["funding", "open_interest"]

    def test_default_registry_exposes_five_features(self, tmp_stores) -> None:
        """create_default_registry 注册 5 个 P0 特征，dependencies 经协议可读。"""
        env = _make_env(tmp_stores)
        features = {f.name: f.dependencies for f in env.registry.all_features()}
        assert set(features) == {
            "returns",
            "realized_vol",
            "iv_surface",
            "funding_oi_divergence",
            "btc_market_stress",
        }
        assert features["btc_market_stress"] == BTCMarketStressFeature.dependencies


# ── 黄金值（固定输入→固定输出）──────────────────────────────────────────


class TestGoldenValues:

    def test_returns_golden(self, tmp_stores) -> None:
        env = _make_env(tmp_stores)
        result = env.registry.compute("returns", env.service, {})

        expected = _expected_returns()
        actual = result.frame["returns"].to_list()
        assert result.frame.height == 6
        assert actual[0] is None
        for a, e in zip(actual[1:], expected[1:], strict=True):
            assert a == pytest.approx(e, rel=1e-12)
        # 输出升序（QueryService 规则 6）
        times = result.frame["event_time"].to_list()
        assert times == sorted(times)

    def test_realized_vol_golden_window2(self, tmp_stores) -> None:
        env = _make_env(tmp_stores)
        result = env.registry.compute("realized_vol", env.service, {"window": 2})

        returns = _expected_returns()[1:]
        assert result.frame.height == 6
        vol = result.frame["realized_vol"].to_list()
        assert vol[0] is None and vol[1] is None  # 样本不足
        for i in range(2, 6):
            expected = statistics.stdev(returns[i - 2:i])
            assert vol[i] == pytest.approx(expected, rel=1e-12)

    def test_realized_vol_default_window_all_null(self, tmp_stores) -> None:
        """缺省 window=20 > 6 行 → 全 null（min_periods=window_size）。"""
        env = _make_env(tmp_stores)
        result = env.registry.compute("realized_vol", env.service, {})
        assert result.params["window"] == 20
        assert all(v is None for v in result.frame["realized_vol"].to_list())

    def test_iv_surface_golden(self, tmp_stores) -> None:
        env = _make_env(tmp_stores)
        result = env.registry.compute("iv_surface", env.service, {})

        # PERP 行排除；ATM 带 [0.95,1.05]：strike=100000/forward=100000 → ATM，
        # 90000/100000=0.9 → OTM；按 (event_time, expiry, data_tier) 聚合
        assert result.frame.height == 4
        rows = result.frame.to_dicts()
        assert (rows[0]["event_time"], rows[0]["data_tier"]) == (GRID[0], "ATM")
        assert rows[0]["iv_mean"] == pytest.approx(0.50)
        assert (rows[1]["event_time"], rows[1]["data_tier"]) == (GRID[0], "OTM")
        assert rows[1]["iv_mean"] == pytest.approx(0.60)
        assert (rows[2]["event_time"], rows[2]["data_tier"]) == (GRID[1], "ATM")
        assert rows[2]["iv_mean"] == pytest.approx(0.52)
        assert (rows[3]["event_time"], rows[3]["data_tier"]) == (GRID[1], "OTM")
        assert rows[3]["iv_mean"] == pytest.approx(0.64)
        assert all(r["n_options"] == 1 for r in rows)
        assert all(r["expiry"] == EXPIRY for r in rows)

    def test_funding_oi_divergence_golden(self, tmp_stores) -> None:
        env = _make_env(tmp_stores)
        result = env.registry.compute("funding_oi_divergence", env.service, {})

        assert result.frame.height == 6
        rows = result.frame.to_dicts()
        # 窗口首行 Δ null → 不标记
        assert rows[0]["divergence_signal"] is None
        assert rows[0]["signal"] is None
        # product 符号黄金值：Δf·ΔOI
        expected_products = [
            None,
            (0.0200 - 0.0100) * (1100.0 - 1000.0),
            (0.0050 - 0.0200) * (1200.0 - 1100.0),
            (0.0120 - 0.0050) * (1050.0 - 1200.0),
            (0.0180 - 0.0120) * (1300.0 - 1050.0),
            (0.0040 - 0.0180) * (1250.0 - 1300.0),
        ]
        expected_signals = [
            None,
            "CONVERGENT",
            "DIVERGENT",
            "DIVERGENT",
            "CONVERGENT",
            "CONVERGENT",
        ]
        for row, ep, es in zip(rows[1:], expected_products[1:], expected_signals[1:], strict=True):
            assert row["divergence_signal"] == pytest.approx(ep, rel=1e-9)
            assert row["signal"] == es

    def test_btc_market_stress_golden_with_chain(self, tmp_stores) -> None:
        """黄金值：realized_vol 链上游落盘 + 并集网格行级可用分量归一。"""
        env = _make_env(tmp_stores)
        for feature in env.registry.all_features():
            feature.register()

        # 链：realized_vol 经 QueryService 计算 → FEATURE parquet 落盘 → 视图重注册
        rv = env.registry.compute("realized_vol", env.service, {"window": 2})
        rv_times = rv.frame["event_time"].to_list()
        vol_col = rv.frame["realized_vol"].to_list()
        chain_times = [t for t, v in zip(rv_times, vol_col, strict=True) if v is not None]
        chain_values = [v for v in vol_col if v is not None]
        assert len(chain_values) == 4  # T2..T5
        _persist_feature_parquet(env.data_dir, "realized_vol", chain_times, chain_values)
        env = _reopen_read_only(env)

        result = env.registry.compute(
            "btc_market_stress",
            env.service,
            {"w_vol": 0.3, "w_funding": 0.2, "w_oi": 0.15},
        )
        # 网格 = 各分量事件时间并集 T0..T5（realized_vol 仅 T2..T5、IV 仅 T0/T1）
        assert result.frame.height == 6
        assert result.frame.columns == [
            "event_time",
            "realized_vol_z",
            "funding_z",
            "open_interest_z",
            "implied_volatility_z",
            "prediction_price_z",
            "btc_market_stress",
        ]

        # 独立公式黄金值：z-score（样本 std，跳过 null）+ 行级可用分量加权
        def z(xs: list[float]) -> list[float]:
            m = statistics.fmean(xs)
            s = statistics.stdev(xs)
            return [(x - m) / s for x in xs]

        z_rv = z(chain_values)
        z_f = z(FUNDING_RATES)
        z_oi = z(OI_VALUES)
        z_pred = z([0.4, 0.6, 0.5, 0.55, 0.45, 0.5])
        # IV 同刻多观测均值聚合：T0 mean(0.50, 0.60, 0.40)=0.50；T1 mean(0.52, 0.64)=0.58
        z_iv = z([0.50, 0.58])
        rows = result.frame.to_dicts()
        for i in range(6):
            row = rows[i]
            assert row["event_time"] == GRID[i]
            if i < 2:  # T0/T1：rv 未覆盖；f+oi+iv+pred 可用
                assert row["realized_vol_z"] is None
                assert row["implied_volatility_z"] == pytest.approx(z_iv[i], rel=1e-9)
                terms = {0.2: z_f[i], 0.15: z_oi[i], 0.1: z_iv[i], 0.05: z_pred[i]}
            else:  # T2..T5：rv+f+oi+pred 可用；IV 未覆盖
                assert row["realized_vol_z"] == pytest.approx(z_rv[i - 2], rel=1e-9)
                assert row["implied_volatility_z"] is None
                terms = {0.3: z_rv[i - 2], 0.2: z_f[i], 0.15: z_oi[i], 0.05: z_pred[i]}
            assert row["funding_z"] == pytest.approx(z_f[i], rel=1e-9)
            assert row["open_interest_z"] == pytest.approx(z_oi[i], rel=1e-9)
            assert row["prediction_price_z"] == pytest.approx(z_pred[i], rel=1e-9)
            expected = sum(w * zv for w, zv in terms.items()) / sum(terms)
            assert row["btc_market_stress"] == pytest.approx(expected, rel=1e-9)

    def test_btc_market_stress_all_components_empty(self, tmp_stores) -> None:
        """全分量无数据 → 空帧（schema 保持：event_time + 6 z 列 + stress）。"""
        env = _make_env(tmp_stores, with_data=False)
        for feature in env.registry.all_features():
            feature.register()
        env = _reopen_read_only(env)

        result = env.registry.compute("btc_market_stress", env.service, {})
        assert result.frame.height == 0
        assert result.frame.columns == [
            "event_time",
            "realized_vol_z",
            "funding_z",
            "open_interest_z",
            "liquidation_aggregate_z",
            "implied_volatility_z",
            "prediction_price_z",
            "btc_market_stress",
        ]


# ── GWT-2 / 确定性与 replay hash（D05 §4）───────────────────────────────


class TestDeterminism:

    def test_same_input_same_output_hash(self, tmp_stores) -> None:
        """GWT-2: 同 dependencies 版本 + 同代码 When 重算 Then output hash 一致。"""
        env = _make_env(tmp_stores)
        h1 = env.registry.compute("returns", env.service, {}).output_hash()
        h2 = env.registry.compute("returns", env.service, {}).output_hash()
        assert h1 == h2

    def test_hash_across_service_instances(self, tmp_stores) -> None:
        """跨 service 实例（重开连接）同输入 → hash 一致（replay 语义）。"""
        env = _make_env(tmp_stores)
        h1 = env.registry.compute("funding_oi_divergence", env.service, {}).output_hash()
        env2 = _reopen_read_only(env)
        h2 = env2.registry.compute("funding_oi_divergence", env2.service, {}).output_hash()
        assert h1 == h2

    def test_hash_changes_with_params(self, tmp_stores) -> None:
        """params 变化 → hash 变化（随行溯源生效）。"""
        env = _make_env(tmp_stores)
        h1 = env.registry.compute("realized_vol", env.service, {"window": 2}).output_hash()
        h2 = env.registry.compute("realized_vol", env.service, {"window": 3}).output_hash()
        assert h1 != h2

    def test_hash_changes_with_code_version(self) -> None:
        """code_version 变化 → hash 变化（dependencies 版本 + code_version 进 hash）。"""
        frame = pl.DataFrame({"x": [1.0, 2.0]})
        r1 = FeatureResult(
            frame=frame, feature_name="f", dependencies=["d"], params={}, code_version="0.1.0"
        )
        r2 = FeatureResult(
            frame=frame, feature_name="f", dependencies=["d"], params={}, code_version="0.2.0"
        )
        assert r1.output_hash() != r2.output_hash()

    def test_hash_independent_of_dependency_order(self) -> None:
        """dependencies 声明顺序不影响 hash（排序后入 hash）。"""
        frame = pl.DataFrame({"x": [1.0]})
        r1 = FeatureResult(
            frame=frame, feature_name="f", dependencies=["a", "b"], params={}, code_version="0.1.0"
        )
        r2 = FeatureResult(
            frame=frame, feature_name="f", dependencies=["b", "a"], params={}, code_version="0.1.0"
        )
        assert r1.output_hash() == r2.output_hash()


# ── 注册中心与 register()（D07 §2 register 语义）────────────────────────


class TestRegistryAndRegister:

    def test_register_writes_dataset_registry(self, tmp_stores) -> None:
        """register() 写 dataset_registry（FEATURE 类型）+ 自产 source 行。"""
        env = _make_env(tmp_stores, with_data=False)
        for feature in env.registry.all_features():
            feature.register()

        rows = env.meta.connection.execute(
            "SELECT dataset_id, source_id, canonical_type, revision_supported "
            "FROM dataset_registry WHERE canonical_type = 'FEATURE' ORDER BY dataset_id"
        ).fetchall()
        assert [r[0] for r in rows] == [
            "btc_market_stress",
            "funding_oi_divergence",
            "iv_surface",
            "realized_vol",
            "returns",
        ]
        assert all(r[1] == "CHRONOFORGE" for r in rows)
        assert all(r[2] == "FEATURE" and r[3] == 0 for r in rows)
        src = env.meta.connection.execute(
            "SELECT source_id FROM source_registry WHERE source_id = 'CHRONOFORGE'"
        ).fetchone()
        assert src is not None

    def test_register_idempotent(self, tmp_stores) -> None:
        """重复 register() 不产生重复行（INSERT OR REPLACE 幂等）。"""
        env = _make_env(tmp_stores, with_data=False)
        feature = env.registry.get("realized_vol")
        assert feature is not None
        feature.register()
        feature.register()
        count = env.meta.connection.execute(
            "SELECT COUNT(*) FROM dataset_registry WHERE dataset_id = 'realized_vol'"
        ).fetchone()[0]
        assert count == 1

    def test_register_without_connection_rejected(self) -> None:
        """未注入连接 → register() ValueError（协议签名冻结的写依赖注入契约）。"""
        feature = ReturnsFeature()
        with pytest.raises(ValueError, match="register"):
            feature.register()

    def test_duplicate_feature_rejected(self) -> None:
        """同名重复注册 → ValueError。"""
        registry = FeatureRegistry()
        registry.register(ReturnsFeature())
        with pytest.raises(ValueError, match="Duplicate feature: returns"):
            registry.register(ReturnsFeature())

    def test_unknown_feature_compute_rejected(self, tmp_stores) -> None:
        """未知特征 compute → ValueError。"""
        env = _make_env(tmp_stores, with_data=False)
        with pytest.raises(ValueError, match="Unknown feature: nope"):
            env.registry.compute("nope", env.service, {})

    def test_negative_weight_rejected(self, tmp_stores) -> None:
        """负权重 → ValueError（合成语义畸变防护）。"""
        env = _make_env(tmp_stores)
        with pytest.raises(ValueError, match="权重必须 >= 0"):
            env.registry.compute("btc_market_stress", env.service, {"w_vol": -0.3})

    def test_register_params_json_defaults(self, tmp_stores) -> None:
        """params_json 写入默认参数（realized_vol window=20）。"""
        env = _make_env(tmp_stores, with_data=False)
        feature = env.registry.get("realized_vol")
        assert feature is not None
        feature.register()
        import json as _json

        row = env.meta.connection.execute(
            "SELECT params_json FROM dataset_registry WHERE dataset_id = 'realized_vol'"
        ).fetchone()
        assert _json.loads(str(row[0])) == {"window": 20}


# ── 特征链查询（D09 TC-R-005：依赖注册后经 QueryService 可查）───────────


class TestFeatureChain:

    def test_realized_vol_registered_and_queryable(self, tmp_stores) -> None:
        """register → FEATURE 登记；落盘后 QueryService 按 name 过滤可查。"""
        env = _make_env(tmp_stores)
        feature = env.registry.get("realized_vol")
        assert feature is not None
        feature.register()

        entry = DatasetRegistry(env.meta.connection).get("realized_vol")
        assert entry is not None
        assert entry.canonical_type == CanonicalType.FEATURE
        assert entry.revision_supported is False

        rv = env.registry.compute("realized_vol", env.service, {"window": 2})
        vol_col = rv.frame["realized_vol"].to_list()
        _persist_feature_parquet(
            env.data_dir,
            "realized_vol",
            [t for t, v in zip(GRID, vol_col, strict=True) if v is not None],
            [float(v) for v in vol_col if v is not None],
        )
        env2 = _reopen_read_only(env)

        result = env2.service.query(
            "realized_vol",
            columns=["computed_at", "value"],
            filters={"name": "realized_vol"},
        )
        assert result.row_count == 4
        values = result.frame["value"].to_list()
        returns = _expected_returns()[1:]
        for i in range(4):
            expected = statistics.stdev(returns[i:i + 2])
            assert values[i] == pytest.approx(expected, rel=1e-12)

    def test_feature_compute_only_via_query_service(
        self, tmp_stores, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """取数只经 QueryService：阻断 q.query → 特征无法取数（D07 §2 规则）。"""
        env = _make_env(tmp_stores)

        def _forbidden(*args: Any, **kwargs: Any) -> None:
            raise AssertionError("feature bypassed QueryService")

        # 连接层禁写双保险：只读连接上写操作必须失败（架构 02 规则 4 执行点）
        con_ro = env.con_ro
        try:
            con_ro.execute("CREATE TABLE forbidden(a INT)")
        except duckdb.Error:
            pass
        else:
            raise AssertionError("read_only connection accepted a write")

        service = DuckDBQueryService(con_ro, DatasetRegistry(env.meta.connection))
        monkeypatch.setattr(
            type(service), "query", _forbidden, raising=True
        )
        with pytest.raises(AssertionError, match="bypassed QueryService"):
            env.registry.compute("returns", service, {})


# ── 直接构造 sqlite3 连接防误用检查（防 py 侧连接混入）──────────────────


def test_feature_result_is_frozen() -> None:
    """FeatureResult 不可变（dataclass frozen）。"""
    frame = pl.DataFrame({"x": [1.0]})
    result = FeatureResult(
        frame=frame, feature_name="f", dependencies=["d"], params={}, code_version="0.1.0"
    )
    with pytest.raises(AttributeError):
        result.feature_name = "g"  # type: ignore[misc]


def test_default_registry_accepts_plain_connection() -> None:
    """create_default_registry 接受裸 sqlite3 连接（类型契约）。"""
    registry = create_default_registry(sqlite3.connect(":memory:"))
    assert len(registry.all_features()) == 5
