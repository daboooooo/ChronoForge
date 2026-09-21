"""审计 H-4 集成测试 — 冻结契约 Arrow schema（D03 §3）。

覆盖：
- 写盘类型不随批内值漂移（批内全 int 值 → 声明 float64）
- 批间值类型漂移时 merge 后 schema 稳定
- 缺失可选字段 → 声明列存在且为 null
- 声明外额外字段保真追加
- legacy 推断 schema 旧分区 cast 兼容；不可 cast 熔断（StorageError）
- DERIVED.params dict → JSON 文本确定性序列化
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from chronoforge.exceptions import StorageError
from chronoforge.models.enums import CanonicalType
from chronoforge.storage.canonical import CanonicalStoreImpl


def _make_ohlcv(
    volume: float,
    close: float = 50500.0,
    event_time: datetime | None = None,
    **overrides: object,
) -> dict:
    now = datetime.now(UTC).replace(tzinfo=None)
    et = event_time or now
    rec: dict = {
        "schema_version": "1.0",
        "source": "test_source",
        "source_id": "btcusdt",
        "source_timestamp": et,
        "ingest_timestamp": now,
        "raw_record_id": f"test:ohlcv:{et.isoformat()}",
        "market_id": "BINANCE:BTCUSDT:SPOT",
        "event_time": et,
        "interval": "1m",
        "open": 50000.0,
        "high": 51000.0,
        "low": 49000.0,
        "close": close,
        "volume": volume,
    }
    rec.update(overrides)
    return rec


def _partition_files(data_dir: Path, canonical_type: str) -> list[Path]:
    base = data_dir / "canonical" / canonical_type
    return sorted(base.glob("entity=*/year=*/month=*/part-*.parquet"))


# ── 声明 schema：类型不随批内值漂移 ──────────────────────────────────


class TestDeclaredSchema:
    def test_given_int_values_when_upsert_then_declared_types(self, tmp_path) -> None:
        """批内 volume=100（int）不驱动类型：写盘为声明 float64/timestamp[us]"""
        store = CanonicalStoreImpl(str(tmp_path))
        store.upsert([_make_ohlcv(volume=100)], CanonicalType.OHLCV, "BTC")

        path = _partition_files(tmp_path, "OHLCV")[0]
        schema = pq.read_schema(str(path))
        assert schema.field("volume").type == pa.float64()
        assert schema.field("event_time").type == pa.timestamp("us")
        assert schema.field("interval").type == pa.string()

    def test_given_type_drift_between_batches_when_merge_then_schema_stable(
        self, tmp_path
    ) -> None:
        """批1 volume=int、批2 volume=float，merge 后 schema 仍为声明 float64"""
        store = CanonicalStoreImpl(str(tmp_path))
        et = datetime(2026, 9, 11, 10, 0, 0)
        store.upsert([_make_ohlcv(volume=100, event_time=et)], CanonicalType.OHLCV, "BTC")
        stats = store.upsert(
            [_make_ohlcv(volume=100.5, event_time=et)], CanonicalType.OHLCV, "BTC"
        )

        assert stats.updated == 1
        path = _partition_files(tmp_path, "OHLCV")[0]
        table = pq.read_table(str(path))
        assert table.schema.field("volume").type == pa.float64()
        assert table.schema.field("event_time").type == pa.timestamp("us")
        assert table.column("volume")[0].as_py() == 100.5

    def test_given_missing_optional_fields_when_upsert_then_declared_null_columns(
        self, tmp_path
    ) -> None:
        """记录缺 quality_status/quality_reason：声明列仍存在（string，null）"""
        store = CanonicalStoreImpl(str(tmp_path))
        rec = _make_ohlcv(volume=1.0)
        assert "quality_status" not in rec and "quality_reason" not in rec
        store.upsert([rec], CanonicalType.OHLCV, "BTC")

        path = _partition_files(tmp_path, "OHLCV")[0]
        table = pq.read_table(str(path))
        assert table.schema.field("quality_status").type == pa.string()
        assert table.schema.field("quality_reason").type == pa.string()
        assert table.column("quality_status")[0].as_py() is None

    def test_given_extra_field_when_upsert_then_column_preserved(self, tmp_path) -> None:
        """声明外额外字段保真追加为列，不丢弃"""
        store = CanonicalStoreImpl(str(tmp_path))
        store.upsert(
            [_make_ohlcv(volume=1.0, note="connector remark")],
            CanonicalType.OHLCV,
            "BTC",
        )

        path = _partition_files(tmp_path, "OHLCV")[0]
        table = pq.read_table(str(path))
        assert "note" in table.column_names
        assert table.column("note")[0].as_py() == "connector remark"

    def test_given_params_dict_when_upsert_then_json_text(self, tmp_path) -> None:
        """DERIVED.params（dict）→ JSON 文本确定性落盘"""
        store = CanonicalStoreImpl(str(tmp_path))
        now = datetime.now(UTC).replace(tzinfo=None)
        rec = {
            "schema_version": "1.0",
            "source": "derived",
            "source_id": "btc",
            "source_timestamp": None,
            "ingest_timestamp": now,
            "raw_record_id": "derived:1",
            "name": "btc_vol",
            "computed_at": now,
            "dependencies": [("BINANCE:BTCUSDT:SPOT", "1")],
            "value": 0.42,
            "params": {"window": 20, "src": "a"},
            "source_type": "derived",
        }
        store.upsert([rec], CanonicalType.DERIVED, "BTC")

        path = _partition_files(tmp_path, "DERIVED")[0]
        table = pq.read_table(str(path))
        assert table.schema.field("params").type == pa.string()
        assert json.loads(str(table.column("params")[0].as_py())) == {
            "src": "a",
            "window": 20,
        }
        assert table.schema.field("dependencies").type == pa.list_(pa.list_(pa.string()))


# ── legacy 兼容：旧推断 schema 分区 ──────────────────────────────────


class TestLegacyCompat:
    def _write_legacy(self, data_dir: Path, et: datetime, volume_value: int) -> None:
        """手工写入 legacy 推断 schema 分区（volume int64、缺 quality 列）"""
        part_dir = data_dir / "canonical" / "OHLCV" / "entity=BTC" / "year=2026" / "month=09"
        part_dir.mkdir(parents=True, exist_ok=True)
        legacy = pa.table({
            "schema_version": ["1.0"],
            "source": ["test_source"],
            "source_id": ["btcusdt"],
            "market_id": ["BINANCE:BTCUSDT:SPOT"],
            "event_time": pa.array([et], type=pa.timestamp("us")),
            "interval": ["1m"],
            "open": pa.array([50000], type=pa.int64()),
            "high": pa.array([51000], type=pa.int64()),
            "low": pa.array([49000], type=pa.int64()),
            "close": pa.array([50500], type=pa.int64()),
            "volume": pa.array([volume_value], type=pa.int64()),
        })
        pq.write_table(legacy, str(part_dir / "part-0001.parquet"))

    def test_given_legacy_drifted_schema_when_merge_then_cast_to_declared(
        self, tmp_path
    ) -> None:
        """legacy int64 旧分区：merge cast 到声明 float64，无数据丢失"""
        et = datetime(2026, 9, 11, 10, 0, 0)
        self._write_legacy(tmp_path, et, volume_value=100)

        store = CanonicalStoreImpl(str(tmp_path))
        stats = store.upsert(
            [_make_ohlcv(volume=100.5, event_time=et)], CanonicalType.OHLCV, "BTC"
        )
        assert stats.updated == 1

        path = _partition_files(tmp_path, "OHLCV")[0]
        table = pq.read_table(str(path))
        # 同 NK keep-last：仅保留新记录
        assert table.num_rows == 1
        assert table.schema.field("volume").type == pa.float64()
        assert table.column("volume")[0].as_py() == 100.5

    def test_given_uncastable_legacy_when_merge_then_storage_error(self, tmp_path) -> None:
        """legacy event_time 非法字符串：cast 失败熔断 StorageError，禁止静默降级"""
        part_dir = tmp_path / "canonical" / "OHLCV" / "entity=BTC" / "year=2026" / "month=09"
        part_dir.mkdir(parents=True, exist_ok=True)
        legacy = pa.table({
            "schema_version": ["1.0"],
            "source": ["test_source"],
            "source_id": ["btcusdt"],
            "market_id": ["BINANCE:BTCUSDT:SPOT"],
            "event_time": ["not-a-date"],
            "interval": ["1m"],
            "volume": pa.array([100], type=pa.int64()),
        })
        pq.write_table(legacy, str(part_dir / "part-0001.parquet"))

        store = CanonicalStoreImpl(str(tmp_path))
        with pytest.raises(StorageError, match="cannot be cast"):
            store.upsert(
                [_make_ohlcv(volume=1.0, event_time=datetime(2026, 9, 11, 10, 0, 0))],
                CanonicalType.OHLCV,
                "BTC",
            )
