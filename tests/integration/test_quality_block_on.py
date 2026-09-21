"""TC-Q-004 集成 — block_on 阻断不回滚（D09 §3，VALIDATION-001.2）。

语义：block_on 命中抛 StorageError 阻断当前 run，但此前已写入
Canonical 分区的历史数据原样保留（不回滚已提交的存储变更）；
坏批次在阻断下整体不落盘（validate → upsert 管道序）。
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from chronoforge.exceptions import StorageError
from chronoforge.models.enums import CanonicalType
from chronoforge.quality.rules import run
from chronoforge.storage.canonical import CanonicalStoreImpl

_BASE_TIME = datetime(2026, 6, 1, 0, 0, 0)


def _make_ohlcv_record(
    event_time: datetime = _BASE_TIME,
    raw_record_id: str = "test:ohlcv:1",
    **overrides: object,
) -> dict[str, object]:
    """构造一条 OHLCV dict 记录（CanonicalStoreImpl 输入形态）。"""
    rec: dict[str, object] = {
        "schema_version": "1.0",
        "source": "test_source",
        "source_id": "btcusdt",
        "source_timestamp": event_time,
        "ingest_timestamp": datetime.now(UTC).replace(tzinfo=None),
        "raw_record_id": raw_record_id,
        "market_id": "BINANCE:BTCUSDT:SPOT",
        "event_time": event_time,
        "interval": "1m",
        "open": 50000.0,
        "high": 51000.0,
        "low": 49000.0,
        "close": 50500.0,
        "volume": 100.0,
    }
    rec.update(overrides)
    return rec


def _partition_dir(tmp_stores) -> object:
    return (
        tmp_stores.data_dir / "canonical" / "OHLCV" / "entity=BTC"
        / "year=2026" / "month=06"
    )


class TestBlockOnNoRollback:
    """TC-Q-004: block_on 阻断不回滚。"""

    def test_block_on_preserves_previously_written_partition(
        self, tmp_stores
    ) -> None:
        """阻断抛 StorageError，历史已写分区数据原样保留。"""
        import pyarrow.parquet as pq

        store = CanonicalStoreImpl(str(tmp_stores.data_dir))
        good = _make_ohlcv_record()
        store.upsert([good], CanonicalType.OHLCV, "BTC")

        bad = _make_ohlcv_record(**{"open": float("nan")}, raw_record_id="t:bad")
        with pytest.raises(StorageError):
            run([bad], CanonicalType.OHLCV, block_on=["Q-SCHEMA-001"])

        # 历史分区数据完整
        p = _partition_dir(tmp_stores)
        rows = pq.read_table(str(next(p.glob("*.parquet")))).to_pylist()
        assert len(rows) == 1
        assert rows[0]["close"] == 50500.0

    def test_block_on_rejects_bad_batch_without_partial_write(
        self, tmp_stores
    ) -> None:
        """坏批次被阻断后不写入（validate 先于 upsert 的管道序）。"""
        bad = _make_ohlcv_record(**{"open": float("nan")}, raw_record_id="t:bad")

        with pytest.raises(StorageError):
            run([bad], CanonicalType.OHLCV, block_on=["Q-SCHEMA-001"])
        # 阻断 → 不调用 upsert → 分区不存在
        assert not _partition_dir(tmp_stores).exists()
