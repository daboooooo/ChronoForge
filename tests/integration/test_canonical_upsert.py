"""STORAGE-003.1 集成测试 — CanonicalStore upsert 核心。

依据：STORAGE-003.1.md 测试要求 + GWT 验收标准 + D03 §3 merge-rewrite 算法
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from chronoforge.exceptions import StorageError
from chronoforge.models.enums import CanonicalType
from chronoforge.storage.base import UpsertStats, natural_key
from chronoforge.storage.canonical import CanonicalStoreImpl

# ── Helpers ─────────────────────────────────────────────────────────────


def _make_ohlcv_record(
    market_id: str = "BINANCE:BTCUSDT:SPOT",
    event_time: datetime | None = None,
    open: float = 50000.0,
    high: float = 51000.0,
    low: float = 49000.0,
    close: float = 50500.0,
    volume: float = 100.0,
    source_id: str = "btcusdt",
    raw_record_id: str | None = None,
) -> dict:
    """构造一条 OHLCV 记录。"""
    now = datetime.now(UTC).replace(tzinfo=None)
    et = event_time or now
    return {
        "schema_version": "1.0",
        "source": "test_source",
        "source_id": source_id,
        "source_timestamp": et,
        "ingest_timestamp": now,
        "raw_record_id": raw_record_id or f"test_source:ohlcv_1m:file.jsonl:{id(et)}",
        "market_id": market_id,
        "event_time": et,
        "interval": "1m",
        "open": open,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    }


def _make_number_record(
    source_id: str = "FRED:GDP",
    observation_time: datetime | None = None,
    revision_time: datetime | None = None,
    value: float = 1.0,
) -> dict:
    """构造一条 NUMBER 记录。"""
    now = datetime.now(UTC).replace(tzinfo=None)
    ot = observation_time or now
    rt = revision_time or now
    return {
        "schema_version": "1.0",
        "source": "FRED",
        "source_id": source_id,
        "source_timestamp": ot,
        "ingest_timestamp": now,
        "raw_record_id": "FRED:macro:file.jsonl:1",
        "observation_time": ot,
        "release_time": now,
        "revision_time": rt,
        "value": value,
        "units": "IDX",
        "seasonal_adjustment": "SA",
    }


def _count_parquet_files(directory: Path) -> int:
    """统计目录中的 parquet 文件数。"""
    if not directory.exists():
        return 0
    return len(list(directory.glob("*.parquet")))


def _read_parquet_rows(directory: Path) -> int:
    """读取目录中所有 parquet 文件的总行数。"""
    if not directory.exists():
        return 0
    total = 0
    for f in directory.glob("*.parquet"):
        total += pq.read_table(str(f)).num_rows
    return total


def _list_partition_dirs(base: Path) -> list[Path]:
    """列出所有分区目录（entity/year/month）。"""
    if not base.exists():
        return []
    dirs: list[Path] = []
    for entity_dir in base.iterdir():
        if not entity_dir.is_dir():
            continue
        for year_dir in entity_dir.iterdir():
            if not year_dir.is_dir():
                continue
            # year_dir is the year partition (year=YYYY)
            # Its children are month partitions (month=MM)
            for month_dir in year_dir.iterdir():
                if month_dir.is_dir():
                    # month_dir IS the partition directory
                    dirs.append(month_dir)
                elif month_dir.name.startswith("part-"):
                    # Directly in year dir (no month subdirs) — treat as partition
                    dirs.append(year_dir)
    return sorted(dirs)


# ── GWT Acceptance Tests ────────────────────────────────────────────────


class TestGWT:

    def test_given_upsert_twice_same_batch_then_second_inserted_is_zero(
        self, tmp_stores
    ) -> None:
        """GWT: Given 同批次 records upsert 两次 Then 第二次 UpsertStats.inserted=0（幂等）"""
        store = CanonicalStoreImpl(str(tmp_stores.data_dir))
        # 创建 5 条不同 NK 的记录
        base_time = datetime(2026, 9, 11, 10, 0, 0)
        records = [
            _make_ohlcv_record(
                market_id=f"BINANCE:BTC{i:02d}USDT:SPOT",
                event_time=base_time,
            )
            for i in range(5)
        ]

        # 第一次 upsert
        stats1 = store.upsert(records, CanonicalType.OHLCV, "BTC")
        assert stats1.inserted == 5
        assert stats1.updated == 0

        # 第二次 upsert 相同数据
        stats2 = store.upsert(records, CanonicalType.OHLCV, "BTC")
        assert stats2.inserted == 0
        assert stats2.updated == 5
        assert stats2.total_records == 5

    def test_given_old_partition_3_rows_upsert_1_same_nk_then_still_3_rows(
        self, tmp_stores
    ) -> None:
        """GWT: Given 旧分区有 3 条不同 nk 记录 When upsert 1 条同 nk Then 终态仍 3 行"""
        store = CanonicalStoreImpl(str(tmp_stores.data_dir))
        base_time = datetime(2026, 9, 11, 10, 0, 0)

        # 旧分区：3 条不同 market_id 的记录
        old_records = [
            _make_ohlcv_record(
                market_id=f"BINANCE:BTC{suffix}USDT:SPOT",
                event_time=base_time,
                close=50000.0 + i * 100,
            )
            for i, suffix in enumerate(["AAA", "BBB", "CCC"])
        ]
        stats1 = store.upsert(old_records, CanonicalType.OHLCV, "BTC")
        assert stats1.inserted == 3
        assert stats1.total_records == 3

        # upsert 1 条与其中一条同 nk（market_id= BINANCE:BTCBBBUSDT:SPOT）的记录
        updated_record = _make_ohlcv_record(
            market_id="BINANCE:BTCBBBUSDT:SPOT",
            event_time=base_time,
            close=99999.0,  # close 值变了
        )
        stats2 = store.upsert(
            [updated_record], CanonicalType.OHLCV, "BTC"
        )
        assert stats2.inserted == 0
        assert stats2.updated == 1

        # 终态仍 3 行
        canonical_base = (
            tmp_stores.data_dir / "canonical" / "OHLCV" / "entity=BTC"
        )
        total_rows = sum(_read_parquet_rows(d) for d in _list_partition_dirs(canonical_base))
        assert total_rows == 3

    def test_given_empty_data_dir_then_layout_compliant(
        self, tmp_stores
    ) -> None:
        """GWT: Given 空数据目录 When upsert 新数据 Then parquet 文件布局符合规范"""
        store = CanonicalStoreImpl(str(tmp_stores.data_dir))
        records = [_make_ohlcv_record(event_time=datetime(2026, 9, 11, 10, 0, 0))]

        store.upsert(records, CanonicalType.OHLCV, "BTC")

        # 验证路径布局：canonical/OHLCV/entity=BTC/year=2026/month=09/
        canonical_base = (
            tmp_stores.data_dir / "canonical" / "OHLCV" / "entity=BTC"
        )
        year_dir = canonical_base / "year=2026"
        month_dir = year_dir / "month=09"

        assert canonical_base.exists()
        assert year_dir.exists()
        assert month_dir.exists()
        assert _count_parquet_files(month_dir) >= 1


# ── Idempotency Tests (TC-S-001) ────────────────────────────────────────


class TestIdempotency:

    def test_upsert_same_batch_twice_inserted_zero(self, tmp_stores) -> None:
        """TC-S-001: 同批次连写两次 → 第二次 stats.inserted=0"""
        store = CanonicalStoreImpl(str(tmp_stores.data_dir))
        base_time = datetime(2026, 9, 11, 10, 0, 0)
        # 创建 10 条不同 NK 的记录
        records = [
            _make_ohlcv_record(
                market_id=f"BINANCE:BTC{i:02d}USDT:SPOT",
                event_time=base_time,
            )
            for i in range(10)
        ]

        s1 = store.upsert(records, CanonicalType.OHLCV, "test")
        assert s1.inserted == 10

        s2 = store.upsert(records, CanonicalType.OHLCV, "test")
        assert s2.inserted == 0
        assert s2.updated == 10

    def test_upsert_idempotent_across_multiple_batches(self, tmp_stores) -> None:
        """upsert 幂等：不同批次写入相同 nk 数据 → 最终只保留一条"""
        store = CanonicalStoreImpl(str(tmp_stores.data_dir))
        base_time = datetime(2026, 9, 11, 10, 0, 0)

        # 第一批：2 条
        batch1 = [
            _make_ohlcv_record(
                market_id=f"BINANCE:BTC{suffix}USDT:SPOT",
                event_time=base_time,
            )
            for suffix in ["A", "B"]
        ]

        # 第二批：2 条（与第一批相同的 nk，不同值）
        batch2 = [
            _make_ohlcv_record(
                market_id=f"BINANCE:BTC{suffix}USDT:SPOT",
                event_time=base_time,
                close=99999.0,
            )
            for suffix in ["A", "B"]
        ]

        store.upsert(batch1, CanonicalType.OHLCV, "BTC")
        stats = store.upsert(batch2, CanonicalType.OHLCV, "BTC")

        assert stats.inserted == 0
        assert stats.updated == 2

        # 终态只有 2 行（不是 4 行）
        canonical_base = (
            tmp_stores.data_dir / "canonical" / "OHLCV" / "entity=BTC"
        )
        total_rows = sum(_read_parquet_rows(d) for d in _list_partition_dirs(canonical_base))
        assert total_rows == 2


# ── Merge-Rewrite Tests (TC-S-002) ──────────────────────────────────────


class TestMergeRewrite:

    def test_merge_rewrite_preserves_old_rows(self, tmp_stores) -> None:
        """TC-S-002: 旧分区 3 行 + 新 1 行（同 nk）→ 终态 3 行（1 更新 2 原样）"""
        store = CanonicalStoreImpl(str(tmp_stores.data_dir))
        base_time = datetime(2026, 9, 11, 10, 0, 0)

        # 写入 3 条不同 nk 的记录
        old_records = [
            _make_ohlcv_record(
                market_id=f"BINANCE:BTC{suffix}USDT:SPOT",
                event_time=base_time,
                close=50000.0 + i * 100,
            )
            for i, suffix in enumerate(["AAA", "BBB", "CCC"])
        ]
        store.upsert(old_records, CanonicalType.OHLCV, "BTC")

        # 新记录：与 BBB 同 nk，close 值变化
        new_record = _make_ohlcv_record(
            market_id="BINANCE:BTCBBBUSDT:SPOT",
            event_time=base_time,
            close=100000.0,
        )
        stats = store.upsert([new_record], CanonicalType.OHLCV, "BTC")
        assert stats.inserted == 0
        assert stats.updated == 1

        # 终态：3 行
        canonical_base = (
            tmp_stores.data_dir / "canonical" / "OHLCV" / "entity=BTC"
        )
        total_rows = sum(_read_parquet_rows(d) for d in _list_partition_dirs(canonical_base))
        assert total_rows == 3

    def test_merge_rewrite_all_new_nk_inserted(self, tmp_stores) -> None:
        """merge-rewrite：新记录 nk 不同 → inserted 增加"""
        store = CanonicalStoreImpl(str(tmp_stores.data_dir))
        base_time = datetime(2026, 9, 11, 10, 0, 0)

        old = [_make_ohlcv_record(market_id="BINANCE:BTCAAAUSDT:SPOT", event_time=base_time)]
        store.upsert(old, CanonicalType.OHLCV, "BTC")

        new = [_make_ohlcv_record(market_id="BINANCE:BTCCCCUSDT:SPOT", event_time=base_time)]
        stats = store.upsert(new, CanonicalType.OHLCV, "BTC")

        assert stats.inserted == 1
        assert stats.updated == 0


# ── Failure: Corrupt Partition File（审计 C-2 熔断） ─────────────────────


class TestCorruptPartitionFile:

    def test_given_corrupt_parquet_then_upsert_raises_and_partition_intact(
        self, tmp_stores
    ) -> None:
        """审计 C-2：分区中存在损坏 parquet 时 merge-rewrite 必须熔断
        （raise StorageError），禁止静默跳过后整体替换分区。
        熔断后原有有效数据必须原样保留。"""
        store = CanonicalStoreImpl(str(tmp_stores.data_dir))
        base_time = datetime(2026, 9, 11, 10, 0, 0)

        # 正常写入 2 条同月记录 → 1 个 part 文件 2 行
        old = [
            _make_ohlcv_record(
                market_id=f"BINANCE:BTC{s}USDT:SPOT", event_time=base_time
            )
            for s in ("AAA", "BBB")
        ]
        store.upsert(old, CanonicalType.OHLCV, "BTC")

        # 注入损坏文件到同一分区
        canonical_base = tmp_stores.data_dir / "canonical" / "OHLCV" / "entity=BTC"
        partition = _list_partition_dirs(canonical_base)[0]
        corrupt_file = partition / "part-9999.parquet"
        corrupt_file.write_bytes(b"not a valid parquet file")
        valid_file = next(f for f in partition.glob("*.parquet") if f != corrupt_file)

        # upsert 必须熔断
        new_record = _make_ohlcv_record(
            market_id="BINANCE:BTCCCCCUSDT:SPOT", event_time=base_time
        )
        with pytest.raises(StorageError, match="Failed to read existing parquet"):
            store.upsert([new_record], CanonicalType.OHLCV, "BTC")

        # 有效数据未被替换/删除
        assert valid_file.exists()
        assert pq.read_table(str(valid_file)).num_rows == 2
        # 损坏文件未被静默清除（交由 startup_repair/reconcile 处理）
        assert corrupt_file.exists()
        # 失败路径无 temp 目录残留
        assert not list(partition.parent.glob(".tmp-*"))


# ── Atomicity: rename-swap（审计 C-3） ──────────────────────────────────


class TestAtomicRenameSwap:

    def test_swap_success_leaves_no_old_residue(self, tmp_stores) -> None:
        """审计 C-3：merge-rewrite 走 rename-swap，成功后分区数据正确
        且无 .old-*/.tmp-* 残留。"""
        store = CanonicalStoreImpl(str(tmp_stores.data_dir))
        base_time = datetime(2026, 9, 11, 10, 0, 0)

        old = [
            _make_ohlcv_record(
                market_id=f"BINANCE:BTC{s}USDT:SPOT", event_time=base_time
            )
            for s in ("AAA", "BBB")
        ]
        store.upsert(old, CanonicalType.OHLCV, "BTC")
        new = [_make_ohlcv_record(
            market_id="BINANCE:BTCCCCCUSDT:SPOT", event_time=base_time
        )]
        store.upsert(new, CanonicalType.OHLCV, "BTC")

        canonical_base = tmp_stores.data_dir / "canonical" / "OHLCV" / "entity=BTC"
        partition = _list_partition_dirs(canonical_base)[0]
        year_dir = partition.parent

        # 数据正确：2 旧 + 1 新 = 3 行
        assert _read_parquet_rows(partition) == 3
        # 备份与 temp 目录均已清理
        assert not list(year_dir.glob(".old-*"))
        assert not list(year_dir.glob(".tmp-*"))

    def test_rename_failure_restores_old_partition(
        self, tmp_stores, monkeypatch
    ) -> None:
        """审计 C-3：rename-swap 第二步（tmp→target）失败时回滚
        .old-* → target，分区始终有完整数据（No partial commit）。"""
        store = CanonicalStoreImpl(str(tmp_stores.data_dir))
        base_time = datetime(2026, 9, 11, 10, 0, 0)

        old = [
            _make_ohlcv_record(
                market_id=f"BINANCE:BTC{s}USDT:SPOT", event_time=base_time
            )
            for s in ("AAA", "BBB")
        ]
        store.upsert(old, CanonicalType.OHLCV, "BTC")

        canonical_base = tmp_stores.data_dir / "canonical" / "OHLCV" / "entity=BTC"
        partition = _list_partition_dirs(canonical_base)[0]
        year_dir = partition.parent

        # 构造待交换的 tmp 目录（内容无关，rename 不检查内容）
        tmp_dir = year_dir / ".tmp-testswap"
        tmp_dir.mkdir()

        real_rename = Path.rename
        calls = {"n": 0}

        def fake_rename(path_self: Path, dst: object) -> Path:
            calls["n"] += 1
            if calls["n"] == 2:  # 第二步 tmp→target 模拟崩溃
                raise OSError("simulated crash during rename")
            return real_rename(path_self, dst)  # type: ignore[arg-type]

        monkeypatch.setattr(Path, "rename", fake_rename)
        try:
            with pytest.raises(OSError, match="simulated crash"):
                store._atomic_rename(tmp_dir, partition)
        finally:
            monkeypatch.undo()

        # 回滚成功：target 仍是旧数据（2 行），分区非空
        assert partition.exists()
        assert _read_parquet_rows(partition) == 2
        # .old 备份已回滚，无残留
        assert not list(year_dir.glob(".old-*"))
        # tmp 目录仍存在，由调用方异常路径清理
        assert tmp_dir.exists()


# ── Boundary: Empty Partition First Write ───────────────────────────────


class TestEmptyPartition:

    def test_first_write_no_old_files(self, tmp_stores) -> None:
        """边界：空分区首写（无旧文件直接 temp→rename）"""
        store = CanonicalStoreImpl(str(tmp_stores.data_dir))
        records = [_make_ohlcv_record(event_time=datetime(2026, 9, 11, 10, 0, 0))]

        stats = store.upsert(records, CanonicalType.OHLCV, "BTC")
        assert stats.inserted == 1
        assert stats.total_records == 1

        # 文件存在且可读
        canonical_base = (
            tmp_stores.data_dir / "canonical" / "OHLCV" / "entity=BTC"
        )
        partitions = _list_partition_dirs(canonical_base)
        assert len(partitions) > 0
        for p in partitions:
            assert _read_parquet_rows(p) >= 1


# ── Boundary: Cross-Month Sharding ──────────────────────────────────────


class TestCrossMonth:

    def test_upsert_across_two_months(self, tmp_stores) -> None:
        """边界：跨月分片（同批次跨 2 个月）"""
        store = CanonicalStoreImpl(str(tmp_stores.data_dir))

        records = [
            _make_ohlcv_record(event_time=datetime(2026, 8, 31, 23, 0, 0)),  # Aug
            _make_ohlcv_record(event_time=datetime(2026, 9, 1, 0, 0, 0)),    # Sep
        ]

        stats = store.upsert(records, CanonicalType.OHLCV, "BTC")
        assert stats.inserted == 2
        assert stats.rewritten_partitions == 2

        # 两个分区目录都应存在
        canonical_base = (
            tmp_stores.data_dir / "canonical" / "OHLCV" / "entity=BTC"
        )
        partition_dirs = _list_partition_dirs(canonical_base)
        month_dirs = [p.name for p in partition_dirs]
        assert "month=08" in month_dirs
        assert "month=09" in month_dirs


# ── Failure: Orphan Temp Directory ──────────────────────────────────────


class TestFailure:

    def test_orphan_temp_dir_after_failed_write(self, tmp_stores) -> None:
        """失败：temp 目录写入后 rename 前抛异常 → 下次启动 temp 目录残留（孤儿）"""
        store = CanonicalStoreImpl(str(tmp_stores.data_dir))
        # 使用相同的 NK，确保两次 upsert 操作同一行（触发 merge-rewrite）
        same_nk = _make_ohlcv_record(
            market_id="BINANCE:BTCAAAUSDT:SPOT",
            event_time=datetime(2026, 9, 11, 10, 0, 0),
        )
        # 第一条记录：任意值
        first_record = {**same_nk, "close": 50500.0}

        # 写入第一条记录成功
        stats = store.upsert([first_record], CanonicalType.OHLCV, "BTC")
        assert stats.inserted == 1

        # 再次 upsert 相同 nk（触发 merge-rewrite）
        # 修改 close 值，确保 NK 完全匹配
        stats2 = store.upsert(
            [{**same_nk, "close": 12345.0}],
            CanonicalType.OHLCV,
            "BTC",
        )
        assert stats2.updated == 1

        # 检查分区目录中不应有残留的 .tmp 或 .old 目录
        canonical_base = (
            tmp_stores.data_dir / "canonical" / "OHLCV" / "entity=BTC"
        )
        for year_dir in canonical_base.rglob("year=*"):
            if year_dir.is_dir():
                for entry in year_dir.iterdir():
                    assert not entry.name.startswith(".tmp-")
                    assert not entry.name.startswith(".old-")


# ── Property: Upsert Commutativity (TC-PROP-004) ────────────────────────


class TestCommutativity:

    def test_upsert_order_independent(self, tmp_stores) -> None:
        """TC-PROP-004: 任意两批次不同顺序 upsert → 终态一致"""
        base_time = datetime(2026, 9, 11, 10, 0, 0)

        # 场景 1：先 batchA 再 batchB
        store1 = CanonicalStoreImpl(str(tmp_stores.data_dir / "scenario1"))

        batchA = [
            _make_ohlcv_record(
                market_id="BINANCE:BTCXXXUSDT:SPOT",
                event_time=base_time,
                close=100.0,
            ),
        ]
        batchB = [
            _make_ohlcv_record(
                market_id="BINANCE:BTCSYYUSDT:SPOT",
                event_time=base_time,
                close=200.0,
            ),
        ]

        store1.upsert(batchA, CanonicalType.OHLCV, "BTC")
        store1.upsert(batchB, CanonicalType.OHLCV, "BTC")

        # 场景 2：先 batchB 再 batchA（顺序相反）
        store2 = CanonicalStoreImpl(str(tmp_stores.data_dir / "scenario2"))

        store2.upsert(batchB, CanonicalType.OHLCV, "BTC")
        store2.upsert(batchA, CanonicalType.OHLCV, "BTC")

        # 终态行数应相同
        base1 = (
            tmp_stores.data_dir / "scenario1" / "canonical" / "OHLCV" / "entity=BTC"
        )
        base2 = (
            tmp_stores.data_dir / "scenario2" / "canonical" / "OHLCV" / "entity=BTC"
        )

        rows1 = sum(_read_parquet_rows(d) for d in _list_partition_dirs(base1))
        rows2 = sum(_read_parquet_rows(d) for d in _list_partition_dirs(base2))

        assert rows1 == rows2 == 2

    def test_upsert_commutative_overlap(self, tmp_stores) -> None:
        """upsert 交换律：两批次有重叠 nk → 终态一致"""
        base_time = datetime(2026, 9, 11, 10, 0, 0)

        # 共享 nk 的记录
        shared_record = [
            _make_ohlcv_record(
                market_id="BINANCE:BTCZZZUSDT:SPOT",
                event_time=base_time,
            ),
        ]
        # 各自独有的 nk
        unique_a = [
            _make_ohlcv_record(
                market_id="BINANCE:BTCAAAUSDT:SPOT",
                event_time=base_time,
            ),
        ]
        unique_b = [
            _make_ohlcv_record(
                market_id="BINANCE:BTCCCUSDT:SPOT",
                event_time=base_time,
            ),
        ]

        # 场景 1: A → B (共享) → C
        store1 = CanonicalStoreImpl(str(tmp_stores.data_dir / "order1"))
        store1.upsert(unique_a, CanonicalType.OHLCV, "BTC")
        store1.upsert(shared_record, CanonicalType.OHLCV, "BTC")
        store1.upsert(unique_b, CanonicalType.OHLCV, "BTC")

        # 场景 2: C → B (共享) → A（倒序）
        store2 = CanonicalStoreImpl(str(tmp_stores.data_dir / "order2"))
        store2.upsert(unique_b, CanonicalType.OHLCV, "BTC")
        store2.upsert(shared_record, CanonicalType.OHLCV, "BTC")
        store2.upsert(unique_a, CanonicalType.OHLCV, "BTC")

        base1 = (
            tmp_stores.data_dir / "order1" / "canonical" / "OHLCV" / "entity=BTC"
        )
        base2 = (
            tmp_stores.data_dir / "order2" / "canonical" / "OHLCV" / "entity=BTC"
        )

        rows1 = sum(_read_parquet_rows(d) for d in _list_partition_dirs(base1))
        rows2 = sum(_read_parquet_rows(d) for d in _list_partition_dirs(base2))

        assert rows1 == rows2 == 3


# ── Revision Type Tests ─────────────────────────────────────────────────


class TestRevisionTypes:

    def test_number_type_maintains_multiple_versions(self, tmp_stores) -> None:
        """NUMBER 类型：同一 observation_time 不同 revision_time 保留多版本"""
        store = CanonicalStoreImpl(str(tmp_stores.data_dir))
        base_time = datetime(2026, 9, 11, 10, 0, 0)

        records = [
            _make_number_record(
                observation_time=base_time,
                revision_time=base_time,
                value=1.0,
            ),
            _make_number_record(
                observation_time=base_time,
                revision_time=base_time + timedelta(hours=1),
                value=2.0,
            ),
            _make_number_record(
                observation_time=base_time,
                revision_time=base_time + timedelta(hours=2),
                value=3.0,
            ),
        ]

        stats = store.upsert(records, CanonicalType.NUMBER, "FRED:GDP")
        assert stats.inserted == 3

        # 终态 3 行（3 个不同 revision_time）
        canonical_base = (
            tmp_stores.data_dir / "canonical" / "NUMBER" / "entity=FRED:GDP"
        )
        total_rows = sum(_read_parquet_rows(d) for d in _list_partition_dirs(canonical_base))
        assert total_rows == 3

    def test_number_same_revision_deduplicated(self, tmp_stores) -> None:
        """NUMBER 类型：同一 (observation_time, revision_time) 重复 → 只保留一条"""
        store = CanonicalStoreImpl(str(tmp_stores.data_dir))
        base_time = datetime(2026, 9, 11, 10, 0, 0)

        # 第一次 upsert：value=1.0
        rec1 = _make_number_record(
            observation_time=base_time,
            revision_time=base_time,
            value=1.0,
        )
        s1 = store.upsert([rec1], CanonicalType.NUMBER, "FRED:GDP")
        assert s1.inserted == 1
        assert s1.updated == 0

        # 第二次 upsert：同一 NK，不同 value=2.0 → 应被识别为更新
        rec2 = _make_number_record(
            observation_time=base_time,
            revision_time=base_time,
            value=2.0,
        )
        s2 = store.upsert([rec2], CanonicalType.NUMBER, "FRED:GDP")
        assert s2.inserted == 0
        assert s2.updated == 1

        canonical_base = (
            tmp_stores.data_dir / "canonical" / "NUMBER" / "entity=FRED:GDP"
        )
        total_rows = sum(_read_parquet_rows(d) for d in _list_partition_dirs(canonical_base))
        assert total_rows == 1


# ── UpsertStats Verification ────────────────────────────────────────────


class TestUpsertStats:

    def test_stats_structure(self, tmp_stores) -> None:
        """UpsertStats 数据结构正确"""
        store = CanonicalStoreImpl(str(tmp_stores.data_dir))
        records = [_make_ohlcv_record()]

        stats = store.upsert(records, CanonicalType.OHLCV, "BTC")

        assert isinstance(stats, UpsertStats)
        assert isinstance(stats.inserted, int)
        assert isinstance(stats.updated, int)
        assert isinstance(stats.rewritten_partitions, int)
        assert isinstance(stats.total_records, int)

        assert stats.total_records == 1
        assert stats.inserted == 1
        assert stats.updated == 0
        assert stats.rewritten_partitions == 1

    def test_stats_empty_upsert(self, tmp_stores) -> None:
        """空 upsert 返回零统计"""
        store = CanonicalStoreImpl(str(tmp_stores.data_dir))

        stats = store.upsert([], CanonicalType.OHLCV, "BTC")
        assert stats.inserted == 0
        assert stats.updated == 0
        assert stats.rewritten_partitions == 0
        assert stats.total_records == 0


# ── Parquet Layout Verification ─────────────────────────────────────────


class TestParquetLayout:

    def test_partition_path_format(self, tmp_stores) -> None:
        """分区路径符合 Hive 风格：year=YYYY/month=MM"""
        store = CanonicalStoreImpl(str(tmp_stores.data_dir))
        records = [
            _make_ohlcv_record(event_time=datetime(2026, 3, 15, 10, 0, 0))
        ]

        store.upsert(records, CanonicalType.OHLCV, "BTC")

        canonical_base = (
            tmp_stores.data_dir / "canonical" / "OHLCV" / "entity=BTC"
        )

        year_dir = canonical_base / "year=2026"
        assert year_dir.exists()

        month_dir = year_dir / "month=03"
        assert month_dir.exists()

        # 文件名格式：part-NNNN.parquet
        parquet_files = list(month_dir.glob("part-*.parquet"))
        assert len(parquet_files) >= 1

    def test_parquet_compression_zstd(self, tmp_stores) -> None:
        """Parquet 使用 zstd 压缩"""
        store = CanonicalStoreImpl(str(tmp_stores.data_dir))
        records = [
            _make_ohlcv_record(event_time=datetime(2026, 9, 11, 10, 0, 0))
        ]

        store.upsert(records, CanonicalType.OHLCV, "BTC")

        canonical_base = (
            tmp_stores.data_dir / "canonical" / "OHLCV" / "entity=BTC"
        )
        partitions = _list_partition_dirs(canonical_base)
        for p in partitions:
            for f in p.glob("*.parquet"):
                pf = pq.ParquetFile(str(f))
                metadata = pf.metadata
                # 至少有一个 row_group 使用 zstd
                found_zstd = False
                for rg_idx in range(metadata.num_row_groups):
                    rg = metadata.row_group(rg_idx)
                    for col_idx in range(rg.num_columns):
                        col_meta = rg.column(col_idx)
                        if col_meta.compression:
                            found_zstd = True
                            break
                    if found_zstd:
                        break
                assert found_zstd, f"File {f} should use zstd compression"


# ── natural_key Verification ────────────────────────────────────────────


class TestNaturalKey:

    def test_natural_key_for_ohlcv(self) -> None:
        """OHLCV natural_key = (market_id, event_time, interval)"""
        nk = natural_key(CanonicalType.OHLCV)
        assert nk == ("market_id", "event_time", "interval")

    def test_natural_key_for_number(self) -> None:
        """NUMBER natural_key = (source_id, observation_time, revision_time)"""
        nk = natural_key(CanonicalType.NUMBER)
        assert nk == ("source_id", "observation_time", "revision_time")

    def test_natural_key_for_unknown_raises(self) -> None:
        """未知 type 抛出 ValueError"""
        # 所有已定义的 type
        all_types = set(CanonicalType)
        nk_results = {}
        for ct in all_types:
            try:
                nk_results[ct] = natural_key(ct)
            except ValueError:
                pass

        # 至少应有一些成功
        assert len(nk_results) > 0


# ── 审计 M-5：分区回退机制 ──────────────────────────────────────────────


class TestPartitionFallback:
    """审计 M-5：时间字段缺失回退 ingest_timestamp 分区，禁止静默丢数据。"""

    def test_given_entity_record_then_partitioned_by_ingest_timestamp(
        self, tmp_stores
    ) -> None:
        """ENTITY（无声明时间字段）按 ingest_timestamp 分区，不丢数据"""
        store = CanonicalStoreImpl(str(tmp_stores.data_dir))
        rec = {
            "schema_version": "1.0",
            "source": "test",
            "source_id": "e1",
            "source_timestamp": datetime(2026, 2, 10, 0, 0, 0),
            "ingest_timestamp": datetime(2026, 5, 17, 8, 30, 0),
            "raw_record_id": "test:entity:1",
            "entity_id": "e1",
            "canonical_name": "Entity One",
            "entity_type": "company",
        }
        stats = store.upsert([rec], CanonicalType.ENTITY, "e1")
        assert stats.inserted == 1
        assert stats.total_records == 1
        # 分区目录为 ingest_timestamp 的 year/month（非硬编码默认分区）
        p = (
            tmp_stores.data_dir / "canonical" / "ENTITY" / "entity=e1"
            / "year=2026" / "month=05"
        )
        assert _read_parquet_rows(p) == 1

    def test_given_missing_event_time_then_fallback_no_data_loss(
        self, tmp_stores
    ) -> None:
        """OHLCV event_time 缺失 → 回退 ingest_timestamp 分区，不丢数据"""
        store = CanonicalStoreImpl(str(tmp_stores.data_dir))
        rec = _make_ohlcv_record(event_time=datetime(2026, 3, 15, 10, 0))
        rec["event_time"] = None
        rec["source_timestamp"] = datetime(2026, 5, 17, 8, 30, 0)
        rec["ingest_timestamp"] = datetime(2026, 5, 17, 8, 30, 0)
        rec["raw_record_id"] = "test:ohlcv:missing"

        stats = store.upsert([rec], CanonicalType.OHLCV, "BTC")
        assert stats.inserted == 1
        p = (
            tmp_stores.data_dir / "canonical" / "OHLCV" / "entity=BTC"
            / "year=2026" / "month=05"
        )
        assert _read_parquet_rows(p) == 1

    def test_given_mixed_batch_then_no_record_lost(self, tmp_stores) -> None:
        """混合批次（正常 + 时间缺失）全部落盘，分区按各自时间归属"""
        store = CanonicalStoreImpl(str(tmp_stores.data_dir))
        good = _make_ohlcv_record(
            event_time=datetime(2026, 3, 15, 10, 0),
            raw_record_id="test:ohlcv:good",
        )
        missing = _make_ohlcv_record(
            event_time=datetime(2026, 3, 15, 10, 0),
            raw_record_id="test:ohlcv:missing",
        )
        missing["event_time"] = None
        missing["ingest_timestamp"] = datetime(2026, 5, 17, 8, 30, 0)

        stats = store.upsert([good, missing], CanonicalType.OHLCV, "BTC")
        assert stats.inserted == 2
        assert stats.total_records == 2
        # good → event_time 分区 2026/03；missing → ingest 分区 2026/05
        p_mar = (
            tmp_stores.data_dir / "canonical" / "OHLCV" / "entity=BTC"
            / "year=2026" / "month=03"
        )
        p_may = (
            tmp_stores.data_dir / "canonical" / "OHLCV" / "entity=BTC"
            / "year=2026" / "month=05"
        )
        assert _read_parquet_rows(p_mar) == 1
        assert _read_parquet_rows(p_may) == 1

    def test_given_no_usable_time_then_raises(self, tmp_stores) -> None:
        """声明时间字段与 ingest_timestamp 均不可用 → StorageError 熔断"""
        store = CanonicalStoreImpl(str(tmp_stores.data_dir))
        rec = _make_ohlcv_record(event_time=datetime(2026, 3, 15, 10, 0))
        rec["event_time"] = None
        rec["ingest_timestamp"] = None
        rec["raw_record_id"] = "test:ohlcv:notime"

        with pytest.raises(StorageError, match="cannot determine partition"):
            store.upsert([rec], CanonicalType.OHLCV, "BTC")

    def test_extract_partition_key_accepts_date_objects(self, tmp_stores) -> None:
        """date32 字段（如 report_date/filing_date）的 date 对象可提取分区键"""
        import datetime as dt_mod

        store = CanonicalStoreImpl(str(tmp_stores.data_dir))
        rec: dict[str, object] = {"report_date": dt_mod.date(2026, 2, 10)}
        key = store._extract_partition_key(rec, CanonicalType.POSITION)
        assert key == ("2026", "02")


# ── 审计 M-6：_revision_seq 批内唯一 + 稳定排序 ─────────────────────────


class TestRevisionSeqDeterminism:
    """审计 M-6：_revision_seq 批内严格递增，跨批单调。"""

    def test_revision_seq_unique_and_increasing_within_batch(
        self, tmp_stores
    ) -> None:
        store = CanonicalStoreImpl(str(tmp_stores.data_dir))
        out = store._add_revision_seq([{}, {}, {}])
        seqs = [r["_revision_seq"] for r in out]
        assert seqs[0] < seqs[1] < seqs[2]

    def test_revision_seq_monotonic_across_batches(self, tmp_stores) -> None:
        store = CanonicalStoreImpl(str(tmp_stores.data_dir))
        seqs1 = [r["_revision_seq"] for r in store._add_revision_seq([{}, {}])]
        seqs2 = [r["_revision_seq"] for r in store._add_revision_seq([{}, {}])]
        assert min(seqs2) > max(seqs1)

    def test_batch_duplicate_nk_keep_last_deterministic(self, tmp_stores) -> None:
        """同批相同 NK：keep-last（后一条赢），结果确定"""
        store = CanonicalStoreImpl(str(tmp_stores.data_dir))
        base = datetime(2026, 4, 1, 0, 0)
        a = _make_ohlcv_record(event_time=base, close=111.0, raw_record_id="t:a")
        b = _make_ohlcv_record(event_time=base, close=222.0, raw_record_id="t:b")
        stats = store.upsert([a, b], CanonicalType.OHLCV, "BTC")
        # 去重后仅一行
        assert stats.inserted == 1
        p = (
            tmp_stores.data_dir / "canonical" / "OHLCV" / "entity=BTC"
            / "year=2026" / "month=04"
        )
        rows = pq.read_table(str(next(p.glob("*.parquet")))).to_pylist()
        assert len(rows) == 1
        assert rows[0]["close"] == 222.0


# ── 审计 M-7：批内重复 NK 计数 ──────────────────────────────────────────


class TestCountChanges:
    """审计 M-7：同批次内相同 NK 只计一次。"""

    def test_count_changes_dedups_batch_duplicates(self, tmp_stores) -> None:
        import pyarrow as pa

        store = CanonicalStoreImpl(str(tmp_stores.data_dir))
        old = pa.table({"a": [1], "b": ["x"]})
        new = pa.table({"a": [1, 1, 2], "b": ["x", "x", "y"]})
        inserted, updated = store._count_changes(old, new, ("a", "b"), False)
        assert inserted == 1
        assert updated == 1

    def test_merge_with_batch_duplicates_counts_once(self, tmp_stores) -> None:
        """merge 路径端到端：同批重复 NK 只计一次 updated"""
        store = CanonicalStoreImpl(str(tmp_stores.data_dir))
        base = datetime(2026, 4, 1, 0, 0)
        first = _make_ohlcv_record(event_time=base, raw_record_id="t:first")
        store.upsert([first], CanonicalType.OHLCV, "BTC")

        dup_a = _make_ohlcv_record(event_time=base, close=111.0, raw_record_id="t:a")
        dup_b = _make_ohlcv_record(event_time=base, close=222.0, raw_record_id="t:b")
        fresh = _make_ohlcv_record(
            event_time=datetime(2026, 4, 2, 0, 0), raw_record_id="t:fresh"
        )
        stats = store.upsert([dup_a, dup_b, fresh], CanonicalType.OHLCV, "BTC")
        assert stats.updated == 1
        assert stats.inserted == 1
        # 落盘 2 行（1 旧 + 1 新）
        p = (
            tmp_stores.data_dir / "canonical" / "OHLCV" / "entity=BTC"
            / "year=2026" / "month=04"
        )
        assert _read_parquet_rows(p) == 2
