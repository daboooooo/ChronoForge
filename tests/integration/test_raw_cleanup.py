"""STORAGE-002.2 集成测试 — RawStore cleanup / list_partitions / get_raw_stats。

依据：STORAGE-002.2.md 测试要求 + GWT 验收标准
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from chronoforge.storage.raw import RawStore

# ── Helpers ────────────────────────────────────────────────────────────


def _make_batch(
    source: str = "src",
    dataset: str = "ds",
    url: str = "https://example.com/data.json",
    payload: bytes = b'{"data": "test"}',
    fetched_at: datetime | None = None,
    ingest_timestamp: datetime | None = None,
    ingest_batch_id: str = "batch_001",
) -> dict:
    """构造单条 batch 数据。"""
    now = datetime.now(UTC).replace(tzinfo=None)
    return {
        "url": url,
        "payload": payload,
        "fetched_at": fetched_at or now,
        "ingest_batch_id": ingest_batch_id,
        "ingest_timestamp": ingest_timestamp or fetched_at or now,
    }


# ── cleanup_old_partitions tests ──────────────────────────────────────


class TestCleanupOldPartitions:

    def test_retention_0_deletes_all_partitions(self, tmp_stores) -> None:
        """retention_days=0 删除所有分区"""
        store = RawStore(str(tmp_stores.data_dir))

        now = datetime.now(UTC).replace(tzinfo=None)
        batches = [_make_batch(
            fetched_at=now, ingest_timestamp=now,
            payload=b"keep",
        )]
        store.append("src", "ds", batches)

        deleted = store.cleanup_old_partitions(0)
        assert deleted == 1

        # 分区目录应被删除
        date_dir = tmp_stores.data_dir / "raw" / "src" / "ds" / "ingest_date=*"
        assert list(date_dir.parent.glob("ingest_date=*")) == []

    def test_retention_365_keeps_recent(self, tmp_stores) -> None:
        """retention_days=365 保留最近 365 天的分区"""
        store = RawStore(str(tmp_stores.data_dir))

        now = datetime.now(UTC).replace(tzinfo=None)
        # 最近 100 天的数据
        recent = _make_batch(
            fetched_at=now, ingest_timestamp=now,
            payload=b"recent",
        )
        store.append("src", "ds", [recent])

        deleted = store.cleanup_old_partitions(365)
        assert deleted == 0  # 保留

    def test_retention_10_keeps_only_within_window(self, tmp_stores) -> None:
        """retention_days=10 仅保留最近 10 天的分区"""
        store = RawStore(str(tmp_stores.data_dir))

        now = datetime.now(UTC).replace(tzinfo=None)
        # 5 天前（应保留）
        store.append("src", "ds", [_make_batch(
            fetched_at=now - timedelta(days=5),
            ingest_timestamp=now - timedelta(days=5),
            payload=b"keep",
        )])
        # 20 天前（应删除）
        store.append("src", "ds", [_make_batch(
            fetched_at=now - timedelta(days=20),
            ingest_timestamp=now - timedelta(days=20),
            payload=b"delete",
        )])

        deleted = store.cleanup_old_partitions(10)
        assert deleted == 1

    def test_cleanup_empty_dir_returns_0(self, tmp_stores) -> None:
        """空目录返回 0"""
        store = RawStore(str(tmp_stores.data_dir))
        assert store.cleanup_old_partitions(30) == 0

    def test_cleanup_with_subdirectories(self, tmp_stores) -> None:
        """分区目录含子目录（递归删除）"""
        store = RawStore(str(tmp_stores.data_dir))

        now = datetime.now(UTC).replace(tzinfo=None)
        store.append("src", "ds", [_make_batch(
            fetched_at=now, ingest_timestamp=now,
            payload=b"data",
        )])

        # 手动在分区目录下创建一个子目录
        date_dir = tmp_stores.data_dir / "raw" / "src" / "ds"
        ingest_date = now.date().isoformat()
        sub_dir = date_dir / f"ingest_date={ingest_date}" / "subdir"
        sub_dir.mkdir(parents=True, exist_ok=True)
        (sub_dir / "dummy.txt").write_text("temp")

        deleted = store.cleanup_old_partitions(0)
        assert deleted == 1
        assert not (date_dir / f"ingest_date={ingest_date}").exists()


# ── list_partitions tests ─────────────────────────────────────────────


class TestListPartitions:

    def test_list_all_partitions_no_filter(self, tmp_stores) -> None:
        """无参数列出所有分区"""
        store = RawStore(str(tmp_stores.data_dir))

        now = datetime.now(UTC).replace(tzinfo=None)
        store.append("src_a", "ds", [_make_batch(fetched_at=now, ingest_timestamp=now)])
        store.append("src_b", "ds", [_make_batch(
            fetched_at=now - timedelta(days=1),
            ingest_timestamp=now - timedelta(days=1),
        )])

        partitions = store.list_partitions()
        assert len(partitions) == 2
        # 应包含两个 source
        assert any("src_a" in p for p in partitions)
        assert any("src_b" in p for p in partitions)

    def test_list_partitions_filter_by_source(self, tmp_stores) -> None:
        """按 source 过滤"""
        store = RawStore(str(tmp_stores.data_dir))

        now = datetime.now(UTC).replace(tzinfo=None)
        store.append("alpha", "ds", [_make_batch(fetched_at=now, ingest_timestamp=now)])
        store.append("beta", "ds", [_make_batch(
            fetched_at=now - timedelta(days=1),
            ingest_timestamp=now - timedelta(days=1),
        )])

        partitions = store.list_partitions(source="alpha")
        assert len(partitions) == 1
        assert "alpha" in partitions[0]
        assert "beta" not in partitions[0]

    def test_list_partitions_filter_by_dataset(self, tmp_stores) -> None:
        """按 dataset 过滤"""
        store = RawStore(str(tmp_stores.data_dir))

        now = datetime.now(UTC).replace(tzinfo=None)
        store.append("src", "ds_a", [_make_batch(fetched_at=now, ingest_timestamp=now)])
        store.append("src", "ds_b", [_make_batch(
            fetched_at=now - timedelta(days=1),
            ingest_timestamp=now - timedelta(days=1),
        )])

        partitions = store.list_partitions(dataset="ds_a")
        assert len(partitions) == 1
        assert "/ds_a/" in partitions[0]
        assert "/ds_b/" not in partitions[0]

    def test_list_partitions_filter_by_source_and_dataset(self, tmp_stores) -> None:
        """同时按 source + dataset 过滤"""
        store = RawStore(str(tmp_stores.data_dir))

        now = datetime.now(UTC).replace(tzinfo=None)
        store.append("src", "ds", [_make_batch(fetched_at=now, ingest_timestamp=now)])

        partitions = store.list_partitions(source="src", dataset="ds")
        assert len(partitions) == 1

    def test_list_partitions_empty_returns_empty_list(self, tmp_stores) -> None:
        """无分区返回空列表"""
        store = RawStore(str(tmp_stores.data_dir))
        assert store.list_partitions() == []

    def test_list_partitions_sorted(self, tmp_stores) -> None:
        """返回结果按路径排序"""
        store = RawStore(str(tmp_stores.data_dir))

        now = datetime.now(UTC).replace(tzinfo=None)
        for days in [1, 3, 2]:
            store.append("src", "ds", [_make_batch(
                fetched_at=now - timedelta(days=days),
                ingest_timestamp=now - timedelta(days=days),
            )])

        partitions = store.list_partitions(source="src", dataset="ds")
        assert partitions == sorted(partitions)

    def test_list_partitions_nonexistent_source_returns_empty(self, tmp_stores) -> None:
        """不存在的 source 返回空列表"""
        store = RawStore(str(tmp_stores.data_dir))
        assert store.list_partitions(source="nonexistent") == []


# ── get_raw_stats tests ───────────────────────────────────────────────


class TestGetRawStats:

    def test_get_raw_stats_empty_returns_zeros(self, tmp_stores) -> None:
        """空目录返回 0"""
        store = RawStore(str(tmp_stores.data_dir))
        stats = store.get_raw_stats("src", "ds")
        assert stats["total_records"] == 0
        assert stats["total_size_bytes"] == 0
        assert stats["partition_count"] == 0
        assert stats["date_range"] == (None, None)

    def test_get_raw_stats_with_data(self, tmp_stores) -> None:
        """有数据时统计准确"""
        store = RawStore(str(tmp_stores.data_dir))

        # 固定基准时间远离 UTC 午夜：now+1h 若跨日会使 partition_count 变 2
        now = datetime(2026, 1, 15, 10, 0)
        store.append("src", "ds", [
            _make_batch(fetched_at=now, ingest_timestamp=now, payload=b"aaaa"),
            _make_batch(fetched_at=now, ingest_timestamp=now, payload=b"bbbb"),
            _make_batch(
                fetched_at=now + timedelta(hours=1),
                ingest_timestamp=now + timedelta(hours=1),
                payload=b"cccc",
            ),
        ])

        stats = store.get_raw_stats("src", "ds")
        assert stats["total_records"] == 3
        assert stats["partition_count"] == 1
        assert stats["total_size_bytes"] > 0
        assert stats["date_range"][0] is not None
        assert stats["date_range"][1] is not None

    def test_get_raw_stats_cross_day(self, tmp_stores) -> None:
        """跨日数据的 date_range 包含两端"""
        store = RawStore(str(tmp_stores.data_dir))

        now = datetime.now(UTC).replace(tzinfo=None)
        store.append("src", "ds", [
            _make_batch(fetched_at=now, ingest_timestamp=now, payload=b"day1"),
            _make_batch(
                fetched_at=now + timedelta(days=1),
                ingest_timestamp=now + timedelta(days=1),
                payload=b"day2",
            ),
        ])

        stats = store.get_raw_stats("src", "ds")
        assert stats["total_records"] == 2
        assert stats["partition_count"] == 2
        # date_range 的 end 应包含 day2 的时间
        assert stats["date_range"][1] >= (now + timedelta(days=1)).isoformat()

    def test_get_raw_stats_nonexistent_source(self, tmp_stores) -> None:
        """不存在的 source/dataset 返回全 0"""
        store = RawStore(str(tmp_stores.data_dir))
        stats = store.get_raw_stats("no_such_src", "no_such_ds")
        assert stats["total_records"] == 0
        assert stats["partition_count"] == 0

    def test_get_raw_stats_multiple_source_dataset(self, tmp_stores) -> None:
        """不同 source/dataset 统计独立"""
        store = RawStore(str(tmp_stores.data_dir))

        now = datetime.now(UTC).replace(tzinfo=None)
        store.append("src_a", "ds", [
            _make_batch(fetched_at=now, ingest_timestamp=now, payload=b"aaa")
        ])
        store.append("src_b", "ds", [_make_batch(
            fetched_at=now, ingest_timestamp=now,
            payload=b"bbb",
        )])

        stats_a = store.get_raw_stats("src_a", "ds")
        stats_b = store.get_raw_stats("src_b", "ds")

        assert stats_a["total_records"] == 1
        assert stats_b["total_records"] == 1

    def test_get_raw_stats_includes_size_bytes(self, tmp_stores) -> None:
        """total_size_bytes 正确计算"""
        store = RawStore(str(tmp_stores.data_dir))

        now = datetime.now(UTC).replace(tzinfo=None)
        store.append("src", "ds", [
            _make_batch(fetched_at=now, ingest_timestamp=now, payload=b"x" * 1000)
        ])

        stats = store.get_raw_stats("src", "ds")
        assert stats["total_size_bytes"] > 0
        # jsonl 每行 ≈ batch_id(32)+fetched_at(20)+url(30)+base64(1333)+分隔符 ~50 = ~1470
        assert stats["total_size_bytes"] >= 1000
