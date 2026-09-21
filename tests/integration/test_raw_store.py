"""STORAGE-002.1 集成测试 — RawStore append + iter_refs 核心。

依据：STORAGE-002.1.md 测试要求 + GWT 验收标准 + D03 §2 Raw 层 JSONL 只追加存储
"""

from __future__ import annotations

import json
import os
import stat
from datetime import UTC, datetime

import pytest

from chronoforge.exceptions import StorageError
from chronoforge.storage.raw import RawRef, RawStore

# ── Helpers ────────────────────────────────────────────────────────────


def _make_batch(
    source: str = "test_source",
    dataset: str = "test_dataset",
    url: str = "https://example.com/data.json",
    payload: bytes = b'{"data": "test"}',
    fetched_at: datetime | None = None,
    ingest_batch_id: str = "batch_001",
    ingest_timestamp: datetime | None = None,
) -> dict:
    """构造单条 batch 数据。"""
    now = datetime.now(UTC).replace(tzinfo=None)
    return {
        "source": source,
        "dataset": dataset,
        "url": url,
        "payload": payload,
        "fetched_at": fetched_at or now,
        "ingest_batch_id": ingest_batch_id,
        "ingest_timestamp": ingest_timestamp or fetched_at or now,
    }


def _count_lines(file_path: str) -> int:
    """统计文件中的非空行数。"""
    count = 0
    with open(file_path, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                count += 1
    return count


def _read_jsonl_lines(file_path: str) -> list[dict]:
    """读取 JSONL 文件中的所有 JSON 对象。"""
    results = []
    with open(file_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                results.append(json.loads(line))
    return results


# ── GWT acceptance tests ──────────────────────────────────────────────


class TestGWT:

    def test_given_append_100_rows_when_iter_refs_then_all_returned_with_locatable_refs(
        self, tmp_stores
    ) -> None:
        """Given append 100 行 When iter_refs Then 逐行可回读且 raw_record_id 可定位"""
        store = RawStore(str(tmp_stores.data_dir))

        now = datetime(2026, 9, 11, 10, 0, 0)
        batches = [
            _make_batch(
                fetched_at=now.replace(minute=i % 59, second=(i * 10) % 60),
                payload=f"data_{i}".encode(),
            )
            for i in range(100)
        ]

        refs = store.append("test_source", "test_dataset", batches)
        assert len(refs) == 100

        # 读回所有记录
        read_refs = list(store.iter_refs("test_source", "test_dataset"))
        assert len(read_refs) == 100

        # 验证 raw_record_id 可定位到文件
        for ref in refs:
            assert ref.raw_record_id.startswith("test_source:test_dataset:")
            assert ref.payload == f"data_{ref.line_no - 1}".encode()

    def test_given_same_file_double_append_then_line_count_monotonically_increases(
        self, tmp_stores
    ) -> None:
        """Given 同一文件二次 append Then 文件行数单调递增"""
        store = RawStore(str(tmp_stores.data_dir))

        now = datetime(2026, 9, 11, 10, 0, 0)

        # 第一次写入 10 条
        batches_1 = [
            _make_batch(
                fetched_at=now, ingest_timestamp=now,
                payload=f"batch1_{i}".encode(),
            )
            for i in range(10)
        ]
        refs_1 = store.append("test_source", "test_dataset", batches_1)
        assert len(refs_1) == 10

        # 获取写入的文件路径
        file_path = refs_1[0].file_path
        lines_1 = _count_lines(file_path)

        # 第二次写入 5 条（相同时间戳，同一文件）
        batches_2 = [
            _make_batch(
                fetched_at=now, ingest_timestamp=now,
                payload=f"batch2_{i}".encode(),
            )
            for i in range(5)
        ]
        refs_2 = store.append("test_source", "test_dataset", batches_2)
        assert len(refs_2) == 5

        lines_2 = _count_lines(file_path)

        # 行数单调递增：15 > 10
        assert lines_2 == lines_1 + 5

    def test_given_cross_date_batch_when_append_then_partitioned_by_date(
        self, tmp_stores
    ) -> None:
        """Given ingest_date 跨日 When append Then 按日期分区"""
        store = RawStore(str(tmp_stores.data_dir))

        # 跨两天的数据
        batches = [
            _make_batch(
                fetched_at=datetime(2026, 9, 11, 23, 30, 0),
                ingest_timestamp=datetime(2026, 9, 11, 23, 30, 0),
                payload=b"day1_data",
            ),
            _make_batch(
                fetched_at=datetime(2026, 9, 12, 0, 30, 0),
                ingest_timestamp=datetime(2026, 9, 12, 0, 30, 0),
                payload=b"day2_data",
            ),
        ]

        refs = store.append("test_source", "test_dataset", batches)
        assert len(refs) == 2

        # 验证分区目录
        base = tmp_stores.data_dir / "raw" / "test_source" / "test_dataset"
        day1_path = base / "ingest_date=2026-09-11"
        day2_path = base / "ingest_date=2026-09-12"

        assert day1_path.exists()
        assert day2_path.exists()

        # 每条记录的 file_path 对应正确的分区
        day1_ref = [r for r in refs if "ingest_date=2026-09-11" in r.file_path][0]
        day2_ref = [r for r in refs if "ingest_date=2026-09-12" in r.file_path][0]
        assert day1_ref.fetched_at == datetime(2026, 9, 11, 23, 30, 0)
        assert day2_ref.fetched_at == datetime(2026, 9, 12, 0, 30, 0)


# ── append→iter_refs roundtrip tests ──────────────────────────────────


class TestRoundtrip:

    def test_single_record_roundtrip(self, tmp_stores) -> None:
        """单条记录写入后 iter_refs 读回一致"""
        store = RawStore(str(tmp_stores.data_dir))

        now = datetime(2026, 9, 11, 12, 0, 0)
        payload = b"single_record_payload"
        batches = [_make_batch(fetched_at=now, ingest_timestamp=now, payload=payload)]

        refs = store.append("src", "ds", batches)
        assert len(refs) == 1

        read_refs = list(store.iter_refs("src", "ds"))
        assert len(read_refs) == 1

        read_ref = read_refs[0]
        assert read_ref.url == refs[0].url
        assert read_ref.payload == payload
        assert read_ref.fetched_at == now
        assert read_ref.ingest_batch_id == "batch_001"

    def test_multiple_records_roundtrip(self, tmp_stores) -> None:
        """多条记录写入后 iter_refs 读回一致"""
        store = RawStore(str(tmp_stores.data_dir))

        base_time = datetime(2026, 9, 11, 10, 0, 0)
        payloads = [f"payload_{i}".encode() for i in range(50)]
        batches = [_make_batch(
            fetched_at=base_time.replace(second=i),
            ingest_timestamp=base_time.replace(second=i),
            payload=payloads[i],
        ) for i in range(50)]

        refs = store.append("src", "ds", batches)
        assert len(refs) == 50

        read_refs = list(store.iter_refs("src", "ds"))
        assert len(read_refs) == 50

        for i, read_ref in enumerate(read_refs):
            assert read_ref.payload == payloads[i]
            assert read_ref.url == "https://example.com/data.json"

    def test_raw_record_id_format(self, tmp_stores) -> None:
        """raw_record_id 格式正确：{source}:{dataset}:{filename}:{line_no}"""
        store = RawStore(str(tmp_stores.data_dir))

        now = datetime(2026, 9, 11, 12, 0, 0)
        batches = [_make_batch(fetched_at=now, ingest_timestamp=now)]

        refs = store.append("my_source", "my_dataset", batches)

        ref = refs[0]
        # 格式：my_source:my_dataset:{filename}:{line_no}
        parts = ref.raw_record_id.split(":")
        assert parts[0] == "my_source"
        assert parts[1] == "my_dataset"
        # filename 和 line_no 用 : 分隔
        assert ref.line_no == int(parts[-1])
        assert ref.line_no == 1

    def test_iter_refs_locates_by_raw_record_id(self, tmp_stores) -> None:
        """raw_record_id 可用于定位到具体文件和行号"""
        store = RawStore(str(tmp_stores.data_dir))

        now = datetime(2026, 9, 11, 12, 0, 0)
        batches = [
            _make_batch(
                fetched_at=now, ingest_timestamp=now,
                payload=f"item_{i}".encode(),
            )
            for i in range(20)
        ]

        refs = store.append("src", "ds", batches)

        # 随机抽查几条，验证 raw_record_id 能定位到文件
        for ref in refs[::5]:  # 每 5 条抽查一条
            assert ref.file_path.endswith(".jsonl")
            lines = _read_jsonl_lines(ref.file_path)
            assert len(lines) >= ref.line_no
            # line_no 行的数据应包含正确的 payload
            line_data = lines[ref.line_no - 1]
            assert line_data["fetched_at"] == ref.fetched_at.isoformat()


# ── Append-only (no overwrite) tests ──────────────────────────────────


class TestAppendOnly:

    def test_append_monotonically_increases_line_count(self, tmp_stores) -> None:
        """多次 append 后，文件行数单调递增"""
        store = RawStore(str(tmp_stores.data_dir))

        now = datetime(2026, 9, 11, 12, 0, 0)
        file_path = None

        for i in range(5):
            payload = f"round_{i}_data".encode()
            batches = [_make_batch(
                fetched_at=now, ingest_timestamp=now, payload=payload,
            )]
            refs = store.append("src", "ds", batches)
            assert len(refs) == 1
            if file_path is None:
                file_path = refs[0].file_path
                assert file_path.endswith(".jsonl")

            current_lines = _count_lines(file_path)
            assert current_lines == i + 1

    def test_no_data_loss_after_multiple_appends(self, tmp_stores) -> None:
        """多次 append 后，iter_refs 能读回全部数据"""
        store = RawStore(str(tmp_stores.data_dir))

        now = datetime(2026, 9, 11, 12, 0, 0)
        all_payloads = []

        for i in range(10):
            payloads = [f"batch_{i}_item_{j}".encode() for j in range(5)]
            all_payloads.extend(payloads)
            batches = [_make_batch(
                fetched_at=now,
                ingest_timestamp=now,
                payload=p,
            ) for p in payloads]
            store.append("src", "ds", batches)

        read_refs = list(store.iter_refs("src", "ds"))
        assert len(read_refs) == 50  # 10 * 5

        for i, ref in enumerate(read_refs):
            expected = all_payloads[i]
            assert ref.payload == expected


# ── Partitioning tests ────────────────────────────────────────────────


class TestPartitioning:

    def test_partition_by_ingest_date(self, tmp_stores) -> None:
        """数据按 ingest_date 正确分区"""
        store = RawStore(str(tmp_stores.data_dir))

        batches = [
            _make_batch(
                fetched_at=datetime(2026, 9, 10, 23, 59, 59),
                ingest_timestamp=datetime(2026, 9, 10, 23, 59, 59),
                payload=b"sep10",
            ),
            _make_batch(
                fetched_at=datetime(2026, 9, 11, 0, 0, 1),
                ingest_timestamp=datetime(2026, 9, 11, 0, 0, 1),
                payload=b"sep11",
            ),
        ]

        refs = store.append("src", "ds", batches)
        assert len(refs) == 2

        # 两条记录应在不同分区
        partitions = set()
        for ref in refs:
            assert "/ingest_date=" in ref.file_path
            partitions.add(ref.file_path)

        assert len(partitions) == 2

    def test_cross_day_batch_correct_partitioning(self, tmp_stores) -> None:
        """跨日批次（同一批次内含不同日期）正确分区"""
        store = RawStore(str(tmp_stores.data_dir))

        # 同一批次，跨两天
        batches = [
            _make_batch(
                fetched_at=datetime(2026, 9, 11, 22, 0, 0),
                ingest_timestamp=datetime(2026, 9, 11, 22, 0, 0),
                payload=b"day11",
            ),
            _make_batch(
                fetched_at=datetime(2026, 9, 12, 1, 0, 0),
                ingest_timestamp=datetime(2026, 9, 12, 1, 0, 0),
                payload=b"day12",
            ),
            _make_batch(
                fetched_at=datetime(2026, 9, 13, 5, 0, 0),
                ingest_timestamp=datetime(2026, 9, 13, 5, 0, 0),
                payload=b"day13",
            ),
        ]

        refs = store.append("src", "ds", batches)
        assert len(refs) == 3

        # 三个日期应有三个分区
        date_dirs = set()
        for ref in refs:
            date_dirs.add(ref.file_path.split("ingest_date=")[1].split("/")[0])

        assert len(date_dirs) == 3
        assert "2026-09-11" in date_dirs
        assert "2026-09-12" in date_dirs
        assert "2026-09-13" in date_dirs

    def test_different_source_dataset_isolation(self, tmp_stores) -> None:
        """不同 source/dataset 数据隔离"""
        store = RawStore(str(tmp_stores.data_dir))

        now = datetime(2026, 9, 11, 12, 0, 0)
        batches_a = [_make_batch(fetched_at=now, ingest_timestamp=now, payload=b"source_a")]
        batches_b = [_make_batch(fetched_at=now, ingest_timestamp=now, payload=b"source_b")]

        store.append("source_a", "ds_a", batches_a)
        store.append("source_b", "ds_b", batches_b)

        refs_a = list(store.iter_refs("source_a", "ds_a"))
        refs_b = list(store.iter_refs("source_b", "ds_b"))

        assert len(refs_a) == 1
        assert len(refs_b) == 1
        assert refs_a[0].payload == b"source_a"
        assert refs_b[0].payload == b"source_b"


# ── Empty batch tests ─────────────────────────────────────────────────


class TestEmptyBatch:

    def test_empty_batch_creates_no_files(self, tmp_stores) -> None:
        """batches=[] 不创建文件"""
        store = RawStore(str(tmp_stores.data_dir))

        refs = store.append("src", "ds", [])
        assert refs == []

        raw_dir = tmp_stores.data_dir / "raw" / "src" / "ds"
        assert not raw_dir.exists()

    def test_empty_batch_no_error(self, tmp_stores) -> None:
        """空批次不抛异常"""
        store = RawStore(str(tmp_stores.data_dir))

        # 不应抛异常
        store.append("src", "ds", [])


# ── Large payload tests ───────────────────────────────────────────────


class TestLargePayload:

    def test_large_payload_base64_roundtrip(self, tmp_stores) -> None:
        """大 payload base64 编码/解码往返一致"""
        store = RawStore(str(tmp_stores.data_dir))

        now = datetime(2026, 9, 11, 12, 0, 0)
        # 1MB payload
        large_payload = os.urandom(1024 * 1024)
        batches = [_make_batch(fetched_at=now, ingest_timestamp=now, payload=large_payload)]

        refs = store.append("src", "ds", batches)
        assert len(refs) == 1

        read_refs = list(store.iter_refs("src", "ds"))
        assert len(read_refs) == 1

        assert read_refs[0].payload == large_payload

    def test_binary_payload_roundtrip(self, tmp_stores) -> None:
        """二进制 payload 编码/解码往返一致"""
        store = RawStore(str(tmp_stores.data_dir))

        now = datetime(2026, 9, 11, 12, 0, 0)
        binary_payload = bytes(range(256)) * 10  # 2560 bytes with all byte values
        batches = [_make_batch(fetched_at=now, ingest_timestamp=now, payload=binary_payload)]

        refs = store.append("src", "ds", batches)
        assert len(refs) == 1

        read_refs = list(store.iter_refs("src", "ds"))
        assert read_refs[0].payload == binary_payload

    def test_very_large_payload(self, tmp_stores) -> None:
        """超大 payload（10MB）正常处理"""
        store = RawStore(str(tmp_stores.data_dir))

        now = datetime(2026, 9, 11, 12, 0, 0)
        large_payload = os.urandom(10 * 1024 * 1024)
        batches = [_make_batch(fetched_at=now, ingest_timestamp=now, payload=large_payload)]

        refs = store.append("src", "ds", batches)
        assert len(refs) == 1

        read_refs = list(store.iter_refs("src", "ds"))
        assert read_refs[0].payload == large_payload


# ── Boundary: file size limits ────────────────────────────────────────


class TestBoundary:

    def test_max_lines_10000_no_truncation(self, tmp_stores) -> None:
        """单文件 10000 行不截断，全部写入同一文件"""
        store = RawStore(str(tmp_stores.data_dir))

        now = datetime(2026, 9, 11, 12, 0, 0)
        batches = [
            _make_batch(
                fetched_at=now, ingest_timestamp=now,
                payload=f"item_{i}".encode(),
            )
            for i in range(10000)
        ]

        refs = store.append("src", "ds", batches)
        assert len(refs) == 10000

        # 验证所有 ref 指向同一文件
        unique_files = set(ref.file_path for ref in refs)
        assert len(unique_files) == 1

        # 验证文件行数
        file_path = refs[0].file_path
        lines = _count_lines(file_path)
        assert lines == 10000

    def test_over_10000_lines_creates_new_file(self, tmp_stores) -> None:
        """超过 10000 行创建新文件"""
        store = RawStore(str(tmp_stores.data_dir))

        now = datetime(2026, 9, 11, 12, 0, 0)

        # 第一次写入 10000 条
        batches_1 = [
            _make_batch(
                fetched_at=now, ingest_timestamp=now,
                payload=f"batch1_item_{i}".encode(),
            )
            for i in range(10000)
        ]
        refs_1 = store.append("src", "ds", batches_1)
        assert len(refs_1) == 10000

        file_1 = refs_1[0].file_path

        # 第二次写入 101 条（总计 10101）
        batches_2 = [
            _make_batch(
                fetched_at=now, ingest_timestamp=now,
                payload=f"batch2_item_{i}".encode(),
            )
            for i in range(101)
        ]
        refs_2 = store.append("src", "ds", batches_2)
        assert len(refs_2) == 101

        # 新文件应不同于旧文件
        file_2 = refs_2[0].file_path
        assert file_1 != file_2

        # 验证总行数
        lines_1 = _count_lines(file_1)
        lines_2 = _count_lines(file_2)
        assert lines_1 == 10000
        assert lines_2 == 101

    def test_iter_refs_reads_across_multiple_files(self, tmp_stores) -> None:
        """iter_refs 能跨多个文件读取"""
        store = RawStore(str(tmp_stores.data_dir))

        now = datetime(2026, 9, 11, 12, 0, 0)
        all_payloads = []

        for batch_num in range(5):
            count = 2000
            payloads = [
                f"batch{batch_num}_item_{i}".encode()
                for i in range(count)
            ]
            all_payloads.extend(payloads)

            batches = [_make_batch(
                fetched_at=now,
                ingest_timestamp=now,
                payload=p,
            ) for p in payloads]
            store.append("src", "ds", batches)

        read_refs = list(store.iter_refs("src", "ds"))
        assert len(read_refs) == 10000

        for i, ref in enumerate(read_refs):
            expected = all_payloads[i]
            assert ref.payload == expected


# ── Time filter tests ─────────────────────────────────────────────────


class TestTimeFilter:

    def test_iter_refs_with_start_filter(self, tmp_stores) -> None:
        """iter_refs start 过滤：仅返回 fetched_at >= start 的记录"""
        store = RawStore(str(tmp_stores.data_dir))

        now = datetime(2026, 9, 11, 12, 0, 0)
        batches = [
            _make_batch(
                fetched_at=now.replace(minute=i),
                ingest_timestamp=now.replace(minute=i),
                payload=f"t{i}".encode(),
            )
            for i in range(30)
        ]

        store.append("src", "ds", batches)

        # 过滤 start = 12:15
        start = datetime(2026, 9, 11, 12, 15, 0)
        filtered = list(store.iter_refs("src", "ds", start=start))

        for ref in filtered:
            assert ref.fetched_at >= start

    def test_iter_refs_with_end_filter(self, tmp_stores) -> None:
        """iter_refs end 过滤：仅返回 fetched_at <= end 的记录"""
        store = RawStore(str(tmp_stores.data_dir))

        now = datetime(2026, 9, 11, 12, 0, 0)
        batches = [
            _make_batch(
                fetched_at=now.replace(minute=i),
                ingest_timestamp=now.replace(minute=i),
                payload=f"t{i}".encode(),
            )
            for i in range(30)
        ]

        store.append("src", "ds", batches)

        # 过滤 end = 12:15
        end = datetime(2026, 9, 11, 12, 15, 0)
        filtered = list(store.iter_refs("src", "ds", end=end))

        for ref in filtered:
            assert ref.fetched_at <= end

    def test_iter_refs_with_start_and_end_filter(self, tmp_stores) -> None:
        """iter_refs start+end 范围过滤"""
        store = RawStore(str(tmp_stores.data_dir))

        now = datetime(2026, 9, 11, 12, 0, 0)
        batches = [
            _make_batch(
                fetched_at=now.replace(minute=i),
                ingest_timestamp=now.replace(minute=i),
                payload=f"t{i}".encode(),
            )
            for i in range(30)
        ]

        store.append("src", "ds", batches)

        # 过滤 12:10 - 12:20
        start = datetime(2026, 9, 11, 12, 10, 0)
        end = datetime(2026, 9, 11, 12, 20, 0)
        filtered = list(store.iter_refs("src", "ds", start=start, end=end))

        for ref in filtered:
            assert start <= ref.fetched_at <= end
        assert len(filtered) == 11  # 12:10 到 12:20 共 11 条

    def test_iter_refs_no_match_returns_empty(self, tmp_stores) -> None:
        """过滤条件无匹配时返回空列表"""
        store = RawStore(str(tmp_stores.data_dir))

        now = datetime(2026, 9, 11, 12, 0, 0)
        batches = [_make_batch(fetched_at=now, ingest_timestamp=now)]
        store.append("src", "ds", batches)

        # 未来时间无匹配
        future = datetime(2099, 1, 1)
        filtered = list(store.iter_refs("src", "ds", start=future))
        assert filtered == []


# ── Failure tests ─────────────────────────────────────────────────────


class TestFailure:

    def test_write_to_readonly_dir_raises_storage_error(self, tmp_stores) -> None:
        """data_dir 权限不足 → StorageError"""
        # 创建一个只读目录，内部创建好 raw 子目录
        readonly_dir = tmp_stores.data_dir / "readonly"
        raw_dir = readonly_dir / "raw"
        readonly_dir.mkdir(parents=True)
        raw_dir.mkdir(parents=True)

        # 设置为只读（禁止写入）
        os.chmod(str(raw_dir), stat.S_IRUSR | stat.S_IXUSR)

        try:
            store = RawStore(str(raw_dir))
            now = datetime(2026, 9, 11, 12, 0, 0)
            batches = [_make_batch(fetched_at=now, ingest_timestamp=now)]
            with pytest.raises(StorageError):
                store.append("src", "ds", batches)
        finally:
            # 恢复权限以便清理
            os.chmod(str(raw_dir), stat.S_IRWXU)

    def test_jsonl_file_consistency_after_append(self, tmp_stores) -> None:
        """写入后 JSONL 文件格式正确"""
        store = RawStore(str(tmp_stores.data_dir))

        now = datetime(2026, 9, 11, 12, 0, 0)
        batches = [_make_batch(fetched_at=now, ingest_timestamp=now, payload=b"test_payload")]

        refs = store.append("src", "ds", batches)
        assert len(refs) == 1

        # 读取文件内容验证格式
        lines = _read_jsonl_lines(refs[0].file_path)
        assert len(lines) == 1

        line = lines[0]
        assert "ingest_batch_id" in line
        assert "fetched_at" in line
        assert "url" in line
        assert "payload" in line
        assert line["ingest_batch_id"] == "batch_001"
        assert line["fetched_at"] == now.isoformat()
        assert line["url"] == "https://example.com/data.json"


# ── Integration: append + iter_refs flow ─────────────────────────────


class TestIntegration:

    def test_full_pipeline_write_read_verify(self, tmp_stores) -> None:
        """完整流程：写入→读回→验证"""
        store = RawStore(str(tmp_stores.data_dir))

        now = datetime(2026, 9, 11, 12, 0, 0)
        test_cases = [
            {"fetched_at": now, "payload": b"test_1"},
            {"fetched_at": now.replace(second=1), "payload": b"test_2"},
            {"fetched_at": now.replace(second=2), "payload": b"test_3"},
        ]

        batches = [
            _make_batch(
                fetched_at=tc["fetched_at"],
                ingest_timestamp=tc["fetched_at"],
                payload=tc["payload"],
            )
            for tc in test_cases
        ]

        refs = store.append("src", "ds", batches)
        assert len(refs) == 3

        # 验证返回的 refs
        assert all(isinstance(ref, RawRef) for ref in refs)
        assert all(ref.raw_record_id for ref in refs)
        assert all(ref.file_path for ref in refs)
        assert all(ref.line_no > 0 for ref in refs)

        # 读回并验证
        read_refs = list(store.iter_refs("src", "ds"))
        assert len(read_refs) == 3

        for i, ref in enumerate(read_refs):
            assert ref.payload == test_cases[i]["payload"]
            # 所有记录使用同一个 batch_id（每次 append 调用生成一个）
            assert ref.ingest_batch_id is not None
            assert ref.fetched_at == test_cases[i]["fetched_at"]
