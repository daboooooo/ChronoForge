"""故障注入测试（审计 M-10，D09 §35/§36）。

- crash-restart：子进程写入中途 os._exit(1) 模拟进程崩溃 → 父进程
  重开 MetaStore，孤儿锁阻塞 → startup_repair 恢复 → 锁可重新获取，
  崩溃前 checkpoint 数据不丢
- disk full：parquet 写入抛 OSError(ENOSPC) → upsert 熔断，旧分区
  数据完整、无 temp 目录残留（原子性）
- checkpoint 损坏：非法 cursor 值 → 消费方显式 ValueError（不静默重置）
"""

from __future__ import annotations

import base64
import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

from chronoforge.exceptions import StorageError
from chronoforge.models.enums import CanonicalType
from chronoforge.pipeline.cursor import advance_cursor
from chronoforge.storage.canonical import CanonicalStoreImpl
from chronoforge.storage.consistency import startup_repair
from chronoforge.storage.meta import MetaStore
from chronoforge.storage.raw import RawStore

# 测试用的极小 Chunk/ChunkResult 构造不依赖 connectors：
# advance_cursor 只读 chunk.end / chunk_result.success


class TestCrashRestart:
    """审计 M-10：真实进程崩溃 → 重启 → 恢复。"""

    def test_crash_during_run_recovered_by_startup_repair(
        self, tmp_stores
    ) -> None:
        """子进程持锁中途崩溃 → 重启后孤儿锁阻塞 → startup_repair 恢复。"""
        meta_dir = str(tmp_stores.meta_dir)
        data_dir = str(tmp_stores.data_dir)
        crash_cursor = "2026-06-01T00:00:00"

        # 子进程：持锁 + 写 checkpoint 后硬崩溃（无 finish_run/无清理）
        script = f"""
import os
from chronoforge.storage.meta import MetaStore

store = MetaStore({meta_dir!r})
store.migrate()
conn = store.connection
conn.execute(
    "INSERT OR IGNORE INTO source_registry "
    "(source_id, display_name, access_type, base_url, rate_limit_json, license) "
    "VALUES ('test_source', 'T', 'PUBLIC', 'https://t.com', '{{}}', 'MIT')"
)
conn.execute(
    "INSERT OR IGNORE INTO dataset_registry "
    "(dataset_id, source_id, canonical_type, entity_id, params_json, "
    "continuity_model, status, created_at, revision_supported) "
    "VALUES ('ds_crash', 'test_source', 'OHLCV', 'BTC', '{{}}', "
    "'ALWAYS_OPEN', 'UNKNOWN', '2026-01-01', 0)"
)
conn.commit()
store.try_lock_dataset("ds_crash", source_id="test_source")
store.save_checkpoint("test_source", "ds_crash", {crash_cursor!r})
os._exit(1)  # 模拟崩溃：SIGKILL 级，无任何清理
"""
        result = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            timeout=30,
        )
        assert result.returncode == 1  # 确实崩溃退出

        # 重启后：孤儿锁阻塞新 run
        with MetaStore(meta_dir) as store:
            store.migrate()
            with pytest.raises(StorageError, match="dataset locked"):
                store.try_lock_dataset("ds_crash")

            # startup_repair（timeout=0 → 崩溃 run 立即视为孤儿）→ 恢复
            raw_store = RawStore(data_dir)
            canonical_store = CanonicalStoreImpl(data_dir)
            startup_repair(
                store, raw_store, canonical_store,
                data_dir, stale_run_timeout_seconds=0.0,
            )

            # 锁已可重新获取
            run_row = store.try_lock_dataset("ds_crash", source_id="test_source")
            assert run_row is not None
            store.finish_run(run_row.run_id, "SUCCESS")

            # 崩溃前写入的 checkpoint 数据不丢
            assert (
                store.get_checkpoint("test_source", "ds_crash")
                == crash_cursor
            )


class TestDiskFull:
    """审计 M-10：disk full 注入（ENOSPC）。"""

    def test_upsert_enospc_preserves_old_data_no_tmp_left(
        self, tmp_stores, monkeypatch
    ) -> None:
        """写盘 ENOSPC → 熔断抛错，旧分区数据完整、无 .tmp 残留。"""
        import pyarrow.parquet as pq

        store = CanonicalStoreImpl(str(tmp_stores.data_dir))
        base_time = datetime(2026, 6, 1, 0, 0, 0)
        good = {
            "schema_version": "1.0",
            "source": "test_source",
            "source_id": "btcusdt",
            "source_timestamp": base_time,
            "ingest_timestamp": datetime.now(UTC).replace(tzinfo=None),
            "raw_record_id": "test:ohlcv:good",
            "market_id": "BINANCE:BTCUSDT:SPOT",
            "event_time": base_time,
            "interval": "1m",
            "open": 50000.0,
            "high": 51000.0,
            "low": 49000.0,
            "close": 50500.0,
            "volume": 100.0,
        }
        store.upsert([good], CanonicalType.OHLCV, "BTC")

        def enospc(*args: object, **kwargs: object) -> None:
            raise OSError(28, "No space left on device")

        monkeypatch.setattr(pq, "write_table", enospc)
        full = {**good, "raw_record_id": "test:ohlcv:full", "close": 60000.0}
        with pytest.raises(OSError, match="No space left"):
            store.upsert([full], CanonicalType.OHLCV, "BTC")

        # 旧数据完整（原子性：部分写不可见）
        p = (
            tmp_stores.data_dir / "canonical" / "OHLCV" / "entity=BTC"
            / "year=2026" / "month=06"
        )
        rows = pq.read_table(str(next(p.glob("*.parquet")))).to_pylist()
        assert len(rows) == 1
        assert rows[0]["close"] == 50500.0
        # 无 temp 目录残留
        assert not list(
            (tmp_stores.data_dir / "canonical" / "OHLCV").glob(".tmp-*")
        )


class TestCorruptedCheckpoint:
    """审计 M-10：checkpoint 损坏注入。"""

    def test_corrupted_cursor_rejected_not_silently_reset(
        self, tmp_stores
    ) -> None:
        """损坏 cursor 原样读出，消费方显式 ValueError（不静默重置/跳过）。"""
        meta_dir = str(tmp_stores.meta_dir)
        with MetaStore(meta_dir) as store:
            store.migrate()
            store.save_checkpoint("test_source", "ds_bad", "not-a-cursor")
            # 原样返回（不吞掉损坏值）
            assert (
                store.get_checkpoint("test_source", "ds_bad")
                == "not-a-cursor"
            )

        from datetime import datetime as dt

        from chronoforge.connectors.base import FetchRequest
        from chronoforge.pipeline.windows import Chunk, ChunkResult

        chunk = Chunk(
            chunk_id="c1",
            dataset_id="ds_bad",
            start=dt(2026, 6, 1, 1, 0),
            end=dt(2026, 6, 1, 2, 0),
            request=FetchRequest(
                dataset_id="ds_bad",
                params={},
                start=dt(2026, 6, 1, 1, 0),
                end=dt(2026, 6, 1, 2, 0),
                cursor=None,
            ),
        )
        # 损坏 cursor 进入推进逻辑 → 显式 ValueError（熔断，不静默重置）
        with pytest.raises(ValueError, match="invalid cursor"):
            advance_cursor(
                "not-a-cursor", ChunkResult(chunk=chunk, success=True)
            )


class TestRawFsyncCrash:
    """READY-002：RawStore 批量 fsync 的 crash 窗口语义（SR-09）。

    子进程写入中途 os._exit(1) 模拟进程崩溃；durable boundary 不变量：
    已 fsync 边界内数据完整可读，fsync 前窗口内允许丢失，但行数与
    boundary 一致、无 torn line。
    """

    @staticmethod
    def _crash_child_script() -> str:
        """子进程脚本：fsync_every_n=10 写 25 行，在第 1 个 fsync 点
        （fsync 系统调用生效前）硬崩溃。data_dir 经 argv[1] 传入。"""
        return """
import os
import sys
from datetime import datetime

import chronoforge.storage.raw as raw_mod


def crash_before_fsync(fd):
    # _sync_handle 已 flush（本批行进入 page cache），fsync 尚未生效
    os._exit(1)


os.fsync = crash_before_fsync
store = raw_mod.RawStore(sys.argv[1], fsync_every_n=10)
items = []
for i in range(25):
    items.append({
        "url": "https://example.com/data.json",
        "payload": ("row_%d" % i).encode(),
        "fetched_at": datetime(2026, 9, 21, 12, 0, 0),
        "ingest_batch_id": "b1",
        "ingest_timestamp": datetime(2026, 9, 21, 12, 0, 0),
    })
store.append("src", "ds", items)
os._exit(0)  # 不可达：append 内首个 fsync 点即崩溃
"""

    def _read_raw_lines(self, data_dir: Path) -> tuple[Path, list[str]]:
        """读取子进程落盘的唯一 jsonl 文件，返回 (路径, 非空行列表)。"""
        files = sorted((data_dir / "raw" / "src" / "ds").glob("ingest_date=*/*.jsonl"))
        assert len(files) == 1
        text = files[0].read_text(encoding="utf-8")
        return files[0], [ln for ln in text.splitlines() if ln]

    def test_crash_before_fsync_boundary_consistent(self, tmp_stores) -> None:
        """GWT-2：fsync 前崩溃 → 可读行数 = durable boundary（10 行），
        全部完整可解析、无 torn line；丢失行（11~25）均在 boundary 之外。"""
        result = subprocess.run(
            [sys.executable, "-c", self._crash_child_script(), str(tmp_stores.data_dir)],
            capture_output=True,
            timeout=30,
        )
        assert result.returncode == 1  # 确实在 fsync 点崩溃

        file_path, lines = self._read_raw_lines(tmp_stores.data_dir)
        # durable boundary = 第 1 个 fsync 点 = 10 行；page cache 中恰好
        # 保留 boundary 内的行（flush 已执行），窗口内行未越界落盘
        assert len(lines) == 10
        assert file_path.read_bytes().endswith(b"\n")  # 无 torn line
        for ln in lines:
            record = json.loads(ln)  # 全部完整可解析
            assert record["ingest_batch_id"] == "b1"

    def test_crash_after_flush_batch_data_complete(self, tmp_stores) -> None:
        """GWT-1：chunk 内 append 完成 + flush_batch 后 crash → 该批数据
        完整可读，cursor 可安全推进（checkpoint 只会越过已 fsync 边界）。"""
        script = """
import os
import sys
from datetime import datetime

from chronoforge.storage.raw import RawStore

store = RawStore(sys.argv[1], fsync_every_n=10)
items = []
for i in range(25):
    items.append({
        "url": "https://example.com/data.json",
        "payload": ("row_%d" % i).encode(),
        "fetched_at": datetime(2026, 9, 21, 12, 0, 0),
        "ingest_batch_id": "b1",
        "ingest_timestamp": datetime(2026, 9, 21, 12, 0, 0),
    })
store.append("src", "ds", items)
store.flush_batch()
os._exit(1)  # flush + fsync 完成后崩溃
"""
        result = subprocess.run(
            [sys.executable, "-c", script, str(tmp_stores.data_dir)],
            capture_output=True,
            timeout=30,
        )
        assert result.returncode == 1

        _file_path, lines = self._read_raw_lines(tmp_stores.data_dir)
        assert len(lines) == 25  # 全批 durable
        for i, ln in enumerate(lines):
            record = json.loads(ln)
            assert record["payload"] == base64.b64encode(
                f"row_{i}".encode()
            ).decode("ascii")

        # cursor 可安全推进：新 RawStore 实例（模拟重启）读回 boundary 内全部行
        fresh = RawStore(str(tmp_stores.data_dir))
        assert len(list(fresh.iter_refs("src", "ds"))) == 25

    def test_crash_then_replay_duplicates_converge(self, tmp_stores) -> None:
        """GWT-2（重放收敛）：fsync 前崩溃后重放同批数据 → 重放行落盘
        （新文件），raw 允许重复行，幂等由 canonical upsert 收敛（D03 §3）。"""
        result = subprocess.run(
            [sys.executable, "-c", self._crash_child_script(), str(tmp_stores.data_dir)],
            capture_output=True,
            timeout=30,
        )
        assert result.returncode == 1

        # 上游 chunk 重试重放同批 25 行（新 RawStore 实例 = 新句柄/新文件）
        store = RawStore(str(tmp_stores.data_dir))
        items = []
        for i in range(25):
            items.append(
                {
                    "url": "https://example.com/data.json",
                    "payload": f"row_{i}".encode(),
                    "fetched_at": datetime(2026, 9, 21, 12, 0, 0),
                    "ingest_batch_id": "b1",
                    "ingest_timestamp": datetime(2026, 9, 21, 12, 0, 0),
                }
            )
        store.append("src", "ds", items)

        # 重放批完整 durable（25 行）+ 崩溃批 boundary 内 10 行 = 35 行可读
        read_refs = list(store.iter_refs("src", "ds"))
        assert len(read_refs) == 35
        payloads = [r.payload for r in read_refs]
        assert payloads.count(b"row_0") == 2  # 重复行允许，幂等由 upsert 收敛
