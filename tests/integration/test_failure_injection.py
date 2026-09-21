"""故障注入测试（审计 M-10，D09 §35/§36）。

- crash-restart：子进程写入中途 os._exit(1) 模拟进程崩溃 → 父进程
  重开 MetaStore，孤儿锁阻塞 → startup_repair 恢复 → 锁可重新获取，
  崩溃前 checkpoint 数据不丢
- disk full：parquet 写入抛 OSError(ENOSPC) → upsert 熔断，旧分区
  数据完整、无 temp 目录残留（原子性）
- checkpoint 损坏：非法 cursor 值 → 消费方显式 ValueError（不静默重置）
"""

from __future__ import annotations

import subprocess
import sys
from datetime import UTC, datetime

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
