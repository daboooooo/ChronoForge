"""RawStore — Raw 层 JSONL 只追加存储（D03 §2）。

职责：
- 按 ingest_date 分区写入 raw JSONL 文件
- 支持按时间范围遍历 raw 记录
- payload 以 base64 编码存储

持久性契约（READY-002 / SR-09，durable boundary 不变量）：
- 常驻文件句柄 + 按批 fsync：append() 期间写入经句柄缓冲，在
  **append() 返回前** 对全部本批脏句柄 flush + fsync（per-call 边界）
- fsync_every_n > 0 时，单个 append() 调用内每 N 行追加一次中间
  flush + fsync（缩小 crash 窗口，可配；默认 0 = 仅调用边界）
- flush_batch() 为显式持久化点（幂等）：flush + fsync 全部含未落盘
  写入的句柄
- cursor 推进（save_checkpoint）只允许越过已 fsync 的数据边界：
  append() 返回 ⇒ 本批全部行已 durable，故 runner 现有调用序
  （RawAppendStage.append → … → RunLogStage.save_checkpoint）天然满足
  该不变量；FAILED/CANCELLED 异常路径不写 cursor，无需 run 结束补偿 flush
- crash 窗口语义：append() 执行期间未 fsync 的行允许丢失，但 cursor
  不得越过 durable boundary；raw 允许重复行，重放部分由上游 chunk 重试
  覆盖，幂等由 canonical upsert 按 natural key 收敛（D03 §3）
"""

from __future__ import annotations

import base64
import json
import os
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import IO, TypedDict

from chronoforge.exceptions import StorageError


class _BatchItem(TypedDict):
    """单条 batch 数据的类型定义。"""

    url: str
    payload: bytes
    fetched_at: datetime
    ingest_batch_id: str  # optional
    ingest_timestamp: datetime  # optional


@dataclass
class RawRef:
    """单条 raw 记录的引用元数据。"""

    raw_record_id: str
    ingest_batch_id: str
    fetched_at: datetime
    url: str
    payload: bytes
    file_path: str
    line_no: int


@dataclass
class _FileHandle:
    """内部文件句柄，跟踪写入状态。

    file_obj 为常驻打开的追加句柄（惰性创建）；unsynced_lines 计量距
    上次 fsync 的行数（crash 窗口）。
    """

    file_path: Path
    line_count: int
    seq: int
    file_obj: IO[str] | None = None
    unsynced_lines: int = 0


class RawStore:
    """Raw JSONL 只追加存储。

    路径布局：
        {data_dir}/raw/{source}/{dataset}/ingest_date={YYYY-MM-DD}/{HHmmss}-{seq}.jsonl

    单文件最大行数：10000，超限自动创建新文件（同时切换常驻句柄）。

    Args:
        data_dir: 数据存储根目录。
        fsync_every_n: 批内中间 fsync 间隔（行数）。0（默认）= 仅在
            append() 调用边界 flush + fsync；>0 时调用内每 N 行额外
            fsync 一次，缩小 crash 窗口（backfill 大批量时可调）。
    """

    _MAX_LINES = 10000

    def __init__(self, data_dir: str, fsync_every_n: int = 0) -> None:
        """初始化 RawStore，确保 data_dir 存在。

        Args:
            data_dir: 数据存储根目录。
            fsync_every_n: 批内中间 fsync 间隔（行数），0 = 仅调用边界。

        Raises:
            StorageError: data_dir 创建失败。
        """
        try:
            Path(data_dir).mkdir(parents=True, exist_ok=True)
        except OSError as e:
            raise StorageError(f"Failed to create data directory {data_dir}") from e

        self._data_dir = Path(data_dir)
        self._raw_dir = self._data_dir / "raw"
        self._fsync_every_n = max(0, fsync_every_n)
        # 内存中缓存当前打开的文件句柄 {source:{dataset}:{ingest_date}:key -> _FileHandle}
        # key 由 min timestamp 的 HHmmss 决定
        self._handles: dict[str, _FileHandle] = {}

    def _partition_key(self, source: str, dataset: str, ingest_date: str) -> str:
        """生成分区目录键。"""
        return f"{source}/{dataset}/ingest_date={ingest_date}"

    def _get_handle(
        self, source: str, dataset: str, ingest_date_str: str, timestamp: datetime
    ) -> _FileHandle:
        """获取或创建文件句柄。

        如果已有相同分区且文件未满，返回现有句柄；否则创建新句柄
        （10000 行滚动切文件时同步关闭并切换常驻句柄）。
        """
        key = f"{source}/{dataset}/ingest_date={ingest_date_str}"

        if key in self._handles:
            handle = self._handles[key]
            # 检查是否超出单文件行数限制
            if handle.line_count < self._MAX_LINES:
                return handle
            # 滚动新建文件：关闭旧常驻句柄
            if handle.file_obj is not None:
                try:
                    handle.file_obj.close()
                except OSError as e:
                    raise StorageError(
                        f"Failed to close {handle.file_path}"
                    ) from e
                handle.file_obj = None

        # 创建新句柄
        partition_dir = self._raw_dir / self._partition_key(source, dataset, ingest_date_str)
        try:
            partition_dir.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            raise StorageError(f"Failed to create partition directory {partition_dir}") from e

        # 文件名时间戳 = min(ingest_timestamp) 的 HHmmss
        time_str = timestamp.strftime("%H%M%S")
        seq = 1

        # 查找该目录下的最大 seq
        files = sorted(partition_dir.glob(f"{time_str}-*.jsonl"))
        if files:
            # 从已有文件名中提取 seq，并递增
            for f in reversed(files):
                fname = f.name
                # 格式: {HHmmss}-{seq}.jsonl
                seq_part = fname[len(time_str) + 1:-6]  # 去掉 {HHmmss}- 和 .jsonl
                try:
                    seq = max(seq, int(seq_part) + 1)
                except ValueError:
                    continue

        file_path = partition_dir / f"{time_str}-{seq:04d}.jsonl"
        self._handles[key] = _FileHandle(
            file_path=file_path,
            line_count=0,
            seq=seq,
        )
        return self._handles[key]

    def _open_handle(self, handle: _FileHandle) -> None:
        """惰性打开常驻追加句柄。"""
        if handle.file_obj is not None:
            return
        try:
            handle.file_obj = open(handle.file_path, "a", encoding="utf-8")
        except OSError as e:
            raise StorageError(f"Failed to open {handle.file_path}") from e

    def _sync_handle(self, handle: _FileHandle) -> None:
        """flush + fsync 单个句柄（有未落盘写入时）；推进 durable boundary。"""
        if handle.file_obj is None or handle.unsynced_lines == 0:
            return
        try:
            handle.file_obj.flush()
            os.fsync(handle.file_obj.fileno())
        except OSError as e:
            raise StorageError(f"Failed to fsync {handle.file_path}") from e
        handle.unsynced_lines = 0

    def append(
        self,
        source: str,
        dataset: str,
        batches: list[_BatchItem],
    ) -> list[RawRef]:
        """只追加写入 raw JSONL 批次。

        写入经常驻句柄缓冲；**返回前对本批全部脏句柄 flush + fsync**
        （per-call 边界，durable boundary 推进到本批末尾），
        fsync_every_n > 0 时批内每 N 行追加中间 fsync。

        Args:
            source: 数据源标识。
            dataset: 数据集标识。
            batches: [{"url": str, "payload": bytes, "fetched_at": datetime,
                        "ingest_batch_id": str}, ...]

        Returns:
            RawRef 列表，每条对应一个写入的 raw 记录。

        Raises:
            StorageError: 磁盘写满、权限不足等。
        """
        if not batches:
            return []

        refs: list[RawRef] = []
        # 使用新的 batch ID（每个 append 调用独立）
        ingest_batch_id = batches[0].get("ingest_batch_id", uuid.uuid4().hex)
        dirty: list[_FileHandle] = []

        for batch_item in batches:
            url = batch_item["url"]
            payload = batch_item["payload"]
            fetched_at = batch_item["fetched_at"]

            # 按 ingest_timestamp 确定分区日期（优先使用 ingest_timestamp，否则用 fetched_at）
            ingest_ts = batch_item.get("ingest_timestamp", fetched_at)
            ingest_date_str = ingest_ts.date().isoformat()  # YYYY-MM-DD

            handle = self._get_handle(source, dataset, ingest_date_str, ingest_ts)
            self._open_handle(handle)
            assert handle.file_obj is not None

            # 写入一行
            line_data = {
                "ingest_batch_id": ingest_batch_id,
                "fetched_at": fetched_at.isoformat(),
                "url": url,
                "payload": base64.b64encode(payload).decode("ascii"),
            }
            line_json = json.dumps(line_data, ensure_ascii=False) + "\n"

            try:
                handle.file_obj.write(line_json)
            except OSError as e:
                raise StorageError(f"Failed to write to {handle.file_path}") from e

            handle.line_count += 1
            handle.unsynced_lines += 1
            if handle not in dirty:
                dirty.append(handle)

            # 批内中间 fsync（fsync_every_n > 0 时缩小 crash 窗口）
            if (
                self._fsync_every_n
                and handle.unsynced_lines >= self._fsync_every_n
            ):
                self._sync_handle(handle)

            line_no = handle.line_count

            raw_record_id = f"{source}:{dataset}:{handle.file_path.name}:{line_no}"

            refs.append(RawRef(
                raw_record_id=raw_record_id,
                ingest_batch_id=ingest_batch_id,
                fetched_at=fetched_at,
                url=url,
                payload=payload,
                file_path=str(handle.file_path),
                line_no=line_no,
            ))

        # 调用边界：全部脏句柄 flush + fsync（append 返回 ⇒ 本批已 durable）
        for handle in dirty:
            self._sync_handle(handle)

        return refs

    def flush_batch(self) -> None:
        """显式持久化点（幂等）：flush + fsync 全部含未落盘写入的常驻句柄。

        durable boundary 契约见模块 docstring：append() 返回即本批
        durable，此方法供调用方在依赖落盘数据前显式调用（如 chunk 级
        写入后、checkpoint 写入前的强制持久化点）。
        """
        for handle in self._handles.values():
            self._sync_handle(handle)

    def close(self) -> None:
        """关闭全部常驻句柄（数据已在 append() 返回前 fsync，无需补偿）。"""
        for handle in self._handles.values():
            if handle.file_obj is not None:
                try:
                    handle.file_obj.close()
                except OSError as e:
                    raise StorageError(
                        f"Failed to close {handle.file_path}"
                    ) from e
                handle.file_obj = None

    def iter_refs(
        self,
        source: str,
        dataset: str,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> Iterator[RawRef]:
        """遍历 raw JSONL 记录。

        Args:
            source: 数据源标识。
            dataset: 数据集标识。
            start: 可选起始时间过滤（按 fetched_at）。
            end: 可选结束时间过滤（按 fetched_at）。

        Yields:
            RawRef 迭代器（逐行读取，不全部加载到内存）。
        """
        raw_base = self._raw_dir / source / dataset
        if not raw_base.exists():
            return

        for ingest_date_dir in sorted(raw_base.glob("ingest_date=*/")):
            for jsonl_file in sorted(ingest_date_dir.glob("*.jsonl")):
                line_no = 0
                try:
                    with open(jsonl_file, encoding="utf-8") as f:
                        for line in f:
                            line = line.strip()
                            if not line:
                                continue
                            line_no += 1

                            try:
                                data = json.loads(line)
                            except json.JSONDecodeError:
                                continue

                            fetched_at = datetime.fromisoformat(data["fetched_at"])

                            # 时间过滤
                            if start is not None and fetched_at < start:
                                continue
                            if end is not None and fetched_at > end:
                                continue

                            raw_record_id = f"{source}:{dataset}:{jsonl_file.name}:{line_no}"

                            yield RawRef(
                                raw_record_id=raw_record_id,
                                ingest_batch_id=data["ingest_batch_id"],
                                fetched_at=fetched_at,
                                url=data["url"],
                                payload=base64.b64decode(data["payload"]),
                                file_path=str(jsonl_file),
                                line_no=line_no,
                            )
                except OSError:
                    continue

    def cleanup_old_partitions(self, retention_days: int) -> int:
        """清理过期的 raw 分区目录。

        扫描 ``{data_dir}/raw/`` 下所有 ``ingest_date=`` 分区，
        删除 ``ingest_date`` 早于 ``now - retention_days`` 的分区目录。

        Args:
            retention_days: 保留天数。0 表示删除所有分区。

        Returns:
            被删除的分区目录数量。
        """
        import shutil as _shutil

        now_date = datetime.now().replace(tzinfo=None).date()
        if retention_days == 0:
            cutoff = now_date
        else:
            cutoff = now_date - __import__("datetime").timedelta(days=retention_days)

        deleted = 0
        raw_base = self._raw_dir
        if not raw_base.exists():
            return 0

        # 遍历 {source}/{dataset}/ingest_date={YYYY-MM-DD}
        for source_dir in raw_base.iterdir():
            if not source_dir.is_dir():
                continue
            for dataset_dir in source_dir.iterdir():
                if not dataset_dir.is_dir():
                    continue
                for date_dir in dataset_dir.glob("ingest_date=*"):
                    if not date_dir.is_dir():
                        continue
                    date_str = date_dir.name.split("=", 1)[1]
                    try:
                        ingest_date = __import__("datetime").date.fromisoformat(date_str)
                    except (ValueError, IndexError):
                        continue
                    if ingest_date <= cutoff:
                        _shutil.rmtree(date_dir, ignore_errors=True)
                        deleted += 1

        return deleted

    def list_partitions(
        self,
        source: str | None = None,
        dataset: str | None = None,
    ) -> list[str]:
        """列出 raw 层分区路径。

        Args:
            source: 可选数据源过滤。
            dataset: 可选数据集过滤。

        Returns:
            按路径排序的分区目录列表，元素格式为
            ``{source}/{dataset}/ingest_date={YYYY-MM-DD}``。
        """
        raw_base = self._raw_dir
        if not raw_base.exists():
            return []

        result: list[str] = []

        def _collect(dir_path: Path, prefix: str) -> None:
            if dir_path.name.startswith("ingest_date="):
                parts = [p for p in (prefix + "/" + dir_path.name).split("/") if p]
                result.append("/".join(parts))
                return
            if not dir_path.is_dir():
                return
            new_prefix = prefix + "/" + dir_path.name if prefix else dir_path.name
            for child in sorted(dir_path.iterdir()):
                _collect(child, new_prefix)

        if source:
            src_path = raw_base / source
            if src_path.is_dir():
                _collect(src_path, source)
        else:
            for entry in sorted(raw_base.iterdir()):
                if entry.is_dir():
                    _collect(entry, entry.name)

        # 二次过滤 dataset
        if dataset:
            result = [p for p in result if f"/{dataset}/" in p]

        return sorted(result)

    def get_raw_stats(self, source: str, dataset: str) -> dict[str, object]:
        """统计 raw 层数据量。

        Args:
            source: 数据源标识。
            dataset: 数据集标识。

        Returns:
            ``{total_records, total_size_bytes, partition_count,
              date_range: (start_iso, end_iso)}``。
            date_range 在无可读分区时返回 ``(None, None)``。
        """
        raw_base = self._raw_dir / source / dataset
        if not raw_base.exists():
            return {
                "total_records": 0,
                "total_size_bytes": 0,
                "partition_count": 0,
                "date_range": (None, None),
            }

        total_records = 0
        total_size = 0
        partition_count = 0
        dates: list[str] = []

        for date_dir in sorted(raw_base.glob("ingest_date=*")):
            if not date_dir.is_dir():
                continue
            partition_count += 1
            for jsonl_file in date_dir.glob("*.jsonl"):
                if not jsonl_file.is_file():
                    continue
                total_size += jsonl_file.stat().st_size
                try:
                    with open(jsonl_file, encoding="utf-8") as f:
                        for line in f:
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                data = json.loads(line)
                                total_records += 1
                                fa = data.get("fetched_at")
                                if fa:
                                    dates.append(fa)
                            except json.JSONDecodeError:
                                continue
                except OSError:
                    continue

        date_range: tuple[str | None, str | None]
        if dates:
            date_range = (min(dates), max(dates))
        else:
            date_range = (None, None)

        return {
            "total_records": total_records,
            "total_size_bytes": total_size,
            "partition_count": partition_count,
            "date_range": date_range,
        }
