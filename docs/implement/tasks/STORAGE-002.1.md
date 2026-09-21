# STORAGE-002.1 — RawStore append + iter_refs 核心

## 派发信息

- 任务单：D10 §2 STORAGE-002 拆分（子任务 1/2）
- 设计依据：D03 §2 Raw 层 JSONL 只追加存储
- Agent：待定
- 派发时间：2026-09-11
- 状态：DONE
- 完成时间：2026-09-12
- 完成依据：27 passed；RawStore append + iter_refs 核心验收通过
- 依赖：MODEL-001（BaseRecord）

## file_ownership

- `src/chronoforge/storage/raw.py`（新建）
- `tests/integration/test_raw_store.py`（新建）

## 交付物契约摘要

### RawStore 类

```python
class RawStore:
    def __init__(self, data_dir: str):
        """初始化，确保 data_dir 存在。"""
        ...

    def append(self, source: str, dataset: str, batches: list[dict]) -> list[RawRef]:
        """只追加写入 raw JSONL 批次。

        参数：
          source: 数据源标识
          dataset: 数据集标识
          batches: [{"url": str, "payload": bytes, "fetched_at": datetime}, ...]

        返回：
          RawRef 列表，每条对应一个写入的 raw 记录
        """
        ...

    def iter_refs(self, source: str, dataset: str, start: datetime = None, end: datetime = None) -> Iterator[RawRef]:
        """遍历 raw JSONL 记录。

        参数：
          start/end: 可选时间过滤（按 fetched_at）

        返回：
          RawRef 迭代器（逐行读取，不全部加载到内存）
        """
        ...
```

### RawRef 数据类

```python
@dataclass
class RawRef:
    raw_record_id: str  # "{source}:{dataset}:{jsonl_file}:{line_no}"
    ingest_batch_id: str
    fetched_at: datetime
    url: str
    payload: bytes
    file_path: str  # 所在 jsonl 文件路径
    line_no: int
```

### 路径布局

- 分区目录：`{data_dir}/raw/{source}/{dataset}/ingest_date={YYYY-MM-DD}/`
- 文件名：`{HHmmss}-{seq}.jsonl`（原子追加，不覆盖）
- 单文件最大行数：10000（超限创建新文件）

### 写入协议

1. 按 ingest_date 分区（datetime.date(ingest_timestamp)）
2. 文件名时间戳 = min(ingest_timestamp) 的 HHmmss
3. seq 计数器：同一文件递增
4. 每行 JSON：`{"ingest_batch_id", "fetched_at", "url", "payload"}`（payload 为 base64 编码）
5. 写入后 fsync（os.fsync）
6. raw_record_id 格式：`"{source}:{dataset}:{filename}:{line_no}"`

### 错误处理

- 磁盘写满 → StorageError
- 权限不足 → StorageError
- 无静默丢行（写入失败抛异常，不吞）

## 测试要求

- **append→iter_refs 往返**：写入 N 条 → iter_refs 读回 N 条，raw_record_id 可定位
- **只追加不覆盖**：同一文件二次 append → 文件行数单调递增
- **分区归属**：跨日批次按 ingest_date 正确分区
- **空批次**：batches=[] 不创建文件
- **大 payload**：base64 编码/解码往返一致
- **边界**：单文件 10000 行（不截断）、10001 行（创建新文件）
- **失败**：data_dir 权限不足 → StorageError

## acceptance（GWT）

- [x] Given append 100 行 When iter_refs Then 逐行可回读且 raw_record_id 可定位
- [x] Given 同一文件二次 append Then 文件行数单调递增
- [x] Given ingest_date 跨日 When append Then 按日期分区

## 验收清单（2026-09-12 验收通过）

DoD 16 项：

- [x] 实现 / [x] 公共 API / [x] 数据契约 / [x] 错误处理（StorageError 语义，不吞不转）
- [x] 日志（N/A 核心存储）/ [x] 指标（N/A）/ [x] 单测（27 用例）/ [x] 边界（10000/10001 行拆分、空批次）
- [x] 失败（只读目录 → StorageError）/ [x] 恢复（N/A 只追加不覆盖）/ [x] 集成（append↔iter_refs 往返）
- [x] 静态分析 / [x] 类型检查 / [x] 无未声明假设 / [x] 验收通过

测试输出摘要：pytest 27 passed（全 27/27）｜接管性抽查：✅ 通过——raw.py 模块 docstring 标注设计依据（D03 §2），RawRef 数据类字段与任务单完全一致，路径布局、写入协议、fsync 均可直接验证
