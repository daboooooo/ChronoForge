# STORAGE-002.2 — RawStore 分区清理 + 元数据管理

## 派发信息

- 任务单：D10 §2 STORAGE-002 拆分（子任务 2/2）
- 设计依据：D03 §2 Raw 层 JSONL 只追加存储
- Agent：待定
- 派发时间：2026-09-11
- 状态：READY
- 依赖：STORAGE-002.1（append + iter_refs 核心）

## file_ownership

- `src/chronoforge/storage/raw.py`（追加方法）
- `tests/integration/test_raw_cleanup.py`（新建）

## 交付物契约摘要

### cleanup_old_partitions(retention_days: int) -> int

- 扫描 `{data_dir}/raw/` 下所有 ingest_date 分区
- 删除 ingest_date < now - retention_days 的分区目录
- 返回删除的分区数量

### list_partitions(source: str = None, dataset: str = None) -> list[str]

- 列出 raw 层分区目录
- 可选按 source/dataset 过滤
- 返回 sorted 的分区路径列表

### get_raw_stats(source: str, dataset: str) -> dict

- 统计 raw 层数据量
- 返回：{total_records: int, total_size_bytes: int, partition_count: int, date_range: (start, end)}

## 测试要求

- **cleanup**：retention_days=0 删除所有分区、retention_days=365 保留最近 365 天
- **list_partitions**：无参数列出所有、按 source 过滤、按 dataset 过滤
- **get_raw_stats**：空目录返回 0、有数据时统计准确
- **边界**：分区目录含子目录（递归删除）、空分区目录（跳过）

## acceptance（GWT）

- [x] Given retention_days=0 When cleanup_old_partitions Then 所有分区删除
- [x] Given list_partitions source="A" Then 仅返回 source=A 的分区

## 验收清单（2026-09-12 验收通过）

DoD 16 项：

- [x] 实现 / [x] 公共 API / [x] 数据契约 / [x] 错误处理（StorageError 语义）
- [x] 日志（N/A）/ [x] 指标（N/A）/ [x] 单测（18 用例）/ [x] 边界（子目录递归删除、空分区跳过）
- [x] 失败（N/A）/ [x] 恢复（N/A）/ [x] 集成（cleanup/list/stats 联动）
- [x] 静态分析 / [x] 类型检查 / [x] 无未声明假设 / [x] 验收通过

测试输出摘要：pytest 18 passed（全 18/18）｜接管性抽查：✅ 通过——raw.py 新增三个方法 docstring 标注设计依据（D03 §2），`list_partitions` 返回 sorted 路径列表，`get_raw_stats` 返回结构化 dict
