# QUERY-001 — DuckDBQueryService

## 派发信息

- 任务单：D10 §7 QUERY-001
- 设计依据：D07 §1（修复后）
- Agent：Orchestrator 主会话直执（Coding-Agent 模式）
- 派发时间：2026-09-12
- 状态：DONE（2026-09-21）
- 依赖：STORAGE-001（MetaStore + dataset_registry）、STORAGE-004（register_views）

## file_ownership

- `src/chronoforge/research/query.py`（新建，DuckDBQueryService）
- `src/chronoforge/research/__init__.py`（新建，__all__ 导出）
- `tests/integration/test_query.py`（新建）

## 交付物契约摘要

### QueryResult 数据类（research/query.py）

```python
@dataclass
class QueryResult:
    frame: pl.DataFrame           # polars（批量语义，架构 04 §4）
    dataset_id: str
    dataset_version: str
    schema_version: str
    row_count: int
    elapsed_ms: int
```

### DuckDBQueryService 类（research/query.py）

```python
class DuckDBQueryService:
    def __init__(self, con: duckdb.DuckDBPyConnection, registry: DatasetRegistry):
        """
        Args:
            con: read_only DuckDB 连接（架构 02 规则 4 的执行点）
            registry: DatasetRegistry 实例（STORAGE-001 的 registry 查询接口）
        """
        self.con = con
        self.registry = registry

    def query(
        self,
        dataset_id: str,
        columns: list[str] | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        asof: datetime | None = None,
        filters: Mapping[str, Any] | None = None,
    ) -> QueryResult:
        """
        查询入口（D07 §1 六规则按序执行）。
        """
        start_ts = time.monotonic()

        # 规则 1：dataset_id → canonical_type → 视图名
        dataset = self.registry.get(dataset_id)
        if not dataset:
            raise ValueError(f"Dataset not found: {dataset_id}")

        canonical_type = dataset.canonical_type
        view_name = canonical_type.lower()

        # 规则 2：时间列选择
        time_col = self._resolve_time_column(canonical_type)

        # 规则 3：as-of 语义
        if asof is not None:
            if dataset.revision_supported:
                # 切换 as-of 视图
                view_name = f"{view_name}_asof"
                self.con.execute(f"SET VARIABLE asof = '{asof.isoformat()}'")

        # 规则 4：filters 白名单校验
        valid_columns = self._get_view_columns(view_name)
        if filters:
            for col in filters:
                if col not in valid_columns:
                    raise ValueError(f"Unknown column: {col}")

        # 规则 5+6：构建 SQL
        sql = self._build_sql(
            view_name=view_name,
            time_col=time_col,
            columns=columns,
            start=start,
            end=end,
            filters=filters,
        )

        # 执行查询
        result = self.con.execute(sql)
        df = pl.from_records(result.fetchall(), schema=result.description)

        elapsed_ms = int((time.monotonic() - start_ts) * 1000)

        # 规则 5：恒附加 dataset_version/schema_version 元信息
        dataset_version = self._get_dataset_version(dataset_id)

        return QueryResult(
            frame=df,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            schema_version="1.0",  # 从 registry 获取
            row_count=len(df),
            elapsed_ms=elapsed_ms,
        )

    def _resolve_time_column(self, canonical_type: CanonicalType) -> str:
        """时间列选择（D07 §1 规则 2）"""
        if canonical_type in (CanonicalType.NUMBER, CanonicalType.FLOW, CanonicalType.FUNDAMENTAL):
            return "observation_time"
        elif canonical_type == CanonicalType.FILING:
            return "filing_date"
        else:
            return "event_time"  # 默认

    def _build_sql(
        self,
        view_name: str,
        time_col: str,
        columns: list[str] | None,
        start: datetime | None,
        end: datetime | None,
        filters: Mapping[str, Any] | None,
    ) -> str:
        """构建查询 SQL（单条）"""
        # 规则 6：默认排序（主时间列升序，并列按 natural key 次列稳定排序）
        order_by = f"{time_col} ASC"

        sql = f"SELECT * FROM {view_name}"
        if columns:
            sql = f"SELECT {', '.join(columns)} FROM {view_name}"

        where_clauses = []
        if start:
            where_clauses.append(f"{time_col} >= '{start.isoformat()}'")
        if end:
            where_clauses.append(f"{time_col} < '{end.isoformat()}'")

        if filters:
            for col, value in filters.items():
                where_clauses.append(f"{col} = '{value}'")

        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)

        sql += f" ORDER BY {order_by}"
        return sql
```

### 六规则实现总结（D07 §1）

| 规则 | 实现 |
|---|---|
| 1. dataset_id → 视图名 | `registry.get(dataset_id).canonical_type.lower()` |
| 2. 时间列选择 | NUMBER/FILING→observation_time/filing_date；其余→event_time |
| 3. as-of 语义 | revision_supported=true → `{view}_asof` 视图 + `SET VARIABLE asof` |
| 4. filters 白名单 | `_get_view_columns()` 校验列名存在性 |
| 5. 恒附加元信息 | `dataset_version`/`schema_version` 附加到 QueryResult |
| 6. 默认排序 | 主时间列升序 + natural key 次列稳定排序，不可关闭 |

### dataset_version 获取

```python
def _get_dataset_version(self, dataset_id: str) -> str:
    """获取 dataset_version（最近 SUCCESS run 的 code_version+schema_version 复合）"""
    # 查 dataset_registry.current_version 扩展列
    ...
```

- 迁移 0003 添加 `current_version` 列到 dataset_registry

### 错误处理

- 未知 dataset_id → ValueError
- 未知列 → ValueError
- 连接只读：QueryService 持有的 DuckDB 连接以 read_only 模式打开

## 测试要求（D09 TC-R 组）

- **TC-R-001**：as-of 两 vintage——构造 v1/v2 → asof=T1 取 v1，asof=T2 取 v2
- **TC-R-002**：SQL 注入防御——filters 含 `"; DROP"` → ValueError，连接无恙
- **TC-R-003**：read_only 写失败——QueryService 连接上尝试写 → 异常
- **边界**：空结果集、asof=最早时刻
- **默认排序**：同输入两次查询结果顺序一致

## 实现决策（How 自由度）与偏差

| ID | 决策/偏差 | 依据 |
|---|---|---|
| D-1 | `DatasetRegistry` 定义于 research/query.py（读侧最小接口：`get()`/`version_info()`，构造参数为 `MetaStore.connection`） | D07 §1 构造签名要求 `registry: DatasetRegistry`，但 D01 §2 规划的 registry/service.py 未派发任何任务；STORAGE-001 file_ownership 不含该类。QUERY-001 在自有 ownership 内提供读侧实现，与 runner.py 查询 dataset_registry 同模式 |
| D-2 | `dataset_version` 经 `run_log` 最近一次 SUCCESS run 推导（格式 `"{code_version}+{schema_version}"`，无 SUCCESS run 为 `""`）；未创建迁移 0003/`current_version` 扩展列 | D07 §1 的语义即"最近 SUCCESS run 的复合"；迁移 0003 未派发给任何任务（QUERY-001 file_ownership 无 migrations/）。run_log 为同一事实源，语义一致，无需 schema 变更 |
| D-3 | 时间列选择采用 `storage.base.partition_time_field`（D02 §4 冻结矩阵），ENTITY/INSTRUMENT 回退 `ingest_timestamp`（审计 M-5 同款）；非任务单伪代码的简化"其余→event_time" | 规则 6 排序不可关闭：POSITION/DOCUMENT/DERIVED 等类型无 event_time 列，简化映射将生成非法 SQL。D02 §4 矩阵为设计权威 |
| D-4 | revision_supported=true 时无论 asof 是否为 None 一律走 `{type}_asof` 视图（asof=None → `SET VARIABLE "asof"=now()`） | D07 §1 签名注释"缺省=now()"；基础视图含全部 vintage（未 release 数据可见），违背 as-of 防前视语义 |
| D-5 | 视图未注册（数据未写入，STORAGE-004 惰性语义）→ 返回空 QueryResult（元信息照常附加） | 已登记 dataset 无数据属正常态；空结果语义与惰性注册一致 |
| D-6 | ORDER BY 的 nk 次列仅取视图列集中实际存在的列 | schema 缺列防御（fixture/部分类型缺可选 nk 列时避免 Binder 错误）；正常 canonical 数据恒有全列，行为不变 |
| D-7 | filters 值全部 `?` 参数化（相等过滤）；注入载荷位于列名位 → 白名单 ValueError，位于值位 → 参数化安全执行零匹配 | D07 §1 规则 4"列名来自 introspection，值全部参数化"；TC-R-002 两种载荷位均覆盖 |

## acceptance（GWT）

- [x] Given 两 vintage When asof=T1 Then 仅 v1 且默认按 observation_time 升序稳定排序（test_asof_t1_returns_v1_stable_ordered：T1 下 v2(200) 不可见，observation_time 升序 + nk 次列 source_id 稳定排序；test_nk_tiebreak_ordering 佐证）
- [x] Given filters 含 "; DROP" Then ValueError 且连接无恙（test_injection_in_filter_key_valueerror：列名位 → ValueError + 后续查询正常；test_injection_in_filter_value_parameterized：值位 → 参数化零匹配且视图未被 DROP）

## 验收清单（验收时填写）

DoD 逐项勾选（howto §43）：

- [x] 实现完整（research/query.py：六规则按序执行、单条 SQL 生成）
- [x] Public API 完整（DuckDBQueryService.query 签名与 D07 §1 逐参对齐；QueryResult 六字段；DatasetRegistry.get/version_info；__init__.py __all__ 导出）
- [x] 数据契约实现（QueryResult.frame=polars、元信息三字段恒附加，含空结果/视图未注册路径）
- [x] 错误处理实现（未知 dataset_id/未知列 → ValueError；read_only 连接由构造方保证，TC-R-003 验证写路径被拒）
- [x] 日志实现（D10 任务单无 observability 条目，未引入日志面；耗时经 elapsed_ms 返回供调用方记录）
- [x] 指标实现（elapsed_ms/row_count 随 QueryResult 返回；无额外计数器需求）
- [x] 单测完整（D10 tests 字段仅定义 integration TC-R 组 + boundary + failure，均已覆盖）
- [x] 边界测试完整（空结果集、asof=最早时刻、视图未注册（惰性）、naive/aware 等价）
- [x] 失败测试完整（TC-R-002 注入防御两载荷位、TC-R-003 read_only 写失败、未知列/未知 dataset）
- [x] 恢复测试完整（不适用——只读无状态服务，无恢复路径；D10 tests 字段未定义 recovery 组）
- [x] 集成测试完整（22 用例：真实 MetaStore/SQLite + 手工 parquet + write 注册视图后 read_only 重开）
- [x] 静态分析通过（ruff All checks passed——research/query.py、research/__init__.py、test_query.py）
- [x] 类型检查通过（mypy Success：全库 56 source files 0 issues）
- [x] 无未声明假设（D-1~D-7 全部入档本记录）
- [x] 验收标准满足（GWT-1/2 逐条通过）

测试输出摘要：test_query.py **22 passed** in 4.86s；全量回归 **1234 passed / 0 failed** in 123.66s（基线 1212 + 新增 22，零破坏）；ruff All checks passed；mypy Success（56 source files）；lint-imports 1 broken 为已入档存量问题（models.reference→exceptions→connectors.errors，0 处涉及 chronoforge.research，非本任务引入）。

接管性抽查：模块 docstring 完整陈述六规则与只读契约；query() 内联注释逐条标注规则编号（规则 1~6 与 D07 §1 一一对应）；D-1~D-7 决策均给出设计依据，陌生 Agent 仅凭任务单 + D07 §1 + 本记录可接管维护。**通过**。

git commit：**PENDING**（git 因 Xcode 许可证未接受不可用；建议许可接受后执行 `feat(research): QUERY-001 DuckDBQueryService with as-of/injection-defense/read-only/stable-order`）

## 执行记录

| 时间 | 事件 |
|---|---|
| 2026-09-21 | 主会话直执：实现六规则 + DatasetRegistry 读侧最小接口（D-1）+ dataset_version 经 run_log 推导（D-2）+ D02 §4 时间列矩阵（D-3）；发现 DuckDB `asof` 为保留字需引号（与 STORAGE-004 测试惯例一致） |
| 2026-09-21 | 交付 research/query.py（309 行）+ research/__init__.py（17 行，__all__ 导出）+ tests/integration/test_query.py（464 行，22 用例）；22 passed，全量 1234 passed，ruff/mypy 清零 |
| 2026-09-21 | 验收：GWT 逐条核对通过 + DoD 15 项全勾 + 接管性抽查通过 → DONE；无新增 Design Issue，无 Deferred 项 |

## Deferred Acceptance

无。本子任务独立闭环。
