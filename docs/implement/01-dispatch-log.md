# IMP-01 — 任务派发台账

状态：READY（可派发）/ DISPATCHED（已派发）/ IN_PROGRESS（执行中）/ IN_REVIEW（验收中）/ DONE（完成）/ BLOCKED（受阻，见任务记录）/ FAILED（验收失败）。

## 1. 任务状态总表（含子任务，依据 D10 §10 派发 DAG）

| Task ID | 层 | 标题 | 状态 | 派发时间 | 完成时间 | 记录 |
|---|---|---|---|---|---|---|
| MODEL-001 | L0 | BaseRecord 基座 | DONE | 2026-09-11 | 2026-09-11 | [tasks/MODEL-001.md](tasks/MODEL-001.md) |
| INFRA-001 | L0 | 测试脚手架 | DONE | 2026-09-11 | 2026-09-11 | [tasks/INFRA-001.md](tasks/INFRA-001.md) |
| MODEL-002 | L1 | 27 Canonical Type schema | DONE | 2026-09-11 | 2026-09-13 | [tasks/MODEL-002.md](tasks/MODEL-002.md) |
| ↳ MODEL-002.1 | L1 | market.py 类型 | DONE | 2026-09-11 | 2026-09-11 | [tasks/MODEL-002.1.md](tasks/MODEL-002.1.md) |
| ↳ MODEL-002.2 | L1 | derivatives.py 类型 | DONE | 2026-09-11 | 2026-09-11 | [tasks/MODEL-002.2.md](tasks/MODEL-002.2.md) |
| ↳ MODEL-002.3 | L1 | macro/fundamental/positioning/prediction/text | DONE | 2026-09-11 | 2026-09-11 | [tasks/MODEL-002.3.md](tasks/MODEL-002.3.md) |
| ↳ MODEL-002.4 | L1 | reference/derived + natural_key | DONE | 2026-09-11 | 2026-09-13 | [tasks/MODEL-002.4.md](tasks/MODEL-002.4.md) |
| MODEL-003 | L1 | InstrumentResolver | DONE | 2026-09-11 | 2026-09-13 | [tasks/MODEL-003.md](tasks/MODEL-003.md) |
| ↳ MODEL-003.1 | L1 | parse_binance | DONE | 2026-09-11 | 2026-09-11 | [tasks/MODEL-003.1.md](tasks/MODEL-003.1.md) |
| ↳ MODEL-003.2 | L1 | parse_deribit/parse_yahoo + Resolver | DONE | 2026-09-11 | 2026-09-13 | [tasks/MODEL-003.2.md](tasks/MODEL-003.2.md) |
| STORAGE-001 | L1 | MetaStore + 迁移 | DONE | 2026-09-11 | 2026-09-13 | [tasks/STORAGE-001.md](tasks/STORAGE-001.md) |
| ↳ STORAGE-001.1 | L1 | DDL + 迁移 0001 | DONE | 2026-09-11 | 2026-09-11 | [tasks/STORAGE-001.1.md](tasks/STORAGE-001.1.md) |
| ↳ STORAGE-001.2 | L1 | 锁/checkpoint/run_log/状态推导 | DONE | 2026-09-11 | 2026-09-14 | [tasks/STORAGE-001.2.md](tasks/STORAGE-001.2.md) |
| STORAGE-002 | L1 | RawStore JSONL | DONE | 2026-09-11 | 2026-09-13 | [tasks/STORAGE-002.md](tasks/STORAGE-002.md) |
| ↳ STORAGE-002.1 | L1 | append + iter_refs 核心 | DONE | 2026-09-11 | 2026-09-12 | [tasks/STORAGE-002.1.md](tasks/STORAGE-002.1.md) |
| ↳ STORAGE-002.2 | L1 | 分区清理 + 元数据 | DONE | 2026-09-11 | 2026-09-12 | [tasks/STORAGE-002.2.md](tasks/STORAGE-002.2.md) |
| STORAGE-003 | L2 | CanonicalStore merge-rewrite | DONE | 2026-09-12 | 2026-09-13 | [tasks/STORAGE-003.md](tasks/STORAGE-003.md) |
| ↳ STORAGE-003.1 | L2 | upsert 核心（分组/去重/merge-rewrite/分区布局） | DONE | 2026-09-12 | 2026-09-13 | [tasks/STORAGE-003.1.md](tasks/STORAGE-003.1.md) |
| ↳ STORAGE-003.2 | L2 | drift 检测 + Q-DRIFT-001 findings | DONE | 2026-09-12 | 2026-09-13 | [tasks/STORAGE-003.2.md](tasks/STORAGE-003.2.md) |
| ACQUISITION-001 | L2 | Connector 协议/限流/重试 | DONE | 2026-09-12 | 2026-09-14 | [tasks/ACQUISITION-001.md](tasks/ACQUISITION-001.md) |
| ↳ ACQUISITION-001.1 | L2 | DataConnector 协议 + 错误映射 | DONE | 2026-09-12 | 2026-09-14 | [tasks/ACQUISITION-001.1.md](tasks/ACQUISITION-001.1.md) |
| ↳ ACQUISITION-001.2 | L2 | 令牌桶限流器 | DONE | 2026-09-12 | 2026-09-14 | [tasks/ACQUISITION-001.2.md](tasks/ACQUISITION-001.2.md) |
| ↳ ACQUISITION-001.3 | L2 | 重试装饰器 + HTTP 映射 | DONE | 2026-09-12 | 2026-09-14 | [tasks/ACQUISITION-001.3.md](tasks/ACQUISITION-001.3.md) |
| VALIDATION-001 | L2 | 质量规则引擎 | DONE | 2026-09-12 | 2026-09-14 | [tasks/VALIDATION-001.md](tasks/VALIDATION-001.md) |
| ↳ VALIDATION-001.1 | L2 | 规则引擎框架（协议/注册表/run/report） | DONE | 2026-09-12 | 2026-09-14 | [tasks/VALIDATION-001.1.md](tasks/VALIDATION-001.1.md) |
| ↳ VALIDATION-001.2 | L2 | 17 条质量规则实现 | DONE | 2026-09-12 | 2026-09-14 | [tasks/VALIDATION-001.2.md](tasks/VALIDATION-001.2.md) |
| STORAGE-004 | L3 | DuckDB 视图 | DONE | 2026-09-12 | 2026-09-15 | [tasks/STORAGE-004.md](tasks/STORAGE-004.md) |
| STORAGE-005 | L3 | 原子性/对账 | DONE | 2026-09-12 | 2026-09-15 | [tasks/STORAGE-005.md](tasks/STORAGE-005.md) |
| ACQUISITION-002 | L3 | 窗口/cursor 引擎 | DONE | 2026-09-12 | 2026-09-15 | [tasks/ACQUISITION-002.md](tasks/ACQUISITION-002.md) |
| VALIDATION-002 | L3 | Continuity + 日历 | DONE | 2026-09-12 | 2026-09-15 | [tasks/VALIDATION-002.md](tasks/VALIDATION-002.md) |
| DATA-SOURCE-001 | L3 | binance_spot | DONE | 2026-09-12 | 2026-09-16 | [tasks/DATA-SOURCE-001.md](tasks/DATA-SOURCE-001.md) |
| DATA-SOURCE-002 | L3 | binance_futures | DONE | 2026-09-12 | 2026-09-16 | [tasks/DATA-SOURCE-002.md](tasks/DATA-SOURCE-002.md) |
| DATA-SOURCE-003 | L3 | deribit | DONE | 2026-09-12 | 2026-09-17 | [tasks/DATA-SOURCE-003.md](tasks/DATA-SOURCE-003.md) |
| DATA-SOURCE-004 | L3 | ccxt_bridge | DONE | 2026-09-12 | 2026-09-17 | [tasks/DATA-SOURCE-004.md](tasks/DATA-SOURCE-004.md) |
| DATA-SOURCE-005 | L3 | yahoo | DONE | 2026-09-12 | 2026-09-17 | [tasks/DATA-SOURCE-005.md](tasks/DATA-SOURCE-005.md) |
| DATA-SOURCE-006 | L3 | fred | DONE | 2026-09-12 | 2026-09-18 | [tasks/DATA-SOURCE-006.md](tasks/DATA-SOURCE-006.md) |
| DATA-SOURCE-007 | L3 | sec_edgar | DONE | 2026-09-12 | 2026-09-18 | [tasks/DATA-SOURCE-007.md](tasks/DATA-SOURCE-007.md) |
| PIPELINE-001 | L4 | Runner 七阶段 | DONE | 2026-09-12 | 2026-09-20 | [tasks/PIPELINE-001.md](tasks/PIPELINE-001.md) |
| QUERY-001 | L4 | QueryService | DONE | 2026-09-12 | 2026-09-21 | [tasks/QUERY-001.md](tasks/QUERY-001.md) |
| QUERY-002 | L5 | FeatureEngine | DONE | 2026-09-12 | 2026-09-21 | [tasks/QUERY-002.md](tasks/QUERY-002.md) |
| QUERY-003 | L5 | ResearchSnapshot | READY | 2026-09-12 | | [tasks/QUERY-003.md](tasks/QUERY-003.md) |
| CLI-001 | L6 | 命令树/Settings/脱敏 | READY | 2026-09-12 | | [tasks/CLI-001.md](tasks/CLI-001.md) |

## 2. 派发事件流水

| 时间 | 事件 | 备注 |
|---|---|---|
| 2026-09-11 | 派发 MODEL-001（Agent-A）、INFRA-001（Agent-B）并行 | L0 首批；file_ownership 无交集；Agent 不执行 git |
| 2026-09-11 | MODEL-001 验收通过 → DONE | 12 用例；UP042 契约优先决策入档；DEF-003 登记 |
| 2026-09-11 | INFRA-001 验收通过 → DONE | 24 用例全绿；DEF-001/002/004 登记；.gitignore 补 .hypothesis/ |
| 2026-09-11 | 派发 MODEL-002+003（Agent-C 顺序执行，models 域收口） | L1 第一部分；STORAGE-001/002 留第二部分 |
| 2026-09-11 | MODEL-002 拆分为 4 个子任务（MODEL-002.1~2.4） | market→derivatives→macro/fundamental/positioning/prediction/text→reference/derived，单任务 30 分钟粒度 |
| 2026-09-11 | MODEL-003 拆分为 2 个子任务（MODEL-003.1~3.2） | parse_binance→parse_deribit/parse_yahoo 顺序执行 |
| 2026-09-11 | STORAGE-001 拆分为 2 个子任务（STORAGE-001.1~1.2） | DDL+迁移→锁/checkpoint/run_log/状态推导 |
| 2026-09-11 | STORAGE-002 拆分为 2 个子任务（STORAGE-002.1~2.2） | append+iter_refs→分区清理+元数据 |
| 2026-09-11 | MODEL-002.1 验收通过 → DONE | 58 用例全绿；mypy strict 通过；ruff 通过；OHLCV 字段顺序调整决策入档 |
| 2026-09-11 | MODEL-002.2 验收通过 → DONE | 84 passed in 0.58s；OPTION/IV/GREEKS/LIQUIDATION 类型闭环 |
| 2026-09-11 | MODEL-002.3 验收通过 → DONE | 81 passed in 0.56s；macro/fundamental/positioning/prediction/text 类型闭环 |
| 2026-09-11 | MODEL-003.1 验收通过 → DONE | 13 passed（全库 293 passed）；parse_binance + EntityResolver 完成 |
| 2026-09-11 | STORAGE-001.1 验收通过 → DONE | DDL + 迁移 0001 完成 |
| 2026-09-12 | STORAGE-002.1 验收通过 → DONE | 27 passed；RawStore append + iter_refs 核心完成 |
| 2026-09-12 | STORAGE-002.2 验收通过 → DONE | 18 passed；RawStore 分区清理 + 元数据完成 |
| 2026-09-12 | 派发 STORAGE-003 拆分为 2 个子任务（STORAGE-003.1~3.2） | upsert 核心→drift 检测；ACQUISITION-001 拆为 3 子任务；VALIDATION-001 拆为 2 子任务；STORAGE-004/005/ACQUISITION-002/VALIDATION-002/DATA-SOURCE-001~007/PIPELINE-001/QUERY-001/002/003/CLI-001 首次派发 |
| 2026-09-13 | STORAGE-003.1 验收通过 → DONE | 21 passed in 3.61s；merge-rewrite upsert 核心完成；ruff + mypy 无错误 |
| 2026-09-13 | STORAGE-003.2 验收通过 → DONE | 27 passed；drift 检测 + Q-DRIFT-001 findings 完成；全部测试 145/145 passed |
| 2026-09-13 | STORAGE-004 验收通过 → DONE | 13 passed in 3.18s；DuckDB 视图注册（含 as-of）完成 |
| 2026-09-13 | MODEL-002.4 验收通过 → DONE | reference/derived 类型 + natural_key() 完整实现完成 |
| 2026-09-13 | MODEL-003.2 验收通过 → DONE | 38 passed in 0.73s；parse_deribit + parse_yahoo + InstrumentResolver 完成 |
| 2026-09-13 | MODEL-002 所有子任务完成 → DONE（父任务收尾） | MODEL-002.4 验收完成，27 Canonical Type schema 全部闭环 |
| 2026-09-13 | MODEL-003 所有子任务完成 → DONE（父任务收尾） | MODEL-003.2 验收完成，InstrumentResolver 完整闭环 |
| 2026-09-13 | STORAGE-001 所有子任务完成 → DONE（父任务收尾） | 子任务 1.1/1.2 完成后，DDL + 迁移/锁/checkpoint/run_log/状态推导 闭环 |
| 2026-09-13 | STORAGE-002 所有子任务完成 → DONE（父任务收尾） | 子任务 2.1/2.2 完成后，RawStore append/iter_refs/分区清理/元数据 闭环 |
| 2026-09-13 | STORAGE-003 所有子任务完成 → DONE（父任务收尾） | STORAGE-003.1 + STORAGE-003.2 验收完成，CanonicalStore merge-rewrite + drift 检测 闭环 |
| 2026-09-14 | STORAGE-001.2 验收通过 → DONE | 34 passed；MetaStore 锁/checkpoint/run_log/状态推导/rebuild 七方法完整实现；ruff + mypy 通过 |
| 2026-09-14 | VALIDATION-001.2 验收通过 → DONE | 50 passed in 2.42s（全库 451 passed）；17 条质量规则全部实现闭环 |
| 2026-09-14 | VALIDATION-001 所有子任务完成 → DONE（父任务收尾） | VALIDATION-001.1 + VALIDATION-001.2 验收完成，质量规则引擎框架 + 17 条规则闭环 |
| 2026-09-14 | ACQUISITION-001.1 验收通过 → DONE | 49 passed in 0.27s；DataConnector 协议/数据类/错误映射/HTTP映射表实现闭环 |
| 2026-09-14 | ACQUISITION-001.2 验收通过 → DONE | 24 passed；令牌桶限流器（RateLimiter）完整实现；时钟注入/线程安全/权重适配/429 冷却闭环；ruff + mypy 通过 |
| 2026-09-14 | ACQUISITION-001.3 验收通过 → DONE | 21 passed；retry 装饰器（指数退避+jitter+structlog）完整实现；ruff + mypy 通过；全库 669/669 passed |
| 2026-09-14 | ACQUISITION-001 所有子任务完成 → DONE（父任务收尾） | ACQUISITION-001.1 + 001.2 + 001.3 验收完成，Connector 协议/限流/重试 闭环 |
| 2026-09-15 | ACQUISITION-002 验收通过 → DONE | pipeline/windows.py（AcquisitionJob/Chunk/plan_chunks，表驱动窗口配置覆盖 10 数据集）+ pipeline/cursor.py（CursorAction/CursorUpdate/advance_cursor，chunk_failed 传播）+ 63 测试用例；ruff + mypy strict 通过；全库 731/731 passed |
| 2026-09-15 | STORAGE-005 验收通过 → DONE | consistency.py（cleanup_orphans + reconcile + startup_repair）+ 22 测试用例；mypy strict 通过；ruff 4 issues（2 处 import 排序可 fix、2 处 docstring 行超长） |
| 2026-09-15 | VALIDATION-002 验收通过 → DONE | calendar.py（简化交易日历 is_trading_day）+ continuity.py（ContinuityModel 枚举 + expected_grid）+ 46 测试用例（TC-Q-007 + 日历矩阵 + ALWAYS_OPEN + 边界）+ Q-GAP-001 集成 expected_grid（EXPECTED_GAP/INFO 判定）；ruff + mypy 无新增错误；全库 776/776 passed |
| 2026-09-16 | DATA-SOURCE-001 验收通过 → DONE | BinanceSpotConnector 完整实现（connectors/binance_spot.py）+ 27 集成测试（TC-C-001 + TC-Q-008 + Q-TS-003 + 错误路径 + 分页 + checkpoint/validate/GWT）+ 3 个 fixture 端点（klines/aggTrades/ticker24hr）；ruff + mypy 无错误；27 passed in 0.20s |
| 2026-09-16 | DATA-SOURCE-002 验收通过 → DONE | BinanceFuturesConnector 完整实现（connectors/binance_futures.py）+ 33 集成测试（TC-C-002 klines/funding/OI + TC-C-011 funding 分页 + OI upsert + GWT + 错误路径 + checkpoint/validate/边界）+ 6 个 fixture 文件（klines/happy+empty+edge, fundingRate/happy+two_pages+empty, openInterest/happy）；ruff + mypy strict 无错误；33 passed in 0.58s |
| 2026-09-17 | DATA-SOURCE-003 验收通过 → DONE | DeribitConnector 完整实现（connectors/deribit.py）+ 35 集成测试（TC-C-003 get_instruments/get_book_summary/chart fixture→canonical 逐字段比对 + discover 全量 + TC-M-004/005 parse_deribit 月份码/PERP 永续合约 + 过期合约 + 非法月份码 + 错误路径 + 分页 + checkpoint/validate/GWT）+ 4 个 fixture 目录；修复：parse_deribit 增加 PERP 支持、normalize_instruments 优先 instrument_name 解析；ruff + mypy 无错误；35 passed in 0.41s |
| 2026-09-17 | DATA-SOURCE-004 验收通过 → DONE | CcxtBridgeConnector 完整实现（connectors/ccxt_bridge.py）+ 29 集成测试（TC-C-004 fixture→canonical 逐字段比对 OHLCV/TRADE + TC-C-013 capability detection exchange.has 驱动 + fetchOHLCV 2 页 cursor 推进 + 空结果边界 + 错误路径 + GWT）+ 4 个 fixture 文件；修复：OHLCV 字段索引（ccxt 格式 [timestamp,datetime_str,o,h,l,c,v]）、naive datetime→ms epoch 转换（calendar.timegm）、pagination 无限循环 guard、_extract_symbol 支持 raw_meta；ruff + mypy 无错误；29 passed in 0.98s |
| 2026-09-17 | DATA-SOURCE-005 验收通过 → DONE | YahooConnector 完整实现（connectors/yahoo.py）+ 19 集成测试（TC-C-005 fixture→canonical 逐字段比对 OHLCV + TC-C-012 split 事件解析 + TC-Q-007 周末 EXPECTED_GAP + 秒→us 精度 + 边界 429/404/空结果）+ 4 个 fixture 文件（happy/split/empty/error_no_result.json）；修复：fixture OHLCV 约束（low ≤ min(open,close)）、Interval 类型注解、list[float] 类型标注；ruff + mypy strict 无错误；19 passed in 1.02s |
| 2026-09-18 | DATA-SOURCE-006 验收通过 → DONE | FREDConnector 完整实现（connectors/fred.py）+ 29 集成测试（TC-C-006 fixture→canonical 逐字段比对 + TC-M-007 双 vintage 并存 + TC-Q-007 周末 EXPECTED_GAP + 边界 429/404/500 + checkpoint/validate/GWT）+ 6 个 fixture 文件（observations/happy+vintage1+vintage2+empty+edge, release_dates/happy.json）；修复：NUMBER 模型使用 source_id（非 series_id）、quality_status 用 QualityStatus.VALID、validate 测试用 model_construct 绕过 pydantic validator；ruff + mypy 无错误；29 passed in 0.42s |
| 2026-09-18 | DATA-SOURCE-007 验收通过 → DONE | SECEdgarConnector 完整实现（connectors/sec_edgar.py）+ 34 集成测试（TC-C-007 fixture→canonical 逐字段比对 FILING/FUNDAMENTAL + TC-C-014 UA 注入断言 + acceptanceDatetime EDT/EST→UTC 时区转换 + 边界 429/404/500 + checkpoint/validate/GWT）+ 4 个 fixture 文件（submissions/happy+empty, companyfacts/happy+empty）；修复：SEC submissions API 返回并行数组格式改为按索引遍历、checkpoint_from 处理并行数组格式、companyfacts normalize 兼容 `$` 和 `value` 键；ruff + mypy strict 无错误；34 passed in 0.44s |
| 2026-09-19 | 全量审计完成（[review/2026-09-19-full-audit.md](../review/2026-09-19-full-audit.md)）→ NOT READY | 发现 4 CRITICAL（C-1~C-4）+ 7 HIGH（H-1~H-7）+ 13 MEDIUM（M-1~M-13）；基线 1033 passed / 3 failed，ruff 109 errors，mypy 37 errors |
| 2026-09-20 | 审计修复轮全部闭环（C-1~C-4 / H-1~H-7 / M-1~M-13） | 存储正确性：NK 映射重写 + 一致性架构测试 + D03 §3 冻结对照表（C-1）、读失败熔断/ rename-swap / 孤儿清理（C-2/C-3/C-4）；checkpoint lineage、drift 落盘、reconcile 语义、resolved 机制、孤儿锁恢复（H-2/H-3/H-5/H-6/H-7）；Arrow schema 冻结契约（H-4）；分区回退/稳定排序/重复计数/Retry-After/STALE 契约/normalize 日志/QUARANTINED 契约（M-4~M-8/M-11~M-13）；补 TC-S-003/TC-M-002/TC-Q-004/006/TC-PROP-001/003 + crash-restart/disk-full/checkpoint 损坏注入测试（M-9/M-10）；ruff + mypy 全库清零（M-1）；全量回归 **1181 passed / 0 failed**，ruff All checks passed，mypy Success（52 文件） |
| 2026-09-20 | 派发 PIPELINE-001（Coding-Agent-P 子会话执行） | L4；依赖 ACQUISITION-002、STORAGE-001/002/003/005、VALIDATION-001、DATA-SOURCE-001~007 均已 DONE；file_ownership 独占 pipeline/{runner,state,replay,__init__}.py + tests/integration/test_pipeline.py；Agent 不执行 git |
| 2026-09-20 | PIPELINE-001 验收通过 → DONE | runner/state/replay/__init__ 四文件 + 31 集成测试（TC-P-001~011 全组 + 锁互斥 + drift 落盘，含参数化共 38 用例）；GWT 逐条核对通过（GWT-3 derived 层登记 DEF-005 依赖 QUERY-002）；偏差裁决入档（DEVIATION-1 replay run_id 前缀、DEC-F0 首 chunk 失败→FAILED 等 8 项）；复跑：31 passed，全量 **1212 passed / 0 failed**，ruff All checks passed，mypy strict（pipeline 6 文件）0 issues，lint-imports 1 broken 为存量问题（0 处涉及 pipeline）；git commit PENDING（Xcode 许可证未接受） |
| 2026-09-21 | QUERY-001 主会话直执并验收通过 → DONE | research/query.py（六规则：视图选择/时间列/as-of/白名单/投影/稳定排序）+ research/__init__.py（__all__）+ test_query.py 22 用例（TC-R-001/002/003 + 边界 + 稳定排序）；决策 D-1~D-7 入档（DatasetRegistry 读侧最小接口定义于 research/query.py、dataset_version 经 run_log 推导替代未派发的迁移 0003、时间列取 D02 §4 矩阵、asof 缺省=now() 走点时视图等）；复跑：22 passed，全量 **1234 passed / 0 failed**，ruff All checks passed，mypy Success（56 文件），lint-imports 1 broken 为存量问题（0 处涉及 research）；git commit PENDING（Xcode 许可证未接受） |
| 2026-09-21 | 两周积压按域分 10 笔补提交完成（Xcode 许可证已接受） | 8d347c6(model)→a021c1b(storage)→c4da48a(acquisition)→6aa7492(data-source)→c7cd885(quality)→b67d15a(pipeline)→2ffe43f(research: QUERY-001)→969f60d(config)→353b27d(docs: implement)→ecdf377(chore)；QUERY-001 git commit PENDING→DONE（8fa86e5）；清理误存 index.html |
| 2026-09-21 | QUERY-002 主会话直执并验收通过 → DONE | features/engine.py（FeatureEngine 协议 + FeatureRegistry + FeatureResult.output_hash + register 构造注入）+ features/builtin.py（5 个 P0 特征：returns/realized_vol/iv_surface/funding_oi_divergence/btc_market_stress）+ features/__init__.py（`__all__`）+ test_features.py 26 用例（GWT 双条 + 黄金值 + 确定性/replay hash + 注册中心 + realized_vol→stress 特征链闭环）；决策 D-1~D-8 入档（compute 返回 FeatureResult、写连接构造注入、版本语义同 QUERY-001 D-2、QueryServiceLike 同层协议、IV strike/expiry 自 instrument_id 镜像解析、stress 并集网格 + 行级可用分量归一替代伪代码缺陷等）；复跑：26 passed，全量 **1260 passed / 0 failed**，ruff features scope All checks passed，mypy Success（58 文件），lint-imports 无新增违规（存量 2 处 broken：models.reference 与 quality.rules → exceptions → connectors.errors 传递链；另存量 2 处 pipeline I001 import 排序随补提交引入，均 0 处涉及 features）；git commit PENDING |

## 3. Design Issue 登记

| ID | 时间 | 任务 | 契约条目 | 状态 |
|---|---|---|---|---|

（空——期望保持为空；任何条目都意味着受控变更流程启动）

## 4. Deferred Acceptance 登记

| ID | 登记时间 | 任务 | 验收项 | 依赖 | 状态 |
|---|---|---|---|---|---|
| DEF-001 | 2026-09-11 | INFRA-001 | strategies.ohlcv() 通过 MODEL-002 schema 校验 | MODEL-002 | CLOSED（2026-09-20：ohlcv() 策略已被 TC-PROP-001 hypothesis 测试消费并通过，OHLCV 不变量断言见 tests/unit/test_strategies.py） |
| DEF-002 | 2026-09-11 | INFRA-001 | conftest 的 settings fixture（Settings 类） | CLI-001 | OPEN |
| DEF-003 | 2026-09-11 | MODEL-001 | TC-M-001/002 的 OHLCV 实例断言 | MODEL-002 | CLOSED（2026-09-20：TC-M-001 tests/unit/test_market_types.py::test_ohlcv_valid；TC-M-002 ::test_ohlcv_high_lt_low_rejected） |
| DEF-004 | 2026-09-11 | INFRA-001 | CI 覆盖率门槛启用 | 首个业务逻辑任务 | OPEN |
| DEF-005 | 2026-09-20 | PIPELINE-001 | GWT-3 derived 层：replay("derived") 输出与原快照逐字节一致（当前 NotImplementedError） | QUERY-002（FeatureEngine） | OPEN（依赖 QUERY-002 已 DONE（2026-09-21），待 PIPELINE-001 接线 FeatureEngine.recompute 后复核） |
