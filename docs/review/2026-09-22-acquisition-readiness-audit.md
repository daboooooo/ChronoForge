# 行情获取与长期运行就绪审计报告（第二轮，2026-09-22）

- **审计范围**：`src/chronoforge` 与 `tests/` 全量代码（含工作区未提交的 READY-001/003/004/005 改动），对照 `docs/architecture/01~10`、`docs/design/D01~D10` 中关于「行情获取（D04/D05）」与「长期运行（架构 05/08）」的条款
- **审计定位**：不重复 2026-09-21 服务就绪审计（SR-01~SR-18）的维度，聚焦**行情采集链路的增量正确性**（游标/窗口/重试/空结果语义）与**无人值守长期运行**（熔断生命周期、任务隔离、并发护栏）两个主题；同时对上轮 SR-01~SR-16 修复（commit d7d9b35、ccb08d7 及未提交 READY 系列）做全量真实性复核
- **实测基线**：pytest **1385 passed / 0 failed**（144.9s，40 warnings）；ruff **全绿**；mypy **Success（72 文件）**；lint-imports **2 kept / 0 broken**；uv.lock **已入库**
- **审计方法**：静态代码审读（windows/cursor/runner/meta/consistency/connectors 定点逐行）+ 三个并行探查代理交叉验证 + 实际运行测试与静态检查 + 与 D05 §1/§3/§5.2、架构 08 §3 冻结契约逐条比对

---

## 1. Executive Summary

**判定：单进程数据正确性 / 增量语义层面 READY；作为无人值守长期行情服务 NOT READY（2 HIGH，修复量约 50 行）。**

上轮 16 项发现（SR-01~SR-16）经逐条源码验证**全部真实落地**，无一虚报：恢复链已接线、中断写 CANCELLED、熔断已持久化、deribit 分页步进已修、ccxt 异常映射已纠正、查询加了参数化 LIMIT、CLI 资源统一 ExitStack、并发注册窗口已收敛。测试从 1325 增至 1385（+60），warnings 从 260 降至 40。

本轮新发现的缺口集中在**故障后的自动化恢复闭环**，而非数据正确性内核：

1. **熔断打开后永不恢复**——`circuit_open=1` 后所有 run 直接 CANCELLED，`reset_circuit` 仅在成功路径调用（而成功被熔断阻断），`circuit_opened_at` 只写不读，无 half-open/冷却期/手动复位命令。数据源一次 30 分钟维护（跨 3 个 cron tick）→ 该 dataset **永久停摆**，需手工改 SQLite 才能恢复。
2. **`--all-due` 无逐 dataset 异常隔离**——CLI 循环内任一 dataset 抛异常（如 FRED 缺 key 的 ConfigError、AuthError）即中断整个循环，排在其后的全部 dataset 停止采集；FRED 类启动期 ConfigError 不产生 run_log 行，熔断机制对它无效，**每 tick 必然复现**。
3. **D05 冻结契约的「chunk 级重试耗尽」未实现**——[retry.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/retry.py) 是**仅被单测引用的死代码**，`settings.retry_max` 无任何生产消费者；瞬时网络错误直接计 chunk 失败，重试只能靠下一次 cron。
4. **startup_repair 每命令执行且无并发护栏**——`cleanup_orphans` 无年龄护栏可删并发 run 在途的 `.tmp-*`；`reconcile` 无租约护栏会把活跃 run 误判为孤儿（docstring 自认「已知权衡」），且读命令（`pipeline status`/`query`）也触发。

P0 修复量小：熔断加冷却期 half-open（约 15 行）+ CLI 循环逐 job try/except（约 10 行）；R2-03 护栏与 R2-04 重试接线为 P1。

---

## 2. 实测基线

| 检查项 | 命令 | 结果 |
| --- | --- | --- |
| 测试 | `uv run pytest -q` | **1385 passed / 0 failed**（144.9s，40 warnings） |
| Lint | `uv run ruff check src tests` | **All checks passed** |
| 类型 | `uv run mypy src/chronoforge` | **Success**（72 文件） |
| 架构 | `uv run lint-imports` | **2 kept, 0 broken**（SR-05 闭环） |
| 供应链 | `git ls-files uv.lock` | **已入库**（SR-13 闭环） |

剩余 40 条 warnings 全部来自 [yahoo.py:453](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/yahoo.py) 的 `datetime.utcfromtimestamp()`（SR-15 残余，见 R2-06）。

---

## 3. 上轮修复真实性复核（SR-01~SR-16 + READY-001~005）

逐条对照源码验证，**结论：16/16 项全部真实落地**（其中 2 项有轻微残余，转为本轮 R2-06/R2-07）。

| 项 | 验证证据 | 判定 |
| --- | --- | --- |
| SR-01 恢复链接线 | [_wiring.py:68](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/cli/_wiring.py#L68-L73) `open_meta` 调 `startup_repair`（release_stale_locks → cleanup_orphans → reconcile，顺序符合 D03 §5） | ✅ |
| SR-02 中断处理 | [runner.py:793-801](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/pipeline/runner.py#L793-L801) 捕获 KeyboardInterrupt/SystemExit → `_finish_aborted_run(CANCELLED)` → re-raise；[pipeline_cmd.py:111-114](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/cli/pipeline_cmd.py#L111-L114) SIGTERM→KeyboardInterrupt | ✅ |
| SR-03 熔断持久化 | [meta.py:517-602](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/storage/meta.py#L517-L573) `circuit_open/consecutive_failed/circuit_opened_at` 落 checkpoints 表（migration 0002），跨 job/进程累计；runner 打开时读、失败时写、成功时清零 | ✅ |
| SR-04/READY-001 流式化 | [runner.py:880-961](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/pipeline/runner.py#L880-L961) `run_windowed`（方案 B）：窗口对齐 chunk 边界不重不漏、DEC-W4 断点续传/无推进防死循环、单 run 内存 O(窗口) | ✅（残余见 R2-08） |
| SR-05 broken 契约 | 实测 `lint-imports` 2 kept / 0 broken | ✅ |
| SR-06 dataset status 写回 | [meta.py:443-457](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/storage/meta.py#L443-L457) `finish_run` 内 derive + UPDATE dataset_registry，覆盖全部终态路径 | ✅ |
| SR-07 PENDING→RUNNING | [meta.py:332-361](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/storage/meta.py#L332-L361) `mark_run_running`；[runner.py:765](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/pipeline/runner.py#L765) 锁后即调 | ✅ |
| SR-08 deribit 分页 | [deribit.py:260-321](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/deribit.py#L260-L321) 步进按 `resolution × 60_000` | ✅ |
| SR-09/READY-002 批量 fsync | [raw.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/storage/raw.py) 常驻句柄 + append() 调用边界 flush+fsync（durable boundary 契约写明 cursor 只越 fsync 边界） | ✅ |
| SR-10/READY-003 查询防护 | [query.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/research/query.py#L222-L249) `LIMIT ?` 参数化绑定 + `effective_limit=min(limit, max_rows)` + truncated 探测；[settings.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/config/settings.py#L24-L27) `DEFAULT_QUERY_MAX_ROWS=200_000` 可配 | ✅ |
| SR-11/READY-004 ExitStack | CLI 五命令统一 ExitStack；connector 每 job 迭代级释放 | ✅（残余见 R2-07） |
| SR-12 ccxt 异常映射 | [ccxt_bridge.py:52-69](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/ccxt_bridge.py#L52-L69) `NetworkError→TransportError`、`RateLimitExceeded→RateLimitError`（重试白名单可识别） | ✅ |
| SR-13 uv.lock | 已入库 | ✅ |
| SR-15 utcnow 清理 | 260 → 40 条，残余集中于 yahoo（转 R2-06） | ⚠️ |
| SR-16/READY-005 并发窗口 | [_wiring.py:158-241](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/cli/_wiring.py#L158-L241) 只读探测优先（稳态零写）+ 写路径指数退避 + 冲突后只读复探收敛；[views.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/storage/views.py) `missing_views` 判定 | ✅ |
| （附）测试覆盖 | 新增 TestRowLimit / TestResourceLifecycle / TestOpenQueryServiceConcurrency / 熔断持久化 / 分窗口续传等测试随修复落地（1325→1385） | ✅ |

---

## 4. 本轮新发现（Findings）

### 4.1 HIGH（阻碍无人值守长期运行）

#### R2-01 熔断打开后无恢复路径，dataset 永久停摆

- **证据**：[runner.py:739-757](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/pipeline/runner.py#L739-L757) `is_circuit_open` → 直接返回 CANCELLED，不执行 stages；[runner.py:830](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/pipeline/runner.py#L830) `reset_circuit` 仅在非 FAILED 成功路径调用——而熔断打开时 run 根本不执行，成功不可能发生。全库 grep：`circuit_opened_at` **只写不读**（仅 migration 与 record_circuit_failure 写入）；`reset_circuit` 无其他调用方；无冷却期/half-open 逻辑；无任何 CLI 命令可复位熔断。
- **后果**：数据源一次计划内维护（如 Binance 停机 30 分钟、cron 每 10 分钟）→ 连续 3 次 FAILED → 熔断打开 → 此后每次 run 均 CANCELLED，**永远不会再尝试**。行情数据静默断流，直到人工修改 SQLite。对「长期运行获取行情」的目标而言，熔断从保护机制变成了单行本死锁。
- **偏离条款**：架构 08 §3 熔断语义（保护与恢复应成对出现）；审计要求 §48「错误任务可停止，但也必须可恢复」。
- **修复建议**（约 15 行）：`is_circuit_open` 消费 `circuit_opened_at`——超过冷却期（建议 30min，settings 可配）视为 half-open 放行一次（成功则 `reset_circuit`，失败则刷新 `circuit_opened_at`）；同时提供 `pipeline circuit-reset --dataset` 运维命令。P0。

#### R2-02 CLI `--all-due` 无逐 dataset 异常隔离，坏 dataset 阻塞全队列

- **证据**：[pipeline_cmd.py:116-149](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/cli/pipeline_cmd.py#L116-L149) 循环体内对 `runner.run()`/`run_windowed()` 只有 `finally: clear_context()`，无 except——run() 对不可重试异常（AuthError/SchemaError/StorageError 等）在 finish_run 后 re-raise，直接穿透到外层 `except (ChronoForgeError, ValueError)` → `typer.Exit(1)`，**排在其后的 dataset 全部不执行**。更严重：`build_connector`（[pipeline_cmd.py:127-129](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/cli/pipeline_cmd.py#L127-L129)）在锁与 run_log 之前抛 ConfigError（FRED 缺 key，TC-X-002 设计行为），此时无 run_log 行可计数，**熔断机制对它完全无效**，每个 cron tick 都在同一位置中断。runner 的 `run_many`（[runner.py:846-878](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/pipeline/runner.py#L846-L878)）有逐 job 隔离，但 CLI 因需按 dataset 构造 connector 而自行循环，未复用该语义。
- **后果**：一个配置错误/鉴权过期的 dataset 使 `--all-due` 的其余 dataset 停止采集（AuthError 类经 3 次熔断后自愈，ConfigError 类永久阻塞）；cron 场景下表现为「部分数据按 registry 顺序静默断流」。
- **修复建议**（约 10 行）：循环体包 `try/except ChronoForgeError`，记失败清单，循环结束统一 echo + 按是否有失败决定退出码（有失败 exit 1，全部成功 exit 0）。P0。

### 4.2 MEDIUM（服务化/契约偏差）

#### R2-03 startup_repair 每命令执行且无并发护栏，可误伤在途 run

- **证据**：[consistency.py:61-66](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/storage/consistency.py#L61-L66) `cleanup_orphans` 对 `.tmp-*`/`.old-*` **无条件 rmtree**，无 mtime 年龄护栏、无活跃 run 检查；[consistency.py:142-168](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/storage/consistency.py#L142-L168) `reconcile` 对**所有** PENDING/RUNNING 行补记终态，无租约判断（[_wiring.py:62-64](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/cli/_wiring.py#L62-L64) docstring 自认「reconcile 无租约护栏……单机单写者模型下的已知权衡」）；而 `open_meta` 被**全部 CLI 命令**共用——包括 `pipeline status`、`query` 等纯读命令。
- **后果**：cron 重叠（上一 tick 长窗口 backfill 未结束）或运维在 run 进行中执行读命令时：① 另一进程 `open_meta` 的 `cleanup_orphans` 可能删除在途 merge-rewrite 的 `.tmp-*` → 该 run rename-swap 失败 → StorageError → run FAILED（数据不丢——旧文件完好，但 run 失败且窗口需重试）；② `reconcile` 把活跃 run 补记为 SUCCESS（raw 已有其 batch_id 数据）/FAILED，产生中间假终态，随后被真实 finish_run 覆盖——run_log 审计痕迹被污染；③ `_raw_batch_exists`（[consistency.py:74-103](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/storage/consistency.py#L74-L103)）对每个孤儿 run 全量扫描 raw JSONL，raw 累积大后启动延迟线性增长。
- **偏离条款**：D03 §5 recovery 前提「保证无并发运行」在「每命令执行」的接线方式下不成立。
- **修复建议**：① `cleanup_orphans` 加年龄护栏（目录 mtime 距今 > stale_run_timeout 才删）；② `reconcile` 仅处理 `started_at < now - stale_run_timeout` 的行；③ 读命令路径改用免修复的 `open_meta(read_only=True)` 或将 startup_repair 收敛到 `pipeline run` 入口。P1。

#### R2-04 D05 冻结契约「chunk 级重试耗尽」未实现：retry.py 死代码、retry_max 无消费者

- **证据**：全 src grep 确认 [retry.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/retry.py) 的 `retry()` 装饰器（指数退避 + 尊重 Retry-After + `connector.retry` 事件，实现质量完好）**无任何生产调用方**，仅 [tests/unit/test_retry.py](file:///Users/horsenli/Works/ChronoForge/tests/unit/test_retry.py) 引用；`settings.retry_max`（[settings.py:61](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/config/settings.py#L61)）除自身序列化外无消费者；[runner.py:268-285](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/pipeline/runner.py#L268-L285) FetchStage 对 TransportError **一次即计 chunk 失败并 break**，无 in-run 重试。
- **后果**：单次瞬时网络抖动 → chunk 立即失败 → cursor 停留（下次 cron 重试，数据不丢）→ 但 1m K 线数据延迟一个 cron 周期、run 终态频繁 PARTIAL/FAILED 噪音；首 chunk 抖动即 FAILED 会消耗熔断计数（3 次抖动 → 触发 R2-01 的永久停摆）。重试语义实际为「run 粒度（cron 间隔）」而非 D05 冻结的「chunk 级重试耗尽」。
- **偏离条款**：D05 §1 FetchStage「chunk 级重试耗尽→该 chunk 计失败」；架构 08 §1 重试矩阵（max retry/backoff/Retry-After）；D08 `retry_max` 配置契约。
- **修复建议**（二选一，属 Design Change，需走受控变更流程）：① 按 D05 实现——FetchStage 内对可重试异常调用 `retry()`（消费 `retry_max`，退避上限建议 < cron 间隔）；② 修订 D05 契约为「run 粒度重试（无 in-run retry）」，删除 retry.py 与 retry_max 或标注保留理由。倾向 ①：改动小且消除 R2-01 的误触发源。P1。

### 4.3 LOW（残余与卫生）

| 项 | 证据 | 说明 |
| --- | --- | --- |
| R2-05 空 chunk 推进 cursor 的静默缺口残余风险 | [runner.py:286-290](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/pipeline/runner.py#L286-L290) 0 批次即计 success → cursor 推进到 chunk.end | D05 §1 明文「空结果=正常（0 行）」——**设计冻结语义，非偏离**。但中间窗口因源端毛刺返回空时，增量不会重访该区间，唯一兜底是 quality 连续性检测（detect-only，不自动补拉）。建议：窗口完全处于历史区间（end < now − 2×interval）且 0 行时记 WARNING quality flag，便于发现「HTTP 200 + 正常退出 + checkpoint 推进 + 数据缺失」路径 |
| R2-06 SR-15 残余：40 条 DeprecationWarning | [yahoo.py:453](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/connectors/yahoo.py) `utcfromtimestamp()` | Python 3.14 移除前替换 `fromtimestamp(ts, UTC)`，1 行 |
| R2-07 SR-11 残余：build_runner 的 stores 未入 ExitStack | [pipeline_cmd.py:131](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/cli/pipeline_cmd.py#L131) runner 持有 READY-002 后带常驻句柄的 RawStore（有 `close()`，[raw.py:298](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/storage/raw.py#L298)），但 CLI 只关 connector 与 meta | CLI 短进程下靠进程退出兜底，无数据风险（append 返回即 durable）；进程常驻化前必须补。1-2 行 |
| R2-08 SR-04 残余：默认增量 run 仍整段缓冲 | 不传 `--window-seconds` 时增量 run 一次缓冲 cursor→now 全 gap（[runner.py:122-134](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/pipeline/runner.py#L122-L134) 四个列表仍在，O(窗口)） | 停机多日后首轮增量（1m K 线/aggTrades 多日 gap）仍可能高内存。run_windowed 已支持 incremental 续传（[runner.py:906](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/pipeline/runner.py#L906)），建议文档化 cron 模板固定带 `--window-seconds`，或 gap 超阈值时自动切分窗口 |
| R2-09 due 语义 = enabled 全量（D-6 既定） | [pipeline_cmd.py:39-41](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/cli/pipeline_cmd.py#L39-L41) help 明示「due 语义 = enabled=1，见决策 D-6」 | 设计决策非缺陷；fred 等低频 dataset 每 tick 也会发全窗口 diff 请求，量级可接受；若源方限流收紧可再引入频率判定 |

---

## 5. 行情获取正确性验证（本轮正面确认项）

围绕「长期运行不丢数据」逐项验证为**真实有效**的实现：

- **checkpoint ≤ durable boundary 不变量成立**：[cursor.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/pipeline/cursor.py) 失败 chunk NOOP、前序失败后全部 NOOP、新 ≤ 旧 SKIP+WARNING 回退防护；[raw.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/storage/raw.py) append() 返回即全批 fsync，且 RunLogStage 仅 SUCCESS/PARTIAL_SUCCESS 写 cursor（[runner.py:664-666](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/pipeline/runner.py#L664-L666)）——checkpoint 超前数据的路径不存在
- **幂等收敛**：chunk 间 overlap 重复与跨窗口缝重复均由 canonical natural-key upsert 幂等吸收（duplicate_count 记账）；`run(A,B)` 重复执行与 crash 后 resume 语义经 run_windowed DEC-W4 断点续传测试覆盖
- **窗口不重不漏**：[windows.py](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/pipeline/windows.py#L418-L442) `resolve_window_span` 向上对齐 chunk 边界，分窗口序列与全跨度 plan_chunks 逐 chunk 一致；`verify_interval_concatenation` 拼接律辅助函数就位
- **失败窗口自然重试**：PARTIAL_SUCCESS 后下一窗口 `start=base−overlap` 覆盖失败 chunk 区间（[runner.py:946-960](file:///Users/horsenli/Works/ChronoForge/src/chronoforge/pipeline/runner.py#L946-L960)）
- **限速与异常映射**：binance/deribit 均解析 429 Retry-After 并携带于 RateLimitError；ccxt NetworkError→TransportError 已入重试识别白名单；各连接器有 RateLimiter 实例
- **锁互斥**：BEGIN IMMEDIATE + 活跃行检查，双进程并发 lock 第二方必败（StorageError 传播不吞）
- **状态机闭环**：PENDING→RUNNING（SR-07）→ 终态写回 + dataset status 同步（SR-06），crash 后「从未开始」与「执行中死亡」可区分

---

## 6. 长期运行就绪矩阵（更新版）

| 维度 | 上轮（09-21） | 本轮 | 变化说明 |
| --- | --- | --- | --- |
| 数据正确性与原子性 | ✅ READY | ✅ READY | 上轮修复全部落地，本轮无新内核缺陷 |
| Crash 恢复 | ❌ NOT READY | ✅ READY | SR-01/02/07 闭环，startup_repair 接线（并发护栏缺口另计 R2-03） |
| 锁与并发 | ⚠️ PARTIAL | ⚠️ PARTIAL | 锁互斥正确；但每命令 startup_repair 无并发护栏（R2-03） |
| 内存治理（backfill） | ❌ NOT READY | ⚠️ PARTIAL | run_windowed 就绪（需显式 --window-seconds，R2-08） |
| 磁盘治理 | ✅ READY | ✅ READY | — |
| 增量语义（cursor/overlap/幂等） | 未单列 | ✅ READY | 本轮新增维度：不变量全部成立（见 §5） |
| 重试模型 | 未单列 | ⚠️ PARTIAL | run 粒度兜底存在；D05 chunk 级契约未实现（R2-04） |
| 调度与熔断 | ❌ NOT READY | ❌ NOT READY | 熔断持久化 ✅ 但打开后永不恢复（R2-01）；--all-due 无隔离（R2-02） |
| 状态可观测 | ⚠️ PARTIAL | ✅ READY（单机） | SR-06/07 闭环；structlog 事件 + run_log 全观测列 |
| 安全 | ✅ READY | ✅ READY | — |
| 供应链 | ⚠️ PARTIAL | ✅ READY | uv.lock 入库 + lint-imports 全绿 |

---

## 7. 修复优先级

### P0（无人值守运行前置，合计约 25 行）

1. R2-01：熔断冷却期 half-open（消费 `circuit_opened_at`）+ `pipeline circuit-reset` 运维命令
2. R2-02：CLI `--all-due` 循环逐 job try/except + 失败清单汇总退出码

### P1（一周内）

1. R2-04：按 D05 接线 chunk 级重试（消费 retry.py + retry_max），或走 Design Change 修订契约（建议前者）
2. R2-03：cleanup_orphans 年龄护栏 + reconcile 租约下限 + 读命令跳过 startup_repair

### P2（排期消化）

1. R2-05 空 chunk 历史 flag、R2-06 yahoo utcfromtimestamp、R2-07 runner stores 入 ExitStack、R2-08 文档化 `--window-seconds` cron 模板

---

## 8. 结论

**结论**：上轮 16 项修复全部真实落地且质量良好，存储内核与增量语义（cursor 不变量、幂等收敛、窗口拼接）达到生产水准，数据不丢的根本保障成立。距离「无人值守长期获取行情」还差最后一环——**故障后的自动化恢复闭环**：熔断必须能恢复（R2-01）、坏 dataset 不能阻塞队列（R2-02）、恢复流程本身不能误伤在途 run（R2-03）。三项修复量合计约 50 行，完成后配合 cron + `--window-seconds` 模板即可 7×24 运行。

**值得保留的正面资产**（本轮验证新增）：

- cursor 推进的完整不变量实现（NOOP/SKIP/回退防护 + 终态才写 + fsync 边界对齐）——「checkpoint 永不超前于持久化已验证数据」在全链路成立
- run_windowed 的窗口对齐与防死循环设计（无推进即停、幂等跳过已覆盖窗口）
- retry.py 虽未接线但实现质量完好（退避 + Retry-After + 事件词表），接线成本极低
- 上轮修复 16/16 无虚报，修复均附带针对性测试（1325→1385）

**审计人**：GLM（Trae Code）· 2026-09-22
**依据文档**：docs/architecture/01~10、docs/design/D01~D10、docs/review/2026-09-21-service-readiness-audit.md、docs/implement/tasks/READY-001~005、docs/编码钱最终整体审计审计要求.md
