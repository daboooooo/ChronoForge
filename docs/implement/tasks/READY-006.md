# READY-006 — 采集调度 cron 模板文档化（`--window-seconds` 固定带上）

## 派发信息

- 任务单：2026-09-22 采集就绪审计 R2-08（P2，SR-04/READY-001 残余）
- 设计依据：D05 §2（run_windowed 方案 B）、READY-001（DEC-W4 断点续传）、架构 08 §2（重试/熔断）
- Agent：Orchestrator 主会话直执（沿用 READY-002~005 惯例）
- 派发时间：2026-09-22
- 状态：DONE（2026-09-22 验收通过）
- 依赖：READY-001（run_windowed + CLI `--window-seconds`，DONE）

## 审计证据（R2-08 原文要点）

- 不传 `--window-seconds` 时增量 run 一次缓冲 cursor→now 全 gap（runner FetchStage 流式缓冲列表 O(gap)）：停机多日后首轮增量（1m K 线 / aggTrades 多日 gap）仍可能高内存。
- 审计建议：**文档化 cron 模板固定带 `--window-seconds`**，或 gap 超阈值时自动切分窗口。

## file_ownership

- `docs/implement/tasks/READY-006.md`（本任务单，含模板交付物）
- `docs/implement/01-dispatch-log.md`（登记）

## 候选方案（执行前设计对比，已择一实施）

| 方案 | 内容 | 代价 | 裁决 |
| --- | --- | --- | --- |
| A. cron 模板文档化 | 调度模板固定带 `--window-seconds`，内存上界由运维显式选择 | 零代码风险；依赖模板被遵循 | **采纳**（审计首选；run_windowed 已支持 incremental 续传，无需新代码） |
| B. gap 超阈值自动切分窗口 | FetchStage 前探测 gap 跨度，超阈值自动走 run_windowed | 隐式改变 run 语义：单次命令产生 N 行 run_log（READY-001 已列明的语义变化）在调用方无感知下发生；阈值本身成为新配置面 | 拒：调度频率是运维语义（窗口大小 = 重启容忍度 × 内存预算），交配置模板显式表达优于隐式魔法；如需后续可立独立任务 |

约束核对：`run_windowed` 对 incremental 任务已支持（cursor 起点续传，DEC-W4）✓；`--all-due` 与 `--window-seconds` 可组合（CLI 逐 dataset 传参）✓；熔断冷却期 half-open（R2-01）下 cron 高频重试安全（冷却期内 CANCELLED 快速返回，不打源）✓。

## 交付物：采集调度 cron 模板

```cron
# ── ChronoForge 采集调度模板（crontab -e）────────────────────────────
# 凭证与环境：.env（CHRONOFORGE_*，见 .env.example）；cron 无登录 shell，
# PATH/HOME 需显式（uv 通常在 ~/.local/bin 或 ~/.cargo/bin）。
#
# --window-seconds（READY-001 内存治理，必带）：
#   单次 run 只处理一个时间窗（秒，向上对齐 chunk 边界），窗口间经
#   checkpoint 推进续传 → 单 run 内存 O(窗口)，与停机天数无关。
#   停机多日后首轮增量不传该参数会整段缓冲 cursor→now 全 gap。
#   取值建议 ≈ 调度周期 × 2~4（如 5m 调度 → 3600；1h 调度 → 21600），
#   需 ≥ dataset chunk_size（diff 类 fred/sec 为 full_window 不拆分，传了也单 run）。
#
# 熔断（R2-01）：连续 3 次 FAILED → 打开；冷却期（默认 1800s，可配
#   CHRONOFORGE_CIRCUIT_COOLDOWN_S）内 run 直接 CANCELLED 不打源；
#   冷却期满后自动半开放行一次探测 run。数据源维护结束可即时恢复：
#   uv run chronoforge pipeline circuit-reset --dataset <dataset_id>
# 观测：uv run chronoforge pipeline status --last 20

# 5m 高频行情（binance/deribit 1m K 线、aggTrades、funding 等）
*/5 * * * *  cd /path/to/ChronoForge && ~/.local/bin/uv run chronoforge \
  pipeline run --all-due --window-seconds 3600 >> var/log/cron.log 2>&1

# 1h 中频（yahoo/fred/sec/cftc 等日级源；fred/sec 为 full_window 自动单 run）
0 * * * *  cd /path/to/ChronoForge && ~/.local/bin/uv run chronoforge \
  pipeline run --all-due --window-seconds 21600 >> var/log/cron.log 2>&1
```

要点：

1. `--window-seconds` 固定携带——消除 R2-08 指出的「默认增量整段缓冲」残余风险（运维显式选择内存上界）。
2. 模板注明冷却期/熔断自动恢复语义，cron 高频调度下无需额外守护。
3. `--all-due` 具备逐 dataset 异常隔离（R2-02）：单 dataset 失败汇总至 stderr、退出码 1，不阻塞队列。

## acceptance（GWT）

- [x] Given 运维按模板部署 cron When 停机多日后首轮增量 run Then 单 run 只处理一个时间窗，内存 O(窗口) 不随 gap 增长（依据 READY-001 `test_ready001_peak_memory_bounded_by_window` 既有断言；模板层面闭环，无新代码）
- [x] Given 模板文档 When 审阅 Then 覆盖窗口取值建议、full_window 例外、熔断冷却/半开/手工复位、观测入口（本任务单「交付物」节）

## DoD 逐项核对（howto §43）

- [x] 实现（模板交付物入档，零代码变更）
- [x] 公共 API（无变化）
- [x] 数据契约（无变化）
- [x] 错误处理（无变化）
- [x] 日志（无变化）
- [x] 指标（无变化）
- [x] 单测（无新增——文档任务；内存上界由 READY-001 既有测试背书）
- [x] 边界（full_window 类不拆分、停机多日 gap、熔断打开时 cron 高频重试安全均已注明）
- [x] 失败（不适用）
- [x] 恢复（circuit-reset 运维入口注明）
- [x] 集成测试（全量回归随本轮审计修复统一执行：1399 passed / 0 failed）
- [x] 静态分析（随本轮统一执行：ruff All checks passed）
- [x] 类型检查（随本轮统一执行：mypy Success，72 文件）
- [x] 无未声明假设（方案 A/B 对比与裁决入档）
- [x] 验收通过（GWT 2/2）

## Deferred Acceptance

（无——方案 B（gap 超阈值自动切分）已裁决拒绝并记录理由；若后续源方限流收紧再立独立任务。）
