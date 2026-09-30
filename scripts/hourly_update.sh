#!/usr/bin/env bash
# ChronoForge 每小时数据更新（backfill_datasets.py 的前台手动调度包装）。
#
# 设计要点：
#   - 不安装任何系统服务（无 launchd/cron），直接在终端前台常驻运行；
#     等待期间在终端原地刷新倒计时，提示下一次运行时间，Ctrl+C 退出。
#   - 每轮固定带 --window-seconds（READY-006 内存治理）：1h 调度取 21600（6h
#     窗口），单轮内存 O(窗口)，与停机天数无关；可用 WINDOW_SECONDS 覆盖。
#   - backfill 基于 checkpoint 续传：已覆盖窗口直接 COVERED 跳过，每小时执行
#     默认起点即等价增量更新，同时自动回补缺口，无需指定 --start。
#   - 启动即轮询、此后每整点轮询，每轮只更新"该更新"的 dataset：轮询时按
#     时间结构（frequency）+ 最后更新时刻判断是否到期，到期才尝试更新
#     （--due-only）——
#       · 1h：每小时整点更新（以 UTC 00:00 为原点的整点网格）；
#       · 4h：以 UTC 00:00 为原点，00/04/08/12/16/20 时更新；
#       · 1d 及以上：按数据源发布时刻更新（config/update_schedule.py：
#         FRED 北京 16:10、Binance 北京 08:00、其余北京 00:00），避免源
#         未发布时抓取导致数据滞后一天。
#     周期取 registry frequency，最后更新取 checkpoints.last_success_time；
#     从未成功（新注册/待补历史）或周期未知（含 tick）视为到期每轮更新，
#     失败/熔断 dataset 因 last_success 不推进而保持到期，冷却期内仍快速
#     CANCELLED 不打源。
#     手动 run 子命令为全量更新（人工显式调用，含补缺口/抓修订）。
#   - 熔断冷却期内 run 快速 CANCELLED 不打源、冷却后自动半开探测，故此处
#     刻意不带 --reset-circuit（否则定时清熔断等于失去保护）。
#   - 每轮 backfill 成功（rc=0）后，自动执行 scripts/plot_*.py 更新分析图；
#     单个图表失败仅告警，不影响下一轮调度。
#   - mkdir 互斥锁：循环运行期间全程持锁，其他手动调用直接跳过，绝不并发重入。
#
# 用法:
#   ./scripts/hourly_update.sh [loop] [透传给 backfill_datasets.py 的额外参数]
#       前台常驻（默认）：启动立即轮询一次，之后每整点轮询；每轮仅更新
#       到期（该更新）的 dataset，成功后更新分析图，循环往复；Ctrl+C 退出。
#       额外参数透传，如 --dry-run
#   ./scripts/hourly_update.sh run [透传给 backfill_datasets.py 的额外参数]
#       只执行一轮【全量】（更新+图表）后退出，额外参数透传，如 --dry-run
#
# 环境变量:
#   INTERVAL_SECONDS  循环调度周期（默认 3600，按整小时对齐触发）
#   WINDOW_SECONDS    backfill 窗口（默认 21600 = 6h）
#   UV_BIN            uv 路径（默认 ~/.local/bin/uv）
#
# 日志:
#   - shell 自身的轮次/调度事件 → var/log/hourly_update.log
#   - Python pipeline 日志走 chronoforge 统一方案（CHRONOFORGE_LOG_FILE）：
#     终端直连 TTY → rich 彩色渲染；同时全量 DEBUG 写 JSONL 轮转文件
#     var/log/chronoforge.jsonl（10MB×5）。不再经 tee 落盘，避免日志文件
#     混入 ANSI 控制符、管道下终端退化为 JSON。
#   - 倒计时仅显示在终端，不写入任何日志。

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$PROJECT_DIR/var/log"
RUN_DIR="$PROJECT_DIR/var/run"
LOG_FILE="$LOG_DIR/hourly_update.log"
# chronoforge 结构化日志 JSONL 文件 sink（RotatingFileHandler，10MB×5）
export CHRONOFORGE_LOG_FILE="$LOG_DIR/chronoforge.jsonl"
LOCK_DIR="$RUN_DIR/hourly-update.lock"
# READY-006：1h 调度 → 窗口取调度周期 ×6（21600s = 6h）
WINDOW_SECONDS="${WINDOW_SECONDS:-21600}"
# 前台循环调度周期（默认 1h，触发点对齐到周期整点边界）
INTERVAL_SECONDS="${INTERVAL_SECONDS:-3600}"

# 手动终端下也保证从项目根目录运行：uv 找得到 pyproject，图表 PNG 落在项目根
cd "$PROJECT_DIR"

UV_BIN="${UV_BIN:-$HOME/.local/bin/uv}"
if [ ! -x "$UV_BIN" ]; then
    UV_BIN="$(command -v uv || true)"
fi

log() {
    printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" | tee -a "$LOG_FILE" >/dev/null
}

require_uv() {
    if [ -z "$UV_BIN" ]; then
        echo "错误: 找不到 uv，请设置 UV_BIN 环境变量或安装 uv" >&2
        exit 1
    fi
}

acquire_lock() {
    if mkdir "$LOCK_DIR" 2>/dev/null; then
        echo "$$" > "$LOCK_DIR/pid"
        return 0
    fi
    local pid
    pid="$(cat "$LOCK_DIR/pid" 2>/dev/null || true)"
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        log "跳过：已有更新进程在运行 (pid=$pid)"
        exit 0
    fi
    log "清理陈旧锁 (pid=${pid:-unknown})"
    rm -rf "$LOCK_DIR"
    mkdir "$LOCK_DIR"
    echo "$$" > "$LOCK_DIR/pid"
}

release_lock() {
    rm -rf "$LOCK_DIR"
}

# INT/TERM 处理：直接追加写日志（信号上下文中避免再起管道），随后退出；
# 真正的清锁由 EXIT trap 兜底，任何退出路径都不会残留锁。
on_interrupt() {
    printf '%s 收到中断信号，退出调度\n' "$(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
    exit 130
}

# 执行一轮 backfill；成功（rc=0）后更新全部 plot_*.py 分析图。
# 返回 0 表示更新与图表均成功，非 0 时调用方决定退出还是继续调度。
do_round() {
    log "=== hourly update 开始 (window=${WINDOW_SECONDS}s, args: $*) ==="
    local rc=0
    set +e
    # stderr/stdout 直连终端（TTY → rich 彩色）；结构化日志由 Python 侧
    # 自行写 CHRONOFORGE_LOG_FILE（JSONL），不再 tee。
    "$UV_BIN" run python "$PROJECT_DIR/scripts/backfill_datasets.py" \
        --window-seconds "$WINDOW_SECONDS" "$@"
    rc=$?
    set -e
    log "=== hourly update 结束 (rc=$rc) ==="
    if [ "$rc" -ne 0 ]; then
        log "更新失败 (rc=$rc)，跳过图表更新"
        return "$rc"
    fi
    run_plots
}

# 依次执行 scripts/plot_*.py（自主发现，新增 plot 脚本无需改本文件）。
# 图表输出为相对路径，依赖外层已 cd 到 PROJECT_DIR，PNG 落在项目根目录。
run_plots() {
    local shopt_was_off=0
    shopt -q nullglob || shopt_was_off=1
    shopt -s nullglob
    local plots=("$PROJECT_DIR"/scripts/plot_*.py)
    [ "$shopt_was_off" -eq 1 ] && shopt -u nullglob

    if [ "${#plots[@]}" -eq 0 ]; then
        log "未找到 scripts/plot_*.py，跳过图表更新"
        return 0
    fi

    local failed=0 plot prc
    for plot in "${plots[@]}"; do
        log "--- 更新图表: $(basename "$plot") ---"
        set +e
        "$UV_BIN" run python "$plot"
        prc=$?
        set -e
        if [ "$prc" -ne 0 ]; then
            log "警告: 图表脚本 $(basename "$plot") 失败 (rc=$prc)"
            failed=1
        fi
    done
    if [ "$failed" -eq 0 ]; then
        log "全部图表更新完成 (${#plots[@]} 个)"
    fi
    return "$failed"
}

# 计算下一个调度整点（epoch 对齐到 INTERVAL_SECONDS 边界）。
next_run_at() {
    local now
    now=$(date +%s)
    echo $(( (now / INTERVAL_SECONDS + 1) * INTERVAL_SECONDS ))
}

# 原地刷新倒计时到目标 epoch；只写终端（\r 刷新），不入日志。
# sleep 后台化 + wait：收到 INT/TERM 时 wait 立即被中断执行 trap（前台
# sleep 写法下 bash 需等 sleep 跑完才响应，最多滞後 1s）。
countdown_to() {
    local target="$1" now remain h m s
    while :; do
        now=$(date +%s)
        remain=$((target - now))
        [ "$remain" -le 0 ] && break
        h=$((remain / 3600))
        m=$(((remain % 3600) / 60))
        s=$((remain % 60))
        printf '\r下一次运行 %s │ 剩余 %02d:%02d:%02d │ Ctrl+C 退出   ' \
            "$(date -r "$target" '+%Y-%m-%d %H:%M:%S')" "$h" "$m" "$s"
        sleep 1 &
        wait $! 2>/dev/null || true
    done
    printf '%*s\r' 72 ""
}

do_once() {
    require_uv
    mkdir -p "$LOG_DIR" "$RUN_DIR"
    acquire_lock
    trap release_lock EXIT

    local rc=0
    do_round "$@" || rc=$?
    exit "$rc"
}

do_loop() {
    require_uv
    mkdir -p "$LOG_DIR" "$RUN_DIR"
    acquire_lock
    trap release_lock EXIT
    trap on_interrupt INT TERM

    local round=0 target
    log "hourly update 前台调度启动 (interval=${INTERVAL_SECONDS}s, window=${WINDOW_SECONDS}s)"
    while :; do
        round=$((round + 1))
        # 每轮均为到期轮询：启动立即轮询一次，之后每整点轮询；由 Python 侧
        # 按 frequency + last_success_time 判断到期，只更新该更新的 dataset
        log "────── 第 ${round} 轮（到期轮询 --due-only）──────"
        do_round --due-only "$@" || true
        target=$(next_run_at)
        log "第 ${round} 轮完成，下一次运行: $(date -r "$target" '+%Y-%m-%d %H:%M:%S')"
        countdown_to "$target"
    done
}

case "${1:-loop}" in
    loop)
        shift
        do_loop "$@"
        ;;
    run)
        shift
        do_once "$@"
        ;;
    *)
        echo "用法: $0 [loop|run] [透传给 backfill_datasets.py 的额外参数，如 --dry-run]" >&2
        echo "  loop（默认）: 前台常驻，立即执行一轮后倒计时到下一整点循环" >&2
        echo "  run        : 只执行一轮（更新+图表）后退出" >&2
        exit 2
        ;;
esac
