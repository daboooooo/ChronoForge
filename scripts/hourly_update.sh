#!/usr/bin/env bash
# ChronoForge 数据更新调度（backfill_datasets.py 的前台/外部调度包装）。
#
# 执行模型（2026-11-08 起）：
#   - 每日单一批次：北京时间 15:40（07:40 UTC）执行一次批次更新，
#     随后 16:10（08:10 UTC）生成日报。批次实测约 320s，08:10 留约
#     24 分钟缓冲吸收抖动，保证报告读到当批数据。批次完成状态写入
#     var/run/daily_update.json，日报以该文件为前置条件，不能只靠日历
#     时刻硬碰；超时/失败时报告使用最近一次成功批次并显式标注 as-of。
#   - 每日批次默认只刷新日报输入源（fred/treasury_fiscal/nyfed/yahoo/sosovalue，
#     全量刷新，不做到期过滤）；可用 DAILY_SOURCES 覆盖：
#       · DAILY_SOURCES="fred treasury_fiscal" → 只刷指定源（全量）
#       · DAILY_SOURCES=all → 刷新注册表全部 dataset（--due-only，只补到期，
#         避免对 378 个 CCXT dataset 做全历史回填）
#     原因：全量刷新 378 个 dataset 在 15:40→16:00 的 20 分钟窗口内不可行，
#     且加密交易所网络在当前环境不可达（单 dataset 约 34s 超时）；日报只消费
#     上列输入源，CCXT 等高频数据继续由 loop 负责，能力不被删除。
#   - 高频项保留整点轮询：loop 子命令仍按小时轮询（--due-only），只为
#     1h/4h 等子日频数据集服务（加密行情等）。
#   - 锁模型：loop 的"单例锁"仍为 mkdir + 生命周期持有（防止两个循环
#     并发）；真正的写锁改为【每轮短持有】的 var/run/update-write.lock，
#     loop 每轮、run 每次、daily 整批各自获取，因此单批日报不会被一整
#     小时的循环锁挡在外面。
#   - 每日批次不安装系统服务：可用外部调度器（cron/launchd/CI）在
#     15:40 调用 `hourly_update.sh daily`，也可手动执行；本脚本不自动安装。
#
# 用法:
#   ./scripts/hourly_update.sh daily [透传参数]
#       执行每日全量批次（带超时与状态文件），完成后退出；供 15:40 调度调用。
#   ./scripts/hourly_update.sh report [透传参数]
#       生成当日日报（只读，不获取更新写锁）；供 16:10 调度调用。
#       退出码：0 已生成/非交易日跳过，2 数据降级，3 生成失败。
#   ./scripts/hourly_update.sh [loop] [透传参数]
#       前台常驻：启动立即轮询一次，之后每整点轮询；每轮仅更新到期
#       dataset（--due-only），成功后更新分析图；Ctrl+C 退出。
#   ./scripts/hourly_update.sh run [透传参数]
#       手动执行一轮【全量】更新后退出（补缺口/抓修订）；不写日报状态文件。
#
# 环境变量:
#   INTERVAL_SECONDS       loop 调度周期（默认 3600，按整小时对齐）
#   WINDOW_SECONDS         backfill 窗口（默认 21600 = 6h，READY-006）
#   BATCH_TIMEOUT_SECONDS  daily 批次硬超时（默认 900 = 15 分钟）
#   LOCK_WAIT_SECONDS      等待写锁上限（默认 300 = 5 分钟）
#   DAILY_SOURCES          daily 刷新的数据源（默认日报输入源；all = 全部）
#   UV_BIN                 uv 路径（默认 ~/.local/bin/uv）
#
# 日志:
#   - shell 自身的轮次/调度事件 → var/log/hourly_update.log
#   - daily 批次的 backfill 输出 → var/log/daily_batch.log
#   - 日报生成输出 → var/log/report.log（失败时同样落盘，便于排查）
#   - Python pipeline 日志走 chronoforge 统一方案（CHRONOFORGE_LOG_FILE）：
#     终端直连 TTY → rich 彩色渲染；同时全量 DEBUG 写 JSONL 轮转文件
#     var/log/chronoforge.jsonl（10MB×5）。

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$PROJECT_DIR/var/log"
RUN_DIR="$PROJECT_DIR/var/run"
LOG_FILE="$LOG_DIR/hourly_update.log"
# chronoforge 结构化日志 JSONL 文件 sink（RotatingFileHandler，10MB×5）
export CHRONOFORGE_LOG_FILE="$LOG_DIR/chronoforge.jsonl"
# loop 单例锁：生命周期持有，防止两个循环同时运行（不做写互斥）
LOCK_DIR="$RUN_DIR/hourly-update.lock"
# 写锁：每次更新（loop 每轮 / run / daily 整批）短持有
WRITE_LOCK_DIR="$RUN_DIR/update-write.lock"
# daily 批次完成状态：日报前置条件
DAILY_STATUS_FILE="$RUN_DIR/daily_update.json"
DAILY_BATCH_LOG="$LOG_DIR/daily_batch.log"
REPORT_LOG="$LOG_DIR/report.log"
# READY-006：1h 调度 → 窗口取调度周期 ×6（21600s = 6h）
WINDOW_SECONDS="${WINDOW_SECONDS:-21600}"
# 前台循环调度周期（默认 1h，触发点对齐到周期整点边界）
INTERVAL_SECONDS="${INTERVAL_SECONDS:-3600}"
# daily 批次硬超时：15:40 起算，16:10 报告前留足余量
BATCH_TIMEOUT_SECONDS="${BATCH_TIMEOUT_SECONDS:-900}"
# 等待写锁上限：超过则放弃本轮，避免阻塞报告触发
LOCK_WAIT_SECONDS="${LOCK_WAIT_SECONDS:-300}"
# daily 批次刷新范围：默认日报输入源（全量）；all = 注册表全部 dataset（--due-only）
DAILY_SOURCES="${DAILY_SOURCES:-fred treasury_fiscal nyfed yahoo sosovalue}"

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

# loop 单例锁（生命周期持有）：mkdir + pid，陈旧锁自动清理。
acquire_lock() {
    if mkdir "$LOCK_DIR" 2>/dev/null; then
        echo "$$" > "$LOCK_DIR/pid"
        return 0
    fi
    local pid
    pid="$(cat "$LOCK_DIR/pid" 2>/dev/null || true)"
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        log "跳过：已有更新循环在运行 (pid=$pid)"
        exit 0
    fi
    log "清理陈旧循环锁 (pid=${pid:-unknown})"
    rm -rf "$LOCK_DIR"
    mkdir "$LOCK_DIR"
    echo "$$" > "$LOCK_DIR/pid"
}

release_lock() {
    rm -rf "$LOCK_DIR"
}

# 写锁：每轮/每批短持有，可等待；陈旧锁（持有进程已退出）自动清理。
# macOS 无 flock 命令，沿用项目已有的 mkdir+pid 原子锁语义。
acquire_write_lock() {
    local deadline
    deadline=$(( $(date +%s) + LOCK_WAIT_SECONDS ))
    while :; do
        if mkdir "$WRITE_LOCK_DIR" 2>/dev/null; then
            echo "$$" > "$WRITE_LOCK_DIR/pid"
            return 0
        fi
        local pid
        pid="$(cat "$WRITE_LOCK_DIR/pid" 2>/dev/null || true)"
        if [ -n "$pid" ] && ! kill -0 "$pid" 2>/dev/null; then
            log "清理陈旧写锁 (pid=$pid)"
            rm -rf "$WRITE_LOCK_DIR"
            continue
        fi
        if [ "$(date +%s)" -ge "$deadline" ]; then
            log "获取写锁超时（${LOCK_WAIT_SECONDS}s），已有更新进程 pid=${pid:-unknown}"
            return 1
        fi
        sleep 2
    done
}

release_write_lock() {
    rm -rf "$WRITE_LOCK_DIR"
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
    log "=== 数据更新开始 (window=${WINDOW_SECONDS}s, args: $*) ==="
    local rc=0
    set +e
    # stderr/stdout 直连终端（TTY → rich 彩色）；结构化日志由 Python 侧
    # 自行写 CHRONOFORGE_LOG_FILE（JSONL），不再 tee。
    "$UV_BIN" run python "$PROJECT_DIR/scripts/backfill_datasets.py" \
        --window-seconds "$WINDOW_SECONDS" "$@"
    rc=$?
    set -e
    log "=== 数据更新结束 (rc=$rc) ==="
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

# 写日报状态文件（原子替换：先写临时文件再 mv，避免报告读到半截 JSON）。
# status: ok / partial / timeout / blocked / failed
write_daily_status() {
    local status="$1" rc="$2" started="$3" finished="$4" tmp
    tmp="$DAILY_STATUS_FILE.tmp.$$"
    printf '%s' \
        "{\"schema_version\":\"1.0\",\"batch\":\"daily\",\"started_at\":\"$started\",\"finished_at\":\"$finished\",\"status\":\"$status\",\"exit_code\":$rc,\"sources\":\"${DAILY_SOURCES}\",\"window_seconds\":$WINDOW_SECONDS,\"timeout_seconds\":$BATCH_TIMEOUT_SECONDS}" > "$tmp"
    mv "$tmp" "$DAILY_STATUS_FILE"
}

# TGA 派生重建（canonical DTS → data/derived/tga_daily_balance）。
# 数据更新批次的一部分：重建是幂等增量，实测约 10s；失败只降级为 partial，
# 不阻塞报告（报告会按 derived 的最新日期标注 as-of）。
run_tga_reconstruct() {
    log "--- TGA 派生重建 ---"
    local rc=0
    set +e
    "$UV_BIN" run python "$PROJECT_DIR/scripts/tga_reconstruct.py" --no-report \
        >>"$DAILY_BATCH_LOG" 2>&1
    rc=$?
    set -e
    log "TGA 派生重建结束 (rc=$rc)"
    return "$rc"
}

# daily 批次：全量更新 + 硬超时；结果写入状态文件。
do_daily() {
    require_uv
    mkdir -p "$LOG_DIR" "$RUN_DIR"

    local started finished rc=0 status=ok
    started="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"

    if ! acquire_write_lock; then
        finished="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
        write_daily_status "blocked" 3 "$started" "$finished"
        log "=== daily 批次被写锁阻塞，未执行 (status=blocked) ==="
        exit 3
    fi
    trap release_write_lock EXIT

    log "=== daily 批次开始（范围: ${DAILY_SOURCES}，超时 ${BATCH_TIMEOUT_SECONDS}s）==="
    local -a batch_args=("--window-seconds" "$WINDOW_SECONDS")
    if [ "$DAILY_SOURCES" = "all" ]; then
        # 全部 dataset：只补到期，避免对 378 个 CCXT 做全历史回填
        batch_args+=(--due-only)
    else
        # 默认范围：日报输入源全量刷新（不做到期过滤，确保 16:00 前拿到最新）
        # shellcheck disable=SC2086 — DAILY_SOURCES 允许空格分隔的多个源
        batch_args+=(--sources $DAILY_SOURCES)
    fi
    batch_args+=("$@")
    set +e
    "$UV_BIN" run python "$PROJECT_DIR/scripts/backfill_datasets.py" \
        "${batch_args[@]}" >"$DAILY_BATCH_LOG" 2>&1 &
    local pid=$! waited=0 timed_out=0
    while kill -0 "$pid" 2>/dev/null; do
        sleep 1
        waited=$((waited + 1))
        if [ "$waited" -ge "$BATCH_TIMEOUT_SECONDS" ]; then
            log " daily 批次超时（${BATCH_TIMEOUT_SECONDS}s），终止 pid=$pid"
            kill "$pid" 2>/dev/null || true
            sleep 2
            kill -9 "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
            timed_out=1
            rc=124
            break
        fi
    done
    if [ "$timed_out" -eq 0 ]; then
        wait "$pid"
        rc=$?
    fi
    set -e

    if [ "$timed_out" -eq 1 ]; then
        status="timeout"
    elif [ "$rc" -eq 0 ]; then
        status="ok"
        # TGA 派生属于数据更新（canonical → derived），随批次一起完成；
        # 失败只降级为 partial，报告仍可用旧 derived 并标注 as-of。
        if ! run_tga_reconstruct; then
            status="partial"
            rc=1
            log "TGA 派生重建失败，批次降级为 partial"
        fi
        # 图表失败不改变数据批次状态：状态文件必须写出，否则报告会误判为缺失
        run_plots || log "图表更新有失败项（不影响数据批次状态）"
    elif [ "$rc" -eq 1 ]; then
        status="partial"
        log "daily 批次部分失败 (rc=1)，明细见 $DAILY_BATCH_LOG"
    else
        status="failed"
        log "daily 批次失败 (rc=$rc)，明细见 $DAILY_BATCH_LOG"
    fi

    release_write_lock
    finished="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    write_daily_status "$status" "$rc" "$started" "$finished"
    log "=== daily 批次结束 (status=$status, rc=$rc, 用时 ${waited}s) ==="
    exit "$rc"
}

# 生成日报：只读链路，不获取更新写锁（与 daily/loop 互不阻塞）。
do_report() {
    require_uv
    mkdir -p "$LOG_DIR" "$RUN_DIR"

    log "=== 日报生成开始（只读，不取写锁）==="
    local rc=0
    set +e
    "$UV_BIN" run python "$PROJECT_DIR/scripts/daily_liquidity_report.py" "$@" \
        2>&1 | tee -a "$REPORT_LOG"
    rc=${PIPESTATUS[0]}
    set -e
    log "=== 日报生成结束 (rc=$rc)，日志: $REPORT_LOG ==="
    exit "$rc"
}

# 手动一轮全量：不写日报状态文件。
do_once() {
    require_uv
    mkdir -p "$LOG_DIR" "$RUN_DIR"
    if ! acquire_write_lock; then
        exit 3
    fi
    trap release_write_lock EXIT

    local rc=0
    do_round "$@" || rc=$?
    exit "$rc"
}

do_loop() {
    require_uv
    mkdir -p "$LOG_DIR" "$RUN_DIR"
    acquire_lock
    trap 'release_write_lock; release_lock' EXIT
    trap on_interrupt INT TERM

    local round=0 target
    log "更新循环启动 (interval=${INTERVAL_SECONDS}s, window=${WINDOW_SECONDS}s)"
    while :; do
        round=$((round + 1))
        # 每轮均为到期轮询：启动立即轮询一次，之后每整点轮询；由 Python 侧
        # 按 frequency + last_success_time 判断到期，只更新该更新的 dataset
        log "────── 第 ${round} 轮（到期轮询 --due-only）──────"
        if acquire_write_lock; then
            do_round --due-only "$@" || true
            release_write_lock
        else
            log "跳过第 ${round} 轮：写锁被 daily 批次占用"
        fi
        target=$(next_run_at)
        log "第 ${round} 轮完成，下一次运行: $(date -r "$target" '+%Y-%m-%d %H:%M:%S')"
        countdown_to "$target"
    done
}

case "${1:-loop}" in
    daily)
        shift
        do_daily "$@"
        ;;
    loop)
        shift
        do_loop "$@"
        ;;
    run)
        shift
        do_once "$@"
        ;;
    report)
        shift
        do_report "$@"
        ;;
    *)
        echo "用法: $0 [daily|loop|run|report] [透传给 Python 的额外参数]" >&2
        echo "  daily（15:40 调度）: 全量更新 + 硬超时 + 写日报状态文件" >&2
        echo "  report（16:10 调度）: 生成当日日报（只读，不取写锁）" >&2
        echo "  loop（默认）      : 前台常驻，每小时到期轮询（子日频数据集）" >&2
        echo "  run               : 手动执行一轮全量更新后退出" >&2
        exit 2
        ;;
esac
