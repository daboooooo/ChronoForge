#!/usr/bin/env python3
"""比特币(BTC)价格与比特币ETF基金流向(EFT Fund Flow)每日叠加对比图。

数据来源：本地 ChronoForge 数据库（只读）
    - BTC 日频收盘价   : 比特币（橙色线，左轴；直读 canonical OHLCV 分区）
    - ETF 日频净流入    : 全市场比特币ETF每日净申购/赎回（下方柱状图，
      正值=净流入绿色，负值=净流出红色；来自 SoSoValue 聚合数据）

理论依据：ETF 资金流向反映机构资金对比特币的配置意愿——
持续净流入支撑价格，大规模净流出往往伴随价格回调。

输出：
    1. 终端：最新净流入/流出、累计净流入、流入/流出日统计
    2. PNG 叠加图（默认 btc_etf_flow.png，--output 可改）

用法:
    python scripts/plot_btc_etf_flow.py                  # 默认近 2 年
    python scripts/plot_btc_etf_flow.py --years 1        # 近 1 年
    python scripts/plot_btc_etf_flow.py --start 2024-01-01
    python scripts/plot_btc_etf_flow.py --show           # 存图后弹窗
"""

from __future__ import annotations

import argparse
import contextlib
from datetime import datetime, timedelta
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

from chronoforge.cli._wiring import open_meta, open_query_service  # noqa: E402
from chronoforge.config.settings import Settings  # noqa: E402

# ── 常量 ─────────────────────────────────────────────────────────────
DATASET_BTC_EFFR = "fred_series_EFFR"
BTC_ENTITY = "CCXT-BINANCE:BTCUSDT:USDT:SPOT"

# ETF 聚合净流入（全市场汇总）
ETF_ENTITY = "etf_us_btc_summary_total_net_inflow"

# 颜色（与 plot_rates_2y_effr.py 一致）
BG = "#0b0e14"
GRID = "#1f2733"
WHITE = "#ffffff"
BLUE = "#3d9bff"
ORANGE = "#f7931a"
INFLOW = "#3ddc97"   # 净流入 = 绿色
OUTFLOW = "#ff5c5c"  # 净流出 = 红色
TEXT = "#c9d4e3"

# 分析参数
MOM_WINDOW = 30  # 动量观察窗（交易日）

# 显示参数
SHOW_EFFR = False  # 是否在主图中叠加显示 EFFR


# ── 数据读取 ─────────────────────────────────────────────────────────
def load_btc(data_dir: Path, start: datetime | None) -> pd.Series:
    """直读 BTC 日频 canonical 分区（只读，绕过无法注册的全局 ohlcv 视图）。"""
    import duckdb

    pattern = f"{data_dir}/canonical/OHLCV/entity={BTC_ENTITY}/**/*.parquet"
    con = duckdb.connect()
    try:
        df = con.execute(
            f"SELECT event_time, close FROM read_parquet('{pattern}', "
            "hive_partitioning=1) WHERE event_time >= ? ORDER BY event_time",
            [start],
        ).pl()
    finally:
        con.close()
    if df.is_empty():
        raise SystemExit("本地数据库中 BTC 日频数据为空（请先 backfill）")
    pdf = df.to_pandas()
    s = pdf.set_index(pd.to_datetime(pdf["event_time"]))["close"].sort_index()
    s.index = s.index.normalize()
    return s.astype(float)


def load_etf_flow(service: object, start: datetime | None) -> pd.Series:
    """经只读 QueryService 读取 ETF 日频净流入。"""
    result = service.query(  # type: ignore[attr-defined]
        "sosovalue_etf_us_btc_summary_total_net_inflow",
        columns=["observation_time", "value", "revision_time"],
        start=start,
        filters={"entity": ETF_ENTITY},
    )
    df = result.frame.to_pandas()
    if df.empty:
        raise SystemExit("本地数据库中 ETF 净流入数据为空（请先 backfill）")
    # 同一观测日多版本时保留 revision_time 最新的一行
    df = df.sort_values("revision_time").drop_duplicates(
        "observation_time", keep="last"
    )
    s = df.set_index(pd.to_datetime(df["observation_time"]))["value"].sort_index()
    s.index = s.index.normalize()
    return s.astype(float)


def load_effr(service: object, start: datetime | None) -> pd.Series:
    """读取 EFFR（用于标注联邦基金利率，辅助判断宏观环境）。"""
    result = service.query(  # type: ignore[attr-defined]
        DATASET_BTC_EFFR,
        columns=["observation_time", "value", "revision_time"],
        start=start,
        filters={"entity": "EFFR"},
    )
    df = result.frame.to_pandas()
    if df.empty:
        return pd.Series(dtype=float)
    df = df.sort_values("revision_time").drop_duplicates(
        "observation_time", keep="last"
    )
    s = df.set_index(pd.to_datetime(df["observation_time"]))["value"].sort_index()
    s.index = s.index.normalize()
    return s.astype(float)


def build_frame(start: datetime | None) -> pd.DataFrame:
    """对齐三条序列：BTC 与 ETF 流按日对齐。"""
    settings = Settings.load()
    meta = open_meta(settings)
    with contextlib.ExitStack() as stack:
        stack.callback(meta.close)
        service, con = open_query_service(settings, meta)
        stack.callback(con.close)

        btc = load_btc(settings.data_dir, start).rename("btc")
        etf_flow = load_etf_flow(service, start).rename("etf_flow")

        frame = pd.DataFrame({"btc": btc, "etf_flow": etf_flow})
        frame = frame.dropna()
        frame.index = pd.DatetimeIndex(frame.index).normalize()

        # 可选：在主图中叠加显示 EFFR
        if SHOW_EFFR:
            effr = load_effr(service, start)
            if not effr.empty:
                frame["effr"] = effr.reindex(frame.index).ffill()

    return frame


# ── 分析 ─────────────────────────────────────────────────────────────
def flow_stats(flow: pd.Series) -> dict:
    """统计 ETF 净流入/流出的基本统计量。"""
    total_days = len(flow)
    inflow_days = int((flow > 0).sum())
    outflow_days = int((flow < 0).sum())
    net_inflow_days = int((flow >= 0).sum())

    total_net = float(flow.sum())
    avg_inflow = float(flow[flow > 0].mean()) if inflow_days > 0 else 0.0
    avg_outflow = float(flow[flow < 0].mean()) if outflow_days > 0 else 0.0

    # 累计净流入（从最早到有最新）
    cumulative = float(flow.cumsum().iloc[-1])

    # 连续净流入/流出最长天数
    max_consecutive_in = _max_consecutive(flow > 0)
    max_consecutive_out = _max_consecutive(flow < 0)

    return {
        "total_days": total_days,
        "inflow_days": inflow_days,
        "outflow_days": outflow_days,
        "net_inflow_days": net_inflow_days,
        "total_net_usd": total_net,
        "avg_inflow_usd": avg_inflow,
        "avg_outflow_usd": avg_outflow,
        "cumulative_net_usd": cumulative,
        "max_consecutive_in": max_consecutive_in,
        "max_consecutive_out": max_consecutive_out,
    }


def _max_consecutive(series: pd.Series) -> int:
    """计算布尔序列的最大连续 True 天数。"""
    if not series.any():
        return 0
    groups = (series != series.shift()).cumsum()
    counts = groups[series].value_counts()
    return int(counts.max())


def correlation_analysis(frame: pd.DataFrame) -> dict:
    """BTC 日收益率与 ETF 净流入的相关性。"""
    btc_ret = frame["btc"].pct_change().dropna()
    flow = frame["etf_flow"]
    common = pd.DataFrame({"ret": btc_ret, "flow": flow}).dropna()
    if len(common) < 10:
        return {"corr_30d": 0.0, "corr_all": 0.0}
    corr_all = float(common["ret"].corr(common["flow"]))
    if len(common) >= MOM_WINDOW:
        corr_30 = float(
            common["ret"].iloc[-MOM_WINDOW:].corr(
                common["flow"].iloc[-MOM_WINDOW:]
            )
        )
    else:
        corr_30 = 0.0
    return {"corr_30d": round(corr_30, 3), "corr_all": round(corr_all, 3)}


# ── 绘图 ─────────────────────────────────────────────────────────────
def plot(frame: pd.DataFrame, stats: dict, corr: dict, output: Path) -> None:
    plt.rcParams["font.sans-serif"] = [
        "PingFang SC", "Heiti SC", "Arial Unicode MS", "DejaVu Sans",
    ]
    plt.rcParams["axes.unicode_minus"] = False

    has_effr = "effr" in frame.columns

    fig, (ax1, ax2, ax3) = plt.subplots(
        3, 1, figsize=(14, 11), height_ratios=[3.2, 1.3, 1.3], sharex=True,
        gridspec_kw={"hspace": 0.05},
    )

    fig.patch.set_facecolor(BG)
    axes = [ax1, ax2, ax3]

    for a in axes:
        a.set_facecolor(BG)
        for spine in a.spines.values():
            spine.set_color(GRID)
        a.tick_params(colors=TEXT, labelsize=10)
        a.grid(color=GRID, lw=0.7, alpha=0.7)

    # ── 上图：BTC 价格 ──
    line_btc, = ax1.plot(frame.index, frame["btc"], color=ORANGE, lw=2.0,
                         label="BTC 收盘价 (USD)")
    ax1.set_ylabel("BTC 价格 USD", color=ORANGE, fontsize=11)
    ax1.tick_params(colors=ORANGE, labelsize=10)

    # BTC 最新值标注
    last_d = frame.index[-1]
    ax1.annotate(f"${frame['btc'].iloc[-1]:,.0f}",
                 xy=(last_d, frame["btc"].iloc[-1]),
                 xytext=(8, 0), textcoords="offset points",
                 color=ORANGE, fontsize=11, fontweight="bold")

    # 可选：EFFR 叠加（右轴）
    if has_effr and not frame["effr"].isna().all():
        ax1b = ax1.twinx()
        ax1b.set_facecolor(BG)
        for spine in ax1b.spines.values():
            spine.set_color(GRID)
        line_effr, = ax1b.plot(frame.index, frame["effr"], color=BLUE, lw=1.5,
                               alpha=0.7, label="EFFR (%)")
        ax1b.set_ylabel("EFFR (%)", color=BLUE, fontsize=10)
        ax1b.tick_params(colors=BLUE, labelsize=10)
        ax1b.annotate(f"{frame['effr'].iloc[-1]:.2f}%",
                      xy=(last_d, frame["effr"].iloc[-1]),
                      xytext=(8, 0), textcoords="offset points", color=BLUE,
                      fontsize=10, va="center", fontweight="bold")
        ax1.legend([line_btc, line_effr],
                   [line_btc.get_label(), line_effr.get_label()],
                   loc="upper left", edgecolor=GRID,
                   labelcolor=TEXT, fontsize=10, frameon=False)
        for txt in ax1.legend().get_texts():
            txt.set_color(TEXT)
    else:
        ax1.legend([line_btc], [line_btc.get_label()],
                   loc="upper left", edgecolor=GRID,
                   labelcolor=TEXT, fontsize=10, frameon=False)

    ax1.set_title("比特币价格 vs ETF 基金流向", color=TEXT,
                  fontsize=15, pad=28, fontweight="bold")
    ax1.set_ylim(frame["btc"].min() * 0.95, frame["btc"].max() * 1.05)

    # ── 背景高亮：ETF 日净流入 > $900M 或 < -$600M（数据单位为 USD） ──
    def highlight_extreme(ax, inflow_dates, outflow_dates,
                          color_in="#ffd700", color_out="#ff6b6b"):
        """在指定 axes 上按日期范围添加背景高亮。"""
        for d in inflow_dates:
            ax.axvspan(d, d + timedelta(days=1), color=color_in, alpha=0.18)
        for d in outflow_dates:
            ax.axvspan(d, d + timedelta(days=1), color=color_out, alpha=0.18)

    INFLOW_THRESHOLD = 900_000_000      # 9 亿美元
    OUTFLOW_THRESHOLD = -600_000_000    # 6 亿美元
    large_inflow = frame.index[frame["etf_flow"] > INFLOW_THRESHOLD]
    large_outflow = frame.index[frame["etf_flow"] < OUTFLOW_THRESHOLD]

    # 高亮所有子图（先设置背景再添加高亮）
    ax1.set_facecolor(BG)
    ax2.set_facecolor(BG)
    ax3.set_facecolor(BG)
    for sp in ax2.spines.values():
        sp.set_color(GRID)
    ax2.tick_params(colors=TEXT, labelsize=10)
    ax2.grid(color=GRID, lw=0.7, alpha=0.7)
    highlight_extreme(ax1, large_inflow, large_outflow)
    highlight_extreme(ax2, large_inflow, large_outflow)
    highlight_extreme(ax3, large_inflow, large_outflow)

    # ── 中图：ETF 每日净流入/流出柱状图 ──
    colors = [INFLOW if v >= 0 else OUTFLOW for v in frame["etf_flow"]]
    ax2.bar(frame.index, frame["etf_flow"], color=colors, width=1.0,
            edgecolor="none", alpha=0.85)
    ax2.axhline(0, color=GRID, lw=1)
    ax2.set_ylabel("ETF 净流入 (USD)", color=TEXT, fontsize=10)
    ax2.annotate(f"${frame['etf_flow'].iloc[-1]:,.0f}",
                 xy=(last_d, frame["etf_flow"].iloc[-1]),
                 xytext=(8, 0), textcoords="offset points",
                 color=INFLOW if frame["etf_flow"].iloc[-1] >= 0 else OUTFLOW,
                 fontsize=10, va="center")

    # ── 下图：累计净流入 ──
    ax3.fill_between(frame.index, frame["etf_flow"].cumsum(), 0,
                     color="#a06bff", alpha=0.25)
    ax3.plot(frame.index, frame["etf_flow"].cumsum(), color="#a06bff", lw=1.5)
    ax3.axhline(0, color=GRID, lw=1)
    ax3.set_ylabel("累计净流入 (USD)", color=TEXT, fontsize=10)
    ax3.annotate(f"${stats['cumulative_net_usd']:,.0f}",
                 xy=(last_d, frame["etf_flow"].cumsum().iloc[-1]),
                 xytext=(8, 0), textcoords="offset points",
                 color="#c9b3ff", fontsize=10, va="center")

    # X 轴格式化
    for a in [ax2, ax3]:
        a.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
        a.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        a.set_xlim(frame.index[0], frame.index[-1] + timedelta(days=20))

    fig.savefig(output, facecolor=BG, bbox_inches="tight", dpi=150)
    plt.close(fig)


# ── 终端报告 ─────────────────────────────────────────────────────────
def report(frame: pd.DataFrame, stats: dict, corr: dict, output: Path) -> None:
    last = frame.iloc[-1]
    line = "=" * 68
    print(f"\n{line}\nBTC / ETF Flow 截至 {frame.index[-1]:%Y-%m-%d}\n{line}")
    print(f"  BTC = ${last['btc']:,.0f}")

    flow_val = last["etf_flow"]
    direction = "流入" if flow_val >= 0 else "流出"
    print(f"  当日 ETF {direction} = ${abs(flow_val):,.0f}")

    print(f"\n{line}\nETF 基金流向统计\n{line}")
    print(f"  总交易日数       : {stats['total_days']}")
    pct_in = stats['inflow_days'] / max(stats['total_days'], 1)
    pct_out = stats['outflow_days'] / max(stats['total_days'], 1)
    print(f"  净流入交易日     : {stats['inflow_days']} ({pct_in:.0%})")
    print(f"  净流出交易日     : {stats['outflow_days']} ({pct_out:.0%})")
    print(f"  累计净流入       : ${stats['cumulative_net_usd']:,.0f}")
    print(f"  平均净流入日     : ${stats['avg_inflow_usd']:,.0f}")
    print(f"  平均净流出日     : ${stats['avg_outflow_usd']:,.0f}")
    print(f"  最长连续净流入   : {stats['max_consecutive_in']} 个交易日")
    print(f"  最长连续净流出   : {stats['max_consecutive_out']} 个交易日")

    print(f"\n{line}\nBTC 日收益 vs ETF 净流入相关性\n{line}")
    print(f"  全样本相关系数   : {corr['corr_all']:+.3f}")
    print(f"  近{MOM_WINDOW}交易日相关系数 : {corr['corr_30d']:+.3f}")
    print(f"\n图表已保存：{output.resolve()}\n")


# ── CLI ──────────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--years", type=float, default=2.0,
                   help="回看年数（默认 2）")
    p.add_argument("--start", type=str, default=None,
                   help="起始日期 YYYY-MM-DD")
    p.add_argument("--output", type=str, default="./btc_etf_flow.png",
                   help="输出 PNG 路径")
    p.add_argument("--show", action="store_true",
                   help="存图后弹窗显示")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if args.start:
        start = datetime.fromisoformat(args.start)
    else:
        start = datetime.now() - timedelta(days=int(args.years * 365))

    frame = build_frame(start)
    if len(frame) < 10:
        raise SystemExit(f"有效数据仅 {len(frame)} 行，不足以分析")

    stats = flow_stats(frame["etf_flow"])
    corr = correlation_analysis(frame)

    output = Path(args.output)
    plot(frame, stats, corr, output)
    report(frame, stats, corr, output)

    if args.show:
        import os
        os.system(f'open "{output}"')


if __name__ == "__main__":
    main()
