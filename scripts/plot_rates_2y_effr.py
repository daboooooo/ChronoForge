#!/usr/bin/env python3
"""2年期美债收益率(DGS2)、美联储隔夜利率(EFFR) 与比特币(BTC) 叠加对比与前瞻判断。

数据来源：本地 ChronoForge 数据库（只读）
    - fred_series_DGS2 : 2年期美国国债收益率（白色线，市场定价）
    - fred_series_EFFR : 有效联邦基金利率（蓝色线，美联储政策）
    - BTC 日频收盘价    : 比特币（橙色线，右轴；直读 canonical OHLCV
      分区，因全局 ohlcv 视图受历史 hive 分区不一致影响无法注册）

理论依据：2Y 收益率对货币政策预期高度敏感——市场先在 2Y 上定价加息/降息，
FOMC 随后在议息会议上落地。历史上表现为「白色先动、蓝色跟随」。

输出：
    1. 终端：最新利差、2Y 动量、滞后相关（量化领先天数）、历次转向的领先验证、
       以及下次议息会议 hold/hike/cut 的启发式概率
    2. PNG 叠加图（默认 rate_2y_effr.png，--output 可改）

注意：概率为基于「未被政策兑现的 2Y 变动」的启发式软投票，非联邦基金利率期货
隐含概率，仅供研究参考。

用法:
    python scripts/plot_rates_2y_effr.py                 # 默认近 3 年
    python scripts/plot_rates_2y_effr.py --years 2       # 近 2 年
    python scripts/plot_rates_2y_effr.py --start 2022-01-01
    python scripts/plot_rates_2y_effr.py --show          # 存图后弹窗
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
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from chronoforge.cli._wiring import open_meta, open_query_service  # noqa: E402
from chronoforge.config.settings import Settings  # noqa: E402

# ── 常量 ─────────────────────────────────────────────────────────────
DATASET_2Y = "fred_series_DGS2"
DATASET_EFFR = "fred_series_EFFR"
BP = 100.0  # 1% = 100bp
POLICY_STEP_BP = 25.0  # 一次标准议息动作 = 25bp
MOVE_MIN_BP = 20.0  # |EFFR 日变动| >= 20bp 计为一次政策动作（过滤 ±1bp 噪声）
MOM_WINDOW = 30  # 短期动量观察窗（交易日）
LEAD_MAX_LAG = 60  # 领先-滞后相关最大滞后（交易日）
LEAD_WINDOWS = (15, 30, 60, 90)  # 历次政策动作前 DGS2 提前走向的观察窗（交易日）

BG = "#0b0e14"
GRID = "#1f2733"
WHITE = "#ffffff"
BLUE = "#3d9bff"
ORANGE = "#f7931a"
HIKE = "#ff5c5c"
CUT = "#3ddc97"
TEXT = "#c9d4e3"

# BTC 日频收盘价所在 hive 分区（CCXT-BINANCE 日频，干净的 1 行/日）
BTC_ENTITY = "CCXT-BINANCE:BTCUSDT:USDT:SPOT"


# ── 数据读取 ─────────────────────────────────────────────────────────
def load_series(service: object, dataset_id: str, entity_id: str,
                start: datetime | None) -> pd.Series:
    """经只读 QueryService 读取单个 FRED 序列。

    NUMBER 视图是全部 entity 的并集，必须按 hive 分区列 entity 过滤，
    否则会混入 ETF 资产等其他 NUMBER 数据集。
    """
    result = service.query(  # type: ignore[attr-defined]
        dataset_id,
        columns=["observation_time", "value", "revision_time"],
        start=start,
        filters={"entity": entity_id},
    )
    df = result.frame.to_pandas()
    if df.empty:
        raise SystemExit(f"本地数据库中 {dataset_id} 无数据（请先 backfill）")
    # 同一观测日多版本时保留 revision_time 最新的一行
    df = df.sort_values("revision_time").drop_duplicates(
        "observation_time", keep="last"
    )
    s = df.set_index(pd.to_datetime(df["observation_time"]))["value"].sort_index()
    s.index = s.index.normalize()
    return s.astype(float)


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


def build_frame(start: datetime | None) -> pd.DataFrame:
    """对齐三条序列：利率内连接定基准日，BTC 按日对齐（缺失日前值填充）。"""
    settings = Settings.load()
    meta = open_meta(settings)  # 读命令：不做启动修复
    with contextlib.ExitStack() as stack:
        stack.callback(meta.close)
        service, con = open_query_service(settings, meta)
        stack.callback(con.close)
        dgs2 = load_series(service, DATASET_2Y, "DGS2", start).rename("dgs2")
        effr = load_series(service, DATASET_EFFR, "EFFR", start).rename("effr")
        btc = load_btc(settings.data_dir, start).rename("btc")

    frame = pd.concat([dgs2, effr], axis=1, join="inner").dropna()
    # BTC 每日均有收盘；按利率交易日对齐，极端缺失日前值填充
    frame["btc"] = btc.reindex(frame.index).ffill()
    frame = frame.dropna(subset=["btc"])
    frame["spread"] = frame["dgs2"] - frame["effr"]
    return frame


# ── 分析 ─────────────────────────────────────────────────────────────
def policy_moves(effr: pd.Series) -> pd.DataFrame:
    """识别政策动作日（|ΔEFFR| >= MOVE_MIN_BP）。"""
    delta = effr.diff()
    out = pd.DataFrame(
        {
            "date": delta.index,
            "delta_bp": (delta * BP).round(0),
            "rate_after": effr.values,
        }
    )
    return out[out["delta_bp"].abs() >= MOVE_MIN_BP].reset_index(drop=True)


def best_lead_lag(frame: pd.DataFrame) -> tuple[int, float]:
    """DGS2 变动领先 EFFR 变动的最佳滞后（交易日）及相关系数。

    corr(DGS2_t, EFFR_{t+lag})，lag=0..LEAD_MAX_LAG。EFFR 仅在议息日
    跳变（样本窗内仅约 8 个动作日），日变动相关被噪声稀释，故采用
    5 个交易日滚动累计变动。
    """
    chg = frame[["dgs2", "effr"]].diff().rolling(5).sum().dropna()
    x = chg["dgs2"].to_numpy()
    y = chg["effr"].to_numpy()
    best_lag, best_corr = 0, 0.0
    for lag in range(0, LEAD_MAX_LAG + 1):
        if len(x) - lag < 30:
            break
        c = float(np.corrcoef(x[: len(x) - lag], y[lag:])[0, 1])
        if c > best_corr:
            best_corr, best_lag = c, lag
    return best_lag, best_corr


def historical_lead_check(moves: pd.DataFrame, frame: pd.DataFrame) -> pd.DataFrame:
    """历次政策动作前 N 个交易日 DGS2 的提前变动，验证「白线先动」。

    对 LEAD_WINDOWS 中每个窗口（15/30/60/90 交易日），统计动作生效前一日
    相对窗口起点的 DGS2 变动（bp）以及方向是否与政策动作同向，全部列
    编入同一张表（lead{w}_bp / ok{w}）。样本起点不足时窗口自动钳制。
    """
    rows = []
    dgs2 = frame["dgs2"]
    for _, m in moves.iterrows():
        d = m["date"]
        pos = dgs2.index.get_indexer([d], method="nearest")[0]
        if pos < 1:
            continue
        now = dgs2.iloc[pos - 1]  # 动作生效前一日
        row: dict = {
            "date": d.strftime("%Y-%m-%d"),
            "effr_bp": int(m["delta_bp"]),
        }
        for w in LEAD_WINDOWS:
            prev = dgs2.iloc[max(0, pos - w - 1)]
            lead_bp = round((now - prev) * BP, 0)
            row[f"lead{w}_bp"] = int(lead_bp)
            # 持平（0bp）记为不可判定，不计入同向率
            row[f"ok{w}"] = (
                (m["delta_bp"] > 0) == (lead_bp > 0) if lead_bp != 0 else False
            )
            row[f"flat{w}"] = lead_bp == 0
        rows.append(row)
    return pd.DataFrame(rows)


def next_meeting_probabilities(frame: pd.DataFrame, moves: pd.DataFrame) -> dict:
    """启发式三分类概率：下次议息会议 hold / hike / cut。

    核心信号 s（bp）= 尚未被 EFFR 兑现的 2Y 定价变动，取两项均值：
      s_mom  : 近 30 个交易日 DGS2 变动
      s_gap  : 最近一次政策动作以来 DGS2 变动 − 该次动作幅度
    每 25bp ≈ 一次标准动作；以 s/25 为对数几率做 softmax 三分类：
    hold 基线对数几率 1；同向方向按幅度加分，反向方向按 0.8 倍幅度减分
    （强单向信号下，对立结果的概率应被压到个位数）。
    """
    dgs2 = frame["dgs2"]
    s_mom = (dgs2.iloc[-1] - dgs2.iloc[-1 - MOM_WINDOW]) * BP

    if not moves.empty:
        last = moves.iloc[-1]
        since = dgs2[dgs2.index > last["date"]]
        if len(since) >= 2:
            s_gap = (since.iloc[-1] - dgs2.asof(last["date"])) * BP - last["delta_bp"]
        else:
            s_gap = 0.0
    else:
        s_gap = s_mom

    signal = 0.5 * s_mom + 0.5 * s_gap
    up = max(0.0, signal) / POLICY_STEP_BP
    down = max(0.0, -signal) / POLICY_STEP_BP
    logits = np.array([1.0, up - 0.8 * down, down - 0.8 * up])
    probs = np.exp(logits - logits.max())
    probs /= probs.sum()
    return {
        "s_mom_bp": round(s_mom, 1),
        "s_gap_bp": round(s_gap, 1),
        "signal_bp": round(signal, 1),
        "hold": probs[0],
        "hike": probs[1],
        "cut": probs[2],
    }


# ── 绘图 ─────────────────────────────────────────────────────────────
def plot(frame: pd.DataFrame, moves: pd.DataFrame, probs: dict, output: Path) -> None:
    plt.rcParams["font.sans-serif"] = [
        "PingFang SC", "Heiti SC", "Arial Unicode MS", "DejaVu Sans",
    ]
    plt.rcParams["axes.unicode_minus"] = False

    fig, (ax, axb) = plt.subplots(
        2, 1, figsize=(14, 9), height_ratios=[3.2, 1], sharex=True,
        gridspec_kw={"hspace": 0.05},
    )
    fig.patch.set_facecolor(BG)
    for a in (ax, axb):
        a.set_facecolor(BG)
        for spine in a.spines.values():
            spine.set_color(GRID)
        a.tick_params(colors=TEXT, labelsize=10)
        a.grid(color=GRID, lw=0.7, alpha=0.7)

    # 政策动作日竖线 + EFFR 上的标记
    for _, m in moves.iterrows():
        color = HIKE if m["delta_bp"] > 0 else CUT
        ax.axvline(m["date"], color=color, lw=0.8, ls="--", alpha=0.35)
        ax.scatter(
            m["date"], m["rate_after"], marker="^" if m["delta_bp"] > 0 else "v",
            s=70, color=color, zorder=5,
        )

    line_dgs2, = ax.plot(frame.index, frame["dgs2"], color=WHITE, lw=2.0,
                         label="2年期美债收益率 DGS2（市场先定价）")
    line_effr, = ax.plot(frame.index, frame["effr"], color=BLUE, lw=2.0,
                         label="有效联邦基金利率 EFFR（美联储后跟随）")

    # BTC：数量级（数万美元）与利率差异大，用独立右轴
    ax2 = ax.twinx()
    ax2.set_facecolor(BG)
    for spine in ax2.spines.values():
        spine.set_color(GRID)
    line_btc, = ax2.plot(frame.index, frame["btc"], color=ORANGE, lw=1.8,
                         alpha=0.9, label="比特币 BTC 收盘价（右轴）")
    ax2.tick_params(colors=ORANGE, labelsize=10)
    ax2.set_ylabel("BTC 价格 USD", color=ORANGE, fontsize=11)
    btc_min, btc_max = frame["btc"].min(), frame["btc"].max()
    ax2.set_ylim(btc_min * 0.97, btc_max * 1.05)

    # 右端最新值标注
    last_d = frame.index[-1]
    for col, color in (("dgs2", WHITE), ("effr", BLUE)):
        ax.annotate(f"{frame[col].iloc[-1]:.2f}%", xy=(last_d, frame[col].iloc[-1]),
                    xytext=(8, 0), textcoords="offset points", color=color,
                    fontsize=11, va="center", fontweight="bold")
    ax2.annotate(f"${frame['btc'].iloc[-1]:,.0f}",
                 xy=(last_d, frame["btc"].iloc[-1]),
                 xytext=(8, 0), textcoords="offset points", color=ORANGE,
                 fontsize=11, va="center", fontweight="bold")

    title = (f"下次议息概率  按兵不动 {probs['hold']:.0%} │ "
             f"加息 {probs['hike']:.0%} │ 降息 {probs['cut']:.0%}")
    ax.set_title("2年期美债收益率 vs 美联储隔夜利率 vs 比特币", color=TEXT,
                 fontsize=15, pad=28, fontweight="bold")
    ax.text(0.5, 1.02, title, transform=ax.transAxes, ha="center",
            color=TEXT, fontsize=11)
    ax.set_ylabel("利率 %", color=TEXT, fontsize=11)
    leg = ax.legend(
        [line_dgs2, line_effr, line_btc],
        [line_dgs2.get_label(), line_effr.get_label(), line_btc.get_label()],
        loc="upper left", facecolor=BG, edgecolor=GRID,
        labelcolor=TEXT, fontsize=10,
    )
    for txt in leg.get_texts():
        txt.set_color(TEXT)
    ax.set_ylim(frame[["dgs2", "effr"]].min().min() - 0.4,
                frame[["dgs2", "effr"]].max().max() + 0.6)

    # 下方面板：利差
    axb.fill_between(frame.index, frame["spread"], 0,
                     color="#a06bff", alpha=0.25)
    axb.plot(frame.index, frame["spread"], color="#a06bff", lw=1.2)
    axb.axhline(0, color=GRID, lw=1)
    axb.set_ylabel("利差\nDGS2−EFFR %", color=TEXT, fontsize=10)
    axb.annotate(f"{frame['spread'].iloc[-1]:+.2f}%",
                 xy=(last_d, frame["spread"].iloc[-1]),
                 xytext=(8, 0), textcoords="offset points",
                 color="#c9b3ff", fontsize=10, va="center")

    axb.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    axb.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    axb.set_xlim(frame.index[0], frame.index[-1] + timedelta(days=20))

    fig.savefig(output, facecolor=BG, bbox_inches="tight", dpi=150)
    plt.close(fig)


# ── 终端报告 ─────────────────────────────────────────────────────────
def _lead_cell(lead_bp: int, aligned: bool, flat: bool) -> str:
    """单窗口统计单元格：↗️涨 / ↘️跌 / ↔️持平，✅同向 / ❌背离 / ⚪不可判定。

    固定 11 个终端显示列，配合 3 空格分隔形成 14 列等宽槽位。
    """
    if flat:
        return "↔️   0bp ⚪"
    trend = "↗️" if lead_bp > 0 else "↘️"
    mark = "✅" if aligned else "❌"
    return f"{trend}{lead_bp:>+4}bp {mark}"


def report(frame: pd.DataFrame, lead: tuple[int, float],
           hist: pd.DataFrame, probs: dict, output: Path) -> None:
    last = frame.iloc[-1]
    cal_days = int(round(lead[0] * 365 / 252))
    line = "=" * 68
    print(f"\n{line}\nDGS2(2Y) / EFFR(美联储隔夜) / BTC 截至 {frame.index[-1]:%Y-%m-%d}\n{line}")
    print(f"  DGS2 = {last['dgs2']:.2f}%   EFFR = {last['effr']:.2f}%"
          f"   利差 = {last['spread']:+.2f}%")
    btc_30d = (last["btc"] / frame["btc"].iloc[-1 - MOM_WINDOW] - 1)
    print(f"  BTC = ${last['btc']:,.0f}（近{MOM_WINDOW}个交易日 {btc_30d:+.1%}）")
    signal_icon = ("↗️" if probs["signal_bp"] > 0
                   else "↘️" if probs["signal_bp"] < 0 else "↔️")
    print(f"  近{MOM_WINDOW}个交易日 2Y 变动 = {probs['s_mom_bp']:+.0f}bp"
          f" | 上次动作后未兑现部分 = {probs['s_gap_bp']:+.0f}bp"
          f" | 综合信号 {signal_icon} {probs['signal_bp']:+.0f}bp")
    # BTC 日收益率与 DGS2 日变动的相关性（理论上利率上行压制风险资产 → 负相关）
    chg = frame[["dgs2", "btc"]].pct_change().dropna()
    corr_30 = chg.iloc[-MOM_WINDOW:].corr().iloc[0, 1]
    corr_all = chg.corr().iloc[0, 1]
    print(f"  领先-滞后分析：DGS2 日变动领先 EFFR 约 {lead[0]} 个交易日"
          f"（≈{cal_days} 个自然日），最大相关系数 {lead[1]:.2f}")
    print(f"  BTC 日收益 vs DGS2 日变动相关：近窗 {corr_30:+.2f}"
          f" | 全样本 {corr_all:+.2f}")

    win_label = "/".join(str(w) for w in LEAD_WINDOWS)
    print(f"\n{line}\n历史验证：政策动作前 DGS2 的提前走向"
          f"（{win_label} 个交易日）\n{line}")
    if not hist.empty:
        # 统一表格：每个观察窗占 14 个显示列（单元格 11 列 + 3 列间距）
        head_slots = "".join("    " + f"前{w}日" + "    " for w in LEAD_WINDOWS)
        print("  日期" + " " * 9 + "政策动作" + " " * 8 + "│" + head_slots)
        print("  " + "─" * 70)
        for _, r in hist.iterrows():
            hike = r["effr_bp"] > 0
            action = f"{' + 加息' if hike else ' - 降息'} {abs(r['effr_bp']):>3}bp"
            cells = "   ".join(
                _lead_cell(r[f"lead{w}_bp"], r[f"ok{w}"], r[f"flat{w}"])
                for w in LEAD_WINDOWS
            )
            print(f"  {r['date']}   {action}   │ {cells}")
        # 各窗口同向率（0bp 持平不可判定，剔除出分母）
        rate_slots = []
        for w in LEAD_WINDOWS:
            valid = ~hist[f"flat{w}"]
            if valid.any():
                ok = int(hist.loc[valid, f"ok{w}"].sum())
                n = int(valid.sum())
                rate_slots.append(f"{ok / n:.0%}({ok}/{n})")
            else:
                rate_slots.append("—")
        print("  同向率" + " " * 23 + "│ "
              + "   ".join(f"{s:<11}" for s in rate_slots))
        print("  图例：↗️ DGS2 上行  ↘️ 下行  ↔️ 持平  "
              "✅ 与政策动作同向  ❌ 背离  ⚪ 不可判定")

    print(f"\n{line}\n下次议息会议概率（启发式，非利率期货隐含）\n{line}")
    outcomes = [
        (" ✋", "不变    ", "hold"),
        (" + ", "加息    ", "hike"),
        (" - ", "降息    ", "cut"),
    ]
    top_key = max(("hold", "hike", "cut"), key=lambda k: probs[k])
    for icon, label, key in outcomes:
        star = "⭐" if key == top_key else "  "
        print(f"  {star} {icon} {label}: {probs[key]:.1%}")
    print(f"\n图表已保存：{output.resolve()}\n")


# ── CLI ──────────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--years", type=float, default=3.0, help="回看年数（默认 3）")
    p.add_argument("--start", type=str, default=None, help="起始日期 YYYY-MM-DD")
    p.add_argument("--output", type=str, default="rate_2y_effr.png", help="输出 PNG 路径")
    p.add_argument("--show", action="store_true", help="存图后弹窗显示")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if args.start:
        start = datetime.fromisoformat(args.start)
    else:
        start = datetime.now() - timedelta(days=int(args.years * 365))

    frame = build_frame(start)
    if len(frame) < MOM_WINDOW + 5:
        raise SystemExit(f"有效数据仅 {len(frame)} 行，不足以分析")

    moves = policy_moves(frame["effr"])
    lead = best_lead_lag(frame)
    hist = historical_lead_check(moves, frame)
    probs = next_meeting_probabilities(frame, moves)

    output = Path(args.output)
    plot(frame, moves, probs, output)
    report(frame, lead, hist, probs, output)

    if args.show:
        import os
        os.system(f'open "{output}"')


if __name__ == "__main__":
    main()
