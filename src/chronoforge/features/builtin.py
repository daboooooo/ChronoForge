"""内置 P0 特征（D07 §2 表，QUERY-002；需求 §27 子集）。

5 个特征全部声明 dependencies（D07 §2 表为冻结契约，公式列即契约）：

| name | dependencies | 公式 |
|---|---|---|
| returns | {ohlcv} | log(c_t/c_{t-1}) |
| realized_vol | {ohlcv} | rolling std(returns, window) |
| iv_surface | {implied_volatility} | 每 expiry 按 moneyness=k/S 聚合 → ATM/OTM 层级标注 |
| funding_oi_divergence | {funding, open_interest} | Δfunding·ΔOI 符号背离标记 |
| btc_market_stress | 6 项（见类 dependencies） | z-score 加权合成（weights=参数） |

实现决策（How 层，随 QUERY-002.md 决策表入档）：
- 确定性：全路径无 now()/随机源；sort 显式 maintain_order，join 显式
  maintain_order="left"（polars 1.44）
- 取数仅经 QueryService；时间列按 D02 §4 矩阵（FEATURE 链上游 = computed_at）
- IV schema（MODEL-002 冻结）无 strike/expiry 列：自标准化 instrument_id
  （D02 §3 格式）提取，不引入 dependencies 之外的数据集
- Δ/差分基于查询返回序（QueryService 规则 6 稳定排序）；P0 公式作用于
  单市场数据集语义，多市场参数化留待需求演进
"""

from __future__ import annotations

import math
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from typing import Any, ClassVar

import polars as pl

from chronoforge.features.engine import (
    FeatureRegistry,
    FeatureResult,
    QueryServiceLike,
    _BaseFeature,
)

# ── 公共工具 ────────────────────────────────────────────────────────────


def _parse_option_instrument_id(instrument_id: str) -> tuple[date | None, float | None]:
    """从标准化 instrument_id 提取 (expiry, strike)（D02 §3 生成规则的镜像解析）。

    格式 ``{underlying}-{YYYY-MM-DD}-{strike}-{C|P}``（models/reference.py
    parse_deribit 产出的 instrument_id 即此格式，deribit connector 写入 IV 记录）。
    ISO 日期本身含连字符：整串共 6 段（underlying + 3 段日期 + strike + type）。
    段数不符（PERP/现货）或字段非法 → (None, None)。
    """
    parts = instrument_id.split("-")
    if len(parts) != 6:
        return None, None
    try:
        expiry = date.fromisoformat(f"{parts[1]}-{parts[2]}-{parts[3]}")
        strike = float(parts[4])
    except ValueError:
        return None, None
    if parts[5] not in ("C", "P"):
        return None, None
    if not math.isfinite(strike) or strike <= 0:
        return None, None
    return expiry, strike


# ── 1. returns（对数收益率）────────────────────────────────────────────


class ReturnsFeature(_BaseFeature):
    """returns = log(c_t / c_{t-1})（D07 §2）。

    窗口首行（无前一收盘）与 prev_close <= 0 的行 → returns=null。
    """

    name = "returns"
    dependencies = ["ohlcv"]

    def compute(self, q: QueryServiceLike, params: Mapping[str, Any]) -> FeatureResult:
        # OHLCV 时间列 = event_time（D02 §4）；QueryService 规则 6 保证升序稳定
        df = q.query("ohlcv", columns=["event_time", "close"]).frame
        if df.height == 0:
            return self._result(
                pl.DataFrame(
                    schema={
                        "event_time": pl.Datetime("us"),
                        "close": pl.Float64,
                        "returns": pl.Float64,
                    }
                ),
                params,
            )
        prev_close = pl.col("close").shift(1)
        frame = df.with_columns(
            pl.when(prev_close > 0)
            .then((pl.col("close") / prev_close).log())
            .otherwise(pl.lit(None))
            .alias("returns")
        )
        return self._result(frame, params)


# ── 2. realized_vol（实现波动率）───────────────────────────────────────


class RealizedVolFeature(_BaseFeature):
    """realized_vol = rolling std(returns, window)（D07 §2；window 参数缺省 20）。

    前 window 个样本不足的行 → null（polars min_periods=window_size 缺省）。
    """

    name = "realized_vol"
    dependencies = ["ohlcv"]

    def _default_params(self) -> dict[str, Any]:
        return {"window": 20}

    def compute(self, q: QueryServiceLike, params: Mapping[str, Any]) -> FeatureResult:
        window = int(params.get("window", 20))
        if window < 2:
            raise ValueError(f"realized_vol: window 必须 >= 2，得到 {window}")
        df = q.query("ohlcv", columns=["event_time", "close"]).frame
        if df.height == 0:
            return self._result(
                pl.DataFrame(
                    schema={
                        "event_time": pl.Datetime("us"),
                        "close": pl.Float64,
                        "realized_vol": pl.Float64,
                    }
                ),
                {**self._default_params(), "window": window},
            )
        returns = (pl.col("close") / pl.col("close").shift(1)).log()
        frame = df.with_columns(
            returns.rolling_std(window_size=window).alias("realized_vol")
        )
        return self._result(frame, {**self._default_params(), "window": window})


# ── 3. iv_surface（隐含波动率曲面）─────────────────────────────────────


class IVSurfaceFeature(_BaseFeature):
    """iv_surface：每 expiry 按 moneyness=strike/forward 聚合 → ATM/OTM 标注。

    IV schema（MODEL-002 冻结）无 strike/expiry 列：自标准化 instrument_id
    （D02 §3 格式）提取；不可解析行（PERP/外来命名）与 implied_forward/mark_iv
    空值行排除。ATM 带 [0.95, 1.05]（任务单伪代码常量），其余 OTM。
    输出按 (event_time, expiry, data_tier) 聚合：iv_mean=mean(mark_iv)、
    n_options=行数，排序后输出（确定性）。
    """

    name = "iv_surface"
    dependencies = ["implied_volatility"]

    def compute(self, q: QueryServiceLike, params: Mapping[str, Any]) -> FeatureResult:
        df = q.query(
            "implied_volatility",
            columns=["event_time", "instrument_id", "mark_iv", "implied_forward"],
        ).frame
        surface_schema: dict[str, Any] = {
            "event_time": pl.Datetime("us"),
            "expiry": pl.Date,
            "data_tier": pl.String,
            "moneyness": pl.Float64,
            "iv_mean": pl.Float64,
            "n_options": pl.UInt32,
        }
        rows: list[dict[str, Any]] = []
        for row in df.to_dicts():
            expiry, strike = _parse_option_instrument_id(str(row["instrument_id"]))
            forward = row["implied_forward"]
            mark_iv = row["mark_iv"]
            if (
                expiry is None
                or strike is None
                or forward is None
                or forward <= 0
                or mark_iv is None
            ):
                continue
            moneyness = strike / float(forward)
            rows.append(
                {
                    "event_time": row["event_time"],
                    "expiry": expiry,
                    "data_tier": "ATM" if 0.95 <= moneyness <= 1.05 else "OTM",
                    "moneyness": moneyness,
                    "mark_iv": float(mark_iv),
                }
            )
        if not rows:
            return self._result(pl.DataFrame(schema=surface_schema), params)
        frame = (
            pl.DataFrame(rows)
            .group_by(["event_time", "expiry", "data_tier"])
            .agg(
                pl.col("mark_iv").mean().alias("iv_mean"),
                pl.len().alias("n_options"),
            )
            .sort(["event_time", "expiry", "data_tier"], maintain_order=True)
        )
        return self._result(frame, params)


# ── 4. funding_oi_divergence（资金费率-OI 背离）────────────────────────


class FundingOIDivergenceFeature(_BaseFeature):
    """funding_oi_divergence：Δfunding·ΔOI 符号背离标记（D07 §2）。

    Δ 基于各数据集查询返回序（QueryService 规则 6 稳定排序）后 join（内连接
    对齐 event_time）。信号：product < 0 → DIVERGENT（符号背离）；否则
    CONVERGENT；Δ 窗口首行为 null 或两侧时间不齐 → divergence_signal/signal
    为 null（不标记）。
    """

    name = "funding_oi_divergence"
    dependencies = ["funding", "open_interest"]

    def compute(self, q: QueryServiceLike, params: Mapping[str, Any]) -> FeatureResult:
        out_schema: dict[str, Any] = {
            "event_time": pl.Datetime("us"),
            "funding_rate": pl.Float64,
            "open_interest": pl.Float64,
            "divergence_signal": pl.Float64,
            "signal": pl.String,
        }
        funding_df = q.query("funding", columns=["event_time", "funding_rate"]).frame
        oi_df = q.query("open_interest", columns=["event_time", "open_interest"]).frame
        if funding_df.height == 0 or oi_df.height == 0:
            return self._result(pl.DataFrame(schema=out_schema), params)

        joined = (
            funding_df.with_columns(pl.col("funding_rate").diff().alias("delta_funding"))
            .join(
                oi_df.with_columns(pl.col("open_interest").diff().alias("delta_oi")),
                on="event_time",
                how="inner",
                maintain_order="left",
            )
            .with_columns(
                (pl.col("delta_funding") * pl.col("delta_oi")).alias("divergence_signal")
            )
            .with_columns(
                pl.when(pl.col("divergence_signal").is_null())
                .then(pl.lit(None))
                .when(pl.col("divergence_signal") < 0)
                .then(pl.lit("DIVERGENT"))
                .otherwise(pl.lit("CONVERGENT"))
                .alias("signal")
            )
        )
        frame = joined.select(
            ["event_time", "funding_rate", "open_interest", "divergence_signal", "signal"]
        ).sort("event_time", maintain_order=True)
        return self._result(frame, params)


# ── 5. btc_market_stress（BTC 市场压力指数）────────────────────────────


@dataclass(frozen=True)
class _StressComponent:
    """btc_market_stress 组成项规格（数据集/值列/时间列/权重参数；D02 §4 时间列）。"""

    dataset_id: str
    value_col: str
    time_col: str
    weight_param: str
    default_weight: float
    filter: Mapping[str, str] | None = None  # FEATURE 链上游按 name 过滤


class BTCMarketStressFeature(_BaseFeature):
    """btc_market_stress：六分量 z-score 加权合成（D07 §2；需求 §27）。

    分量值列（MODEL-002 schema；realized_vol 为 FEATURE 链上游，走 feature
    视图 value 列 + name 过滤，时间列 computed_at）：

    ================  ==========================  ==============================
    dataset_id        值列                         时间列
    ================  ==========================  ==============================
    realized_vol      value（name=realized_vol）  computed_at（D02 §4 FEATURE）
    funding           funding_rate                event_time
    open_interest     open_interest               event_time
    liquidation_agg.  total_notional              event_time
    implied_volatility mark_iv                    event_time
    prediction_price  price                       event_time
    ================  ==========================  ==============================

    合成：z_i = (x_i - mean_i) / std_i（样本 std，polars 缺省 ddof=1；std<=0
    或空数据分量整体剔除）；网格 = 各分量事件时间的并集（同刻多观测先按
    event_time 均值聚合，如 IV 多 instrument），分量在该时刻无数据 → null →
    行级可用分量归一：stress = Σ(w_i·z_i) / Σ(w_i 非空 z 分量)；无可用 z
    贡献的行剔除。全分量空数据 → 空帧（schema 保持）。权重经 params
    （w_vol/w_funding/w_oi/w_liq/w_iv/w_pred），负权重 ValueError。
    """

    name = "btc_market_stress"
    dependencies = [
        "realized_vol",
        "funding",
        "open_interest",
        "liquidation_aggregate",
        "implied_volatility",
        "prediction_price",
    ]

    _COMPONENTS: ClassVar[tuple[_StressComponent, ...]] = (
        _StressComponent(
            "realized_vol", "value", "computed_at", "w_vol", 0.3,
            filter={"name": "realized_vol"},
        ),
        _StressComponent("funding", "funding_rate", "event_time", "w_funding", 0.2),
        _StressComponent("open_interest", "open_interest", "event_time", "w_oi", 0.15),
        _StressComponent(
            "liquidation_aggregate", "total_notional", "event_time", "w_liq", 0.2
        ),
        _StressComponent("implied_volatility", "mark_iv", "event_time", "w_iv", 0.1),
        _StressComponent("prediction_price", "price", "event_time", "w_pred", 0.05),
    )

    def _default_params(self) -> dict[str, Any]:
        return {c.weight_param: c.default_weight for c in self._COMPONENTS}

    def compute(self, q: QueryServiceLike, params: Mapping[str, Any]) -> FeatureResult:
        weights = {
            c.weight_param: float(params.get(c.weight_param, c.default_weight))
            for c in self._COMPONENTS
        }
        if any(w < 0 for w in weights.values()):
            raise ValueError("btc_market_stress: 权重必须 >= 0")

        # 1. 逐分量取数（时间列各异；空数据/视图未注册 → 剔除该分量）
        series: dict[str, pl.DataFrame] = {}
        for c in self._COMPONENTS:
            df = q.query(
                c.dataset_id, columns=[c.time_col, c.value_col], filters=c.filter
            ).frame
            if df.height == 0:
                continue
            # 同一 event_time 多观测（如 IV 多 instrument）→ 均值聚合（确定性）
            series[c.dataset_id] = (
                df.select(
                    pl.col(c.time_col).alias("event_time"),
                    pl.col(c.value_col).alias(c.dataset_id),
                )
                .group_by("event_time")
                .agg(pl.col(c.dataset_id).mean())
                .sort("event_time")
            )

        empty_schema: dict[str, Any] = {"event_time": pl.Datetime("us")}
        for c in self._COMPONENTS:
            empty_schema[f"{c.dataset_id}_z"] = pl.Float64
        empty_schema["btc_market_stress"] = pl.Float64

        if not series:
            return self._result(
                pl.DataFrame(schema=empty_schema), {**self._default_params(), **weights}
            )

        # 2. 网格 = 各分量事件时间并集（unique + sort），逐分量 left join
        # （该时刻无数据的分量 → null → 行级归一剔除其贡献）
        grid = (
            pl.concat([s.select("event_time") for s in series.values()])
            .unique()
            .sort("event_time")
        )
        wide = grid
        for c in self._COMPONENTS:
            if c.dataset_id in series:
                wide = wide.join(
                    series[c.dataset_id], on="event_time", how="left", maintain_order="left"
                )

        # 3. z-score（std<=0 → null 剔除）+ 行级可用分量加权合成
        ordered = [c for c in self._COMPONENTS if c.dataset_id in series]
        z_exprs = [
            pl.when(pl.col(c.dataset_id).std() > 0)
            .then(
                (pl.col(c.dataset_id) - pl.col(c.dataset_id).mean())
                / pl.col(c.dataset_id).std()
            )
            .otherwise(pl.lit(None))
            .alias(f"{c.dataset_id}_z")
            for c in ordered
        ]
        numerator = sum(
            (
                weights[c.weight_param] * pl.col(f"{c.dataset_id}_z").fill_null(0.0)
                for c in ordered
            ),
            pl.lit(0.0),
        )
        denominator = sum(
            (
                weights[c.weight_param]
                * pl.col(f"{c.dataset_id}_z").is_not_null().cast(pl.Float64)
                for c in ordered
            ),
            pl.lit(0.0),
        )
        frame = (
            wide.with_columns(z_exprs)
            .with_columns(
                pl.when(denominator > 0)
                .then(numerator / denominator)
                .otherwise(pl.lit(None))
                .alias("btc_market_stress")
            )
            .filter(pl.col("btc_market_stress").is_not_null())
            .select(
                [
                    "event_time",
                    *[f"{c.dataset_id}_z" for c in ordered],
                    "btc_market_stress",
                ]
            )
            .sort("event_time", maintain_order=True)
        )
        return self._result(frame, {**self._default_params(), **weights})


# ── 默认注册中心 ───────────────────────────────────────────────────────


def create_default_registry(connection: sqlite3.Connection | None = None) -> FeatureRegistry:
    """内置特征注册中心（5 个 P0 特征，D07 §2 表顺序注册）。

    Args:
        connection: 透传给各特征的 SQLite 连接（register() 需要；仅 compute
            时可为 None）。
    """
    registry = FeatureRegistry()
    for feature in (
        ReturnsFeature(connection),
        RealizedVolFeature(connection),
        IVSurfaceFeature(connection),
        FundingOIDivergenceFeature(connection),
        BTCMarketStressFeature(connection),
    ):
        registry.register(feature)
    return registry
