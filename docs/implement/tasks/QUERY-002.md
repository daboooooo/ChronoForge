# QUERY-002 — FeatureEngine 协议 + 5 个 P0 特征

## 派发信息

- 任务单：D10 §7 QUERY-002
- 设计依据：D07 §2（特征定义）
- Agent：待定
- 派发时间：2026-09-12
- 状态：READY
- 依赖：QUERY-001（QueryService）、STORAGE-003（DerivedStore 布局）

## file_ownership

- `src/chronoforge/features/engine.py`（新建，FeatureEngine 协议 + 注册中心）
- `src/chronoforge/features/builtin.py`（新建，5 个 P0 特征实现）
- `tests/integration/test_features.py`（新建）

## 交付物契约摘要

### FeatureEngine Protocol（features/engine.py）

```python
class FeatureEngine(Protocol):
    name: str
    dependencies: list[str]  # dataset_id 列表（声明式，架构 03 §7）

    def compute(self, q: QueryService, params: Mapping[str, Any]) -> pl.DataFrame: ...
    def register(self) -> None: ...  # 写 dataset_registry（FEATURE 类型）+ 版本
```

### FeatureRegistry 类（features/engine.py）

```python
class FeatureRegistry:
    """特征注册中心（单例）"""

    def __init__(self):
        self._features: dict[str, FeatureEngine] = {}

    def register(self, feature: FeatureEngine) -> None:
        if feature.name in self._features:
            raise ValueError(f"Duplicate feature: {feature.name}")
        self._features[feature.name] = feature

    def get(self, name: str) -> FeatureEngine | None:
        return self._features.get(name)

    def all_features(self) -> list[FeatureEngine]:
        return list(self._features.values())

    def compute(self, name: str, q: QueryService, params: Mapping[str, Any]) -> pl.DataFrame:
        feature = self._features.get(name)
        if not feature:
            raise ValueError(f"Unknown feature: {name}")
        return feature.compute(q, params)
```

### P0 特征实现（features/builtin.py）

#### 1. returns（对数收益率）

```python
class ReturnsFeature:
    name = "returns"
    dependencies = ["ohlcv"]

    def compute(self, q: QueryService, params: Mapping[str, Any]) -> pl.DataFrame:
        """
        returns = log(c_t / c_{t-1})
        """
        df = q.query(dataset_id="ohlcv", columns=["event_time", "close"]).frame

        # 计算 log return
        close = df["close"]
        prev_close = close.shift(1)
        returns = pl.when(prev_close > 0).then(pl.log(close / prev_close)).otherwise(None)

        return df.with_columns(returns.alias("returns"))

    def register(self) -> None:
        ...
```

#### 2. realized_vol（实现波动率）

```python
class RealizedVolFeature:
    name = "realized_vol"
    dependencies = ["ohlcv"]

    def compute(self, q: QueryService, params: Mapping[str, Any]) -> pl.DataFrame:
        """
        realized_vol = rolling std(returns, window)
        """
        window = int(params.get("window", 20))

        df = q.query(dataset_id="ohlcv", columns=["event_time", "close"]).frame
        close = df["close"]
        returns = pl.log(close / close.shift(1))

        vol = returns.rolling_std(window=window)

        return df.with_columns(vol.alias("realized_vol"))

    def register(self) -> None:
        ...
```

#### 3. iv_surface（隐含波动率曲面）

```python
class IVSurfaceFeature:
    name = "iv_surface"
    dependencies = ["implied_volatility"]

    def compute(self, q: QueryService, params: Mapping[str, Any]) -> pl.DataFrame:
        """
        iv_surface: 每 expiry 按 moneyness=k/S 聚合 → ATM/OTM 层级标注
        """
        df = q.query(dataset_id="implied_volatility", columns=[
            "event_time", "strike", "mark_iv", "implied_forward",
        ]).frame

        # 计算 moneyness = strike / forward
        iv_surface = df.with_columns(
            (pl.col("strike") / pl.col("implied_forward")).alias("moneyness"),
            pl.when((pl.col("moneyness") >= 0.95) & (pl.col("moneyness") <= 1.05))
              .then("ATM")
              .otherwise("OTM").alias("data_tier"),
        )

        return iv_surface

    def register(self) -> None:
        ...
```

#### 4. funding_oi_divergence（资金费率-OI 背离）

```python
class FundingOIDivergenceFeature:
    name = "funding_oi_divergence"
    dependencies = ["funding", "open_interest"]

    def compute(self, q: QueryService, params: Mapping[str, Any]) -> pl.DataFrame:
        """
        Δfunding · ΔOI 符号背离标记
        """
        funding_df = q.query(dataset_id="funding", columns=["event_time", "funding_rate"]).frame
        oi_df = q.query(dataset_id="open_interest", columns=["event_time", "open_interest"]).frame

        # 计算变化量
        funding_df = funding_df.with_columns(
            pl.col("funding_rate").diff().alias("delta_funding"),
        )
        oi_df = oi_df.with_columns(
            pl.col("open_interest").diff().alias("delta_oi"),
        )

        # 符号背离：Δfunding 和 ΔOI 符号相反
        result = funding_df.join(oi_df, on="event_time", how="inner").with_columns(
            (pl.col("delta_funding") * pl.col("delta_oi")).alias("divergence_signal"),
            pl.when(pl.col("divergence_signal") < 0).then("DIVERGENT")
              .otherwise("CONVERGENT").alias("signal"),
        )

        return result.select(["event_time", "funding_rate", "open_interest", "divergence_signal", "signal"])

    def register(self) -> None:
        ...
```

#### 5. btc_market_stress（BTC 市场压力指数）

```python
class BTCMarketStressFeature:
    name = "btc_market_stress"
    dependencies = [
        "realized_vol", "funding", "open_interest",
        "liquidation_aggregate", "implied_volatility", "prediction_price",
    ]

    def compute(self, q: QueryService, params: Mapping[str, Any]) -> pl.DataFrame:
        """
        z-score 加权合成（D07 §2 表）
        """
        weights = {
            "realized_vol": float(params.get("w_vol", 0.3)),
            "funding": float(params.get("w_funding", 0.2)),
            "open_interest": float(params.get("w_oi", 0.15)),
            "liquidation_aggregate": float(params.get("w_liq", 0.2)),
            "implied_volatility": float(params.get("w_iv", 0.1)),
            "prediction_price": float(params.get("w_pred", 0.05)),
        }

        # 取各特征数据，计算 z-score
        stress_components = []

        for feat_name, weight in weights.items():
            df = q.query(dataset_id=feat_name, columns=["event_time"]).frame
            if len(df) > 0:
                values = df.select(pl.col(df.columns[1])).to_series()
                mean = values.mean()
                std = values.std()
                if std > 0:
                    z_score = (values - mean) / std
                    stress_components.append(z_score.alias(feat_name))

        if stress_components:
            # 加权合成
            stress = sum(c * weights[list(weights.keys())[i]] for i, c in enumerate(stress_components)) / sum(weights.values())
            result = df.with_columns(stress.alias("btc_market_stress"))
        else:
            result = df.with_columns(pl.lit(0.0).alias("btc_market_stress"))

        return result

    def register(self) -> None:
        ...
```

### dependencies 声明式

每个特征的 `dependencies` 声明（D07 §2 表）：

| name | dependencies |
|---|---|
| returns | {ohlcv} |
| realized_vol | {ohlcv} |
| iv_surface | {implied_volatility} |
| funding_oi_divergence | {funding, open_interest} |
| btc_market_stress | {realized_vol, funding, open_interest, liquidation_aggregate, implied_volatility, prediction_price} |

### 输出随行元信息

```python
@dataclass
class FeatureResult:
    frame: pl.DataFrame
    feature_name: str
    dependencies: list[str]
    params: dict
    code_version: str  # chronoforge.__version__
```

- compute 返回 `FeatureResult`（而非 `pl.DataFrame`），包含 dependencies + params + code_version

## 测试要求（D09 TC-R 组）

- **特征链依赖注册**：btc_market_stress 声明 dependencies 含 6 项 D07 §2 表所列 dataset
- **黄金值**：固定输入→固定输出
- **property**：确定性——同输入两次计算逐字节一致
- **replay**：dependencies 版本 + code_version → output hash 一致

## acceptance（GWT）

- [ ] Given btc_market_stress 声明 Then dependencies 含 6 项 D07 §2 表所列 dataset
- [ ] Given 同 dependencies 版本+同代码 When 重算 Then output hash 一致

## 验收清单（验收时填写）

DoD 16 项 + GWT 逐条勾选。

测试输出摘要：（待填）｜接管性抽查：（待填）｜git commit：（待填）

## 执行记录

| 时间 | 事件 |
|---|---|
| （执行时填写） | |

## Deferred Acceptance

无。本子任务独立闭环。
