# QUERY-002 — FeatureEngine 协议 + 5 个 P0 特征

## 派发信息

- 任务单：D10 §7 QUERY-002
- 设计依据：D07 §2（特征定义）
- Agent：Orchestrator 主会话直执（Coding-Agent 模式）
- 派发时间：2026-09-12
- 状态：DONE（2026-09-21）
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

## 实现决策（How 自由度）与偏差

| ID | 决策/偏差 | 依据 |
|---|---|---|
| D-1 | `compute` 返回 `FeatureResult`（frame + dependencies/params/code_version 随行元信息）；`FeatureResult` 为 frozen dataclass，`output_hash()` = sha256（元信息排序序列化 + frame.to_dicts()） | 任务单"输出随行元信息"节明确 compute 返回 FeatureResult（D07 §2 协议注释写 pl.DataFrame，以任务单随行元信息为准——hash 输入须含 dependencies/params/code_version）；frozen 保证随行元信息不可篡改 |
| D-2 | 特征的 SQLite 写连接经构造函数注入（`__init__(connection=None)`），协议不约束 `__init__`；未注入连接调用 `register()` → ValueError | D07 §2 协议签名冻结 `register() -> None`；分层契约禁止 features 依赖 storage，写依赖只能由装配方（持 MetaStore.connection 者）注入；test_register_without_connection_rejected 锁定契约 |
| D-3 | 版本语义同 QUERY-001 D-2：迁移 0003 未派发，`dataset_version` 查询侧经 run_log 最近 SUCCESS run 推导；特征版本随行 `FeatureResult.code_version`（`chronoforge.__version__`）与 FEATURE 模型 `feature_engine_version` 字段；`register()` 不虚构 run_log 行 | 与 QUERY-001 D-2 同源处置；run_log 记账由调用方（compute-run）执行 |
| D-4 | `QueryServiceLike`/`QueryResultLike` 结构化协议定义于 features/engine.py，不 import research | .importlinter layers：features 与 research 同层 sibling 默认不可互依；DuckDBQueryService 以鸭子类型满足协议（TestFeatureChain 全程以真实 service 实证） |
| D-5 | IV 的 strike/expiry 自标准化 instrument_id（D02 §3 格式 `{U}-{YYYY-MM-DD}-{K}-{C|P}`，ISO 日期含连字符共 6 段）镜像解析；不可解析行（PERP/外来命名）与 forward<=0/mark_iv 空值行排除 | MODEL-002 IV schema 无 strike/expiry 列，任务单伪代码 `columns=[...,"strike",...]` 将触发 QueryService 白名单 ValueError；不引入 dependencies 之外的数据集 |
| D-6 | Δ/差分基于查询返回序（QueryService 规则 6 升序稳定排序保证）；确定性靠 sort 显式 `maintain_order` + join 显式 `maintain_order="left"`（polars 1.44 join 该参数为 str，bool 会 TypeError）；P0 公式作用于单市场数据集语义 | D07 §2 公式以单数据集为输入；多市场参数化留待需求演进 |
| D-7 | btc_market_stress：网格 = 各分量事件时间**并集**（unique+sort）；同刻多观测先按 event_time 均值聚合（如 IV 多 instrument）；行级可用分量归一 `stress = Σ(w_i·z_i) / Σ(w_i·[z_i≠null])`，无可用 z 贡献的行剔除；std<=0 或空数据分量整体剔除；负权重 ValueError | 任务单伪代码未定义网格对齐语义且 `sum(weights.values())` 为全权重和（缺陷：空分量仍计入分母）；D07 §2"z-score 加权合成"+ 行级归一是最忠实实现——稀疏分量（IV 仅部分时刻有数据）不应清空网格，也不应计入无数据时刻的分母 |
| D-8 | `register()` 自实现 SQL：`INSERT OR REPLACE` 写 dataset_registry（canonical_type='FEATURE'/continuity_model='EVENT_BASED'/revision_supported=0/params_json=默认参数）+ `INSERT OR IGNORE` 自产 source_registry `CHRONOFORGE` 行（幂等） | MetaStore（STORAGE-001 ownership）无 dataset_registry 写 API；特征为自产数据（非外部源），自产 source 行满足 FK；幂等性 test_register_idempotent 锁定 |

## 测试要求（D09 TC-R 组）

- **特征链依赖注册**：btc_market_stress 声明 dependencies 含 6 项 D07 §2 表所列 dataset
- **黄金值**：固定输入→固定输出
- **property**：确定性——同输入两次计算逐字节一致
- **replay**：dependencies 版本 + code_version → output hash 一致

## acceptance（GWT）

- [x] Given btc_market_stress 声明 Then dependencies 含 6 项 D07 §2 表所列 dataset（test_btc_market_stress_dependencies_six：逐项断言 6 个 dataset_id 及其顺序；test_all_builtin_dependencies_match_d07_table 覆盖其余 4 特征）
- [x] Given 同 dependencies 版本+同代码 When 重算 Then output hash 一致（test_same_input_same_output_hash 同实例两次计算 + test_hash_across_service_instances 跨 service 重开连接；反例 test_hash_changes_with_params / test_hash_changes_with_code_version 证明 hash 对溯源信息敏感；test_hash_independent_of_dependency_order 证明声明顺序无关）

## 验收清单（验收时填写）

DoD 逐项勾选（howto §43）：

- [x] 实现完整（features/engine.py：FeatureEngine 协议 + FeatureRegistry + FeatureResult + _BaseFeature.register；features/builtin.py：5 个 P0 特征）
- [x] Public API 完整（name/dependencies 声明式 + compute/register 与 D07 §2 协议逐项对齐；FeatureRegistry.register/get/all_features/compute；`features/__init__.py` `__all__` 导出 10 符号）
- [x] 数据契约实现（FeatureResult 随行 dependencies/params/code_version；FEATURE 输出时间列 computed_at；instrument_id 解析与 D02 §3 生成规则镜像对称）
- [x] 错误处理实现（重复注册/未知特征/负权重/window<2 → ValueError；register 未注入连接 → ValueError；SQLite 写失败 → RuntimeError 且 rollback）
- [x] 日志实现（D10 任务单无 observability 条目，未引入日志面；确定性由确定性公式保证，无随机/时钟源）
- [x] 指标实现（FeatureResult.output_hash() 供 replay 对账；row 数随 frame 返回）
- [x] 单测完整（D10 tests 字段：integration 覆盖黄金值/确定性/replay hash/注册中心/特征链）
- [x] 边界测试完整（空数据早返回 schema 保持、全分量空帧、缺省 window 全 null、PERP 不可解析行排除、Δ 首行 null 不标记）
- [x] 失败测试完整（注入防御——取数只经 QueryService monkeypatch 断言、read_only 连接写失败双保险、未知特征/重复注册/负权重拒绝）
- [x] 恢复测试完整（不适用——纯计算无状态；register 幂等重入由 test_register_idempotent 覆盖）
- [x] 集成测试完整（26 用例：真实 MetaStore/SQLite + 手工 parquet + write 注册视图后 read_only 重开 + realized_vol→FEATURE parquet→stress 链路闭环）
- [x] 静态分析通过（ruff All checks passed——`features/engine.py`、`features/builtin.py`、`features/__init__.py`、`test_features.py`）
- [x] 类型检查通过（mypy Success：全库 58 source files 0 issues）
- [x] 无未声明假设（D-1~D-8 全部入档本记录）
- [x] 验收标准满足（GWT-1/2 逐条通过）

测试输出摘要：test_features.py **26 passed** in 5.17s；全量回归 **1260 passed / 0 failed** in 124.09s（基线 1234 + 新增 26，零破坏）；ruff features scope All checks passed（存量 2 处 pipeline I001 import 排序随积压补提交引入，0 处涉及 features，非本任务引入）；mypy Success（58 source files）；lint-imports 无新增违规（存量 2 处 broken：models.reference 与 quality.rules → exceptions → connectors.errors，0 处涉及 chronoforge.features）。

接管性抽查：builtin.py 模块 docstring 完整陈述 D07 §2 公式表与 How 层决策索引；每个特征类 docstring 给出公式/空值语义/排除规则；D-1~D-8 决策均给出设计依据；陌生 Agent 仅凭任务单 + D07 §2 + 本记录可接管维护。**通过**。

git commit：**DONE** — `9af25ce` feat(features): QUERY-002 FeatureEngine 协议 + 5 个 P0 特征（D07 §2）（2026-09-21 主会话直执，交付物 + 任务单 + 台账独立成笔）

## 执行记录

| 时间 | 事件 |
|---|---|
| 2026-09-21 | 主会话直执：实现协议 + 注册中心（D-1/D-2/D-4）+ 5 特征（D-5/D-6/D-7）+ register 自实现 SQL（D-8）；发现 IV schema 无 strike/expiry 列（伪代码取数将触发白名单 ValueError）→ instrument_id 镜像解析（D-5） |
| 2026-09-21 | 发现 polars 1.44 `join(maintain_order=True)` 运行时 TypeError（该参数为 str）→ 改 `maintain_order="left"`（D-6）；stress 黄金值测试暴露伪代码网格语义缺陷 → 并集网格 + 行级归一（D-7） |
| 2026-09-21 | 交付 features/engine.py + features/builtin.py + features/__init__.py + tests/integration/test_features.py（686 行，26 用例）；26 passed，全量 1260 passed，ruff/mypy features scope 清零 |
| 2026-09-21 | 验收：GWT 逐条核对通过 + DoD 15 项全勾 + 接管性抽查通过 → DONE；无新增 Design Issue，无 Deferred 项；lint-imports 新增违规 0（engine.py 曾引 exceptions→connectors.errors 传递链，已改为内建异常后清零） |

## Deferred Acceptance

无。本子任务独立闭环。
