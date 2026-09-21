"""FeatureEngine 协议 + 注册中心（D07 §2，QUERY-002）。

职责（D10 §7 QUERY-002）：
- FeatureEngine 协议：name/dependencies 声明式（架构 03 §7）+ compute/register
- FeatureRegistry：特征注册中心（重复注册拒绝；按名计算入口）
- FeatureResult：输出随行元信息载体（dependencies + params + code_version，
  D10 §7 data_contract）

规则（D07 §2）：
- 特征计算只经 QueryService 取数；分层契约中 features 与 research 同层独立
  （.importlinter layers），故 q 参数以本地结构化协议 QueryServiceLike 声明，
  实例由调用方运行时注入（D10 §7 inputs: QueryService from QUERY-001；
  chronoforge.research.query.DuckDBQueryService 结构满足本协议）
- register() 写 dataset_registry（FEATURE 类型）；协议签名冻结为
  register(self) -> None，写路径依赖（SQLite 连接 = MetaStore.connection）
  经构造函数注入（协议不约束 __init__）
- replay 确定性（D05 §4 验收）：dependencies 版本 + code_version → output_hash
  一致；compute 全路径无 now()/随机源，同输入两次计算逐字节一致
  （golden/property 测试守卫）
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

import polars as pl

import chronoforge

# register() 写 dataset_registry 的自产 source_id（特征非外部源，FK 满足所需）
FEATURE_SOURCE_ID = "CHRONOFORGE"


@dataclass(frozen=True)
class FeatureResult:
    """特征计算结果（输出随行元信息，D10 §7 data_contract）。"""

    frame: pl.DataFrame
    feature_name: str
    dependencies: list[str]
    params: dict[str, Any]  # compute 实际生效的参数（缺省填充后）
    code_version: str  # chronoforge.__version__

    def output_hash(self) -> str:
        """确定性输出指纹：sha256(元信息 + 行序列化)。

        hash 输入含 feature_name/dependencies（排序后）/params/code_version：
        dependencies 版本或 code_version 任一变化 → hash 变化（D05 §4 replay
        契约的 QUERY-002 侧载体；跨 run 的删除重放比对由 pipeline/replay 承担）。
        """
        payload = {
            "feature_name": self.feature_name,
            "dependencies": sorted(self.dependencies),
            "params": self.params,
            "code_version": self.code_version,
            "rows": self.frame.to_dicts(),
        }
        blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()


class QueryResultLike(Protocol):
    """DuckDBQueryService.query 返回值的最小结构视图（features 层本地声明）。"""

    frame: pl.DataFrame
    dataset_id: str
    dataset_version: str
    schema_version: str
    row_count: int
    elapsed_ms: int


class QueryServiceLike(Protocol):
    """DuckDBQueryService 的结构化最小视图（D10 §7 inputs 契约）。

    features 与 research 同层独立（.importlinter layers 默认 sibling 不可互依），
    以结构化协议解耦；chronoforge.research.query.DuckDBQueryService 满足本协议。
    """

    def query(
        self,
        dataset_id: str,
        columns: list[str] | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        asof: datetime | None = None,
        filters: Mapping[str, Any] | None = None,
    ) -> QueryResultLike: ...


class FeatureEngine(Protocol):
    """特征引擎协议（D07 §2）。

    name/dependencies 声明式（架构 03 §7）；compute 只经 QueryService 取数；
    register() 写 dataset_registry（FEATURE 类型）+ 版本（版本语义见
    _BaseFeature.register）。
    """

    name: str
    dependencies: list[str]

    def compute(self, q: QueryServiceLike, params: Mapping[str, Any]) -> FeatureResult: ...
    def register(self) -> None: ...


class FeatureRegistry:
    """特征注册中心（D07 §2；QUERY-002 任务单契约）。"""

    def __init__(self) -> None:
        self._features: dict[str, FeatureEngine] = {}

    def register(self, feature: FeatureEngine) -> None:
        """注册特征实现；同名重复注册拒绝。"""
        if feature.name in self._features:
            raise ValueError(f"Duplicate feature: {feature.name}")
        self._features[feature.name] = feature

    def get(self, name: str) -> FeatureEngine | None:
        """按名查询特征；不存在返回 None。"""
        return self._features.get(name)

    def all_features(self) -> list[FeatureEngine]:
        """全部已注册特征（注册序）。"""
        return list(self._features.values())

    def compute(
        self, name: str, q: QueryServiceLike, params: Mapping[str, Any]
    ) -> FeatureResult:
        """按名计算特征（未知名 ValueError）。"""
        feature = self._features.get(name)
        if feature is None:
            raise ValueError(f"Unknown feature: {name}")
        return feature.compute(q, params)


class _BaseFeature:
    """内置特征基类：register() 的 SQLite 写路径（协议签名冻结，连接构造注入）。

    子类只需声明 name/dependencies 并实现 compute()（与默认参数钩子）。
    """

    name: str
    dependencies: list[str]

    def __init__(self, connection: sqlite3.Connection | None = None) -> None:
        """Args:
            connection: SQLite 连接（MetaStore.connection）；None 时 compute
                可用、register() 拒绝执行。
        """
        self._connection = connection

    def _default_params(self) -> dict[str, Any]:
        """默认参数（register() 写入 params_json 的缺省值；子类覆写）。"""
        return {}

    def _result(self, frame: pl.DataFrame, params: Mapping[str, Any]) -> FeatureResult:
        """构造 FeatureResult（params 传入 compute 解析后的生效参数）。"""
        return FeatureResult(
            frame=frame,
            feature_name=self.name,
            dependencies=list(self.dependencies),
            params=dict(params),
            code_version=chronoforge.__version__,
        )

    def register(self) -> None:
        """幂等登记 FEATURE 数据集（D07 §2 register 语义）。

        - source_registry：自产源 CHRONOFORGE 行（FK 满足；幂等）
        - dataset_registry：dataset_id=特征 name、canonical_type=FEATURE、
          params_json=默认参数、continuity_model=EVENT_BASED、revision_supported=0

        版本语义（决策 D-3）：迁移 0003（dataset_registry.current_version 扩展列）
        未派发，与 QUERY-001 D-2 同源处置——dataset_version 查询侧经 run_log
        最近 SUCCESS run 推导；特征版本随行于 FeatureResult.code_version 与
        FEATURE 模型 feature_engine_version 字段，run_log 记账由调用方
        （compute-run）执行，register() 不虚构 run 记录。

        Raises:
            ValueError: 构造未注入连接。
            RuntimeError: SQLite 写入失败。
        """
        if self._connection is None:
            raise ValueError(
                f"{self.name}: register() 需要构造时注入 SQLite 连接（MetaStore.connection）"
            )
        try:
            self._connection.execute(
                "INSERT OR IGNORE INTO source_registry "
                "(source_id, display_name, access_type, base_url, rate_limit_json, "
                "historical_limit_days, license, enabled) "
                "VALUES (?, ?, 'PUBLIC', '', '{}', NULL, 'self-produced', 1)",
                (FEATURE_SOURCE_ID, FEATURE_SOURCE_ID),
            )
            self._connection.execute(
                "INSERT OR REPLACE INTO dataset_registry "
                "(dataset_id, source_id, canonical_type, entity_id, params_json, "
                "frequency, continuity_model, status, revision_supported, "
                "enabled, created_at) "
                "VALUES (?, ?, 'FEATURE', ?, ?, NULL, 'EVENT_BASED', "
                "'UNKNOWN', 0, 1, ?)",
                (
                    self.name,
                    FEATURE_SOURCE_ID,
                    self.name,
                    json.dumps(self._default_params(), sort_keys=True),
                    datetime.now(UTC).isoformat(),
                ),
            )
            self._connection.commit()
        except sqlite3.Error as e:
            self._connection.rollback()
            raise RuntimeError(f"Failed to register feature {self.name}: {e}") from e

    def compute(self, q: QueryServiceLike, params: Mapping[str, Any]) -> FeatureResult:
        """特征计算（子类实现；公式即契约，D07 §2 表）。"""
        raise NotImplementedError
