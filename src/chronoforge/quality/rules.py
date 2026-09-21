"""质量规则引擎（D06 §1, VALIDATION-001.1/001.2）。

职责：
- QualityRule ABC：规则协议
- 规则注册表：全局唯一 rule_id
- run()：筛选规则 → 执行 → 构建 QualityReport
- 17 条质量规则实现（D06 §2 规则清单）
"""

from __future__ import annotations

import json
import math
from abc import ABC, abstractmethod
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Literal, cast

from chronoforge.exceptions import StorageError
from chronoforge.models.enums import CanonicalType

from .continuity import expected_grid
from .report import GapContext, QualityFinding, QualityReport

# ── QualityRule ABC ────────────────────────────────────────────────────


class QualityRule(ABC):
    """质量规则基类。

    子类必须定义：
    - rule_id: Q-xx-nnn
    - applies_to: 适用的 CanonicalType 集合
    - severity: ERROR / WARNING / INFO
    - check(): 执行检测逻辑
    """

    @property
    @abstractmethod
    def rule_id(self) -> str: ...

    @property
    @abstractmethod
    def applies_to(self) -> frozenset[CanonicalType] | Literal["ALL"]: ...

    @property
    @abstractmethod
    def severity(self) -> Literal["ERROR", "WARNING", "INFO"]: ...

    @abstractmethod
    def check(
        self,
        records: Sequence[dict[str, object]],
        canonical_type: CanonicalType,
        *,
        context: GapContext | None = None,
    ) -> list[QualityFinding]: ...


# ── 规则注册表 ─────────────────────────────────────────────────────────


_rule_registry: dict[str, QualityRule] = {}


def register_rule(rule: QualityRule) -> None:
    """注册质量规则（单例，模块加载时调用）。

    Raises:
        ValueError: rule_id 重复。
    """
    if rule.rule_id in _rule_registry:
        raise ValueError(f"Duplicate rule_id: {rule.rule_id}")
    _rule_registry[rule.rule_id] = rule


def get_all_rules() -> list[QualityRule]:
    """返回全部注册规则（testing 用）。"""
    return list(_rule_registry.values())


def get_rule(rule_id: str) -> QualityRule | None:
    """按 rule_id 获取规则。"""
    return _rule_registry.get(rule_id)


def validate_registry() -> None:
    """架构测试用：校验 rule_id 无重复、severity 合法。"""
    for rule in _rule_registry.values():
        if rule.rule_id not in _rule_registry:
            raise ValueError(f"Unregistered rule_id: {rule.rule_id}")
        if rule.severity not in ("ERROR", "WARNING", "INFO"):
            raise ValueError(
                f"Invalid severity for {rule.rule_id}: {rule.severity}"
            )


# ── run 函数 ───────────────────────────────────────────────────────────


def run(
    records: Sequence[dict[str, object]],
    canonical_type: CanonicalType,
    *,
    context: GapContext | None = None,
    block_on: list[str] | None = None,
) -> QualityReport:
    """执行质量规则检查（D06 §1）。

    Args:
        records: 待检查记录。
        canonical_type: 记录类型。
        context: GapContext 上下文。
        block_on: 阻断规则列表（来自 Settings.quality_block_on）。

    Returns:
        QualityReport。

    Raises:
        StorageError: block_on 命中。
    """
    # 1. 筛选适用于 canonical_type 的规则
    applicable_rules = [
        rule
        for rule in get_all_rules()
        if rule.applies_to == "ALL" or canonical_type in rule.applies_to
    ]

    # 2. 逐个执行规则
    all_findings: list[QualityFinding] = []
    for rule in applicable_rules:
        try:
            findings = rule.check(records, canonical_type, context=context)
            all_findings.extend(findings)
        except Exception as exc:
            # 规则内部异常 → ERROR finding（不中断）
            all_findings.append(QualityFinding(
                record_key=f"RULE_ERROR:{rule.rule_id}",
                rule_id=rule.rule_id,
                severity="ERROR",
                detail=json.dumps({"exception": str(exc)}),
            ))

    # 3. 构建 report
    report = QualityReport(findings=all_findings, rule_id=None)

    # 4. 检查 block_on
    if block_on:
        for finding in all_findings:
            if finding.rule_id in block_on and finding.severity == "ERROR":
                report.rule_id = finding.rule_id
                raise StorageError(
                    f"Block on rule {finding.rule_id}",
                )

    return report


# ── Q-DRIFT-001 规则 ──────────────────────────────────────────────────


class QDRIFT001Rule(QualityRule):
    """Q-DRIFT-001：同 natural key 值列漂移检测。

    适用：非 revision 全类型（NUMBER/POSITION 排除，天然多版本）。
    severity: WARNING
    """

    @property
    def rule_id(self) -> str:
        return "Q-DRIFT-001"

    @property
    def applies_to(self) -> frozenset[CanonicalType] | Literal["ALL"]:
        from chronoforge.storage.base import REVISION_TYPES
        return frozenset(
            ct for ct in CanonicalType
            if ct not in REVISION_TYPES
        )

    @property
    def severity(self) -> Literal["ERROR", "WARNING", "INFO"]:
        return "WARNING"

    def check(
        self,
        records: Sequence[dict[str, object]],
        canonical_type: CanonicalType,
        *,
        context: GapContext | None = None,
    ) -> list[QualityFinding]:
        """Q-DRIFT-001 check 入口。P0：返回空列表（findings 由 upsert 路径生成）。"""
        return []


# ── Q-SCHEMA-001 规则 ─────────────────────────────────────────────────


class QSchema001Rule(QualityRule):
    """Q-SCHEMA-001：Pydantic 校验失败捕获转 finding（记录级，不中断）。"""

    @property
    def rule_id(self) -> str:
        return "Q-SCHEMA-001"

    @property
    def applies_to(self) -> frozenset[CanonicalType] | Literal["ALL"]:
        return "ALL"

    @property
    def severity(self) -> Literal["ERROR", "WARNING", "INFO"]:
        return "ERROR"

    def check(
        self,
        records: Sequence[dict[str, object]],
        canonical_type: CanonicalType,
        *,
        context: GapContext | None = None,
    ) -> list[QualityFinding]:
        findings = []
        for rec in records:
            try:
                # 尝试构建模型实例触发校验
                _build_record(canonical_type, rec)
            except Exception:
                findings.append(QualityFinding(
                    record_key=_extract_nk(rec, canonical_type),
                    rule_id=self.rule_id,
                    severity=self.severity,
                    detail=json.dumps({"type": str(canonical_type)}),
                ))
        return findings


# ── Q-PROV-001 规则 ───────────────────────────────────────────────────


class QProv001Rule(QualityRule):
    """Q-PROV-001：provenance 五字段空缺检查。

    五个 required 字段：source, source_id, source_timestamp,
    ingest_timestamp, raw_record_id 均不可为空。
    """

    @property
    def rule_id(self) -> str:
        return "Q-PROV-001"

    @property
    def applies_to(self) -> frozenset[CanonicalType] | Literal["ALL"]:
        return "ALL"

    @property
    def severity(self) -> Literal["ERROR", "WARNING", "INFO"]:
        return "ERROR"

    def check(
        self,
        records: Sequence[dict[str, object]],
        canonical_type: CanonicalType,
        *,
        context: GapContext | None = None,
    ) -> list[QualityFinding]:
        provenance_fields = (
            "source", "source_id", "source_timestamp",
            "ingest_timestamp", "raw_record_id",
        )
        findings = []
        for rec in records:
            missing = [f for f in provenance_fields if not rec.get(f)]
            if missing:
                findings.append(QualityFinding(
                    record_key=_extract_nk(rec, canonical_type),
                    rule_id=self.rule_id,
                    severity=self.severity,
                    detail=json.dumps({"missing_fields": missing}),
                ))
        return findings


# ── Q-NULL-001 规则 ───────────────────────────────────────────────────


class QNull001Rule(QualityRule):
    """Q-NULL-001：声明必填字段为 None（FRED value 允许 None→INFO）。"""

    @property
    def rule_id(self) -> str:
        return "Q-NULL-001"

    @property
    def applies_to(self) -> frozenset[CanonicalType] | Literal["ALL"]:
        return "ALL"

    @property
    def severity(self) -> Literal["ERROR", "WARNING", "INFO"]:
        return "WARNING"

    def check(
        self,
        records: Sequence[dict[str, object]],
        canonical_type: CanonicalType,
        *,
        context: GapContext | None = None,
    ) -> list[QualityFinding]:
        # 各类型的必填业务字段定义
        required_fields_map: dict[CanonicalType, tuple[str, ...]] = {
            CanonicalType.OHLCV: ("open", "close", "high", "low", "volume"),
            CanonicalType.TRADE: ("price", "quantity", "side", "trade_id"),
            CanonicalType.TICKER: ("last_price", "bid", "ask", "volume_24h"),
            CanonicalType.FUNDING: ("funding_rate",),
            CanonicalType.OPTION: ("strike", "option_type"),
            CanonicalType.IMPLIED_VOLATILITY: ("iv", "data_tier"),
            CanonicalType.NUMBER: ("units", "seasonal_adjustment"),
            CanonicalType.PREDICTION_PRICE: ("price", "volume"),
            CanonicalType.POSITION: ("long_positions", "short_positions"),
        }
        required = required_fields_map.get(canonical_type)
        if not required:
            return []

        findings = []
        for rec in records:
            null_fields = [f for f in required if rec.get(f) is None]
            # NUMBER.value 允许 None（FRED 缺失观测），不标记
            if canonical_type == CanonicalType.NUMBER:
                null_fields = [f for f in null_fields if f != "value"]
            if null_fields:
                is_fred_none = canonical_type == CanonicalType.NUMBER and "value" in null_fields
                sev = "INFO" if is_fred_none else self.severity
                findings.append(QualityFinding(
                    record_key=_extract_nk(rec, canonical_type),
                    rule_id=self.rule_id,
                    severity=sev,
                    detail=json.dumps({"null_fields": null_fields}),
                ))
        return findings


# ── Q-TS-001 规则 ─────────────────────────────────────────────────────


class QTS001Rule(QualityRule):
    """Q-TS-001：event/observation_time > ingest_time+5min（时钟异常）。"""

    @property
    def rule_id(self) -> str:
        return "Q-TS-001"

    @property
    def applies_to(self) -> frozenset[CanonicalType] | Literal["ALL"]:
        return "ALL"

    @property
    def severity(self) -> Literal["ERROR", "WARNING", "INFO"]:
        return "WARNING"

    def check(
        self,
        records: Sequence[dict[str, object]],
        canonical_type: CanonicalType,
        *,
        context: GapContext | None = None,
    ) -> list[QualityFinding]:
        findings = []
        for rec in records:
            ts = _get_event_time(rec, canonical_type)
            ingest = rec.get("ingest_timestamp")
            if ts is None or ingest is None:
                continue
            if _is_datetime(ts):
                threshold = _to_datetime(ingest) + timedelta(minutes=5)
                if _to_datetime(ts) > threshold:
                    findings.append(QualityFinding(
                        record_key=_extract_nk(rec, canonical_type),
                        rule_id=self.rule_id,
                        severity=self.severity,
                        detail=json.dumps({
                            "event_time": str(ts),
                            "ingest_time": str(ingest),
                        }),
                    ))
        return findings


# ── Q-TS-002 规则 ─────────────────────────────────────────────────────


class QTS002Rule(QualityRule):
    """Q-TS-002：相邻 event_time 间隔 ≠ interval（乱序检测）。"""

    @property
    def rule_id(self) -> str:
        return "Q-TS-002"

    @property
    def applies_to(self) -> frozenset[CanonicalType] | Literal["ALL"]:
        return frozenset({CanonicalType.OHLCV, CanonicalType.TRADE})

    @property
    def severity(self) -> Literal["ERROR", "WARNING", "INFO"]:
        return "WARNING"

    def check(
        self,
        records: Sequence[dict[str, object]],
        canonical_type: CanonicalType,
        *,
        context: GapContext | None = None,
    ) -> list[QualityFinding]:
        if len(records) < 2:
            return []

        # 按 event_time 排序
        def _event_dt(r: dict[str, object]) -> datetime:
            return _to_datetime(_get_event_time(r, canonical_type))

        sorted_recs = sorted(
            (r for r in records if _get_event_time(r, canonical_type) is not None),
            key=_event_dt,
        )
        findings = []
        for i in range(1, len(sorted_recs)):
            prev_t = _event_dt(sorted_recs[i - 1])
            curr_t = _event_dt(sorted_recs[i])
            gap = abs((curr_t - prev_t).total_seconds())
            # OHLCV 预期间隔取决于 interval（从 context 或 market_id 推断）
            expected_seconds = self._expected_seconds(canonical_type, context)
            if expected_seconds and gap != expected_seconds:
                findings.append(QualityFinding(
                    record_key=_extract_nk(sorted_recs[i], canonical_type),
                    rule_id=self.rule_id,
                    severity=self.severity,
                    detail=json.dumps({
                        "prev_event_time": str(sorted_recs[i - 1].get("event_time")),
                        "curr_event_time": str(sorted_recs[i].get("event_time")),
                        "gap_seconds": gap,
                        "expected_seconds": expected_seconds,
                    }),
                ))
        return findings

    @staticmethod
    def _expected_seconds(canonical_type: CanonicalType, context: GapContext | None) -> int | None:
        if canonical_type == CanonicalType.OHLCV and context and context.frequency:
            return _parse_freq_seconds(context.frequency)
        return None


# ── Q-TS-003 规则 ─────────────────────────────────────────────────────


class QTS003Rule(QualityRule):
    """Q-TS-003：未收盘 K 线（event_time+interval > now）。"""

    @property
    def rule_id(self) -> str:
        return "Q-TS-003"

    @property
    def applies_to(self) -> frozenset[CanonicalType] | Literal["ALL"]:
        return frozenset({CanonicalType.OHLCV})

    @property
    def severity(self) -> Literal["ERROR", "WARNING", "INFO"]:
        return "INFO"

    def check(
        self,
        records: Sequence[dict[str, object]],
        canonical_type: CanonicalType,
        *,
        context: GapContext | None = None,
    ) -> list[QualityFinding]:
        now = datetime.now(UTC)
        findings = []
        for rec in records:
            event_time = _get_event_time(rec, canonical_type)
            if event_time is None:
                continue
            interval_sec = self._interval_seconds(rec, context)
            if interval_sec is None:
                continue
            if _to_datetime(event_time) + timedelta(seconds=interval_sec) > now:
                findings.append(QualityFinding(
                    record_key=_extract_nk(rec, canonical_type),
                    rule_id=self.rule_id,
                    severity=self.severity,
                    detail=json.dumps({"event_time": str(event_time)}),
                ))
        return findings

    @staticmethod
    def _interval_seconds(rec: dict[str, object], context: GapContext | None) -> int | None:
        if context and context.frequency:
            return _parse_freq_seconds(context.frequency)
        # 尝试从 market_id 推断（如 BINANCE:BTCUSDT:1m）
        market_id = rec.get("market_id", "")
        if isinstance(market_id, str):
            for suffix in ("1m", "5m", "15m", "1h", "4h", "1d"):
                if market_id.endswith(suffix):
                    return _parse_freq_seconds(suffix)
        return None


# ── Q-DUP-001 规则 ────────────────────────────────────────────────────


class QDup001Rule(QualityRule):
    """Q-DUP-001：natural key 在批次内重复。"""

    @property
    def rule_id(self) -> str:
        return "Q-DUP-001"

    @property
    def applies_to(self) -> frozenset[CanonicalType] | Literal["ALL"]:
        return "ALL"

    @property
    def severity(self) -> Literal["ERROR", "WARNING", "INFO"]:
        return "ERROR"

    def check(
        self,
        records: Sequence[dict[str, object]],
        canonical_type: CanonicalType,
        *,
        context: GapContext | None = None,
    ) -> list[QualityFinding]:
        seen: dict[tuple[object, ...], int] = {}  # nk → first index
        findings = []
        for i, rec in enumerate(records):
            nk = _nk_tuple(rec, canonical_type)
            if nk in seen:
                findings.append(QualityFinding(
                    record_key=_extract_nk(rec, canonical_type),
                    rule_id=self.rule_id,
                    severity=self.severity,
                    detail=json.dumps({"index": i, "first_index": seen[nk]}),
                ))
            else:
                seen[nk] = i
        return findings


# ── Q-SEQ-001 规则 ────────────────────────────────────────────────────


class QSeq001Rule(QualityRule):
    """Q-SEQ-001：aggTrades 归集序列对账。

    窗口内相邻记录 prev.trade_id+1 == curr.trade_id（数字比较）。
    """

    @property
    def rule_id(self) -> str:
        return "Q-SEQ-001"

    @property
    def applies_to(self) -> frozenset[CanonicalType] | Literal["ALL"]:
        return frozenset({CanonicalType.TRADE})

    @property
    def severity(self) -> Literal["ERROR", "WARNING", "INFO"]:
        return "WARNING"

    def check(
        self,
        records: Sequence[dict[str, object]],
        canonical_type: CanonicalType,
        *,
        context: GapContext | None = None,
    ) -> list[QualityFinding]:
        findings = []
        for i in range(1, len(records)):
            prev = records[i - 1]
            curr = records[i]
            prev_id = prev.get("trade_id")
            curr_id = curr.get("trade_id")
            if prev_id is None or curr_id is None:
                continue
            try:
                prev_num = int(cast("int | str", prev_id))
                curr_num = int(cast("int | str", curr_id))
                if curr_num != prev_num + 1:
                    findings.append(QualityFinding(
                        record_key=_extract_nk(curr, canonical_type),
                        rule_id=self.rule_id,
                        severity=self.severity,
                        detail=json.dumps({
                            "prev_trade_id": prev_id,
                            "curr_trade_id": curr_id,
                            "gap": curr_num - prev_num - 1,
                        }),
                    ))
            except (ValueError, TypeError):
                pass  # trade_id 非数字格式，跳过
        return findings


# ── Q-RANGE-001 规则 ──────────────────────────────────────────────────


class QRange001Rule(QualityRule):
    """Q-RANGE-001：OHLCV 值域校验。

    high >= max(open, close) 或 low <= min(open, close) 违反；volume < 0；NaN/Inf。
    """

    @property
    def rule_id(self) -> str:
        return "Q-RANGE-001"

    @property
    def applies_to(self) -> frozenset[CanonicalType] | Literal["ALL"]:
        return frozenset({CanonicalType.OHLCV})

    @property
    def severity(self) -> Literal["ERROR", "WARNING", "INFO"]:
        return "ERROR"

    def check(
        self,
        records: Sequence[dict[str, object]],
        canonical_type: CanonicalType,
        *,
        context: GapContext | None = None,
    ) -> list[QualityFinding]:
        findings = []
        for rec in records:
            errors = []
            open_v = rec.get("open")
            close_v = rec.get("close")
            high_v = rec.get("high")
            low_v = rec.get("low")
            volume_v = rec.get("volume")

            # NaN/Inf 检查
            for fname, val in [("open", open_v), ("close", close_v),
                               ("high", high_v), ("low", low_v), ("volume", volume_v)]:
                if isinstance(val, (int, float)) and not math.isfinite(val):
                    errors.append(f"{fname} is NaN/Inf")

            # high >= max(open, close)
            if (high_v is not None and open_v is not None and close_v is not None
                    and isinstance(high_v, (int, float))
                    and isinstance(open_v, (int, float))
                    and isinstance(close_v, (int, float))):
                if high_v < max(open_v, close_v):
                    errors.append("high < max(open, close)")

            # low <= min(open, close)
            if (low_v is not None and open_v is not None and close_v is not None
                    and isinstance(low_v, (int, float))
                    and isinstance(open_v, (int, float))
                    and isinstance(close_v, (int, float))):
                if low_v > min(open_v, close_v):
                    errors.append("low > min(open, close)")

            # volume >= 0
            if isinstance(volume_v, (int, float)) and volume_v < 0:
                errors.append("volume < 0")

            if errors:
                findings.append(QualityFinding(
                    record_key=_extract_nk(rec, canonical_type),
                    rule_id=self.rule_id,
                    severity=self.severity,
                    detail=json.dumps({"errors": errors}),
                ))
        return findings


# ── Q-RANGE-002 规则 ──────────────────────────────────────────────────


class QRange002Rule(QualityRule):
    """Q-RANGE-002：iv∈[0,5]；probability∈[0,1]。"""

    @property
    def rule_id(self) -> str:
        return "Q-RANGE-002"

    @property
    def applies_to(self) -> frozenset[CanonicalType] | Literal["ALL"]:
        return frozenset({
            CanonicalType.IMPLIED_VOLATILITY,
            CanonicalType.PREDICTION_PRICE,
        })

    @property
    def severity(self) -> Literal["ERROR", "WARNING", "INFO"]:
        return "ERROR"

    def check(
        self,
        records: Sequence[dict[str, object]],
        canonical_type: CanonicalType,
        *,
        context: GapContext | None = None,
    ) -> list[QualityFinding]:
        findings = []
        for rec in records:
            errors = []
            if canonical_type == CanonicalType.IMPLIED_VOLATILITY:
                iv = rec.get("iv")
                if isinstance(iv, (int, float)):
                    if iv < 0 or iv > 5:
                        errors.append(f"iv={iv} not in [0, 5]")
            elif canonical_type == CanonicalType.PREDICTION_PRICE:
                prob = rec.get("implied_probability")
                price = rec.get("price")
                if isinstance(prob, (int, float)):
                    if prob < 0 or prob > 1:
                        errors.append(f"implied_probability={prob} not in [0, 1]")
                if isinstance(price, (int, float)):
                    if price < 0 or price > 1:
                        errors.append(f"price={price} not in [0, 1]")
            if errors:
                findings.append(QualityFinding(
                    record_key=_extract_nk(rec, canonical_type),
                    rule_id=self.rule_id,
                    severity=self.severity,
                    detail=json.dumps({"errors": errors}),
                ))
        return findings


# ── Q-RANGE-003 规则 ──────────────────────────────────────────────────


class QRange003Rule(QualityRule):
    """Q-RANGE-003：OPTION strike≤0；expiry 过期（SUSPECT 不 INVALID）。"""

    @property
    def rule_id(self) -> str:
        return "Q-RANGE-003"

    @property
    def applies_to(self) -> frozenset[CanonicalType] | Literal["ALL"]:
        return frozenset({CanonicalType.OPTION})

    @property
    def severity(self) -> Literal["ERROR", "WARNING", "INFO"]:
        return "INFO"

    def check(
        self,
        records: Sequence[dict[str, object]],
        canonical_type: CanonicalType,
        *,
        context: GapContext | None = None,
    ) -> list[QualityFinding]:
        from datetime import date as _date
        findings = []
        for rec in records:
            errors = []
            strike = rec.get("strike")
            if isinstance(strike, (int, float)) and strike <= 0:
                errors.append(f"strike={strike} <= 0")
            expiry = rec.get("expiry")
            if isinstance(expiry, _date) and expiry < _date.today():
                errors.append(f"expiry={expiry} is in the past")
            if errors:
                findings.append(QualityFinding(
                    record_key=_extract_nk(rec, canonical_type),
                    rule_id=self.rule_id,
                    severity=self.severity,
                    detail=json.dumps({"errors": errors}),
                ))
        return findings


# ── Q-GAP-001 规则 ────────────────────────────────────────────────────


class QGap001Rule(QualityRule):
    """Q-GAP-001：网格连续性检测（按 continuity_model 判定）。

    集成 expected_grid（D06 §2, VALIDATION-002）：
    - ALWAYS_OPEN → 全网格，缺失 → WARNING
    - TRADING_CALENDAR → 交易日缺失 → WARNING，非交易日 → EXPECTED_GAP/INFO
    - EVENT_BASED / RELEASE_SCHEDULE → 无网格 gap 概念
    """

    @property
    def rule_id(self) -> str:
        return "Q-GAP-001"

    @property
    def applies_to(self) -> frozenset[CanonicalType] | Literal["ALL"]:
        return frozenset({CanonicalType.OHLCV})

    @property
    def severity(self) -> Literal["ERROR", "WARNING", "INFO"]:
        return "INFO"

    def check(
        self,
        records: Sequence[dict[str, object]],
        canonical_type: CanonicalType,
        *,
        context: GapContext | None = None,
    ) -> list[QualityFinding]:
        if not records or context is None or context.frequency is None:
            return []

        from datetime import datetime as dt_type

        from .continuity import ContinuityModel

        # 解析 continuity_model
        try:
            cm = ContinuityModel(context.continuity_model)
        except ValueError:
            return []

        if cm in (ContinuityModel.EVENT_BASED, ContinuityModel.RELEASE_SCHEDULE):
            # 无网格 gap 概念，返回空
            return []

        # 提取 actual event_time
        actual_times: list[dt_type] = []
        for rec in records:
            et = rec.get("event_time")
            if et is not None:
                try:
                    actual_times.append(dt_type.fromisoformat(str(et)))
                except (ValueError, TypeError):
                    continue

        if not actual_times:
            return []

        # 生成 expected_grid
        start = min(actual_times)
        end = max(actual_times)
        freq = context.frequency

        grid = expected_grid(
            context.dataset_id,
            freq,
            cm,
            start,
            end,
        )

        if not grid:
            return []

        # 检查缺失
        gaps = []
        for entry_start, entry_end, _entry_type in grid:
            found = False
            for t in actual_times:
                if entry_start <= t < entry_end:
                    found = True
                    break
            if not found:
                gaps.append((str(entry_start), str(entry_end)))

        if not gaps:
            return []

        # 按 EXPECTED_GAP 判定 severity
        sev: Literal["ERROR", "WARNING", "INFO"] = "INFO"
        for entry_start, entry_end, entry_type in grid:
            if entry_type == "REGULAR":
                found = False
                for t in actual_times:
                    if entry_start <= t < entry_end:
                        found = True
                        break
                if not found:
                    sev = "WARNING"  # 交易日缺失 → WARNING
                    break

        detail = json.dumps({
            "continuity_model": context.continuity_model,
            "frequency": context.frequency,
            "gap_ranges": [str(g) for g in gaps],
            "severity": sev,
        })
        return [QualityFinding(
            record_key=f"OHLCV:{context.dataset_id}",
            rule_id=self.rule_id,
            severity=sev,
            detail=detail,
        )]


def gap_detection(
    records: Sequence[dict[str, object]],
    freq: str,
    continuity_model: str,
) -> list[tuple[str, str]]:
    """Q-GAP-001 gap 检测。

    Returns:
        区间合并列表 [(start_str, end_str), ...]
    """
    try:
        import pandas as pd  # type: ignore[import-untyped]  # pandas 已在项目依赖中
    except ImportError:
        return []

    raw_ts = [
        r.get("event_time")
        for r in records
        if r.get("event_time") is not None
    ]
    if not raw_ts:
        return []

    # 将 event_time 转为 pandas Timestamp 并对齐到频率边界
    try:
        ts_series = pd.Series([pd.Timestamp(t) for t in raw_ts])
        # 对齐到 freq 边界（如 1m → 整分钟）
        aligned = ts_series.dt.floor(freq)
        actual_aligned = list(aligned.drop_duplicates().sort_values())
        if not actual_aligned:
            return []
        # 用对齐后的 min/max 生成 expected grid
        expected = pd.date_range(min(actual_aligned), max(actual_aligned), freq=freq)
        expected_set = set(expected)
        missing_times = sorted(expected_set - set(actual_aligned))
    except Exception:
        return []

    if not missing_times:
        return []

    # 区间合并
    freq_seconds = _parse_freq_seconds(freq)
    if freq_seconds is None:
        freq_seconds = 60  # 默认 1m

    gaps = []
    gap_start = missing_times[0]
    gap_end = missing_times[0]
    for t in missing_times[1:]:
        if (pd.Timestamp(t) - pd.Timestamp(gap_end)).total_seconds() <= freq_seconds:
            gap_end = t
        else:
            gaps.append((gap_start, gap_end))
            gap_start = t
            gap_end = t
    gaps.append((gap_start, gap_end))
    return gaps


# ── Q-GAP-002 规则 ────────────────────────────────────────────────────


class QGap002Rule(QualityRule):
    """Q-GAP-002：与 frequency 声明对比缺行。"""

    @property
    def rule_id(self) -> str:
        return "Q-GAP-002"

    @property
    def applies_to(self) -> frozenset[CanonicalType] | Literal["ALL"]:
        return frozenset({
            CanonicalType.LIQUIDATION_AGGREGATE,
            CanonicalType.NUMBER,
        })

    @property
    def severity(self) -> Literal["ERROR", "WARNING", "INFO"]:
        return "INFO"

    def check(
        self,
        records: Sequence[dict[str, object]],
        canonical_type: CanonicalType,
        *,
        context: GapContext | None = None,
    ) -> list[QualityFinding]:
        if not records or context is None or context.frequency is None:
            return []

        gaps = gap_detection(records, context.frequency, context.continuity_model)
        if not gaps:
            return []

        return [QualityFinding(
            record_key=_extract_nk(records[0], canonical_type) if records else "unknown",
            rule_id=self.rule_id,
            severity=self.severity,
            detail=json.dumps({
                "frequency": context.frequency,
                "gap_ranges": [str(g) for g in gaps],
            }),
        )]


# ── Q-OHLC-001 规则 ───────────────────────────────────────────────────


class QOHLC001Rule(QualityRule):
    """Q-OHLC-001：closeTime-openTime ≈ interval（Binance 语义）。"""

    @property
    def rule_id(self) -> str:
        return "Q-OHLC-001"

    @property
    def applies_to(self) -> frozenset[CanonicalType] | Literal["ALL"]:
        return frozenset({CanonicalType.OHLCV})

    @property
    def severity(self) -> Literal["ERROR", "WARNING", "INFO"]:
        return "INFO"

    def check(
        self,
        records: Sequence[dict[str, object]],
        canonical_type: CanonicalType,
        *,
        context: GapContext | None = None,
    ) -> list[QualityFinding]:
        findings = []
        for rec in records:
            open_time = rec.get("open_time")
            close_time = rec.get("close_time")
            if open_time is None or close_time is None:
                continue
            if _is_datetime(open_time) and _is_datetime(close_time):
                diff = abs((_to_datetime(close_time) - _to_datetime(open_time)).total_seconds())
                expected_sec = self._expected_seconds(rec, context)
                if expected_sec is not None and diff > expected_sec * 1.01:
                    findings.append(QualityFinding(
                        record_key=_extract_nk(rec, canonical_type),
                        rule_id=self.rule_id,
                        severity=self.severity,
                        detail=json.dumps({
                            "open_time": str(open_time),
                            "close_time": str(close_time),
                            "diff_seconds": diff,
                            "expected_seconds": expected_sec,
                        }),
                    ))
        return findings

    @staticmethod
    def _expected_seconds(rec: dict[str, object], context: GapContext | None) -> int | None:
        if context and context.frequency:
            return _parse_freq_seconds(context.frequency)
        market_id = rec.get("market_id", "")
        if isinstance(market_id, str):
            for suffix in ("1m", "5m", "15m", "1h", "4h", "1d"):
                if market_id.endswith(suffix):
                    return _parse_freq_seconds(suffix)
        return None


# ── Q-CROSS-001 规则 ──────────────────────────────────────────────────


class QCross001Rule(QualityRule):
    """Q-CROSS-001：bid>ask、last 出界 [low24, high24]。"""

    @property
    def rule_id(self) -> str:
        return "Q-CROSS-001"

    @property
    def applies_to(self) -> frozenset[CanonicalType] | Literal["ALL"]:
        return frozenset({CanonicalType.TICKER})

    @property
    def severity(self) -> Literal["ERROR", "WARNING", "INFO"]:
        return "WARNING"

    def check(
        self,
        records: Sequence[dict[str, object]],
        canonical_type: CanonicalType,
        *,
        context: GapContext | None = None,
    ) -> list[QualityFinding]:
        findings = []
        for rec in records:
            errors = []
            bid = rec.get("bid")
            ask = rec.get("ask")
            last = rec.get("last_price")
            low24 = rec.get("low_24h")
            high24 = rec.get("high_24h")

            if (isinstance(bid, (int, float)) and isinstance(ask, (int, float))
                    and bid > ask):
                errors.append(f"bid={bid} > ask={ask}")

            if (isinstance(last, (int, float)) and isinstance(low24, (int, float))
                    and isinstance(high24, (int, float))):
                if last < low24 or last > high24:
                    errors.append(
                        f"last={last} out of [{low24}, {high24}]"
                    )

            if errors:
                findings.append(QualityFinding(
                    record_key=_extract_nk(rec, canonical_type),
                    rule_id=self.rule_id,
                    severity=self.severity,
                    detail=json.dumps({"errors": errors}),
                ))
        return findings


# ── Q-REV-001 规则 ────────────────────────────────────────────────────


class QRev001Rule(QualityRule):
    """Q-REV-001：同 nk 多 revision_time 并存 → 确认追加而非覆盖（计数）。"""

    @property
    def rule_id(self) -> str:
        return "Q-REV-001"

    @property
    def applies_to(self) -> frozenset[CanonicalType] | Literal["ALL"]:
        return frozenset({CanonicalType.NUMBER, CanonicalType.POSITION})

    @property
    def severity(self) -> Literal["ERROR", "WARNING", "INFO"]:
        return "INFO"

    def check(
        self,
        records: Sequence[dict[str, object]],
        canonical_type: CanonicalType,
        *,
        context: GapContext | None = None,
    ) -> list[QualityFinding]:
        # 按 (nk_without_revision_time, revision_time) 分组
        groups: dict[tuple[object, ...], list[object]] = {}
        for rec in records:
            nk = _nk_without_revision(rec, canonical_type)
            rev_time = rec.get("revision_time")
            groups.setdefault(nk, []).append(rev_time)

        findings = []
        for nk, rev_times in groups.items():
            unique_revs = set(str(rt) for rt in rev_times if rt is not None)
            if len(unique_revs) > 1:
                findings.append(QualityFinding(
                    record_key=_format_nk(nk),
                    rule_id=self.rule_id,
                    severity=self.severity,
                    detail=json.dumps({
                        "revision_count": len(unique_revs),
                        "revision_times": sorted(unique_revs),
                    }),
                ))
        return findings


# ── 辅助函数 ──────────────────────────────────────────────────────────


def _build_record(canonical_type: CanonicalType, data: dict[str, object]) -> object:
    """根据 CanonicalType 构建对应的 Pydantic 模型实例。

    仅传递模型实际接受的字段（过滤 open_time/close_time 等额外字段）。
    """
    from chronoforge.models.base import BaseRecord
    from chronoforge.models.derivatives import (
        GREEKS,
        IMPLIED_VOLATILITY,
        LIQUIDATION_AGGREGATE,
        OPTION,
    )
    from chronoforge.models.macro import NUMBER
    from chronoforge.models.market import FUNDING, OHLCV, TICKER, TRADE
    from chronoforge.models.positioning import POSITION
    from chronoforge.models.prediction import PREDICTION_PRICE

    _TYPE_MAP: dict[CanonicalType, type[BaseRecord]] = {
        CanonicalType.OHLCV: OHLCV,
        CanonicalType.TRADE: TRADE,
        CanonicalType.TICKER: TICKER,
        CanonicalType.FUNDING: FUNDING,
        CanonicalType.OPTION: OPTION,
        CanonicalType.IMPLIED_VOLATILITY: IMPLIED_VOLATILITY,
        CanonicalType.GREEKS: GREEKS,
        CanonicalType.NUMBER: NUMBER,
        CanonicalType.PREDICTION_PRICE: PREDICTION_PRICE,
        CanonicalType.POSITION: POSITION,
        CanonicalType.LIQUIDATION_AGGREGATE: LIQUIDATION_AGGREGATE,
    }
    model_cls = _TYPE_MAP.get(canonical_type)
    if model_cls is None:
        raise ValueError(f"Unknown canonical type for schema check: {canonical_type}")
    # 仅传递模型接受的字段（排除 open_time/close_time 等额外字段）
    model_fields = set(model_cls.model_fields.keys())
    filtered = {k: v for k, v in data.items() if k in model_fields}
    return model_cls(**filtered)  # type: ignore[arg-type]


def _extract_nk(rec: dict[str, object], canonical_type: CanonicalType) -> str:
    """从记录提取 natural key 字符串。"""
    nk = _nk_tuple(rec, canonical_type)
    return _format_nk(nk)


def _nk_tuple(rec: dict[str, object], canonical_type: CanonicalType) -> tuple[object, ...]:
    """从记录提取 natural key 元组。"""
    from chronoforge.storage.base import natural_key as _nk_cols
    cols = _nk_cols(canonical_type)
    return tuple(rec.get(col) for col in cols)


def _format_nk(nk: tuple[object, ...]) -> str:
    """将 natural key 元组格式化为字符串。"""
    return ":".join(str(v) if v is not None else "" for v in nk)


def _nk_without_revision(
    rec: dict[str, object], canonical_type: CanonicalType
) -> tuple[object, ...]:
    """提取不含 revision_time 的 natural key（用于 Q-REV-001）。"""
    from chronoforge.storage.base import natural_key as _nk_cols
    cols = _nk_cols(canonical_type)
    # 排除 revision_time 列
    cols_no_rev = [c for c in cols if c != "revision_time"]
    return tuple(rec.get(col) for col in cols_no_rev)


def _get_event_time(rec: dict[str, object], canonical_type: CanonicalType) -> object:
    """获取记录的事件时间字段。"""
    from chronoforge.storage.base import partition_time_field
    field = partition_time_field(canonical_type)
    if field:
        return rec.get(field)
    return rec.get("event_time")


def _is_datetime(v: object) -> bool:
    """检查值是否为 datetime 对象。"""
    return isinstance(v, datetime)


def _to_datetime(v: object) -> datetime:
    """将值转为 datetime。"""
    if isinstance(v, datetime):
        return v
    raise TypeError(f"Cannot convert {type(v)} to datetime")


def _parse_freq_seconds(freq: str) -> int | None:
    """将频率字符串解析为秒数。"""
    _FREQ_MAP: dict[str, int] = {
        "1m": 60, "5m": 300, "15m": 900, "30m": 1800,
        "1h": 3600, "2h": 7200, "4h": 14400,
        "1d": 86400, "1w": 604800,
    }
    return _FREQ_MAP.get(freq)


# ── 注册所有规则 ──────────────────────────────────────────────────────

register_rule(QDRIFT001Rule())
register_rule(QSchema001Rule())
register_rule(QProv001Rule())
register_rule(QNull001Rule())
register_rule(QTS001Rule())
register_rule(QTS002Rule())
register_rule(QTS003Rule())
register_rule(QDup001Rule())
register_rule(QSeq001Rule())
register_rule(QRange001Rule())
register_rule(QRange002Rule())
register_rule(QRange003Rule())
register_rule(QGap001Rule())
register_rule(QGap002Rule())
register_rule(QOHLC001Rule())
register_rule(QCross001Rule())
register_rule(QRev001Rule())
