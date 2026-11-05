#!/usr/bin/env python3
"""TGA 公共函数（Phase 2，依据 docs/strategy/TGA前瞻模型-实现方案.md §5/§10.2）。

包含：
- taxonomy.yaml 加载与匹配（归一分类、is_subtotal、include_in_net、置信度）
- 联邦工作日日历（pandas USFederalHolidayCalendar）
- 配置 hash（taxonomy 规范化 JSON + 代码版本）

约束（D-TGA-01/13）：本模块只放公共函数；采集/存储/模型职责仍在 src。
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import yaml

# ── taxonomy ───────────────────────────────────────────────────────────

_REPO_ROOT = Path(__file__).resolve().parents[1]
TAXONOMY_PATH = _REPO_ROOT / "config" / "tga" / "taxonomy.yaml"
CALENDAR_PATH = _REPO_ROOT / "config" / "tga" / "calendar.yaml"
MODEL_PATH = _REPO_ROOT / "config" / "tga" / "model.yaml"
SCENARIOS_PATH = _REPO_ROOT / "config" / "tga" / "scenarios.yaml"


@lru_cache(maxsize=16)
def load_yaml(path: str) -> dict:
    """加载 YAML 配置文件为 dict（缓存；路径以字符串为键）。"""
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


@dataclass(frozen=True)
class TaxonomyRule:
    """单条 taxonomy 规则（编译后）。"""

    pattern: str
    match_type: str  # prefix | contains | regex
    category: str
    flow_type: str
    subcategory: str | None
    include_in_net: bool
    is_subtotal: bool
    confidence: float
    transaction_type: str | None  # Deposits / Withdrawals / None(不限)


@dataclass(frozen=True)
class Classification:
    """一条现金流的归一分类结果。"""

    category: str
    flow_type: str
    subcategory: str | None
    direction: str  # INFLOW / OUTFLOW / INTERNAL / UNKNOWN
    include_in_net: bool
    is_subtotal: bool
    confidence: float
    matched_rule: str  # 命中规则 pattern；UNMATCHED 表示落入 unmatched


class Taxonomy:
    """taxonomy.yaml 编译后的匹配器。"""

    def __init__(self, data: dict) -> None:
        self.version: str = str(data.get("taxonomy_version") or data.get("version") or "")
        self.rules: tuple[TaxonomyRule, ...] = tuple(
            TaxonomyRule(
                pattern=str(r["match"]),
                match_type=str(r.get("match_type", "prefix")),
                category=str(r["category"]),
                flow_type=str(r["flow_type"]),
                subcategory=r.get("subcategory"),
                include_in_net=bool(r.get("include_in_net", True)),
                is_subtotal=bool(r.get("is_subtotal", False)),
                confidence=float(r.get("confidence", 0.5)),
                transaction_type=r.get("transaction_type"),
            )
            for r in data.get("rules", [])
        )
        u = data.get("unmatched", {})
        self.unmatched = TaxonomyRule(
            pattern="__unmatched__",
            match_type="regex",
            category=str(u.get("category", "UNCLASSIFIED")),
            flow_type=str(u.get("flow_type", "UNCLASSIFIED")),
            subcategory=u.get("subcategory"),
            include_in_net=bool(u.get("include_in_net", True)),
            is_subtotal=bool(u.get("is_subtotal", False)),
            confidence=float(u.get("confidence", 0.1)),
            transaction_type=None,
        )

    def classify(
        self, raw_category: str, transaction_type: str | None = None
    ) -> Classification:
        """匹配原始分类（大小写不敏感）；未命中走 unmatched。

        direction 由 transaction_type 决定（Deposits→INFLOW、
        Withdrawals→OUTFLOW、其他→UNKNOWN），与 category 语义正交。
        """
        text = (raw_category or "").strip().lower()
        tt = (transaction_type or "").strip()
        for rule in self.rules:
            if rule.transaction_type is not None and rule.transaction_type != tt:
                continue
            if self._hits(rule, text):
                return Classification(
                    category=rule.category,
                    flow_type=rule.flow_type,
                    subcategory=rule.subcategory,
                    direction=_direction(tt),
                    include_in_net=rule.include_in_net,
                    is_subtotal=rule.is_subtotal,
                    confidence=rule.confidence,
                    matched_rule=rule.pattern,
                )
        return Classification(
            category=self.unmatched.category,
            flow_type=self.unmatched.flow_type,
            subcategory=self.unmatched.subcategory,
            direction=_direction(tt),
            include_in_net=self.unmatched.include_in_net,
            is_subtotal=self.unmatched.is_subtotal,
            confidence=self.unmatched.confidence,
            matched_rule="UNMATCHED",
        )

    @staticmethod
    def _hits(rule: TaxonomyRule, text: str) -> bool:
        pattern = rule.pattern.lower()
        if rule.match_type == "regex":
            return re.search(pattern, text) is not None
        if rule.match_type == "contains":
            return pattern in text
        return text.startswith(pattern)  # prefix（默认）


def _direction(transaction_type: str) -> str:
    if transaction_type == "Deposits":
        return "INFLOW"
    if transaction_type == "Withdrawals":
        return "OUTFLOW"
    if transaction_type == "Internal":
        return "INTERNAL"
    return "UNKNOWN"


@lru_cache(maxsize=8)
def load_taxonomy(path: str | None = None) -> Taxonomy:
    """加载并编译 taxonomy.yaml（path 缺省用 config/tga/taxonomy.yaml）。"""
    p = Path(path) if path else TAXONOMY_PATH
    with open(p, encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    return Taxonomy(data)


def config_hash(
    taxonomy: Taxonomy,
    calendar: dict | None = None,
    model: dict | None = None,
    scenarios: dict | None = None,
) -> str:
    """config_hash = sha256(规范化 JSON of taxonomy + calendar + model + scenarios)。

    §10.2：scenarios 自 Phase 4 起叠加；传 None 按"未参与"处理（与旧调用方兼容）。
    """
    payload: dict = {
        "taxonomy_version": taxonomy.version,
        "rules": [
            {
                "pattern": r.pattern,
                "match_type": r.match_type,
                "category": r.category,
                "flow_type": r.flow_type,
                "subcategory": r.subcategory,
                "include_in_net": r.include_in_net,
                "is_subtotal": r.is_subtotal,
                "transaction_type": r.transaction_type,
            }
            for r in taxonomy.rules
        ],
        "unmatched": {
            "category": taxonomy.unmatched.category,
            "include_in_net": taxonomy.unmatched.include_in_net,
        },
    }
    if calendar is not None:
        payload["calendar"] = calendar
    if model is not None:
        payload["model"] = model
    if scenarios is not None:
        payload["scenarios"] = scenarios
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


# ── 联邦工作日日历 ─────────────────────────────────────────────────────


@lru_cache(maxsize=1)
def federal_holidays() -> frozenset:
    """美国联邦假日日期集合（pandas 内置日历，§15.2 calendar 约定）。"""
    from pandas.tseries.holiday import USFederalHolidayCalendar

    cal = USFederalHolidayCalendar()
    return frozenset(ts.date() for ts in cal.holidays())


def is_business_day(day) -> bool:
    """联邦工作日 = 周一至周五且非联邦假日。"""
    return day.weekday() < 5 and day not in federal_holidays()


def next_business_day(day):
    """严格晚于 day 的下一个联邦工作日。"""
    from datetime import timedelta

    nxt = day + timedelta(days=1)
    while not is_business_day(nxt):
        nxt += timedelta(days=1)
    return nxt
