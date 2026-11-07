"""Normalize 行级跳过原因的结构化计数（审计 A-8）。

连接器在逐行丢弃时调用 :func:`note_skip`，NormalizeStage 用
:func:`begin_normalize` / :func:`end_normalize` 收集每个 batch 的计数，
据此计算行级丢弃率并触发 `QualityError`（不依赖日志文本解析）。

`LEGIT_SKIP_REASONS` 是冻结白名单：只有经审计确认的"源侧正常丢弃"
才可列入；新增/删除必须同步修改
`tests/quality/test_normalize_drop_gate.py::test_legit_skip_reasons_whitelist_is_locked`。
"""

from __future__ import annotations

from contextvars import ContextVar, Token

_NormalizeCounts = dict[str, int]

# 合法丢弃（源侧/协议侧正常行为，不计入丢弃率分子）：
# - unclosed_bar / unfinished_kline：未收盘 K 线（增量抓取的常态最后一根）
# - unfrozen_today：T+1 冻结源当日尚未冻结（coinmetrics / alternative）
# - pending_settlement：T+0 结算值未发布（sosovalue）
# - source_missing_value：源显式缺失（null/"."/空行），如 FRED "."、Yahoo 全 null bar
# - non_target_account：treasury Table I/II 中非 TGA 账户行（域过滤）
# - duplicate_observation：同一观测键多行收敛（PIT/revision 去重）
# - sve_test_operation：nyfed 的 Small Value Exercise 测试操作（非真实 SRF 用量）
# - non_overnight_operation：nyfed Term Repo 行（不属 SRF 隔夜口径）
# - not_yet_published：nyfed SRF 操作尚未到公布时刻（不写未来发布值的占位 0）
LEGIT_SKIP_REASONS: frozenset[str] = frozenset({
    "unclosed_bar",
    "unfinished_kline",
    "unfrozen_today",
    "pending_settlement",
    "source_missing_value",
    "non_target_account",
    "duplicate_observation",
    "sve_test_operation",
    "non_overnight_operation",
    "not_yet_published",
})

_counts: ContextVar[_NormalizeCounts | None] = ContextVar(
    "chronoforge_normalize_skips", default=None
)


def begin_normalize() -> Token[_NormalizeCounts | None]:
    """开始一个 batch 的跳过计数收集。"""
    return _counts.set({})


def end_normalize(token: Token[_NormalizeCounts | None]) -> _NormalizeCounts:
    """结束收集并返回 reason → count（未启用收集时返回空 dict）。"""
    counts = _counts.get() or {}
    _counts.reset(token)
    return dict(counts)


def note_skip(reason: str, count: int = 1) -> None:
    """记录一条（或 count 条）行级跳过；无收集上下文时为 no-op。"""
    if count <= 0:
        return
    counts = _counts.get()
    if counts is None:
        return
    counts[reason] = counts.get(reason, 0) + count
