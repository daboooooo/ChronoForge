"""架构测试：README 与代码事实的一致性（审计 D-1/D-2 / 门禁 G7）。

守卫两处易漂移的数字：
1. `DEFAULT_SOURCES` 的源数量与 README 的 "bootstrapped: N sources" /
   "N pre-configured sources" 一致，且每个 source_id 都出现在 README。
2. `CanonicalType` 的成员数量与 README 的 "N types" 一致，且每个成员名都出现在 README。
"""

from __future__ import annotations

import re
from pathlib import Path

from chronoforge.models.enums import CanonicalType
from chronoforge.registry.service import DEFAULT_SOURCES

_ROOT = Path(__file__).resolve().parents[2]
_README = (_ROOT / "README.md").read_text(encoding="utf-8")


def test_readme_source_count_matches_default_sources() -> None:
    count = len(DEFAULT_SOURCES)
    assert f"bootstrapped: {count} sources" in _README
    assert f"with {count} pre-configured sources" in _README
    for spec in DEFAULT_SOURCES:
        assert f"`{spec.source_id}`" in _README, f"README 缺少 source `{spec.source_id}`"


def test_readme_canonical_type_count_matches_enum() -> None:
    count = len(CanonicalType)
    match = re.search(r"\*\*Canonical type system\*\* — (\d+) types", _README)
    assert match is not None, "README 未找到 Canonical type system 数量声明"
    assert int(match.group(1)) == count, f"README 声明 {match.group(1)}，实际 {count}"
    for member in CanonicalType:
        assert member.name in _README, f"README 分类表缺少 `{member.name}`"
