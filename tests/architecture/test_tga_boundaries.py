"""架构测试：TGA 子系统的脚本/分层边界（TGA-DECISIONS D-TGA-01/13）。

守卫三条不变量：
1. `src/chronoforge` 下不存在 `tga/` 包（TGA 业务逻辑只放 scripts）。
2. `scripts/tga_*.py` 不 import `chronoforge.connectors`（采集走 pipeline CLI）。
3. `src/chronoforge` 不 import `scripts`（依赖方向单向）。
"""

from __future__ import annotations

import ast
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_SRC = _ROOT / "src" / "chronoforge"
_SCRIPTS = _ROOT / "scripts"


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
    return modules


def test_src_has_no_tga_package() -> None:
    assert not (_SRC / "tga").exists()


def test_tga_scripts_do_not_import_connectors() -> None:
    for path in sorted(_SCRIPTS.glob("tga_*.py")):
        modules = _imports(path)
        offending = {m for m in modules if m.startswith("chronoforge.connectors")}
        assert not offending, f"{path.name} 直接 import 了 {sorted(offending)}"


def test_src_does_not_import_scripts() -> None:
    for path in sorted(_SRC.rglob("*.py")):
        modules = _imports(path)
        offending = {m for m in modules if m == "scripts" or m.startswith("scripts.")}
        assert not offending, f"{path.relative_to(_ROOT)} import 了 scripts"
