"""CLI（D08 §2）：typer 命令树，薄层——零业务逻辑。

入口：chronoforge = chronoforge.cli.main:app
"""

from chronoforge.cli.main import app

__all__ = ["app"]
