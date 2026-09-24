"""CLI 命令树（D08 §2，typer）。

chronoforge
├── pipeline run | replay | status | circuit-reset
├── registry sync | list-sources | list-datasets
├── dataset add
├── query                                   # 顶层单命令
├── quality report
└── research reproduce

薄层约束：cli 层零业务逻辑（参数解析 → service 调用 → 渲染，架构 02 规则 3）。
与 CLI-001 任务单 main.py 骨架的差异（query/quality/dataset 挂载方式）以
D08 §2 冻结命令树为准——query 为顶层单命令而非子组（偏差登记见 CLI-001.md）。
"""

from __future__ import annotations

import typer

from chronoforge.cli.dataset_cmd import dataset_app
from chronoforge.cli.pipeline_cmd import pipeline_app
from chronoforge.cli.quality_cmd import quality_app
from chronoforge.cli.query_cmd import query
from chronoforge.cli.registry_cmd import registry_app
from chronoforge.cli.research_cmd import research_app
from chronoforge.logging import setup_logging

app = typer.Typer(
    name="chronoforge",
    help="ChronoForge — Financial Data Pipeline",
    add_completion=False,
    no_args_is_help=True,
)


@app.callback()
def _main() -> None:
    """全局入口：按 Settings.log_level 配置结构化日志（D08 §3）。"""
    from chronoforge.config.settings import Settings

    setup_logging(Settings.load().log_level)


app.add_typer(pipeline_app, name="pipeline")
app.add_typer(registry_app, name="registry")
app.add_typer(dataset_app, name="dataset")
app.add_typer(quality_app, name="quality")
app.add_typer(research_app, name="research")
app.command("query")(query)

if __name__ == "__main__":
    app()
