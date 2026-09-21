"""源/数据集注册表（D04 §5）。

service：bootstrap_defaults（source_registry 引导）+ dataset_registry 写入/查询。
"""

from chronoforge.registry.service import (
    DEFAULT_SOURCES,
    SourceSpec,
    add_dataset,
    bootstrap_defaults,
    get_dataset,
    list_datasets,
    list_sources,
)

__all__ = [
    "DEFAULT_SOURCES",
    "SourceSpec",
    "add_dataset",
    "bootstrap_defaults",
    "get_dataset",
    "list_datasets",
    "list_sources",
]
