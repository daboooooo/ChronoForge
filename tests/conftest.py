"""ChronoForge 测试共享 fixture（INFRA-001）。

约定（docs/design/09-testing-plan.md §1、docs/design/08-cli-config-security.md §1）：
- 环境变量名以 D08 §1 为契约：CHRONOFORGE_ENV / CHRONOFORGE_DATA_DIR / CHRONOFORGE_META_DIR；
- 每个测试在 pytest tmp_path 下隔离的 data/meta 子目录中运行，测试结束后 env 由
  monkeypatch 自动还原（含删除本测试新设的变量）；
- 本文件不 import chronoforge.config——Settings 尚未实现（DEF-002，延后至 CLI-001
  落地后在此补充 `settings` fixture），当前仅负责 env 隔离与目录准备。

pytest markers（quality/smoke）已在 pyproject.toml 注册，此处勿重复注册。
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

# D08 §1 的 env 变量名（契约，勿改名）
ENV_VAR = "CHRONOFORGE_ENV"
DATA_DIR_VAR = "CHRONOFORGE_DATA_DIR"
META_DIR_VAR = "CHRONOFORGE_META_DIR"


@dataclass(frozen=True)
class StorePaths:
    """tmp_stores fixture 的返回形态：data/meta 两级存储根目录。"""

    data_dir: Path
    meta_dir: Path


def _store_paths(tmp_path: Path) -> StorePaths:
    """由 pytest tmp_path 派生每测试隔离的 data/meta 子目录。

    env fixture 与 tmp_stores fixture 共用本函数，保证两者指向完全一致。
    """
    return StorePaths(data_dir=tmp_path / "data", meta_dir=tmp_path / "meta")


@pytest.fixture(autouse=True)
def _isolated_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """autouse（function scope）：注入 test 环境变量 + tmp 数据目录。

    tmp_path 每个测试唯一，实现测试间目录隔离；monkeypatch 在测试拆解阶段
    自动还原 env（此前未设置的变量会被删除）。
    """
    paths = _store_paths(tmp_path)
    monkeypatch.setenv(ENV_VAR, "test")
    monkeypatch.setenv(DATA_DIR_VAR, str(paths.data_dir))
    monkeypatch.setenv(META_DIR_VAR, str(paths.meta_dir))


@pytest.fixture(autouse=True)
def _no_retry_backoff_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    """R2-04：屏蔽 chunk 级重试（FetchStage → connectors.retry）的真实退避。

    生产退避 1→2→4→8→16s；测试注入失败场景下真实等待会使套件不可接受地
    变慢。经 retry._sleep 间接层精准屏蔽（不影响被测代码的退避计算断言，
    unit/test_retry.py 自行 patch time.sleep 的用例不受影响）。
    """
    monkeypatch.setattr("chronoforge.connectors.retry._sleep", lambda _s: None)


@pytest.fixture
def tmp_stores(tmp_path: Path) -> StorePaths:
    """返回已创建的 {data_dir, meta_dir}（与 _isolated_env 写入 env 的路径一致）。"""
    paths = _store_paths(tmp_path)
    paths.data_dir.mkdir(parents=True, exist_ok=True)
    paths.meta_dir.mkdir(parents=True, exist_ok=True)
    return paths
