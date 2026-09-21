"""获取管道（D05）：七阶段、AcquisitionJob、状态机、replay。

模块布局：
- windows：窗口拆分（AcquisitionJob、Chunk、ChunkResult、plan_chunks）
- cursor：cursor 推进与回退防护（CursorAction、CursorUpdate、advance_chunks）
- state：RunStatus 运行状态机
- runner：PipelineRunner 七阶段编排 + RunContext/StageResult 契约
- replay：canonical / derived 重放
"""

from __future__ import annotations

from .cursor import CursorAction, CursorUpdate, advance_chunks
from .replay import replay
from .runner import (
    CanonicalStage,
    FetchStage,
    NormalizeStage,
    PipelineRunner,
    QualityStage,
    RawAppendStage,
    RunContext,
    RunLogStage,
    Stage,
    StageResult,
    ValidateStage,
)
from .state import RunStatus
from .windows import AcquisitionJob, Chunk, ChunkResult, plan_chunks

__all__ = [
    "AcquisitionJob",
    "CanonicalStage",
    "Chunk",
    "ChunkResult",
    "CursorAction",
    "CursorUpdate",
    "FetchStage",
    "NormalizeStage",
    "PipelineRunner",
    "QualityStage",
    "RawAppendStage",
    "RunContext",
    "RunLogStage",
    "RunStatus",
    "Stage",
    "StageResult",
    "ValidateStage",
    "advance_chunks",
    "plan_chunks",
    "replay",
]
