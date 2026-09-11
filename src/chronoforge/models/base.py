"""BaseRecord 基座（D02 §1，MODEL-001）。

全部 Canonical Type 的公共基座：schema_version、provenance 五字段、quality 两字段。
时间业务字段不进本层（防"裸 timestamp"架构约束），由各 Type 显式声明（MODEL-002）。
本模块零业务依赖（.importlinter 最底层），仅依赖 pydantic + stdlib。
"""

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from chronoforge.models.enums import QualityStatus


class BaseRecord(BaseModel):
    """全部 Canonical Type 的公共基座（D02 §1）。

    extra="forbid"：拒绝任何未声明字段（裸 timestamp 等时间字段必须由各 Type 显式声明）。
    validate_assignment：实例创建后的字段赋值同样触发校验。
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    schema_version: str
    # provenance 五字段（架构 03 §4）
    source: str  # source_registry.source_id
    source_id: str  # 源侧唯一标识（symbol/series_id/cik...）
    source_timestamp: datetime | None  # 源侧时间，无则显式传 None（必填、可空）
    ingest_timestamp: datetime
    # "{source}:{dataset}:{jsonl_file}:{line_no}"（str 非空即可，不做格式强校验）
    raw_record_id: str = Field(min_length=1)
    # 质量两字段（架构 05 §4）
    quality_status: QualityStatus = QualityStatus.VALID
    quality_reason: str | None = None
