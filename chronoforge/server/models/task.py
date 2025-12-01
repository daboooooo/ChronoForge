from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime


class TimeRange(BaseModel):
    """时间范围模型"""
    start_ts_ms: Optional[int] = Field(None, description="开始时间戳（毫秒）")
    end_ts_ms: Optional[int] = Field(None, description="结束时间戳（毫秒）")


class TimeSlotModel(BaseModel):
    """时间槽模型"""
    start: str = Field(..., description="开始时间，格式为HH:MM或HH:MM:SS")
    end: str = Field(..., description="结束时间，格式为HH:MM或HH:MM:SS")


class TaskCreate(BaseModel):
    """创建任务请求模型"""
    name: str = Field(..., description="任务名称")
    data_source_name: str = Field(..., description="数据源名称")
    data_source_config: Dict[str, Any] = Field(default_factory=dict, description="数据源配置")
    storage_name: str = Field(..., description="存储名称")
    storage_config: Dict[str, Any] = Field(default_factory=dict, description="存储配置")
    time_slot: TimeSlotModel = Field(..., description="时间槽")
    symbols: List[str] = Field(..., description="交易对列表")
    timeframe: Optional[str] = Field("1d", description="时间框架，默认1d")
    timerange_str: Optional[str] = Field("20220101-", description="时间范围字符串，默认20220101-")
    inplace: Optional[bool] = Field(False, description="是否覆盖已存在任务，默认False")


class TaskResponse(BaseModel):
    """任务响应模型"""
    name: str
    data_source_name: str
    storage_name: str
    time_slot: dict
    symbols: List[str]
    timeframe: str
    timerange_str: str
    status: Optional[str] = None
    created_at: Optional[datetime] = None

    @classmethod
    def from_task(cls, task, status="idle"):
        """从Task对象创建TaskResponse"""
        return cls(
            name=task.name,
            data_source_name=task.data_source_name,
            storage_name=task.storage_name,
            time_slot={
                "start": task.time_slot.start,
                "end": task.time_slot.end
            },
            symbols=task.symbols,
            timeframe=task.timeframe,
            timerange_str=f"{task.timerange.start_ts_ms}-{task.timerange.end_ts_ms}" if (
                task.timerange.end_ts_ms) else f"{task.timerange.start_ts_ms}-",
            status=status
        )


class TaskStatus(BaseModel):
    """任务状态模型"""
    name: str
    status: str
    start_time: Optional[float] = None
    message: Optional[str] = None


class TaskListResponse(BaseModel):
    """任务列表响应模型"""
    tasks: List[dict]
    total: int
