from pydantic import BaseModel, Field
from typing import List, Optional


class PluginInfo(BaseModel):
    """插件信息模型"""
    name: str = Field(..., description="插件名称")
    type: str = Field(..., description="插件类型，data_source或storage")
    description: Optional[str] = Field(None, description="插件描述")


class PluginListResponse(BaseModel):
    """插件列表响应模型"""
    plugins: List[PluginInfo]
    total: int


class PluginTypeListResponse(BaseModel):
    """按类型列出插件响应模型"""
    plugin_type: str
    plugins: List[str]
    total: int
