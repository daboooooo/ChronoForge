from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any


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


class FunctionParameter(BaseModel):
    """函数参数模型"""
    name: str = Field(..., description="参数名称")
    type: str = Field(..., description="参数类型")
    default: Optional[str] = Field(None, description="默认值")


class FunctionInfo(BaseModel):
    """函数信息模型"""
    name: str = Field(..., description="函数名称")
    docstring: Optional[str] = Field(None, description="函数文档字符串")
    parameters: List[FunctionParameter] = Field(..., description="函数参数列表")
    return_type: str = Field(..., description="返回值类型")


class DataSourceFunctionsResponse(BaseModel):
    """数据源函数响应模型"""
    data_source_name: str = Field(..., description="数据源名称")
    functions: List[FunctionInfo] = Field(..., description="函数列表")
    total: int = Field(..., description="函数总数")


class DelegateCallRequest(BaseModel):
    """代理调用请求模型"""
    plugin_name: str = Field(..., description="插件名称")
    plugin_type: str = Field(..., description="插件类型，data_source或storage")
    function_name: str = Field(..., description="要调用的函数名称")
    kwargs: Optional[Dict[str, Any]] = Field(default_factory=dict, description="传递给函数的关键字参数")


class DelegateCallResponse(BaseModel):
    """代理调用响应模型"""
    plugin_name: str = Field(..., description="插件名称")
    plugin_type: str = Field(..., description="插件类型")
    function_name: str = Field(..., description="函数名称")
    result: Any = Field(..., description="函数执行结果")
    success: bool = Field(..., description="调用是否成功")
