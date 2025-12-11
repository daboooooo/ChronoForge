from fastapi import APIRouter, HTTPException, Depends
from chronoforge.server.models.plugin import (
    PluginInfo, PluginListResponse, PluginTypeListResponse,
    DataSourceFunctionsResponse, DelegateCallRequest, DelegateCallResponse,
    FunctionInfo, FunctionParameter
)
from chronoforge.scheduler import Scheduler
from ..dependencies import get_scheduler

router = APIRouter(prefix="/plugins", tags=["plugins"])


@router.get("", response_model=PluginListResponse)
def list_plugins(scheduler: Scheduler = Depends(get_scheduler)):
    """列出所有支持的插件"""
    plugins = []

    # 添加数据源插件
    for ds_name in scheduler.list_supported_plugins("data_source"):
        plugins.append(PluginInfo(
            name=ds_name,
            type="data_source",
            description=f"Data source plugin: {ds_name}"
        ))

    # 添加存储插件
    for storage_name in scheduler.list_supported_plugins("storage"):
        plugins.append(PluginInfo(
            name=storage_name,
            type="storage",
            description=f"Storage plugin: {storage_name}"
        ))

    return PluginListResponse(plugins=plugins, total=len(plugins))


@router.get("/{plugin_type}", response_model=PluginTypeListResponse)
def list_plugins_by_type(plugin_type: str, scheduler: Scheduler = Depends(get_scheduler)):
    """按类型列出插件"""
    if plugin_type not in ["data_source", "storage"]:
        raise HTTPException(status_code=400,
                            detail="Invalid plugin type. Must be 'data_source' or 'storage'")

    plugins = scheduler.list_supported_plugins(plugin_type)
    return PluginTypeListResponse(
        plugin_type=plugin_type,
        plugins=plugins,
        total=len(plugins)
    )


@router.get("/data_source/{data_source_name}/functions", response_model=DataSourceFunctionsResponse)
def get_datasource_functions(data_source_name: str, scheduler: Scheduler = Depends(get_scheduler)):
    """获取数据源的函数列表"""
    try:
        functions_info = scheduler.datasource_functions(data_source_name)

        # 转换为响应模型
        function_infos = []
        for func in functions_info["functions"]:
            parameters = [
                FunctionParameter(
                    name=param["name"],
                    type=param["type"],
                    default=param["default"]
                )
                for param in func["parameters"]
            ]

            function_infos.append(FunctionInfo(
                name=func["name"],
                docstring=func["docstring"],
                parameters=parameters,
                return_type=func["return_type"]
            ))

        return DataSourceFunctionsResponse(
            data_source_name=data_source_name,
            functions=function_infos,
            total=len(function_infos)
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.post("/delegate-call", response_model=DelegateCallResponse)
def delegate_call(request: DelegateCallRequest, scheduler: Scheduler = Depends(get_scheduler)):
    """代理调用插件的函数"""
    try:
        # 调用scheduler的delegate_call方法
        result = scheduler.delegate_call(
            plugin_name=request.plugin_name,
            plugin_type=request.plugin_type,
            function_name=request.function_name,
            **(request.kwargs or {})
        )

        return DelegateCallResponse(
            plugin_name=request.plugin_name,
            plugin_type=request.plugin_type,
            function_name=request.function_name,
            result=result,
            success=True
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
