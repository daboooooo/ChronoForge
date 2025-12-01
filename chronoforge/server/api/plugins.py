from fastapi import APIRouter, HTTPException, Depends
from chronoforge.server.models.plugin import PluginInfo, PluginListResponse, PluginTypeListResponse
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
