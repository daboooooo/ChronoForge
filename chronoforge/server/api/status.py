from fastapi import APIRouter, Depends
from chronoforge.scheduler import Scheduler
from ..dependencies import get_scheduler
import time
import httpx
from chronoforge import __version__

router = APIRouter(prefix="/status", tags=["status"])

# 为了兼容，添加 /stats 端点的重定向
from fastapi import APIRouter as APIRouterNoPrefix

# 创建一个没有前缀的路由器，用于添加 /stats 端点
compatibility_router = APIRouterNoPrefix(tags=["status"])


@compatibility_router.get("/stats")
async def get_stats_compatibility(scheduler: Scheduler = Depends(get_scheduler)):
    """获取系统统计信息（兼容端点）"""
    return await get_stats(scheduler)

# 连通性测试缓存
_connectivity_cache = {
    "status": False,
    "last_test": 0
}
_cache_expiry = 60  # 1分钟


async def test_connectivity():
    """测试服务器连通性"""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get("https://www.google.com", follow_redirects=True)
            return response.status_code == 200
    except Exception as e:
        print(f"连通性测试失败: {e}")
        return False


@router.get("")
async def get_status(scheduler: Scheduler = Depends(get_scheduler)):
    """获取服务状态"""
    # 检查调度器是否在运行
    is_running = False
    if hasattr(scheduler, '_runner_thread') and scheduler._runner_thread is not None:
        is_running = scheduler._runner_thread.is_alive()

    # 获取任务状态列表
    task_statuses = []
    running_count = 0
    for task_name, task_state in scheduler.task_states.items():
        status = task_state.get("status", "idle")
        if status == "running":
            running_count += 1

        # 构建完整的任务状态信息
        task_status = {
            "name": task_name,
            "status": status,
            "created_at": task_state.get("created_at"),
            "last_updated_at": task_state.get("last_updated_at"),
            "run_count": task_state.get("run_count", 0),
            "last_run_time": task_state.get("last_run_time"),
            "last_run_status": task_state.get("last_run_status"),
            "error_message": task_state.get("error_message"),
            "message": "Task is running" if status == "running" else "Task is idle"
        }
        task_statuses.append(task_status)

    # 检查连通性，使用缓存
    current_time = time.time()
    if current_time - _connectivity_cache["last_test"] > _cache_expiry:
        # 超过缓存时间，重新测试
        connectivity = await test_connectivity()
        _connectivity_cache["status"] = connectivity
        _connectivity_cache["last_test"] = current_time
    else:
        # 使用缓存结果
        connectivity = _connectivity_cache["status"]

    return {
        "service": "ChronoForge Scheduler",
        "version": __version__,
        "status": "running" if is_running else "stopped",
        "tasks_count": len(scheduler.tasks),
        "running_tasks_count": running_count,
        "supported_data_sources": scheduler.list_supported_plugins("data_source"),
        "supported_storages": scheduler.list_supported_plugins("storage"),
        "task_states": task_statuses,
        "connectivity": {
            "status": connectivity,
            "last_test": _connectivity_cache["last_test"],
            "test_url": "https://www.google.com",
            "cache_expiry": _cache_expiry
        }
    }


@router.get("/tasks")
def get_tasks_status(scheduler: Scheduler = Depends(get_scheduler)):
    """获取所有任务状态"""
    task_statuses = {}

    # 首先处理所有已有的任务状态
    for task_name, task_state in scheduler.task_states.items():
        status = task_state.get("status", "idle")
        
        # 检查任务是否是自动创建的
        task = scheduler.tasks.get(task_name)
        is_auto_created = getattr(task, "is_auto_created", False) if task else False

        # 构建完整的任务状态信息
        task_statuses[task_name] = {
            "name": task_name,
            "status": status,
            "created_at": task_state.get("created_at"),
            "last_updated_at": task_state.get("last_updated_at"),
            "run_count": task_state.get("run_count", 0),
            "last_run_time": task_state.get("last_run_time"),
            "last_run_status": task_state.get("last_run_status"),
            "error_message": task_state.get("error_message"),
            "message": "Task is running" if status == "running" else "Task is idle",
            "is_auto_created": is_auto_created
        }

    # 添加未运行的任务
    for task_name in scheduler.tasks:
        if task_name not in task_statuses:
            task = scheduler.tasks.get(task_name)
            is_auto_created = getattr(task, "is_auto_created", False) if task else False
            
            task_statuses[task_name] = {
                "name": task_name,
                "status": "created",
                "created_at": time.time(),
                "last_updated_at": time.time(),
                "run_count": 0,
                "last_run_time": None,
                "last_run_status": None,
                "error_message": None,
                "message": "Task is idle",
                "is_auto_created": is_auto_created
            }

    return task_statuses


@router.get("/stats")
async def get_stats(scheduler: Scheduler = Depends(get_scheduler)):
    """获取系统统计信息"""
    import psutil
    import os
    from datetime import datetime

    # 系统信息
    try:
        cpu_usage = psutil.cpu_percent(interval=0.1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        boot_time = psutil.boot_time()
        uptime = time.time() - boot_time
    except Exception:
        # 如果无法获取系统信息，使用默认值
        cpu_usage = 0
        memory = type('obj', (object,), {
            'percent': 0,
            'total': 0,
            'used': 0
        })
        disk = type('obj', (object,), {
            'percent': 0,
            'total': 0,
            'used': 0
        })
        uptime = 0

    # 任务信息
    tasks = {
        'total': len(scheduler.tasks),
        'running': 0,
        'idle': 0,
        'failed': 0
    }

    for task_name, task_state in scheduler.task_states.items():
        status = task_state.get("status", "idle")
        if status == "running":
            tasks['running'] += 1
        elif status == "failed":
            tasks['failed'] += 1
        else:
            tasks['idle'] += 1

    # 存储信息
    storage_info = {
        'total_size': "Unknown",
        'used_size': "Unknown",
        'data_count': 0
    }

    # API请求统计（模拟，实际项目中可以使用中间件记录）
    api_info = {
        'requests_count': 0,
        'requests_per_minute': 0
    }

    return {
        "system": {
            "cpu_usage": cpu_usage,
            "memory_usage": memory.percent,
            "disk_usage": disk.percent,
            "uptime_seconds": uptime,
            "python_version": f"{os.sys.version_info.major}.{os.sys.version_info.minor}.{os.sys.version_info.micro}",
            "timestamp": datetime.now().isoformat()
        },
        "tasks": tasks,
        "storage": storage_info,
        "api": api_info
    }
