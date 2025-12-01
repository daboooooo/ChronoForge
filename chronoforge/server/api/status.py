from fastapi import APIRouter, Depends
from chronoforge.scheduler import Scheduler
from ..dependencies import get_scheduler
import time

router = APIRouter(prefix="/status", tags=["status"])


@router.get("")
def get_status(scheduler: Scheduler = Depends(get_scheduler)):
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

    return {
        "service": "ChronoForge Scheduler",
        "version": "0.1.0",
        "status": "running" if is_running else "stopped",
        "tasks_count": len(scheduler.tasks),
        "running_tasks_count": running_count,
        "supported_data_sources": scheduler.list_supported_plugins("data_source"),
        "supported_storages": scheduler.list_supported_plugins("storage"),
        "task_states": task_statuses
    }


@router.get("/tasks")
def get_tasks_status(scheduler: Scheduler = Depends(get_scheduler)):
    """获取所有任务状态"""
    task_statuses = {}

    # 首先处理所有已有的任务状态
    for task_name, task_state in scheduler.task_states.items():
        status = task_state.get("status", "idle")

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
            "message": "Task is running" if status == "running" else "Task is idle"
        }

    # 添加未运行的任务
    for task_name in scheduler.tasks:
        if task_name not in task_statuses:
            task_statuses[task_name] = {
                "name": task_name,
                "status": "created",
                "created_at": time.time(),
                "last_updated_at": time.time(),
                "run_count": 0,
                "last_run_time": None,
                "last_run_status": None,
                "error_message": None,
                "message": "Task is idle"
            }

    return task_statuses
