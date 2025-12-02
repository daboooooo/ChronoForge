from fastapi import APIRouter, HTTPException, Depends
from chronoforge.server.models.task import TaskCreate
from chronoforge.scheduler import Scheduler
from chronoforge.utils import TimeSlot
from ..dependencies import get_scheduler

router = APIRouter(prefix="/tasks", tags=["tasks"])


@router.get("")
def list_tasks(scheduler: Scheduler = Depends(get_scheduler)):
    """列出所有任务"""
    tasks = []
    for task_name, task in scheduler.tasks.items():
        task_status = scheduler.task_states.get(task_name, {})
        task_dict = {
            "name": task.name,
            "data_source_name": task.data_source_name,
            "storage_name": task.storage_name,
            "time_slot": {
                "start": task.time_slot.start,
                "end": task.time_slot.end
            },
            "symbols": task.symbols,
            "timeframe": task.timeframe,
            "timerange_str": f"{task.timerange.start_ts_ms}-{task.timerange.end_ts_ms}" if (
                task.timerange.end_ts_ms) else f"{task.timerange.start_ts_ms}-",
            "status": task_status.get("status", "idle")
        }
        tasks.append(task_dict)
    return {
        "tasks": tasks,
        "total": len(tasks)
    }


@router.post("")
def create_task(task_create: TaskCreate, scheduler: Scheduler = Depends(get_scheduler)):
    """创建新任务"""
    try:
        # 创建TimeSlot对象
        time_slot = TimeSlot(
            start=task_create.time_slot.start,
            end=task_create.time_slot.end
        )

        # 添加任务到调度器
        scheduler.add_task(
            name=task_create.name,
            data_source_name=task_create.data_source_name,
            data_source_config=task_create.data_source_config,
            storage_name=task_create.storage_name,
            storage_config=task_create.storage_config,
            time_slot=time_slot,
            symbols=task_create.symbols,
            timeframe=task_create.timeframe,
            timerange_str=task_create.timerange_str,
            inplace=task_create.inplace
        )

        # 获取创建的任务
        task = scheduler.tasks[task_create.name]

        # 直接返回dict响应
        return {
            "name": task.name,
            "data_source_name": task.data_source_name,
            "storage_name": task.storage_name,
            "time_slot": {
                "start": task.time_slot.start,
                "end": task.time_slot.end
            },
            "symbols": task.symbols,
            "timeframe": task.timeframe,
            "timerange_str": f"{task.timerange.start_ts_ms}-{task.timerange.end_ts_ms}" if (
                task.timerange.end_ts_ms) else f"{task.timerange.start_ts_ms}-",
            "status": "idle"
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create task: {str(e)}")


@router.get("/{task_name}")
def get_task(task_name: str, scheduler: Scheduler = Depends(get_scheduler)):
    """获取任务详情"""
    task = scheduler.tasks.get(task_name)
    if not task:
        raise HTTPException(status_code=404, detail=f"Task {task_name} not found")

    task_status = scheduler.task_states.get(task_name, {})
    return {
        "name": task.name,
        "data_source_name": task.data_source_name,
        "storage_name": task.storage_name,
        "time_slot": {
            "start": task.time_slot.start,
            "end": task.time_slot.end
        },
        "symbols": task.symbols,
        "timeframe": task.timeframe,
        "timerange_str": f"{task.timerange.start_ts_ms}-{task.timerange.end_ts_ms}" if (
            task.timerange.end_ts_ms) else f"{task.timerange.start_ts_ms}-",
        "status": task_status.get("status", "idle")
    }


@router.delete("/{task_name}", status_code=204)
def delete_task(task_name: str, scheduler: Scheduler = Depends(get_scheduler)):
    """删除任务"""
    if task_name not in scheduler.tasks:
        raise HTTPException(status_code=404, detail=f"Task {task_name} not found")

    try:
        # 调用scheduler的delete_task方法
        scheduler.delete_task(task_name)
        return None
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete task: {str(e)}")


@router.post("/{task_name}/start")
def start_task(task_name: str, scheduler: Scheduler = Depends(get_scheduler)):
    """启动任务"""
    if task_name not in scheduler.tasks:
        raise HTTPException(status_code=404, detail=f"Task {task_name} not found")

    try:
        # 直接执行任务
        future = scheduler.thread_pool.submit(scheduler.execute_task, scheduler.tasks[task_name])
        scheduler.task_states[task_name] = {
            'future': future,
            'start_time': future.__dict__.get('_start_time', None),
            'status': 'running'
        }

        return {
            "name": task_name,
            "status": "running",
            "start_time": scheduler.task_states[task_name]['start_time'],
            "message": "Task started successfully"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start task: {str(e)}")


@router.post("/{task_name}/stop")
def stop_task(task_name: str, scheduler: Scheduler = Depends(get_scheduler)):
    """停止任务"""
    if task_name not in scheduler.tasks:
        raise HTTPException(status_code=404, detail=f"Task {task_name} not found")

    task_state = scheduler.task_states.get(task_name)
    if not task_state or 'future' not in task_state:
        return {
            "name": task_name,
            "status": "idle",
            "message": "Task is not running"
        }

    try:
        # 取消任务
        future = task_state['future']
        if not future.done():
            future.cancel()

        # 更新任务状态
        task_state['status'] = 'stopped'

        return {
            "name": task_name,
            "status": "stopped",
            "start_time": task_state.get('start_time'),
            "message": "Task stopped successfully"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to stop task: {str(e)}")


@router.get("/{task_name}/status")
def get_task_status(task_name: str, scheduler: Scheduler = Depends(get_scheduler)):
    """获取任务状态"""
    if task_name not in scheduler.tasks:
        raise HTTPException(status_code=404, detail=f"Task {task_name} not found")

    task_state = scheduler.task_states.get(task_name, {})
    return {
        "name": task_name,
        "status": task_state.get("status", "idle"),
        "start_time": task_state.get("start_time"),
        "message": "Task is running" if task_state.get("status") == "running" else "Task is idle"
    }


@router.get("/{task_name}/data")
async def get_task_data_info(task_name: str, scheduler: Scheduler = Depends(get_scheduler)):
    """获取任务下的所有数据名称以及数据起始和结束时间"""
    if task_name not in scheduler.tasks:
        raise HTTPException(status_code=404, detail=f"Task {task_name} not found")

    task = scheduler.tasks[task_name]

    # 获取任务对应的存储实例
    storage = scheduler.storage_instances.get(task_name)
    if not storage:
        raise HTTPException(status_code=500,
                            detail=f"Storage instance not found for task {task_name}")

    # 获取任务下的数据信息
    data_info = []

    # 遍历任务的所有symbols，每个symbol对应一个数据名称
    for symbol in task.symbols:
        # 构建数据名称
        data_name = f"{symbol}_{task.timeframe}"

        # 从存储中获取实际的数据起始和结束时间
        time_range = await storage.get_time_range(id=data_name, sub=task.sub)

        if time_range:
            if time_range["start_time"]:
                start_time = time_range["start_time"].strftime("%Y-%m-%d %H:%M:%S")
            else:
                start_time = "未知"
            if time_range["end_time"]:
                end_time = time_range["end_time"].strftime("%Y-%m-%d %H:%M:%S")
            else:
                end_time = "至今"
        else:
            start_time = "无法获取"
            end_time = "无法获取"

        data_info.append({
            "data_name": data_name,
            "symbol": symbol,
            "timeframe": task.timeframe,
            "start_time": start_time,
            "end_time": end_time
        })

    return {
        "task_name": task_name,
        "data_info": data_info,
        "total": len(data_info)
    }
