from fastapi import APIRouter, HTTPException, Depends
from chronoforge.server.models.task import TaskCreate
from chronoforge.scheduler import Scheduler
from chronoforge.utils import TimeSlot
from chronoforge.logging_config import get_logger
from ..dependencies import get_scheduler
import time

logger = get_logger(__name__)

router = APIRouter(prefix="/tasks", tags=["tasks"])


@router.get("")
def list_tasks(scheduler: Scheduler = Depends(get_scheduler)):
    """列出所有任务"""
    tasks = []
    for task_name, task in scheduler.tasks.items():
        task_status = scheduler.task_states.get(task_name, {})
        # 处理timerange为None的情况
        timerange_str = "20240101-"  # 默认时间范围
        if hasattr(task, 'timerange') and task.timerange:
            if task.timerange.end_ts_ms:
                timerange_str = f"{task.timerange.start_ts_ms}-{task.timerange.end_ts_ms}"
            else:
                timerange_str = f"{task.timerange.start_ts_ms}-"
        
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
            "timerange_str": timerange_str,
            "status": task_status.get("status", "idle"),
            "is_auto_created": getattr(task, "is_auto_created", False)
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
        # 解析 timerange_str
        timerange_obj = None
        if task_create.timerange_str:
            from chronoforge.utils import TimeRange
            timerange_obj = TimeRange.parse_timerange(task_create.timerange_str)
        
        scheduler.add_task(
            name=task_create.name,
            data_source_name=task_create.data_source_name,
            data_source_config=task_create.data_source_config,
            storage_name=task_create.storage_name,
            storage_config=task_create.storage_config,
            time_slot=time_slot,
            symbols=task_create.symbols,
            timeframe=task_create.timeframe,
            timerange=timerange_obj,
            interval_seconds=task_create.interval_seconds,
            inplace=task_create.inplace
        )

        # 获取创建的任务
        task = scheduler.tasks[task_create.name]

        # 直接返回dict响应
        # 处理timerange为None的情况
        timerange_str = "20240101-"  # 默认时间范围
        if task.timerange:
            if task.timerange.end_ts_ms:
                timerange_str = f"{task.timerange.start_ts_ms}-{task.timerange.end_ts_ms}"
            else:
                timerange_str = f"{task.timerange.start_ts_ms}-"
        
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
            "timerange_str": timerange_str,
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
    # 处理timerange为None的情况
    timerange_str = "20240101-"  # 默认时间范围
    if task.timerange:
        if task.timerange.end_ts_ms:
            timerange_str = f"{task.timerange.start_ts_ms}-{task.timerange.end_ts_ms}"
        else:
            timerange_str = f"{task.timerange.start_ts_ms}-"
    
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
        "timerange_str": timerange_str,
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
async def start_task(task_name: str, scheduler: Scheduler = Depends(get_scheduler)):
    """启动任务"""
    if task_name not in scheduler.tasks:
        raise HTTPException(status_code=404, detail=f"Task {task_name} not found")

    try:
        result = await scheduler.run_task_now(task_name)

        success = result.get("success", False)
        message = result.get("message", result.get("error", "Unknown result"))

        # 更新任务状态，保留已有信息（如run_count等）
        current_state = scheduler.task_states.get(task_name, {})
        current_state.update({
            'start_time': time.time(),
            'status': 'completed' if success else 'failed'
        })
        scheduler.task_states[task_name] = current_state

        return {
            "name": task_name,
            "status": scheduler.task_states[task_name]['status'],
            "start_time": scheduler.task_states[task_name]['start_time'],
            "message": message
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


@router.get("/{task_name}/data_info")
async def get_task_data_info(task_name: str, scheduler: Scheduler = Depends(get_scheduler)):
    """获取任务下的所有数据名称以及数据起始和结束时间"""
    if task_name not in scheduler.tasks:
        raise HTTPException(status_code=404, detail=f"Task {task_name} not found")

    task = scheduler.tasks[task_name]

    storage = scheduler.storage_instances.get(task_name)
    if not storage:
        try:
            from chronoforge.storage.duckdb_storage import DUCKDBStorage
            from chronoforge.storage.localfile_storage import LocalFileStorage
            
            if task.storage_name == "DUCKDBStorage":
                storage = DUCKDBStorage(task.storage_config)
            elif task.storage_name == "LocalFileStorage":
                storage = LocalFileStorage(task.storage_config)
            else:
                from chronoforge.storage.manager import storage_manager
                storage = storage_manager.get_storage(task.storage_name)
                
            if storage:
                scheduler.storage_instances[task_name] = storage
        except Exception as e:
            raise HTTPException(status_code=500,
                                detail=f"Failed to create storage instance: {str(e)}")
    
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
        time_range = await storage.get_time_range(id=data_name)

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


@router.get("/{task_name}/data")
async def get_task_data(
    task_name: str,
    data_name: str = None,
    symbol: str = None,
    start_time: str = None,
    end_time: str = None,
    limit: int = 1000,
    scheduler: Scheduler = Depends(get_scheduler)
):
    """获取任务的数据

    Args:
        task_name: 任务名称
        data_name: 数据名称，如"binance:BTC/USDT_1d"
        symbol: 交易对，如"binance:BTC/USDT"
        start_time: 起始时间，格式为"YYYY-MM-DD HH:MM:SS"
        end_time: 结束时间，格式为"YYYY-MM-DD HH:MM:SS"
        limit: 返回数据的最大条数，默认1000
    """
    if task_name not in scheduler.tasks:
        raise HTTPException(status_code=404, detail=f"Task {task_name} not found")

    task = scheduler.tasks[task_name]

    storage = scheduler.storage_instances.get(task_name)
    if not storage:
        try:
            from chronoforge.storage.duckdb_storage import DUCKDBStorage
            from chronoforge.storage.localfile_storage import LocalFileStorage
            
            if task.storage_name == "DUCKDBStorage":
                storage = DUCKDBStorage(task.storage_config)
            elif task.storage_name == "LocalFileStorage":
                storage = LocalFileStorage(task.storage_config)
            else:
                from chronoforge.storage.manager import storage_manager
                storage = storage_manager.get_storage(task.storage_name)
                
            if storage:
                scheduler.storage_instances[task_name] = storage
        except Exception as e:
            raise HTTPException(status_code=500,
                                detail=f"Failed to create storage instance: {str(e)}")
    
    if not storage:
        raise HTTPException(status_code=500,
                            detail=f"Storage instance not found for task {task_name}")

    # 确定要获取的数据名称列表
    data_names_to_get = []

    if data_name:
        # 单个数据名称
        data_names_to_get = [data_name]
    elif symbol:
        # 单个交易对，构建数据名称
        data_names_to_get = [f"{symbol}_{task.timeframe}"]
    else:
        # 所有数据
        data_names_to_get = [f"{s}_{task.timeframe}" for s in task.symbols]

    # 获取数据
    all_data = []

    for data_name in data_names_to_get:
        data_type_map = {
            'FREDDataSource': 'macro_fred',
            'CryptoUMFutureDataSource': 'futures_metrics',
            'AlthernativeDataSource': 'btc_fgi',
            'CoinGeckoDataSource': 'coin_markets',
        }
        query_type = data_type_map.get(task.data_source_name, 'ohlcv')
        
        metadata = {'query_type': query_type}
        if query_type == 'futures_metrics':
            symbol_part = data_name.rsplit('_', 1)[0] if '_' in data_name else data_name
            metadata['symbol'] = symbol_part
        
        data = await storage.load(id=data_name, metadata=metadata)

        if data is not None and not data.empty:
            if 'ts' in data.columns and 'time' not in data.columns:
                data = data.rename(columns={'ts': 'time'})
            data_dict = data.to_dict(orient="records")
            all_data.extend(data_dict)

    if not all_data:
        return {
            "task_name": task_name,
            "data": [],
            "total": 0,
            "limit": limit
        }

    time_col = "time" if "time" in all_data[0] else "ts"
    all_data.sort(key=lambda x: x.get(time_col, x.get("ts")))

    import pandas as pd

    def normalize_timestamp(ts):
        """标准化时间戳，确保无时区"""
        dt = pd.to_datetime(ts)
        if isinstance(dt, pd.Timestamp) and dt.tz is not None:
            return dt.tz_convert(None)
        return dt

    if start_time:
        start_dt = normalize_timestamp(start_time)
        all_data = [
            item for item in all_data
            if normalize_timestamp(item.get(time_col, item.get("ts"))) >= start_dt
        ]

    if end_time:
        end_dt = normalize_timestamp(end_time)
        all_data = [
            item for item in all_data
            if normalize_timestamp(item.get(time_col, item.get("ts"))) <= end_dt
        ]

    if len(all_data) > limit:
        all_data = all_data[-limit:]

    for item in all_data:
        ts_val = item.get(time_col, item.get("ts"))
        if isinstance(ts_val, pd.Timestamp):
            item[time_col] = ts_val.strftime("%Y-%m-%d %H:%M:%S")
        elif hasattr(ts_val, 'strftime'):
            item[time_col] = ts_val.strftime("%Y-%m-%d %H:%M:%S")
        for key, val in item.items():
            if isinstance(val, float):
                import math
                if math.isnan(val) or math.isinf(val):
                    item[key] = None

    return {
        "task_name": task_name,
        "data": all_data,
        "total": len(all_data),
        "limit": limit
    }
