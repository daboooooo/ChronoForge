"""
Scheduler 监控指标模块

提供调度器运行时的监控指标收集和健康检查功能
"""
import threading
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from collections import defaultdict
from datetime import datetime, timezone


@dataclass
class SchedulerMetrics:
    """调度器指标"""
    start_time: datetime = field(default_factory=datetime.now)
    total_task_runs: int = 0
    successful_runs: int = 0
    failed_runs: int = 0
    total_duration_ms: float = 0.0
    last_task_name: Optional[str] = None
    last_task_time: Optional[datetime] = None
    
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def record_task(self, task_name: str, duration_ms: float, success: bool) -> None:
        with self._lock:
            self.total_task_runs += 1
            self.total_duration_ms += duration_ms
            if success:
                self.successful_runs += 1
            else:
                self.failed_runs += 1
            self.last_task_name = task_name
            self.last_task_time = datetime.now(timezone.utc)

    def get_summary(self) -> Dict[str, Any]:
        with self._lock:
            uptime = (datetime.now(timezone.utc) - self.start_time).total_seconds()
            avg_duration = (
                self.total_duration_ms / self.total_task_runs 
                if self.total_task_runs > 0 else 0
            )
            success_rate = (
                self.successful_runs / self.total_task_runs * 100
                if self.total_task_runs > 0 else 0
            )
            
            return {
                'uptime_seconds': uptime,
                'total_task_runs': self.total_task_runs,
                'successful_runs': self.successful_runs,
                'failed_runs': self.failed_runs,
                'success_rate_percent': round(success_rate, 2),
                'avg_duration_ms': round(avg_duration, 2),
                'last_task_name': self.last_task_name,
                'last_task_time': (
                    self.last_task_time.isoformat() 
                    if self.last_task_time else None
                )
            }


@dataclass
class DataUpdateMetrics:
    """数据更新指标"""
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _update_counts: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    _update_durations: Dict[str, float] = field(default_factory=lambda: defaultdict(float))
    _last_update: Dict[str, datetime] = field(default_factory=dict)

    def record_update(
        self, 
        data_type: str, 
        duration_ms: float, 
        records_count: int
    ) -> None:
        with self._lock:
            key = f"{data_type}"
            self._update_counts[key] = self._update_counts.get(key, 0) + 1
            self._update_durations[key] = self._update_durations.get(key, 0) + duration_ms
            self._last_update[key] = datetime.now(timezone.utc)

    def get_summary(self) -> Dict[str, Any]:
        with self._lock:
            summaries = {}
            for key in self._update_counts:
                count = self._update_counts[key]
                total_duration = self._update_durations[key]
                summaries[key] = {
                    'total_updates': count,
                    'avg_duration_ms': round(total_duration / count, 2) if count > 0 else 0,
                    'last_update': (
                        self._last_update[key].isoformat() 
                        if key in self._last_update else None
                    )
                }
            return summaries


class SchedulerMonitor:
    """调度器监控器"""
    
    def __init__(self):
        self.scheduler_metrics = SchedulerMetrics()
        self.data_update_metrics = DataUpdateMetrics()
        self._running = False
    
    def start(self) -> None:
        self._running = True
    
    def stop(self) -> None:
        self._running = False
    
    def record_task_execution(
        self, 
        task_name: str, 
        duration_ms: float, 
        success: bool
    ) -> None:
        self.scheduler_metrics.record_task(task_name, duration_ms, success)
    
    def record_data_update(
        self, 
        data_type: str, 
        duration_ms: float, 
        records_count: int = 0
    ) -> None:
        self.data_update_metrics.record_update(data_type, duration_ms, records_count)
    
    def get_health_status(self) -> Dict[str, Any]:
        scheduler_summary = self.scheduler_metrics.get_summary()
        
        return {
            'healthy': self._running,
            'scheduler': scheduler_summary,
            'data_updates': self.data_update_metrics.get_summary(),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }


_global_monitor: Optional[SchedulerMonitor] = None


def get_monitor() -> SchedulerMonitor:
    """获取全局监控器"""
    global _global_monitor
    if _global_monitor is None:
        _global_monitor = SchedulerMonitor()
    return _global_monitor
