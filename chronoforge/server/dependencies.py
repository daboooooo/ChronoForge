"""依赖管理模块，避免循环导入"""
from chronoforge.scheduler import Scheduler

# 全局Scheduler实例
scheduler_instance = None


def get_scheduler():
    """获取Scheduler实例的依赖函数"""
    global scheduler_instance
    if scheduler_instance is None:
        scheduler_instance = Scheduler()
    return scheduler_instance


def set_scheduler(scheduler: Scheduler):
    """设置Scheduler实例"""
    global scheduler_instance
    scheduler_instance = scheduler


def get_scheduler_instance():
    """直接获取Scheduler实例，用于内部使用"""
    global scheduler_instance
    return scheduler_instance
