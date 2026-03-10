#!/usr/bin/env python3
"""
ChronoForge 快速开始示例

这个示例展示了如何快速开始使用ChronoForge框架，
适合新用户了解基本功能和用法。
"""

import logging
from rich.console import Console
from rich.table import Table
from rich.logging import RichHandler
from chronoforge import IntegratedScheduler
from chronoforge.data_source import CryptoSpotDataSource
from chronoforge.task_manager_ultimate import ultimate_task_manager
from chronoforge.utils import TimeSlot

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    handlers=[RichHandler(rich_tracebacks=True)]
)
logger = logging.getLogger(__name__)
console = Console()


def step1_create_scheduler():
    """步骤1：创建调度器"""
    console.print("\n[bold cyan]步骤1：创建集成调度器[/bold cyan]")
    
    # 创建调度器，设置最大工作线程数
    scheduler = IntegratedScheduler(max_workers=2)
    
    console.print("✅ 调度器创建完成")
    console.print(f"   最大工作线程数: {scheduler.max_workers}")
    
    return scheduler


def step2_create_data_source():
    """步骤2：创建数据源"""
    console.print("\n[bold cyan]步骤2：创建数据源[/bold cyan]")
    
    # 简单的加密货币数据源配置
    config = {
        'exchange_name': 'binance'
    }
    
    # 创建数据源实例
    data_source = CryptoSpotDataSource(config)
    
    console.print("✅ 数据源创建完成")
    console.print(f"   数据源类型: CryptoSpotDataSource")
    console.print(f"   交易所: binance")
    
    return data_source


def step3_register_data_source(scheduler, data_source):
    """步骤3：注册数据源（自动创建任务）"""
    console.print("\n[bold cyan]步骤3：注册数据源[/bold cyan]")
    
    # 为数据源配置自动创建周期性任务
    data_source.default_config = {
        'auto_create_periodic_tasks': True,
        'periodic_task_config': {
            'interval': 60,  # 60秒间隔
            'symbols': ['BTC/USDT'],
            'timeframe': '1h',
            'exchange_name': 'binance'
        }
    }
    
    # 注册数据源实例，会自动创建周期性任务
    ultimate_task_manager.register_data_source_instance("crypto_demo", data_source)
    
    console.print("✅ 数据源注册完成")
    console.print("   自动创建周期性任务: 启用")
    console.print("   任务间隔: 60秒")
    console.print("   监控交易对: BTC/USDT")
    
    return "crypto_demo"


def step4_create_manual_task(scheduler):
    """步骤4：创建手动任务"""
    console.print("\n[bold cyan]步骤4：创建手动任务[/bold cyan]")
    
    # 创建一个基于时间槽的任务
    scheduler.add_task(
        name="manual_analysis",
        data_source_name="CryptoSpotDataSource",
        data_source_config={'exchange_name': 'binance'},
        storage_name="LocalFileStorage",
        storage_config={'base_path': './data'},
        time_slot=TimeSlot("09:00:00", "17:00:00"),  # 交易时段
        symbols=['ETH/USDT'],
        timeframe='1h',
        timerange_str='20240101-'
    )
    
    console.print("✅ 手动任务创建完成")
    console.print("   任务名称: manual_analysis")
    console.print("   交易对: ETH/USDT")
    console.print("   执行时段: 09:00:00 - 17:00:00")
    
    return "manual_analysis"


def step5_display_tasks(scheduler):
    """步骤5：显示所有任务"""
    console.print("\n[bold cyan]步骤5：任务概览[/bold cyan]")
    
    # 获取所有任务
    tasks = scheduler.tasks
    
    if not tasks:
        console.print("⚠️  当前没有任务")
        return
    
    # 创建任务表格
    table = Table(title="ChronoForge 任务列表", show_header=True, header_style="bold magenta")
    table.add_column("任务名称", style="cyan")
    table.add_column("数据源")
    table.add_column("存储")
    table.add_column("类型")
    table.add_column("交易对")
    table.add_column("时间框架")
    
    for task_name, task in tasks.items():
        task_type = getattr(task, 'task_type', 'unknown')
        if hasattr(task, 'is_auto_created') and task.is_auto_created:
            task_type += " (auto)"
        
        table.add_row(
            task_name,
            task.data_source_name,
            task.storage_name,
            task_type,
            str(task.symbols) if task.symbols else "-",
            task.timeframe or "-"
        )
    
    console.print(table)
    console.print(f"\n✅ 任务总数: {len(tasks)}")


def step6_start_monitoring(scheduler, duration=30):
    """步骤6：启动监控"""
    console.print("\n[bold cyan]步骤6：启动任务监控[/bold cyan]")
    console.print(f"   监控时长: {duration}秒")
    
    # 启动调度器
    scheduler.start()
    console.print("✅ 调度器已启动")
    
    # 简单监控
    import time
    
    for i in range(duration):
        time.sleep(1)
        
        # 每5秒显示一次状态
        if (i + 1) % 5 == 0:
            console.print(f"\n📊 第{i+1}秒 - 任务状态:")
            
            # 获取任务状态
            for task_name in scheduler.tasks:
                state = scheduler._get_task_state(task_name)
                if state:
                    status = state.get('status', 'unknown')
                    run_count = state.get('run_count', 0)
                    last_status = state.get('last_run_status')
                    
                    status_icon = "🔄" if status == "executing" else "✅" if status == "completed" else "⏸️"
                    console.print(f"   {status_icon} {task_name}: {status} (执行次数: {run_count})")
    
    # 停止调度器
    scheduler.stop()
    console.print("\n🛑 调度器已停止")


def show_summary():
    """显示总结"""
    console.print("\n" + "="*60)
    console.print("[bold green]🎉 ChronoForge 快速开始完成！[/bold green]")
    console.print("="*60)
    
    summary = """
[bold cyan]✅ 已完成的功能:[/bold cyan]
  • 创建集成调度器
  • 配置数据源
  • 自动创建周期性任务
  • 手动创建时间槽任务
  • 任务状态监控

[bold yellow]🚀 核心优势:[/bold yellow]
  • 零配置启动 - 自动任务创建
  • 多数据源支持 - 加密货币、经济数据等
  • 灵活任务调度 - 周期性 + 时间槽
  • 实时监控 - 完整的任务状态跟踪
  • 异步架构 - 高性能并发处理

[bold green]📚 下一步:[/bold green]
  • 查看完整示例: examples/complete_workflow_example.py
  • 了解API功能: examples/plugin_functions_example.py
  • 阅读详细文档: docs/
  • 自定义数据源: 继承DataSourceBase
"""
    
    console.print(summary)


def main():
    """主函数"""
    console.print("="*60)
    console.print("[bold cyan]ChronoForge 快速开始指南[/bold cyan]")
    console.print("="*60)
    
    console.print("""
[bold yellow]本示例将带您快速了解ChronoForge的核心功能:[/bold yellow]
1. 创建调度器
2. 配置数据源
3. 自动创建任务
4. 手动创建任务
5. 监控任务执行

[italic]整个过程约需1分钟...[/italic]
""")
    
    # 执行步骤
    scheduler = step1_create_scheduler()
    data_source = step2_create_data_source()
    task_name = step3_register_data_source(scheduler, data_source)
    manual_task = step4_create_manual_task(scheduler)
    step5_display_tasks(scheduler)
    step6_start_monitoring(scheduler, duration=15)
    
    # 显示总结
    show_summary()


if __name__ == "__main__":
    main()