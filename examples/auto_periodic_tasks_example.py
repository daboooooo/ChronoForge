#!/usr/bin/env python3
"""
示例：自动创建周期性任务功能

该示例展示了如何使用ChronoForge的自动创建周期性任务功能，
当数据源注册时会自动创建周期性任务，无需手动配置。
"""

import asyncio
import logging
import time
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.logging import RichHandler
from chronoforge import IntegratedScheduler
from chronoforge.data_source import CryptoSpotDataSource, FREDDataSource, BitcoinFGIDataSource
from chronoforge.task_manager_ultimate import ultimate_task_manager

# 配置rich日志
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    handlers=[RichHandler(rich_tracebacks=True)]
)
logger = logging.getLogger(__name__)
console = Console()


def create_crypto_data_source_with_auto_tasks():
    """创建支持自动周期性任务的加密货币数据源"""
    console.print("\n[bold cyan]=== 创建加密货币数据源（自动周期性任务） ===[/bold cyan]")
    
    # 配置数据源，启用自动创建周期性任务
    crypto_config = {
        'exchange_name': 'binance',
        'default_config': {
            'auto_create_periodic_tasks': True,  # 启用自动创建周期性任务
            'periodic_task_config': {
                'interval': 30,  # 30秒间隔，用于演示
                'symbols': ['BTC/USDT', 'ETH/USDT', 'BNB/USDT'],
                'timeframe': '1h',
                'exchange_name': 'binance'
            }
        }
    }
    
    # 创建数据源实例
    crypto_data_source = CryptoSpotDataSource(crypto_config)
    
    console.print("✅ 加密货币数据源创建完成")
    console.print(f"   交易所: {crypto_config['exchange_name']}")
    console.print(f"   自动创建任务: 启用")
    console.print(f"   任务间隔: {crypto_config['default_config']['periodic_task_config']['interval']}秒")
    console.print(f"   监控交易对: {crypto_config['default_config']['periodic_task_config']['symbols']}")
    console.print(f"   时间框架: {crypto_config['default_config']['periodic_task_config']['timeframe']}")
    
    return crypto_data_source


def create_fred_data_source_with_auto_tasks():
    """创建支持自动周期性任务的FRED数据源"""
    console.print("\n[bold cyan]=== 创建FRED经济数据源（自动周期性任务） ===[/bold cyan]")
    
    # 配置数据源，启用自动创建周期性任务
    fred_config = {
        'default_config': {
            'auto_create_periodic_tasks': True,  # 启用自动创建周期性任务
            'periodic_task_config': {
                'interval': 60,  # 60秒间隔，用于演示
                'symbols': ['DGS10', 'DGS2', 'DGS5'],  # 美国国债收益率
                'timeframe': '1d',
                'exchange_name': 'fred'
            }
        }
    }
    
    # 创建数据源实例
    fred_data_source = FREDDataSource(fred_config)
    
    console.print("✅ FRED经济数据源创建完成")
    console.print("   自动创建任务: 启用")
    console.print(f"   任务间隔: {fred_config['default_config']['periodic_task_config']['interval']}秒")
    console.print(f"   监控指标: {fred_config['default_config']['periodic_task_config']['symbols']}")
    console.print(f"   时间框架: {fred_config['default_config']['periodic_task_config']['timeframe']}")
    
    return fred_data_source


def create_bitcoin_fgi_data_source_with_auto_tasks():
    """创建支持自动周期性任务的比特币恐惧贪婪指数数据源"""
    console.print("\n[bold cyan]=== 创建比特币恐惧贪婪指数数据源（自动周期性任务） ===[/bold cyan]")
    
    # 配置数据源，启用自动创建周期性任务
    fgi_config = {
        'default_config': {
            'auto_create_periodic_tasks': True,  # 启用自动创建周期性任务
            'periodic_task_config': {
                'interval': 120,  # 120秒间隔，用于演示
                'symbols': ['bitcoin_fgi'],
                'timeframe': '1d',
                'exchange_name': 'alternative'
            }
        }
    }
    
    # 创建数据源实例
    fgi_data_source = BitcoinFGIDataSource(fgi_config)
    
    console.print("✅ 比特币恐惧贪婪指数数据源创建完成")
    console.print("   自动创建任务: 启用")
    console.print(f"   任务间隔: {fgi_config['default_config']['periodic_task_config']['interval']}秒")
    console.print(f"   监控指标: {fgi_config['default_config']['periodic_task_config']['symbols']}")
    console.print(f"   时间框架: {fgi_config['default_config']['periodic_task_config']['timeframe']}")
    
    return fgi_data_source


def register_data_sources_and_create_tasks(scheduler, *data_sources):
    """注册数据源并创建自动周期性任务"""
    console.print("\n[bold cyan]=== 注册数据源并创建自动周期性任务 ===[/bold cyan]")
    
    task_names = []
    
    for i, data_source in enumerate(data_sources):
        # 根据数据源类型确定名称
        if isinstance(data_source, CryptoSpotDataSource):
            task_name = f"crypto_auto_{i}"
        elif isinstance(data_source, FREDDataSource):
            task_name = f"fred_auto_{i}"
        elif isinstance(data_source, BitcoinFGIDataSource):
            task_name = f"fgi_auto_{i}"
        else:
            task_name = f"data_source_auto_{i}"
        
        console.print(f"\n🔄 注册数据源: {task_name}")
        
        # 注册数据源实例（会自动创建周期性任务）
        ultimate_task_manager.register_data_source_instance(task_name, data_source)
        task_names.append(task_name)
        
        console.print(f"   ✅ 数据源 {task_name} 注册完成")
    
    return task_names


def display_auto_created_tasks(scheduler):
    """显示自动创建的任务"""
    console.print("\n[bold cyan]=== 自动创建的任务详情 ===[/bold cyan]")
    
    # 获取所有任务
    all_tasks = scheduler.tasks
    auto_created_tasks = []
    
    # 筛选自动创建的任务
    for task_name, task in all_tasks.items():
        if hasattr(task, 'is_auto_created') and task.is_auto_created:
            auto_created_tasks.append(task_name)
    
    if not auto_created_tasks:
        console.print("⚠️  未发现自动创建的任务")
        return
    
    # 创建表格展示任务详情
    table = Table(title="自动创建的周期性任务", show_header=True, header_style="bold magenta")
    table.add_column("任务名称", style="bold cyan")
    table.add_column("数据源")
    table.add_column("任务类型")
    table.add_column("时间槽")
    table.add_column("交易对/指标")
    table.add_column("时间框架")
    
    for task_name in auto_created_tasks:
        task = all_tasks[task_name]
        table.add_row(
            task_name,
            task.data_source_name,
            task.task_type or "unknown",
            f"{task.time_slot.start} - {task.time_slot.end}",
            str(task.symbols) if task.symbols else "-",
            task.timeframe or "-"
        )
    
    console.print(table)
    console.print(f"\n✅ 共发现 {len(auto_created_tasks)} 个自动创建的任务")


def monitor_task_execution(scheduler, task_names, duration=15):
    """监控任务执行情况"""
    console.print(f"\n[bold cyan]=== 监控任务执行情况（{duration}秒） ===[/bold cyan]")
    
    # 启动调度器
    scheduler.start()
    console.print("🚀 调度器已启动")
    
    # 监控循环
    for i in range(duration):
        time.sleep(1)
        
        # 每5秒显示一次状态（加快反馈）
        if (i + 1) % 5 == 0:
            console.print(f"\n📊 第{i+1}秒 - 任务执行状态:")
            
            # 简化的状态显示
            for task_name in task_names:
                auto_task_name = f"{task_name}_periodic"
                state = scheduler._get_task_state(auto_task_name)
                
                if state:
                    status = state.get('status', 'unknown')
                    run_count = state.get('run_count', 0)
                    last_status = state.get('last_run_status')
                    
                    status_icon = "🔄" if status == "executing" else "✅" if last_status == "success" else "❌"
                    console.print(f"   {status_icon} {auto_task_name}: {status} (执行: {run_count}次)")
            
            # 显示简要统计
            total_executions = sum(scheduler._get_task_state(f"{task_name}_periodic").get('run_count', 0) 
                                 for task_name in task_names 
                                 if scheduler._get_task_state(f"{task_name}_periodic"))
            console.print(f"   📈 总执行次数: {total_executions}")
    
    # 停止调度器
    scheduler.stop()
    console.print("\n🛑 调度器已停止")


def show_configuration_options():
    """展示配置选项"""
    console.print("\n[bold cyan]=== 自动周期性任务配置选项 ===[/bold cyan]")
    
    config_examples = """
    [bold green]1. 基本配置（启用自动任务）:[/bold green]
    
    config = {
        'default_config': {
            'auto_create_periodic_tasks': True,  # 启用自动创建
            'periodic_task_config': {
                'interval': 60,      # 执行间隔（秒）
                'symbols': ['BTC/USDT'],  # 监控的交易对
                'timeframe': '1h',   # 时间框架
                'exchange_name': 'binance'  # 交易所名称
            }
        }
    }
    
    [bold green]2. 高级配置（自定义参数）:[/bold green]
    
    config = {
        'exchange_name': 'binance',
        'api_key': 'your_api_key',
        'default_config': {
            'auto_create_periodic_tasks': True,
            'periodic_task_config': {
                'interval': 300,     # 5分钟间隔
                'symbols': [
                    'BTC/USDT', 'ETH/USDT', 'BNB/USDT',
                    'ADA/USDT', 'DOT/USDT', 'LINK/USDT'
                ],
                'timeframe': '1h',
                'exchange_name': 'binance',
                'enable_rate_limit': True,
                'rate_limit': 1200   # 每分钟请求限制
            }
        }
    }
    
    [bold green]3. 多时间框架配置:[/bold green]
    
    config = {
        'default_config': {
            'auto_create_periodic_tasks': True,
            'periodic_task_config': {
                'interval': 60,
                'symbols': ['BTC/USDT'],
                'timeframe': '1h',
                'exchange_name': 'binance'
            },
            'additional_tasks': [
                {
                    'interval': 300,
                    'symbols': ['BTC/USDT'],
                    'timeframe': '1d',
                    'task_name_suffix': 'daily'
                },
                {
                    'interval': 900,
                    'symbols': ['BTC/USDT'],
                    'timeframe': '4h',
                    'task_name_suffix': '4h'
                }
            ]
        }
    }
    """
    
    console.print(config_examples)


def main():
    """主函数"""
    console.print("=" * 80)
    console.print("[bold cyan]ChronoForge 自动创建周期性任务功能演示[/bold cyan]")
    console.print("=" * 80)
    
    # 展示配置选项
    show_configuration_options()
    
    # 创建集成调度器
    console.print("\n[bold cyan]=== 初始化系统 ===[/bold cyan]")
    scheduler = IntegratedScheduler(max_workers=4)
    console.print("✅ 集成调度器创建完成")
    
    # 创建带自动任务配置的数据源
    crypto_data_source = create_crypto_data_source_with_auto_tasks()
    fred_data_source = create_fred_data_source_with_auto_tasks()
    fgi_data_source = create_bitcoin_fgi_data_source_with_auto_tasks()
    
    # 注册数据源并创建自动任务
    task_names = register_data_sources_and_create_tasks(
        scheduler, 
        crypto_data_source, 
        fred_data_source, 
        fgi_data_source
    )
    
    # 显示自动创建的任务
    display_auto_created_tasks(scheduler)
    
    # 监控任务执行
    monitor_task_execution(scheduler, task_names, duration=15)
    
    # 显示最终结果
    console.print("\n[bold cyan]=== 最终结果统计 ===[/bold cyan]")
    
    final_stats = []
    for task_name in task_names:
        auto_task_name = f"{task_name}_periodic"
        state = scheduler._get_task_state(auto_task_name)
        if state:
            final_stats.append({
                'task_name': auto_task_name,
                'run_count': state.get('run_count', 0),
                'last_status': state.get('last_run_status', 'unknown')
            })
    
    # 创建结果表格
    result_table = Table(title="任务执行统计", show_header=True, header_style="bold magenta")
    result_table.add_column("任务名称")
    result_table.add_column("执行次数")
    result_table.add_column("最后状态")
    
    for stat in final_stats:
        result_table.add_row(
            stat['task_name'],
            str(stat['run_count']),
            f"[green]{stat['last_status']}[/green]" if stat['last_status'] == 'success' else f"[red]{stat['last_status']}[/red]"
        )
    
    console.print(result_table)
    
    # 总结
    total_runs = sum(stat['run_count'] for stat in final_stats)
    success_count = sum(1 for stat in final_stats if stat['last_status'] == 'success')
    
    console.print(f"\n[bold green]✅ 演示完成！[/bold green]")
    console.print(f"   总执行次数: {total_runs}")
    console.print(f"   成功任务数: {success_count}/{len(final_stats)}")
    console.print(f"   自动创建任务数: {len(final_stats)}")
    
    console.print("\n" + "=" * 80)
    console.print("[bold cyan]ChronoForge 自动创建周期性任务功能演示完成[/bold cyan]")
    console.print("=" * 80)


if __name__ == "__main__":
    main()