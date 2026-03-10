#!/usr/bin/env python3
"""
完整工作流示例：ChronoForge综合演示

该示例展示了ChronoForge的完整工作流程，包括：
1. 创建多种数据源
2. 配置自动周期性任务
3. 使用装饰器创建任务
4. 任务调度和执行监控
5. 数据获取和处理
6. 存储和缓存管理
7. API调用和插件功能
"""

import asyncio
import logging
import time
from datetime import datetime, timedelta
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
from rich.logging import RichHandler
from chronoforge import IntegratedScheduler
from chronoforge.data_source import (
    CryptoSpotDataSource, 
    FREDDataSource, 
    BitcoinFGIDataSource,
    GlobalMarketDataSource
)
from chronoforge.task_manager_ultimate import ultimate_task_manager
from chronoforge.decorators import create_task, api_callable
from chronoforge.data_source.base import DataSourceBase
from chronoforge.storage.base import StorageBase
import pandas as pd

# 配置rich日志
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    handlers=[RichHandler(rich_tracebacks=True)]
)
logger = logging.getLogger(__name__)
console = Console()


class CustomDataSource(DataSourceBase):
    """自定义数据源插件，展示装饰器功能"""
    
    @property
    def name(self):
        return "CustomDataSource"
    
    async def fetch(self, symbol, timeframe, start_ts_ms, end_ts_ms=None):
        """实现fetch方法"""
        # 模拟数据获取
        import random
        
        # 生成模拟数据
        data = []
        current_time = start_ts_ms
        
        while current_time < (end_ts_ms or int(time.time() * 1000)):
            # 生成随机价格数据
            base_price = 50000 if 'BTC' in symbol else 3000
            price = base_price + random.uniform(-1000, 1000)
            
            data.append({
                'time': current_time,
                'open': price,
                'high': price + random.uniform(0, 100),
                'low': price - random.uniform(0, 100),
                'close': price + random.uniform(-50, 50),
                'volume': random.uniform(1000, 10000)
            })
            
            # 根据时间框架增加时间
            if timeframe == '1h':
                current_time += 3600 * 1000  # 1小时
            elif timeframe == '1d':
                current_time += 24 * 3600 * 1000  # 1天
            else:
                current_time += 3600 * 1000  # 默认1小时
        
        return pd.DataFrame(data)
    
    @api_callable
    def get_market_summary(self, exchange_name="binance"):
        """获取市场摘要信息"""
        return {
            "exchange": exchange_name,
            "total_symbols": 100,
            "active_trading_pairs": 50,
            "last_updated": datetime.now().isoformat()
        }
    
    @create_task(
        interval=300,  # 5分钟间隔
        symbols=['BTC/USDT', 'ETH/USDT'],
        timeframe='1h',
        storage_name="LocalFileStorage",
        params={'exchange_name': 'binance'}
    )
    async def market_data_periodic(self, exchange_name):
        """周期性获取市场数据任务"""
        console.print(f"[green]📊 执行周期性任务 - 获取{exchange_name}市场数据[/green]")
        
        # 模拟数据获取
        data = pd.DataFrame({
            'time': [int(time.time() * 1000)],
            'open': [50000],
            'high': [51000],
            'low': [49000],
            'close': [50500],
            'volume': [1000]
        })
        
        return data


def create_diversified_data_sources():
    """创建多样化的数据源配置"""
    console.print("\n[bold cyan]=== 创建多样化数据源配置 ===[/bold cyan]")
    
    data_sources = {}
    
    # 1. 加密货币数据源（带自动任务）
    console.print("\n[bold green]1. 创建加密货币数据源（自动任务）[/bold green]")
    crypto_config = {
        'exchange_name': 'binance',
        'default_config': {
            'auto_create_periodic_tasks': True,
            'periodic_task_config': {
                'interval': 60,  # 1分钟间隔
                'symbols': ['BTC/USDT', 'ETH/USDT', 'BNB/USDT'],
                'timeframe': '1h',
                'exchange_name': 'binance'
            }
        }
    }
    data_sources['crypto'] = CryptoSpotDataSource(crypto_config)
    console.print("✅ 加密货币数据源创建完成")
    
    # 2. FRED经济数据源（带自动任务）
    console.print("\n[bold green]2. 创建FRED经济数据源（自动任务）[/bold green]")
    fred_config = {
        'default_config': {
            'auto_create_periodic_tasks': True,
            'periodic_task_config': {
                'interval': 120,  # 2分钟间隔
                'symbols': ['DGS10', 'DGS2', 'EFFR'],
                'timeframe': '1d',
                'exchange_name': 'fred'
            }
        }
    }
    data_sources['fred'] = FREDDataSource(fred_config)
    console.print("✅ FRED经济数据源创建完成")
    
    # 3. 比特币恐惧贪婪指数（带自动任务）
    console.print("\n[bold green]3. 创建比特币恐惧贪婪指数数据源（自动任务）[/bold green]")
    fgi_config = {
        'default_config': {
            'auto_create_periodic_tasks': True,
            'periodic_task_config': {
                'interval': 180,  # 3分钟间隔
                'symbols': ['bitcoin_fgi'],
                'timeframe': '1d',
                'exchange_name': 'alternative'
            }
        }
    }
    data_sources['fgi'] = BitcoinFGIDataSource(fgi_config)
    console.print("✅ 比特币恐惧贪婪指数数据源创建完成")
    
    # 4. 全球市场数据（带自动任务）
    console.print("\n[bold green]4. 创建全球市场数据源（自动任务）[/bold green]")
    market_config = {
        'default_config': {
            'auto_create_periodic_tasks': True,
            'periodic_task_config': {
                'interval': 240,  # 4分钟间隔
                'symbols': ['^GSPC', '^DJI', 'GC=F'],  # S&P500, Dow Jones, Gold
                'timeframe': '1d',
                'exchange_name': 'yahoo'
            }
        }
    }
    data_sources['market'] = GlobalMarketDataSource(market_config)
    console.print("✅ 全球市场数据源创建完成")
    
    # 5. 自定义数据源（带装饰器任务）
    console.print("\n[bold green]5. 创建自定义数据源（装饰器任务）[/bold green]")
    data_sources['custom'] = CustomDataSource({})
    console.print("✅ 自定义数据源创建完成")
    
    return data_sources


def register_all_data_sources(scheduler, data_sources):
    """注册所有数据源并创建自动任务"""
    console.print("\n[bold cyan]=== 注册数据源并创建自动任务 ===[/bold cyan]")
    
    registration_results = {}
    
    for name, data_source in data_sources.items():
        task_name = f"{name}_auto_source"
        
        console.print(f"\n🔄 注册数据源: [bold]{name}[/bold]")
        
        # 注册数据源（自动创建周期性任务）
        ultimate_task_manager.register_data_source_instance(task_name, data_source)
        
        registration_results[name] = {
            'task_name': task_name,
            'data_source': data_source
        }
        
        console.print(f"   ✅ {name} 数据源注册完成")
    
    return registration_results


def create_additional_tasks(scheduler):
    """创建额外的手动任务"""
    console.print("\n[bold cyan]=== 创建额外的手动任务 ===[/bold cyan]")
    
    from chronoforge.utils import TimeSlot
    
    # 创建时间槽任务
    tasks = [
        {
            'name': 'crypto_1h_analysis',
            'data_source_name': 'CryptoSpotDataSource',
            'data_source_config': {'exchange_name': 'binance'},
            'storage_name': 'DuckDBStorage',
            'storage_config': {'db_path': './data/analysis.db'},
            'time_slot': TimeSlot('09:00:00', '17:00:00'),  # 交易时段
            'symbols': ['BTC/USDT', 'ETH/USDT'],
            'timeframe': '1h',
            'timerange_str': '20240101-'
        },
        {
            'name': 'fred_daily_report',
            'data_source_name': 'FREDDataSource',
            'data_source_config': {},
            'storage_name': 'LocalFileStorage',
            'storage_config': {'base_path': './data/reports'},
            'time_slot': TimeSlot('06:00:00', '07:00:00'),  # 早晨报告时段
            'symbols': ['DGS10', 'EFFR', 'SOFR'],
            'timeframe': '1d',
            'timerange_str': '20240101-'
        }
    ]
    
    for task_config in tasks:
        console.print(f"\n📋 创建任务: [bold]{task_config['name']}[/bold]")
        
        # 添加任务到调度器
        scheduler.add_task(
            name=task_config['name'],
            data_source_name=task_config['data_source_name'],
            data_source_config=task_config['data_source_config'],
            storage_name=task_config['storage_name'],
            storage_config=task_config['storage_config'],
            time_slot=task_config['time_slot'],
            symbols=task_config['symbols'],
            timeframe=task_config['timeframe'],
            timerange_str=task_config['timerange_str']
        )
        
        console.print(f"   ✅ 任务 {task_config['name']} 创建完成")
    
    return len(tasks)


def display_system_overview(scheduler):
    """显示系统概览"""
    console.print("\n[bold cyan]=== 系统概览 ===[/bold cyan]")
    
    # 获取所有任务
    all_tasks = scheduler.tasks
    
    # 分类统计
    auto_created_tasks = []
    manual_tasks = []
    periodic_tasks = []
    time_slot_tasks = []
    
    for task_name, task in all_tasks.items():
        if hasattr(task, 'is_auto_created') and task.is_auto_created:
            auto_created_tasks.append(task_name)
        else:
            manual_tasks.append(task_name)
        
        if hasattr(task, 'task_type'):
            if task.task_type == 'periodic':
                periodic_tasks.append(task_name)
            elif task.task_type == 'time_slot':
                time_slot_tasks.append(task_name)
    
    # 创建概览面板
    overview_data = f"""
[bold green]任务统计:[/bold green]
  总任务数: {len(all_tasks)}
  自动创建任务: {len(auto_created_tasks)}
  手动创建任务: {len(manual_tasks)}
  周期性任务: {len(periodic_tasks)}
  时间槽任务: {len(time_slot_tasks)}

[bold blue]数据源类型:[/bold blue]
  加密货币: CryptoSpotDataSource
  经济数据: FREDDataSource  
  情绪指标: BitcoinFGIDataSource
  全球市场: GlobalMarketDataSource
  自定义: CustomDataSource

[bold yellow]存储后端:[/bold yellow]
  本地文件: LocalFileStorage
  DuckDB: DuckDBStorage
  Redis: RedisStorage
  MongoDB: MongoDBStorage
"""
    
    panel = Panel(overview_data, title="ChronoForge 系统概览", border_style="cyan")
    console.print(panel)
    
    return {
        'total_tasks': len(all_tasks),
        'auto_tasks': len(auto_created_tasks),
        'manual_tasks': len(manual_tasks),
        'periodic_tasks': len(periodic_tasks),
        'time_slot_tasks': len(time_slot_tasks)
    }


def monitor_comprehensive_execution(scheduler, duration=120):
    """全面监控任务执行"""
    console.print(f"\n[bold cyan]=== 全面任务执行监控（{duration}秒） ===[/bold cyan]")
    
    # 启动调度器
    scheduler.start()
    console.print("🚀 调度器启动完成")
    
    # 执行统计
    execution_stats = {
        'total_executions': 0,
        'successful_executions': 0,
        'failed_executions': 0,
        'task_performance': {}
    }
    
    # 监控循环
    for i in range(duration):
        time.sleep(1)
        
        # 每5秒显示一次状态（加快反馈）
        if (i + 1) % 5 == 0:
            console.print(f"\n📊 第{i+1}秒 - 系统状态:")
            
            # 简化的状态显示
            all_tasks = scheduler.tasks
            active_count = 0
            completed_count = 0
            
            for task_name, task in all_tasks.items():
                state = scheduler._get_task_state(task_name)
                if state:
                    status = state.get('status', 'unknown')
                    run_count = state.get('run_count', 0)
                    last_status = state.get('last_run_status')
                    
                    # 更新统计
                    execution_stats['total_executions'] += run_count
                    if last_status == 'success':
                        execution_stats['successful_executions'] += 1
                        completed_count += 1
                    elif last_status == 'failed':
                        execution_stats['failed_executions'] += 1
                    
                    if status in ['executing', 'completed']:
                        active_count += 1
                    
                    # 显示状态
                    status_icon = "🔄" if status == "executing" else "✅" if last_status == "success" else "❌"
                    task_type = "(auto)" if hasattr(task, 'is_auto_created') and task.is_auto_created else "(manual)"
                    console.print(f"   {status_icon} {task_name} {task_type}: {status} (执行: {run_count}次)")
            
            # 显示简要统计
            if execution_stats['total_executions'] > 0:
                success_rate = (execution_stats['successful_executions'] / max(1, execution_stats['total_executions'])) * 100
                console.print(f"\n📈 活跃任务: {active_count}/{len(all_tasks)}, 总执行: {execution_stats['total_executions']}, 成功率: {success_rate:.1f}%")
    
    # 停止调度器
    scheduler.stop()
    console.print("\n🛑 调度器停止完成")
    
    return execution_stats


def demonstrate_data_processing():
    """演示数据处理功能"""
    console.print("\n[bold cyan]=== 数据处理演示 ===[/bold cyan]")
    
    # 模拟数据
    import pandas as pd
    import numpy as np
    
    # 创建模拟时间序列数据
    dates = pd.date_range(start='2024-01-01', periods=100, freq='H')
    data = pd.DataFrame({
        'time': dates.astype(np.int64) // 10**6,  # 转换为毫秒时间戳
        'open': 50000 + np.random.randn(100).cumsum() * 1000,
        'high': 51000 + np.random.randn(100).cumsum() * 1000,
        'low': 49000 + np.random.randn(100).cumsum() * 1000,
        'close': 50000 + np.random.randn(100).cumsum() * 1000,
        'volume': np.random.randint(1000, 10000, 100)
    })
    
    console.print("\n📈 原始数据样本:")
    console.print(data.head().to_string())
    
    # 数据处理示例
    console.print("\n🔧 数据处理操作:")
    
    # 1. 计算技术指标
    data['sma_20'] = data['close'].rolling(window=20).mean()
    data['rsi'] = calculate_rsi(data['close'])
    
    console.print("✅ 计算了20周期简单移动平均线和RSI指标")
    
    # 2. 数据清洗
    data_cleaned = data.dropna()
    console.print(f"✅ 数据清洗完成，从{len(data)}条记录到{len(data_cleaned)}条记录")
    
    # 3. 数据聚合
    data['date'] = pd.to_datetime(data['time'], unit='ms').dt.date
    daily_data = data.groupby('date').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).reset_index()
    
    console.print(f"✅ 数据聚合完成，生成{len(daily_data)}条日K线数据")
    
    return data_cleaned


def calculate_rsi(prices, period=14):
    """计算RSI指标"""
    delta = prices.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def show_final_summary(system_overview, execution_stats, processing_results):
    """显示最终总结"""
    console.print("\n" + "=" * 80)
    console.print("[bold cyan]ChronoForge 完整工作流演示 - 最终总结[/bold cyan]")
    console.print("=" * 80)
    
    # 系统配置总结
    config_summary = f"""
[bold green]系统配置:[/bold green]
  数据源类型: 5种 (加密货币、经济数据、情绪指标、全球市场、自定义)
  任务总数: {system_overview['total_tasks']} (自动: {system_overview['auto_tasks']}, 手动: {system_overview['manual_tasks']})
  任务类型: 周期性({system_overview['periodic_tasks']}) + 时间槽({system_overview['time_slot_tasks']})

[bold blue]执行性能:[/bold blue]
  总执行次数: {execution_stats['total_executions']}
  成功执行: {execution_stats['successful_executions']}
  失败执行: {execution_stats['failed_executions']}
  系统稳定性: {'优秀' if execution_stats['failed_executions'] == 0 else '良好'}

[bold yellow]数据处理能力:[/bold yellow]
  技术指标计算: RSI、移动平均线
  数据清洗: 空值处理、异常值检测
  数据聚合: 时间序列重采样
  数据量: {len(processing_results)}条处理记录
"""
    
    summary_panel = Panel(config_summary, title="演示结果总结", border_style="green")
    console.print(summary_panel)
    
    # 功能亮点
    highlights = """
[bold magenta]🎯 功能亮点:[/bold magenta]
  ✅ 自动周期性任务创建 - 数据源注册时自动生成
  ✅ 多数据源集成 - 支持5种不同类型数据源
  ✅ 装饰器任务系统 - @create_task 简化任务创建
  ✅ 智能任务调度 - 时间槽和周期性任务混合调度
  ✅ 实时执行监控 - 完整的任务状态跟踪
  ✅ 数据处理管道 - 技术指标计算和数据清洗
  ✅ 存储后端支持 - 多种存储方案可选
  ✅ API集成能力 - RESTful API完整支持

[bold cyan]🚀 应用场景:[/bold cyan]
  • 量化交易系统 - 多源数据自动获取和处理
  • 金融数据分析 - 实时经济指标监控
  • 风险管理平台 - 市场情绪指标跟踪
  • 投资组合管理 - 全球市场数据整合
  • 研究分析工具 - 自定义数据处理和报告生成
"""
    
    console.print(highlights)
    
    console.print("\n" + "=" * 80)
    console.print("[bold green]✨ ChronoForge 完整工作流演示成功完成！[/bold green]")
    console.print("=" * 80)


def main():
    """主函数"""
    console.print("=" * 80)
    console.print("[bold cyan]ChronoForge 完整工作流演示[/bold cyan]")
    console.print("=" * 80)
    
    console.print("""
[bold yellow]演示内容:[/bold yellow]
1. 多样化数据源创建（自动任务配置）
2. 数据源注册与自动任务生成
3. 手动任务创建（时间槽任务）
4. 自定义数据源（装饰器任务）
5. 任务执行监控与性能分析
6. 数据处理管道演示
7. 系统总结与能力展示
""")
    
    # 1. 创建多样化数据源
    data_sources = create_diversified_data_sources()
    
    # 2. 创建集成调度器
    scheduler = IntegratedScheduler(max_workers=4)
    console.print("\n✅ 集成调度器创建完成")
    
    # 3. 注册数据源并创建自动任务
    registration_results = register_all_data_sources(scheduler, data_sources)
    
    # 4. 创建额外的手动任务
    manual_task_count = create_additional_tasks(scheduler)
    
    # 5. 显示系统概览
    system_overview = display_system_overview(scheduler)
    
    # 6. 全面监控任务执行
    execution_stats = monitor_comprehensive_execution(scheduler, duration=60)
    
    # 7. 演示数据处理功能
    processing_results = demonstrate_data_processing()
    
    # 8. 显示最终总结
    show_final_summary(system_overview, execution_stats, processing_results)


if __name__ == "__main__":
    main()