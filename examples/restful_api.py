#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import subprocess
import sys
import time
from datetime import datetime, timedelta
import requests
import traceback
from rich.console import Console
from rich.table import Table
from rich.logging import RichHandler

# 配置rich日志
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    handlers=[RichHandler(rich_tracebacks=True)]
)
logger = logging.getLogger(__name__)
console = Console()

# 状态颜色映射
STATUS_COLORS = {
    "created": "blue",
    "replaced": "yellow",
    "waiting": "cyan",
    "running": "green",
    "executing": "bright_green",
    "completed": "bright_blue",
    "failed": "red",
    "deleted": "gray"
}

# API基础URL
API_BASE_URL = "http://localhost:8000/api"
# API_BASE_URL = "http://192.168.1.22:8000/api"

# 直接使用正确格式的symbol，包含交易所信息
crypto_symbols = ['binance:BTC/USDT', 'okx:ETH/USDT']

fred_api_key = "64a2def57e5b65c216e35e580f78f0f7"

fred_daily_rates = [
    "IORB",
    "RRPONTSYAWARD",
    "EFFR",
    "SOFR",
    "DTB4WK",
    "DTB3",
    "DTB6",
    "DTB1YR",
    "DGS2",
    "DGS5",
    "DGS10",
    "DGS20",
    "DGS30"
]

fred_daily_volumes = [
    "RRPONTSYD",
    "EFFRVOL",
    "SOFRVOL",
    "RPONTSYD",
    "RPMBSD",
    "RPAGYD",
]

fred_weekly_volumes = [
    "WRBWFRBL",
]

um_future_symbols = [
    "BTCUSDT",
    "ETHUSDT",
    "LINKUSDT",
    "XLMUSDT",
    "XMRUSDT",
    "UNIUSDT",
    "AAVEUSDT",
    "COMPUSDT",
    "CRVUSDT"
]

global_market_symbols = [
    "^GSPC",  # S&P 500
    "^DJI",   # Dow Jones
    "^IXIC",  # NASDAQ
    "^RUT",   # Russell 2000
    "^VIX",   # 波动率指数
    "DX=F",   # 美元指数
    "GC=F",   # 黄金价格
    "SI=F",   # 白银价格
    "HG=F",   # 铜价格
    "CNY=X",     # 人民币/美元汇率
    "JPY=X"      # 日元/美元汇率
]


def check_service_running():
    """
    检查ChronoForge服务是否正在运行
    """
    try:
        response = requests.get(f"{API_BASE_URL}/status", timeout=3)
        if response.status_code == 200:
            console.print("[green]✅ ChronoForge服务已经在运行[/green]")
            return True
        else:
            console.print(f"[yellow]⚠️  ChronoForge服务返回错误状态: {response.status_code}[/yellow]")
            return False
    except requests.exceptions.ConnectionError:
        console.print("[yellow]⚠️  ChronoForge服务未在运行[/yellow]")
        return False
    except Exception as e:
        console.print(f"[red]❌ 检查服务状态时出错: {e}[/red]")
        return False


def start_chronoforge_service():
    """
    启动ChronoForge服务
    """
    # 先检查服务是否已经在运行
    if check_service_running():
        return None

    console.print("[bold blue]启动ChronoForge服务...[/bold blue]")
    try:
        # 使用subprocess启动服务
        process = subprocess.Popen(
            [sys.executable, "-m", "chronoforge.cli", "serve"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        # 等待服务启动，同时读取输出
        console.print("[bold blue]等待服务启动...[/bold blue]")
        time.sleep(2)

        # 读取初始输出
        stdout, stderr = process.communicate(timeout=3)
        if stdout:
            console.print(f"[blue]服务输出: {stdout.strip()}[/blue]")
        if stderr:
            console.print(f"[red]服务错误: {stderr.strip()}[/red]")

        # 检查服务是否仍在运行
        if process.poll() is not None:
            console.print(f"[red]服务已退出，退出码: {process.returncode}[/red]")
            return None

        # 检查服务是否启动成功
        try:
            response = requests.get(f"{API_BASE_URL}/status", timeout=5)
            if response.status_code == 200:
                console.print("[green]✅ ChronoForge服务启动成功[/green]")
                return process
            else:
                console.print(f"[red]❌ 服务返回错误状态: {response.status_code} - {response.text}[/red]")
                process.terminate()
                return None
        except requests.exceptions.ConnectionError:
            console.print("[red]❌ 无法连接到ChronoForge服务，启动失败[/red]")
            process.terminate()
            return None
        except requests.exceptions.Timeout:
            console.print("[red]❌ 连接超时，服务可能未启动成功[/red]")
            process.terminate()
            return None

    except subprocess.TimeoutExpired:
        # 如果超时，服务可能仍在运行，继续检查
        console.print("[blue]服务启动中，继续检查...[/blue]")
        try:
            response = requests.get(f"{API_BASE_URL}/status", timeout=5)
            if response.status_code == 200:
                console.print("[green]✅ ChronoForge服务启动成功[/green]")
                return process
            else:
                console.print(f"[red]❌ 服务返回错误状态: {response.status_code} - {response.text}[/red]")
                process.terminate()
                return None
        except Exception as e:
            console.print(f"[red]❌ 检查服务状态时出错: {e}[/red]")
            process.terminate()
            return None
    except Exception as e:
        console.print(f"[red]❌ 启动ChronoForge服务时出错: {e}[/red]")
        traceback.print_exc()
        if 'process' in locals() and process.poll() is None:
            process.terminate()
        return None


def add_tasks():
    """
    向ChronoForge服务添加任务
    """
    logger.info("向ChronoForge服务添加任务...")

    # 定义任务列表
    tasks = [
        {
            "name": "crypto_1d",
            "data_source_name": "CryptoSpotDataSource",
            "data_source_config": {},
            "storage_name": "DUCKDBStorage",
            "storage_config": {
                'db_path': './tmp/geigei.db'
            },
            "time_slot": {
                "start": "00:30",
                "end": "59:00"
            },
            "symbols": crypto_symbols,
            "timeframe": "1d",
            "timerange_str": "20240101-",
        },
        {
            "name": "crypto_4h",
            "data_source_name": "CryptoSpotDataSource",
            "data_source_config": {},
            "storage_name": "DUCKDBStorage",
            "storage_config": {
                'db_path': './tmp/geigei.db'
            },
            "time_slot": {
                "start": "00:30",
                "end": "59:00"
            },
            "symbols": crypto_symbols,
            "timeframe": "4h",
            "timerange_str": "20240101-",
        },
        {
            "name": "crypto_1h",
            "data_source_name": "CryptoSpotDataSource",
            "data_source_config": {},
            "storage_name": "DUCKDBStorage",
            "storage_config": {
                'db_path': './tmp/geigei.db'
            },
            "time_slot": {
                "start": "00:30",
                "end": "59:00"
            },
            "symbols": crypto_symbols,
            "timeframe": "1h",
            "timerange_str": "20240101-",
        },
        {
            "name": "fred_daily_test",
            "data_source_name": "FREDDataSource",
            "data_source_config": {
                "api_key": fred_api_key
            },
            "storage_name": "DUCKDBStorage",
            "storage_config": {
                'db_path': './tmp/geigei.db'
            },
            "time_slot": {
                "start": "00:30",
                "end": "59:00"
            },
            "symbols": fred_daily_rates + fred_daily_volumes,
            "timeframe": "1d",
            "timerange_str": "20240101-",
        },
        {
            "name": "fred_weekly_test",
            "data_source_name": "FREDDataSource",
            "data_source_config": {
                "api_key": fred_api_key
            },
            "storage_name": "DUCKDBStorage",
            "storage_config": {
                'db_path': './tmp/geigei.db'
            },
            "time_slot": {
                "start": "00:30",
                "end": "59:00"
            },
            "symbols": fred_weekly_volumes,
            "timeframe": "1w",
            "timerange_str": "20240101-",
        },
        {
            "name": "crypto_um_future_test",
            "data_source_name": "CryptoUMFutureDataSource",
            "data_source_config": {},
            "storage_name": "DUCKDBStorage",
            "storage_config": {
                'db_path': './tmp/geigei.db'
            },
            "time_slot": {
                "start": "00:30",
                "end": "59:00"
            },
            "symbols": um_future_symbols,
            "timeframe": "1h",
            "timerange_str": f"{(datetime.now() - timedelta(days=29)).strftime('%Y%m%d')}-",  # only support last 30 days
        },
        {
            "name": "bitcoin_fgi",
            "data_source_name": "BitcoinFGIDataSource",
            "data_source_config": {},
            "storage_name": "DUCKDBStorage",
            "storage_config": {
                'db_path': './tmp/geigei.db'
            },
            "time_slot": {
                "start": "00:30",
                "end": "59:00"
            },
            "symbols": ["bitcoin_fgi"],
            "timeframe": "1d",
            "timerange_str": "20240101-",
        },
        {
            "name": "global_market_test",
            "data_source_name": "GlobalMarketDataSource",
            "data_source_config": {},
            "storage_name": "DUCKDBStorage",
            "storage_config": {
                'db_path': './tmp/geigei.db'
            },
            "time_slot": {
                "start": "00:30",
                "end": "59:00"
            },
            "symbols": global_market_symbols,
            "timeframe": "1d",
            "timerange_str": "20240101-",
        }
    ]

    # 发送每个任务
    console.print("\n[bold magenta]添加任务列表:[/bold magenta]")
    for task in tasks:
        try:
            response = requests.post(f"{API_BASE_URL}/tasks", json=task)
            if response.status_code == 200:
                console.print(f"[green]✅ 任务 {task['name']} 添加成功[/green]")
            else:
                console.print(f"[red]❌ 任务 {task['name']} 添加失败: "
                              f"{response.status_code} - {response.text}[/red]")
        except Exception as e:
            console.print(f"[red]❌ 添加任务 {task['name']} 时出错: {e}[/red]")


def get_status():
    """
    获取ChronoForge服务状态
    """
    try:
        response = requests.get(f"{API_BASE_URL}/status")
        if response.status_code == 200:
            status = response.json()
            return status
        else:
            logger.error(f"获取服务状态失败: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logger.error(f"获取服务状态时出错: {e}")
        return None


def get_tasks_status():
    """
    获取所有任务状态
    """
    try:
        response = requests.get(f"{API_BASE_URL}/status/tasks")
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"获取任务状态失败: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logger.error(f"获取任务状态时出错: {e}")
        return None


def monitor_task_status(duration=30, interval=2):
    """
    监控任务状态变化

    Args:
        duration: 监控持续时间（秒）
        interval: 监控间隔（秒）
    """
    console.print("\n[bold cyan]开始监控任务状态变化...[/bold cyan]")

    # 记录任务状态变化
    task_status_history = {}

    # 开始监控
    end_time = time.time() + duration
    while time.time() < end_time:
        # 获取最新状态
        tasks_status = get_tasks_status()
        if tasks_status:
            # 创建新表格
            table = Table(title="任务状态监控", show_header=True, header_style="bold magenta")
            table.add_column("任务名称", style="bold")
            table.add_column("状态", justify="center")
            table.add_column("创建时间")
            table.add_column("最后更新")
            table.add_column("执行次数", justify="center")
            table.add_column("上次执行")
            table.add_column("上次状态")

            # 添加任务状态到表格
            for task_name, task_status in tasks_status.items():
                status = task_status.get("status", "idle")
                created_at = task_status.get("created_at")
                last_updated_at = task_status.get("last_updated_at")
                run_count = task_status.get("run_count", 0)
                last_run_time = task_status.get("last_run_time")
                last_run_status = task_status.get("last_run_status")

                # 格式化时间
                def format_time(timestamp):
                    if timestamp:
                        return time.strftime("%H:%M:%S", time.localtime(timestamp))
                    return "-"

                # 获取状态颜色
                status_color = STATUS_COLORS.get(status, "white")

                # 添加行
                table.add_row(
                    task_name,
                    f"[{status_color}]{status}[/{status_color}]",
                    format_time(created_at),
                    format_time(last_updated_at),
                    str(run_count),
                    format_time(last_run_time),
                    last_run_status or "-"
                )

                # 检查状态变化
                if task_name not in task_status_history:
                    task_status_history[task_name] = status
                elif task_status_history[task_name] != status:
                    console.print(f"[yellow]任务 {task_name} 状态变化: "
                                  f"{task_status_history[task_name]} → {status}[/yellow]")
                    task_status_history[task_name] = status

            # 打印表格
            console.clear()
            console.print("\n[bold cyan]开始监控任务状态变化...[/bold cyan]")
            console.print(table)

        # 等待下一次检查
        time.sleep(interval)

    console.print("\n[bold cyan]任务状态监控结束[/bold cyan]")


def main():
    """
    主函数
    """
    process = None
    try:
        # 显示页眉
        console.print("=" * 60)
        console.print("[bold cyan]ChronoForge RESTful API 示例[/bold cyan]")
        console.print("=" * 60)

        # 启动服务（如果未运行）
        process = start_chronoforge_service()

        # 无论服务是否是本次启动，都继续执行后续操作
        # 添加任务
        add_tasks()

        # 监控任务状态变化
        monitor_task_status(duration=300, interval=2)

        # 只有当本次启动了服务时，才停止服务
        if process:
            console.print("\n[bold blue]停止ChronoForge服务...[/bold blue]")
            process.terminate()
            process.wait()
            console.print("[green]✅ ChronoForge服务已停止[/green]")
        else:
            console.print("\n[yellow]⚠️  服务不是本次启动，不停止[/yellow]")

        # 显示页脚
        console.print("=" * 60)
        console.print("[bold cyan]ChronoForge RESTful API 示例执行完成[/bold cyan]")
        console.print("=" * 60)

        return True

    except KeyboardInterrupt:
        console.print("\n[yellow]⚠️  检测到用户中断[/yellow]")
        if process:
            process.terminate()
            process.wait()
            console.print("[green]✅ ChronoForge服务已停止[/green]")
        return True
    except Exception as e:
        console.print(f"\n[red]❌ 主函数执行异常: {type(e).__name__}: {e}[/red]")
        traceback.print_exc()
        if process:
            process.terminate()
            process.wait()
            console.print("[green]✅ ChronoForge服务已停止[/green]")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
