#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import subprocess
import sys
import time
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
    "SOLUSDT",
    "LINKUSDT",
    "UNIUSDT",
    "AAVEUSDT",
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
        response = requests.get(f"{API_BASE_URL}/status", timeout=10)
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


def delete_existing_tasks():
    """
    删除现有任务，但保留由scheduler自动添加的任务
    """
    logger.info("删除现有任务，但保留由scheduler自动添加的任务...")

    # 检查服务器是否开启
    if not check_service_running():
        console.print("[red]❌ 服务器未开启，无法删除任务[/red]")
        return

    try:
        # 获取所有任务
        tasks_url = f"{API_BASE_URL}/tasks"
        response = requests.get(tasks_url)
        if response.status_code == 200:
            tasks = response.json().get("tasks", [])
            console.print(f"[green]✅ 获取到 {len(tasks)} 个任务[/green]")

            # 删除非自动添加的任务
            for task in tasks:
                task_name = task["name"]
                # 检查任务是否需要保留
                # 使用is_auto_created标记来判断任务是否由scheduler自动添加
                is_auto_created = task.get("is_auto_created", False)
                if is_auto_created:
                    console.print(f"[yellow]⚠️  保留自动创建的任务: {task_name}[/yellow]")
                    continue

                # 删除任务
                delete_url = f"{API_BASE_URL}/tasks/{task_name}"
                delete_response = requests.delete(delete_url)
                if delete_response.status_code in [200, 204]:
                    console.print(f"[green]✅ 删除任务成功: {task_name}[/green]")
                else:
                    console.print(f"[red]❌ 删除任务失败: {task_name} - "
                                  f"{delete_response.status_code} - {delete_response.text}[/red]")
        else:
            console.print(f"[red]❌ 获取任务列表失败: {response.status_code} - {response.text}[/red]")
    except Exception as e:
        console.print(f"[red]❌ 删除任务时出错: {e}[/red]")


def start_tickers_scheduler(interval_seconds=30):
    """
    启动tickers定时获取调度器

    Args:
        interval_seconds: 获取间隔（秒），默认30秒
    """
    logger.info(f"启动tickers定时获取调度器（每{interval_seconds}秒）...")

    # 检查服务器是否开启
    if not check_service_running():
        console.print("[red]❌ 服务器未开启，无法启动tickers调度器[/red]")
        return False

    try:
        # 先尝试直接调用数据更新API获取tickers
        console.print(f"[blue]启动tickers定时获取（每{interval_seconds}秒）...[/blue]")

        # 启动后台线程定期获取tickers
        import threading

        def tickers_worker():
            """后台tickers获取工作线程"""
            # 跟踪哪些交易所可用
            available_exchanges = ["binance", "okx"]
            restricted_exchanges = set()

            while getattr(threading.current_thread(), "_running", True):
                try:
                    # 调用插件方法获取tickers
                    for exchange in available_exchanges:
                        # 跳过已知被限制的交易所
                        if exchange in restricted_exchanges:
                            continue

                        try:
                            delegate_call_url = f"{API_BASE_URL}/plugins/delegate-call"
                            request_data = {
                                "plugin_name": "CryptoSpotDataSource",
                                "plugin_type": "data_source",
                                "function_name": "tickers",
                                "kwargs": {
                                    "exchange_name": exchange,
                                    "quote": "USDT"
                                }
                            }
                            response = requests.post(
                                delegate_call_url,
                                json=request_data,
                                timeout=10
                            )
                            if response.status_code == 200:
                                result = response.json()
                                if result.get("success"):
                                    tickers_count = len(result.get("result", {}))
                                    logger.debug(
                                        f"✅ {exchange} tickers获取成功: "
                                        f"{tickers_count}个交易对"
                                    )
                                else:
                                    logger.warning(
                                        f"⚠️ {exchange} tickers获取失败: "
                                        f"{result.get('detail')}"
                                    )
                            else:
                                error_text = response.text
                                # 检查是否是地区限制错误
                                if ("restricted location" in error_text
                                        or "451" in error_text):
                                    logger.warning(
                                        f"⚠️ {exchange} 被地区限制，"
                                        f"将不再尝试"
                                    )
                                    restricted_exchanges.add(exchange)
                                else:
                                    logger.warning(
                                        f"⚠️ {exchange} tickers请求失败: "
                                        f"{response.status_code}"
                                    )
                        except Exception as e:
                            logger.error(f"❌ 获取{exchange} tickers时出错: {e}")

                    # 如果所有交易所都被限制，停止调度器
                    if len(restricted_exchanges) == len(available_exchanges):
                        logger.error("❌ 所有交易所都被地区限制，停止tickers调度器")
                        break

                    # 等待下一次执行
                    time.sleep(interval_seconds)
                except Exception as e:
                    logger.error(f"❌ tickers工作线程出错: {e}")
                    time.sleep(interval_seconds)

        # 创建并启动后台线程
        tickers_thread = threading.Thread(
            target=tickers_worker,
            name="TickersScheduler",
            daemon=True
        )
        tickers_thread._running = True
        tickers_thread.start()

        console.print(
            f"[green]✅ tickers定时获取已启动（每{interval_seconds}秒）[/green]"
        )

        # 保存线程引用以便后续停止
        global _tickers_thread
        _tickers_thread = tickers_thread

        return True

    except Exception as e:
        console.print(f"[red]❌ 启动tickers调度器时出错: {e}[/red]")
        traceback.print_exc()
        return False


def stop_tickers_scheduler():
    """停止tickers调度器"""
    global _tickers_thread
    if '_tickers_thread' in globals() and _tickers_thread:
        _tickers_thread._running = False
        console.print("[yellow]⚠️ tickers调度器已停止[/yellow]")


def add_crypto_tasks():
    """
    向ChronoForge服务添加任务
    """
    logger.info("向ChronoForge服务添加任务...")

    # 检查服务器是否开启
    if not check_service_running():
        console.print("[red]❌ 服务器未开启，无法添加任务[/red]")
        return

    # 删除现有任务，但保留自动添加的任务
    # delete_existing_tasks()

    # 自动继续执行，无需用户输入
    console.print("[green]✅ 自动继续执行...[/green]")

    # 1. 首先启动tickers定时获取调度器（每30秒）
    tickers_scheduler_started = start_tickers_scheduler(interval_seconds=30)
    if not tickers_scheduler_started:
        console.print("[yellow]⚠️  无法启动tickers调度器，继续使用默认流程[/yellow]")

    # 2. 获取按成交量排序的top 80%交易对
    logger.info("获取按成交量排序的top 80%交易对...")
    crypto_symbols = []
    delegate_call_url = f"{API_BASE_URL}/plugins/delegate-call"

    # 尝试从binance获取，如果失败则尝试okx
    exchanges_to_try = [
        ("binance", "binance"),
        ("okx", "okx")
    ]

    for exchange_name, prefix in exchanges_to_try:
        if crypto_symbols:
            break

        try:
            console.print(f"[blue]尝试从 {exchange_name} 获取交易对...[/blue]")
            request_data = {
                "plugin_name": "CryptoSpotDataSource",
                "plugin_type": "data_source",
                "function_name": "top_volume_symbols",
                "kwargs": {
                    "exchange_name": exchange_name,
                    "quote": "USDT",
                    "top_percent": 80
                }
            }

            response = requests.post(delegate_call_url, json=request_data, timeout=30)
            if response.status_code == 200:
                result = response.json()
                if result.get("success"):
                    symbols = result.get("result", [])
                    # 转换为带交易所前缀的格式
                    crypto_symbols = [f"{prefix}:{symbol}" for symbol in symbols]
                    console.print(
                        f"[green]✅ 成功从 {exchange_name} 获取 "
                        f"{len(crypto_symbols)} 个交易对[/green]"
                    )
                    if crypto_symbols:
                        console.print(
                            f"[green]前10个交易对: {', '.join(crypto_symbols[:10])}[/green]"
                        )
                        break
                else:
                    console.print(
                        f"[yellow]⚠️ {exchange_name} 获取交易对失败: "
                        f"{result.get('detail')}[/yellow]"
                    )
            else:
                error_text = response.text
                # 检查是否是地区限制错误
                if "restricted location" in error_text or "451" in error_text:
                    console.print(
                        f"[yellow]⚠️ {exchange_name} 不可用（地区限制），"
                        f"尝试其他交易所...[/yellow]"
                    )
                else:
                    console.print(
                        f"[yellow]⚠️ {exchange_name} 请求失败: "
                        f"{response.status_code} - {error_text[:100]}[/yellow]"
                    )
        except Exception as e:
            console.print(f"[yellow]⚠️ 获取{exchange_name}交易对时出错: {e}[/yellow]")

    if not crypto_symbols:
        console.print(
            f"[yellow]⚠️  未获取到交易对，使用默认交易对: "
            f"{', '.join(['binance:BTC/USDT', 'binance:ETH/USDT'])}[/yellow]"
        )
        crypto_symbols = ['binance:BTC/USDT', 'binance:ETH/USDT']

    user_input = console.input("\n[bold yellow]确认使用以上交易对吗？(y/n)[/bold yellow]")
    if user_input.strip().lower() != 'y':
        console.print("[red]❌ 用户取消操作[/red]")
        return

    # 定义任务列表 - 只创建1d、4h、1h的获取bars的任务
    tasks = [
        {
            "name": "crypto_1d",
            "data_source_name": "CryptoSpotDataSource",
            "data_source_config": {
                "exchange_name": "binance"
            },
            "storage_name": "LocalFileStorage",
            "storage_config": {
                "base_path": "./data"
            },
            "time_slot": {
                "start": "00:00",
                "end": "23:59"
            },
            "symbols": crypto_symbols,
            "timeframe": "1d",
            "timerange_str": "20240101-",
            "inplace": True
        },
        {
            "name": "crypto_4h",
            "data_source_name": "CryptoSpotDataSource",
            "data_source_config": {
                "exchange_name": "binance"
            },
            "storage_name": "LocalFileStorage",
            "storage_config": {
                "base_path": "./data"
            },
            "time_slot": {
                "start": "00:00",
                "end": "23:59"
            },
            "symbols": crypto_symbols,
            "timeframe": "4h",
            "timerange_str": "20240101-",
            "interval_seconds": 14400,
            "inplace": True
        },
        {
            "name": "crypto_1h",
            "data_source_name": "CryptoSpotDataSource",
            "data_source_config": {
                "exchange_name": "binance"
            },
            "storage_name": "LocalFileStorage",
            "storage_config": {
                "base_path": "./data"
            },
            "time_slot": {
                "start": "00:00",
                "end": "23:59"
            },
            "symbols": crypto_symbols,
            "timeframe": "1h",
            "timerange_str": "20240101-",
            "interval_seconds": 3600,
            "inplace": True
        }
    ]

    # 发送每个任务
    console.print("\n[bold magenta]添加任务列表:[/bold magenta]")
    created_tasks = []
    for task in tasks:
        try:
            response = requests.post(f"{API_BASE_URL}/tasks", json=task)
            if response.status_code == 200:
                console.print(f"[green]✅ 任务 {task['name']} 添加成功[/green]")
                created_tasks.append(task['name'])
            else:
                console.print(f"[red]❌ 任务 {task['name']} 添加失败: "
                              f"{response.status_code} - {response.text}[/red]")
        except Exception as e:
            console.print(f"[red]❌ 添加任务 {task['name']} 时出错: {e}[/red]")

    # 立即执行所有创建的任务
    if created_tasks:
        console.print("\n[bold blue]立即执行任务...[/bold blue]")
        for task_name in created_tasks:
            try:
                response = requests.post(f"{API_BASE_URL}/tasks/{task_name}/start")
                if response.status_code == 200:
                    result = response.json()
                    msg = result.get('message', 'OK')
                    console.print(f"[green]✅ 任务 {task_name} 执行成功: {msg}[/green]")
                else:
                    console.print(f"[red]❌ 任务 {task_name} 执行失败: "
                                  f"{response.status_code} - {response.text}[/red]")
            except Exception as e:
                console.print(f"[red]❌ 执行任务 {task_name} 时出错: {e}[/red]")

    return created_tasks


def add_fred_task():
    """
    向ChronoForge服务添加FRED任务
    """
    logger.info("向ChronoForge服务添加FRED任务...")

    if not check_service_running():
        console.print("[red]❌ 服务器未开启，无法添加任务[/red]")
        return []

    # delete_existing_tasks()
    console.print("[green]✅ 自动继续执行...[/green]")

    tasks = [
        {
            "name": "fred_daily_rates",
            "data_source_name": "FREDDataSource",
            "data_source_config": {
                "api_key": fred_api_key
            },
            "storage_name": "DUCKDBStorage",
            "storage_config": {
                "db_path": ".data/chronoforge.duckdb"
            },
            "time_slot": {
                "start": "00:00",
                "end": "23:59"
            },
            "symbols": fred_daily_rates,
            "timeframe": "1d",
            "timerange_str": "20200101-",
            "inplace": True
        },
        {
            "name": "fred_daily_volumes",
            "data_source_name": "FREDDataSource",
            "data_source_config": {
                "api_key": fred_api_key
            },
            "storage_name": "DUCKDBStorage",
            "storage_config": {
                "db_path": ".data/chronoforge.duckdb"
            },
            "time_slot": {
                "start": "00:00",
                "end": "23:59"
            },
            "symbols": fred_daily_volumes,
            "timeframe": "1d",
            "timerange_str": "20200101-",
            "inplace": True
        },
        {
            "name": "fred_weekly_volumes",
            "data_source_name": "FREDDataSource",
            "data_source_config": {
                "api_key": fred_api_key
            },
            "storage_name": "DUCKDBStorage",
            "storage_config": {
                "db_path": ".data/chronoforge.duckdb"
            },
            "time_slot": {
                "start": "00:00",
                "end": "23:59"
            },
            "symbols": fred_weekly_volumes,
            "timeframe": "1w",
            "timerange_str": "20200101-",
            "inplace": True
        }
    ]

    console.print("\n[bold magenta]添加FRED任务列表:[/bold magenta]")
    created_tasks = []
    for task in tasks:
        try:
            response = requests.post(f"{API_BASE_URL}/tasks", json=task)
            if response.status_code == 200:
                console.print(f"[green]✅ 任务 {task['name']} 添加成功[/green]")
                created_tasks.append(task['name'])
            else:
                console.print(f"[red]❌ 任务 {task['name']} 添加失败: "
                              f"{response.status_code} - {response.text}[/red]")
        except Exception as e:
            console.print(f"[red]❌ 添加任务 {task['name']} 时出错: {e}[/red]")

    if created_tasks:
        console.print("\n[bold blue]立即执行任务...[/bold blue]")
        for task_name in created_tasks:
            try:
                response = requests.post(f"{API_BASE_URL}/tasks/{task_name}/start")
                if response.status_code == 200:
                    result = response.json()
                    msg = result.get('message', 'OK')
                    console.print(f"[green]✅ 任务 {task_name} 执行成功: {msg}[/green]")
                else:
                    console.print(f"[red]❌ 任务 {task_name} 执行失败: "
                                  f"{response.status_code} - {response.text}[/red]")
            except Exception as e:
                console.print(f"[red]❌ 执行任务 {task_name} 时出错: {e}[/red]")

    return created_tasks


def add_global_market_task():
    """
    向ChronoForge服务添加全局市场任务
    """
    logger.info("向ChronoForge服务添加全局市场任务...")

    if not check_service_running():
        console.print("[red]❌ 服务器未开启，无法添加任务[/red]")
        return []

    # delete_existing_tasks()
    # console.print("[green]✅ 自动继续执行...[/green]")

    tasks = [
        {
            "name": "global_market_1d",
            "data_source_name": "GlobalMarketDataSource",
            "data_source_config": {},
            "storage_name": "DUCKDBStorage",
            "storage_config": {
                "db_path": ".data/chronoforge.duckdb"
            },
            "time_slot": {
                "start": "00:00",
                "end": "23:59"
            },
            "symbols": global_market_symbols,
            "timeframe": "1d",
            "timerange_str": "20200101-",
            "inplace": True
        }
    ]

    console.print("\n[bold magenta]添加全局市场任务列表:[/bold magenta]")
    created_tasks = []
    for task in tasks:
        try:
            response = requests.post(f"{API_BASE_URL}/tasks", json=task)
            if response.status_code == 200:
                console.print(f"[green]✅ 任务 {task['name']} 添加成功[/green]")
                created_tasks.append(task['name'])
            else:
                console.print(f"[red]❌ 任务 {task['name']} 添加失败: "
                              f"{response.status_code} - {response.text}[/red]")
        except Exception as e:
            console.print(f"[red]❌ 添加任务 {task['name']} 时出错: {e}[/red]")

    if created_tasks:
        console.print("\n[bold blue]立即执行任务...[/bold blue]")
        for task_name in created_tasks:
            try:
                response = requests.post(f"{API_BASE_URL}/tasks/{task_name}/start")
                if response.status_code == 200:
                    result = response.json()
                    msg = result.get('message', 'OK')
                    console.print(f"[green]✅ 任务 {task_name} 执行成功: {msg}[/green]")
                else:
                    console.print(f"[red]❌ 任务 {task_name} 执行失败: "
                                  f"{response.status_code} - {response.text}[/red]")
            except Exception as e:
                console.print(f"[red]❌ 执行任务 {task_name} 时出错: {e}[/red]")

    return created_tasks


def add_um_future_task():
    """
    向ChronoForge服务添加UM Future任务
    """
    logger.info("向ChronoForge服务添加UM Future任务...")

    if not check_service_running():
        console.print("[red]❌ 服务器未开启，无法添加任务[/red]")
        return []

    delete_existing_tasks()
    console.print("[green]✅ 自动继续执行...[/green]")

    tasks = [
        {
            "name": "um_future_1d",
            "data_source_name": "CryptoUMFutureDataSource",
            "data_source_config": {},
            "storage_name": "DUCKDBStorage",
            "storage_config": {
                "db_path": ".data/chronoforge.duckdb"
            },
            "time_slot": {
                "start": "00:00",
                "end": "23:59"
            },
            "symbols": um_future_symbols,
            "timeframe": "4h",
            "timerange_str": "20240101-",
            "inplace": True
        }
    ]

    console.print("\n[bold magenta]添加UM Future任务列表:[/bold magenta]")
    created_tasks = []
    for task in tasks:
        try:
            response = requests.post(f"{API_BASE_URL}/tasks", json=task)
            if response.status_code == 200:
                console.print(f"[green]✅ 任务 {task['name']} 添加成功[/green]")
                created_tasks.append(task['name'])
            else:
                console.print(f"[red]❌ 任务 {task['name']} 添加失败: "
                              f"{response.status_code} - {response.text}[/red]")
        except Exception as e:
            console.print(f"[red]❌ 添加任务 {task['name']} 时出错: {e}[/red]")

    if created_tasks:
        console.print("\n[bold blue]立即执行任务...[/bold blue]")
        for task_name in created_tasks:
            try:
                response = requests.post(f"{API_BASE_URL}/tasks/{task_name}/start")
                if response.status_code == 200:
                    result = response.json()
                    msg = result.get('message', 'OK')
                    console.print(f"[green]✅ 任务 {task_name} 执行成功: {msg}[/green]")
                else:
                    console.print(f"[red]❌ 任务 {task_name} 执行失败: "
                                  f"{response.status_code} - {response.text}[/red]")
            except Exception as e:
                console.print(f"[red]❌ 执行任务 {task_name} 时出错: {e}[/red]")

    return created_tasks


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


def run_tasks_periodically(task_names, interval_seconds=3600, duration_seconds=300):
    """
    定时执行任务

    Args:
        task_names: 任务名称列表
        interval_seconds: 执行间隔（秒），默认1小时
        duration_seconds: 总运行时间（秒）
    """
    if not task_names:
        console.print("[yellow]⚠️  没有任务需要定时执行[/yellow]")
        return

    console.print(f"\n[bold cyan]开始定时执行任务（每{interval_seconds}秒执行一次）...[/bold cyan]")

    start_time = time.time()
    last_execution_time = 0

    while time.time() - start_time < duration_seconds:
        current_time = time.time()

        # 检查是否需要执行
        if current_time - last_execution_time >= interval_seconds:
            console.print("\n[bold blue]定时触发任务执行...[/bold blue]")

            for task_name in task_names:
                try:
                    response = requests.post(f"{API_BASE_URL}/tasks/{task_name}/start")
                    if response.status_code == 200:
                        result = response.json()
                        msg = result.get('message', 'OK')
                        console.print(f"[green]✅ 任务 {task_name} 执行成功: {msg}[/green]")
                    else:
                        console.print(f"[red]❌ 任务 {task_name} 执行失败: "
                                      f"{response.status_code} - {response.text}[/red]")
                except Exception as e:
                    console.print(f"[red]❌ 执行任务 {task_name} 时出错: {e}[/red]")

            last_execution_time = current_time

        # 显示状态监控
        tasks_status = get_tasks_status()
        if tasks_status:
            console.print("\n[dim]任务状态监控（按Ctrl+C停止）...[/dim]")
            for task_name in task_names:
                if task_name in tasks_status:
                    status = tasks_status[task_name].get("status", "idle")
                    run_count = tasks_status[task_name].get("run_count", 0)
                    console.print(f"  {task_name}: {status} (执行次数: {run_count})")

        # 等待下一次检查
        time.sleep(5)

    console.print("\n[bold cyan]定时任务执行结束[/bold cyan]")


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
        # 添加任务并获取创建的任务列表
        # add_crypto_tasks()
        # add_global_market_task()
        add_um_future_task()
        # add_fred_task()

        # # 定时执行任务（每1小时执行一次，总共运行5分钟示例）
        # # 实际使用时可以调整 duration_seconds 为更长的时间，如 86400（24小时）
        # run_tasks_periodically(
        #     task_names=created_tasks,
        #     interval_seconds=3600,  # 每小时执行一次
        #     duration_seconds=300    # 运行5分钟示例
        # )

        # 监控任务状态变化
        monitor_task_status(duration=60, interval=2)

        # 停止tickers调度器
        stop_tickers_scheduler()

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
        stop_tickers_scheduler()
        if process:
            process.terminate()
            process.wait()
            console.print("[green]✅ ChronoForge服务已停止[/green]")
        return True
    except Exception as e:
        console.print(f"\n[red]❌ 主函数执行异常: {type(e).__name__}: {e}[/red]")
        traceback.print_exc()
        stop_tickers_scheduler()
        if process:
            process.terminate()
            process.wait()
            console.print("[green]✅ ChronoForge服务已停止[/green]")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
