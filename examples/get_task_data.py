#!/usr/bin/env python3
"""
示例：获取任务数据
"""

import requests
import argparse
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
import pandas as pd

console = Console()


def get_task_data(
    task_name,
    host="localhost",
    port=8000,
    data_name=None,
    symbol=None,
    start_time=None,
    end_time=None,
    limit=1000
):
    """
    获取任务数据

    Args:
        task_name: 任务名称
        host: 服务器主机名
        port: 服务器端口
        data_name: 数据名称，如"binance:BTC/USDT_1d"
        symbol: 交易对，如"binance:BTC/USDT"
        start_time: 起始时间，格式为"YYYY-MM-DD HH:MM:SS"
        end_time: 结束时间，格式为"YYYY-MM-DD HH:MM:SS"
        limit: 返回数据的最大条数
    """
    # 构建URL和参数
    url = f"http://{host}:{port}/api/tasks/{task_name}/data"
    params = {
        "data_name": data_name,
        "symbol": symbol,
        "start_time": start_time,
        "end_time": end_time,
        "limit": limit
    }

    # 移除None值
    params = {k: v for k, v in params.items() if v is not None}

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()

        data = response.json()

        return data
    except requests.exceptions.ConnectionError:
        console.print(f"[red bold]连接失败:[/red bold] 无法连接到 {host}:{port}，请确保 ChronoForge 服务正在运行")
        return None
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            console.print(f"[red bold]错误:[/red bold] 任务 {task_name} 不存在")
        else:
            console.print(f"[red bold]HTTP 错误:[/red bold] {e}")
        return None
    except Exception as e:
        console.print(f"[red bold]错误:[/red bold] {e}")
        return None


def display_data(data):
    """
    显示获取的数据

    Args:
        data: API返回的数据
    """
    if not data:
        return

    console.print(Panel.fit(
        f"[bold cyan]任务数据获取结果[/bold cyan]\n"
        f"任务名称: {data['task_name']}\n"
        f"数据总数: {data['total']}\n"
        f"返回条数: {len(data['data'])}\n"
        f"限制条数: {data['limit']}",
        title="数据概览",
        border_style="blue"
    ))

    if not data['data']:
        console.print("\n[yellow]没有找到数据[/yellow]")
        return

    # 显示数据表格
    sample_data = data['data'][0]
    columns = list(sample_data.keys())

    table = Table(title="任务数据", show_header=True, header_style="bold magenta")

    # 添加列
    for col in columns:
        if col == "time":
            table.add_column(col, style="green")
        elif col in ["open", "high", "low", "close"]:
            table.add_column(col, style="yellow", justify="right")
        elif col == "volume":
            table.add_column(col, style="cyan", justify="right")
        else:
            table.add_column(col, style="blue")

    # 添加数据行
    for row in data['data'][-20:]:  # 只显示最后20条数据
        table_row = []
        for col in columns:
            value = row[col]
            if isinstance(value, (int, float)):
                if col in ["open", "high", "low", "close"]:
                    table_row.append(f"{value:.2f}")
                else:
                    table_row.append(f"{value:,}")
            else:
                table_row.append(str(value))
        table.add_row(*table_row)

    console.print(table)

    if len(data['data']) > 20:
        console.print(f"\n[dim]仅显示最后20条数据，共 {len(data['data'])} 条[/dim]")

    # 转换为DataFrame并打印最后5行
    console.print("\n[bold green]DataFrame 最后5行数据:[/bold green]")
    df = pd.DataFrame(data['data'])
    # 转换时间列为UTC datetime
    df['time'] = pd.to_datetime(df['time'], utc=True)
    console.print(df.tail())


def main():
    parser = argparse.ArgumentParser(description="获取任务数据示例")
    parser.add_argument("task_name", type=str, help="任务名称")
    parser.add_argument("--host", type=str, default="localhost", help="服务器主机名")
    parser.add_argument("--port", type=int, default=8000, help="服务器端口")
    parser.add_argument("--data_name", type=str, help="数据名称，如\"binance:BTC/USDT_1d\"")
    parser.add_argument("--symbol", type=str, help="交易对，如\"binance:BTC/USDT\"")
    parser.add_argument("--start_time", type=str, help="起始时间，格式为\"YYYY-MM-DD HH:MM:SS\"")
    parser.add_argument("--end_time", type=str, help="结束时间，格式为\"YYYY-MM-DD HH:MM:SS\"")
    parser.add_argument("--limit", type=int, default=1000, help="返回数据的最大条数")

    args = parser.parse_args()

    console.print("=" * 80)
    console.print("[bold cyan]ChronoForge 任务数据获取[/bold cyan]")
    console.print("=" * 80)

    # 显示请求信息
    console.print("\n[bold]请求信息:[/bold]")
    console.print(f"[dim]任务名称:[/dim] {args.task_name}")
    console.print(f"[dim]服务器地址:[/dim] {args.host}:{args.port}")
    if args.data_name:
        console.print(f"[dim]数据名称:[/dim] {args.data_name}")
    if args.symbol:
        console.print(f"[dim]交易对:[/dim] {args.symbol}")
    if args.start_time:
        console.print(f"[dim]起始时间:[/dim] {args.start_time}")
    if args.end_time:
        console.print(f"[dim]结束时间:[/dim] {args.end_time}")
    console.print(f"[dim]数据条数限制:[/dim] {args.limit}")

    # 获取数据
    console.print("\n[bold]正在获取数据...[/bold]")
    data = get_task_data(
        task_name=args.task_name,
        host=args.host,
        port=args.port,
        data_name=args.data_name,
        symbol=args.symbol,
        start_time=args.start_time,
        end_time=args.end_time,
        limit=args.limit
    )

    if data:
        display_data(data)

    console.print("\n" + "=" * 80)
    console.print("[bold cyan]ChronoForge 任务数据获取完成[/bold cyan]")
    console.print("=" * 80)


if __name__ == "__main__":
    main()
