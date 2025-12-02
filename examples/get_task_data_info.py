#!/usr/bin/env python3
"""
示例：获取所有任务下的所有数据名称以及数据起始和结束时间
"""

import requests
import argparse
from rich.console import Console
from rich.table import Table

console = Console()


def get_tasks(host: str = "localhost", port: int = 8000):
    """
    获取所有任务列表

    Args:
        host: 服务器主机名
        port: 服务器端口

    Returns:
        list: 任务列表
    """
    url = f"http://{host}:{port}/api/tasks"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data.get("tasks", [])
    except requests.exceptions.ConnectionError:
        console.print(f"[red bold]连接失败:[/red bold] 无法连接到 {host}:{port}，请确保 ChronoForge 服务正在运行")
        return []
    except requests.exceptions.HTTPError as e:
        console.print(f"[red bold]HTTP 错误:[/red bold] {e}")
        return []
    except Exception as e:
        console.print(f"[red bold]错误:[/red bold] {e}")
        return []


def get_task_data_info(task_name: str, host: str = "localhost", port: int = 8000):
    """
    获取任务下的所有数据名称以及数据起始和结束时间

    Args:
        task_name: 任务名称
        host: 服务器主机名
        port: 服务器端口
    """
    url = f"http://{host}:{port}/api/tasks/{task_name}/data"

    try:
        response = requests.get(url)
        response.raise_for_status()

        data = response.json()

        console.print(f"\n[bold green]获取任务 {task_name} 的数据信息成功[/bold green]")
        console.print(f"[bold]任务名称:[/bold] {data['task_name']}")
        console.print(f"[bold]数据总数:[/bold] {data['total']}")

        # 创建表格显示数据信息
        table = Table(title="任务数据信息")
        table.add_column("数据名称", style="cyan")
        table.add_column("交易对", style="magenta")
        table.add_column("时间周期", style="yellow")
        table.add_column("起始时间", style="green")
        table.add_column("结束时间", style="red")

        for item in data['data_info']:
            table.add_row(
                item['data_name'],
                item['symbol'],
                item['timeframe'],
                item['start_time'],
                item['end_time']
            )

        console.print(table)

        return data
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            console.print(f"[red bold]错误:[/red bold] 任务 {task_name} 不存在")
        else:
            console.print(f"[red bold]HTTP 错误:[/red bold] {e}")
        return None
    except Exception as e:
        console.print(f"[red bold]错误:[/red bold] {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description="获取所有任务下的所有数据名称以及数据起始和结束时间")
    parser.add_argument("--host", type=str, default="localhost", help="服务器主机名")
    parser.add_argument("--port", type=int, default=8000, help="服务器端口")

    args = parser.parse_args()

    console.print("=" * 80)
    console.print("[bold cyan]ChronoForge 任务数据信息获取[/bold cyan]")
    console.print("=" * 80)

    # 获取所有任务列表
    tasks = get_tasks(args.host, args.port)

    if not tasks:
        console.print("\n[red bold]没有找到任何任务[/red bold]")
        return

    console.print(f"\n[bold green]找到 {len(tasks)} 个任务[/bold green]")

    total_data_count = 0
    processed_tasks = 0
    successful_tasks = 0

    # 遍历所有任务
    for task in tasks:
        task_name = task['name']
        processed_tasks += 1

        console.print(f"\n[bold]处理任务 {processed_tasks}/{len(tasks)}: {task_name}[/bold]")
        console.print(f"[dim]数据来源:[/dim] {task['data_source_name']}")
        console.print(f"[dim]存储方式:[/dim] {task['storage_name']}")

        data_info = get_task_data_info(task_name, args.host, args.port)
        if data_info:
            successful_tasks += 1
            total_data_count += data_info['total']

    console.print("\n" + "=" * 80)

    # 显示统计信息
    summary_table = Table(title="统计信息")
    summary_table.add_column("项目", style="bold")
    summary_table.add_column("数值")
    summary_table.add_row("总任务数", str(len(tasks)))
    summary_table.add_row("已处理任务数", str(processed_tasks))
    summary_table.add_row("成功获取数据任务数", str(successful_tasks))
    summary_table.add_row("总数据数量", str(total_data_count))

    console.print(summary_table)
    console.print("=" * 80)


if __name__ == "__main__":
    main()
