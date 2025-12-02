#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import time
import requests
from rich.console import Console
from rich.table import Table
from rich import box

# 配置rich控制台
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


def get_server_status(base_url):
    """
    获取服务器状态

    Args:
        base_url: 服务器基础URL

    Returns:
        dict: 服务器状态信息
    """
    try:
        response = requests.get(f"{base_url}/status", timeout=5)
        if response.status_code == 200:
            return response.json()
        else:
            console.print(f"[red]获取服务器状态失败: {response.status_code} - {response.text}[/red]")
            return None
    except requests.exceptions.ConnectionError:
        console.print(f"[red]无法连接到服务器: {base_url}[/red]")
        return None
    except Exception as e:
        console.print(f"[red]获取服务器状态时出错: {e}[/red]")
        return None


def get_tasks_status(base_url):
    """
    获取所有任务状态

    Args:
        base_url: 服务器基础URL

    Returns:
        dict: 任务状态信息
    """
    try:
        response = requests.get(f"{base_url}/status/tasks", timeout=5)
        if response.status_code == 200:
            return response.json()
        else:
            console.print(f"[red]获取任务状态失败: {response.status_code} - {response.text}[/red]")
            return None
    except Exception as e:
        console.print(f"[red]获取任务状态时出错: {e}[/red]")
        return None


def monitor_server(base_url, duration=None, interval=2):
    """
    监控服务器状态

    Args:
        base_url: 服务器基础URL
        duration: 监控持续时间（秒），None表示不自动停止
        interval: 监控间隔（秒）
    """
    # 记录任务状态变化
    task_status_history = {}

    # 开始监控
    end_time = time.time() + duration if duration else None
    while True:
        # 检查是否达到结束时间
        if end_time and time.time() >= end_time:
            console.print("\n[bold green]监控结束[/bold green]")
            break

        # 清空终端
        console.clear()

        # 打印页眉
        console.print("=" * 70)
        console.print(f"[bold cyan]ChronoForge 服务器监控[/bold cyan]")
        console.print(f"[bold cyan]监控地址: {base_url}[/bold cyan]")
        console.print(f"[bold cyan]监控间隔: {interval}秒[/bold cyan]")
        if duration:
            remaining_time = max(0, int(end_time - time.time()))
            console.print(f"[bold cyan]监控时长: {duration}秒 (剩余: {remaining_time}秒)[/bold cyan]")
        else:
            console.print(f"[bold cyan]监控时长: 持续监控（按 Ctrl+C 停止）[/bold cyan]")
        console.print("=" * 70)

        # 获取服务器状态
        server_status = get_server_status(base_url)
        if server_status:
            # 创建表格
            table = Table(title="服务器状态监控", box=box.SQUARE, header_style="bold magenta")
            table.add_column("监控时间", style="bold")
            table.add_column("服务状态", justify="center")
            table.add_column("任务数量", justify="center")
            table.add_column("运行中任务", justify="center")
            table.add_column("支持的数据源", justify="left")
            table.add_column("支持的存储", justify="left")

            # 添加当前时间
            current_time = time.strftime("%Y-%m-%d %H:%M:%S")

            # 添加服务器状态行
            table.add_row(
                current_time,
                f"[{STATUS_COLORS.get(server_status['status'], 'white')}]{server_status['status']}[/{STATUS_COLORS.get(server_status['status'], 'white')}]",
                str(server_status['tasks_count']),
                str(server_status['running_tasks_count']),
                ", ".join(server_status['supported_data_sources']),
                ", ".join(server_status['supported_storages'])
            )

            # 显示任务状态
            console.print("\n[bold magenta]任务状态详情:[/bold magenta]")
            task_table = Table(title="任务状态列表", box=box.SQUARE, header_style="bold magenta")
            task_table.add_column("任务名称", style="bold")
            task_table.add_column("状态", justify="center")
            task_table.add_column("创建时间")
            task_table.add_column("最后更新")
            task_table.add_column("执行次数", justify="center")
            task_table.add_column("上次执行")
            task_table.add_column("上次状态")

            # 获取任务状态
            tasks_status = get_tasks_status(base_url)
            if tasks_status:
                for task_name, task_status in tasks_status.items():
                    status = task_status.get("status", "unknown")
                    created_at = time.strftime("%H:%M:%S", time.localtime(task_status.get("created_at", 0)))
                    last_updated = time.strftime("%H:%M:%S", time.localtime(task_status.get("last_updated_at", 0)))
                    run_count = task_status.get("run_count", 0)
                    last_run_time = time.strftime("%H:%M:%S", time.localtime(task_status.get("last_run_time", 0)))
                    last_status = task_status.get("last_run_status", "unknown")

                    task_table.add_row(
                        task_name,
                        f"[{STATUS_COLORS.get(status, 'white')}]{status}[/{STATUS_COLORS.get(status, 'white')}]",
                        created_at,
                        last_updated,
                        str(run_count),
                        last_run_time,
                        f"[{STATUS_COLORS.get(last_status, 'white')}]{last_status}[/{STATUS_COLORS.get(last_status, 'white')}]"
                    )
                console.print(task_table)
            else:
                console.print("[yellow]没有任务状态信息[/yellow]")

            # 检查任务状态变化
            for task_name, task_status in tasks_status.items():
                status = task_status.get("status", "unknown")
                if task_name not in task_status_history:
                    task_status_history[task_name] = status
                elif task_status_history[task_name] != status:
                    console.print(f"[yellow]任务 {task_name} 状态变化: {task_status_history[task_name]} → {status}[/yellow]")
                    task_status_history[task_name] = status

        # 等待下一次检查
        time.sleep(interval)


def main():
    """
    主函数
    """
    parser = argparse.ArgumentParser(description="ChronoForge 服务器监控工具")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="服务器主机地址")
    parser.add_argument("--port", type=int, default=8000, help="服务器端口")
    parser.add_argument("--duration", type=int, default=None, help="监控持续时间（秒），默认不自动停止")
    parser.add_argument("--interval", type=int, default=2, help="监控间隔（秒）")
    parser.add_argument("--api-path", type=str, default="/api", help="API路径前缀")

    args = parser.parse_args()

    # 构建基础URL
    base_url = f"http://{args.host}:{args.port}{args.api_path}"

    try:
        monitor_server(base_url, args.duration, args.interval)
    except KeyboardInterrupt:
        console.print("\n[bold green]监控已停止[/bold green]")
    except Exception as e:
        console.print(f"[red]监控过程中发生错误: {e}[/red]")


if __name__ == "__main__":
    main()
