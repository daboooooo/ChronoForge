#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import sys
import requests
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

# API基础URL
API_BASE_URL = "http://localhost:8000/api"


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


def get_data_source_functions(data_source_name):
    """
    获取数据源的函数列表

    Args:
        data_source_name: 数据源名称

    Returns:
        dict: 数据源函数信息
    """
    try:
        response = requests.get(f"{API_BASE_URL}/plugins/data_source/{data_source_name}/functions")
        if response.status_code == 200:
            return response.json()
        else:
            console.print(f"[red]❌ 获取数据源函数列表失败: {response.status_code} - {response.text}[/red]")
            return None
    except Exception as e:
        console.print(f"[red]❌ 获取数据源函数列表时出错: {e}[/red]")
        return None


def delegate_call_plugin_function(plugin_name, plugin_type, function_name, **kwargs):
    """
    代理调用插件的函数

    Args:
        plugin_name: 插件名称
        plugin_type: 插件类型
        function_name: 函数名称
        **kwargs: 函数参数

    Returns:
        dict: 函数执行结果
    """
    try:
        request_data = {
            "plugin_name": plugin_name,
            "plugin_type": plugin_type,
            "function_name": function_name,
            "kwargs": kwargs
        }
        response = requests.post(f"{API_BASE_URL}/plugins/delegate-call", json=request_data)
        if response.status_code == 200:
            return response.json()
        else:
            console.print(f"[red]❌ 代理调用函数失败: {response.status_code} - {response.text}[/red]")
            return None
    except Exception as e:
        console.print(f"[red]❌ 代理调用函数时出错: {e}[/red]")
        return None


def display_data_source_functions(functions_info):
    """
    展示数据源函数信息

    Args:
        functions_info: 数据源函数信息
    """
    if not functions_info:
        return

    data_source_name = functions_info.get("data_source_name", "未知数据源")
    functions = functions_info.get("functions", [])
    total = functions_info.get("total", 0)

    console.print(f"\n[bold magenta]=== {data_source_name} 数据源函数列表 ({total}) ===[/bold magenta]")

    # 创建表格
    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("函数名称", style="bold")
    table.add_column("返回类型")
    table.add_column("参数数量")
    table.add_column("描述")

    for func in functions:
        name = func.get("name", "未知函数")
        return_type = func.get("return_type", "未知类型")
        parameters = func.get("parameters", [])
        param_count = len(parameters)
        docstring = func.get("docstring", "")

        # 提取描述（第一行）
        description = docstring.split("\n")[0] if docstring else "无描述"

        table.add_row(
            name,
            return_type,
            str(param_count),
            description
        )

    console.print(table)

    # 显示详细参数
    for func in functions:
        name = func.get("name", "未知函数")
        parameters = func.get("parameters", [])

        if parameters:
            console.print(f"\n[bold yellow]{name} 函数参数:[/bold yellow]")
            for param in parameters:
                param_name = param.get("name", "未知参数")
                param_type = param.get("type", "未知类型")
                param_default = param.get("default", "无默认值")
                console.print(f"  - {param_name} ({param_type})  默认值: {param_default}")

        if func.get("docstring", ""):
            console.print(f"\n[italic blue]{name} 函数文档:[/italic blue]")
            console.print(f"  {func['docstring']}")


def display_delegate_call_result(result):
    """
    展示代理调用结果

    Args:
        result: 代理调用结果
    """
    if not result:
        return

    plugin_name = result.get("plugin_name", "未知插件")
    function_name = result.get("function_name", "未知函数")
    success = result.get("success", False)

    console.print("\n[bold magenta]=== 代理调用结果 ===[/bold magenta]")
    console.print(f"[cyan]插件名称:[/cyan] {plugin_name}")
    console.print(f"[cyan]函数名称:[/cyan] {function_name}")
    console.print(f"[cyan]执行状态:[/cyan] {'[green]成功[/green]' if success else '[red]失败[/red]'}")

    result_data = result.get("result", {})
    if isinstance(result_data, dict):
        # 如果是字典，显示基本信息
        if "data" in result_data and "metadata" in result_data:
            # 处理tickers返回结果
            data_count = len(result_data["data"])
            metadata = result_data["metadata"]
            console.print(f"[cyan]数据数量:[/cyan] {data_count}")
            console.print(f"[cyan]元数据:[/cyan] {metadata}")
        else:
            # 显示字典的键
            keys = list(result_data.keys())[:10]  # 只显示前10个键
            console.print("[cyan]结果类型:[/cyan] dict")
            console.print(f"[cyan]字典键:[/cyan] {keys}{'...' if len(result_data) > 10 else ''}")
            console.print(f"[cyan]字典大小:[/cyan] {len(result_data)}")
    elif isinstance(result_data, list):
        console.print("[cyan]结果类型:[/cyan] list")
        console.print(f"[cyan]列表长度:[/cyan] {len(result_data)}")
        if result_data:
            console.print(f"[cyan]第一个元素:[/cyan] {result_data[0]}")
    else:
        console.print(f"[cyan]结果类型:[/cyan] {type(result_data).__name__}")
        console.print(f"[cyan]结果值:[/cyan] {result_data}")


def main():
    """
    主函数
    """
    console.print("=" * 60)
    console.print("[bold cyan]ChronoForge 插件函数 API 示例[/bold cyan]")
    console.print("=" * 60)

    # 检查服务状态
    if not check_service_running():
        console.print("[red]❌ 服务未运行，无法执行后续操作[/red]")
        return False

    # 获取数据源列表
    try:
        response = requests.get(f"{API_BASE_URL}/plugins/data_source")
        if response.status_code == 200:
            data_sources = response.json().get("plugins", [])
            console.print(f"\n[green]✅ 支持的数据源:[/green] {data_sources}")
        else:
            console.print("[yellow]⚠️ 获取数据源列表失败，使用默认数据源[/yellow]")
            data_sources = ["BitcoinFGIDataSource"]
    except Exception:
        console.print("[yellow]⚠️ 获取数据源列表时出错，使用默认数据源[/yellow]")
        data_sources = ["BitcoinFGIDataSource"]

    # 选择第一个数据源进行测试
    test_data_source = data_sources[0]

    # 获取数据源函数列表
    functions_info = get_data_source_functions(test_data_source)
    if functions_info:
        display_data_source_functions(functions_info)

    # 测试代理调用
    console.print("\n" + "=" * 60)
    console.print("[bold magenta]=== 测试代理调用 ===[/bold magenta]")

    # 测试1: 调用tickers函数
    console.print("\n[bold blue]1. 测试调用 tickers 函数:[/bold blue]")
    result = delegate_call_plugin_function(
        plugin_name=test_data_source,
        plugin_type="data_source",
        function_name="tickers"
    )
    display_delegate_call_result(result)

    # 测试2: 调用close_all_connections函数
    console.print("\n[bold blue]2. 测试调用 close_all_connections 函数:[/bold blue]")
    result = delegate_call_plugin_function(
        plugin_name=test_data_source,
        plugin_type="data_source",
        function_name="close_all_connections"
    )
    display_delegate_call_result(result)

    console.print("\n" + "=" * 60)
    console.print("[bold cyan]ChronoForge 插件函数 API 示例执行完成[/bold cyan]")
    console.print("=" * 60)

    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
