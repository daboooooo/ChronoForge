#!/usr/bin/env python3
"""
示例脚本：检查 API 服务状态并获取 FRED 数据

此脚本会：
1. 检查 ChronoForge API 服务是否开启
2. 获取 FRED 数据源的 rate 和 volume 数据
3. 打印获取的数据
"""

import requests
import time
import json

# API 基础 URL
BASE_URL = "http://localhost:8000/api"

def check_api_status():
    """检查 API 服务状态"""
    print("检查 API 服务状态...")
    try:
        response = requests.get(f"{BASE_URL}/status", timeout=5)
        if response.status_code == 200:
            status_data = response.json()
            print(f"✅ API 服务状态: {status_data.get('status', 'unknown')}")
            print(f"✅ 服务版本: {status_data.get('version', 'unknown')}")
            print(f"✅ 任务数量: {status_data.get('tasks_count', 0)}")
            print(f"✅ 运行中任务: {status_data.get('running_tasks_count', 0)}")
            return True
        else:
            print(f"❌ API 服务返回错误状态码: {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"❌ API 服务不可达: {e}")
        return False

def get_fred_data(data_type, symbols=None, start_time=None, end_time=None, limit=100):
    """获取 FRED 数据
    
    Args:
        data_type: 数据类型，如 'rate' 或 'volume'
        symbols: 数据符号列表，如 ['IORB', 'EFFR']
        start_time: 起始时间，格式为 "YYYY-MM-DD HH:MM:SS"
        end_time: 结束时间，格式为 "YYYY-MM-DD HH:MM:SS"
        limit: 返回数据的最大条数
    
    Returns:
        获取的数据或 None
    """
    print(f"\n获取 FRED {data_type} 数据...")
    
    # 构建请求参数
    params = {}
    if symbols:
        # 对于单个符号，直接使用 symbol 参数
        if len(symbols) == 1:
            params['symbol'] = symbols[0]
            print(f"  符号: {symbols[0]}")
        else:
            print(f"  符号数量: {len(symbols)}")
            print(f"  符号列表: {', '.join(symbols[:5])}{'...' if len(symbols) > 5 else ''}")
    if start_time:
        params['start_time'] = start_time
        print(f"  起始时间: {start_time}")
    if end_time:
        params['end_time'] = end_time
        print(f"  结束时间: {end_time}")
    if limit:
        params['limit'] = limit
        print(f"  限制条数: {limit}")
    
    try:
        # 假设 FRED 数据存储在名为 'fred_daily' 的任务中
        response = requests.get(f"{BASE_URL}/tasks/fred_daily/data", params=params, timeout=10)
        
        if response.status_code == 200:
            try:
                data = response.json()
                print(f"✅ 成功获取 {data.get('total', 0)} 条数据")
                return data
            except json.JSONDecodeError as e:
                print(f"❌ 解析响应失败: {e}")
                print(f"响应内容: {response.text[:100]}...")
                return None
        else:
            print(f"❌ 获取数据失败，状态码: {response.status_code}")
            print(f"错误信息: {response.text}")
            # 尝试获取任务状态，看看是否需要启动任务
            try:
                task_status_response = requests.get(f"{BASE_URL}/tasks/fred_daily/status", timeout=5)
                if task_status_response.status_code == 200:
                    task_status = task_status_response.json()
                    print(f"任务状态: {task_status.get('status', 'unknown')}")
                    print(f"任务消息: {task_status.get('message', '无')}")
            except:
                pass
            return None
    except requests.exceptions.RequestException as e:
        print(f"❌ 请求失败: {e}")
        return None

def get_task_data_info(task_name):
    """获取任务数据信息"""
    print(f"\n获取任务 {task_name} 数据信息...")
    try:
        response = requests.get(f"{BASE_URL}/tasks/{task_name}/data_info", timeout=10)
        if response.status_code == 200:
            try:
                data_info = response.json()
                total = data_info.get('total', 0)
                print(f"✅ 成功获取 {total} 条数据信息")
                if data_info.get('data_info'):
                    print("前 5 条数据信息:")
                    for info in data_info['data_info'][:5]:
                        print(f"  - {info.get('data_name')}")
                        print(f"    时间范围: {info.get('start_time')} 至 {info.get('end_time')}")
                return data_info
            except json.JSONDecodeError as e:
                print(f"❌ 解析响应失败: {e}")
                print(f"响应内容: {response.text[:100]}...")
                return None
        else:
            print(f"❌ 获取数据信息失败，状态码: {response.status_code}")
            print(f"错误信息: {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"❌ 请求失败: {e}")
        return None

def start_fred_task(task_name):
    """启动 FRED 任务"""
    print(f"\n启动 FRED 任务 {task_name}...")
    try:
        response = requests.post(f"{BASE_URL}/tasks/{task_name}/start", timeout=10)
        if response.status_code == 200:
            result = response.json()
            print(f"✅ 任务启动成功: {result.get('status', 'unknown')}")
            print(f"启动时间: {result.get('start_time', '未知')}")
            return True
        else:
            print(f"❌ 启动任务失败，状态码: {response.status_code}")
            print(f"错误信息: {response.text}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"❌ 请求失败: {e}")
        return False

def get_fred_tasks():
    """获取所有与 FRED 相关的任务"""
    print("\n获取 FRED 相关任务...")
    try:
        response = requests.get(f"{BASE_URL}/tasks", timeout=5)
        if response.status_code == 200:
            tasks_data = response.json()
            fred_tasks = [task for task in tasks_data.get('tasks', []) 
                         if 'fred' in task.get('name', '').lower()]
            
            if fred_tasks:
                print(f"找到 {len(fred_tasks)} 个 FRED 相关任务:")
                for task in fred_tasks:
                    print(f"  - {task['name']} (状态: {task.get('status', 'unknown')})")
                return fred_tasks
            else:
                print("未找到 FRED 相关任务")
                return []
        else:
            print(f"获取任务列表失败，状态码: {response.status_code}")
            return []
    except requests.exceptions.RequestException as e:
        print(f"请求失败: {e}")
        return []

def display_data(data, data_type):
    """显示获取的数据"""
    if not data or 'data' not in data:
        print(f"没有 {data_type} 数据可显示")
        return
    
    print(f"\n显示 FRED {data_type} 数据 (前 10 条):")
    print("-" * 80)
    
    # 打印表头
    if data['data']:
        first_item = data['data'][0]
        headers = list(first_item.keys())
        print(" | ".join(f"{h:15}" for h in headers))
        print("-" * 80)
        
        # 打印数据
        for item in data['data'][:10]:  # 只显示前 10 条
            values = [str(item.get(h, "N/A")) for h in headers]
            print(" | ".join(f"{v:15}" for v in values))
        
        if len(data['data']) > 10:
            print(f"... 还有 {len(data['data']) - 10} 条数据未显示")
    
    print("-" * 80)

def main():
    """主函数"""
    print("=== ChronoForge FRED 数据获取示例 ===")
    print(f"API 基础 URL: {BASE_URL}")
    print()
    
    # 检查 API 状态
    if not check_api_status():
        print("\n⚠️ API 服务不可用，无法获取数据")
        return
    
    # 获取 FRED 相关任务
    fred_tasks = get_fred_tasks()
    
    # 启动 FRED 任务，确保任务正在运行
    if fred_tasks:
        for task in fred_tasks:
            task_name = task.get('name')
            if task_name == 'fred_daily':
                print(f"\n" + "=" * 60)
                print(f"启动 FRED 任务: {task_name}")
                print("=" * 60)
                start_fred_task(task_name)
                # 等待 3 秒，让任务有时间启动
                time.sleep(3)
                
                # 获取任务数据信息
                print(f"\n" + "=" * 60)
                print(f"获取任务数据信息: {task_name}")
                print("=" * 60)
                get_task_data_info(task_name)
                # 等待 2 秒
                time.sleep(2)
                break
    
    # 等待 1 秒，避免请求过快
    time.sleep(1)
    
    # 获取 FRED rate 数据
    print("\n" + "=" * 60)
    print("获取 FRED 利率数据")
    print("=" * 60)
    
    # 示例：获取失业率数据
    rate_data = get_fred_data(
        data_type="rate",
        symbols=[
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
        ],
        start_time="2024-01-01",
        limit=50
    )
    
    if rate_data:
        display_data(rate_data, "rate")
    
    # 等待 1 秒
    time.sleep(1)
    
    # 获取 FRED volume 数据
    print("\n" + "=" * 60)
    print("获取 FRED 交易量数据")
    print("=" * 60)
    
    # 示例：获取 GDP 数据
    volume_data = get_fred_data(
        data_type="volume",
        symbols=[
            "RRPONTSYD",
            "EFFRVOL",
            "SOFRVOL",
            "RPONTSYD",
            "RPMBSD",
            "RPAGYD",
        ],
        start_time="2024-01-01",
        limit=50
    )
    
    if volume_data:
        display_data(volume_data, "volume")
    
    # 打印系统统计信息
    print("\n" + "=" * 60)
    print("系统统计信息")
    print("=" * 60)
    
    try:
        response = requests.get(f"{BASE_URL}/stats", timeout=5)
        if response.status_code == 200:
            stats = response.json()
            print(f"CPU 使用率: {stats['system']['cpu_usage']}%")
            print(f"内存使用率: {stats['system']['memory_usage']}%")
            print(f"磁盘使用率: {stats['system']['disk_usage']}%")
            print(f"系统运行时间: {stats['system']['uptime_seconds']:.0f} 秒")
            print(f"Python 版本: {stats['system']['python_version']}")
            print()
            print(f"任务总数: {stats['tasks']['total']}")
            print(f"运行中任务: {stats['tasks']['running']}")
            print(f"空闲任务: {stats['tasks']['idle']}")
            print(f"失败任务: {stats['tasks']['failed']}")
    except requests.exceptions.RequestException as e:
        print(f"获取系统统计信息失败: {e}")
    
    print("\n=== 示例脚本执行完成 ===")

if __name__ == "__main__":
    main()
