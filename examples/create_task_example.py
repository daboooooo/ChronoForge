#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
示例：使用@create_task装饰器创建任务

该示例展示了如何使用@create_task装饰器创建周期性任务和基于time_slot的任务。
"""

from chronoforge.decorators import create_task
from chronoforge.data_source.base import DataSourceBase
from chronoforge.storage.base import StorageBase
import pandas as pd


class ExampleDataSource(DataSourceBase):
    """示例数据源插件"""
    
    @property
    def name(self):
        return "ExampleDataSource"
    
    async def fetch(self, symbol, timeframe, start_ts_ms, end_ts_ms=None):
        """实现fetch方法"""
        return pd.DataFrame()
    
    # 使用新的@create_task装饰器创建周期性任务（旧的periodic_task功能）
    @create_task(
        interval=3600,  # 每小时执行一次
        symbols=['BTC/USDT'],
        timeframe='1h',
        storage_name="LocalFileStorage",
        params={'exchange_name': 'binance'}
    )
    async def ohlcv_1h_periodic(self, exchange_name):
        """每小时获取BTC/USDT的K线数据（周期性任务）
        
        Args:
            exchange_name: 交易所名称
            
        Returns:
            pd.DataFrame: K线数据
        """
        print(f"周期性任务 - 获取{exchange_name}交易所的BTC/USDT 1h K线数据")
        # 实际实现中，这里会调用fetch方法获取数据
        return pd.DataFrame({
            'time': [1620000000000],
            'open': [30000],
            'high': [31000],
            'low': [29000],
            'close': [30500],
            'volume': [1000000]
        })
    
    # 使用新的@create_task装饰器创建基于time_slot的任务
    @create_task(
        symbols=['BTC/USDT'],
        timeframe='1h',
        storage_name="LocalFileStorage",
        time_slot={'start': '00:00:00', 'end': '00:10:00'},  # 每天凌晨00:00-00:10执行
        params={'exchange_name': 'binance'}
    )
    async def ohlcv_1h_daily(self, exchange_name):
        """每天凌晨获取BTC/USDT的K线数据（基于time_slot的任务）
        
        Args:
            exchange_name: 交易所名称
            
        Returns:
            pd.DataFrame: K线数据
        """
        print(f"基于time_slot的任务 - 获取{exchange_name}交易所的BTC/USDT 1h K线数据")
        # 实际实现中，这里会调用fetch方法获取数据
        return pd.DataFrame({
            'time': [1620000000000],
            'open': [30000],
            'high': [31000],
            'low': [29000],
            'close': [30500],
            'volume': [1000000]
        })
    
    # 使用新的@create_task装饰器创建带有time_slot的任务，执行特定时间段内的任务
    @create_task(
        symbols=['ETH/USDT'],
        timeframe='4h',
        storage_name="LocalFileStorage",
        time_slot={'start': '09:00:00', 'end': '18:00:00'},  # 每天9:00-18:00执行
        params={'exchange_name': 'okx'}
    )
    async def ohlcv_4h_during_trading(self, exchange_name):
        """交易时段内每4小时获取ETH/USDT的K线数据（基于time_slot的任务）
        
        Args:
            exchange_name: 交易所名称
            
        Returns:
            pd.DataFrame: K线数据
        """
        print(f"交易时段任务 - 获取{exchange_name}交易所的ETH/USDT 4h K线数据")
        # 实际实现中，这里会调用fetch方法获取数据
        return pd.DataFrame({
            'time': [1620000000000],
            'open': [2000],
            'high': [2100],
            'low': [1900],
            'close': [2050],
            'volume': [500000]
        })


def main():
    """主函数，演示@create_task装饰器的使用"""
    print("=== @create_task装饰器示例 ===")
    
    # 创建示例数据源实例
    data_source = ExampleDataSource({})
    
    # 打印任务配置
    print("\n1. 周期性任务配置（每小时执行一次）：")
    task_config = data_source.ohlcv_1h_periodic.task_config
    print(f"   任务名称: ohlcv_1h_periodic")
    print(f"   是否周期性: {data_source.ohlcv_1h_periodic.is_periodic_task}")
    print(f"   执行间隔: {task_config['interval']}秒")
    print(f"   交易对: {task_config['symbols']}")
    print(f"   时间框架: {task_config['timeframe']}")
    print(f"   存储名称: {task_config['storage_name']}")
    print(f"   参数: {task_config['params']}")
    
    print("\n2. 基于time_slot的任务配置（每天凌晨执行）：")
    task_config = data_source.ohlcv_1h_daily.task_config
    print(f"   任务名称: ohlcv_1h_daily")
    print(f"   是否周期性: {data_source.ohlcv_1h_daily.is_periodic_task}")
    print(f"   时间槽: {task_config['time_slot']}")
    print(f"   交易对: {task_config['symbols']}")
    print(f"   时间框架: {task_config['timeframe']}")
    print(f"   存储名称: {task_config['storage_name']}")
    print(f"   参数: {task_config['params']}")
    
    print("\n3. 交易时段任务配置（每天9:00-18:00执行）：")
    task_config = data_source.ohlcv_4h_during_trading.task_config
    print(f"   任务名称: ohlcv_4h_during_trading")
    print(f"   是否周期性: {data_source.ohlcv_4h_during_trading.is_periodic_task}")
    print(f"   时间槽: {task_config['time_slot']}")
    print(f"   交易对: {task_config['symbols']}")
    print(f"   时间框架: {task_config['timeframe']}")
    print(f"   存储名称: {task_config['storage_name']}")
    print(f"   参数: {task_config['params']}")
    
    print("\n=== 装饰器使用说明 ===")
    print("\n1. 旧的@periodic_task装饰器已替换为@create_task装饰器")
    print("2. 保持了向后兼容性，原有的periodic_task功能仍然可用")
    print("3. 新增了time_slot参数，用于指定任务在每天的什么时间段执行")
    print("4. 可以通过is_periodic_task属性判断任务是否是周期性任务")
    print("5. 任务配置存储在task_config属性中")


if __name__ == "__main__":
    main()
