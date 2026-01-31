#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
示例：使用api_callable装饰器标记插件函数

该示例展示了如何使用@api_callable装饰器来标记插件中可以被delegate_call调用的函数。
"""

from chronoforge.decorators import api_callable
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
    
    @api_callable
    def get_data_info(self, symbol):
        """获取数据信息（可以被delegate_call调用）
        
        Args:
            symbol: 数据符号
            
        Returns:
            dict: 数据信息
        """
        return {
            "symbol": symbol,
            "type": "spot",
            "exchange": "example"
        }
    
    @api_callable
    def calculate_indicator(self, data, period):
        """计算指标（可以被delegate_call调用）
        
        Args:
            data: 输入数据
            period: 计算周期
            
        Returns:
            float: 指标值
        """
        return sum(data) / period if period > 0 else 0
    
    def internal_method(self):
        """内部方法（不能被delegate_call调用）"""
        return "internal_only"


class ExampleStorage(StorageBase):
    """示例存储插件"""
    
    @property
    def name(self):
        return "ExampleStorage"
    
    async def save(self, id, data, sub=None, metadata=None):
        """实现save方法"""
        return True
    
    async def load(self, id, sub=None, metadata=None):
        """实现load方法"""
        return None
    
    async def delete(self, id, sub=None, metadata=None):
        """实现delete方法"""
        return True
    
    async def exists(self, id, sub=None, metadata=None):
        """实现exists方法"""
        return False
    
    async def lists(self, sub=None):
        """实现lists方法"""
        return []
    
    async def get_time_range(self, id, sub=None):
        """实现get_time_range方法"""
        return None
    
    async def get_metadata(self, id, sub=None):
        """实现get_metadata方法"""
        return {}
    
    async def update_metadata(self, id, metadata, sub=None):
        """实现update_metadata方法"""
        return True
    
    @api_callable
    def get_storage_stats(self):
        """获取存储统计信息（可以被delegate_call调用）
        
        Returns:
            dict: 存储统计信息
        """
        return {
            "total_size": 1024,
            "item_count": 50,
            "status": "healthy"
        }
    
    @api_callable
    def clear_cache(self, prefix=None):
        """清除缓存（可以被delegate_call调用）
        
        Args:
            prefix: 缓存键前缀
            
        Returns:
            bool: 是否成功清除
        """
        return True


def main():
    """主函数"""
    print("=== api_callable装饰器示例 ===")
    
    # 演示装饰器效果
    print("\n1. 检查函数是否被api_callable装饰：")
    
    # 创建实例
    data_source = ExampleDataSource({})
    storage = ExampleStorage({})
    
    # 检查示例数据源的函数
    print(f"   ExampleDataSource.get_data_info.is_api_callable: {hasattr(data_source.get_data_info, 'is_api_callable') and data_source.get_data_info.is_api_callable}")
    print(f"   ExampleDataSource.calculate_indicator.is_api_callable: {hasattr(data_source.calculate_indicator, 'is_api_callable') and data_source.calculate_indicator.is_api_callable}")
    print(f"   ExampleDataSource.internal_method.is_api_callable: {hasattr(data_source.internal_method, 'is_api_callable') and data_source.internal_method.is_api_callable}")
    
    # 检查示例存储的函数
    print(f"   ExampleStorage.get_storage_stats.is_api_callable: {hasattr(storage.get_storage_stats, 'is_api_callable') and storage.get_storage_stats.is_api_callable}")
    print(f"   ExampleStorage.clear_cache.is_api_callable: {hasattr(storage.clear_cache, 'is_api_callable') and storage.clear_cache.is_api_callable}")
    print(f"   ExampleStorage.save.is_api_callable: {hasattr(storage.save, 'is_api_callable') and storage.save.is_api_callable}")
    
    print("\n2. 装饰器功能说明：")
    print("   - 被@api_callable装饰的函数可以通过delegate_call调用")
    print("   - 未被装饰的函数无法通过delegate_call调用")
    print("   - 装饰器会给函数添加is_api_callable属性")
    print("   - delegate_call会检查函数是否有is_api_callable=True")
    
    print("\n3. 使用方法：")
    print("   @api_callable")
    print("   def your_function(self, param1, param2):")
    print("       # 函数实现")
    print("       return result")


if __name__ == "__main__":
    main()
