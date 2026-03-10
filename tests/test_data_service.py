import pytest
import pandas as pd
import numpy as np
import asyncio
import tempfile
import os
import shutil

from chronoforge.services import DataService
from chronoforge.data_source.manager import DataSourceManager
from chronoforge.storage.manager import StorageManager
from chronoforge.cache import CacheManager


class MockDataSource:
    """模拟数据源"""
    
    async def fetch(self, symbol, timeframe, start_ts_ms, end_ts_ms) -> pd.DataFrame:
        """获取数据"""
        # 创建模拟数据
        dates = pd.date_range(start='2023-01-01', periods=10, freq='D')
        values = np.random.randn(10)
        
        data = pd.DataFrame({
            'timestamp': dates,
            'open': values,
            'high': values + 0.1,
            'low': values - 0.1,
            'close': values,
            'volume': np.abs(values) * 1000
        })
        
        return data
    
    @property
    def name(self):
        """数据源名称"""
        return "mock"
    
    @property
    def exchange_name(self):
        """交易所名称"""
        return "mock_exchange"
    
    def __init__(self, config=None):
        """初始化"""
        pass


class TestDataService:
    """测试 DataService 类"""

    def setup_method(self):
        """设置测试环境"""
        self.temp_dir = tempfile.mkdtemp()

        from chronoforge.storage.localfile_storage.adapter import LocalFileStorage
        local_storage = LocalFileStorage({'data_dir': self.temp_dir})

        self.data_source_manager = DataSourceManager()
        self.data_source_manager.register_data_source('mock', MockDataSource())

        self.storage_manager = StorageManager()
        self.storage_manager.register_storage('local', local_storage)

        self.cache_manager = CacheManager()
        self.cache_manager.create_cache('default')

        self.data_service = DataService(
            data_source_manager=self.data_source_manager,
            storage_manager=self.storage_manager,
            cache_manager=self.cache_manager
        )

    def teardown_method(self):
        """清理测试环境"""
        if hasattr(self, 'temp_dir') and os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    async def test_get_data(self):
        """测试获取数据"""
        # 测试获取数据
        data = await self.data_service.get_data('mock', 'test')
        
        # 验证数据是否获取成功
        assert data is not None
        assert isinstance(data, pd.DataFrame)
        assert not data.empty
        assert 'timestamp' in data.columns
        assert 'open' in data.columns
        assert 'high' in data.columns
        assert 'low' in data.columns
        assert 'close' in data.columns
        assert 'volume' in data.columns
    
    async def test_process_data(self):
        """测试处理数据"""
        # 创建测试数据
        dates = pd.date_range(start='2023-01-01', periods=10, freq='D')
        values = np.random.randn(10)
        data = pd.DataFrame({
            'timestamp': dates,
            'value': values
        })
        
        # 测试数据处理
        operations = [
            {
                'type': 'filter',
                'params': {
                    'condition': 'value > 0'
                }
            },
            {
                'type': 'sort',
                'params': {
                    'by': 'value',
                    'ascending': False
                }
            }
        ]
        
        processed_data = self.data_service.process_data(data, operations)
        
        # 验证处理结果
        assert processed_data is not None
        assert isinstance(processed_data, pd.DataFrame)
        assert all(processed_data['value'] > 0)
    
    @pytest.mark.skip(reason="Storage backend requires proper configuration")
    async def test_save_and_load_data(self):
        """测试保存和加载数据"""
        # 创建测试数据 - 使用 OHLCV 格式
        dates = pd.date_range(start='2023-01-01', periods=10, freq='D')
        values = np.random.randn(10)
        data = pd.DataFrame({
            'timestamp': dates,
            'open': values,
            'high': values + 0.1,
            'low': values - 0.1,
            'close': values,
            'volume': np.abs(values) * 1000
        })

        # 测试保存数据
        save_result = await self.data_service.save_data(data, 'local', 'test_data')
        assert save_result is True

        # 测试加载数据
        loaded_data = await self.data_service.load_data('local', 'test_data')
        assert loaded_data is not None
        assert isinstance(loaded_data, pd.DataFrame)
        assert not loaded_data.empty
    
    @pytest.mark.skip(reason="Storage backend requires proper configuration")
    async def test_update_data_strategy(self):
        """测试更新数据策略"""
        # 测试更新数据策略
        strategy = {
            'update_frequency': 'daily',
            'retention_days': 30
        }

        update_result = await self.data_service.update_data_strategy('mock', 'test', strategy)
        assert update_result is True
    
    def test_data_quality_check(self):
        """测试数据质量检查"""
        # 创建包含空值和重复值的数据
        dates = pd.date_range(start='2023-01-01', periods=5, freq='D')
        values = [1.0, np.nan, 3.0, 3.0, 5.0]
        
        data = pd.DataFrame({
            'timestamp': dates,
            'value': values
        })
        
        # 测试数据质量检查
        checked_data = self.data_service._check_data_quality(data)
        
        # 验证空值是否被移除
        assert not checked_data.isnull().values.any()
        # 验证数据长度（空值被移除，重复值在不同日期下不算重复）
        assert len(checked_data) == 4
    
    def test_standardize_data(self):
        """测试数据标准化"""
        # 创建原始数据
        dates = pd.date_range(start='2023-01-01', periods=3, freq='D')
        values = [1.0, 2.0, 3.0]
        
        data = pd.DataFrame({
            'Date Time': dates,
            'Value': values
        })
        
        # 测试数据标准化
        standardized_data = self.data_service._standardize_data(data, 'mock', 'test')
        
        # 验证列名是否标准化
        assert 'date_time' not in standardized_data.columns
        assert 'value' in standardized_data.columns
        assert 'timestamp' in standardized_data.columns
        
        # 验证数据是否按时间戳排序
        assert standardized_data['timestamp'].is_monotonic_increasing
    
    # 同步测试包装器
    def test_get_data_sync(self):
        asyncio.run(self.test_get_data())
    
    def test_process_data_sync(self):
        asyncio.run(self.test_process_data())
    
    @pytest.mark.skip(reason="Storage backend requires proper configuration")
    def test_save_and_load_data_sync(self):
        asyncio.run(self.test_save_and_load_data())

    @pytest.mark.skip(reason="Storage backend requires proper configuration")
    def test_update_data_strategy_sync(self):
        asyncio.run(self.test_update_data_strategy())
