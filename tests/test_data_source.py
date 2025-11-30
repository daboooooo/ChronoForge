import pytest
from chronoforge.data_source import DataSourceBase

import pandas as pd


# 创建一个具体的数据源子类用于测试
class ConcreteDataSource(DataSourceBase):
    """具体的数据源实现，用于测试"""

    def __init__(self, config=None):
        super().__init__(config)

    @property
    def name(self):
        """实现name属性"""
        return "ConcreteDataSource"

    async def fetch(self, symbol, timeframe, start_ts_ms, end_ts_ms=None):
        """实现fetch方法"""
        return pd.DataFrame()

    def validate(self, data):
        """实现validate方法"""
        return True, "Data is valid"

    async def close_all_connections(self):
        """实现close_all_connections方法"""
        pass


class TestDataSourceBase:
    """测试数据源基类"""

    def test_initialization(self):
        """测试数据源子类初始化"""
        data_source = ConcreteDataSource(config={"api_key": "test_key"})
        assert data_source.config == {"api_key": "test_key"}
        assert data_source.name == "ConcreteDataSource"

    @pytest.mark.asyncio
    async def test_fetch_method(self):
        """测试fetch方法"""
        data_source = ConcreteDataSource()
        result = await data_source.fetch(
            symbol="test_symbol",
            timeframe="1d",
            start_ts_ms=1640995200000
        )
        assert result is not None
        assert isinstance(result, pd.DataFrame)

    def test_validate_data(self):
        """测试validate方法"""
        data_source = ConcreteDataSource()
        is_valid, message = data_source.validate(pd.DataFrame())
        assert is_valid is True
        assert isinstance(message, str)

    @pytest.mark.asyncio
    async def test_close_connections(self):
        """测试关闭连接方法"""
        data_source = ConcreteDataSource()
        # 应该不抛出异常
        await data_source.close_all_connections()

    def test_plugin_type(self):
        """测试plugin_type属性"""
        data_source = ConcreteDataSource()
        assert data_source.plugin_type == "datasource"
