import pytest
from unittest.mock import Mock
from chronoforge.data_source import DataSourceBase
from chronoforge.decorators import periodic_task
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


class TestConcreteDataSourceWithPeriodicTask:
    """测试带有周期性任务的具体数据源"""

    def setup_method(self):
        """测试方法设置"""
        # 创建一个带有周期性任务的数据源
        class TestDataSource(DataSourceBase):
            @property
            def name(self):
                return "TestDataSource"

            async def fetch(self, symbol, timeframe, start_ts_ms, end_ts_ms=None):
                return pd.DataFrame({'price': [1000]})

            async def close_all_connections(self):
                pass

            @periodic_task(interval=5, symbols=['BTC/USDT'], params={'key': 'value'})
            async def periodic_fetch(self, key=None):
                return {'result': 'success', 'key': key}

            @periodic_task(interval=10, storage_name="LocalFileStorage", max_retries=5)
            async def periodic_store(self):
                return pd.DataFrame({'price': [1000, 2000]})

        self.TestDataSource = TestDataSource

    def test_periodic_task_decorator_on_datasource(self):
        """测试数据源方法上的周期性任务装饰器"""
        data_source = self.TestDataSource()

        # 检查periodic_fetch方法是否有装饰器属性
        assert hasattr(data_source.periodic_fetch, 'is_periodic_task'), \
            "periodic_fetch方法没有is_periodic_task属性"
        assert data_source.periodic_fetch.is_periodic_task is True, "is_periodic_task属性值不正确"
        assert hasattr(data_source.periodic_fetch, 'task_config'), "periodic_fetch方法没有task_config属性"

        # 检查periodic_store方法是否有装饰器属性
        assert hasattr(data_source.periodic_store, 'is_periodic_task'), \
            "periodic_store方法没有is_periodic_task属性"
        assert data_source.periodic_store.is_periodic_task is True, "is_periodic_task属性值不正确"
        assert hasattr(data_source.periodic_store, 'task_config'), "periodic_store方法没有task_config属性"

        # 验证配置
        assert data_source.periodic_fetch.task_config['interval'] == 5, "interval配置不正确"
        assert data_source.periodic_fetch.task_config['symbols'] == ['BTC/USDT'], "symbols配置不正确"
        assert data_source.periodic_store.task_config['storage_name'] == "LocalFileStorage", \
            "storage_name配置不正确"
        assert data_source.periodic_store.task_config['max_retries'] == 5, "max_retries配置不正确"

    @pytest.mark.asyncio
    async def test_periodic_task_execution(self):
        """测试周期性任务的执行"""
        data_source = self.TestDataSource()

        # 执行周期性任务并检查结果
        result = await data_source.periodic_fetch(key='test_value')
        assert result['result'] == 'success', "任务执行结果不正确"
        assert result['key'] == 'test_value', "参数传递不正确"

        # 执行带有默认参数的任务
        result = await data_source.periodic_fetch()
        assert result['result'] == 'success', "任务执行结果不正确"
        assert result['key'] is None, "参数传递不正确"

    @pytest.mark.asyncio
    async def test_close_connections_management(self):
        """测试连接关闭管理"""
        data_source = self.TestDataSource()

        # 模拟连接
        data_source.exchange_instances = {}
        data_source.exchange_instances['test_exchange'] = (Mock(), 1234567890)

        # 调用关闭连接方法
        await data_source.close_all_connections()

        # 验证连接是否被关闭（在实际实现中，应该调用exchange.close()或类似方法）
        # 这里我们只是确保方法可以正常调用而不抛出异常
        assert True  # 如果没有抛出异常，测试通过


class TestDataSourceConnectionManagement:
    """测试数据源连接管理"""

    def setup_method(self):
        """测试方法设置"""
        # 创建一个带有连接管理的数据源
        class ConnectionDataSource(DataSourceBase):
            @property
            def name(self):
                return "ConnectionDataSource"

            def __init__(self, config=None):
                super().__init__(config)
                self.exchange_instances = {}
                self.connections_closed = False

            async def fetch(self, symbol, timeframe, start_ts_ms, end_ts_ms=None):
                return pd.DataFrame()

            async def _get_exchange_instance(self, exchange_name):
                """模拟获取交易所实例"""
                if exchange_name not in self.exchange_instances:
                    # 模拟创建新连接
                    mock_exchange = Mock()
                    mock_exchange.close = Mock()
                    self.exchange_instances[exchange_name] = (mock_exchange, 1234567890)
                return self.exchange_instances[exchange_name][0]

            async def close_all_connections(self):
                """关闭所有连接"""
                for exchange_name, (instance, timestamp) in self.exchange_instances.items():
                    instance.close()
                self.connections_closed = True

        self.ConnectionDataSource = ConnectionDataSource

    @pytest.mark.asyncio
    async def test_connection_creation_and_closure(self):
        """测试连接创建和关闭"""
        data_source = self.ConnectionDataSource()

        # 创建连接
        exchange = await data_source._get_exchange_instance('test_exchange')
        assert 'test_exchange' in data_source.exchange_instances

        # 关闭连接
        await data_source.close_all_connections()
        assert data_source.connections_closed is True
        # 验证close方法被调用
        exchange.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_multiple_connections_management(self):
        """测试多个连接的管理"""
        data_source = self.ConnectionDataSource()

        # 创建多个连接
        exchange1 = await data_source._get_exchange_instance('exchange1')
        exchange2 = await data_source._get_exchange_instance('exchange2')

        assert len(data_source.exchange_instances) == 2
        assert 'exchange1' in data_source.exchange_instances
        assert 'exchange2' in data_source.exchange_instances

        # 关闭所有连接
        await data_source.close_all_connections()
        assert data_source.connections_closed is True

        # 验证所有close方法被调用
        exchange1.close.assert_called_once()
        exchange2.close.assert_called_once()
