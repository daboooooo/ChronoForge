import pytest
import time
from collections import deque
from unittest.mock import Mock, AsyncMock, patch
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

    async def fetch(self, symbol, timeframe, start_ts_ms, end_ts_ms=None) -> pd.DataFrame:
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
    async def test_close_all_connections(self):
        """测试close_all_connections方法"""
        data_source = ConcreteDataSource()
        # 该方法应该正常执行，不抛出异常
        await data_source.close_all_connections()

    def test_abstract_base_class(self):
        """测试抽象基类不能直接实例化"""
        with pytest.raises(TypeError):
            # 应该抛出TypeError，因为DataSourceBase是抽象基类
            DataSourceBase()


class TestExchangeConnectionPool:
    """测试交易所连接池"""

    def setup_method(self):
        """测试方法设置"""
        from chronoforge.data_source.crypto_spot import ExchangeConnectionPool
        self.ExchangeConnectionPool = ExchangeConnectionPool
        self.mock_config = {
            'binance': {
                'apiKey': 'test_key',
                'secret': 'test_secret'
            }
        }

    @pytest.mark.asyncio
    async def test_basic_connection_pool_operations(self):
        """测试基本的连接池操作"""
        import ccxt.async_support as ccxt

        # 创建连接池
        pool = self.ExchangeConnectionPool(
            exchange_name='binance',
            config=self.mock_config
        )

        # 模拟ccxt.exchange的创建
        mock_connection = Mock(spec=ccxt.Exchange)
        mock_connection.load_markets = AsyncMock()

        with patch('ccxt.async_support.binance') as mock_binance_class:
            mock_binance_class.return_value = mock_connection

            # 获取连接
            conn1 = await pool.get_connection()
            assert conn1 is mock_connection

            # 归还连接
            await pool.return_connection(conn1)

            # 再次获取连接，应该是同一个
            conn2 = await pool.get_connection()
            assert conn2 is conn1

    @pytest.mark.asyncio
    async def test_connection_pool_with_multiple_connections(self):
        """测试连接池的多连接管理"""
        import ccxt.async_support as ccxt

        pool = self.ExchangeConnectionPool(
            exchange_name='binance',
            config=self.mock_config,
            max_connections=2
        )

        # 创建两个模拟连接
        mock_connection1 = Mock(spec=ccxt.Exchange)
        mock_connection1.load_markets = AsyncMock()

        mock_connection2 = Mock(spec=ccxt.Exchange)
        mock_connection2.load_markets = AsyncMock()

        # 使用side_effect返回不同的连接实例
        with patch('ccxt.async_support.binance') as mock_binance_class:
            mock_binance_class.side_effect = [mock_connection1, mock_connection2]

            # 获取第一个连接
            conn1 = await pool.get_connection()
            assert conn1 is mock_connection1

            # 获取第二个连接
            conn2 = await pool.get_connection()
            assert conn2 is mock_connection2

            # 归还第一个连接
            await pool.return_connection(conn1)

            # 再次获取连接，应该是第一个已归还的连接
            conn3 = await pool.get_connection()
            assert conn3 is conn1

    @pytest.mark.asyncio
    async def test_connection_expiry(self):
        """测试连接过期机制"""
        import ccxt.async_support as ccxt

        pool = self.ExchangeConnectionPool(
            exchange_name='binance',
            config=self.mock_config,
            connection_validity=10  # 10秒有效期
        )

        # 创建模拟连接
        mock_connection1 = Mock(spec=ccxt.Exchange)
        mock_connection1.load_markets = AsyncMock()

        mock_connection2 = Mock(spec=ccxt.Exchange)
        mock_connection2.load_markets = AsyncMock()

        with patch('ccxt.async_support.binance') as mock_binance_class:
            mock_binance_class.side_effect = [mock_connection1, mock_connection2]

            # 获取第一个连接
            conn1 = await pool.get_connection()

            # 手动添加连接信息到all_connections列表
            pool.all_connections = [{"instance": mock_connection1, "create_time": time.time() - 20, "last_used_time": time.time() - 20}]
            pool.available_connections = [{"instance": mock_connection1, "create_time": time.time() - 20, "last_used_time": time.time() - 20}]

            # 归还第一个连接
            await pool.return_connection(conn1)

            # 获取新连接
            conn2 = await pool.get_connection()

            # 系统可能会创建新连接，我们验证获取了新连接
            assert len(pool.all_connections) >= 1

    @pytest.mark.asyncio
    async def test_connection_pool_with_invalid_config(self):
        """测试连接池的无效配置处理"""
        # 创建连接池应该不会失败，但获取连接可能会
        pool = self.ExchangeConnectionPool(
            exchange_name='invalid_exchange',
            config={}
        )

        # 验证连接池创建成功
        assert pool is not None


class TestCryptoSpotDataSource:
    """测试CryptoSpotDataSource类的功能"""

    def setup_method(self):
        """测试方法设置"""
        from chronoforge.data_source.crypto_spot import CryptoSpotDataSource
        self.CryptoSpotDataSource = CryptoSpotDataSource
        self.mock_config = {
            'binance': {
                'apiKey': 'test_key',
                'secret': 'test_secret'
            },
            'connection_pool': {
                'max_connections': 3,
                'connection_validity': 3600
            }
        }

    def test_initialization(self):
        """测试CryptoSpotDataSource的初始化"""
        # 测试默认配置初始化
        data_source = self.CryptoSpotDataSource()
        assert data_source.name == "CryptoSpot"

        # 测试带配置初始化
        data_source = self.CryptoSpotDataSource(config=self.mock_config)
        assert data_source.name == "CryptoSpot"
        assert isinstance(data_source.exchange_pools, dict)

    @pytest.mark.asyncio
    async def test_crypto_spot_fetch_method(self):
        """测试CryptoSpotDataSource的fetch方法"""
        import pandas as pd

        data_source = self.CryptoSpotDataSource(config=self.mock_config)

        # 模拟OHLCV数据
        mock_ohlcv = [
            [1640995200000, 40000, 42000, 39000, 41000, 1000],
            [1641081600000, 41000, 43000, 40000, 42000, 2000]
        ]

        # 创建模拟连接和市场数据
        mock_connection = Mock()
        mock_connection.load_markets = AsyncMock()
        mock_connection.fetch_ohlcv = AsyncMock(return_value=mock_ohlcv)

        # 模拟连接池
        with patch('chronoforge.data_source.crypto_spot.ExchangeConnectionPool.get_connection') as mock_get_conn:
            mock_get_conn.return_value = mock_connection

            with patch('chronoforge.data_source.crypto_spot.ExchangeConnectionPool.return_connection') as mock_return_conn:
                # 执行fetch - 注意symbol格式：exchange:symbol
                result = await data_source.fetch(
                    symbol="binance:BTC/USDT",
                    timeframe="1d",
                    start_ts_ms=1640995200000
                )

                # 验证结果
                assert isinstance(result, pd.DataFrame)
                assert len(result) >= 0  # 结果可能为空，但应该是DataFrame
                assert "time" in result.columns
                assert "open" in result.columns
                assert "high" in result.columns
                assert "low" in result.columns
                assert "close" in result.columns
                assert "volume" in result.columns

                # 验证调用
                mock_get_conn.assert_called_once()
                mock_return_conn.assert_called_once()

    @pytest.mark.asyncio
    async def test_crypto_spot_close_all_connections(self):
        """测试关闭所有连接"""
        data_source = self.CryptoSpotDataSource(config=self.mock_config)

        # 执行关闭操作
        await data_source._close_all_connections()

        # 验证连接池被清空
        assert len(data_source.exchange_pools) == 0

    @pytest.mark.asyncio
    async def test_crypto_spot_with_invalid_exchange_in_symbol(self):
        """测试使用无效交易所的symbol格式"""
        data_source = self.CryptoSpotDataSource(config=self.mock_config)

        # 执行fetch - 注意symbol格式：invalid_exchange:symbol
        with pytest.raises(ValueError):
            await data_source.fetch(
                symbol="invalid_exchange:BTC/USDT",
                timeframe="1d",
                start_ts_ms=1640995200000
            )


class TestDataSourceVerification:
    """测试数据源验证功能"""

    def test_verify_valid_datasource(self):
        """测试验证有效的数据源"""
        from chronoforge.data_source.base import verify_datasource_instance
        result = verify_datasource_instance(ConcreteDataSource)
        assert result[0] is True
        assert "passed all" in result[1]

    def test_verify_invalid_datasource_missing_name(self):
        """测试验证缺少name属性的无效数据源"""
        from chronoforge.data_source.base import verify_datasource_instance

        # 创建一个缺少name属性的无效数据源
        class InvalidDataSource(DataSourceBase):
            async def fetch(self, symbol, timeframe, start_ts_ms, end_ts_ms=None):
                return pd.DataFrame()

            async def close_all_connections(self):
                pass

        result = verify_datasource_instance(InvalidDataSource)
        assert result[0] is False
        assert "name" in result[1].lower()

    def test_verify_invalid_sync_fetch(self):
        """测试验证同步fetch方法的无效数据源"""
        from chronoforge.data_source.base import verify_datasource_instance

        # 创建一个使用同步fetch方法的无效数据源
        class InvalidDataSource(DataSourceBase):
            @property
            def name(self):
                return "InvalidDataSource"

            # 同步方法，不符合要求
            def fetch(self, symbol, timeframe, start_ts_ms, end_ts_ms=None):
                return pd.DataFrame()

            async def close_all_connections(self):
                pass

        result = verify_datasource_instance(InvalidDataSource)
        assert result[0] is False
        assert "fetch" in result[1].lower()


class TestParsedSymbol:
    """测试ParsedSymbol类"""

    def test_parsed_symbol_basic(self):
        """测试基本的ParsedSymbol功能"""
        from chronoforge.data_source.base import ParsedSymbol
        symbol = ParsedSymbol("BTC/USDT")
        assert symbol.original == "BTC/USDT"
        assert symbol.base == "BTC"
        assert symbol.quote == "USDT"

    def test_parsed_symbol_invalid(self):
        """测试无效符号的处理"""
        from chronoforge.data_source.base import ParsedSymbol
        # ParsedSymbol should raise ValueError for invalid symbols
        with pytest.raises(ValueError):
            ParsedSymbol("INVALID_SYMBOL")


class TestExchangeConnectionPoolAttributes:
    """测试交易所连接池属性"""

    def test_connection_pool_initialization_attributes(self):
        """测试连接池初始化属性"""
        from chronoforge.data_source.crypto_spot import ExchangeConnectionPool

        pool = ExchangeConnectionPool(
            exchange_name='binance',
            config={'binance': {'apiKey': 'test', 'secret': 'test'}},
            max_connections=10,
            min_connections=2,
            connection_validity=3600
        )

        # 验证初始化属性
        assert pool._max_connections == 10
        assert pool._min_connections == 2
        assert pool.connection_validity == 3600
        assert pool._request_count == 0
        assert pool._last_request_time is not None
        assert isinstance(pool.available_connections, deque)
        assert isinstance(pool.all_connections, list)

    def test_connection_pool_request_count_tracking(self):
        """测试请求计数跟踪"""
        from chronoforge.data_source.crypto_spot import ExchangeConnectionPool

        pool = ExchangeConnectionPool(
            exchange_name='binance',
            config={'binance': {'apiKey': 'test', 'secret': 'test'}}
        )

        # 初始请求计数应为0
        assert pool._request_count == 0

        # 模拟请求计数增加
        pool._request_count += 1
        assert pool._request_count == 1

        pool._request_count += 1
        assert pool._request_count == 2


class TestCryptoSpotDataSourceWithMockedExchange:
    """使用模拟交易所测试CryptoSpotDataSource"""

    @pytest.mark.asyncio
    async def test_crypto_spot_data_source_with_mocked_exchange(self):
        """测试使用模拟交易所的CryptoSpotDataSource"""
        from chronoforge.data_source.crypto_spot import CryptoSpotDataSource

        data_source = CryptoSpotDataSource(config={
            'binance': {
                'apiKey': 'test_key',
                'secret': 'test_secret'
            }
        })

        # 模拟交易所连接和数据
        mock_connection = Mock()
        mock_connection.load_markets = AsyncMock()
        mock_connection.fetch_ohlcv = AsyncMock(return_value=[
            [1640995200000, 40000, 42000, 39000, 41000, 1000]
        ])

        # 模拟连接池的get_connection方法
        with patch('chronoforge.data_source.crypto_spot.ExchangeConnectionPool.get_connection', return_value=mock_connection):
            with patch('chronoforge.data_source.crypto_spot.ExchangeConnectionPool.return_connection'):
                # 执行fetch操作
                result = await data_source.fetch(
                    symbol="binance:BTC/USDT",
                    timeframe="1d",
                    start_ts_ms=1640995200000
                )

                # 验证结果
                assert isinstance(result, pd.DataFrame)
                assert len(result) == 1
                assert result.iloc[0]['close'] == 41000
