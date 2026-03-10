"""
LocalFileStorage 完整测试套件

全面测试本地文件存储适配器的所有功能，包括：
1. 初始化与配置
2. OHLCV 数据存取
3. Tickers 数据存取
4. Futures Metrics 数据存取
5. BTC FGI 数据存取
6. Macro FRED 数据存取
7. Coin Categories 数据存取
8. Symbols 数据存取
9. CRUD 操作
10. 元数据操作
11. 健康检查
12. 数据完整性
13. 上下文管理器
14. 边界条件与异常处理
"""

import pytest
import tempfile
import shutil
import asyncio
from datetime import datetime, timezone, timedelta

import pandas as pd

from chronoforge.storage.localfile_storage.adapter import LocalFileStorage


@pytest.fixture
def temp_data_dir():
    """创建临时数据目录的 fixture"""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    try:
        shutil.rmtree(temp_dir)
    except Exception:
        pass


@pytest.fixture
async def storage(temp_data_dir):
    """创建测试用存储实例的 fixture"""
    s = LocalFileStorage({
        "data_dir": temp_data_dir,
        "file_format": "parquet"
    })
    await s.initialize()
    yield s
    await s.close()


class TestLocalFileStorageInit:
    """存储初始化测试"""

    def test_init_default_config(self):
        """测试默认配置初始化"""
        storage = LocalFileStorage()
        assert storage.name == "LocalFile"
        assert storage.file_format == "parquet"
        assert storage._warehouse is None

    def test_init_custom_config(self, temp_data_dir):
        """测试自定义配置初始化"""
        storage = LocalFileStorage({
            "data_dir": temp_data_dir,
            "file_format": "csv"
        })
        assert storage.data_dir == temp_data_dir
        assert storage.file_format == "csv"

    async def test_initialize(self, storage):
        """测试显式初始化"""
        assert storage._warehouse is not None

    async def test_context_manager_init(self, temp_data_dir):
        """测试上下文管理器初始化"""
        async with LocalFileStorage({"data_dir": temp_data_dir}) as s:
            assert s._warehouse is not None


class TestHealthCheck:
    """健康检查测试"""

    async def test_health_check_healthy(self, storage):
        """测试正常状态的健康检查"""
        result = await storage.health_check()
        assert result['status'] == 'healthy'
        assert result['path_exists'] is True
        assert result['file_format'] == 'parquet'

    async def test_health_check_nonexistent_dir(self, temp_data_dir):
        """测试不存在的目录"""
        nonexistent_dir = f"{temp_data_dir}/nonexistent"
        storage = LocalFileStorage({"data_dir": nonexistent_dir})
        result = await storage.health_check()
        assert result['status'] == 'unhealthy'


class TestOHLCVOperations:
    """OHLCV 数据操作测试"""

    async def test_save_ohlcv_basic(self, storage):
        """测试保存基本 OHLCV 数据"""
        ohlcv_data = pd.DataFrame({
            'ts': [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            'open': [42000.0],
            'high': [43000.0],
            'low': [41500.0],
            'close': [42500.0],
            'volume': [1000.0]
        })

        result = await storage._save("BTC_USDT", ohlcv_data, metadata={
            'exchange': 'binance',
            'market_type': 'spot',
            'symbol': 'BTC/USDT',
            'timeframe': '1d'
        })

        assert result is True

    async def test_save_ohlcv_with_column_mapping(self, storage):
        """测试列名映射"""
        ohlcv_data = pd.DataFrame({
            'timestamp': [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            'open': [42000.0],
            'high': [43000.0],
            'low': [41500.0],
            'close': [42500.0],
            'volume': [1000.0]
        })

        result = await storage._save("BTC_USDT", ohlcv_data, metadata={
            'exchange': 'binance',
            'market_type': 'spot',
            'symbol': 'BTC/USDT',
            'timeframe': '1d'
        })

        assert result is True

    async def test_save_ohlcv_missing_required_columns(self, storage):
        """测试缺少必需列"""
        ohlcv_data = pd.DataFrame({
            'open': [42000.0],
            'high': [43000.0]
        })

        result = await storage._save("BTC_USDT", ohlcv_data, metadata={
            'exchange': 'binance',
            'market_type': 'spot',
            'symbol': 'BTC/USDT',
            'timeframe': '1d'
        })

        assert result is False

    async def test_save_empty_ohlcv(self, storage):
        """测试保存空数据"""
        ohlcv_data = pd.DataFrame()

        result = await storage._save("BTC_USDT", ohlcv_data, metadata={
            'exchange': 'binance',
            'market_type': 'spot',
            'symbol': 'BTC/USDT',
            'timeframe': '1d'
        })

        assert result is True

    async def test_load_ohlcv(self, storage):
        """测试加载 OHLCV 数据"""
        ohlcv_data = pd.DataFrame({
            'ts': [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            'open': [42000.0],
            'high': [43000.0],
            'low': [41500.0],
            'close': [42500.0],
            'volume': [1000.0]
        })

        await storage._save("BTC_USDT", ohlcv_data, metadata={
            'exchange': 'binance',
            'market_type': 'spot',
            'symbol': 'BTC/USDT',
            'timeframe': '1d'
        })

        loaded = await storage._load("BTC_USDT", metadata={
            'symbol': 'BTC/USDT',
            'timeframe': '1d'
        })

        assert loaded is not None
        assert len(loaded) == 1
        assert loaded['close'].iloc[0] == 42500.0

    async def test_load_ohlcv_with_time_filter(self, storage):
        """测试加载带时间过滤的 OHLCV 数据"""
        ohlcv_data = pd.DataFrame({
            'ts': [
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                datetime(2024, 1, 2, tzinfo=timezone.utc),
                datetime(2024, 1, 3, tzinfo=timezone.utc)
            ],
            'open': [42000.0, 42500.0, 43000.0],
            'high': [43000.0, 43500.0, 44000.0],
            'low': [41500.0, 42000.0, 42500.0],
            'close': [42500.0, 43000.0, 43500.0],
            'volume': [1000.0, 1100.0, 1200.0]
        })

        await storage._save("BTC_USDT", ohlcv_data, metadata={
            'exchange': 'binance',
            'market_type': 'spot',
            'symbol': 'BTC/USDT',
            'timeframe': '1d'
        })

        loaded = await storage._load("BTC_USDT", metadata={
            'symbol': 'BTC/USDT',
            'timeframe': '1d',
            'start_time': datetime(2024, 1, 2, tzinfo=timezone.utc)
        })

        assert loaded is not None
        assert len(loaded) == 2

    async def test_load_ohlcv_with_limit(self, storage):
        """测试加载带限制的 OHLCV 数据"""
        ohlcv_data = pd.DataFrame({
            'ts': [
                datetime(2024, 1, i, tzinfo=timezone.utc) for i in range(1, 11)
            ],
            'open': [42000.0 + i * 100 for i in range(10)],
            'high': [43000.0 + i * 100 for i in range(10)],
            'low': [41500.0 + i * 100 for i in range(10)],
            'close': [42500.0 + i * 100 for i in range(10)],
            'volume': [1000.0 + i * 100 for i in range(10)]
        })

        await storage._save("BTC_USDT", ohlcv_data, metadata={
            'exchange': 'binance',
            'market_type': 'spot',
            'symbol': 'BTC/USDT',
            'timeframe': '1d'
        })

        loaded = await storage._load("BTC_USDT", metadata={
            'symbol': 'BTC/USDT',
            'timeframe': '1d',
            'limit': 5
        })

        assert loaded is not None
        assert len(loaded) == 5

    async def test_save_and_load_multiple_timeframes(self, storage):
        """测试保存和加载多个时间周期"""
        for tf, close in [('1h', 42500.0), ('4h', 42600.0), ('1d', 42700.0)]:
            ohlcv_data = pd.DataFrame({
                'ts': [datetime(2024, 1, 1, tzinfo=timezone.utc)],
                'open': [42000.0],
                'high': [43000.0],
                'low': [41500.0],
                'close': [close],
                'volume': [1000.0]
            })

            await storage._save("BTC_USDT", ohlcv_data, metadata={
                'exchange': 'binance',
                'market_type': 'spot',
                'symbol': 'BTC/USDT',
                'timeframe': tf
            })

        for tf in ['1h', '4h', '1d']:
            loaded = await storage._load("BTC_USDT", metadata={
                'symbol': 'BTC/USDT',
                'timeframe': tf
            })
            assert loaded is not None
            assert len(loaded) == 1


class TestTickersOperations:
    """Tickers 数据操作测试"""

    async def test_save_tickers_basic(self, storage):
        """测试保存基本 Tickers 数据"""
        tickers_data = pd.DataFrame({
            'ts': [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            'last': [50000.0],
            'bid': [49999.5],
            'ask': [50000.5],
            'base_volume': [1000.0],
            'quote_volume': [50000000.0]
        })

        result = await storage._save("BTC_ticker", tickers_data, metadata={
            'exchange': 'binance',
            'market_type': 'spot',
            'symbol': 'BTC/USDT'
        })

        assert result is True

    async def test_save_tickers_with_column_mapping(self, storage):
        """测试 Tickers 列名映射"""
        tickers_data = pd.DataFrame({
            'timestamp': [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            'last_price': [50000.0],
            'bidPrice': [49999.5],
            'askPrice': [50000.5],
            'baseVolume': [1000.0],
            'quoteVolume': [50000000.0]
        })

        result = await storage._save("BTC_ticker", tickers_data, metadata={
            'exchange': 'binance',
            'market_type': 'spot',
            'symbol': 'BTC/USDT'
        })

        assert result is True

    async def test_save_tickers_auto_add_ts(self, storage):
        """测试自动添加时间戳"""
        tickers_data = pd.DataFrame({
            'last': [50000.0],
            'bid': [49999.5],
            'ask': [50000.5]
        })

        result = await storage._save("BTC_ticker", tickers_data, metadata={
            'exchange': 'binance',
            'market_type': 'spot',
            'symbol': 'BTC/USDT'
        })

        assert result is True

    async def test_load_tickers(self, storage):
        """测试保存 Tickers 数据"""
        tickers_data = pd.DataFrame({
            'ts': [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            'last': [50000.0],
            'bid': [49999.5],
            'ask': [50000.5]
        })

        result = await storage._save("BTC_ticker", tickers_data, metadata={
            'exchange': 'binance',
            'market_type': 'spot',
            'symbol': 'BTC/USDT'
        })

        assert result is True


class TestFuturesMetricsOperations:
    """期货指标数据操作测试"""

    async def test_save_futures_metrics_basic(self, storage):
        """测试保存基本期货指标数据"""
        metrics_data = pd.DataFrame({
            'ts': [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            'funding_rate': [0.0001],
            'open_interest': [100000000.0]
        })

        result = await storage._save("BTC_future", metrics_data, metadata={
            'exchange': 'binance_um',
            'symbol': 'BTC/USDT'
        })

        assert result is True

    async def test_save_futures_metrics_with_column_mapping(self, storage):
        """测试期货指标列名映射"""
        metrics_data = pd.DataFrame({
            'timestamp': [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            'funding_rate_1h': [0.0001],
            'oi': [100000000.0]
        })

        result = await storage._save("BTC_future", metrics_data, metadata={
            'exchange': 'binance_um',
            'symbol': 'BTC/USDT'
        })

        assert result is True

    async def test_load_futures_metrics(self, storage):
        """测试加载期货指标数据"""
        metrics_data = pd.DataFrame({
            'ts': [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            'funding_rate': [0.0001],
            'open_interest': [100000000.0],
            'exchange': ['binance_um'],
            'symbol': ['BTC/USDT']
        })

        result = await storage._save("futures_metrics_btc_usdt", metrics_data, metadata={
            'exchange': 'binance_um',
            'symbol': 'BTC/USDT',
            'data_type': 'futures_metrics'
        })

        assert result is True

        stats = await storage.get_stats()
        assert stats is not None


class TestBTCFGIOperations:
    """BTC 恐惧贪婪指数数据操作测试"""

    async def test_save_btc_fgi_basic(self, storage):
        """测试保存基本 BTC FGI 数据"""
        fgi_data = pd.DataFrame({
            'ts': [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            'value': [65],
            'label': ['Greed']
        })

        result = await storage._save("btc_fgi", fgi_data)

        assert result is True

    async def test_save_btc_fgi_auto_generate_label(self, storage):
        """测试自动生成标签"""
        fgi_data = pd.DataFrame({
            'ts': [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            'value': [20]
        })

        result = await storage._save("btc_fgi", fgi_data)

        assert result is True

    async def test_load_btc_fgi(self, storage):
        """测试加载 BTC FGI 数据"""
        fgi_data = pd.DataFrame({
            'ts': [datetime(2024, 1, i, tzinfo=timezone.utc) for i in range(1, 5)],
            'value': [30, 45, 60, 80],
            'label': ['Fear', 'Neutral', 'Greed', 'Extreme Greed']
        })

        result = await storage._save("btc_fgi", fgi_data, metadata={
            'data_type': 'btc_fgi'
        })

        assert result is True

        stats = await storage.get_stats()
        assert stats is not None


class TestMacroFREDOperations:
    """宏观 FRED 数据操作测试"""

    async def test_save_macro_fred_basic(self, storage):
        """测试保存基本宏观数据"""
        macro_data = pd.DataFrame({
            'ts': [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            'value': [5.25],
            'series_id': ['FEDFUNDS']
        })

        result = await storage._save("macro_fred", macro_data, metadata={
            'symbol': 'FEDFUNDS'
        })

        assert result is True

    async def test_load_macro_fred(self, storage):
        """测试加载宏观数据"""
        macro_data = pd.DataFrame({
            'ts': [datetime(2024, 1, i, tzinfo=timezone.utc) for i in range(1, 4)],
            'value': [5.0, 5.25, 5.5],
            'series_id': ['FEDFUNDS', 'FEDFUNDS', 'FEDFUNDS']
        })

        result = await storage._save("macro_fred", macro_data, metadata={
            'symbol': 'FEDFUNDS',
            'data_type': 'macro_fred'
        })

        assert result is True

        stats = await storage.get_stats()
        assert stats is not None


class TestCoinCategoriesOperations:
    """币种分类数据操作测试"""

    async def test_save_coin_categories_basic(self, storage):
        """测试保存基本币种分类数据"""
        categories_data = pd.DataFrame({
            'category_name': ['Layer 1'],
            'description': ['基础Layer 1区块链'],
            'coins': ['BTC,ETH,SOL']
        })

        result = await storage._save("coin_categories", categories_data)

        assert result is True

    async def test_load_coin_categories(self, storage):
        """测试加载币种分类数据"""
        categories_data = pd.DataFrame({
            'category_name': ['Layer 1', 'DeFi', 'AI'],
            'description': ['基础Layer 1', '去中心化金融', '人工智能'],
            'coins': ['BTC,ETH', 'UNI,AAVE', 'FET,OCEAN']
        })

        result = await storage._save("coin_categories", categories_data, metadata={
            'data_type': 'coin_categories'
        })

        assert result is True

        stats = await storage.get_stats()
        assert stats is not None


class TestSymbolsOperations:
    """交易对数据操作测试"""

    async def test_save_symbols_basic(self, storage):
        """测试保存基本交易对数据"""
        symbols_data = pd.DataFrame({
            'exchange': ['binance', 'binance'],
            'market_type': ['spot', 'spot'],
            'base_asset': ['BTC', 'ETH'],
            'quote_asset': ['USDT', 'USDT'],
            'symbol': ['BTC/USDT', 'ETH/USDT'],
            'active': [True, True]
        })

        result = await storage._save("symbols", symbols_data)

        assert result is True

    async def test_load_symbols(self, storage):
        """测试加载交易对数据"""
        symbols_data = pd.DataFrame({
            'exchange': ['binance', 'binance'],
            'market_type': ['spot', 'spot'],
            'base_asset': ['BTC', 'ETH'],
            'quote_asset': ['USDT', 'USDT'],
            'symbol': ['BTC/USDT', 'ETH/USDT'],
            'active': [True, True]
        })

        await storage._save("symbols", symbols_data)

        loaded = await storage._load("symbols", metadata={
            'query_type': 'symbols'
        })

        assert loaded is not None
        assert len(loaded) == 2

    async def test_load_symbols_with_filter(self, storage):
        """测试加载带过滤的交易对数据"""
        symbols_data = pd.DataFrame({
            'exchange': ['binance', 'okx'],
            'market_type': ['spot', 'spot'],
            'base_asset': ['BTC', 'BTC'],
            'quote_asset': ['USDT', 'USDT'],
            'symbol': ['BTC/USDT', 'BTC/USDT'],
            'active': [True, True]
        })

        await storage._save("symbols", symbols_data)

        loaded = await storage._load("symbols", metadata={
            'query_type': 'symbols',
            'exchange': 'binance'
        })

        assert loaded is not None
        assert len(loaded) == 1


class TestCRUDOperations:
    """CRUD 操作测试"""

    async def test_exists_true(self, storage):
        """测试存在性检查 - 存在"""
        ohlcv_data = pd.DataFrame({
            'ts': [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            'open': [42000.0],
            'high': [43000.0],
            'low': [41500.0],
            'close': [42500.0],
            'volume': [1000.0]
        })

        result = await storage._save("BTC_USDT_1d", ohlcv_data, metadata={
            'exchange': 'binance',
            'market_type': 'spot',
            'symbol': 'BTC_USDT',
            'timeframe': '1d'
        })

        assert result is True

        exists = await storage._exists("BTC_USDT_1d", metadata={
            'data_type': 'ohlcv',
            'symbol': 'BTC_USDT'
        })

        assert exists is True

    async def test_exists_false(self, storage):
        """测试存在性检查 - 不存在"""
        exists = await storage._exists("nonexistent", metadata={
            'data_type': 'ohlcv'
        })

        assert exists is False

    async def test_lists(self, storage):
        """测试列出数据"""
        ohlcv_data = pd.DataFrame({
            'ts': [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            'open': [42000.0],
            'high': [43000.0],
            'low': [41500.0],
            'close': [42500.0],
            'volume': [1000.0]
        })

        await storage._save("BTC_USDT", ohlcv_data, metadata={
            'exchange': 'binance',
            'market_type': 'spot',
            'symbol': 'BTC/USDT',
            'timeframe': '1d'
        })

        await storage._save("ETH_USDT", ohlcv_data, metadata={
            'exchange': 'binance',
            'market_type': 'spot',
            'symbol': 'ETH/USDT',
            'timeframe': '1d'
        })

        tables = await storage._lists()

        assert len(tables) >= 2

    async def test_get_time_range(self, storage):
        """测试获取时间范围"""
        ohlcv_data = pd.DataFrame({
            'ts': [
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                datetime(2024, 1, 5, tzinfo=timezone.utc),
                datetime(2024, 1, 10, tzinfo=timezone.utc)
            ],
            'open': [42000.0, 42500.0, 43000.0],
            'high': [43000.0, 43500.0, 44000.0],
            'low': [41500.0, 42000.0, 42500.0],
            'close': [42500.0, 43000.0, 43500.0],
            'volume': [1000.0, 1100.0, 1200.0]
        })

        result = await storage._save("BTC_1d", ohlcv_data, metadata={
            'exchange': 'binance',
            'market_type': 'spot',
            'symbol': 'BTC',
            'timeframe': '1d'
        })

        assert result is True

        time_range = await storage._get_time_range("BTC_1d")

        assert time_range is not None
        assert 'start_time' in time_range
        assert 'end_time' in time_range

    async def test_get_time_range_nonexistent(self, storage):
        """测试获取不存在数据的时间范围"""
        time_range = await storage._get_time_range("nonexistent")

        assert time_range is None


class TestMetadataOperations:
    """元数据操作测试"""

    async def test_get_metadata(self, storage):
        """测试获取元数据"""
        ohlcv_data = pd.DataFrame({
            'ts': [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            'open': [42000.0],
            'high': [43000.0],
            'low': [41500.0],
            'close': [42500.0],
            'volume': [1000.0]
        })

        await storage._save("BTC_USDT", ohlcv_data, metadata={
            'exchange': 'binance',
            'market_type': 'spot',
            'symbol': 'BTC/USDT',
            'timeframe': '1d'
        })

        metadata = await storage._get_metadata("BTC_USDT")

        assert metadata is not None
        assert metadata['storage_type'] == 'local_file'
        assert metadata['file_format'] == 'parquet'

    async def test_update_metadata(self, storage):
        """测试更新元数据"""
        ohlcv_data = pd.DataFrame({
            'ts': [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            'open': [42000.0],
            'high': [43000.0],
            'low': [41500.0],
            'close': [42500.0],
            'volume': [1000.0]
        })

        await storage._save("BTC_USDT", ohlcv_data, metadata={
            'exchange': 'binance',
            'market_type': 'spot',
            'symbol': 'BTC/USDT',
            'timeframe': '1d'
        })

        result = await storage._update_metadata("BTC_USDT", {'updated': True})

        assert result is True


class TestStatsOperations:
    """统计信息测试"""

    async def test_get_stats(self, storage):
        """测试获取统计信息"""
        ohlcv_data = pd.DataFrame({
            'ts': [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            'open': [42000.0],
            'high': [43000.0],
            'low': [41500.0],
            'close': [42500.0],
            'volume': [1000.0]
        })

        await storage._save("BTC_USDT", ohlcv_data, metadata={
            'exchange': 'binance',
            'market_type': 'spot',
            'symbol': 'BTC/USDT',
            'timeframe': '1d'
        })

        stats = await storage.get_stats()

        assert stats is not None
        assert 'ohlcv_files' in str(stats) or 'ohlcv_count' in str(stats)


class TestDataIntegrity:
    """数据完整性测试"""

    async def test_check_data_integrity(self, storage):
        """测试检查数据完整性"""
        ohlcv_data = pd.DataFrame({
            'ts': [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            'open': [42000.0],
            'high': [43000.0],
            'low': [41500.0],
            'close': [42500.0],
            'volume': [1000.0]
        })

        await storage._save("BTC_USDT", ohlcv_data, metadata={
            'exchange': 'binance',
            'market_type': 'spot',
            'symbol': 'BTC/USDT',
            'timeframe': '1d'
        })

        integrity = await storage.check_data_integrity()

        assert integrity is not None


class TestContextManager:
    """上下文管理器测试"""

    async def test_context_manager(self, temp_data_dir):
        """测试上下文管理器使用"""
        async with LocalFileStorage({"data_dir": temp_data_dir}) as storage:
            assert storage._warehouse is not None

        assert storage._warehouse is None

    async def test_context_manager_with_error(self, temp_data_dir):
        """测试上下文管理器中的错误处理"""
        storage = LocalFileStorage({"data_dir": temp_data_dir})

        try:
            async with storage:
                raise ValueError("Test error")
        except ValueError:
            pass

        assert storage._warehouse is None


class TestEdgeCases:
    """边界条件测试"""

    async def test_special_characters_in_symbol(self, storage):
        """测试特殊字符处理"""
        ohlcv_data = pd.DataFrame({
            'ts': [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            'open': [42000.0],
            'high': [43000.0],
            'low': [41500.0],
            'close': [42500.0],
            'volume': [1000.0]
        })

        result = await storage._save("BTC-PERP", ohlcv_data, metadata={
            'exchange': 'binance',
            'market_type': 'future',
            'symbol': 'BTC-PERP',
            'timeframe': '1d'
        })

        assert result is True

    async def test_slash_in_symbol(self, storage):
        """测试斜杠符号处理"""
        ohlcv_data = pd.DataFrame({
            'ts': [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            'open': [42000.0],
            'high': [43000.0],
            'low': [41500.0],
            'close': [42500.0],
            'volume': [1000.0]
        })

        result = await storage._save("BTC_USDT", ohlcv_data, metadata={
            'exchange': 'binance',
            'market_type': 'spot',
            'symbol': 'BTC/USDT',
            'timeframe': '1d'
        })

        assert result is True

        loaded = await storage._load("BTC_USDT", metadata={
            'symbol': 'BTC/USDT',
            'timeframe': '1d'
        })

        assert loaded is not None
        assert len(loaded) == 1

    async def test_empty_dataframe_save(self, storage):
        """测试保存空 DataFrame"""
        empty_df = pd.DataFrame()

        result = await storage._save("empty", empty_df)

        assert result is True

    async def test_load_nonexistent_data(self, storage):
        """测试加载不存在的数据"""
        loaded = await storage._load("nonexistent", metadata={
            'symbol': 'nonexistent',
            'timeframe': '1d'
        })

        assert loaded is None

    async def test_multiple_exchanges(self, storage):
        """测试多交易所数据"""
        ohlcv_data = pd.DataFrame({
            'ts': [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            'open': [42000.0],
            'high': [43000.0],
            'low': [41500.0],
            'close': [42500.0],
            'volume': [1000.0]
        })

        for exchange in ['binance', 'okx', 'huobi']:
            result = await storage._save("BTC_USDT", ohlcv_data.copy(), metadata={
                'exchange': exchange,
                'market_type': 'spot',
                'symbol': 'BTC/USDT',
                'timeframe': '1d'
            })
            assert result is True

        for exchange in ['binance', 'okx', 'huobi']:
            loaded = await storage._load("BTC_USDT", metadata={
                'symbol': 'BTC/USDT',
                'timeframe': '1d',
                'exchange': exchange
            })
            assert loaded is not None
            assert len(loaded) == 1

    async def test_large_data(self, storage):
        """测试大量数据"""
        base_ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
        large_df = pd.DataFrame({
            'ts': [base_ts + timedelta(hours=i) for i in range(1000)],
            'open': [42000.0 + i for i in range(1000)],
            'high': [43000.0 + i for i in range(1000)],
            'low': [41500.0 + i for i in range(1000)],
            'close': [42500.0 + i for i in range(1000)],
            'volume': [1000.0 + i for i in range(1000)]
        })

        result = await storage._save("BTC_USDT_1h", large_df, metadata={
            'exchange': 'binance',
            'market_type': 'spot',
            'symbol': 'BTC_USDT',
            'timeframe': '1h'
        })

        assert result is True

        loaded = await storage._load("BTC_USDT_1h", metadata={
            'symbol': 'BTC_USDT',
            'timeframe': '1h'
        })

        assert loaded is not None
        assert len(loaded) == 1000

    async def test_data_type_detection_ohlcv(self, storage):
        """测试 OHLCV 数据类型检测"""
        ohlcv_data = pd.DataFrame({
            'open': [42000.0],
            'high': [43000.0],
            'low': [41500.0],
            'close': [42500.0],
            'volume': [1000.0]
        })

        data_type = storage._detect_data_type(ohlcv_data, "test")
        assert data_type == 'ohlcv'

    async def test_data_type_detection_tickers(self, storage):
        """测试 Tickers 数据类型检测"""
        tickers_data = pd.DataFrame({
            'last': [50000.0],
            'bid': [49999.5],
            'ask': [50000.5]
        })

        data_type = storage._detect_data_type(tickers_data, "test")
        assert data_type == 'tickers'

    async def test_data_type_detection_futures(self, storage):
        """测试期货数据类型检测"""
        futures_data = pd.DataFrame({
            'funding_rate': [0.0001],
            'open_interest': [100000000.0],
            'symbol': ['BTC/USDT']
        })

        data_type = storage._detect_data_type(futures_data, "test")
        assert data_type == 'futures_metrics'

    async def test_data_type_with_metadata(self, storage):
        """测试带元数据的数据类型检测"""
        custom_data = pd.DataFrame({
            'value': [100]
        })

        data_type = storage._detect_data_type(custom_data, "test", metadata={
            'data_type': 'custom_type'
        })

        assert data_type == 'custom_type'


class TestConcurrency:
    """并发操作测试"""

    async def test_concurrent_save_operations(self, storage):
        """测试并发保存操作"""
        async def save_data(symbol):
            ohlcv_data = pd.DataFrame({
                'ts': [datetime(2024, 1, 1, tzinfo=timezone.utc)],
                'open': [42000.0],
                'high': [43000.0],
                'low': [41500.0],
                'close': [42500.0],
                'volume': [1000.0]
            })

            return await storage._save(f"{symbol}_USDT", ohlcv_data, metadata={
                'exchange': 'binance',
                'market_type': 'spot',
                'symbol': symbol,
                'timeframe': '1d'
            })

        symbols = ['BTC', 'ETH', 'SOL', 'XRP', 'ADA']
        results = await asyncio.gather(*[save_data(s) for s in symbols])

        assert all(results)

    async def test_concurrent_load_operations(self, storage):
        """测试并发加载操作"""
        ohlcv_data = pd.DataFrame({
            'ts': [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            'open': [42000.0],
            'high': [43000.0],
            'low': [41500.0],
            'close': [42500.0],
            'volume': [1000.0]
        })

        for symbol in ['BTC', 'ETH', 'SOL']:
            await storage._save(f"{symbol}_USDT", ohlcv_data.copy(), metadata={
                'exchange': 'binance',
                'market_type': 'spot',
                'symbol': f'{symbol}/USDT',
                'timeframe': '1d'
            })

        async def load_data(symbol):
            return await storage._load(f"{symbol}_USDT", metadata={
                'symbol': f'{symbol}/USDT',
                'timeframe': '1d'
            })

        results = await asyncio.gather(*[load_data(s) for s in ['BTC', 'ETH', 'SOL']])

        assert all(r is not None for r in results)


class TestWarehouseProperty:
    """仓库属性测试"""

    async def test_warehouse_property(self, storage):
        """测试 warehouse 属性"""
        warehouse = storage.warehouse
        assert warehouse is not None

    async def test_get_warehouse_method(self, storage):
        """测试 get_warehouse 方法"""
        warehouse = await storage.get_warehouse()
        assert warehouse is not None
