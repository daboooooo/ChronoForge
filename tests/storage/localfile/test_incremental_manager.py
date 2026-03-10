"""
LocalIncrementalManager 全面测试

针对本地文件存储增量更新管理器的全面测试，
覆盖所有关键功能和边界条件。
"""

import pytest
import tempfile
import shutil
from datetime import datetime, timezone, timedelta

import pandas as pd

from chronoforge.storage.localfile_storage.warehouse import LocalDataWarehouse
from chronoforge.storage.localfile_storage.incremental import LocalIncrementalManager
from chronoforge.storage.localfile_storage.quality import (
    LocalDataValidator, LocalIncrementalUpdater
)


@pytest.fixture
def temp_data_dir():
    """创建临时数据目录的fixture"""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    try:
        shutil.rmtree(temp_dir)
    except Exception:
        pass


@pytest.fixture
async def warehouse(temp_data_dir):
    """创建测试用数据仓库"""
    w = LocalDataWarehouse(data_dir=temp_data_dir)
    yield w
    await w.close()


@pytest.fixture
async def incremental_manager(warehouse):
    """创建测试用增量更新管理器"""
    manager = LocalIncrementalManager(warehouse)
    yield manager
    manager.clear_cache()


class TestLocalDataValidator:
    """本地数据验证器测试"""

    def test_validate_empty_data(self):
        """测试空数据验证"""
        validator = LocalDataValidator()
        result = validator.validate_data(pd.DataFrame(), 'ohlcv')
        assert result['valid'] is True
        assert result['errors'] == []

    def test_validate_unsupported_type(self):
        """测试不支持的数据类型"""
        validator = LocalDataValidator()
        df = pd.DataFrame({'col': [1, 2, 3]})
        result = validator.validate_data(df, 'unsupported_type')
        assert result['valid'] is False

    def test_validate_ohlcv_missing_columns(self):
        """测试OHLCV缺少必需列"""
        validator = LocalDataValidator()
        df = pd.DataFrame({'open': [100], 'high': [110]})
        result = validator.validate_data(df, 'ohlcv')
        assert result['valid'] is False
        assert '缺少必需列' in result['errors'][0]

    def test_validate_ohlcv_valid_data(self):
        """测试有效的OHLCV数据"""
        validator = LocalDataValidator()
        df = pd.DataFrame({
            'open': [100.0], 'high': [110.0], 'low': [95.0],
            'close': [105.0], 'volume': [1000.0],
            'ts': [datetime(2024, 1, 1, tzinfo=timezone.utc)]
        })
        result = validator.validate_data(df, 'ohlcv')
        assert result['valid'] is True


class TestLocalIncrementalUpdater:
    """本地增量更新器测试"""

    def test_detect_new_records_empty_existing(self):
        """测试空现有数据时的新记录检测"""
        updater = LocalIncrementalUpdater()
        new_data = pd.DataFrame({
            'ts': [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            'close': [42000.0]
        })
        existing_data = pd.DataFrame()
        result = updater.detect_new_records(new_data, existing_data, 'ohlcv')
        assert len(result) == len(new_data)

    def test_detect_new_records_with_existing(self):
        """测试有现有数据时的新记录检测"""
        updater = LocalIncrementalUpdater()
        
        existing_data = pd.DataFrame({
            'exchange': ['binance'],
            'market_type': ['spot'],
            'symbol': ['BTC/USDT'],
            'timeframe': ['1d'],
            'ts': [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            'close': [42000.0]
        })
        
        new_data = pd.DataFrame({
            'exchange': ['binance', 'binance'],
            'market_type': ['spot', 'spot'],
            'symbol': ['BTC/USDT', 'BTC/USDT'],
            'timeframe': ['1d', '1d'],
            'ts': [
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                datetime(2024, 1, 2, tzinfo=timezone.utc)
            ],
            'close': [42000.0, 43000.0]
        })
        
        result = updater.detect_new_records(new_data, existing_data, 'ohlcv')
        assert len(result) == 1

    def test_detect_duplicate_records(self):
        """测试重复记录检测"""
        updater = LocalIncrementalUpdater()
        
        data = pd.DataFrame({
            'exchange': ['binance', 'binance', 'binance'],
            'market_type': ['spot', 'spot', 'spot'],
            'symbol': ['BTC/USDT', 'BTC/USDT', 'BTC/USDT'],
            'timeframe': ['1d', '1d', '1d'],
            'ts': [
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                datetime(2024, 1, 2, tzinfo=timezone.utc)
            ],
            'close': [42000.0, 42000.0, 43000.0]
        })
        
        deduped, duplicates = updater.detect_duplicate_records(data, 'ohlcv')
        assert len(deduped) == 2
        assert len(duplicates) == 1


class TestLocalIncrementalManagerInit:
    """管理器初始化测试"""

    async def test_init(self, warehouse):
        """测试管理器初始化"""
        manager = LocalIncrementalManager(warehouse)
        assert manager.warehouse is not None
        assert manager.validator is not None
        assert manager.updater is not None


class TestGetLatestTickerTimestamp:
    """获取最新Ticker时间戳测试 - 关键功能！"""

    async def test_get_latest_ticker_timestamp_empty(self, incremental_manager):
        """测试空数据时返回None"""
        result = await incremental_manager._get_latest_ticker_timestamp()
        assert result is None

    async def test_get_latest_ticker_timestamp_single_file(self, warehouse, incremental_manager):
        """测试单个文件时返回正确时间戳"""
        tickers_data = pd.DataFrame([
            {
                'exchange': 'binance',
                'market_type': 'spot',
                'symbol': 'BTC/USDT',
                'ts': datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
                'last': 50000.0,
                'bid': 49999.5,
                'ask': 50000.5
            }
        ])
        await warehouse.insert_tickers(tickers_data)
        
        result = await incremental_manager._get_latest_ticker_timestamp()
        assert result is not None
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 1

    async def test_get_latest_ticker_timestamp_multiple_files(self, warehouse, incremental_manager):
        """测试多个文件时返回最新时间戳"""
        tickers_data1 = pd.DataFrame([
            {
                'exchange': 'binance',
                'market_type': 'spot',
                'symbol': 'BTC/USDT',
                'ts': datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
                'last': 50000.0,
                'bid': 49999.5,
                'ask': 50000.5
            }
        ])
        tickers_data2 = pd.DataFrame([
            {
                'exchange': 'binance',
                'market_type': 'spot',
                'symbol': 'ETH/USDT',
                'ts': datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc),
                'last': 3000.0,
                'bid': 2999.5,
                'ask': 3000.5
            }
        ])
        await warehouse.insert_tickers(tickers_data1)
        await warehouse.insert_tickers(tickers_data2)
        
        result = await incremental_manager._get_latest_ticker_timestamp()
        assert result is not None
        assert result.day == 2

    async def test_get_latest_ticker_timestamp_no_ts_column(self, warehouse, incremental_manager):
        """测试无时间戳列时返回None"""
        await warehouse.insert_tickers(pd.DataFrame([{
            'exchange': 'binance',
            'market_type': 'spot',
            'symbol': 'BTC/USDT',
            'last': 50000.0
        }]))
        
        result = await incremental_manager._get_latest_ticker_timestamp()
        assert result is None


class TestGetExistingFuturesMetrics:
    """获取现有期货指标数据测试 - 关键功能！"""

    async def test_get_existing_futures_metrics_empty(self, incremental_manager):
        """测试空数据时返回空DataFrame"""
        result = await incremental_manager._get_existing_futures_metrics('BTC/USDT')
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    async def test_get_existing_futures_metrics_with_data(self, warehouse, incremental_manager):
        """测试有数据时返回正确数据"""
        metrics_data = pd.DataFrame([
            {
                'exchange': 'binance',
                'symbol': 'BTC/USDT',
                'ts': datetime(2024, 1, 1, tzinfo=timezone.utc),
                'funding_rate': 0.0001,
                'open_interest': 100000000.0
            }
        ])
        await warehouse.insert_futures_metrics(metrics_data)
        
        result = await incremental_manager._get_existing_futures_metrics('BTC/USDT')
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result['symbol'].iloc[0] == 'BTC/USDT'

    async def test_get_existing_futures_metrics_caching(self, warehouse, incremental_manager):
        """测试缓存机制"""
        metrics_data = pd.DataFrame([
            {
                'exchange': 'binance',
                'symbol': 'BTC/USDT',
                'ts': datetime(2024, 1, 1, tzinfo=timezone.utc),
                'funding_rate': 0.0001,
                'open_interest': 100000000.0
            }
        ])
        await warehouse.insert_futures_metrics(metrics_data)
        
        result1 = await incremental_manager._get_existing_futures_metrics('BTC/USDT')
        result2 = await incremental_manager._get_existing_futures_metrics('BTC/USDT')
        
        pd.testing.assert_frame_equal(result1, result2)


class TestSmartInsertTickersIncremental:
    """智能Tickers增量插入测试 - 关键功能！"""

    async def test_old_data_skipped_with_incremental(self, warehouse, incremental_manager):
        """测试旧数据被正确跳过 - 这是发现bug的核心测试！"""
        initial_data = pd.DataFrame([
            {
                'exchange': 'binance',
                'market_type': 'spot',
                'symbol': 'BTC/USDT',
                'ts': datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
                'last': 50000.0,
                'bid': 49999.5,
                'ask': 50000.5
            }
        ])
        await warehouse.insert_tickers(initial_data)
        
        old_data = pd.DataFrame([
            {
                'exchange': 'binance',
                'market_type': 'spot',
                'symbol': 'BTC/USDT',
                'ts': datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
                'last': 49000.0,
                'bid': 48999.5,
                'ask': 49000.5
            }
        
        ])
        
        result = await incremental_manager.smart_insert_tickers(
            old_data, incremental=True, validation=False
        )
        
        assert result['status'] == 'success'
        assert result['inserted'] == 0
        assert result['skipped'] == 1

    async def test_new_data_inserted_with_incremental(self, warehouse, incremental_manager):
        """测试新数据被正确插入"""
        initial_data = pd.DataFrame([
            {
                'exchange': 'binance',
                'market_type': 'spot',
                'symbol': 'BTC/USDT',
                'ts': datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
                'last': 50000.0,
                'bid': 49999.5,
                'ask': 50000.5
            }
        ])
        await warehouse.insert_tickers(initial_data)
        
        new_data = pd.DataFrame([
            {
                'exchange': 'binance',
                'market_type': 'spot',
                'symbol': 'BTC/USDT',
                'ts': datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc),
                'last': 51000.0,
                'bid': 50999.5,
                'ask': 51000.5
            }
        
        ])
        
        result = await incremental_manager.smart_insert_tickers(
            new_data, incremental=True, validation=False
        )
        
        assert result['status'] == 'success'
        assert result['inserted'] == 1
        assert result['skipped'] == 0


class TestSmartInsertOHLCV:
    """智能OHLCV插入测试"""

    async def test_smart_insert_ohlcv_empty(self, incremental_manager):
        """测试空数据插入"""
        result = await incremental_manager.smart_insert_ohlcv(pd.DataFrame())
        assert result['status'] == 'success'
        assert result['inserted'] == 0

    async def test_smart_insert_ohlcv_basic(self, warehouse, incremental_manager):
        """测试基本插入"""
        ohlcv_data = pd.DataFrame([
            {
                'exchange': 'binance',
                'market_type': 'spot',
                'symbol': 'BTC/USDT',
                'timeframe': '1d',
                'ts': datetime(2024, 1, 1, tzinfo=timezone.utc),
                'open': 42000.0,
                'high': 43000.0,
                'low': 41500.0,
                'close': 42500.0,
                'volume': 1000.0
            }
        ])
        result = await incremental_manager.smart_insert_ohlcv(ohlcv_data)
        assert result['status'] == 'success'
        assert result['inserted'] == 1

    async def test_smart_insert_ohlcv_incremental(self, warehouse, incremental_manager):
        """测试增量插入"""
        initial_data = pd.DataFrame([
            {
                'exchange': 'binance',
                'market_type': 'spot',
                'symbol': 'BTC/USDT',
                'timeframe': '1d',
                'ts': datetime(2024, 1, 1, tzinfo=timezone.utc),
                'open': 42000.0,
                'high': 43000.0,
                'low': 41500.0,
                'close': 42500.0,
                'volume': 1000.0
            }
        ])
        await warehouse.insert_ohlcv(initial_data)
        incremental_manager.clear_cache()
        
        incremental_data = pd.DataFrame([
            {
                'exchange': 'binance',
                'market_type': 'spot',
                'symbol': 'BTC/USDT',
                'timeframe': '1d',
                'ts': datetime(2024, 1, 2, tzinfo=timezone.utc),
                'open': 42500.0,
                'high': 43500.0,
                'low': 42000.0,
                'close': 43000.0,
                'volume': 1200.0
            }
        
        ])
        result = await incremental_manager.smart_insert_ohlcv(incremental_data)
        assert result['status'] == 'success'
        assert result['inserted'] == 1


class TestSmartInsertFuturesMetrics:
    """智能期货指标插入测试"""

    async def test_smart_insert_futures_metrics_basic(self, warehouse, incremental_manager):
        """测试基本插入"""
        metrics_data = pd.DataFrame([
            {
                'exchange': 'binance',
                'symbol': 'BTC/USDT',
                'ts': datetime(2024, 1, 1, tzinfo=timezone.utc),
                'funding_rate': 0.0001,
                'open_interest': 100000000.0
            }
        ])
        result = await incremental_manager.smart_insert_futures_metrics(metrics_data)
        assert result['status'] == 'success'
        assert result['inserted'] == 1

    async def test_smart_insert_futures_metrics_incremental(self, warehouse, incremental_manager):
        """测试增量插入"""
        initial_data = pd.DataFrame([
            {
                'exchange': 'binance',
                'symbol': 'BTC/USDT',
                'ts': datetime(2024, 1, 1, tzinfo=timezone.utc),
                'funding_rate': 0.0001,
                'open_interest': 100000000.0
            }
        ])
        await warehouse.insert_futures_metrics(initial_data)
        
        incremental_data = pd.DataFrame([
            {
                'exchange': 'binance',
                'symbol': 'BTC/USDT',
                'ts': datetime(2024, 1, 2, tzinfo=timezone.utc),
                'funding_rate': 0.00015,
                'open_interest': 105000000.0
            }
        
        ])
        result = await incremental_manager.smart_insert_futures_metrics(incremental_data)
        assert result['status'] == 'success'
        assert result['inserted'] == 1


class TestCacheOperations:
    """缓存操作测试"""

    async def test_is_cache_valid_empty(self, incremental_manager):
        """测试空缓存"""
        assert incremental_manager._is_cache_valid('nonexistent') is False

    async def test_cache_set_and_validate(self, incremental_manager):
        """测试缓存设置和验证"""
        incremental_manager._cache['test_key'] = {'data': 'value'}
        incremental_manager._last_cache_update['test_key'] = datetime.now()
        assert incremental_manager._is_cache_valid('test_key') is True

    async def test_cache_expired(self, incremental_manager):
        """测试缓存过期"""
        incremental_manager._cache['test_key'] = {'data': 'value'}
        incremental_manager._last_cache_update['test_key'] = datetime.now() - timedelta(seconds=400)
        assert incremental_manager._is_cache_valid('test_key') is False

    async def test_clear_cache(self, incremental_manager):
        """测试清除缓存"""
        incremental_manager._cache['test_key'] = {'data': 'value'}
        incremental_manager._last_cache_update['test_key'] = datetime.now()
        incremental_manager.clear_cache()
        assert len(incremental_manager._cache) == 0
        assert incremental_manager._is_cache_valid('test_key') is False


class TestGetLatestTimestamp:
    """获取最新时间戳测试"""

    async def test_get_latest_timestamp_empty(self, incremental_manager):
        """测试空数据"""
        result = await incremental_manager.get_latest_timestamp('BTC/USDT', '1d')
        assert result is None

    async def test_get_latest_timestamp_with_data(self, warehouse, incremental_manager):
        """测试有数据时返回正确时间戳"""
        ohlcv_data = pd.DataFrame([
            {
                'exchange': 'binance',
                'market_type': 'spot',
                'symbol': 'BTC/USDT',
                'timeframe': '1d',
                'ts': datetime(2024, 1, 1, tzinfo=timezone.utc),
                'open': 42000.0,
                'high': 43000.0,
                'low': 41500.0,
                'close': 42500.0,
                'volume': 1000.0
            },
            {
                'exchange': 'binance',
                'market_type': 'spot',
                'symbol': 'BTC/USDT',
                'timeframe': '1d',
                'ts': datetime(2024, 1, 2, tzinfo=timezone.utc),
                'open': 42500.0,
                'high': 43500.0,
                'low': 42000.0,
                'close': 43000.0,
                'volume': 1200.0
            }
        ])
        await warehouse.insert_ohlcv(ohlcv_data)
        
        result = await incremental_manager.get_latest_timestamp('BTC/USDT', '1d')
        assert result is not None
        assert result.day == 2


class TestSuggestDataFetchRange:
    """建议数据获取范围测试"""

    async def test_suggest_range_no_data(self, incremental_manager):
        """测试无数据时的建议"""
        result = await incremental_manager.suggest_data_fetch_range('BTC/USDT', '1d')
        assert result['status'] == 'no_data'
        assert 'suggested_start' in result
        assert 'suggested_end' in result

    async def test_suggest_range_needs_update(self, warehouse, incremental_manager):
        """测试需要更新时的建议"""
        ohlcv_data = pd.DataFrame([
            {
                'exchange': 'binance',
                'market_type': 'spot',
                'symbol': 'BTC/USDT',
                'timeframe': '1h',
                'ts': datetime(2024, 1, 1, tzinfo=timezone.utc),
                'open': 42000.0,
                'high': 43000.0,
                'low': 41500.0,
                'close': 42500.0,
                'volume': 1000.0
            }
        ])
        await warehouse.insert_ohlcv(ohlcv_data)
        
        result = await incremental_manager.suggest_data_fetch_range('BTC/USDT', '1h')
        assert result['status'] == 'needs_update'
        assert 'suggested_start' in result
        assert 'suggested_end' in result


class TestGetIncrementalStats:
    """获取增量统计测试"""

    async def test_get_incremental_stats(self, incremental_manager):
        """测试获取统计信息"""
        result = await incremental_manager.get_incremental_stats()
        assert 'cache_size' in result
        assert 'cache_entries' in result
        assert 'cache_ttl' in result


class TestEdgeCases:
    """边界条件和异常测试"""

    async def test_special_characters_in_symbol(self, warehouse, incremental_manager):
        """测试特殊字符symbol"""
        ohlcv_data = pd.DataFrame([
            {
                'exchange': 'binance',
                'market_type': 'future',
                'symbol': 'BTC-PERP',
                'timeframe': '1d',
                'ts': datetime(2024, 1, 1, tzinfo=timezone.utc),
                'open': 42000.0,
                'high': 43000.0,
                'low': 41500.0,
                'close': 42500.0,
                'volume': 1000.0
            }
        ])
        result = await incremental_manager.smart_insert_ohlcv(ohlcv_data)
        assert result['status'] == 'success'

    async def test_slash_in_symbol(self, warehouse, incremental_manager):
        """测试斜杠符号 (BTC/USDT)"""
        ohlcv_data = pd.DataFrame([
            {
                'exchange': 'binance',
                'market_type': 'spot',
                'symbol': 'BTC/USDT',
                'timeframe': '1d',
                'ts': datetime(2024, 1, 1, tzinfo=timezone.utc),
                'open': 42000.0,
                'high': 43000.0,
                'low': 41500.0,
                'close': 42500.0,
                'volume': 1000.0
            }
        ])
        result = await incremental_manager.smart_insert_ohlcv(ohlcv_data)
        assert result['status'] == 'success'
        
        df = await warehouse.get_ohlcv('BTC/USDT', '1d')
        assert df is not None
        assert len(df) == 1

    async def test_duplicate_prevention(self, warehouse, incremental_manager):
        """测试重复数据预防"""
        data = pd.DataFrame([
            {
                'exchange': 'binance',
                'market_type': 'spot',
                'symbol': 'BTC/USDT',
                'timeframe': '1d',
                'ts': datetime(2024, 1, 1, tzinfo=timezone.utc),
                'open': 42000.0,
                'high': 43000.0,
                'low': 41500.0,
                'close': 42500.0,
                'volume': 1000.0
            }
        ])

        result1 = await incremental_manager.smart_insert_ohlcv(data)
        incremental_manager.clear_cache()
        result2 = await incremental_manager.smart_insert_ohlcv(data)
        
        assert result1['inserted'] == 1
        assert result2['inserted'] == 1

    async def test_multiple_symbols_grouped(self, warehouse, incremental_manager):
        """测试多symbol分组处理"""
        data = pd.DataFrame([
            {
                'exchange': 'binance',
                'market_type': 'spot',
                'symbol': 'BTC/USDT',
                'timeframe': '1d',
                'ts': datetime(2024, 1, 1, tzinfo=timezone.utc),
                'open': 42000.0,
                'high': 43000.0,
                'low': 41500.0,
                'close': 42500.0,
                'volume': 1000.0
            },
            {
                'exchange': 'binance',
                'market_type': 'spot',
                'symbol': 'ETH/USDT',
                'timeframe': '1d',
                'ts': datetime(2024, 1, 1, tzinfo=timezone.utc),
                'open': 2200.0,
                'high': 2300.0,
                'low': 2150.0,
                'close': 2250.0,
                'volume': 5000.0
            }
        ])
        result = await incremental_manager.smart_insert_ohlcv(data)
        assert result['status'] == 'success'
        assert result['inserted'] == 2
