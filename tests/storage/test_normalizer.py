"""
DataFrameNormalizer 单元测试

测试数据标准化器的所有功能，包括：
1. 数据类型检测
2. 列名标准化
3. OHLCV数据标准化
4. Tickers数据标准化
5. 期货指标数据标准化
6. BTC FGI数据标准化
7. 宏观FRED数据标准化
"""

import pytest
from datetime import datetime, timezone

import pandas as pd

from chronoforge.storage.normalizer import DataFrameNormalizer


class TestDataTypeDetection:
    """数据类型检测测试"""

    def test_detect_ohlcv_by_columns(self):
        """测试通过列名检测OHLCV类型"""
        data = pd.DataFrame({
            'open': [42000.0],
            'high': [43000.0],
            'low': [41500.0],
            'close': [42500.0],
            'volume': [1000.0]
        })
        result = DataFrameNormalizer.detect_data_type(data, "test")
        assert result == 'ohlcv'

    def test_detect_tickers_by_columns(self):
        """测试通过列名检测Tickers类型"""
        data = pd.DataFrame({
            'last': [50000.0],
            'bid': [49999.5],
            'ask': [50000.5]
        })
        result = DataFrameNormalizer.detect_data_type(data, "test")
        assert result == 'tickers'

    def test_detect_futures_metrics_by_columns(self):
        """测试通过列名检测期货指标类型"""
        data = pd.DataFrame({
            'funding_rate': [0.0001],
            'open_interest': [100000000.0],
            'symbol': ['BTC/USDT']
        })
        result = DataFrameNormalizer.detect_data_type(data, "test")
        assert result == 'futures_metrics'

    def test_detect_btc_fgi_by_columns(self):
        """测试通过列名检测BTC FGI类型"""
        data = pd.DataFrame({
            'value': [65],
            'label': ['Greed']
        })
        result = DataFrameNormalizer.detect_data_type(data, "test")
        assert result == 'btc_fgi'

    def test_detect_macro_fred_by_columns(self):
        """测试通过列名检测宏观FRED类型"""
        data = pd.DataFrame({
            'series_id': ['FEDFUNDS'],
            'ts': [datetime(2024, 1, 1)],
            'value': [5.25]
        })
        result = DataFrameNormalizer.detect_data_type(data, "test")
        assert result == 'macro_fred'

    def test_detect_from_metadata(self):
        """测试从metadata中获取数据类型"""
        data = pd.DataFrame({'value': [100]})
        result = DataFrameNormalizer.detect_data_type(
            data, "test", metadata={'data_type': 'custom_type'}
        )
        assert result == 'custom_type'

    def test_detect_from_id_ohlcv(self):
        """测试从ID中检测OHLCV类型"""
        data = pd.DataFrame({
            'close': [42500.0],
            'volume': [1000.0]
        })
        result = DataFrameNormalizer.detect_data_type(data, "BTC_USDT_1d")
        assert result == 'ohlcv'

    def test_detect_from_id_btc_fgi(self):
        """测试从ID中检测BTC FGI类型"""
        data = pd.DataFrame({'value': [50]})
        result = DataFrameNormalizer.detect_data_type(data, "btc_fgi_index")
        assert result == 'btc_fgi'

    def test_detect_from_id_futures(self):
        """测试从ID中检测期货类型"""
        data = pd.DataFrame({'value': [50]})
        result = DataFrameNormalizer.detect_data_type(data, "BTC_PERP_futures")
        assert result == 'futures_metrics'

    def test_detect_from_id_symbols(self):
        """测试从ID中检测Symbols类型"""
        data = pd.DataFrame({'value': [50]})
        result = DataFrameNormalizer.detect_data_type(data, "symbol_universe")
        assert result == 'symbols'


class TestColumnNormalization:
    """列名标准化测试"""

    def test_normalize_columns_basic(self):
        """测试基本列名标准化"""
        data = pd.DataFrame({
            'timestamp': [datetime(2024, 1, 1)],
            'open': [42000.0]
        })
        result = DataFrameNormalizer.normalize_columns(
            data, DataFrameNormalizer.OHLCV_COLUMN_MAPPING
        )
        assert 'ts' in result.columns
        assert 'timestamp' not in result.columns

    def test_normalize_columns_preserve_existing(self):
        """测试保留已存在的目标列名"""
        data = pd.DataFrame({
            'timestamp': [datetime(2024, 1, 1)],
            'ts': [datetime(2024, 1, 1)]
        })
        result = DataFrameNormalizer.normalize_columns(
            data, DataFrameNormalizer.OHLCV_COLUMN_MAPPING
        )
        assert 'ts' in result.columns

    def test_normalize_columns_multiple(self):
        """测试多列同时标准化"""
        data = pd.DataFrame({
            'time': [datetime(2024, 1, 1)],
            'amount': [1000.0]
        })
        result = DataFrameNormalizer.normalize_columns(
            data, DataFrameNormalizer.OHLCV_COLUMN_MAPPING
        )
        assert 'ts' in result.columns
        assert 'quote_volume' in result.columns


class TestOHLCVNormalization:
    """OHLCV数据标准化测试"""

    def test_normalize_ohlcv_basic(self):
        """测试基本OHLCV标准化"""
        data = pd.DataFrame({
            'ts': [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            'open': [42000.0],
            'high': [43000.0],
            'low': [41500.0],
            'close': [42500.0],
            'volume': [1000.0]
        })
        result = DataFrameNormalizer.normalize_ohlcv(
            data, "BTC_USDT_1d",
            metadata={'exchange': 'binance', 'market_type': 'spot'}
        )
        assert result is not None
        assert 'exchange' in result.columns
        assert 'market_type' in result.columns

    def test_normalize_ohlcv_with_column_mapping(self):
        """测试带列名映射的OHLCV标准化"""
        data = pd.DataFrame({
            'timestamp': [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            'open': [42000.0],
            'high': [43000.0],
            'low': [41500.0],
            'close': [42500.0],
            'volume': [1000.0]
        })
        result = DataFrameNormalizer.normalize_ohlcv(
            data, "BTC_USDT",
            metadata={'exchange': 'binance', 'timeframe': '1d'}
        )
        assert result is not None
        assert 'ts' in result.columns

    def test_normalize_ohlcv_missing_ts(self):
        """测试缺少ts列时的行为"""
        data = pd.DataFrame({
            'open': [42000.0],
            'high': [43000.0],
            'low': [41500.0],
            'close': [42500.0],
            'volume': [1000.0]
        })
        result = DataFrameNormalizer.normalize_ohlcv(
            data, "BTC_USDT",
            metadata={'exchange': 'binance'}
        )
        assert result is None

    def test_normalize_ohlcv_add_optional_columns(self):
        """测试添加可选列"""
        data = pd.DataFrame({
            'ts': [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            'open': [42000.0],
            'high': [43000.0],
            'low': [41500.0],
            'close': [42500.0],
            'volume': [1000.0]
        })
        result = DataFrameNormalizer.normalize_ohlcv(
            data, "BTC_USDT",
            metadata={'exchange': 'binance', 'market_type': 'spot'}
        )
        assert 'quote_volume' in result.columns
        assert 'source' in result.columns


class TestTickersNormalization:
    """Tickers数据标准化测试"""

    def test_normalize_tickers_basic(self):
        """测试基本Tickers标准化"""
        data = pd.DataFrame({
            'ts': [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            'last': [50000.0],
            'bid': [49999.5],
            'ask': [50000.5]
        })
        result = DataFrameNormalizer.normalize_tickers(
            data, "BTC_ticker",
            metadata={'exchange': 'binance'}
        )
        assert result is not None
        assert 'exchange' in result.columns


class TestFuturesMetricsNormalization:
    """期货指标数据标准化测试"""

    def test_normalize_futures_metrics_basic(self):
        """测试基本期货指标标准化"""
        data = pd.DataFrame({
            'ts': [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            'funding_rate': [0.0001],
            'open_interest': [100000000.0]
        })
        result = DataFrameNormalizer.normalize_futures_metrics(
            data, "BTC_PERP",
            metadata={'exchange': 'binance_um'}
        )
        assert result is not None
        assert 'exchange' in result.columns


class TestBTCFGINormalization:
    """BTC FGI数据标准化测试"""

    def test_normalize_btc_fgi_basic(self):
        """测试基本BTC FGI标准化"""
        data = pd.DataFrame({
            'ts': [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            'value': [65],
            'label': ['Greed']
        })
        result = DataFrameNormalizer.normalize_btc_fgi(data, "btc_fgi", None)
        assert result is not None

    def test_normalize_btc_fgi_auto_label(self):
        """测试自动生成标签"""
        data = pd.DataFrame({
            'ts': [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            'value': [30]  # 30属于Fear范围 (25-45)
        })
        result = DataFrameNormalizer.normalize_btc_fgi(data, "btc_fgi", None)
        assert result is not None
        assert result['label'].iloc[0] == 'Fear'


class TestMacroFREDNormalization:
    """宏观FRED数据标准化测试"""

    def test_normalize_macro_fred_basic(self):
        """测试基本宏观FRED标准化"""
        data = pd.DataFrame({
            'ts': [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            'value': [5.25]
        })
        result = DataFrameNormalizer.normalize_macro_fred(
            data, "FEDFUNDS",
            metadata={'series_id': 'FEDFUNDS'}
        )
        assert result is not None
        assert 'series_id' in result.columns


class TestHelperMethods:
    """辅助方法测试"""

    def test_to_records(self):
        """测试转换为记录列表"""
        data = pd.DataFrame({
            'open': [42000.0],
            'close': [42500.0]
        })
        records = DataFrameNormalizer.to_records(data)
        assert len(records) == 1
        assert records[0]['open'] == 42000.0

    def test_parse_timeframe_from_id(self):
        """测试从ID解析时间周期"""
        assert DataFrameNormalizer.parse_timeframe_from_id("BTC_USDT_1d") == "1d"
        assert DataFrameNormalizer.parse_timeframe_from_id("BTC_USDT_1h") == "1h"
        assert DataFrameNormalizer.parse_timeframe_from_id("BTC_USDT_4h") == "4h"
        assert DataFrameNormalizer.parse_timeframe_from_id("BTC_USDT") == "1d"

    def test_parse_symbol_from_id(self):
        """测试从ID解析交易对"""
        assert DataFrameNormalizer.parse_symbol_from_id("BTC_USDT_1d") == "BTC_USDT"
        assert DataFrameNormalizer.parse_symbol_from_id("ETH_USDT_1h") == "ETH_USDT"
        assert DataFrameNormalizer.parse_symbol_from_id("BTC") == "BTC"


class TestConstants:
    """常量定义测试"""

    def test_timeframes_constant(self):
        """测试时间周期常量"""
        assert '1m' in DataFrameNormalizer.TIMEFRAMES
        assert '5m' in DataFrameNormalizer.TIMEFRAMES
        assert '1h' in DataFrameNormalizer.TIMEFRAMES
        assert '1d' in DataFrameNormalizer.TIMEFRAMES

    def test_ohlcv_required_cols(self):
        """测试OHLCV必需列"""
        assert 'ts' in DataFrameNormalizer.OHLCV_REQUIRED_COLS
        assert 'open' in DataFrameNormalizer.OHLCV_REQUIRED_COLS
        assert 'high' in DataFrameNormalizer.OHLCV_REQUIRED_COLS
        assert 'low' in DataFrameNormalizer.OHLCV_REQUIRED_COLS
        assert 'close' in DataFrameNormalizer.OHLCV_REQUIRED_COLS
        assert 'volume' in DataFrameNormalizer.OHLCV_REQUIRED_COLS

    def test_ohlcv_optional_cols(self):
        """测试OHLCV可选列"""
        assert 'quote_volume' in DataFrameNormalizer.OHLCV_OPTIONAL_COLS
        assert 'source' in DataFrameNormalizer.OHLCV_OPTIONAL_COLS
