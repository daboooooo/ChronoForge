"""
存储后端功能一致性测试

测试 DuckDB 和 LocalFile 两种存储后端的功能一致性，
确保它们在相同操作下表现一致。
"""

import tempfile
import pytest
from datetime import datetime, timezone
import pandas as pd


class TestWarehouseSymbolsConsistency:
    """测试 symbols 数据在两种后端的一致性"""

    @pytest.fixture
    def sample_symbols_data(self):
        """生成测试用的 symbols 数据"""
        return pd.DataFrame([
            {
                'symbol': 'BTC/USDT',
                'exchange': 'binance',
                'market_type': 'spot',
                'base_asset': 'BTC',
                'quote_asset': 'USDT',
                'active': True
            },
            {
                'symbol': 'ETH/USDT',
                'exchange': 'binance',
                'market_type': 'spot',
                'base_asset': 'ETH',
                'quote_asset': 'USDT',
                'active': True
            },
            {
                'symbol': 'BTC-PERP',
                'exchange': 'binance',
                'market_type': 'perpetual',
                'base_asset': 'BTC',
                'quote_asset': 'USDT',
                'active': True
            }
        ])

    @pytest.mark.asyncio
    async def test_duckdb_symbols_insert_and_query(self, sample_symbols_data):
        """测试 DuckDB symbols 插入和查询"""
        from chronoforge.storage.duckdb_storage.warehouse import FinancialDataWarehouse

        with tempfile.TemporaryDirectory() as tmpdir:
            warehouse = FinancialDataWarehouse(db_path=f"{tmpdir}/test.duckdb")

            result = await warehouse.insert_symbols(sample_symbols_data)
            assert result is True

            df = await warehouse.get_symbols()
            assert df is not None
            assert len(df) == 3

            df_binance = await warehouse.get_symbols(exchange='binance')
            assert len(df_binance) == 3

            df_spot = await warehouse.get_symbols(market_type='spot')
            assert len(df_spot) == 2

            await warehouse.close()

    @pytest.mark.asyncio
    async def test_localfile_symbols_insert_and_query(self, sample_symbols_data):
        """测试 LocalFile symbols 插入和查询"""
        from chronoforge.storage.localfile_storage.warehouse import LocalDataWarehouse

        with tempfile.TemporaryDirectory() as tmpdir:
            warehouse = LocalDataWarehouse(data_dir=f"{tmpdir}/local_data")

            result = await warehouse.insert_symbols(sample_symbols_data)
            assert result is True

            df = await warehouse.get_symbols()
            assert df is not None
            assert len(df) == 3

            df_binance = await warehouse.get_symbols(exchange='binance')
            assert len(df_binance) == 3

            df_spot = await warehouse.get_symbols(market_type='spot')
            assert len(df_spot) == 2

            await warehouse.close()

    @pytest.mark.asyncio
    async def test_symbols_consistency_between_backends(self, sample_symbols_data):
        """测试两种后端 symbols 操作结果一致性"""
        from chronoforge.storage.duckdb_storage.warehouse import FinancialDataWarehouse
        from chronoforge.storage.localfile_storage.warehouse import LocalDataWarehouse

        with tempfile.TemporaryDirectory() as tmpdir:
            duckdb_path = f"{tmpdir}/duckdb"
            localfile_path = f"{tmpdir}/localfile"

            duckdb_wh = FinancialDataWarehouse(db_path=f"{duckdb_path}/test.duckdb")
            localfile_wh = LocalDataWarehouse(data_dir=localfile_path)

            duckdb_result = await duckdb_wh.insert_symbols(sample_symbols_data)
            localfile_result = await localfile_wh.insert_symbols(sample_symbols_data)

            assert duckdb_result is True
            assert localfile_result is True

            duckdb_df = await duckdb_wh.get_symbols()
            localfile_df = await localfile_wh.get_symbols()

            assert len(duckdb_df) == 3
            assert len(localfile_df) == 3

            duckdb_df_sorted = duckdb_df.sort_values('symbol').reset_index(drop=True)
            localfile_df_sorted = localfile_df.sort_values('symbol').reset_index(drop=True)

            pd.testing.assert_frame_equal(
                duckdb_df_sorted[['symbol', 'exchange', 'market_type', 'active']],
                localfile_df_sorted[['symbol', 'exchange', 'market_type', 'active']]
            )

            await duckdb_wh.close()
            await localfile_wh.close()


class TestWarehouseOHLCVConsistency:
    """测试 OHLCV 数据在两种后端的一致性"""

    @pytest.fixture
    def sample_ohlcv_data(self):
        """生成测试用的 OHLCV 数据"""
        base_time = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        return pd.DataFrame([
            {
                'exchange': 'binance',
                'market_type': 'spot',
                'symbol': 'BTC/USDT',
                'timeframe': '1h',
                'ts': base_time,
                'open': 50000.0,
                'high': 50200.0,
                'low': 49800.0,
                'close': 50100.0,
                'volume': 100.5,
                'quote_volume': 5025000.0,
                'source': 'ccxt'
            },
            {
                'exchange': 'binance',
                'market_type': 'spot',
                'symbol': 'BTC/USDT',
                'timeframe': '1h',
                'ts': base_time.replace(hour=1),
                'open': 50100.0,
                'high': 50300.0,
                'low': 50000.0,
                'close': 50200.0,
                'volume': 150.2,
                'quote_volume': 7545000.0,
                'source': 'ccxt'
            }
        ])

    @pytest.mark.asyncio
    async def test_duckdb_ohlcv_insert_and_query(self, sample_ohlcv_data):
        """测试 DuckDB OHLCV 插入和查询"""
        from chronoforge.storage.duckdb_storage.warehouse import FinancialDataWarehouse

        with tempfile.TemporaryDirectory() as tmpdir:
            warehouse = FinancialDataWarehouse(db_path=f"{tmpdir}/test.duckdb")

            result = await warehouse.insert_ohlcv(sample_ohlcv_data)
            assert result is True

            df = await warehouse.get_ohlcv(symbol='BTC/USDT', timeframe='1h')
            assert df is not None
            assert len(df) == 2

            start_time = datetime(2024, 1, 1, 0, 30, 0, tzinfo=timezone.utc)
            end_time = datetime(2024, 1, 1, 1, 30, 0, tzinfo=timezone.utc)
            df_range = await warehouse.get_ohlcv(
                symbol='BTC/USDT', timeframe='1h',
                start_time=start_time, end_time=end_time
            )
            assert df_range is not None
            assert len(df_range) == 1

            await warehouse.close()

    @pytest.mark.asyncio
    async def test_localfile_ohlcv_insert_and_query(self, sample_ohlcv_data):
        """测试 LocalFile OHLCV 插入和查询"""
        from chronoforge.storage.localfile_storage.warehouse import LocalDataWarehouse

        with tempfile.TemporaryDirectory() as tmpdir:
            warehouse = LocalDataWarehouse(data_dir=f"{tmpdir}/local_data")

            result = await warehouse.insert_ohlcv(sample_ohlcv_data)
            assert result is True

            stats = await warehouse.get_stats()
            assert stats['ohlcv_files'] == 1

            df = await warehouse.get_ohlcv(symbol='BTC/USDT', timeframe='1h')
            assert df is not None
            assert len(df) == 2

            await warehouse.close()

    @pytest.mark.asyncio
    async def test_ohlcv_consistency_between_backends(self, sample_ohlcv_data):
        """测试两种后端 OHLCV 操作结果一致性"""
        from chronoforge.storage.duckdb_storage.warehouse import FinancialDataWarehouse
        from chronoforge.storage.localfile_storage.warehouse import LocalDataWarehouse

        with tempfile.TemporaryDirectory() as tmpdir:
            duckdb_path = f"{tmpdir}/duckdb"
            localfile_path = f"{tmpdir}/localfile"

            duckdb_wh = FinancialDataWarehouse(db_path=f"{duckdb_path}/test.duckdb")
            localfile_wh = LocalDataWarehouse(data_dir=localfile_path)

            duckdb_result = await duckdb_wh.insert_ohlcv(sample_ohlcv_data)
            localfile_result = await localfile_wh.insert_ohlcv(sample_ohlcv_data)

            assert duckdb_result is True
            assert localfile_result is True

            duckdb_df = await duckdb_wh.get_ohlcv(symbol='BTC/USDT', timeframe='1h')
            localfile_df = await localfile_wh.get_ohlcv(symbol='BTC/USDT', timeframe='1h')

            assert duckdb_df is not None
            assert localfile_df is not None
            assert len(duckdb_df) == 2
            assert len(localfile_df) == 2

            await duckdb_wh.close()
            await localfile_wh.close()


class TestWarehouseBTCFGIConsistency:
    """测试 BTC FGI 数据在两种后端的一致性"""

    @pytest.fixture
    def sample_btc_fgi_data(self):
        """生成测试用的 BTC FGI 数据"""
        return pd.DataFrame([
            {
                'ts': datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                'value': 45,
                'label': 'Fear'
            },
            {
                'ts': datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc),
                'value': 55,
                'label': 'Neutral'
            },
            {
                'ts': datetime(2024, 1, 3, 0, 0, 0, tzinfo=timezone.utc),
                'value': 70,
                'label': 'Greed'
            }
        ])

    @pytest.mark.asyncio
    async def test_duckdb_btc_fgi_insert(self, sample_btc_fgi_data):
        """测试 DuckDB BTC FGI 插入"""
        from chronoforge.storage.duckdb_storage.warehouse import FinancialDataWarehouse

        with tempfile.TemporaryDirectory() as tmpdir:
            warehouse = FinancialDataWarehouse(db_path=f"{tmpdir}/test.duckdb")

            result = await warehouse.insert_btc_fgi(sample_btc_fgi_data)
            assert result is True

            result2 = await warehouse.insert_btc_fgi(sample_btc_fgi_data)
            assert result2 is True

            stats = await warehouse.get_stats()
            assert stats['btc_fgi_count'] == 3

            await warehouse.close()

    @pytest.mark.asyncio
    async def test_localfile_btc_fgi_insert(self, sample_btc_fgi_data):
        """测试 LocalFile BTC FGI 插入"""
        from chronoforge.storage.localfile_storage.warehouse import LocalDataWarehouse

        with tempfile.TemporaryDirectory() as tmpdir:
            warehouse = LocalDataWarehouse(data_dir=f"{tmpdir}/local_data")

            result = await warehouse.insert_btc_fgi(sample_btc_fgi_data)
            assert result is True

            result2 = await warehouse.insert_btc_fgi(sample_btc_fgi_data)
            assert result2 is True

            stats = await warehouse.get_stats()
            assert stats['btc_fgi_files'] == 1

            await warehouse.close()

    @pytest.mark.asyncio
    async def test_btc_fgi_upsert_behavior(self, sample_btc_fgi_data):
        """测试两种后端的 UPSERT 行为一致性"""
        from chronoforge.storage.duckdb_storage.warehouse import FinancialDataWarehouse
        from chronoforge.storage.localfile_storage.warehouse import LocalDataWarehouse

        with tempfile.TemporaryDirectory() as tmpdir:
            duckdb_path = f"{tmpdir}/duckdb"
            localfile_path = f"{tmpdir}/localfile"

            duckdb_wh = FinancialDataWarehouse(db_path=f"{duckdb_path}/test.duckdb")
            localfile_wh = LocalDataWarehouse(data_dir=localfile_path)

            await duckdb_wh.insert_btc_fgi(sample_btc_fgi_data)
            await localfile_wh.insert_btc_fgi(sample_btc_fgi_data)

            stats1_duckdb = await duckdb_wh.get_stats()
            stats1_localfile = await localfile_wh.get_stats()

            assert stats1_duckdb['btc_fgi_count'] == 3
            assert stats1_localfile.get('btc_fgi_files') == 1

            new_data = pd.DataFrame([
                {
                    'ts': datetime(2024, 1, 4, 0, 0, 0, tzinfo=timezone.utc),
                    'value': 80,
                    'label': 'Extreme Greed'
                }
            ])
            await duckdb_wh.insert_btc_fgi(new_data)
            await localfile_wh.insert_btc_fgi(new_data)

            stats2_duckdb = await duckdb_wh.get_stats()
            stats2_localfile = await localfile_wh.get_stats()

            assert stats2_duckdb['btc_fgi_count'] == 4
            assert stats2_localfile.get('btc_fgi_files') == 1

            await duckdb_wh.close()
            await localfile_wh.close()


class TestWarehouseMacroFREDConsistency:
    """测试 Macro FRED 数据在两种后端的一致性"""

    @pytest.fixture
    def sample_macro_fred_data(self):
        """生成测试用的 Macro FRED 数据"""
        return pd.DataFrame([
            {
                'series_id': 'DGS10',
                'symbol': 'US10Y',
                'ts': datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc),
                'value': 4.25,
                'frequency': 'daily'
            },
            {
                'series_id': 'DGS10',
                'symbol': 'US10Y',
                'ts': datetime(2024, 1, 3, 0, 0, 0, tzinfo=timezone.utc),
                'value': 4.30,
                'frequency': 'daily'
            }
        ])

    @pytest.mark.asyncio
    async def test_macro_fred_consistency_between_backends(self, sample_macro_fred_data):
        """测试两种后端 Macro FRED 操作结果一致性"""
        from chronoforge.storage.duckdb_storage.warehouse import FinancialDataWarehouse
        from chronoforge.storage.localfile_storage.warehouse import LocalDataWarehouse

        with tempfile.TemporaryDirectory() as tmpdir:
            duckdb_path = f"{tmpdir}/duckdb"
            localfile_path = f"{tmpdir}/localfile"

            duckdb_wh = FinancialDataWarehouse(db_path=f"{duckdb_path}/test.duckdb")
            localfile_wh = LocalDataWarehouse(data_dir=localfile_path)

            duckdb_result = await duckdb_wh.insert_macro_fred(sample_macro_fred_data)
            localfile_result = await localfile_wh.insert_macro_fred(sample_macro_fred_data)

            assert duckdb_result is True
            assert localfile_result is True

            stats_duckdb = await duckdb_wh.get_stats()
            stats_localfile = await localfile_wh.get_stats()

            assert stats_duckdb['macro_fred_count'] >= 2
            assert stats_localfile.get('macro_fred_files') == 1

            await duckdb_wh.close()
            await localfile_wh.close()


class TestWarehouseFuturesMetricsConsistency:
    """测试 Futures Metrics 数据在两种后端的一致性"""

    @pytest.fixture
    def sample_futures_metrics_data(self):
        """生成测试用的 Futures Metrics 数据"""
        return pd.DataFrame([
            {
                'exchange': 'binance',
                'symbol': 'BTC-PERP',
                'ts': datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                'funding_rate': 0.0001,
                'open_interest': 150000000,
                'oi_value': 7500000000.0
            },
            {
                'exchange': 'binance',
                'symbol': 'BTC-PERP',
                'ts': datetime(2024, 1, 1, 8, 0, 0, tzinfo=timezone.utc),
                'funding_rate': 0.00015,
                'open_interest': 155000000,
                'oi_value': 7750000000.0
            }
        ])

    @pytest.mark.asyncio
    async def test_futures_metrics_consistency_between_backends(self, sample_futures_metrics_data):
        """测试两种后端 Futures Metrics 操作结果一致性"""
        from chronoforge.storage.duckdb_storage.warehouse import FinancialDataWarehouse
        from chronoforge.storage.localfile_storage.warehouse import LocalDataWarehouse

        with tempfile.TemporaryDirectory() as tmpdir:
            duckdb_path = f"{tmpdir}/duckdb"
            localfile_path = f"{tmpdir}/localfile"

            duckdb_wh = FinancialDataWarehouse(db_path=f"{duckdb_path}/test.duckdb")
            localfile_wh = LocalDataWarehouse(data_dir=localfile_path)

            duckdb_result = await duckdb_wh.insert_futures_metrics(
                sample_futures_metrics_data
            )
            localfile_result = await localfile_wh.insert_futures_metrics(
                sample_futures_metrics_data
            )

            assert duckdb_result is True
            assert localfile_result is True

            stats_duckdb = await duckdb_wh.get_stats()
            stats_localfile = await localfile_wh.get_stats()

            assert stats_duckdb['futures_metrics_count'] >= 2
            assert stats_localfile.get('futures_metrics_files') == 1

            await duckdb_wh.close()
            await localfile_wh.close()


class TestWarehouseCoinCategoriesConsistency:
    """测试 Coin Categories 数据在两种后端的一致性"""

    @pytest.fixture
    def sample_coin_categories_data(self):
        """生成测试用的 Coin Categories 数据"""
        return pd.DataFrame([
            {
                'symbol': 'BTC',
                'category': 'Layer1',
                'market_cap_rank': 1
            },
            {
                'symbol': 'ETH',
                'category': 'Layer1',
                'market_cap_rank': 2
            },
            {
                'symbol': 'SOL',
                'category': 'Layer1',
                'market_cap_rank': 5
            },
            {
                'symbol': 'BTC',
                'category': 'StoreOfValue',
                'market_cap_rank': 1
            }
        ])

    @pytest.mark.asyncio
    async def test_coin_categories_consistency_between_backends(self, sample_coin_categories_data):
        """测试两种后端 Coin Categories 操作结果一致性"""
        from chronoforge.storage.duckdb_storage.warehouse import FinancialDataWarehouse
        from chronoforge.storage.localfile_storage.warehouse import LocalDataWarehouse

        with tempfile.TemporaryDirectory() as tmpdir:
            duckdb_path = f"{tmpdir}/duckdb"
            localfile_path = f"{tmpdir}/localfile"

            duckdb_wh = FinancialDataWarehouse(db_path=f"{duckdb_path}/test.duckdb")
            localfile_wh = LocalDataWarehouse(data_dir=localfile_path)

            duckdb_result = await duckdb_wh.insert_coin_categories(sample_coin_categories_data)
            localfile_result = await localfile_wh.insert_coin_categories(
                sample_coin_categories_data
            )

            assert duckdb_result is True
            assert localfile_result is True

            stats_duckdb = await duckdb_wh.get_stats()
            stats_localfile = await localfile_wh.get_stats()

            assert stats_duckdb['coin_categories_count'] >= 3
            assert stats_localfile['coin_categories_files'] == 1

            await duckdb_wh.close()
            await localfile_wh.close()


class TestWarehouseTickersConsistency:
    """测试 Tickers 数据在两种后端的一致性"""

    @pytest.fixture
    def sample_tickers_data(self):
        """生成测试用的 Tickers 数据"""
        return pd.DataFrame([
            {
                'exchange': 'binance',
                'market_type': 'spot',
                'symbol': 'BTC/USDT',
                'ts': datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                'last': 50000.0,
                'bid': 49999.5,
                'ask': 50000.5,
                'base_volume': 1000.0,
                'quote_volume': 50000000.0,
                'open_interest': None,
                'funding_rate': None
            },
            {
                'exchange': 'binance',
                'market_type': 'spot',
                'symbol': 'BTC/USDT',
                'ts': datetime(2024, 1, 1, 0, 5, 0, tzinfo=timezone.utc),
                'last': 50100.0,
                'bid': 50099.0,
                'ask': 50101.0,
                'base_volume': 1200.0,
                'quote_volume': 60120000.0,
                'open_interest': None,
                'funding_rate': None
            }
        ])

    @pytest.mark.asyncio
    async def test_tickers_consistency_between_backends(self, sample_tickers_data):
        """测试两种后端 Tickers 操作结果一致性"""
        from chronoforge.storage.duckdb_storage.warehouse import FinancialDataWarehouse
        from chronoforge.storage.localfile_storage.warehouse import LocalDataWarehouse

        with tempfile.TemporaryDirectory() as tmpdir:
            duckdb_path = f"{tmpdir}/duckdb"
            localfile_path = f"{tmpdir}/localfile"

            duckdb_wh = FinancialDataWarehouse(db_path=f"{duckdb_path}/test.duckdb")
            localfile_wh = LocalDataWarehouse(data_dir=localfile_path)

            duckdb_result = await duckdb_wh.insert_tickers(sample_tickers_data)
            localfile_result = await localfile_wh.insert_tickers(sample_tickers_data)

            assert duckdb_result is True
            assert localfile_result is True

            stats_duckdb = await duckdb_wh.get_stats()
            stats_localfile = await localfile_wh.get_stats()

            assert stats_duckdb['tickers_count'] >= 2
            assert stats_localfile.get('tickers_files', 0) >= 1

            await duckdb_wh.close()
            await localfile_wh.close()


class TestDataIntegrityConsistency:
    """测试数据完整性检查的一致性"""

    @pytest.mark.asyncio
    async def test_data_integrity_duckdb(self):
        """测试 DuckDB 数据完整性检查"""
        from chronoforge.storage.duckdb_storage.warehouse import FinancialDataWarehouse

        with tempfile.TemporaryDirectory() as tmpdir:
            warehouse = FinancialDataWarehouse(db_path=f"{tmpdir}/test.duckdb")

            sample_ohlcv = pd.DataFrame([
                {
                    'exchange': 'binance',
                    'market_type': 'spot',
                    'symbol': 'BTC/USDT',
                    'timeframe': '1h',
                    'ts': datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                    'open': 50000.0,
                    'high': 50200.0,
                    'low': 49800.0,
                    'close': 50100.0,
                    'volume': 100.5
                }
            ])
            await warehouse.insert_ohlcv(sample_ohlcv)

            result = await warehouse.check_data_integrity()

            assert result is not None
            assert 'orphan_ohlcv_symbols' in result
            assert 'duplicate_ohlcv_records' in result

            await warehouse.close()

    @pytest.mark.asyncio
    async def test_data_integrity_localfile(self):
        """测试 LocalFile 数据完整性检查"""
        from chronoforge.storage.localfile_storage.warehouse import LocalDataWarehouse

        with tempfile.TemporaryDirectory() as tmpdir:
            warehouse = LocalDataWarehouse(data_dir=f"{tmpdir}/local_data")

            sample_ohlcv = pd.DataFrame([
                {
                    'exchange': 'binance',
                    'market_type': 'spot',
                    'symbol': 'BTC/USDT',
                    'timeframe': '1h',
                    'ts': datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                    'open': 50000.0,
                    'high': 50200.0,
                    'low': 49800.0,
                    'close': 50100.0,
                    'volume': 100.5
                }
            ])
            await warehouse.insert_ohlcv(sample_ohlcv)

            result = await warehouse.check_data_integrity()

            assert result is not None
            assert 'status' in result

            await warehouse.close()


class TestEmptyDataConsistency:
    """测试空数据处理的一致性"""

    @pytest.mark.asyncio
    async def test_empty_insert_both_backends(self):
        """测试空数据插入"""
        from chronoforge.storage.duckdb_storage.warehouse import FinancialDataWarehouse
        from chronoforge.storage.localfile_storage.warehouse import LocalDataWarehouse

        with tempfile.TemporaryDirectory() as tmpdir:
            duckdb_path = f"{tmpdir}/duckdb"
            localfile_path = f"{tmpdir}/localfile"

            duckdb_wh = FinancialDataWarehouse(db_path=f"{duckdb_path}/test.duckdb")
            localfile_wh = LocalDataWarehouse(data_dir=localfile_path)

            result_duckdb = await duckdb_wh.insert_symbols(pd.DataFrame())
            result_localfile = await localfile_wh.insert_symbols(pd.DataFrame())

            assert result_duckdb is True
            assert result_localfile is True

            df_duckdb = await duckdb_wh.get_symbols()
            df_localfile = await localfile_wh.get_symbols()

            assert df_duckdb is None
            assert df_localfile is None

            await duckdb_wh.close()
            await localfile_wh.close()

    @pytest.mark.asyncio
    async def test_nonexistent_query_both_backends(self):
        """测试不存在数据的查询"""
        from chronoforge.storage.duckdb_storage.warehouse import FinancialDataWarehouse
        from chronoforge.storage.localfile_storage.warehouse import LocalDataWarehouse

        with tempfile.TemporaryDirectory() as tmpdir:
            duckdb_path = f"{tmpdir}/duckdb"
            localfile_path = f"{tmpdir}/localfile"

            duckdb_wh = FinancialDataWarehouse(db_path=f"{duckdb_path}/test.duckdb")
            localfile_wh = LocalDataWarehouse(data_dir=localfile_path)

            df_duckdb = await duckdb_wh.get_ohlcv(
                symbol='NONEXISTENT/USDT',
                timeframe='1h'
            )
            df_localfile = await localfile_wh.get_ohlcv(
                symbol='NONEXISTENT/USDT',
                timeframe='1h'
            )

            assert df_duckdb is None
            assert df_localfile is None

            await duckdb_wh.close()
            await localfile_wh.close()


class TestContextManagerConsistency:
    """测试异步上下文管理器的一致性"""

    @pytest.mark.asyncio
    async def test_duckdb_context_manager(self):
        """测试 DuckDB 上下文管理器"""
        from chronoforge.storage.duckdb_storage.warehouse import FinancialDataWarehouse

        with tempfile.TemporaryDirectory() as tmpdir:
            async with FinancialDataWarehouse(db_path=f"{tmpdir}/test.duckdb") as warehouse:
                result = await warehouse.insert_symbols(pd.DataFrame([
                    {'symbol': 'TEST/USDT', 'exchange': 'test', 'market_type': 'spot',
                     'base_asset': 'TEST', 'quote_asset': 'USDT', 'active': True}
                ]))
                assert result is True

    @pytest.mark.asyncio
    async def test_localfile_context_manager(self):
        """测试 LocalFile 上下文管理器"""
        from chronoforge.storage.localfile_storage.warehouse import LocalDataWarehouse

        with tempfile.TemporaryDirectory() as tmpdir:
            async with LocalDataWarehouse(data_dir=f"{tmpdir}/local_data") as warehouse:
                result = await warehouse.insert_symbols(pd.DataFrame([
                    {'symbol': 'TEST/USDT', 'exchange': 'test', 'market_type': 'spot',
                     'base_asset': 'TEST', 'quote_asset': 'USDT', 'active': True}
                ]))
                assert result is True


class TestStatsConsistency:
    """测试统计信息的一致性"""

    @pytest.mark.asyncio
    async def test_stats_format_consistency(self):
        """测试统计信息格式一致性"""
        from chronoforge.storage.duckdb_storage.warehouse import FinancialDataWarehouse
        from chronoforge.storage.localfile_storage.warehouse import LocalDataWarehouse

        with tempfile.TemporaryDirectory() as tmpdir:
            duckdb_path = f"{tmpdir}/duckdb"
            localfile_path = f"{tmpdir}/localfile"

            duckdb_wh = FinancialDataWarehouse(db_path=f"{duckdb_path}/test.duckdb")
            localfile_wh = LocalDataWarehouse(data_dir=localfile_path)

            await duckdb_wh.insert_symbols(pd.DataFrame([
                {'symbol': 'TEST/USDT', 'exchange': 'test', 'market_type': 'spot',
                 'base_asset': 'TEST', 'quote_asset': 'USDT', 'active': True}
            ]))
            await localfile_wh.insert_symbols(pd.DataFrame([
                {'symbol': 'TEST/USDT', 'exchange': 'test', 'market_type': 'spot',
                 'base_asset': 'TEST', 'quote_asset': 'USDT', 'active': True}
            ]))

            duckdb_stats = await duckdb_wh.get_stats()
            localfile_stats = await localfile_wh.get_stats()

            assert 'symbols_count' in duckdb_stats
            assert 'symbols_files' in localfile_stats

            await duckdb_wh.close()
            await localfile_wh.close()
