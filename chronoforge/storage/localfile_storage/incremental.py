"""
本地文件存储增量更新管理器

提供智能的增量更新功能，包括：
1. 自动检测新数据
2. 智能去重
3. 时间连续性检查
4. 批量优化插入
"""

import logging
import pandas as pd
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta, timezone

from .quality import LocalDataValidator, LocalIncrementalUpdater
from .warehouse import LocalDataWarehouse

logger = logging.getLogger(__name__)


class LocalIncrementalManager:
    """本地文件存储增量更新管理器"""

    def __init__(self, warehouse: 'LocalDataWarehouse'):
        """
        初始化增量更新管理器

        Args:
            warehouse: 本地数据仓库实例
        """
        self.warehouse = warehouse
        self.validator = LocalDataValidator()
        self.updater = LocalIncrementalUpdater()

        self._cache: Dict[str, Any] = {}
        self._cache_ttl = 300
        self._last_cache_update: Dict[str, datetime] = {}

    async def smart_insert_ohlcv(self, ohlcv_data: pd.DataFrame,
                                 validation: bool = True,
                                 incremental: bool = True,
                                 batch_size: int = 10000) -> Dict[str, Any]:
        """智能插入OHLCV数据"""
        if ohlcv_data is None or ohlcv_data.empty:
            return {'status': 'success', 'inserted': 0, 'skipped': 0, 'errors': []}

        try:
            df = ohlcv_data

            if validation:
                validation_result = self.validator.validate_data(df, 'ohlcv')
                if not validation_result['valid']:
                    return {
                        'status': 'validation_failed',
                        'inserted': 0,
                        'skipped': len(ohlcv_data),
                        'errors': validation_result['errors'],
                        'warnings': validation_result['warnings']
                    }

            if incremental:
                grouped = df.groupby(['symbol', 'timeframe'])

                total_inserted = 0
                total_skipped = 0
                all_errors = []

                for (symbol, timeframe), group_df in grouped:
                    try:
                        existing_data = await self._get_existing_ohlcv(symbol, timeframe)
                        new_records = self.updater.detect_new_records(group_df, existing_data,
                                                                      'ohlcv')

                        if len(new_records) > 0:
                            deduped_records, duplicates = self.updater.detect_duplicate_records(
                                new_records, 'ohlcv')

                            if len(deduped_records) > 0:
                                for i in range(0, len(deduped_records), batch_size):
                                    batch = deduped_records.iloc[i:i + batch_size]
                                    success = await self.warehouse.insert_ohlcv(batch)
                                    if success:
                                        total_inserted += len(batch)
                                    else:
                                        all_errors.append(f"批量插入失败: {symbol}_{timeframe}")

                            total_skipped += len(duplicates)
                        else:
                            total_skipped += len(group_df)

                    except Exception as e:
                        error_msg = f"处理 {symbol}_{timeframe} 时出错: {str(e)}"
                        logger.error(error_msg)
                        all_errors.append(error_msg)

                result = {
                    'status': 'success' if len(all_errors) == 0 else 'partial_success',
                    'inserted': total_inserted,
                    'skipped': total_skipped,
                    'errors': all_errors
                }

            else:
                deduped_df, duplicates = self.updater.detect_duplicate_records(df, 'ohlcv')

                if len(deduped_df) > 0:
                    total_inserted = 0

                    for i in range(0, len(deduped_df), batch_size):
                        batch = deduped_df.iloc[i:i + batch_size]
                        success = await self.warehouse.insert_ohlcv(batch)
                        if success:
                            total_inserted += len(batch)

                    result = {
                        'status': 'success',
                        'inserted': total_inserted,
                        'skipped': len(duplicates),
                        'errors': []
                    }
                else:
                    result = {
                        'status': 'success',
                        'inserted': 0,
                        'skipped': len(ohlcv_data),
                        'errors': [],
                        'warnings': ['所有记录都是重复的']
                    }

            return result

        except Exception as e:
            logger.error(f"智能插入OHLCV数据失败: {str(e)}")
            return {
                'status': 'error',
                'inserted': 0,
                'skipped': len(ohlcv_data),
                'errors': [str(e)]
            }

    async def smart_insert_tickers(self, tickers_data: pd.DataFrame,
                                   validation: bool = True,
                                   incremental: bool = True,
                                   deduplicate: bool = True) -> Dict[str, Any]:
        """智能插入Tickers数据"""
        if tickers_data is None or tickers_data.empty:
            return {'status': 'success', 'inserted': 0, 'skipped': 0, 'errors': []}

        try:
            df = tickers_data

            if validation:
                validation_result = self.validator.validate_data(df, 'tickers')
                if not validation_result['valid']:
                    return {
                        'status': 'validation_failed',
                        'inserted': 0,
                        'skipped': len(tickers_data),
                        'errors': validation_result['errors']
                    }

            if deduplicate:
                deduped_df, duplicates = self.updater.detect_duplicate_records(df, 'tickers')
            else:
                deduped_df = df
                duplicates = pd.DataFrame()

            if incremental:
                latest_timestamp = await self._get_latest_ticker_timestamp()

                if latest_timestamp:
                    new_records = deduped_df[deduped_df['ts'] > latest_timestamp]
                else:
                    new_records = deduped_df
            else:
                new_records = deduped_df

            if len(new_records) > 0:
                success = await self.warehouse.insert_tickers(new_records)

                if success:
                    inserted = len(new_records)
                    skipped = len(duplicates) + (len(deduped_df) - len(new_records))

                    return {
                        'status': 'success',
                        'inserted': inserted,
                        'skipped': skipped,
                        'errors': []
                    }
                else:
                    return {
                        'status': 'insert_failed',
                        'inserted': 0,
                        'skipped': len(tickers_data),
                        'errors': ['插入失败']
                    }
            else:
                return {
                    'status': 'success',
                    'inserted': 0,
                    'skipped': len(tickers_data),
                    'errors': []
                }

        except Exception as e:
            logger.error(f"智能插入Tickers数据失败: {str(e)}")
            return {
                'status': 'error',
                'inserted': 0,
                'skipped': len(tickers_data),
                'errors': [str(e)]
            }

    async def smart_insert_futures_metrics(self, metrics_data: pd.DataFrame,
                                           validation: bool = True,
                                           incremental: bool = True) -> Dict[str, Any]:
        """智能插入期货指标数据"""
        if metrics_data is None or metrics_data.empty:
            return {'status': 'success', 'inserted': 0, 'skipped': 0, 'errors': []}

        try:
            df = metrics_data

            if validation:
                validation_result = self.validator.validate_data(df, 'futures_metrics')
                if not validation_result['valid']:
                    return {
                        'status': 'validation_failed',
                        'inserted': 0,
                        'skipped': len(metrics_data),
                        'errors': validation_result['errors']
                    }

            grouped = df.groupby('symbol')

            total_inserted = 0
            total_skipped = 0
            all_errors = []

            for symbol, group_df in grouped:
                try:
                    if incremental:
                        existing_data = await self._get_existing_futures_metrics(symbol)
                        new_records = self.updater.detect_new_records(group_df, existing_data,
                                                                      'futures_metrics')

                        if len(new_records) > 0:
                            success = await self.warehouse.insert_futures_metrics(new_records)
                            if success:
                                total_inserted += len(new_records)
                        else:
                            total_skipped += len(group_df)
                    else:
                        success = await self.warehouse.insert_futures_metrics(group_df)
                        if success:
                            total_inserted += len(group_df)

                except Exception as e:
                    error_msg = f"处理 {symbol} 时出错: {str(e)}"
                    logger.error(error_msg)
                    all_errors.append(error_msg)

            return {
                'status': 'success' if len(all_errors) == 0 else 'partial_success',
                'inserted': total_inserted,
                'skipped': total_skipped,
                'errors': all_errors
            }

        except Exception as e:
            logger.error(f"智能插入期货指标数据失败: {str(e)}")
            return {
                'status': 'error',
                'inserted': 0,
                'skipped': len(metrics_data),
                'errors': [str(e)]
            }

    async def _get_existing_ohlcv(self, symbol: str, timeframe: str) -> pd.DataFrame:
        """获取现有的OHLCV数据"""
        cache_key = f"ohlcv_{symbol}_{timeframe}"

        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]

        try:
            start_time = datetime.now() - timedelta(days=30)
            existing_data = await self.warehouse.get_ohlcv(
                symbol=symbol,
                timeframe=timeframe,
                start_time=start_time,
                limit=1000
            )

            if existing_data is not None:
                self._cache[cache_key] = existing_data
                self._last_cache_update[cache_key] = datetime.now()
            else:
                existing_data = pd.DataFrame()
                self._cache[cache_key] = existing_data
                self._last_cache_update[cache_key] = datetime.now()

            return existing_data

        except Exception as e:
            logger.warning(f"获取现有OHLCV数据失败 {symbol}_{timeframe}: {str(e)}")
            return pd.DataFrame()

    async def _get_latest_ticker_timestamp(self) -> Optional[datetime]:
        """获取最新的Ticker时间戳"""
        try:
            stats = await self.warehouse.get_stats()
            if stats.get('tickers_files', 0) > 0:
                tickers_dir = self.warehouse._get_data_subdir('tickers')
                for file_path in tickers_dir.glob(f"*.{self.warehouse.file_format}"):
                    df = self.warehouse._load_dataframe(file_path)
                    if df is not None and not df.empty and 'ts' in df.columns:
                        latest_ts = df['ts'].max()
                        if latest_ts is not None:
                            return pd.Timestamp(latest_ts).to_pydatetime()
            return None
        except Exception as e:
            logger.warning(f"获取最新Ticker时间戳失败: {str(e)}")
            return None

    async def _get_existing_futures_metrics(self, symbol: str) -> pd.DataFrame:
        """获取现有的期货指标数据"""
        cache_key = f"futures_metrics_{symbol}"

        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]

        try:
            stats = await self.warehouse.get_stats()
            if stats.get('futures_metrics_files', 0) > 0:
                futures_dir = self.warehouse._get_data_subdir('futures_metrics')
                for file_path in futures_dir.glob(f"*.{self.warehouse.file_format}"):
                    df = self.warehouse._load_dataframe(file_path)
                    if df is not None and not df.empty:
                        symbol_df = df[df['symbol'] == symbol] if 'symbol' in df.columns else df
                        if not symbol_df.empty:
                            self._cache[cache_key] = symbol_df
                            self._last_cache_update[cache_key] = datetime.now()
                            return symbol_df

            return pd.DataFrame()
        except Exception as e:
            logger.warning(f"获取现有期货指标数据失败 {symbol}: {str(e)}")
            return pd.DataFrame()

    def _is_cache_valid(self, cache_key: str) -> bool:
        """检查缓存是否有效"""
        if cache_key not in self._cache:
            return False

        if cache_key not in self._last_cache_update:
            return False

        cache_age = (datetime.now() - self._last_cache_update[cache_key]).total_seconds()
        return cache_age < self._cache_ttl

    def clear_cache(self):
        """清除缓存"""
        self._cache.clear()
        self._last_cache_update.clear()
        logger.debug("增量更新缓存已清除")

    async def get_incremental_stats(self) -> Dict[str, Any]:
        """获取增量更新统计信息"""
        return {
            'cache_size': len(self._cache),
            'cache_entries': list(self._cache.keys()),
            'cache_ttl': self._cache_ttl
        }

    async def get_latest_timestamp(self,
                                   symbol: Optional[str] = None,
                                   timeframe: Optional[str] = None) -> Optional[datetime]:
        """获取最新时间戳"""
        try:
            if symbol and timeframe:
                ohlcv_data = await self.warehouse.get_ohlcv(
                    symbol=symbol,
                    timeframe=timeframe,
                    limit=1
                )

                if ohlcv_data is not None and not ohlcv_data.empty:
                    return ohlcv_data['ts'].max()

            return None

        except Exception as e:
            logger.error(f"获取最新时间戳失败: {str(e)}")
            return None

    async def suggest_data_fetch_range(self,
                                       symbol: str,
                                       timeframe: str,
                                       lookback_days: int = 30) -> Dict[str, Any]:
        """建议数据获取范围"""
        now = datetime.now(timezone.utc)
        lookback_start = now - timedelta(days=lookback_days)

        latest_ts = await self.get_latest_timestamp(symbol, timeframe)

        if not latest_ts:
            return {
                'status': 'no_data',
                'suggested_start': lookback_start,
                'suggested_end': now,
                'reason': '没有该交易对的数据',
                'fetch_days': lookback_days
            }

        latest_ts = pd.Timestamp(latest_ts).to_pydatetime()
        if latest_ts.tzinfo is None:
            latest_ts = latest_ts.replace(tzinfo=timezone.utc)

        time_since_latest = (now - latest_ts).total_seconds()

        timeframe_thresholds = {
            '1m': 300, '5m': 900, '15m': 1800, '30m': 3600,
            '1h': 7200, '4h': 14400, '1d': 86400, '1w': 604800, '1M': 2592000
        }

        threshold = timeframe_thresholds.get(timeframe, 86400)

        if time_since_latest > threshold:
            timeframe_deltas = {
                '1m': timedelta(minutes=1), '5m': timedelta(minutes=5),
                '15m': timedelta(minutes=15), '30m': timedelta(minutes=30),
                '1h': timedelta(hours=1), '4h': timedelta(hours=4),
                '1d': timedelta(days=1), '1w': timedelta(weeks=1),
                '1M': timedelta(days=30)
            }
            timeframe_delta = timeframe_deltas.get(timeframe, timedelta(days=1))

            return {
                'status': 'needs_update',
                'suggested_start': latest_ts + timeframe_delta,
                'suggested_end': now,
                'current_latest': latest_ts,
                'time_since_latest_hours': time_since_latest / 3600,
                'reason': f'最新数据已过时 {time_since_latest/3600:.1f} 小时',
                'fetch_days': (now - latest_ts).days + 1
            }

        return {
            'status': 'up_to_date',
            'current_latest': latest_ts,
            'reason': '数据已是最新',
            'fetch_days': 0
        }
