"""
增量更新管理器 - 集成到DuckDB存储中

提供智能的增量更新功能，包括：
1. 自动检测新数据
2. 智能去重
3. 时间连续性检查
4. 批量优化插入
"""

import logging
import pandas as pd
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta

from .quality import DataValidator, IncrementalUpdater, DataQualityMonitor
from .warehouse import FinancialDataWarehouse

logger = logging.getLogger(__name__)


class SmartIncrementalManager:
    """智能增量更新管理器 - 集成到FinancialDataWarehouse"""

    def __init__(self, warehouse: 'FinancialDataWarehouse'):
        """
        初始化增量更新管理器

        Args:
            warehouse: 金融数据仓库实例
        """
        self.warehouse = warehouse
        self.validator = DataValidator()
        self.updater = IncrementalUpdater()
        self.monitor = DataQualityMonitor()

        # 缓存配置
        self._cache: Dict[str, Any] = {}
        self._cache_ttl = 300  # 5分钟缓存
        self._last_cache_update: Dict[str, datetime] = {}

    async def smart_insert_ohlcv(self, ohlcv_data: pd.DataFrame,
                                 validation: bool = True,
                                 incremental: bool = True,
                                 batch_size: int = 10000) -> Dict[str, Any]:
        """
        智能插入OHLCV数据

        Args:
            ohlcv_data: OHLCV数据DataFrame
            validation: 是否进行数据验证
            incremental: 是否进行增量检测
            batch_size: 批处理大小

        Returns:
            插入结果统计
        """
        if ohlcv_data is None or ohlcv_data.empty:
            return {'status': 'success', 'inserted': 0, 'skipped': 0, 'errors': []}

        try:
            # 使用传入的DataFrame
            df = ohlcv_data

            # 数据验证
            if validation:
                validation_result = self.validator.validate_data(df, 'ohlcv')
                if not validation_result['valid']:
                    logger.error(f"OHLCV数据验证失败: {validation_result['errors']}")
                    return {
                        'status': 'validation_failed',
                        'inserted': 0,
                        'skipped': len(ohlcv_data),
                        'errors': validation_result['errors'],
                        'warnings': validation_result['warnings']
                    }

                if validation_result['warnings']:
                    logger.warning(f"OHLCV数据验证警告: {str(validation_result['warnings'])}")

            # 增量检测
            if incremental:
                # 按symbol和timeframe分组处理
                grouped = df.groupby(['symbol', 'timeframe'])

                total_inserted = 0
                total_skipped = 0
                all_errors = []
                all_warnings = []

                for (symbol, timeframe), group_df in grouped:
                    try:
                        # 获取现有数据的时间范围
                        existing_data = await self._get_existing_ohlcv(symbol, timeframe)

                        # 检测新记录
                        new_records = self.updater.detect_new_records(group_df, existing_data,
                                                                      'ohlcv')

                        if len(new_records) > 0:
                            # 检测重复记录
                            deduped_records, duplicates = self.updater.detect_duplicate_records(
                                new_records, 'ohlcv')

                            if len(deduped_records) > 0:
                                # 分批处理
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
                    'errors': all_errors,
                    'warnings': all_warnings
                }

            else:
                # 全量插入（不进行增量检测）
                # 检测重复记录
                deduped_df, duplicates = self.updater.detect_duplicate_records(df, 'ohlcv')

                if len(deduped_df) > 0:
                    # 分批处理
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
                        'errors': [],
                        'warnings': []
                    }
                else:
                    result = {
                        'status': 'success',
                        'inserted': 0,
                        'skipped': len(ohlcv_data),
                        'errors': [],
                        'warnings': ['所有记录都是重复的']
                    }

            # 质量监控
            if result['inserted'] > 0:
                self.monitor.monitor_data_quality(
                    df, 'ohlcv', 'smart_incremental',
                    {'batch_size': batch_size, 'incremental': incremental}
                )

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
        """
        智能插入Tickers数据

        Args:
            tickers_data: Tickers数据DataFrame
            validation: 是否进行数据验证
            incremental: 是否进行增量检测
            deduplicate: 是否进行去重

        Returns:
            插入结果统计
        """
        if tickers_data is None or tickers_data.empty:
            return {'status': 'success', 'inserted': 0, 'skipped': 0, 'errors': []}

        try:
            df = tickers_data

            # 数据验证
            if validation:
                validation_result = self.validator.validate_data(df, 'tickers')
                if not validation_result['valid']:
                    return {
                        'status': 'validation_failed',
                        'inserted': 0,
                        'skipped': len(tickers_data),
                        'errors': validation_result['errors']
                    }

            # 去重处理
            if deduplicate:
                deduped_df, duplicates = self.updater.detect_duplicate_records(df, 'tickers')
            else:
                deduped_df = df
                duplicates = pd.DataFrame()

            # 增量检测
            if incremental:
                # 获取最新时间戳
                latest_timestamp = await self._get_latest_ticker_timestamp()

                if latest_timestamp:
                    # 只保留新于最新时间戳的数据
                    new_records = deduped_df[deduped_df['ts'] > latest_timestamp]
                else:
                    new_records = deduped_df
            else:
                new_records = deduped_df

            # 插入数据
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
        """
        智能插入期货指标数据

        Args:
            metrics_data: 期货指标数据DataFrame
            validation: 是否进行数据验证
            incremental: 是否进行增量检测

        Returns:
            插入结果统计
        """
        if metrics_data is None or metrics_data.empty:
            return {'status': 'success', 'inserted': 0, 'skipped': 0, 'errors': []}

        try:
            df = metrics_data

            # 数据验证
            if validation:
                validation_result = self.validator.validate_data(df, 'futures_metrics')
                if not validation_result['valid']:
                    return {
                        'status': 'validation_failed',
                        'inserted': 0,
                        'skipped': len(metrics_data),
                        'errors': validation_result['errors']
                    }

            # 按symbol分组处理
            grouped = df.groupby('symbol')

            total_inserted = 0
            total_skipped = 0
            all_errors = []

            for symbol, group_df in grouped:
                try:
                    if incremental:
                        # 获取现有数据
                        existing_data = await self._get_existing_futures_metrics(symbol)

                        # 检测新记录
                        new_records = self.updater.detect_new_records(group_df, existing_data,
                                                                      'futures_metrics')

                        if len(new_records) > 0:
                            success = await self.warehouse.insert_futures_metrics(new_records)
                            if success:
                                total_inserted += len(new_records)
                        else:
                            total_skipped += len(group_df)
                    else:
                        # 全量插入
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

    # ===== 私有辅助方法 =====

    async def _get_existing_ohlcv(self, symbol: str, timeframe: str) -> pd.DataFrame:
        """获取现有的OHLCV数据（用于增量检测）"""
        cache_key = f"ohlcv_{symbol}_{timeframe}"

        # 检查缓存
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]

        try:
            # 只获取最近30天的数据进行比较（优化性能）
            start_time = datetime.now() - timedelta(days=30)
            existing_data = await self.warehouse.get_ohlcv(
                symbol=symbol,
                timeframe=timeframe,
                start_time=start_time,
                limit=1000  # 限制返回数量
            )

            if existing_data is not None:
                # 缓存结果
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
        cache_key = "latest_ticker_ts"

        # 检查缓存
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]

        try:
            conn = self.warehouse._get_connection()
            result = conn.execute("""
                SELECT MAX(ts) as latest_ts
                FROM tickers
            """).fetchone()

            if result and result[0]:
                latest_ts = result[0]
                self._cache[cache_key] = latest_ts
                self._last_cache_update[cache_key] = datetime.now()
                return latest_ts

            return None

        except Exception as e:
            logger.warning(f"获取最新Ticker时间戳失败: {str(e)}")
            return None

    async def _get_existing_futures_metrics(self, symbol: str) -> pd.DataFrame:
        """获取现有的期货指标数据"""
        cache_key = f"futures_{symbol}"

        # 检查缓存
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]

        try:
            # 只获取最近7天的数据
            start_time = datetime.now() - timedelta(days=7)

            conn = self.warehouse._get_connection()
            result = conn.execute("""
                SELECT * FROM futures_metrics
                WHERE symbol = ? AND ts >= ?
                ORDER BY ts DESC
                LIMIT 1000
            """, [symbol, start_time]).fetchdf()

            if not result.empty:
                self._cache[cache_key] = result
                self._last_cache_update[cache_key] = datetime.now()
            else:
                result = pd.DataFrame()
                self._cache[cache_key] = result
                self._last_cache_update[cache_key] = datetime.now()

            return result

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
        try:
            # 获取质量监控统计
            recent_quality_checks = self.monitor.quality_history[-100:]  # 最近100次

            if recent_quality_checks:
                avg_quality_score = (sum(check['overall_quality_score']
                                         for check in recent_quality_checks) /
                                     len(recent_quality_checks))
                quality_trends = self.monitor.get_quality_trends()
            else:
                avg_quality_score = 0.0
                quality_trends = {}

            return {
                'cache_size': len(self._cache),
                'avg_quality_score': avg_quality_score,
                'quality_trends': quality_trends,
                'cache_entries': list(self._cache.keys()),
                'cache_ttl': self._cache_ttl
            }

        except Exception as e:
            logger.error(f"获取增量更新统计失败: {str(e)}")
            return {'error': str(e)}

    async def get_latest_timestamp(self,
                                   table_name: str = 'ohlcv',
                                   symbol: Optional[str] = None,
                                   timeframe: Optional[str] = None,
                                   exchange: Optional[str] = None) -> Optional[datetime]:
        """
        获取最新时间戳 - 核心功能

        先读取数据库中存储条目的最后一个时间戳，根据timeframe和当前时间分析如何获取新数据

        Args:
            table_name: 表名 (ohlcv, tickers, futures_metrics, macro_fred, btc_fgi, coin_categories)
            symbol: 交易对符号（可选）
            timeframe: 时间框架（可选，主要用于OHLCV）
            exchange: 交易所（可选）

        Returns:
            最新时间戳，如果没有数据则返回None
        """
        cache_key = f"latest_ts_{table_name}_{symbol}_{timeframe}_{exchange}"

        # 检查缓存
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]

        try:
            conn = self.warehouse._get_connection()

            # 构建查询条件
            conditions = []
            params = []

            if symbol and table_name in ['ohlcv', 'tickers', 'futures_metrics']:
                conditions.append("symbol = ?")
                params.append(symbol)

            if timeframe and table_name == 'ohlcv':
                conditions.append("timeframe = ?")
                params.append(timeframe)

            if exchange and table_name in ['ohlcv', 'tickers', 'futures_metrics']:
                conditions.append("exchange = ?")
                params.append(exchange)

            # 构建WHERE子句
            where_clause = ""
            if conditions:
                where_clause = "WHERE " + " AND ".join(conditions)

            # 执行查询
            query = f"SELECT MAX(ts) as latest_ts FROM {table_name} {where_clause}"
            result = conn.execute(query, params).fetchone()

            if result and result[0]:
                latest_ts = result[0]
                self._cache[cache_key] = latest_ts
                self._last_cache_update[cache_key] = datetime.now()
                return latest_ts

            return None

        except Exception as e:
            logger.error(f"获取最新时间戳失败 [{table_name}]: {str(e)}")
            return None

    async def suggest_data_fetch_range(self,
                                       symbol: str,
                                       timeframe: str,
                                       exchange: Optional[str] = None,
                                       lookback_days: int = 30) -> Dict[str, Any]:
        """
        建议数据获取范围 - 核心功能

        分析现有数据，建议需要获取的新数据范围

        功能流程：
        1. 读取数据库中存储条目的最后一个时间戳
        2. 根据timeframe和当前时间分析如何获取新数据
        3. 返回建议的数据获取范围

        Args:
            symbol: 交易对符号
            timeframe: 时间框架（如'1d', '1h'等）
            exchange: 交易所（可选）
            lookback_days: 回顾天数

        Returns:
            包含建议获取范围的信息字典
        """
        now = datetime.now()
        lookback_start = now - timedelta(days=lookback_days)

        # 获取当前最新时间戳
        latest_ts = await self.get_latest_timestamp('ohlcv', symbol, timeframe, exchange)

        if not latest_ts:
            # 没有数据，建议获取完整范围
            return {
                'status': 'no_data',
                'suggested_start': lookback_start,
                'suggested_end': now,
                'reason': '数据库中没有该交易对的数据',
                'fetch_days': lookback_days
            }

        # 检查是否需要更新最新数据
        time_since_latest = (now - latest_ts).total_seconds()

        # 根据时间框架确定更新阈值
        timeframe_thresholds = {
            '1m': 300,    # 5分钟
            '5m': 900,    # 15分钟
            '15m': 1800,  # 30分钟
            '30m': 3600,  # 1小时
            '1h': 7200,   # 2小时
            '4h': 14400,  # 4小时
            '1d': 86400,  # 1天
            '1w': 604800,   # 1周
            '1M': 2592000   # 1月
        }

        threshold = timeframe_thresholds.get(timeframe, 86400)  # 默认1天

        if time_since_latest > threshold:
            # 需要获取最新数据
            # 根据时间框架计算下一个时间点
            timeframe_deltas = {
                '1m': timedelta(minutes=1),
                '5m': timedelta(minutes=5),
                '15m': timedelta(minutes=15),
                '30m': timedelta(minutes=30),
                '1h': timedelta(hours=1),
                '4h': timedelta(hours=4),
                '1d': timedelta(days=1),
                '1w': timedelta(weeks=1),
                '1M': timedelta(days=30)  # 近似值
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

        # 数据是最新的
        return {
            'status': 'up_to_date',
            'current_latest': latest_ts,
            'reason': '数据已是最新',
            'fetch_days': 0
        }

    async def get_time_range(self,
                             table_name: str = 'ohlcv',
                             symbol: Optional[str] = None,
                             timeframe: Optional[str] = None,
                             exchange: Optional[str] = None) -> Optional[Dict[str, datetime]]:
        """
        获取数据时间范围（最早和最新时间戳）

        Args:
            table_name: 表名
            symbol: 交易对符号（可选）
            timeframe: 时间框架（可选）
            exchange: 交易所（可选）

        Returns:
            包含start_time和end_time的字典，如果没有数据则返回None
        """
        try:
            conn = self.warehouse._get_connection()

            # 构建查询条件
            conditions = []
            params = []

            if symbol and table_name in ['ohlcv', 'tickers', 'futures_metrics']:
                conditions.append("symbol = ?")
                params.append(symbol)

            if timeframe and table_name == 'ohlcv':
                conditions.append("timeframe = ?")
                params.append(timeframe)

            if exchange and table_name in ['ohlcv', 'tickers', 'futures_metrics']:
                conditions.append("exchange = ?")
                params.append(exchange)

            # 构建WHERE子句
            where_clause = ""
            if conditions:
                where_clause = "WHERE " + " AND ".join(conditions)

            # 执行查询
            query = "SELECT MIN(ts) as min_time, MAX(ts) as max_time" +\
                    f"FROM {table_name} {where_clause}"
            result = conn.execute(query, params).fetchone()

            if result and result[0]:
                min_time, max_time = result
                return {
                    'start_time': min_time,
                    'end_time': max_time,
                    'duration': max_time - min_time if max_time and min_time else None
                }

            return None

        except Exception as e:
            logger.error(f"获取时间范围失败 [{table_name}]: {str(e)}")
            return None

    async def get_batch_update_suggestions(self,
                                           symbols: List[str],
                                           timeframes: List[str],
                                           exchange: Optional[str] = None) -> Dict[str, Any]:
        """
        批量获取数据更新建议

        Args:
            symbols: 交易对列表
            timeframes: 时间框架列表
            exchange: 交易所（可选）

        Returns:
            每个交易对/时间框架的更新建议
        """
        suggestions = {}

        for symbol in symbols:
            suggestions[symbol] = {}
            for timeframe in timeframes:
                suggestion = await self.suggest_data_fetch_range(
                    symbol, timeframe, exchange
                )
                suggestions[symbol][timeframe] = suggestion

        return {
            'timestamp': datetime.now(),
            'suggestions': suggestions,
            'summary': self._generate_update_summary(suggestions)
        }

    def _generate_update_summary(self, suggestions: Dict[str, Any]) -> Dict[str, Any]:
        """生成更新建议摘要"""
        total_symbols = len(suggestions)
        total_combinations = 0
        needs_update = 0
        no_data = 0
        up_to_date = 0

        total_fetch_days = 0

        for symbol_suggestions in suggestions.values():
            for suggestion in symbol_suggestions.values():
                total_combinations += 1
                status = suggestion['status']

                if status == 'needs_update':
                    needs_update += 1
                elif status == 'no_data':
                    no_data += 1
                elif status == 'up_to_date':
                    up_to_date += 1

                total_fetch_days += suggestion.get('fetch_days', 0)

        return {
            'total_symbols': total_symbols,
            'total_combinations': total_combinations,
            'needs_update': needs_update,
            'no_data': no_data,
            'up_to_date': up_to_date,
            'total_fetch_days': total_fetch_days,
            'efficiency': (up_to_date / total_combinations * 100) if total_combinations > 0 else 0
        }
