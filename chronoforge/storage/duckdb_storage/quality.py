"""
数据验证和增量更新模块 - 提供高级数据质量控制

包含以下功能：
1. 数据质量验证
2. 增量更新检测
3. 重复数据去重
4. 时间连续性检查
5. 数据完整性验证
"""

import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import hashlib

logger = logging.getLogger(__name__)


class DataValidator:
    """数据验证器 - 确保数据质量和一致性"""

    def __init__(self):
        """初始化数据验证器"""
        self.validation_rules = {
            'ohlcv': self._validate_ohlcv,
            'tickers': self._validate_tickers,
            'futures_metrics': self._validate_futures_metrics,
            'btc_fgi': self._validate_btc_fgi,
            'symbols': self._validate_symbols,
            'macro_fred': self._validate_macro_fred
        }

    def validate_data(self, data: pd.DataFrame, data_type: str,
                      metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        验证数据质量

        Args:
            data: 待验证的数据
            data_type: 数据类型 ('ohlcv', 'tickers', 等)
            metadata: 元数据信息

        Returns:
            验证结果字典
        """
        if data.empty:
            return {'valid': True, 'warnings': [], 'errors': [], 'stats': {'total_rows': 0}}

        if data_type not in self.validation_rules:
            return {
                'valid': False,
                'errors': [f"不支持的数据类型: {data_type}"],
                'warnings': [],
                'stats': {'total_rows': len(data)}
            }

        try:
            return self.validation_rules[data_type](data, metadata)
        except Exception as e:
            return {
                'valid': False,
                'errors': [f"验证过程出错: {str(e)}"],
                'warnings': [],
                'stats': {'total_rows': len(data)}
            }

    def _validate_ohlcv(self, data: pd.DataFrame,
                        metadata: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """验证OHLCV数据"""
        errors = []
        warnings = []
        stats = {'total_rows': len(data)}

        # 必需列检查
        required_columns = ['open', 'high', 'low', 'close', 'volume', 'ts']
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            errors.append(f"缺少必需列: {missing_columns}")
            return {'valid': False, 'errors': errors, 'warnings': warnings, 'stats': stats}

        # 数据类型检查
        numeric_columns = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_columns:
            if not pd.api.types.is_numeric_dtype(data[col]):
                errors.append(f"列 {col} 不是数值类型")

        # 价格逻辑验证
        if not data.empty:
            # High >= Low
            invalid_high_low = data['high'] < data['low']
            if invalid_high_low.any():
                invalid_count = invalid_high_low.sum()
                errors.append(f"发现 {invalid_count} 条记录 high < low")

            # High >= Open, High >= Close
            invalid_high_open = data['high'] < data['open']
            invalid_high_close = data['high'] < data['close']
            if invalid_high_open.any() or invalid_high_close.any():
                invalid_count = (invalid_high_open | invalid_high_close).sum()
                errors.append(f"发现 {invalid_count} 条记录 high 不是最高值")

            # Low <= Open, Low <= Close
            invalid_low_open = data['low'] > data['open']
            invalid_low_close = data['low'] > data['close']
            if invalid_low_open.any() or invalid_low_close.any():
                invalid_count = (invalid_low_open | invalid_low_close).sum()
                errors.append(f"发现 {invalid_count} 条记录 low 不是最低值")

            # 负交易量检查
            negative_volume = data['volume'] < 0
            if negative_volume.any():
                negative_count = negative_volume.sum()
                errors.append(f"发现 {negative_count} 条记录交易量为负")

            # 零交易量警告
            zero_volume = data['volume'] == 0
            if zero_volume.any():
                zero_count = zero_volume.sum()
                warnings.append(f"发现 {zero_count} 条记录交易量为零")

            # 价格异常值检测（使用IQR方法）
            for price_col in ['open', 'high', 'low', 'close']:
                if price_col in data.columns:
                    Q1 = data[price_col].quantile(0.25)
                    Q3 = data[price_col].quantile(0.75)
                    IQR = Q3 - Q1
                    lower_bound = Q1 - 3 * IQR
                    upper_bound = Q3 + 3 * IQR

                    outliers = (data[price_col] < lower_bound) | (data[price_col] > upper_bound)
                    if outliers.any():
                        outlier_count = outliers.sum()
                        warnings.append(f"列 {price_col} 发现 {outlier_count} 个异常值")

            # 时间戳验证
            if 'ts' in data.columns:
                # 检查时间戳格式
                try:
                    pd.to_datetime(data['ts'])
                except (ValueError, pd.errors.ParserError) as e:
                    errors.append(f"时间戳格式无效: {str(e)}")

                # 检查时间顺序
                if len(data) > 1:
                    # 先确保时间戳是排序的
                    time_series = data['ts'].sort_values()
                    time_diff = time_series.diff().dropna()  # 移除第一个NaT值
                    if len(time_diff) > 0:
                        negative_diff = time_diff < pd.Timedelta(0)
                        if negative_diff.any():
                            backward_count = negative_diff.sum()
                            errors.append(f"发现 {backward_count} 条时间逆序记录")

                # 时间范围统计
                time_range = data['ts'].max() - data['ts'].min()
                stats['time_range_days'] = time_range.days
                stats['start_time'] = data['ts'].min().isoformat()
                stats['end_time'] = data['ts'].max().isoformat()

        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings,
            'stats': stats
        }

    def _validate_tickers(self, data: pd.DataFrame,
                          metadata: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """验证Tickers数据"""
        errors = []
        warnings = []
        stats = {'total_rows': len(data)}

        # 必需列检查
        required_columns = ['symbol', 'ts']
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            errors.append(f"缺少必需列: {missing_columns}")

        # 价格逻辑验证
        price_columns = ['last', 'bid', 'ask']
        existing_price_cols = [col for col in price_columns if col in data.columns]

        if existing_price_cols:
            for col in existing_price_cols:
                if not pd.api.types.is_numeric_dtype(data[col]):
                    errors.append(f"价格列 {col} 不是数值类型")

            # Bid-Ask价差验证
            if 'bid' in data.columns and 'ask' in data.columns:
                negative_spread = data['bid'] > data['ask']
                if negative_spread.any():
                    invalid_count = negative_spread.sum()
                    errors.append(f"发现 {invalid_count} 条记录 bid > ask")

            # 价格异常值检测
            for col in existing_price_cols:
                if pd.api.types.is_numeric_dtype(data[col]):
                    Q1 = data[col].quantile(0.25)
                    Q3 = data[col].quantile(0.75)
                    IQR = Q3 - Q1
                    lower_bound = Q1 - 3 * IQR
                    upper_bound = Q3 + 3 * IQR

                    outliers = (data[col] < lower_bound) | (data[col] > upper_bound)
                    if outliers.any():
                        outlier_count = outliers.sum()
                        warnings.append(f"列 {col} 发现 {outlier_count} 个异常值")

        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings,
            'stats': stats
        }

    def _validate_futures_metrics(self, data: pd.DataFrame,
                                  metadata: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """验证期货指标数据"""
        errors = []
        warnings = []
        stats = {'total_rows': len(data)}

        # 必需列检查
        required_columns = ['symbol', 'ts']
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            errors.append(f"缺少必需列: {missing_columns}")

        # 资金费率验证
        if 'funding_rate' in data.columns:
            if not pd.api.types.is_numeric_dtype(data['funding_rate']):
                errors.append("资金费率列不是数值类型")
            else:
                # 资金费率通常在-1到1之间
                extreme_rates = (data['funding_rate'] < -1) | (data['funding_rate'] > 1)
                if extreme_rates.any():
                    extreme_count = extreme_rates.sum()
                    warnings.append(f"发现 {extreme_count} 条记录资金费率超出正常范围(-1, 1)")

        # 持仓量验证
        if 'open_interest' in data.columns:
            if not pd.api.types.is_numeric_dtype(data['open_interest']):
                errors.append("持仓量列不是数值类型")
            else:
                negative_oi = data['open_interest'] < 0
                if negative_oi.any():
                    negative_count = negative_oi.sum()
                    errors.append(f"发现 {negative_count} 条记录持仓量为负")

        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings,
            'stats': stats
        }

    def _validate_btc_fgi(self, data: pd.DataFrame,
                          metadata: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """验证BTC恐惧贪婪指数数据"""
        errors = []
        warnings = []
        stats = {'total_rows': len(data)}

        # 必需列检查
        required_columns = ['value', 'ts']
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            errors.append(f"缺少必需列: {missing_columns}")

        # 数值范围验证
        if 'value' in data.columns:
            if not pd.api.types.is_numeric_dtype(data['value']):
                errors.append("FGI值列不是数值类型")
            else:
                # FGI值应该在0-100之间
                out_of_range = (data['value'] < 0) | (data['value'] > 100)
                if out_of_range.any():
                    invalid_count = out_of_range.sum()
                    errors.append(f"发现 {invalid_count} 条记录FGI值超出范围(0-100)")

        # 标签验证
        if 'label' in data.columns:
            valid_labels = {'Extreme Fear', 'Fear', 'Neutral', 'Greed', 'Extreme Greed'}
            invalid_labels = set(data['label'].dropna().unique()) - valid_labels
            if invalid_labels:
                warnings.append(f"发现无效标签: {invalid_labels}")

        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings,
            'stats': stats
        }

    def _validate_symbols(self, data: pd.DataFrame,
                          metadata: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """验证Symbols数据"""
        errors = []
        warnings = []
        stats = {'total_rows': len(data)}

        # 必需列检查
        required_columns = ['symbol', 'exchange', 'market_type', 'base_asset']
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            errors.append(f"缺少必需列: {missing_columns}")

        # Symbol格式验证
        if 'symbol' in data.columns:
            # 检查是否包含非法字符
            invalid_symbols = data['symbol'].str.contains(r'[^a-zA-Z0-9/_\-]', na=False)
            if invalid_symbols.any():
                invalid_count = invalid_symbols.sum()
                errors.append(f"发现 {invalid_count} 个symbol包含非法字符")

        # 交易所验证
        if 'exchange' in data.columns:
            valid_exchanges = {'binance', 'okx', 'bybit', 'gate', 'mexc', 'kucoin', 'yahoo', 'fred'}
            invalid_exchanges = set(data['exchange'].dropna().unique()) - valid_exchanges
            if invalid_exchanges:
                warnings.append(f"发现未知交易所: {invalid_exchanges}")

        # 市场类型验证
        if 'market_type' in data.columns:
            valid_market_types = {'spot', 'future', 'perpetual', 'index', 'macro'}
            invalid_types = set(data['market_type'].dropna().unique()) - valid_market_types
            if invalid_types:
                errors.append(f"发现无效市场类型: {invalid_types}")

        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings,
            'stats': stats
        }

    def _validate_macro_fred(self, data: pd.DataFrame,
                             metadata: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """验证宏观数据"""
        errors = []
        warnings = []
        stats = {'total_rows': len(data)}

        # 必需列检查
        required_columns = ['series_id', 'symbol', 'value', 'ts']
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            errors.append(f"缺少必需列: {missing_columns}")

        # 数值验证
        if 'value' in data.columns:
            if not pd.api.types.is_numeric_dtype(data['value']):
                errors.append("数值列不是数值类型")
            else:
                # 检查负值（某些宏观指标可能为负，但发出警告）
                negative_values = data['value'] < 0
                if negative_values.any():
                    negative_count = negative_values.sum()
                    warnings.append(f"发现 {negative_count} 条记录数值为负")

        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings,
            'stats': stats
        }


class IncrementalUpdater:
    """增量更新管理器 - 处理数据的增量更新和去重"""

    def __init__(self):
        """初始化增量更新管理器"""
        self.update_strategies = {
            'ohlcv': self._get_ohlcv_update_keys,
            'tickers': self._get_tickers_update_keys,
            'futures_metrics': self._get_futures_update_keys,
            'btc_fgi': self._get_fgi_update_keys,
            'symbols': self._get_symbols_update_keys,
            'macro_fred': self._get_macro_update_keys
        }

    def detect_new_records(self, new_data: pd.DataFrame, existing_data: pd.DataFrame,
                           data_type: str) -> pd.DataFrame:
        """
        检测新记录（增量数据）

        Args:
            new_data: 新数据
            existing_data: 现有数据
            data_type: 数据类型

        Returns:
            真正的新记录
        """
        if new_data.empty:
            return existing_data

        if existing_data.empty:
            return new_data

        # 获取更新键
        update_keys = self.get_update_keys(data_type)
        if not update_keys:
            logger.warning(f"无法获取 {data_type} 的更新键，返回全部新数据")
            return new_data

        # 确保键存在于数据中
        missing_keys = [key for key in update_keys if key not in new_data.columns or
                        key not in existing_data.columns]
        if missing_keys:
            logger.warning(f"缺少更新键 {missing_keys}，返回全部新数据")
            return new_data

        # 使用merge检测新记录
        merged = new_data.merge(existing_data[update_keys], on=update_keys, how='left',
                                indicator=True)
        new_records = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])

        logger.debug(f"检测到 {len(new_records)} 条新记录，原始数据 {len(new_data)} 条")
        return new_records

    def detect_duplicate_records(self, data: pd.DataFrame,
                                 data_type: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        检测重复记录

        Args:
            data: 待检测数据
            data_type: 数据类型

        Returns:
            (去重后的数据, 重复记录)
        """
        if data.empty:
            return data, pd.DataFrame()

        # 获取更新键（用于识别重复）
        update_keys = self.get_update_keys(data_type)
        if not update_keys:
            logger.warning(f"无法获取 {data_type} 的更新键，不进行检测")
            return data, pd.DataFrame()

        # 确保键存在于数据中
        missing_keys = [key for key in update_keys if key not in data.columns]
        if missing_keys:
            logger.warning(f"缺少更新键 {missing_keys}，不进行检测")
            return data, pd.DataFrame()

        # 检测重复记录
        duplicated = data.duplicated(subset=update_keys, keep='first')

        deduplicated_data = data[~duplicated].copy()
        duplicate_records = data[duplicated].copy()

        logger.debug(f"检测到 {len(duplicate_records)} 条重复记录，保留 {len(deduplicated_data)} 条")
        return deduplicated_data, duplicate_records

    def get_update_keys(self, data_type: str) -> List[str]:
        """获取指定数据类型的更新键"""
        if data_type in self.update_strategies:
            return self.update_strategies[data_type]()
        else:
            logger.warning(f"不支持的数据类型: {data_type}")
            return []

    def _get_ohlcv_update_keys(self) -> List[str]:
        """OHLCV数据更新键"""
        return ['exchange', 'market_type', 'symbol', 'timeframe', 'ts']

    def _get_tickers_update_keys(self) -> List[str]:
        """Tickers数据更新键"""
        return ['exchange', 'market_type', 'symbol', 'ts']

    def _get_futures_update_keys(self) -> List[str]:
        """期货指标数据更新键"""
        return ['exchange', 'symbol', 'ts']

    def _get_fgi_update_keys(self) -> List[str]:
        """FGI数据更新键"""
        return ['ts']

    def _get_symbols_update_keys(self) -> List[str]:
        """Symbols数据更新键"""
        return ['symbol']

    def _get_macro_update_keys(self) -> List[str]:
        """宏观数据更新键"""
        return ['series_id', 'symbol', 'ts']

    def check_time_continuity(self, data: pd.DataFrame, data_type: str,
                              expected_interval: Optional[str] = None) -> Dict[str, Any]:
        """
        检查时间连续性

        Args:
            data: 待检查数据
            data_type: 数据类型
            expected_interval: 期望的时间间隔（如 '1d', '1h'）

        Returns:
            连续性检查结果
        """
        if data.empty or 'ts' not in data.columns:
            return {'continuous': True, 'gaps': [], 'warnings': []}

        errors = []
        warnings = []
        gaps = []

        try:
            # 按时间排序
            data_sorted = data.sort_values('ts')

            # 计算时间差
            time_diff = data_sorted['ts'].diff()

            # 检测时间逆序
            negative_diff = time_diff < pd.Timedelta(0)
            if negative_diff.any():
                backward_count = negative_diff.sum()
                errors.append(f"发现 {backward_count} 条时间逆序记录")

            # 如果提供了期望间隔，检查间隔一致性
            if expected_interval:
                expected_delta = pd.Timedelta(expected_interval)

                # 允许10%的容差
                tolerance = expected_delta * 0.1

                # 检测间隔异常（过大或过小）
                abnormal_intervals = (time_diff > expected_delta + tolerance) | \
                    (time_diff < expected_delta - tolerance)

                if abnormal_intervals.any():
                    # 区分间隔过大和过小
                    large_gaps = time_diff > expected_delta + tolerance
                    small_gaps = time_diff < expected_delta - tolerance

                    large_gap_count = large_gaps.sum()
                    small_gap_count = small_gaps.sum()

                    if large_gap_count > 0:
                        warnings.append(f"发现 {large_gap_count} 个时间间隔过大")

                        # 记录具体的间隔信息
                        for idx, gap in time_diff[large_gaps].items():
                            gap_info = {
                                'type': 'large_gap',
                                'before_time': (data_sorted.loc[idx - 1, 'ts'].isoformat()
                                                if idx - 1 in data_sorted.index else None),
                                'after_time': data_sorted.loc[idx, 'ts'].isoformat(),
                                'actual_interval': str(gap),
                                'expected_interval': expected_interval,
                                'gap_size': gap / expected_delta
                            }
                            gaps.append(gap_info)

                    if small_gap_count > 0:
                        warnings.append(f"发现 {small_gap_count} 个时间间隔过小")

            # 统计信息
            stats = {
                'total_points': len(data),
                'time_range': str(data['ts'].max() - data['ts'].min()),
                'start_time': data['ts'].min().isoformat(),
                'end_time': data['ts'].max().isoformat()
            }

            return {
                'continuous': len(errors) == 0 and len(gaps) == 0,
                'gaps': gaps,
                'warnings': warnings,
                'errors': errors,
                'stats': stats
            }

        except Exception as e:
            return {
                'continuous': False,
                'gaps': [],
                'warnings': [f"连续性检查出错: {str(e)}"],
                'errors': [],
                'stats': {'total_points': len(data)}
            }

    def generate_data_hash(self, data: pd.DataFrame, data_type: str) -> str:
        """
        生成数据哈希值（用于快速比较）

        Args:
            data: 待哈希数据
            data_type: 数据类型

        Returns:
            数据的哈希值
        """
        if data.empty:
            return ""

        # 获取更新键用于排序
        update_keys = self.get_update_keys(data_type)
        if update_keys and all(key in data.columns for key in update_keys):
            # 按更新键排序以确保一致性
            data_sorted = data.sort_values(update_keys)
            # 只使用更新键生成哈希
            hash_data = data_sorted[update_keys]
        else:
            # 使用全部数据
            data_sorted = data.sort_index()
            hash_data = data_sorted

        # 转换为字符串并生成哈希
        data_str = hash_data.to_string()
        return hashlib.md5(data_str.encode()).hexdigest()

    def compare_data_sets(self, new_data: pd.DataFrame, existing_data: pd.DataFrame,
                          data_type: str) -> Dict[str, Any]:
        """
        比较两个数据集

        Args:
            new_data: 新数据
            existing_data: 现有数据
            data_type: 数据类型

        Returns:
            比较结果
        """
        comparison = {
            'new_records': 0,
            'updated_records': 0,
            'deleted_records': 0,
            'unchanged_records': 0,
            'total_new': len(new_data),
            'total_existing': len(existing_data),
            'data_hash_match': False
        }

        if new_data.empty and existing_data.empty:
            return comparison

        # 生成哈希值进行快速比较
        new_hash = self.generate_data_hash(new_data, data_type)
        existing_hash = self.generate_data_hash(existing_data, data_type)
        comparison['data_hash_match'] = (new_hash == existing_hash)

        if comparison['data_hash_match']:
            comparison['unchanged_records'] = len(existing_data)
            return comparison

        # 获取更新键
        update_keys = self.get_update_keys(data_type)
        if not update_keys or not all(key in new_data.columns for key in update_keys):
            logger.warning("无法进行比较，缺少更新键")
            return comparison

        # 检测新记录
        new_records = self.detect_new_records(new_data, existing_data, data_type)
        comparison['new_records'] = len(new_records)

        # 检测重复记录（在新数据中）
        deduped_new, duplicates = self.detect_duplicate_records(new_data, data_type)
        comparison['duplicate_records_in_new'] = len(duplicates)

        return comparison


class DataQualityMonitor:
    """数据质量监控器 - 持续监控数据质量"""

    def __init__(self):
        """初始化数据质量监控器"""
        self.validator = DataValidator()
        self.updater = IncrementalUpdater()
        self.quality_history = []

    def monitor_data_quality(self, data: pd.DataFrame, data_type: str, source: str,
                             metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        监控数据质量

        Args:
            data: 待监控数据
            data_type: 数据类型
            source: 数据来源
            metadata: 元数据

        Returns:
            质量监控结果
        """
        timestamp = datetime.now().isoformat()

        # 数据验证
        validation_result = self.validator.validate_data(data, data_type, metadata)

        # 增量更新检测
        incremental_info = {
            'data_hash': self.updater.generate_data_hash(data, data_type),
            'record_count': len(data)
        }

        # 时间连续性检查（如果适用）
        continuity_result = None
        if 'ts' in data.columns and data_type in ['ohlcv', 'tickers', 'futures_metrics']:
            expected_interval = metadata.get('expected_interval') if metadata else None
            continuity_result = self.updater.check_time_continuity(data, data_type,
                                                                   expected_interval)

        # 生成质量报告
        quality_report = {
            'timestamp': timestamp,
            'source': source,
            'data_type': data_type,
            'validation': validation_result,
            'incremental': incremental_info,
            'continuity': continuity_result,
            'overall_quality_score': self._calculate_quality_score(validation_result,
                                                                   continuity_result)
        }

        # 记录历史
        self.quality_history.append(quality_report)

        # 保持历史记录在合理大小内
        if len(self.quality_history) > 1000:
            self.quality_history = self.quality_history[-500:]

        return quality_report

    def _calculate_quality_score(self, validation_result: Dict[str, Any],
                                 continuity_result: Optional[Dict[str, Any]]) -> float:
        """计算质量分数"""
        score = 100.0

        # 验证结果扣分
        if validation_result:
            if not validation_result.get('valid', True):
                score -= 50.0  # 严重错误扣50分

            # 警告扣分
            warnings = validation_result.get('warnings', [])
            score -= len(warnings) * 5.0  # 每个警告扣5分

        # 连续性扣分
        if continuity_result:
            if not continuity_result.get('continuous', True):
                score -= 20.0  # 不连续扣20分

            warnings = continuity_result.get('warnings', [])
            score -= len(warnings) * 2.0  # 连续性警告每个扣2分

        return max(0.0, min(100.0, score))

    def get_quality_trends(self, data_type: Optional[str] = None,
                           source: Optional[str] = None,
                           days: int = 7) -> Dict[str, Any]:
        """
        获取质量趋势

        Args:
            data_type: 数据类型过滤
            source: 数据来源过滤
            days: 天数

        Returns:
            质量趋势分析
        """
        if not self.quality_history:
            return {'error': '没有质量历史数据'}

        # 过滤历史记录
        filtered_history = self.quality_history
        if data_type:
            filtered_history = [h for h in filtered_history if h['data_type'] == data_type]
        if source:
            filtered_history = [h for h in filtered_history if h['source'] == source]

        if not filtered_history:
            return {'error': '没有符合条件的质量历史数据'}

        # 计算趋势
        quality_scores = [h['overall_quality_score'] for h in filtered_history]

        trends = {
            'total_records': len(filtered_history),
            'average_score': np.mean(quality_scores),
            'min_score': min(quality_scores),
            'max_score': max(quality_scores),
            'score_std': np.std(quality_scores),
            'recent_scores': quality_scores[-10:],  # 最近10个分数
            'improving': quality_scores[-1] > quality_scores[0] if len(quality_scores) > 1 else None
        }

        return trends
