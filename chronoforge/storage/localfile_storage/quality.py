"""
本地文件存储数据验证和增量更新模块

提供数据验证、去重和时间连续性检查功能。
"""

import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)


class LocalDataValidator:
    """本地存储数据验证器"""

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
        """验证数据质量"""
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

        required_columns = ['open', 'high', 'low', 'close', 'volume', 'ts']
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            errors.append(f"缺少必需列: {missing_columns}")
            return {'valid': False, 'errors': errors, 'warnings': warnings, 'stats': stats}

        numeric_columns = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_columns:
            if not pd.api.types.is_numeric_dtype(data[col]):
                errors.append(f"列 {col} 不是数值类型")

        if not data.empty:
            invalid_high_low = data['high'] < data['low']
            if invalid_high_low.any():
                invalid_count = invalid_high_low.sum()
                errors.append(f"发现 {invalid_count} 条记录 high < low")

            invalid_high_open = data['high'] < data['open']
            invalid_high_close = data['high'] < data['close']
            if invalid_high_open.any() or invalid_high_close.any():
                invalid_count = (invalid_high_open | invalid_high_close).sum()
                errors.append(f"发现 {invalid_count} 条记录 high 不是最高值")

            invalid_low_open = data['low'] > data['open']
            invalid_low_close = data['low'] > data['close']
            if invalid_low_open.any() or invalid_low_close.any():
                invalid_count = (invalid_low_open | invalid_low_close).sum()
                errors.append(f"发现 {invalid_count} 条记录 low 不是最低值")

            negative_volume = data['volume'] < 0
            if negative_volume.any():
                errors.append(f"发现 {negative_volume.sum()} 条记录交易量为负")

            zero_volume = data['volume'] == 0
            if zero_volume.any():
                warnings.append(f"发现 {zero_volume.sum()} 条记录交易量为零")

            if 'ts' in data.columns:
                try:
                    pd.to_datetime(data['ts'])
                except (ValueError, pd.errors.ParserError) as e:
                    errors.append(f"时间戳格式无效: {str(e)}")

                if len(data) > 1:
                    time_series = data['ts'].sort_values()
                    time_diff = time_series.diff().dropna()
                    if len(time_diff) > 0:
                        negative_diff = time_diff < pd.Timedelta(0)
                        if negative_diff.any():
                            errors.append(f"发现 {negative_diff.sum()} 条时间逆序记录")

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

        required_columns = ['symbol', 'ts']
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            errors.append(f"缺少必需列: {missing_columns}")

        price_columns = ['last', 'bid', 'ask']
        existing_price_cols = [col for col in price_columns if col in data.columns]

        if existing_price_cols:
            for col in existing_price_cols:
                if not pd.api.types.is_numeric_dtype(data[col]):
                    errors.append(f"价格列 {col} 不是数值类型")

            if 'bid' in data.columns and 'ask' in data.columns:
                negative_spread = data['bid'] > data['ask']
                if negative_spread.any():
                    errors.append(f"发现 {negative_spread.sum()} 条记录 bid > ask")

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

        required_columns = ['symbol', 'ts']
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            errors.append(f"缺少必需列: {missing_columns}")

        if 'funding_rate' in data.columns:
            if not pd.api.types.is_numeric_dtype(data['funding_rate']):
                errors.append("资金费率列不是数值类型")

        if 'open_interest' in data.columns:
            if not pd.api.types.is_numeric_dtype(data['open_interest']):
                errors.append("持仓量列不是数值类型")
            else:
                negative_oi = data['open_interest'] < 0
                if negative_oi.any():
                    errors.append(f"发现 {negative_oi.sum()} 条记录持仓量为负")

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

        required_columns = ['value', 'ts']
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            errors.append(f"缺少必需列: {missing_columns}")

        if 'value' in data.columns:
            if not pd.api.types.is_numeric_dtype(data['value']):
                errors.append("FGI值列不是数值类型")
            else:
                out_of_range = (data['value'] < 0) | (data['value'] > 100)
                if out_of_range.any():
                    errors.append(f"发现 {out_of_range.sum()} 条记录FGI值超出范围(0-100)")

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

        required_columns = ['symbol', 'exchange', 'market_type', 'base_asset']
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            errors.append(f"缺少必需列: {missing_columns}")

        if 'symbol' in data.columns:
            invalid_symbols = data['symbol'].str.contains(r'[^a-zA-Z0-9/_\-]', na=False)
            if invalid_symbols.any():
                errors.append(f"发现 {invalid_symbols.sum()} 个symbol包含非法字符")

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

        required_columns = ['series_id', 'symbol', 'value', 'ts']
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            errors.append(f"缺少必需列: {missing_columns}")

        if 'value' in data.columns:
            if not pd.api.types.is_numeric_dtype(data['value']):
                errors.append("数值列不是数值类型")

        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings,
            'stats': stats
        }


class LocalIncrementalUpdater:
    """本地存储增量更新管理器"""

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
        """检测新记录"""
        if new_data.empty:
            return existing_data

        if existing_data.empty:
            return new_data

        update_keys = self.get_update_keys(data_type)
        if not update_keys:
            return new_data

        missing_keys = [key for key in update_keys if key not in new_data.columns or
                        key not in existing_data.columns]
        if missing_keys:
            return new_data

        merged = new_data.merge(existing_data[update_keys], on=update_keys, how='left',
                                indicator=True)
        new_records = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])

        return new_records

    def detect_duplicate_records(self, data: pd.DataFrame,
                                 data_type: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """检测重复记录"""
        if data.empty:
            return data, pd.DataFrame()

        update_keys = self.get_update_keys(data_type)
        if not update_keys:
            return data, pd.DataFrame()

        missing_keys = [key for key in update_keys if key not in data.columns]
        if missing_keys:
            return data, pd.DataFrame()

        duplicated = data.duplicated(subset=update_keys, keep='first')
        deduplicated_data = data[~duplicated].copy()
        duplicate_records = data[duplicated].copy()

        return deduplicated_data, duplicate_records

    def get_update_keys(self, data_type: str) -> List[str]:
        """获取指定数据类型的更新键"""
        if data_type in self.update_strategies:
            return self.update_strategies[data_type]()
        return []

    def _get_ohlcv_update_keys(self) -> List[str]:
        return ['exchange', 'market_type', 'symbol', 'timeframe', 'ts']

    def _get_tickers_update_keys(self) -> List[str]:
        return ['exchange', 'market_type', 'symbol', 'ts']

    def _get_futures_update_keys(self) -> List[str]:
        return ['exchange', 'symbol', 'ts']

    def _get_fgi_update_keys(self) -> List[str]:
        return ['ts']

    def _get_symbols_update_keys(self) -> List[str]:
        return ['symbol']

    def _get_macro_update_keys(self) -> List[str]:
        return ['series_id', 'symbol', 'ts']

    def check_time_continuity(self, data: pd.DataFrame, data_type: str,
                              expected_interval: Optional[str] = None) -> Dict[str, Any]:
        """检查时间连续性"""
        if data.empty or 'ts' not in data.columns:
            return {'continuous': True, 'gaps': [], 'warnings': []}

        errors = []
        warnings = []

        try:
            data_sorted = data.sort_values('ts')
            time_diff = data_sorted['ts'].diff()

            negative_diff = time_diff < pd.Timedelta(0)
            if negative_diff.any():
                errors.append(f"发现 {negative_diff.sum()} 条时间逆序记录")

            return {
                'continuous': len(errors) == 0,
                'gaps': [],
                'warnings': warnings,
                'errors': errors,
                'stats': {
                    'total_points': len(data),
                    'time_range': str(data['ts'].max() - data['ts'].min()),
                    'start_time': data['ts'].min().isoformat(),
                    'end_time': data['ts'].max().isoformat()
                }
            }

        except Exception as e:
            return {
                'continuous': False,
                'gaps': [],
                'warnings': [f"连续性检查出错: {str(e)}"],
                'errors': [],
                'stats': {'total_points': len(data)}
            }


class LocalDataQualityMonitor:
    """本地存储数据质量监控器"""

    def __init__(self):
        """初始化数据质量监控器"""
        self.validator = LocalDataValidator()
        self.updater = LocalIncrementalUpdater()
        self.quality_history = []

    def monitor_data_quality(self, data: pd.DataFrame, data_type: str, source: str,
                             metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """监控数据质量"""
        timestamp = datetime.now().isoformat()

        validation_result = self.validator.validate_data(data, data_type, metadata)

        quality_score = 100.0
        if not validation_result.get('valid', True):
            quality_score -= 50.0

        warnings = validation_result.get('warnings', [])
        quality_score -= len(warnings) * 5.0
        quality_score = max(0.0, min(100.0, quality_score))

        quality_report = {
            'timestamp': timestamp,
            'source': source,
            'data_type': data_type,
            'validation': validation_result,
            'overall_quality_score': quality_score
        }

        self.quality_history.append(quality_report)

        if len(self.quality_history) > 1000:
            self.quality_history = self.quality_history[-500:]

        return quality_report

    def get_quality_trends(self, data_type: Optional[str] = None,
                           days: int = 7) -> Dict[str, Any]:
        """获取质量趋势"""
        if not self.quality_history:
            return {'error': '没有质量历史数据'}

        filtered_history = self.quality_history
        if data_type:
            filtered_history = [h for h in filtered_history if h['data_type'] == data_type]

        if not filtered_history:
            return {'error': '没有符合条件的质量历史数据'}

        quality_scores = [h['overall_quality_score'] for h in filtered_history]

        trends = {
            'total_records': len(filtered_history),
            'average_score': np.mean(quality_scores),
            'min_score': min(quality_scores),
            'max_score': max(quality_scores),
            'recent_scores': quality_scores[-10:]
        }

        return trends
