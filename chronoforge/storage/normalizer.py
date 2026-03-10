import logging
from typing import Dict, List, Any, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class DataFrameNormalizer:
    """数据标准化器 - 消除存储适配器中的重复逻辑

    提供统一的数据类型检测、列名映射、数据验证等功能，
    供所有存储适配器（DuckDB、LocalFile等）使用。
    """

    OHLCV_COLUMN_MAPPING = {
        'time': 'ts',
        'timestamp': 'ts',
        'datetime': 'ts',
        'open': 'open',
        'high': 'high',
        'low': 'low',
        'close': 'close',
        'volume': 'volume',
        'quote_volume': 'quote_volume',
        'amount': 'quote_volume',
        'quoteAmount': 'quote_volume',
        'source': 'source'
    }

    TICKERS_COLUMN_MAPPING = {
        'time': 'ts',
        'timestamp': 'ts',
        'datetime': 'ts',
        'last_price': 'last',
        'price': 'last',
        'baseVolume': 'base_volume',
        'quoteVolume': 'quote_volume',
        'volume': 'base_volume',
        'bidPrice': 'bid',
        'askPrice': 'ask',
        'bid_price': 'bid',
        'ask_price': 'ask'
    }

    FUTURES_METRICS_COLUMN_MAPPING = {
        'time': 'ts',
        'timestamp': 'ts',
        'datetime': 'ts',
        'oi': 'open_interest',
        'open_interest': 'open_interest',
        'open_interest_value': 'oi_value',
        'oi_value': 'oi_value',
        'oi_value_usd': 'oi_value',
        'sumOpenInterestValue': 'oi_value',
        'funding_rate_1h': 'funding_rate',
        'funding_rate': 'funding_rate',
        'taker_long_short_ratio': 'taker_long_short_ratio',
        'top_long_short_position_ratio': 'top_long_short_position_ratio',
        'top_long_short_account_ratio': 'top_long_short_account_ratio',
        'global_long_short_account_ratio': 'global_long_short_account_ratio'
    }

    BTC_FGI_COLUMN_MAPPING = {
        'time': 'ts',
        'timestamp': 'ts',
        'datetime': 'ts',
        'fgi_value': 'value',
        'fgi': 'value',
        'fgi_value_classification': 'label'
    }

    MACRO_FRED_COLUMN_MAPPING = {
        'time': 'ts',
        'timestamp': 'ts',
        'datetime': 'ts',
        'value': 'value',
        'volume': 'value',
        'fred_series_id': 'series_id'
    }

    OHLCV_REQUIRED_COLS = [
        'exchange', 'market_type', 'symbol', 'timeframe', 'ts',
        'open', 'high', 'low', 'close', 'volume'
    ]

    OHLCV_OPTIONAL_COLS = {
        'quote_volume': 0.0,
        'source': 'unknown'
    }

    TIMEFRAMES = ['1m', '5m', '15m', '30m', '1h', '4h', '1d', '1w', '1M']

    @staticmethod
    def detect_data_type(
        data: pd.DataFrame,
        id: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """自动检测数据类型

        Args:
            data: DataFrame数据
            id: 数据标识符
            metadata: 元数据

        Returns:
            str: 数据类型
        """
        columns = set(data.columns.str.lower())

        if metadata and 'data_type' in metadata:
            return metadata['data_type']

        if {'open', 'high', 'low', 'close', 'volume'}.issubset(columns):
            return 'ohlcv'
        elif {'last', 'bid', 'ask'}.intersection(columns):
            return 'tickers'
        elif {'funding_rate', 'open_interest'}.intersection(columns):
            return 'futures_metrics'
        elif {'fgi_value', 'value'}.intersection(columns) \
                and 'label' in columns:
            return 'btc_fgi'
        elif {'series_id'}.intersection(columns):
            return 'macro_fred'
        elif {'category_name', 'description'}.issubset(columns):
            return 'coin_categories'
        elif {'current_price', 'market_cap_rank'}.issubset(columns) \
                and 'market_cap' in columns:
            return 'coin_markets'
        elif {'base_asset', 'quote_asset', 'exchange', 'market_type'}.issubset(columns):
            return 'symbols'
        else:
            id_lower = id.lower()
            if 'fgi' in id_lower or 'fear' in id_lower or 'greed' in id_lower:
                return 'btc_fgi'
            elif 'future' in id_lower or 'perp' in id_lower or 'swap' in id_lower:
                return 'futures_metrics'
            elif 'ticker' in id_lower or 'quote' in id_lower or 'price' in id_lower:
                return 'tickers'
            elif 'macro' in id_lower or 'fred' in id_lower or 'rate' in id_lower:
                return 'macro_fred'
            elif 'category' in id_lower or 'coin' in id_lower:
                return 'coin_categories'
            elif 'symbol' in id_lower or 'universe' in id_lower:
                return 'symbols'
            else:
                return 'ohlcv'

    @staticmethod
    def normalize_columns(
        data: pd.DataFrame,
        mapping: Dict[str, str]
    ) -> pd.DataFrame:
        """标准化列名

        Args:
            data: 原始数据
            mapping: 列名映射字典

        Returns:
            pd.DataFrame: 列名标准化后的数据
        """
        data = data.copy()
        for old_col, new_col in mapping.items():
            if old_col in data.columns and new_col not in data.columns:
                data.rename(columns={old_col: new_col}, inplace=True)
        return data

    @staticmethod
    def extract_metadata_fields(
        metadata: Optional[Dict[str, Any]],
        id: str,
        data_type: str
    ) -> Dict[str, Any]:
        """提取元数据字段

        Args:
            metadata: 元数据字典
            id: 数据标识符
            data_type: 数据类型

        Returns:
            Dict: 提取的字段字典
        """
        result = {}

        if data_type == 'ohlcv':
            result['exchange'] = metadata.get('exchange') if metadata else None
            if not result['exchange']:
                result['exchange'] = metadata.get('ex', 'unknown') if metadata else 'unknown'
            result['market_type'] = metadata.get('market_type', 'spot') if metadata else 'spot'
            result['symbol'] = metadata.get('symbol') if metadata \
                else id.split('_')[0] if '_' in id else id
            result['timeframe'] = metadata.get('timeframe') if metadata else '1d'
            if metadata:
                id_parts = id.split('_')
                if len(id_parts) > 1:
                    possible_tf = id_parts[-1]
                    if possible_tf in DataFrameNormalizer.TIMEFRAMES:
                        result['timeframe'] = possible_tf

        elif data_type in ['tickers', 'futures_metrics', 'macro_fred', 'btc_fgi']:
            result['exchange'] = metadata.get('exchange') if metadata else 'unknown'
            result['market_type'] = metadata.get('market_type') if metadata else 'spot'
            result['symbol'] = metadata.get('symbol') if metadata \
                else id.split('_')[0] if '_' in id else id

        return result

    @staticmethod
    def normalize_ohlcv(
        data: pd.DataFrame,
        id: str,
        metadata: Optional[Dict[str, Any]]
    ) -> Optional[pd.DataFrame]:
        """标准化OHLCV数据

        Args:
            data: 原始数据
            id: 数据标识符
            metadata: 元数据

        Returns:
            Optional[pd.DataFrame]: 标准化后的数据，失败返回None
        """
        try:
            data = DataFrameNormalizer.normalize_columns(
                data, DataFrameNormalizer.OHLCV_COLUMN_MAPPING
            )

            fields = DataFrameNormalizer.extract_metadata_fields(
                metadata, id, 'ohlcv'
            )
            exchange = fields.get('exchange', 'unknown')
            market_type = fields.get('market_type', 'spot')
            symbol = fields.get('symbol', id)
            timeframe = fields.get('timeframe', '1d')

            field_values = {
                'exchange': exchange,
                'market_type': market_type,
                'symbol': symbol,
                'timeframe': timeframe
            }
            required_cols = ['exchange', 'market_type', 'symbol', 'timeframe']
            for col in required_cols:
                if col not in data.columns:
                    data[col] = field_values[col]

            if 'ts' not in data.columns:
                logger.error("OHLCV数据缺少必需列: ts")
                return None

            if not pd.api.types.is_datetime64_any_dtype(data['ts']):
                data['ts'] = pd.to_datetime(data['ts'], utc=True)

            numeric_cols = ['open', 'high', 'low', 'close', 'volume']
            for col in numeric_cols:
                if col in data.columns:
                    data[col] = pd.to_numeric(data[col], errors='coerce')

            missing_cols = [
                col for col in DataFrameNormalizer.OHLCV_REQUIRED_COLS
                if col not in data.columns
            ]
            if missing_cols:
                logger.error(f"OHLCV数据缺少必需列: {missing_cols}")
                return None

            for col, default_value in DataFrameNormalizer.OHLCV_OPTIONAL_COLS.items():
                if col not in data.columns:
                    data[col] = default_value

            return data

        except Exception as e:
            logger.error(f"标准化OHLCV数据失败: {str(e)}")
            return None

    @staticmethod
    def normalize_tickers(
        data: pd.DataFrame,
        id: str,
        metadata: Optional[Dict[str, Any]]
    ) -> Optional[pd.DataFrame]:
        """标准化Tickers数据

        Args:
            data: 原始数据
            id: 数据标识符
            metadata: 元数据

        Returns:
            Optional[pd.DataFrame]: 标准化后的数据，失败返回None
        """
        try:
            data = DataFrameNormalizer.normalize_columns(
                data, DataFrameNormalizer.TICKERS_COLUMN_MAPPING
            )

            fields = DataFrameNormalizer.extract_metadata_fields(
                metadata, id, 'tickers'
            )
            exchange = fields.get('exchange', 'unknown')
            market_type = fields.get('market_type', 'spot')
            symbol = fields.get('symbol', id)

            if 'ts' not in data.columns:
                data['ts'] = pd.Timestamp.now(tz='UTC')

            if not pd.api.types.is_datetime64_any_dtype(data['ts']):
                data['ts'] = pd.to_datetime(data['ts'], utc=True)

            field_values = {
                'exchange': exchange,
                'market_type': market_type,
                'symbol': symbol
            }
            required_cols = ['exchange', 'market_type', 'symbol']
            for col in required_cols:
                if col not in data.columns:
                    data[col] = field_values[col]

            return data

        except Exception as e:
            logger.error(f"标准化Tickers数据失败: {str(e)}")
            return None

    @staticmethod
    def normalize_futures_metrics(
        data: pd.DataFrame,
        id: str,
        metadata: Optional[Dict[str, Any]]
    ) -> Optional[pd.DataFrame]:
        """标准化期货指标数据

        Args:
            data: 原始数据
            id: 数据标识符
            metadata: 元数据

        Returns:
            Optional[pd.DataFrame]: 标准化后的数据，失败返回None
        """
        try:
            data = DataFrameNormalizer.normalize_columns(
                data, DataFrameNormalizer.FUTURES_METRICS_COLUMN_MAPPING
            )

            fields = DataFrameNormalizer.extract_metadata_fields(
                metadata, id, 'futures_metrics'
            )
            exchange = fields.get('exchange', 'binance_um')
            symbol = fields.get('symbol', id)

            if 'ts' not in data.columns:
                data['ts'] = pd.Timestamp.now(tz='UTC')

            if 'ts' in data.columns:
                data['ts'] = pd.to_datetime(data['ts'], utc=True, errors='coerce')
                if hasattr(data['ts'].dtype, 'tz') and data['ts'].dtype.tz is not None:
                    data['ts'] = data['ts'].dt.tz_convert('UTC').dt.tz_localize(None)
                elif data['ts'].dtype == object:
                    data['ts'] = pd.to_datetime(data['ts'], errors='coerce')

            field_values = {
                'exchange': exchange,
                'symbol': symbol
            }
            required_cols = ['exchange', 'symbol']
            for col in required_cols:
                if col not in data.columns:
                    data[col] = field_values[col]

            optional_cols = [
                'funding_rate', 'open_interest', 'oi_value',
                'taker_long_short_ratio', 'top_long_short_position_ratio',
                'top_long_short_account_ratio', 'global_long_short_account_ratio'
            ]
            for col in optional_cols:
                if col not in data.columns:
                    data[col] = None

            return data

        except Exception as e:
            logger.error(f"标准化期货指标数据失败: {str(e)}")
            return None

    @staticmethod
    def normalize_btc_fgi(
        data: pd.DataFrame,
        id: str,
        metadata: Optional[Dict[str, Any]]
    ) -> Optional[pd.DataFrame]:
        """标准化BTC恐惧贪婪指数数据

        Args:
            data: 原始数据
            id: 数据标识符
            metadata: 元数据

        Returns:
            Optional[pd.DataFrame]: 标准化后的数据，失败返回None
        """
        try:
            data = DataFrameNormalizer.normalize_columns(
                data, DataFrameNormalizer.BTC_FGI_COLUMN_MAPPING
            )

            if 'ts' not in data.columns:
                data['ts'] = pd.Timestamp.now(tz='UTC')

            if not pd.api.types.is_datetime64_any_dtype(data['ts']):
                data['ts'] = pd.to_datetime(data['ts'], utc=True)

            if 'value' not in data.columns:
                data['value'] = 50

            if 'label' not in data.columns:
                fgi_value = data['value'].iloc[0] if len(data) > 0 else 50
                if fgi_value < 25:
                    data['label'] = 'Extreme Fear'
                elif fgi_value < 45:
                    data['label'] = 'Fear'
                elif fgi_value < 55:
                    data['label'] = 'Neutral'
                elif fgi_value < 75:
                    data['label'] = 'Greed'
                else:
                    data['label'] = 'Extreme Greed'

            return data

        except Exception as e:
            logger.error(f"标准化BTC FGI数据失败: {str(e)}")
            return None

    @staticmethod
    def normalize_macro_fred(
        data: pd.DataFrame,
        id: str,
        metadata: Optional[Dict[str, Any]]
    ) -> Optional[pd.DataFrame]:
        """标准化宏观FRED数据"""
        try:
            data = DataFrameNormalizer.normalize_columns(
                data, DataFrameNormalizer.MACRO_FRED_COLUMN_MAPPING
            )

            series_id = metadata.get('symbol', 'UNKNOWN') if metadata else id
            symbol = metadata.get('symbol') if metadata else id

            if 'ts' not in data.columns:
                data['ts'] = pd.Timestamp.now(tz='UTC')

            if not pd.api.types.is_datetime64_any_dtype(data['ts']):
                data['ts'] = pd.to_datetime(data['ts'], utc=True)

            if 'series_id' not in data.columns:
                data['series_id'] = series_id
            if 'symbol' not in data.columns:
                data['symbol'] = symbol

            if 'frequency' not in data.columns:
                data['frequency'] = 'daily'

            return data

        except Exception as e:
            logger.error(f"标准化宏观FRED数据失败: {str(e)}")
            return None

    @staticmethod
    def to_records(data: pd.DataFrame) -> List[Dict[str, Any]]:
        """将DataFrame转换为记录列表

        Args:
            data: DataFrame数据

        Returns:
            List[Dict]: 记录列表
        """
        return data.to_dict('records')

    @staticmethod
    def parse_timeframe_from_id(id: str) -> str:
        """从ID中解析时间周期

        Args:
            id: 数据ID

        Returns:
            str: 时间周期，默认为'1d'
        """
        if '_' in id:
            parts = id.split('_')
            if len(parts) > 1:
                possible_tf = parts[-1]
                if possible_tf in DataFrameNormalizer.TIMEFRAMES:
                    return possible_tf
        return '1d'

    @staticmethod
    def parse_symbol_from_id(id: str) -> str:
        """从ID中解析交易对

        Args:
            id: 数据ID

        Returns:
            str: 交易对符号
        """
        if '_' in id:
            parts = id.split('_')
            if len(parts) > 1:
                tf = parts[-1]
                if tf in DataFrameNormalizer.TIMEFRAMES:
                    return '_'.join(parts[:-1])
            return parts[0]
        return id
