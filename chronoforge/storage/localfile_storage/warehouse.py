"""
本地数据仓库 - 基于文件系统的数据持久化

使用本地文件系统存储金融数据，支持多种文件格式：
- CSV: 便于查看和编辑
- JSON: 结构化数据存储
- Parquet: 列式存储，高效查询
"""

import asyncio
import logging
from pathlib import Path
from typing import Dict, Optional, Any
from datetime import datetime, timezone

import pandas as pd

logger = logging.getLogger(__name__)


class LocalDataWarehouse:
    """基于本地文件系统的金融数据仓库"""

    def __init__(self, data_dir: str = "~/.chronoforge/data",
                 file_format: str = "parquet"):
        """
        初始化本地数据仓库

        Args:
            data_dir: 数据目录路径
            file_format: 默认文件格式 (csv, json, parquet)
        """
        self.data_dir = Path(data_dir).expanduser().absolute()
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.file_format = file_format.lower()

        self._lock = asyncio.Lock()

        self._init_warehouse()

    def _get_data_subdir(self, table_name: str) -> Path:
        """获取数据子目录"""
        subdir = self.data_dir / table_name
        subdir.mkdir(parents=True, exist_ok=True)
        return subdir

    def _get_file_path(self, table_name: str, key: str = "main") -> Path:
        """获取数据文件路径"""
        subdir = self._get_data_subdir(table_name)
        return subdir / f"{key}.{self.file_format}"

    def _init_warehouse(self):
        """初始化仓库结构"""
        tables = ['symbols', 'ohlcv', 'tickers', 'futures_metrics',
                  'macro_fred', 'btc_fgi', 'coin_categories']

        for table in tables:
            self._get_data_subdir(table)

        logger.info("本地数据仓库初始化完成: %s", self.data_dir)

    def _generate_file_key(self, **kwargs) -> str:
        """生成文件键名"""
        sorted_items = sorted(kwargs.items())
        key_str = "_".join(f"{k}_{v}" for k, v in sorted_items if v)
        if not key_str:
            return "main"
        key_str = key_str.replace('/', '_')
        return key_str

    async def insert_symbols(self, data: pd.DataFrame) -> bool:
        """批量插入symbols数据

        Args:
            data: 包含symbol数据的DataFrame
        """
        if data is None or data.empty:
            return True

        try:
            df = data.copy()
            df['updated_at'] = datetime.now(timezone.utc)

            file_path = self._get_file_path('symbols', 'main')
            self._save_dataframe(df, file_path)

            logger.debug("成功保存 %s 条symbols记录到 %s", len(df), file_path)
            return True

        except Exception as e:
            logger.error("保存symbols数据失败: %s", str(e))
            return False

    async def insert_ohlcv(self, data: pd.DataFrame) -> bool:
        """批量插入OHLCV数据

        Args:
            data: 包含OHLCV数据的DataFrame
        """
        if data is None or data.empty:
            return True

        try:
            df = data.copy()

            if 'ts' in df.columns:
                df['ts'] = pd.to_datetime(df['ts'], utc=True)

            exchange = df['exchange'].iloc[0] if 'exchange' in df.columns else 'unknown'
            market_type = df['market_type'].iloc[0] if 'market_type' in df.columns else 'spot'
            symbol = df['symbol'].iloc[0] if 'symbol' in df.columns else 'unknown'
            timeframe = df['timeframe'].iloc[0] if 'timeframe' in df.columns else '1d'

            file_key = self._generate_file_key(
                exchange=exchange,
                market_type=market_type,
                symbol=symbol,
                timeframe=timeframe
            )

            file_path = self._get_file_path('ohlcv', file_key)

            existing_df = self._load_dataframe(file_path)
            if existing_df is not None and not existing_df.empty:
                df = pd.concat([existing_df, df]).drop_duplicates(
                    subset=['exchange', 'market_type', 'symbol', 'timeframe', 'ts']
                ).sort_values('ts', ascending=False)

            self._save_dataframe(df, file_path)

            logger.debug("成功保存 %s 条OHLCV记录到 %s", len(df), file_path)
            return True

        except Exception as e:
            logger.error("保存OHLCV数据失败: %s", str(e))
            return False

    async def insert_tickers(self, data: pd.DataFrame) -> bool:
        """批量插入tickers数据

        Args:
            data: 包含tickers数据的DataFrame
        """
        if data is None or data.empty:
            return True

        try:
            df = data.copy()

            if 'ts' in df.columns:
                df['ts'] = pd.to_datetime(df['ts'], utc=True)

            exchange = df['exchange'].iloc[0] if 'exchange' in df.columns else 'unknown'
            symbol = df['symbol'].iloc[0] if 'symbol' in df.columns else 'unknown'

            file_key = self._generate_file_key(exchange=exchange, symbol=symbol)
            file_path = self._get_file_path('tickers', file_key)

            existing_df = self._load_dataframe(file_path)
            if existing_df is not None and not existing_df.empty:
                df = pd.concat([existing_df, df]).drop_duplicates(
                    subset=['exchange', 'market_type', 'symbol', 'ts']
                ).sort_values('ts', ascending=False)

            self._save_dataframe(df, file_path)

            logger.debug("成功保存 %s 条tickers记录", len(df))
            return True

        except Exception as e:
            logger.error("保存tickers数据失败: %s", str(e))
            return False

    async def insert_futures_metrics(self, data: pd.DataFrame) -> bool:
        """批量插入期货指标数据

        Args:
            data: 包含期货指标数据的DataFrame
        """
        if data is None or data.empty:
            return True

        try:
            df = data.copy()

            if 'ts' in df.columns:
                df['ts'] = pd.to_datetime(df['ts'], utc=True)

            symbol = df['symbol'].iloc[0] if 'symbol' in df.columns else 'unknown'
            exchange = df['exchange'].iloc[0] if 'exchange' in df.columns else 'unknown'

            file_key = self._generate_file_key(exchange=exchange, symbol=symbol)
            file_path = self._get_file_path('futures_metrics', file_key)

            existing_df = self._load_dataframe(file_path)
            if existing_df is not None and not existing_df.empty:
                df = pd.concat([existing_df, df]).drop_duplicates(
                    subset=['exchange', 'symbol', 'ts']
                ).sort_values('ts', ascending=False)

            self._save_dataframe(df, file_path)

            logger.debug("成功保存 %s 条期货指标记录", len(df))
            return True

        except Exception as e:
            logger.error("保存期货指标数据失败: %s", str(e))
            return False

    async def insert_macro_fred(self, data: pd.DataFrame) -> bool:
        """批量插入宏观数据

        Args:
            data: 包含宏观数据的DataFrame
        """
        if data is None or data.empty:
            return True

        try:
            df = data.copy()

            if 'ts' in df.columns:
                df['ts'] = pd.to_datetime(df['ts'], utc=True)

            symbol = df['symbol'].iloc[0] if 'symbol' in df.columns else 'unknown'
            series_id = df['series_id'].iloc[0] if 'series_id' in df.columns else 'unknown'

            file_key = self._generate_file_key(series_id=series_id, symbol=symbol)
            file_path = self._get_file_path('macro_fred', file_key)

            existing_df = self._load_dataframe(file_path)
            if existing_df is not None and not existing_df.empty:
                df = pd.concat([existing_df, df]).drop_duplicates(
                    subset=['series_id', 'symbol', 'ts']
                ).sort_values('ts', ascending=False)

            self._save_dataframe(df, file_path)

            logger.debug("成功保存 %s 条宏观数据记录", len(df))
            return True

        except Exception as e:
            logger.error("保存宏观数据失败: %s", str(e))
            return False

    async def insert_btc_fgi(self, data: pd.DataFrame) -> bool:
        """批量插入BTC恐惧贪婪指数数据

        Args:
            data: 包含BTC FGI数据的DataFrame
        """
        if data is None or data.empty:
            return True

        try:
            df = data.copy()

            if 'ts' in df.columns:
                df['ts'] = pd.to_datetime(df['ts'], utc=True)

            file_path = self._get_file_path('btc_fgi', 'main')

            existing_df = self._load_dataframe(file_path)
            if existing_df is not None and not existing_df.empty:
                df = pd.concat([existing_df, df]).drop_duplicates(
                    subset=['ts']
                ).sort_values('ts', ascending=False)

            self._save_dataframe(df, file_path)

            logger.debug("成功保存 %s 条BTC FGI记录", len(df))
            return True

        except Exception as e:
            logger.error("保存BTC FGI数据失败: %s", str(e))
            return False

    async def insert_coin_categories(self, data: pd.DataFrame) -> bool:
        """批量插入币种分类数据

        Args:
            data: 包含币种分类数据的DataFrame
        """
        if data is None or data.empty:
            return True

        try:
            df = data.copy()
            df['updated_at'] = datetime.now(timezone.utc)

            file_path = self._get_file_path('coin_categories', 'main')

            existing_df = self._load_dataframe(file_path)
            if existing_df is not None and not existing_df.empty:
                df = pd.concat([existing_df, df]).drop_duplicates(
                    subset=['symbol', 'category']
                )

            self._save_dataframe(df, file_path)

            logger.debug("成功保存 %s 条币种分类记录", len(df))
            return True

        except Exception as e:
            logger.error("保存币种分类数据失败: %s", str(e))
            return False

    async def get_ohlcv(self, symbol: str, timeframe: str,
                        start_time: Optional[datetime] = None,
                        end_time: Optional[datetime] = None,
                        exchange: Optional[str] = None,
                        limit: Optional[int] = None) -> Optional[pd.DataFrame]:
        """查询OHLCV数据"""
        try:
            ohlcv_dir = self._get_data_subdir('ohlcv')

            all_data = []
            for file_path in ohlcv_dir.glob(f"*.{self.file_format}"):
                df = self._load_dataframe(file_path)
                if df is not None and not df.empty:
                    all_data.append(df)

            if not all_data:
                return None

            merged_df = pd.concat(all_data, ignore_index=True)

            if exchange:
                merged_df = merged_df[merged_df['exchange'] == exchange]
            merged_df = merged_df[merged_df['symbol'] == symbol]
            merged_df = merged_df[merged_df['timeframe'] == timeframe]

            if start_time:
                merged_df = merged_df[merged_df['ts'] >= start_time]
            if end_time:
                merged_df = merged_df[merged_df['ts'] <= end_time]

            merged_df = merged_df.sort_values('ts', ascending=False)

            if limit:
                merged_df = merged_df.head(limit)

            if merged_df.empty:
                return None

            return merged_df

        except Exception as e:
            logger.error("查询OHLCV数据失败: %s", str(e))
            return None

    async def get_symbols(self, exchange: Optional[str] = None,
                          market_type: Optional[str] = None,
                          active: Optional[bool] = None) -> Optional[pd.DataFrame]:
        """查询symbols数据"""
        try:
            file_path = self._get_file_path('symbols', 'main')

            if not file_path.exists():
                return None

            df = self._load_dataframe(file_path)

            if df is None or df.empty:
                return None

            if exchange:
                df = df[df['exchange'] == exchange]
            if market_type:
                df = df[df['market_type'] == market_type]
            if active is not None:
                df = df[df['active'] == active]

            return df

        except Exception as e:
            logger.error("查询symbols数据失败: %s", str(e))
            return None

    def _save_dataframe(self, df: pd.DataFrame, file_path: Path):
        """保存DataFrame到文件"""
        file_path.parent.mkdir(parents=True, exist_ok=True)

        if self.file_format == 'csv':
            df.to_csv(file_path, index=False)
        elif self.file_format == 'json':
            df.to_json(file_path, orient='records', date_format='iso')
        elif self.file_format == 'parquet':
            df.to_parquet(file_path, index=False)
        else:
            raise ValueError(f"不支持的文件格式: {self.file_format}")

    def _load_dataframe(self, file_path: Path) -> Optional[pd.DataFrame]:
        """从文件加载DataFrame"""
        if not file_path.exists():
            return None

        try:
            if self.file_format == 'csv':
                return pd.read_csv(file_path)
            elif self.file_format == 'json':
                return pd.read_json(file_path, orient='records')
            elif self.file_format == 'parquet':
                return pd.read_parquet(file_path)
            else:
                raise ValueError(f"不支持的文件格式: {self.file_format}")
        except Exception as e:
            logger.warning("加载文件失败 %s: %s", file_path, str(e))
            return None

    async def get_stats(self) -> Dict[str, Any]:
        """获取数据仓库统计信息"""
        try:
            stats = {}

            tables = ['symbols', 'ohlcv', 'tickers', 'futures_metrics',
                      'macro_fred', 'btc_fgi', 'coin_categories']

            total_files = 0
            total_size = 0

            for table in tables:
                table_dir = self._get_data_subdir(table)
                files = list(table_dir.glob(f"*.{self.file_format}"))
                file_count = len(files)

                table_size = sum(f.stat().st_size for f in files if f.exists())

                stats[f'{table}_files'] = file_count
                stats[f'{table}_size_bytes'] = table_size

                total_files += file_count
                total_size += table_size

            stats['total_files'] = total_files
            stats['total_size_bytes'] = total_size
            stats['data_dir'] = str(self.data_dir)
            stats['file_format'] = self.file_format

            return stats

        except Exception as e:
            logger.error("获取统计信息失败: %s", str(e))
            return {'error': str(e)}

    async def check_data_integrity(self) -> Dict[str, Any]:
        """检查数据完整性"""
        try:
            results = {}

            ohlcv_dir = self._get_data_subdir('ohlcv')
            ohlcv_files = list(ohlcv_dir.glob(f"*.{self.file_format}"))

            for file_path in ohlcv_files:
                df = self._load_dataframe(file_path)
                if df is not None and not df.empty:
                    required_cols = ['ts', 'open', 'high', 'low', 'close', 'volume']
                    missing_cols = [col for col in required_cols if col not in df.columns]
                    if missing_cols:
                        results[f'{file_path.name}_missing_cols'] = missing_cols

            has_issues = any('missing_cols' in str(k) for k in results.keys())
            results['status'] = 'healthy' if not has_issues else 'issues_found'

            return results

        except Exception as e:
            logger.error("数据完整性检查失败: %s", str(e))
            return {'error': str(e)}

    async def close(self):
        """关闭仓库连接"""
        logger.debug("关闭本地数据仓库")

    async def __aenter__(self):
        """异步上下文管理器进入"""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器退出"""
        await self.close()
        return False
