"""
DuckDB金融数据仓库 - 统一时间轴、跨市场、跨资产、跨数据源的研究型数据存储

核心设计原则：
1. 统一Symbol语义
2. 统一时间语义（UTC）
3. 统一表类型（事实表/维度表）
4. 按「数据性质」建表，不按「数据来源」，仅在数据结构不同时"分表"
"""

import logging
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)


class FinancialDataWarehouse:
    """基于DuckDB的统一金融数据仓库"""

    def __init__(self, db_path: str,
                 read_only: bool = False,
                 max_connections: int = 10,
                 config: dict = None):
        """
        初始化金融数据仓库

        Args:
            db_path: 数据库文件路径
            read_only: 是否只读模式
            max_connections: 最大连接数
            config: 额外的配置字典
                - threads: 并行线程数 (默认: 4)
                - memory_limit: 内存限制 (默认: '10GB')
        """
        self.db_path = Path(db_path).absolute()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.read_only = read_only
        self.max_connections = max_connections

        config = config or {}
        self.threads = config.get('threads', 4)
        self.memory_limit = config.get('memory_limit', '10GB')

        self._connection = duckdb.connect(str(self.db_path), read_only=self.read_only)
        self._connection.execute(f"PRAGMA threads={self.threads}")
        self._connection.execute(f"PRAGMA memory_limit='{self.memory_limit}'")

        self._init_database()

    def _init_database(self):
        """初始化数据库表结构"""
        if self.read_only:
            logger.info("只读模式：跳过数据库表创建")
            return

        self._create_symbols_table()
        self._create_ohlcv_table()
        self._create_tickers_table()
        self._create_futures_metrics_table()
        self._create_macro_fred_table()
        self._create_btc_fgi_table()
        self._create_coin_categories_table()
        self._create_coin_markets_table()
        self._create_indexes()
        logger.info("金融数据仓库初始化完成")

    def _create_symbols_table(self):
        """创建symbols维度表"""
        self._connection.execute("""
            CREATE TABLE IF NOT EXISTS symbols (
                symbol        TEXT PRIMARY KEY,
                exchange      TEXT NOT NULL,
                market_type   TEXT NOT NULL,
                base_asset    TEXT NOT NULL,
                quote_asset   TEXT,
                active        BOOLEAN DEFAULT TRUE,
                created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self._connection.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_symbols_unique
            ON symbols (exchange, symbol, market_type)
        """)

    def _create_ohlcv_table(self):
        """创建OHLCV事实表"""
        self._connection.execute("""
            CREATE TABLE IF NOT EXISTS ohlcv (
                exchange      TEXT NOT NULL,
                market_type   TEXT NOT NULL,
                symbol        TEXT NOT NULL,
                timeframe     TEXT NOT NULL,
                ts            TIMESTAMP NOT NULL,
                open          DOUBLE NOT NULL,
                high          DOUBLE NOT NULL,
                low           DOUBLE NOT NULL,
                close         DOUBLE NOT NULL,
                volume        DOUBLE NOT NULL,
                quote_volume  DOUBLE,
                source        TEXT,
                created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (exchange, market_type, symbol, timeframe, ts)
            )
        """)

    def _create_tickers_table(self):
        """创建tickers事实表"""
        self._connection.execute("""
            CREATE TABLE IF NOT EXISTS tickers (
                exchange      TEXT NOT NULL,
                symbol        TEXT NOT NULL,
                quote_asset   TEXT,
                ts            TIMESTAMP NOT NULL,
                price         DOUBLE,
                price_change_24h DOUBLE,
                price_change_pct_24h DOUBLE,
                volume        DOUBLE,
                quote_volume  DOUBLE,
                high_24h      DOUBLE,
                low_24h       DOUBLE,
                market_type   TEXT DEFAULT 'spot',
                created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (exchange, symbol, quote_asset, ts)
            )
        """)

    def _create_futures_metrics_table(self):
        """创建futures_metrics事实表"""
        self._connection.execute("""
            CREATE TABLE IF NOT EXISTS futures_metrics (
                exchange      TEXT NOT NULL,
                symbol        TEXT NOT NULL,
                ts            TIMESTAMP NOT NULL,
                funding_rate  DOUBLE,
                open_interest DOUBLE,
                oi_value      DOUBLE,
                taker_long_short_ratio DOUBLE,
                top_long_short_position_ratio DOUBLE,
                top_long_short_account_ratio DOUBLE,
                global_long_short_account_ratio DOUBLE,
                created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (exchange, symbol, ts)
            )
        """)

    def _create_macro_fred_table(self):
        """创建macro_fred事实表"""
        self._connection.execute("""
            CREATE TABLE IF NOT EXISTS macro_fred (
                series_id     TEXT NOT NULL,
                symbol        TEXT NOT NULL,
                ts            TIMESTAMP NOT NULL,
                value         DOUBLE,
                frequency     TEXT,
                created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (series_id, ts)
            )
        """)

    def _create_btc_fgi_table(self):
        """创建btc_fgi事实表"""
        self._connection.execute("""
            CREATE TABLE IF NOT EXISTS btc_fgi (
                ts            TIMESTAMP NOT NULL,
                value         INTEGER,
                label         TEXT,
                created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ts)
            )
        """)

    def _create_coin_categories_table(self):
        """创建coin_categories维度表"""
        self._connection.execute("""
            CREATE TABLE IF NOT EXISTS coin_categories (
                symbol          TEXT NOT NULL,
                category        TEXT NOT NULL,
                market_cap_rank INTEGER,
                updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (symbol, category)
            )
        """)

    def _create_coin_markets_table(self):
        """创建coin_markets事实表"""
        self._connection.execute("""
            CREATE TABLE IF NOT EXISTS coin_markets (
                id                  TEXT NOT NULL,
                symbol              TEXT NOT NULL,
                name                TEXT NOT NULL,
                ts                  TIMESTAMP NOT NULL,
                current_price       DOUBLE,
                market_cap          DOUBLE,
                market_cap_rank     INTEGER,
                total_volume        DOUBLE,
                high_24h            DOUBLE,
                low_24h             DOUBLE,
                price_change_24h    DOUBLE,
                price_change_pct_24h DOUBLE,
                market_cap_change_24h DOUBLE,
                circulating_supply  DOUBLE,
                total_supply        DOUBLE,
                max_supply          DOUBLE,
                image_id            TEXT,
                created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id, ts)
            )
        """)

    def _create_indexes(self):
        """创建索引优化查询性能"""
        try:
            self._connection.execute("""
                CREATE INDEX IF NOT EXISTS idx_ohlcv_symbol
                ON ohlcv (symbol, exchange, timeframe)
            """)
            self._connection.execute("""
                CREATE INDEX IF NOT EXISTS idx_ohlcv_ts
                ON ohlcv (ts DESC)
            """)
            self._connection.execute("""
                CREATE INDEX IF NOT EXISTS idx_tickers_symbol
                ON tickers (symbol, exchange)
            """)
            self._connection.execute("""
                CREATE INDEX IF NOT EXISTS idx_tickers_ts
                ON tickers (ts DESC)
            """)
            self._connection.execute("""
                CREATE INDEX IF NOT EXISTS idx_futures_symbol
                ON futures_metrics (symbol, exchange)
            """)
            self._connection.execute("""
                CREATE INDEX IF NOT EXISTS idx_futures_ts
                ON futures_metrics (ts DESC)
            """)
            self._connection.execute("""
                CREATE INDEX IF NOT EXISTS idx_macro_ts
                ON macro_fred (ts DESC)
            """)
            self._connection.execute("""
                CREATE INDEX IF NOT EXISTS idx_coin_markets_ts
                ON coin_markets (ts DESC, market_cap_rank)
            """)
            logger.info("索引创建完成")
        except Exception as e:
            logger.warning("创建索引失败: %s", str(e))

    def _get_connection(self):
        """获取数据库连接"""
        if self._connection is None:
            self._connection = duckdb.connect(str(self.db_path), read_only=False)
            self._init_database()
        return self._connection

    def _register_df(self, df: pd.DataFrame) -> str:
        """注册DataFrame并返回表名"""
        df_name = f"_df_{id(df)}"
        self._connection.register(df_name, df)
        return df_name

    def _unregister_df(self, df_name: str):
        """注销DataFrame"""
        try:
            self._connection.unregister(df_name)
        except Exception:
            pass

    async def insert_symbols(self, data: pd.DataFrame) -> bool:
        """批量插入交易对数据

        Args:
            data: 包含symbol数据的DataFrame
        """
        if data is None or data.empty:
            return True
        try:
            df = data.copy()
            for col in ['created_at', 'updated_at']:
                if col not in df.columns:
                    df[col] = pd.Timestamp.now(tz='UTC')
            df_name = self._register_df(df)
            self._connection.execute(
                f"INSERT INTO symbols BY NAME SELECT * FROM {df_name} ON CONFLICT DO NOTHING"
            )
            self._unregister_df(df_name)
            logger.debug("成功插入 %s 条symbol记录", len(df))
            return True
        except Exception as e:
            logger.error("插入symbol数据失败: %s", str(e))
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
                df['ts'] = pd.to_datetime(df['ts'], utc=True, errors='coerce')
                if hasattr(df['ts'].dtype, 'tz') and df['ts'].dtype.tz is not None:
                    df['ts'] = df['ts'].dt.tz_localize(None)
                df['ts'] = df['ts'].astype('datetime64[ns]')

            required_cols = ['exchange', 'market_type', 'symbol', 'timeframe', 'ts',
                             'open', 'high', 'low', 'close', 'volume']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                logger.error("插入OHLCV数据失败: 缺少必需列 %s", missing_cols)
                return False

            df_with_nulls = df[df[required_cols].isna().any(axis=1)]
            if not df_with_nulls.empty:
                logger.warning("OHLCV数据包含空值，已过滤 %d 条记录", len(df_with_nulls))
                df = df.dropna(subset=required_cols)

            if df.empty:
                logger.warning("过滤空值后数据为空，跳过插入")
                return True

            # 批量验证数据完整性
            invalid_conditions = {
                'high < low': df['high'] < df['low'],
                'open < low': df['open'] < df['low'],
                'open > high': df['open'] > df['high'],
                'close < low': df['close'] < df['low'],
                'close > high': df['close'] > df['high'],
            }
            invalid_mask = pd.Series(False, index=df.index)
            for cond_name, cond_mask in invalid_conditions.items():
                invalid_mask = invalid_mask | cond_mask

            if invalid_mask.any():
                invalid_rows = df[invalid_mask].copy()
                error_details = []
                for idx, row in invalid_rows.iterrows():
                    failed_conds = []
                    cond_list = list(invalid_conditions.items())
                    for cond_name, cond_mask in cond_list:
                        if cond_mask.loc[idx]:
                            failed_conds.append(cond_name)
                    error_details.append(
                        f"symbol={row.get('symbol', 'N/A')}, ts={row.get('ts', 'N/A')}, "
                        f"O={row.get('open', 0):.4f}, H={row.get('high', 0):.4f}, "
                        f"L={row.get('low', 0):.4f}, C={row.get('close', 0):.4f}, "
                        f"失败条件: {failed_conds}"
                    )
                logger.error("插入OHLCV数据失败: %d 条记录价格数据不合法", len(invalid_rows))
                for detail in error_details[:10]:
                    logger.error("  - %s", detail)
                if len(error_details) > 10:
                    logger.error("  ... 还有 %d 条记录", len(error_details) - 10)
                return False

            expected_cols = [
                'exchange', 'market_type', 'symbol', 'timeframe', 'ts',
                'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'source'
            ]
            for col in expected_cols:
                if col not in df.columns:
                    df[col] = None
            df = df[expected_cols]

            # 使用 DuckDB 的批量插入功能 - 通过 FROM 子句直接插入 DataFrame
            cols_str = ', '.join(expected_cols)
            self._connection.execute(
                f"INSERT OR REPLACE INTO ohlcv ({cols_str}) SELECT {cols_str} FROM df"
            )

            logger.debug("成功插入 %s 条OHLCV记录", len(df))
            return True
        except Exception as e:
            logger.error("插入OHLCV数据失败: %s", str(e))
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
                df['ts'] = pd.to_datetime(df['ts'], utc=True, errors='coerce')
                if hasattr(df['ts'].dtype, 'tz') and df['ts'].dtype.tz is not None:
                    df['ts'] = df['ts'].dt.tz_localize(None)
                df['ts'] = df['ts'].astype('datetime64[ns]')

            expected_cols = [
                'exchange', 'symbol', 'quote_asset', 'ts', 'price',
                'price_change_24h', 'price_change_pct_24h', 'volume', 'quote_volume',
                'high_24h', 'low_24h', 'market_type'
            ]
            for col in expected_cols:
                if col not in df.columns:
                    if col == 'quote_asset':
                        df[col] = 'USDT'
                    else:
                        df[col] = None
            df = df[expected_cols]

            for _, row in df.iterrows():
                values = [None if pd.isna(v) else v for v in row.values]
                placeholders = ', '.join(['?' for _ in expected_cols])
                cols_str = ', '.join(expected_cols)
                sql = f"INSERT OR REPLACE INTO tickers ({cols_str}) VALUES ({placeholders})"
                self._connection.execute(sql, values)

            logger.debug("成功插入 %s 条tickers记录", len(df))
            return True
        except Exception as e:
            logger.error("插入tickers数据失败: %s", str(e))
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
                df['ts'] = pd.to_datetime(df['ts'], utc=True, errors='coerce')
                if hasattr(df['ts'].dtype, 'tz') and df['ts'].dtype.tz is not None:
                    df['ts'] = df['ts'].dt.tz_localize(None)
                df['ts'] = df['ts'].astype('datetime64[ns]')

            expected_cols = [
                'exchange', 'symbol', 'ts', 'funding_rate', 'open_interest', 'oi_value',
                'taker_long_short_ratio', 'top_long_short_position_ratio',
                'top_long_short_account_ratio', 'global_long_short_account_ratio'
            ]
            for col in expected_cols:
                if col not in df.columns:
                    df[col] = None
            df = df[expected_cols]

            for _, row in df.iterrows():
                values = [None if pd.isna(v) else v for v in row.values]
                placeholders = ', '.join(['?' for _ in expected_cols])
                cols_str = ', '.join(expected_cols)
                self._connection.execute(
                    f"INSERT OR REPLACE INTO futures_metrics ({cols_str}) VALUES ({placeholders})",
                    values
                )

            logger.debug("成功插入 %s 条futures metrics记录", len(df))
            return True
        except Exception as e:
            logger.error("插入futures metrics数据失败: %s", str(e))
            return False

    async def insert_macro_fred(self, data: pd.DataFrame) -> bool:
        """批量插入FRED宏观数据

        Args:
            data: 包含FRED宏观数据的DataFrame
        """
        if data is None or data.empty:
            return True
        try:
            df = data.copy()
            if 'ts' in df.columns:
                df['ts'] = pd.to_datetime(df['ts'], utc=True)

            expected_cols = ['series_id', 'symbol', 'ts', 'value', 'frequency']
            for col in expected_cols:
                if col not in df.columns:
                    df[col] = None
            df = df[expected_cols]

            for _, row in df.iterrows():
                values = [None if pd.isna(v) else v for v in row.values]
                placeholders = ', '.join(['?' for _ in expected_cols])
                cols_str = ', '.join(expected_cols)
                sql = f"INSERT OR REPLACE INTO macro_fred ({cols_str}) VALUES ({placeholders})"
                self._connection.execute(sql, values)

            logger.debug("成功插入 %s 条macro FRED记录", len(df))
            return True
        except Exception as e:
            logger.error("插入macro FRED数据失败: %s", str(e))
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

            expected_cols = ['ts', 'value', 'label']
            for col in expected_cols:
                if col not in df.columns:
                    df[col] = None
            df = df[expected_cols]

            for _, row in df.iterrows():
                values = [None if pd.isna(v) else v for v in row.values]
                placeholders = ', '.join(['?' for _ in expected_cols])
                cols_str = ', '.join(expected_cols)
                sql = f"INSERT OR REPLACE INTO btc_fgi ({cols_str}) VALUES ({placeholders})"
                self._connection.execute(sql, values)

            logger.debug("成功插入 %s 条BTC FGI记录", len(df))
            return True
        except Exception as e:
            logger.error("插入BTC FGI数据失败: %s", str(e))
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
            if 'updated_at' not in df.columns:
                df['updated_at'] = pd.Timestamp.now(tz='UTC')
            df_name = self._register_df(df)

            expected_cols = ['symbol', 'category', 'market_cap_rank', 'updated_at']
            cols = [c for c in expected_cols if c in df.columns]
            self._connection.execute(
                f"INSERT OR REPLACE INTO coin_categories ({', '.join(cols)}) "
                f"SELECT {', '.join(cols)} FROM {df_name}"
            )
            self._unregister_df(df_name)
            logger.debug("成功插入 %s 条coin categories记录", len(df))
            return True
        except Exception as e:
            logger.error("插入coin categories数据失败: %s", str(e))
            return False

    async def insert_coin_markets(self, data: pd.DataFrame) -> bool:
        """批量插入CoinGecko市场数据

        Args:
            data: 包含CoinGecko市场数据的DataFrame
        """
        if data is None or data.empty:
            return True
        try:
            df = data.copy()
            if 'ts' not in df.columns:
                df['ts'] = pd.Timestamp.now(tz='UTC')
            df['ts'] = pd.to_datetime(df['ts'], utc=True, errors='coerce')
            if hasattr(df['ts'].dtype, 'tz') and df['ts'].dtype.tz is not None:
                df['ts'] = df['ts'].dt.tz_localize(None)
            df['ts'] = df['ts'].astype('datetime64[ns]')
            if 'created_at' not in df.columns:
                df['created_at'] = pd.Timestamp.now(tz='UTC')
                if hasattr(df['created_at'].dtype, 'tz') and df['created_at'].dtype.tz is not None:
                    df['created_at'] = df['created_at'].dt.tz_localize(None)
                df['created_at'] = df['created_at'].astype('datetime64[ns]')

            expected_cols = [
                'id', 'symbol', 'name', 'ts', 'current_price', 'market_cap',
                'market_cap_rank', 'total_volume', 'high_24h', 'low_24h',
                'price_change_24h', 'price_change_pct_24h', 'market_cap_change_24h',
                'circulating_supply', 'total_supply', 'max_supply', 'image_id', 'created_at'
            ]
            for col in expected_cols:
                if col not in df.columns:
                    df[col] = None
            df = df[expected_cols]
            df_name = self._register_df(df)
            self._connection.execute(f"""
                INSERT OR REPLACE INTO coin_markets
                SELECT id, symbol, name, ts, current_price, market_cap, market_cap_rank,
                       total_volume, high_24h, low_24h, price_change_24h,
                       price_change_pct_24h, market_cap_change_24h,
                       circulating_supply, total_supply, max_supply, image_id, created_at
                FROM {df_name}
            """)
            self._unregister_df(df_name)
            logger.debug("成功插入 %s 条coin markets记录", len(df))
            return True
        except Exception as e:
            logger.error("插入coin markets数据失败: %s", str(e))
            return False

    async def get_ohlcv(self, symbol: str = None, timeframe: str = '1d',
                        exchange: str = None, market_type: str = None,
                        start_time: datetime = None, end_time: datetime = None,
                        limit: int = None) -> Optional[pd.DataFrame]:
        """查询OHLCV数据"""
        try:
            conn = self._get_connection()
            query = "SELECT * FROM ohlcv WHERE 1=1"
            params = []

            if symbol:
                query += " AND symbol = ?"
                params.append(symbol)
            if timeframe:
                query += " AND timeframe = ?"
                params.append(timeframe)
            if exchange:
                query += " AND exchange = ?"
                params.append(exchange)
            if market_type:
                query += " AND market_type = ?"
                params.append(market_type)
            if start_time:
                if start_time.tzinfo is not None:
                    start_time = start_time.replace(tzinfo=None)
                query += " AND ts >= ?"
                params.append(start_time)
            if end_time:
                if end_time.tzinfo is not None:
                    end_time = end_time.replace(tzinfo=None)
                query += " AND ts <= ?"
                params.append(end_time)

            query += " ORDER BY ts ASC"

            if limit:
                query += f" LIMIT {limit}"

            result = conn.execute(query, params).fetchdf()
            return result if not result.empty else None
        except Exception as e:
            logger.error("查询OHLCV数据失败: %s", str(e))
            return None

    async def get_symbols(self, exchange: str = None, market_type: str = None,
                          active: bool = True) -> Optional[pd.DataFrame]:
        """查询symbols数据"""
        try:
            conn = self._get_connection()
            query = "SELECT * FROM symbols WHERE 1=1"
            params = []

            if exchange:
                query += " AND exchange = ?"
                params.append(exchange)
            if market_type:
                query += " AND market_type = ?"
                params.append(market_type)
            if active is not None:
                query += " AND active = ?"
                params.append(active)

            result = conn.execute(query, params).fetchdf()
            return result if not result.empty else None
        except Exception as e:
            logger.error("查询symbols数据失败: %s", str(e))
            return None

    async def get_futures_metrics(self, symbol: str = None,
                                  exchange: str = None,
                                  start_time: datetime = None,
                                  end_time: datetime = None,
                                  limit: int = None) -> Optional[pd.DataFrame]:
        """查询期货指标数据"""
        try:
            conn = self._get_connection()
            query = "SELECT * FROM futures_metrics WHERE 1=1"
            params = []

            if symbol:
                query += " AND symbol = ?"
                params.append(symbol)
            if exchange:
                query += " AND exchange = ?"
                params.append(exchange)
            if start_time:
                if start_time.tzinfo is not None:
                    start_time = start_time.replace(tzinfo=None)
                query += " AND ts >= ?"
                params.append(start_time)
            if end_time:
                if end_time.tzinfo is not None:
                    end_time = end_time.replace(tzinfo=None)
                query += " AND ts <= ?"
                params.append(end_time)

            query += " ORDER BY ts ASC"

            if limit:
                query += f" LIMIT {limit}"

            result = conn.execute(query, params).fetchdf()
            return result if not result.empty else None
        except Exception as e:
            logger.error("查询futures_metrics数据失败: %s", str(e))
            return None

    async def get_macro_fred(self, series_id: str = None,
                             symbol: str = None,
                             start_time: datetime = None,
                             end_time: datetime = None,
                             limit: int = None) -> Optional[pd.DataFrame]:
        """查询FRED宏观数据"""
        try:
            conn = self._get_connection()
            query = "SELECT * FROM macro_fred WHERE 1=1"
            params = []

            if series_id:
                query += " AND series_id = ?"
                params.append(series_id)
            if symbol:
                query += " AND symbol = ?"
                params.append(symbol)
            if start_time:
                if start_time.tzinfo is not None:
                    start_time = start_time.replace(tzinfo=None)
                query += " AND ts >= ?"
                params.append(start_time)
            if end_time:
                if end_time.tzinfo is not None:
                    end_time = end_time.replace(tzinfo=None)
                query += " AND ts <= ?"
                params.append(end_time)

            query += " ORDER BY ts ASC"

            if limit:
                query += f" LIMIT {limit}"

            result = conn.execute(query, params).fetchdf()
            return result if not result.empty else None
        except Exception as e:
            logger.error("查询macro_fred数据失败: %s", str(e))
            return None

    async def get_coin_markets(self, limit: int = None) -> Optional[pd.DataFrame]:
        """查询CoinGecko市场数据"""
        try:
            conn = self._get_connection()
            query = "SELECT * FROM coin_markets ORDER BY ts DESC"

            if limit:
                query += f" LIMIT {limit}"

            result = conn.execute(query).fetchdf()
            return result if not result.empty else None
        except Exception as e:
            logger.error("查询coin_markets数据失败: %s", str(e))
            return None

    async def __aenter__(self):
        """异步上下文管理器入口"""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器退出"""
        await self.close()

    async def close(self):
        """关闭数据库连接"""
        if self._connection:
            self._connection.close()
            self._connection = None
        logger.info("数据库连接已关闭")

    async def get_stats(self) -> Dict[str, Any]:
        """获取数据库统计信息"""
        try:
            stats = {}

            ohlcv_count = self._connection.execute(
                "SELECT COUNT(*) FROM ohlcv"
            ).fetchone()[0]
            stats['ohlcv_count'] = ohlcv_count

            symbols_count = self._connection.execute(
                "SELECT COUNT(*) FROM symbols"
            ).fetchone()[0]
            stats['symbols_count'] = symbols_count

            tickers_count = self._connection.execute(
                "SELECT COUNT(*) FROM tickers"
            ).fetchone()[0]
            stats['tickers_count'] = tickers_count

            try:
                futures_count = self._connection.execute(
                    "SELECT COUNT(*) FROM futures_metrics"
                ).fetchone()[0]
                stats['futures_metrics_count'] = futures_count
            except Exception:
                pass

            try:
                fred_count = self._connection.execute(
                    "SELECT COUNT(*) FROM macro_fred"
                ).fetchone()[0]
                stats['macro_fred_count'] = fred_count
            except Exception:
                pass

            try:
                fgi_count = self._connection.execute(
                    "SELECT COUNT(*) FROM btc_fgi"
                ).fetchone()[0]
                stats['btc_fgi_count'] = fgi_count
            except Exception:
                pass

            try:
                categories_count = self._connection.execute(
                    "SELECT COUNT(*) FROM coin_categories"
                ).fetchone()[0]
                stats['coin_categories_count'] = categories_count
            except Exception:
                pass

            try:
                markets_count = self._connection.execute(
                    "SELECT COUNT(*) FROM coin_markets"
                ).fetchone()[0]
                stats['coin_markets_count'] = markets_count
            except Exception:
                pass

            try:
                time_range = self._connection.execute("""
                    SELECT MIN(ts) as start_time, MAX(ts) as end_time
                    FROM ohlcv
                """).fetchone()
                if time_range[0] and time_range[1]:
                    stats['ohlcv_time_range'] = {
                        'start': str(time_range[0]),
                        'end': str(time_range[1])
                    }
            except Exception:
                pass

            try:
                exchange_dist = self._connection.execute("""
                    SELECT exchange, COUNT(DISTINCT symbol) as count
                    FROM symbols
                    WHERE active = true
                    GROUP BY exchange
                """).fetchall()
                if exchange_dist:
                    stats['active_symbols_by_exchange'] = {
                        row[0]: row[1] for row in exchange_dist
                    }
            except Exception:
                pass

            return stats
        except Exception as e:
            logger.error("获取统计信息失败: %s", str(e))
            return {}

    async def check_data_integrity(self) -> Dict[str, Any]:
        """检查数据完整性"""
        try:
            result = {
                'status': 'ok',
                'checks': []
            }

            ohlcv_count = self._connection.execute(
                "SELECT COUNT(*) FROM ohlcv"
            ).fetchone()[0]
            result['checks'].append({
                'table': 'ohlcv',
                'record_count': ohlcv_count
            })

            try:
                symbols_count = self._connection.execute(
                    "SELECT COUNT(*) FROM symbols"
                ).fetchone()[0]
                orphan_symbols = self._connection.execute("""
                    SELECT DISTINCT o.symbol
                    FROM ohlcv o
                    LEFT JOIN symbols s
                    ON o.exchange = s.exchange
                    AND o.symbol = s.symbol
                    AND o.market_type = s.market_type
                    WHERE s.symbol IS NULL
                """).fetchall()
                result['checks'].append({
                    'table': 'symbols',
                    'record_count': symbols_count
                })
                if orphan_symbols:
                    result['orphan_ohlcv_symbols'] = [row[0] for row in orphan_symbols]
                else:
                    result['orphan_ohlcv_symbols'] = []
            except Exception as e:
                logger.warning("检查symbols完整性失败: %s", str(e))

            try:
                dup_count = self._connection.execute("""
                    SELECT COUNT(*) FROM (
                        SELECT exchange, market_type, symbol, timeframe, ts,
                               COUNT(*) as cnt
                        FROM ohlcv
                        GROUP BY exchange, market_type, symbol, timeframe, ts
                        HAVING cnt > 1
                    )
                """).fetchone()[0]
                result['duplicate_ohlcv_records'] = dup_count
            except Exception:
                pass

            try:
                gaps = self._connection.execute("""
                    WITH grouped AS (
                        SELECT exchange, market_type, symbol, timeframe,
                               ts,
                               LAG(ts) OVER (
                                   PARTITION BY exchange, market_type, symbol, timeframe
                                   ORDER BY ts
                               ) as prev_ts
                        FROM ohlcv
                    )
                    SELECT COUNT(*) FROM grouped
                    WHERE prev_ts IS NOT NULL
                    AND (ts - prev_ts) > INTERVAL '1 day'
                """).fetchone()[0]
                result['time_gaps_check'] = gaps
            except Exception:
                pass

            return result
        except Exception as e:
            logger.error("检查数据完整性失败: %s", str(e))
            return {'status': 'error', 'error': str(e)}
