"""
DuckDB统一存储适配器 - 桥接新架构与现有接口

这个适配器类提供了与现有StorageBase接口的兼容性，
同时内部使用全新的FinancialDataWarehouse实现。

使用示例:
    from chronoforge.storage.duckdb_storage import DUCKDBStorage

    # 创建存储实例
    storage = DUCKDBStorage({"db_path": "./data.duckdb"})

    # 保存OHLCV数据
    ohlcv_data = pd.DataFrame({
        'ts': [pd.Timestamp('2024-01-01')],
        'open': [42000], 'high': [43000], 'low': [41500], 'close': [42500],
        'volume': [1000]
    })
    await storage._save("BTC_USDT", ohlcv_data, metadata={
        'exchange': 'binance', 'market_type': 'spot', 'symbol': 'BTC/USDT',
        'timeframe': '1d'
    })

    # 加载数据
    data = await storage._load("BTC/USDT", metadata={'timeframe': '1d'})

    # 获取统计信息
    stats = await storage.get_stats()
"""

import logging
from typing import Dict, List, Optional, Any
import asyncio
from pathlib import Path

import pandas as pd

from chronoforge.storage.base import DataFrameStorageBase
from chronoforge.storage.normalizer import DataFrameNormalizer
from .warehouse import FinancialDataWarehouse

logger = logging.getLogger(__name__)


class DUCKDBStorage(DataFrameStorageBase):
    """统一的DuckDB存储适配器 - 兼容现有接口的新架构实现

    继承关系:
        DUCKDBStorage → DataFrameStorageBase → StorageBase[pd.DataFrame]

    接口说明:
        - 公共方法 (继承自基类): save(), load(), delete(), exists(), lists(),
          get_time_range(), get_metadata(), update_metadata()
          (这些方法包含缓存逻辑，自动调用对应的 _xxx 方法)

        - 抽象方法 (需要实现): _save(), _load(), _delete(), _exists(),
          _lists(), _get_time_range(), _get_metadata(), _update_metadata()

        - 特有方法: health_check(), get_stats(), check_data_integrity()

    数据类型支持:
        - OHLCV K线数据
        - 实时行情数据 (Tickers)
        - 期货指标数据 (Funding Rate, Open Interest等)
        - BTC恐惧贪婪指数
        - 宏观经济数据
        - 币种分类信息
    """

    def __init__(self, config: Dict[str, Any] = None):
        """
        初始化统一DuckDB存储

        Args:
            config: 配置字典，包含数据库路径等参数

                - "db_path": 数据库文件路径 (默认: "~/.chronoforge/duckdb.db")
                - "read_only": 是否只读模式 (默认: False)
                - "threads": 并行线程数 (默认: 4)
                - "memory_limit": 内存限制 (默认: '2GB')
        """
        super().__init__(config)

        default_db_path = "~/.chronoforge/duckdb.db"
        self.db_path = config.get("db_path", default_db_path) if config else default_db_path
        self.db_path = str(Path(self.db_path).expanduser().absolute())
        self.read_only = config.get("read_only", False) if config else False

        self.warehouse_config = {
            'threads': config.get('threads', 4) if config else 4,
            'memory_limit': config.get('memory_limit', '10GB') if config else '10GB'
        }

        self._warehouse: Optional[FinancialDataWarehouse] = None
        self._warehouse_lock = asyncio.Lock()

    @property
    def name(self) -> str:
        """返回存储插件名称"""
        return "DuckDB"

    @property
    def warehouse(self) -> Optional[FinancialDataWarehouse]:
        """同步获取数据仓库实例（非异步方法使用）"""
        return self._warehouse

    async def _get_warehouse(self) -> FinancialDataWarehouse:
        """获取数据仓库实例（线程安全，懒加载）"""
        if self._warehouse is None:
            async with self._warehouse_lock:
                if self._warehouse is None:
                    self._warehouse = FinancialDataWarehouse(
                        db_path=self.db_path,
                        read_only=self.read_only,
                        config=self.warehouse_config
                    )
                    logger.info(f"DuckDB仓库初始化完成: {self.db_path}")
        return self._warehouse

    async def initialize(self):
        """显式初始化仓库（可选，用于预加载）"""
        await self._get_warehouse()
        logger.debug("DUCKDBStorage初始化完成")

    async def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        try:
            warehouse = await self._get_warehouse()
            conn = warehouse._get_connection()
            result = conn.execute("SELECT 1").fetchone()

            return {
                'status': 'healthy',
                'database': self.db_path,
                'connection': result[0] == 1,
                'read_only': self.read_only
            }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e)
            }

    # ===== 数据保存接口 =====

    async def _save(self, id: str, data: pd.DataFrame,
                    metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        保存数据到统一数据仓库

        根据数据内容自动路由到相应的表：
        - OHLCV数据 -> ohlcv表
        - 包含last/bid/ask的 -> tickers表
        - 包含funding_rate/open_interest的 -> futures_metrics表
        - 包含fgi_value的 -> btc_fgi表
        - 包含series_id的 -> macro_fred表

        Args:
            id: 数据标识符
            data: pandas DataFrame数据
            metadata: 元数据字典

        Returns:
            bool: 保存是否成功
        """
        if not isinstance(data, pd.DataFrame):
            logger.warning("数据不是DataFrame类型，无法保存: %s", id)
            return False

        if data.empty:
            logger.warning("尝试保存空数据! id: %s", id)
            return True

        try:
            warehouse = await self._get_warehouse()

            data_type = self._detect_data_type(data, id, metadata)
            logger.debug("检测到数据类型: %s, id: %s", data_type, id)

            # 根据数据类型调用相应的插入方法
            if data_type == 'ohlcv':
                return await self._save_ohlcv(warehouse, data, id, metadata)
            elif data_type == 'tickers':
                return await self._save_tickers(warehouse, data, id, metadata)
            elif data_type == 'futures_metrics':
                return await self._save_futures_metrics(warehouse, data, id, metadata)
            elif data_type == 'btc_fgi':
                return await self._save_btc_fgi(warehouse, data, id, metadata)
            elif data_type == 'macro_fred':
                return await self._save_macro_fred(warehouse, data, id, metadata)
            elif data_type == 'coin_categories':
                return await self._save_coin_categories(warehouse, data, id, metadata)
            elif data_type == 'coin_markets':
                return await self._save_coin_markets(warehouse, data, id, metadata)
            elif data_type == 'symbols':
                return await self._save_symbols(warehouse, data, id, metadata)
            else:
                logger.warning("无法识别数据类型，使用默认OHLCV格式保存: %s", id)
                return await self._save_ohlcv(warehouse, data, id, metadata)

        except Exception as e:
            logger.error("保存数据失败: %s", str(e))
            return False

    def _detect_data_type(self, data: pd.DataFrame, id: str,
                          metadata: Optional[Dict[str, Any]] = None) -> str:
        """自动检测数据类型

        Args:
            data: DataFrame数据
            id: 数据标识符
            metadata: 元数据

        Returns:
            str: 数据类型
        """
        return DataFrameNormalizer.detect_data_type(data, id, metadata)

    async def _save_ohlcv(self, warehouse: FinancialDataWarehouse, data: pd.DataFrame,
                          id: str, metadata: Optional[Dict[str, Any]]) -> bool:
        """保存OHLCV数据

        metadata 包含:
            - exchange: 交易所名称
            - market_type: 市场类型 (spot/futures)
            - symbol: 交易对符号
            - timeframe: 时间框架 (1d/1h等)
        """
        try:
            normalized = DataFrameNormalizer.normalize_ohlcv(data, id, metadata)
            if normalized is None:
                return False

            return await warehouse.insert_ohlcv(normalized)

        except Exception as e:
            logger.error(f"保存OHLCV数据失败: {str(e)}")
            return False

    async def _save_tickers(self, warehouse: FinancialDataWarehouse, data: pd.DataFrame,
                            id: str, metadata: Optional[Dict[str, Any]]) -> bool:
        """保存Tickers数据"""
        try:
            normalized = DataFrameNormalizer.normalize_tickers(data, id, metadata)
            if normalized is None:
                return False

            return await warehouse.insert_tickers(normalized)

        except Exception as e:
            logger.error(f"保存Tickers数据失败: {str(e)}")
            return False

    async def _save_futures_metrics(self, warehouse: FinancialDataWarehouse, data: pd.DataFrame,
                                    id: str, metadata: Optional[Dict[str, Any]]) -> bool:
        """保存期货指标数据"""
        try:
            normalized = DataFrameNormalizer.normalize_futures_metrics(data, id, metadata)
            if normalized is None:
                return False

            return await warehouse.insert_futures_metrics(normalized)

        except Exception as e:
            logger.error(f"保存期货指标数据失败: {str(e)}")
            return False

    async def _save_btc_fgi(self, warehouse: FinancialDataWarehouse, data: pd.DataFrame,
                            id: str, metadata: Optional[Dict[str, Any]]) -> bool:
        """保存BTC恐惧贪婪指数数据"""
        try:
            normalized = DataFrameNormalizer.normalize_btc_fgi(data, id, metadata)
            if normalized is None:
                return False

            return await warehouse.insert_btc_fgi(normalized)

        except Exception as e:
            logger.error(f"保存BTC恐惧贪婪指数数据失败: {str(e)}")
            return False

    async def _save_macro_fred(self, warehouse: FinancialDataWarehouse, data: pd.DataFrame,
                               id: str, metadata: Optional[Dict[str, Any]]) -> bool:
        """保存宏观数据"""
        try:
            normalized = DataFrameNormalizer.normalize_macro_fred(data, id, metadata)
            if normalized is None:
                return False

            return await warehouse.insert_macro_fred(normalized)

        except Exception as e:
            logger.error(f"保存宏观数据失败: {str(e)}")
            return False

    async def _save_coin_categories(self, warehouse: FinancialDataWarehouse, data: pd.DataFrame,
                                    id: str, metadata: Optional[Dict[str, Any]]) -> bool:
        """保存币种分类数据"""
        try:
            return await warehouse.insert_coin_categories(data)

        except Exception as e:
            logger.error(f"保存币种分类数据失败: {str(e)}")
            return False

    async def _save_coin_markets(self, warehouse: FinancialDataWarehouse, data: pd.DataFrame,
                                 id: str, metadata: Optional[Dict[str, Any]]) -> bool:
        """保存CoinGecko市场数据"""
        try:
            data = data.copy()

            if data.index.name == 'symbol':
                data = data.reset_index()

            column_mapping = {
                'time': 'ts',
                'timestamp': 'ts',
                'datetime': 'ts',
                'market_cap_rank': 'market_cap_rank',
                'market_cap_change_24h': 'market_cap_change_24h',
                'price_change_percentage_24h': 'price_change_pct_24h',
                'total_volume': 'total_volume',
                'high_24h': 'high_24h',
                'low_24h': 'low_24h',
                'price_change_24h': 'price_change_24h',
                'circulating_supply': 'circulating_supply',
                'total_supply': 'total_supply',
                'max_supply': 'max_supply'
            }

            for old_col, new_col in column_mapping.items():
                if old_col in data.columns and new_col not in data.columns:
                    data.rename(columns={old_col: new_col}, inplace=True)

            if 'ts' not in data.columns:
                data['ts'] = pd.Timestamp.now(tz='UTC')

            if 'ts' in data.columns:
                data['ts'] = pd.to_datetime(data['ts'], utc=True, errors='coerce')
                if hasattr(data['ts'].dtype, 'tz') and data['ts'].dtype.tz is not None:
                    data['ts'] = data['ts'].dt.tz_localize(None)
                elif data['ts'].dtype == object:
                    data['ts'] = pd.to_datetime(data['ts'], errors='coerce')

            return await warehouse.insert_coin_markets(data)

        except Exception as e:
            logger.error(f"保存CoinGecko市场数据失败: {str(e)}")
            return False

    async def _save_symbols(self, warehouse: FinancialDataWarehouse, data: pd.DataFrame,
                            id: str, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """保存symbols数据"""
        try:
            if data is None or data.empty:
                return True
            return await warehouse.insert_symbols(data)

        except Exception as e:
            logger.error(f"保存symbols数据失败: {str(e)}")
            return False

    async def _load(self, id: str,
                    metadata: Optional[Dict[str, Any]] = None) -> Optional[pd.DataFrame]:
        """
        从统一数据仓库加载数据

        根据id和metadata信息路由到相应的查询方法
        """
        try:
            warehouse = await self._get_warehouse()

            query_type = self._detect_query_type(id, metadata)

            if query_type == 'ohlcv':
                return await self._load_ohlcv(warehouse, id, metadata)
            elif query_type == 'symbols':
                return await self._load_symbols(warehouse, id, metadata)
            elif query_type == 'stats':
                return await self._load_stats(warehouse, id, metadata)
            elif query_type == 'futures_metrics':
                return await self._load_futures_metrics(warehouse, id, metadata)
            elif query_type == 'macro_fred':
                return await self._load_macro_fred(warehouse, id, metadata)
            elif query_type == 'coin_markets':
                return await self._load_coin_markets(warehouse, id, metadata)
            else:
                logger.warning("无法识别查询类型，尝试默认OHLCV查询: %s", id)
                return await self._load_ohlcv(warehouse, id, metadata)

        except Exception as e:
            logger.error("加载数据失败: %s", str(e))
            return None

    def _detect_query_type(self, id: str,
                           metadata: Optional[Dict[str, Any]] = None) -> str:
        """检测查询类型"""
        if metadata and 'query_type' in metadata:
            return metadata['query_type']

        if metadata and 'data_type' in metadata:
            data_type = metadata['data_type']
            if data_type in ['futures_metrics', 'futures']:
                return 'futures_metrics'
            if data_type in ['macro_fred', 'fred']:
                return 'macro_fred'

        id_lower = id.lower()
        if 'stats' in id_lower or 'summary' in id_lower:
            return 'stats'
        elif 'symbol' in id_lower or 'universe' in id_lower:
            return 'symbols'
        elif 'future' in id_lower or 'umfuture' in id_lower or 'perp' in id_lower:
            return 'futures_metrics'
        elif 'fred' in id_lower or 'macro' in id_lower or 'rate' in id_lower:
            return 'macro_fred'
        else:
            return 'ohlcv'

    async def _load_ohlcv(self, warehouse: FinancialDataWarehouse, id: str,
                          metadata: Optional[Dict[str, Any]] = None) -> Optional[pd.DataFrame]:
        """加载OHLCV数据"""
        timeframe_list = ['1m', '5m', '15m', '30m', '1h', '4h', '1d', '1w', '1M']

        # 默认值的逻辑与 normalize_ohlcv 保持一致
        symbol = id.split('_')[0] if '_' in id else id
        timeframe = '1d'
        exchange = 'unknown'
        market_type = 'spot'

        if metadata:
            symbol = metadata.get('symbol', symbol)
            timeframe = metadata.get('timeframe', timeframe)
            exchange = metadata.get('exchange', exchange)
            market_type = metadata.get('market_type', market_type)

        # 解析 id 中的 timeframe 信息
        if '_' in id and not (metadata and 'symbol' in metadata):
            parts = id.rsplit('_', 1)
            if len(parts) == 2 and parts[1] in timeframe_list:
                symbol_with_exchange = parts[0]
                if ':' in symbol_with_exchange:
                    exchange = symbol_with_exchange.split(':')[0]
                    symbol = symbol_with_exchange.split(':')[1]
                else:
                    symbol = symbol_with_exchange
                if not (metadata and 'timeframe' in metadata):
                    timeframe = parts[1]

        start_time = metadata.get('start_time') if metadata else None
        end_time = metadata.get('end_time') if metadata else None
        limit = metadata.get('limit') if metadata else None

        return await warehouse.get_ohlcv(
            symbol=symbol,
            timeframe=timeframe,
            exchange=exchange,
            market_type=market_type,
            start_time=start_time,
            end_time=end_time,
            limit=limit
        )

    async def _load_symbols(self, warehouse: FinancialDataWarehouse, id: str,
                            metadata: Optional[Dict[str, Any]] = None) -> Optional[pd.DataFrame]:
        """加载symbols数据"""
        exchange = metadata.get('exchange') if metadata else None
        market_type = metadata.get('market_type') if metadata else None
        active = metadata.get('active') if metadata else True

        return await warehouse.get_symbols(
            exchange=exchange,
            market_type=market_type,
            active=active
        )

    async def _load_stats(self, warehouse: FinancialDataWarehouse, id: str,
                          metadata: Optional[Dict[str, Any]] = None) -> Optional[pd.DataFrame]:
        """加载统计信息"""
        stats = await warehouse.get_stats()

        if stats:
            flat_stats = []

            for key, value in stats.items():
                if not isinstance(value, list):
                    flat_stats.append({
                        'metric': key,
                        'value': str(value),
                        'category': 'basic'
                    })

            if 'active_symbols_by_exchange' in stats:
                for exch_stat in stats['active_symbols_by_exchange']:
                    flat_stats.append({
                        'metric': f"symbols_{exch_stat['exchange']}",
                        'value': exch_stat['symbol_count'],
                        'category': 'exchange'
                    })

            return pd.DataFrame(flat_stats)

        return None

    async def _load_futures_metrics(self, warehouse: FinancialDataWarehouse,
                                    id: str,
                                    metadata: Optional[Dict[str, Any]] = None
                                    ) -> Optional[pd.DataFrame]:
        """加载期货指标数据"""
        symbol = id
        exchange = None

        if metadata:
            symbol = metadata.get('symbol', symbol)
            exchange = metadata.get('exchange')

        if ':' in id and not (metadata and 'symbol' in metadata):
            parts = id.split(':')
            if len(parts) == 2:
                exchange = parts[0]
                symbol = parts[1]

        start_time = metadata.get('start_time') if metadata else None
        end_time = metadata.get('end_time') if metadata else None
        limit = metadata.get('limit') if metadata else None

        return await warehouse.get_futures_metrics(
            symbol=symbol,
            exchange=exchange,
            start_time=start_time,
            end_time=end_time,
            limit=limit
        )

    async def _load_macro_fred(self, warehouse: FinancialDataWarehouse,
                               id: str,
                               metadata: Optional[Dict[str, Any]] = None
                               ) -> Optional[pd.DataFrame]:
        """加载FRED宏观数据"""
        series_id = id
        symbol = None

        if metadata:
            series_id = metadata.get('symbol', series_id)
            symbol = metadata.get('symbol')

        if '_' in id:
            parts = id.rsplit('_', 1)
            if len(parts) == 2:
                series_id = parts[0]

        start_time = metadata.get('start_time') if metadata else None
        end_time = metadata.get('end_time') if metadata else None
        limit = metadata.get('limit') if metadata else None

        return await warehouse.get_macro_fred(
            series_id=series_id,
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            limit=limit
        )

    async def _load_coin_markets(self, warehouse: FinancialDataWarehouse,
                                 id: str,
                                 metadata: Optional[Dict[str, Any]] = None
                                 ) -> Optional[pd.DataFrame]:
        """加载CoinGecko市场数据"""
        limit = metadata.get('limit') if metadata else None
        return await warehouse.get_coin_markets(limit=limit)

    async def _delete(self, id: str,
                      metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        删除数据

        注意：统一数据仓库设计为追加模式，删除操作需要特殊处理
        """
        try:
            warehouse = await self._get_warehouse()
            conn = warehouse._get_connection()

            # 根据数据类型删除（需要谨慎使用）
            data_type = metadata.get('data_type') if metadata else 'ohlcv'

            # 默认值逻辑与 normalize_ohlcv 保持一致
            symbol = metadata.get('symbol') if metadata else (id.split('_')[0] if '_' in id else id)
            timeframe = metadata.get('timeframe') if metadata else '1d'
            exchange = metadata.get('exchange') if metadata else 'unknown'
            market_type = metadata.get('market_type') if metadata else 'spot'

            # 如果 metadata 中有 symbol，则不覆盖
            if metadata and 'symbol' in metadata:
                symbol = metadata.get('symbol', symbol)

            if data_type == 'ohlcv':
                # 删除特定symbol的OHLCV数据
                conditions = ["symbol = ?"]
                params = [symbol]

                if timeframe:
                    conditions.append("timeframe = ?")
                    params.append(timeframe)

                if exchange:
                    conditions.append("exchange = ?")
                    params.append(exchange)

                if market_type:
                    conditions.append("market_type = ?")
                    params.append(market_type)

                where_clause = " AND ".join(conditions)
                conn.execute(f"DELETE FROM ohlcv WHERE {where_clause}", params)

            elif data_type == 'symbols':
                # 删除symbol记录
                conn.execute("DELETE FROM symbols WHERE symbol = ?", [symbol])

            logger.info("成功删除数据: %s (%s)", id, data_type)
            return True

        except Exception as e:
            logger.error("删除数据失败: %s", str(e))
            return False

    async def _exists(self, id: str,
                      metadata: Optional[Dict[str, Any]] = None) -> bool:
        """检查数据是否存在"""
        try:
            warehouse = await self._get_warehouse()
            conn = warehouse._get_connection()

            query_type = self._detect_query_type(id, metadata)

            if query_type == 'symbols':
                result = conn.execute(
                    "SELECT COUNT(*) FROM symbols WHERE symbol = ?",
                    [id]
                ).fetchone()
            else:
                # 默认检查OHLCV
                symbol = metadata.get('symbol') if metadata \
                    else id.split('_')[0] if '_' in id else id
                result = conn.execute(
                    "SELECT COUNT(*) FROM ohlcv WHERE symbol = ?",
                    [symbol]
                ).fetchone()

            exists = result[0] > 0
            logger.debug("检查数据存在: %s -> %s", id, exists)
            return exists

        except Exception as e:
            logger.error("检查数据存在性失败: %s", str(e))
            return False

    async def _lists(self) -> List[Dict[str, Any]]:
        """列出所有数据表"""
        try:
            warehouse = await self._get_warehouse()
            conn = warehouse._get_connection()

            tables = []

            # 获取symbols列表
            symbols_result = conn.execute("""
                SELECT symbol, exchange, market_type, base_asset, quote_asset, active
                FROM symbols
                ORDER BY symbol
            """).fetchall()

            for row in symbols_result:
                symbol, exchange, market_type, base_asset, quote_asset, active = row
                tables.append({
                    'id': symbol,
                    'table_name': 'symbols',
                    'symbol': symbol,
                    'exchange': exchange,
                    'market_type': market_type,
                    'base_asset': base_asset,
                    'quote_asset': quote_asset,
                    'active': active,
                    'data_type': 'symbols'
                })

            # 获取OHLCV统计
            ohlcv_stats = conn.execute("""
                SELECT symbol, exchange, market_type, timeframe,
                       COUNT(*) as record_count,
                       MIN(ts) as start_time,
                       MAX(ts) as end_time
                FROM ohlcv
                GROUP BY symbol, exchange, market_type, timeframe
                ORDER BY symbol, timeframe
            """).fetchall()

            for row in ohlcv_stats:
                symbol, exchange, market_type, timeframe, count, start_time, end_time = row
                tables.append({
                    'id': f"{symbol}_{timeframe}",
                    'table_name': 'ohlcv',
                    'symbol': symbol,
                    'exchange': exchange,
                    'market_type': market_type,
                    'timeframe': timeframe,
                    'record_count': count,
                    'start_time': start_time.isoformat() if start_time else None,
                    'end_time': end_time.isoformat() if end_time else None,
                    'data_type': 'ohlcv'
                })

            logger.debug("列出 %s 个数据项", len(tables))
            return tables

        except Exception as e:
            logger.error("列出数据表失败: %s", str(e))
            return []

    async def _get_time_range(self, id: str, data_type: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """获取数据时间范围"""
        try:
            warehouse = await self._get_warehouse()
            conn = warehouse._get_connection()

            if data_type == 'macro_fred':
                result = conn.execute("""
                    SELECT MIN(ts) as min_time, MAX(ts) as max_time
                    FROM macro_fred
                    WHERE series_id = ?
                """, [id]).fetchone()

                if result and result[0]:
                    min_time, max_time = result
                    time_range = {
                        "start_time": min_time,
                        "end_time": max_time
                    }
                    logger.debug("获取时间范围: %s (macro_fred) -> %s", id, time_range)
                    return time_range
                return None

            if data_type == 'futures_metrics':
                result = conn.execute("""
                    SELECT MIN(ts) as min_time, MAX(ts) as max_time
                    FROM futures_metrics
                    WHERE symbol = ?
                """, [id]).fetchone()

                if result and result[0]:
                    min_time, max_time = result
                    time_range = {
                        "start_time": min_time,
                        "end_time": max_time
                    }
                    logger.debug("获取时间范围: %s (futures_metrics) -> %s", id, time_range)
                    return time_range
                return None

            timeframe_list = ['1m', '5m', '15m', '30m', '1h', '4h', '1d', '1w', '1M']
            symbol = id
            timeframe = '1d'
            exchange = None

            if '_' in id:
                parts = id.rsplit('_', 1)
                if len(parts) == 2 and parts[1] in timeframe_list:
                    symbol_with_exchange = parts[0]
                    if ':' in symbol_with_exchange:
                        exchange = symbol_with_exchange.split(':')[0]
                        symbol = symbol_with_exchange.split(':')[1]
                    else:
                        symbol = symbol_with_exchange
                    timeframe = parts[1]

            if exchange:
                result = conn.execute("""
                    SELECT MIN(ts) as min_time, MAX(ts) as max_time
                    FROM ohlcv
                    WHERE symbol = ? AND timeframe = ? AND exchange = ?
                """, [symbol, timeframe, exchange]).fetchone()
            else:
                result = conn.execute("""
                    SELECT MIN(ts) as min_time, MAX(ts) as max_time
                    FROM ohlcv
                    WHERE symbol = ? AND timeframe = ?
                """, [symbol, timeframe]).fetchone()

            if result and result[0]:
                min_time, max_time = result
                time_range = {
                    "start_time": min_time,
                    "end_time": max_time
                }
                logger.debug("获取时间范围: %s (%s) -> %s", id, timeframe, time_range)
                return time_range

            return None

        except Exception as e:
            logger.error("获取数据时间范围失败: %s", str(e))
            return None

    async def _get_metadata(self, id: str) -> Optional[Dict[str, Any]]:
        """获取数据元信息"""
        # 返回统一格式的元信息
        try:
            warehouse = await self._get_warehouse()

            # 获取基本统计信息
            stats = await warehouse.get_stats()

            metadata = {
                'storage_type': 'unified_duckdb',
                'warehouse_version': '1.0',
                'database_path': self.db_path,
                'total_tables': 7,  # 固定表数
                'stats': stats
            }

            return metadata

        except Exception as e:
            logger.error("获取元信息失败: %s", str(e))
            return {}

    async def _update_metadata(self, id: str, metadata: Dict[str, Any]) -> bool:
        """更新数据元信息"""
        # 统一存储的元信息是动态生成的，不支持直接更新
        logger.debug("统一存储的元信息不支持直接更新: %s", id)
        return True

    # ===== 新架构特有方法 =====

    async def get_warehouse(self) -> FinancialDataWarehouse:
        """获取底层数据仓库实例（用于高级操作）"""
        return await self._get_warehouse()

    async def check_data_integrity(self) -> Dict[str, Any]:
        """检查数据完整性"""
        warehouse = await self._get_warehouse()
        return await warehouse.check_data_integrity()

    async def get_stats(self) -> Optional[pd.DataFrame]:
        """获取统计信息"""
        try:
            warehouse = await self._get_warehouse()
            stats = await warehouse.get_stats()

            if stats:
                flat_stats = []

                for key, value in stats.items():
                    if not isinstance(value, list):
                        flat_stats.append({
                            'metric': key,
                            'value': str(value),
                            'category': 'basic'
                        })

                if 'active_symbols_by_exchange' in stats:
                    for exch_stat in stats['active_symbols_by_exchange']:
                        flat_stats.append({
                            'metric': f"symbols_{exch_stat['exchange']}",
                            'value': exch_stat['symbol_count'],
                            'category': 'exchange'
                        })

                return pd.DataFrame(flat_stats)

            return None

        except Exception as e:
            logger.error("获取统计信息失败: %s", str(e))
            return None

    async def close(self):
        """关闭存储连接"""
        if self._warehouse:
            await self._warehouse.close()
            self._warehouse = None

    async def __aenter__(self):
        """异步上下文管理器进入"""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器退出"""
        await self.close()
        return False
