"""
本地文件存储适配器 - 桥接StorageBase接口与本地文件系统

使用示例:
    from chronoforge.storage.localfile_storage import LocalFileStorage

    # 创建存储实例
    storage = LocalFileStorage({
        "data_dir": "./data",
        "file_format": "parquet"
    })

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
    data = await storage._load("BTC_USDT", metadata={'timeframe': '1d'})

    # 获取统计信息
    stats = await storage.get_stats()
"""

import asyncio
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any

import pandas as pd

from chronoforge.storage.base import DataFrameStorageBase
from chronoforge.storage.normalizer import DataFrameNormalizer
from .warehouse import LocalDataWarehouse

logger = logging.getLogger(__name__)


class LocalFileStorage(DataFrameStorageBase):
    """本地文件存储适配器 - 兼容StorageBase接口的本地文件系统实现

    继承关系:
        LocalFileStorage → DataFrameStorageBase → StorageBase[pd.DataFrame]

    接口说明:
        - 公共方法 (继承自基类): save(), load(), delete(), exists(), lists(),
          get_time_range(), get_metadata(), update_metadata()
          (这些方法包含缓存逻辑，自动调用对应的 _xxx 方法)

        - 抽象方法 (需要实现): _save(), _load(), _delete(), _exists(),
          _lists(), _get_time_range(), _get_metadata(), _update_metadata()

        - 特有方法: health_check(), get_stats(), check_data_integrity()

    数据格式支持:
        - CSV: 便于查看和编辑
        - JSON: 结构化数据存储
        - Parquet: 列式存储，高效查询
    """

    def __init__(self, config: Dict[str, Any] = None):
        """
        初始化本地文件存储

        Args:
            config: 配置字典

                - "data_dir" 或 "base_path": 数据目录路径 (默认: "~/.chronoforge/data")
                - "file_format": 文件格式 (默认: "parquet", 可选: csv, json, parquet)
        """
        super().__init__(config)

        default_data_dir = "~/.chronoforge/data"
        data_dir = None
        if config:
            data_dir = config.get("data_dir") or config.get("base_path", default_data_dir)
        else:
            data_dir = default_data_dir
        self.data_dir = str(Path(data_dir).expanduser().absolute())

        self.file_format = config.get("file_format", "parquet") if config else "parquet"

        self._warehouse: Optional[LocalDataWarehouse] = None
        self._warehouse_lock = asyncio.Lock()

    @property
    def name(self) -> str:
        """返回存储插件名称"""
        return "LocalFile"

    @property
    def warehouse(self) -> Optional[LocalDataWarehouse]:
        """同步获取数据仓库实例"""
        return self._warehouse

    async def _get_warehouse(self) -> LocalDataWarehouse:
        """获取数据仓库实例（线程安全，懒加载）"""
        if self._warehouse is None:
            async with self._warehouse_lock:
                if self._warehouse is None:
                    self._warehouse = LocalDataWarehouse(
                        data_dir=self.data_dir,
                        file_format=self.file_format
                    )
                    logger.info(f"本地文件仓库初始化完成: {self.data_dir}")
        return self._warehouse

    async def initialize(self):
        """显式初始化仓库（可选，用于预加载）"""
        await self._get_warehouse()
        logger.debug("LocalFileStorage初始化完成")

    async def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        try:
            data_path = Path(self.data_dir)
            exists = data_path.exists()
            is_readable = data_path.stat().st_mode & 0o444 == 0o444 if exists else False

            return {
                'status': 'healthy' if exists else 'unhealthy',
                'data_dir': self.data_dir,
                'path_exists': exists,
                'path_readable': is_readable,
                'file_format': self.file_format
            }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e)
            }

    async def _save(self, id: str, data: pd.DataFrame,
                    metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        保存数据到本地文件系统

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
        """自动检测数据类型"""
        return DataFrameNormalizer.detect_data_type(data, id, metadata)

    async def _save_ohlcv(self, warehouse: LocalDataWarehouse, data: pd.DataFrame,
                          id: str, metadata: Optional[Dict[str, Any]]) -> bool:
        """保存OHLCV数据"""
        try:
            normalized = DataFrameNormalizer.normalize_ohlcv(data, id, metadata)
            if normalized is None:
                return False

            return await warehouse.insert_ohlcv(normalized)

        except Exception as e:
            logger.error(f"保存OHLCV数据失败: {str(e)}")
            return False

    async def _save_tickers(self, warehouse: LocalDataWarehouse, data: pd.DataFrame,
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

    async def _save_futures_metrics(self, warehouse: LocalDataWarehouse, data: pd.DataFrame,
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

    async def _save_btc_fgi(self, warehouse: LocalDataWarehouse, data: pd.DataFrame,
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

    async def _save_macro_fred(self, warehouse: LocalDataWarehouse, data: pd.DataFrame,
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

    async def _save_coin_categories(self, warehouse: LocalDataWarehouse, data: pd.DataFrame,
                                    id: str, metadata: Optional[Dict[str, Any]]) -> bool:
        """保存币种分类数据"""
        try:
            return await warehouse.insert_coin_categories(data)

        except Exception as e:
            logger.error(f"保存币种分类数据失败: {str(e)}")
            return False

    async def _save_symbols(self, warehouse: LocalDataWarehouse, data: pd.DataFrame,
                            id: str, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """保存symbols数据"""
        try:
            if isinstance(data, list):
                data = pd.DataFrame(data)
            if data is None or data.empty:
                return True
            return await warehouse.insert_symbols(data)

        except Exception as e:
            logger.error(f"保存symbols数据失败: {str(e)}")
            return False

    async def _load(self, id: str,
                    metadata: Optional[Dict[str, Any]] = None) -> Optional[pd.DataFrame]:
        """从本地文件系统加载数据"""
        try:
            warehouse = await self._get_warehouse()

            query_type = self._detect_query_type(id, metadata)

            if query_type == 'ohlcv':
                return await self._load_ohlcv(warehouse, id, metadata)
            elif query_type == 'symbols':
                return await self._load_symbols(warehouse, id, metadata)
            elif query_type == 'stats':
                return await self._load_stats(warehouse, id, metadata)
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

        id_lower = id.lower()
        if 'stats' in id_lower or 'summary' in id_lower:
            return 'stats'
        elif 'symbol' in id_lower or 'universe' in id_lower:
            return 'symbols'
        else:
            return 'ohlcv'

    async def _load_ohlcv(self, warehouse: LocalDataWarehouse, id: str,
                          metadata: Optional[Dict[str, Any]] = None) -> Optional[pd.DataFrame]:
        """加载OHLCV数据"""
        symbol = metadata.get('symbol') if metadata else id.split('_')[0] if '_' in id else id
        timeframe = metadata.get('timeframe') if metadata else '1d'
        exchange = metadata.get('exchange') if metadata else 'unknown'
        start_time = metadata.get('start_time') if metadata else None
        end_time = metadata.get('end_time') if metadata else None
        limit = metadata.get('limit') if metadata else None

        return await warehouse.get_ohlcv(
            symbol=symbol,
            timeframe=timeframe,
            exchange=exchange,
            start_time=start_time,
            end_time=end_time,
            limit=limit
        )

    async def _load_symbols(self, warehouse: LocalDataWarehouse, id: str,
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

    async def _load_stats(self, warehouse: LocalDataWarehouse, id: str,
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

            return pd.DataFrame(flat_stats)

        return None

    async def _delete(self, id: str,
                      metadata: Optional[Dict[str, Any]] = None) -> bool:
        """删除数据"""
        try:
            warehouse = await self._get_warehouse()

            data_type = metadata.get('data_type') if metadata else 'ohlcv'
            symbol = metadata.get('symbol') if metadata else (id.split('_')[0] if '_' in id else id)

            if data_type == 'ohlcv':
                ohlcv_dir = warehouse._get_data_subdir('ohlcv')
                for file_path in ohlcv_dir.glob(f"*.{warehouse.file_format}"):
                    if symbol in file_path.name:
                        file_path.unlink()
                        logger.info("删除文件: %s", file_path)

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

            data_type = metadata.get('data_type') if metadata else 'ohlcv'
            symbol = metadata.get('symbol') if metadata else id.split('_')[0] if '_' in id else id

            if data_type == 'ohlcv':
                ohlcv_dir = warehouse._get_data_subdir('ohlcv')
                for file_path in ohlcv_dir.glob(f"*.{warehouse.file_format}"):
                    if symbol in file_path.name:
                        return True
                return False
            elif data_type == 'symbols':
                symbols_file = warehouse._get_file_path('symbols', 'main')
                return symbols_file.exists()
            else:
                return False

        except Exception as e:
            logger.error("检查数据存在性失败: %s", str(e))
            return False

    async def _lists(self) -> List[Dict[str, Any]]:
        """列出所有数据"""
        try:
            warehouse = await self._get_warehouse()

            tables = []

            symbols_file = warehouse._get_file_path('symbols', 'main')
            if symbols_file.exists():
                tables.append({
                    'id': 'symbols',
                    'file_path': str(symbols_file),
                    'data_type': 'symbols'
                })

            ohlcv_dir = warehouse._get_data_subdir('ohlcv')
            for file_path in ohlcv_dir.glob(f"*.{warehouse.file_format}"):
                stat = file_path.stat()
                tables.append({
                    'id': file_path.stem,
                    'file_path': str(file_path),
                    'file_size': stat.st_size,
                    'modified_time': stat.st_mtime,
                    'data_type': 'ohlcv'
                })

            logger.debug("列出 %s 个数据项", len(tables))
            return tables

        except Exception as e:
            logger.error("列出数据失败: %s", str(e))
            return []

    async def _get_time_range(
        self, id: str, data_type: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """获取数据时间范围"""
        try:
            warehouse = await self._get_warehouse()

            if data_type == 'macro_fred':
                macro_data = await warehouse.get_macro_fred(series_id=id)
                if macro_data is not None and not macro_data.empty:
                    min_time = macro_data['ts'].min()
                    max_time = macro_data['ts'].max()
                    time_range = {
                        "start_time": (
                            min_time.isoformat() if hasattr(min_time, 'isoformat')
                            else str(min_time)
                        ),
                        "end_time": (
                            max_time.isoformat() if hasattr(max_time, 'isoformat')
                            else str(max_time)
                        )
                    }
                    logger.debug("获取时间范围: %s (macro_fred) -> %s", id, time_range)
                    return time_range
                return None

            if data_type == 'futures_metrics':
                futures_data = await warehouse.get_futures_metrics(symbol=id)
                if futures_data is not None and not futures_data.empty:
                    min_time = futures_data['ts'].min()
                    max_time = futures_data['ts'].max()
                    time_range = {
                        "start_time": (
                            min_time.isoformat() if hasattr(min_time, 'isoformat')
                            else str(min_time)
                        ),
                        "end_time": (
                            max_time.isoformat() if hasattr(max_time, 'isoformat')
                            else str(max_time)
                        )
                    }
                    logger.debug("获取时间范围: %s (futures_metrics) -> %s", id, time_range)
                    return time_range
                return None

            timeframe_list = ['1m', '5m', '15m', '30m', '1h', '4h', '1d', '1w', '1M']
            symbol = id
            timeframe = '1d'

            if '_' in id:
                parts = id.rsplit('_', 1)
                if len(parts) == 2 and parts[1] in timeframe_list:
                    symbol = parts[0]
                    timeframe = parts[1]

            ohlcv_data = await warehouse.get_ohlcv(symbol=symbol, timeframe=timeframe)

            if ohlcv_data is not None and not ohlcv_data.empty:
                min_time = ohlcv_data['ts'].min()
                max_time = ohlcv_data['ts'].max()

                time_range = {
                    "start_time": (
                        min_time.isoformat() if hasattr(min_time, 'isoformat')
                        else str(min_time)
                    ),
                    "end_time": (
                        max_time.isoformat() if hasattr(max_time, 'isoformat')
                        else str(max_time)
                    )
                }
                logger.debug("获取时间范围: %s (%s) -> %s", id, timeframe, time_range)
                return time_range

            return None

        except Exception as e:
            logger.error("获取数据时间范围失败: %s", str(e))
            return None

    async def _get_metadata(self, id: str) -> Optional[Dict[str, Any]]:
        """获取数据元信息"""
        try:
            warehouse = await self._get_warehouse()

            stats = await warehouse.get_stats()

            metadata = {
                'storage_type': 'local_file',
                'warehouse_version': '1.0',
                'data_dir': self.data_dir,
                'file_format': self.file_format,
                'stats': stats
            }

            return metadata

        except Exception as e:
            logger.error("获取元信息失败: %s", str(e))
            return {}

    async def _update_metadata(self, id: str, metadata: Dict[str, Any]) -> bool:
        """更新数据元信息"""
        logger.debug("本地文件存储的元信息更新（忽略）: %s", id)
        return True

    async def get_warehouse(self) -> LocalDataWarehouse:
        """获取底层数据仓库实例"""
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
