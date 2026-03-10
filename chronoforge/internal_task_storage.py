"""内部任务统一存储方案实现"""
import logging
import time
from typing import Any, Dict, Optional
from datetime import datetime

from .storage.manager import storage_manager

logger = logging.getLogger(__name__)

# 存储类型枚举
StorageType = {
    'LOCAL': 'local',
    'DUCKDB': 'duckdb',
    'REDIS': 'redis',
    'MONGODB': 'mongodb'
}

# 数据类型枚举
DataType = {
    'TICKERS': 'tickers',
    'COIN_MARKETS': 'coin_markets',
    'COIN_CATEGORIES': 'coin_categories',
    'TOPS': 'tops',
    'OHLCV': 'ohlcv',
    'CUSTOM': 'custom'
}


class InternalTaskStorageManager:
    """内部任务存储管理器（兼容层）

    基于统一存储管理器实现，保持向后兼容
    """

    def __init__(self):
        # 复用统一存储管理器
        self.storage_manager = storage_manager

    def register_storage(self, storage_type: str, storage: Any) -> None:
        """注册存储实现"""
        self.storage_manager.register_storage(storage_type, storage)

    def set_storage_route(self, task_type: str, data_type: str, storage_type: str) -> None:
        """设置存储路由规则"""
        self.storage_manager.set_storage_route(task_type, data_type, storage_type)

    async def save_data(self, task_id: str, data: Any, data_type: str,
                        metadata: Optional[Dict[str, Any]] = None) -> bool:
        """保存任务数据"""
        try:
            # 生成完整的任务ID
            full_task_id = self._generate_full_task_id(task_id, data_type)

            # 准备元数据
            full_metadata = self._prepare_metadata(metadata, data_type)

            # 使用统一存储管理器保存数据
            task_type = task_id.split('_')[0]
            success = await self.storage_manager.save(
                full_task_id,
                data,
                None,
                full_metadata,
                task_type,
                data_type
            )

            if success:
                logger.info(f"Successfully saved data for task: {full_task_id}")

            return success
        except Exception as e:
            logger.error(f"Error saving data for task {task_id}: {str(e)}", exc_info=True)
            return False

    async def load_data(self, task_id: str, data_type: str,
                        metadata: Optional[Dict[str, Any]] = None) -> Optional[Any]:
        """加载任务数据"""
        try:
            # 生成完整的任务ID
            full_task_id = self._generate_full_task_id(task_id, data_type)

            # 使用统一存储管理器加载数据
            task_type = task_id.split('_')[0]
            data = await self.storage_manager.load(
                full_task_id,
                None,
                metadata,
                task_type,
                data_type
            )

            if data:
                logger.info(f"Successfully loaded data for task: {full_task_id}")

            return data
        except Exception as e:
            logger.error(f"Error loading data for task {task_id}: {str(e)}", exc_info=True)
            return None

    async def delete_data(self, task_id: str, data_type: str,
                          metadata: Optional[Dict[str, Any]] = None) -> bool:
        """删除任务数据"""
        try:
            # 生成完整的任务ID
            full_task_id = self._generate_full_task_id(task_id, data_type)

            # 使用统一存储管理器删除数据
            task_type = task_id.split('_')[0]
            success = await self.storage_manager.delete(
                full_task_id,
                None,
                metadata,
                task_type,
                data_type
            )

            if success:
                logger.info(f"Successfully deleted data for task: {full_task_id}")

            return success
        except Exception as e:
            logger.error(f"Error deleting data for task {task_id}: {str(e)}", exc_info=True)
            return False

    def _generate_full_task_id(self, task_id: str, data_type: str) -> str:
        """生成完整的任务ID"""
        # 格式: task_id:data_type:timestamp
        timestamp = int(time.time())
        return f"{task_id}:{data_type}:{timestamp}"

    def _prepare_metadata(self, metadata: Optional[Dict[str, Any]],
                          data_type: str) -> Dict[str, Any]:
        """准备元数据"""
        full_metadata = {
            'data_type': data_type,
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat(),
            'version': '1.0'
        }

        if metadata:
            full_metadata.update(metadata)

        return full_metadata


# 全局存储管理器实例
internal_storage_manager = InternalTaskStorageManager()


# 初始化存储管理器
def init_internal_storage():
    """初始化内部存储管理器"""
    from .storage.localfile import LocalFileStorage

    # 注册默认存储实现（如果尚未注册）
    if not storage_manager.storages:
        local_storage = LocalFileStorage({})
        storage_manager.register_storage(StorageType['LOCAL'], local_storage)

    # 设置默认存储路由
    storage_manager.set_storage_route('CryptoSpotDataSource',
                                      DataType['TICKERS'], StorageType['LOCAL'])
    storage_manager.set_storage_route('CoinGeckoDataSource',
                                      DataType['COIN_MARKETS'], StorageType['LOCAL'])
    storage_manager.set_storage_route('CoinGeckoDataSource',
                                      DataType['COIN_CATEGORIES'], StorageType['LOCAL'])
    storage_manager.set_storage_route('CoinGeckoDataSource',
                                      DataType['TOPS'], StorageType['LOCAL'])
    storage_manager.set_storage_route('CryptoSpotDataSource',
                                      DataType['OHLCV'], StorageType['LOCAL'])

    logger.info("Internal storage manager initialized successfully")
