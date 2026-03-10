import logging
from typing import Dict, Optional, Any, List, Type
from datetime import datetime

from . import StorageBase, DUCKDBStorage, LocalFileStorage

logger = logging.getLogger(__name__)


class StorageManager:
    """统一存储管理器

    职责:
        - 存储实例注册与管理
        - 任务数据路由规则配置
        - 协调存储后端的CRUD操作

    设计说明:
        - 缓存逻辑由各存储实现自行处理（继承自 StorageBase）
        - Manager 不直接操作缓存，避免双重缓存问题
        - 路由优先级: 规则路由 > 默认存储 > 第一个注册的存储
    """

    def __init__(self):
        self.storages: Dict[str, StorageBase] = {}
        self.storage_classes: Dict[str, Type[StorageBase]] = {
            'DUCKDBStorage': DUCKDBStorage,
            'LocalFileStorage': LocalFileStorage,
            'duckdb': DUCKDBStorage,
            'localfile': LocalFileStorage,
        }
        self.storage_routes: Dict[str, Dict[str, str]] = {}
        self.default_storage: Optional[str] = None

    def register_storage(self, storage_type: str, storage: StorageBase) -> bool:
        """注册存储实现

        Args:
            storage_type: 存储类型标识符 (如 'duckdb', 'local', 'redis')
            storage: 存储实例

        Returns:
            bool: 是否注册成功
        """
        from . import verify_storage_instance

        valid, message = verify_storage_instance(storage)
        if not valid:
            logger.error(f"Failed to register storage {storage_type}: {message}")
            return False

        self.storages[storage_type] = storage
        logger.info(f"Registered storage: {storage_type}")

        if self.default_storage is None:
            self.default_storage = storage_type
            logger.info(f"Set default storage: {storage_type}")

        return True

    def create_storage(
        self,
        name: str,
        config: Optional[Dict[str, Any]] = None
    ) -> Optional[StorageBase]:
        """创建存储实例

        Args:
            name: 存储类名或类型标识符 (如 'DUCKDBStorage', 'duckdb')
            config: 存储配置

        Returns:
            Optional[StorageBase]: 存储实例，创建失败返回 None
        """
        try:
            if name not in self.storage_classes:
                logger.error(f"Storage class {name} not found")
                return None

            storage_class = self.storage_classes[name]
            storage = storage_class(config or {})

            # 使用类名或映射后的名称注册
            storage_type = name.lower().replace('storage', '')
            self.register_storage(storage_type, storage)

            return storage
        except Exception as e:
            logger.error(f"Failed to create storage {name}: {str(e)}")
            return None

    def register_all_internal_storages(self):
        """注册所有内部存储实现"""
        from .duckdb_storage import DUCKDBStorage
        from .localfile_storage import LocalFileStorage

        self.register_storage('duckdb', DUCKDBStorage({}))
        self.register_storage('localfile', LocalFileStorage({}))

    def set_default_storage(self, storage_type: str) -> bool:
        """设置默认存储

        Args:
            storage_type: 存储类型标识符

        Returns:
            bool: 是否设置成功
        """
        if storage_type in self.storages:
            self.default_storage = storage_type
            logger.info(f"Set default storage: {storage_type}")
            return True
        else:
            logger.error(f"Storage type {storage_type} not registered")
            return False

    def set_storage_route(self, task_type: str, data_type: str, storage_type: str) -> bool:
        """设置存储路由规则

        Args:
            task_type: 任务类型 (如 'ohlcv', 'ticker', 'macro')
            data_type: 数据类型 (如 'spot', 'futures')
            storage_type: 存储类型标识符

        Returns:
            bool: 是否设置成功
        """
        if storage_type not in self.storages:
            logger.error(f"Storage type {storage_type} not registered")
            return False

        if task_type not in self.storage_routes:
            self.storage_routes[task_type] = {}
        self.storage_routes[task_type][data_type] = storage_type
        logger.info(f"Set storage route: {task_type}.{data_type} -> {storage_type}")
        return True

    def get_storage(self, task_type: Optional[str] = None,
                    data_type: Optional[str] = None) -> Optional[StorageBase]:
        """根据任务类型和数据类型获取存储实现

        路由优先级:
            1. 精确匹配: task_type.data_type -> storage_type
            2. 默认存储
            3. 第一个注册的存储

        Args:
            task_type: 任务类型
            data_type: 数据类型

        Returns:
            Optional[StorageBase]: 存储实例，找不到返回 None
        """
        if task_type and data_type:
            if task_type in self.storage_routes and data_type in self.storage_routes[task_type]:
                storage_type = self.storage_routes[task_type][data_type]
                return self.storages.get(storage_type)

        if self.default_storage:
            return self.storages.get(self.default_storage)

        if self.storages:
            return next(iter(self.storages.values()))

        logger.error("No storage registered")
        return None

    async def save(
        self,
        id: str,
        data: Any,
        metadata: Optional[Dict[str, Any]] = None,
        task_type: Optional[str] = None,
        data_type: Optional[str] = None
    ) -> bool:
        """保存数据到存储介质

        路由到对应的存储后端，缓存由存储实现处理

        Args:
            id: 数据ID
            data: 要保存的数据
            metadata: 数据元信息
            task_type: 任务类型，用于路由
            data_type: 数据类型，用于路由

        Returns:
            bool: 是否成功保存数据
        """
        storage = self.get_storage(task_type, data_type)
        if not storage:
            logger.error("No storage available")
            return False

        full_metadata = self._prepare_metadata(metadata)
        success = await storage.save(id, data, full_metadata)

        if success:
            logger.debug(f"Successfully saved data: {id}")

        return success

    async def load(
        self,
        id: str,
        metadata: Optional[Dict[str, Any]] = None,
        task_type: Optional[str] = None,
        data_type: Optional[str] = None
    ) -> Optional[Any]:
        """从存储介质加载数据

        路由到对应的存储后端，缓存由存储实现处理

        Args:
            id: 数据ID
            metadata: 数据元信息
            task_type: 任务类型，用于路由
            data_type: 数据类型，用于路由

        Returns:
            Optional[Any]: 从存储介质加载的数据
        """
        storage = self.get_storage(task_type, data_type)
        if not storage:
            logger.error("No storage available")
            return None

        data = await storage.load(id, metadata)

        if data is not None:
            logger.debug(f"Successfully loaded data: {id}")

        return data

    async def delete(
        self,
        id: str,
        metadata: Optional[Dict[str, Any]] = None,
        task_type: Optional[str] = None,
        data_type: Optional[str] = None
    ) -> bool:
        """从存储介质删除数据

        Args:
            id: 数据ID
            metadata: 数据元信息
            task_type: 任务类型，用于路由
            data_type: 数据类型，用于路由

        Returns:
            bool: 是否成功删除数据
        """
        storage = self.get_storage(task_type, data_type)
        if not storage:
            logger.error("No storage available")
            return False

        success = await storage.delete(id, metadata)

        if success:
            logger.debug(f"Successfully deleted data: {id}")

        return success

    async def exists(
        self,
        id: str,
        metadata: Optional[Dict[str, Any]] = None,
        task_type: Optional[str] = None,
        data_type: Optional[str] = None
    ) -> bool:
        """检查存储介质是否存在数据

        Args:
            id: 数据ID
            metadata: 数据元信息
            task_type: 任务类型，用于路由
            data_type: 数据类型，用于路由

        Returns:
            bool: 存储介质是否存在数据
        """
        storage = self.get_storage(task_type, data_type)
        if not storage:
            logger.error("No storage available")
            return False

        return await storage.exists(id, metadata)

    async def lists(
        self,
        task_type: Optional[str] = None,
        data_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """列出存储介质中的所有数据

        Args:
            task_type: 任务类型，用于路由
            data_type: 数据类型，用于路由

        Returns:
            List[Dict[str, Any]]: 存储介质中的所有数据信息
        """
        storage = self.get_storage(task_type, data_type)
        if not storage:
            logger.error("No storage available")
            return []

        return await storage.lists()

    async def get_time_range(
        self,
        id: str,
        task_type: Optional[str] = None,
        data_type: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """获取数据的时间范围

        Args:
            id: 数据ID
            task_type: 任务类型，用于路由
            data_type: 数据类型，用于路由

        Returns:
            Optional[Dict[str, Any]]: 数据的时间范围
        """
        storage = self.get_storage(task_type, data_type)
        if not storage:
            logger.error("No storage available")
            return None

        return await storage.get_time_range(id, data_type)

    async def get_metadata(
        self,
        id: str,
        task_type: Optional[str] = None,
        data_type: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """获取数据元信息

        Args:
            id: 数据ID
            task_type: 任务类型，用于路由
            data_type: 数据类型，用于路由

        Returns:
            Optional[Dict[str, Any]]: 数据元信息
        """
        storage = self.get_storage(task_type, data_type)
        if not storage:
            logger.error("No storage available")
            return None

        return await storage.get_metadata(id)

    async def update_metadata(
        self,
        id: str,
        metadata: Dict[str, Any],
        task_type: Optional[str] = None,
        data_type: Optional[str] = None
    ) -> bool:
        """更新数据元信息

        Args:
            id: 数据ID
            metadata: 要更新的元信息
            task_type: 任务类型，用于路由
            data_type: 数据类型，用于路由

        Returns:
            bool: 是否成功更新元信息
        """
        storage = self.get_storage(task_type, data_type)
        if not storage:
            logger.error("No storage available")
            return False

        success = await storage.update_metadata(id, metadata)

        if success:
            logger.debug(f"Successfully updated metadata: {id}")

        return success

    def _prepare_metadata(self, metadata: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """准备元数据

        Args:
            metadata: 原始元数据

        Returns:
            Dict[str, Any]: 处理后的元数据
        """
        full_metadata = {
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat(),
            'version': '1.0'
        }

        if metadata:
            full_metadata.update(metadata)

        return full_metadata


storage_manager = StorageManager()


def init_storage_manager():
    """初始化存储管理器"""
    from .duckdb_storage import DUCKDBStorage

    duckdb_storage = DUCKDBStorage({})
    storage_manager.register_storage('duckdb', duckdb_storage)

    logger.info("Storage manager initialized successfully")
