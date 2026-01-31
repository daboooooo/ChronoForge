"""内部任务统一存储方案实现"""
import logging
import asyncio
import time
import json
import os
from typing import Any, Dict, Optional, List, TypeVar, Generic, Callable
from collections import OrderedDict
from datetime import datetime

from .storage.base import StorageBase

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

# 泛型类型变量
T = TypeVar('T')


class LRUTTLCache(Generic[T]):
    """LRU+TTL缓存实现"""
    
    def __init__(self, maxsize: int = 100, ttl: int = 3600):
        self.cache: OrderedDict[str, tuple[float, T]] = OrderedDict()
        self.maxsize = maxsize
        self.ttl = ttl
        self.lock = asyncio.Lock()
    
    async def get(self, key: str) -> Optional[T]:
        """获取缓存数据"""
        async with self.lock:
            if key not in self.cache:
                return None
            
            timestamp, value = self.cache[key]
            current_time = time.time()
            
            # 检查是否过期
            if current_time - timestamp > self.ttl:
                del self.cache[key]
                return None
            
            # 更新访问顺序
            self.cache.move_to_end(key)
            return value
    
    async def put(self, key: str, value: T, ttl: Optional[int] = None) -> None:
        """设置缓存数据"""
        async with self.lock:
            # 移除过期数据
            current_time = time.time()
            keys_to_remove = []
            for k, (ts, _) in self.cache.items():
                if current_time - ts > (ttl or self.ttl):
                    keys_to_remove.append(k)
            
            for k in keys_to_remove:
                del self.cache[k]
            
            # 移除最久未使用的数据
            if len(self.cache) >= self.maxsize:
                self.cache.popitem(last=False)
            
            # 设置新数据
            self.cache[key] = (current_time, value)
    
    async def delete(self, key: str) -> None:
        """删除缓存数据"""
        async with self.lock:
            if key in self.cache:
                del self.cache[key]
    
    async def clear(self) -> None:
        """清空缓存"""
        async with self.lock:
            self.cache.clear()
    
    def size(self) -> int:
        """获取缓存大小"""
        return len(self.cache)


class InternalTaskStorageManager:
    """内部任务存储管理器"""
    
    def __init__(self):
        self.storages: Dict[str, StorageBase] = {}
        self.cache = LRUTTLCache(maxsize=500, ttl=3600)
        self.storage_routes: Dict[str, Dict[str, str]] = {}  # {task_type: {data_type: storage_type}}
        
    def register_storage(self, storage_type: str, storage: StorageBase) -> None:
        """注册存储实现"""
        self.storages[storage_type] = storage
        logger.info(f"Registered storage: {storage_type}")
    
    def set_storage_route(self, task_type: str, data_type: str, storage_type: str) -> None:
        """设置存储路由规则"""
        if task_type not in self.storage_routes:
            self.storage_routes[task_type] = {}
        self.storage_routes[task_type][data_type] = storage_type
        logger.info(f"Set storage route: {task_type}.{data_type} -> {storage_type}")
    
    def get_storage(self, task_type: str, data_type: str) -> Optional[StorageBase]:
        """根据任务类型和数据类型获取存储实现"""
        # 查找路由规则
        if task_type in self.storage_routes and data_type in self.storage_routes[task_type]:
            storage_type = self.storage_routes[task_type][data_type]
        else:
            # 默认使用local存储
            storage_type = StorageType['LOCAL']
        
        return self.storages.get(storage_type)
    
    async def save_data(self, task_id: str, data: Any, data_type: str, 
                       metadata: Optional[Dict[str, Any]] = None) -> bool:
        """保存任务数据"""
        try:
            # 生成完整的任务ID
            full_task_id = self._generate_full_task_id(task_id, data_type)
            
            # 获取对应的存储
            storage = self.get_storage(task_id.split('_')[0], data_type)
            if not storage:
                logger.error(f"No storage found for task_id: {task_id}, data_type: {data_type}")
                return False
            
            # 准备元数据
            full_metadata = self._prepare_metadata(metadata, data_type)
            
            # 保存到存储
            success = await storage.save(full_task_id, data, None, full_metadata)
            if success:
                # 更新缓存
                await self.cache.put(full_task_id, (data, full_metadata))
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
            
            # 尝试从缓存加载
            cached_data = await self.cache.get(full_task_id)
            if cached_data:
                logger.debug(f"Loaded data from cache for task: {full_task_id}")
                return cached_data[0]
            
            # 获取对应的存储
            storage = self.get_storage(task_id.split('_')[0], data_type)
            if not storage:
                logger.error(f"No storage found for task_id: {task_id}, data_type: {data_type}")
                return None
            
            # 从存储加载
            data = await storage.load(full_task_id, None, metadata)
            if data:
                # 获取元数据并更新缓存
                data_metadata = await storage.get_metadata(full_task_id, None)
                await self.cache.put(full_task_id, (data, data_metadata or {}))
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
            
            # 获取对应的存储
            storage = self.get_storage(task_id.split('_')[0], data_type)
            if not storage:
                logger.error(f"No storage found for task_id: {task_id}, data_type: {data_type}")
                return False
            
            # 从存储删除
            success = await storage.delete(full_task_id, None, metadata)
            if success:
                # 从缓存删除
                await self.cache.delete(full_task_id)
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
    
    def _prepare_metadata(self, metadata: Optional[Dict[str, Any]], data_type: str) -> Dict[str, Any]:
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


class LocalFileStorageImpl(StorageBase[Any]):
    """本地文件存储实现"""
    
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)
        self.base_path = config.get('base_path', './internal_task_data')
        os.makedirs(self.base_path, exist_ok=True)
    
    @property
    def name(self):
        """返回存储插件名称"""
        return "local_file_storage"
    
    async def save(self, id: str, data: Any, sub: str = None, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """保存数据到本地文件"""
        try:
            # 创建目录结构
            dir_path = os.path.join(self.base_path, id.split(':')[0])
            os.makedirs(dir_path, exist_ok=True)
            
            # 保存数据文件
            data_file = os.path.join(dir_path, f"{id}_data.json")
            with open(data_file, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            
            # 保存元数据文件
            metadata_file = os.path.join(dir_path, f"{id}_metadata.json")
            with open(metadata_file, 'w') as f:
                json.dump(metadata or {}, f, indent=2, default=str)
            
            return True
        except Exception as e:
            logger.error(f"Error saving to local file: {str(e)}")
            return False
    
    async def load(self, id: str, sub: str = None, metadata: Optional[Dict[str, Any]] = None) -> Optional[Any]:
        """从本地文件加载数据"""
        try:
            data_file = os.path.join(self.base_path, id.split(':')[0], f"{id}_data.json")
            if not os.path.exists(data_file):
                return None
            
            with open(data_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading from local file: {str(e)}")
            return None
    
    async def delete(self, id: str, sub: str = None, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """从本地文件删除数据"""
        try:
            # 删除数据文件
            data_file = os.path.join(self.base_path, id.split(':')[0], f"{id}_data.json")
            if os.path.exists(data_file):
                os.remove(data_file)
            
            # 删除元数据文件
            metadata_file = os.path.join(self.base_path, id.split(':')[0], f"{id}_metadata.json")
            if os.path.exists(metadata_file):
                os.remove(metadata_file)
            
            return True
        except Exception as e:
            logger.error(f"Error deleting from local file: {str(e)}")
            return False
    
    async def exists(self, id: str, sub: str = None, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """检查数据是否存在"""
        try:
            data_file = os.path.join(self.base_path, id.split(':')[0], f"{id}_data.json")
            return os.path.exists(data_file)
        except Exception as e:
            logger.error(f"Error checking existence: {str(e)}")
            return False
    
    async def lists(self, sub: str = None) -> List[Dict[str, Any]]:
        """列出存储介质中的所有数据"""
        try:
            result = []
            if not os.path.exists(self.base_path):
                return result
            
            for root, dirs, files in os.walk(self.base_path):
                for file in files:
                    if file.endswith('_data.json'):
                        task_id = file.replace('_data.json', '')
                        metadata_file = os.path.join(root, f"{task_id}_metadata.json")
                        metadata = {}
                        if os.path.exists(metadata_file):
                            with open(metadata_file, 'r') as f:
                                metadata = json.load(f)
                        
                        result.append({
                            'id': task_id,
                            'path': os.path.join(root, file),
                            'metadata': metadata
                        })
            
            return result
        except Exception as e:
            logger.error(f"Error listing files: {str(e)}")
            return []
    
    async def get_time_range(self, id: str, sub: str = None) -> Optional[Dict[str, Any]]:
        """获取数据的时间范围"""
        try:
            metadata_file = os.path.join(self.base_path, id.split(':')[0], f"{id}_metadata.json")
            if not os.path.exists(metadata_file):
                return None
            
            with open(metadata_file, 'r') as f:
                metadata = json.load(f)
            
            # 尝试从元数据中获取时间范围
            if 'created_at' in metadata:
                return {
                    'start_time': metadata['created_at'],
                    'end_time': metadata.get('updated_at', metadata['created_at'])
                }
            
            return None
        except Exception as e:
            logger.error(f"Error getting time range: {str(e)}")
            return None
    
    async def get_metadata(self, id: str, sub: str = None) -> Optional[Dict[str, Any]]:
        """获取元数据"""
        try:
            metadata_file = os.path.join(self.base_path, id.split(':')[0], f"{id}_metadata.json")
            if not os.path.exists(metadata_file):
                return None
            
            with open(metadata_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error getting metadata: {str(e)}")
            return None
    
    async def update_metadata(self, id: str, metadata: Dict[str, Any], sub: str = None) -> bool:
        """更新元数据"""
        try:
            metadata_file = os.path.join(self.base_path, id.split(':')[0], f"{id}_metadata.json")
            
            # 读取现有元数据
            existing_metadata = {}
            if os.path.exists(metadata_file):
                with open(metadata_file, 'r') as f:
                    existing_metadata = json.load(f)
            
            # 更新元数据
            existing_metadata.update(metadata)
            existing_metadata['updated_at'] = datetime.now().isoformat()
            
            # 保存更新后的元数据
            with open(metadata_file, 'w') as f:
                json.dump(existing_metadata, f, indent=2, default=str)
            
            return True
        except Exception as e:
            logger.error(f"Error updating metadata: {str(e)}")
            return False


# 初始化存储管理器
def init_internal_storage():
    """初始化内部存储管理器"""
    # 注册默认存储实现
    local_storage = LocalFileStorageImpl({})
    internal_storage_manager.register_storage(StorageType['LOCAL'], local_storage)
    
    # 设置默认存储路由
    internal_storage_manager.set_storage_route('CryptoSpotDataSource', DataType['TICKERS'], StorageType['LOCAL'])
    internal_storage_manager.set_storage_route('CoinGeckoDataSource', DataType['COIN_MARKETS'], StorageType['LOCAL'])
    internal_storage_manager.set_storage_route('CoinGeckoDataSource', DataType['COIN_CATEGORIES'], StorageType['LOCAL'])
    internal_storage_manager.set_storage_route('CoinGeckoDataSource', DataType['TOPS'], StorageType['LOCAL'])
    internal_storage_manager.set_storage_route('CryptoSpotDataSource', DataType['OHLCV'], StorageType['LOCAL'])
    
    logger.info("Internal storage manager initialized successfully")
