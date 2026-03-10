import abc
import inspect
from typing import Any, Dict, Optional, List, Tuple, get_type_hints, Generic, TypeVar

import pandas as pd

from ..cache import cache_manager

T = TypeVar('T')


class StorageBase(abc.ABC, Generic[T]):
    """存储插件基类

    存储插件负责将数据保存到指定的存储介质中。
    """

    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.enable_cache = self.config.get('enable_cache', True)
        self.cache_ttl = self.config.get('cache_ttl', 3600)
        self.cache_name = self.config.get('cache_name', 'storage')

    @property
    @abc.abstractmethod
    def name(self):
        """返回存储插件名称"""
        pass

    @property
    def plugin_type(self):
        """返回存储插件类型"""
        return "storage"

    async def save(
        self,
        id: str,
        data: T,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """保存数据到存储介质

        Args:
            id: 数据ID
            data: 要保存的数据
            metadata: 数据元信息

        Returns:
            bool: 是否成功保存数据
        """
        success = await self._save(id, data, metadata)

        if success and self.enable_cache:
            cache_key = self._generate_cache_key(id)
            await cache_manager.put(cache_key, (data, metadata),
                                    ttl=self.cache_ttl, cache_name=self.cache_name)

        return success

    async def load(
        self,
        id: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Optional[T]:
        """从存储介质加载数据

        Args:
            id: 数据ID
            metadata: 数据元信息

        Returns:
            Optional[T]: 从存储介质加载的数据
        """
        if self.enable_cache:
            cache_key = self._generate_cache_key(id)
            cached_data = await cache_manager.get(cache_key, cache_name=self.cache_name)
            if cached_data:
                return cached_data[0]

        data = await self._load(id, metadata)

        if data is not None and self.enable_cache:
            try:
                if hasattr(data, 'empty') and data.empty:
                    return data
            except Exception:
                pass

            cache_key = self._generate_cache_key(id)
            data_metadata = await self.get_metadata(id)
            await cache_manager.put(cache_key, (data, data_metadata),
                                    ttl=self.cache_ttl, cache_name=self.cache_name)

        return data

    async def delete(
        self,
        id: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """从存储介质删除数据

        Args:
            id: 数据ID
            metadata: 数据元信息

        Returns:
            bool: 是否成功删除数据
        """
        success = await self._delete(id, metadata)

        if success and self.enable_cache:
            cache_key = self._generate_cache_key(id)
            await cache_manager.delete(cache_key, cache_name=self.cache_name)

        return success

    async def exists(
        self,
        id: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """检查存储介质是否存在数据

        Args:
            id: 数据ID
            metadata: 数据元信息

        Returns:
            bool: 存储介质是否存在数据
        """
        return await self._exists(id, metadata)

    async def lists(self) -> List[Dict[str, Any]]:
        """列出存储介质中的所有数据

        Returns:
            List[Dict[str, Any]]: 存储介质中的所有数据信息
        """
        return await self._lists()

    async def get_time_range(
        self,
        id: str,
        data_type: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """获取数据的时间范围

        Args:
            id: 数据ID
            data_type: 数据类型（如 'ohlcv', 'macro_fred', 'futures_metrics' 等）

        Returns:
            Optional[Dict[str, Any]]: 数据的时间范围，包含start_time和end_time
        """
        return await self._get_time_range(id, data_type)

    async def get_metadata(
        self,
        id: str
    ) -> Optional[Dict[str, Any]]:
        """获取数据元信息

        Args:
            id: 数据ID

        Returns:
            Optional[Dict[str, Any]]: 数据元信息
        """
        if self.enable_cache:
            cache_key = self._generate_cache_key(id)
            cached_data = await cache_manager.get(cache_key, cache_name=self.cache_name)
            if cached_data:
                return cached_data[1]

        return await self._get_metadata(id)

    async def update_metadata(
        self,
        id: str,
        metadata: Dict[str, Any]
    ) -> bool:
        """更新数据元信息

        Args:
            id: 数据ID
            metadata: 要更新的元信息

        Returns:
            bool: 是否成功更新元信息
        """
        success = await self._update_metadata(id, metadata)

        if success and self.enable_cache:
            cache_key = self._generate_cache_key(id)
            cached_data = await cache_manager.get(cache_key, cache_name=self.cache_name)
            if cached_data:
                cached_metadata = cached_data[1].copy() if cached_data[1] else {}
                cached_metadata.update(metadata)
                await cache_manager.put(cache_key, (cached_data[0], cached_metadata),
                                        ttl=self.cache_ttl, cache_name=self.cache_name)

        return success

    @abc.abstractmethod
    async def _save(
        self,
        id: str,
        data: T,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """保存数据到存储介质的具体实现

        Args:
            id: 数据ID
            data: 要保存的数据
            metadata: 数据元信息

        Returns:
            bool: 是否成功保存数据
        """
        pass

    @abc.abstractmethod
    async def _load(
        self,
        id: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Optional[T]:
        """从存储介质加载数据的具体实现

        Args:
            id: 数据ID
            metadata: 数据元信息

        Returns:
            Optional[T]: 从存储介质加载的数据
        """
        pass

    @abc.abstractmethod
    async def _delete(
        self,
        id: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """从存储介质删除数据的具体实现

        Args:
            id: 数据ID
            metadata: 数据元信息

        Returns:
            bool: 是否成功删除数据
        """
        pass

    @abc.abstractmethod
    async def _exists(
        self,
        id: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """检查存储介质是否存在数据的具体实现

        Args:
            id: 数据ID
            metadata: 数据元信息

        Returns:
            bool: 存储介质是否存在数据
        """
        pass

    @abc.abstractmethod
    async def _lists(self) -> List[Dict[str, Any]]:
        """列出存储介质中的所有数据的具体实现

        Returns:
            List[Dict[str, Any]]: 存储介质中的所有数据信息
        """
        pass

    @abc.abstractmethod
    async def _get_time_range(
        self,
        id: str,
        data_type: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """获取数据的时间范围的具体实现

        Args:
            id: 数据ID
            data_type: 数据类型（如 'ohlcv', 'macro_fred', 'futures_metrics' 等）

        Returns:
            Optional[Dict[str, Any]]: 数据的时间范围，包含start_time和end_time
        """
        pass

    @abc.abstractmethod
    async def _get_metadata(
        self,
        id: str
    ) -> Optional[Dict[str, Any]]:
        """获取数据元信息的具体实现

        Args:
            id: 数据ID

        Returns:
            Optional[Dict[str, Any]]: 数据元信息
        """
        pass

    @abc.abstractmethod
    async def _update_metadata(
        self,
        id: str,
        metadata: Dict[str, Any]
    ) -> bool:
        """更新数据元信息的具体实现

        Args:
            id: 数据ID
            metadata: 要更新的元信息

        Returns:
            bool: 是否成功更新元信息
        """
        pass

    def _generate_cache_key(self, id: str) -> str:
        """生成缓存键

        Args:
            id: 数据ID

        Returns:
            str: 缓存键
        """
        return f"{self.name}:{id}"


class DataFrameStorageBase(StorageBase[pd.DataFrame]):
    """DataFrame 存储基类，保持向后兼容"""
    pass


def verify_storage_instance(storage) -> Tuple[bool, str]:
    """严格验证一个类或实例是否符合 StorageBase 的要求"""
    errors = []
    if inspect.isclass(storage):
        cls = storage
        try:
            temp_instance = storage({})
        except Exception as e:
            return False, f"无法创建{storage.__name__}实例: {str(e)}"
    else:
        cls = storage.__class__
        temp_instance = storage

    if not isinstance(getattr(cls, "name", None), property):
        errors.append("Missing required @property 'name'.")

    save = getattr(temp_instance, "save", None)
    if not save or not callable(save):
        errors.append("'save' method is missing.")
    else:
        sig = inspect.signature(save)
        expected = ["id", "data", "metadata"]
        actual = list(sig.parameters.keys())
        if actual != expected:
            errors.append(f"'save' must have parameters {expected}, "
                          f"got {actual}")

        try:
            hints = get_type_hints(save)
            if hints.get("id") is not str:
                errors.append("'save' 'id' parameter must be annotated as str")
        except (TypeError, ValueError):
            pass

    load = getattr(temp_instance, "load", None)
    if not load or not callable(load):
        errors.append("'load' method is missing.")
    else:
        sig = inspect.signature(load)
        expected = ["id", "metadata"]
        actual = list(sig.parameters.keys())
        if actual != expected:
            errors.append(f"'load' must have parameters {expected}, "
                          f"got {actual}")

        try:
            hints = get_type_hints(load)
            if hints.get("id") is not str:
                errors.append("'load' 'id' parameter must be annotated as str")
        except (TypeError, ValueError):
            pass

    delete = getattr(temp_instance, "delete", None)
    if not delete or not callable(delete):
        errors.append("'delete' method is missing.")
    else:
        sig = inspect.signature(delete)
        expected = ["id", "metadata"]
        actual = list(sig.parameters.keys())
        if actual != expected:
            errors.append(f"'delete' must have parameters {expected}, "
                          f"got {actual}")

    exists = getattr(temp_instance, "exists", None)
    if not exists or not callable(exists):
        errors.append("'exists' method is missing.")
    else:
        sig = inspect.signature(exists)
        expected = ["id", "metadata"]
        actual = list(sig.parameters.keys())
        if actual != expected:
            errors.append(f"'exists' must have parameters {expected}, "
                          f"got {actual}")

    lists_method = getattr(temp_instance, "lists", None)
    if not lists_method or not callable(lists_method):
        errors.append("'lists' method is missing.")
    else:
        sig = inspect.signature(lists_method)
        expected = []
        actual = list(sig.parameters.keys())
        if actual != expected:
            errors.append(f"'lists' must have parameters {expected}, "
                          f"got {actual}")

    get_time_range = getattr(temp_instance, "get_time_range", None)
    if not get_time_range or not callable(get_time_range):
        errors.append("'get_time_range' method is missing.")
    else:
        sig = inspect.signature(get_time_range)
        expected = [["id"], ["id", "data_type"]]
        actual = list(sig.parameters.keys())
        if actual not in expected:
            errors.append(f"'get_time_range' must have parameters {expected}, "
                          f"got {actual}")

    get_metadata = getattr(temp_instance, "get_metadata", None)
    if not get_metadata or not callable(get_metadata):
        errors.append("'get_metadata' method is missing.")
    else:
        sig = inspect.signature(get_metadata)
        expected = ["id"]
        actual = list(sig.parameters.keys())
        if actual != expected:
            errors.append(f"'get_metadata' must have parameters {expected}, "
                          f"got {actual}")

    update_metadata = getattr(temp_instance, "update_metadata", None)
    if not update_metadata or not callable(update_metadata):
        errors.append("'update_metadata' method is missing.")
    else:
        sig = inspect.signature(update_metadata)
        expected = ["id", "metadata"]
        actual = list(sig.parameters.keys())
        if actual != expected:
            errors.append(f"'update_metadata' must have parameters {expected}, "
                          f"got {actual}")

    init_sig = inspect.signature(cls.__init__)
    if "config" not in init_sig.parameters:
        errors.append("__init__ must accept 'config' parameter.")

    if errors:
        msg = "\n".join(f"- {e}" for e in errors)
        result_msg = (
            f"{cls.__name__} does not conform to "
            f"StorageBase requirements:\n{msg}"
        )
        return False, result_msg
    else:
        result_msg = (
            f"{cls.__name__} ✅ passed all "
            f"StorageBase validation checks."
        )
        return True, result_msg
