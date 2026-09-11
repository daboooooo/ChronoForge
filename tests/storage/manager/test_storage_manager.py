"""
StorageManager 测试用例

测试存储管理器的所有功能，包括：
1. 初始化与配置
2. 存储注册
3. 默认存储设置
4. 存储路由
5. CRUD 操作
6. 时间范围查询
7. 元数据操作
8. 缓存集成
"""

from unittest.mock import patch
from chronoforge.storage.manager import StorageManager, storage_manager
from chronoforge.storage.base import StorageBase


class MockStorage(StorageBase):
    """模拟存储实现"""

    def __init__(self, config=None):
        super().__init__(config or {})
        self.saved_data = {}
        self.saved_metadata = {}

    @property
    def name(self):
        return "mock_storage"

    async def _save(self, id, data, metadata=None):
        self.saved_data[id] = data
        self.saved_metadata[id] = metadata
        return True

    async def _load(self, id, metadata=None):
        return self.saved_data.get(id)

    async def _delete(self, id, metadata=None):
        if id in self.saved_data:
            del self.saved_data[id]
            if id in self.saved_metadata:
                del self.saved_metadata[id]
            return True
        return False

    async def _exists(self, id, metadata=None):
        return id in self.saved_data

    async def _lists(self):
        result = []
        for key in self.saved_data.keys():
            result.append({"id": key})
        return result

    async def _get_time_range(self, id, data_type=None):
        if id in self.saved_data:
            return {
                "start_time": "2023-01-01T00:00:00",
                "end_time": "2023-12-31T23:59:59"
            }
        return None

    async def _get_metadata(self, id):
        return self.saved_metadata.get(id)

    async def _update_metadata(self, id, metadata):
        if id in self.saved_metadata:
            self.saved_metadata[id].update(metadata)
        else:
            self.saved_metadata[id] = metadata
        return True


class TestStorageManagerInit:
    """存储管理器初始化测试"""

    def test_init_default(self):
        """测试默认初始化"""
        manager = StorageManager()
        assert manager.default_storage is None
        assert len(manager.storages) == 0
        assert len(manager.storage_routes) == 0

    def test_singleton_instance(self):
        """测试单例实例"""
        assert storage_manager is not None
        assert isinstance(storage_manager, StorageManager)


class TestStorageRegistration:
    """存储注册测试"""

    def setup_method(self):
        """测试前初始化"""
        self.manager = StorageManager()
        self.mock_storage = MockStorage()
        self.manager.register_storage('mock', self.mock_storage)

    def test_register_storage_success(self):
        """测试成功注册存储"""
        new_storage = MockStorage()
        result = self.manager.register_storage('new_mock', new_storage)
        assert result is True
        assert 'new_mock' in self.manager.storages

    def test_register_storage_invalid(self):
        """测试无效存储注册"""
        invalid_storage = object()
        with patch('chronoforge.storage.base.verify_storage_instance',
                   return_value=(False, 'Invalid storage')):
            result = self.manager.register_storage('invalid', invalid_storage)
            assert result is False
            assert 'invalid' not in self.manager.storages


class TestDefaultStorage:
    """默认存储测试"""

    def setup_method(self):
        """测试前初始化"""
        self.manager = StorageManager()
        self.mock_storage = MockStorage()
        self.manager.register_storage('mock', self.mock_storage)

    def test_set_default_storage_success(self):
        """测试成功设置默认存储"""
        result = self.manager.set_default_storage('mock')
        assert result is True
        assert self.manager.default_storage == 'mock'

    def test_set_default_storage_nonexistent(self):
        """测试设置不存在的存储"""
        result = self.manager.set_default_storage('non_existent')
        assert result is False


class TestStorageRoutes:
    """存储路由测试"""

    def setup_method(self):
        """测试前初始化"""
        self.manager = StorageManager()
        self.mock_storage = MockStorage()
        self.manager.register_storage('mock', self.mock_storage)

    def test_set_storage_route_success(self):
        """测试成功设置存储路由"""
        result = self.manager.set_storage_route('test_task', 'test_data', 'mock')
        assert result is True
        assert self.manager.storage_routes['test_task']['test_data'] == 'mock'

    def test_set_storage_route_nonexistent(self):
        """测试设置不存在的路由"""
        result = self.manager.set_storage_route('test_task', 'test_data', 'non_existent')
        assert result is False


class TestGetStorage:
    """获取存储测试"""

    def setup_method(self):
        """测试前初始化"""
        self.manager = StorageManager()
        self.mock_storage = MockStorage()
        self.manager.register_storage('mock', self.mock_storage)
        self.manager.set_default_storage('mock')

    def test_get_storage_by_route(self):
        """测试通过路由获取存储"""
        self.manager.set_storage_route('test_task', 'test_data', 'mock')
        storage = self.manager.get_storage('test_task', 'test_data')
        assert storage is not None

    def test_get_storage_default(self):
        """测试获取默认存储"""
        storage = self.manager.get_storage()
        assert storage is not None

    def test_get_storage_no_storage(self):
        """测试无存储时获取"""
        manager = StorageManager()
        storage = manager.get_storage()
        assert storage is None


class TestCRUDOperations:
    """CRUD 操作测试"""

    def setup_method(self):
        """测试前初始化"""
        self.manager = StorageManager()
        self.mock_storage = MockStorage()
        self.manager.register_storage('mock', self.mock_storage)
        self.manager.set_default_storage('mock')

    async def test_save_success(self):
        """测试成功保存"""
        result = await self.manager.save('test_id', {'key': 'value'})
        assert result is True

    async def test_save_no_storage(self):
        """测试无存储时保存"""
        manager = StorageManager()
        result = await manager.save('test_id', {'key': 'value'})
        assert result is False

    async def test_load_success(self):
        """测试成功加载"""
        await self.manager.save('test_id', {'key': 'value'})
        data = await self.manager.load('test_id')
        assert data == {'key': 'value'}

    async def test_load_nonexistent(self):
        """测试加载不存在的数据"""
        data = await self.manager.load('non_existent_id')
        assert data is None

    async def test_load_no_storage(self):
        """测试无存储时加载"""
        manager = StorageManager()
        data = await manager.load('test_id')
        assert data is None

    async def test_delete_success(self):
        """测试成功删除"""
        await self.manager.save('test_id', {'key': 'value'})
        result = await self.manager.delete('test_id')
        assert result is True

    async def test_delete_nonexistent(self):
        """测试删除不存在的数据"""
        result = await self.manager.delete('non_existent_id')
        assert result is False

    async def test_delete_no_storage(self):
        """测试无存储时删除"""
        manager = StorageManager()
        result = await manager.delete('test_id')
        assert result is False

    async def test_exists_true(self):
        """测试数据存在"""
        await self.manager.save('test_id', {'key': 'value'})
        result = await self.manager.exists('test_id')
        assert result is True

    async def test_exists_false(self):
        """测试数据不存在"""
        result = await self.manager.exists('non_existent_id')
        assert result is False

    async def test_exists_no_storage(self):
        """测试无存储时检查"""
        manager = StorageManager()
        result = await manager.exists('test_id')
        assert result is False

    async def test_lists_success(self):
        """测试列出数据"""
        await self.manager.save('test_id1', {'key': 'value1'})
        await self.manager.save('test_id2', {'key': 'value2'})
        result = await self.manager.lists()
        assert len(result) == 2

    async def test_lists_no_storage(self):
        """测试无存储时列出"""
        manager = StorageManager()
        result = await manager.lists()
        assert len(result) == 0


class TestTimeRangeOperations:
    """时间范围操作测试"""

    def setup_method(self):
        """测试前初始化"""
        self.manager = StorageManager()
        self.mock_storage = MockStorage()
        self.manager.register_storage('mock', self.mock_storage)
        self.manager.set_default_storage('mock')

    async def test_get_time_range_success(self):
        """测试成功获取时间范围"""
        await self.manager.save('test_id', {'key': 'value'})
        result = await self.manager.get_time_range('test_id')
        assert result is not None
        assert 'start_time' in result
        assert 'end_time' in result

    async def test_get_time_range_nonexistent(self):
        """测试获取不存在数据的时间范围"""
        result = await self.manager.get_time_range('non_existent_id')
        assert result is None

    async def test_get_time_range_no_storage(self):
        """测试无存储时获取时间范围"""
        manager = StorageManager()
        result = await manager.get_time_range('test_id')
        assert result is None


class TestMetadataOperations:
    """元数据操作测试"""

    def setup_method(self):
        """测试前初始化"""
        self.manager = StorageManager()
        self.mock_storage = MockStorage()
        self.manager.register_storage('mock', self.mock_storage)
        self.manager.set_default_storage('mock')

    async def test_get_metadata_success(self):
        """测试成功获取元数据"""
        await self.manager.save(
            'test_id', {'key': 'value'},
            metadata={'author': 'test'}
        )
        result = await self.manager.get_metadata('test_id')
        assert result is not None
        assert 'author' in result

    async def test_get_metadata_nonexistent(self):
        """测试获取不存在数据的元数据"""
        result = await self.manager.get_metadata('non_existent_id')
        assert result is None

    async def test_get_metadata_no_storage(self):
        """测试无存储时获取元数据"""
        manager = StorageManager()
        result = await manager.get_metadata('test_id')
        assert result is None

    async def test_update_metadata_success(self):
        """测试成功更新元数据"""
        await self.manager.save(
            'test_id', {'key': 'value'},
            metadata={'author': 'test'}
        )
        result = await self.manager.update_metadata(
            'test_id', {'author': 'updated'}
        )
        assert result is True
        metadata = await self.manager.get_metadata('test_id')
        assert metadata['author'] == 'updated'

    async def test_update_metadata_no_storage(self):
        """测试无存储时更新元数据"""
        manager = StorageManager()
        result = await manager.update_metadata('test_id', {'author': 'test'})
        assert result is False


class TestCacheIntegration:
    """缓存集成测试"""

    def setup_method(self):
        """测试前初始化"""
        self.manager = StorageManager()
        self.mock_storage = MockStorage()
        self.manager.register_storage('mock', self.mock_storage)
        self.manager.set_default_storage('mock')

    async def test_cache_hit(self):
        """测试缓存命中"""
        await self.manager.save('test_id', {'key': 'value'})
        data1 = await self.manager.load('test_id')
        assert data1 == {'key': 'value'}
        data2 = await self.manager.load('test_id')
        assert data2 == {'key': 'value'}

    async def test_cache_invalidation_on_delete(self):
        """测试删除后缓存失效"""
        await self.manager.save('test_id', {'key': 'value'})
        await self.manager.load('test_id')
        await self.manager.delete('test_id')
        data3 = await self.manager.load('test_id')
        assert data3 is None
