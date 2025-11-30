import pytest
import pandas as pd
import os
import shutil
from datetime import datetime
from chronoforge.storage import LocalFileStorage


class TestLocalFileStorage:
    """测试本地文件存储"""

    @pytest.fixture
    def test_dir(self):
        """创建临时测试目录"""
        test_dir = "./tmp/test_storage"
        # 清理之前的测试数据
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)
        # 创建新的测试目录
        os.makedirs(test_dir, exist_ok=True)
        yield test_dir
        # 测试完成后清理
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)

    @pytest.fixture
    def local_storage(self, test_dir):
        """创建本地文件存储实例"""
        config = {"base_path": test_dir}
        return LocalFileStorage(config)

    @pytest.fixture
    def test_dataframe(self):
        """创建测试数据框"""
        data = {
            "time": [datetime(2024, 1, 1), datetime(2024, 1, 2), datetime(2024, 1, 3)],
            "open": [100, 200, 300],
            "high": [110, 210, 310],
            "low": [90, 190, 290],
            "close": [105, 205, 305],
            "volume": [1000, 2000, 3000]
        }
        return pd.DataFrame(data)

    def test_initialization(self, test_dir):
        """测试本地文件存储初始化"""
        storage = LocalFileStorage(config={"base_path": test_dir})
        assert storage.config == {"base_path": test_dir}
        assert storage.name == "LocalFile"

    def test_initialization_with_data_format(self, test_dir):
        """测试使用不同数据格式初始化"""
        # 测试feather格式
        storage_feather = LocalFileStorage(config={"base_path": test_dir, "data_format": "feather"})
        assert storage_feather.data_format == "feather"

        # 测试parquet格式
        storage_parquet = LocalFileStorage(config={"base_path": test_dir, "data_format": "parquet"})
        assert storage_parquet.data_format == "parquet"

    @pytest.mark.asyncio
    async def test_save_and_load(self, local_storage, test_dataframe):
        """测试保存和加载数据"""
        # 保存数据
        save_result = await local_storage.save("test_symbol_1d", test_dataframe, sub="test_sub")
        assert save_result is True

        # 检查数据是否存在
        exists_result = await local_storage.exists("test_symbol_1d", sub="test_sub")
        assert exists_result is True

        # 加载数据
        loaded_data = await local_storage.load("test_symbol_1d", sub="test_sub")
        assert loaded_data is not None
        assert not loaded_data.empty
        assert len(loaded_data) == len(test_dataframe)

    @pytest.mark.asyncio
    async def test_exists_nonexistent(self, local_storage):
        """测试检查不存在的数据"""
        exists_result = await local_storage.exists("nonexistent_id", sub="nonexistent_sub")
        assert exists_result is False

    @pytest.mark.asyncio
    async def test_load_nonexistent(self, local_storage):
        """测试加载不存在的数据"""
        loaded_data = await local_storage.load("nonexistent_id", sub="nonexistent_sub")
        assert loaded_data is not None
        assert loaded_data.empty

    @pytest.mark.asyncio
    async def test_save_without_sub(self, local_storage, test_dataframe):
        """测试不使用sub参数保存数据"""
        # 保存数据
        save_result = await local_storage.save("test_symbol_1d", test_dataframe)
        assert save_result is True

        # 检查数据是否存在
        exists_result = await local_storage.exists("test_symbol_1d")
        assert exists_result is True

        # 加载数据
        loaded_data = await local_storage.load("test_symbol_1d")
        assert loaded_data is not None
        assert not loaded_data.empty

    @pytest.mark.asyncio
    async def test_lists_method(self, local_storage, test_dataframe):
        """测试lists方法"""
        # 先保存一些数据
        await local_storage.save("symbol1_1d", test_dataframe, sub="sub1")
        await local_storage.save("symbol2_1d", test_dataframe, sub="sub1")
        await local_storage.save("symbol1_1h", test_dataframe, sub="sub2")

        # 测试列出所有数据
        all_items = await local_storage.lists()
        assert isinstance(all_items, list)

        # 测试列出特定sub下的数据
        sub1_items = await local_storage.lists(sub="sub1")
        assert isinstance(sub1_items, list)

    @pytest.mark.asyncio
    async def test_delete_method(self, local_storage, test_dataframe):
        """测试delete方法"""
        # 保存数据
        await local_storage.save("test_symbol_1d", test_dataframe, sub="test_sub")

        # 检查数据是否存在
        assert await local_storage.exists("test_symbol_1d", sub="test_sub") is True

        # 删除数据
        delete_result = await local_storage.delete("test_symbol_1d", sub="test_sub")
        assert delete_result is True

        # 检查数据是否已删除
        assert await local_storage.exists("test_symbol_1d", sub="test_sub") is False

    @pytest.mark.asyncio
    async def test_different_formats(self, test_dir, test_dataframe):
        """测试不同的数据格式"""
        # 测试feather格式
        storage_feather = LocalFileStorage(config={"base_path": test_dir, "data_format": "feather"})
        await storage_feather.save("test_feather", test_dataframe)

        # 测试parquet格式
        storage_parquet = LocalFileStorage(config={"base_path": test_dir, "data_format": "parquet"})
        await storage_parquet.save("test_parquet", test_dataframe)

        # 检查两种格式的数据都存在
        assert await storage_feather.exists("test_feather") is True
        assert await storage_parquet.exists("test_parquet") is True
