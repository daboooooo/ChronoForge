"""MongoDB存储实现"""

import logging
from typing import Dict, List, Optional, Any
import pandas as pd
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import ConnectionFailure, OperationFailure
from .base import StorageBase

logger = logging.getLogger(__name__)


class MongoDBStorage(StorageBase):
    """MongoDB存储实现，用于将时间序列数据存储到MongoDB数据库中"""

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)

        # 配置默认值
        self.uri = self.config.get("uri", "mongodb://localhost:27017")
        self.db_name = self.config.get("db_name", "chronoforge")
        self.collection_prefix = self.config.get("collection_prefix", "data_")
        self.client = None
        self.db = None

        # 连接到MongoDB
        try:
            self._connect()
        except Exception as e:
            logger.warning(f"MongoDB连接失败 (将在第一次操作时重试): {str(e)}")

    @property
    def name(self):
        """返回存储插件名称"""
        return "MongoDBStorage"

    def _connect(self):
        """连接到MongoDB"""
        try:
            self.client = MongoClient(self.uri, serverSelectionTimeoutMS=5000)
            # 验证连接
            self.client.admin.command('ping')
            self.db = self.client[self.db_name]
            logger.info(f"成功连接到MongoDB: {self.uri}，数据库: {self.db_name}")
        except ConnectionFailure as e:
            logger.error(f"无法连接到MongoDB: {self.uri} - {str(e)}")
            raise
        except Exception as e:
            logger.error(f"MongoDB连接错误: {str(e)}")
            raise

    def _get_collection(self, sub: str = None):
        """获取MongoDB集合

        Args:
            sub: 子目录或子数据库，用于区分不同的数据集合

        Returns:
            pymongo.collection.Collection: MongoDB集合对象
        """
        # 确保连接存在
        if self.client is None or self.db is None:
            self._connect()

        collection_name = f"{self.collection_prefix}{sub}" if sub else self.collection_prefix
        return self.db[collection_name]

    async def save(
        self,
        id: str,
        data: pd.DataFrame,
        sub: str = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """保存数据到MongoDB

        Args:
            id: 数据ID
            data: 要保存的数据
            sub: 子目录或子数据库，用于区分不同的数据集合
            metadata: 数据元信息

        Returns:
            bool: 是否成功保存数据
        """
        if data.empty:
            logger.debug(f"空数据，跳过保存: {id}")
            return True

        try:
            collection = self._get_collection(sub)

            # 将DataFrame转换为字典列表
            records = data.to_dict(orient="records")

            # 为每个记录添加id字段
            for record in records:
                record["_id"] = f"{id}_{record['time']}"
                record["data_id"] = id

            # 使用bulk_write提高性能
            # 先删除旧数据
            collection.delete_many({"data_id": id})

            # 插入新数据
            if records:
                collection.insert_many(records)

            logger.debug(f"成功保存数据到MongoDB: {id}，记录数: {len(records)}")
            return True
        except OperationFailure as e:
            logger.error(f"MongoDB操作失败 - 保存数据: {id} - {str(e)}")
            return False
        except Exception as e:
            logger.error(f"保存数据到MongoDB失败: {id} - {str(e)}")
            return False

    async def load(
        self,
        id: str,
        sub: str = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Optional[pd.DataFrame]:
        """从MongoDB加载数据

        Args:
            id: 数据ID
            sub: 子目录或子数据库，用于区分不同的数据集合
            metadata: 数据元信息

        Returns:
            Optional[pandas.DataFrame]: 从MongoDB加载的数据
        """
        try:
            collection = self._get_collection(sub)

            # 查询数据
            cursor = collection.find({"data_id": id}, sort=[("time", ASCENDING)])

            # 将查询结果转换为DataFrame
            records = list(cursor)
            if not records:
                logger.debug(f"未找到数据: {id}")
                return None

            # 移除_id字段，将data_id字段转换为id列
            for record in records:
                record.pop("_id")
                record.pop("data_id")

            df = pd.DataFrame(records)

            # 确保time列是UTC时区的datetime类型
            if 'time' in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df['time']):
                    # 如果是datetime类型，确保它是UTC时区的
                    df['time'] = pd.to_datetime(df['time'], utc=True)
                else:
                    # 如果不是datetime类型，先转换为datetime再设置UTC时区
                    try:
                        df['time'] = pd.to_datetime(df['time'], utc=True)
                    except Exception as e:
                        # 如果转换失败，记录警告
                        logger.warning(f"无法将MongoDB中的time列转换为datetime: {id} - {str(e)}")

            logger.debug(f"成功从MongoDB加载数据: {id}，记录数: {len(df)}")
            return df
        except OperationFailure as e:
            logger.error(f"MongoDB操作失败 - 加载数据: {id} - {str(e)}")
            return None
        except Exception as e:
            logger.error(f"从MongoDB加载数据失败: {id} - {str(e)}")
            return None

    async def delete(
        self,
        id: str,
        sub: str = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """从MongoDB删除数据

        Args:
            id: 数据ID
            sub: 子目录或子数据库，用于区分不同的数据集合

        Returns:
            bool: 是否成功删除数据
        """
        try:
            collection = self._get_collection(sub)

            # 删除数据
            result = collection.delete_many({"data_id": id})

            logger.debug(f"成功从MongoDB删除数据: {id}，删除记录数: {result.deleted_count}")
            return True
        except OperationFailure as e:
            logger.error(f"MongoDB操作失败 - 删除数据: {id} - {str(e)}")
            return False
        except Exception as e:
            logger.error(f"从MongoDB删除数据失败: {id} - {str(e)}")
            return False

    async def exists(
        self,
        id: str,
        sub: str = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """检查MongoDB中是否存在数据

        Args:
            id: 数据ID
            sub: 子目录或子数据库，用于区分不同的数据集合

        Returns:
            bool: 存储介质是否存在数据
        """
        try:
            collection = self._get_collection(sub)

            # 检查数据是否存在
            count = collection.count_documents({"data_id": id}, limit=1)

            return count > 0
        except OperationFailure as e:
            logger.error(f"MongoDB操作失败 - 检查数据存在: {id} - {str(e)}")
            return False
        except Exception as e:
            logger.error(f"检查MongoDB数据存在失败: {id} - {str(e)}")
            return False

    async def lists(
        self,
        sub: str = None
    ) -> List[Dict[str, Any]]:
        """列出MongoDB中的所有数据

        Args:
            sub: 子目录或子数据库，用于区分不同的数据集合

        Returns:
            List[Dict[str, Any]]: 存储介质中的所有数据信息
        """
        try:
            collection = self._get_collection(sub)

            # 获取所有唯一的data_id
            data_ids = collection.distinct("data_id")

            # 为每个data_id获取时间范围
            data_list = []
            for data_id in data_ids:
                # 获取时间范围
                time_range = await self.get_time_range(data_id, sub)

                data_info = {
                    "id": data_id,
                    "start_time": time_range["start_time"] if time_range else None,
                    "end_time": time_range["end_time"] if time_range else None
                }
                data_list.append(data_info)

            logger.debug(f"成功列出MongoDB中的数据，数量: {len(data_list)}")
            return data_list
        except OperationFailure as e:
            logger.error(f"MongoDB操作失败 - 列出数据: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"列出MongoDB数据失败: {str(e)}")
            return []

    async def get_time_range(
        self,
        id: str,
        sub: str = None
    ) -> Optional[Dict[str, Any]]:
        """获取数据的时间范围

        Args:
            id: 数据ID
            sub: 子目录或子数据库，用于区分不同的数据集合

        Returns:
            Optional[Dict[str, Any]]: 数据的时间范围，包含start_time和end_time
        """
        try:
            collection = self._get_collection(sub)

            # 获取最小时间
            min_time_doc = collection.find_one(
                {"data_id": id},
                sort=[("time", ASCENDING)],
                projection={"time": 1, "_id": 0}
            )

            # 获取最大时间
            max_time_doc = collection.find_one(
                {"data_id": id},
                sort=[("time", DESCENDING)],
                projection={"time": 1, "_id": 0}
            )

            if not min_time_doc or not max_time_doc:
                return None

            return {
                "start_time": min_time_doc["time"],
                "end_time": max_time_doc["time"]
            }
        except OperationFailure as e:
            logger.error(f"MongoDB操作失败 - 获取时间范围: {id} - {str(e)}")
            return None
        except Exception as e:
            logger.error(f"从MongoDB获取时间范围失败: {id} - {str(e)}")
            return None

    async def get_metadata(
        self,
        id: str,
        sub: str = None
    ) -> Optional[Dict[str, Any]]:
        """
        获取数据元信息

        Args:
            id: 数据ID
            sub: 子目录或子数据库，用于区分不同的数据集合

        Returns:
            Optional[Dict[str, Any]]: 数据元信息
        """
        # 对于MongoDB存储，暂时返回空字典
        # 后续可以扩展为从单独的元数据集合中加载
        logger.debug("获取数据元信息: %s/%s", sub or 'root', id)
        return {}

    async def update_metadata(
        self,
        id: str,
        metadata: Dict[str, Any],
        sub: str = None
    ) -> bool:
        """
        更新数据元信息

        Args:
            id: 数据ID
            metadata: 要更新的元信息
            sub: 子目录或子数据库，用于区分不同的数据集合

        Returns:
            bool: 是否成功更新元信息
        """
        # 对于MongoDB存储，暂时返回True
        # 后续可以扩展为将元数据保存到单独的集合中
        logger.debug("更新数据元信息: %s/%s", sub or 'root', id)
        return True

    async def close(self):
        """关闭MongoDB连接"""
        try:
            if self.client:
                self.client.close()
                logger.info(f"成功关闭MongoDB连接: {self.uri}")
                self.client = None
                self.db = None
        except Exception as e:
            logger.error(f"关闭MongoDB连接失败: {str(e)}")
            raise

    async def __aenter__(self):
        """异步上下文管理器进入方法"""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器退出方法"""
        await self.close()
