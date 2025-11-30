"""存储层 - Redis时间序列数据存储实现"""

import asyncio
import json
import logging
from typing import Dict, List, Optional, Any

import pandas as pd
import redis.asyncio as redis

from .base import StorageBase
from chronoforge.utils import format_size

logger = logging.getLogger(__name__)


class RedisStorage(StorageBase):
    """Redis存储插件，支持高效的时间序列数据存储和查询"""

    def __init__(self, config: Dict[str, Any] = None):
        """
        初始化Redis存储

        Args:
            config: 配置字典，包含Redis连接参数和可选的key_prefix
        """
        super().__init__(config)

        # 默认配置
        default_config = {
            'connection_url': 'redis://localhost:6379/0',
            'key_prefix': 'chronoforge:',
            'password': None,
        }

        # 更新配置
        self.config = {**default_config, **(config or {})}

        # Redis连接
        self._connection = None
        self._key_prefix = self.config['key_prefix']

    @property
    def name(self) -> str:
        """返回存储插件名称"""
        return "Redis"

    async def _get_connection(self) -> redis.Redis:
        """获取Redis连接"""
        if self._connection is None:
            try:
                self._connection = redis.from_url(
                    self.config['connection_url'],
                    password=self.config['password'],
                    decode_responses=False  # 保持原始字节，这样我们可以控制解码
                )
                logger.debug("成功连接到Redis: %s", self.config['connection_url'])
            except Exception as e:
                logger.error("连接Redis失败: %s", str(e))
                raise ConnectionError(f"Redis连接失败: {str(e)}")
        return self._connection

    async def _close(self):
        """关闭Redis连接"""
        if self._connection:
            await self._connection.close()
            self._connection = None
            logger.debug("关闭了Redis连接")

    def __del__(self):
        """析构函数，清理资源"""
        try:
            # 同步地关闭连接，避免异步析构函数的警告
            if self._connection:
                try:
                    # 首先尝试使用当前事件循环（如果存在且未关闭）
                    loop = asyncio.get_event_loop()
                    if not loop.is_closed():
                        # 创建task并同步运行
                        task = loop.create_task(self._close())
                        # 等待task完成，但不阻塞过长时间
                        loop.run_until_complete(asyncio.wait_for(task, timeout=1.0))
                    else:
                        # 当前循环已关闭，创建新的循环
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        try:
                            loop.run_until_complete(self._close())
                        finally:
                            loop.close()
                except RuntimeError:
                    # 没有事件循环，创建新的循环
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        loop.run_until_complete(self._close())
                    finally:
                        loop.close()
                except Exception:
                    # 忽略所有其他错误，避免在析构时崩溃
                    pass
                finally:
                    self._connection = None
        except Exception:
            # 忽略析构函数中的所有错误
            pass

    async def __aenter__(self):
        """异步上下文管理器的进入方法"""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器的退出方法，确保所有Redis连接都被正确关闭"""
        await self._close()
        return False

    def _build_key(self, table_name: str) -> str:
        """构建带前缀的Redis键"""
        return f"{self._key_prefix}{table_name}"

    def _normalize_table_name(self, id: str, sub: str = None) -> str:
        """
        标准化表名, 确保符合Redis命名规范

        Args:
            id: 数据ID
            sub: 子目录或子数据库

        Returns:
            str: 标准化的表名
        """
        # 清理和标准化表名
        table_name = id.replace("/", "_").replace("-", "_").replace(".", "_")

        if sub:
            sub_normalized = sub.replace("/", "_").replace("-", "_").replace(".", "_")
            table_name = f"{sub_normalized}:{table_name}"

        return table_name

    async def save(
        self,
        id: str,
        data: pd.DataFrame,
        sub: str = None
    ) -> bool:
        """
        保存DataFrame到Redis

        Args:
            id: 数据ID，用作键的一部分
            data: 要保存的数据
            sub: 子目录或子数据库，用作键前缀

        Returns:
            bool: 是否成功保存数据
        """
        if data.empty:
            logger.warning("尝试保存空数据! sub: %s, id: %s", sub or 'unknown', id)
            return True  # 空数据也认为保存成功

        try:
            conn = await self._get_connection()
            table_name = self._normalize_table_name(id, sub)
            redis_key = self._build_key(table_name)

            # 准备数据
            data_to_save = data.copy()

            # 确保time列以UTC时区保存
            if 'time' in data_to_save.columns:
                if data_to_save['time'].dt.tz is not None:
                    # 如果已经有时区信息，转换为UTC
                    data_to_save['time'] = data_to_save['time'].dt.tz_convert('UTC')
                else:
                    # 如果没有时区信息，直接设为UTC
                    data_to_save['time'] = pd.to_datetime(data_to_save['time'], utc=True)

            # 转换为JSON字符串，保留时间信息
            data_json = data_to_save.to_json(orient='records', date_format='iso',
                                             date_unit='ms', index=False)

            # 使用管道操作确保原子性
            async with conn.pipeline() as pipe:
                # 删除可能存在的旧数据
                pipe.delete(redis_key)

                # 保存数据
                pipe.set(redis_key, data_json)

                # 如果有时间列，创建时间索引
                if 'time' in data_to_save.columns:
                    # 创建时间戳索引键
                    time_index_key = f"{redis_key}:time_index"

                    # 清除旧索引
                    pipe.delete(time_index_key)

                    # 添加时间戳到有序集合
                    timestamps = []
                    for i, record in enumerate(json.loads(data_json)):
                        if 'time' in record:
                            # 将ISO时间字符串转换为时间戳
                            time_val = record['time']
                            try:
                                if isinstance(time_val, str):
                                    timestamp = pd.to_datetime(time_val).timestamp()
                                else:
                                    # 如果是数字（毫秒时间戳），转换为秒
                                    timestamp = time_val / 1000.0 if time_val > 1e10 else time_val
                                # 使用记录索引作为成员，确保唯一性
                                timestamps.append((timestamp, str(i)))
                            except Exception as e:
                                logger.debug("转换时间戳失败: %s", str(e))

                    if timestamps:
                        # 批量添加时间戳索引
                        pipe.zadd(time_index_key, dict(timestamps))

                # 执行管道中的所有命令
                results = await pipe.execute()
                # Redis命令返回0表示没有错误（比如删除不存在的键），所以检查是否有False值
                success = all(result is not False for result in results)

            logger.debug("成功保存数据到Redis键 %s，共 %s 条记录", redis_key, len(data))
            return success

        except Exception as e:
            logger.error("保存数据失败: %s", str(e))
            return False

    async def load(
        self,
        id: str,
        sub: str = None
    ) -> Optional[pd.DataFrame]:
        """
        从Redis加载DataFrame

        Args:
            id: 数据ID，用作键的一部分
            sub: 子目录或子数据库，用作键前缀

        Returns:
            Optional[pd.DataFrame]: 加载的数据，如果不存在返回None
        """
        try:
            conn = await self._get_connection()
            table_name = self._normalize_table_name(id, sub)
            redis_key = self._build_key(table_name)

            # 检查键是否存在
            exists = await conn.exists(redis_key)
            if not exists:
                logger.debug("Redis键 %s 不存在", redis_key)
                return None

            # 获取数据
            raw_data = await conn.get(redis_key)
            if not raw_data:
                logger.debug("Redis键 %s 为空", redis_key)
                return None

            # 解码和解析JSON
            try:
                if isinstance(raw_data, bytes):
                    data_str = raw_data.decode('utf-8')
                else:
                    data_str = str(raw_data)

                data_json = json.loads(data_str)

                # 转换为DataFrame
                data = pd.DataFrame(data_json)

                # 转换时间列为UTC datetime
                if 'time' in data.columns:
                    if data['time'].dtype == 'object':
                        # 如果是字符串，转换为datetime并设置为UTC
                        data['time'] = pd.to_datetime(data['time'], utc=True)
                    elif pd.api.types.is_datetime64_any_dtype(data['time']):
                        # 如果已经是datetime类型，确保为UTC
                        if data['time'].dt.tz is None:
                            data['time'] = data['time'].dt.tz_localize('UTC')
                        else:
                            data['time'] = data['time'].dt.tz_convert('UTC')
                    else:
                        # 其他情况，尝试转换为UTC datetime
                        data['time'] = pd.to_datetime(data['time'], utc=True)

                logger.debug("成功从Redis键 %s 加载 %s 条记录", redis_key, len(data))
                return data

            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.error("解析Redis数据失败: %s", str(e))
                return None

        except Exception as e:
            logger.error("加载数据失败: %s", str(e))
            return None

    async def delete(
        self,
        id: str,
        sub: str = None
    ) -> bool:
        """
        从Redis删除数据

        Args:
            id: 数据ID，用作键的一部分
            sub: 子目录或子数据库，用作键前缀

        Returns:
            bool: 是否成功删除数据
        """
        try:
            conn = await self._get_connection()
            table_name = self._normalize_table_name(id, sub)
            redis_key = self._build_key(table_name)

            # 检查键是否存在
            exists = await conn.exists(redis_key)
            if not exists:
                logger.debug("Redis键 %s 不存在，无需删除", redis_key)
                return True

            # 删除相关键（数据和索引）
            keys_to_delete = [redis_key, f"{redis_key}:time_index"]

            deleted_count = await conn.delete(*keys_to_delete)

            logger.debug("成功删除Redis键 %s 及相关索引", redis_key)
            return deleted_count > 0

        except Exception as e:
            logger.error("删除数据失败: %s", str(e))
            return False

    async def exists(
        self,
        id: str,
        sub: str = None
    ) -> bool:
        """
        检查Redis中的数据是否存在

        Args:
            id: 数据ID，用作键的一部分
            sub: 子目录或子数据库，用作键前缀

        Returns:
            bool: 数据是否存在
        """
        try:
            conn = await self._get_connection()
            table_name = self._normalize_table_name(id, sub)
            redis_key = self._build_key(table_name)

            exists = await conn.exists(redis_key)

            logger.debug("检查Redis键 %s 是否存在: %s", redis_key, exists)
            return bool(exists)

        except Exception as e:
            logger.error("检查数据存在性失败: %s", str(e))
            return False

    async def lists(
        self,
        sub: str = None
    ) -> List[Dict[str, Any]]:
        """
        列出Redis中的所有数据

        Args:
            sub: 子目录或子数据库，用作键前缀过滤

        Returns:
            List[Dict[str, Any]]: 数据信息列表
        """
        try:
            conn = await self._get_connection()
            # 构建搜索模式
            if sub:
                sub_normalized = sub.replace("/", "_").replace("-", "_").replace(".", "_")
                pattern = f"{self._key_prefix}{sub_normalized}:*"
            else:
                pattern = f"{self._key_prefix}*"

            # 使用管道操作优化性能
            pipe = conn.pipeline()
            pipe.keys(pattern)

            # 获取匹配的键
            keys_result = await pipe.execute()
            keys = keys_result[0] if keys_result else []

            if not keys:
                logger.debug("没有找到匹配的Redis键")
                return []

            # 预处理键，转换为字符串并过滤
            valid_keys = []
            processed_keys = set()

            for key in keys:
                if isinstance(key, bytes):
                    key_str = key.decode('utf-8')
                else:
                    key_str = str(key)

                # 跳过时间索引键
                if key_str.endswith(':time_index'):
                    continue

                # 避免重复处理
                if key_str in processed_keys:
                    continue
                processed_keys.add(key_str)
                valid_keys.append(key_str)

            if not valid_keys:
                logger.debug("过滤后没有有效的数据键")
                return []

            # 使用管道批量获取数据，减少网络往返
            pipe = conn.pipeline()
            for key in valid_keys:
                pipe.get(key)
                pipe.strlen(key)  # 获取键值长度

            # 执行管道
            pipe_results = await pipe.execute()

            tables = []

            # 处理管道结果
            for i, key_str in enumerate(valid_keys):
                data = pipe_results[i * 2]  # GET结果
                size = pipe_results[i * 2 + 1]  # STRLEN结果

                try:
                    # 解析键名
                    prefix_len = len(self._key_prefix)
                    table_name = key_str[prefix_len:] if key_str.startswith(self._key_prefix) \
                        else key_str

                    # 分离sub和id
                    parts = table_name.split(':', 1)
                    if len(parts) > 1:
                        table_sub = parts[0]
                        table_id = parts[1]
                    else:
                        table_sub = None
                        table_id = table_name

                    # 估算行数（仅对有效数据）
                    row_count = 0
                    if data:
                        if isinstance(data, bytes):
                            try:
                                data_str = data.decode('utf-8')
                                data_json = json.loads(data_str)
                                if isinstance(data_json, list):
                                    row_count = len(data_json)
                                elif isinstance(data_json, dict) and 'records' in data_json:
                                    row_count = len(data_json['records'])
                                elif isinstance(data_json, dict):
                                    # 尝试从数据结构推断行数
                                    row_count = 1
                            except (json.JSONDecodeError, UnicodeDecodeError):
                                pass

                    # 格式化大小
                    size_pretty = format_size(size) if size else "0 B"

                    # 添加创建时间（如果可能）
                    creation_time = None
                    try:
                        # 尝试获取键的创建时间
                        ttl = await conn.ttl(key_str)
                        if ttl > 0:
                            # TTL大于0说明键有设置过期时间
                            creation_time = "unknown"
                    except Exception as e:
                        logger.debug("获取键 %s 创建时间时出错: %s", key_str, str(e))
                        creation_time = "unknown"

                    table_info = {
                        'id': table_id,
                        'sub': table_sub,
                        'table_name': table_name,
                        'column_count': 0,  # 无法直接从Redis获取列数
                        'row_count': row_count,
                        'size': size_pretty,
                        'creation_time': creation_time,
                        'has_time_index': key_str.endswith(':time_index')
                    }

                    tables.append(table_info)

                except Exception as e:
                    logger.debug("处理键 %s 时出错: %s", key_str, str(e))
                    # 添加基本信息以避免完全丢失数据
                    tables.append({
                        'id': key_str,
                        'sub': None,
                        'table_name': key_str,
                        'column_count': 0,
                        'row_count': 0,
                        'size': format_size(size) if size else "0 B",
                        'creation_time': 'unknown',
                        'has_time_index': key_str.endswith(':time_index'),
                        'error': str(e)
                    })
                    continue

            # 按表名排序，提高一致性
            tables.sort(key=lambda x: (x.get('sub') or '', x.get('id', '')))

            return tables

        except asyncio.CancelledError:
            logger.warning("列出Redis数据操作被取消")
            raise
        except Exception as e:
            # 专门处理事件循环关闭错误
            error_msg = str(e)
            if "Event loop is closed" in error_msg:
                logger.warning("事件循环已关闭，无法列出Redis数据")
            else:
                logger.error("列出数据失败: %s", error_msg)
            return []
