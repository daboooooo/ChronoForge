"""存储层 - 管理时间序列数据的持久化和更新逻辑"""

import logging
import re
from pathlib import Path
from typing import Dict, List, Optional, Any

import duckdb
import pandas as pd

from .base import StorageBase
from chronoforge.utils import format_size

logger = logging.getLogger(__name__)


class DUCKDBStorage(StorageBase):
    """DuckDB存储插件，支持高效的时间序列数据存储和查询"""

    def __init__(self, config: Dict[str, Any] = None):
        """
        初始化DuckDB存储

        Args:
            config: 配置字典，包含数据库路径等参数
        """
        super().__init__(config)

        # 数据库文件路径配置
        self.db_path = Path(config.get("db_path", "./chronoforge.db"))
        self.db_path = self.db_path.absolute()

        # 确保数据库目录存在
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # 数据库连接
        self.connection = None

    @property
    def name(self) -> str:
        """返回存储插件名称"""
        return "DuckDB"

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """获取数据库连接"""
        if self.connection is None:
            try:
                self.connection = duckdb.connect(str(self.db_path))
                logger.debug("成功连接到DuckDB数据库: %s", self.db_path)
            except Exception as e:
                logger.error("连接DuckDB数据库失败: %s", str(e))
                raise
        return self.connection

    async def _close(self):
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.debug("关闭了DuckDB数据库连接")

    def __del__(self):
        """析构函数，清理资源"""
        # 在析构函数中直接关闭连接，避免异步调用警告
        if self.connection:
            try:
                self.connection.close()
                self.connection = None
                logger.debug("关闭了DuckDB数据库连接")
            except Exception as e:
                logger.warning("关闭数据库连接时出错: %s", str(e))

    async def __aenter__(self):
        """异步上下文管理器的进入方法"""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器的退出方法，确保所有exchange连接都被正确关闭"""
        await self._close()
        return False  # 不抑制异常

    def _normalize_table_name(self, id: str, sub: str = None) -> str:
        """
        标准化表名，确保符合DuckDB命名规范

        Args:
            id: 数据ID
            sub: 子目录或子数据库

        Returns:
            str: 标准化的表名
        """
        # 移除或替换不符合标识符规范的字符
        uncompactable_chars = ['/', '-', '.', '=', ':', '^']
        for char in uncompactable_chars:
            id = id.replace(char, '_')
        table_name = id
        # 如果有子目录，添加到表名前缀
        if sub:
            sub_normalized = sub.replace("/", "_").replace("-", "_").replace(".", "_")
            table_name = f"{sub_normalized}_{table_name}"

        # 确保以字母或下划线开头
        if not re.match(r'^[a-zA-Z_]', table_name):
            table_name = f"tbl_{table_name}"

        return table_name.lower()

    async def save(
        self,
        id: str,
        data: pd.DataFrame,
        sub: str = None
    ) -> bool:
        """
        保存数据到DuckDB数据库

        Args:
            id: 数据ID，用作表名的一部分
            data: 要保存的数据
            sub: 子目录或子数据库，用作表名前缀

        Returns:
            bool: 是否成功保存数据
        """
        if data.empty:
            logger.warning("尝试保存空数据! sub: %s, id: %s", sub or 'unknown', id)
            return True  # 空数据也认为保存成功

        try:
            conn = self._get_connection()
            table_name = self._normalize_table_name(id, sub)

            # 如果表已存在，先删除
            try:
                conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception as e:
                logger.warning("删除旧表失败: %s", str(e))

            # 确保time列以UTC时区保存
            data_to_save = data.copy()
            if 'time' in data_to_save.columns:
                # 强制转换为UTC时区，然后去除时区标记（存储为无时区的UTC时间戳）
                if data_to_save['time'].dt.tz is not None:
                    # 如果已经有时区信息，转换为UTC
                    data_to_save['time'] = \
                        data_to_save['time'].dt.tz_convert('UTC').dt.tz_localize(None)
                else:
                    # 如果没有时区信息，直接设为UTC并去除时区标记
                    data_to_save['time'] = \
                        pd.to_datetime(data_to_save['time'], utc=True).dt.tz_localize(None)

            # 注册DataFrame并创建表
            conn.register('df_view', data_to_save)
            conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df_view")
            conn.unregister('df_view')

            logger.debug("成功保存数据到表 %s，共 %s 条记录", table_name, len(data))
            return True

        except Exception as e:
            logger.error("保存数据失败: %s", str(e))
            return False

    async def load(
        self,
        id: str,
        sub: str = None
    ) -> Optional[pd.DataFrame]:
        """
        从DuckDB数据库加载数据

        Args:
            id: 数据ID，用作表名的一部分
            sub: 子目录或子数据库，用作表名前缀

        Returns:
            Optional[pd.DataFrame]: 加载的数据，如果不存在返回None
        """
        try:
            conn = self._get_connection()
            table_name = self._normalize_table_name(id, sub)

            # 检查表是否存在
            result = conn.execute("""
                SELECT COUNT(*) as count FROM information_schema.tables
                WHERE table_name = ?
            """, [table_name]).fetchone()

            if result[0] == 0:
                logger.debug("表 %s 不存在", table_name)
                return None

            # 加载数据
            data = conn.execute(f"SELECT * FROM {table_name}").fetchdf()
            logger.debug("成功从表 %s 加载 %s 条记录", table_name, len(data))

            # 转换时间列为UTC时区
            if 'time' in data.columns:
                # DuckDB以TIMESTAMP_NS或TIMESTAMP WITH TIME ZONE存储，转换为pandas UTC datetime
                if pd.api.types.is_datetime64_any_dtype(data['time']):
                    # 如果是datetime类型，强制设为UTC时区
                    if data['time'].dt.tz is None:
                        # 如果没有时区信息，设为UTC
                        data['time'] = data['time'].dt.tz_localize('UTC')
                    else:
                        # 如果有时区信息，转换为UTC
                        data['time'] = data['time'].dt.tz_convert('UTC')
                else:
                    # 如果不是datetime类型，先转换为datetime再设为UTC
                    data['time'] = pd.to_datetime(data['time'], utc=True)

            return data

        except Exception as e:
            logger.error("加载数据失败: %s", str(e))
            return None

    async def delete(
        self,
        id: str,
        sub: str = None
    ) -> bool:
        """
        从DuckDB数据库删除数据

        Args:
            id: 数据ID，用作表名的一部分
            sub: 子目录或子数据库，用作表名前缀

        Returns:
            bool: 是否成功删除数据
        """
        try:
            conn = self._get_connection()
            table_name = self._normalize_table_name(id, sub)

            # 检查表是否存在
            result = conn.execute("""
                SELECT COUNT(*) as count FROM information_schema.tables
                WHERE table_name = ?
            """, [table_name]).fetchone()

            if result[0] == 0:
                logger.debug("表 %s 不存在，无需删除", table_name)
                return True

            # 删除表
            conn.execute(f"DROP TABLE {table_name}")
            logger.debug("成功删除表 %s", table_name)

            return True

        except Exception as e:
            logger.error("删除数据失败: %s", str(e))
            return False

    async def exists(
        self,
        id: str,
        sub: str = None
    ) -> bool:
        """
        检查数据是否存在

        Args:
            id: 数据ID，用作表名的一部分
            sub: 子目录或子数据库，用作表名前缀

        Returns:
            bool: 数据是否存在
        """
        try:
            conn = self._get_connection()
            table_name = self._normalize_table_name(id, sub)

            result = conn.execute("""
                SELECT COUNT(*) as count FROM information_schema.tables
                WHERE table_name = ?
            """, [table_name]).fetchone()

            exists = result[0] > 0
            logger.debug("检查表 %s 是否存在: %s", table_name, exists)

            return exists

        except Exception as e:
            logger.error("检查数据存在性失败: %s", str(e))
            return False

    async def lists(
        self,
        sub: str = None
    ) -> List[Dict[str, Any]]:
        """
        列出数据库中的所有数据表

        Args:
            sub: 子目录或子数据库，用作表名前缀过滤

        Returns:
            List[Dict[str, Any]]: 数据表信息列表
        """
        try:
            conn = self._get_connection()

            # 构建查询条件
            if sub:
                sub_normalized = sub.replace("/", "_").replace("-", "_").replace(".", "_").lower()
                query = """
                    SELECT table_name,
                           (SELECT COUNT(*) FROM information_schema.columns
                            WHERE table_name = t.table_name) as column_count
                    FROM information_schema.tables t
                    WHERE table_schema = 'main'
                      AND table_name LIKE ?
                      AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """
                params = [f"{sub_normalized}_%"]
            else:
                query = """
                    SELECT table_name,
                           (SELECT COUNT(*) FROM information_schema.columns
                            WHERE table_name = t.table_name) as column_count
                    FROM information_schema.tables t
                    WHERE table_schema = 'main'
                      AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """
                params = []

            result = conn.execute(query, params).fetchall()

            tables = []
            for row in result:
                table_name, column_count = row

                # 获取表的大小信息 - DuckDB版本
                try:
                    # 首先获取行数
                    count_result = conn.execute(f"""
                        SELECT COUNT(*) as row_count FROM {table_name}
                    """).fetchone()

                    row_count = count_result[0] if count_result and count_result[0] is not None \
                        else 0

                    # 计算大概的字节大小（通过估算行的大小）
                    try:
                        if row_count > 0:
                            # 获取一行样本来估算大小
                            sample_row = conn.execute(f"""
                                SELECT * FROM {table_name} LIMIT 1
                            """).fetchdf()

                            if len(sample_row) > 0:
                                # 估算单行大小
                                approx_row_size = len(sample_row.to_json()) / len(sample_row)
                                byte_size = int(approx_row_size * row_count)
                            else:
                                byte_size = row_count * 100  # 假设每行100字节
                        else:
                            byte_size = 0
                    except Exception as e:
                        logger.debug("估算表 %s 大小失败: %s", table_name, str(e))
                        # 如果无法估算，使用固定的字节大小
                        byte_size = row_count * 100  # 假设每行100字节

                except Exception as e:
                    logger.debug("获取表 %s 大小失败: %s", table_name, str(e))
                    row_count = 0
                    byte_size = 0

                # 解析sub和id
                table_parts = table_name.split('_', 1)
                if len(table_parts) > 1 and sub:
                    # 如果指定了sub，检查是否匹配
                    actual_sub = table_parts[0]
                    actual_id = table_parts[1]
                    if actual_sub == sub_normalized:
                        tables.append({
                            'id': actual_id,
                            'sub': sub,
                            'table_name': table_name,
                            'column_count': column_count,
                            'row_count': row_count,
                            'size': format_size(byte_size)
                        })
                elif not sub:
                    # 没有指定sub，返回所有表
                    tables.append({
                        'id': table_name,
                        'sub': None,
                        'table_name': table_name,
                        'column_count': column_count,
                        'row_count': row_count,
                        'size': format_size(byte_size)
                    })

            logger.debug("列出 %s 个数据表", len(tables))
            return tables

        except Exception as e:
            logger.error("列出数据表失败: %s", str(e))
            return []
