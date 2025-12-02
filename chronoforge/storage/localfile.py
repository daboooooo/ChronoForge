import logging
import os
import shutil
from pathlib import Path
from typing import Dict, Any, List, Optional
import pandas as pd
from datetime import datetime

from .base import StorageBase
from chronoforge.utils import format_size

logger = logging.getLogger(__name__)


class LocalFileStorage(StorageBase):
    """本地文件存储插件，支持多种数据格式的读写"""

    # 支持的数据格式及其文件扩展名
    SUPPORTED_FORMATS = {
        'feather': '.feather',
        'parquet': '.parquet',
        'json': '.json',
        'jsongz': '.json.gz'
    }

    def __init__(self,
                 config: Dict[str, Any] = None):
        """
        初始化数据存储模块

        Args:
            config: 配置字典，包含数据目录和数据格式
        """
        # 确保config不是None
        config = config or {}
        # 初始化父类
        super().__init__(config)

        # 从参数或配置中获取数据目录
        self.datadir = Path(config.get("datadir", "./data"))
        self.data_format = config.get("data_format", "feather")
        self.extension = self.SUPPORTED_FORMATS[self.data_format]

        # 确保数据目录存在
        self.datadir.mkdir(parents=True, exist_ok=True)

    @property
    def name(self):
        """返回存储插件名称"""
        return self.__class__.__name__.replace("Storage", "")

    def _get_file_path(
        self,
        id: str,
        sub: str = None,
        create_subdir: bool = True
    ) -> Path:
        """
        获取数据文件的路径，确保始终使用绝对路径，不依赖于当前工作目录

        Args:
            id: 数据ID，用于构建文件名
            sub: 子目录路径（可选）
            create_subdir: 是否创建子目录，默认True

        Returns:
            Path: 数据文件的绝对路径对象
        """
        # 构建基础路径
        base_path = Path(self.datadir)

        # 确保基础路径是绝对路径
        if not base_path.is_absolute():
            # 获取项目根目录作为参考点，而不是依赖当前工作目录
            # 这确保无论从哪个目录运行脚本，路径都能正确解析
            import __main__  # noqa: C0415
            main_script_dir = Path(__main__.__file__).parent if hasattr(__main__, '__file__') \
                else Path.cwd()
            # 检查是否在examples目录下运行
            if 'examples' in str(main_script_dir):
                # 如果在examples目录下运行，使用父目录作为参考点
                project_root = main_script_dir.parent
                base_path = (project_root / base_path).absolute()
            else:
                # 否则使用当前工作目录
                base_path = base_path.absolute()

        # 如果提供了子目录参数，添加子目录
        if sub:
            base_path = base_path / sub
            # 只有在create_subdir为True时才创建子目录
            if create_subdir:
                # 确保子目录存在
                base_path.mkdir(parents=True, exist_ok=True)

        # 确定文件格式，优先使用file_format，同时保持向后兼容性
        # 构建文件名 - 保持与测试的兼容性
        id_str = id.replace("/", "_")
        filename = f'{id_str}{self.extension}'

        # 构建完整路径
        file_path = base_path / filename

        # 返回绝对路径
        abs_path = file_path.absolute()
        logger.debug("绝对路径: %s", abs_path)
        return abs_path

    async def save(
        self,
        id: str,
        data: pd.DataFrame,
        sub: str = None
    ) -> bool:  # noqa: R0917
        """
        保存数据（同步方法，向后兼容并符合StorageBase接口）

        Args:
            id: 数据ID
            data: 要保存的数据
            sub: 子目录或子数据库，用于区分不同的数据集合

        Returns:
            bool: 是否成功保存数据
        """
        if data.empty:
            logger.warning(
                "尝试保存空数据! sub: %s, id: %s", sub or 'unknown', id)
            return True

        # 使用_get_file_path方法获取文件路径，确保路径一致性
        # 不自动创建子目录，由save方法手动处理以正确捕获权限错误
        file_path = self._get_file_path(id, sub, create_subdir=False)

        # 转换为字符串路径以便使用os.path函数
        file_path_str = str(file_path)

        # 检查文件是否已经存在
        file_already_exists = os.path.isfile(file_path_str)
        logger.debug("文件是否已存在: %s", file_already_exists)

        # 确保目录存在
        base_dir = os.path.dirname(file_path_str)
        if not os.path.exists(base_dir):
            logger.debug("尝试创建目录: %s", base_dir)
            try:
                os.makedirs(base_dir, exist_ok=True)
            except (PermissionError, OSError) as e:
                logger.error("创建目录失败，没有写权限: %s", str(e))
                return False

        # 尝试保存文件
        try:
            # 根据数据格式保存文件
            if self.data_format == 'feather':
                data.to_feather(file_path_str)
            elif self.data_format == 'parquet':
                data.to_parquet(file_path_str)
            elif self.data_format == 'json':
                data.to_json(file_path_str, orient='records', lines=True)
            elif self.data_format == 'jsongz':
                actual_path = file_path_str.replace('.jsongz', '.json.gz')
                data.to_json(actual_path, orient='records', lines=True,
                             compression='gzip')
                # 为了兼容性，创建副本
                try:
                    shutil.copyfile(actual_path, file_path_str)
                except Exception as e:
                    logger.warning("创建副本失败: %s", str(e))

                # 成功保存后记录日志
            if file_already_exists:
                logger.debug("更新保存数据: %s/%s %s", sub, id, self.data_format)
            else:
                logger.debug("首次保存数据: %s/%s %s", sub, id, self.data_format)

            logger.debug("成功保存数据到 %s，共 %s 条记录", file_path_str, len(data))
            return True
        except Exception as e:
            logger.error("保存数据失败: %s", str(e))
            return False

    async def load(
        self,
        id: str,
        sub: str = None
    ) -> Optional[pd.DataFrame]:  # noqa: R0917
        """
        加载数据

        Args:
            id: 数据ID
            sub: 子目录或子数据库，用于区分不同的数据集合

        Returns:
            Optional[pd.DataFrame]: 加载的数据，如果不存在则返回空DataFrame
        """
        # 获取文件路径
        file_path = self._get_file_path(id, sub)
        file_path = file_path.absolute()
        # 添加详细日志
        file_exists = os.path.exists(file_path)
        logger.debug(
            "加载数据: %s/%s %s, 文件路径: %s, 存在: %s",
            sub, id, self.data_format, file_path, file_exists
        )  # noqa: R0917
        if file_exists:
            logger.debug("文件大小: %s", os.path.getsize(file_path))

        # 检查文件是否存在
        if not file_exists:
            logger.warning("数据文件不存在: %s", file_path)
            return pd.DataFrame()

        try:
            # 根据文件扩展名确定加载方式
            suffix = file_path.suffix

            if suffix == '.feather':
                data = pd.read_feather(file_path)
            elif suffix == '.parquet':
                data = pd.read_parquet(file_path)
            elif suffix == '.json':
                data = pd.read_json(file_path, orient='records', lines=True)
            # 对于.gz文件和jsongz格式，使用正确的方式加载
            elif suffix == '.gz' or file_path.suffix == '.jsongz':
                # 如果是.jsongz后缀，替换为.json.gz
                actual_path = str(file_path).replace('.jsongz', '.json.gz')
                # 使用pandas内置的compression参数处理gzip压缩
                data = pd.read_json(actual_path, orient='records', lines=True, compression='gzip')
            else:
                raise ValueError(f"未知的文件格式: {suffix}")

            # 创建数据副本，避免修改原始数据
            data_copy = data.copy()

            # 确保timestamp列转换为整数类型（如果存在）
            if ('timestamp' in data_copy.columns and
                    pd.api.types.is_datetime64_any_dtype(data_copy['timestamp'])):
                # 转换为毫秒级时间戳
                data_copy['timestamp'] = data_copy['timestamp'].astype('int64') // 10**6

            # 确保value列转换为浮点数类型（如果存在）
            if 'value' in data_copy.columns and pd.api.types.is_integer_dtype(data_copy['value']):
                data_copy['value'] = data_copy['value'].astype('float64')

            return data_copy
        except Exception as e:
            logger.error("加载数据失败: %s", str(e))
            return pd.DataFrame()

    async def delete(self, id: str, sub: str = None) -> Optional[bool]:
        """
        删除数据（同步方法，向后兼容并符合StorageBase接口）

        Args:
            id: 数据ID
            sub: 子目录或子数据库，用于区分不同的数据集合

        Returns:
            Optional[bool]: 如果数据存在并成功删除返回True，如果删除失败返回False，如果数据不存在返回None
        """
        # 使用更新后的_get_file_path方法
        file_path = self._get_file_path(id, sub)
        if file_path.exists():
            try:
                file_path.unlink()
                logger.debug("成功删除数据文件: %s", file_path)
                return True
            except Exception as e:
                logger.error("删除数据文件失败: %s", str(e))
                return False
        else:
            logger.debug("数据文件不存在: %s", file_path)
            return None

    async def exists(
        self,
        id: str,
        sub: str = None
    ) -> bool:
        """
        检查数据是否存在（同步方法，向后兼容并符合StorageBase接口）

        Args:
            id: 数据ID
            sub: 子目录或子数据库，用于区分不同的数据集合

        Returns:
            bool: 数据是否存在
        """
        # 使用更新后的_get_file_path方法
        file_path = self._get_file_path(id, sub)
        return file_path.exists()

    async def lists(
        self,
        sub: str = None
    ) -> List[Dict[str, Any]]:
        """
        列出所有数据（同步方法，向后兼容并符合StorageBase接口）

        Args:
            sub: 子目录或子数据库，用于区分不同的数据集合

        Returns:
            List[Dict[str, Any]]: 数据文件列表
        """
        # 获取所有可用数据
        if sub:
            target_dir = self.datadir / sub
        else:
            target_dir = self.datadir

        results = []
        # 构建搜索模式，查找目标目录下的所有具有指定扩展名的文件
        search_pattern = f"*{self.extension}"
        # 使用pathlib的glob方法递归搜索
        for file_path in target_dir.glob(search_pattern):
            # 构建符合测试用例格式的结果
            results.append({
                'id': file_path.stem,  # 文件名（不包含扩展名）
                'sub': sub,
                'file_path': str(file_path),
                'size': format_size(file_path.stat().st_size),
                'modified': datetime.fromtimestamp(file_path.stat().st_mtime)
            })

        return results

    async def get_time_range(
        self,
        id: str,
        sub: str = None
    ) -> Optional[Dict[str, Any]]:
        """
        获取数据的时间范围

        Args:
            id: 数据ID
            sub: 子目录或子数据库，用于区分不同的数据集合

        Returns:
            Optional[Dict[str, Any]]: 数据的时间范围，包含start_time和end_time
        """
        try:
            # 获取文件路径
            file_path = self._get_file_path(id, sub)

            # 检查文件是否存在
            if not file_path.exists():
                logger.debug("文件 %s 不存在", file_path)
                return None

            # 加载数据
            data = await self.load(id, sub)

            if data is None or data.empty:
                logger.debug("文件 %s 数据为空", file_path)
                return {
                    "start_time": None,
                    "end_time": None
                }

            # 检查time列是否存在
            if 'time' not in data.columns:
                logger.debug("文件 %s 中没有time列", file_path)
                return {
                    "start_time": None,
                    "end_time": None
                }

            # 获取最小和最大时间值
            min_time = data['time'].min()
            max_time = data['time'].max()

            time_range = {
                "start_time": min_time,
                "end_time": max_time
            }

            logger.debug("获取文件 %s 的时间范围: %s", file_path, time_range)
            return time_range

        except Exception as e:
            logger.error("获取数据时间范围失败: %s", str(e))
            return None
