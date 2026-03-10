import asyncio
from typing import Dict, Any, Optional, List, Tuple
import pandas as pd
import logging
from datetime import datetime, timedelta

from chronoforge.data_source.manager import DataSourceManager
from chronoforge.storage.manager import StorageManager
from chronoforge.cache import CacheManager

logger = logging.getLogger(__name__)


class DataService:
    """
    数据服务类，实现数据业务逻辑，提供统一的数据访问接口
    
    功能：
    - 数据获取和处理
    - 数据转换和标准化
    - 数据质量检查
    - 数据更新策略
    """
    
    def __init__(self, 
                 data_source_manager: Optional[DataSourceManager] = None, 
                 storage_manager: Optional[StorageManager] = None, 
                 cache_manager: Optional[CacheManager] = None):
        """
        初始化 DataService
        
        Args:
            data_source_manager: 数据源管理器实例
            storage_manager: 存储管理器实例
            cache_manager: 缓存管理器实例
        """
        self.data_source_manager = data_source_manager
        self.storage_manager = storage_manager
        self.cache_manager = cache_manager
        
    async def get_data(self, 
                      data_source: str, 
                      data_type: str, 
                      **params) -> Optional[pd.DataFrame]:
        """
        获取数据
        
        Args:
            data_source: 数据源名称
            data_type: 数据类型
            **params: 附加参数
            
        Returns:
            数据帧，如果获取失败返回 None
        """
        try:
            # 尝试从缓存获取
            cache_key = f"{data_source}_{data_type}_{hash(str(params))}"
            if self.cache_manager:
                cached_data = await self.cache_manager.get(cache_key)
                if cached_data is not None:
                    logger.info(f"从缓存获取数据: {data_source} - {data_type}")
                    return cached_data
            
            # 从数据源获取数据
            if not self.data_source_manager:
                logger.error("数据源管理器未初始化")
                return None
            
            # 从参数中提取必要的信息
            symbol = params.get('symbol', 'BTC/USDT')
            timeframe = params.get('timeframe', '1d')
            start_ts_ms = params.get('start_ts_ms')
            end_ts_ms = params.get('end_ts_ms')
            
            # 如果没有提供时间戳，使用默认值
            if start_ts_ms is None:
                import time
                # 默认获取过去30天的数据
                end_ts_ms = int(time.time() * 1000)
                start_ts_ms = end_ts_ms - (30 * 24 * 60 * 60 * 1000)
            
            # 直接获取数据源实例
            data_source_instance = self.data_source_manager.get_data_source(data_source)
            if not data_source_instance:
                logger.error(f"数据源 {data_source} 未注册")
                return None
            
            # 直接调用数据源的 fetch 方法
            data = await data_source_instance.fetch(
                symbol=symbol,
                timeframe=timeframe,
                start_ts_ms=start_ts_ms,
                end_ts_ms=end_ts_ms
            )
            
            if data is not None:
                # 数据质量检查
                data = self._check_data_quality(data)
                
                # 数据标准化
                data = self._standardize_data(data, data_source, data_type)
                
                # 缓存数据
                if self.cache_manager:
                    await self.cache_manager.put(cache_key, data)
            
            return data
            
        except Exception as e:
            logger.error(f"获取数据失败: {e}")
            return None
    
    def process_data(self, 
                     data: pd.DataFrame, 
                     operations: List[Dict[str, Any]]) -> Optional[pd.DataFrame]:
        """
        处理数据
        
        Args:
            data: 原始数据帧
            operations: 操作列表，每个操作包含操作类型和参数
            
        Returns:
            处理后的数据帧，如果处理失败返回 None
        """
        try:
            processed_data = data.copy()
            
            for operation in operations:
                op_type = operation.get('type')
                op_params = operation.get('params', {})
                
                if op_type == 'filter':
                    processed_data = self._filter_data(processed_data, **op_params)
                elif op_type == 'transform':
                    processed_data = self._transform_data(processed_data, **op_params)
                elif op_type == 'aggregate':
                    processed_data = self._aggregate_data(processed_data, **op_params)
                elif op_type == 'sort':
                    processed_data = self._sort_data(processed_data, **op_params)
            
            return processed_data
            
        except Exception as e:
            logger.error(f"处理数据失败: {e}")
            return None
    
    async def save_data(self, 
                       data: pd.DataFrame, 
                       storage_name: str, 
                       key: str) -> bool:
        """
        保存数据
        
        Args:
            data: 数据帧
            storage_name: 存储名称
            key: 存储键
            
        Returns:
            是否保存成功
        """
        try:
            if not self.storage_manager:
                logger.error("存储管理器未初始化")
                return False
            
            # 数据质量检查
            data = self._check_data_quality(data)
            
            # 保存数据
            success = await self.storage_manager.save(key, data, task_type=storage_name)
            logger.info(f"数据保存成功: {storage_name} - {key}")
            return success
            
        except Exception as e:
            logger.error(f"保存数据失败: {e}")
            return False
    
    async def load_data(self, 
                       storage_name: str, 
                       key: str) -> Optional[pd.DataFrame]:
        """
        加载数据
        
        Args:
            storage_name: 存储名称
            key: 存储键
            
        Returns:
            数据帧，如果加载失败返回 None
        """
        try:
            if not self.storage_manager:
                logger.error("存储管理器未初始化")
                return None
            
            data = await self.storage_manager.load(key, task_type=storage_name)
            
            if data is not None:
                # 数据质量检查
                data = self._check_data_quality(data)
            
            return data
            
        except Exception as e:
            logger.error(f"加载数据失败: {e}")
            return None
    
    async def update_data_strategy(self, 
                                  data_source: str, 
                                  data_type: str, 
                                  strategy: Dict[str, Any]) -> bool:
        """
        更新数据策略
        
        Args:
            data_source: 数据源名称
            data_type: 数据类型
            strategy: 更新策略
            
        Returns:
            是否更新成功
        """
        try:
            # 这里可以实现具体的策略更新逻辑
            # 例如，存储策略配置到存储系统中
            if not self.storage_manager:
                logger.error("存储管理器未初始化")
                return False
            
            strategy_key = f"data_strategy_{data_source}_{data_type}"
            success = await self.storage_manager.save(strategy_key, strategy, task_type="local")
            logger.info(f"更新数据策略成功: {data_source} - {data_type}")
            return success
            
        except Exception as e:
            logger.error(f"更新数据策略失败: {e}")
            return False
    
    def _check_data_quality(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        数据质量检查
        
        Args:
            data: 原始数据帧
            
        Returns:
            检查后的数据帧
        """
        # 移除空值
        data = data.dropna()
        
        # 移除重复值（基于所有列）
        data = data.drop_duplicates()
        
        # 检查数据类型
        if 'timestamp' in data.columns:
            if not pd.api.types.is_datetime64_any_dtype(data['timestamp']):
                try:
                    data['timestamp'] = pd.to_datetime(data['timestamp'])
                except Exception as e:
                    logger.warning(f"转换时间戳失败: {e}")
        
        return data
    
    def _standardize_data(self, 
                          data: pd.DataFrame, 
                          data_source: str, 
                          data_type: str) -> pd.DataFrame:
        """
        数据标准化
        
        Args:
            data: 原始数据帧
            data_source: 数据源名称
            data_type: 数据类型
            
        Returns:
            标准化后的数据帧
        """
        # 标准化列名
        data.columns = [col.lower().replace(' ', '_') for col in data.columns]
        
        # 确保时间戳列存在
        if 'timestamp' not in data.columns:
            for col in data.columns:
                if 'date' in col or 'time' in col:
                    data.rename(columns={col: 'timestamp'}, inplace=True)
                    break
        
        # 排序
        if 'timestamp' in data.columns:
            data = data.sort_values('timestamp')
        
        return data
    
    def _filter_data(self, 
                     data: pd.DataFrame, 
                     condition: str) -> pd.DataFrame:
        """
        过滤数据
        
        Args:
            data: 数据帧
            condition: 过滤条件
            
        Returns:
            过滤后的数据帧
        """
        try:
            return data.query(condition)
        except Exception as e:
            logger.error(f"过滤数据失败: {e}")
            return data
    
    def _transform_data(self, 
                        data: pd.DataFrame, 
                        transform_func: str, 
                        **params) -> pd.DataFrame:
        """
        转换数据
        
        Args:
            data: 数据帧
            transform_func: 转换函数名称
            **params: 转换参数
            
        Returns:
            转换后的数据帧
        """
        try:
            if transform_func == 'normalize':
                # 归一化处理
                for col in data.select_dtypes(include=['number']).columns:
                    if col != 'timestamp':
                        min_val = data[col].min()
                        max_val = data[col].max()
                        if max_val > min_val:
                            data[col] = (data[col] - min_val) / (max_val - min_val)
            
            elif transform_func == 'rolling_mean':
                # 滚动平均
                window = params.get('window', 7)
                for col in data.select_dtypes(include=['number']).columns:
                    if col != 'timestamp':
                        data[f'{col}_rolling_mean'] = data[col].rolling(window=window).mean()
            
            return data
        except Exception as e:
            logger.error(f"转换数据失败: {e}")
            return data
    
    def _aggregate_data(self, 
                        data: pd.DataFrame, 
                        freq: str, 
                        agg_funcs: Dict[str, str]) -> pd.DataFrame:
        """
        聚合数据
        
        Args:
            data: 数据帧
            freq: 聚合频率
            agg_funcs: 聚合函数
            
        Returns:
            聚合后的数据帧
        """
        try:
            if 'timestamp' not in data.columns:
                return data
            
            data.set_index('timestamp', inplace=True)
            aggregated = data.resample(freq).agg(agg_funcs)
            aggregated.reset_index(inplace=True)
            
            return aggregated
        except Exception as e:
            logger.error(f"聚合数据失败: {e}")
            return data
    
    def _sort_data(self, 
                   data: pd.DataFrame, 
                   by: str, 
                   ascending: bool = True) -> pd.DataFrame:
        """
        排序数据
        
        Args:
            data: 数据帧
            by: 排序列
            ascending: 是否升序
            
        Returns:
            排序后的数据帧
        """
        try:
            if by in data.columns:
                return data.sort_values(by=by, ascending=ascending)
            return data
        except Exception as e:
            logger.error(f"排序数据失败: {e}")
            return data
