#!/usr/bin/env python3
"""简化版测试内部任务存储功能"""

import asyncio
import logging
from chronoforge.internal_task_storage import internal_storage_manager, DataType, init_internal_storage

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    """主测试函数"""
    # 初始化存储
    init_internal_storage()
    
    # 测试数据
    test_data = {
        'test_key': 'test_value',
        'number': 42,
        'timestamp': asyncio.get_event_loop().time()
    }
    
    # 保存数据到内部存储
    logger.info("Saving test data to internal storage...")
    success = await internal_storage_manager.save_data(
        task_id="TestDataSource_test_task",
        data=test_data,
        data_type=DataType['CUSTOM'],
        metadata={
            'task_name': 'test_task',
            'data_source': 'TestDataSource',
            'method_name': 'test_task',
            'timestamp': asyncio.get_event_loop().time()
        }
    )
    
    if success:
        logger.info("✅ Data saved successfully")
    else:
        logger.error("❌ Failed to save data")
        return
    
    # 从内部存储加载数据
    logger.info("Loading data from internal storage...")
    saved_data = await internal_storage_manager.load_data(
        task_id="TestDataSource_test_task",
        data_type=DataType['CUSTOM']
    )
    
    logger.info(f"Loaded data: {saved_data}")
    
    if saved_data and saved_data['test_key'] == test_data['test_key']:
        logger.info("✅ Internal task storage test passed!")
    else:
        logger.error("❌ Internal task storage test failed!")
    
    # 测试删除数据
    logger.info("Testing data deletion...")
    success = await internal_storage_manager.delete_data(
        task_id="TestDataSource_test_task",
        data_type=DataType['CUSTOM']
    )
    
    if success:
        logger.info("✅ Data deleted successfully")
    else:
        logger.error("❌ Failed to delete data")
    
    # 验证数据已删除
    deleted_data = await internal_storage_manager.load_data(
        task_id="TestDataSource_test_task",
        data_type=DataType['CUSTOM']
    )
    
    if deleted_data is None:
        logger.info("✅ Data deletion verified!")
    else:
        logger.error("❌ Data still exists after deletion!")


if __name__ == "__main__":
    asyncio.run(main())
