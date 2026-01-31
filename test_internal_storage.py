#!/usr/bin/env python3
"""测试内部任务存储功能"""

import asyncio
import logging
from chronoforge.decorators import create_task
from chronoforge.internal_task_storage import internal_storage_manager, DataType, init_internal_storage

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestDataSource:
    """测试数据源类"""
    
    def __init__(self):
        self.name = "TestDataSource"
    
    @create_task(interval=60, enable_storage=True)
    async def test_task(self):
        """测试任务，返回一些数据"""
        logger.info("Running test_task...")
        return {
            'test_key': 'test_value',
            'number': 42,
            'timestamp': asyncio.get_event_loop().time()
        }


async def main():
    """主测试函数"""
    # 初始化存储
    init_internal_storage()
    
    # 创建测试数据源实例
    test_ds = TestDataSource()
    
    # 直接调用测试任务
    result = await test_ds.test_task()
    logger.info(f"Task result: {result}")
    
    # 模拟任务配置和任务实例
    class MockTask:
        def __init__(self):
            self.name = "test_task"
            self.data_source_name = "TestDataSource"
            self.method_name = "test_task"
            self.config = {
                'enable_storage': True,
                'method_params': {
                    'params': {}
                }
            }
    
    mock_task = MockTask()
    
    # 模拟scheduler中的_handle_task_result调用
    from chronoforge.scheduler import Scheduler
    scheduler = Scheduler()
    await scheduler._handle_task_result(mock_task, result)
    
    # 验证数据是否被保存
    saved_data = await internal_storage_manager.load_data(
        task_id="TestDataSource_test_task",
        data_type=DataType['CUSTOM']
    )
    
    logger.info(f"Saved data: {saved_data}")
    if saved_data:
        logger.info("✅ Internal task storage test passed!")
    else:
        logger.error("❌ Internal task storage test failed!")


if __name__ == "__main__":
    asyncio.run(main())
