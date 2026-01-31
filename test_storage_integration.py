#!/usr/bin/env python3
"""测试存储整合功能"""
from chronoforge.internal_task_storage import init_internal_storage, internal_storage_manager
import asyncio

async def test_storage():
    """测试存储功能"""
    # 初始化存储管理器
    init_internal_storage()
    
    # 测试保存数据
    save_success = await internal_storage_manager.save_data('test_task', {'key': 'value'}, 'CUSTOM')
    print('Save successful:', save_success)
    
    # 测试加载数据
    data = await internal_storage_manager.load_data('test_task', 'CUSTOM')
    print('Loaded data:', data)
    
    # 测试删除数据
    delete_success = await internal_storage_manager.delete_data('test_task', 'CUSTOM')
    print('Delete successful:', delete_success)
    
    print('All tests completed!')

if __name__ == '__main__':
    asyncio.run(test_storage())
