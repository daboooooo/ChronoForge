import pytest


@pytest.fixture(autouse=True)
def cleanup_before_test():
    """每个测试之前清理全局 storage_manager 状态"""
    try:
        import chronoforge.storage.manager
        from chronoforge.storage import manager as manager_module
        if hasattr(manager_module, 'storage_manager'):
            sm = manager_module.storage_manager
            sm.storages.clear()
            sm.default_storage = None
            sm.storage_routes.clear()
    except Exception:
        pass
    
    yield
