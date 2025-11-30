from chronoforge.data_source import verify_datasource_instance
from chronoforge.storage import verify_storage_instance


class TestPluginVerification:
    """测试插件验证功能"""

    def test_verify_invalid_class_type(self):
        """测试验证无效的类类型"""
        # 测试使用普通类而非插件类，确保它能接受参数
        class NotADataSource:
            def __init__(self, config=None):
                pass

        success, message = verify_datasource_instance(NotADataSource)
        assert success is False

    def test_verify_invalid_storage_class(self):
        """测试验证无效的存储类"""
        # 测试使用普通类而非存储类，确保它能接受参数
        class NotAStorage:
            def __init__(self, config=None):
                pass

        success, message = verify_storage_instance(NotAStorage)
        assert success is False
