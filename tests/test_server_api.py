import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock, AsyncMock
import sys


# 创建一个更全面的duckdb模拟模块，避免实际导入
class MockDuckDB:
    # 模拟所需的类
    class DuckDBPyConnection:
        pass

    class sqltypes:
        pass

    # 模拟所需的方法
    @staticmethod
    def connect(*args, **kwargs):
        return MagicMock(spec=MockDuckDB.DuckDBPyConnection)


sys.modules['duckdb'] = MockDuckDB()
sys.modules['duckdb.sqltypes'] = MockDuckDB.sqltypes

# 现在可以安全地导入ChronoForge模块
from chronoforge.server.main import app
from chronoforge.server.dependencies import get_scheduler


# 创建测试客户端
client = TestClient(app)


@pytest.fixture
def mock_scheduler():
    """创建一个模拟的Scheduler实例"""
    # 创建一个模拟的Scheduler实例
    scheduler_mock = MagicMock()

    # 设置基本属性
    scheduler_mock.tasks = {"test_task": MagicMock(name="test_task")}
    scheduler_mock.task_states = {"test_task": {"status": "idle"}}
    scheduler_mock.storage_instances = {}
    scheduler_mock.thread_pool = MagicMock()
    scheduler_mock.add_task.return_value = None
    scheduler_mock.delete_task.return_value = None

    # 模拟runner_thread，确保它不是None且is_alive()返回True
    mock_thread = MagicMock()
    mock_thread.is_alive.return_value = True
    scheduler_mock._runner_thread = mock_thread

    # 模拟list_supported_plugins方法
    def mock_list_supported_plugins(plugin_type):
        if plugin_type == "data_source":
            return ["CryptoSpotDataSource", "FREDDataSource"]
        elif plugin_type == "storage":
            return ["LocalFileStorage"]
        return []
    scheduler_mock.list_supported_plugins.side_effect = mock_list_supported_plugins

    # 直接模拟dependencies.py中的全局变量scheduler_instance
    with patch('chronoforge.server.dependencies.scheduler_instance', scheduler_mock):
        # 同时模拟get_scheduler函数，确保它也返回我们的mock实例
        with patch('chronoforge.server.dependencies.get_scheduler', return_value=scheduler_mock):
            # 模拟set_scheduler函数，确保它不改变我们的mock实例
            with patch('chronoforge.server.dependencies.set_scheduler'):
                yield scheduler_mock


class TestServerAPI:
    """测试服务器API端点"""

    def test_root_endpoint(self):
        """测试根路径"""
        response = client.get("/")
        assert response.status_code == 200
        assert "ChronoForge Scheduler API" in response.json()["message"]

    def test_get_status(self, mock_scheduler):
        """测试获取服务状态"""
        response = client.get("/api/status")
        assert response.status_code == 200
        data = response.json()
        assert data["service"] == "ChronoForge Scheduler"
        assert data["status"] == "running"

    def test_list_tasks_via_status(self, mock_scheduler):
        """通过status路由测试列出所有任务"""
        response = client.get("/api/status/tasks")
        assert response.status_code == 200
        data = response.json()
        assert "test_task" in data

    def test_create_task(self, mock_scheduler):
        """测试创建新任务的接口调用"""
        task_data = {
            "name": "new_task",
            "data_source_name": "test_source",
            "data_source_config": {},
            "storage_name": "test_storage",
            "storage_config": {},
            "time_slot": {
                "start": "00:00",
                "end": "23:59"
            },
            "symbols": ["BTC/USDT"],
            "timeframe": "1d",
            "timerange_str": "20220101-",
            "inplace": False
        }

        response = client.post("/api/tasks", json=task_data)
        # 验证接口被调用
        assert mock_scheduler.add_task.called

    def test_delete_task(self, mock_scheduler):
        """测试删除任务的接口调用"""
        response = client.delete("/api/tasks/test_task")
        # 验证接口被调用
        assert mock_scheduler.delete_task.called

    def test_start_task(self, mock_scheduler):
        """测试启动任务的接口调用"""
        response = client.post("/api/tasks/test_task/start")
        # 验证线程池被调用
        assert mock_scheduler.thread_pool.submit.called
        assert response.status_code == 200
        assert response.json()["status"] == "running"

    def test_stop_task(self, mock_scheduler):
        """测试停止任务的接口调用"""
        # 设置任务状态为运行中
        mock_future = MagicMock()
        mock_future.done.return_value = False
        mock_future.cancel.return_value = True
        mock_scheduler.task_states = {
            "test_task": {
                "status": "running",
                "future": mock_future,
                "start_time": 1234567890
            }
        }

        response = client.post("/api/tasks/test_task/stop")
        assert response.status_code == 200
        assert response.json()["status"] == "stopped"
        assert mock_future.cancel.called

    def test_get_task_status(self, mock_scheduler):
        """测试获取任务状态的接口调用"""
        # 设置任务状态
        mock_scheduler.task_states = {
            "test_task": {
                "status": "running",
                "start_time": 1234567890
            }
        }

        response = client.get("/api/tasks/test_task/status")
        assert response.status_code == 200
        assert response.json()["status"] == "running"

    def test_get_task(self, mock_scheduler):
        """测试获取任务详情的接口调用"""
        # 设置任务的time_slot和timerange属性
        task_mock = MagicMock()
        task_mock.name = "test_task"
        task_mock.data_source_name = "test_source"
        task_mock.storage_name = "test_storage"
        task_mock.time_slot = MagicMock(start="00:00", end="23:59")
        task_mock.symbols = ["BTC/USDT"]
        task_mock.timeframe = "1d"
        task_mock.timerange = MagicMock(start_ts_ms=1234567890, end_ts_ms=None)
        mock_scheduler.tasks = {"test_task": task_mock}
        mock_scheduler.task_states = {
            "test_task": {"status": "idle"}
        }

        response = client.get("/api/tasks/test_task")
        assert response.status_code == 200
        assert response.json()["name"] == "test_task"

    def test_list_tasks(self, mock_scheduler):
        """测试列出所有任务的接口调用"""
        # 设置任务的time_slot和timerange属性
        task_mock = MagicMock()
        task_mock.name = "test_task"
        task_mock.data_source_name = "test_source"
        task_mock.storage_name = "test_storage"
        task_mock.time_slot = MagicMock(start="00:00", end="23:59")
        task_mock.symbols = ["BTC/USDT"]
        task_mock.timeframe = "1d"
        task_mock.timerange = MagicMock(start_ts_ms=1234567890, end_ts_ms=None)
        mock_scheduler.tasks = {"test_task": task_mock}
        mock_scheduler.task_states = {
            "test_task": {"status": "idle"}
        }

        response = client.get("/api/tasks")
        assert response.status_code == 200
        assert len(response.json()["tasks"]) == 1
        assert response.json()["tasks"][0]["name"] == "test_task"

    def test_get_task_data_info(self, mock_scheduler):
        """测试获取任务数据信息的接口调用"""
        # 设置任务
        task_mock = MagicMock()
        task_mock.name = "test_task"
        task_mock.symbols = ["BTC/USDT", "ETH/USDT"]
        task_mock.timeframe = "1d"
        task_mock.sub = "test_sub"
        mock_scheduler.tasks = {"test_task": task_mock}

        # 模拟存储实例
        mock_storage = MagicMock()
        # 模拟get_time_range方法返回时间范围
        mock_storage.get_time_range = AsyncMock(return_value={
            "start_time": MagicMock(strftime=MagicMock(return_value="2022-01-01 00:00:00")),
            "end_time": MagicMock(strftime=MagicMock(return_value="2022-12-31 23:59:59"))
        })
        mock_scheduler.storage_instances = {"test_task": mock_storage}

        response = client.get("/api/tasks/test_task/data_info")
        assert response.status_code == 200
        assert response.json()["task_name"] == "test_task"
        assert len(response.json()["data_info"]) == 2
        assert response.json()["data_info"][0]["data_name"] == "BTC/USDT_1d"
        assert response.json()["data_info"][1]["data_name"] == "ETH/USDT_1d"
        assert response.json()["data_info"][0]["start_time"] == "2022-01-01 00:00:00"
        assert response.json()["data_info"][0]["end_time"] == "2022-12-31 23:59:59"

    def test_get_task_data_info_no_storage(self, mock_scheduler):
        """测试当存储实例不存在时获取任务数据信息的接口调用"""
        # 设置任务
        task_mock = MagicMock()
        task_mock.name = "test_task"
        mock_scheduler.tasks = {"test_task": task_mock}

        # 不设置存储实例
        mock_scheduler.storage_instances = {}

        response = client.get("/api/tasks/test_task/data_info")
        assert response.status_code == 500

    def test_get_task_data_info_time_range_none(self, mock_scheduler):
        """测试当时间范围为None时获取任务数据信息的接口调用"""
        # 设置任务
        task_mock = MagicMock()
        task_mock.name = "test_task"
        task_mock.symbols = ["BTC/USDT"]
        task_mock.timeframe = "1d"
        task_mock.sub = "test_sub"
        mock_scheduler.tasks = {"test_task": task_mock}

        # 模拟存储实例
        mock_storage = MagicMock()
        # 模拟get_time_range方法返回None
        mock_storage.get_time_range = AsyncMock(return_value=None)
        mock_scheduler.storage_instances = {"test_task": mock_storage}

        response = client.get("/api/tasks/test_task/data_info")
        assert response.status_code == 200
        assert response.json()["data_info"][0]["start_time"] == "无法获取"
        assert response.json()["data_info"][0]["end_time"] == "无法获取"

    def test_get_task_data(self, mock_scheduler):
        """测试获取任务数据的接口调用"""
        # 设置任务
        task_mock = MagicMock()
        task_mock.name = "test_task"
        task_mock.symbols = ["BTC/USDT", "ETH/USDT"]
        task_mock.timeframe = "1d"
        task_mock.sub = "test_sub"
        mock_scheduler.tasks = {"test_task": task_mock}

        # 模拟存储实例
        mock_storage = MagicMock()

        # 模拟DataFrame数据
        mock_data = MagicMock()
        mock_data.empty = False
        mock_data.to_dict.return_value = [
            {"time": "2022-01-01 00:00:00",
             "open": 30000, "high": 31000, "low": 29000, "close": 30500, "volume": 1000},
            {"time": "2022-01-02 00:00:00",
             "open": 30500, "high": 32000, "low": 30000, "close": 31500, "volume": 1500}
        ]
        mock_storage.load = AsyncMock(return_value=mock_data)
        mock_scheduler.storage_instances = {"test_task": mock_storage}

        response = client.get("/api/tasks/test_task/data")
        assert response.status_code == 200
        assert len(response.json()["data"]) == 4
        assert response.json()["data"][0]["time"] == "2022-01-01 00:00:00"
        assert response.json()["data"][0]["open"] == 30000
        assert mock_storage.load.call_count == 2

    def test_get_task_data_by_data_name(self, mock_scheduler):
        """测试通过data_name获取特定任务数据的接口调用"""
        # 设置任务
        task_mock = MagicMock()
        task_mock.name = "test_task"
        task_mock.symbols = ["BTC/USDT", "ETH/USDT"]
        task_mock.timeframe = "1d"
        task_mock.sub = "test_sub"
        mock_scheduler.tasks = {"test_task": task_mock}

        # 模拟存储实例
        mock_storage = MagicMock()

        # 模拟DataFrame数据
        mock_data = MagicMock()
        mock_data.empty = False
        mock_data.to_dict.return_value = [
            {"time": MagicMock(strftime=MagicMock(return_value="2022-01-01 00:00:00")),
             "open": 30000, "high": 31000, "low": 29000, "close": 30500, "volume": 1000}
        ]
        mock_storage.load = AsyncMock(return_value=mock_data)
        mock_scheduler.storage_instances = {"test_task": mock_storage}

        response = client.get("/api/tasks/test_task/data?data_name=BTC/USDT_1d")
        assert response.status_code == 200
        assert len(response.json()["data"]) == 1
        mock_storage.load.assert_called_once_with(id="BTC/USDT_1d", sub="test_sub")

    def test_get_task_data_by_symbol(self, mock_scheduler):
        """测试通过symbol获取特定任务数据的接口调用"""
        # 设置任务
        task_mock = MagicMock()
        task_mock.name = "test_task"
        task_mock.symbols = ["BTC/USDT", "ETH/USDT"]
        task_mock.timeframe = "1d"
        task_mock.sub = "test_sub"
        mock_scheduler.tasks = {"test_task": task_mock}

        # 模拟存储实例
        mock_storage = MagicMock()

        # 模拟DataFrame数据
        mock_data = MagicMock()
        mock_data.empty = False
        mock_data.to_dict.return_value = [
            {"time": MagicMock(strftime=MagicMock(return_value="2022-01-01 00:00:00")),
             "open": 30000, "high": 31000, "low": 29000, "close": 30500, "volume": 1000}
        ]
        mock_storage.load = AsyncMock(return_value=mock_data)
        mock_scheduler.storage_instances = {"test_task": mock_storage}

        response = client.get("/api/tasks/test_task/data?symbol=BTC/USDT")
        assert response.status_code == 200
        assert len(response.json()["data"]) == 1
        mock_storage.load.assert_called_once_with(id="BTC/USDT_1d", sub="test_sub")

    def test_get_task_data_with_time_filter(self, mock_scheduler):
        """测试带时间范围过滤的任务数据获取接口调用"""
        # 设置任务
        task_mock = MagicMock()
        task_mock.name = "test_task"
        task_mock.symbols = ["BTC/USDT"]
        task_mock.timeframe = "1d"
        task_mock.sub = "test_sub"
        mock_scheduler.tasks = {"test_task": task_mock}

        # 模拟存储实例
        mock_storage = MagicMock()

        # 模拟DataFrame数据
        mock_data = MagicMock()
        mock_data.empty = False
        # 模拟按时间排序的数据
        mock_data.to_dict.return_value = [
            {"time": "2022-01-01 00:00:00", "open": 30000},
            {"time": "2022-01-02 00:00:00", "open": 30500},
            {"time": "2022-01-03 00:00:00", "open": 31000}
        ]
        mock_storage.load = AsyncMock(return_value=mock_data)
        mock_scheduler.storage_instances = {"test_task": mock_storage}

        response = client.get("/api/tasks/test_task/data?start_time=2022-01-02&end_time=2022-01-02")
        assert response.status_code == 200
        # 这里需要模拟pandas的时间过滤功能，但由于我们在测试中直接返回模拟数据，所以实际过滤不会发生
        # 我们只需要验证接口调用成功
        assert len(response.json()["data"]) == 1

    def test_get_task_data_no_storage(self, mock_scheduler):
        """测试当存储实例不存在时获取任务数据的接口调用"""
        # 设置任务
        task_mock = MagicMock()
        task_mock.name = "test_task"
        mock_scheduler.tasks = {"test_task": task_mock}

        # 不设置存储实例
        mock_scheduler.storage_instances = {}

        response = client.get("/api/tasks/test_task/data")
        assert response.status_code == 500


# 测试plugins相关的API端点
class TestPluginsAPI:
    """测试插件相关API端点"""

    def test_list_plugins(self, mock_scheduler):
        """测试列出所有插件"""
        response = client.get("/api/plugins")
        assert response.status_code == 200
        assert "plugins" in response.json()

    def test_list_plugins_by_type(self, mock_scheduler):
        """测试按类型列出插件"""
        # 测试数据源插件
        response = client.get("/api/plugins/data_source")
        assert response.status_code == 200
        assert response.json()["plugin_type"] == "data_source"

        # 测试存储插件
        response = client.get("/api/plugins/storage")
        assert response.status_code == 200
        assert response.json()["plugin_type"] == "storage"

    def test_get_datasource_functions(self, mock_scheduler):
        """测试获取数据源函数列表"""
        # 模拟datasource_functions方法返回值
        mock_scheduler.datasource_functions.return_value = {
            "functions": [
                {
                    "name": "get_data",
                    "docstring": "获取数据",
                    "parameters": [
                        {
                            "name": "symbol",
                            "type": "str",
                            "default": "BTC/USDT"
                        }
                    ],
                    "return_type": "DataFrame"
                }
            ]
        }

        response = client.get("/api/plugins/data_source/test_source/functions")
        assert response.status_code == 200
        assert len(response.json()["functions"]) == 1
        assert response.json()["functions"][0]["name"] == "get_data"

    def test_delegate_call(self, mock_scheduler):
        """测试代理调用插件函数"""
        # 模拟delegate_call方法返回值
        mock_scheduler.delegate_call.return_value = {"result": "success"}

        request_data = {
            "plugin_name": "test_source",
            "plugin_type": "data_source",
            "function_name": "get_data",
            "kwargs": {
                "symbol": "BTC/USDT"
            }
        }

        response = client.post("/api/plugins/delegate-call", json=request_data)
        assert response.status_code == 200
        assert response.json()["success"] is True
        mock_scheduler.delegate_call.assert_called_once_with(
            plugin_name="test_source",
            plugin_type="data_source",
            function_name="get_data",
            symbol="BTC/USDT"
        )


# 测试错误处理和边界情况
class TestServerAPIErrorHandling:
    """测试服务器API的错误处理和边界情况"""

    def test_create_task_error(self, mock_scheduler):
        """测试创建任务时的错误处理"""
        task_data = {
            "name": "new_task",
            "data_source_name": "test_source",
            "data_source_config": {},
            "storage_name": "test_storage",
            "storage_config": {},
            "time_slot": {
                "start": "00:00",
                "end": "23:59"
            },
            "symbols": ["BTC/USDT"],
            "timeframe": "1d",
            "timerange_str": "20220101-",
            "inplace": False
        }

        # 模拟add_task抛出异常
        mock_scheduler.add_task.side_effect = ValueError("Task already exists")

        response = client.post("/api/tasks", json=task_data)
        assert response.status_code == 400
        assert "Task already exists" in response.json()["detail"]

    def test_delete_task_not_found(self, mock_scheduler):
        """测试删除不存在的任务时的错误处理"""
        # 设置没有test_task任务
        mock_scheduler.tasks = {}

        response = client.delete("/api/tasks/test_task")
        assert response.status_code == 404
        assert "Task test_task not found" in response.json()["detail"]

    def test_start_task_not_found(self, mock_scheduler):
        """测试启动不存在的任务时的错误处理"""
        # 设置没有test_task任务
        mock_scheduler.tasks = {}

        response = client.post("/api/tasks/test_task/start")
        assert response.status_code == 404
        assert "Task test_task not found" in response.json()["detail"]

    def test_stop_task_not_found(self, mock_scheduler):
        """测试停止不存在的任务时的错误处理"""
        # 设置没有test_task任务
        mock_scheduler.tasks = {}

        response = client.post("/api/tasks/test_task/stop")
        assert response.status_code == 404
        assert "Task test_task not found" in response.json()["detail"]

    def test_stop_task_not_running(self, mock_scheduler):
        """测试停止未运行的任务时的处理"""
        # 设置任务存在但未运行
        mock_scheduler.tasks = {"test_task": MagicMock()}
        mock_scheduler.task_states = {"test_task": {"status": "idle"}}

        response = client.post("/api/tasks/test_task/stop")
        assert response.status_code == 200
        assert response.json()["status"] == "idle"
        assert "Task is not running" in response.json()["message"]

    def test_get_task_not_found(self, mock_scheduler):
        """测试获取不存在的任务时的错误处理"""
        # 设置没有test_task任务
        mock_scheduler.tasks = {}

        response = client.get("/api/tasks/test_task")
        assert response.status_code == 404
        assert "Task test_task not found" in response.json()["detail"]

    def test_get_task_status_not_found(self, mock_scheduler):
        """测试获取不存在的任务状态时的错误处理"""
        # 设置没有test_task任务
        mock_scheduler.tasks = {}

        response = client.get("/api/tasks/test_task/status")
        assert response.status_code == 404
        assert "Task test_task not found" in response.json()["detail"]

    def test_list_plugins_invalid_type(self, mock_scheduler):
        """测试使用无效插件类型时的错误处理"""
        response = client.get("/api/plugins/invalid_type")
        assert response.status_code == 400
        assert "Invalid plugin type" in response.json()["detail"]

    def test_get_datasource_functions_error(self, mock_scheduler):
        """测试获取数据源函数时的错误处理"""
        # 模拟datasource_functions抛出异常
        mock_scheduler.datasource_functions.side_effect = ValueError("Data source not found")

        response = client.get("/api/plugins/data_source/invalid_source/functions")
        assert response.status_code == 400
        assert "Data source not found" in response.json()["detail"]

    def test_delegate_call_error(self, mock_scheduler):
        """测试代理调用时的错误处理"""
        request_data = {
            "plugin_name": "invalid_source",
            "plugin_type": "data_source",
            "function_name": "get_data",
            "kwargs": {
                "symbol": "BTC/USDT"
            }
        }

        # 模拟delegate_call抛出异常
        mock_scheduler.delegate_call.side_effect = ValueError("Plugin not found")

        response = client.post("/api/plugins/delegate-call", json=request_data)
        assert response.status_code == 400
        assert "Plugin not found" in response.json()["detail"]
