#!/usr/bin/env python3
"""测试CLI命令行工具"""
import pytest
import sys
from unittest.mock import patch, MagicMock
from io import StringIO
from chronoforge import cli


class TestCLI:
    """测试CLI命令行工具"""

    @patch('sys.argv', ['chronoforge'])
    @patch('chronoforge.__version__', '0.1.0')
    def test_default_arguments(self):
        """测试默认参数"""
        # 捕获输出
        captured_output = StringIO()
        sys.stdout = captured_output

        try:
            # 模拟KeyboardInterrupt，以便函数能够退出
            with patch('uvicorn.run', side_effect=KeyboardInterrupt):
                cli.main()
        except SystemExit:
            pass

        # 恢复stdout
        sys.stdout = sys.__stdout__

        # 验证输出
        output = captured_output.getvalue()
        assert "ChronoForge 版本 0.1.0" in output
        assert "启动ChronoForge调度器服务..." in output
        assert "服务地址: http://127.0.0.1:8000" in output

    @patch('sys.argv', ['chronoforge', '--host', '0.0.0.0', '--port', '8080'])
    @patch('chronoforge.__version__', '0.1.0')
    def test_custom_host_port(self):
        """测试自定义主机和端口"""
        # 捕获输出
        captured_output = StringIO()
        sys.stdout = captured_output

        try:
            # 模拟KeyboardInterrupt，以便函数能够退出
            with patch('uvicorn.run', side_effect=KeyboardInterrupt):
                cli.main()
        except SystemExit:
            pass

        # 恢复stdout
        sys.stdout = sys.__stdout__

        # 验证输出
        output = captured_output.getvalue()
        assert "服务地址: http://0.0.0.0:8080" in output

    @patch('sys.argv', ['chronoforge', 'serve', '--reload', '--workers', '4'])
    @patch('chronoforge.__version__', '0.1.0')
    def test_serve_command(self):
        """测试serve命令"""
        # 捕获输出
        captured_output = StringIO()
        sys.stdout = captured_output

        try:
            # 模拟KeyboardInterrupt，以便函数能够退出
            with patch('uvicorn.run', side_effect=KeyboardInterrupt):
                cli.main()
        except SystemExit:
            pass

        # 恢复stdout
        sys.stdout = sys.__stdout__

        # 验证输出
        output = captured_output.getvalue()
        assert "启动ChronoForge调度器服务..." in output

    @patch('sys.argv', ['chronoforge', '--reload'])
    @patch('chronoforge.__version__', '0.1.0')
    def test_reload_option(self):
        """测试reload选项"""
        # 模拟uvicorn.run
        with patch('uvicorn.run') as mock_run:
            # 模拟KeyboardInterrupt，以便函数能够退出
            mock_run.side_effect=KeyboardInterrupt

            try:
                cli.main()
            except SystemExit:
                pass

            # 验证run函数是否被正确调用
            mock_run.assert_called_once()
            kwargs = mock_run.call_args[1]
            assert kwargs['reload'] is True
            assert kwargs['workers'] == 1

    @patch('sys.argv', ['chronoforge', '--workers', '3'])
    @patch('chronoforge.__version__', '0.1.0')
    def test_workers_option(self):
        """测试workers选项"""
        # 模拟uvicorn.run
        with patch('uvicorn.run') as mock_run:
            # 模拟KeyboardInterrupt，以便函数能够退出
            mock_run.side_effect=KeyboardInterrupt

            try:
                cli.main()
            except SystemExit:
                pass

            # 验证run函数是否被正确调用
            mock_run.assert_called_once()
            kwargs = mock_run.call_args[1]
            assert kwargs['workers'] == 3

    @patch('sys.argv', ['chronoforge'])
    def test_import_error(self):
        """测试导入错误处理"""
        # 捕获输出
        captured_output = StringIO()
        sys.stdout = captured_output

        # 模拟ImportError
        with patch('uvicorn.run', side_effect=ImportError("测试导入错误")):
            try:
                cli.main()
            except SystemExit as e:
                # 验证退出码
                assert e.code == 1

        # 恢复stdout
        sys.stdout = sys.__stdout__

        # 验证输出
        output = captured_output.getvalue()
        assert "错误: 无法导入依赖包: 测试导入错误" in output
        assert "请确保已安装所有依赖: pip install -e ." in output

    @patch('sys.argv', ['chronoforge'])
    @patch('chronoforge.__version__', '0.1.0')
    def test_keyboard_interrupt(self):
        """测试KeyboardInterrupt处理"""
        # 捕获输出
        captured_output = StringIO()
        sys.stdout = captured_output

        # 模拟KeyboardInterrupt
        with patch('uvicorn.run', side_effect=KeyboardInterrupt):
            try:
                cli.main()
            except SystemExit as e:
                # 验证退出码
                assert e.code == 0

        # 恢复stdout
        sys.stdout = sys.__stdout__

        # 验证输出
        output = captured_output.getvalue()
        assert "服务已停止" in output

    @patch('sys.argv', ['chronoforge', 'serve', '--host', 'localhost', '--port', '9000'])
    @patch('chronoforge.__version__', '0.1.0')
    def test_serve_command_with_args(self):
        """测试带有参数的serve命令"""
        # 模拟uvicorn.run
        with patch('uvicorn.run') as mock_run:
            # 模拟KeyboardInterrupt，以便函数能够退出
            mock_run.side_effect=KeyboardInterrupt

            try:
                cli.main()
            except SystemExit:
                pass

            # 验证run函数是否被正确调用
            mock_run.assert_called_once()
            kwargs = mock_run.call_args[1]
            assert kwargs['host'] == 'localhost'
            assert kwargs['port'] == 9000
            assert kwargs['reload'] is False


if __name__ == "__main__":
    pytest.main([__file__])
