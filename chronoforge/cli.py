#!/usr/bin/env python3
"""ChronoForge命令行工具"""

import argparse
import sys


def main():
    """命令行工具主函数"""
    parser = argparse.ArgumentParser(
        prog="chronoforge",
        description="ChronoForge Scheduler - 异步、插件式的时间序列数据处理框架",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 启动调度器服务
  chronoforge serve --host 0.0.0.0 --port 8000
        """
    )

    # 创建子命令解析器
    subparsers = parser.add_subparsers(dest="command", help="可用命令")

    # serve命令 - 启动HTTP服务
    serve_parser = subparsers.add_parser(
        "serve",
        help="启动ChronoForge调度器HTTP服务",
        description="启动ChronoForge调度器HTTP服务，提供RESTful API访问"
    )
    serve_parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help="服务主机地址，默认127.0.0.1"
    )
    serve_parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="服务端口，默认8000"
    )
    serve_parser.add_argument(
        "--reload",
        action="store_true",
        default=False,
        help="启用开发模式，代码修改时自动重启服务"
    )
    serve_parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="工作进程数，默认1"
    )

    # 解析命令行参数
    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(0)

    # 处理serve命令
    if args.command == "serve":
        try:
            from uvicorn import run
            from chronoforge.server.main import app

            print("启动ChronoForge调度器服务...")
            print(f"服务地址: http://{args.host}:{args.port}")
            print(f"API文档: http://{args.host}:{args.port}/docs")
            print("按 Ctrl+C 停止服务")

            # 配置uvicorn日志格式
            log_config = {
                "version": 1,
                "disable_existing_loggers": False,
                "formatters": {
                    "default": {
                        "()": "uvicorn.logging.DefaultFormatter",
                        "fmt": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                        "datefmt": "%Y-%m-%d %H:%M:%S",
                    },
                    "access": {
                        "()": "uvicorn.logging.AccessFormatter",
                        "fmt": "%(asctime)s - %(name)s - %(levelname)s - %(client_addr)s" +
                               " - %(request_line)s - %(status_code)s",
                        "datefmt": "%Y-%m-%d %H:%M:%S",
                    },
                },
                "handlers": {
                    "default": {
                        "formatter": "default",
                        "class": "logging.StreamHandler",
                        "stream": "ext://sys.stderr",
                    },
                    "access": {
                        "formatter": "access",
                        "class": "logging.StreamHandler",
                        "stream": "ext://sys.stdout",
                    },
                },
                "loggers": {
                    "uvicorn": {
                        "handlers": ["default"],
                        "level": "INFO",
                    },
                    "uvicorn.error": {
                        "level": "INFO",
                    },
                    "uvicorn.access": {
                        "handlers": ["access"],
                        "level": "INFO",
                        "propagate": False,
                    },
                    # 添加应用程序日志配置
                    "chronoforge": {
                        "handlers": ["default"],
                        "level": "INFO",
                        "propagate": False,
                    },
                },
            }

            # 运行uvicorn服务器
            run(
                app=app,
                host=args.host,
                port=args.port,
                reload=args.reload,
                workers=args.workers,
                log_level="info",
                log_config=log_config
            )
        except ImportError as e:
            print(f"错误: 无法导入依赖包: {e}")
            print("请确保已安装所有依赖: pip install -e .")
            sys.exit(1)
        except KeyboardInterrupt:
            print("\n服务已停止")
            sys.exit(0)
        except Exception as e:
            print(f"错误: 启动服务失败: {e}")
            sys.exit(1)

    else:
        parser.print_help()
        sys.exit(0)


if __name__ == "__main__":
    main()
