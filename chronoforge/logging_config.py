"""统一的日志配置模块，使用rich库美化日志输出"""

import logging
from typing import Optional
from rich.logging import RichHandler
from rich.console import Console
from rich.theme import Theme

# 自定义日志主题
CUSTOM_THEME = Theme({
    "log.level": "bold cyan",
    "log.time": "dim white",
    "log.name": "bold blue",
    "log.message": "white",
    "log.error": "bold red",
    "log.warning": "bold yellow",
    "log.info": "bold green",
    "log.debug": "dim blue",
})

# 创建控制台实例
console = Console(theme=CUSTOM_THEME, stderr=True)


def setup_logging(level: int = logging.INFO, log_file: Optional[str] = None) -> None:
    """
    配置统一的日志设置

    Args:
        level: 日志级别，默认为logging.INFO
        log_file: 可选的日志文件路径，None表示只输出到控制台
    """
    # 创建根日志记录器
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # 清除现有的处理器，避免重复
    root_logger.handlers.clear()

    # 创建RichHandler配置
    rich_handler = RichHandler(
        console=console,
        show_time=True,
        show_level=True,
        show_path=False,
        rich_tracebacks=True,
        tracebacks_show_locals=True,
        level=level,
    )

    # 设置RichHandler的日志格式
    rich_formatter = logging.Formatter(
        fmt="%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    rich_handler.setFormatter(rich_formatter)
    root_logger.addHandler(rich_handler)

    # 如果指定了日志文件，添加文件处理器
    if log_file:
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(level)
        file_formatter = logging.Formatter(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)

    # 配置特定模块的日志级别
    # 例如，降低第三方库的日志级别
    logging.getLogger("uvicorn").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("ccxt").setLevel(logging.WARNING)
    logging.getLogger("yfinance").setLevel(logging.WARNING)
    logging.getLogger("pymongo").setLevel(logging.WARNING)
    logging.getLogger("redis").setLevel(logging.WARNING)

    # 提高关键模块的日志级别（如果需要）
    logging.getLogger("chronoforge").setLevel(level)


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    获取日志记录器

    Args:
        name: 记录器名称，默认为None

    Returns:
        logging.Logger: 配置好的日志记录器
    """
    return logging.getLogger(name)
