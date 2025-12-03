#!/usr/bin/env python3
import os
import re
from setuptools import setup, find_packages


# 安全读取 __version__，避免触发依赖导入
def get_version():
    init_path = os.path.join(os.path.dirname(__file__), 'chronoforge', '__init__.py')
    with open(init_path, 'r', encoding='utf-8') as f:
        content = f.read()
    version_match = re.search(r'__version__\s*=\s*"([^"]+)"', content)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("无法从 __init__.py 中读取版本号")


# 安全读取 __description__
def get_description():
    init_path = os.path.join(os.path.dirname(__file__), 'chronoforge', '__init__.py')
    with open(init_path, 'r', encoding='utf-8') as f:
        content = f.read()
    desc_match = re.search(r'__description__\s*=\s*"([^"]+)"', content)
    if desc_match:
        return desc_match.group(1)
    return "异步、插件式的时间序列数据处理框架"


setup(
    name="chrono_forge",
    version=get_version(),
    description=get_description(),
    author="Daboooooo",
    author_email="horsen666@gmail.com",
    url="https://github.com/yourusername/chronoforge",
    packages=find_packages(include=['chronoforge', 'chronoforge.*']),
    install_requires=[
        "pyarrow>=22.0.0",
        "duckdb>=1.4.1",
        "pandas>=2.3.3",
        "asyncio>=3.4.3",
        "psutil>=7.1.3",
        "redis>=7.1.0",
        "ccxt>=4.5.18",
        "yfinance==0.2.66",
        "pycoingecko==3.2.0",
        "fredapi==0.5.2",
        "binance-futures-connector==4.1.0",
        "fastapi>=0.120.0",
        "uvicorn>=0.38.0",
        "pydantic>=2.12.5",
        "rich>=14.2.0"
    ],
    entry_points={
        "console_scripts": [
            "chronoforge=chronoforge.cli:main"
        ]
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent"
    ],
    python_requires='>=3.8'
)
