from setuptools import setup, find_packages
import pathlib

# 获取项目根目录
HERE = pathlib.Path(__file__).parent

# 读取README.md内容作为long_description
README = (HERE / "README.md").read_text(encoding="utf-8")

# 直接定义版本信息，避免在构建时导入模块
__version__ = "0.1.0"
__description__ = "异步、插件式时间序列数据处理框架"
__author__ = "Daboooooo"

setup(
    name="chrono_forge",  # package name for pip install
    version=__version__,
    description=__description__,
    long_description=README,
    long_description_content_type="text/markdown",
    author=__author__ or "Daboooooo",
    author_email="horsen666@gmail.com",
    url="https://github.com/daboooooo/ChronoForge",
    packages=find_packages(),
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",  # 根据LICENSE文件调整
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',
    install_requires=[
        "pyarrow>=22.0.0",
        "duckdb>=1.4.1",
        "pandas>=2.3.3",
        "asyncio>=3.4.3",
        "ccxt>=4.5.18",
        "psutil>=7.1.3",
        "redis>=7.1.0",
        "yfinance==0.2.66",
        "pycoingecko==3.2.0",
        "fredapi==0.5.2",
        "binance-futures-connector==4.1.0",
        "fastapi>=0.120.0",
        "uvicorn>=0.38.0",
        "pydantic>=2.12.5",
        "rich>=14.2.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-asyncio>=0.18.0",
            "black>=23.0.0",
            "isort>=5.10.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "chronoforge=chronoforge.cli:main",
        ],
    },
)
