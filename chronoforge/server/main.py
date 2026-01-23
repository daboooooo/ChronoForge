import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from chronoforge.scheduler import Scheduler
from .api import tasks_router, plugins_router, status_router, compatibility_router
from .dependencies import set_scheduler, get_scheduler_instance
from chronoforge import __version__
from chronoforge.logging_config import setup_logging, get_logger

# 配置统一日志
setup_logging()
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    # 启动事件逻辑
    scheduler = Scheduler()
    set_scheduler(scheduler)

    scheduler.start()
    yield  # 应用运行期间

    # 关闭事件逻辑
    scheduler = get_scheduler_instance()
    if scheduler is not None:
        await scheduler.async_stop()


def create_app():
    """创建FastAPI应用"""
    app = FastAPI(
        title="ChronoForge Scheduler API",
        description="RESTful API for ChronoForge Scheduler",
        version=__version__,
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan
    )

    # 配置CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # 在生产环境中应该限制允许的域名
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(tasks_router, prefix="/api")
    app.include_router(plugins_router, prefix="/api")
    app.include_router(status_router, prefix="/api")
    app.include_router(compatibility_router, prefix="/api")

    # 根路径
    @app.get("/")
    async def root():
        return {
            "message": "ChronoForge Scheduler API",
            "docs": "/docs",
            "redoc": "/redoc",
            "version": __version__
        }

    return app


# 创建应用实例
app = create_app()
