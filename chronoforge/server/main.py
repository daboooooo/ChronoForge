from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from chronoforge.scheduler import Scheduler
from .api import tasks_router, plugins_router, status_router
from .dependencies import set_scheduler, get_scheduler_instance
from chronoforge import __version__


def create_app():
    """创建FastAPI应用"""
    app = FastAPI(
        title="ChronoForge Scheduler API",
        description="RESTful API for ChronoForge Scheduler",
        version=__version__,
        docs_url="/docs",
        redoc_url="/redoc"
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

    # 启动事件
    @app.on_event("startup")
    async def startup_event():
        """应用启动事件"""
        # 初始化Scheduler实例
        scheduler = Scheduler()
        # 设置全局Scheduler实例
        set_scheduler(scheduler)
        # 启动调度器
        scheduler.start()

    # 关闭事件
    @app.on_event("shutdown")
    async def shutdown_event():
        """应用关闭事件"""
        # 获取全局Scheduler实例
        scheduler = get_scheduler_instance()
        if scheduler is not None:
            # 停止调度器
            scheduler.stop()

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
