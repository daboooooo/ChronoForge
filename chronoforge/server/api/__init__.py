from .tasks import router as tasks_router
from .plugins import router as plugins_router
from .status import router as status_router

__all__ = ["tasks_router", "plugins_router", "status_router"]
