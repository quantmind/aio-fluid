from fastapi import FastAPI

from fluid.scheduler import TaskManager
from fluid.utils.worker import Workers

from .endpoints import router
from .service import FastapiAppWorkers


def app_workers(app: FastAPI) -> Workers:
    if workers := getattr(app.state, "workers", None):
        return workers
    else:
        workers = FastapiAppWorkers.setup(app)
        app.state.workers = workers
        return workers


def task_manager_routes(app: FastAPI, task_manager: TaskManager) -> None:
    app.state.task_manager = task_manager
    app.include_router(router, tags=["Tasks"])
    app_workers(app).add_workers(task_manager)
