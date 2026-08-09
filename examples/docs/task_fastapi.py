from typing import Annotated, cast

from fastapi import APIRouter, Depends, FastAPI

from examples.docs.task_deps import Deps, task_scheduler
from fluid.scheduler import task_manager_fastapi
from fluid.scheduler.endpoints import TaskManagerDep
from fluid.utils.http_client import ResponseType


def get_deps(task_manager: TaskManagerDep) -> Deps:
    """Typed access to the task manager dependencies."""
    return cast(Deps, task_manager.deps)


DepsDep = Annotated[Deps, Depends(get_deps)]

router = APIRouter()


@router.get("/quotes/{symbol}")
async def get_quote(symbol: str, deps: DepsDep) -> ResponseType:
    """Fetch a quote with the same HTTP client the tasks use."""
    return await deps.http_client.get(f"https://api.example.com/quotes/{symbol}")


def scheduler_app() -> FastAPI:
    app = FastAPI(title="Quotes API")
    app.include_router(router)
    return task_manager_fastapi(task_scheduler(), app=app)
