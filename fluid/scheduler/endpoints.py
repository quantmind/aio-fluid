from typing import Annotated, Any, cast

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Path, Request
from pydantic import BaseModel, Field

from fluid.scheduler import (
    TaskInfo,
    TaskManager,
    TaskPriority,
    TaskRun,
)
from fluid.scheduler.errors import UnknownTaskError
from fluid.tools_fastapi import app_workers
from fluid.utils.worker import Worker

router = APIRouter()


def get_task_manger_from_request(request: Request) -> TaskManager:
    return get_task_manger(request.app)


def get_task_manger(app: FastAPI) -> TaskManager:
    return cast(TaskManager, app.state.task_manager)


TaskManagerDep = Annotated[TaskManager, Depends(get_task_manger_from_request)]


class TaskUpdate(BaseModel):
    enabled: bool = Field(description="Task enabled or disabled")


class TaskCreate(BaseModel):
    name: str = Field(description="Task name")
    params: dict[str, Any] = Field(description="Task parameters", default_factory=dict)
    priority: TaskPriority | None = Field(default=None, description="Task priority")


@router.get(
    "/tasks",
    response_model=list[TaskInfo],
    summary="List Tasks",
    description="Retrieve a list of tasks runs",
)
async def get_tasks(task_manager: TaskManagerDep) -> list[TaskInfo]:
    return await task_manager.broker.get_tasks_info()


@router.get(
    "/tasks/status",
    response_model=dict,
    summary="Task consumer status",
    description="Retrieve a list of tasks runs",
)
async def get_task_status(task_manager: TaskManagerDep) -> dict:
    if isinstance(task_manager, Worker):
        return await task_manager.status()
    return {}


@router.post(
    "/tasks",
    response_model=TaskRun,
    summary="Queue a new Tasks",
    description="Queue a new task to be run",
)
async def queue_task(
    task_manager: TaskManagerDep,
    task: TaskCreate,
) -> TaskRun:
    try:
        return await task_manager.queue(task.name, task.priority, **task.params)
    except UnknownTaskError as exc:
        raise HTTPException(status_code=404, detail="Task not found") from exc


@router.patch(
    "/tasks/{task_name}",
    response_model=TaskInfo,
    summary="Update a task",
    description="Update a task configuration and enable/disable it",
)
async def patch_task(
    task_manager: TaskManagerDep,
    task_update: TaskUpdate,
    task_name: str = Path(title="Task name"),
) -> TaskInfo:
    try:
        return await task_manager.broker.enable_task(task_name, task_update.enabled)
    except UnknownTaskError as exc:
        raise HTTPException(status_code=404, detail="Task not found") from exc


def setup_fastapi(
    task_manager: TaskManager,
    *,
    app: FastAPI | None = None,
    include_router: bool = True,
    **kwargs: Any,
) -> FastAPI:
    """Setup the FastAPI app and add the task manager to the state"""
    app = app or FastAPI(**kwargs)
    if include_router:
        app.include_router(router, tags=["Tasks"])
    app.state.task_manager = task_manager
    if isinstance(task_manager, Worker):
        app_workers(app).add_workers(task_manager)
    return app
