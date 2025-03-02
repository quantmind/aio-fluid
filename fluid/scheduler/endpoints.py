from typing import Annotated, Any, Callable, cast

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Path, Request
from pydantic import BaseModel, Field

from fluid.scheduler import (
    Task,
    TaskInfo,
    TaskManager,
    TaskPriority,
    TaskRun,
)
from fluid.scheduler.errors import UnknownTaskError
from fluid.tools_fastapi import app_workers
from fluid.utils.worker import Worker


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


def get_router(task_manager: TaskManager) -> APIRouter:
    router = APIRouter()

    router.add_api_route(
        "/tasks",
        get_tasks,
        methods=["GET"],
        response_model=list[TaskInfo],
        summary="List Tasks",
        description="Retrieve a list of tasks runs",
    )

    router.add_api_route(
        "/tasks-status",
        get_task_status,
        methods=["GET"],
        response_model=dict,
        summary="Task consumer status",
        description="Status of the task consumer",
    )

    router.add_api_route(
        "/tasks/{task_name}",
        get_task,
        methods=["GET"],
        response_model=TaskInfo,
        summary="Get a Task",
        description="Retrieve information about a task",
    )

    router.add_api_route(
        "/tasks/{task_name}",
        patch_task,
        methods=["PATCH"],
        response_model=TaskInfo,
        summary="Update a task",
        description="Update a task configuration and enable/disable it",
    )

    for task in task_manager.registry.values():
        router.add_api_route(
            f"/tasks/{task.name}",
            create_queue_task(task),
            methods=["POST"],
            response_model=TaskRun,
            summary=f"Queue a new {task.name} task",
            description=f"Queue a new {task.name} task to be run",
        )

    return router


def create_queue_task(task: Task) -> Callable:
    TaskParams = task.params_model or BaseModel  # noqa

    async def queue_task(
        task_manager: TaskManagerDep,
        params: TaskParams | None = None,  # type: ignore [valid-type]
    ) -> TaskRun:
        try:
            return await task_manager.queue(
                task,
                params=params.model_dump() if params is not None else {},  # type: ignore
            )
        except UnknownTaskError as exc:
            raise HTTPException(status_code=404, detail="Task not found") from exc

    return queue_task


async def get_tasks(task_manager: TaskManagerDep) -> list[TaskInfo]:
    return await task_manager.broker.get_tasks_info()


async def get_task(
    task_manager: TaskManagerDep,
    task_name: str = Path(title="Task name"),
) -> TaskInfo:
    data = await task_manager.broker.get_tasks_info(task_name)
    if not data:
        raise HTTPException(status_code=404, detail="Task not found")
    return data[0]


async def get_task_status(task_manager: TaskManagerDep) -> dict:
    if isinstance(task_manager, Worker):
        return await task_manager.status()
    return {}


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
        app.include_router(get_router(task_manager), tags=["Tasks"])
    app.state.task_manager = task_manager
    if isinstance(task_manager, Worker):
        app_workers(app).add_workers(task_manager)
    else:
        app.add_event_handler("startup", task_manager.on_startup)
        app.add_event_handler("shutdown", task_manager.on_shutdown)
    return app
