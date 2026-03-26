from enum import Enum
from typing import Any, Callable, cast

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Path, Request
from pydantic import BaseModel, Field
from typing_extensions import Annotated, Doc

from fluid.tools_fastapi import app_workers
from fluid.utils.worker import Worker

from .consumer import TaskManager
from .errors import UnknownTaskError
from .models import EmptyParams, Task, TaskInfo, TaskPriority, TaskRun


def get_task_manager_from_request(request: Request) -> TaskManager:
    return get_task_manager(request.app)


def get_task_manager(app: FastAPI) -> TaskManager:
    return cast(TaskManager, app.state.task_manager)


TaskManagerDep = Annotated[TaskManager, Depends(get_task_manager_from_request)]


class TaskUpdate(BaseModel):
    enabled: bool = Field(description="Task enabled or disabled")


class TaskCreate(BaseModel):
    name: str = Field(description="Task name")
    params: dict[str, Any] = Field(description="Task parameters", default_factory=dict)
    priority: TaskPriority | None = Field(default=None, description="Task priority")


def get_router(task_manager: TaskManager) -> APIRouter:
    router = APIRouter()

    router.add_api_route(
        "",
        get_tasks,
        methods=["GET"],
        response_model=list[TaskInfo],
        summary="List Tasks",
        description="Retrieve a list of tasks runs",
    )

    router.add_api_route(
        "-status",
        get_task_status,
        methods=["GET"],
        response_model=dict,
        summary="Task consumer status",
        description="Status of the task consumer",
    )

    router.add_api_route(
        "/{task_name}",
        get_task,
        methods=["GET"],
        response_model=TaskInfo,
        summary="Get a Task",
        description="Retrieve information about a task",
    )

    router.add_api_route(
        "/{task_name}",
        patch_task,
        methods=["PATCH"],
        response_model=TaskInfo,
        summary="Update a task",
        description="Update a task configuration and enable/disable it",
    )

    for task in task_manager.registry.values():
        router.add_api_route(
            f"/{task.name}",
            (
                create_queue_task_no_params(task)
                if task.params_model is EmptyParams
                else create_queue_task(task)
            ),
            methods=["POST"],
            response_model=TaskRun,
            summary=f"Queue a new {task.name} task",
            description=f"Queue a new {task.name} task to be run",
        )

    return router


def create_queue_task_no_params(task: Task) -> Callable:

    async def queue_task(
        task_manager: TaskManagerDep,
    ) -> TaskRun:
        return await task_manager.queue(task)

    return queue_task


def create_queue_task(task: Task) -> Callable:
    TaskParams = task.params_model  # noqa

    async def queue_task(
        task_manager: TaskManagerDep,
        params: TaskParams,  # type: ignore [valid-type]
    ) -> TaskRun:
        kwargs: Any = params.model_dump() if params is not None else {}
        return await task_manager.queue(task, **kwargs)

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


def task_manager_fastapi(
    task_manager: Annotated[
        TaskManager,
        Doc(
            (
                "A [TaskManager][fluid.scheduler.TaskManager], "
                "[TaskConsumer][fluid.scheduler.TaskConsumer] or "
                "[TaskScheduler][fluid.scheduler.TaskScheduler] instance"
            )
        ),
    ],
    *,
    app: Annotated[
        FastAPI | None,
        Doc("FastAPI app instance. If not provided, a new instance is created."),
    ] = None,
    include_router: Annotated[
        bool,
        Doc("Whether to include the task manager router in the FastAPI app."),
    ] = True,
    prefix: Annotated[
        str,
        Doc("Prefix for the task manager routes."),
    ] = "/tasks",
    tags: Annotated[
        list[str | Enum] | None,
        Doc("Tags for the task manager routes."),
    ] = None,
    **kwargs: Annotated[
        Any,
        Doc("Additional keyword arguments for the FastAPI app if not provided"),
    ],
) -> FastAPI:
    """Setup the FastAPI app and add the task manager to the state

    If the task manager is a [Worker][fluid.utils.worker.Worker], it is also added
    to the app workers to be started with the app.
    """
    app = app or FastAPI(**kwargs)
    if include_router:
        tags_ = tags if tags is not None else ["Tasks"]
        app.include_router(get_router(task_manager), prefix=prefix, tags=tags_)
    app.state.task_manager = task_manager
    if isinstance(task_manager, Worker):
        app_workers(app).add_workers(task_manager)
    else:
        app.router.on_startup.append(task_manager.on_startup)
        app.router.on_shutdown.append(task_manager.on_shutdown)
    return app
