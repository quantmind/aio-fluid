from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, Request
from pydantic import BaseModel, Field

from fluid.scheduler import QueuedTask, TaskInfo, TaskManager, TaskPriority
from fluid.scheduler.errors import UnknownTaskError

router = APIRouter()


def get_task_manger(request: Request) -> TaskManager:
    return request.app.state.task_manager


TaskManagerDep = Annotated[TaskManager, Depends(get_task_manger)]


class TaskUpdate(BaseModel):
    enabled: bool = Field(description="Task enabled or disabled")


class TaskCreate(BaseModel):
    name: str = Field(description="Task name")
    params: dict = Field(description="Task parameters", default_factory=dict)
    priority: TaskPriority | None = Field(default=None, description="Task priority")


@router.get(
    "/tasks",
    response_model=list[TaskInfo],
    summary="List Tasks",
    description="Retrieve a list of tasks runs",
)
async def get_tasks(task_manager: TaskManagerDep) -> list[TaskInfo]:
    return await task_manager.broker.get_tasks_info()


@router.post(
    "/tasks",
    response_model=QueuedTask,
    summary="Queue a new Tasks",
    description="Queue a new task to be run",
)
async def queue_task(
    task_manager: TaskManagerDep,
    task: TaskCreate,
) -> QueuedTask:
    try:
        return task_manager.queue(task.name, task.priority, **task.params)
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
