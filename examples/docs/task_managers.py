from fastapi import FastAPI

from examples.docs import task_deps
from fluid.scheduler import TaskManager, TaskScheduler, task_manager_fastapi


def api_app() -> FastAPI:
    """Frontend app, it queues task runs but never executes them.

    It registers the tasks because a task must be in the registry to be queued,
    but it needs no dependencies: nothing runs here.
    """
    task_manager = TaskManager()
    task_manager.register_from_module(task_deps)
    return task_manager_fastapi(task_manager, title="Task producer")


def worker_app() -> FastAPI:
    """Worker app, it schedules and executes the same tasks."""
    deps = task_deps.Deps()
    scheduler = TaskScheduler(deps=deps)
    scheduler.add_async_context_manager(deps.http_client)
    scheduler.register_from_module(task_deps)
    return task_manager_fastapi(scheduler, title="Task worker")
