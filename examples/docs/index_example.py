import os
from datetime import timedelta

from fastapi import FastAPI
from pydantic import BaseModel

from fluid.scheduler import TaskRun, TaskScheduler, every, task, task_manager_fastapi
from fluid.scheduler.cli import TaskManagerCLI


class Report(BaseModel):
    rows: int = 5_000_000


def heavy_pandas_work(rows: int) -> None:
    """Stand-in for the CPU-heavy work you would do in a real task."""
    sum(range(rows))


@task(schedule=every(timedelta(seconds=5)))
async def heartbeat(ctx: TaskRun) -> None:
    """IO-bound task, scheduled every five seconds

    runs concurrently on the event loop
    """
    ctx.logger.info("still alive")


@task(
    cpu_bound=True,
    schedule=every(timedelta(seconds=20), delay=timedelta(seconds=5)),
    timeout_seconds=600,
)
async def crunch(ctx: TaskRun[Report]) -> None:
    """CPU-bound task, scheduled every 20 seconds with an initial delay of 5 seconds

    Same decorator, one flag. Runs in a subprocess (or a Kubernetes Job in-cluster)
    so the heavy work never blocks the event loop.
    Identical code in both places.
    """
    heavy_pandas_work(ctx.params.rows)
    ctx.logger.info("crunch finished on pid %d", os.getpid())


def scheduler_app() -> FastAPI:
    scheduler = TaskScheduler()
    scheduler.register_from_dict(globals())
    return task_manager_fastapi(scheduler)


if __name__ == "__main__":
    TaskManagerCLI(
        scheduler_app,
        help="Simple Task Manager CLI with default commands",
        log_config=dict(app_names=("__main__", "fluid")),
    )()
