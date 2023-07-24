import asyncio
import logging
import os
import sys
from logging.config import dictConfig
from typing import Any, overload

from fluid import kernel, log
from fluid.scheduler import (
    Scheduler,
    Task,
    TaskConstructor,
    TaskContext,
    TaskDecoratorError,
    TaskExecutor,
    TaskManager,
    TaskPriority,
    TaskRun,
    TaskState,
    create_task_app,
    settings,
)
from fluid.scheduler.task import RandomizeType


class RemoteLog:
    def __init__(self, out: Any) -> None:
        self.out = out

    def __call__(self, data: bytes) -> None:
        self.out.write(data.decode("utf-8"))


async def spawn(ctx: TaskContext) -> None:
    env = dict(os.environ)
    env["TASK_MANAGER_SPAWN"] = "true"
    result = await kernel.run(
        "python",
        "-W",
        "ignore",
        "-m",
        "fluid.scheduler.cpubound",
        ctx.name,
        ctx.task_run.id,
        result_callback=RemoteLog(sys.stdout),
        error_callback=RemoteLog(sys.stderr),
        env=env,
        stream_output=True,
        stream_error=True,
    )
    if result:
        ctx.task_run.set_state(TaskState.failure)


class CpuTaskConstructor(TaskConstructor):
    def __call__(self, executor: TaskExecutor) -> Task:
        if settings.TASK_MANAGER_SPAWN == "true":
            cpu_executor = executor
        else:
            self.kwargs["name"] = executor.__name__
            cpu_executor = spawn
        return super().__call__(cpu_executor)


@overload
def cpu_task(executor: TaskExecutor) -> Task:
    ...


@overload
def cpu_task(
    *,
    name: str | None = None,
    schedule: Scheduler | None = None,
    description: str | None = None,
    randomize: RandomizeType | None = None,
    max_concurrency: int = 1,
    priority: TaskPriority = TaskPriority.medium,
) -> CpuTaskConstructor:
    ...


def cpu_task(
    executor: TaskExecutor | None = None, **kwargs: Any
) -> Task | CpuTaskConstructor:
    """Decorator for creating a CPU bound task"""
    if kwargs and executor:
        raise TaskDecoratorError("cannot use positional parameters")
    elif kwargs:
        return CpuTaskConstructor(**kwargs)
    elif not executor:
        raise TaskDecoratorError("this is a decorator cannot be invoked in this way")
    else:
        return CpuTaskConstructor()(executor)


async def main(name: str, run_id: str) -> int:
    dictConfig(
        log.log_config(logging.INFO, logging.CRITICAL, f"{log.APP_NAME}.task.{name}")
    )
    app = create_task_app()
    task_manager: TaskManager = app["task_manager"]
    await app.startup()
    logger = task_manager.logger
    try:
        task = task_manager.broker.task_from_registry(name)
        task_run = TaskRun(id=run_id, queued=0, task=task, params={})
        task_context = task.create_context(task_manager, task_run=task_run)
        logger = task_context.logger
        await task.executor(task_context)
    except Exception:
        logger.exception("Unhandled exception executing CPU task")
        return 1
    finally:
        await app.shutdown()
    return 0


if __name__ == "__main__":
    exit(asyncio.get_event_loop().run_until_complete(main(*sys.argv[1:])))
