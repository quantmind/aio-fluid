import asyncio
import logging
import os
import sys
from importlib import import_module
from logging.config import dictConfig

from fluid import kernel, log
from fluid.node import WorkerApplication
from fluid.scheduler import (
    Task,
    TaskConstructor,
    TaskContext,
    TaskDecoratorError,
    TaskExecutor,
    TaskManager,
)

TASK_MANAGER_SPAWN: str = os.getenv("TASK_MANAGER_SPAWN", "")
TASK_MANAGER_APP: str = os.getenv("TASK_MANAGER_APP", "")


class ImproperlyConfigured(RuntimeError):
    pass


def create_task_app() -> WorkerApplication:
    if not TASK_MANAGER_APP:
        raise ImproperlyConfigured("missing TASK_MANAGER_APP environment variable")
    bits = TASK_MANAGER_APP.split(":")
    if len(bits) != 2:
        raise ImproperlyConfigured(
            "TASK_MANAGER_APP must be of the form <module>:<function>"
        )
    mod = import_module(bits[0])
    return getattr(mod, bits[1])()


class RemoteLog:
    def __init__(self, out):
        self.out = out

    def __call__(self, data: bytes) -> None:
        self.out.write(data.decode("utf-8"))


async def spawn(ctx: TaskContext):
    env = dict(os.environ)
    env["TASK_MANAGER_SPAWN"] = "true"
    await kernel.run(
        "python",
        "-W",
        "ignore",
        "-m",
        "fluid.scheduler.cpubound",
        ctx.task.name,
        ctx.run_id,
        result_callback=RemoteLog(sys.stdout),
        error_callback=RemoteLog(sys.stderr),
        env=env,
        stream_output=True,
        stream_error=True,
    )


class CpuTaskConstructor(TaskConstructor):
    def __call__(self, executor: TaskExecutor) -> Task:
        if TASK_MANAGER_SPAWN == "true":
            cpu_executor = executor
        else:
            self.kwargs["name"] = executor.__name__
            cpu_executor = spawn
        return super().__call__(cpu_executor)


def cpu_task(*args, **kwargs) -> Task:
    """Decorator for creating a CPU bound task"""
    if kwargs and args:
        raise TaskDecoratorError("cannot use positional parameters")
    elif kwargs:
        return CpuTaskConstructor(**kwargs)
    elif len(args) > 1:
        raise TaskDecoratorError("cannot use positional parameters")
    elif not args:
        raise TaskDecoratorError("this is a decorator cannot be invoked in this way")
    else:
        return CpuTaskConstructor()(args[0])


async def main(name: str, run_id: str):
    dictConfig(
        log.log_config(logging.INFO, logging.CRITICAL, f"{log.APP_NAME}.task.{name}")
    )
    app = create_task_app()
    task_manager: TaskManager = app["task_manager"]
    await app.startup()
    try:
        task = task_manager.broker.task_from_registry(name)
        await task(task_manager, run_id=run_id)
    finally:
        await app.shutdown()


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main(*sys.argv[1:]))
