import asyncio
import os
from datetime import timedelta
from typing import cast

from fluid.scheduler import TaskManager, TaskRun, every, task


@task
async def dummy(context: TaskRun) -> float:
    sleep = cast(float, context.params.get("sleep", 0.1))
    await asyncio.sleep(sleep)
    if context.params.get("error"):
        raise RuntimeError("just an error")
    return sleep


@task(schedule=every(timedelta(seconds=1)))
async def scheduled(context: TaskRun) -> str:
    """A simple scheduled task"""
    await asyncio.sleep(0.1)
    return "OK"


@task
async def disabled(context: TaskRun) -> float:
    sleep = cast(float, context.params.get("sleep", 0.1))
    await asyncio.sleep(sleep)
    return sleep


@task(cpu_bound=True)
async def cpu_bound(context: TaskRun) -> int:
    await asyncio.sleep(0.1)
    redis = context.task_manager.broker.redis.cli  # type: ignore
    await redis.setex(context.run_id, os.getpid(), 10)
    return 0


def add_task_manager(manager: TaskManager) -> TaskManager:
    manager.register_task(dummy)
    manager.register_task(scheduled)
    manager.register_task(disabled)
    manager.register_task(cpu_bound)
    return manager
