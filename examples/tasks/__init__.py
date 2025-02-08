import asyncio
import os
import time
from datetime import timedelta
from typing import cast

from fastapi import FastAPI
from pydantic import BaseModel

from fluid.scheduler import TaskRun, TaskScheduler, every, task
from fluid.scheduler.broker import RedisTaskBroker
from fluid.scheduler.endpoints import setup_fastapi


def task_app() -> FastAPI:
    task_manager = TaskScheduler()
    task_manager.register_from_dict(globals())
    return setup_fastapi(task_manager)


@task
async def dummy(context: TaskRun) -> float:
    """A task that sleeps for a while or errors"""
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


class AddValues(BaseModel):
    a: float = 0
    b: float = 0


# @task
async def add(context: TaskRun[AddValues]) -> None:
    """A task that sleeps for a while"""
    c = context.params.a + context.params.b
    context.logger.info(f"Adding {context.params.a} + {context.params.b} = {c}")


@task(cpu_bound=True, schedule=every(timedelta(seconds=5)))
async def cpu_bound(context: TaskRun) -> None:
    """A CPU bound task running on subprocess

    CPU bound tasks are executed on a subprocess to avoid blocking the event loop.
    """
    time.sleep(1)
    broker = cast(RedisTaskBroker, context.task_manager.broker)
    redis = broker.redis_cli
    await redis.setex(context.id, os.getpid(), 10)
