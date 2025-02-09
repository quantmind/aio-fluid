import asyncio
import os
import time
from datetime import timedelta
from typing import cast

from fastapi import FastAPI
from pydantic import BaseModel, Field

from fluid.scheduler import TaskRun, TaskScheduler, every, task
from fluid.scheduler.broker import RedisTaskBroker
from fluid.scheduler.endpoints import setup_fastapi


def task_app() -> FastAPI:
    task_manager = TaskScheduler()
    task_manager.register_from_dict(globals())
    return setup_fastapi(task_manager)


class Sleep(BaseModel):
    sleep: float = Field(default=0.1, ge=0, description="Sleep time")
    error: bool = False


@task(max_concurrency=1)
async def dummy(context: TaskRun[Sleep]) -> None:
    """A task that sleeps for a while or errors"""
    await asyncio.sleep(context.params.sleep)
    if context.params.error:
        raise RuntimeError("just an error")


@task(schedule=every(timedelta(seconds=2)))
async def scheduled(context: TaskRun) -> None:
    """A simple scheduled task"""
    await asyncio.sleep(0.1)


class AddValues(BaseModel):
    a: float = 0
    b: float = 0


@task
async def add(context: TaskRun[AddValues]) -> None:
    """Log the addition of two numbers"""
    c = context.params.a + context.params.b
    context.logger.info(f"Adding {context.params.a} + {context.params.b} = {c}")


@task(cpu_bound=True)
async def cpu_bound(context: TaskRun) -> None:
    """A CPU bound task running on subprocess

    CPU bound tasks are executed on a subprocess to avoid blocking the event loop.
    """
    time.sleep(1)
    broker = cast(RedisTaskBroker, context.task_manager.broker)
    redis = broker.redis_cli
    await redis.setex(context.id, os.getpid(), 10)
