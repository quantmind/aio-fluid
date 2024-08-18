import asyncio
import os
import time
from datetime import timedelta
from typing import cast

from fluid.scheduler import TaskRun, every, task
from fluid.scheduler.broker import RedisTaskBroker


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


@task
async def disabled(context: TaskRun) -> float:
    """A task that sleeps for a while"""
    sleep = cast(float, context.params.get("sleep", 0.1))
    await asyncio.sleep(sleep)
    return sleep


@task(cpu_bound=True, schedule=every(timedelta(seconds=5)))
async def cpu_bound(context: TaskRun) -> None:
    """A CPU bound task running on subprocess"""
    time.sleep(1)
    broker = cast(RedisTaskBroker, context.task_manager.broker)
    redis = broker.redis_cli
    await redis.setex(context.id, os.getpid(), 10)
