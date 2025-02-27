import asyncio
import os
import time
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Self, cast

from fastapi import FastAPI
from pydantic import BaseModel, Field

from fluid.scheduler import TaskRun, TaskScheduler, every, task
from fluid.scheduler.broker import RedisTaskBroker
from fluid.scheduler.endpoints import setup_fastapi
from fluid.utils.http_client import HttpxClient


@dataclass
class Deps:
    http_client: HttpxClient = field(default_factory=HttpxClient)

    @classmethod
    def get(cls, context: TaskRun) -> Self:
        return context.deps


def task_scheduler(*, deps: Deps | None = None, **kwargs: Any) -> TaskScheduler:
    deps = deps or Deps()
    task_manager = TaskScheduler(deps=deps, **kwargs)
    task_manager.add_async_context_manager(deps.http_client)
    task_manager.register_from_dict(globals())
    return task_manager


def task_app() -> FastAPI:
    return setup_fastapi(task_scheduler())


class Sleep(BaseModel):
    sleep: float = Field(default=0.1, ge=0, description="Sleep time")
    error: bool = False


@task(max_concurrency=1, timeout_seconds=120)
async def dummy(context: TaskRun[Sleep]) -> None:
    """A task that sleeps for a while or errors"""
    await asyncio.sleep(context.params.sleep)
    if context.params.error:
        raise RuntimeError("just an error")


@task(schedule=every(timedelta(seconds=2)))
async def ping(context: TaskRun) -> None:
    """A simple scheduled task that ping the broker"""
    redis_cli = cast(RedisTaskBroker, context.task_manager.broker).redis_cli
    await redis_cli.ping()


class AddValues(BaseModel):
    a: float = Field(default=0, description="First number to add")
    b: float = Field(default=0, description="Second number to add")


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


class Scrape(BaseModel):
    url: str = Field(default="https://httpbin.org/get", description="URL to scrape")


@task
async def scrape(context: TaskRun[Scrape]) -> None:
    """Scrape a website"""
    deps = Deps.get(context)
    response = await deps.http_client.get(context.params.url, callback=True)
    text = await response.text()
    context.logger.info(text)
