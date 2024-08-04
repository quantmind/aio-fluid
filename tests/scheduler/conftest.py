import os
from contextlib import asynccontextmanager
from typing import AsyncIterator, cast

import pytest
from fastapi import FastAPI
from redis.asyncio import Redis

from fluid.scheduler import TaskManager, TaskScheduler
from fluid.scheduler.broker import RedisTaskBroker
from fluid.scheduler.endpoints import get_task_manger, setup_fastapi
from tests.scheduler.tasks import task_application

os.environ["TASK_MANAGER_APP"] = "tests.scheduler.tasks:task_application"


@asynccontextmanager
async def start_fastapi(app: FastAPI) -> AsyncIterator:
    async with app.router.lifespan_context(app):
        yield app


@pytest.fixture
async def task_scheduler() -> AsyncIterator[TaskManager]:
    task_manager = task_application(TaskScheduler())
    async with start_fastapi(setup_fastapi(task_manager)) as app:
        yield get_task_manger(app)


@pytest.fixture
def redis(task_scheduler: TaskScheduler) -> Redis:  # type: ignore
    return cast(RedisTaskBroker, task_scheduler.broker).redis.redis_cli
