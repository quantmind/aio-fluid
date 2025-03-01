from contextlib import asynccontextmanager
from typing import AsyncIterator, Iterator, cast

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from redis.asyncio import Redis

from examples import tasks
from fluid.scheduler import TaskManager, TaskScheduler
from fluid.scheduler.broker import RedisTaskBroker
from fluid.scheduler.endpoints import get_task_manger, setup_fastapi
from fluid.tools_fastapi import backdoor
from fluid.utils.stacksampler import Sampler
from tests.scheduler.tasks import TaskClient


@asynccontextmanager
async def start_fastapi(app: FastAPI) -> AsyncIterator:
    backdoor.setup(app, port=0)
    async with app.router.lifespan_context(app):
        yield app


def redis_broker(task_manager: TaskManager) -> RedisTaskBroker:
    return cast(RedisTaskBroker, task_manager.broker)


@pytest.fixture(scope="module", autouse=True)
def sampler() -> Iterator[Sampler]:
    sampler = Sampler()
    sampler.start()
    yield sampler
    sampler.stop()


@pytest.fixture(scope="module")
async def task_app():
    task_manager = tasks.task_scheduler(max_concurrent_tasks=2, schedule_tasks=False)
    broker = redis_broker(task_manager)
    await broker.clear()
    async with start_fastapi(setup_fastapi(task_manager)) as app:
        yield app


@pytest.fixture(scope="module")
async def task_scheduler(task_app: FastAPI) -> TaskManager:
    return get_task_manger(task_app)


@pytest.fixture(scope="module")
def redis(task_scheduler: TaskScheduler) -> Redis:  # type: ignore
    return redis_broker(task_scheduler).redis.redis_cli


@pytest.fixture(scope="module")
async def cli(task_app):
    base_url = TaskClient().url
    async with AsyncClient(
        transport=ASGITransport(app=task_app), base_url=base_url
    ) as session:
        async with TaskClient(url=base_url, session=session) as client:
            yield client
