from typing import Iterator

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from redis.asyncio import Redis

from examples import tasks
from examples.db import get_db
from fluid.scheduler import TaskManager, TaskScheduler
from fluid.scheduler.db import TaskDbPlugin
from fluid.scheduler.endpoints import get_task_manager, task_manager_fastapi
from fluid.utils.stacksampler import Sampler
from tests.scheduler.tasks import TaskClient, redis_broker, start_fastapi


@pytest.fixture(scope="module")
def db_plugin() -> TaskDbPlugin:
    db = get_db()
    plugin = TaskDbPlugin(db)
    mig = db.migration()
    if not mig.db_create():
        mig.drop_all_schemas()
    mig.create_all()
    return plugin


@pytest.fixture(scope="module", autouse=True)
def sampler() -> Iterator[Sampler]:
    sampler = Sampler()
    sampler.start()
    yield sampler
    sampler.stop()


@pytest.fixture(scope="module")
async def task_app():
    task_manager = tasks.task_scheduler(max_concurrent_tasks=2, schedule_tasks=True)
    broker = redis_broker(task_manager)
    await broker.clear()
    async with start_fastapi(task_manager_fastapi(task_manager)) as app:
        yield app


@pytest.fixture(scope="module")
async def task_scheduler(task_app: FastAPI) -> TaskManager:
    return get_task_manager(task_app)


@pytest.fixture(scope="module")
def redis(task_scheduler: TaskScheduler) -> Redis:
    return redis_broker(task_scheduler).redis_cli


@pytest.fixture(scope="module")
async def cli(task_app):
    base_url = TaskClient().url
    async with AsyncClient(
        transport=ASGITransport(app=task_app), base_url=base_url
    ) as session:
        async with TaskClient(url=base_url, session=session) as client:
            yield client
