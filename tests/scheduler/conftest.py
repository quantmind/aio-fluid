import os
from typing import AsyncIterator, cast

import pytest
from redis.asyncio import Redis

from fluid.tools_aiohttp.node import WorkerApplication
from fluid.scheduler import TaskConsumer, TaskScheduler
from fluid.scheduler.broker import RedisBroker

from .tasks import add_task_manager, task_application

os.environ["TASK_MANAGER_APP"] = "tests.scheduler.tasks:task_application"


@pytest.fixture(scope="module")
async def task_app() -> AsyncIterator[WorkerApplication]:
    app = task_application(TaskConsumer())
    add_task_manager(app, TaskScheduler())
    await app.startup()
    try:
        yield app
    finally:
        await app.shutdown()


@pytest.fixture
def task_consumer(task_app: WorkerApplication) -> TaskConsumer:
    return cast(TaskConsumer, task_app["task_consumer"])


@pytest.fixture
def task_scheduler(task_app: WorkerApplication) -> TaskScheduler:
    return cast(TaskScheduler, task_app["task_scheduler"])


@pytest.fixture
def redis(task_consumer: TaskConsumer) -> Redis:  # type: ignore
    return cast(RedisBroker, task_consumer.broker).redis.cli
