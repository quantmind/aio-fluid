import os

import pytest

from fluid.node import WorkerApplication
from fluid.redis import Redis
from fluid.scheduler import TaskConsumer, TaskScheduler

from .tasks import add_task_manager, task_application

os.environ["TASK_MANAGER_APP"] = "tests.scheduler.tasks:task_application"


@pytest.fixture(scope="module")
async def task_app() -> WorkerApplication:
    app = task_application(TaskConsumer())
    add_task_manager(app, TaskScheduler())
    await app.startup()
    try:
        yield app
    finally:
        await app.shutdown()


@pytest.fixture
def task_consumer(task_app: WorkerApplication) -> TaskConsumer:
    return task_app["task_consumer"]


@pytest.fixture
def task_scheduler(task_app: WorkerApplication) -> TaskScheduler:
    return task_app["task_scheduler"]


@pytest.fixture
def redis(task_consumer: TaskConsumer) -> Redis:
    return task_consumer.broker.redis.cli
