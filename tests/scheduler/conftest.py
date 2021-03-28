import os

import pytest
from aiohttp.web import Application
from openapi.rest import rest

from fluid.scheduler import TaskConsumer, TaskManager, TaskScheduler
from fluid.webcli import app_cli

from . import tasks

os.environ["PYTHON_ENV"] = "test"


@pytest.fixture(scope="module")
async def task_app(loop) -> TaskConsumer:
    cli = rest()
    app = cli.get_serve_app()
    app["consumer"] = task_manager(app, TaskConsumer())
    app["scheduler"] = task_manager(app, TaskScheduler())
    async with app_cli(app):
        yield app


@pytest.fixture()
def consumer(task_app) -> TaskConsumer:
    return task_app["consumer"]


@pytest.fixture()
def scheduler(task_app) -> TaskScheduler:
    return task_app["scheduler"]


def task_manager(app: Application, manager: TaskManager) -> TaskManager:
    manager.register_task(tasks.dummy)
    manager.register_task(tasks.scheduled)
    app.on_startup.append(manager.start_app)
    app.on_shutdown.append(manager.close_app)
    return manager
