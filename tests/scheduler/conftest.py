import os

import pytest
from aiohttp.web import Application
from openapi.rest import rest

from fluid.scheduler import Consumer, Scheduler, TaskManager
from fluid.webcli import app_cli

from . import tasks

os.environ["PYTHON_ENV"] = "test"


@pytest.fixture(scope="module")
async def task_app(loop) -> Consumer:
    cli = rest()
    app = cli.get_serve_app()
    app["consumer"] = task_manager(app, Consumer())
    app["scheduler"] = task_manager(app, Scheduler())
    async with app_cli(app):
        yield app


@pytest.fixture()
def consumer(task_app) -> Consumer:
    return task_app["consumer"]


@pytest.fixture()
def scheduler(task_app) -> Scheduler:
    return task_app["scheduler"]


def task_manager(app: Application, manager: TaskManager) -> TaskManager:
    manager.register_task(tasks.dummy)
    manager.register_task(tasks.scheduled)
    app.on_startup.append(manager.start_app)
    app.on_shutdown.append(manager.close_app)
    return manager
