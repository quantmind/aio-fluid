import asyncio
import os

import pytest
from aiohttp.web import Application
from openapi.rest import rest

from fluid.redis import RedisPubSub
from fluid.scheduler import Consumer, Scheduler, TaskContext, TaskManager, task
from fluid.webcli import TestClient, app_cli

from .app import AppClient, create_app

os.environ["PYTHON_ENV"] = "test"


@pytest.fixture(scope="session")
def loop():
    """Return an instance of the event loop."""
    # Shared loop makes everything easier. Just don't mess it up.
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


@pytest.fixture
async def redis(loop):
    cli = RedisPubSub()
    try:
        yield cli
    finally:
        await cli.close()


@pytest.fixture(scope="module")
async def scheduler(loop) -> Scheduler:
    cli = rest()
    cli.index = 0
    app = cli.get_serve_app()
    scheduler = task_manager(app, Scheduler())
    async with app_cli(app) as client:
        try:
            yield scheduler
        finally:
            await client.close()


@pytest.fixture(scope="module")
async def restcli(loop) -> TestClient:
    api_app = create_app()
    app = api_app.web()
    # single_db_connection(app)
    async with app_cli(app) as client:
        cli = AppClient("")
        cli.session = client
        try:
            yield cli
        finally:
            await cli.close()


@pytest.fixture(scope="module")
async def consumer(loop) -> Consumer:
    cli = rest()
    cli.index = 0
    app = cli.get_serve_app()
    consumer = task_manager(app, Consumer())
    async with app_cli(app) as client:
        try:
            yield consumer
        finally:
            await client.close()


def task_manager(app: Application, manager: TaskManager) -> TaskManager:
    manager.register_task(dummy)
    app.on_startup.append(manager.start_app)
    app.on_shutdown.append(manager.close_app)
    return manager


@task
async def dummy(context: TaskContext) -> None:
    await asyncio.sleep(0.1)
    if context.params.get("error"):
        raise RuntimeError("just an error")
