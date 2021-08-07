import asyncio
import os

import pytest

from fluid.redis import FluidRedis
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
    cli = FluidRedis()
    try:
        yield cli
    finally:
        await cli.close()


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
