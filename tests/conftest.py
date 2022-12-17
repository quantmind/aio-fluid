import asyncio
import os

import pytest
from openapi.testing import TestClient, app_cli

from fluid.redis import FluidRedis

from .app import AppClient, create_app

os.environ["PYTHON_ENV"] = "test"


@pytest.fixture(scope="module", autouse=True)
def event_loop():
    """Return an instance of the event loop."""
    loop = asyncio.new_event_loop()
    try:
        yield loop
    finally:
        loop.close()


@pytest.fixture
async def redis():
    cli = FluidRedis()
    try:
        yield cli
    finally:
        await cli.close()


@pytest.fixture
async def restcli() -> TestClient:
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
