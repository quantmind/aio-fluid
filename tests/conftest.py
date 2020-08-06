import asyncio

import pytest
from openapi.rest import rest

from fluid.redis import RedisPubSub
from fluid.scheduler import Scheduler
from fluid.webcli import app_cli


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
    scheduler = Scheduler()
    app.on_startup.append(scheduler.start_app)
    app.on_shutdown.append(scheduler.close_app)
    async with app_cli(app) as client:
        try:
            yield scheduler
        finally:
            await client.close()
