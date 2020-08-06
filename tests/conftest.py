import asyncio

import pytest

from fluid.redis import RedisPubSub


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
