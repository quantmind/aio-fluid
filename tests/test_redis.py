import asyncio
from typing import Any, NamedTuple

from fluid.redis import FluidRedis


class Loader(NamedTuple):
    value: Any

    async def __call__(self):
        return self.value


async def test_receiver(redis: FluidRedis):
    def on_message(message):
        pass

    receiver = redis.receiver(
        on_message=on_message, channels=["test"], patterns=["blaaa-*"]
    )
    await receiver.setup()
    channels = await redis.cli.pubsub_channels()
    assert b"test" in channels
    await receiver.teardown()
    channels = await redis.cli.pubsub_channels()
    assert b"test" not in channels


async def test_cache(redis: FluidRedis):

    data = await redis.from_cache("whatever-test", expire=1, loader=Loader(400))
    assert data == 400
    data = await redis.from_cache("whatever-test", expire=1, loader=Loader(401))
    assert data == 400
    await asyncio.sleep(1.1)
    data = await redis.from_cache("whatever-test", expire=1, loader=Loader(402))
    assert data == 402
