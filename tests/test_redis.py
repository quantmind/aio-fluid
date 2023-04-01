import asyncio
from dataclasses import dataclass, field
from typing import Any, List, NamedTuple

from fluid import json
from fluid.redis import FluidRedis
from fluid.utils import wait_for


class Loader(NamedTuple):
    value: Any

    async def __call__(self):
        return self.value


@dataclass
class MessageHandler:
    data: List = field(default_factory=list)

    def __call__(self, message):
        self.data.append(message)


async def test_receiver(redis: FluidRedis):
    on_message = MessageHandler()

    receiver = redis.receiver(
        on_message=on_message, channels=["test"], patterns=["blaaa-*"]
    )
    await receiver.start()
    await asyncio.sleep(0.5)
    await redis.cli.publish("test", json.dumps(dict(result="test")))
    await wait_for(lambda: len(on_message.data) > 0)
    msg = on_message.data[0]
    assert msg["channel"] == b"test"
    assert json.loads(msg["data"]) == dict(result="test")

    #
    channels = await redis.cli.pubsub_channels()
    assert b"test" in channels
    #
    await receiver.close()
    await asyncio.sleep(0.5)
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
