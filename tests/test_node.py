import asyncio
from dataclasses import dataclass, field

import pytest

from fluid.tools_aiohttp import node


@dataclass
class Waiter:
    waiter: asyncio.Future = field(
        default_factory=lambda: asyncio.get_event_loop().create_future()
    )

    def __call__(self, message):
        self.waiter.set_result(message)


async def test_node_worker() -> None:
    worker = node.NodeWorker()
    assert worker.name() == "node_worker"
    assert worker.uid == worker.uid


async def test_consumer() -> None:
    process = Waiter()
    consumer = node.Consumer(process)
    assert consumer.qsize() == 0
    with pytest.raises(RuntimeError):
        consumer.submit("test")
    await consumer.start()
    assert consumer.is_running()
    consumer.submit("test")
    assert consumer.qsize() == 1
    assert await process.waiter == "test"
    assert consumer.qsize() == 0
    await consumer.close()
