import asyncio
from dataclasses import dataclass, field

import pytest

from fluid.utils.worker import QueueConsumerWorker


@dataclass
class Waiter:
    waiter: asyncio.Future = field(
        default_factory=lambda: asyncio.get_event_loop().create_future()
    )

    async def __call__(self, message):
        self.waiter.set_result(message)


async def test_consumer() -> None:
    process = Waiter()
    consumer = QueueConsumerWorker(process)
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
