import asyncio
from dataclasses import dataclass, field

from fluid.utils.worker import QueueConsumerWorker, Workers


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
    assert consumer.queue_size() == 0
    consumer.send("test")
    assert consumer.queue_size() == 1
    runner = Workers(consumer)
    await runner.startup()
    assert consumer.is_running()
    assert await process.waiter == "test"
    assert consumer.queue_size() == 0
    runner.gracefully_stop()
