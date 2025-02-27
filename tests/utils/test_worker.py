import asyncio
from dataclasses import dataclass, field

from fluid.utils.worker import QueueConsumerWorker, Worker, Workers, WorkerState


class BadWorker(Worker):
    async def run(self):
        while True:
            await asyncio.sleep(0.1)


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
    assert consumer.is_stopping() is False
    assert await process.waiter == "test"
    assert consumer.queue_size() == 0
    await runner.shutdown()
    assert runner.is_stopped()


async def test_froce_shutdown() -> None:
    worker = BadWorker(stopping_grace_period=2)
    await worker.startup()
    assert worker.is_running()
    await worker.shutdown()
    assert worker.is_stopped()
    assert worker.worker_state is WorkerState.FORCE_STOPPED
