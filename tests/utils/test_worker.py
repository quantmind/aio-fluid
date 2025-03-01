import asyncio
from dataclasses import dataclass, field

import pytest

from fluid.utils.errors import WorkerStartError
from fluid.utils.worker import QueueConsumerWorker, Worker, Workers, WorkerState


class NiceWorker(Worker):
    async def run(self):
        while self.is_running():
            await asyncio.sleep(0.1)


class BadWorker(Worker):
    async def run(self):
        while True:
            await asyncio.sleep(0.1)


class BadWorker2(Worker):
    async def run(self):
        await asyncio.sleep(0.1)
        raise RuntimeError("I'm so bad")


class BadWorker3(Worker):
    async def run(self):
        await asyncio.sleep(0.1)
        raise asyncio.CancelledError


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


async def test_metadata() -> None:
    worker = BadWorker(stopping_grace_period=2)
    assert worker.num_workers == 1
    assert list(worker.workers()) == [worker]
    await worker.wait_for_shutdown()
    assert worker.worker_state is WorkerState.INIT


async def test_force_shutdown() -> None:
    worker = BadWorker(stopping_grace_period=2)
    await worker.startup()
    with pytest.raises(WorkerStartError):
        await worker.startup()
    assert worker.is_running()
    await worker.shutdown()
    assert worker.is_stopped()
    assert worker.worker_state is WorkerState.FORCE_STOPPED


async def test_exeception2() -> None:
    worker = BadWorker2()
    await worker.startup()
    with pytest.raises(RuntimeError) as exc:
        await worker.wait_for_shutdown()
    assert str(exc.value) == "I'm so bad"
    assert worker.is_stopped()


async def test_exeception3() -> None:
    worker = BadWorker3()
    await worker.startup()
    with pytest.raises(asyncio.CancelledError):
        await worker.wait_for_shutdown()
    assert worker.is_stopped()


async def test_workers() -> None:
    workers = Workers(NiceWorker(), BadWorker(stopping_grace_period=2))
    assert workers.num_workers == 2
    assert workers.has_started() is False
    await workers.startup()
    assert workers.has_started()
    status = await workers.status()
    assert status
    assert workers.is_running()
    [nice, bad] = list(workers.workers())
    assert nice.is_running()
    nice.gracefully_stop()
    assert nice.is_stopping()
    assert bad.is_running()
    await workers.wait_for_shutdown()
    assert workers.is_stopped()
