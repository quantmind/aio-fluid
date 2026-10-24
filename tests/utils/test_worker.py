import asyncio
from dataclasses import dataclass, field

import pytest

from fluid.utils.errors import WorkerStartError
from fluid.utils.worker import (
    QueueConsumer,
    QueueConsumerWorker,
    Worker,
    Workers,
    WorkerState,
    WorkerTaskRunner,
)


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


class BlockingConsumer(QueueConsumer[str]):
    """A consumer which waits for messages with a long timeout."""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.messages: list[str | None] = []

    async def run(self):
        while self.is_running():
            self.messages.append(await self.get_message(timeout=30))


async def test_run_worker_already_running() -> None:
    worker = NiceWorker()
    await worker.startup()
    waiter = asyncio.get_event_loop().create_future()
    with pytest.raises(WorkerStartError):
        await WorkerTaskRunner(worker).run(waiter)
    assert not waiter.done()
    assert worker.is_running()
    await worker.shutdown()
    assert worker.worker_state is WorkerState.STOPPED


async def test_queue_consumer_status() -> None:
    consumer = BlockingConsumer()
    consumer.send("a")
    consumer.send("b")
    assert await consumer.status() == {
        "stopping": False,
        "running": False,
        "queue_size": 2,
    }


async def test_get_message_cancelled_when_not_stopping() -> None:
    consumer = BlockingConsumer()
    task = asyncio.create_task(consumer.get_message(timeout=30))
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_get_message_cancelled_while_stopping() -> None:
    """A consumer blocked on the queue exits when force cancelled on shutdown."""
    consumer = BlockingConsumer(stopping_grace_period=0.1)
    await consumer.startup()
    consumer.send("hello")
    await asyncio.sleep(0.05)
    assert consumer.messages == ["hello"]
    await consumer.shutdown()
    # the cancellation is swallowed by get_message, so run exits normally
    assert consumer.messages == ["hello", None]
    assert consumer.is_stopped()


async def test_worker_start_error_message() -> None:
    worker = NiceWorker(name="nice-one")
    await worker.startup()
    try:
        with pytest.raises(WorkerStartError) as exc:
            await worker.startup()
        assert str(exc.value) == "worker nice-one already started: RUNNING"
        waiter = asyncio.get_event_loop().create_future()
        with pytest.raises(WorkerStartError) as exc:
            await WorkerTaskRunner(worker).run(waiter)
        assert str(exc.value) == "worker nice-one already running"
    finally:
        await worker.shutdown()


class CancellingConsumer(QueueConsumer[str]):
    """Records the pending cancellations of its task after each message."""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.cancelling: list[int] = []

    async def run(self):
        task = asyncio.current_task()
        assert task is not None
        while self.is_running():
            await self.get_message(timeout=30)
            self.cancelling.append(task.cancelling())


async def test_get_message_uncancels_on_forced_shutdown() -> None:
    """A consumer stopped while waiting on its queue exits cleanly, with no
    cancellation left pending on its task"""
    consumer = CancellingConsumer(stopping_grace_period=0.1)
    await consumer.startup()
    await asyncio.sleep(0.05)
    await consumer.shutdown()
    assert consumer.cancelling == [0]
    assert consumer.worker_state is WorkerState.STOPPED
