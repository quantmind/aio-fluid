from __future__ import annotations

import asyncio
import logging
import random
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from typing import (
    AsyncGenerator,
    Awaitable,
    Callable,
    Generator,
    Iterator,
    Protocol,
    Sequence,
    TypeVar,
)

from inflection import underscore

from fluid import settings

from .dispatcher import AsyncDispatcher, MessageType

logger = logging.getLogger(__name__)


class HasWorkers(ABC):
    @abstractmethod
    def get_workers(self) -> Sequence[Worker]:
        """Get the workers that can be added to a the Workers Manager"""


class Worker(ABC):
    """The base class of a worker that can be run"""

    def __init__(self, name: str = "") -> None:
        self._name: str = name or underscore(type(self).__name__)

    @property
    def worker_name(self) -> str:
        """The name of the worker"""
        return self._name

    @property
    def num_workers(self) -> int:
        """The number of workers in this worker"""
        return 1

    @abstractmethod
    async def status(self) -> dict:
        """
        Get the status of the worker.
        """

    @abstractmethod
    def gracefully_stop(self) -> None:
        "gracefully stop the worker"

    @abstractmethod
    def is_running(self) -> bool:
        """Is the worker running?"""

    @abstractmethod
    def is_stopping(self) -> bool:
        """Is the worker stopping?"""

    @abstractmethod
    async def run(self) -> None:
        """run the worker

        THis is the main entry point of the worker.
        """


class RunningWorker(Worker):
    """A Worker that can be started"""

    def __init__(self, name: str = "") -> None:
        super().__init__(name)
        self._running: bool = False

    def is_running(self) -> bool:
        return self._running

    @contextmanager
    def start_running(self) -> Generator:
        if self._running:
            raise RuntimeError("Worker is already running")
        self._running = True
        try:
            logger.info("%s started running", self.worker_name)
            yield
        finally:
            self._running = False
            logger.warning("%s stopped running", self.worker_name)


class StoppingWorker(RunningWorker):
    """A Worker that can be stopped"""

    def __init__(self, name: str = "") -> None:
        super().__init__(name)
        self._stopping: bool = False

    def is_stopping(self) -> bool:
        return self._stopping

    def gracefully_stop(self) -> None:
        self._stopping = True

    async def status(self) -> dict:
        return {"stopping": self.is_stopping(), "running": self.is_running()}


class WorkerFunction(StoppingWorker):
    """A Worker that runs a coroutine function"""

    def __init__(
        self,
        run_function: Callable[[], Awaitable[None]],
        heartbeat: float | int = 0,
        name: str = "",
    ) -> None:
        super().__init__(name=name)
        self._run_function = run_function
        self._heartbeat = heartbeat

    async def run(self) -> None:
        with self.start_running():
            while not self.is_stopping():
                await self._run_function()
                await asyncio.sleep(self._heartbeat)


T = TypeVar("T", contravariant=True)


class MessageProducer(Protocol[T]):
    def send(self, message: T | None) -> None: ...


class QueueConsumer(StoppingWorker, MessageProducer[MessageType]):
    """A Worker that can receive messages

    This worker can receive messages but not consume them.
    """

    def __init__(self, name: str = "") -> None:
        super().__init__(name=name)
        self._queue: asyncio.Queue[MessageType | None] = asyncio.Queue()

    async def get_message(self, timeout: float = 0.5) -> MessageType | None:
        try:
            async with asyncio.timeout(timeout):
                return await self._queue.get()
        except asyncio.TimeoutError:
            return None
        except (asyncio.CancelledError, RuntimeError):
            if not self.is_stopping():
                raise
        return None

    def queue_size(self) -> int:
        return self._queue.qsize()

    async def status(self) -> dict:
        status = await super().status()
        status.update(queue_size=self.queue_size())
        return status

    def send(self, message: MessageType | None) -> None:
        """Send a message into the worker"""
        self._queue.put_nowait(message)


class QueueConsumerWorker(QueueConsumer[MessageType]):
    """A Worker that can receive and consume messages"""

    def __init__(
        self,
        on_message: Callable[[MessageType], Awaitable[None]],
        name: str = "",
    ) -> None:
        super().__init__(name=name)
        self.on_message = on_message

    async def run(self) -> None:
        with self.start_running():
            while not self.is_stopping():
                message = await self.get_message()
                if message is not None:
                    await self.on_message(message)


class AsyncConsumer(QueueConsumer[MessageType]):
    """A Worker that can dispatch async callbacks"""

    AsyncCallback: AsyncDispatcher[MessageType]

    def __init__(
        self, dispatcher: AsyncDispatcher[MessageType], name: str = ""
    ) -> None:
        super().__init__(name)
        self.dispatcher: AsyncDispatcher[MessageType] = dispatcher

    async def run(self) -> None:
        with self.start_running():
            while not self.is_stopping():
                message = await self.get_message()
                if message is not None:
                    await self.dispatcher.dispatch(message)


@dataclass
class WorkerTasks:
    workers: Sequence[Worker] = field(default_factory=list)
    tasks: Sequence[asyncio.Task] = field(default_factory=list)

    def is_stopping(self) -> bool:
        return any(worker.is_stopping() for worker in self.workers)

    @property
    def num_workers(self) -> int:
        return sum(worker.num_workers for worker in self.workers)

    @property
    def frozen(self) -> bool:
        if isinstance(self.workers, list) and isinstance(self.tasks, list):
            return False
        return True

    def get_worker_by_name(self, worker_name: str) -> Worker | None:
        for worker in self.workers:
            if worker.worker_name == worker_name:
                return worker
        return None

    def workers_tasks(self) -> tuple[list[Worker], list[asyncio.Task]]:
        if self.frozen:
            raise RuntimeError("Cannot add workers")
        return self.workers, self.tasks  # type: ignore

    def gracefully_stop(self) -> None:
        for worker in self.workers:
            worker.gracefully_stop()

    async def status(self) -> dict:
        status_workers = await asyncio.gather(
            *[worker.status() for worker in self.workers],
        )
        return {
            worker.worker_name: status
            for worker, status in zip(self.workers, status_workers)
        }

    def cancel(self) -> None:
        for task in self.tasks:
            task.cancel()


class MultipleWorkers(RunningWorker):
    """A worker managing several workers"""

    def __init__(
        self,
        *workers: Worker,
        name: str = "",
        heartbeat: float | int = 0.1,
        stopping_grace_period: int = settings.STOPPING_GRACE_PERIOD,
    ) -> None:
        super().__init__(name)
        self._heartbeat = heartbeat
        self._workers = WorkerTasks()
        self._has_shutdown = False
        self._force_shutdown = False
        self._stopping_grace_period = stopping_grace_period
        self.add_workers(*workers)

    @abstractmethod
    def add_workers(self, *workers: Worker) -> None: ...

    @abstractmethod
    async def wait_for_exit(self) -> None: ...

    def is_stopping(self) -> bool:
        return self._workers.is_stopping()

    @property
    def num_workers(self) -> int:
        return self._workers.num_workers

    def __iter__(self) -> Iterator[Worker]:
        return iter(self._workers.workers)

    def gracefully_stop(self) -> None:
        self._workers.gracefully_stop()

    async def status(self) -> dict:
        return await self._workers.status()

    def create_task(self, worker: Worker) -> asyncio.Task:
        return asyncio.create_task(
            self._run_worker(worker), name=f"{self.worker_name}-{worker.worker_name}"
        )

    async def on_shutdown(self) -> None:
        """called after the workers are stopped"""

    async def shutdown(self) -> None:
        """shutdown the workers"""
        if self._has_shutdown:
            return
        self._has_shutdown = True
        logger.warning(
            "gracefully stopping %d workers: %s",
            self.num_workers,
            ", ".join(w.worker_name for w in self._workers.workers),
        )
        self.gracefully_stop()
        try:
            async with asyncio.timeout(self._stopping_grace_period):
                await self.wait_for_exit()
            await self.on_shutdown()
            return
        except asyncio.TimeoutError:
            logger.warning(
                "could not stop workers %s gracefully after %s"
                " seconds - force shutdown",
                ", ".join(
                    task.get_name() for task in self._workers.tasks if not task.done()
                ),
                self._stopping_grace_period,
            )
        except asyncio.CancelledError:
            pass
        self._force_shutdown = True
        self._workers.cancel()
        try:
            await self.wait_for_exit()
        except asyncio.CancelledError:
            pass
        await self.on_shutdown()

    def bail_out(self, reason: str, code: int = 1) -> None:
        self.gracefully_stop()

    @asynccontextmanager
    async def safe_run(self) -> AsyncGenerator:
        """Context manager to run a worker safely"""
        try:
            yield
        except asyncio.CancelledError:
            if self._force_shutdown:
                # we are shutting down, this is expected
                pass
            raise
        except Exception as e:
            reason = f"unhandled exception while running workers: {e}"
            logger.exception(reason)
            asyncio.get_event_loop().call_soon(self.bail_out, reason, 2)
        else:
            # worker finished without error
            # make sure we are shutting down
            asyncio.get_event_loop().call_soon(self.bail_out, "worker exit", 1)

    async def _run_worker(self, worker: Worker) -> None:
        async with self.safe_run():
            await worker.run()


class DynamicWorkers(MultipleWorkers):
    def add_workers(self, *workers: Worker) -> None:
        """add workers to the workers

        They can be added while the workers are running.
        """
        workers_, tasks_ = self._workers.workers_tasks()
        for worker in workers:
            workers_.append(worker)
            tasks_.append(self.create_task(worker))

    async def run(self) -> None:
        with self.start_running():
            while not self.is_stopping():
                for worker, task in zip(self._workers.workers, self._workers.tasks):
                    if worker.is_stopping() or task.done():
                        break
                await asyncio.sleep(self._heartbeat)
            await self.shutdown()

    async def wait_for_exit(self) -> None:
        await asyncio.gather(*self._workers.tasks)


class Workers(MultipleWorkers):
    """A worker managing several workers"""

    def __init__(
        self,
        *workers: Worker,
        name: str = "",
        stopping_grace_period: int = settings.STOPPING_GRACE_PERIOD,
    ) -> None:
        super().__init__(
            *workers, name=name, stopping_grace_period=stopping_grace_period
        )
        self._workers_task: asyncio.Task | None = None
        self._delayed_callbacks: list[
            tuple[Callable[[], None], float, float, float]
        ] = []

    @property
    def running(self) -> bool:
        return self._workers.frozen

    # Multiple Worker implementation
    def add_workers(self, *workers: Worker) -> None:
        """add workers to the workers"""
        workers_, _ = self._workers.workers_tasks()
        for worker in workers:
            if worker not in workers_:
                workers_.append(worker)

    async def run(self) -> None:
        """run the workers"""
        with self.start_running():
            async with self.safe_run():
                workers, _ = self._workers.workers_tasks()
                self._workers.workers = tuple(workers)
                self._workers.tasks = tuple(
                    self.create_task(worker) for worker in workers
                )
                await asyncio.gather(*self._workers.tasks)
            await self.shutdown()

    async def wait_for_exit(self) -> None:
        if self._workers_task is not None:
            await self._workers_task

    def remove_workers(self, *workers: Worker) -> None:
        "remove workers from the workers"
        workers_, _ = self._workers.workers_tasks()
        for worker in workers:
            try:
                workers_.remove(worker)
            except ValueError:
                pass

    async def startup(self) -> None:
        """start the workers"""
        if self._workers_task is None:
            self._workers_task = asyncio.create_task(self.run(), name=self.worker_name)
            for args in self._delayed_callbacks:
                self._delayed_callback(*args)
            self._delayed_callbacks = []

    def register_callback(
        self,
        callback: Callable[[], None],
        seconds: float,
        jitter: float = 0.0,
        periodic: bool | float = False,
    ) -> None:
        """Register a callback

        The callback can be periodic or not.
        """
        if periodic is True:
            periodic_float = seconds
        elif periodic is False:
            periodic_float = 0.0
        else:
            periodic_float = periodic
        if not self.running:
            self._delayed_callbacks.append((callback, seconds, jitter, periodic_float))
        else:
            self._delayed_callback(callback, seconds, jitter, periodic_float)

    def _delayed_callback(
        self,
        callback: Callable[[], None],
        delay: float,
        jitter: float,
        periodic: float,
    ) -> None:
        asyncio.get_event_loop().call_later(
            delay + random.uniform(0, jitter),
            self._run_callback,
            callback,
            delay,
            jitter,
            periodic,
        )

    def _run_callback(
        self,
        callback: Callable[[], None],
        delay: float,
        jitter: float,
        periodic: float,
    ) -> None:
        try:
            callback()
        except Exception:
            logger.exception("error while executing callback")
            if periodic > 0:
                # stop the worker because the callback failed and it is periodic
                return self.bail_out("error while executing callback")
        if periodic:
            self._delayed_callback(callback, periodic, jitter, periodic)
