from __future__ import annotations

import asyncio
import enum
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import (
    Annotated,
    Awaitable,
    Callable,
    Iterator,
    Protocol,
    Self,
    Sequence,
    TypeVar,
)

from inflection import underscore
from typing_extensions import Doc

from fluid import settings

from .dispatcher import AsyncDispatcher, MessageType
from .errors import WorkerStartError

logger = logging.getLogger(__name__)


class WorkerState(enum.StrEnum):
    """The state of a worker"""

    INIT = enum.auto()
    RUNNING = enum.auto()
    STOPPING = enum.auto()
    STOPPED = enum.auto()
    FORCE_STOPPED = enum.auto()


class HasWorkers(ABC):
    @abstractmethod
    def get_workers(self) -> Sequence[Worker]:
        """Get the workers that can be added to a the Workers Manager"""


@dataclass
class WorkerTaskRunner:
    worker: Worker
    task: asyncio.Task | None = None
    started_shutdown: bool = False
    force_shutdown: bool = False

    @classmethod
    async def start(cls, worker: Worker) -> Self:
        worker_task = cls(worker)
        waiter = asyncio.Future[Self]()
        worker_task.task = asyncio.create_task(
            worker_task.run(waiter), name=worker.worker_name
        )
        return await waiter

    async def shutdown(self) -> None:
        if self.task is None:  # pragma: no cover
            return
        elif not self.started_shutdown:
            self.started_shutdown = True
            await self.gracefully_shutdown()
        await self.wait_for_shutdown()

    async def wait_for_shutdown(self) -> None:
        if self.task is None:
            return
        await self.task

    async def gracefully_shutdown(self) -> None:
        self.started_shutdown = True
        self.worker.gracefully_stop()
        try:
            async with asyncio.timeout(self.worker._stopping_grace_period):
                while self.task is not None:
                    await asyncio.sleep(0)
            return None
        except asyncio.TimeoutError:
            pass
        if self.task is None:  # pragma: no cover
            return None
        self.force_shutdown = True
        self.task.cancel()
        logger.warning("%s forced shutdown", self.worker.worker_name)

    async def run(self, waiter: asyncio.Future) -> None:
        if self.worker.is_running():
            raise WorkerStartError("worker %s already running", self.worker.worker_name)
        self.worker._worker_state = WorkerState.RUNNING
        logger.info("%s started running", self.worker.worker_name)
        waiter.set_result(self)
        exit_reason = "normal shutdown"
        exit_code = 0
        stopped_state = WorkerState.STOPPED
        try:
            await self.worker.on_startup()
            await self.worker.run()
            await self.worker.on_shutdown()
        except Exception as e:
            exit_reason = str(e)
            exit_code = 2
            raise
        except asyncio.CancelledError as e:
            if self.force_shutdown:
                exit_reason = "forced shutdown"
                exit_code = 1
                stopped_state = WorkerState.FORCE_STOPPED
                # we are shutting down, this is expected
                pass
            else:
                exit_reason = str(e)
                exit_code = 2
                raise
        finally:
            logger.warning(
                "%s stopped running: %s",
                self.worker.worker_name,
                exit_reason,
            )
            asyncio.get_event_loop().call_soon(
                self.worker.after_shutdown,
                exit_reason,
                exit_code,
            )
            self.worker._worker_state = stopped_state
            self.worker._worker_task_runner = None
            self.task = None


class Worker(ABC):
    """An Abstract Worker that can be started and stopped

    All other workers derive from this class.
    """

    def __init__(
        self,
        name: Annotated[
            str,
            Doc("Worker's name, if not provided it is evaluated from the class name"),
        ] = "",
        stopping_grace_period: Annotated[
            int,
            Doc(
                "Grace period in seconds to wait for workers to stop running "
                "when this worker is shutdown. "
                "It defaults to the `FLUID_STOPPING_GRACE_PERIOD` "
                "environment variable or 10 seconds."
            ),
        ] = settings.STOPPING_GRACE_PERIOD,
    ) -> None:
        self._worker_name: str = name or underscore(type(self).__name__)
        self._worker_state: WorkerState = WorkerState.INIT
        self._stopping_grace_period = stopping_grace_period
        self._worker_task_runner: WorkerTaskRunner | None = None

    @property
    def worker_state(self) -> WorkerState:
        """The running state of the worker"""
        return self._worker_state

    @property
    def worker_name(self) -> str:
        """The name of the worker"""
        return self._worker_name

    @property
    def num_workers(self) -> int:
        """The number of workers in this worker"""
        return 1

    def has_started(self) -> bool:
        return self._worker_state != WorkerState.INIT

    def is_running(self) -> bool:
        return self._worker_state == WorkerState.RUNNING

    def is_stopping(self) -> bool:
        return self._worker_state == WorkerState.STOPPING

    def is_stopped(self) -> bool:
        return self._worker_state in (WorkerState.STOPPED, WorkerState.FORCE_STOPPED)

    def gracefully_stop(self) -> None:
        if self.is_running():
            self._worker_state = WorkerState.STOPPING

    def after_shutdown(self, reason: str, code: int) -> None:  # noqa: B027
        """Called after shutdown of worker

        By default it does nothing, but can be overriden to do something such as
        exit the process.
        """

    async def status(self) -> dict:
        return {"stopping": self.is_stopping(), "running": self.is_running()}

    async def on_startup(self) -> None:  # noqa: B027
        """Called when the worker starts running

        Use this function to initialize other async resources connected with the worker
        """

    async def on_shutdown(self) -> None:  # noqa: B027
        """called after the worker stopped running

        Use this function to cleanup resources connected with the worker
        """

    async def startup(self) -> None:
        """start the worker

        This method creates a task to run the worker.
        """
        if self.has_started():
            raise WorkerStartError(
                "worker %s already started: %s", self.worker_name, self._worker_state
            )
        else:
            self._worker_task_runner = await WorkerTaskRunner.start(self)

    async def shutdown(self) -> None:
        """Shutdown a running worker and wait for it to stop

        This method will try to gracefully stop the worker and wait for it to stop.
        If the worker does not stop in the grace period, it will force shutdown
        by cancelling the task.
        """
        if self._worker_task_runner is not None:
            await self._worker_task_runner.shutdown()

    async def wait_for_shutdown(self) -> None:
        """Wait for the worker to stop

        This method will wait for the worker to stop running, but doesn't
        try to gracefully stop it nor force shutdown.
        """
        if self._worker_task_runner is not None:
            await self._worker_task_runner.wait_for_shutdown()

    def workers(self) -> Iterator[Worker]:
        """An iterator of workers in this worker"""
        yield self

    @abstractmethod
    async def run(self) -> None:
        """run the worker

        This is the only abstract method and that needs implementing.
        It is the coroutine that mantains the worker running.
        """


class WorkerFunction(Worker):
    """A Worker that runs and wait a coroutine function in a loop"""

    def __init__(
        self,
        run_function: Annotated[
            Callable[[], Awaitable[None]],
            Doc(
                "The coroutine function tuo run and await at each iteration "
                "of the worker loop"
            ),
        ],
        heartbeat: Annotated[
            float | int, Doc("The time to wait between each coroutine function run")
        ] = 0,
        name: Annotated[
            str,
            Doc("Worker's name, if not provided it is evaluated from the class name"),
        ] = "",
    ) -> None:
        super().__init__(name=name)
        self._run_function = run_function
        self._heartbeat = heartbeat

    async def run(self) -> None:
        while self.is_running():
            await self._run_function()
            await asyncio.sleep(self._heartbeat)


T = TypeVar("T", contravariant=True)


class MessageProducer(Protocol[T]):
    def send(self, message: T | None) -> None: ...


class QueueConsumer(Worker, MessageProducer[MessageType]):
    """An Abstract Worker that can receive messages

    This worker can receive messages but not consume them.
    """

    def __init__(
        self,
        name: Annotated[
            str,
            Doc("Worker's name, if not provided it is evaluated from the class name"),
        ] = "",
    ) -> None:
        super().__init__(name=name)
        self._queue: asyncio.Queue[MessageType | None] = asyncio.Queue()

    async def get_message(self, timeout: float = 0.5) -> MessageType | None:
        """Get the next message from the queue"""
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
        """Get the size of the queue"""
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
        on_message: Annotated[
            Callable[[MessageType], Awaitable[None]],
            Doc("The async callback to call when a message is received"),
        ],
        name: Annotated[
            str,
            Doc("Worker's name, if not provided it is evaluated from the class name"),
        ] = "",
    ) -> None:
        super().__init__(name=name)
        self.on_message = on_message

    async def run(self) -> None:
        while not self.is_stopping():
            message = await self.get_message()
            if message is not None:
                await self.on_message(message)


class AsyncConsumer(QueueConsumer[MessageType]):
    """A Worker that can dispatch to async callbacks"""

    def __init__(
        self,
        dispatcher: Annotated[
            AsyncDispatcher[MessageType],
            Doc("Async message dispatcher to dispatch messages"),
        ],
        name: Annotated[
            str,
            Doc("Worker's name, if not provided it is evaluated from the class name"),
        ] = "",
    ) -> None:
        super().__init__(name)
        self.dispatcher: AsyncDispatcher[MessageType] = dispatcher

    async def run(self) -> None:
        while not self.is_stopping():
            message = await self.get_message()
            if message is not None:
                await self.dispatcher.dispatch(message)


class Workers(Worker):
    """An worker managing several workers"""

    def __init__(
        self,
        *workers: Annotated[
            Worker,
            Doc(
                "Workers to manage, they can also be added later "
                "via `add_workers` method"
            ),
        ],
        name: Annotated[
            str,
            Doc("Worker's name, if not provided it is evaluated from the class name"),
        ] = "",
        heartbeat: Annotated[
            float | int,
            Doc("The time to wait between each workers status check"),
        ] = 0.1,
        stopping_grace_period: Annotated[
            int,
            Doc(
                "Grace period in seconds to wait for workers to stop running "
                "when this worker is shutdown. "
                "It defaults to the `FLUID_STOPPING_GRACE_PERIOD` "
                "environment variable or 10 seconds."
            ),
        ] = settings.STOPPING_GRACE_PERIOD,
    ) -> None:
        super().__init__(name, stopping_grace_period=stopping_grace_period)
        self._heartbeat = heartbeat
        self._workers: list[Worker] = []
        self.add_workers(*workers)

    def add_workers(self, *workers: Worker) -> None:
        """add workers to the workers

        They can be added while the worker is running.
        """
        for worker in workers:
            if worker not in self._workers:
                self._workers.append(worker)

    @property
    def num_workers(self) -> int:
        return len(self._workers)

    def workers(self) -> Iterator[Worker]:
        return iter(self._workers)

    def gracefully_stop(self) -> None:
        super().gracefully_stop()
        for worker in self._workers:
            worker.gracefully_stop()

    async def status(self) -> dict:
        status_workers = await asyncio.gather(
            *[worker.status() for worker in self._workers],
        )
        return {
            worker.worker_name: status
            for worker, status in zip(self._workers, status_workers, strict=False)
        }

    async def run(self) -> None:
        while self.is_running():
            for worker in self._workers:
                if not worker.has_started():
                    await worker.startup()
                if not worker.is_running():
                    self.gracefully_stop()
                    break
            await asyncio.sleep(self._heartbeat)
        await self._wait_for_workers()

    async def _wait_for_workers(self) -> None:
        await asyncio.gather(
            *[worker.shutdown() for worker in self._workers], return_exceptions=True
        )
