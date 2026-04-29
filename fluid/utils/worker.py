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
    """The lifecycle state of a [Worker][fluid.utils.worker.Worker]."""

    INIT = enum.auto()
    """Worker has been created but not yet started."""
    RUNNING = enum.auto()
    """Worker is actively executing its [run][fluid.utils.worker.Worker.run] loop."""
    STOPPING = enum.auto()
    """Graceful stop requested; [run][fluid.utils.worker.Worker.run] should
    exit at its next safe point."""
    STOPPED = enum.auto()
    """Worker exited cleanly after a graceful stop."""
    FORCE_STOPPED = enum.auto()
    """Worker was cancelled because it did not exit within the grace period."""


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
    """Abstract base class for all workers.

    A worker encapsulates a long-running async task with a managed lifecycle.
    Subclasses implement [run][fluid.utils.worker.Worker.run], which is called
    once the worker is started and should loop until
    [is_running][fluid.utils.worker.Worker.is_running] returns `False`.

    Use [startup][fluid.utils.worker.Worker.startup] to start the worker as an
    asyncio task, and [shutdown][fluid.utils.worker.Worker.shutdown] (or
    [gracefully_stop][fluid.utils.worker.Worker.gracefully_stop] +
    [wait_for_shutdown][fluid.utils.worker.Worker.wait_for_shutdown]) to stop it.

    Override [on_startup][fluid.utils.worker.Worker.on_startup] and
    [on_shutdown][fluid.utils.worker.Worker.on_shutdown] to initialise and clean
    up async resources that the worker owns.
    """

    def __init__(
        self,
        *,
        name: Annotated[
            str,
            Doc("Worker's name, if not provided it is evaluated from the class name"),
        ] = "",
        stopping_grace_period: Annotated[
            float,
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
        """Try to gracefully stop the worker"""
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

    async def __aenter__(self) -> Self:
        """Start the worker, allowing it to be used as an async context manager."""
        await self.startup()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        """Shut down the worker."""
        await self.shutdown()

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
    """A [Worker][fluid.utils.worker.Worker] that calls a coroutine function in a loop.

    On each iteration the supplied `run_function` is awaited, then the worker
    sleeps for `heartbeat` seconds before repeating. The loop exits when
    [is_running][fluid.utils.worker.Worker.is_running] returns `False`.
    """

    def __init__(
        self,
        run_function: Annotated[
            Callable[[], Awaitable[None]],
            Doc(
                "The coroutine function tuo run and await at each iteration "
                "of the worker loop"
            ),
        ],
        *,
        heartbeat: Annotated[
            float | int, Doc("The time to wait between each coroutine function run")
        ] = 0,
        name: Annotated[
            str,
            Doc("Worker's name, if not provided it is evaluated from the class name"),
        ] = "",
        stopping_grace_period: Annotated[
            float,
            Doc("Grace period in seconds before force-cancelling this worker"),
        ] = settings.STOPPING_GRACE_PERIOD,
    ) -> None:
        super().__init__(name=name, stopping_grace_period=stopping_grace_period)
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
    """Abstract [Worker][fluid.utils.worker.Worker] backed by an asyncio queue.

    Provides [send][fluid.utils.worker.QueueConsumer.send] for thread-safe
    message delivery and [get_message][fluid.utils.worker.QueueConsumer.get_message]
    for retrieving the next message with a timeout. Subclasses implement
    [run][fluid.utils.worker.Worker.run] to consume messages from the queue.
    """

    def __init__(
        self,
        *,
        name: Annotated[
            str,
            Doc("Worker's name, if not provided it is evaluated from the class name"),
        ] = "",
        stopping_grace_period: Annotated[
            float,
            Doc(
                "Grace period in seconds to wait for workers to stop running "
                "when this worker is shutdown. "
                "It defaults to the `FLUID_STOPPING_GRACE_PERIOD` "
                "environment variable or 10 seconds."
            ),
        ] = settings.STOPPING_GRACE_PERIOD,
    ) -> None:
        super().__init__(name=name, stopping_grace_period=stopping_grace_period)
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
    """A [QueueConsumer][fluid.utils.worker.QueueConsumer] that dispatches each
    message to a single async callback."""

    def __init__(
        self,
        on_message: Annotated[
            Callable[[MessageType], Awaitable[None]],
            Doc("The async callback to call when a message is received"),
        ],
        *,
        name: Annotated[
            str,
            Doc("Worker's name, if not provided it is evaluated from the class name"),
        ] = "",
        stopping_grace_period: Annotated[
            float,
            Doc(
                "Grace period in seconds to wait for workers to stop running "
                "when this worker is shutdown. "
                "It defaults to the `FLUID_STOPPING_GRACE_PERIOD` "
                "environment variable or 10 seconds."
            ),
        ] = settings.STOPPING_GRACE_PERIOD,
    ) -> None:
        super().__init__(name=name, stopping_grace_period=stopping_grace_period)
        self.on_message = on_message

    async def run(self) -> None:
        while not self.is_stopping():
            message = await self.get_message()
            if message is not None:
                await self.on_message(message)


class AsyncConsumer(QueueConsumer[MessageType]):
    """A [QueueConsumer][fluid.utils.worker.QueueConsumer] that fans out each
    message to all registered async handlers via an
    [AsyncDispatcher][fluid.utils.dispatcher.AsyncDispatcher].

    The run loop processes messages until
    [is_stopping][fluid.utils.worker.Worker.is_stopping] returns `True`.
    Any messages remaining in the queue when the worker stops are discarded;
    callers that need guaranteed delivery should drain the queue before
    requesting a stop.
    """

    def __init__(
        self,
        dispatcher: Annotated[
            AsyncDispatcher[MessageType],
            Doc("Async message dispatcher to dispatch messages"),
        ],
        *,
        name: Annotated[
            str,
            Doc("Worker's name, if not provided it is evaluated from the class name"),
        ] = "",
        stopping_grace_period: Annotated[
            float,
            Doc(
                "Grace period in seconds to wait for workers to stop running "
                "when this worker is shutdown. "
                "It defaults to the `FLUID_STOPPING_GRACE_PERIOD` "
                "environment variable or 10 seconds."
            ),
        ] = settings.STOPPING_GRACE_PERIOD,
    ) -> None:
        super().__init__(name=name, stopping_grace_period=stopping_grace_period)
        self.dispatcher: AsyncDispatcher[MessageType] = dispatcher

    async def run(self) -> None:
        while not self.is_stopping():
            message = await self.get_message()
            if message is not None:
                await self.dispatcher.dispatch(message)


class Workers(Worker):
    """A [Worker][fluid.utils.worker.Worker] that owns and manages a collection
    of child workers.

    Child workers are registered with
    [add_workers][fluid.utils.worker.Workers.add_workers].
    When the `Workers` instance starts, its [run][fluid.utils.worker.Workers.run] loop
    starts each child worker and monitors their health — if any child stops
    unexpectedly the whole group is gracefully stopped.

    On shutdown all child workers are stopped concurrently via
    [_wait_for_workers][fluid.utils.worker.Workers._wait_for_workers].
    Workers that do not exit within `stopping_grace_period` seconds are
    force-cancelled.

    !!! note "Shutdown ordering"
        By default all child workers are shut down concurrently. If a subclass
        needs a specific shutdown order (e.g. an event dispatcher that must
        outlive the workers that produce events), override
        [_wait_for_workers][fluid.utils.worker.Workers._wait_for_workers].
    """

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
            float,
            Doc(
                "Grace period in seconds to wait for workers to stop running "
                "when this worker is shutdown. "
                "It defaults to the `FLUID_STOPPING_GRACE_PERIOD` "
                "environment variable or 10 seconds."
            ),
        ] = settings.STOPPING_GRACE_PERIOD,
    ) -> None:
        super().__init__(name=name, stopping_grace_period=stopping_grace_period)
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
        """Try to gracefully stop the workers and this worker"""
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
