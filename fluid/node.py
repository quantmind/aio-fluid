import asyncio
import inspect
import logging
import os
import random
import time
import uuid
from abc import ABC, abstractmethod
from functools import cached_property, wraps
from logging import Logger
from typing import Any, Callable, Dict, List, Optional, Tuple

from aiohttp.client import ClientConnectionError, ClientConnectorError
from aiohttp.web import Application, GracefulExit

from .log import get_logger
from .utils import close_task, dot_name, underscore


class Id:
    @classmethod
    def name(cls) -> str:
        """My name"""
        return underscore(cls.__name__)

    @cached_property
    def uid(self) -> str:
        """My unique ID"""
        return uuid.uuid4().hex

    @classmethod
    def create_logger(cls, logger: Optional[logging.Logger] = None) -> logging.Logger:
        return logger or get_logger(dot_name(cls.name()))


class IdLog(Id):
    @cached_property
    def logger(self):
        return self.create_logger()


class NodeBase(ABC, Id):
    exit_lag: int = 1
    app: Optional[Application] = None

    async def start_app(self, app: Application) -> None:
        """Start application"""
        self.app = app
        await self.start()

    async def close_app(self, app: Application) -> None:
        await self.close()

    @abstractmethod
    def is_running(self) -> bool:
        """True if the Node is running"""

    @abstractmethod
    async def start(self) -> None:
        """called when the node worker has started"""
        pass

    @abstractmethod
    async def close(self) -> None:
        """called when the node worker closed"""
        pass

    async def setup(self) -> None:
        """Called by the :meth:`.start` method when the worker starts

        This can be optionally implemented by derived classes
        """
        pass

    async def teardown(self) -> None:
        """Called my :meth:`close` when the worker is stopping.

        This can be optionally implemented by derived classes
        """
        pass

    async def done(self) -> None:
        try:
            await self.teardown()
        except Exception:
            self.logger.exception("unhandled exception while tear down worker")

    async def system_exit(self) -> None:
        """Gracefully exiting the app if possible"""
        if self.is_running():
            await self.done()
        self.system_exit_sync()

    def system_exit_sync(self) -> None:
        """Exit the app"""
        self.logger.warning("bailing out!")
        asyncio.get_event_loop().call_later(self.exit_lag, self._exit)

    def _exit(self) -> None:  # pragma: no cover
        if os.getenv("PYTHON_ENV") != "test":
            raise GracefulExit


class NodeWorker(NodeBase):
    def __init__(self, *, logger: Optional[Logger] = None) -> None:
        self.logger: Logger = self.create_logger(logger)
        self._worker = None

    @property
    def debug(self) -> bool:
        return self.logger.isEnabledFor(logging.DEBUG)

    # FOR DERIVED CLASSES

    async def work(self) -> None:
        """Main work coroutine, this is where you define the asynchronous loop.

        Must be implemented by derived classes
        """
        raise NotImplementedError

    # API

    def is_running(self) -> bool:
        """True if the Node is running"""
        return bool(self._worker)

    async def start(self) -> None:
        """Start the node"""
        assert not self.is_running(), "Node already running - cannot start"
        await self.setup()
        self._worker = asyncio.ensure_future(self._work())

    async def close(self, close_worker: bool = True) -> None:
        if self._worker:
            self.logger.info("closing")
            worker = self._worker
            self._worker = None
            if close_worker:
                await close_task(worker, self.done)
            else:
                await self.done()
            self.logger.warning("closed")

    # INTERNAL

    async def _work(self) -> None:
        self.logger.warning("started")
        try:
            await self.work()
        except asyncio.CancelledError:
            pass
        except Exception:
            self.logger.exception("unhandled exception in worker")
            await self.system_exit()
        else:
            await self.close(close_worker=False)


class WorkerApplication(Dict[str, Any]):
    def __init__(self):
        super().__init__()
        self.on_startup = []
        self.on_shutdown = []

    async def startup(self):
        for on_startup in self.on_startup:
            await on_startup(self)

    async def shutdown(self):
        for on_shutdown in self.on_shutdown:
            await on_shutdown(self)


class NodeWorkers(NodeBase):
    def __init__(self, *workers: NodeWorker, logger: Optional[Logger] = None) -> None:
        self.logger: Logger = self.create_logger(logger)
        self._closing: bool = False
        self._workers: List[NodeBase] = list(workers)

    @property
    def debug(self) -> bool:
        return self.logger.isEnabledFor(logging.DEBUG)

    def is_running(self) -> bool:
        return isinstance(self._workers, tuple)

    def is_closing(self) -> bool:
        return self._closing

    def add_workers(self, *workers: NodeBase) -> None:
        if self.is_running():
            raise RuntimeError("Cannot add workers when started")
        self._workers.extend(workers)

    async def start(self) -> None:
        await self.setup()
        self.logger.warning("started")
        workers = self._freeze_workers()
        await asyncio.gather(*[w.start_app(self.app) for w in workers])

    async def close(self) -> None:
        if self.is_running():
            self._closing = True
            await asyncio.gather(*[w.close_app(self.app) for w in self._workers])
            await self.teardown()

    def _freeze_workers(self) -> Tuple[NodeBase, ...]:
        if isinstance(self._workers, tuple):
            raise RuntimeError("worker already started")
        self._workers = tuple(self._workers)
        return self._workers


class Node(NodeWorker):
    """A nodeworker with an heartbeat work loop and ability to publish
    messages into a pubsub
    """

    heartbeat: float = 1
    ticks: int = 0

    async def tick(self) -> None:
        """called at every iteration in the worker"""
        pass

    async def work(self) -> None:
        while True:
            start = time.monotonic()
            self.ticks += 1
            await self.tick()
            dt = time.monotonic() - start
            await asyncio.sleep(max(self.heartbeat - dt, 0))


class Consumer(NodeWorker):
    def __init__(
        self,
        process_message,
        queue: Optional[asyncio.Queue] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.process_message = process_message
        self._message_queue = queue or asyncio.Queue()

    def qsize(self) -> int:
        return self._message_queue.qsize()

    async def work(self):
        while self.is_running():
            message = await self._message_queue.get()
            await self.process_message(message)
            await asyncio.sleep(0)

    def submit(self, message) -> None:
        self._message_queue.put_nowait(message)


class Worker(NodeWorker):
    def __init__(
        self,
        work: Callable[[], None],
        logger: Optional[Logger] = None,
    ) -> None:
        super().__init__(logger=logger)
        self.work = work


class TickWorker(Node):
    def __init__(
        self,
        tick: Callable[[], None],
        heartbeat: float = 1,
        logger: Optional[Logger] = None,
    ) -> None:
        super().__init__(logger=logger)
        self.heartbeat = heartbeat
        self.tick = tick


class every:
    def __init__(self, seconds: float, noise: float = 0) -> None:
        self.seconds = seconds
        self.noise = min(noise, seconds)
        self.last = 0
        self.gap = self._gap()
        self.ticks = 0

    def __call__(self, method):
        method.every = self

        @wraps(method)
        async def _(node, *args) -> None:
            now = time.time()
            if now - self.last > self.gap:
                self.last = now
                self.gap = self._gap()
                self.ticks += 1
                try:
                    await method(node, *args)
                except (ClientConnectionError, ClientConnectorError) as exc:
                    node.logger.error(str(exc))

        return _

    def _gap(self) -> float:
        return self.seconds + self.noise * (random.random() - 0.5)


def on_error_exit(
    method: Callable[[NodeBase, Any], None]
) -> Callable[[NodeBase, Any], None]:
    @wraps(method)
    def sync_wrap(node: NodeBase, *args) -> None:
        try:
            method(node, *args)
        except Exception:
            node.logger.exception("unhandled exception, bailing out!")
            node.system_exit_sync()

    @wraps(method)
    async def async_wrap(node: NodeBase, *args) -> None:
        try:
            await method(node, *args)
        except Exception:
            node.logger.exception("unhandled exception, bailing out!")
            await node.system_exit()

    return async_wrap if inspect.iscoroutinefunction(method) else sync_wrap
