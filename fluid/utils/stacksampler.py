import atexit
import logging
import signal
from collections import defaultdict
from contextlib import asynccontextmanager
from tempfile import NamedTemporaryFile
from time import monotonic
from typing import AsyncIterator

from fluid import settings
from fluid.tools.worker import TickWorker

from . import executor, kernel
from .dispatcher import SimpleDispatcher

logger = logging.getLogger(__name__)


class FlamegraphError(RuntimeError):
    pass


class Sampler:
    """
    A simple stack sampler for low-overhead CPU profiling: samples the call
    stack every `interval` seconds and keeps track of counts by frame. Because
    this uses signals, it only works on the main thread.
    """

    def __init__(self, interval: float = 0.005) -> None:
        self.interval = interval
        self._started = None
        self._stack_counts = defaultdict(int)

    @property
    def started(self) -> float:
        return self._started

    def start(self) -> None:
        self.reset()
        signal.signal(signal.SIGVTALRM, self._sample)
        signal.setitimer(signal.ITIMER_VIRTUAL, self.interval)
        atexit.register(self.stop)

    def stop(self) -> None:
        self.reset()
        self._started = None
        signal.setitimer(signal.ITIMER_VIRTUAL, 0)

    def reset(self) -> None:
        self._started = monotonic()
        self._stack_counts = defaultdict(int)

    def stats(self) -> str:
        if self._started is None:
            return ""
        elapsed = monotonic() - self._started
        lines = ["elapsed {}".format(elapsed), "granularity {}".format(self.interval)]
        ordered_stacks = sorted(
            self._stack_counts.items(), key=lambda kv: kv[1], reverse=True
        )
        lines.extend(["{} {}".format(frame, count) for frame, count in ordered_stacks])
        return "\n".join(lines) + "\n"

    @asynccontextmanager
    async def flamegraph_file(self, title: str, stats: str) -> AsyncIterator[bytes]:
        with NamedTemporaryFile(prefix="flamegraph-") as f:
            f.write(stats.encode("utf-8"))
            f.flush()
            result = kernel.CollectBytes()
            error = kernel.CollectBytes()
            code = await kernel.run(
                settings.FLAMEGRAPH_EXECUTABLE,
                f.name,
                "--title",
                title,
                result_callback=result,
                error_callback=error,
            )
            if code:
                raise FlamegraphError(error.data)
            yield result.data

    # INTERNALS

    def _sample(self, signum, frame) -> None:
        if self._started:
            stack = []
            while frame is not None:
                stack.append(self._format_frame(frame))
                frame = frame.f_back
            stack = ";".join(reversed(stack))
            self._stack_counts[stack] += 1
            signal.setitimer(signal.ITIMER_VIRTUAL, self.interval)

    def _format_frame(self, frame) -> str:
        return "{}({})".format(frame.f_code.co_name, frame.f_globals.get("__name__"))

    def __del__(self):
        self.stop()


class WorkerSampler(TickWorker):
    def __init__(
        self,
        name: str = "",
        heartbeat=settings.STACK_SAMPLER_PERIOD,
        sampler: Sampler | None = None,
    ):
        super().__init__(self._tick, name=name, heartbeat=heartbeat)
        self.sampler: Sampler = sampler or Sampler()
        self.dispatcher = SimpleDispatcher[bytes]()

    async def _tick(self) -> None:
        if not self.sampler.started:
            self.sampler.start()
        else:
            stats = self.sampler.stats()
            self.sampler.reset()
            try:
                async with self.sampler.flamegraph_file(self.name, stats) as svg:
                    await executor.run(self.upload, svg)
            except FlamegraphError as e:
                logger.warning("could not create flamegraph svg: %s", e)
