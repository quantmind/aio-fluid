import atexit
import logging
import signal
from collections import defaultdict
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from tempfile import NamedTemporaryFile
from time import monotonic
from typing import AsyncIterator

from fluid import settings

from . import kernel

logger = logging.getLogger(__name__)


class FlamegraphError(RuntimeError):
    pass


@dataclass
class Sampler:
    """
    A simple stack sampler for low-overhead CPU profiling: samples the call
    stack every `interval` seconds and keeps track of counts by frame. Because
    this uses signals, it only works on the main thread.
    """

    interval: float = 0.005
    _started: float | None = None
    _stack_counts: defaultdict[str, int] = field(
        default_factory=lambda: defaultdict(int),
        repr=False,
    )

    @property
    def started(self) -> float | None:
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
            stack_str = ";".join(reversed(stack))
            self._stack_counts[stack_str] += 1
            signal.setitimer(signal.ITIMER_VIRTUAL, self.interval)

    def _format_frame(self, frame) -> str:
        return "{}({})".format(frame.f_code.co_name, frame.f_globals.get("__name__"))

    def __del__(self):
        self.stop()
