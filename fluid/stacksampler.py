import atexit
import os
import signal
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime
from tempfile import NamedTemporaryFile
from time import monotonic
from typing import Optional

import boto3
from aiohttp import web

from . import executor, kernel
from .node import Node

sampler_routes = web.RouteTableDef()


FLAMEGRAPH = os.getenv("FLAMEGRAPH_EXECUTABLE") or "/bin/flamegraph.pl"
FLAMEGRAPH_DATA_BUCKET, FLAMEGRAPH_DATA_PATH = os.getenv(
    "FLAMEGRAPH_DATA_BUCKET_PATH", "replace/me"
).split("/")


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
    async def flamegraph_file(self, title: str, stats: str) -> None:
        with NamedTemporaryFile(prefix="flamegraph-") as f:
            f.write(stats.encode("utf-8"))
            f.flush()
            result = kernel.CollectBytes()
            error = kernel.CollectBytes()
            code = await kernel.run(
                FLAMEGRAPH,
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


class NodeSampler(Node):
    heartbeat = STACK_SAMPLER_PERIOD = int(os.getenv("STACK_SAMPLER_PERIOD", "60"))

    def __init__(self, title: str, sampler: Optional[Sampler] = None, **kwargs):
        super().__init__(**kwargs)
        self.title: str = title
        self.sampler: Sampler = sampler or Sampler()
        self.s3 = boto3.client("s3")

    async def tick(self) -> None:
        if not self.sampler.started:
            self.sampler.start()
        else:
            stats = self.sampler.stats()
            self.sampler.reset()
            try:
                async with self.sampler.flamegraph_file(self.title, stats) as svg:
                    await executor.run(self.upload, svg)
            except FlamegraphError as e:
                self.logger.warning("could not create flamegraph svg: %s", e)

    def upload(self, svg: bytes) -> None:
        dte = datetime.utcnow()
        date_str = dte.date().isoformat()
        time_str = dte.time().strftime("%H-%M-%S")
        s3_path = f"{FLAMEGRAPH_DATA_PATH}/{date_str}/{self.title}/{time_str}.svg"
        self.logger.info("upload flamegraph svg file to %s", s3_path)
        try:
            self.s3.put_object(
                Bucket=FLAMEGRAPH_DATA_BUCKET,
                Key=s3_path,
                Body=svg,
                ContentType="image/svg+xml",
            )
        except Exception:
            self.logger.exception(
                "Could not upload flamegraph data file to %s",
                s3_path,
            )


@sampler_routes.get("/stacksampler/stats")
async def stacksampler_stats(request):
    sampler: Sampler = request.app["service"].sampler
    return web.Response(text=sampler.stats())


@sampler_routes.get("/stacksampler/start")
async def stacksampler_start(request):
    sampler: Sampler = request.app["service"].sampler
    if not sampler.started:
        sampler.start()
        return web.Response(text="Sampler started")
    else:
        return web.Response(text="Sampler already started")


@sampler_routes.get("/stacksampler/stop")
async def stacksampler_stop(request):
    sampler: Sampler = request.app["service"].sampler
    if sampler.started:
        sampler.stop()
        return web.Response(text="Sampler stopped")
    else:
        return web.Response(text="Sampler already stopped")


@sampler_routes.get("/stacksampler/reset")
async def stacksampler_reset(request):
    sampler: Sampler = request.app["service"].sampler
    sampler.reset()
    return web.Response(text="Sampler reset")


@sampler_routes.get("/stacksampler/flamegraph")
async def stacksampler_flamegraph(request):
    sampler: Sampler = request.app["service"].sampler
    if sampler.started:
        title = request.app["cli"].name
        args = []
        with NamedTemporaryFile(prefix="flamegraph-") as f:
            f.write(sampler.stats().encode("utf-8"))
            f.flush()
            result = kernel.CollectBytes()
            error = kernel.CollectBytes()
            code = await kernel.run(
                FLAMEGRAPH,
                f.name,
                "--title",
                title,
                *args,
                result_callback=result,
                error_callback=error,
            )
            if code:
                return web.Response(body=error.data, content_type="text/plain")
            else:
                return web.Response(body=result.data, content_type="image/svg+xml")
    return web.Response(text="Sampler not started")
