import time

import pytest

from fluid.utils import errors
from fluid.utils.stacksampler import FlamegraphError, Sampler

pytestmark = pytest.mark.asyncio(loop_scope="module")


def _burn_cpu(seconds: float) -> int:
    """Busy loop, the sampler timer only advances with process CPU time."""
    total = 0
    end = time.process_time() + seconds
    while time.process_time() < end:
        total += 1
    return total


async def test_sampler_lifecycle() -> None:
    sampler = Sampler(interval=0.001)
    assert sampler.started is None
    assert sampler.stats() == ""
    sampler.start()
    try:
        assert sampler.started is not None
        _burn_cpu(0.2)
        stats = sampler.stats()
    finally:
        sampler.stop()
    lines = stats.splitlines()
    assert lines[0].startswith("elapsed ")
    assert lines[1] == "granularity 0.001"
    assert any("_burn_cpu(" in line for line in lines[2:])
    assert sampler.started is None
    assert sampler.stats() == ""


async def test_sampler_reset() -> None:
    sampler = Sampler()
    sampler.reset()
    assert sampler.started is not None
    assert sampler.stats().splitlines()[1] == "granularity 0.005"
    sampler.stop()


async def test_flamegraph_file() -> None:
    sampler = Sampler()
    stats = "main(__main__);work(app) 10\nmain(__main__);idle(app) 3\n"
    async with sampler.flamegraph_file("test graph", stats) as svg:
        assert svg.startswith(b"<?xml")
        assert b"test graph" in svg


async def test_flamegraph_file_error() -> None:
    sampler = Sampler()
    with pytest.raises(FlamegraphError) as exc_info:
        async with sampler.flamegraph_file("empty", ""):
            pass  # pragma: no cover
    assert b"No stack counts found" in exc_info.value.args[0]


async def test_sampler_del_stops_timer() -> None:
    sampler = Sampler()
    sampler.__del__()
    assert sampler.started is None


async def test_flamegraph_file_error_is_fluid_error() -> None:
    sampler = Sampler()
    with pytest.raises(errors.FlamegraphError):
        async with sampler.flamegraph_file("empty", ""):
            pass  # pragma: no cover
