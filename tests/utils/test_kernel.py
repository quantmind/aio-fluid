import asyncio
import os
import sys

import pytest

from fluid.utils import kernel
from fluid.utils.waiter import wait_for
from tests.scripts import long_line

SLEEPER = "import os, sys, time; print(os.getpid(), flush=True); time.sleep(30)"

IGNORE_SIGTERM = (
    "import os, signal, sys, time;"
    "signal.signal(signal.SIGTERM, signal.SIG_IGN);"
    "print(os.getpid(), flush=True);"
    "time.sleep(30)"
)


def is_gone(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    return False


async def test_env_variable() -> None:
    result = kernel.CollectBytes()
    await kernel.run("env", result_callback=result, env=dict(KERNEL_TEST_ME="yes"))
    assert result.data == b"KERNEL_TEST_ME=yes\n"


async def test_long_line() -> None:
    result = kernel.CollectBytes()
    code = await kernel.run(
        "python",
        "-m",
        "tests.scripts.long_line",
        result_callback=result,
        stream_output=True,
        stream_error=True,
    )
    assert code == 0
    assert result.data
    lines = [text for text in result.data.decode("utf-8").split("\n") if text]
    assert len(lines) == 1
    assert len(lines[0]) == long_line.length


async def test_long_lines() -> None:
    result = kernel.CollectBytes()
    code = await kernel.run(
        "python",
        "-m",
        "tests.scripts.long_line",
        "3",
        result_callback=result,
        stream_output=True,
        stream_error=True,
    )
    assert code == 0
    assert result.data
    lines = [text for text in result.data.decode("utf-8").split("\n") if text]
    assert len(lines) == 3
    for text in lines:
        assert len(text) == long_line.length


async def test_cancel_terminates_process() -> None:
    """A cancelled run must not leave the subprocess running

    This is what a task timeout does to a cpu bound task.
    """
    pids: list[bytes] = []

    with pytest.raises(asyncio.TimeoutError):
        async with asyncio.timeout(1):
            await kernel.run(
                sys.executable,
                "-c",
                SLEEPER,
                result_callback=pids.append,
                stream_output=True,
            )

    assert pids, "the subprocess did not report its pid"
    await wait_for(lambda: is_gone(int(pids[0].decode())), timeout=5.0)


async def test_cancel_kills_process_ignoring_sigterm() -> None:
    """A process which ignores SIGTERM is killed after the grace period"""
    pids: list[bytes] = []

    with pytest.raises(asyncio.TimeoutError):
        async with asyncio.timeout(1):
            await kernel.run(
                sys.executable,
                "-c",
                IGNORE_SIGTERM,
                result_callback=pids.append,
                stream_output=True,
                terminate_grace_period=0.2,
            )

    assert pids, "the subprocess did not report its pid"
    await wait_for(lambda: is_gone(int(pids[0].decode())), timeout=5.0)


async def test_terminate_is_noop_when_process_exited() -> None:
    process = await asyncio.create_subprocess_exec(sys.executable, "-c", "pass")
    await process.wait()
    await kernel.terminate(process)
    assert process.returncode == 0
