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


async def test_run_python_encoded_log() -> None:
    """stdout is decoded by EncodedLog, stderr without a callback is discarded"""
    logged: list[str] = []
    result = kernel.EncodedLog(log=logged.append)
    code = await kernel.run_python(
        "-c",
        "import sys; print('ciao'); sys.stderr.write('ignored')",
        result_callback=result,
    )
    assert code == 0
    assert result.data == ["ciao\n"]
    assert logged == ["ciao\n"]


async def test_failing_callback_keeps_draining() -> None:
    """A callback which raises must not stop the pipe being read, otherwise
    the process blocks once the pipe buffer is full"""
    calls = []

    def fail(data: bytes) -> None:
        calls.append(data)
        raise ValueError("bad callback")

    code = await asyncio.wait_for(
        kernel.run(
            sys.executable,
            "-c",
            "import sys; [print('x' * 1000) for _ in range(1000)]",
            result_callback=fail,
            stream_output=True,
        ),
        10,
    )
    assert code == 0
    assert len(calls) == 1000


async def test_stdin_is_closed_without_input() -> None:
    """A process reading stdin sees an end of file rather than blocking"""
    result = kernel.CollectBytes()
    code = await asyncio.wait_for(
        kernel.run_python(
            "-c",
            "import sys; print(repr(sys.stdin.read()))",
            result_callback=result,
        ),
        10,
    )
    assert code == 0
    assert result.data == b"''\n"


async def test_input_is_written_to_stdin() -> None:
    result = kernel.CollectBytes()
    code = await asyncio.wait_for(
        kernel.run_python(
            "-c",
            "import sys; sys.stdout.write(sys.stdin.read().upper())",
            result_callback=result,
            input=b"ciao\n",
        ),
        10,
    )
    assert code == 0
    assert result.data == b"CIAO\n"


async def test_large_input_does_not_deadlock() -> None:
    """The process echoes its input, filling stdout while stdin is written"""
    data = b"x" * (2**20)
    result = kernel.CollectBytes()
    code = await asyncio.wait_for(
        kernel.run_python(
            "-c",
            "import shutil, sys;"
            "shutil.copyfileobj(sys.stdin.buffer, sys.stdout.buffer)",
            result_callback=result,
            input=data,
        ),
        10,
    )
    assert code == 0
    assert result.data == data


async def test_input_ignored_by_process() -> None:
    """A process exiting without reading its input is not an error"""
    code = await asyncio.wait_for(
        kernel.run_python("-c", "pass", input=b"x" * (2**20)), 10
    )
    assert code == 0


async def test_utf8_decoder_split_character() -> None:
    data = "é€".encode("utf-8")
    decoder = kernel.Utf8Decoder()
    text = "".join(decoder.decode(data[i : i + 1]) for i in range(len(data)))
    assert text == "é€"


async def test_encoded_log_replaces_invalid_bytes() -> None:
    log = kernel.EncodedLog(lambda msg: None)
    log(b"ok \xff\n")
    assert log.data == ["ok �\n"]
