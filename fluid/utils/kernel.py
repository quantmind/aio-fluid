"""Run an external program from async code without blocking the event loop.

Output is delivered to callbacks as it arrives, and the process is terminated
when the coroutine waiting for it is cancelled.
"""

import asyncio
import codecs
import logging
import sys
from typing import Any, Callable, Dict, Optional

from typing_extensions import Annotated, Doc

KernelCallback = Callable[[bytes], None]
"""A callback receiving a chunk of the output of a process"""
logger = logging.getLogger(__name__)
READ_LIMIT = 2**16  # 64 KiB
TERMINATE_GRACE_PERIOD = 5.0
"""Seconds to wait for a terminated process before killing it"""


async def run_python(*args: str, **kwargs: Any) -> int:
    """Run the current python interpreter in a subprocess.

    A shortcut for [run][fluid.utils.kernel.run] with `sys.executable` as the
    executable, it accepts the same keyword arguments.
    """
    return await run(sys.executable, *args, **kwargs)


async def run(
    executable: Annotated[
        str,
        Doc("The program to run, a path or a name looked up in `PATH`."),
    ],
    *args: Annotated[
        str,
        Doc("Arguments passed to the program, no shell is involved."),
    ],
    result_callback: Annotated[
        Optional[KernelCallback],
        Doc("Called with the bytes of stdout, discarded when not given."),
    ] = None,
    error_callback: Annotated[
        Optional[KernelCallback],
        Doc("Called with the bytes of stderr, discarded when not given."),
    ] = None,
    env: Annotated[
        Optional[Dict[str, str]],
        Doc(
            "Environment of the process. It replaces the environment of the "
            "current process rather than extending it, pass "
            "`{**os.environ, ...}` to extend it. "
            "When not given the current environment is inherited."
        ),
    ] = None,
    stream_output: Annotated[
        bool,
        Doc(
            "Call `result_callback` once per line as it arrives. When false it "
            "is called once, with the whole of stdout, after the process "
            "closes it."
        ),
    ] = False,
    stream_error: Annotated[
        bool,
        Doc("The same as `stream_output`, for stderr and `error_callback`."),
    ] = False,
    terminate_grace_period: Annotated[
        float,
        Doc(
            "Seconds to wait after a SIGTERM before sending a SIGKILL, when the "
            "run is cancelled."
        ),
    ] = TERMINATE_GRACE_PERIOD,
    input: Annotated[  # noqa: A002, the name subprocess.run uses
        Optional[bytes],
        Doc(
            "Bytes written to the stdin of the process, which is closed once "
            "they are written. When not given stdin is `/dev/null`, so a "
            "process reading it sees an end of file rather than waiting forever."
        ),
    ] = None,
) -> int:
    """Run an executable in a subprocess and wait for it to finish.

    It returns the exit code of the process, negative when it was stopped by a
    signal. A non zero code is not raised as an error, check it.

    The process is terminated when this coroutine is cancelled, which is what
    happens when the caller times out, so the process cannot outlive the code
    waiting for it.
    """
    process = await asyncio.create_subprocess_exec(
        executable,
        *args,
        stdin=asyncio.subprocess.DEVNULL if input is None else asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        limit=READ_LIMIT,
        env=env,
    )
    if not process.stdout or not process.stderr:
        raise RuntimeError("Failed to create subprocess")
    tasks = [
        asyncio.create_task(
            _read_stream(process.stdout, stream_output, result_callback or noop)
        ),
        asyncio.create_task(
            _read_stream(process.stderr, stream_error, error_callback or noop)
        ),
    ]
    if input is not None and process.stdin:
        # written concurrently with the reads, a process filling its stdout
        # before it reads all of its input would otherwise deadlock
        tasks.append(asyncio.create_task(_write_stdin(process.stdin, input)))
    try:
        await asyncio.wait(tasks)
        return await process.wait()
    finally:
        for task in tasks:
            task.cancel()
        await terminate(process, terminate_grace_period)


async def terminate(
    process: asyncio.subprocess.Process,
    grace_period: float = TERMINATE_GRACE_PERIOD,
) -> None:
    """Terminate a process unless it has already exited.

    It sends a SIGTERM first and escalates to a SIGKILL when the process is
    still running after the grace period.
    """
    if process.returncode is not None:
        return
    process.terminate()
    try:
        await asyncio.wait_for(asyncio.shield(process.wait()), grace_period)
    except asyncio.TimeoutError:
        process.kill()
        await asyncio.shield(process.wait())


async def _write_stdin(stdin: asyncio.StreamWriter, data: bytes) -> None:
    try:
        stdin.write(data)
        await stdin.drain()
    except (BrokenPipeError, ConnectionResetError):
        # the process exited without reading all of its input
        pass
    finally:
        stdin.close()


async def _read_line(stream: asyncio.StreamReader) -> bytes:
    chunks = []
    while True:
        try:
            chunk = await stream.readuntil()
        except asyncio.IncompleteReadError as e:
            chunks.append(e.partial)
            break
        except asyncio.LimitOverrunError:
            chunk = await stream.readexactly(READ_LIMIT)
            chunks.append(chunk)
        else:
            chunks.append(chunk)
            break
    return b"".join(chunks)


async def _read_stream(
    stream: asyncio.StreamReader, stream_output: bool, cb: KernelCallback
) -> None:
    while True:
        if stream_output:
            chunk = await _read_line(stream)
        else:
            chunk = await stream.read()
        if not chunk:
            break
        try:
            cb(chunk)
        except Exception:
            # keep draining the pipe, a reader which stops would leave the
            # process blocked on a full pipe until it times out
            logger.exception("subprocess output callback failed")


def noop(data: bytes) -> None:
    pass


class Utf8Decoder:
    """Decode a stream of bytes chunks as utf-8.

    A character split across two chunks is decoded once both have arrived,
    and invalid bytes are replaced rather than raising.
    """

    def __init__(self) -> None:
        self._decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")

    def decode(self, data: bytes) -> str:
        return self._decoder.decode(data)


class EncodedLog:
    """A callback decoding the output as utf-8 and passing it to a log function.

    The decoded text is also kept in `data`.
    """

    def __init__(self, log: Callable[[str], Any] = sys.stdout.write) -> None:
        self.log = log
        self.data: list[str] = []
        self._decoder = Utf8Decoder()

    def __call__(self, data: bytes) -> None:
        msg = self._decoder.decode(data)
        self.data.append(msg)
        self.log(msg)


class CollectBytes:
    """A callback collecting the output in `data`"""

    data: bytes = b""

    def __call__(self, data: bytes) -> None:
        self.data += data
