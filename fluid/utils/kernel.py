import asyncio
import sys
from typing import Any, Callable, Dict, Optional

KernelCallback = Callable[[bytes], None]
READ_LIMIT = 2**16  # 64 KiB
TERMINATE_GRACE_PERIOD = 5.0
"""Seconds to wait for a terminated process before killing it"""


async def run_python(*args: str, **kwargs) -> int:
    return await run(sys.executable, *args, **kwargs)


async def run(
    executable: str,
    *args: str,
    result_callback: Optional[KernelCallback] = None,
    error_callback: Optional[KernelCallback] = None,
    env: Optional[Dict[str, str]] = None,
    stream_output: bool = False,
    stream_error: bool = False,
    terminate_grace_period: float = TERMINATE_GRACE_PERIOD,
) -> int:
    """Run an executable in a subprocess and wait for it to finish.

    The process is terminated when this coroutine is cancelled, which is what
    happens when the caller times out, so the process cannot outlive the code
    waiting for it.
    """
    process = await asyncio.create_subprocess_exec(
        executable,
        *args,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        limit=READ_LIMIT,
        env=env,
    )
    if not process.stdout or not process.stderr:
        raise RuntimeError("Failed to create subprocess")
    readers = [
        asyncio.create_task(
            _read_stream(process.stdout, stream_output, result_callback or noop)
        ),
        asyncio.create_task(
            _read_stream(process.stderr, stream_error, error_callback or noop)
        ),
    ]
    try:
        await asyncio.wait(readers)
        return await process.wait()
    finally:
        for reader in readers:
            reader.cancel()
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
        if chunk:
            cb(chunk)
        else:
            break


def noop(data: bytes) -> None:
    pass


class EncodedLog:
    def __init__(self, log: Callable[[str], Any] = sys.stdout.write) -> None:
        self.log = log
        self.data: list[str] = []

    def __call__(self, data: bytes) -> None:
        msg = data.decode("utf-8")
        self.data.append(msg)
        self.log(msg)


class CollectBytes:
    data: bytes = b""

    def __call__(self, data: bytes) -> None:
        self.data += data
