import asyncio
import sys
from typing import Callable, Dict, Optional

KernelCallback = Callable[[bytes], None]
READ_LIMIT = 2 ** 16  # 64 KiB


async def run_python(*args: str) -> str:
    return await run(sys.executable, *args)


async def run(
    executable: str,
    *args: str,
    result_callback: Optional[KernelCallback] = None,
    error_callback: Optional[KernelCallback] = None,
    env: Optional[Dict[str, str]] = None,
    stream_output: bool = False,
    stream_error: bool = False,
):
    process = await asyncio.create_subprocess_exec(
        executable,
        *args,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        limit=READ_LIMIT,
        env=env,
    )
    await asyncio.wait(
        [
            _read_stream(process.stdout, stream_output, result_callback or noop),
            _read_stream(process.stderr, stream_error, error_callback or noop),
        ]
    )
    return await process.wait()


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
):
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
    def __init__(self, log: Callable[[str], None] = sys.stdout.write):
        self.log = log
        self.data = []

    def __call__(self, data: bytes) -> None:
        msg = data.decode("utf-8")
        self.data.append(msg)
        self.log(msg)


class CollectBytes:
    data: bytes = b""

    def __call__(self, data: bytes) -> None:
        self.data += data
