import asyncio

import pytest

from fluid.utils.backdoor import CONSOLE_MESSAGE, ConsoleManager

pytestmark = pytest.mark.asyncio(loop_scope="module")

PROMPT = b">>> "


async def test_console_manager() -> None:
    app = {"name": "my-app"}
    manager = ConsoleManager(port=0)
    await manager.on_startup(app)
    assert manager.aio_console is not None
    port = manager.aio_console.sockets[0].getsockname()[1]
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        banner = await asyncio.wait_for(reader.readuntil(PROMPT), 5)
        assert CONSOLE_MESSAGE.encode() in banner
        assert b"Python " in banner
        writer.write(b"app['name']\n")
        await writer.drain()
        output = await asyncio.wait_for(reader.readuntil(PROMPT), 5)
        assert b"'my-app'" in output
    finally:
        writer.close()
        await writer.wait_closed()
    await manager.on_cleanup(app)
    assert manager.aio_console is None
    # cleanup is idempotent
    await manager.on_cleanup(app)
