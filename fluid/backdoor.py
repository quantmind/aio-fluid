import os
import sys
from functools import partial

import aioconsole
from aiohttp.web import Application

from .log import get_logger

logger = get_logger("backdoor")
AIO_BACKDOOR_PORT = os.environ.get("AIO_BACKDOOR_PORT", 8087)
CONSOLE_MESSAGE = """\
---
This console is running in the same asyncio event loop as the Service application.
Try: await asyncio.sleep(1, result=3)
---"""


def setup(app: Application, port: int = AIO_BACKDOOR_PORT) -> None:
    console = ConsoleManager(port)
    app.on_startup.append(console.on_startup)
    app.on_cleanup.insert(0, console.on_cleanup)
    app["console_manager"] = console


class Console(aioconsole.AsynchronousConsole):
    def get_default_banner(self):
        cprt = (
            'Type "help", "copyright", "credits" ' 'or "license" for more information.'
        )
        return f"Python {sys.version} on {sys.platform}\n{cprt}\n{CONSOLE_MESSAGE}"


class ConsoleManager:
    aio_console = None

    def __init__(self, port: int, host: str = "0.0.0.0") -> None:
        self.port = port
        self.host = host

    async def on_startup(self, app) -> None:
        self.aio_console = await aioconsole.start_interactive_server(
            partial(self._console, app), self.host, self.port
        )
        logger.warning("console running on port %i", self.port)

    async def on_cleanup(self, app) -> None:
        if self.aio_console:
            self.aio_console.close()
            await self.aio_console.wait_closed()
            self.aio_console = None

    def _console(self, app, streams=None) -> Console:
        return Console(locals={"app": app}, streams=streams)
