import logging
import sys
from dataclasses import dataclass
from functools import partial

import aioconsole

from fluid import settings

logger = logging.getLogger(__name__)
CONSOLE_MESSAGE = """\
---
This console is running in the same asyncio event loop as the Service application.
Try: await asyncio.sleep(1, result=3)
---"""


class Console(aioconsole.AsynchronousConsole):
    def get_default_banner(self):
        cprt = (
            'Type "help", "copyright", "credits" ' 'or "license" for more information.'
        )
        return f"Python {sys.version} on {sys.platform}\n{cprt}\n{CONSOLE_MESSAGE}"


@dataclass
class ConsoleManager:
    aio_console = None
    port: int = settings.AIO_BACKDOOR_PORT
    host: str = "0.0.0.0"

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
