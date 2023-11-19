from aiohttp.web import Application

from fluid.tools.backdoor import AIO_BACKDOOR_PORT, ConsoleManager


def setup(app: Application, port: int = AIO_BACKDOOR_PORT) -> None:
    console = ConsoleManager(port)
    app.on_startup.append(console.on_startup)
    app.on_cleanup.insert(0, console.on_cleanup)
    app["console_manager"] = console
