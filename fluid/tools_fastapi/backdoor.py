from functools import partial

from fastapi import FastAPI

from fluid.utils.backdoor import AIO_BACKDOOR_PORT, ConsoleManager


def setup(app: FastAPI, port: int = AIO_BACKDOOR_PORT) -> None:
    console = ConsoleManager(port)
    app.add_event_handler("startup", partial(console.startup, app))
    app.add_event_handler("shutdown", console.shutdown)
