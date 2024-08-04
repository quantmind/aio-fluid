from functools import partial

from fastapi import FastAPI

from fluid.utils.backdoor import ConsoleManager


def setup(app: FastAPI) -> None:
    console = ConsoleManager()
    app.add_event_handler("startup", partial(console.on_startup, app))
    app.add_event_handler("shutdown", partial(console.on_cleanup, app))
