from functools import partial
from typing import Any

from fastapi import FastAPI

from fluid.utils.backdoor import ConsoleManager


def setup(app: FastAPI, **kwargs: Any) -> None:
    console = ConsoleManager(**kwargs)
    app.add_event_handler("startup", partial(console.on_startup, app))
    app.add_event_handler("shutdown", partial(console.on_cleanup, app))
