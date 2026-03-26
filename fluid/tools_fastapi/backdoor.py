from functools import partial
from typing import Any

from fastapi import FastAPI

from fluid.utils.backdoor import ConsoleManager


def setup(app: FastAPI, **kwargs: Any) -> None:
    console = ConsoleManager(**kwargs)
    app.router.on_startup.append(partial(console.on_startup, app))
    app.router.on_shutdown.append(partial(console.on_cleanup, app))
